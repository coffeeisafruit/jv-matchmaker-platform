#!/usr/bin/env python3
"""
Batch email scraper — uses ContactScraper to find emails from profile websites.

Queries Supabase for profiles missing email, scrapes their websites in parallel,
writes results (primary email, secondary emails, phone, booking link) to DB.

Usage:
    python scripts/scrape_emails.py --concurrency 8
    python scripts/scrape_emails.py --dry-run --limit 20
    python scripts/scrape_emails.py --concurrency 5 --limit 100
"""
import os, sys, argparse, time, logging, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django; django.setup()
import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
load_dotenv()

from matching.enrichment.contact_scraper import ContactScraper

DATABASE_URL = os.environ['DATABASE_URL']
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# Initialized in main() after we know concurrency
db_pool = None

# ── DB queries ────────────────────────────────────────────────────────

def get_profiles_needing_email(limit: int = 5000, offset: int = 0):
    """Get profiles with websites but no email."""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT id, name, company, website, linkedin, email,
               secondary_emails, phone, booking_link
        FROM profiles
        WHERE (email IS NULL OR email = '')
          AND website IS NOT NULL AND website != ''
          AND website LIKE 'http%%'
          AND name IS NOT NULL AND name != ''
        ORDER BY name
        OFFSET %s
        LIMIT %s
    """, (offset, limit))
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def write_result(profile_id: str, profile: dict, result: dict):
    """Write scraped contact info to Supabase."""
    conn = db_pool.getconn()
    try:
        cur = conn.cursor()
        now = datetime.now()

        updates = []
        values = []
        fields_written = []

        # Primary email
        email = result.get('email')
        existing_email = (profile.get('email') or '').strip()
        if email and not existing_email:
            updates.append("email = %s")
            values.append(email)
            fields_written.append('email')

        # Secondary emails
        secondary = result.get('secondary_emails', [])
        if email and existing_email and email.lower() != existing_email.lower():
            # Primary slot taken — our "best" email becomes secondary
            secondary = [email] + secondary

        existing_secondary = profile.get('secondary_emails') or []
        existing_lower = {e.lower() for e in existing_secondary}
        existing_lower.add(existing_email.lower() if existing_email else '')

        new_secondary = []
        for sec_email in secondary:
            if sec_email.lower() not in existing_lower:
                new_secondary.append(sec_email)
                existing_lower.add(sec_email.lower())
                fields_written.append('secondary_email')

        if new_secondary:
            # Single update: concatenate new emails array onto existing
            updates.append(
                "secondary_emails = COALESCE(secondary_emails, '{}') || %s::text[]"
            )
            values.append(new_secondary)

        # Phone
        phone = result.get('phone')
        if phone and not (profile.get('phone') or '').strip():
            updates.append("phone = %s")
            values.append(phone)
            fields_written.append('phone')

        # Booking link
        booking = result.get('booking_link')
        if booking and not (profile.get('booking_link') or '').strip():
            updates.append("booking_link = %s")
            values.append(booking)
            fields_written.append('booking_link')

        # LinkedIn
        linkedin = result.get('linkedin')
        if linkedin and not (profile.get('linkedin') or '').strip():
            updates.append("linkedin = %s")
            values.append(linkedin)
            fields_written.append('linkedin')

        if not updates:
            cur.close()
            return fields_written

        # Provenance stamping
        updates.append("""
            enrichment_metadata = COALESCE(enrichment_metadata, '{}'::jsonb)
                || jsonb_build_object(
                    'field_meta',
                    COALESCE(enrichment_metadata->'field_meta', '{}'::jsonb)
                        || jsonb_build_object('email', jsonb_build_object(
                            'source', 'website_scrape',
                            'updated_at', %s,
                            'pipeline_version', 4
                        ))
                )
        """)
        values.append(now.isoformat())

        updates.append("updated_at = %s")
        values.append(now)
        values.append(profile_id)

        sql = f"UPDATE profiles SET {', '.join(updates)} WHERE id = %s"
        cur.execute(sql, values)
        conn.commit()
        cur.close()
        return fields_written
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)


# ── Main ──────────────────────────────────────────────────────────────

lock = threading.Lock()
stats = {
    'processed': 0, 'emails_found': 0, 'secondary_found': 0,
    'phones_found': 0, 'bookings_found': 0,
    'no_contact': 0, 'errors': 0,
}


def process_one(profile, scraper):
    """Scrape one profile's website for contact info."""
    name = profile['name']
    website = profile['website']
    company = profile.get('company') or ''

    try:
        result = scraper.scrape_contact_info(website, name, company)

        if result['email'] or result['secondary_emails'] or result['phone'] or result['booking_link']:
            fields = write_result(profile['id'], profile, result)

            with lock:
                stats['processed'] += 1
                if 'email' in fields:
                    stats['emails_found'] += 1
                stats['secondary_found'] += sum(1 for f in fields if f == 'secondary_email')
                if 'phone' in fields:
                    stats['phones_found'] += 1
                if 'booking_link' in fields:
                    stats['bookings_found'] += 1

                tag = []
                if result['email']:
                    tag.append(f"email={result['email'][:30]}")
                if result['secondary_emails']:
                    tag.append(f"+{len(result['secondary_emails'])} sec")
                if result['phone']:
                    tag.append("phone")
                if result['booking_link']:
                    tag.append("booking")
                print(f"  OK  {name:35s} {' | '.join(tag)}")
        else:
            with lock:
                stats['processed'] += 1
                stats['no_contact'] += 1

    except Exception as e:
        with lock:
            stats['processed'] += 1
            stats['errors'] += 1
            logger.warning(f"Error scraping {name} ({website}): {e}")


def main():
    parser = argparse.ArgumentParser(description='Scrape websites for email/contact info')
    parser.add_argument('--concurrency', type=int, default=8,
                        help='Parallel workers (default: 8)')
    parser.add_argument('--limit', type=int, default=5000,
                        help='Max profiles to process')
    parser.add_argument('--offset', type=int, default=0,
                        help='Skip first N profiles (for sharding across terminals)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview without scraping')
    args = parser.parse_args()

    profiles = get_profiles_needing_email(args.limit, args.offset)

    print(f"\n{'='*60}")
    print("EMAIL SCRAPER — Contact Page Discovery")
    print(f"{'='*60}")
    print(f"Profiles to scrape:  {len(profiles)}")
    if args.offset:
        print(f"Offset:              {args.offset} (skipped first {args.offset})")
    print(f"Concurrency:         {args.concurrency}")
    print(f"Method:              Playwright + smart link discovery")
    print(f"Cost:                $0.00")

    if not profiles:
        print("\nNo profiles need email scraping.")
        return

    # Initialize connection pool sized to concurrency
    global db_pool
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=2, maxconn=args.concurrency + 2, dsn=DATABASE_URL,
    )

    if args.dry_run:
        print(f"\nDRY RUN — showing first 20:")
        for p in profiles[:20]:
            print(f"  {p['name']:35s} {p['website'][:50]}")
        if len(profiles) > 20:
            print(f"  ... and {len(profiles) - 20} more")
        return

    print()
    scraper = ContactScraper()
    start = time.time()

    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = [executor.submit(process_one, p, scraper) for p in profiles]

        done = 0
        for future in as_completed(futures):
            future.result()
            done += 1
            if done % 50 == 0:
                elapsed = time.time() - start
                rate = done / elapsed * 60 if elapsed > 0 else 0
                print(f"\n  [{done}/{len(profiles)}] "
                      f"{stats['emails_found']} emails | "
                      f"{stats['secondary_found']} secondary | "
                      f"{stats['phones_found']} phones | "
                      f"{stats['errors']} errors | "
                      f"{rate:.0f} profiles/min\n")

    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print("SCRAPING COMPLETE")
    print(f"{'='*60}")
    print(f"Processed:         {stats['processed']}")
    print(f"Primary emails:    {stats['emails_found']}")
    print(f"Secondary emails:  {stats['secondary_found']}")
    print(f"Phones:            {stats['phones_found']}")
    print(f"Booking links:     {stats['bookings_found']}")
    print(f"No contact found:  {stats['no_contact']}")
    print(f"Errors:            {stats['errors']}")
    print(f"Time:              {elapsed:.0f}s ({stats['processed']/elapsed*60:.0f} profiles/min)")

    db_pool.closeall()


if __name__ == '__main__':
    main()
