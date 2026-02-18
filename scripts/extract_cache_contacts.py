#!/usr/bin/env python3
"""
Extract email/phone/booking_link from research cache files
and write them to Supabase where the DB is missing that data.

Quick win — no scraping needed, just mining existing cached data.

Usage:
    python scripts/extract_cache_contacts.py
    python scripts/extract_cache_contacts.py --dry-run
"""
import os, sys, json, glob
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django; django.setup()
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from datetime import datetime
import argparse
import re

load_dotenv()

DATABASE_URL = os.environ['DATABASE_URL']
CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         'Chelsea_clients', 'research_cache')

EMAIL_RE = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')


def main():
    parser = argparse.ArgumentParser(description='Extract contacts from cache')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    # Load all cache files
    cache_files = glob.glob(os.path.join(CACHE_DIR, '*.json'))
    print(f"Cache files found: {len(cache_files)}")

    # Build name→cache data mapping
    cache_contacts = {}
    for path in cache_files:
        try:
            with open(path) as f:
                data = json.load(f)
            name = data.get('name', '').strip()
            if not name:
                continue

            entry = {}
            email = data.get('email', '').strip() if data.get('email') else ''
            phone = data.get('phone', '').strip() if data.get('phone') else ''
            booking = data.get('booking_link', '').strip() if data.get('booking_link') else ''

            # Validate email
            if email and EMAIL_RE.match(email):
                entry['email'] = email
            # Validate phone (basic: at least 7 digits)
            if phone and len(re.sub(r'\D', '', phone)) >= 7:
                # Skip non-phone strings
                if not any(x in phone.lower() for x in ['provided', 'available', 'none', 'n/a', 'mon ', 'tue ']):
                    entry['phone'] = phone
            if booking and booking.startswith('http'):
                entry['booking_link'] = booking

            if entry:
                cache_contacts[name.lower()] = {**entry, 'name': name}
        except Exception:
            continue

    print(f"Cache entries with contact info: {len(cache_contacts)}")
    print(f"  With email: {sum(1 for v in cache_contacts.values() if 'email' in v)}")
    print(f"  With phone: {sum(1 for v in cache_contacts.values() if 'phone' in v)}")
    print(f"  With booking: {sum(1 for v in cache_contacts.values() if 'booking_link' in v)}")

    # Get DB profiles missing contact info
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT id, name, email, phone, booking_link, secondary_emails
        FROM profiles
        WHERE name IS NOT NULL AND name != ''
    """)
    db_profiles = {r['name'].lower().strip(): dict(r) for r in cur.fetchall()}
    print(f"\nDB profiles: {len(db_profiles)}")

    # Cross-reference
    new_emails = 0
    new_secondary = 0
    new_phones = 0
    new_bookings = 0
    now = datetime.now()

    for name_lower, cache_data in cache_contacts.items():
        db_profile = db_profiles.get(name_lower)
        if not db_profile:
            continue

        pid = db_profile['id']
        updates = []
        values = []

        # Email
        cache_email = cache_data.get('email')
        if cache_email:
            db_email = (db_profile.get('email') or '').strip()
            if not db_email:
                updates.append("email = %s")
                values.append(cache_email)
                new_emails += 1
            elif cache_email.lower() != db_email.lower():
                # Add as secondary if different
                existing_secondary = db_profile.get('secondary_emails') or []
                if cache_email.lower() not in [e.lower() for e in existing_secondary] \
                   and cache_email.lower() != db_email.lower():
                    updates.append(
                        "secondary_emails = array_append("
                        "COALESCE(secondary_emails, '{}'), %s)"
                    )
                    values.append(cache_email)
                    new_secondary += 1

        # Phone
        cache_phone = cache_data.get('phone')
        if cache_phone and not (db_profile.get('phone') or '').strip():
            updates.append("phone = %s")
            values.append(cache_phone)
            new_phones += 1

        # Booking link
        cache_booking = cache_data.get('booking_link')
        if cache_booking and not (db_profile.get('booking_link') or '').strip():
            updates.append("booking_link = %s")
            values.append(cache_booking)
            new_bookings += 1

        if updates and not args.dry_run:
            updates.append("updated_at = %s")
            values.append(now)
            values.append(pid)
            sql = f"UPDATE profiles SET {', '.join(updates)} WHERE id = %s"
            cur.execute(sql, values)

    if not args.dry_run:
        conn.commit()

    cur.close()
    conn.close()

    print(f"\n{'='*60}")
    print(f"CACHE EXTRACTION {'(DRY RUN)' if args.dry_run else 'COMPLETE'}")
    print(f"{'='*60}")
    print(f"New primary emails:    {new_emails}")
    print(f"New secondary emails:  {new_secondary}")
    print(f"New phones:            {new_phones}")
    print(f"New booking links:     {new_bookings}")


if __name__ == '__main__':
    main()
