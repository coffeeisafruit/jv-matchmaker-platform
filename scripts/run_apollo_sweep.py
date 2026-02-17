#!/usr/bin/env python3
"""
Apollo.io API Sweep — Enrich profiles with contact data via API.

Uses the ApolloEnrichmentService to batch-enrich profiles that have
Tier 1-2 gaps (missing email, phone, or linkedin). Leverages existing
profile data (name, company, website, linkedin) for better match rates.

Rate-limit aware:
- Batches of 10 (Apollo max per request)
- 1-second delay between batches (stays under 200/min paid limit)
- Daily limit tracking with auto-pause at 2,000 calls/day
- Exponential backoff on 429 responses

Usage:
    python scripts/run_apollo_sweep.py --limit 10 --dry-run    # Preview
    python scripts/run_apollo_sweep.py --limit 50               # Small test
    python scripts/run_apollo_sweep.py --max-credits 4000       # Full sweep
"""

import os
import sys
import json
import argparse
import time
from datetime import datetime
from typing import Dict, List

# Django setup
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django
django.setup()

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

from matching.enrichment.apollo_enrichment import (
    ApolloEnrichmentService,
    APOLLO_SOURCE,
    validate_email,
)

load_dotenv()

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set", file=sys.stderr)
    sys.exit(1)


def get_profiles_with_gaps(limit: int = None) -> List[Dict]:
    """Query profiles that have Tier 1-2 gaps Apollo can fill."""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    limit_clause = f"LIMIT {limit}" if limit else ""

    cur.execute(f"""
        SELECT id, name, email, phone, company, website, linkedin,
               enrichment_metadata
        FROM profiles
        WHERE (email IS NULL OR email = ''
               OR phone IS NULL OR phone = ''
               OR linkedin IS NULL OR linkedin = '')
        ORDER BY
            CASE WHEN email IS NULL OR email = '' THEN 0 ELSE 1 END,
            CASE WHEN linkedin IS NULL OR linkedin = '' THEN 0 ELSE 1 END,
            name
        {limit_clause}
    """)

    profiles = cur.fetchall()
    cur.close()
    conn.close()
    return profiles


def write_results(results: List[Dict], dry_run: bool = False) -> Dict:
    """Write Apollo enrichment results to Supabase."""
    from scripts.automated_enrichment_pipeline_safe import SOURCE_PRIORITY

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    stats = {
        'updated': 0,
        'skipped': 0,
        'errors': 0,
        'fields': {},
    }

    apollo_priority = SOURCE_PRIORITY.get(APOLLO_SOURCE, 30)

    for result in results:
        if result.get('error'):
            stats['errors'] += 1
            continue

        profile_id = result.get('_profile_id')
        if not profile_id:
            stats['errors'] += 1
            continue

        # Get current profile for source priority checks
        cur.execute(
            "SELECT id, email, phone, linkedin, website, company, "
            "enrichment_metadata FROM profiles WHERE id = %s",
            (profile_id,)
        )
        profile = cur.fetchone()
        if not profile:
            stats['errors'] += 1
            continue

        existing_meta = profile.get('enrichment_metadata') or {}
        if isinstance(existing_meta, str):
            try:
                existing_meta = json.loads(existing_meta)
            except (json.JSONDecodeError, TypeError):
                existing_meta = {}

        def should_write(field: str, new_value) -> bool:
            if not new_value:
                return False
            field_info = existing_meta.get('field_meta', {}).get(field, {})
            existing_source = field_info.get('source', 'unknown')
            existing_priority = SOURCE_PRIORITY.get(existing_source, 0)
            if apollo_priority < existing_priority:
                return False
            if apollo_priority > existing_priority:
                return True
            current = profile.get(field)
            return not current or (isinstance(current, str) and not current.strip())

        set_parts = []
        params = []
        fields_written = []

        # Write all extracted fields
        column_fields = [
            'email', 'linkedin', 'website', 'phone', 'company',
            'business_size', 'revenue_tier', 'service_provided',
            'niche', 'avatar_url',
        ]

        for field in column_fields:
            value = result.get(field)
            if value and should_write(field, value):
                set_parts.append(
                    sql.SQL("{} = %s").format(sql.Identifier(field))
                )
                params.append(value)
                fields_written.append(field)
                stats['fields'][field] = stats['fields'].get(field, 0) + 1

        if not set_parts and not result.get('_apollo_data'):
            stats['skipped'] += 1
            continue

        # Update enrichment_metadata with apollo_data
        meta = dict(existing_meta)
        meta['apollo_data'] = result.get('_apollo_data', {})
        meta['last_apollo_enrichment'] = datetime.now().isoformat()

        now_iso = datetime.now().isoformat()
        field_meta = meta.get('field_meta', {})
        for f in fields_written:
            field_meta[f] = {
                'source': APOLLO_SOURCE,
                'updated_at': now_iso,
                'pipeline_version': 1,
            }
        meta['field_meta'] = field_meta

        set_parts.append(sql.SQL("enrichment_metadata = %s::jsonb"))
        params.append(json.dumps(meta))
        set_parts.append(sql.SQL("last_enriched_at = %s"))
        params.append(datetime.now())
        set_parts.append(sql.SQL("updated_at = %s"))
        params.append(datetime.now())

        if dry_run:
            name = result.get('_apollo_data', {}).get('apollo_id', profile_id)
            print(f"  Would update {profile_id}: {fields_written}")
        else:
            update_query = sql.SQL(
                "UPDATE profiles SET {} WHERE id = %s"
            ).format(sql.SQL(", ").join(set_parts))
            params.append(profile_id)
            cur.execute(update_query, params)
            stats['updated'] += 1

    if not dry_run:
        conn.commit()

    cur.close()
    conn.close()
    return stats


def run_sweep(
    limit: int = None,
    max_credits: int = 4000,
    dry_run: bool = False,
    batch_delay: float = 1.0,
    webhook_url: str = None,
):
    """Run Apollo enrichment sweep."""
    print(f"\n{'='*60}")
    print("APOLLO.IO API SWEEP")
    print(f"{'='*60}\n")

    # Get profiles with gaps
    profiles = get_profiles_with_gaps(limit)
    print(f"Profiles with contact gaps: {len(profiles)}")
    print(f"Max credits: {max_credits}")
    print(f"Batch delay: {batch_delay}s")
    print(f"Dry run: {dry_run}")
    print()

    if not profiles:
        print("No profiles need enrichment.")
        return

    api_key = os.environ.get('APOLLO_API_KEY', '')
    if not api_key and not dry_run:
        print("ERROR: APOLLO_API_KEY not set", file=sys.stderr)
        sys.exit(1)

    service = ApolloEnrichmentService(api_key=api_key, webhook_url=webhook_url)

    all_results = []
    credits_used = 0
    batch_size = 10
    start_time = time.time()

    for batch_start in range(0, len(profiles), batch_size):
        batch = profiles[batch_start:batch_start + batch_size]
        batch_num = (batch_start // batch_size) + 1
        total_batches = (len(profiles) + batch_size - 1) // batch_size

        # Check credit limit
        if credits_used + len(batch) > max_credits:
            remaining = max_credits - credits_used
            if remaining <= 0:
                print(f"\n  Credit limit reached ({credits_used}/{max_credits}). Stopping.")
                break
            batch = batch[:remaining]

        print(f"  Batch {batch_num}/{total_batches} ({len(batch)} profiles): ", end='', flush=True)

        if dry_run:
            for p in batch:
                service_result = {
                    '_profile_id': p['id'],
                    '_apollo_data': {'dry_run': True},
                }
                needs = service.needs_enrichment(p)
                all_results.append(service_result)
            print(f"[dry-run] {len(batch)} profiles would be queried")
        else:
            # Convert psycopg2 RealDictRow to plain dict
            batch_dicts = [dict(p) for p in batch]
            results = service.enrich_batch(batch_dicts)

            # Handle rate limiting
            for r in results:
                if r.get('error') == 'rate_limited':
                    retry_after = r.get('retry_after', 60)
                    print(f"\n  Rate limited! Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    # Retry this batch
                    results = service.enrich_batch(batch_dicts)
                    break

            emails_found = sum(1 for r in results if r.get('email'))
            phones_found = sum(1 for r in results if r.get('phone'))
            linkedin_found = sum(1 for r in results if r.get('linkedin'))
            errors = sum(1 for r in results if r.get('error'))

            print(f"email:{emails_found} phone:{phones_found} linkedin:{linkedin_found} err:{errors}")

            all_results.extend(results)
            credits_used += len(batch)

        # Rate limit delay between batches
        if batch_start + batch_size < len(profiles):
            time.sleep(batch_delay)

    elapsed = time.time() - start_time

    # Write results to database
    print(f"\nWriting {len(all_results)} results to Supabase...")
    write_stats = write_results(all_results, dry_run=dry_run)

    # Generate report
    report = {
        'timestamp': datetime.now().isoformat(),
        'profiles_queried': len(profiles),
        'credits_used': credits_used,
        'runtime_seconds': round(elapsed, 1),
        'results': {
            'updated': write_stats['updated'],
            'skipped': write_stats['skipped'],
            'errors': write_stats['errors'],
        },
        'fields_written': write_stats.get('fields', {}),
        'rate_limit_remaining': service.last_rate_limit_remaining,
    }

    # Save report
    os.makedirs('reports', exist_ok=True)
    report_file = f"reports/apollo_sweep_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)

    # Print summary
    print(f"\n{'='*60}")
    print("SWEEP SUMMARY")
    print(f"{'='*60}\n")
    print(f"Profiles queried:  {len(profiles)}")
    print(f"Credits used:      {credits_used}")
    print(f"Runtime:           {elapsed:.1f}s")
    print(f"Updated:           {write_stats['updated']}")
    print(f"Skipped:           {write_stats['skipped']}")
    print(f"Errors:            {write_stats['errors']}")
    if write_stats.get('fields'):
        print(f"\nFields written:")
        for field, count in sorted(write_stats['fields'].items(), key=lambda x: -x[1]):
            print(f"  {field:25s} {count:5d}")
    print(f"\nReport: {report_file}")
    print(f"Cost:   ~${credits_used * 0.10:.2f} (estimated at $0.10/credit)")
    print(f"\n{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(
        description='Apollo.io API sweep for profile enrichment'
    )
    parser.add_argument('--limit', type=int, default=None,
                        help='Maximum profiles to process')
    parser.add_argument('--max-credits', type=int, default=4000,
                        help='Maximum Apollo credits to use (default: 4000)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without API calls')
    parser.add_argument('--batch-delay', type=float, default=1.0,
                        help='Delay between batches in seconds (default: 1.0)')
    parser.add_argument('--webhook-url', default=None,
                        help='Webhook URL for async phone/email delivery')

    args = parser.parse_args()

    run_sweep(
        limit=args.limit,
        max_credits=args.max_credits,
        dry_run=args.dry_run,
        batch_delay=args.batch_delay,
        webhook_url=args.webhook_url,
    )


if __name__ == '__main__':
    main()
