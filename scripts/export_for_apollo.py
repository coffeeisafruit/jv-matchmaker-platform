#!/usr/bin/env python3
"""
Export profiles with Tier 1-2 gaps to CSV for Apollo.io web UI upload.

Generates a CSV with columns Apollo uses for matching:
first_name, last_name, company, domain, linkedin_url, email

Profiles already fully enriched (have email + phone + linkedin) are skipped
to save Apollo credits.

Usage:
    python scripts/export_for_apollo.py                    # Export all with gaps
    python scripts/export_for_apollo.py --limit 100        # Export first 100
    python scripts/export_for_apollo.py --missing-email    # Only profiles missing email
"""

import os
import sys
import csv
import argparse
from datetime import datetime

# Django setup
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django
django.setup()

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

from matching.enrichment.apollo_enrichment import extract_domain, split_name

load_dotenv()

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set", file=sys.stderr)
    sys.exit(1)


def export_profiles(
    output_file: str,
    limit: int = None,
    missing_email: bool = False,
    missing_any_contact: bool = True,
):
    """Export profiles with gaps to Apollo-compatible CSV."""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Build WHERE clause based on gaps
    conditions = []
    if missing_email:
        conditions.append("(email IS NULL OR email = '')")
    elif missing_any_contact:
        conditions.append(
            "(email IS NULL OR email = '' "
            "OR phone IS NULL OR phone = '' "
            "OR linkedin IS NULL OR linkedin = '')"
        )

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    limit_clause = f"LIMIT {limit}" if limit else ""

    query = f"""
        SELECT id, name, email, company, website, linkedin, phone,
               enrichment_metadata
        FROM profiles
        {where_clause}
        ORDER BY name
        {limit_clause}
    """

    cur.execute(query)
    profiles = cur.fetchall()

    cur.close()
    conn.close()

    # Write Apollo-compatible CSV
    fieldnames = [
        'profile_id',       # Our internal reference (Apollo ignores this)
        'first_name',
        'last_name',
        'company',
        'domain',
        'linkedin_url',
        'email',
        # Additional context for review
        'has_email',
        'has_phone',
        'has_linkedin',
    ]

    rows_written = 0
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for profile in profiles:
            first_name, last_name = split_name(profile.get('name') or '')
            domain = extract_domain(profile.get('website') or '')

            writer.writerow({
                'profile_id': str(profile['id']),
                'first_name': first_name,
                'last_name': last_name,
                'company': profile.get('company') or '',
                'domain': domain or '',
                'linkedin_url': profile.get('linkedin') or '',
                'email': profile.get('email') or '',
                'has_email': 'yes' if profile.get('email') else 'no',
                'has_phone': 'yes' if profile.get('phone') else 'no',
                'has_linkedin': 'yes' if profile.get('linkedin') else 'no',
            })
            rows_written += 1

    return rows_written, len(profiles)


def main():
    parser = argparse.ArgumentParser(
        description='Export profiles with gaps to CSV for Apollo.io upload'
    )
    parser.add_argument('--output', '-o', default=None,
                        help='Output CSV file path')
    parser.add_argument('--limit', type=int, default=None,
                        help='Maximum profiles to export')
    parser.add_argument('--missing-email', action='store_true',
                        help='Only export profiles missing email')

    args = parser.parse_args()

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = args.output or f'exports/apollo_upload_{timestamp}.csv'

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)

    print(f"\n{'='*60}")
    print("APOLLO.IO CSV EXPORT")
    print(f"{'='*60}\n")

    rows_written, total_with_gaps = export_profiles(
        output_file=output_file,
        limit=args.limit,
        missing_email=args.missing_email,
    )

    print(f"Profiles with contact gaps: {total_with_gaps}")
    print(f"Exported to: {output_file}")
    print(f"Rows written: {rows_written}")
    print(f"\nNext steps:")
    print(f"  1. Upload {output_file} to Apollo.io web app")
    print(f"  2. Select enrichment options (email, phone, company data)")
    print(f"  3. Download enriched CSV from Apollo")
    print(f"  4. Run: python scripts/import_apollo_csv.py <enriched_csv>")
    print(f"\n{'='*60}\n")


if __name__ == '__main__':
    main()
