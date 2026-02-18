#!/usr/bin/env python3
"""
Import Apollo.io enriched CSV back into Supabase.

Maps ALL Apollo CSV columns to SupabaseProfile fields by tier,
with overflow stored in enrichment_metadata.apollo_data JSONB.

Respects source priority: Apollo (30) never overwrites Exa (50) or client data (90+).

Usage:
    python scripts/import_apollo_csv.py enriched_apollo.csv --dry-run
    python scripts/import_apollo_csv.py enriched_apollo.csv
    python scripts/import_apollo_csv.py enriched_apollo.csv --match-by name  # if no profile_id column
"""

import os
import sys
import csv
import json
import argparse
import re
from datetime import datetime
from typing import Dict, List, Optional

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
    APOLLO_SOURCE,
    map_employee_count,
    map_annual_revenue,
    validate_email,
    validate_url,
)

load_dotenv()

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set", file=sys.stderr)
    sys.exit(1)

# Apollo CSV column → our field mapping
# Apollo exports use these column headers (may vary by export config)
APOLLO_CSV_MAP = {
    # Tier 1 fields
    'Email': 'email',
    'email': 'email',
    'Person Linkedin Url': 'linkedin',
    'LinkedIn URL': 'linkedin',
    'linkedin_url': 'linkedin',
    'Website': 'website',
    'Company Website': 'website',
    'website': 'website',
    # Tier 2 fields — phone (multiple Apollo export formats)
    'Phone': 'phone',
    'Mobile Phone': 'phone',
    'phone': 'phone',
    'Enriched Mobile Phone': '_phone_mobile',
    'Enriched Work Direct Phone': '_phone_work',
    'Enriched Home Phone': '_phone_home',
    'Enriched Other Phone': '_phone_other',
    'Enriched Corporate Phone': '_phone_corporate',
    'Company Phone': '_phone_company',
    'Company': 'company',
    'Organization Name': 'company',
    'Company Name': 'company',
    'company': 'company',
    'Title': 'service_provided',
    'title': 'service_provided',
    # Tier 3 fields
    'Industry': 'niche',
    'industry': 'niche',
    'Photo Url': 'avatar_url',
    'photo_url': 'avatar_url',
    # Mapping fields (need post-processing)
    '# Employees': '_employee_count',
    'Number of Employees': '_employee_count',
    'estimated_num_employees': '_employee_count',
    'Annual Revenue': '_annual_revenue',
    'annual_revenue': '_annual_revenue',
    # Raw storage fields → enrichment_metadata.apollo_data
    'Seniority': '_seniority',
    'seniority': '_seniority',
    'City': '_city',
    'city': '_city',
    'State': '_state',
    'state': '_state',
    'Country': '_country',
    'country': '_country',
    'Person Id': '_apollo_id',
    'apollo_id': '_apollo_id',
    'Email Status': '_email_status',
    'email_status': '_email_status',
    'Headline': '_headline',
    'headline': '_headline',
    'Departments': '_departments',
    'departments': '_departments',
    'Twitter Url': '_twitter_url',
    'twitter_url': '_twitter_url',
    'Facebook Url': '_facebook_url',
    'facebook_url': '_facebook_url',
    'Github Url': '_github_url',
    'github_url': '_github_url',
    'Company Linkedin Url': '_org_linkedin_url',
    'org_linkedin_url': '_org_linkedin_url',
    'Company Twitter Url': '_org_twitter_url',
    'Company Facebook Url': '_org_facebook_url',
    'Founded Year': '_founded_year',
    'founded_year': '_founded_year',
    'Company Founded Year': '_founded_year',
    'Company Street': '_company_street',
    'Company City': '_company_city',
    'Company Postal Code': '_company_postal_code',
    'Company State': '_company_state',
    'Company Country': '_company_country',
    'Result': '_match_result',
    'Company Short Description': '_org_short_description',
    'Keywords': '_org_keywords',
    'Total Funding': '_total_funding',
    'Email Confidence': '_email_confidence',
}

# Our reference fields (not from Apollo)
REFERENCE_FIELDS = {'profile_id', 'first_name', 'last_name', 'First Name', 'Last Name',
                     'has_email', 'has_phone', 'has_linkedin', 'domain'}


def parse_apollo_row(row: Dict, column_map: Dict) -> Dict:
    """Parse a single Apollo CSV row into our field structure."""
    result = {'_source': APOLLO_SOURCE}

    # Map known columns
    for csv_col, our_field in column_map.items():
        value = row.get(csv_col, '').strip()
        if value:
            result[our_field] = value

    # Post-processing: resolve best phone from multiple Apollo phone columns
    # Priority: mobile > work direct > home > other > corporate > company
    phone_priority = ['_phone_mobile', '_phone_work', '_phone_home',
                      '_phone_other', '_phone_corporate', '_phone_company']
    all_phones = {}
    for pf in phone_priority:
        val = result.pop(pf, None)
        if val:
            all_phones[pf] = val
    if not result.get('phone') and all_phones:
        # Pick first available by priority
        for pf in phone_priority:
            if pf in all_phones:
                result['phone'] = all_phones[pf]
                break
    # Store all phones in apollo_data for reference
    if all_phones:
        result.setdefault('_all_phones', all_phones)

    # Post-processing: employee count → business_size
    emp_count = result.pop('_employee_count', None)
    if emp_count:
        try:
            business_size = map_employee_count(int(emp_count))
            if business_size:
                result['business_size'] = business_size
        except (ValueError, TypeError):
            pass

    # Post-processing: annual revenue → revenue_tier
    revenue = result.pop('_annual_revenue', None)
    if revenue:
        try:
            # Apollo may export as "$1M - $10M" or as a number
            revenue_num = re.sub(r'[^0-9.]', '', str(revenue))
            if revenue_num:
                tier = map_annual_revenue(float(revenue_num))
                if tier:
                    result['revenue_tier'] = tier
        except (ValueError, TypeError):
            pass

    # Validation
    if result.get('email') and not validate_email(result['email']):
        result.pop('email', None)

    for url_field in ('linkedin', 'website', 'avatar_url'):
        if result.get(url_field) and not validate_url(result[url_field]):
            result.pop(url_field, None)

    # Build apollo_data from underscore-prefixed fields
    apollo_data = {}
    for key in list(result.keys()):
        if key.startswith('_') and key != '_source':
            apollo_data[key.lstrip('_')] = result.pop(key)

    if emp_count:
        apollo_data['employee_count'] = emp_count
    if revenue:
        apollo_data['annual_revenue'] = revenue

    apollo_data['enriched_at'] = datetime.now().isoformat()
    result['_apollo_data'] = apollo_data

    return result


def build_column_map(csv_headers: List[str]) -> Dict:
    """Build mapping from actual CSV headers to our fields."""
    column_map = {}
    unmapped = []

    for header in csv_headers:
        header_clean = header.strip()
        if header_clean in APOLLO_CSV_MAP:
            column_map[header_clean] = APOLLO_CSV_MAP[header_clean]
        elif header_clean in REFERENCE_FIELDS:
            pass  # Skip reference fields
        else:
            unmapped.append(header_clean)

    return column_map, unmapped


def import_apollo_csv(
    csv_path: str,
    dry_run: bool = False,
    match_by: str = 'profile_id',
):
    """Import Apollo-enriched CSV into Supabase."""
    # Read CSV
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        rows = list(reader)

    if not rows:
        print("ERROR: CSV is empty", file=sys.stderr)
        return

    # Build column mapping
    column_map, unmapped = build_column_map(headers)

    print(f"\n{'='*60}")
    print("APOLLO CSV IMPORT")
    print(f"{'='*60}\n")
    print(f"CSV: {csv_path}")
    print(f"Rows: {len(rows)}")
    print(f"Mapped columns: {len(column_map)}")
    if unmapped:
        print(f"Unmapped columns (stored in apollo_data): {unmapped}")
    print(f"Match by: {match_by}")
    print(f"Dry run: {dry_run}")
    print()

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    stats = {
        'total': len(rows),
        'matched': 0,
        'updated': 0,
        'skipped_no_match': 0,
        'skipped_no_new_data': 0,
        'fields_written': {},
    }

    for i, row in enumerate(rows, 1):
        # Parse Apollo row
        parsed = parse_apollo_row(row, column_map)

        # Also store any unmapped columns in apollo_data
        for col in unmapped:
            value = row.get(col, '').strip()
            if value:
                parsed.setdefault('_apollo_data', {})
                parsed['_apollo_data'][col] = value

        # Find matching profile
        profile_id = row.get('profile_id', '').strip()

        if match_by == 'profile_id' and profile_id:
            cur.execute(
                "SELECT id, name, email, phone, linkedin, website, company, "
                "enrichment_metadata FROM profiles WHERE id = %s",
                (profile_id,)
            )
        elif match_by == 'name':
            first = row.get('first_name', row.get('First Name', '')).strip()
            last = row.get('last_name', row.get('Last Name', '')).strip()
            full_name = f"{first} {last}".strip()
            cur.execute(
                "SELECT id, name, email, phone, linkedin, website, company, "
                "enrichment_metadata FROM profiles WHERE name ILIKE %s LIMIT 1",
                (full_name,)
            )
        else:
            continue

        profile = cur.fetchone()
        if not profile:
            stats['skipped_no_match'] += 1
            continue

        stats['matched'] += 1

        # Build SQL update using source priority
        existing_meta = profile.get('enrichment_metadata') or {}
        if isinstance(existing_meta, str):
            try:
                existing_meta = json.loads(existing_meta)
            except (json.JSONDecodeError, TypeError):
                existing_meta = {}

        set_parts = []
        params = []
        fields_written = []

        # Source priority check
        from scripts.automated_enrichment_pipeline_safe import SOURCE_PRIORITY
        apollo_priority = SOURCE_PRIORITY.get(APOLLO_SOURCE, 30)

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
            # Equal priority — only write if current value is empty
            current = profile.get(field)
            return not current or (isinstance(current, str) and not current.strip())

        # Write profile column fields
        column_fields = [
            'email', 'linkedin', 'website', 'phone', 'company',
            'business_size', 'revenue_tier', 'service_provided',
            'niche', 'avatar_url',
        ]

        for field in column_fields:
            value = parsed.get(field)
            if value and should_write(field, value):
                set_parts.append(
                    sql.SQL("{} = %s").format(sql.Identifier(field))
                )
                params.append(value)
                fields_written.append(field)
                stats['fields_written'][field] = stats['fields_written'].get(field, 0) + 1

        if not set_parts and not parsed.get('_apollo_data'):
            stats['skipped_no_new_data'] += 1
            continue

        # Update enrichment_metadata
        meta = dict(existing_meta)
        meta['apollo_data'] = parsed.get('_apollo_data', {})
        meta['last_apollo_enrichment'] = datetime.now().isoformat()

        # Field-level provenance
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
            name = profile.get('name', 'Unknown')
            print(f"  [{i}/{len(rows)}] {name}: would write {fields_written}")
        else:
            update_query = sql.SQL(
                "UPDATE profiles SET {} WHERE id = %s"
            ).format(sql.SQL(", ").join(set_parts))
            params.append(profile['id'])

            cur.execute(update_query, params)
            stats['updated'] += 1

            if i % 50 == 0:
                conn.commit()
                print(f"  Progress: {i}/{len(rows)} ({stats['updated']} updated)")

    if not dry_run:
        conn.commit()

    cur.close()
    conn.close()

    # Print summary
    print(f"\n{'='*60}")
    print("IMPORT SUMMARY")
    print(f"{'='*60}\n")
    print(f"Total rows:       {stats['total']}")
    print(f"Matched profiles: {stats['matched']}")
    print(f"Updated:          {stats['updated']}")
    print(f"No match found:   {stats['skipped_no_match']}")
    print(f"No new data:      {stats['skipped_no_new_data']}")
    print(f"\nFields written:")
    for field, count in sorted(stats['fields_written'].items(), key=lambda x: -x[1]):
        print(f"  {field:25s} {count:5d}")
    print(f"\n{'='*60}\n")

    return stats


def main():
    parser = argparse.ArgumentParser(
        description='Import Apollo.io enriched CSV into Supabase'
    )
    parser.add_argument('csv_file', help='Path to Apollo enriched CSV')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without writing')
    parser.add_argument('--match-by', choices=['profile_id', 'name'],
                        default='profile_id',
                        help='How to match CSV rows to profiles (default: profile_id)')

    args = parser.parse_args()

    if not os.path.exists(args.csv_file):
        print(f"ERROR: File not found: {args.csv_file}", file=sys.stderr)
        sys.exit(1)

    import_apollo_csv(
        csv_path=args.csv_file,
        dry_run=args.dry_run,
        match_by=args.match_by,
    )


if __name__ == '__main__':
    main()
