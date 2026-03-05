#!/usr/bin/env python3
"""
Export ICF coaches in clean Apollo-ready CSV format.

Cleans names by stripping ICF credentials (ACC, PCC, MCC, etc.) and honorifics,
and extracts city/state from bio for Apollo location matching.

Output: tmp/icf_apollo_export.csv

Usage:
    python3 scripts/export_for_apollo_icf.py
    python3 scripts/export_for_apollo_icf.py --limit 100 --out tmp/icf_test.csv
"""
import argparse
import csv
import os
import re
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()

# ICF credential suffixes to strip from names
ICF_CREDENTIALS = {
    'ACC', 'PCC', 'MCC',                          # core ICF credentials
    'ICF', 'CPCC', 'CPC', 'CEC', 'CPQC',          # other coaching certs
    'CBC', 'CNTC', 'CPTD',                         # training/development
    'MBA', 'PhD', 'PhD.', 'EdD', 'EdD.',           # degrees
    'MA', 'MS', 'MSc', 'MEd', 'MFA',
    'SHRM', 'SPHR', 'PHR', 'SHRM-SCP', 'SHRM-CP',
    'CSP', 'CPT', 'CPC', 'CMC', 'CEC',
    'LCSW', 'LPC', 'LMFT',                        # licensed therapists
    'Hogan',                                        # common suffix seen in data
}

# Honorific prefixes to strip
HONORIFICS = {'Mr', 'Mrs', 'Ms', 'Dr', 'Prof', 'Rev', 'Sir', 'Dame'}

# Regex for bio: "City, STATE COUNTRY" or "City, State"
BIO_LOCATION_RE = re.compile(
    r'\|\s*([A-Za-z\s\-\.]+),\s*([A-Z]{2,})\s+(?:UNITED STATES|USA|CANADA|UK|AUSTRALIA|[A-Z]+)?'
)


def clean_name(raw_name: str) -> tuple[str, str]:
    """
    Parse a raw ICF name into (first_name, last_name).

    Handles patterns like:
      "Jill Brown, ACC, PCC"       → ("Jill", "Brown")
      "Mrs. Melissa Ann Busse, PCC" → ("Melissa", "Busse")
      "Lisa S Aronson"              → ("Lisa", "Aronson")
      "Mrs. Marion Riehemann, Hogan, Global Leadership Profile" → ("Marion", "Riehemann")
    """
    name = raw_name.strip()

    # Remove everything after a comma-separated credential
    # Split on commas, keep only the name portion (first segment, minus creds)
    parts = [p.strip() for p in name.split(',')]
    # First part is the actual name; rest are credentials/certifications
    name = parts[0]

    # Strip honorific prefix (with or without trailing period)
    tokens = name.split()
    if tokens and tokens[0].rstrip('.') in HONORIFICS:
        tokens = tokens[1:]

    # Remaining tokens are the actual name words
    # Strip any token that looks like a credential
    clean_tokens = [t for t in tokens if t.rstrip('.').upper() not in ICF_CREDENTIALS]

    if not clean_tokens:
        return '', ''

    first = clean_tokens[0]
    # Last name = last token (skip middle initials that are single chars)
    last = clean_tokens[-1] if len(clean_tokens) > 1 else ''

    return first, last


def extract_location(bio: str) -> tuple[str, str]:
    """
    Extract city and state/country from ICF bio format:
    "ICF PCC Credentialed Coach | Hampton, NB CANADA"
    "ICF ACC Credentialed Coach | Ardmore, PA UNITED STATES"
    Returns (city, state).
    """
    if not bio:
        return '', ''
    m = BIO_LOCATION_RE.search(bio)
    if m:
        city = m.group(1).strip().title()
        state = m.group(2).strip()
        return city, state
    return '', ''


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--limit', type=int, default=None)
    p.add_argument('--out', type=str, default='tmp/icf_apollo_export.csv')
    p.add_argument('--skip-already-enriched', action='store_true', default=True,
                   help='Skip profiles that already have email or website (default: True)')
    return p.parse_args()


def main():
    args = parse_args()

    dsn = os.environ.get('DIRECT_DATABASE_URL') or os.environ.get('DATABASE_URL')
    conn = psycopg2.connect(dsn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    limit_clause = f'LIMIT {args.limit}' if args.limit else ''
    cur.execute(f"""
        SELECT id, name, bio, website, email
        FROM profiles
        WHERE jv_tier = 'B'
          AND website LIKE '%coachingfederation%'
          AND (email IS NULL OR email = '')
        ORDER BY jv_readiness_score DESC
        {limit_clause}
    """)
    rows = cur.fetchall()
    conn.close()

    print(f'Fetched {len(rows):,} ICF profiles from DB')

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    skipped_no_name = 0
    written = 0

    with open(out_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'profile_id', 'first_name', 'last_name', 'city', 'state',
            'raw_name', 'icf_url',
        ])
        writer.writeheader()

        for r in rows:
            first, last = clean_name(r['name'] or '')
            if not first or not last:
                skipped_no_name += 1
                continue

            city, state = extract_location(r['bio'] or '')

            writer.writerow({
                'profile_id': str(r['id']),
                'first_name': first,
                'last_name': last,
                'city': city,
                'state': state,
                'raw_name': r['name'],
                'icf_url': r['website'] or '',
            })
            written += 1

    print(f'Wrote {written:,} rows → {out_path}')
    print(f'Skipped {skipped_no_name:,} rows (unparseable name)')
    print()
    print('Next step: upload to Apollo People Search CSV enrichment,')
    print('  map first_name/last_name/city/state, request email + personal_website.')


if __name__ == '__main__':
    main()
