#!/usr/bin/env python3
"""
Export unenriched Tier C and D profiles from DB into batch files for Vast.ai enrichment.

Only exports profiles with bio >= 100 chars (enough signal for extraction).
Profiles with thin bios are skipped — they need website scraping first.

Each batch file: 5 profiles with id + bio as scraped_text.
Output: tmp/tier_c_batches/batch_XXXX.json  (or tier_d_batches)

Usage:
    python3 scripts/export_unenriched_tier_cd.py --tier C
    python3 scripts/export_unenriched_tier_cd.py --tier D
    python3 scripts/export_unenriched_tier_cd.py --tier C --min-bio-len 200 --limit 10000
"""
import argparse
import json
import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

BATCH_SIZE = 5


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--tier', required=True, choices=['C', 'D'], help='Tier to export')
    p.add_argument('--min-bio-len', type=int, default=100, help='Minimum bio length to include')
    p.add_argument('--limit', type=int, default=None, help='Max profiles to export (for testing)')
    p.add_argument('--out-dir', type=str, default=None, help='Override output directory')
    return p.parse_args()


def main():
    args = parse_args()
    tier = args.tier
    min_bio = args.min_bio_len
    limit = args.limit

    out_dir = args.out_dir or f'tmp/tier_{tier.lower()}_batches'
    os.makedirs(out_dir, exist_ok=True)

    dsn = os.environ.get('DIRECT_DATABASE_URL') or os.environ.get('DATABASE_URL')
    conn = psycopg2.connect(dsn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    limit_clause = f'LIMIT {limit}' if limit else ''
    cur.execute(f"""
        SELECT id, name, bio, website, company, email, phone,
               who_you_serve, seeking, offering, niche, tags,
               signature_programs, booking_link, revenue_tier, service_provided
        FROM profiles
        WHERE jv_tier = %s
          AND (what_you_do IS NULL OR what_you_do = '')
          AND bio IS NOT NULL AND LENGTH(bio) >= %s
        ORDER BY jv_readiness_score DESC
        {limit_clause}
    """, (tier, min_bio))

    rows = cur.fetchall()
    conn.close()

    total = len(rows)
    batch_num = 0

    for i in range(0, total, BATCH_SIZE):
        chunk = rows[i:i + BATCH_SIZE]
        batch = []
        for r in chunk:
            batch.append({
                'id': str(r['id']),
                'name': r['name'] or '',
                'scraped_text': (r['bio'] or '')[:1500],  # truncate for vLLM context
                'website': r['website'] or '',
                'company': r['company'] or '',
                'email': r['email'] or '',
                'phone': r['phone'] or '',
                'who_you_serve': r['who_you_serve'] or '',
                'seeking': r['seeking'] or '',
                'offering': r['offering'] or '',
                'niche': r['niche'] or '',
            })

        out_path = os.path.join(out_dir, f'batch_{batch_num:04d}.json')
        with open(out_path, 'w') as f:
            json.dump(batch, f, indent=2, default=str)

        batch_num += 1

    print(f'Tier {tier}: exported {total:,} profiles (bio >= {min_bio} chars)')
    print(f'  → {batch_num:,} batches in {out_dir}/')
    print(f'  Skipped: profiles with bio < {min_bio} chars need website scraping first')


if __name__ == '__main__':
    main()
