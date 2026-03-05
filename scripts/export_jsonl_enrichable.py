#!/usr/bin/env python3
"""
Export enrichable profiles from tier_b_scraped.jsonl into batch files for Haiku enrichment.

Filters to profiles where:
  - bio >= 300 chars (enough signal for AI extraction)
  - Not ICF/coachingfederation source
  - Profile ID exists in DB (to ensure we can update it)

Output: tmp/jsonl_batches/batch_XXXX.json (5 profiles each)

Usage:
    python3 scripts/export_jsonl_enrichable.py
    python3 scripts/export_jsonl_enrichable.py --min-bio 200 --limit 1000
    python3 scripts/export_jsonl_enrichable.py --skip-db-check  # faster, no DB lookup
"""
import argparse
import json
import os
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

JSONL_PATH = project_root / 'Filling Database' / 'tier_b_scraped.jsonl'
BATCH_SIZE = 5
ICF_SOURCES = {'icf', 'coachingfederation', 'coaching_federation'}


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--min-bio', type=int, default=300)
    p.add_argument('--limit', type=int, default=None)
    p.add_argument('--out-dir', type=str, default='tmp/jsonl_batches')
    p.add_argument('--skip-db-check', action='store_true',
                   help='Skip checking if IDs exist in DB (faster, may include stale IDs)')
    return p.parse_args()


def get_db_ids():
    """Return set of profile IDs that exist in the DB."""
    import psycopg2
    from dotenv import load_dotenv
    load_dotenv()
    dsn = os.environ.get('DIRECT_DATABASE_URL') or os.environ.get('DATABASE_URL')
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    # Only IDs that are unenriched (what_you_do empty)
    cur.execute("""
        SELECT id::text FROM profiles
        WHERE jv_tier = 'B'
          AND (what_you_do IS NULL OR what_you_do = '')
    """)
    ids = {row[0] for row in cur.fetchall()}
    conn.close()
    print(f'DB: {len(ids):,} unenriched Tier B profiles')
    return ids


def main():
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    db_ids = None
    if not args.skip_db_check:
        db_ids = get_db_ids()

    profiles = []
    skipped_thin = 0
    skipped_icf = 0
    skipped_no_db = 0

    with open(JSONL_PATH) as f:
        for line in f:
            if args.limit and len(profiles) >= args.limit:
                break
            try:
                p = json.loads(line)
            except Exception:
                continue

            src = (p.get('source') or 'unknown').lower()
            bio = p.get('bio') or ''
            pid = str(p.get('id') or '')

            # Skip ICF
            if any(s in src for s in ICF_SOURCES):
                skipped_icf += 1
                continue

            # Skip thin bios
            if len(bio) < args.min_bio:
                skipped_thin += 1
                continue

            # Skip if not in DB (optional)
            if db_ids is not None and pid not in db_ids:
                skipped_no_db += 1
                continue

            profiles.append({
                'id': pid,
                'name': p.get('name') or '',
                'scraped_text': bio[:1500],
                'website': p.get('website') or '',
                'company': p.get('company') or '',
                'email': p.get('email') or '',
                'phone': p.get('phone') or '',
                'who_you_serve': '',
                'seeking': '',
                'offering': '',
                'niche': '',
            })

    total = len(profiles)
    batch_num = 0
    for i in range(0, total, BATCH_SIZE):
        chunk = profiles[i:i + BATCH_SIZE]
        out_path = out_dir / f'batch_{batch_num:04d}.json'
        with open(out_path, 'w') as f:
            json.dump(chunk, f, indent=2, default=str)
        batch_num += 1

    print(f'Exported {total:,} profiles → {batch_num:,} batches in {out_dir}/')
    print(f'Skipped: {skipped_thin:,} thin bios, {skipped_icf:,} ICF, {skipped_no_db:,} not in DB')


if __name__ == '__main__':
    main()
