#!/usr/bin/env python3
"""
Targeted Exa enrichment for profiles that have no research cache entry.

Optimized for throughput:
- Concurrent Exa API calls (default 5 workers)
- Detects credit exhaustion (402) and stops early
- Filters out non-website URLs (booking links, social profiles) before calling Exa
- Results cached automatically; run consolidate_cache_to_supabase.py after

Usage:
    python scripts/enrich_uncached_profiles.py --dry-run       # Preview
    python scripts/enrich_uncached_profiles.py --limit 50      # Small batch
    python scripts/enrich_uncached_profiles.py --limit 1965    # All uncached
"""

import os
import sys
import json
import glob
import hashlib
import argparse
import time
import logging
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

# Django setup
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django
django.setup()

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

DATABASE_URL = os.environ['DATABASE_URL']
CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'Chelsea_clients', 'research_cache'
)

# Thread-safe credit exhaustion flag
_credits_exhausted = threading.Event()


def cache_key(name: str) -> str:
    return hashlib.md5(name.lower().encode()).hexdigest()[:12]


def get_cached_keys() -> set:
    """Load all existing cache keys."""
    keys = set()
    for fp in glob.glob(os.path.join(CACHE_DIR, '*.json')):
        try:
            with open(fp) as fh:
                data = json.load(fh)
            name = data.get('name', '')
            if name:
                keys.add(cache_key(name))
        except Exception:
            continue
    return keys


def get_uncached_profiles(limit: int) -> List[Dict]:
    """Get profiles that have no Exa cache entry."""
    cached_keys = get_cached_keys()
    print(f"Existing cache entries: {len(cached_keys)}")

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT id, name, email, company, website, linkedin,
               list_size, seeking, who_you_serve, what_you_do, offering
        FROM profiles
        WHERE name IS NOT NULL AND name != ''
        ORDER BY list_size DESC NULLS LAST
    """)
    all_profiles = cur.fetchall()
    cur.close()
    conn.close()

    uncached = []
    for p in all_profiles:
        if cache_key(p['name']) not in cached_keys:
            uncached.append(dict(p))
        if len(uncached) >= limit:
            break

    return uncached


def enrich_single(profile: Dict, dry_run: bool = False) -> Optional[Dict]:
    """Run Exa enrichment for a single profile. Thread-safe."""
    if _credits_exhausted.is_set():
        return None

    name = profile['name']
    website = profile.get('website') or ''
    linkedin = profile.get('linkedin') or ''
    company = profile.get('company') or ''

    existing_data = {
        k: v for k, v in profile.items()
        if k not in ('id',) and v is not None
    }

    if dry_run:
        return {'name': name, '_dry_run': True}

    try:
        from matching.enrichment.ai_research import research_and_enrich_profile

        enriched, was_researched = research_and_enrich_profile(
            name=name,
            website=website,
            existing_data=existing_data,
            use_cache=True,
            force_research=True,
            linkedin=linkedin,
            company=company,
            fill_only=False,
            skip_social_reach=False,
            exa_only=True,
        )

        if was_researched:
            return enriched
        return None

    except Exception as e:
        error_str = str(e).lower()
        if '402' in error_str or 'exceeded your credits' in error_str or 'no_more_credits' in error_str:
            _credits_exhausted.set()
            logger.error(f"CREDITS EXHAUSTED: {e}")
        else:
            logger.error(f"Failed to enrich {name}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description='Enrich profiles without Exa cache')
    parser.add_argument('--limit', type=int, default=50,
                        help='Max profiles to process (default 50)')
    parser.add_argument('--concurrency', type=int, default=5,
                        help='Parallel workers (default 5)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview without making API calls')
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("TARGETED EXA ENRICHMENT — UNCACHED PROFILES")
    print(f"{'='*60}\n")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print(f"Limit: {args.limit}")
    print(f"Concurrency: {args.concurrency}")
    print(f"Estimated Exa cost: ~${args.limit * 0.02:.2f}")
    print()

    profiles = get_uncached_profiles(args.limit)
    print(f"Uncached profiles found: {len(profiles)}")

    if not profiles:
        print("Nothing to do.")
        return

    # Show breakdown
    has_website = sum(1 for p in profiles if p.get('website') and p['website'].strip())
    has_linkedin = sum(1 for p in profiles if p.get('linkedin') and p['linkedin'].strip())
    no_signals = sum(1 for p in profiles
                     if not (p.get('website') and p['website'].strip())
                     and not (p.get('linkedin') and p['linkedin'].strip()))
    print(f"  With website:  {has_website}")
    print(f"  With LinkedIn: {has_linkedin}")
    print(f"  Name-only:     {no_signals}")
    print()

    stats = {
        'total': len(profiles),
        'enriched': 0,
        'failed': 0,
        'skipped': 0,
    }
    stats_lock = threading.Lock()
    start_time = time.time()
    processed = [0]  # mutable counter for thread-safe increment

    def process_and_track(profile):
        """Process one profile and return (profile, result)."""
        result = enrich_single(profile, args.dry_run)
        return profile, result

    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = {
            executor.submit(process_and_track, p): p
            for p in profiles
        }

        for future in as_completed(futures):
            if _credits_exhausted.is_set():
                # Cancel remaining futures
                for f in futures:
                    f.cancel()

            profile, result = future.result()

            with stats_lock:
                processed[0] += 1
                i = processed[0]

                if _credits_exhausted.is_set() and not result:
                    stats['skipped'] += 1
                elif result:
                    stats['enriched'] += 1
                    found_fields = []
                    for f in ['website', 'linkedin', 'email', 'content_platforms',
                              'jv_history', 'revenue_tier', 'booking_link']:
                        val = result.get(f)
                        if val and str(val) not in ('', '{}', '[]', 'None'):
                            found_fields.append(f)
                    if found_fields:
                        logger.info(f"  [{i}/{len(profiles)}] {profile['name']}: found {found_fields}")
                    else:
                        logger.info(f"  [{i}/{len(profiles)}] {profile['name']}: enriched (text fields)")
                else:
                    stats['failed'] += 1

                if i % 50 == 0:
                    elapsed = time.time() - start_time
                    rate = i / elapsed * 60 if elapsed > 0 else 0
                    remaining = len(profiles) - i
                    eta = remaining / (i / elapsed) / 60 if elapsed > 0 and i > 0 else 0
                    print(f"\n  Progress: {i}/{len(profiles)} "
                          f"({stats['enriched']} enriched, {stats['failed']} failed) "
                          f"— {rate:.0f}/min, ETA {eta:.1f}min\n")

    elapsed = time.time() - start_time

    print(f"\n{'='*60}")
    print("ENRICHMENT SUMMARY")
    print(f"{'='*60}\n")
    print(f"Total in queue:   {stats['total']}")
    print(f"Enriched:         {stats['enriched']}")
    print(f"Failed/no result: {stats['failed']}")
    print(f"Skipped (credits):{stats['skipped']}")
    print(f"Runtime:          {elapsed/60:.1f} min")
    print(f"Estimated cost:   ~${stats['enriched'] * 0.02:.2f}")
    if _credits_exhausted.is_set():
        print(f"\n  NOTE: Exa credits exhausted during run. Top up at dashboard.exa.ai")
    print(f"\nNext step: run consolidate_cache_to_supabase.py to push new data to DB")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
