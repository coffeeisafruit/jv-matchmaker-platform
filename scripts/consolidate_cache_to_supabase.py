#!/usr/bin/env python3
"""
Consolidate Exa research cache files directly into Supabase.

Reads all JSON files from Chelsea_clients/research_cache/, matches them to
profiles by name hash, and upserts enrichment data. No API calls — purely
local cache → database.

Source priority: Exa (50) overwrites Apollo (30) and unknown, but never
overwrites client data (90+) or manual corrections (80).

Usage:
    python scripts/consolidate_cache_to_supabase.py --dry-run   # Preview
    python scripts/consolidate_cache_to_supabase.py              # Live
"""

import os
import sys
import json
import glob
import hashlib
import argparse
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

load_dotenv()

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set", file=sys.stderr)
    sys.exit(1)

# Source priority (from automated_enrichment_pipeline_safe.py)
SOURCE_PRIORITY = {
    'client': 90,
    'manual': 80,
    'client_edit': 75,
    'exa': 50,
    'exa_pipeline': 50,
    'ai_research': 40,
    'apollo': 30,
    'unknown': 0,
}

EXA_SOURCE = 'exa_pipeline'
EXA_PRIORITY = SOURCE_PRIORITY[EXA_SOURCE]

# Fields we write from cache data
PROFILE_TEXT_FIELDS = [
    'what_you_do', 'who_you_serve', 'seeking', 'offering', 'bio',
    'niche', 'company', 'service_provided', 'business_focus',
    'signature_programs', 'current_projects', 'booking_link',
    'audience_type', 'business_size', 'revenue_tier',
]

PROFILE_URL_FIELDS = ['website', 'linkedin', 'email']

PROFILE_INT_FIELDS = ['list_size', 'social_reach']


def cache_key(name: str) -> str:
    return hashlib.md5(name.lower().encode()).hexdigest()[:12]


def should_write(field: str, new_value, existing_meta: Dict, existing_profile: Dict) -> bool:
    """Check source priority to decide if we should write this field."""
    if not new_value:
        return False
    if isinstance(new_value, str) and not new_value.strip():
        return False

    field_info = existing_meta.get('field_meta', {}).get(field, {})
    existing_source = field_info.get('source', 'unknown')
    existing_priority = SOURCE_PRIORITY.get(existing_source, 0)

    if EXA_PRIORITY < existing_priority:
        return False
    if EXA_PRIORITY > existing_priority:
        # Even if we have higher priority, only write if current is empty
        # (for non-empty fields, equal-or-higher priority writes)
        current = existing_profile.get(field)
        if current and isinstance(current, str) and current.strip():
            # We have higher priority — OK to write
            return True
        return True
    # Equal priority — only write if current value is empty
    current = existing_profile.get(field)
    return not current or (isinstance(current, str) and not current.strip())


def consolidate_cache(dry_run: bool = False):
    cache_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'Chelsea_clients', 'research_cache'
    )

    cache_files = glob.glob(os.path.join(cache_dir, '*.json'))
    print(f"\n{'='*60}")
    print("CACHE → SUPABASE CONSOLIDATION")
    print(f"{'='*60}\n")
    print(f"Cache files: {len(cache_files)}")
    print(f"Dry run: {dry_run}\n")

    # Load all cache data keyed by name hash
    cache_data = {}
    for f in cache_files:
        try:
            with open(f) as fh:
                data = json.load(fh)
            name = data.get('name', '')
            if name:
                key = cache_key(name)
                cache_data[key] = data
        except Exception:
            continue

    print(f"Cache entries with names: {len(cache_data)}")

    # Connect to Supabase
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Fetch all profiles
    cur.execute("""
        SELECT id, name, email, phone, company, website, linkedin,
               what_you_do, who_you_serve, seeking, offering, bio,
               niche, service_provided, business_focus, business_size,
               revenue_tier, audience_type, signature_programs,
               current_projects, booking_link, list_size, social_reach,
               enrichment_metadata
        FROM profiles
        WHERE name IS NOT NULL AND name != ''
    """)
    profiles = cur.fetchall()
    print(f"Profiles in DB: {len(profiles)}")

    stats = {
        'matched': 0,
        'updated': 0,
        'skipped_no_cache': 0,
        'skipped_no_new_data': 0,
        'fields_written': {},
    }

    for i, profile in enumerate(profiles, 1):
        name = profile['name']
        key = cache_key(name)
        cached = cache_data.get(key)

        if not cached:
            stats['skipped_no_cache'] += 1
            continue

        stats['matched'] += 1

        existing_meta = profile.get('enrichment_metadata') or {}
        if isinstance(existing_meta, str):
            try:
                existing_meta = json.loads(existing_meta)
            except (json.JSONDecodeError, TypeError):
                existing_meta = {}

        set_parts = []
        params = []
        fields_written = []

        # Text fields
        for field in PROFILE_TEXT_FIELDS:
            value = cached.get(field)
            if isinstance(value, str):
                value = value.strip()
            if value and should_write(field, value, existing_meta, profile):
                set_parts.append(sql.SQL("{} = %s").format(sql.Identifier(field)))
                params.append(value)
                fields_written.append(field)
                stats['fields_written'][field] = stats['fields_written'].get(field, 0) + 1

        # URL fields
        for field in PROFILE_URL_FIELDS:
            value = cached.get(field)
            if isinstance(value, str):
                value = value.strip()
            if value and should_write(field, value, existing_meta, profile):
                set_parts.append(sql.SQL("{} = %s").format(sql.Identifier(field)))
                params.append(value)
                fields_written.append(field)
                stats['fields_written'][field] = stats['fields_written'].get(field, 0) + 1

        # Integer fields
        for field in PROFILE_INT_FIELDS:
            value = cached.get(field)
            if value is not None:
                try:
                    int_val = int(value)
                    if int_val > 0:
                        current = profile.get(field) or 0
                        # For list_size: only write if larger or current is 0
                        if field == 'list_size' and current and int_val <= current:
                            continue
                        set_parts.append(sql.SQL("{} = %s").format(sql.Identifier(field)))
                        params.append(int_val)
                        fields_written.append(field)
                        stats['fields_written'][field] = stats['fields_written'].get(field, 0) + 1
                except (ValueError, TypeError):
                    pass

        # Content platforms (JSONB)
        content_platforms = cached.get('content_platforms')
        if content_platforms and isinstance(content_platforms, dict):
            set_parts.append(sql.SQL(
                "content_platforms = COALESCE(content_platforms, '{}'::jsonb) || %s::jsonb"
            ))
            params.append(json.dumps(content_platforms))
            fields_written.append('content_platforms')
            stats['fields_written']['content_platforms'] = stats['fields_written'].get('content_platforms', 0) + 1

        # Tags (text array)
        tags = cached.get('tags')
        if tags and isinstance(tags, list) and len(tags) > 0:
            set_parts.append(sql.SQL("tags = %s"))
            params.append(tags)
            fields_written.append('tags')
            stats['fields_written']['tags'] = stats['fields_written'].get('tags', 0) + 1

        if not set_parts:
            stats['skipped_no_new_data'] += 1
            continue

        # Update enrichment_metadata
        now_iso = datetime.now().isoformat()
        meta = dict(existing_meta)
        meta['last_enrichment'] = EXA_SOURCE
        meta['enriched_at'] = now_iso

        field_meta = meta.get('field_meta', {})
        for f in fields_written:
            field_meta[f] = {
                'source': EXA_SOURCE,
                'updated_at': now_iso,
                'pipeline_version': 2,
            }
        meta['field_meta'] = field_meta

        set_parts.append(sql.SQL("enrichment_metadata = COALESCE(enrichment_metadata, '{}'::jsonb) || %s::jsonb"))
        params.append(json.dumps(meta))
        set_parts.append(sql.SQL("last_enriched_at = %s"))
        params.append(datetime.now())
        set_parts.append(sql.SQL("updated_at = %s"))
        params.append(datetime.now())

        if dry_run:
            if fields_written:
                print(f"  [{i}] {name}: would write {fields_written}")
        else:
            update_query = sql.SQL(
                "UPDATE profiles SET {} WHERE id = %s"
            ).format(sql.SQL(", ").join(set_parts))
            params.append(profile['id'])
            cur.execute(update_query, params)
            stats['updated'] += 1

            if i % 100 == 0:
                conn.commit()
                print(f"  Progress: {i}/{len(profiles)} ({stats['updated']} updated)")

    if not dry_run:
        conn.commit()

    cur.close()
    conn.close()

    # Summary
    print(f"\n{'='*60}")
    print("CONSOLIDATION SUMMARY")
    print(f"{'='*60}\n")
    print(f"Profiles scanned:  {len(profiles)}")
    print(f"Cache matches:     {stats['matched']}")
    print(f"Updated:           {stats['updated']}")
    print(f"No cache entry:    {stats['skipped_no_cache']}")
    print(f"No new data:       {stats['skipped_no_new_data']}")
    print(f"\nFields written:")
    for field, count in sorted(stats['fields_written'].items(), key=lambda x: -x[1]):
        print(f"  {field:25s} {count:5d}")
    print(f"\n{'='*60}\n")

    return stats


def main():
    parser = argparse.ArgumentParser(
        description='Consolidate Exa research cache into Supabase'
    )
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without writing')
    args = parser.parse_args()

    consolidate_cache(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
