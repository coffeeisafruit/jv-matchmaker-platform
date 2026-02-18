#!/usr/bin/env python3
"""
AI Inference Pipeline: Fill missing profile fields from existing data.

Reads existing fields from Supabase, uses Claude Max CLI to infer
missing ones, writes results directly back to Supabase. No web scraping —
purely inferring from what we already have.

Usage:
    python scripts/infer_missing_fields.py --field niche --concurrency 4
    python scripts/infer_missing_fields.py --field offering --dry-run
    python scripts/infer_missing_fields.py --field all --concurrency 6
    python scripts/infer_missing_fields.py --field all --model opus --concurrency 3
"""

import os
import sys
import json
import argparse
import time
import logging
import subprocess
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
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

DATABASE_URL = os.environ['DATABASE_URL']

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ── Field configurations ──────────────────────────────────────────────

FIELD_CONFIG = {
    'what_you_do': {
        'context_fields': ['bio', 'niche', 'service_provided', 'company', 'business_focus', 'offering', 'who_you_serve'],
        'array_context': ['tags'],
        'prompt_template': (
            'Based on the profile data below, write a concise 1-2 sentence description of '
            'what {name} does professionally. Focus on their core service or business. '
            'Return ONLY the description text, nothing else.\n\n'
            'Profile data:\n{context}'
        ),
    },
    'who_you_serve': {
        'context_fields': ['what_you_do', 'audience_type', 'niche', 'bio', 'service_provided', 'business_focus'],
        'array_context': ['tags'],
        'prompt_template': (
            'Based on the profile data below, describe who {name} serves — their target '
            'audience or ideal clients — in 1-2 sentences. Be specific about the type of '
            'people or businesses. Return ONLY the description text, nothing else.\n\n'
            'Profile data:\n{context}'
        ),
    },
    'seeking': {
        'context_fields': ['what_you_do', 'niche', 'who_you_serve', 'offering', 'bio', 'business_focus'],
        'array_context': ['tags'],
        'prompt_template': (
            'Based on the profile data below, write 1-2 sentences describing what {name} '
            'is likely seeking in joint venture partnerships or collaborations. Think about '
            'what would complement their business — referral partners, audience access, '
            'co-creation opportunities, etc. Return ONLY the description text, nothing else.\n\n'
            'Profile data:\n{context}'
        ),
    },
    'offering': {
        'context_fields': ['what_you_do', 'service_provided', 'bio', 'niche', 'who_you_serve', 'business_focus'],
        'array_context': ['tags'],
        'prompt_template': (
            'Based on the profile data below, write 1-2 sentences describing what {name} '
            'can offer to joint venture partners. What value, expertise, audience, or '
            'resources do they bring to collaborations? Return ONLY the description text, '
            'nothing else.\n\n'
            'Profile data:\n{context}'
        ),
    },
    'niche': {
        'context_fields': ['what_you_do', 'who_you_serve', 'service_provided', 'business_focus', 'bio'],
        'array_context': ['tags'],
        'prompt_template': (
            'Based on the profile data below, identify {name}\'s professional niche in '
            '2-5 words. Examples: "Executive Leadership Coaching", "Real Estate Investing", '
            '"Health & Wellness for Women", "Digital Marketing for Coaches". '
            'Return ONLY the niche label, nothing else.\n\n'
            'Profile data:\n{context}'
        ),
    },
    'bio': {
        'context_fields': ['what_you_do', 'who_you_serve', 'niche', 'company', 'offering', 'seeking',
                           'service_provided', 'business_focus'],
        'array_context': ['tags'],
        'prompt_template': (
            'Based on the profile data below, write a professional bio for {name} in '
            '2-3 sentences. It should read naturally and cover what they do, who they serve, '
            'and their area of expertise. Write in third person. '
            'Return ONLY the bio text, nothing else.\n\n'
            'Profile data:\n{context}'
        ),
    },
    'network_role': {
        'context_fields': ['what_you_do', 'offering', 'seeking', 'niche', 'who_you_serve', 'bio'],
        'array_context': ['tags'],
        'prompt_template': (
            'Based on the profile data below, classify {name}\'s primary network role. '
            'Choose ONE from: "Connector" (introduces people), "Expert" (deep domain knowledge), '
            '"Promoter" (large audience/platform), "Service Provider" (delivers services), '
            '"Thought Leader" (creates content/ideas), "Educator" (teaches/trains), '
            '"Investor" (provides capital), "Creator" (builds products/programs). '
            'Return ONLY the role label, nothing else.\n\n'
            'Profile data:\n{context}'
        ),
    },
    'audience_type': {
        'context_fields': ['who_you_serve', 'niche', 'what_you_do', 'business_focus', 'bio'],
        'array_context': ['tags'],
        'prompt_template': (
            'Based on the profile data below, classify {name}\'s primary audience type. '
            'Choose ONE from: "B2B" (serves businesses), "B2C" (serves consumers), '
            '"B2B2C" (serves businesses that serve consumers), "Coaches/Consultants" '
            '(serves other coaches/consultants), "Entrepreneurs" (serves business owners), '
            '"Corporate" (serves large organizations), "Nonprofit" (serves nonprofits). '
            'Return ONLY the audience type, nothing else.\n\n'
            'Profile data:\n{context}'
        ),
    },
}

# How many profiles per Claude CLI call
BATCH_SIZE = 10


def get_profiles_missing_field(field: str, limit: int = 5000) -> List[Dict]:
    """Get profiles that are missing the target field but have enough context to infer it."""
    config = FIELD_CONFIG[field]
    all_context = config['context_fields'] + config.get('array_context', [])

    # Build query: missing target field, but has at least one context field
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Select all needed fields
    select_fields = ['id', 'name', field] + all_context
    select_fields = list(dict.fromkeys(select_fields))  # dedupe preserving order

    # Missing target field condition
    if field == 'tags':
        missing_cond = f"(tags IS NULL OR array_length(tags, 1) IS NULL)"
    else:
        missing_cond = f"({field} IS NULL OR {field} = '')"

    # Has at least one context field
    context_conds = []
    for cf in config['context_fields']:
        context_conds.append(f"({cf} IS NOT NULL AND {cf} != '')")
    for cf in config.get('array_context', []):
        context_conds.append(f"({cf} IS NOT NULL AND array_length({cf}, 1) > 0)")
    has_context = " OR ".join(context_conds)

    query = f"""
        SELECT {', '.join(select_fields)}
        FROM profiles
        WHERE {missing_cond}
          AND ({has_context})
          AND name IS NOT NULL AND name != ''
        ORDER BY name
        LIMIT {limit}
    """
    cur.execute(query)
    profiles = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(p) for p in profiles]


def build_context_string(profile: Dict, field: str) -> str:
    """Build a context string from available profile fields."""
    config = FIELD_CONFIG[field]
    lines = []
    for cf in config['context_fields']:
        val = profile.get(cf)
        if val and isinstance(val, str) and val.strip():
            lines.append(f"- {cf}: {val.strip()}")
    for cf in config.get('array_context', []):
        val = profile.get(cf)
        if val and isinstance(val, list) and len(val) > 0:
            lines.append(f"- {cf}: {', '.join(str(v) for v in val)}")
    return '\n'.join(lines)


def infer_batch(batch: List[Dict], field: str, model: str = 'haiku') -> Dict[str, str]:
    """Use Claude Max CLI to infer a field for a batch of profiles. Returns {id: value}."""
    config = FIELD_CONFIG[field]

    # Build index-to-id mapping (avoids relying on Claude to echo UUIDs)
    idx_to_id = {}

    # Build batch prompt
    parts = []
    parts.append(f"For each person below, {config['prompt_template'].split('.')[0].lower().replace('based on the profile data below, ', '')}.")
    parts.append(f"\nReturn a JSON array of objects, each with \"idx\" (the person number) and \"{field}\" keys.")
    parts.append("Return ONLY valid JSON, no explanation or markdown fences.\n")

    valid_idx = 0
    for i, profile in enumerate(batch):
        context = build_context_string(profile, field)
        if not context:
            continue
        valid_idx += 1
        idx_to_id[valid_idx] = profile['id']
        parts.append(f"--- Person {valid_idx} ---")
        parts.append(f"Name: {profile['name']}")
        parts.append(context)
        parts.append("")

    if not idx_to_id:
        return {}

    prompt = '\n'.join(parts)

    try:
        result = subprocess.run(
            ['claude', '--print', '--model', model, '-p', prompt],
            capture_output=True,
            text=True,
            timeout=180 if model == 'opus' else 120,
        )
        if result.returncode != 0:
            logger.error(f"Claude CLI failed: {result.stderr[:200]}")
            return {}

        text = result.stdout.strip()

        # Strip markdown fences
        if text.startswith('```'):
            lines = text.split('\n')
            text = '\n'.join(lines[1:-1] if lines[-1].startswith('```') else lines[1:])

        # Extract JSON array
        if not text.startswith('['):
            match = re.search(r'\[[\s\S]*\]', text)
            if match:
                text = match.group(0)

        results_list = json.loads(text)
        # Map idx back to profile ID
        mapped = {}
        for item in results_list:
            idx = item.get('idx')
            val = item.get(field, '').strip() if isinstance(item.get(field), str) else item.get(field)
            if idx is not None and val and str(val).strip():
                profile_id = idx_to_id.get(int(idx))
                if profile_id:
                    mapped[profile_id] = str(val).strip()
        return mapped

    except subprocess.TimeoutExpired:
        logger.error("Claude CLI timed out")
        return {}
    except (json.JSONDecodeError, KeyError) as e:
        logger.debug(f"Parse error: {e}")
        # Fallback: try one-by-one for this batch
        return infer_singles(batch, field, model)
    except Exception as e:
        logger.error(f"Inference error: {e}")
        return {}


def infer_singles(batch: List[Dict], field: str, model: str = 'haiku') -> Dict[str, str]:
    """Fallback: infer one profile at a time when batch parsing fails."""
    config = FIELD_CONFIG[field]
    results = {}

    for profile in batch:
        context = build_context_string(profile, field)
        if not context:
            continue

        prompt = config['prompt_template'].format(
            name=profile['name'],
            context=context,
        )

        try:
            result = subprocess.run(
                ['claude', '--print', '--model', model, '-p', prompt],
                capture_output=True,
                text=True,
                timeout=120 if model == 'opus' else 60,
            )
            if result.returncode == 0:
                text = result.stdout.strip()
                # Clean up common artifacts
                text = text.strip('"\'')
                if text and len(text) > 3 and len(text) < 500:
                    results[profile['id']] = text

        except Exception:
            continue

    return results


def write_results_to_db(results: Dict[str, str], field: str) -> int:
    """Write inferred values to Supabase. Returns count of rows updated."""
    if not results:
        return 0

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    now = datetime.now()
    updated = 0

    for profile_id, value in results.items():
        if not value or not value.strip():
            continue

        try:
            # Update field + enrichment metadata
            cur.execute(f"""
                UPDATE profiles
                SET {field} = %s,
                    enrichment_metadata = COALESCE(enrichment_metadata, '{{}}'::jsonb)
                        || jsonb_build_object(
                            'field_meta',
                            COALESCE(enrichment_metadata->'field_meta', '{{}}'::jsonb)
                                || jsonb_build_object(%s, jsonb_build_object(
                                    'source', 'ai_inference',
                                    'updated_at', %s,
                                    'pipeline_version', 3
                                ))
                        ),
                    updated_at = %s
                WHERE id = %s
                  AND ({field} IS NULL OR {field} = '')
            """, (value.strip(), field, now.isoformat(), now, profile_id))
            if cur.rowcount > 0:
                updated += 1
        except Exception as e:
            logger.error(f"DB write error for {profile_id}: {e}")
            conn.rollback()
            continue

    conn.commit()
    cur.close()
    conn.close()
    return updated


def process_field(field: str, limit: int = 5000, concurrency: int = 4,
                  dry_run: bool = False, model: str = 'haiku') -> Dict:
    """Main pipeline for a single field."""
    profiles = get_profiles_missing_field(field, limit)

    print(f"\n{'='*60}")
    print(f"INFER: {field.upper()}")
    print(f"{'='*60}")
    print(f"Profiles missing {field}: {len(profiles)}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"Concurrency: {concurrency}")
    print(f"Batch size: {BATCH_SIZE}")

    if not profiles:
        print("Nothing to infer.")
        return {'field': field, 'total': 0, 'inferred': 0, 'written': 0}

    if dry_run:
        for i, p in enumerate(profiles[:10]):
            ctx = build_context_string(p, field)
            ctx_preview = ctx[:100] + '...' if len(ctx) > 100 else ctx
            print(f"  {i+1}. {p['name']}: {ctx_preview}")
        if len(profiles) > 10:
            print(f"  ... and {len(profiles) - 10} more")
        return {'field': field, 'total': len(profiles), 'inferred': 0, 'written': 0}

    # Create batches
    batches = []
    for i in range(0, len(profiles), BATCH_SIZE):
        batches.append(profiles[i:i + BATCH_SIZE])

    print(f"Batches: {len(batches)}")

    stats = {'total': len(profiles), 'inferred': 0, 'written': 0, 'failed': 0}
    stats_lock = threading.Lock()
    start_time = time.time()
    batch_count = [0]

    def process_batch(batch):
        results = infer_batch(batch, field, model)
        written = write_results_to_db(results, field)
        return len(results), written

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(process_batch, b): b for b in batches}

        for future in as_completed(futures):
            try:
                inferred, written = future.result()
            except Exception as e:
                logger.error(f"Batch error: {e}")
                with stats_lock:
                    stats['failed'] += BATCH_SIZE
                continue

            with stats_lock:
                batch_count[0] += 1
                stats['inferred'] += inferred
                stats['written'] += written
                i = batch_count[0]

                if i % 5 == 0 or i == len(batches):
                    elapsed = time.time() - start_time
                    profiles_done = i * BATCH_SIZE
                    rate = profiles_done / elapsed * 60 if elapsed > 0 else 0
                    print(f"  [{i}/{len(batches)}] {stats['inferred']} inferred, "
                          f"{stats['written']} written — {rate:.0f} profiles/min")

    elapsed = time.time() - start_time
    print(f"\nCompleted {field}: {stats['inferred']} inferred, "
          f"{stats['written']} written in {elapsed:.0f}s")

    return {'field': field, **stats}


def main():
    parser = argparse.ArgumentParser(description='Infer missing profile fields from existing data')
    parser.add_argument('--field', required=True,
                        choices=list(FIELD_CONFIG.keys()) + ['all'],
                        help='Which field to infer (or "all")')
    parser.add_argument('--limit', type=int, default=5000,
                        help='Max profiles per field')
    parser.add_argument('--concurrency', type=int, default=4,
                        help='Parallel workers')
    parser.add_argument('--model', default='haiku',
                        choices=['haiku', 'sonnet', 'opus'],
                        help='Claude model to use (default: haiku)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview without writing')
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("AI INFERENCE PIPELINE — Fill Missing Fields")
    print(f"{'='*60}")
    print(f"Model: {args.model}")
    print(f"Cost: $0.00 (Claude Max CLI)\n")

    if args.field == 'all':
        # Run fields in dependency order
        field_order = [
            'what_you_do',   # foundational
            'who_you_serve', # depends on what_you_do
            'niche',         # depends on what_you_do
            'offering',      # depends on what_you_do
            'seeking',       # depends on what_you_do + offering
            'audience_type', # depends on who_you_serve
            'network_role',  # depends on what_you_do + offering
            'bio',           # depends on everything
        ]
        all_stats = []
        for f in field_order:
            stats = process_field(f, args.limit, args.concurrency, args.dry_run, args.model)
            all_stats.append(stats)

        print(f"\n{'='*60}")
        print("ALL FIELDS SUMMARY")
        print(f"{'='*60}")
        total_inferred = sum(s['inferred'] for s in all_stats)
        total_written = sum(s['written'] for s in all_stats)
        for s in all_stats:
            print(f"  {s['field']:20s}  {s['total']:4d} missing → "
                  f"{s['inferred']:4d} inferred, {s['written']:4d} written")
        print(f"\n  TOTAL: {total_inferred} inferred, {total_written} written")
    else:
        process_field(args.field, args.limit, args.concurrency, args.dry_run, args.model)


if __name__ == '__main__':
    main()
