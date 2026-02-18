#!/usr/bin/env python3
"""
Infer the 'seeking' field for profiles using Claude SDK.

For profiles that have business context (what_you_do, who_you_serve, niche, offering)
but are missing the 'seeking' field, this script uses Claude to intelligently infer
what kind of JV partnerships they would be looking for.

Uses Claude Max plan via Claude Agent SDK ($0 cost).
Source priority: 'ai_research' = 40 (overrides 'unknown' and 'apollo',
never overwrites Exa, client, or manual data).

Usage:
    python scripts/infer_seeking_field.py --limit 10 --dry-run   # Preview
    python scripts/infer_seeking_field.py --limit 100             # Small batch
    python scripts/infer_seeking_field.py                         # All eligible
"""

import os
import sys
import json
import argparse
import time
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

INFERENCE_SOURCE = 'ai_research'
INFERENCE_PRIORITY = 40

SEEKING_INFERENCE_PROMPT = """You are a JV (Joint Venture) partnership expert. Given a person's business profile,
infer what kinds of partnerships they would likely be SEEKING.

Profile:
- Name: {name}
- Company: {company}
- What they do: {what_you_do}
- Who they serve: {who_you_serve}
- Niche: {niche}
- Offering: {offering}
- Business size: {business_size}
- Revenue tier: {revenue_tier}

Based on this profile, write 1-3 sentences describing what JV partnerships this person
would likely be SEEKING. Focus on:
- What type of partner would complement their business
- What audiences they'd want access to
- What formats would work (podcast swaps, email list shares, co-created content, affiliate partnerships, summit speaking, etc.)

Be specific and practical. Write from their perspective (what THEY are looking for).
Do NOT start with their name. Start directly with what they're seeking.

Example good output: "Looking for podcast hosts in the personal development space to cross-promote.
Seeking email list partners with audiences of 10K+ entrepreneurs interested in mindset coaching.
Open to co-creating summit content with complementary coaches."

Return ONLY the seeking text, no JSON, no labels, no explanation."""


def get_profiles_needing_seeking(limit: int = None) -> List[Dict]:
    """Query profiles that have business context but no seeking field."""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    limit_clause = f"LIMIT {limit}" if limit else ""

    cur.execute(f"""
        SELECT id, name, company, what_you_do, who_you_serve, niche,
               offering, business_size, revenue_tier, enrichment_metadata
        FROM profiles
        WHERE (seeking IS NULL OR seeking = '')
          AND (
              (what_you_do IS NOT NULL AND what_you_do != '')
              OR (who_you_serve IS NOT NULL AND who_you_serve != '')
              OR (niche IS NOT NULL AND niche != '')
              OR (offering IS NOT NULL AND offering != '')
          )
        ORDER BY
            CASE WHEN what_you_do IS NOT NULL AND what_you_do != '' THEN 1 ELSE 0 END +
            CASE WHEN who_you_serve IS NOT NULL AND who_you_serve != '' THEN 1 ELSE 0 END +
            CASE WHEN niche IS NOT NULL AND niche != '' THEN 1 ELSE 0 END +
            CASE WHEN offering IS NOT NULL AND offering != '' THEN 1 ELSE 0 END
            DESC,
            name
        {limit_clause}
    """)

    profiles = cur.fetchall()
    cur.close()
    conn.close()
    return profiles


async def infer_seeking_claude_sdk(profile: Dict) -> Optional[str]:
    """Infer seeking using Claude Agent SDK (Claude Max plan, $0)."""
    try:
        from claude_agent_sdk import query, ClaudeAgentOptions, ResultMessage
    except ImportError:
        try:
            from claude_agent_sdk import query, ClaudeAgentOptions
            from claude_agent_sdk.types import ResultMessage
        except ImportError:
            return None

    prompt = SEEKING_INFERENCE_PROMPT.format(
        name=profile.get('name', ''),
        company=profile.get('company', ''),
        what_you_do=profile.get('what_you_do', 'Not specified'),
        who_you_serve=profile.get('who_you_serve', 'Not specified'),
        niche=profile.get('niche', 'Not specified'),
        offering=profile.get('offering', 'Not specified'),
        business_size=profile.get('business_size', 'Not specified'),
        revenue_tier=profile.get('revenue_tier', 'Not specified'),
    )

    # Strip ANTHROPIC_API_KEY so the SDK uses Max plan OAuth instead of API
    clean_env = {k: v for k, v in os.environ.items() if k != 'ANTHROPIC_API_KEY'}

    try:
        result_text = None
        async for message in query(
            prompt=prompt,
            options=ClaudeAgentOptions(
                max_turns=1,
                env=clean_env,
            ),
        ):
            if isinstance(message, ResultMessage) and message.result:
                result_text = message.result

        if result_text:
            seeking = result_text.strip()
            if len(seeking) > 20 and len(seeking) < 1000:
                return seeking
        return None
    except Exception as e:
        print(f"    Claude SDK error: {e}")
        return None


def infer_seeking_anthropic(profile: Dict) -> Optional[str]:
    """Fallback: infer seeking using Anthropic API directly."""
    api_key = os.environ.get('ANTHROPIC_API_KEY') or os.environ.get('OPENROUTER_API_KEY')
    if not api_key:
        return None

    import requests

    prompt = SEEKING_INFERENCE_PROMPT.format(
        name=profile.get('name', ''),
        company=profile.get('company', ''),
        what_you_do=profile.get('what_you_do', 'Not specified'),
        who_you_serve=profile.get('who_you_serve', 'Not specified'),
        niche=profile.get('niche', 'Not specified'),
        offering=profile.get('offering', 'Not specified'),
        business_size=profile.get('business_size', 'Not specified'),
        revenue_tier=profile.get('revenue_tier', 'Not specified'),
    )

    # Try Anthropic API first
    if os.environ.get('ANTHROPIC_API_KEY'):
        try:
            response = requests.post(
                'https://api.anthropic.com/v1/messages',
                headers={
                    'x-api-key': os.environ['ANTHROPIC_API_KEY'],
                    'content-type': 'application/json',
                    'anthropic-version': '2023-06-01',
                },
                json={
                    'model': 'claude-sonnet-4-5-20250929',
                    'max_tokens': 300,
                    'messages': [{'role': 'user', 'content': prompt}],
                },
                timeout=30,
            )
            if response.status_code == 200:
                data = response.json()
                seeking = data['content'][0]['text'].strip()
                if seeking and len(seeking) > 20:
                    return seeking
        except Exception as e:
            print(f"    Anthropic API error: {e}")

    return None


async def infer_seeking(profile: Dict) -> Optional[str]:
    """Infer seeking field — uses Claude SDK (free on Max plan)."""
    result = await infer_seeking_claude_sdk(profile)
    if result:
        return result
    return None


async def run_inference(limit: int = None, dry_run: bool = False, batch_delay: float = 0.5):
    """Run seeking inference on eligible profiles."""
    print(f"\n{'='*60}")
    print("SEEKING FIELD INFERENCE")
    print(f"{'='*60}\n")

    profiles = get_profiles_needing_seeking(limit)
    print(f"Profiles eligible for seeking inference: {len(profiles)}")
    print(f"Dry run: {dry_run}")
    print()

    if not profiles:
        print("No profiles need seeking inference.")
        return

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    stats = {
        'total': len(profiles),
        'inferred': 0,
        'failed': 0,
        'skipped': 0,
    }

    from scripts.automated_enrichment_pipeline_safe import SOURCE_PRIORITY

    for i, profile in enumerate(profiles, 1):
        name = profile.get('name', 'Unknown')
        context_fields = sum(1 for f in ('what_you_do', 'who_you_serve', 'niche', 'offering')
                             if profile.get(f))

        print(f"  [{i}/{len(profiles)}] {name} ({context_fields} context fields): ", end='', flush=True)

        # Check source priority
        existing_meta = profile.get('enrichment_metadata') or {}
        if isinstance(existing_meta, str):
            try:
                existing_meta = json.loads(existing_meta)
            except (json.JSONDecodeError, TypeError):
                existing_meta = {}

        field_info = existing_meta.get('field_meta', {}).get('seeking', {})
        existing_source = field_info.get('source', 'unknown')
        existing_priority = SOURCE_PRIORITY.get(existing_source, 0)

        if INFERENCE_PRIORITY < existing_priority:
            print(f"skipped (higher-priority source: {existing_source})")
            stats['skipped'] += 1
            continue

        if dry_run:
            print("[dry-run] would infer")
            stats['inferred'] += 1
            continue

        seeking = await infer_seeking(profile)

        if not seeking:
            print("failed (no inference)")
            stats['failed'] += 1
            continue

        # Write to database
        now_iso = datetime.now().isoformat()

        # Update field_meta provenance
        field_meta = existing_meta.get('field_meta', {})
        field_meta['seeking'] = {
            'source': INFERENCE_SOURCE,
            'updated_at': now_iso,
            'pipeline_version': 1,
        }
        existing_meta['field_meta'] = field_meta

        cur.execute(
            """UPDATE profiles
               SET seeking = %s,
                   enrichment_metadata = %s::jsonb,
                   updated_at = %s
               WHERE id = %s""",
            (seeking, json.dumps(existing_meta), datetime.now(), profile['id'])
        )

        stats['inferred'] += 1
        # Show first 80 chars of inference
        preview = seeking[:80] + '...' if len(seeking) > 80 else seeking
        print(f"'{preview}'")

        if i % 20 == 0:
            conn.commit()

        time.sleep(batch_delay)

    conn.commit()
    cur.close()
    conn.close()

    # Summary
    print(f"\n{'='*60}")
    print("INFERENCE SUMMARY")
    print(f"{'='*60}\n")
    print(f"Total eligible: {stats['total']}")
    print(f"Inferred:       {stats['inferred']}")
    print(f"Failed:         {stats['failed']}")
    print(f"Skipped:        {stats['skipped']}")
    print(f"\n{'='*60}\n")

    return stats


def main():
    parser = argparse.ArgumentParser(
        description='Infer seeking field using Claude SDK'
    )
    parser.add_argument('--limit', type=int, default=None,
                        help='Maximum profiles to process')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without writing')
    parser.add_argument('--delay', type=float, default=0.5,
                        help='Delay between inferences in seconds (default: 0.5)')

    args = parser.parse_args()

    import asyncio
    asyncio.run(run_inference(
        limit=args.limit,
        dry_run=args.dry_run,
        batch_delay=args.delay,
    ))


if __name__ == '__main__':
    main()
