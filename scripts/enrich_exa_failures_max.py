#!/usr/bin/env python3
"""
Fallback enrichment for profiles that Exa couldn't index.

Uses crawl4ai (free) + Claude Max via CLI (no API cost) to:
1. Scrape the website directly
2. Extract structured profile data using Claude Max

This is the second pass - runs AFTER enrich_uncached_profiles.py finishes.

Usage:
    python scripts/enrich_exa_failures_max.py --dry-run        # Preview
    python scripts/enrich_exa_failures_max.py --limit 50       # Small batch
    python scripts/enrich_exa_failures_max.py --limit 500      # All failures
"""

import os
import sys
import json
import glob
import argparse
import time
import logging
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from _common import (
    setup_django, cache_key, get_db_connection, extract_json_from_claude,
    call_claude_cli, save_to_research_cache, CACHE_DIR, SKIP_DOMAINS,
)
setup_django()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

DATABASE_URL = os.environ['DATABASE_URL']

# Max content to send to Claude (keep under token limits)
MAX_CONTENT_CHARS = 15000

EXTRACTION_PROMPT = """You are a business research assistant extracting FACTUAL profile data.

CRITICAL: Only extract information that is EXPLICITLY stated on the website.
DO NOT make assumptions or infer anything. If information is not clearly stated, leave that field empty.

Person: {name}
Website: {website}

Website Content:
<content>
{content}
</content>

Extract the following fields. Only include information that is DIRECTLY stated:

1. what_you_do: What is their primary business/service? (1-2 sentences max)
2. who_you_serve: Who is their target audience? (1 sentence max)
3. seeking: What partnerships/collaborations they're actively looking for?
4. offering: What do they offer to partners/collaborators?
5. social_proof: Notable credentials (bestseller, certifications, audience size)
6. signature_programs: Named courses, books, frameworks, certifications
7. booking_link: Calendar booking URL (Calendly, Acuity, etc.)
8. niche: Primary market niche (1-3 words)
9. phone: Business phone number if publicly displayed
10. company: Company or business name
11. list_size: Email list or audience size as integer (null if unknown)
12. tags: 3-7 keyword tags describing expertise (lowercase, 1-3 words each)
13. audience_type: Type of audience they serve (B2B, B2C, coaches, etc.)
14. business_focus: Primary business focus in 1 sentence
15. service_provided: Specific services (comma-separated)

Return ONLY valid JSON, no markdown fences:
{{
    "what_you_do": "",
    "who_you_serve": "",
    "seeking": "",
    "offering": "",
    "social_proof": "",
    "signature_programs": "",
    "booking_link": "",
    "niche": "",
    "phone": "",
    "company": "",
    "list_size": null,
    "tags": [],
    "audience_type": "",
    "business_focus": "",
    "service_provided": "",
    "confidence": "high/medium/low"
}}

IMPORTANT: Business accuracy matters - do NOT fabricate or assume."""


def get_exa_failed_profiles(limit: int) -> List[Dict]:
    """
    Find profiles that have a cache entry but Exa returned 'not indexed' or empty.
    These have websites that Exa can't process - perfect candidates for crawl4ai.
    """
    # Load cache entries to find ones with minimal data (Exa failures)
    cache_entries = {}
    for fp in glob.glob(os.path.join(CACHE_DIR, '*.json')):
        try:
            with open(fp) as fh:
                data = json.load(fh)
            name = data.get('name', '')
            if name:
                key = cache_key(name)
                cache_entries[key] = data
        except Exception:
            continue

    # Get profiles from DB that have websites
    conn, cur = get_db_connection()
    cur.execute("""
        SELECT id, name, email, company, website, linkedin,
               list_size, seeking, who_you_serve, what_you_do, offering
        FROM profiles
        WHERE name IS NOT NULL AND name != ''
          AND website IS NOT NULL AND website != ''
        ORDER BY list_size DESC NULLS LAST
    """)
    all_profiles = cur.fetchall()
    cur.close()
    conn.close()

    failures = []
    for p in all_profiles:
        key = cache_key(p['name'])
        cached = cache_entries.get(key)

        if not cached:
            # No cache entry at all — might still be running in Exa
            # Only include if it has a website (for crawl4ai)
            continue

        # Skip if already enriched by crawl4ai
        if cached.get('_crawl4ai_enriched'):
            continue

        # Check if cache entry is sparse (Exa failed to extract much)
        has_content_platforms = bool(cached.get('content_platforms'))
        has_what_you_do = bool(cached.get('what_you_do'))

        # If Exa already got good data, skip
        if has_content_platforms and has_what_you_do:
            continue

        # This profile has a website but Exa couldn't extract much
        website = p.get('website', '').strip()
        if not website or not website.startswith('http'):
            continue

        # Skip booking links, linktree, social-only URLs
        if any(pat in website.lower() for pat in SKIP_DOMAINS):
            continue

        failures.append(dict(p))
        if len(failures) >= limit:
            break

    return failures


def scrape_website(url: str, max_retries: int = 3) -> Optional[str]:
    """Scrape a website using crawl4ai with retries. Returns markdown content."""
    from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, BrowserConfig

    for attempt in range(1, max_retries + 1):
        try:
            # Enable JS on all attempts for retry runs (more sites need it)
            browser_config = BrowserConfig(
                headless=True,
                java_script_enabled=True,
            )

            crawl_config = CrawlerRunConfig(
                exclude_external_links=False,
                verbose=False,
                page_timeout=30000 if attempt == 1 else 45000,
            )

            async def _crawl():
                async with AsyncWebCrawler(config=browser_config) as crawler:
                    result = await crawler.arun(url=url, config=crawl_config)
                    if result and result.success:
                        content = (
                            result.markdown_v2.fit_markdown
                            if hasattr(result, 'markdown_v2') and result.markdown_v2
                            else result.markdown
                        )
                        return content
                return None

            content = asyncio.run(_crawl())
            if content and len(content) >= 100:
                return content

            if attempt < max_retries:
                time.sleep(2 * attempt)

        except Exception as e:
            if attempt < max_retries:
                logger.debug(f"crawl4ai attempt {attempt} failed for {url}: {e}")
                time.sleep(2 * attempt)
            else:
                logger.error(f"crawl4ai failed after {max_retries} attempts for {url}: {e}")

    return None


def _get_openrouter_client():
    """Get OpenRouter client (cached)."""
    from openai import OpenAI
    return OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=os.environ['OPENROUTER_API_KEY'],
    )


# Set to 'api' to use OpenRouter, 'cli' to use Claude Max CLI
EXTRACTION_MODE = os.environ.get('EXTRACTION_MODE', 'cli')


def extract_with_haiku(name: str, website: str, content: str, max_retries: int = 2) -> Optional[Dict]:
    """
    Extract structured profile data.
    Mode 'api': OpenRouter Haiku (~$0.001/call, ~2-5s)
    Mode 'cli': Claude Max CLI (free, ~18-25s)
    """
    prompt = EXTRACTION_PROMPT.format(
        name=name,
        website=website,
        content=content[:MAX_CONTENT_CHARS],
    )

    for attempt in range(1, max_retries + 1):
        try:
            if EXTRACTION_MODE == 'api':
                client = _get_openrouter_client()
                response = client.chat.completions.create(
                    model="anthropic/claude-3-haiku",
                    max_tokens=2048,
                    temperature=0,
                    messages=[{"role": "user", "content": prompt}],
                )
                text = response.choices[0].message.content.strip()
                parsed = extract_json_from_claude(text)
                if parsed is not None:
                    return parsed
                raise json.JSONDecodeError("No JSON found in response", text[:100], 0)
            else:
                parsed = call_claude_cli(prompt, model='haiku', timeout=120)
                if parsed is not None:
                    return parsed
                # call_claude_cli logs its own errors
                if attempt < max_retries:
                    time.sleep(3)
                    continue
                return None

        except json.JSONDecodeError as e:
            logger.debug(f"JSON parse failed for {name} (attempt {attempt}): {e}")
            if attempt < max_retries:
                time.sleep(1)
                continue
            logger.error(f"Failed to parse response for {name} after {max_retries} attempts")
        except Exception as e:
            error_str = str(e).lower()
            if '403' in error_str or 'key limit' in error_str:
                logger.error(f"OpenRouter key limit hit: {e}")
                raise
            logger.error(f"Extraction failed for {name} (attempt {attempt}): {e}")
            if attempt < max_retries:
                time.sleep(2)
                continue

    return None


def process_single_profile(profile: Dict) -> Dict:
    """Process one profile: scrape + extract + save. Thread-safe."""
    name = profile['name']
    website = profile.get('website', '')
    result = {'name': name, 'status': 'failed'}

    # Step 1: Scrape with crawl4ai
    content = scrape_website(website)
    if not content or len(content) < 100:
        result['status'] = 'scrape_failed'
        return result

    result['scraped_chars'] = len(content)

    # Step 2: Extract with Haiku API
    extracted = extract_with_haiku(name, website, content)
    if not extracted:
        result['status'] = 'extract_failed'
        return result

    # Step 3: Save to cache
    if website and not extracted.get('website'):
        extracted['website'] = website
    if save_to_research_cache(name, extracted, '_crawl4ai_enriched'):
        found_fields = [
            f for f in ['what_you_do', 'who_you_serve', 'niche', 'booking_link',
                        'phone', 'company', 'tags', 'service_provided']
            if extracted.get(f) and str(extracted[f]) not in ('', '[]', '{}')
        ]
        result['status'] = 'saved'
        result['fields'] = found_fields
        result['confidence'] = extracted.get('confidence', 'unknown')
    else:
        result['status'] = 'save_failed'

    return result


def main():
    parser = argparse.ArgumentParser(description='Fallback enrichment via crawl4ai + Claude Max')
    parser.add_argument('--limit', type=int, default=50,
                        help='Max profiles to process (default 50)')
    parser.add_argument('--concurrency', type=int, default=8,
                        help='Parallel workers (default 10 for API)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview without scraping or calling Claude')
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("CRAWL4AI + OPENROUTER HAIKU FALLBACK ENRICHMENT")
    print(f"{'='*60}\n")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print(f"Limit: {args.limit}")
    print(f"Concurrency: {args.concurrency}")
    mode_label = "OpenRouter Haiku API" if EXTRACTION_MODE == 'api' else "Claude Max CLI (free)"
    cost = f"~${args.limit * 0.001:.2f}" if EXTRACTION_MODE == 'api' else "$0.00"
    print(f"Extraction: {mode_label}")
    print(f"Cost: {cost}")
    print()

    profiles = get_exa_failed_profiles(args.limit)
    print(f"Exa-failed profiles with websites: {len(profiles)}")

    if not profiles:
        print("Nothing to do — either Exa hasn't finished or no failures found.")
        return

    if args.dry_run:
        print("\nFirst 10 profiles that would be processed:")
        for i, p in enumerate(profiles[:10]):
            print(f"  {i+1}. {p['name']} — {p.get('website', 'N/A')}")
        return

    stats = {
        'total': len(profiles),
        'scraped': 0,
        'extracted': 0,
        'saved': 0,
        'scrape_failed': 0,
        'extract_failed': 0,
    }
    stats_lock = threading.Lock()
    start_time = time.time()
    processed = [0]

    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = {
            executor.submit(process_single_profile, p): p
            for p in profiles
        }

        for future in as_completed(futures):
            result = future.result()

            with stats_lock:
                processed[0] += 1
                i = processed[0]

                if result['status'] == 'saved':
                    stats['saved'] += 1
                    stats['scraped'] += 1
                    stats['extracted'] += 1
                    fields = result.get('fields', [])
                    conf = result.get('confidence', '?')
                    logger.info(f"  [{i}/{len(profiles)}] {result['name']}: "
                                f"{fields} (confidence: {conf})")
                elif result['status'] == 'scrape_failed':
                    stats['scrape_failed'] += 1
                elif result['status'] == 'extract_failed':
                    stats['scraped'] += 1
                    stats['extract_failed'] += 1
                    logger.warning(f"  [{i}/{len(profiles)}] {result['name']}: extract failed")
                else:
                    stats['scrape_failed'] += 1

                if i % 25 == 0:
                    elapsed = time.time() - start_time
                    rate = i / elapsed * 60 if elapsed > 0 else 0
                    remaining = len(profiles) - i
                    eta = remaining / (i / elapsed) / 60 if elapsed > 0 and i > 0 else 0
                    print(f"\n  === Progress: {i}/{len(profiles)} "
                          f"({stats['saved']} saved, {stats['scrape_failed']} scrape fails) "
                          f"— {rate:.0f}/min, ETA {eta:.1f}min ===\n")

    elapsed = time.time() - start_time

    print(f"\n{'='*60}")
    print("FALLBACK ENRICHMENT SUMMARY")
    print(f"{'='*60}\n")
    print(f"Total candidates:  {stats['total']}")
    print(f"Scraped:           {stats['scraped']}")
    print(f"Extracted:         {stats['extracted']}")
    print(f"Saved to cache:    {stats['saved']}")
    print(f"Scrape failed:     {stats['scrape_failed']}")
    print(f"Extract failed:    {stats['extract_failed']}")
    print(f"Runtime:           {elapsed/60:.1f} min")
    print(f"Cost:              ~${stats['saved'] * 0.001:.2f}")
    print(f"\nNext: run consolidate_cache_to_supabase.py to push to DB")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
