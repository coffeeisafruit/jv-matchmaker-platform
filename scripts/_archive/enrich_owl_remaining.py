#!/usr/bin/env python3
"""
OWL-powered enrichment for the remaining ~68 profiles that crawl4ai couldn't handle.

Strategy:
1. DuckDuckGo search to find additional pages about each person
2. Playwright (via OWL venv) to browse websites (handles anti-bot better)
3. Claude Max CLI for extraction/synthesis (free, no API cost)

Saves results to the same research cache format for consolidation.

Usage:
    python scripts/enrich_owl_remaining.py --dry-run   # Preview
    python scripts/enrich_owl_remaining.py              # Live run
    python scripts/enrich_owl_remaining.py --limit 10   # Small batch
"""

import os
import sys
import json
import glob
import argparse
import time
import logging
import subprocess
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional

from _common import (
    setup_django, cache_key, get_db_connection, call_claude_cli,
    save_to_research_cache, PROJECT_ROOT, CACHE_DIR, SKIP_DOMAINS,
)
setup_django()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

OWL_DIR = os.path.join(PROJECT_ROOT, 'owl_framework')
OWL_PYTHON = os.path.join(OWL_DIR, '.venv', 'bin', 'python')

MAX_CONTENT_CHARS = 15000

# Contact page paths to check
CONTACT_PATHS = [
    '/contact', '/contact-us', '/about', '/book', '/book-a-call',
    '/schedule', '/work-with-me', '/connect', '/get-started',
]

EXTRACTION_PROMPT = """You are a business research assistant extracting FACTUAL profile data.

CRITICAL: Only extract information that is EXPLICITLY stated in the research data.
DO NOT make assumptions or infer anything. If information is not clearly stated, leave that field empty.

Person: {name}
Company: {company}
Website: {website}

Research Data (from DuckDuckGo search + website browsing):
<research>
{research}
</research>

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
16. email: Contact email address if publicly displayed

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
    "email": "",
    "confidence": "high/medium/low"
}}

IMPORTANT: Business accuracy matters - do NOT fabricate or assume."""


def get_remaining_profiles(limit: int) -> List[Dict]:
    """Find profiles that crawl4ai also couldn't enrich."""
    cache_entries = {}
    for fp in glob.glob(os.path.join(CACHE_DIR, '*.json')):
        try:
            with open(fp) as fh:
                data = json.load(fh)
            name = data.get('name', '')
            if name:
                cache_entries[cache_key(name)] = data
        except Exception:
            continue

    conn, cur = get_db_connection()
    cur.execute("""
        SELECT id, name, email, company, website, linkedin
        FROM profiles
        WHERE name IS NOT NULL AND name != ''
          AND website IS NOT NULL AND website != ''
        ORDER BY name
    """)
    profiles = cur.fetchall()
    cur.close()
    conn.close()

    remaining = []
    for p in profiles:
        key = cache_key(p['name'])
        cached = cache_entries.get(key)
        if not cached:
            continue
        # Skip already enriched by crawl4ai or OWL
        if cached.get('_crawl4ai_enriched') or cached.get('_owl_enriched'):
            continue
        has_cp = bool(cached.get('content_platforms'))
        has_wyd = bool(cached.get('what_you_do'))
        if has_cp and has_wyd:
            continue
        website = (p.get('website') or '').strip()
        if not website or not website.startswith('http'):
            continue
        if any(pat in website.lower() for pat in SKIP_DOMAINS):
            continue
        remaining.append(dict(p))
        if len(remaining) >= limit:
            break

    return remaining


def search_duckduckgo(query: str, max_results: int = 8) -> List[Dict]:
    """Search DuckDuckGo using ddgs package."""
    try:
        from ddgs import DDGS
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results))
            return [{
                'title': r.get('title', ''),
                'href': r.get('href', ''),
                'body': r.get('body', ''),
            } for r in results]
    except Exception as e:
        logger.warning(f"DuckDuckGo search failed: {e}")
        return []


def browse_with_playwright(url: str, timeout: int = 45) -> Optional[str]:
    """Browse a URL using Playwright in OWL's venv. Returns text content."""
    script = f'''
import json
import asyncio
from playwright.async_api import async_playwright

async def fetch_page():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        try:
            await page.goto("{url}", timeout=30000, wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle", timeout=10000)
            text = await page.evaluate("() => document.body.innerText")
            title = await page.title()
            result = {{"url": "{url}", "title": title, "text": text[:15000], "ok": True}}
        except Exception as e:
            result = {{"url": "{url}", "error": str(e), "ok": False}}
        finally:
            await browser.close()
        return result

result = asyncio.run(fetch_page())
print(json.dumps(result))
'''

    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.py', dir=OWL_DIR, delete=False
    ) as f:
        f.write(script)
        script_path = f.name

    try:
        result = subprocess.run(
            [OWL_PYTHON, script_path],
            capture_output=True, text=True,
            timeout=timeout, cwd=OWL_DIR,
        )
        if result.returncode == 0 and result.stdout.strip():
            data = json.loads(result.stdout.strip())
            if data.get('ok') and data.get('text'):
                return data['text']
        return None
    except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception) as e:
        logger.debug(f"Playwright browse failed for {url}: {e}")
        return None
    finally:
        try:
            os.unlink(script_path)
        except OSError:
            pass


def extract_with_claude_cli(name: str, company: str, website: str, research_text: str) -> Optional[Dict]:
    """Use Claude Max CLI to extract structured data from research."""
    prompt = EXTRACTION_PROMPT.format(
        name=name,
        company=company or 'Unknown',
        website=website,
        research=research_text[:MAX_CONTENT_CHARS],
    )

    for attempt in range(1, 3):
        result = call_claude_cli(prompt, model='haiku', timeout=120)
        if result is not None:
            return result
        # call_claude_cli logs its own errors
        if attempt < 2:
            time.sleep(3)

    return None


def process_profile(profile: Dict) -> Dict:
    """Full OWL enrichment pipeline for one profile."""
    name = profile['name']
    company = profile.get('company') or ''
    website = profile.get('website', '')
    result = {'name': name, 'status': 'failed'}

    research_parts = []

    # Step 1: DuckDuckGo search
    logger.info(f"  [DDG] Searching for {name}...")
    queries = [
        f'"{name}" {company} about',
        f'"{name}" contact email "book a call"',
    ]

    for q in queries:
        search_results = search_duckduckgo(q, max_results=5)
        for sr in search_results:
            if sr.get('body'):
                research_parts.append(
                    f"[SEARCH: {sr.get('href', '')}]\n{sr.get('title', '')}\n{sr['body']}"
                )
        time.sleep(0.5)  # Rate limit

    # Step 2: Browse website with Playwright
    if website.startswith('http'):
        # Handle Google redirect URLs
        if 'google.com/url' in website:
            import urllib.parse
            parsed = urllib.parse.urlparse(website)
            params = urllib.parse.parse_qs(parsed.query)
            actual_url = params.get('url', params.get('q', [website]))[0]
            website = actual_url
            logger.info(f"  [URL] Resolved Google redirect → {website}")

        # Skip PDFs
        if website.lower().endswith('.pdf'):
            logger.info(f"  [SKIP] PDF URL: {website}")
            result['status'] = 'pdf_skip'
            # Still try search results
        else:
            logger.info(f"  [PW] Browsing {website}...")
            content = browse_with_playwright(website)
            if content and len(content) > 50:
                research_parts.append(f"[WEBSITE: {website}]\n{content[:8000]}")
                result['browsed'] = True

                # Try contact pages
                base_url = website.rstrip('/')
                for path in CONTACT_PATHS[:5]:
                    contact_url = f"{base_url}{path}"
                    contact_content = browse_with_playwright(contact_url, timeout=30)
                    if contact_content and len(contact_content) > 50:
                        research_parts.append(f"[CONTACT: {contact_url}]\n{contact_content[:3000]}")
                        logger.info(f"    Found contact page: {path}")
                        break  # One contact page is enough
            else:
                logger.info(f"  [PW] Website browse failed, relying on search results")

    # Step 3: Also try to find and browse their actual website if the listed one is third-party
    third_party_domains = [
        'speakerhub.com', 'zoominfo.com', 'forbes.com', 'deezer.com',
        'fountain.fm', 'redcircle.com', 'skool.com', 'spokeo.com',
        'theentrepreneurway.com', 'executivesupportmedia.com',
        'entrepreneursunited.godaddysites.com', 'yaledailynews.com',
        'trempcountytimes.com', 'rgj.com', 'actionera.com',
        'hybridglobalpublishing.com',
    ]
    is_third_party = any(d in website.lower() for d in third_party_domains)

    if is_third_party:
        logger.info(f"  [DDG] Third-party URL detected, searching for real website...")
        discovery = search_duckduckgo(f'"{name}" official website', max_results=5)
        skip_domains = [
            'linkedin.com', 'twitter.com', 'facebook.com', 'instagram.com',
            'youtube.com', 'tiktok.com', 'pinterest.com', 'speakerhub.com',
            'zoominfo.com', 'spokeo.com', 'amazon.com',
        ]
        for sr in discovery:
            url = sr.get('href', '')
            if url and not any(d in url.lower() for d in skip_domains):
                logger.info(f"  [PW] Found potential website: {url}")
                alt_content = browse_with_playwright(url)
                if alt_content and len(alt_content) > 100:
                    research_parts.append(f"[DISCOVERED WEBSITE: {url}]\n{alt_content[:8000]}")
                    result['discovered_site'] = url
                break
        time.sleep(0.5)

    if not research_parts:
        result['status'] = 'no_research_data'
        return result

    # Step 4: Claude Max CLI extraction
    research_text = '\n\n---\n\n'.join(research_parts)
    logger.info(f"  [CLI] Extracting with Claude Max ({len(research_text)} chars)...")

    extracted = extract_with_claude_cli(name, company, website, research_text)
    if not extracted:
        result['status'] = 'extract_failed'
        return result

    # Step 5: Save to cache
    if website and not extracted.get('website'):
        extracted['website'] = website
    if save_to_research_cache(name, extracted, '_owl_enriched'):
        found_fields = [
            f for f in ['what_you_do', 'who_you_serve', 'niche', 'booking_link',
                        'phone', 'email', 'company', 'tags', 'service_provided']
            if extracted.get(f) and str(extracted[f]) not in ('', '[]', '{}')
        ]
        result['status'] = 'saved'
        result['fields'] = found_fields
        result['confidence'] = extracted.get('confidence', 'unknown')
    else:
        result['status'] = 'save_failed'

    return result


def main():
    parser = argparse.ArgumentParser(description='OWL enrichment for remaining profiles')
    parser.add_argument('--limit', type=int, default=100,
                        help='Max profiles to process')
    parser.add_argument('--concurrency', type=int, default=4,
                        help='Parallel workers (default 4)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview without processing')
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print("OWL ENRICHMENT (DuckDuckGo + Playwright + Claude Max CLI)")
    print(f"{'='*60}\n")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print(f"Limit: {args.limit}")
    print(f"Concurrency: {args.concurrency} workers")
    print(f"Extraction: Claude Max CLI (free)")
    print(f"Cost: $0.00")
    print()

    profiles = get_remaining_profiles(args.limit)
    print(f"Remaining profiles to process: {len(profiles)}")

    if not profiles:
        print("All profiles enriched!")
        return

    if args.dry_run:
        print("\nProfiles that would be processed:")
        for i, p in enumerate(profiles):
            print(f"  {i+1}. {p['name']} — {p.get('website', 'N/A')}")
        return

    stats = {'total': len(profiles), 'saved': 0, 'failed': 0, 'no_data': 0}
    stats_lock = threading.Lock()
    start_time = time.time()
    processed = [0]

    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = {
            executor.submit(process_profile, p): p
            for p in profiles
        }

        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as e:
                with stats_lock:
                    processed[0] += 1
                    stats['failed'] += 1
                    logger.error(f"  Unexpected error: {e}")
                continue

            with stats_lock:
                processed[0] += 1
                i = processed[0]

                if result['status'] == 'saved':
                    stats['saved'] += 1
                    fields = result.get('fields', [])
                    conf = result.get('confidence', '?')
                    logger.info(f"  [{i}/{len(profiles)}] {result['name']}: "
                                f"{fields} (confidence: {conf})")
                    if result.get('discovered_site'):
                        logger.info(f"    Discovered: {result['discovered_site']}")
                elif result['status'] == 'no_research_data':
                    stats['no_data'] += 1
                    logger.warning(f"  [{i}/{len(profiles)}] {result['name']}: no research data")
                else:
                    stats['failed'] += 1
                    logger.warning(f"  [{i}/{len(profiles)}] {result['name']}: {result['status']}")

                if i % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = i / elapsed * 60 if elapsed > 0 else 0
                    print(f"\n  === Progress: {i}/{len(profiles)} "
                          f"({stats['saved']} saved, {stats['failed']} failed) "
                          f"— {rate:.1f}/min ===\n")

    elapsed = time.time() - start_time

    print(f"\n{'='*60}")
    print("OWL ENRICHMENT SUMMARY")
    print(f"{'='*60}\n")
    print(f"Total processed:   {stats['saved'] + stats['failed'] + stats['no_data']}")
    print(f"Saved to cache:    {stats['saved']}")
    print(f"No data found:     {stats['no_data']}")
    print(f"Failed:            {stats['failed']}")
    print(f"Runtime:           {elapsed/60:.1f} min")
    print(f"Cost:              $0.00 (Claude Max CLI)")
    print(f"\nNext: run consolidate_cache_to_supabase.py to push to DB")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
