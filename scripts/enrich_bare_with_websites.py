#!/usr/bin/env python3
"""
Phase 2: Enrich bare profiles that have websites but zero key enrichment fields.

Uses DuckDuckGo search + Playwright browsing + Claude Max CLI extraction.
Writes results directly to Supabase.

Usage:
    python scripts/enrich_bare_with_websites.py --dry-run
    python scripts/enrich_bare_with_websites.py --concurrency 3
"""
import os, sys, json, argparse, time, logging, subprocess, tempfile, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from _common import (
    setup_django, cache_key, get_db_connection, call_claude_cli,
    save_to_research_cache, PROJECT_ROOT,
)
setup_django()

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

OWL_PYTHON = os.path.join(PROJECT_ROOT, 'owl_framework', '.venv', 'bin', 'python')

EXTRACTION_PROMPT = """You are a business research assistant extracting FACTUAL profile data.

CRITICAL: Only extract information that is EXPLICITLY stated in the research data.
DO NOT make assumptions or infer anything. If information is not clearly stated, leave that field empty.

Person: {name}
Company: {company}
Website: {website}

Research Data (from search + website browsing):
<research>
{research}
</research>

Extract the following fields. Only include information that is DIRECTLY stated:

1. what_you_do: What is their primary business/service? (1-2 sentences max)
2. who_you_serve: Who is their target audience? (1 sentence max)
3. seeking: What partnerships/collaborations they're looking for?
4. offering: What do they offer to partners/collaborators?
5. niche: Primary market niche (1-3 words)
6. bio: 2-3 sentence professional bio
7. tags: 3-7 keyword tags describing expertise (lowercase, 1-3 words each)
8. audience_type: B2B, B2C, Coaches/Consultants, Entrepreneurs, Corporate
9. business_focus: Primary business focus in 1 sentence
10. service_provided: Specific services (comma-separated)
11. network_role: Connector, Expert, Promoter, Service Provider, Educator, Creator
12. email: Contact email if publicly displayed
13. phone: Business phone if publicly displayed
14. booking_link: Calendar booking URL if found
15. signature_programs: Named courses, books, frameworks

Return ONLY valid JSON, no markdown fences:
{{
    "what_you_do": "",
    "who_you_serve": "",
    "seeking": "",
    "offering": "",
    "niche": "",
    "bio": "",
    "tags": [],
    "audience_type": "",
    "business_focus": "",
    "service_provided": "",
    "network_role": "",
    "email": "",
    "phone": "",
    "booking_link": "",
    "signature_programs": ""
}}

IMPORTANT: Business accuracy matters - do NOT fabricate or assume."""


def get_bare_profiles_with_websites():
    """Get profiles that have zero key enrichment fields but have a website."""
    conn, cur = get_db_connection()
    cur.execute("""
        SELECT id, name, email, company, website, linkedin
        FROM profiles
        WHERE (what_you_do IS NULL OR what_you_do = '')
          AND (who_you_serve IS NULL OR who_you_serve = '')
          AND (niche IS NULL OR niche = '')
          AND (bio IS NULL OR bio = '')
          AND (offering IS NULL OR offering = '')
          AND (seeking IS NULL OR seeking = '')
          AND website IS NOT NULL AND website != ''
          AND website LIKE 'http%'
    """)
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def search_duckduckgo(query, max_results=8):
    """Search DuckDuckGo using ddgs package."""
    try:
        from ddgs import DDGS
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results))
            return [{'title': r.get('title', ''), 'href': r.get('href', ''),
                     'body': r.get('body', '')} for r in results]
    except Exception as e:
        logger.warning(f"DDG search failed: {e}")
        return []


def browse_with_playwright(url, timeout=45):
    """Browse URL using Playwright via OWL venv."""
    script = f"""
import asyncio
from playwright.async_api import async_playwright

async def browse():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            await page.goto("{url}", timeout=30000, wait_until="domcontentloaded")
            await asyncio.sleep(2)
            text = await page.evaluate("() => document.body ? document.body.innerText : ''")
            print(text[:15000])
        except Exception as e:
            print(f"ERROR: {{e}}")
        finally:
            await browser.close()

asyncio.run(browse())
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(script)
        script_path = f.name

    try:
        result = subprocess.run(
            [OWL_PYTHON, script_path],
            capture_output=True, text=True, timeout=timeout
        )
        return result.stdout.strip() if result.returncode == 0 else None
    except subprocess.TimeoutExpired:
        return None
    finally:
        os.unlink(script_path)


def extract_with_claude(name, company, website, research_text, model='haiku'):
    """Extract profile data using Claude Max CLI."""
    prompt = EXTRACTION_PROMPT.format(
        name=name,
        company=company or 'Unknown',
        website=website,
        research=research_text[:15000]
    )
    return call_claude_cli(prompt, model=model, timeout=180 if model == 'opus' else 90)


def enrich_profile(profile, model='haiku'):
    """Full enrichment pipeline for a single profile."""
    name = profile['name']
    website = profile['website']
    company = profile.get('company') or ''
    logger.info(f"Enriching: {name} — {website}")

    research_parts = []

    # 1. DuckDuckGo search
    queries = [f"{name} {company}".strip(), f"{name} site:{website.split('/')[2]}"]
    for q in queries:
        results = search_duckduckgo(q, max_results=5)
        for r in results:
            research_parts.append(f"[Search: {r['title']}]\n{r['body']}")

    # 2. Browse primary website
    page_content = browse_with_playwright(website)
    if page_content and 'ERROR' not in page_content[:20]:
        research_parts.append(f"[Website: {website}]\n{page_content[:8000]}")

    # 3. Try to find and browse their actual website if URL is third-party
    from urllib.parse import urlparse
    domain = urlparse(website).netloc.lower()
    third_party_domains = [
        'speakerhub.com', 'zoominfo.com', 'pickmybrain.world',
        'kruger', 'phenomenalwomen', 'womensmediacenter',
        'theentrepreneurway', 'fourroomsmastermind', 'yokovillage',
        'billionairesinboxers'
    ]
    if any(tp in domain for tp in third_party_domains):
        own_results = search_duckduckgo(f"{name} official website", max_results=3)
        for r in own_results:
            href = r.get('href', '')
            if href and not any(tp in href for tp in third_party_domains):
                own_content = browse_with_playwright(href)
                if own_content and 'ERROR' not in own_content[:20]:
                    research_parts.append(f"[Own site: {href}]\n{own_content[:5000]}")
                    break

    if not research_parts:
        logger.warning(f"No research data for {name}")
        return None

    # 4. Extract with Claude
    research_text = '\n\n---\n\n'.join(research_parts)
    extracted = extract_with_claude(name, company, website, research_text, model)

    if not extracted:
        logger.warning(f"Extraction failed for {name}")
        return None

    return extracted


def save_to_db(profile_id, name, data):
    """Write extracted data to Supabase."""
    fields_to_write = [
        'what_you_do', 'who_you_serve', 'seeking', 'offering', 'niche',
        'bio', 'audience_type', 'business_focus', 'service_provided',
        'network_role', 'email', 'phone', 'booking_link', 'signature_programs'
    ]

    conn, cur = get_db_connection()
    now = datetime.now()
    updates = []
    values = []

    for field in fields_to_write:
        val = data.get(field)
        if val and isinstance(val, str) and val.strip():
            updates.append(f"{field} = %s")
            values.append(val.strip())

    # Handle tags (array)
    tags = data.get('tags')
    if tags and isinstance(tags, list) and len(tags) > 0:
        updates.append("tags = %s")
        values.append(tags)

    if not updates:
        cur.close()
        conn.close()
        return 0

    updates.append("updated_at = %s")
    values.append(now)
    values.append(profile_id)

    sql = f"UPDATE profiles SET {', '.join(updates)} WHERE id = %s"
    cur.execute(sql, values)
    written = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return written


# ── Main ──────────────────────────────────────────────────────────────

lock = threading.Lock()
stats = {'processed': 0, 'saved': 0, 'failed': 0}


def process_one(profile, model='haiku'):
    name = profile['name']
    try:
        data = enrich_profile(profile, model)
        if data:
            written = save_to_db(profile['id'], name, data)
            save_to_research_cache(name, data, '_phase2_enriched', overwrite=True)
            with lock:
                stats['processed'] += 1
                if written > 0:
                    stats['saved'] += 1
                    print(f"  OK  {name} — {written} fields written")
                else:
                    stats['failed'] += 1
                    print(f"  --  {name} — no new fields")
        else:
            with lock:
                stats['processed'] += 1
                stats['failed'] += 1
                print(f"  FAIL {name}")
    except Exception as e:
        with lock:
            stats['processed'] += 1
            stats['failed'] += 1
            print(f"  ERR  {name}: {e}")


def main():
    parser = argparse.ArgumentParser(description='Enrich bare profiles with websites')
    parser.add_argument('--concurrency', type=int, default=3)
    parser.add_argument('--model', default='haiku',
                        choices=['haiku', 'sonnet', 'opus'],
                        help='Claude model for extraction (default: haiku)')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    profiles = get_bare_profiles_with_websites()

    print(f"\n{'='*60}")
    print(f"PHASE 2: ENRICH BARE PROFILES WITH WEBSITES")
    print(f"{'='*60}")
    print(f"Profiles found: {len(profiles)}")
    print(f"Model: {args.model}")
    print(f"Concurrency: {args.concurrency}")

    if not profiles:
        print("No profiles to enrich.")
        return

    for p in profiles:
        print(f"  - {p['name']} — {p['website'][:60]}")

    if args.dry_run:
        print("\nDRY RUN — no changes made.")
        return

    start = time.time()

    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = [executor.submit(process_one, p, args.model) for p in profiles]
        for f in as_completed(futures):
            f.result()  # propagate exceptions

    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"PHASE 2 COMPLETE")
    print(f"{'='*60}")
    print(f"Processed: {stats['processed']}")
    print(f"Saved: {stats['saved']}")
    print(f"Failed: {stats['failed']}")
    print(f"Time: {elapsed:.0f}s")


if __name__ == '__main__':
    main()
