#!/usr/bin/env python3
"""
Scrape bare profiles that have websites but zero enrichment data.
Uses crawl4ai + Claude Max CLI to extract profile data directly to Supabase.
"""

import os
import sys
import json
import re
import time
import logging
import subprocess
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

SKIP_PATTERNS = [
    'calendly.com', 'acuityscheduling.com', 'tidycal.com',
    'oncehub.com', 'youcanbook.me', 'bookme.',
    'linktr.ee', 'linktree.com',
    'facebook.com', 'instagram.com', 'twitter.com',
    'linkedin.com', 'tiktok.com',
    'yaledailynews.com', 'speakerhub.com',
]

EXTRACTION_PROMPT = """Extract a professional profile for {name} from this website content.

Website: {website}

Return a JSON object with these fields (leave empty string if not found):
{{
    "what_you_do": "1-2 sentence description of their professional work",
    "who_you_serve": "their target audience/clients",
    "niche": "2-5 word niche label",
    "offering": "what they can offer to partners",
    "seeking": "what they might seek in collaborations",
    "bio": "2-3 sentence professional bio",
    "company": "company name if found",
    "service_provided": "main services",
    "business_focus": "business focus area",
    "tags": ["tag1", "tag2", "tag3"],
    "email": "email if found",
    "phone": "phone if found",
    "booking_link": "booking URL if found"
}}

Return ONLY valid JSON, no explanation.

Content:
{content}"""


def get_bare_profiles_with_websites() -> List[Dict]:
    """Get profiles with zero enrichment but having a website."""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)
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
          AND name IS NOT NULL AND name != ''
        ORDER BY name
    """)
    profiles = cur.fetchall()
    cur.close()
    conn.close()

    # Filter out skip patterns
    filtered = []
    for p in profiles:
        website = (p.get('website') or '').lower()
        if any(pat in website for pat in SKIP_PATTERNS):
            continue
        if not website.startswith('http'):
            continue
        filtered.append(dict(p))

    return filtered


def scrape_with_crawl4ai(url: str) -> Optional[str]:
    """Scrape website using crawl4ai."""
    try:
        import asyncio
        from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, BrowserConfig

        browser_config = BrowserConfig(headless=True, java_script_enabled=True)
        crawl_config = CrawlerRunConfig(exclude_external_links=False, verbose=False, page_timeout=30000)

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

        return asyncio.run(_crawl())
    except Exception as e:
        logger.error(f"Scrape failed for {url}: {e}")
        return None


def extract_with_claude(name: str, website: str, content: str) -> Optional[Dict]:
    """Extract profile data using Claude Max CLI."""
    prompt = EXTRACTION_PROMPT.format(
        name=name,
        website=website,
        content=content[:12000],
    )

    try:
        result = subprocess.run(
            ['claude', '--print', '--model', 'haiku', '-p', prompt],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            return None

        text = result.stdout.strip()
        if text.startswith('```'):
            lines = text.split('\n')
            text = '\n'.join(lines[1:-1] if lines[-1].startswith('```') else lines[1:])
        if not text.startswith('{'):
            match = re.search(r'\{[\s\S]*\}', text)
            if match:
                text = match.group(0)

        return json.loads(text)
    except Exception as e:
        logger.error(f"Extract failed for {name}: {e}")
        return None


def write_to_db(profile_id: str, name: str, extracted: Dict) -> int:
    """Write extracted data to Supabase."""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    now = datetime.now()
    updates = []
    params = []

    text_fields = ['what_you_do', 'who_you_serve', 'niche', 'offering', 'seeking',
                   'bio', 'company', 'service_provided', 'business_focus',
                   'email', 'phone', 'booking_link']

    for field in text_fields:
        val = extracted.get(field)
        if val and isinstance(val, str) and val.strip():
            updates.append(f"{field} = %s")
            params.append(val.strip())

    tags = extracted.get('tags')
    if tags and isinstance(tags, list) and len(tags) > 0:
        updates.append("tags = %s")
        params.append(tags)

    if not updates:
        cur.close()
        conn.close()
        return 0

    updates.append("enrichment_metadata = COALESCE(enrichment_metadata, '{}'::jsonb) || %s::jsonb")
    meta = {
        'last_enrichment': 'crawl4ai_bare',
        'enriched_at': now.isoformat(),
    }
    params.append(json.dumps(meta))
    updates.append("updated_at = %s")
    params.append(now)
    params.append(profile_id)

    cur.execute(f"UPDATE profiles SET {', '.join(updates)} WHERE id = %s", params)
    count = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return count


def process_profile(profile: Dict) -> Dict:
    """Full pipeline for one bare profile."""
    name = profile['name']
    website = profile['website']
    result = {'name': name, 'status': 'failed'}

    content = scrape_with_crawl4ai(website)
    if not content or len(content) < 100:
        result['status'] = 'scrape_failed'
        return result

    extracted = extract_with_claude(name, website, content)
    if not extracted:
        result['status'] = 'extract_failed'
        return result

    written = write_to_db(profile['id'], name, extracted)
    if written > 0:
        result['status'] = 'saved'
        result['fields'] = [f for f in extracted if extracted.get(f) and str(extracted[f]).strip()]
    else:
        result['status'] = 'no_useful_data'

    return result


def main():
    profiles = get_bare_profiles_with_websites()

    print(f"\n{'='*60}")
    print("SCRAPE BARE PROFILES WITH WEBSITES")
    print(f"{'='*60}")
    print(f"Bare profiles with scrapeable websites: {len(profiles)}")

    if not profiles:
        print("Nothing to scrape.")
        return

    for i, p in enumerate(profiles):
        print(f"  {i+1}. {p['name']} — {p['website']}")

    print(f"\nProcessing...")
    start = time.time()
    saved = 0
    failed = 0

    for p in profiles:
        result = process_profile(p)
        if result['status'] == 'saved':
            saved += 1
            print(f"  OK: {result['name']} — {result.get('fields', [])}")
        else:
            failed += 1
            print(f"  FAIL: {result['name']} — {result['status']}")

    elapsed = time.time() - start
    print(f"\nDone: {saved} saved, {failed} failed in {elapsed:.0f}s")


if __name__ == '__main__':
    main()
