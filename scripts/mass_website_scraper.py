#!/usr/bin/env python3
"""
Mass async website scraper for Tier C/D profiles.

Fetches homepage + /about for each profile's website, extracts clean text,
writes batch files ready for vllm_batch_enricher.py.

Strategy:
  - Pure aiohttp (no JS rendering) — works for 90%+ of business sites
  - 100 concurrent connections
  - 15s timeout per request
  - Skip if already has scraped content (what_you_do filled)
  - Output: tmp/tier_c_scrape_batches/batch_XXXX.json (5 profiles each)

Usage:
    python3 scripts/mass_website_scraper.py --tier C
    python3 scripts/mass_website_scraper.py --tier C --limit 1000 --concurrency 50
    python3 scripts/mass_website_scraper.py --tier D --out-dir tmp/tier_d_scrape_batches
"""
import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scraper")

BATCH_SIZE = 5
MAX_TEXT_CHARS = 4000      # per URL
MAX_SCRAPED_CHARS = 8000   # total per profile (vLLM truncates to 1500 anyway)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

# Tags to strip entirely (content not useful)
STRIP_TAGS = {"script", "style", "nav", "footer", "header", "noscript", "svg", "iframe", "form"}


def clean_url(url: str) -> str | None:
    """Normalize URL — strip tracking params, ensure https."""
    if not url:
        return None
    url = url.strip()
    # Strip common tracking params
    url = re.sub(r"[?&](utm_[^&]+|ref=[^&]+|source=[^&]+)", "", url)
    url = re.sub(r"[?&]$", "", url)
    try:
        parsed = urlparse(url)
        if not parsed.scheme:
            url = "https://" + url
        return url
    except Exception:
        return None


def extract_text(html: str, max_chars: int = MAX_TEXT_CHARS) -> str:
    """Extract clean readable text from HTML using BeautifulSoup."""
    try:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(STRIP_TAGS):
            tag.decompose()

        # Prefer main content areas
        content = (
            soup.find("main")
            or soup.find(id=re.compile(r"content|main|body", re.I))
            or soup.find(class_=re.compile(r"content|main|body|about|hero", re.I))
            or soup.body
            or soup
        )

        text = content.get_text(separator=" ", strip=True)
        # Collapse whitespace
        text = re.sub(r"\s+", " ", text).strip()
        return text[:max_chars]
    except Exception:
        return ""


def get_secondary_urls(base_url: str) -> list[str]:
    """Return secondary pages to try: /about, /services, /work-with-me, /programs."""
    try:
        parsed = urlparse(base_url)
        root = f"{parsed.scheme}://{parsed.netloc}"
        return [
            root + "/about",
            root + "/about-us",
            root + "/services",
            root + "/work-with-me",
            root + "/programs",
        ]
    except Exception:
        return []


async def fetch_url(
    session: aiohttp.ClientSession,
    url: str,
    timeout: int = 15,
) -> str | None:
    """Fetch a single URL and return clean text, or None on failure."""
    try:
        async with session.get(
            url,
            headers=HEADERS,
            timeout=aiohttp.ClientTimeout(total=timeout),
            allow_redirects=True,
            ssl=False,  # skip SSL verification — many sites have cert issues
        ) as resp:
            if resp.status != 200:
                return None
            ct = resp.headers.get("Content-Type", "")
            if "text/html" not in ct and "text/plain" not in ct:
                return None
            html = await resp.text(errors="replace")
            return extract_text(html)
    except Exception:
        return None


# Track scraped website domains to detect franchise/shared-site boilerplate.
# If N+ profiles share the same domain, use bio instead of re-scraping.
_scraped_domains: dict[str, str] = {}  # domain → first scraped_text
_domain_counts: dict[str, int] = {}     # domain → count
FRANCHISE_THRESHOLD = 3  # fallback to bio after this many profiles share a domain


async def scrape_profile(
    session: aiohttp.ClientSession,
    profile: dict,
    semaphore: asyncio.Semaphore,
) -> dict:
    """Fetch homepage + /about for a profile, return enriched dict."""
    url = clean_url(profile.get("website") or "")
    if not url:
        return {**profile, "scraped_text": profile.get("bio") or ""}

    # Franchise detection: if many profiles share the same website domain,
    # the scraped_text will be identical boilerplate. Use bio instead.
    from urllib.parse import urlparse
    domain = urlparse(url).netloc.lower()
    _domain_counts[domain] = _domain_counts.get(domain, 0) + 1
    if _domain_counts[domain] > FRANCHISE_THRESHOLD and domain in _scraped_domains:
        bio = profile.get("bio") or ""
        if bio:
            log.debug("Franchise site %s (seen %dx), using bio for %s",
                       domain, _domain_counts[domain], profile.get("name", "?"))
            return {**profile, "scraped_text": bio}

    async with semaphore:
        # Fetch homepage + all secondary pages concurrently (8s timeout)
        secondary_urls = get_secondary_urls(url)
        all_tasks = [asyncio.create_task(fetch_url(session, url, timeout=8))]
        for u in secondary_urls:
            all_tasks.append(asyncio.create_task(fetch_url(session, u, timeout=8)))
        all_results = await asyncio.gather(*all_tasks)
        home_text = all_results[0]
        secondary_texts = all_results[1:]

    parts = []
    if home_text:
        parts.append(home_text)

    seen = {home_text} if home_text else set()
    for text in secondary_texts:
        if text and text not in seen and len(text) > 100:
            parts.append(text)
            seen.add(text)
            if sum(len(p) for p in parts) >= MAX_SCRAPED_CHARS:
                break

    # Fallback to existing bio if all fetches failed
    scraped = " | ".join(p for p in parts if p)
    if not scraped:
        scraped = profile.get("bio") or ""

    # Cache first scrape per domain for franchise detection
    if domain and domain not in _scraped_domains and scraped:
        _scraped_domains[domain] = scraped[:200]

    return {
        **profile,
        "scraped_text": scraped[:MAX_SCRAPED_CHARS],
    }


async def run(args: argparse.Namespace) -> None:
    tier = args.tier
    concurrency = args.concurrency
    limit = args.limit
    out_dir = Path(args.out_dir or f"tmp/tier_{tier.lower()}_scrape_batches")
    out_dir.mkdir(parents=True, exist_ok=True)

    offset = getattr(args, 'offset', 0) or 0
    input_file = getattr(args, 'input_file', None)

    if input_file:
        # --- Load profiles from local JSON shard file (avoids DB timeout on large queries) ---
        log.info(f"Loading profiles from {input_file} (offset={offset}, limit={limit})...")
        with open(input_file) as f:
            all_rows = json.load(f)
        rows = all_rows[offset:offset + limit] if limit else all_rows[offset:]
        log.info(f"Sliced {len(rows):,} profiles from {len(all_rows):,} total")
    else:
        # --- Load profiles from DB ---
        import psycopg2
        import psycopg2.extras
        log.info(f"Loading Tier {tier} unenriched profiles from DB...")
        dsn = os.environ.get("DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(dsn)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        params: list = [tier]
        query = """
            SELECT id, name, bio, website, company, email, phone,
                   who_you_serve, seeking, offering, niche
            FROM profiles
            WHERE jv_tier = %s
              AND (what_you_do IS NULL OR what_you_do = '')
              AND website IS NOT NULL AND website != ''
              AND (bio IS NULL OR LENGTH(bio) < 100)
            ORDER BY jv_readiness_score DESC
        """
        if limit:
            query += " LIMIT %s"
            params.append(limit)
        if offset:
            query += " OFFSET %s"
            params.append(offset)
        cur.execute(query, params)
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()

    total = len(rows)
    log.info(f"Loaded {total:,} profiles to scrape")

    if total == 0:
        log.info("Nothing to scrape.")
        return

    # Batch numbering starts at offset // BATCH_SIZE so shards don't collide
    start_batch = offset // BATCH_SIZE

    # Resume: skip already-written batches in this shard's range
    existing = {int(p.stem.split("_")[1]) for p in out_dir.glob("batch_*.json")}
    shard_existing = {b for b in existing if b >= start_batch}
    if shard_existing:
        skip_count = len(shard_existing) * BATCH_SIZE
        rows = rows[skip_count:]
        start_batch = max(shard_existing) + 1
        log.info(f"Resuming — skipping {skip_count} already-scraped profiles, starting at batch {start_batch}")

    log.info(f"Scraping {len(rows):,} profiles with concurrency={concurrency}")

    semaphore = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency * 2, ttl_dns_cache=300)

    scraped_count = 0
    failed_count = 0
    batch_num = start_batch
    batch_start_time = time.time()

    async with aiohttp.ClientSession(connector=connector) as session:
        # Process in chunks of BATCH_SIZE * 10 to manage memory
        chunk_size = BATCH_SIZE * 20  # 100 profiles at a time
        for chunk_start in range(0, len(rows), chunk_size):
            chunk = rows[chunk_start:chunk_start + chunk_size]

            tasks = [
                scrape_profile(session, {
                    "id": str(r["id"]),
                    "name": r["name"] or "",
                    "bio": r["bio"] or "",
                    "website": r["website"] or "",
                    "company": r["company"] or "",
                    "email": r["email"] or "",
                    "phone": r["phone"] or "",
                    "who_you_serve": r["who_you_serve"] or "",
                    "seeking": r["seeking"] or "",
                    "offering": r["offering"] or "",
                    "niche": r["niche"] or "",
                }, semaphore)
                for r in chunk
            ]

            results = await asyncio.gather(*tasks)

            for p in results:
                if p.get("scraped_text"):
                    scraped_count += 1
                else:
                    failed_count += 1

            # Write batches
            for i in range(0, len(results), BATCH_SIZE):
                batch = results[i:i + BATCH_SIZE]
                out_path = out_dir / f"batch_{batch_num:04d}.json"
                with open(out_path, "w") as f:
                    json.dump(batch, f, indent=2, default=str)
                batch_num += 1

            # Progress log
            done = chunk_start + len(chunk)
            elapsed = time.time() - batch_start_time
            rate = done / elapsed if elapsed > 0 else 0
            eta_min = (len(rows) - done) / rate / 60 if rate > 0 else 0
            log.info(
                f"  {done:,}/{len(rows):,} scraped "
                f"({scraped_count:,} ok, {failed_count:,} failed) "
                f"— {rate:.0f} profiles/sec — ETA {eta_min:.0f} min"
            )

    total_batches = batch_num - start_batch
    log.info(f"Done — {total_batches:,} batches written to {out_dir}/")
    log.info(f"  Scraped: {scraped_count:,} | Failed/fallback: {failed_count:,}")
    log.info(f"Next step: run vllm_batch_enricher.py --batches-dir {out_dir} ...")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Mass async website scraper for Tier C/D profiles")
    p.add_argument("--tier", required=True, choices=["C", "D"], help="Tier to scrape")
    p.add_argument("--concurrency", type=int, default=100, help="Concurrent HTTP connections")
    p.add_argument("--limit", type=int, default=None, help="Max profiles to process")
    p.add_argument("--offset", type=int, default=0, help="Skip first N profiles (for sharding)")
    p.add_argument("--out-dir", type=str, default=None, help="Override output directory")
    p.add_argument("--input-file", dest="input_file", type=str, default=None,
                   help="Read profiles from local JSON file instead of DB (avoids statement timeout)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
