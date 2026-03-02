#!/usr/bin/env python3
"""
Tier A Enrichment Helper — Scrape, Batch, Update, Stats.

Designed for Claude Code agent enrichment (free under Max plan).
Handles all DB/HTTP operations so agents only need to analyze content.

Usage:
    python3 scripts/enrich_tier_a.py scrape                # Fetch all JV page URLs → JSONL
    python3 scripts/enrich_tier_a.py show-batch 0           # Print batch 0 for agent analysis
    python3 scripts/enrich_tier_a.py update                 # Read enriched JSON from stdin → Supabase
    python3 scripts/enrich_tier_a.py stats                  # Show enrichment progress
"""

import asyncio
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import aiohttp
import psycopg2
import psycopg2.extras
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
JSONL_PATH = Path("Filling Database/tier_a_scraped.jsonl")
BATCH_SIZE = 200  # profiles per agent batch
SCRAPE_CONCURRENCY = 20
SCRAPE_TIMEOUT = 15

SELECT_COLUMNS = (
    "id, name, email, company, website, linkedin, phone, bio, "
    "tags, niche, revenue_tier, jv_history, content_platforms, "
    "list_size, seeking, who_you_serve, what_you_do, offering, "
    "enrichment_metadata"
)

# Fields that agents can enrich (written to Supabase)
ENRICHABLE_FIELDS = [
    "seeking", "who_you_serve", "what_you_do", "offering",
    "email", "phone", "revenue_tier", "niche", "tags",
    "signature_programs", "booking_link", "social_proof",
    "content_platforms", "bio",
]

# Source priority for our writes (matches constants.py)
SOURCE_PRIORITY = {
    'client_confirmed': 100, 'client_ingest': 90, 'manual_edit': 80,
    'csv_import': 60, 'exa_research': 50, 'ai_research': 40,
    'apollo': 30, 'unknown': 0,
}


def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


# ---------------------------------------------------------------------------
# SCRAPE mode
# ---------------------------------------------------------------------------

def html_to_text(html: str) -> str:
    """Strip HTML to clean text, truncate to ~4000 chars."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    # Collapse whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)
    return text[:4000]


async def fetch_one(session: aiohttp.ClientSession, url: str) -> tuple:
    """Fetch a single URL. Returns (text, status)."""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; JVMatchmaker/1.0)"}
        async with session.get(
            url, timeout=aiohttp.ClientTimeout(total=SCRAPE_TIMEOUT),
            headers=headers, allow_redirects=True, ssl=False,
        ) as resp:
            if resp.status == 200:
                html = await resp.text()
                return html_to_text(html), "ok"
            return "", f"http_{resp.status}"
    except asyncio.TimeoutError:
        return "", "timeout"
    except Exception as e:
        return "", f"error: {type(e).__name__}"


async def scrape_all():
    """Fetch all Tier A JV page URLs and save to JSONL."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(f"""
        SELECT {SELECT_COLUMNS}
        FROM profiles
        WHERE jv_tier = 'A'
        ORDER BY jv_readiness_score DESC NULLS LAST
    """)
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()

    print(f"Fetched {len(rows)} Tier A profiles from Supabase")

    # Classify URLs
    valid_urls = []
    tba_profiles = []
    for row in rows:
        url = (row.get("website") or "").strip()
        if not url or url.upper() == "TBA" or not url.startswith("http"):
            tba_profiles.append(row)
        else:
            valid_urls.append(row)

    print(f"  {len(valid_urls)} with valid URLs, {len(tba_profiles)} TBA/missing")

    # Scrape in parallel
    sem = asyncio.Semaphore(SCRAPE_CONCURRENCY)
    results = {}

    async def bounded_fetch(row):
        async with sem:
            url = row["website"].strip()
            text, status = await fetch_one(session, url)
            results[str(row["id"])] = (text, status)

    connector = aiohttp.TCPConnector(limit=SCRAPE_CONCURRENCY, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [bounded_fetch(r) for r in valid_urls]
        total = len(tasks)
        done = 0
        for coro in asyncio.as_completed(tasks):
            await coro
            done += 1
            if done % 50 == 0 or done == total:
                print(f"  Scraped {done}/{total}...")

    # Write JSONL
    JSONL_PATH.parent.mkdir(parents=True, exist_ok=True)
    ok = fail = tba = 0
    with open(JSONL_PATH, "w") as f:
        for row in rows:
            pid = str(row["id"])
            url = (row.get("website") or "").strip()

            if not url or url.upper() == "TBA" or not url.startswith("http"):
                entry = _make_entry(row, "", "tba")
                tba += 1
            elif pid in results:
                text, status = results[pid]
                entry = _make_entry(row, text, status)
                if status == "ok" and text:
                    ok += 1
                else:
                    fail += 1
            else:
                entry = _make_entry(row, "", "skipped")
                fail += 1

            f.write(json.dumps(entry, default=str) + "\n")

    print(f"\nScrape complete → {JSONL_PATH}")
    print(f"  OK: {ok}  |  Failed: {fail}  |  TBA: {tba}  |  Total: {len(rows)}")


def _make_entry(row: dict, scraped_text: str, scrape_status: str) -> dict:
    """Build a JSONL entry from a profile row + scraped content."""
    return {
        "id": str(row["id"]),
        "name": row.get("name") or "",
        "website": row.get("website") or "",
        "email": row.get("email") or "",
        "company": row.get("company") or "",
        "phone": row.get("phone") or "",
        "bio": row.get("bio") or "",
        "niche": row.get("niche") or "",
        "tags": row.get("tags") or [],
        "jv_history": row.get("jv_history") or [],
        "revenue_tier": row.get("revenue_tier") or "",
        "seeking": row.get("seeking") or "",
        "who_you_serve": row.get("who_you_serve") or "",
        "what_you_do": row.get("what_you_do") or "",
        "offering": row.get("offering") or "",
        "list_size": row.get("list_size") or 0,
        "enrichment_metadata": row.get("enrichment_metadata") or {},
        "scraped_text": scraped_text,
        "scrape_status": scrape_status,
    }


# ---------------------------------------------------------------------------
# SHOW-BATCH mode
# ---------------------------------------------------------------------------

def show_batch(batch_num: int):
    """Print a batch of profiles for Claude Code agent analysis."""
    if not JSONL_PATH.exists():
        print(f"ERROR: {JSONL_PATH} not found. Run 'scrape' first.", file=sys.stderr)
        sys.exit(1)

    entries = []
    with open(JSONL_PATH) as f:
        for line in f:
            entries.append(json.loads(line))

    start = batch_num * BATCH_SIZE
    end = min(start + BATCH_SIZE, len(entries))
    batch = entries[start:end]

    if not batch:
        print(f"ERROR: Batch {batch_num} is empty (total entries: {len(entries)})", file=sys.stderr)
        sys.exit(1)

    print(f"// Batch {batch_num}: profiles {start}-{end-1} ({len(batch)} profiles)")
    print(json.dumps(batch, indent=2, default=str))


# ---------------------------------------------------------------------------
# UPDATE mode
# ---------------------------------------------------------------------------

def update_profiles():
    """Read enriched profile JSON from stdin, write to Supabase."""
    data = json.load(sys.stdin)
    if isinstance(data, dict):
        data = [data]

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    updated = 0
    skipped = 0
    errors = 0

    for profile in data:
        pid = profile.get("id")
        if not pid:
            skipped += 1
            continue

        try:
            # Fetch current enrichment_metadata for source priority checks
            cur.execute(
                "SELECT enrichment_metadata FROM profiles WHERE id = %s::uuid",
                (pid,)
            )
            row = cur.fetchone()
            if not row:
                print(f"  SKIP: Profile {pid} not found", file=sys.stderr)
                skipped += 1
                continue

            existing_meta = row["enrichment_metadata"] or {}
            if isinstance(existing_meta, str):
                existing_meta = json.loads(existing_meta)

            # Build SET clauses with source priority protection
            sets = []
            params = []
            fields_written = []

            for field in ENRICHABLE_FIELDS:
                new_val = profile.get(field)
                if not new_val:
                    continue
                if field == "tags" and isinstance(new_val, list):
                    new_val_check = new_val
                else:
                    new_val_check = new_val

                if _should_write(field, new_val_check, existing_meta):
                    if field == "tags":
                        sets.append(f"{field} = %s")
                        params.append(new_val)
                    elif field == "content_platforms":
                        sets.append(f"{field} = %s::jsonb")
                        params.append(json.dumps(new_val) if not isinstance(new_val, str) else new_val)
                    else:
                        sets.append(f"{field} = %s")
                        params.append(new_val)
                    fields_written.append(field)

            if not sets:
                skipped += 1
                continue

            # Update enrichment_metadata
            new_meta = dict(existing_meta)
            new_meta["last_enrichment_source"] = "ai_research"
            new_meta["last_enrichment_at"] = datetime.now().isoformat()
            if profile.get("revenue_amount"):
                new_meta["revenue_amount"] = profile["revenue_amount"]

            # Track field provenance
            field_meta = new_meta.get("field_meta", {})
            for f in fields_written:
                field_meta[f] = {
                    "source": "ai_research",
                    "updated_at": datetime.now().isoformat(),
                }
            new_meta["field_meta"] = field_meta

            sets.append("enrichment_metadata = %s::jsonb")
            params.append(json.dumps(new_meta, default=str))

            sets.append("last_enriched_at = NOW()")

            params.append(pid)
            sql = f"UPDATE profiles SET {', '.join(sets)} WHERE id = %s::uuid"

            cur.execute(sql, params)
            conn.commit()
            updated += 1

            if updated % 10 == 0:
                print(f"  Updated {updated} profiles...", file=sys.stderr)

        except Exception as e:
            conn.rollback()
            print(f"  ERROR updating {pid}: {e}", file=sys.stderr)
            errors += 1

    cur.close()
    conn.close()

    print(f"\nUpdate complete: {updated} updated, {skipped} skipped, {errors} errors")


def _should_write(field: str, new_value, existing_meta: dict) -> bool:
    """Source-priority check (simplified from consolidation_task.py)."""
    if not new_value:
        return False
    field_info = existing_meta.get("field_meta", {}).get(field, {})
    existing_source = field_info.get("source", "unknown")
    existing_priority = SOURCE_PRIORITY.get(existing_source, 0)
    new_priority = SOURCE_PRIORITY.get("ai_research", 40)

    if new_priority < existing_priority:
        return False
    return True


# ---------------------------------------------------------------------------
# STATS mode
# ---------------------------------------------------------------------------

def show_stats():
    """Show Tier A enrichment progress."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE last_enriched_at IS NOT NULL) as enriched,
            COUNT(*) FILTER (WHERE email IS NOT NULL AND email != '') as has_email,
            COUNT(*) FILTER (WHERE phone IS NOT NULL AND phone != '') as has_phone,
            COUNT(*) FILTER (WHERE seeking IS NOT NULL AND seeking != '') as has_seeking,
            COUNT(*) FILTER (WHERE who_you_serve IS NOT NULL AND who_you_serve != '') as has_who_you_serve,
            COUNT(*) FILTER (WHERE what_you_do IS NOT NULL AND what_you_do != '') as has_what_you_do,
            COUNT(*) FILTER (WHERE offering IS NOT NULL AND offering != '') as has_offering,
            COUNT(*) FILTER (WHERE revenue_tier IS NOT NULL AND revenue_tier != '') as has_revenue_tier,
            COUNT(*) FILTER (WHERE niche IS NOT NULL AND niche != '') as has_niche,
            COUNT(*) FILTER (WHERE tags IS NOT NULL AND array_length(tags, 1) > 0) as has_tags,
            COUNT(*) FILTER (WHERE signature_programs IS NOT NULL AND signature_programs != '') as has_sig_programs,
            COUNT(*) FILTER (WHERE booking_link IS NOT NULL AND booking_link != '') as has_booking_link,
            ROUND(AVG(jv_readiness_score)::numeric, 1) as avg_score
        FROM profiles
        WHERE jv_tier = 'A'
    """)
    stats = dict(cur.fetchone())

    print("=" * 60)
    print("TIER A ENRICHMENT PROGRESS")
    print("=" * 60)
    total = stats["total"]
    for key, label in [
        ("enriched", "Enriched (last_enriched_at set)"),
        ("has_email", "Has email"),
        ("has_phone", "Has phone"),
        ("has_seeking", "Has seeking"),
        ("has_who_you_serve", "Has who_you_serve"),
        ("has_what_you_do", "Has what_you_do"),
        ("has_offering", "Has offering"),
        ("has_revenue_tier", "Has revenue_tier"),
        ("has_niche", "Has niche"),
        ("has_tags", "Has tags"),
        ("has_sig_programs", "Has signature_programs"),
        ("has_booking_link", "Has booking_link"),
    ]:
        count = stats[key]
        pct = (count / total * 100) if total else 0
        print(f"  {label:<35} {count:>5} / {total}  ({pct:5.1f}%)")

    print(f"\n  Avg JV readiness score: {stats['avg_score']}")

    # Scrape status from JSONL (if exists)
    if JSONL_PATH.exists():
        status_counts = {}
        with open(JSONL_PATH) as f:
            for line in f:
                entry = json.loads(line)
                s = entry.get("scrape_status", "unknown")
                status_counts[s] = status_counts.get(s, 0) + 1
        print(f"\n  Scrape status ({JSONL_PATH.name}):")
        for s, c in sorted(status_counts.items(), key=lambda x: -x[1]):
            print(f"    {s}: {c}")

    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "scrape":
        asyncio.run(scrape_all())
    elif cmd == "show-batch":
        if len(sys.argv) < 3:
            print("Usage: show-batch <batch_number>", file=sys.stderr)
            sys.exit(1)
        show_batch(int(sys.argv[2]))
    elif cmd == "update":
        update_profiles()
    elif cmd == "stats":
        show_stats()
    else:
        print(f"Unknown command: {cmd}", file=sys.stderr)
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
