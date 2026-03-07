#!/usr/bin/env python3
"""
Tier B Enrichment Helper — Scrape, Batch, Update, Stats.

Adapted from enrich_tier_a.py for ~90K Tier B profiles.
Key differences: higher scrape concurrency, checkpoint/resume,
streaming batch reads (no full-file load), skip already-enriched.

Usage:
    python3 scripts/enrich_tier_b.py scrape                # Fetch ~90K URLs → JSONL (with checkpoint)
    python3 scripts/enrich_tier_b.py show-batch 0           # Print batch 0 (200 profiles)
    python3 scripts/enrich_tier_b.py show-batch-range 0 11  # Print batches 0-11 (for one agent)
    python3 scripts/enrich_tier_b.py update                 # Read enriched JSON from stdin → Supabase
    python3 scripts/enrich_tier_b.py stats                  # Show enrichment progress
    python3 scripts/enrich_tier_b.py info                   # Show total batches + agent assignment
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
JSONL_PATH = Path("Filling Database/tier_b_scraped.jsonl")
CHECKPOINT_PATH = Path("Filling Database/tier_b_scrape_checkpoint.json")
BATCH_SIZE = 200  # profiles per agent sub-batch
SCRAPE_CONCURRENCY = 100  # higher for 90K profiles
SCRAPE_TIMEOUT = 12  # slightly tighter timeout at scale
NUM_AGENTS = 40

SELECT_COLUMNS = (
    "id, name, email, company, website, linkedin, phone, bio, "
    "tags, niche, revenue_tier, jv_history, content_platforms, "
    "list_size, seeking, who_you_serve, what_you_do, offering, "
    "enrichment_metadata"
)

ENRICHABLE_FIELDS = [
    "seeking", "who_you_serve", "what_you_do", "offering",
    "email", "phone", "revenue_tier", "niche", "tags",
    "signature_programs", "booking_link", "social_proof",
    "content_platforms", "bio",
    "service_provided", "audience_type", "current_projects",
]

SOURCE_PRIORITY = {
    'client_confirmed': 100, 'client_ingest': 90, 'manual_edit': 80,
    'csv_import': 60, 'exa_research': 50, 'ai_research': 40,
    'apollo': 30, 'unknown': 0,
}


def get_conn():
    # Add sslmode=require to DATABASE_URL if not present
    db_url = os.environ["DATABASE_URL"]
    if 'sslmode=' not in db_url:
        separator = '&' if '?' in db_url else '?'
        db_url = f"{db_url}{separator}sslmode=require"

    return psycopg2.connect(
        db_url,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
        connect_timeout=10
    )


# ---------------------------------------------------------------------------
# SCRAPE mode — with checkpoint/resume for 90K URLs
# ---------------------------------------------------------------------------

def html_to_text(html: str) -> str:
    """Strip HTML to clean text, truncate to ~4000 chars."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
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
    """Fetch all unenriched Tier B URLs and save to JSONL with checkpoint."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Disable statement timeout for this large query
    cur.execute("SET statement_timeout = 0")

    # Paginate: fetch in chunks of 10K to avoid memory/timeout issues
    rows = []
    page_size = 10000
    offset = 0
    while True:
        cur.execute(f"""
            SELECT {SELECT_COLUMNS}
            FROM profiles
            WHERE jv_tier = 'B'
              AND (last_enriched_at IS NULL
                   OR seeking IS NULL OR seeking = ''
                   OR what_you_do IS NULL OR what_you_do = '')
            ORDER BY id
            LIMIT %s OFFSET %s
        """, (page_size, offset))
        page = [dict(r) for r in cur.fetchall()]
        if not page:
            break
        rows.extend(page)
        offset += page_size
        print(f"  Fetched {len(rows)} profiles so far...")

    cur.close()
    conn.close()

    print(f"Fetched {len(rows)} unenriched Tier B profiles from Supabase")

    # Check for checkpoint (resume interrupted scrape)
    already_scraped = {}
    if CHECKPOINT_PATH.exists():
        ckpt = json.loads(CHECKPOINT_PATH.read_text())
        already_scraped = ckpt.get("scraped_ids", {})
        print(f"Resuming from checkpoint: {len(already_scraped)} already scraped")

    # Classify URLs
    valid_urls = []
    tba_profiles = []
    skip_count = 0
    for row in rows:
        pid = str(row["id"])
        if pid in already_scraped:
            skip_count += 1
            continue
        url = (row.get("website") or "").strip()
        if not url or url.upper() == "TBA" or not url.startswith("http"):
            tba_profiles.append(row)
        else:
            valid_urls.append(row)

    print(f"  {len(valid_urls)} to scrape, {len(tba_profiles)} TBA/missing, {skip_count} already done")

    # Scrape in parallel with progress + periodic checkpoint
    sem = asyncio.Semaphore(SCRAPE_CONCURRENCY)
    results = {}
    done_count = 0
    total_to_scrape = len(valid_urls)

    async def bounded_fetch(row):
        nonlocal done_count
        async with sem:
            url = row["website"].strip()
            text, status = await fetch_one(session, url)
            results[str(row["id"])] = (text, status)
            done_count += 1
            if done_count % 500 == 0 or done_count == total_to_scrape:
                print(f"  Scraped {done_count}/{total_to_scrape}...")

    connector = aiohttp.TCPConnector(limit=SCRAPE_CONCURRENCY, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Process in chunks of 5000 for memory + checkpoint management
        chunk_size = 5000
        for chunk_start in range(0, len(valid_urls), chunk_size):
            chunk = valid_urls[chunk_start:chunk_start + chunk_size]
            chunk_results = {}
            results = chunk_results  # redirect results to chunk dict
            done_count = 0
            total_to_scrape = len(chunk)

            tasks = [bounded_fetch(r) for r in chunk]
            await asyncio.gather(*tasks, return_exceptions=True)

            # Merge chunk results into already_scraped for checkpoint
            for pid, (text, status) in chunk_results.items():
                already_scraped[pid] = {"text": text, "status": status}

            # Save checkpoint after each chunk
            CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
            CHECKPOINT_PATH.write_text(json.dumps({
                "scraped_ids": {pid: v["status"] for pid, v in already_scraped.items()},
                "last_chunk": chunk_start,
                "timestamp": datetime.now().isoformat(),
            }))
            total_done = chunk_start + len(chunk)
            print(f"  Checkpoint saved: {total_done}/{len(valid_urls)} URLs scraped")

    # Write JSONL — all rows (including previously checkpointed)
    JSONL_PATH.parent.mkdir(parents=True, exist_ok=True)
    ok = fail = tba = 0

    with open(JSONL_PATH, "w") as f:
        for row in rows:
            pid = str(row["id"])
            url = (row.get("website") or "").strip()

            if not url or url.upper() == "TBA" or not url.startswith("http"):
                entry = _make_entry(row, "", "tba")
                tba += 1
            elif pid in already_scraped:
                info = already_scraped[pid]
                if isinstance(info, dict):
                    text = info.get("text", "")
                    status = info.get("status", "unknown")
                else:
                    text, status = "", str(info)
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

    # Count total batches
    total_batches = (len(rows) + BATCH_SIZE - 1) // BATCH_SIZE
    batches_per_agent = (total_batches + NUM_AGENTS - 1) // NUM_AGENTS
    print(f"\n  Total batches: {total_batches}  |  Batches per agent ({NUM_AGENTS} agents): {batches_per_agent}")


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
# SHOW-BATCH mode — streaming read (handles 90K+ JSONL)
# ---------------------------------------------------------------------------

def show_batch(batch_num: int):
    """Print a single batch by streaming the JSONL (no full load)."""
    if not JSONL_PATH.exists():
        print(f"ERROR: {JSONL_PATH} not found. Run 'scrape' first.", file=sys.stderr)
        sys.exit(1)

    start = batch_num * BATCH_SIZE
    end = start + BATCH_SIZE
    batch = []

    with open(JSONL_PATH) as f:
        for i, line in enumerate(f):
            if i >= end:
                break
            if i >= start:
                batch.append(json.loads(line))

    if not batch:
        print(f"ERROR: Batch {batch_num} is empty.", file=sys.stderr)
        sys.exit(1)

    print(f"// Batch {batch_num}: profiles {start}-{start + len(batch) - 1} ({len(batch)} profiles)")
    print(json.dumps(batch, indent=2, default=str))


def show_batch_range(start_batch: int, end_batch: int):
    """Print multiple batches (for an agent processing a range)."""
    if not JSONL_PATH.exists():
        print(f"ERROR: {JSONL_PATH} not found. Run 'scrape' first.", file=sys.stderr)
        sys.exit(1)

    start_line = start_batch * BATCH_SIZE
    end_line = (end_batch + 1) * BATCH_SIZE
    batch = []

    with open(JSONL_PATH) as f:
        for i, line in enumerate(f):
            if i >= end_line:
                break
            if i >= start_line:
                batch.append(json.loads(line))

    if not batch:
        print(f"ERROR: Batches {start_batch}-{end_batch} are empty.", file=sys.stderr)
        sys.exit(1)

    print(f"// Batches {start_batch}-{end_batch}: {len(batch)} profiles")
    print(json.dumps(batch, indent=2, default=str))


# ---------------------------------------------------------------------------
# UPDATE mode — same as Tier A
# ---------------------------------------------------------------------------

def _get_direct_conn():
    """Connect via DIRECT_DATABASE_URL (port 5432), bypassing pgbouncer."""
    db_url = os.environ.get("DIRECT_DATABASE_URL") or os.environ["DATABASE_URL"]
    if 'sslmode=' not in db_url:
        separator = '&' if '?' in db_url else '?'
        db_url = f"{db_url}{separator}sslmode=require"
    return psycopg2.connect(
        db_url,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
        connect_timeout=10,
    )


def _build_update_params(profile: dict, existing_meta: dict) -> tuple[list, list, list]:
    """Return (sets, params, fields_written) for a single profile's writable fields."""
    sets, params, fields_written = [], [], []
    now = datetime.now().isoformat()
    for field in ENRICHABLE_FIELDS:
        new_val = profile.get(field)
        if not new_val:
            continue
        if _should_write(field, new_val, existing_meta):
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
    return sets, params, fields_written


def update_profiles():
    """Read enriched profile JSON from stdin, write to Supabase.

    Batch strategy: collect up to 10 profiles, then:
      1. One SELECT ANY(%s::uuid[]) to fetch all metadata
      2. Build per-profile UPDATE params in memory
      3. Single COMMIT per batch of 10
    Uses DIRECT_DATABASE_URL (port 5432) to bypass pgbouncer transaction mode.
    """
    data = json.load(sys.stdin)
    if isinstance(data, dict):
        data = [data]

    conn = _get_direct_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute("SET statement_timeout = '30s'")
    except Exception:
        pass

    updated = 0
    skipped = 0
    errors = 0
    BATCH = 10  # profiles per COMMIT cycle

    def _reconnect():
        nonlocal conn, cur
        try:
            cur.close(); conn.close()
        except Exception:
            pass
        conn = _get_direct_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("SET statement_timeout = '30s'")
        except Exception:
            pass

    # Process in batches of BATCH
    for batch_start in range(0, len(data), BATCH):
        batch = data[batch_start:batch_start + BATCH]
        valid = [(p, str(p["id"])) for p in batch if p.get("id")]
        skipped += len(batch) - len(valid)
        if not valid:
            continue

        pids = [pid for _, pid in valid]

        try:
            # 1 SELECT for the whole batch
            try:
                cur.execute("SELECT 1")
            except Exception:
                print("  Reconnecting to database...", file=sys.stderr)
                _reconnect()

            cur.execute(
                "SELECT id::text, enrichment_metadata FROM profiles WHERE id = ANY(%s::uuid[])",
                (pids,)
            )
            meta_map = {row["id"]: (row["enrichment_metadata"] or {}) for row in cur.fetchall()}

            now = datetime.now().isoformat()

            for profile, pid in valid:
                if pid not in meta_map:
                    skipped += 1
                    continue

                existing_meta = meta_map[pid]
                if isinstance(existing_meta, str):
                    existing_meta = json.loads(existing_meta)

                sets, params, fields_written = _build_update_params(profile, existing_meta)
                if not sets:
                    skipped += 1
                    continue

                new_meta = dict(existing_meta)
                new_meta["last_enrichment_source"] = "ai_research"
                new_meta["last_enrichment_at"] = now
                new_meta["enrichment_context"] = os.environ.get("ENRICHMENT_CONTEXT", "batch")
                if profile.get("revenue_amount"):
                    new_meta["revenue_amount"] = profile["revenue_amount"]
                field_meta = new_meta.get("field_meta", {})
                for f in fields_written:
                    field_meta[f] = {"source": "ai_research", "updated_at": now}
                new_meta["field_meta"] = field_meta

                # Handle confidence → profile_confidence
                if profile.get("confidence") is not None:
                    sets.append("profile_confidence = %s")
                    params.append(float(profile["confidence"]))

                sets.append("enrichment_metadata = %s::jsonb")
                params.append(json.dumps(new_meta, default=str))
                sets.append("last_enriched_at = NOW()")
                params.append(pid)

                sql = f"UPDATE profiles SET {', '.join(sets)} WHERE id = %s::uuid"
                cur.execute(sql, params)
                updated += 1

            # Single COMMIT per batch
            conn.commit()

            if updated % 50 == 0 and updated > 0:
                print(f"  Updated {updated} profiles...", file=sys.stderr)

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            print(f"  DB connection error on batch {batch_start}-{batch_start+BATCH}: {e}", file=sys.stderr)
            try:
                conn.rollback()
            except Exception:
                pass
            errors += len(valid)
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"  ERROR on batch {batch_start}-{batch_start+BATCH}: {e}", file=sys.stderr)
            errors += len(valid)

    try:
        cur.close()
        conn.close()
    except Exception:
        pass
    print(f"\nUpdate complete: {updated} updated, {skipped} skipped, {errors} errors")


def _should_write(field: str, new_value, existing_meta: dict) -> bool:
    """Source-priority check."""
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
    """Show Tier B enrichment progress."""
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
        WHERE jv_tier = 'B'
    """)
    stats = dict(cur.fetchone())

    print("=" * 60)
    print("TIER B ENRICHMENT PROGRESS")
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
        print(f"  {label:<35} {count:>6} / {total}  ({pct:5.1f}%)")

    print(f"\n  Avg JV readiness score: {stats['avg_score']}")

    # Scrape status from JSONL (if exists) — streaming count
    if JSONL_PATH.exists():
        status_counts = {}
        line_count = 0
        with open(JSONL_PATH) as f:
            for line in f:
                entry = json.loads(line)
                s = entry.get("scrape_status", "unknown")
                status_counts[s] = status_counts.get(s, 0) + 1
                line_count += 1
        print(f"\n  JSONL entries: {line_count}")
        total_batches = (line_count + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"  Total batches: {total_batches}")
        print(f"  Scrape status:")
        for s, c in sorted(status_counts.items(), key=lambda x: -x[1]):
            print(f"    {s}: {c}")

    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# INFO mode — show agent assignment plan
# ---------------------------------------------------------------------------

def show_info():
    """Show batch count and agent assignment plan."""
    if not JSONL_PATH.exists():
        print(f"ERROR: {JSONL_PATH} not found. Run 'scrape' first.", file=sys.stderr)
        sys.exit(1)

    line_count = 0
    with open(JSONL_PATH) as f:
        for _ in f:
            line_count += 1

    total_batches = (line_count + BATCH_SIZE - 1) // BATCH_SIZE
    batches_per_agent = (total_batches + NUM_AGENTS - 1) // NUM_AGENTS

    print(f"Total profiles: {line_count}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Total batches: {total_batches}")
    print(f"Agents: {NUM_AGENTS}")
    print(f"Batches per agent: {batches_per_agent}")
    print()
    print("Agent assignments:")
    for agent_num in range(NUM_AGENTS):
        start_batch = agent_num * batches_per_agent
        end_batch = min(start_batch + batches_per_agent - 1, total_batches - 1)
        if start_batch >= total_batches:
            break
        start_profile = start_batch * BATCH_SIZE
        end_profile = min((end_batch + 1) * BATCH_SIZE - 1, line_count - 1)
        profile_count = end_profile - start_profile + 1
        print(f"  Agent {agent_num:2d}: batches {start_batch:3d}-{end_batch:3d}  "
              f"(profiles {start_profile:6d}-{end_profile:6d}, {profile_count:5d} profiles)")


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
    elif cmd == "show-batch-range":
        if len(sys.argv) < 4:
            print("Usage: show-batch-range <start_batch> <end_batch>", file=sys.stderr)
            sys.exit(1)
        show_batch_range(int(sys.argv[2]), int(sys.argv[3]))
    elif cmd == "update":
        update_profiles()
    elif cmd == "stats":
        show_stats()
    elif cmd == "info":
        show_info()
    else:
        print(f"Unknown command: {cmd}", file=sys.stderr)
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
