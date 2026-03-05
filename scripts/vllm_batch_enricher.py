#!/usr/bin/env python3
"""
vLLM Batch Enricher — Async orchestrator for mass enrichment via a vLLM endpoint.

Processes pre-scraped batch files through a vLLM endpoint with guided JSON decoding.
Pairs consecutive batch files (10 profiles per LLM call) for throughput efficiency.

Usage:
    python3 scripts/vllm_batch_enricher.py \\
        --vllm-url http://IP:PORT \\
        --start 0 --end 3829 \\
        --workers 15 \\
        --gpu-cost-per-hour 2.20 \\
        --dry-run
"""

import sys
import os

# Must be before project-relative imports so Python can find the scripts package
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import psycopg2
import psycopg2.extras
import psycopg2.pool
from dotenv import load_dotenv

from scripts.enrich_tier_b import _should_write, ENRICHABLE_FIELDS

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("vllm_enricher")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BATCHES_DIR = PROJECT_ROOT / "tmp" / "enrichment_batches"
RESULTS_DIR = PROJECT_ROOT / "tmp" / "enrichment_results"
PROGRESS_FILE = PROJECT_ROOT / "tmp" / "enrichment_progress.json"

# ---------------------------------------------------------------------------
# guided_json schema (17 fields)
# ---------------------------------------------------------------------------
GUIDED_JSON_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "required": [
            "id", "what_you_do", "who_you_serve", "seeking", "offering",
            "niche", "tags", "signature_programs", "phone", "company",
            "social_proof", "booking_link", "revenue_tier", "service_provided",
            "audience_type", "current_projects", "confidence",
        ],
        "properties": {
            "id":                 {"type": "string"},
            "tags":               {"type": "array", "items": {"type": "string"}, "maxItems": 10},
            "revenue_tier":       {"type": "string", "enum": ["solo", "small_biz", "mid_market", "high", ""]},
            "confidence":         {"type": "number", "minimum": 0.0, "maximum": 1.0},
            "what_you_do":        {"type": "string"},
            "who_you_serve":      {"type": "string"},
            "seeking":            {"type": "string"},
            "offering":           {"type": "string"},
            "niche":              {"type": "string"},
            "signature_programs": {"type": "string"},
            "phone":              {"type": "string"},
            "company":            {"type": "string"},
            "social_proof":       {"type": "string"},
            "booking_link":       {"type": "string"},
            "service_provided":   {"type": "string"},
            "audience_type":      {"type": "string"},
            "current_projects":   {"type": "string"},
        },
    },
}

# ---------------------------------------------------------------------------
# Extraction prompt
# ---------------------------------------------------------------------------
EXTRACTION_PROMPT_TEMPLATE = """You are a business research assistant. Extract structured data from these business profiles.

For each profile below, extract these fields:
- id: copy exactly from input
- what_you_do: 1-2 sentences describing their business/service
- who_you_serve: their target audience/clients
- seeking: what JV partnerships or collaborations they want ("" if not explicit)
- offering: what they bring to a partnership
- niche: 2-4 words describing their market niche
- tags: JSON array of 3-8 keyword tags like ["coaching","b2b","saas"]
- signature_programs: named courses, books, frameworks, or flagship services
- phone: phone number if found
- company: company/business name
- social_proof: testimonials, case studies, awards, or proof of results (1-2 sentences)
- booking_link: Calendly, Acuity, or similar scheduling link
- revenue_tier: one of "solo" (1 person <$500k), "small_biz" ($500k-$5M), "mid_market" ($5M-$50M), "high" (>$50M), or ""
- service_provided: primary service category (e.g. "coaching", "software", "consulting")
- audience_type: "B2B", "B2C", "consumers", "enterprise", "coaches", etc.
- current_projects: 1 sentence on active projects/launches, or ""
- confidence: float 0.0-1.0, your confidence in the extraction quality

Return a JSON array with one object per profile. Use "" for missing/unclear fields.

PROFILES:
{profiles_json}
"""

# ---------------------------------------------------------------------------
# DB connection pool
# ---------------------------------------------------------------------------

def _build_db_url() -> str:
    """Prefer DIRECT_DATABASE_URL (port 5432), fall back to DATABASE_URL."""
    db_url = os.environ.get("DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Neither DIRECT_DATABASE_URL nor DATABASE_URL is set.")
    if "sslmode=" not in db_url:
        sep = "&" if "?" in db_url else "?"
        db_url = f"{db_url}{sep}sslmode=require"
    return db_url


def make_pool() -> psycopg2.pool.ThreadedConnectionPool:
    db_url = _build_db_url()
    return psycopg2.pool.ThreadedConnectionPool(
        5, 15, db_url,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
        connect_timeout=10,
    )


# ---------------------------------------------------------------------------
# DB write (sync — called via asyncio.to_thread)
# ---------------------------------------------------------------------------

def db_write_profiles(pool: psycopg2.pool.ThreadedConnectionPool, profiles: list[dict]) -> tuple[int, int]:
    """
    Write enriched profiles to DB using source-priority logic.

    Strategy:
      1. One SELECT ANY(%s::uuid[]) to fetch existing metadata for all profiles.
      2. Build per-profile UPDATE params in memory using _should_write().
      3. Individual UPDATE per profile (SET columns vary per profile).
      4. Single conn.commit() for the whole batch.

    Returns (updated_count, skipped_count).
    """
    valid = [(p, str(p["id"])) for p in profiles if p.get("id")]
    if not valid:
        return 0, len(profiles)

    pids = [pid for _, pid in valid]
    now = datetime.now(timezone.utc).isoformat()

    conn = pool.getconn()
    try:
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute("SET statement_timeout = '30s'")
        except Exception:
            pass

        # 1. Fetch existing metadata for all profiles in one query
        cur.execute(
            "SELECT id::text, enrichment_metadata FROM profiles WHERE id = ANY(%s::uuid[])",
            (pids,),
        )
        meta_map = {row["id"]: (row["enrichment_metadata"] or {}) for row in cur.fetchall()}

        updated = 0
        skipped = 0

        for profile, pid in valid:
            if pid not in meta_map:
                skipped += 1
                continue

            existing_meta = meta_map[pid]
            if isinstance(existing_meta, str):
                try:
                    existing_meta = json.loads(existing_meta)
                except Exception:
                    existing_meta = {}

            # 2. Build SET clauses using source-priority check
            sets: list[str] = []
            params: list = []
            fields_written: list[str] = []

            for field in ENRICHABLE_FIELDS:
                new_val = profile.get(field)
                if not new_val:
                    continue
                if _should_write(field, new_val, existing_meta):
                    if field == "tags":
                        sets.append(f"{field} = %s")
                        params.append(new_val if isinstance(new_val, list) else [new_val])
                    elif field == "content_platforms":
                        sets.append(f"{field} = %s::jsonb")
                        params.append(
                            json.dumps(new_val) if not isinstance(new_val, str) else new_val
                        )
                    else:
                        sets.append(f"{field} = %s")
                        params.append(new_val)
                    fields_written.append(field)

            if not sets and profile.get("confidence") is None:
                skipped += 1
                continue

            # Update enrichment_metadata
            new_meta = dict(existing_meta)
            new_meta["last_enrichment_source"] = "ai_research"
            new_meta["last_enrichment_at"] = now
            field_meta = new_meta.get("field_meta", {})
            for f in fields_written:
                field_meta[f] = {"source": "ai_research", "updated_at": now}
            new_meta["field_meta"] = field_meta

            # Handle confidence → profile_confidence column
            if profile.get("confidence") is not None:
                sets.append("profile_confidence = %s")
                params.append(float(profile["confidence"]))

            sets.append("enrichment_metadata = %s::jsonb")
            params.append(json.dumps(new_meta, default=str))
            sets.append("last_enriched_at = NOW()")
            params.append(pid)

            # 3. Individual UPDATE (SET columns vary per profile)
            sql = f"UPDATE profiles SET {', '.join(sets)} WHERE id = %s::uuid"
            cur.execute(sql, params)
            updated += 1

        # 4. Single commit for the whole batch
        conn.commit()
        cur.close()
        return updated, skipped

    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        pool.putconn(conn)


# ---------------------------------------------------------------------------
# File I/O helpers
# ---------------------------------------------------------------------------

def load_batch_file(batch_id: int) -> list[dict] | None:
    path = BATCHES_DIR / f"batch_{batch_id:04d}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception as e:
        log.warning(f"Failed to read {path}: {e}")
        return None


def result_file_exists(batch_id: int) -> bool:
    return (RESULTS_DIR / f"batch_{batch_id:04d}.json").exists()


def write_result_file(batch_id: int, profiles: list[dict]) -> None:
    path = RESULTS_DIR / f"batch_{batch_id:04d}.json"
    path.write_text(json.dumps(profiles, indent=2, default=str))


# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------

class ProgressTracker:
    def __init__(
        self,
        total_batch_pairs: int,
        gpu_cost_per_hour: float,
        failed_batches: list[int] | None = None,
    ):
        self.total_batch_pairs = total_batch_pairs
        self.gpu_cost_per_hour = gpu_cost_per_hour
        self.done_pairs = 0
        self.profiles_done = 0
        self.failed_batches: list[int] = failed_batches or []
        self.started_at = datetime.now(timezone.utc)
        self._lock = asyncio.Lock()

    async def record_done(self, profiles_count: int) -> None:
        async with self._lock:
            self.done_pairs += 1
            self.profiles_done += profiles_count
            if self.done_pairs % 10 == 0:
                await asyncio.to_thread(self._flush)

    async def record_failure(self, batch_ids: list[int]) -> None:
        async with self._lock:
            self.failed_batches.extend(batch_ids)

    def _flush(self) -> None:
        elapsed_hours = (
            datetime.now(timezone.utc) - self.started_at
        ).total_seconds() / 3600
        cost_usd = elapsed_hours * self.gpu_cost_per_hour
        remaining_pairs = self.total_batch_pairs - self.done_pairs
        if self.done_pairs > 0:
            secs_per_pair = (
                datetime.now(timezone.utc) - self.started_at
            ).total_seconds() / self.done_pairs
            eta_minutes = (remaining_pairs * secs_per_pair) / 60
        else:
            eta_minutes = 0.0

        cost_per_profile = (cost_usd / self.profiles_done) if self.profiles_done else 0.0

        data = {
            "started_at": self.started_at.isoformat(),
            "batches_done": self.done_pairs * 2,   # pairs → individual batch files
            "batches_total": self.total_batch_pairs * 2,
            "profiles_done": self.profiles_done,
            "gpu_hours_elapsed": round(elapsed_hours, 4),
            "cost_usd": round(cost_usd, 4),
            "cost_per_profile": round(cost_per_profile, 6),
            "eta_minutes": round(eta_minutes, 1),
            "failed_batches": self.failed_batches,
        }
        PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
        PROGRESS_FILE.write_text(json.dumps(data, indent=2))

    def final_flush(self) -> None:
        self._flush()
        log.info(
            f"Progress saved → {PROGRESS_FILE}  "
            f"({self.profiles_done} profiles, {len(self.failed_batches)} failures)"
        )


# ---------------------------------------------------------------------------
# vLLM call with retry
# ---------------------------------------------------------------------------

async def call_vllm(
    session: aiohttp.ClientSession,
    vllm_url: str,
    model_name: str,
    profiles: list[dict],
    max_retries: int = 3,
) -> list[dict] | None:
    """POST to vLLM /v1/chat/completions with guided_json decoding. Returns parsed array or None."""
    # Truncate scraped_text per profile to stay within context window.
    # 10 profiles × 1500 chars ≈ 3750 tokens input, leaving 4000+ for output.
    truncated = []
    for p in profiles:
        pc = dict(p)
        if pc.get("scraped_text"):
            pc["scraped_text"] = pc["scraped_text"][:1500]
        truncated.append(pc)
    profiles_json = json.dumps(truncated, indent=2, default=str)
    prompt = EXTRACTION_PROMPT_TEMPLATE.format(profiles_json=profiles_json)

    payload = {
        "model": model_name,
        "messages": [
            # /no_think disables Qwen3 chain-of-thought — saves 500-2000 tokens/call,
            # no quality loss for structured extraction tasks
            {"role": "system", "content": "/no_think"},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
        "max_tokens": 4096,
        "guided_json": GUIDED_JSON_SCHEMA,
    }

    url = f"{vllm_url.rstrip('/')}/v1/chat/completions"
    backoff = 1.0

    for attempt in range(1, max_retries + 1):
        try:
            async with session.post(
                url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=120),
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    log.warning(f"vLLM HTTP {resp.status} (attempt {attempt}): {body[:200]}")
                    if attempt < max_retries:
                        await asyncio.sleep(backoff)
                        backoff *= 2
                    continue

                data = await resp.json()
                content = data["choices"][0]["message"]["content"]
                # Strip Qwen3 <think>...</think> block (thinking mode on by default)
                import re as _re
                content = _re.sub(r'<think>.*?</think>', '', content, flags=_re.DOTALL).strip()
                # Extract JSON array from markdown code blocks if present
                if "```" in content:
                    m = _re.search(r'```(?:json)?\s*(\[.*?\])\s*```', content, _re.DOTALL)
                    if m:
                        content = m.group(1)
                parsed = json.loads(content)
                if not isinstance(parsed, list):
                    log.warning(f"vLLM returned non-list JSON (attempt {attempt})")
                    if attempt < max_retries:
                        await asyncio.sleep(backoff)
                        backoff *= 2
                    continue
                return parsed

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning(f"vLLM network error (attempt {attempt}): {e}")
            if attempt < max_retries:
                await asyncio.sleep(backoff)
                backoff *= 2
        except json.JSONDecodeError as e:
            log.warning(f"vLLM JSON parse error (attempt {attempt}): {e}")
            if attempt < max_retries:
                await asyncio.sleep(backoff)
                backoff *= 2

    return None


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

async def worker(
    worker_id: int,
    queue: asyncio.Queue,
    semaphore: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    pool: psycopg2.pool.ThreadedConnectionPool,
    progress: ProgressTracker,
    vllm_url: str,
    model_name: str,
    dry_run: bool,
) -> None:
    while True:
        try:
            pair: tuple[int, int] = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        batch_id_a, batch_id_b = pair

        async with semaphore:
            try:
                profiles_a = load_batch_file(batch_id_a)
                profiles_b = load_batch_file(batch_id_b)

                missing = []
                if profiles_a is None:
                    log.warning(f"[W{worker_id}] batch_{batch_id_a:04d}.json not found, skipping")
                    missing.append(batch_id_a)
                if profiles_b is None:
                    log.warning(f"[W{worker_id}] batch_{batch_id_b:04d}.json not found, skipping")
                    missing.append(batch_id_b)

                if missing:
                    await progress.record_failure(missing)
                    continue

                combined_profiles = (profiles_a or []) + (profiles_b or [])
                input_ids = {str(p["id"]) for p in combined_profiles if p.get("id")}

                if dry_run:
                    log.info(
                        f"[W{worker_id}] DRY RUN: would process batches "
                        f"{batch_id_a:04d}+{batch_id_b:04d} "
                        f"({len(combined_profiles)} profiles)"
                    )
                    await progress.record_done(len(combined_profiles))
                    continue

                # --- Call vLLM ---
                result_array = await call_vllm(
                    session, vllm_url, model_name, combined_profiles
                )

                if result_array is None:
                    log.error(
                        f"[W{worker_id}] vLLM failed for batches "
                        f"{batch_id_a:04d}+{batch_id_b:04d} after retries"
                    )
                    await progress.record_failure([batch_id_a, batch_id_b])
                    continue

                if len(result_array) != len(combined_profiles):
                    log.warning(
                        f"[W{worker_id}] Expected {len(combined_profiles)} results, "
                        f"got {len(result_array)} for batches {batch_id_a:04d}+{batch_id_b:04d}"
                    )

                # Validate IDs — drop any that don't match input
                valid_results = []
                for r in result_array:
                    rid = str(r.get("id", ""))
                    if rid in input_ids:
                        valid_results.append(r)
                    else:
                        log.warning(
                            f"[W{worker_id}] Dropping result with unknown id={rid!r}"
                        )

                # Split results back into their source batches by id membership
                ids_a = {str(p["id"]) for p in profiles_a if p.get("id")}
                ids_b = {str(p["id"]) for p in profiles_b if p.get("id")}
                results_a = [r for r in valid_results if str(r.get("id", "")) in ids_a]
                results_b = [r for r in valid_results if str(r.get("id", "")) in ids_b]

                # --- Parallel: write files + DB ---
                async def write_files() -> None:
                    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
                    if results_a:
                        write_result_file(batch_id_a, results_a)
                    if results_b:
                        write_result_file(batch_id_b, results_b)

                async def write_db() -> tuple[int, int]:
                    return await asyncio.to_thread(
                        db_write_profiles, pool, valid_results
                    )

                db_result = await asyncio.gather(
                    write_files(),
                    write_db(),
                    return_exceptions=True,
                )

                file_exc = db_result[0]
                db_exc = db_result[1]

                if isinstance(file_exc, Exception):
                    log.error(f"[W{worker_id}] File write error: {file_exc}")
                if isinstance(db_exc, Exception):
                    log.error(f"[W{worker_id}] DB write error: {db_exc}")
                    updated, skipped = 0, len(valid_results)
                else:
                    updated, skipped = db_exc

                log.info(
                    f"[W{worker_id}] batches {batch_id_a:04d}+{batch_id_b:04d}: "
                    f"{len(valid_results)} valid, {updated} DB updated, {skipped} skipped"
                )
                await progress.record_done(len(combined_profiles))

            except Exception as e:
                log.exception(
                    f"[W{worker_id}] Unexpected error on batches "
                    f"{batch_id_a:04d}+{batch_id_b:04d}: {e}"
                )
                await progress.record_failure([batch_id_a, batch_id_b])
            finally:
                queue.task_done()


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

async def run(args: argparse.Namespace) -> None:
    vllm_url: str = args.vllm_url
    start: int = args.start
    end: int = args.end
    num_workers: int = args.workers
    gpu_cost: float = args.gpu_cost_per_hour
    dry_run: bool = args.dry_run
    model_name: str = args.model

    # Allow overriding batch/result dirs via CLI args
    global BATCHES_DIR, RESULTS_DIR
    if getattr(args, 'batches_dir', None):
        BATCHES_DIR = Path(args.batches_dir)
    if getattr(args, 'results_dir', None):
        RESULTS_DIR = Path(args.results_dir)

    log.info(f"vLLM Batch Enricher starting — range [{start}, {end}], workers={num_workers}, dry_run={dry_run}")
    log.info(f"Batches dir: {BATCHES_DIR}")
    log.info(f"Results dir: {RESULTS_DIR}")

    # 1. Scan for already-completed result files
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    existing_result_ids = {
        int(p.stem.split("_")[1])
        for p in RESULTS_DIR.glob("batch_*.json")
    }
    log.info(f"Found {len(existing_result_ids)} existing result files")

    # 2. Build gap list and pair consecutive IDs
    all_ids = list(range(start, end + 1))
    missing_ids = [bid for bid in all_ids if bid not in existing_result_ids]
    log.info(f"Missing batch files: {len(missing_ids)} of {len(all_ids)}")

    # Pair consecutive missing IDs; if odd count, last one is paired with itself
    pairs: list[tuple[int, int]] = []
    i = 0
    while i < len(missing_ids):
        a = missing_ids[i]
        b = missing_ids[i + 1] if i + 1 < len(missing_ids) else a
        pairs.append((a, b))
        i += 2

    if not pairs:
        log.info("No missing batches — nothing to do.")
        return

    log.info(f"Will process {len(pairs)} batch pairs ({len(missing_ids)} individual batches)")

    # Load existing progress for failed_batches continuity
    failed_batches: list[int] = []
    if PROGRESS_FILE.exists():
        try:
            prev = json.loads(PROGRESS_FILE.read_text())
            failed_batches = prev.get("failed_batches", [])
            log.info(f"Loaded {len(failed_batches)} previously failed batches from progress file")
        except Exception:
            pass

    progress = ProgressTracker(len(pairs), gpu_cost, failed_batches)

    # Build queue
    queue: asyncio.Queue[tuple[int, int]] = asyncio.Queue()
    for pair in pairs:
        await queue.put(pair)

    semaphore = asyncio.Semaphore(num_workers)

    # 3. Create DB pool (skip in dry-run to avoid requiring DB creds)
    pool = None
    if not dry_run:
        try:
            pool = make_pool()
            log.info("DB connection pool created")
        except Exception as e:
            log.error(f"Failed to create DB pool: {e}")
            log.error("Set DIRECT_DATABASE_URL or DATABASE_URL, or use --dry-run")
            return

    # 4. Launch workers
    connector = aiohttp.TCPConnector(limit=num_workers * 2)
    async with aiohttp.ClientSession(connector=connector) as session:
        workers = [
            asyncio.create_task(
                worker(
                    worker_id=wid,
                    queue=queue,
                    semaphore=semaphore,
                    session=session,
                    pool=pool,
                    progress=progress,
                    vllm_url=vllm_url,
                    model_name=model_name,
                    dry_run=dry_run,
                )
            )
            for wid in range(num_workers)
        ]

        await asyncio.gather(*workers)

    if pool is not None:
        pool.closeall()

    progress.final_flush()

    elapsed = (datetime.now(timezone.utc) - progress.started_at).total_seconds()
    log.info(
        f"Done — {progress.profiles_done} profiles in {elapsed:.0f}s "
        f"({len(progress.failed_batches)} failed batches)"
    )
    if progress.failed_batches:
        log.warning(f"Failed batch IDs: {progress.failed_batches}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Async vLLM batch enricher with guided JSON decoding.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--vllm-url",
        required=True,
        help="Base URL of the vLLM server, e.g. http://192.168.1.10:8000",
    )
    parser.add_argument(
        "--model",
        default="default",
        help="Model name to pass in the vLLM request body.",
    )
    parser.add_argument("--start", type=int, default=0, help="First batch ID (inclusive)")
    parser.add_argument("--end", type=int, default=3829, help="Last batch ID (inclusive)")
    parser.add_argument("--workers", type=int, default=15, help="Concurrent async workers")
    parser.add_argument("--batches-dir", type=str, default=None, help="Override batch input directory")
    parser.add_argument("--results-dir", type=str, default=None, help="Override results output directory")
    parser.add_argument(
        "--gpu-cost-per-hour",
        type=float,
        default=2.20,
        help="GPU rental cost per hour (USD) for cost tracking",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan gaps and log what would happen, but skip DB writes and file writes",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
