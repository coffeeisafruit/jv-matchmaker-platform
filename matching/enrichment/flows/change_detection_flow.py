"""
Main change-detection pipeline -- Prefect @flow.

Orchestrates the full Profile Freshness Monitoring pipeline:
  1. Select profiles by freshness tier (A/B/C/D) that are due for checking
  2. Layer 1: content hash comparison (FREE -- hashlib + requests + BS4)
  3. Filter to only changed profiles
  4. Layer 2: semantic triage via Claude (only on changed profiles, ~$0.008 each)
  5. Update enrichment_metadata with new hashes and change history
  6. Queue material changes for re-enrichment

Tier assignment:
  A - Active partners       -> every 2 weeks
  B - High-value prospects  -> monthly
  C - Standard with website -> monthly
  D - No website / >6mo stale -> quarterly

Usage (CLI):
    python -m matching.enrichment.flows.change_detection_flow --tiers A,B,C --limit 200

Usage (Prefect):
    from matching.enrichment.flows.change_detection_flow import change_detection_flow
    change_detection_flow(tiers="A,B,C", limit=200)
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from prefect import flow, get_run_logger

from matching.enrichment.flows.content_hash_check import (
    HashCheckResult,
    check_hashes_batch,
)
from matching.enrichment.flows.semantic_triage import (
    TriageResult,
    triage_batch,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tier -> check interval mapping
# ---------------------------------------------------------------------------

_TIER_INTERVALS: dict[str, timedelta] = {
    "A": timedelta(weeks=2),
    "B": timedelta(days=30),
    "C": timedelta(days=30),
    "D": timedelta(days=90),
}

# ---------------------------------------------------------------------------
# Database helper
# ---------------------------------------------------------------------------

def _get_db_connection() -> psycopg2.extensions.connection:
    """Create a new psycopg2 connection from ``DATABASE_URL``."""
    dsn = os.environ["DATABASE_URL"]
    return psycopg2.connect(dsn)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class ChangeDetectionResult:
    """Summary of a change detection flow run."""

    profiles_checked: int = 0
    pages_fetched: int = 0
    changes_detected: int = 0
    material_changes: int = 0
    cosmetic_changes: int = 0
    errors: int = 0
    queued_for_enrichment: list[str] = field(default_factory=list)
    runtime_seconds: float = 0.0


# ---------------------------------------------------------------------------
# Profile selection helper
# ---------------------------------------------------------------------------

_SELECT_SQL = """
    SELECT
        id, name, website, enrichment_metadata
    FROM profiles
    WHERE website IS NOT NULL
      AND website != ''
      AND (
          enrichment_metadata IS NULL
          OR enrichment_metadata->>'check_tier' = ANY(%(tiers)s)
      )
      AND (
          enrichment_metadata IS NULL
          OR enrichment_metadata->>'next_check_due' IS NULL
          OR (enrichment_metadata->>'next_check_due')::timestamptz <= NOW()
      )
    ORDER BY
        COALESCE(
            (enrichment_metadata->>'last_hash_check_at')::timestamptz,
            '1970-01-01'::timestamptz
        ) ASC
"""


def _select_profiles_for_check(
    tiers: list[str],
    limit: int = 0,
) -> list[dict]:
    """Query DB for profiles that are due for hash checking.

    Selects profiles whose ``check_tier`` is in *tiers* and whose
    ``next_check_due`` is in the past (or NULL / missing).

    Profiles with no ``enrichment_metadata`` at all are included -- they
    have never been checked and need an initial baseline.

    Parameters
    ----------
    tiers:
        List of tier letters, e.g. ["A", "B", "C"].
    limit:
        Max profiles to return; 0 means no limit.

    Returns
    -------
    list[dict]
    """
    conn = _get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        sql = _SELECT_SQL
        if limit > 0:
            sql += " LIMIT %(limit)s"

        cursor.execute(sql, {"tiers": tiers, "limit": limit})
        rows = cursor.fetchall()

        # Convert rows to plain dicts (RealDictRow -> dict)
        return [dict(row) for row in rows]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Metadata update helper
# ---------------------------------------------------------------------------

def _update_hash_metadata(
    profile_id: str,
    hash_result: HashCheckResult,
    triage_result: Optional[TriageResult] = None,
    dry_run: bool = False,
) -> None:
    """Update enrichment_metadata JSONB with new hashes and change history.

    Writes:
      - content_hashes: merged new hashes (only update changed/new pages)
      - last_hash_check_at: now
      - last_hash_change_at: now (only if change detected)
      - change_history[]: appended entry if change detected
      - next_check_due: computed from check_tier interval

    Parameters
    ----------
    profile_id:
        UUID of the profile to update.
    hash_result:
        HashCheckResult from Layer 1.
    triage_result:
        Optional TriageResult from Layer 2 (None if triage was skipped).
    dry_run:
        If True, log the update but do not write to DB.
    """
    if dry_run:
        logger.info(
            "DRY RUN: would update metadata for %s (%s)",
            hash_result.name, profile_id,
        )
        return

    now_iso = datetime.utcnow().isoformat() + "Z"
    conn = _get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Fetch current enrichment_metadata
        cursor.execute(
            "SELECT enrichment_metadata FROM profiles WHERE id = %s",
            (profile_id,),
        )
        row = cursor.fetchone()
        em: dict = (row.get("enrichment_metadata") if row else None) or {}

        # Merge new hashes (keep existing for pages we didn't check this run)
        existing_hashes = em.get("content_hashes", {})
        merged_hashes = {**existing_hashes, **hash_result.new_hashes}
        em["content_hashes"] = merged_hashes

        # Timestamps
        em["last_hash_check_at"] = now_iso
        if hash_result.changed:
            em["last_hash_change_at"] = now_iso

        # Append to change_history if changed
        if hash_result.changed:
            history_entry: dict[str, Any] = {
                "detected_at": now_iso,
                "pages_changed": hash_result.pages_changed,
            }
            if triage_result:
                history_entry["triage_result"] = triage_result.classification
                history_entry["change_summary"] = triage_result.change_summary
                history_entry["re_enriched"] = (
                    triage_result.classification == "material"
                )
            else:
                history_entry["triage_result"] = "pending"
                history_entry["change_summary"] = ""
                history_entry["re_enriched"] = False

            change_history = em.get("change_history", [])
            change_history.append(history_entry)
            # Keep last 20 entries to avoid JSONB bloat
            em["change_history"] = change_history[-20:]

        # Compute next_check_due based on tier
        check_tier = em.get("check_tier", "C")
        interval = _TIER_INTERVALS.get(check_tier, timedelta(days=30))
        next_due = datetime.utcnow() + interval
        em["next_check_due"] = next_due.isoformat() + "Z"

        # Write back
        cursor.execute(
            """
            UPDATE profiles
            SET enrichment_metadata = %s
            WHERE id = %s
            """,
            (json.dumps(em), profile_id),
        )
        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(
    name="change-detection",
    description="Scan profiles for content changes, triage, queue re-enrichment",
    retries=0,
    timeout_seconds=3600,
)
def change_detection_flow(
    tiers: str = "A,B,C",
    limit: int = 0,
    skip_triage: bool = False,
    dry_run: bool = False,
) -> ChangeDetectionResult:
    """Full change detection pipeline.

    Steps:
      1. Select profiles by freshness tier that are due for checking.
      2. ``check_hashes_batch()`` -- Layer 1: content hash comparison (FREE).
      3. Filter to only changed profiles.
      4. ``triage_batch()`` -- Layer 2: semantic classification (changed only).
      5. Update enrichment_metadata with new hashes and change history.
      6. Queue material changes for re-enrichment.

    Parameters
    ----------
    tiers:
        Comma-separated tier letters, e.g. ``"A,B,C"``.
    limit:
        Maximum profiles to check; 0 = no limit.
    skip_triage:
        If True, skip Layer 2 (semantic triage) and treat all changes as
        material.  Useful for debugging or when API budget is exhausted.
    dry_run:
        If True, do not write metadata updates or queue re-enrichment.

    Returns
    -------
    ChangeDetectionResult
    """
    log = get_run_logger()
    start_time = time.time()
    result = ChangeDetectionResult()

    tier_list = [t.strip().upper() for t in tiers.split(",") if t.strip()]
    log.info(
        "Change detection starting: tiers=%s, limit=%s, skip_triage=%s, dry_run=%s",
        tier_list, limit or "unlimited", skip_triage, dry_run,
    )

    # ------------------------------------------------------------------
    # Step 1: Select profiles due for checking
    # ------------------------------------------------------------------
    profiles = _select_profiles_for_check(tier_list, limit)
    result.profiles_checked = len(profiles)
    log.info("Selected %d profiles for hash checking", len(profiles))

    if not profiles:
        log.info("No profiles due for checking. Exiting.")
        result.runtime_seconds = time.time() - start_time
        return result

    # ------------------------------------------------------------------
    # Step 2: Layer 1 -- content hash comparison (FREE)
    # ------------------------------------------------------------------
    hash_results: list[HashCheckResult] = check_hashes_batch(profiles)
    result.pages_fetched = sum(hr.pages_checked for hr in hash_results)
    result.errors = sum(1 for hr in hash_results if hr.error)

    # ------------------------------------------------------------------
    # Step 3: Filter to changed profiles
    # ------------------------------------------------------------------
    changed_hashes = [hr for hr in hash_results if hr.changed]
    result.changes_detected = len(changed_hashes)
    log.info(
        "Layer 1 complete: %d/%d profiles changed (%d pages fetched, %d errors)",
        result.changes_detected, result.profiles_checked,
        result.pages_fetched, result.errors,
    )

    # Build profile lookup for changed profiles
    profiles_by_id = {str(p["id"]): p for p in profiles}
    changed_profiles = [
        profiles_by_id[hr.profile_id]
        for hr in changed_hashes
        if hr.profile_id in profiles_by_id
    ]

    # ------------------------------------------------------------------
    # Step 4: Layer 2 -- semantic triage (only changed profiles)
    # ------------------------------------------------------------------
    triage_results: list[TriageResult] = []
    triage_by_id: dict[str, TriageResult] = {}

    if changed_profiles and not skip_triage:
        log.info(
            "Running semantic triage on %d changed profiles (~$%.3f estimated)",
            len(changed_profiles), len(changed_profiles) * 0.008,
        )
        triage_results = triage_batch(changed_profiles, changed_hashes)
        triage_by_id = {tr.profile_id: tr for tr in triage_results}

        result.material_changes = sum(
            1 for tr in triage_results if tr.classification == "material"
        )
        result.cosmetic_changes = sum(
            1 for tr in triage_results if tr.classification == "cosmetic"
        )

        log.info(
            "Layer 2 complete: %d material, %d cosmetic",
            result.material_changes, result.cosmetic_changes,
        )
    elif changed_profiles and skip_triage:
        # Treat all changes as material when triage is skipped
        result.material_changes = len(changed_profiles)
        log.info(
            "Triage skipped: treating all %d changes as material",
            result.material_changes,
        )

    # ------------------------------------------------------------------
    # Step 5: Update enrichment_metadata
    # ------------------------------------------------------------------
    log.info("Updating enrichment_metadata for %d profiles", len(hash_results))
    for hr in hash_results:
        if hr.error:
            continue
        try:
            tr = triage_by_id.get(hr.profile_id)
            _update_hash_metadata(
                profile_id=hr.profile_id,
                hash_result=hr,
                triage_result=tr,
                dry_run=dry_run,
            )
        except Exception as exc:
            log.error("Failed to update metadata for %s: %s", hr.profile_id, exc)

    # ------------------------------------------------------------------
    # Step 6: Queue material changes for re-enrichment
    # ------------------------------------------------------------------
    if skip_triage:
        # All changed profiles are queued
        queued_ids = [hr.profile_id for hr in changed_hashes]
    else:
        queued_ids = [
            tr.profile_id
            for tr in triage_results
            if tr.classification == "material"
        ]

    result.queued_for_enrichment = queued_ids

    if queued_ids and not dry_run:
        log.info(
            "Queued %d profiles for re-enrichment: %s",
            len(queued_ids),
            ", ".join(queued_ids[:10]) + ("..." if len(queued_ids) > 10 else ""),
        )
    elif queued_ids and dry_run:
        log.info(
            "DRY RUN: would queue %d profiles for re-enrichment", len(queued_ids)
        )

    # ------------------------------------------------------------------
    # Done
    # ------------------------------------------------------------------
    result.runtime_seconds = time.time() - start_time
    log.info(
        "Change detection complete: %d checked, %d changed, "
        "%d material, %d cosmetic, %d errors, %d queued, %.1fs runtime",
        result.profiles_checked,
        result.changes_detected,
        result.material_changes,
        result.cosmetic_changes,
        result.errors,
        len(result.queued_for_enrichment),
        result.runtime_seconds,
    )

    return result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    if not os.environ.get("DATABASE_URL"):
        print("ERROR: DATABASE_URL environment variable is not set.")
        raise SystemExit(1)

    parser = argparse.ArgumentParser(
        description="Profile Freshness Monitoring: change detection pipeline"
    )
    parser.add_argument(
        "--tiers",
        type=str,
        default="A,B,C",
        help="Comma-separated freshness tiers to check (default: A,B,C)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max profiles to check; 0 = no limit (default: 0)",
    )
    parser.add_argument(
        "--skip-triage",
        action="store_true",
        help="Skip Layer 2 semantic triage (treat all changes as material)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log actions without writing to the database",
    )

    args = parser.parse_args()

    result = change_detection_flow(
        tiers=args.tiers,
        limit=args.limit,
        skip_triage=args.skip_triage,
        dry_run=args.dry_run,
    )

    print()
    print("=" * 60)
    print("CHANGE DETECTION SUMMARY")
    print("=" * 60)
    print(f"  Profiles checked:       {result.profiles_checked}")
    print(f"  Pages fetched:          {result.pages_fetched}")
    print(f"  Changes detected:       {result.changes_detected}")
    print(f"  Material changes:       {result.material_changes}")
    print(f"  Cosmetic changes:       {result.cosmetic_changes}")
    print(f"  Errors:                 {result.errors}")
    print(f"  Queued for enrichment:  {len(result.queued_for_enrichment)}")
    print(f"  Runtime:                {result.runtime_seconds:.1f}s")
    print("=" * 60)

    if result.queued_for_enrichment:
        print()
        print("Profiles queued for re-enrichment:")
        for pid in result.queued_for_enrichment:
            print(f"  - {pid}")
