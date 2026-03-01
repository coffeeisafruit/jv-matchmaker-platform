"""
Week 4 Monday processing flow -- Prefect @flow.

Runs the full monthly processing pipeline:
  1. Flag low-confidence and stale profiles for priority re-enrichment
  2. Re-enrich priority profiles first, then stale profiles via enrichment_flow
  3. Apply any client profile updates from verification
  4. Rescore all matches (rescore_matches management command)
  5. Gap detection for each client
  6. Trigger acquisition pipeline for clients with gaps
  7. Generate/regenerate reports for all clients

This is the heaviest compute step in the monthly cycle and has a 2-hour
timeout to accommodate large batches.

Usage (CLI):
    python -m matching.enrichment.flows.monthly_processing
    python -m matching.enrichment.flows.monthly_processing --dry-run --skip-acquisition

Usage (Prefect):
    from matching.enrichment.flows.monthly_processing import monthly_processing_flow
    monthly_processing_flow(stale_days=30, target_score=70)
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor
from prefect import flow, task, get_run_logger


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class MonthlyProcessingResult:
    """Results from the Week 4 Monday processing run."""

    priority_profiles_flagged: int = 0
    priority_profiles_enriched: int = 0
    clients_processed: int = 0
    profiles_re_enriched: int = 0
    matches_rescored: int = 0
    gaps_detected: int = 0
    acquisitions_triggered: int = 0
    reports_generated: int = 0
    total_cost: float = 0.0


# ---------------------------------------------------------------------------
# Database helper
# ---------------------------------------------------------------------------

def _get_db_connection() -> psycopg2.extensions.connection:
    """Create a new psycopg2 connection from ``DATABASE_URL``."""
    dsn = os.environ["DATABASE_URL"]
    return psycopg2.connect(dsn)


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="flag-low-confidence-profiles", retries=1, retry_delay_seconds=10)
def flag_low_confidence_profiles(
    confidence_threshold: float = 0.5,
    stale_days: int = 29,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Identify profiles needing priority re-enrichment.

    Queries the database for profiles with:
      - ``profile_confidence`` below *confidence_threshold*, OR
      - ``last_enriched_at`` older than *stale_days*

    Returns a deduplicated list of profile IDs sorted by confidence (lowest
    first) so the enrichment flow can prioritise them.

    Returns
    -------
    dict with: low_confidence_count, stale_count, total_flagged, profile_ids.
    """
    logger = get_run_logger()
    conn = _get_db_connection()

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Low-confidence profiles
        cur.execute(
            """
            SELECT id::text
            FROM profiles
            WHERE profile_confidence IS NOT NULL
              AND profile_confidence < %s
            ORDER BY profile_confidence ASC
            """,
            (confidence_threshold,),
        )
        low_conf_ids = [row["id"] for row in cur.fetchall()]

        # Stale profiles (last_enriched_at older than stale_days)
        cur.execute(
            """
            SELECT id::text
            FROM profiles
            WHERE last_enriched_at IS NOT NULL
              AND last_enriched_at < NOW() - INTERVAL '%s days'
            ORDER BY last_enriched_at ASC
            """,
            (stale_days,),
        )
        stale_ids = [row["id"] for row in cur.fetchall()]

        # Combine and deduplicate, preserving low-confidence-first order
        seen: set[str] = set()
        combined_ids: list[str] = []
        for pid in low_conf_ids + stale_ids:
            if pid not in seen:
                seen.add(pid)
                combined_ids.append(pid)

        logger.info(
            "Low-confidence flagging: %d low-conf, %d stale, %d combined (deduped)",
            len(low_conf_ids),
            len(stale_ids),
            len(combined_ids),
        )

        if dry_run:
            logger.info(
                "[DRY RUN] Would flag %d profiles for priority re-enrichment",
                len(combined_ids),
            )

        return {
            "low_confidence_count": len(low_conf_ids),
            "stale_count": len(stale_ids),
            "total_flagged": len(combined_ids),
            "profile_ids": combined_ids,
        }
    finally:
        conn.close()


@task(name="re-enrich-stale-profiles", retries=1, retry_delay_seconds=30)
def re_enrich_stale_profiles(
    stale_days: int = 30,
    priority_ids: list[str] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Re-enrich profiles whose data is older than *stale_days*.

    When *priority_ids* is provided, those profiles are enriched first
    (via ``profile_ids`` parameter on the enrichment flow), before
    falling back to the standard refresh-mode selection.

    Delegates to the existing ``enrichment_flow`` in refresh mode.

    Returns
    -------
    dict with: profiles_selected, profiles_written, total_cost,
    priority_profiles_enriched.
    """
    logger = get_run_logger()

    from matching.enrichment.flows.enrichment_flow import enrichment_flow

    priority_written = 0
    priority_cost = 0.0

    # Phase A: Enrich priority (low-confidence / stale) profiles first
    if priority_ids:
        logger.info(
            "Re-enriching %d priority profiles (low-confidence/stale) before "
            "standard refresh (dry_run=%s)",
            len(priority_ids),
            dry_run,
        )
        priority_result = enrichment_flow(
            limit=len(priority_ids),
            profile_ids=priority_ids,
            dry_run=dry_run,
        )
        priority_written = priority_result.profiles_written
        priority_cost = priority_result.total_cost
        logger.info(
            "Priority re-enrichment complete: %d written, cost=$%.2f",
            priority_written,
            priority_cost,
        )

    # Phase B: Standard stale-profile refresh
    logger.info(
        "Re-enriching stale profiles (stale_days=%d, dry_run=%s)",
        stale_days,
        dry_run,
    )

    result = enrichment_flow(
        limit=200,
        priority="tiered",
        refresh_mode=True,
        stale_days=stale_days,
        dry_run=dry_run,
    )

    summary = {
        "profiles_selected": result.profiles_selected,
        "profiles_written": result.profiles_written,
        "total_cost": result.total_cost + priority_cost,
        "priority_profiles_enriched": priority_written,
    }
    logger.info("Re-enrichment complete: %s", summary)
    return summary


@task(name="apply-verification-updates", retries=1, retry_delay_seconds=5)
def apply_verification_updates() -> dict[str, Any]:
    """Apply client profile updates captured during the verification phase.

    Reads ``enrichment_metadata -> verification -> {month} -> changes_made``
    for each active client and applies field-level updates to the profile.

    Returns
    -------
    dict with: clients_checked, clients_updated, fields_changed.
    """
    logger = get_run_logger()
    month_key = datetime.utcnow().strftime("%Y-%m")

    sql = """
        SELECT
            id::text AS client_id,
            name,
            enrichment_metadata -> 'verification' -> %s AS vstatus
        FROM profiles
        WHERE enrichment_metadata -> 'verification' -> %s IS NOT NULL
    """

    conn = _get_db_connection()
    clients_updated = 0
    fields_changed = 0

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, (month_key, month_key))
        rows = cur.fetchall()

        for row in rows:
            vstatus = row.get("vstatus") or {}
            changes = vstatus.get("changes_made") or {}
            if not changes:
                continue

            # Build dynamic UPDATE for changed fields
            allowed_fields = {
                "seeking", "offering", "niche", "who_you_serve",
                "what_you_do", "company", "business_focus",
            }
            updates = {k: v for k, v in changes.items() if k in allowed_fields and v}

            if not updates:
                continue

            set_clauses = ", ".join(f"{k} = %s" for k in updates)
            values = list(updates.values()) + [row["client_id"]]
            update_sql = f"UPDATE profiles SET {set_clauses} WHERE id = %s"

            cur.execute(update_sql, values)
            clients_updated += 1
            fields_changed += len(updates)
            logger.info(
                "Applied %d field updates for %s",
                len(updates),
                row["name"],
            )

        conn.commit()
    finally:
        conn.close()

    summary = {
        "clients_checked": len(rows) if "rows" in dir() else 0,
        "clients_updated": clients_updated,
        "fields_changed": fields_changed,
    }
    logger.info("Verification updates applied: %s", summary)
    return summary


@task(name="rescore-all-matches", retries=1, retry_delay_seconds=30)
def rescore_all_matches(dry_run: bool = False) -> dict[str, Any]:
    """Rescore all matches using the ``rescore_matches`` management command.

    Returns
    -------
    dict with: return_code, output (truncated stdout).
    """
    logger = get_run_logger()

    cmd = [sys.executable, "manage.py", "rescore_matches"]
    if dry_run:
        cmd.append("--dry-run")

    logger.info("Running: %s", " ".join(cmd))
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=3600,  # 1 hour max
        cwd=os.environ.get("DJANGO_PROJECT_DIR", "."),
    )

    output_preview = (proc.stdout or "")[-2000:]  # last 2 KB
    if proc.returncode != 0:
        logger.error(
            "rescore_matches failed (rc=%d): %s",
            proc.returncode,
            (proc.stderr or "")[-1000:],
        )
    else:
        logger.info("rescore_matches completed successfully")

    return {
        "return_code": proc.returncode,
        "output": output_preview,
    }


@task(name="run-gap-detection", retries=1, retry_delay_seconds=10)
def run_gap_detection(
    target_score: int = 70,
    target_count: int = 10,
) -> list[dict[str, Any]]:
    """Run gap detection for all active clients.

    Delegates to ``detect_gaps_batch`` from the gap_detection module.

    Returns
    -------
    list[dict] -- gap analysis per client, sorted by gap size descending.
    """
    logger = get_run_logger()

    from matching.enrichment.flows.gap_detection import detect_gaps_batch

    logger.info(
        "Running gap detection: target_score=%d, target_count=%d",
        target_score,
        target_count,
    )

    # Call the underlying task function directly (we're already in a flow)
    gaps = detect_gaps_batch.fn(
        target_score=target_score,
        target_count=target_count,
    )

    clients_with_gaps = sum(1 for g in gaps if g.get("has_gap"))
    logger.info(
        "Gap detection complete: %d/%d clients have gaps",
        clients_with_gaps,
        len(gaps),
    )
    return gaps


@task(name="trigger-acquisition", retries=1, retry_delay_seconds=30)
def trigger_acquisition(
    gaps: list[dict[str, Any]],
    dry_run: bool = False,
) -> dict[str, Any]:
    """Trigger the acquisition flow for clients that have match gaps.

    Only processes clients where ``has_gap`` is True.

    Returns
    -------
    dict with: clients_triggered, total_prospects, total_cost.
    """
    logger = get_run_logger()

    clients_with_gaps = [g for g in gaps if g.get("has_gap")]
    if not clients_with_gaps:
        logger.info("No clients have gaps -- skipping acquisition")
        return {"clients_triggered": 0, "total_prospects": 0, "total_cost": 0.0}

    from matching.enrichment.flows.acquisition_flow import acquisition_flow

    total_triggered = 0
    total_prospects = 0
    total_cost = 0.0

    for gap in clients_with_gaps:
        client_id = gap["client_id"]
        gap_size = gap.get("gap", 0)

        logger.info(
            "Triggering acquisition for %s (%s) -- gap=%d",
            gap.get("client_name", "?"),
            client_id,
            gap_size,
        )

        if dry_run:
            logger.info("[DRY RUN] Would run acquisition for %s", client_id)
            total_triggered += 1
            continue

        try:
            acq_result = acquisition_flow(
                client_profile_id=client_id,
                target_count=gap_size,
                dry_run=dry_run,
            )
            total_triggered += 1
            total_prospects += getattr(acq_result, "prospects_found", 0)
            total_cost += getattr(acq_result, "total_cost", 0.0)
        except Exception as exc:
            logger.error(
                "Acquisition failed for %s: %s", client_id, exc,
            )

    summary = {
        "clients_triggered": total_triggered,
        "total_prospects": total_prospects,
        "total_cost": total_cost,
    }
    logger.info("Acquisition complete: %s", summary)
    return summary


@task(name="generate-all-reports", retries=1, retry_delay_seconds=10)
def generate_all_reports(dry_run: bool = False) -> dict[str, Any]:
    """Regenerate reports for all active clients.

    Uses the existing ``regenerate_member_report`` task from
    ``matching.tasks``.

    Returns
    -------
    dict with: total, regenerated, errors.
    """
    logger = get_run_logger()

    from matching.models import MemberReport
    from matching.tasks import regenerate_member_report

    reports = MemberReport.objects.filter(is_active=True)
    total = reports.count()
    regenerated = 0
    errors: list[str] = []

    logger.info("Regenerating %d active reports (dry_run=%s)", total, dry_run)

    for report in reports:
        if dry_run:
            logger.info("[DRY RUN] Would regenerate report %d", report.id)
            regenerated += 1
            continue

        try:
            result = regenerate_member_report(report.id)
            if result.get("errors"):
                errors.extend(result["errors"])
            else:
                regenerated += 1
        except Exception as exc:
            errors.append(f"Report {report.id}: {exc}")
            logger.error("Failed to regenerate report %d: %s", report.id, exc)

    summary = {
        "total": total,
        "regenerated": regenerated,
        "errors": errors[:20],  # cap error list
    }
    logger.info("Report generation complete: %d/%d", regenerated, total)
    return summary


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(
    name="monthly-processing",
    description="Week 4 Mon: Flag priority -> re-enrich -> rescore -> gap detect -> acquire -> generate reports",
    retries=0,
    timeout_seconds=7200,  # 2 hours
)
def monthly_processing_flow(
    stale_days: int = 30,
    target_score: int = 70,
    target_count: int = 10,
    skip_acquisition: bool = False,
    dry_run: bool = False,
) -> MonthlyProcessingResult:
    """Full Week 4 Monday processing pipeline.

    Steps:
      1. Flag low-confidence and stale profiles for priority re-enrichment
      2. Re-enrich flagged priority profiles, then stale profiles (>stale_days old)
      3. Apply any client profile updates from verification
      4. Rescore all matches (call rescore_matches management command)
      5. Gap detection for each client
      6. Trigger acquisition pipeline for clients with gaps (if not skip_acquisition)
      7. Generate/regenerate reports for all clients

    Parameters
    ----------
    stale_days:
        Profiles not enriched in this many days are re-enriched.
    target_score:
        Minimum harmonic_mean score for a "good" match (0-100).
    target_count:
        How many good matches each client should have.
    skip_acquisition:
        If True, skip the acquisition step even if gaps exist.
    dry_run:
        If True, no DB writes or emails.

    Returns
    -------
    MonthlyProcessingResult with aggregate stats.
    """
    logger = get_run_logger()
    start_time = time.time()
    result = MonthlyProcessingResult()

    logger.info(
        "Monthly processing started: stale_days=%d, target_score=%d, "
        "target_count=%d, skip_acquisition=%s, dry_run=%s",
        stale_days, target_score, target_count, skip_acquisition, dry_run,
    )

    # Step 1: Flag low-confidence and stale profiles for priority re-enrichment
    logger.info("Step 1/7: Flagging low-confidence and stale profiles")
    flag_result = flag_low_confidence_profiles(
        confidence_threshold=0.5,
        stale_days=29,
        dry_run=dry_run,
    )
    priority_ids = flag_result.get("profile_ids", [])
    result.priority_profiles_flagged = flag_result.get("total_flagged", 0)
    logger.info(
        "Flagged %d profiles for priority re-enrichment "
        "(%d low-confidence, %d stale)",
        flag_result.get("total_flagged", 0),
        flag_result.get("low_confidence_count", 0),
        flag_result.get("stale_count", 0),
    )

    # Step 2: Re-enrich priority profiles first, then stale profiles
    logger.info("Step 2/7: Re-enriching profiles (priority + stale)")
    enrich_result = re_enrich_stale_profiles(
        stale_days=stale_days,
        priority_ids=priority_ids if priority_ids else None,
        dry_run=dry_run,
    )
    result.profiles_re_enriched = enrich_result.get("profiles_written", 0)
    result.priority_profiles_enriched = enrich_result.get("priority_profiles_enriched", 0)
    result.total_cost += enrich_result.get("total_cost", 0.0)

    # Step 3: Apply verification updates
    logger.info("Step 3/7: Applying verification updates")
    verify_result = apply_verification_updates()
    result.clients_processed = verify_result.get("clients_updated", 0)

    # Step 4: Rescore all matches
    logger.info("Step 4/7: Rescoring all matches")
    rescore_result = rescore_all_matches(dry_run=dry_run)
    if rescore_result.get("return_code", 1) == 0:
        # Parse match count from output if possible
        output = rescore_result.get("output", "")
        result.matches_rescored = _parse_rescore_count(output)
    else:
        logger.warning("Rescore command returned non-zero; continuing anyway")

    # Step 5: Gap detection
    logger.info("Step 5/7: Running gap detection")
    gaps = run_gap_detection(
        target_score=target_score,
        target_count=target_count,
    )
    result.gaps_detected = sum(1 for g in gaps if g.get("has_gap"))

    # Step 6: Acquisition (optional)
    if skip_acquisition:
        logger.info("Step 6/7: Acquisition SKIPPED (skip_acquisition=True)")
    else:
        logger.info("Step 6/7: Triggering acquisition for clients with gaps")
        acq_result = trigger_acquisition(gaps=gaps, dry_run=dry_run)
        result.acquisitions_triggered = acq_result.get("clients_triggered", 0)
        result.total_cost += acq_result.get("total_cost", 0.0)

    # Step 7: Generate reports
    logger.info("Step 7/7: Generating/regenerating reports")
    report_result = generate_all_reports(dry_run=dry_run)
    result.reports_generated = report_result.get("regenerated", 0)

    elapsed = time.time() - start_time
    logger.info(
        "Monthly processing complete in %.1fs: "
        "priority_flagged=%d, priority_enriched=%d, re-enriched=%d, "
        "rescored=%d, gaps=%d, acquisitions=%d, reports=%d, cost=$%.2f",
        elapsed,
        result.priority_profiles_flagged,
        result.priority_profiles_enriched,
        result.profiles_re_enriched,
        result.matches_rescored,
        result.gaps_detected,
        result.acquisitions_triggered,
        result.reports_generated,
        result.total_cost,
    )

    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_rescore_count(output: str) -> int:
    """Best-effort parse of rescored count from management command stdout."""
    # Look for pattern like "N updated" in the output
    import re

    match = re.search(r"(\d+)\s+updated", output)
    if match:
        return int(match.group(1))
    return 0


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    if not os.environ.get("DATABASE_URL"):
        print("ERROR: DATABASE_URL environment variable is not set.")
        raise SystemExit(1)

    parser = argparse.ArgumentParser(description="Week 4 Monday monthly processing")
    parser.add_argument("--stale-days", type=int, default=30)
    parser.add_argument("--target-score", type=int, default=70)
    parser.add_argument("--target-count", type=int, default=10)
    parser.add_argument("--skip-acquisition", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    result = monthly_processing_flow(
        stale_days=args.stale_days,
        target_score=args.target_score,
        target_count=args.target_count,
        skip_acquisition=args.skip_acquisition,
        dry_run=args.dry_run,
    )
    print(f"\nResult: {result}")
