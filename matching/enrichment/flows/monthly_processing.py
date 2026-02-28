"""
Week 4 Monday processing flow -- Prefect @flow.

Runs the full monthly processing pipeline:
  1. Re-enrich stale profiles via enrichment_flow(refresh_mode=True)
  2. Apply any client profile updates from verification
  3. Rescore all matches (rescore_matches management command)
  4. Gap detection for each client
  5. Trigger acquisition pipeline for clients with gaps
  6. Generate/regenerate reports for all clients

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

@task(name="re-enrich-stale-profiles", retries=1, retry_delay_seconds=30)
def re_enrich_stale_profiles(
    stale_days: int = 30,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Re-enrich profiles whose data is older than *stale_days*.

    Delegates to the existing ``enrichment_flow`` in refresh mode.

    Returns
    -------
    dict with: profiles_selected, profiles_written, total_cost.
    """
    logger = get_run_logger()

    from matching.enrichment.flows.enrichment_flow import enrichment_flow

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
        "total_cost": result.total_cost,
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
    description="Week 4 Mon: Re-enrich stale -> rescore -> gap detect -> acquire -> generate reports",
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
      1. Re-enrich stale profiles (>stale_days old) via enrichment_flow(refresh_mode=True)
      2. Apply any client profile updates from verification
      3. Rescore all matches (call rescore_matches management command)
      4. Gap detection for each client
      5. Trigger acquisition pipeline for clients with gaps (if not skip_acquisition)
      6. Generate/regenerate reports for all clients

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

    # Step 1: Re-enrich stale profiles
    logger.info("Step 1/6: Re-enriching stale profiles")
    enrich_result = re_enrich_stale_profiles(
        stale_days=stale_days,
        dry_run=dry_run,
    )
    result.profiles_re_enriched = enrich_result.get("profiles_written", 0)
    result.total_cost += enrich_result.get("total_cost", 0.0)

    # Step 2: Apply verification updates
    logger.info("Step 2/6: Applying verification updates")
    verify_result = apply_verification_updates()
    result.clients_processed = verify_result.get("clients_updated", 0)

    # Step 3: Rescore all matches
    logger.info("Step 3/6: Rescoring all matches")
    rescore_result = rescore_all_matches(dry_run=dry_run)
    if rescore_result.get("return_code", 1) == 0:
        # Parse match count from output if possible
        output = rescore_result.get("output", "")
        result.matches_rescored = _parse_rescore_count(output)
    else:
        logger.warning("Rescore command returned non-zero; continuing anyway")

    # Step 4: Gap detection
    logger.info("Step 4/6: Running gap detection")
    gaps = run_gap_detection(
        target_score=target_score,
        target_count=target_count,
    )
    result.gaps_detected = sum(1 for g in gaps if g.get("has_gap"))

    # Step 5: Acquisition (optional)
    if skip_acquisition:
        logger.info("Step 5/6: Acquisition SKIPPED (skip_acquisition=True)")
    else:
        logger.info("Step 5/6: Triggering acquisition for clients with gaps")
        acq_result = trigger_acquisition(gaps=gaps, dry_run=dry_run)
        result.acquisitions_triggered = acq_result.get("clients_triggered", 0)
        result.total_cost += acq_result.get("total_cost", 0.0)

    # Step 6: Generate reports
    logger.info("Step 6/6: Generating/regenerating reports")
    report_result = generate_all_reports(dry_run=dry_run)
    result.reports_generated = report_result.get("regenerated", 0)

    elapsed = time.time() - start_time
    logger.info(
        "Monthly processing complete in %.1fs: "
        "re-enriched=%d, rescored=%d, gaps=%d, acquisitions=%d, reports=%d, cost=$%.2f",
        elapsed,
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
