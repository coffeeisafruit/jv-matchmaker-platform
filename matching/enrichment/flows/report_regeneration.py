"""
Report regeneration tasks -- Prefect @task wrappers.

Wraps the existing ``regenerate_member_report`` task from
``matching/tasks.py`` and the ``generate_member_report`` management
command into Prefect-compatible @task functions for use by the monthly
processing and orchestrator flows.

Usage (from another flow):
    from matching.enrichment.flows.report_regeneration import (
        regenerate_report,
        regenerate_reports_batch,
    )
    result = regenerate_report(client_id="abc-123")
    results = regenerate_reports_batch(["abc-123", "def-456"])
"""

from __future__ import annotations

import os
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor
from prefect import task, get_run_logger


# ---------------------------------------------------------------------------
# Database helper
# ---------------------------------------------------------------------------

def _get_db_connection() -> psycopg2.extensions.connection:
    """Create a new psycopg2 connection from ``DATABASE_URL``."""
    dsn = os.environ["DATABASE_URL"]
    return psycopg2.connect(dsn)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_report_id_for_client(client_id: str) -> int | None:
    """Look up the active MemberReport ID for a client's supabase_profile_id.

    Returns
    -------
    int or None -- the ``matching_memberreport.id``, or None if not found.
    """
    sql = """
        SELECT id
        FROM matching_memberreport
        WHERE supabase_profile_id = %s
          AND is_active = true
        ORDER BY month DESC
        LIMIT 1
    """

    conn = _get_db_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, (client_id,))
        row = cur.fetchone()
        return row["id"] if row else None
    finally:
        conn.close()


def _get_report_summary(report_id: int) -> dict[str, Any]:
    """Fetch a lightweight summary of a MemberReport after regeneration."""
    sql = """
        SELECT
            mr.id            AS report_id,
            mr.member_name,
            mr.access_code,
            (
                SELECT COUNT(*)
                FROM matching_reportpartner rp
                WHERE rp.report_id = mr.id
            )                AS partner_count
        FROM matching_memberreport mr
        WHERE mr.id = %s
    """

    conn = _get_db_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(sql, (report_id,))
        row = cur.fetchone()
        if row:
            return dict(row)
        return {"report_id": report_id, "partner_count": 0, "status": "not_found"}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="regenerate-report", retries=1, retry_delay_seconds=10)
def regenerate_report(client_id: str) -> dict[str, Any]:
    """Regenerate a single client's MemberReport.

    Wraps the existing ``regenerate_member_report`` task from
    ``matching/tasks.py``.  Looks up the active report ID for the given
    client (by ``supabase_profile_id``), then delegates to the existing
    infrastructure.

    Parameters
    ----------
    client_id:
        UUID string of the client's SupabaseProfile.

    Returns
    -------
    dict with: client_id, report_id, partner_count, status.
    """
    logger = get_run_logger()

    report_id = _get_report_id_for_client(client_id)
    if report_id is None:
        logger.warning(
            "No active MemberReport found for client %s", client_id,
        )
        return {
            "client_id": client_id,
            "report_id": None,
            "partner_count": 0,
            "status": "no_report",
        }

    logger.info(
        "Regenerating report %d for client %s", report_id, client_id,
    )

    try:
        from matching.tasks import regenerate_member_report

        result = regenerate_member_report(report_id)

        if result.get("errors"):
            logger.warning(
                "Report %d regenerated with errors: %s",
                report_id,
                result["errors"][:3],
            )

        # Fetch the post-regeneration summary
        summary = _get_report_summary(report_id)
        summary["client_id"] = client_id
        summary["status"] = "regenerated"
        summary["errors"] = result.get("errors", [])

        logger.info(
            "Report %d regenerated: %d partners",
            report_id,
            summary.get("partner_count", 0),
        )
        return summary

    except Exception as exc:
        logger.error(
            "Failed to regenerate report %d for client %s: %s",
            report_id,
            client_id,
            exc,
        )
        return {
            "client_id": client_id,
            "report_id": report_id,
            "partner_count": 0,
            "status": "failed",
            "error": str(exc),
        }


@task(name="regenerate-reports-batch")
def regenerate_reports_batch(client_ids: list[str]) -> list[dict[str, Any]]:
    """Regenerate reports for multiple clients.

    Iterates over *client_ids* and calls ``regenerate_report`` for each.
    Collects all results regardless of individual failures.

    Parameters
    ----------
    client_ids:
        List of UUID strings identifying client SupabaseProfiles.

    Returns
    -------
    list[dict] -- one result dict per client, same shape as
    ``regenerate_report`` output.
    """
    logger = get_run_logger()
    logger.info("Batch regeneration: %d clients", len(client_ids))

    results: list[dict[str, Any]] = []
    succeeded = 0
    failed = 0

    for client_id in client_ids:
        try:
            result = regenerate_report.fn(client_id)
            results.append(result)
            if result.get("status") == "regenerated":
                succeeded += 1
            else:
                failed += 1
        except Exception as exc:
            logger.error("Batch item failed for %s: %s", client_id, exc)
            results.append({
                "client_id": client_id,
                "report_id": None,
                "partner_count": 0,
                "status": "failed",
                "error": str(exc),
            })
            failed += 1

    logger.info(
        "Batch regeneration complete: %d succeeded, %d failed out of %d",
        succeeded,
        failed,
        len(client_ids),
    )
    return results
