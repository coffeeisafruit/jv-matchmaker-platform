"""
Top-level monthly orchestrator -- Prefect @flow.

Coordinates the entire monthly cycle by dispatching to the appropriate
sub-flow based on the current calendar date or an explicit ``--phase``
argument.

Calendar phases:
    Week 1 Mon           : change_detection_flow (profile freshness)
    Week 3 Mon/Wed/Fri   : client_verification_flow
    Week 4 Mon           : monthly_processing_flow
    Week 4 Tue           : admin_notification_flow
    1st of month         : report_delivery_flow

When ``phase="auto"`` (the default), the orchestrator inspects today's
date to determine which phase to run.

Usage (CLI):
    python -m matching.enrichment.flows.monthly_orchestrator
    python -m matching.enrichment.flows.monthly_orchestrator --phase processing --dry-run
    python -m matching.enrichment.flows.monthly_orchestrator --phase auto

Usage (Prefect -- e.g. Prefect cron deployment):
    from matching.enrichment.flows.monthly_orchestrator import monthly_orchestrator_flow
    monthly_orchestrator_flow(phase="auto")
"""

from __future__ import annotations

import calendar
import os
from datetime import date, datetime
from typing import Any

from prefect import flow, get_run_logger


# ---------------------------------------------------------------------------
# Phase constants
# ---------------------------------------------------------------------------

PHASES = (
    "freshness",
    "verification",
    "processing",
    "notification",
    "delivery",
    "auto",
)


# ---------------------------------------------------------------------------
# Calendar helpers
# ---------------------------------------------------------------------------

def _week_of_month(dt: date) -> int:
    """Return the week-of-month (1-based) for a given date.

    Week 1 starts on the first day of the month.  Uses ISO weekday
    alignment so Monday = 1.
    """
    first_day = dt.replace(day=1)
    # Adjust so week boundaries align to Mondays
    adjusted_day = dt.day + first_day.weekday()
    return (adjusted_day - 1) // 7 + 1


def _determine_phase(dt: date | None = None) -> str:
    """Determine which monthly-cycle phase to run based on the calendar date.

    Rules (in priority order):
      - 1st of month             -> ``"delivery"``
      - Week 1, Monday           -> ``"freshness"``
      - Week 3, Mon/Wed/Fri      -> ``"verification"``
      - Week 4, Monday           -> ``"processing"``
      - Week 4, Tuesday          -> ``"notification"``
      - Anything else            -> ``"none"`` (no phase scheduled)

    Parameters
    ----------
    dt:
        The date to evaluate.  Defaults to ``date.today()``.

    Returns
    -------
    str -- one of the phase names or ``"none"``.
    """
    if dt is None:
        dt = date.today()

    day = dt.day
    weekday = dt.isoweekday()  # Mon=1 .. Sun=7
    week = _week_of_month(dt)
    weekday_name = calendar.day_name[weekday - 1].lower()  # "monday", etc.

    # 1st of month: delivery
    if day == 1:
        return "delivery"

    # Week 1, Monday: freshness
    if week == 1 and weekday == 1:
        return "freshness"

    # Week 3, Mon/Wed/Fri: verification
    if week == 3 and weekday in (1, 3, 5):
        return "verification"

    # Week 4, Monday: processing
    if week == 4 and weekday == 1:
        return "processing"

    # Week 4, Tuesday: notification
    if week == 4 and weekday == 2:
        return "notification"

    return "none"


def _day_of_week_name(dt: date | None = None) -> str:
    """Return the lowercase day name for a date (default: today)."""
    if dt is None:
        dt = date.today()
    return calendar.day_name[dt.isoweekday() - 1].lower()


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(
    name="monthly-orchestrator",
    description="Top-level monthly cycle: freshness -> verification -> processing -> notification -> delivery",
    retries=0,
)
def monthly_orchestrator_flow(
    phase: str = "auto",
    dry_run: bool = False,
) -> dict[str, Any]:
    """Coordinate the entire monthly cycle.

    Phases (calendar-driven):
      - Week 1 Mon          : change_detection_flow (profile freshness)
      - Week 3 Mon/Wed/Fri  : client_verification_flow
      - Week 4 Mon          : monthly_processing_flow
      - Week 4 Tue          : admin_notification_flow
      - 1st of month        : report_delivery_flow

    When ``phase="auto"``, determines current phase from the calendar date.

    Parameters
    ----------
    phase:
        One of ``"freshness"``, ``"verification"``, ``"processing"``,
        ``"notification"``, ``"delivery"``, or ``"auto"``.
    dry_run:
        If True, all sub-flows run in dry-run mode.

    Returns
    -------
    dict with: phase_run, result, timestamp.
    """
    logger = get_run_logger()

    # Resolve auto phase
    if phase == "auto":
        resolved_phase = _determine_phase()
        logger.info(
            "Auto-detected phase: '%s' (date=%s, week=%d, day=%s)",
            resolved_phase,
            date.today().isoformat(),
            _week_of_month(date.today()),
            _day_of_week_name(),
        )
    else:
        resolved_phase = phase
        logger.info("Explicit phase: '%s' (dry_run=%s)", resolved_phase, dry_run)

    if resolved_phase == "none":
        logger.info("No phase scheduled for today -- nothing to do")
        return {
            "phase_run": "none",
            "result": None,
            "timestamp": datetime.utcnow().isoformat(),
        }

    # Dispatch to the appropriate sub-flow
    result: Any = None

    if resolved_phase == "freshness":
        result = _run_freshness(dry_run=dry_run)

    elif resolved_phase == "verification":
        result = _run_verification(dry_run=dry_run)

    elif resolved_phase == "processing":
        result = _run_processing(dry_run=dry_run)

    elif resolved_phase == "notification":
        result = _run_notification(dry_run=dry_run)

    elif resolved_phase == "delivery":
        result = _run_delivery(dry_run=dry_run)

    else:
        logger.error("Unknown phase: '%s'", resolved_phase)
        return {
            "phase_run": resolved_phase,
            "result": {"error": f"Unknown phase: {resolved_phase}"},
            "timestamp": datetime.utcnow().isoformat(),
        }

    logger.info("Monthly orchestrator complete: phase='%s'", resolved_phase)

    return {
        "phase_run": resolved_phase,
        "result": _serialise_result(result),
        "timestamp": datetime.utcnow().isoformat(),
    }


# ---------------------------------------------------------------------------
# Phase runners
# ---------------------------------------------------------------------------

def _run_freshness(dry_run: bool = False) -> Any:
    """Week 1 Mon: run change detection / profile freshness check."""
    logger = get_run_logger()
    logger.info("Running freshness phase (change_detection_flow)")

    from matching.enrichment.flows.change_detection_flow import change_detection_flow

    return change_detection_flow(dry_run=dry_run)


def _run_verification(dry_run: bool = False) -> Any:
    """Week 3 Mon/Wed/Fri: send client verification emails."""
    logger = get_run_logger()
    day = _day_of_week_name()
    logger.info("Running verification phase (day=%s)", day)

    from matching.enrichment.flows.client_verification import client_verification_flow

    return client_verification_flow(day_of_week=day, dry_run=dry_run)


def _run_processing(dry_run: bool = False) -> Any:
    """Week 4 Mon: full monthly processing pipeline."""
    logger = get_run_logger()
    logger.info("Running processing phase (monthly_processing_flow)")

    from matching.enrichment.flows.monthly_processing import monthly_processing_flow

    return monthly_processing_flow(
        stale_days=30,
        target_score=70,
        target_count=10,
        dry_run=dry_run,
    )


def _run_notification(dry_run: bool = False) -> Any:
    """Week 4 Tue: admin notification with AI suggestions."""
    logger = get_run_logger()
    logger.info("Running notification phase (admin_notification_flow)")

    from matching.enrichment.flows.admin_notification import admin_notification_flow

    return admin_notification_flow(dry_run=dry_run)


def _run_delivery(dry_run: bool = False) -> Any:
    """1st of month: deliver reports with new access codes."""
    logger = get_run_logger()
    logger.info("Running delivery phase (report_delivery_flow)")

    from matching.enrichment.flows.report_delivery import report_delivery_flow

    return report_delivery_flow(dry_run=dry_run)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _serialise_result(result: Any) -> Any:
    """Best-effort conversion of a dataclass result to a dict."""
    if result is None:
        return None
    if isinstance(result, dict):
        return result
    if isinstance(result, list):
        return [_serialise_result(item) for item in result]
    if hasattr(result, "__dataclass_fields__"):
        from dataclasses import asdict
        return asdict(result)
    return str(result)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    if not os.environ.get("DATABASE_URL"):
        print("ERROR: DATABASE_URL environment variable is not set.")
        raise SystemExit(1)

    parser = argparse.ArgumentParser(
        description="Monthly cycle orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Phases:\n"
            "  freshness     - Week 1 Mon: change detection / profile freshness\n"
            "  verification  - Week 3 Mon/Wed/Fri: client verification emails\n"
            "  processing    - Week 4 Mon: re-enrich, rescore, gap detect, acquire, generate\n"
            "  notification  - Week 4 Tue: admin notification with AI suggestions\n"
            "  delivery      - 1st of month: deliver reports with new access codes\n"
            "  auto          - Determine phase from today's date (default)\n"
        ),
    )
    parser.add_argument(
        "--phase",
        type=str,
        default="auto",
        choices=list(PHASES),
        help="Phase to run (default: auto-detect from calendar)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run all sub-flows in dry-run mode",
    )
    args = parser.parse_args()

    result = monthly_orchestrator_flow(phase=args.phase, dry_run=args.dry_run)
    print(f"\nOrchestrator result: phase={result['phase_run']}")
    if result.get("result"):
        import json as _json

        try:
            print(_json.dumps(result["result"], indent=2, default=str))
        except (TypeError, ValueError):
            print(result["result"])
