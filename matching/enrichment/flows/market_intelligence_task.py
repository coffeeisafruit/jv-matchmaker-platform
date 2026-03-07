"""
Prefect @task: compute market intelligence (supply-demand gaps, role gaps,
niche health) and persist a snapshot.

Wires MarketGapAnalyzer into the monthly Prefect cycle so market intelligence
is refreshed automatically during the Week 4 Monday processing flow.

Usage (within a Prefect flow):
    from matching.enrichment.flows.market_intelligence_task import compute_market_intelligence

    result = compute_market_intelligence(dry_run=False, min_profiles=50)
"""

from __future__ import annotations

import json
import os
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor
from prefect import task, get_run_logger

from matching.enrichment.market_gaps import MarketGapAnalyzer, generate_gap_report


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

PROFILES_QUERY = """
    SELECT id, name, niche, network_role, seeking, offering,
           what_you_do, who_you_serve, jv_tier
    FROM profiles
    WHERE (seeking IS NOT NULL AND seeking != '')
       OR (offering IS NOT NULL AND offering != '')
"""

CREATE_SNAPSHOT_TABLE = """
    CREATE TABLE IF NOT EXISTS niche_statistics_snapshots (
        id SERIAL PRIMARY KEY,
        computed_at TIMESTAMPTZ DEFAULT NOW(),
        snapshot_data JSONB NOT NULL,
        version INTEGER DEFAULT 1
    );
"""

INSERT_SNAPSHOT = """
    INSERT INTO niche_statistics_snapshots (snapshot_data)
    VALUES (%s)
    RETURNING id;
"""


# ---------------------------------------------------------------------------
# Database helper (matches monthly_processing.py pattern)
# ---------------------------------------------------------------------------

def _get_db_connection() -> psycopg2.extensions.connection:
    """Create a new psycopg2 connection, preferring direct over pgbouncer."""
    dsn = os.environ.get("DIRECT_DATABASE_URL") or os.environ["DATABASE_URL"]
    return psycopg2.connect(dsn, options="-c statement_timeout=0")


# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------

@task(name="compute-market-intelligence", retries=1, retry_delay_seconds=10)
def compute_market_intelligence(
    dry_run: bool = False,
    min_profiles: int = 50,
) -> dict[str, Any]:
    """Compute market intelligence from enriched profiles.

    Loads profiles with seeking/offering data, runs MarketGapAnalyzer,
    writes JSON + Markdown reports, and persists a snapshot to the
    ``niche_statistics_snapshots`` table.

    Parameters
    ----------
    dry_run:
        If True, run the analysis and write report files but do not persist
        the snapshot to the database.
    min_profiles:
        Minimum number of enriched profiles required. If fewer are found
        the task returns early with zeroed-out results.

    Returns
    -------
    dict with: enriched_profiles, gaps_found, role_gaps, top_gaps,
    health_score_avg.
    """
    logger = get_run_logger()

    # ── Load enriched profiles ────────────────────────────────────────
    conn = _get_db_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(PROFILES_QUERY)
        profiles = cur.fetchall()
    finally:
        conn.close()

    profile_count = len(profiles)
    logger.info("Loaded %d enriched profiles for market intelligence", profile_count)

    if profile_count < min_profiles:
        logger.warning(
            "Insufficient profiles: %d < %d minimum. Skipping analysis.",
            profile_count,
            min_profiles,
        )
        return {
            "enriched_profiles": profile_count,
            "gaps_found": 0,
            "role_gaps": 0,
            "top_gaps": [],
            "health_score_avg": 0.0,
        }

    # ── Run gap analysis ──────────────────────────────────────────────
    analyzer = MarketGapAnalyzer(profiles)
    report = analyzer.analyze()
    report_dict = report.to_dict()

    logger.info(
        "Analysis complete: %d supply-demand gaps, %d role gaps, %d niche health scores",
        len(report.supply_demand_gaps),
        len(report.role_gaps),
        len(report.niche_health),
    )

    # ── Write report files ────────────────────────────────────────────
    output_dir = os.path.join("reports", "market_intelligence")
    json_path, md_path = generate_gap_report(report, output_dir)
    logger.info("Reports written: %s, %s", json_path, md_path)

    # ── Persist snapshot ──────────────────────────────────────────────
    if dry_run:
        logger.info(
            "[DRY RUN] Would persist snapshot to niche_statistics_snapshots"
        )
    else:
        conn = _get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute(CREATE_SNAPSHOT_TABLE)
            cur.execute(INSERT_SNAPSHOT, (json.dumps(report_dict),))
            snapshot_id = cur.fetchone()[0]
            conn.commit()
            logger.info(
                "Snapshot persisted: niche_statistics_snapshots.id = %d",
                snapshot_id,
            )
        finally:
            conn.close()

    # ── Build summary ─────────────────────────────────────────────────
    high_demand = [g for g in report.supply_demand_gaps if g.gap_type == "high_demand"]
    top_gaps = [
        {
            "keyword": g.keyword,
            "seeking": g.seeking_count,
            "offering": g.offering_count,
            "ratio": round(g.gap_ratio, 2),
        }
        for g in high_demand[:10]
    ]

    health_score_avg = 0.0
    if report.niche_health:
        health_score_avg = round(
            sum(h.health_score for h in report.niche_health) / len(report.niche_health),
            1,
        )

    summary = {
        "enriched_profiles": report.enriched_profile_count,
        "gaps_found": len(report.supply_demand_gaps),
        "role_gaps": len(report.role_gaps),
        "top_gaps": top_gaps,
        "health_score_avg": health_score_avg,
    }
    logger.info("Market intelligence summary: %s", summary)
    return summary
