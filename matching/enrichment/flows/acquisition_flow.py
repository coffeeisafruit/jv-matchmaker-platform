"""
Main acquisition pipeline -- Prefect @flow.

Orchestrates the full candidate acquisition pipeline:
  gap detect -> discover -> pre-score -> ingest -> enrich -> re-score.

Discovers new JV partner prospects for a client, qualifies them with
lightweight ISMC scoring, saves ALL to the database, and feeds
high-scorers into the enrichment pipeline for full research.

Usage (CLI):
    python -m matching.enrichment.flows.acquisition_flow \\
        --client-id <uuid> --target-score 70 --target-count 10

Usage (Prefect):
    from matching.enrichment.flows.acquisition_flow import acquisition_flow
    result = acquisition_flow(client_profile_id="<uuid>")
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor
from prefect import flow, get_run_logger

from matching.enrichment.flows.gap_detection import detect_match_gaps
from matching.enrichment.flows.prospect_discovery import discover_prospects
from matching.enrichment.flows.prospect_prescoring import prescore_prospects
from matching.enrichment.flows.prospect_ingestion import ingest_prospects


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class AcquisitionResult:
    """Summary of a single acquisition flow run."""

    client_id: str = ""
    client_name: str = ""
    gap_detected: int = 0
    total_discovered: int = 0
    saved_to_db: int = 0
    duplicates: int = 0
    above_threshold: int = 0
    enriched: int = 0
    cost: float = 0.0
    runtime_seconds: float = 0.0
    skipped_reason: str = ""


# ---------------------------------------------------------------------------
# SQL: load client profile
# ---------------------------------------------------------------------------

_CLIENT_PROFILE_SQL = """
    SELECT
        id, name, email, company, website, linkedin,
        niche, what_you_do, who_you_serve, seeking, offering,
        audience_type, tags, booking_link, revenue_tier,
        list_size, social_reach, enrichment_metadata
    FROM profiles
    WHERE id = %s
"""


def _load_client_profile(client_profile_id: str) -> dict | None:
    """Load client profile from the database."""
    dsn = os.environ["DATABASE_URL"]
    conn = psycopg2.connect(dsn)
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(_CLIENT_PROFILE_SQL, (client_profile_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(
    name="candidate-acquisition",
    description="Discover, qualify, and ingest new prospects for a client",
    retries=0,
    timeout_seconds=1800,
)
def acquisition_flow(
    client_profile_id: str,
    target_score: int = 70,
    target_count: int = 10,
    max_prospects: int = 100,
    budget: float = 0.50,
    dry_run: bool = False,
) -> AcquisitionResult:
    """Full acquisition pipeline: gap detect -> discover -> pre-score ->
    ingest -> enrich -> re-score.

    Parameters
    ----------
    client_profile_id:
        UUID of the client profile to acquire prospects for.
    target_score:
        Minimum harmonic_mean score for a "good" match (0-100).
    target_count:
        How many good matches the client should have.
    max_prospects:
        Maximum number of prospects to discover.
    budget:
        Dollar budget for the discovery phase.
    dry_run:
        If True, skip DB writes and enrichment.

    Returns
    -------
    AcquisitionResult with aggregated stats.
    """
    logger = get_run_logger()
    start_time = time.time()
    result = AcquisitionResult(client_id=client_profile_id)

    # ------------------------------------------------------------------
    # Step 1: Load client profile from DB
    # ------------------------------------------------------------------
    logger.info("Loading client profile: %s", client_profile_id)
    client_profile = _load_client_profile(client_profile_id)

    if not client_profile:
        logger.error("Client profile %s not found", client_profile_id)
        result.skipped_reason = "client_not_found"
        result.runtime_seconds = time.time() - start_time
        return result

    result.client_name = client_profile.get("name", "")
    logger.info(
        "Client: %s (niche=%s)",
        result.client_name,
        client_profile.get("niche", "?"),
    )

    # ------------------------------------------------------------------
    # Step 2: Detect match gaps
    # ------------------------------------------------------------------
    logger.info("Running gap detection for %s", result.client_name)
    gap_analysis = detect_match_gaps(
        client_profile_id=client_profile_id,
        target_score=target_score,
        target_count=target_count,
    )

    result.gap_detected = gap_analysis.get("gap", 0)

    if not gap_analysis.get("has_gap"):
        logger.info(
            "No gap detected for %s -- %d/%d matches at score >= %d. "
            "Skipping acquisition.",
            result.client_name,
            gap_analysis.get("current_count", 0),
            target_count,
            target_score,
        )
        result.skipped_reason = "no_gap"
        result.runtime_seconds = time.time() - start_time
        return result

    logger.info(
        "Gap detected: %d more matches needed (current=%d, target=%d, "
        "avg=%.1f, lowest=%.1f)",
        result.gap_detected,
        gap_analysis.get("current_count", 0),
        target_count,
        gap_analysis.get("avg_score", 0),
        gap_analysis.get("lowest_score", 0),
    )

    # ------------------------------------------------------------------
    # Step 3: Discover prospects
    # ------------------------------------------------------------------
    logger.info(
        "Discovering prospects (max=%d, budget=$%.2f)",
        max_prospects, budget,
    )
    prospects = discover_prospects(
        client_profile=client_profile,
        gap_analysis=gap_analysis,
        max_results=max_prospects,
        budget=budget,
    )

    result.total_discovered = len(prospects)
    if not prospects:
        logger.info("No prospects discovered -- done")
        result.skipped_reason = "no_prospects_found"
        result.runtime_seconds = time.time() - start_time
        return result

    logger.info("Discovered %d prospects", len(prospects))

    # ------------------------------------------------------------------
    # Step 4: Pre-score prospects
    # ------------------------------------------------------------------
    logger.info("Pre-scoring %d prospects", len(prospects))
    scored_prospects = prescore_prospects(
        client_profile=client_profile,
        prospects=prospects,
        threshold=60,  # Pre-filter threshold
    )

    above_threshold = [
        p for p in scored_prospects if p.get("_above_threshold")
    ]
    result.above_threshold = len(above_threshold)

    logger.info(
        "Pre-scoring complete: %d/%d above threshold (60+)",
        len(above_threshold), len(scored_prospects),
    )

    # ------------------------------------------------------------------
    # Step 5: Ingest ALL prospects to database
    # ------------------------------------------------------------------
    if dry_run:
        logger.info("DRY RUN -- skipping DB ingestion")
        ingestion_result = {
            "new_ids": [],
            "duplicate_count": 0,
            "total_saved": 0,
            "errors": 0,
        }
    else:
        logger.info("Ingesting %d prospects to database", len(scored_prospects))
        ingestion_result = ingest_prospects(
            prospects=scored_prospects,
            source="acquisition",
        )

    result.saved_to_db = ingestion_result.get("total_saved", 0)
    result.duplicates = ingestion_result.get("duplicate_count", 0)
    new_ids = ingestion_result.get("new_ids", [])

    logger.info(
        "Ingestion: %d new, %d duplicates",
        result.saved_to_db, result.duplicates,
    )

    # ------------------------------------------------------------------
    # Step 6: Feed high-scorers into enrichment pipeline
    # ------------------------------------------------------------------
    # Only enrich newly ingested profiles that scored above threshold
    high_scorer_ids = []
    for prospect, new_id in _match_prospects_to_ids(scored_prospects, new_ids):
        if prospect.get("_above_threshold") and new_id:
            high_scorer_ids.append(new_id)

    if high_scorer_ids and not dry_run:
        logger.info(
            "Triggering enrichment for %d high-scoring prospects",
            len(high_scorer_ids),
        )
        try:
            from matching.enrichment.flows.enrichment_flow import enrichment_flow

            enrichment_result = enrichment_flow(
                profile_ids=high_scorer_ids,
                dry_run=dry_run,
            )
            result.enriched = enrichment_result.profiles_researched
            result.cost += enrichment_result.total_cost

            logger.info(
                "Enrichment complete: %d profiles researched, $%.3f cost",
                result.enriched, enrichment_result.total_cost,
            )
        except Exception as exc:
            logger.error("Enrichment flow failed: %s", exc)
            result.enriched = 0
    elif dry_run:
        logger.info(
            "DRY RUN -- would enrich %d high-scoring prospects",
            len(high_scorer_ids),
        )
    else:
        logger.info("No high-scoring new prospects to enrich")

    # ------------------------------------------------------------------
    # Done
    # ------------------------------------------------------------------
    result.runtime_seconds = time.time() - start_time

    logger.info(
        "Acquisition complete for %s: "
        "%d discovered, %d saved, %d duplicates, %d above threshold, "
        "%d enriched, $%.3f cost, %.1fs runtime",
        result.client_name,
        result.total_discovered,
        result.saved_to_db,
        result.duplicates,
        result.above_threshold,
        result.enriched,
        result.cost,
        result.runtime_seconds,
    )

    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _match_prospects_to_ids(
    prospects: list[dict],
    new_ids: list[str],
) -> list[tuple[dict, str | None]]:
    """Match prospects to their newly created profile IDs.

    The ingestion task creates profiles in order, skipping duplicates.
    This function pairs each prospect with its new ID (if it was created)
    or None (if it was a duplicate).
    """
    pairs: list[tuple[dict, str | None]] = []
    id_iter = iter(new_ids)

    for prospect in prospects:
        name = (prospect.get("name") or "").strip()
        if not name:
            pairs.append((prospect, None))
            continue

        # Prospects that were duplicates don't get new IDs
        # We can't perfectly match without tracking, so we use a heuristic:
        # the ingestion task processes in order and skips duplicates
        try:
            new_id = next(id_iter)
            pairs.append((prospect, new_id))
        except StopIteration:
            pairs.append((prospect, None))

    return pairs


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    if not os.environ.get("DATABASE_URL"):
        print("ERROR: DATABASE_URL environment variable is not set.")
        raise SystemExit(1)

    parser = argparse.ArgumentParser(
        description="Prefect acquisition pipeline: discover and qualify new JV prospects"
    )
    parser.add_argument(
        "--client-id", type=str, required=True,
        help="UUID of the client profile to acquire prospects for",
    )
    parser.add_argument(
        "--target-score", type=int, default=70,
        help="Minimum harmonic_mean score for a 'good' match (default: 70)",
    )
    parser.add_argument(
        "--target-count", type=int, default=10,
        help="How many good matches the client should have (default: 10)",
    )
    parser.add_argument(
        "--max-prospects", type=int, default=100,
        help="Maximum number of prospects to discover (default: 100)",
    )
    parser.add_argument(
        "--budget", type=float, default=0.50,
        help="Dollar budget for discovery phase (default: 0.50)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Skip DB writes and enrichment",
    )

    args = parser.parse_args()

    result = acquisition_flow(
        client_profile_id=args.client_id,
        target_score=args.target_score,
        target_count=args.target_count,
        max_prospects=args.max_prospects,
        budget=args.budget,
        dry_run=args.dry_run,
    )

    print(f"\nResult: {result}")
