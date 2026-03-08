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
from matching.enrichment.flows.db_prospect_search import search_database_prospects

CLIENT_BUDGET_CAP = 2.00  # Hard cap per client per acquisition run


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
    db_search_count: int = 0
    db_search_cost: float = 0.0
    discovery_cost: float = 0.0
    enrichment_cost: float = 0.0
    budget_cap_reached: bool = False
    new_matches_created: int = 0


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
    dsn = os.environ.get("DIRECT_DATABASE_URL") or os.environ["DATABASE_URL"]
    conn = psycopg2.connect(dsn)
    conn.cursor().execute("SET statement_timeout = 0")
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(_CLIENT_PROFILE_SQL, (client_profile_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _filter_needs_enrichment(
    profile_ids: list[str],
    min_jvr: float = 50.0,
    max_stale_days: int = 30,
) -> tuple[list[str], int]:
    """Return profile IDs that actually need enrichment.

    Profiles are skipped if they have:
      - jv_readiness_score >= min_jvr (50 = "matchable"), AND
      - last_enriched_at within max_stale_days

    Returns (ids_needing_enrichment, skipped_count).
    """
    if not profile_ids:
        return [], 0

    dsn = os.environ.get("DIRECT_DATABASE_URL") or os.environ["DATABASE_URL"]
    conn = psycopg2.connect(dsn)
    conn.cursor().execute("SET statement_timeout = 0")
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            SELECT id, jv_readiness_score, last_enriched_at
            FROM profiles
            WHERE id = ANY(%s::uuid[])
            """,
            (profile_ids,),
        )
        rows = {str(r["id"]): r for r in cursor.fetchall()}
    finally:
        conn.close()

    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(days=max_stale_days)
    needs = []
    skipped = 0

    for pid in profile_ids:
        row = rows.get(pid)
        if not row:
            needs.append(pid)  # Not found in DB — enrich to be safe
            continue

        jvr = float(row.get("jv_readiness_score") or 0)
        enriched_at = row.get("last_enriched_at")

        # Normalize naive timestamps to UTC for comparison
        if enriched_at and enriched_at.tzinfo is None:
            enriched_at = enriched_at.replace(tzinfo=timezone.utc)

        # Skip if JVR is high enough AND recently enriched
        if jvr >= min_jvr and enriched_at and enriched_at >= cutoff:
            skipped += 1
        else:
            needs.append(pid)

    return needs, skipped


def _log_budget_alert(client_id: str, client_name: str, total_cost: float):
    """Log a budget cap alert to the cascade learning log."""
    try:
        from matching.enrichment.cascade.learning import CascadeLearningLog, Alert
        log = CascadeLearningLog()
        log.record_action(Alert(
            severity="warning",
            message=f"Budget cap ${CLIENT_BUDGET_CAP:.2f} reached for {client_name} "
                    f"(id={client_id}). Total cost: ${total_cost:.2f}",
        ))
    except Exception:
        pass  # alerting should never block the pipeline


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
    # Step 2.5: Build ideal partner profile (data-driven learning)
    # ------------------------------------------------------------------
    ideal_partner = None
    try:
        from matching.enrichment.cascade.partner_pipeline import PartnerPipeline
        pipeline = PartnerPipeline()
        ideal_partner = pipeline.build_ideal_partner_profile(client_profile_id)
        logger.info(
            "Ideal partner profile: learning_level=%s, match_count=%d, "
            "engagement_weighted=%s",
            ideal_partner.learning_level,
            ideal_partner.match_count,
            ideal_partner.engagement_weighted,
        )
    except Exception as exc:
        logger.warning("Failed to build ideal partner profile: %s", exc)

    # ------------------------------------------------------------------
    # Step 3a: Search own database first ($0 cost)
    # ------------------------------------------------------------------
    rotation_ids = []
    try:
        from matching.enrichment.cascade.partner_pipeline import PartnerPipeline
        rotation_ids = list(PartnerPipeline().get_rotation_filter(client_profile_id))
    except Exception:
        pass

    if not dry_run:
        logger.info("Searching own database first (gap=%d)", result.gap_detected)
        try:
            db_prospects = search_database_prospects(
                client_profile=client_profile,
                ideal_partner=ideal_partner,
                exclude_ids=rotation_ids,
                max_results=max_prospects,
                min_readiness_score=20.0,
                max_staleness_days=180,
            )
        except Exception as exc:
            logger.warning("DB prospect search failed: %s", exc)
            db_prospects = []
        result.db_search_count = len(db_prospects)
        logger.info("DB search: %d prospects found ($0.00)", len(db_prospects))
    else:
        db_prospects = []
        logger.info("DRY RUN -- skipping DB search")

    # ------------------------------------------------------------------
    # Step 3b: Pre-score DB prospects to gauge quality before external decision
    # ------------------------------------------------------------------
    db_candidates = [p for p in db_prospects if p.get("_db_profile_id")]
    if db_candidates:
        logger.info(
            "Pre-scoring %d DB-sourced prospects at threshold 64",
            len(db_candidates),
        )
        scored_db = prescore_prospects(
            client_profile=client_profile,
            prospects=db_candidates,
            threshold=64,
        )
        db_above = [p for p in scored_db if p.get("_above_threshold")]
        db_borderline = [
            p for p in scored_db
            if not p.get("_above_threshold")
            and (p.get("_pre_score") or 0) >= 55
        ]
    else:
        scored_db = []
        db_above = []
        db_borderline = []

    db_quality_count = len(db_above) + len(db_borderline)

    logger.info(
        "DB pre-score: %d above 64, %d borderline (55-63), %d below 55",
        len(db_above), len(db_borderline),
        len(db_candidates) - len(db_above) - len(db_borderline),
    )

    # ------------------------------------------------------------------
    # Step 3c: Adjust external budget based on quality-adjusted DB results
    # ------------------------------------------------------------------
    # Use quality count (above + borderline), not raw total, to decide
    # whether external discovery is needed.
    external_budget = min(budget, CLIENT_BUDGET_CAP)
    if db_quality_count >= result.gap_detected * 2:
        external_budget *= 0.25
        logger.info("DB quality 2x gap — external budget reduced to $%.2f", external_budget)
    elif db_quality_count >= result.gap_detected:
        external_budget *= 0.50
        logger.info("DB quality 1x gap — external budget reduced to $%.2f", external_budget)
    else:
        logger.info(
            "DB quality (%d) < gap (%d) — full external budget $%.2f",
            db_quality_count, result.gap_detected, external_budget,
        )

    # ------------------------------------------------------------------
    # Step 3d: External discovery (Exa + Apollo)
    # ------------------------------------------------------------------
    # remaining_slots is based on how many MORE we need, not raw DB count.
    # If DB found 20 but only 5 are quality, we still need external help.
    remaining_need = max(0, result.gap_detected - db_quality_count)
    remaining_slots = min(remaining_need, max_prospects - len(db_prospects))
    if remaining_slots > 0 and external_budget > 0.05:
        logger.info(
            "Discovering external prospects (max=%d, budget=$%.2f)",
            remaining_slots, external_budget,
        )
        external_prospects = discover_prospects(
            client_profile=client_profile,
            gap_analysis=gap_analysis,
            max_results=remaining_slots,
            budget=external_budget,
            ideal_partner=ideal_partner,
        )
        result.discovery_cost = sum(
            p.get("discovery_cost", 0) for p in external_prospects
        )
    else:
        external_prospects = []
        logger.info("Skipping external discovery — DB filled enough slots")

    # Combine DB + external prospects
    all_prospects = db_prospects + external_prospects
    result.total_discovered = len(all_prospects)
    result.cost = result.discovery_cost

    if not all_prospects:
        logger.info("No prospects discovered -- done")
        result.skipped_reason = "no_prospects_found"
        result.runtime_seconds = time.time() - start_time
        return result

    logger.info(
        "Total discovered: %d (%d DB + %d external, $%.3f cost)",
        len(all_prospects), len(db_prospects), len(external_prospects),
        result.cost,
    )

    # Budget cap check
    if result.cost >= CLIENT_BUDGET_CAP:
        result.budget_cap_reached = True
        _log_budget_alert(client_profile_id, result.client_name, result.cost)
        logger.warning("Budget cap reached ($%.2f >= $%.2f)", result.cost, CLIENT_BUDGET_CAP)

    # ------------------------------------------------------------------
    # Step 4: Separate external prospects (DB already prescored in 3b)
    # ------------------------------------------------------------------
    # External prospects skip prescoring — intelligent targeting is the
    # filter.  They go straight to enrichment for full ISMC scoring.
    external_candidates = [p for p in all_prospects if not p.get("_db_profile_id")]

    result.above_threshold = len(db_above) + len(db_borderline) + len(external_candidates)

    logger.info(
        "Quality candidates: %d DB above 64, %d DB borderline (55-63), "
        "%d external (skip prescore) = %d total",
        len(db_above), len(db_borderline), len(external_candidates),
        result.above_threshold,
    )

    # ------------------------------------------------------------------
    # Step 5: Ingest external prospects to database
    # ------------------------------------------------------------------
    # DB-sourced prospects already exist; only external ones need ingestion.
    new_prospects = external_candidates

    if dry_run:
        logger.info("DRY RUN -- skipping DB ingestion")
        ingestion_result = {
            "new_ids": [],
            "duplicate_count": 0,
            "total_saved": 0,
            "errors": 0,
        }
    elif new_prospects:
        logger.info(
            "Ingesting %d new prospects to database (skipping %d DB-sourced)",
            len(new_prospects), len(scored_db),
        )
        ingestion_result = ingest_prospects(
            prospects=new_prospects,
            source="acquisition",
        )
    else:
        ingestion_result = {
            "new_ids": [],
            "duplicate_count": 0,
            "total_saved": 0,
            "errors": 0,
        }

    result.saved_to_db = ingestion_result.get("total_saved", 0)
    result.duplicates = ingestion_result.get("duplicate_count", 0)
    new_ids = ingestion_result.get("new_ids", [])

    logger.info(
        "Ingestion: %d new, %d duplicates, %d DB-sourced (skipped)",
        result.saved_to_db, result.duplicates, len(scored_db),
    )

    # ------------------------------------------------------------------
    # Step 6: Feed prospects into enrichment pipeline
    # ------------------------------------------------------------------
    # Three paths to enrichment:
    #   - External prospects: ALL get enriched (they're new, no data yet)
    #   - DB above 64: enrichment gate (JVR 50+ & recent → skip)
    #   - DB borderline 55-63: enrichment gate (same check — these might
    #     jump above 64 with better data)
    #
    # JVR gate: skip if JVR >= 50 AND enriched within 30 days.
    # JVR 50+ = "matchable" — enough data to be in our DB.
    # Profiles below JVR 50 need enrichment to become matchable.
    # Borderline profiles (55-63) are close — enrichment could push
    # them over 64 by filling in sparse offering/seeking/who_you_serve.
    enrich_ids = []

    # External: all newly-ingested prospects get enriched
    for prospect, new_id in _match_prospects_to_ids(new_prospects, new_ids):
        if new_id:
            enrich_ids.append(new_id)
    ext_count = len(enrich_ids)

    # DB-sourced: above 64 + borderline 55-63, both go through JVR gate
    db_enrich_candidates = db_above + db_borderline
    db_candidate_ids = [
        p["_db_profile_id"] for p in db_enrich_candidates
        if p.get("_db_profile_id")
    ]
    if db_candidate_ids:
        enrich_ids_db, skipped_db = _filter_needs_enrichment(
            db_candidate_ids, min_jvr=50, max_stale_days=30,
        )
        enrich_ids.extend(enrich_ids_db)
        logger.info(
            "Enrichment gate: %d/%d DB candidates need enrichment "
            "(%d above-64 + %d borderline 55-63), "
            "%d skipped (JVR >= 50 + enriched within 30 days)",
            len(enrich_ids_db), len(db_candidate_ids),
            len(db_above), len(db_borderline), skipped_db,
        )
    else:
        enrich_ids_db = []

    if enrich_ids and not dry_run and not result.budget_cap_reached:
        remaining_budget = CLIENT_BUDGET_CAP - result.cost
        if remaining_budget > 0.10:
            logger.info(
                "Triggering enrichment for %d prospects "
                "(%d external + %d DB needing enrichment, "
                "remaining budget: $%.2f)",
                len(enrich_ids), ext_count, len(enrich_ids_db),
                remaining_budget,
            )
            try:
                from matching.enrichment.flows.enrichment_flow import enrichment_flow

                # refresh_mode=True: if we're paying for enrichment,
                # new data should overwrite same-priority stale data.
                enrichment_result = enrichment_flow(
                    profile_ids=enrich_ids,
                    refresh_mode=True,
                    dry_run=dry_run,
                    enrichment_context="acquisition",
                )
                result.enriched = enrichment_result.profiles_researched
                result.enrichment_cost = enrichment_result.total_cost
                result.cost += result.enrichment_cost

                logger.info(
                    "Enrichment complete: %d profiles researched, $%.3f cost",
                    result.enriched, result.enrichment_cost,
                )

                # Check budget after enrichment
                if result.cost >= CLIENT_BUDGET_CAP:
                    result.budget_cap_reached = True
                    _log_budget_alert(
                        client_profile_id, result.client_name, result.cost,
                    )
            except Exception as exc:
                logger.error("Enrichment flow failed: %s", exc)
                result.enriched = 0
        else:
            logger.warning(
                "Skipping enrichment — remaining budget too low ($%.2f)",
                remaining_budget,
            )
    elif dry_run:
        logger.info(
            "DRY RUN -- would enrich %d prospects",
            len(enrich_ids),
        )
    elif result.budget_cap_reached:
        logger.warning("Budget cap reached — skipping enrichment")
    else:
        logger.info("No prospects to enrich")

    # ------------------------------------------------------------------
    # Step 7: Score enriched profiles against all active clients
    # ------------------------------------------------------------------
    if enrich_ids and not dry_run and not result.budget_cap_reached:
        try:
            from matching.enrichment.flows.cross_client_scoring import (
                score_against_all_clients,
                flag_reports_for_update,
            )
            high_quality = score_against_all_clients(
                profile_ids=enrich_ids,
                score_threshold=64,
            )
            result.new_matches_created = len(high_quality)
            if high_quality:
                flag_reports_for_update(high_quality)
            logger.info(
                "Scoring complete: %d high-quality matches created (>= 64)",
                result.new_matches_created,
            )
        except Exception as exc:
            logger.error("Cross-client scoring failed: %s", exc)

    # ------------------------------------------------------------------
    # Done
    # ------------------------------------------------------------------
    result.runtime_seconds = time.time() - start_time

    logger.info(
        "Acquisition complete for %s: "
        "%d discovered, %d saved, %d duplicates, %d above threshold, "
        "%d enriched, %d new matches, $%.3f cost, %.1fs runtime",
        result.client_name,
        result.total_discovered,
        result.saved_to_db,
        result.duplicates,
        result.above_threshold,
        result.enriched,
        result.new_matches_created,
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
