"""
Prefect @flow + @task wiring for the 6-layer enrichment cascade.

Each layer is a Prefect @task, the orchestrator is a @flow.
Composable: layers=[1,2] or layers=[5,6] or full [1,2,3,4,5,6].

Usage:
    from matching.enrichment.flows.cascade_flow import enrichment_cascade_flow
    result = enrichment_cascade_flow(layers=[1,2], tier_filter={"C","D"}, limit=100)
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field, asdict
from typing import Optional

from prefect import flow, task, get_run_logger

from matching.enrichment.cascade.checkpoint import CascadeCheckpoint
from matching.enrichment.cascade.layer1_free_extraction import (
    Layer1FreeExtraction, Layer1Result,
)
from matching.enrichment.cascade.layer2_rescore_filter import (
    Layer2RescoreFilter, Layer2Result,
)
from matching.enrichment.cascade.layer3_gpu_enrichment import (
    Layer3GpuEnrichment, Layer3Result,
)
from matching.enrichment.cascade.layer4_claude_judge import (
    Layer4ClaudeJudge, Layer4Result,
)
from matching.enrichment.cascade.learning import (
    CascadeLearningLog, RunMetrics,
)


# ---------- Result dataclass ----------

@dataclass
class CascadeResult:
    """Aggregated result from all cascade layers."""

    layers_run: list = field(default_factory=list)
    l1: dict = field(default_factory=dict)
    l2: dict = field(default_factory=dict)
    l3: dict = field(default_factory=dict)
    l4: dict = field(default_factory=dict)
    l5: dict = field(default_factory=dict)
    l6: dict = field(default_factory=dict)
    total_cost: float = 0.0
    total_runtime: float = 0.0
    dry_run: bool = False


# ---------- Layer tasks ----------

@task(name="cascade-free-extraction", retries=1, retry_delay_seconds=30)
def free_extraction_task(
    tier_filter: set[str] | None,
    min_score: float,
    limit: int | None,
    dry_run: bool,
    checkpoint_id: str | None,
) -> dict:
    """Layer 1: Free HTTP extraction from websites."""
    logger = get_run_logger()
    logger.info("Starting Layer 1: Free Extraction")

    cp = CascadeCheckpoint(layer=1, run_id=checkpoint_id) if checkpoint_id else None
    layer = Layer1FreeExtraction(
        tier_filter=tier_filter,
        min_score=min_score,
        limit=limit,
        dry_run=dry_run,
        checkpoint=cp,
    )
    result = layer.run()

    logger.info(
        "Layer 1 done: %d found data, %d empty, %d errors",
        result.profiles_found_data, result.profiles_no_data, result.profiles_error,
    )

    return asdict(result)


@task(name="cascade-rescore-filter", retries=2, retry_delay_seconds=10)
def rescore_filter_task(
    affected_ids: list[str] | None,
    tier_filter: set[str] | None,
    threshold: float,
    limit: int | None,
    dry_run: bool,
) -> dict:
    """Layer 2: Rescore and filter by threshold."""
    logger = get_run_logger()
    logger.info("Starting Layer 2: Rescore + Filter (threshold=%.1f)", threshold)

    layer = Layer2RescoreFilter(
        threshold=threshold,
        tier_filter=tier_filter,
        limit=limit,
        dry_run=dry_run,
    )
    result = layer.run(affected_ids=affected_ids)

    logger.info(
        "Layer 2 done: %d rescored, %d qualified",
        result.profiles_rescored, result.qualified_count,
    )

    return asdict(result)


@task(name="cascade-gpu-enrichment", retries=1, retry_delay_seconds=60)
def gpu_enrichment_task(
    profile_ids: list[str] | None,
    tier_filter: set[str] | None,
    min_score: float,
    batch_size: int,
    limit: int | None,
    dry_run: bool,
    checkpoint_id: str | None,
) -> dict:
    """Layer 3: AI enrichment via configurable LLM endpoint."""
    logger = get_run_logger()
    logger.info("Starting Layer 3: GPU Enrichment")

    cp = CascadeCheckpoint(layer=3, run_id=checkpoint_id) if checkpoint_id else None
    layer = Layer3GpuEnrichment(
        tier_filter=tier_filter,
        min_score=min_score,
        batch_size=batch_size,
        limit=limit,
        dry_run=dry_run,
        checkpoint=cp,
    )
    result = layer.run(profile_ids=profile_ids)

    logger.info(
        "Layer 3 done: %d enriched, %d errors, $%.2f cost",
        result.profiles_enriched, result.profiles_error, result.total_cost,
    )

    return asdict(result)


@task(name="cascade-claude-judge", retries=2, retry_delay_seconds=30)
def claude_judge_task(
    enriched_ids: list[str],
    dry_run: bool,
) -> dict:
    """Layer 4: Claude-only conflict resolution."""
    logger = get_run_logger()
    logger.info("Starting Layer 4: Claude Judge (%d profiles)", len(enriched_ids))

    layer = Layer4ClaudeJudge(dry_run=dry_run)
    result = layer.run(enriched_ids=enriched_ids)

    logger.info(
        "Layer 4 done: %d conflicts found, %d resolved",
        result.conflicts_found, result.conflicts_resolved,
    )

    return asdict(result)


@task(name="cascade-cross-client-scoring", retries=2, retry_delay_seconds=30)
def cross_client_scoring_task(
    enriched_ids: list[str],
    score_threshold: int,
    dry_run: bool,
) -> dict:
    """Layer 5: Score enriched profiles against all active clients."""
    logger = get_run_logger()
    logger.info("Starting Layer 5: Cross-Client Scoring (%d profiles)", len(enriched_ids))

    if dry_run:
        logger.info("DRY RUN — skipping cross-client scoring")
        return {"new_matches": 0, "reports_flagged": 0, "dry_run": True}

    from matching.enrichment.flows.cross_client_scoring import (
        score_against_all_clients,
        flag_reports_for_update,
    )

    high_quality = score_against_all_clients.fn(
        profile_ids=enriched_ids,
        score_threshold=score_threshold,
    )

    flagged = 0
    if high_quality:
        flagged = flag_reports_for_update.fn(high_quality)

    logger.info(
        "Layer 5 done: %d high-quality matches, %d reports flagged",
        len(high_quality), flagged,
    )

    return {
        "new_matches": len(high_quality),
        "reports_flagged": flagged,
    }


@task(name="cascade-gap-acquisition", retries=1, retry_delay_seconds=60)
def gap_acquisition_task(
    score_threshold: int,
    buffer_target: int,
    dry_run: bool,
) -> dict:
    """Layer 6: Gap detection + targeted acquisition."""
    logger = get_run_logger()
    logger.info("Starting Layer 6: Gap Detection + Acquisition")

    from matching.enrichment.flows.gap_detection import detect_gaps_batch

    gaps = detect_gaps_batch.fn(
        target_score=score_threshold,
        target_count=buffer_target,
    )

    clients_with_gaps = [g for g in gaps if g.get("has_gap")]
    logger.info(
        "Layer 6: %d/%d clients have gaps",
        len(clients_with_gaps), len(gaps),
    )

    acquisitions = 0
    if clients_with_gaps and not dry_run:
        from matching.enrichment.flows.acquisition_flow import acquisition_flow

        for gap_result in clients_with_gaps[:10]:  # Cap at 10 acquisitions per run
            try:
                acq = acquisition_flow(
                    client_profile_id=gap_result["client_id"],
                    target_score=score_threshold,
                    target_count=buffer_target,
                    dry_run=dry_run,
                )
                if acq.saved_to_db > 0:
                    acquisitions += 1
            except Exception as e:
                logger.error(
                    "Acquisition failed for %s: %s",
                    gap_result.get("client_name", "?"), e,
                )
    elif dry_run:
        logger.info("DRY RUN — would trigger %d acquisitions", len(clients_with_gaps))

    return {
        "clients_checked": len(gaps),
        "clients_with_gaps": len(clients_with_gaps),
        "acquisitions_triggered": acquisitions,
    }


@task(name="cascade-record-learning", retries=1)
def record_learning_task(cascade_result: CascadeResult) -> list[dict]:
    """Record cascade metrics and apply learning actions."""
    logger = get_run_logger()
    logger.info("Recording cascade learning metrics")

    learning = CascadeLearningLog()

    # Build metrics from cascade result
    l1 = cascade_result.l1
    l2 = cascade_result.l2
    l3 = cascade_result.l3
    l4 = cascade_result.l4
    l5 = cascade_result.l5
    l6 = cascade_result.l6

    l1_attempted = l1.get("profiles_attempted", 0)
    metrics = RunMetrics(
        run_id=f"cascade_{int(time.time())}",
        model_used=l3.get("model_used", ""),
        l1_profiles_scraped=l1_attempted,
        l1_hit_rate=(
            l1.get("profiles_found_data", 0) / max(l1_attempted, 1) * 100
        ),
        l2_profiles_rescored=l2.get("profiles_rescored", 0),
        l2_qualified_count=l2.get("qualified_count", 0),
        l2_avg_score_delta=(
            l2.get("avg_score_after", 0) - l2.get("avg_score_before", 0)
        ),
        l3_profiles_enriched=l3.get("profiles_enriched", 0),
        l3_json_parse_rate=(
            l3.get("json_parse_success", 0)
            / max(l3.get("json_parse_success", 0) + l3.get("json_parse_fail", 0), 1)
            * 100
        ),
        l3_field_fill_rate=0,  # TODO: compute from fields_filled
        l3_cost=l3.get("total_cost", 0),
        l4_conflicts_found=l4.get("conflicts_found", 0),
        l4_conflicts_resolved=l4.get("conflicts_resolved", 0),
        l4_verdicts=l4.get("verdicts", {}),
        l5_new_matches=l5.get("new_matches", 0),
        l6_clients_with_gaps=l6.get("clients_with_gaps", 0),
        l6_acquisitions_triggered=l6.get("acquisitions_triggered", 0),
        total_cost=cascade_result.total_cost,
        total_runtime_seconds=cascade_result.total_runtime,
    )

    learning.record_run_metrics(metrics)

    # Apply learning actions
    actions = learning.apply_learning()
    if actions:
        logger.info("Learning actions: %d", len(actions))
        for a in actions:
            logger.info("  %s: %s", a.action_type, a.reason)

    return [asdict(a) for a in actions]


# ---------- Main flow ----------

@flow(
    name="enrichment-cascade",
    description="6-layer self-healing enrichment pipeline",
    retries=0,
    timeout_seconds=7200,
)
def enrichment_cascade_flow(
    layers: list[int] | None = None,
    tier_filter: set[str] | None = None,
    score_threshold: float = 50.0,
    match_threshold: int = 64,
    buffer_target: int = 30,
    batch_size: int = 1000,
    limit: int | None = None,
    dry_run: bool = False,
    checkpoint_id: str | None = None,
) -> CascadeResult:
    """6-layer self-healing enrichment pipeline.

    Layers:
        1: Free HTTP extraction (email, phone, social)
        2: Rescore + filter (jv_triage, threshold 50+)
        3: AI enrichment (configurable LLM via LLM_BASE_URL/LLM_MODEL)
        4: Claude conflict resolution (when AI ≠ existing)
        5: Cross-client scoring (score against all active clients)
        6: Gap detection + targeted acquisition (ensure 30+ matches)

    Composable: layers=[1,2] for free-only, layers=[3,4] for AI-only,
    or full [1,2,3,4,5,6] for complete pipeline.
    """
    logger = get_run_logger()
    start = time.time()

    if layers is None:
        layers = [1, 2, 3, 4, 5, 6]

    result = CascadeResult(layers_run=layers, dry_run=dry_run)

    logger.info(
        "Starting enrichment cascade: layers=%s, tiers=%s, threshold=%.1f, "
        "limit=%s, dry_run=%s",
        layers, tier_filter, score_threshold, limit, dry_run,
    )

    affected_ids: list[str] = []
    qualified_ids: list[str] = []
    enriched_ids: list[str] = []

    # --- Layer 1: Free Extraction ---
    if 1 in layers:
        l1_result = free_extraction_task(
            tier_filter=tier_filter,
            min_score=0,
            limit=limit,
            dry_run=dry_run,
            checkpoint_id=checkpoint_id,
        )
        result.l1 = l1_result
        affected_ids = l1_result.get("affected_ids", [])
        result.total_cost += 0  # Free

    # --- Layer 2: Rescore + Filter ---
    if 2 in layers:
        l2_result = rescore_filter_task(
            affected_ids=affected_ids if affected_ids else None,
            tier_filter=tier_filter,
            threshold=score_threshold,
            limit=limit,
            dry_run=dry_run,
        )
        result.l2 = l2_result
        qualified_ids = l2_result.get("qualified_ids", [])

    # --- Layer 3: GPU AI Enrichment ---
    if 3 in layers:
        l3_result = gpu_enrichment_task(
            profile_ids=qualified_ids if qualified_ids else None,
            tier_filter=tier_filter,
            min_score=score_threshold,
            batch_size=batch_size,
            limit=limit,
            dry_run=dry_run,
            checkpoint_id=checkpoint_id,
        )
        result.l3 = l3_result
        enriched_ids = l3_result.get("enriched_ids", [])
        result.total_cost += l3_result.get("total_cost", 0)

    # --- Layer 4: Claude Conflict Resolution ---
    if 4 in layers and enriched_ids:
        l4_result = claude_judge_task(
            enriched_ids=enriched_ids,
            dry_run=dry_run,
        )
        result.l4 = l4_result
        result.total_cost += l4_result.get("cost", 0)

    # --- Layer 5: Cross-Client Scoring ---
    if 5 in layers and enriched_ids:
        l5_result = cross_client_scoring_task(
            enriched_ids=enriched_ids,
            score_threshold=match_threshold,
            dry_run=dry_run,
        )
        result.l5 = l5_result

    # --- Layer 6: Gap Detection + Acquisition ---
    if 6 in layers:
        l6_result = gap_acquisition_task(
            score_threshold=match_threshold,
            buffer_target=buffer_target,
            dry_run=dry_run,
        )
        result.l6 = l6_result

    result.total_runtime = time.time() - start

    # --- Record learning (always, even for partial runs) ---
    try:
        record_learning_task(result)
    except Exception as e:
        logger.warning("Learning recording failed (non-fatal): %s", e)

    logger.info(
        "Cascade complete: layers=%s, cost=$%.2f, runtime=%.1fs",
        layers, result.total_cost, result.total_runtime,
    )

    return result
