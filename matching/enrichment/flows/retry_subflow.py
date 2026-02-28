"""
Retry subflow — auto-retry quarantined profiles.

Reads quarantine JSONL files, classifies failures using the existing
RetryStrategySelector, and re-runs enrichment with adaptive strategies.

Usage:
    from matching.enrichment.flows.retry_subflow import retry_quarantined_flow
    retry_quarantined_flow(quarantine_file="scripts/enrichment_batches/quarantine/quarantine_20260227.jsonl")
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from prefect import flow, task, get_run_logger

from matching.enrichment.retry_strategy import (
    FailureClassifier,
    FieldFailure,
    RetryStrategySelector,
    RetryPlan,
    LearningLog,
)
from matching.enrichment.flows.ai_research_task import (
    ai_research_profile,
    AIResearchResult,
)
from matching.enrichment.flows.validation_task import (
    validate_profile,
    ValidationResult,
)
from matching.enrichment.flows.consolidation_task import consolidate_to_db


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class RetryResult:
    """Summary of a retry subflow run."""

    profiles_loaded: int = 0
    profiles_retried: int = 0
    profiles_recovered: int = 0
    still_quarantined: int = 0
    profiles_written: int = 0


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="load-quarantine-file")
def load_quarantine_file(quarantine_file: str) -> list[dict]:
    """Load quarantined profiles from a JSONL file.

    Returns a list of quarantine records, each containing at minimum
    ``profile_id``, ``issues``, ``failed_fields``, and ``original_data``.
    """
    logger = get_run_logger()
    filepath = Path(quarantine_file)

    if not filepath.exists():
        logger.warning("Quarantine file not found: %s", quarantine_file)
        return []

    records: list[dict] = []
    with open(filepath, "r", encoding="utf-8") as fh:
        for line_num, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                records.append(record)
            except json.JSONDecodeError as exc:
                logger.warning(
                    "Skipping malformed line %d in %s: %s",
                    line_num, quarantine_file, exc,
                )

    logger.info("Loaded %d quarantine records from %s", len(records), quarantine_file)
    return records


@task(name="classify-failures")
def classify_failures(records: list[dict]) -> list[dict]:
    """Classify quarantine failures and attach retry strategies.

    Each record gets augmented with:
    - ``_failure_types``: list of FailureType strings
    - ``_retry_plan``: the selected RetryPlan
    """
    logger = get_run_logger()
    selector = RetryStrategySelector()

    for record in records:
        failures: list[FieldFailure] = []
        for field_name in record.get("failed_fields", []):
            # Simplified classification from issues list
            issues = record.get("issues", [])
            failure = FieldFailure(
                field_name=field_name,
                failure_type="validation_failed",
                original_value=None,
                issues=[i for i in issues if field_name.lower() in i.lower()],
            )
            failures.append(failure)

        record["_failure_types"] = [f.failure_type for f in failures]

        # Get retry plan from strategy selector
        if failures:
            plan = selector.select_strategy(failures[0])
            record["_retry_plan"] = {
                "methods": plan.methods,
                "max_attempts": plan.max_attempts,
            }
        else:
            record["_retry_plan"] = {"methods": ["exa_research"], "max_attempts": 1}

    logger.info("Classified %d quarantine records", len(records))
    return records


@task(name="rebuild-profile-for-retry")
def rebuild_profile_for_retry(record: dict) -> dict:
    """Reconstruct a profile dict from a quarantine record for re-enrichment."""
    original = record.get("original_data", {})
    return {
        "id": record.get("profile_id", original.get("profile_id", "")),
        "name": record.get("name", original.get("name", "")),
        "email": None,  # Clear email — we're retrying because it failed
        "company": original.get("company", ""),
        "website": original.get("website", original.get("discovered_website", "")),
        "linkedin": original.get("linkedin", original.get("discovered_linkedin", "")),
        "list_size": original.get("list_size", 0),
        "_tier": original.get("_tier", 3),
        "enrichment_metadata": original.get("enrichment_metadata"),
    }


# ---------------------------------------------------------------------------
# Main retry flow
# ---------------------------------------------------------------------------

@flow(
    name="retry-quarantined",
    description="Auto-retry quarantined profiles with adaptive strategies",
    retries=0,
)
def retry_quarantined_flow(
    quarantine_file: str = "",
    quarantine_dir: str = "scripts/enrichment_batches/quarantine",
    max_retries_per_profile: int = 2,
    dry_run: bool = False,
) -> RetryResult:
    """Retry quarantined profiles from JSONL files.

    Args:
        quarantine_file: Path to a specific quarantine file. If empty,
            processes the most recent file in quarantine_dir.
        quarantine_dir: Directory containing quarantine JSONL files.
        max_retries_per_profile: Max retry attempts per profile.
        dry_run: If True, skip DB writes.

    Returns:
        RetryResult with counts of recovered vs still-quarantined profiles.
    """
    logger = get_run_logger()
    result = RetryResult()

    # Resolve quarantine file
    if not quarantine_file:
        qdir = Path(quarantine_dir)
        if not qdir.exists():
            logger.info("No quarantine directory found at %s", quarantine_dir)
            return result
        files = sorted(qdir.glob("quarantine_*.jsonl"), reverse=True)
        if not files:
            logger.info("No quarantine files found in %s", quarantine_dir)
            return result
        quarantine_file = str(files[0])
        logger.info("Using most recent quarantine file: %s", quarantine_file)

    # Load and classify
    records = load_quarantine_file(quarantine_file)
    result.profiles_loaded = len(records)
    if not records:
        return result

    classified = classify_failures(records)

    # Retry each profile
    recovered: list[dict] = []
    still_failed: list[dict] = []

    for record in classified:
        profile = rebuild_profile_for_retry(record)
        retry_plan = record.get("_retry_plan", {})
        max_attempts = min(
            retry_plan.get("max_attempts", 1),
            max_retries_per_profile,
        )

        profile_recovered = False
        for attempt in range(1, max_attempts + 1):
            logger.info(
                "Retrying %s (attempt %d/%d)",
                profile.get("name", "?"), attempt, max_attempts,
            )

            # Re-run AI research
            research_result = ai_research_profile(
                profile,
                refresh_mode=True,  # Force fresh research
            )

            if not research_result.success:
                continue

            # Build enriched result dict for validation
            enriched = {
                "profile_id": str(profile["id"]),
                "name": profile.get("name", ""),
                "company": profile.get("company", ""),
                "email": profile.get("email"),
                "enriched_at": datetime.now().isoformat(),
                "_tier": profile.get("_tier", 0),
                "enrichment_metadata": profile.get("enrichment_metadata"),
            }
            if research_result.enriched_data:
                enriched.update({
                    k: v for k, v in research_result.enriched_data.items()
                    if not k.startswith("_") and v is not None
                })

            # Re-validate
            validation = validate_profile(enriched)

            if validation.status != "quarantined":
                logger.info(
                    "Profile %s recovered (status=%s)",
                    profile.get("name", "?"), validation.status,
                )
                recovered.append(enriched)
                profile_recovered = True
                break

        if not profile_recovered:
            still_failed.append(record)

        result.profiles_retried += 1

    result.profiles_recovered = len(recovered)
    result.still_quarantined = len(still_failed)

    # Write recovered profiles to DB
    if recovered and not dry_run:
        logger.info("Writing %d recovered profiles to database", len(recovered))
        write_stats = consolidate_to_db(recovered, refresh_mode=True)
        result.profiles_written = write_stats.get("profiles_updated", 0)

    # Log outcomes to learning log for strategy adaptation
    _log_retry_outcomes(classified, recovered, still_failed)

    logger.info(
        "Retry complete: %d loaded, %d retried, %d recovered, %d still quarantined",
        result.profiles_loaded,
        result.profiles_retried,
        result.profiles_recovered,
        result.still_quarantined,
    )
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _log_retry_outcomes(
    classified: list[dict],
    recovered: list[dict],
    still_failed: list[dict],
) -> None:
    """Record retry outcomes in the LearningLog for strategy adaptation."""
    try:
        learning_log = LearningLog()
        recovered_ids = {r.get("profile_id") for r in recovered}

        for record in classified:
            pid = record.get("profile_id", "")
            success = pid in recovered_ids
            learning_log.record(
                profile_id=pid,
                field="retry",
                strategy=str(record.get("_retry_plan", {}).get("methods", [])),
                failure_type=str(record.get("_failure_types", [])),
                success=success,
            )
    except Exception:
        pass  # Learning log is best-effort
