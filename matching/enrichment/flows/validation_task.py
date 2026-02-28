"""
Validation Task -- Prefect @task wrappers for the VerificationGate.

Decomposes the inline verification logic from consolidate_to_supabase_batch()
into composable Prefect tasks:

- validate_profile:  L1 deterministic + optional L2/L3 on a single profile
- validate_batch:    run validation across a list of enriched profiles
- write_quarantine:  persist quarantined profiles as JSONL for later retry
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from prefect import task, get_run_logger

from matching.enrichment import VerificationGate, GateStatus


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    """Outcome of running the VerificationGate on a single enriched profile."""

    profile_id: str
    status: str  # 'verified', 'unverified', 'quarantined'
    email: str | None  # Possibly fixed email
    confidence: float  # 0.0 to 1.0
    issues: list[str] = field(default_factory=list)
    failed_fields: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_STATUS_MAP: dict[GateStatus, str] = {
    GateStatus.VERIFIED: "verified",
    GateStatus.UNVERIFIED: "unverified",
    GateStatus.QUARANTINED: "quarantined",
}


def _collect_issues(verdict) -> list[str]:
    """Gather all per-field issue strings from a GateVerdict."""
    issues: list[str] = []
    for fv in verdict.field_verdicts.values():
        issues.extend(fv.issues)
    return issues


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="validate-profile", retries=0)
def validate_profile(
    result: dict,
    enable_ai_verification: bool = False,
) -> ValidationResult:
    """Run verification gate on a single enriched profile result.

    Uses VerificationGate from matching.enrichment for L1 deterministic
    checks (email format, domain validation, field sanity).  Optional L2/L3
    AI verification when *enable_ai_verification* is True.

    Args:
        result: Enriched profile dict.  Expected keys include ``profile_id``,
            ``email``, ``name``, ``company``, and optionally
            ``_extraction_metadata``.
        enable_ai_verification: When True the gate will attempt L3 AI
            verification via ClaudeVerificationService.

    Returns:
        ValidationResult summarising the gate verdict.
    """
    logger = get_run_logger()
    profile_id = result.get("profile_id", "unknown")

    gate = VerificationGate(enable_ai_verification=enable_ai_verification)

    # Build profile_data dict with the fields the gate inspects.
    profile_data: dict = {
        "email": result.get("email", ""),
        "name": result.get("name", ""),
        "company": result.get("company", ""),
    }

    # Forward optional text fields so L1 deterministic checks cover them.
    for text_field in (
        "website",
        "linkedin",
        "seeking",
        "offering",
        "who_you_serve",
        "what_you_do",
        "bio",
    ):
        value = result.get(text_field)
        if value:
            profile_data[text_field] = value

    verdict = gate.evaluate(
        data=profile_data,
        raw_content=None,
        extraction_metadata=result.get("_extraction_metadata"),
    )

    # Apply auto-fixes (e.g. missing https://, placeholder clearing).
    fixed_data = VerificationGate.apply_fixes(profile_data, verdict)
    fixed_email = fixed_data.get("email") or None

    status_str = _STATUS_MAP.get(verdict.status, "unverified")
    issues = _collect_issues(verdict)

    logger.info(
        f"validate-profile {profile_id}: {status_str} "
        f"(confidence={verdict.overall_confidence:.2f}, "
        f"issues={len(issues)})"
    )

    return ValidationResult(
        profile_id=profile_id,
        status=status_str,
        email=fixed_email,
        confidence=verdict.overall_confidence,
        issues=issues,
        failed_fields=list(verdict.failed_fields),
    )


@task(name="validate-batch")
def validate_batch(
    results: list[dict],
    enable_ai_verification: bool = False,
) -> list[ValidationResult]:
    """Validate a batch of enriched profiles.

    Iterates over *results* and runs :func:`validate_profile` for each one,
    then logs summary counts.

    Args:
        results: List of enriched profile dicts.
        enable_ai_verification: Forwarded to each per-profile gate instance.

    Returns:
        List of :class:`ValidationResult`, one per input profile.
    """
    logger = get_run_logger()

    validation_results: list[ValidationResult] = []
    for result in results:
        vr = validate_profile.fn(
            result,
            enable_ai_verification=enable_ai_verification,
        )
        validation_results.append(vr)

    # Summary counts
    counts: dict[str, int] = {"verified": 0, "unverified": 0, "quarantined": 0}
    for vr in validation_results:
        counts[vr.status] = counts.get(vr.status, 0) + 1

    logger.info(
        f"validate-batch complete: {len(validation_results)} profiles -- "
        f"verified={counts['verified']}, "
        f"unverified={counts['unverified']}, "
        f"quarantined={counts['quarantined']}"
    )

    return validation_results


@task(name="write-quarantine")
def write_quarantine(
    quarantined: list[ValidationResult],
    results: list[dict],
) -> str:
    """Write quarantined profiles to JSONL for later retry.

    Each line contains the original enriched profile dict augmented with
    quarantine metadata (issues, failed_fields, quarantined_at).

    Args:
        quarantined: Subset of :class:`ValidationResult` objects that were
            quarantined.
        results: The full list of enriched profile dicts (used to look up
            original data for each quarantined profile).

    Returns:
        Absolute path to the quarantine JSONL file.
    """
    logger = get_run_logger()

    if not quarantined:
        logger.info("write-quarantine: nothing to quarantine")
        return ""

    # Build a lookup from profile_id -> original result dict.
    results_by_id: dict[str, dict] = {
        r.get("profile_id", "unknown"): r for r in results
    }

    # Ensure output directory exists.
    quarantine_dir = Path("scripts/enrichment_batches/quarantine")
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    filename = f"quarantine_{datetime.now().strftime('%Y%m%d')}.jsonl"
    filepath = quarantine_dir / filename

    records_written = 0
    with open(filepath, "a", encoding="utf-8") as fh:
        for vr in quarantined:
            original = results_by_id.get(vr.profile_id, {})
            record = {
                "profile_id": vr.profile_id,
                "name": original.get("name", ""),
                "email": original.get("email", ""),
                "method": original.get("method", ""),
                "issues": vr.issues,
                "failed_fields": vr.failed_fields,
                "confidence": vr.confidence,
                "quarantined_at": datetime.now().isoformat(),
                "original_data": original,
            }
            fh.write(json.dumps(record, default=str) + "\n")
            records_written += 1

    abs_path = str(filepath.resolve())
    logger.info(
        f"write-quarantine: wrote {records_written} records to {abs_path}"
    )

    return abs_path
