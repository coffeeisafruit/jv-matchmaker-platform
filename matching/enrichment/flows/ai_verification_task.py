"""
L3 AI Verification Task — optional AI-powered quality checks.

Wraps the ClaudeVerificationService agents for deeper verification
when deterministic L1/L2 checks pass but confidence is borderline.

Only invoked when enable_ai_verification=True in the enrichment flow.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from prefect import task, get_run_logger


@dataclass
class AIVerificationResult:
    """Result from AI-powered verification."""

    profile_id: str
    passed: bool
    score: float  # 0-100
    issues: list[str] = field(default_factory=list)
    suggestions: list[str] = field(default_factory=list)


@task(name="ai-verify-profile", retries=1, retry_delay_seconds=15)
def ai_verify_profile(
    result: dict,
    verification_type: str = "formatting",
) -> AIVerificationResult:
    """Run a single AI verification agent on enriched profile data.

    Args:
        result: Enriched profile data dict.
        verification_type: One of 'formatting', 'content', 'data_quality'.

    Returns:
        AIVerificationResult with pass/fail, score, and issues.
    """
    logger = get_run_logger()
    profile_id = result.get("profile_id", "unknown")

    try:
        from matching.enrichment.ai_verification import ClaudeVerificationService

        service = ClaudeVerificationService(use_agents=True)

        # Build the profile text to verify
        profile_text = _build_profile_text(result)

        if verification_type == "formatting":
            verdict = service.verify_formatting(profile_text, result.get("name", ""))
        elif verification_type == "content":
            raw_content = result.get("_extraction_metadata", {}).get(
                "raw_content", ""
            )
            verdict = service.verify_content(profile_text, raw_content)
        elif verification_type == "data_quality":
            verdict = service.verify_data_quality(profile_text)
        else:
            logger.warning(f"Unknown verification type: {verification_type}")
            return AIVerificationResult(
                profile_id=profile_id, passed=True, score=100.0
            )

        return AIVerificationResult(
            profile_id=profile_id,
            passed=verdict.get("passed", True),
            score=verdict.get("score", 100.0),
            issues=verdict.get("issues", []),
            suggestions=verdict.get("suggestions", []),
        )

    except Exception as e:
        logger.warning(f"AI verification failed for {profile_id}: {e}")
        # Fail open — don't block enrichment on AI verification failure
        return AIVerificationResult(
            profile_id=profile_id,
            passed=True,
            score=50.0,
            issues=[f"AI verification unavailable: {e}"],
        )


@task(name="ai-verify-batch")
def ai_verify_batch(
    results: list[dict],
    verification_types: list[str] | None = None,
) -> list[AIVerificationResult]:
    """Run AI verification on a batch of enriched profiles.

    Args:
        results: List of enriched profile dicts.
        verification_types: Which checks to run. Defaults to ['formatting'].
    """
    logger = get_run_logger()

    if verification_types is None:
        verification_types = ["formatting"]

    all_results = []
    for result in results:
        # Run all requested verification types, aggregate
        profile_issues = []
        profile_suggestions = []
        min_score = 100.0
        all_passed = True

        for vtype in verification_types:
            vresult = ai_verify_profile.fn(result, verification_type=vtype)
            if not vresult.passed:
                all_passed = False
            min_score = min(min_score, vresult.score)
            profile_issues.extend(vresult.issues)
            profile_suggestions.extend(vresult.suggestions)

        all_results.append(
            AIVerificationResult(
                profile_id=result.get("profile_id", "unknown"),
                passed=all_passed,
                score=min_score,
                issues=profile_issues,
                suggestions=profile_suggestions,
            )
        )

    logger.info(
        f"AI verification: {sum(1 for r in all_results if r.passed)}"
        f"/{len(all_results)} passed"
    )
    return all_results


def _build_profile_text(result: dict) -> str:
    """Build a text representation of profile data for verification."""
    parts = []
    for field_name in (
        "what_you_do",
        "who_you_serve",
        "seeking",
        "offering",
        "bio",
        "social_proof",
        "signature_programs",
        "niche",
    ):
        value = result.get(field_name)
        if value and isinstance(value, str) and value.strip():
            parts.append(f"{field_name}: {value.strip()}")
    return "\n".join(parts)
