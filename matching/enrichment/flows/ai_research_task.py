"""
Prefect @task wrapping AI research logic for individual profiles.

Extracted from scripts/automated_enrichment_pipeline_safe.py _run_ai_research
(lines 979-1083). Provides a composable, retryable unit of work that can be
orchestrated by the enrichment_flow or called independently.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from prefect import task, get_run_logger

from matching.enrichment.ai_research import research_and_enrich_profile

try:
    from matching.enrichment.deep_research import deep_research_profile
except ImportError:
    deep_research_profile = None  # Module archived; deep-research fallback disabled

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Error-classification helpers (ported from the monolith)
# ---------------------------------------------------------------------------

_RATE_LIMIT_SIGNALS = ("rate limit", "429", "too many requests")

_PERMANENT_FAILURE_SIGNALS = (
    "not indexed",
    "not found",
    "invalid api key",
    "unauthorized",
    "forbidden",
    "api key",
    "invalid url",
    "bad request",
    "unprocessable",
)

# Default per-profile cost estimates when extraction metadata is unavailable
_COST_WITH_WEBSITE = 0.020
_COST_WITHOUT_WEBSITE = 0.025


def _is_rate_limit(error: Exception) -> bool:
    """Return True if the exception looks like an API rate-limit response."""
    msg = str(error).lower()
    return any(signal in msg for signal in _RATE_LIMIT_SIGNALS)


def _is_permanent_failure(error: Exception) -> bool:
    """Return True if the error is not worth retrying."""
    msg = str(error).lower()
    return any(signal in msg for signal in _PERMANENT_FAILURE_SIGNALS)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class AIResearchResult:
    """Outcome of a single AI-research invocation.

    Attributes:
        profile_id: Unique identifier for the profile (typically a UUID string).
        name: Human-readable partner name.
        enriched_data: The merged enrichment dict, or ``None`` on failure.
        success: Whether usable enrichment data was produced.
        cost: Estimated dollar cost for this invocation.
        source: Which research path produced the result —
            ``'exa_research'``, ``'ai_research'``, or ``'deep_research'``.
        error: Error description when ``success`` is False, else None.
    """

    profile_id: str
    name: str
    enriched_data: dict | None
    success: bool
    cost: float
    source: str  # 'exa_research', 'ai_research', 'deep_research'
    error: str | None = None


# ---------------------------------------------------------------------------
# Per-profile task
# ---------------------------------------------------------------------------

@task(name="ai-research-profile", retries=3, retry_delay_seconds=[2, 8, 30])
def ai_research_profile(
    profile: dict,
    refresh_mode: bool = False,
    skip_social_reach: bool = False,
    exa_only: bool = False,
    cache_only: bool = False,
) -> AIResearchResult:
    """Research and enrich a single partner profile via AI.

    Mirrors the retry / fallback logic of the monolithic pipeline's
    ``_run_ai_research`` method but delegates retry scheduling to Prefect's
    built-in retry machinery.

    Args:
        profile: Dict containing at minimum ``id`` and ``name``.  May also
            include ``website``, ``linkedin``, ``company``, ``_tier``, and
            any existing enrichment fields.
        refresh_mode: When True, always attempt fresh research even for
            tier-0 profiles (disables fill_only).
        skip_social_reach: Skip social-reach signals during extraction.
        exa_only: If True, skip the crawl4ai + Claude fallback when Exa
            cannot process the profile.
        cache_only: If True, only return cached results — do not call
            external APIs.

    Returns:
        An :class:`AIResearchResult` summarising the outcome.

    Raises:
        Exception: Re-raised rate-limit exceptions so that Prefect's retry
            policy can handle back-off automatically.
    """
    run_logger = get_run_logger()

    # -- Unpack profile fields ------------------------------------------------
    profile_id: str = str(profile.get("id", ""))
    name: str = profile.get("name", "")
    website: str = profile.get("website") or ""
    linkedin: str = profile.get("linkedin") or ""
    company: str = profile.get("company") or ""
    tier: int = profile.get("_tier", 0)

    # Tier 0 without refresh → only fill empty fields
    fill_only: bool = (tier == 0) and not refresh_mode

    # Build existing_data, stripping internal/meta keys
    existing_data: dict[str, Any] = {
        k: v
        for k, v in profile.items()
        if k not in ("id", "_tier", "enrichment_metadata") and v is not None
    }

    # -- Primary research call ------------------------------------------------
    try:
        enriched, was_researched = research_and_enrich_profile(
            name=name,
            website=website,
            existing_data=existing_data,
            use_cache=True,
            force_research=not cache_only,
            linkedin=linkedin,
            company=company,
            fill_only=fill_only,
            skip_social_reach=skip_social_reach,
            exa_only=exa_only,
        )

        # Determine enrichment source from extraction metadata
        ext_meta: dict = (enriched or {}).get("_extraction_metadata") or {}
        source = "ai_research" if ext_meta.get("source") == "ai_research" else "exa_research"

        if was_researched or cache_only:
            cost = _resolve_cost(enriched, has_website=bool(website))
            run_logger.info("AI research succeeded for %s (source=%s, cost=$%.3f)", name, source, cost)
            return AIResearchResult(
                profile_id=profile_id,
                name=name,
                enriched_data=enriched,
                success=True,
                cost=cost,
                source=source,
            )

        # -- Tier 4-5 deep-research fallback ----------------------------------
        if tier in (4, 5) and not was_researched and deep_research_profile is not None:
            run_logger.info("Attempting deep research fallback for %s (tier %d)", name, tier)
            try:
                enriched, was_researched = deep_research_profile(
                    name=name,
                    company=company,
                    existing_data=existing_data,
                    use_gpt_researcher=False,
                )
                if was_researched:
                    cost = _resolve_cost(enriched, has_website=bool(website))
                    run_logger.info("Deep research succeeded for %s (cost=$%.3f)", name, cost)
                    return AIResearchResult(
                        profile_id=profile_id,
                        name=name,
                        enriched_data=enriched,
                        success=True,
                        cost=cost,
                        source="deep_research",
                    )
            except Exception as deep_err:
                run_logger.warning("Deep research failed for %s: %s", name, deep_err)

        # No enrichment found and no error — not retryable
        run_logger.info("No enrichment data found for %s (tier %d)", name, tier)
        return AIResearchResult(
            profile_id=profile_id,
            name=name,
            enriched_data=None,
            success=False,
            cost=0.0,
            source="exa_research",
            error="No enrichment data returned",
        )

    except Exception as exc:
        # -- Rate-limit → re-raise so Prefect retries with back-off -----------
        if _is_rate_limit(exc):
            run_logger.warning("Rate limited for %s — deferring to Prefect retry", name)
            raise

        # -- Permanent failure → no point retrying ----------------------------
        if _is_permanent_failure(exc):
            run_logger.error("Permanent failure for %s (tier %d): %s", name, tier, exc)
            return AIResearchResult(
                profile_id=profile_id,
                name=name,
                enriched_data=None,
                success=False,
                cost=0.0,
                source="exa_research",
                error=str(exc),
            )

        # -- Transient failure → re-raise for Prefect retry -------------------
        run_logger.warning("Transient error for %s — deferring to Prefect retry: %s", name, exc)
        raise


# ---------------------------------------------------------------------------
# Batch convenience task
# ---------------------------------------------------------------------------

@task(name="ai-research-batch")
def ai_research_batch(
    profiles: list[dict],
    **kwargs: Any,
) -> list[AIResearchResult]:
    """Process a batch of profiles through AI research sequentially.

    Runs each profile through :func:`ai_research_profile` one at a time.
    For parallel execution, submit individual ``ai_research_profile`` tasks
    from the flow instead.

    Args:
        profiles: List of profile dicts (same shape accepted by
            :func:`ai_research_profile`).
        **kwargs: Forwarded to :func:`ai_research_profile` (e.g.
            ``refresh_mode``, ``exa_only``).

    Returns:
        List of :class:`AIResearchResult` in the same order as *profiles*.
    """
    run_logger = get_run_logger()
    results: list[AIResearchResult] = []

    for idx, profile in enumerate(profiles, start=1):
        name = profile.get("name", "<unknown>")
        run_logger.info("Processing profile %d/%d: %s", idx, len(profiles), name)
        result = ai_research_profile.fn(profile, **kwargs)
        results.append(result)

    succeeded = sum(1 for r in results if r.success)
    run_logger.info(
        "Batch complete: %d/%d succeeded (total cost $%.3f)",
        succeeded,
        len(results),
        sum(r.cost for r in results),
    )
    return results


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _resolve_cost(enriched: dict | None, *, has_website: bool) -> float:
    """Extract actual cost from extraction metadata, falling back to estimates.

    The Exa enrichment path embeds ``exa_cost`` inside
    ``_extraction_metadata`` when available.  Otherwise we fall back to the
    empirical estimates used by the original pipeline.
    """
    if enriched is not None:
        ext_meta = enriched.get("_extraction_metadata") or {}
        real_cost = ext_meta.get("exa_cost")
        if real_cost is not None:
            return float(real_cost)
    return _COST_WITH_WEBSITE if has_website else _COST_WITHOUT_WEBSITE
