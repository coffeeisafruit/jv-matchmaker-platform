"""
Prefect @task: Lightweight pre-scoring of discovered prospects.

Pre-scores prospects against a client profile using the ISMC framework
(SupabaseMatchScoringService) with partial data. This filters prospects
before the expensive enrichment step -- only those scoring >= threshold
on partial data will be enriched.

Design decisions:
  - Uses SupabaseMatchScoringService.score_pair() which handles null fields
    gracefully (redistributes weights away from null dimensions).
  - ALL prospects are returned (sorted by score), but each is annotated
    with _pre_score and _above_threshold so the caller can decide.
  - Pre-filter threshold default is 60 (on 0-100 scale) -- loose enough
    to catch potential matches with incomplete data.
"""

from __future__ import annotations

import os
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor
from prefect import task, get_run_logger


# ---------------------------------------------------------------------------
# Lightweight profile wrapper for the ISMC scorer
# ---------------------------------------------------------------------------

class _PartialProfile:
    """Lightweight proxy that satisfies SupabaseMatchScoringService's
    attribute access pattern without requiring a Django model instance.

    The ISMC scorer accesses profile attributes via getattr(), so we
    need an object that exposes the same interface. Missing fields
    return None, which the scorer handles via null-aware weight
    redistribution.
    """

    def __init__(self, data: dict):
        self._data = data

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        return self._data.get(name)

    def __repr__(self) -> str:
        return f"_PartialProfile({self._data.get('name', '?')})"


# ---------------------------------------------------------------------------
# SQL: load client profile for scoring
# ---------------------------------------------------------------------------

_CLIENT_PROFILE_SQL = """
    SELECT
        id, name, email, company, website, linkedin,
        niche, what_you_do, who_you_serve, seeking, offering,
        audience_type, booking_link, revenue_tier,
        list_size, social_reach, content_platforms,
        jv_history, current_projects, signature_programs,
        tags, bio, phone, business_size, network_role,
        audience_engagement_score,
        embedding_seeking, embedding_who_you_serve,
        embedding_offering, embedding_what_you_do
    FROM profiles
    WHERE id = %s
"""


def _load_client_profile(client_profile_id: str) -> dict | None:
    """Load full client profile from DB for scoring context."""
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
# Main pre-scoring task
# ---------------------------------------------------------------------------

@task(name="prescore-prospects")
def prescore_prospects(
    client_profile: dict,
    prospects: list[dict],
    threshold: int = 60,
) -> list[dict]:
    """Pre-score prospects against the client using lightweight ISMC.

    Uses SupabaseMatchScoringService.score_pair() with partial data.
    Each prospect gets augmented with:
        - _pre_score: float (harmonic mean, 0-100 scale)
        - _above_threshold: bool

    Parameters
    ----------
    client_profile:
        Client profile dict (must include at minimum: id, niche,
        seeking, who_you_serve, what_you_do).
    prospects:
        List of discovered prospect dicts from prospect_discovery.
    threshold:
        Minimum harmonic_mean score to be considered "above threshold".
        Default 60 -- intentionally loose for partial data.

    Returns
    -------
    list[dict]
        ALL prospects (sorted by score desc), not just above-threshold.
        Each prospect is annotated with _pre_score and _above_threshold.
    """
    logger = get_run_logger()

    if not prospects:
        logger.info("No prospects to pre-score")
        return []

    # Import the scorer
    try:
        from matching.services import SupabaseMatchScoringService
        scorer = SupabaseMatchScoringService()
    except ImportError:
        logger.error(
            "SupabaseMatchScoringService not available -- "
            "falling back to no-score passthrough"
        )
        for p in prospects:
            p["_pre_score"] = 0.0
            p["_above_threshold"] = False
        return prospects

    # Load full client profile from DB for richer scoring context
    client_id = str(client_profile.get("id", ""))
    db_client = None
    if client_id:
        try:
            db_client = _load_client_profile(client_id)
        except Exception as exc:
            logger.warning(
                "Could not load client profile from DB: %s", exc
            )

    # Build the client profile proxy
    client_data = db_client if db_client else client_profile
    client_proxy = _PartialProfile(client_data)

    # Score each prospect
    scored_count = 0
    above_count = 0
    error_count = 0

    for prospect in prospects:
        try:
            prospect_proxy = _PartialProfile(prospect)
            result = scorer.score_pair(client_proxy, prospect_proxy)

            harmonic = result.get("harmonic_mean", 0.0)
            prospect["_pre_score"] = round(harmonic, 2)
            prospect["_above_threshold"] = harmonic >= threshold
            prospect["_score_breakdown"] = {
                "score_ab": result.get("score_ab", 0.0),
                "score_ba": result.get("score_ba", 0.0),
                "harmonic_mean": round(harmonic, 2),
            }

            scored_count += 1
            if harmonic >= threshold:
                above_count += 1

        except Exception as exc:
            logger.warning(
                "Pre-scoring failed for prospect '%s': %s",
                prospect.get("name", "?"), exc,
            )
            prospect["_pre_score"] = 0.0
            prospect["_above_threshold"] = False
            error_count += 1

    # Sort by pre-score descending
    prospects.sort(key=lambda p: p.get("_pre_score", 0.0), reverse=True)

    logger.info(
        "Pre-scoring complete: %d scored, %d above threshold (%d), "
        "%d errors, threshold=%d",
        scored_count, above_count, threshold, error_count, threshold,
    )

    # Log top 5 for visibility
    for i, p in enumerate(prospects[:5]):
        logger.info(
            "  #%d: %s -- score=%.1f %s",
            i + 1,
            p.get("name", "?")[:40],
            p.get("_pre_score", 0.0),
            "(above)" if p.get("_above_threshold") else "(below)",
        )

    return prospects
