"""
Prefect @task: Match gap detection for a client profile.

Analyzes a client's current match pool quality to identify gaps --
how many more high-scoring matches are needed, which niches are
underrepresented, and what the current score distribution looks like.

Used by the acquisition flow to decide whether new prospect discovery
is warranted and to shape search queries.
"""

from __future__ import annotations

import os
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor
from prefect import task, get_run_logger


# ---------------------------------------------------------------------------
# Database helper
# ---------------------------------------------------------------------------

def _get_db_connection() -> psycopg2.extensions.connection:
    """Create a new psycopg2 connection from the ``DATABASE_URL`` env var."""
    dsn = os.environ["DATABASE_URL"]
    return psycopg2.connect(dsn)


# ---------------------------------------------------------------------------
# SQL: fetch existing matches with scores for a client
# ---------------------------------------------------------------------------

_MATCH_SCORES_SQL = """
    SELECT
        ms.id,
        ms.suggested_profile_id,
        ms.harmonic_mean,
        ms.match_score,
        ms.status,
        p.name,
        p.niche,
        p.who_you_serve,
        p.what_you_do
    FROM match_suggestions ms
    JOIN profiles p ON p.id = ms.suggested_profile_id
    WHERE ms.profile_id = %s
      AND ms.status NOT IN ('dismissed')
    ORDER BY ms.harmonic_mean DESC NULLS LAST
"""

_CLIENT_PROFILE_SQL = """
    SELECT
        id, name, niche, who_you_serve, what_you_do, seeking,
        offering, audience_type, tags
    FROM profiles
    WHERE id = %s
"""

_ACTIVE_CLIENTS_SQL = """
    SELECT DISTINCT profile_id
    FROM match_suggestions
    WHERE status NOT IN ('dismissed')
"""


# ---------------------------------------------------------------------------
# Main gap detection task
# ---------------------------------------------------------------------------

@task(name="detect-match-gaps", retries=2, retry_delay_seconds=5)
def detect_match_gaps(
    client_profile_id: str,
    target_score: int = 70,
    target_count: int = 10,
) -> dict:
    """Detect gaps in a client's match pool.

    Queries existing SupabaseMatch (match_suggestions) records for this
    client, counts how many score >= target_score, and returns gap analysis.

    Parameters
    ----------
    client_profile_id:
        UUID of the client profile to analyze.
    target_score:
        Minimum harmonic_mean score to count as a "good" match (0-100 scale).
    target_count:
        How many good matches the client should have.

    Returns
    -------
    dict with:
        - client_id: str
        - client_name: str
        - current_count: int (matches at or above target_score)
        - target_count: int
        - gap: int (how many more needed)
        - has_gap: bool
        - lowest_score: float (lowest score in top N)
        - avg_score: float
        - niche_gaps: list[str] (niches underrepresented)
        - top_niches: list[str] (niches of current top matches)
    """
    logger = get_run_logger()
    conn = _get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Fetch the client profile for context
        cursor.execute(_CLIENT_PROFILE_SQL, (client_profile_id,))
        client_row = cursor.fetchone()
        if not client_row:
            logger.warning(
                "Client profile %s not found in database", client_profile_id
            )
            return {
                "client_id": client_profile_id,
                "client_name": "",
                "current_count": 0,
                "target_count": target_count,
                "gap": target_count,
                "has_gap": True,
                "lowest_score": 0.0,
                "avg_score": 0.0,
                "niche_gaps": [],
                "top_niches": [],
            }

        client_name = client_row.get("name", "")
        client_niche = client_row.get("niche", "")
        client_tags = client_row.get("tags") or []

        # Fetch all match scores for this client
        cursor.execute(_MATCH_SCORES_SQL, (client_profile_id,))
        matches = cursor.fetchall()

        logger.info(
            "Gap detection for %s (%s): %d total matches found",
            client_name, client_profile_id, len(matches),
        )

        # Calculate score statistics
        all_scores: list[float] = []
        qualifying_count = 0
        niche_counts: dict[str, int] = {}

        for match in matches:
            score = float(match.get("harmonic_mean") or 0)
            all_scores.append(score)

            if score >= target_score:
                qualifying_count += 1

            # Track niche distribution
            match_niche = (match.get("niche") or "").strip().lower()
            if match_niche:
                niche_counts[match_niche] = niche_counts.get(match_niche, 0) + 1

        # Sort scores descending for top-N analysis
        all_scores.sort(reverse=True)

        # Lowest score in the top target_count
        top_scores = all_scores[:target_count]
        lowest_score = top_scores[-1] if top_scores else 0.0
        avg_score = sum(top_scores) / len(top_scores) if top_scores else 0.0

        # Calculate gap
        gap = max(0, target_count - qualifying_count)

        # Identify niche gaps -- niches from client tags/niche that are
        # underrepresented in the match pool
        niche_gaps = _identify_niche_gaps(
            client_niche, client_tags, niche_counts
        )

        # Top niches currently represented
        top_niches = sorted(niche_counts, key=niche_counts.get, reverse=True)[:5]

        result = {
            "client_id": client_profile_id,
            "client_name": client_name,
            "current_count": qualifying_count,
            "target_count": target_count,
            "gap": gap,
            "has_gap": gap > 0,
            "lowest_score": round(lowest_score, 2),
            "avg_score": round(avg_score, 2),
            "niche_gaps": niche_gaps,
            "top_niches": top_niches,
        }

        logger.info(
            "Gap analysis for %s: %d/%d qualifying (gap=%d), avg=%.1f, lowest=%.1f",
            client_name, qualifying_count, target_count,
            gap, avg_score, lowest_score,
        )

        return result

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Batch gap detection
# ---------------------------------------------------------------------------

@task(name="detect-gaps-batch", retries=2, retry_delay_seconds=5)
def detect_gaps_batch(
    target_score: int = 70,
    target_count: int = 10,
) -> list[dict]:
    """Run gap detection for all active clients.

    Queries the match_suggestions table to find all distinct profile_ids
    (clients), then runs gap detection for each.

    Parameters
    ----------
    target_score:
        Minimum harmonic_mean score to count as a "good" match.
    target_count:
        How many good matches each client should have.

    Returns
    -------
    list[dict]
        List of gap analysis results, one per client, sorted by gap size
        descending (clients with the biggest gaps first).
    """
    logger = get_run_logger()
    conn = _get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(_ACTIVE_CLIENTS_SQL)
        rows = cursor.fetchall()
        client_ids = [str(row["profile_id"]) for row in rows]
        logger.info("Found %d active clients for batch gap detection", len(client_ids))
    finally:
        conn.close()

    results: list[dict] = []
    for client_id in client_ids:
        try:
            gap_result = detect_match_gaps.fn(
                client_profile_id=client_id,
                target_score=target_score,
                target_count=target_count,
            )
            results.append(gap_result)
        except Exception as exc:
            logger.error(
                "Gap detection failed for client %s: %s", client_id, exc
            )

    # Sort by gap size descending
    results.sort(key=lambda r: r.get("gap", 0), reverse=True)

    clients_with_gaps = sum(1 for r in results if r.get("has_gap"))
    logger.info(
        "Batch gap detection complete: %d/%d clients have gaps",
        clients_with_gaps, len(results),
    )

    return results


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _identify_niche_gaps(
    client_niche: str,
    client_tags: list[str],
    existing_niche_counts: dict[str, int],
) -> list[str]:
    """Identify niches that are underrepresented in the match pool.

    Compares the client's niche and tags against the niches of their
    existing matches to find gaps.

    Returns a list of niche strings that should be targeted in discovery.
    """
    gaps: list[str] = []

    # Normalize client niche and tags
    client_niches: set[str] = set()
    if client_niche:
        client_niches.add(client_niche.strip().lower())
    for tag in (client_tags or []):
        if tag and isinstance(tag, str):
            client_niches.add(tag.strip().lower())

    # Check which client niches are missing or underrepresented
    for niche in client_niches:
        count = existing_niche_counts.get(niche, 0)
        if count < 2:
            gaps.append(niche)

    return gaps
