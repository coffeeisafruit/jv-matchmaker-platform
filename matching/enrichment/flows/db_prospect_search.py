"""
Prefect @task: Database-first prospect search using pgvector + text fallback.

Searches the existing 1.1M+ profiles before paying for external APIs.

Strategy:
  1. Vector search (~3K profiles with embeddings) — pgvector cosine similarity
  2. Text search (remaining profiles) — ILIKE keyword matching
  3. Combine, deduplicate, return with _db_profile_id marker

Cost: $0.00 per search — all local DB queries.

Usage:
    from matching.enrichment.flows.db_prospect_search import search_database_prospects

    results = search_database_prospects(
        client_profile=client_dict,
        ideal_partner=ideal_partner_profile,
        exclude_ids=[...],
    )
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import psycopg2
import psycopg2.extras

from prefect import task, get_run_logger

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQL: Vector similarity search (profiles WITH embeddings)
# ---------------------------------------------------------------------------

_VECTOR_SEARCH_SQL = """
    SELECT p.id, p.name, p.website, p.linkedin, p.email, p.niche,
           p.what_you_do, p.offering, p.seeking, p.who_you_serve,
           p.jv_tier, p.jv_readiness_score, p.revenue_tier,
           p.content_platforms, p.network_role,
           1 - (p.{embed_col} <=> %s::vector) AS similarity
    FROM profiles p
    WHERE p.{embed_col} IS NOT NULL
      AND p.jv_tier IN ('A', 'B', 'C')
      AND p.jv_readiness_score >= %s
      AND p.id != %s
      AND p.id != ALL(%s::uuid[])
      AND p.id NOT IN (
          SELECT suggested_profile_id FROM match_suggestions
          WHERE profile_id = %s
      )
      AND COALESCE(p.last_enriched_at, p.created_at) >= NOW() - INTERVAL '%s days'
    ORDER BY p.{embed_col} <=> %s::vector
    LIMIT %s
"""


# ---------------------------------------------------------------------------
# SQL: Text keyword search (profiles WITHOUT embeddings)
# ---------------------------------------------------------------------------

_TEXT_SEARCH_SQL = """
    SELECT p.id, p.name, p.website, p.linkedin, p.email, p.niche,
           p.what_you_do, p.offering, p.seeking, p.who_you_serve,
           p.jv_tier, p.jv_readiness_score, p.revenue_tier,
           p.content_platforms, p.network_role,
           0.0 AS similarity
    FROM profiles p
    WHERE p.embedding_offering IS NULL
      AND p.jv_tier IN ('A', 'B', 'C')
      AND p.jv_readiness_score >= %s
      AND p.id != %s
      AND p.id != ALL(%s::uuid[])
      AND p.id NOT IN (
          SELECT suggested_profile_id FROM match_suggestions
          WHERE profile_id = %s
      )
      AND COALESCE(p.last_enriched_at, p.created_at) >= NOW() - INTERVAL '%s days'
      AND ({keyword_clauses})
    ORDER BY p.jv_readiness_score DESC
    LIMIT %s
"""


def _get_conn():
    dsn = os.environ.get("DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL", "")
    return psycopg2.connect(dsn, options="-c statement_timeout=30000")


def _embed_text(text: str) -> Optional[list[float]]:
    """Generate a 1024-dim embedding for a text string."""
    try:
        from lib.enrichment.hf_client import HFClient
        client = HFClient()
        return client.embed(text.strip())
    except Exception as e:
        logger.warning("Embedding generation failed: %s", e)
        return None


def _format_vector(vec: list[float]) -> str:
    """Format a vector as pgvector literal: '[0.1,0.2,...]'."""
    return "[" + ",".join(str(v) for v in vec) + "]"


# ---------------------------------------------------------------------------
# Vector search
# ---------------------------------------------------------------------------

def _vector_search(
    cur,
    client_id: str,
    query_text: str,
    embed_col: str,
    exclude_ids: list[str],
    min_readiness_score: float,
    max_staleness_days: int,
    limit: int,
) -> list[dict]:
    """Search profiles using pgvector cosine similarity."""
    embedding = _embed_text(query_text)
    if not embedding:
        return []

    vec_str = _format_vector(embedding)
    sql = _VECTOR_SEARCH_SQL.format(embed_col=embed_col)

    cur.execute(sql, (
        vec_str,                  # embedding comparison
        min_readiness_score,      # jv_readiness_score filter
        client_id,                # exclude self
        exclude_ids or [],        # exclude already-seen IDs
        client_id,                # exclude existing match_suggestions
        max_staleness_days,       # staleness window
        vec_str,                  # ORDER BY clause
        limit,                    # LIMIT
    ))
    return [dict(row) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Text keyword search
# ---------------------------------------------------------------------------

def _build_keyword_clauses(keywords: list[str], max_keywords: int = 8) -> tuple[str, list]:
    """Build ILIKE clauses for keyword text search.

    Returns (sql_fragment, params) where sql_fragment uses %s placeholders.
    """
    keywords = keywords[:max_keywords]
    if not keywords:
        return "TRUE", []

    clauses = []
    params = []
    for kw in keywords:
        pattern = f"%{kw}%"
        clauses.append(
            "(p.what_you_do ILIKE %s OR p.offering ILIKE %s OR p.niche ILIKE %s)"
        )
        params.extend([pattern, pattern, pattern])

    # Require at least 2 keyword matches (if enough keywords), else 1
    min_matches = min(2, len(keywords))
    if min_matches <= 1:
        return " OR ".join(clauses), params

    # Use a scoring subquery: count matches and require >= min_matches
    # Simpler approach: OR all, rely on ORDER BY readiness_score to rank
    return " OR ".join(clauses), params


def _text_search(
    cur,
    client_id: str,
    keywords: list[str],
    exclude_ids: list[str],
    min_readiness_score: float,
    max_staleness_days: int,
    limit: int,
) -> list[dict]:
    """Search profiles using ILIKE keyword matching on text fields."""
    if not keywords:
        return []

    keyword_sql, keyword_params = _build_keyword_clauses(keywords)
    sql = _TEXT_SEARCH_SQL.format(keyword_clauses=keyword_sql)

    params = [
        min_readiness_score,      # jv_readiness_score filter
        client_id,                # exclude self
        exclude_ids or [],        # exclude already-seen IDs
        client_id,                # exclude existing match_suggestions
        max_staleness_days,       # staleness window
        *keyword_params,          # ILIKE keyword params
        limit,                    # LIMIT
    ]
    cur.execute(sql, params)
    return [dict(row) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Main search task
# ---------------------------------------------------------------------------

@task(name="db-prospect-search", retries=1, retry_delay_seconds=5)
def search_database_prospects(
    client_profile: dict,
    ideal_partner=None,
    exclude_ids: Optional[list[str]] = None,
    max_results: int = 100,
    min_readiness_score: float = 20.0,
    max_staleness_days: int = 180,
) -> list[dict]:
    """Search the existing profile database for potential JV partners.

    Two-phase search:
      1. pgvector cosine similarity on embedded profiles (~3K with vectors)
      2. ILIKE text search on remaining profiles (1M+ without vectors)

    All results are marked with ``_db_profile_id`` so the ingestion step
    knows these are existing profiles (no need to create new rows).

    Parameters
    ----------
    client_profile:
        Client profile dict (must include ``id``, ``seeking``, ``offering``).
    ideal_partner:
        Optional IdealPartnerProfile from partner_pipeline.  When provided,
        ``high_scoring_keywords`` are used for text search.
    exclude_ids:
        Profile IDs to skip (rotation filter, already-matched, etc.).
    max_results:
        Maximum prospects to return.
    min_readiness_score:
        Minimum ``jv_readiness_score`` for candidates.
    max_staleness_days:
        Profiles not enriched/created in this many days are excluded.

    Returns
    -------
    list[dict]
        Prospect dicts with ``_db_profile_id`` set to the existing profile ID.
    """
    logger = get_run_logger()
    client_id = str(client_profile.get("id", ""))
    client_name = client_profile.get("name", "Unknown")
    exclude_ids = [str(eid) for eid in (exclude_ids or [])]

    logger.info(
        "DB prospect search for %s (exclude=%d, min_score=%.0f, staleness=%dd)",
        client_name, len(exclude_ids), min_readiness_score, max_staleness_days,
    )

    all_results: list[dict] = []
    seen_ids: set[str] = set(exclude_ids)

    conn = _get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # ------------------------------------------------------------------
        # Phase 1: Vector search (client seeking → partner offering)
        # ------------------------------------------------------------------
        client_seeking = (client_profile.get("seeking") or "").strip()
        if client_seeking:
            logger.info("Phase 1a: vector search on embedding_offering (client seeking)")
            vec_results = _vector_search(
                cur, client_id, client_seeking,
                embed_col="embedding_offering",
                exclude_ids=exclude_ids,
                min_readiness_score=min_readiness_score,
                max_staleness_days=max_staleness_days,
                limit=max_results,
            )
            for row in vec_results:
                pid = str(row["id"])
                if pid not in seen_ids:
                    seen_ids.add(pid)
                    all_results.append(row)
            logger.info("Phase 1a: %d vector results (offering)", len(vec_results))

        # Also search client offering → partner seeking
        client_offering = (client_profile.get("offering") or "").strip()
        if client_offering and len(all_results) < max_results:
            logger.info("Phase 1b: vector search on embedding_seeking (client offering)")
            vec_results = _vector_search(
                cur, client_id, client_offering,
                embed_col="embedding_seeking",
                exclude_ids=list(seen_ids),
                min_readiness_score=min_readiness_score,
                max_staleness_days=max_staleness_days,
                limit=max_results - len(all_results),
            )
            for row in vec_results:
                pid = str(row["id"])
                if pid not in seen_ids:
                    seen_ids.add(pid)
                    all_results.append(row)
            logger.info("Phase 1b: %d vector results (seeking)", len(vec_results))

        # ------------------------------------------------------------------
        # Phase 2: Text keyword search (profiles without embeddings)
        # ------------------------------------------------------------------
        if len(all_results) < max_results:
            # Build keywords from ideal_partner or fallback to client fields
            keywords = []
            if ideal_partner and getattr(ideal_partner, "high_scoring_keywords", None):
                keywords = ideal_partner.high_scoring_keywords[:8]
            else:
                # Fallback: extract keywords from client seeking + niche
                for field in ["seeking", "niche", "who_you_serve"]:
                    text = (client_profile.get(field) or "").strip()
                    if text:
                        words = [
                            w for w in text.lower().split()
                            if len(w) >= 4 and w not in {"with", "that", "from", "they", "their", "this", "have", "been"}
                        ]
                        keywords.extend(words[:3])
                keywords = list(dict.fromkeys(keywords))[:8]  # deduplicate, keep order

            if keywords:
                logger.info(
                    "Phase 2: text search with %d keywords: %s",
                    len(keywords), keywords,
                )
                text_results = _text_search(
                    cur, client_id, keywords,
                    exclude_ids=list(seen_ids),
                    min_readiness_score=min_readiness_score,
                    max_staleness_days=max_staleness_days,
                    limit=max_results - len(all_results),
                )
                for row in text_results:
                    pid = str(row["id"])
                    if pid not in seen_ids:
                        seen_ids.add(pid)
                        all_results.append(row)
                logger.info("Phase 2: %d text results", len(text_results))
            else:
                logger.info("Phase 2: skipped — no keywords available")

    finally:
        conn.close()

    # ------------------------------------------------------------------
    # Format results as prospect dicts with _db_profile_id marker
    # ------------------------------------------------------------------
    prospects = []
    for row in all_results[:max_results]:
        prospect = {
            "name": row.get("name") or "",
            "website": row.get("website") or "",
            "linkedin": row.get("linkedin") or "",
            "email": row.get("email") or "",
            "company": "",
            "niche": row.get("niche") or "",
            "what_you_do": row.get("what_you_do") or "",
            "offering": row.get("offering") or "",
            "seeking": row.get("seeking") or "",
            "who_you_serve": row.get("who_you_serve") or "",
            "source": "db_vector" if row.get("similarity", 0) > 0 else "db_text",
            "discovery_cost": 0.0,
            "_db_profile_id": str(row["id"]),
            "_similarity": round(float(row.get("similarity") or 0), 4),
            "_jv_tier": row.get("jv_tier") or "",
            "_jv_readiness_score": float(row.get("jv_readiness_score") or 0),
            "raw_data": {
                "jv_tier": row.get("jv_tier") or "",
                "jv_readiness_score": float(row.get("jv_readiness_score") or 0),
                "revenue_tier": row.get("revenue_tier") or "",
                "network_role": row.get("network_role") or "",
            },
        }
        prospects.append(prospect)

    logger.info(
        "DB search complete for %s: %d prospects "
        "(%d vector, %d text), $0.00 cost",
        client_name,
        len(prospects),
        sum(1 for p in prospects if p["source"] == "db_vector"),
        sum(1 for p in prospects if p["source"] == "db_text"),
    )

    return prospects
