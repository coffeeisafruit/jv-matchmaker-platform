"""
Per-client partner readiness tracking — the "always have 30 ready" guarantee.

Tracks partner availability, 90-day rotation, and readiness per client.
Builds data-driven IdealPartnerProfile from existing high-scoring matches
with per-client, per-category, and fallback learning levels.

No new tables needed — uses existing ReportPartner, match_suggestions,
EngagementSummary, and PartnerRecommendation models.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

ROTATION_DAYS = 90
BUFFER_TARGET = 30
QUALITY_THRESHOLD = 64

# Minimum matches for each learning level
_MIN_CLIENT_MATCHES = 5
_MIN_CATEGORY_MATCHES = 10

# Stop words for keyword extraction
_STOP_WORDS = frozenset({
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
    "being", "have", "has", "had", "do", "does", "did", "will", "would",
    "could", "should", "may", "might", "shall", "can", "need", "dare",
    "i", "you", "he", "she", "it", "we", "they", "me", "him", "her", "us",
    "them", "my", "your", "his", "its", "our", "their", "this", "that",
    "these", "those", "who", "whom", "which", "what", "where", "when",
    "how", "not", "no", "nor", "as", "if", "then", "than", "too", "very",
    "just", "about", "above", "after", "before", "between", "into",
    "through", "during", "also", "so", "such", "more", "most", "other",
    "some", "any", "each", "every", "all", "both", "few", "many", "much",
    "own", "same", "only", "help", "people", "business", "work", "services",
})


def _top_n(counter_dict: dict, n: int = 5) -> list:
    """Return top N keys from a dict sorted by value descending."""
    return [k for k, _ in sorted(counter_dict.items(), key=lambda x: -x[1])[:n]]


def _extract_keywords(text: str) -> list[str]:
    """Extract meaningful keywords from text, filtering stop words."""
    if not text:
        return []
    words = re.findall(r"[a-z]+", text.lower())
    return [w for w in words if w not in _STOP_WORDS and len(w) >= 3]


@dataclass
class ClientReadiness:
    """Readiness status for a single client."""

    client_id: str = ""
    client_name: str = ""
    available: int = 0  # Undelivered quality matches
    delivered_90d: int = 0  # Recently shown
    in_pipeline: int = 0  # Being enriched
    gap: int = 0  # BUFFER_TARGET - available
    status: str = ""  # "green", "yellow", "red"

    @property
    def is_healthy(self) -> bool:
        return self.available >= BUFFER_TARGET


@dataclass
class IdealPartnerProfile:
    """Data-driven profile of what a quality match looks like.

    Built from actual 64+ scoring matches (per-client or per-category),
    with engagement weighting from OutreachEvent data.
    """

    # Core fields (backward-compatible with original design)
    ideal_offering: str = ""
    ideal_seeking: str = ""
    ideal_audience: str = ""
    target_niches: list = field(default_factory=list)
    must_have: list = field(default_factory=list)
    preferred_tiers: list = field(default_factory=list)

    # Learned fields from match analysis
    common_roles: list = field(default_factory=list)
    revenue_tier_range: list = field(default_factory=list)
    common_platforms: list = field(default_factory=list)
    high_scoring_keywords: list = field(default_factory=list)
    offering_examples: list = field(default_factory=list)
    seeking_examples: list = field(default_factory=list)
    audience_examples: list = field(default_factory=list)
    avg_match_score: float = 0.0
    match_count: int = 0
    learned: bool = False
    learning_level: str = "fallback"  # "client", "category", or "fallback"
    engagement_weighted: bool = False


def _get_conn():
    db_url = os.environ.get("DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL", "")
    return psycopg2.connect(db_url, options="-c statement_timeout=60000")


class PartnerPipeline:
    """Track partner availability and rotation per client."""

    def get_client_readiness(self, client_id: str) -> ClientReadiness:
        """Returns readiness status for a single client.

        Counts:
        - available: undelivered matches with harmonic_mean >= 64
        - delivered_90d: partners delivered in the last 90 days
        - gap: BUFFER_TARGET - available
        """
        conn = _get_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Get client name
            cur.execute("SELECT name FROM profiles WHERE id = %s", (client_id,))
            row = cur.fetchone()
            client_name = row["name"] if row else ""

            # Get delivered partner IDs in last 90 days
            cutoff = (datetime.now(timezone.utc) - timedelta(days=ROTATION_DAYS)).isoformat()
            cur.execute(
                """
                SELECT DISTINCT rp.source_profile_id
                FROM matching_reportpartner rp
                JOIN matching_memberreport mr ON rp.report_id = mr.id
                WHERE mr.supabase_profile_id = %s
                  AND rp.created_at >= %s
                  AND rp.source_profile_id IS NOT NULL
                """,
                (client_id, cutoff),
            )
            delivered_ids = {str(r["source_profile_id"]) for r in cur.fetchall()}
            delivered_90d = len(delivered_ids)

            # Count quality matches not yet delivered
            cur.execute(
                """
                SELECT COUNT(*) as cnt
                FROM match_suggestions
                WHERE profile_id = %s
                  AND harmonic_mean >= %s
                  AND status NOT IN ('dismissed')
                  AND suggested_profile_id NOT IN (
                      SELECT DISTINCT rp.source_profile_id
                      FROM matching_reportpartner rp
                      JOIN matching_memberreport mr ON rp.report_id = mr.id
                      WHERE mr.supabase_profile_id = %s
                        AND rp.created_at >= %s
                        AND rp.source_profile_id IS NOT NULL
                  )
                """,
                (client_id, QUALITY_THRESHOLD, client_id, cutoff),
            )
            available = cur.fetchone()["cnt"]

            gap = max(0, BUFFER_TARGET - available)

            if available >= BUFFER_TARGET:
                status = "green"
            elif available >= 20:
                status = "yellow"
            else:
                status = "red"

            return ClientReadiness(
                client_id=client_id,
                client_name=client_name,
                available=available,
                delivered_90d=delivered_90d,
                gap=gap,
                status=status,
            )

        finally:
            conn.close()

    def get_rotation_filter(self, client_id: str) -> set[str]:
        """Returns profile IDs delivered to this client in last 90 days.

        These should be excluded from the next report's candidate pool.
        """
        conn = _get_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cutoff = (datetime.now(timezone.utc) - timedelta(days=ROTATION_DAYS)).isoformat()

            cur.execute(
                """
                SELECT DISTINCT rp.source_profile_id
                FROM matching_reportpartner rp
                JOIN matching_memberreport mr ON rp.report_id = mr.id
                WHERE mr.supabase_profile_id = %s
                  AND rp.created_at >= %s
                  AND rp.source_profile_id IS NOT NULL
                """,
                (client_id, cutoff),
            )
            return {str(r["source_profile_id"]) for r in cur.fetchall()}

        finally:
            conn.close()

    def get_all_clients_readiness(self) -> list[ClientReadiness]:
        """Dashboard view: per-client available/gap/pipeline counts.

        Returns all active clients sorted by gap (biggest gap first).
        """
        conn = _get_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Get all active clients (those with active MemberReports)
            cur.execute(
                """
                SELECT DISTINCT mr.supabase_profile_id as client_id
                FROM matching_memberreport mr
                WHERE mr.is_active = true
                  AND mr.supabase_profile_id IS NOT NULL
                """,
            )
            client_ids = [str(r["client_id"]) for r in cur.fetchall()]

        finally:
            conn.close()

        results = []
        for client_id in client_ids:
            try:
                readiness = self.get_client_readiness(client_id)
                results.append(readiness)
            except Exception as e:
                logger.error("Readiness check failed for %s: %s", client_id, e)

        results.sort(key=lambda r: r.gap, reverse=True)
        return results

    def build_ideal_partner_profile(self, client_id: str) -> IdealPartnerProfile:
        """Build a data-driven ideal partner profile for this client.

        Learning levels (tried in order):
          1. Per-client: 5+ personal 64+ matches → learn from THIS client's data
          2. Per-category: 10+ matches across clients in same niche → bootstrap
          3. Fallback: simple field-flip (client seeking → partner offering)

        Engagement weighting: partners the client actually clicked/contacted
        in OutreachEvent get 1.5-2x weight in the analysis.
        """
        conn = _get_conn()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Load client profile
            cur.execute(
                "SELECT seeking, offering, who_you_serve, niche, tags "
                "FROM profiles WHERE id = %s",
                (client_id,),
            )
            client = cur.fetchone()
            if not client:
                return IdealPartnerProfile()

            client_niche = (client.get("niche") or "").strip()

            # Get niche gaps
            niche_gaps = self._get_niche_gaps(client_id)

            # --- Level 1: Per-client learning ---
            analysis = self._analyze_high_scoring_matches(cur, client_id)
            if analysis["count"] >= _MIN_CLIENT_MATCHES:
                logger.info(
                    "IdealPartnerProfile for %s: per-client learning "
                    "(%d matches, avg=%.1f, engagement=%s)",
                    client_id, analysis["count"],
                    analysis["avg_score"], analysis["engagement_weighted"],
                )
                return self._build_from_analysis(
                    analysis, niche_gaps, learning_level="client",
                )

            # --- Level 2: Per-category learning ---
            if client_niche:
                cat_analysis = self._analyze_category_matches(
                    cur, client_niche, exclude_client_id=client_id,
                )
                if cat_analysis["count"] >= _MIN_CATEGORY_MATCHES:
                    logger.info(
                        "IdealPartnerProfile for %s: per-category learning "
                        "(niche='%s', %d matches across category)",
                        client_id, client_niche, cat_analysis["count"],
                    )
                    return self._build_from_analysis(
                        cat_analysis, niche_gaps, learning_level="category",
                    )

            # --- Level 3: Fallback (field-flip) ---
            logger.info(
                "IdealPartnerProfile for %s: fallback (field-flip, "
                "%d personal matches, no category data)",
                client_id, analysis["count"],
            )
            return IdealPartnerProfile(
                ideal_offering=client.get("seeking") or "",
                ideal_seeking=client.get("offering") or "",
                ideal_audience=client.get("who_you_serve") or "",
                target_niches=niche_gaps,
                must_have=["website", "email_or_booking_link"],
                preferred_tiers=["A", "B", "C"],
                learned=False,
                learning_level="fallback",
            )

        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Match analysis (per-client and per-category)
    # ------------------------------------------------------------------

    def _analyze_high_scoring_matches(
        self,
        cur,
        client_id: str,
        score_threshold: float = QUALITY_THRESHOLD,
    ) -> dict:
        """Analyze patterns in existing 64+ matches for a specific client.

        Joins with EngagementSummary to weight partners the client
        actually engaged with (clicked, contacted) more heavily.
        """
        cur.execute(
            """
            SELECT p.offering, p.seeking, p.who_you_serve, p.what_you_do,
                   p.niche, p.revenue_tier, p.content_platforms,
                   p.network_role, p.tags,
                   ms.harmonic_mean, ms.suggested_profile_id,
                   es.any_contact_action, es.card_expand_count
            FROM match_suggestions ms
            JOIN profiles p ON p.id = ms.suggested_profile_id
            LEFT JOIN matching_engagementsummary es
              ON es.partner_id = ms.suggested_profile_id::text
              AND es.report_id IN (
                  SELECT id FROM matching_memberreport
                  WHERE supabase_profile_id = ms.profile_id
                    AND is_active = true
              )
            WHERE ms.profile_id = %s
              AND ms.harmonic_mean >= %s
              AND ms.status NOT IN ('dismissed')
            ORDER BY ms.harmonic_mean DESC
            LIMIT 50
            """,
            (client_id, score_threshold),
        )
        rows = cur.fetchall()
        return self._compile_analysis(rows)

    def _analyze_category_matches(
        self,
        cur,
        client_niche: str,
        exclude_client_id: str = "",
        score_threshold: float = QUALITY_THRESHOLD,
    ) -> dict:
        """Analyze patterns across ALL clients in the same niche category.

        Used as a bootstrap when a specific client has < 5 personal matches.
        """
        niche_pattern = f"%{client_niche}%"
        cur.execute(
            """
            SELECT p.offering, p.seeking, p.who_you_serve, p.what_you_do,
                   p.niche, p.revenue_tier, p.content_platforms,
                   p.network_role, p.tags,
                   ms.harmonic_mean, ms.suggested_profile_id,
                   NULL::boolean AS any_contact_action,
                   NULL::int AS card_expand_count
            FROM match_suggestions ms
            JOIN profiles p ON p.id = ms.suggested_profile_id
            JOIN profiles client ON client.id = ms.profile_id
            WHERE client.niche ILIKE %s
              AND ms.harmonic_mean >= %s
              AND ms.status NOT IN ('dismissed')
              AND ms.profile_id != %s
            ORDER BY ms.harmonic_mean DESC
            LIMIT 100
            """,
            (niche_pattern, score_threshold, exclude_client_id),
        )
        rows = cur.fetchall()
        return self._compile_analysis(rows)

    def _compile_analysis(self, rows: list[dict]) -> dict:
        """Compile match rows into an analysis dict with engagement weighting."""
        if not rows:
            return {
                "count": 0, "avg_score": 0.0, "engagement_weighted": False,
                "offering_texts": [], "seeking_texts": [], "audience_texts": [],
                "niche_counts": {}, "role_counts": {}, "revenue_tiers": {},
                "platform_counts": {}, "keywords": {},
            }

        max_score = max(float(r.get("harmonic_mean") or 0) for r in rows)
        if max_score == 0:
            max_score = 1.0

        niche_counts: Counter = Counter()
        role_counts: Counter = Counter()
        revenue_tiers: Counter = Counter()
        platform_counts: Counter = Counter()
        keyword_counts: Counter = Counter()

        offering_texts: list[tuple[float, str]] = []
        seeking_texts: list[tuple[float, str]] = []
        audience_texts: list[tuple[float, str]] = []

        has_engagement = False
        score_sum = 0.0

        for row in rows:
            hm = float(row.get("harmonic_mean") or 0)
            score_sum += hm
            base_weight = hm / max_score

            # Engagement weighting from OutreachEvent
            any_contact = row.get("any_contact_action")
            card_expands = row.get("card_expand_count") or 0

            if any_contact:
                weight = base_weight * 2.0
                has_engagement = True
            elif card_expands and card_expands >= 2:
                weight = base_weight * 1.5
                has_engagement = True
            else:
                weight = base_weight

            # Collect verbatim texts (score-weighted for sorting)
            offering = (row.get("offering") or "").strip()
            if offering:
                offering_texts.append((hm, offering))

            seeking = (row.get("seeking") or "").strip()
            if seeking:
                seeking_texts.append((hm, seeking))

            audience = (row.get("who_you_serve") or "").strip()
            if audience:
                audience_texts.append((hm, audience))

            # Count patterns (weighted)
            niche = (row.get("niche") or "").strip()
            if niche:
                niche_counts[niche] += weight

            role = (row.get("network_role") or "").strip()
            if role:
                role_counts[role] += weight

            rev = (row.get("revenue_tier") or "").strip()
            if rev:
                revenue_tiers[rev] += weight

            # Parse content_platforms
            platforms = row.get("content_platforms")
            if platforms:
                if isinstance(platforms, str):
                    try:
                        platforms = json.loads(platforms)
                    except (json.JSONDecodeError, TypeError):
                        platforms = None
                if isinstance(platforms, dict):
                    for pkey in platforms:
                        platform_counts[pkey] += weight
                elif isinstance(platforms, list):
                    for pkey in platforms:
                        platform_counts[str(pkey)] += weight

            # Extract keywords from offering + what_you_do
            for text_field in [offering, (row.get("what_you_do") or "").strip()]:
                for kw in _extract_keywords(text_field):
                    keyword_counts[kw] += weight

        # Sort texts by score (highest first), take top 5
        offering_texts.sort(key=lambda x: -x[0])
        seeking_texts.sort(key=lambda x: -x[0])
        audience_texts.sort(key=lambda x: -x[0])

        return {
            "count": len(rows),
            "avg_score": score_sum / len(rows) if rows else 0.0,
            "engagement_weighted": has_engagement,
            "offering_texts": [t for _, t in offering_texts[:5]],
            "seeking_texts": [t for _, t in seeking_texts[:5]],
            "audience_texts": [t for _, t in audience_texts[:5]],
            "niche_counts": dict(niche_counts),
            "role_counts": dict(role_counts),
            "revenue_tiers": dict(revenue_tiers),
            "platform_counts": dict(platform_counts),
            "keywords": dict(keyword_counts),
        }

    def _build_from_analysis(
        self,
        analysis: dict,
        niche_gaps: list,
        learning_level: str,
    ) -> IdealPartnerProfile:
        """Build an IdealPartnerProfile from compiled analysis data."""
        return IdealPartnerProfile(
            ideal_offering=(
                analysis["offering_texts"][0]
                if analysis["offering_texts"] else ""
            ),
            ideal_seeking=(
                analysis["seeking_texts"][0]
                if analysis["seeking_texts"] else ""
            ),
            ideal_audience=(
                analysis["audience_texts"][0]
                if analysis["audience_texts"] else ""
            ),
            target_niches=niche_gaps,
            must_have=["website", "email_or_booking_link"],
            preferred_tiers=["A", "B", "C"],
            common_roles=_top_n(analysis["role_counts"], 5),
            revenue_tier_range=_top_n(analysis["revenue_tiers"], 3),
            common_platforms=_top_n(analysis["platform_counts"], 5),
            high_scoring_keywords=_top_n(analysis["keywords"], 10),
            offering_examples=analysis["offering_texts"][:5],
            seeking_examples=analysis["seeking_texts"][:5],
            audience_examples=analysis["audience_texts"][:5],
            avg_match_score=analysis["avg_score"],
            match_count=analysis["count"],
            learned=True,
            learning_level=learning_level,
            engagement_weighted=analysis["engagement_weighted"],
        )

    @staticmethod
    def _get_niche_gaps(client_id: str) -> list:
        """Get niche gaps from gap detection (non-fatal on failure)."""
        try:
            from matching.enrichment.flows.gap_detection import detect_match_gaps
            gap_data = detect_match_gaps.fn(
                client_profile_id=client_id,
                target_score=QUALITY_THRESHOLD,
                target_count=BUFFER_TARGET,
            )
            return gap_data.get("niche_gaps", [])
        except Exception:
            return []
