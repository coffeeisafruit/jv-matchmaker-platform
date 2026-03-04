"""
Layer 2: Rescore + Filter — $0 cost.

Re-runs jv_triage scoring on profiles affected by Layer 1 (new contact data
bumps scores). Filters by threshold (default 50+) and returns sorted IDs
for Layer 3 AI enrichment.

Tracks tier promotions (C→B, D→C) for reporting.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

# Ensure scripts/sourcing is importable
_project_root = Path(__file__).resolve().parents[3]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))


# ---------- Result dataclass ----------

@dataclass
class Layer2Result:
    """Summary of a Layer 2 run."""

    profiles_rescored: int = 0
    tier_promotions: dict = field(default_factory=dict)  # e.g. {"C→B": 5}
    score_distribution: dict = field(default_factory=dict)  # e.g. {"40-50": 120}
    qualified_ids: list = field(default_factory=list)  # IDs scoring >= threshold
    qualified_count: int = 0
    avg_score_before: float = 0.0
    avg_score_after: float = 0.0
    runtime_seconds: float = 0.0


# ---------- DB helpers ----------

def _get_conn():
    db_url = os.environ.get("DIRECT_DATABASE_URL") or os.environ.get("DATABASE_URL", "")
    return psycopg2.connect(db_url, options="-c statement_timeout=120000")


def _fetch_profiles_for_rescore(
    profile_ids: list[str] | None = None,
    tier_filter: set[str] | None = None,
    limit: int | None = None,
) -> list[dict]:
    """Fetch profiles that need rescoring."""
    conn = _get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    conditions = ["jv_tier IS NOT NULL"]
    params: list = []

    if profile_ids:
        conditions.append("id = ANY(%s::uuid[])")
        params.append(profile_ids)

    if tier_filter:
        conditions.append("jv_tier = ANY(%s)")
        params.append(list(tier_filter))

    where = " AND ".join(conditions)
    sql = f"""
        SELECT id, name, email, phone, website, linkedin,
               bio, jv_history, content_platforms, list_size,
               social_reach, revenue_tier, booking_link,
               seeking, offering, who_you_serve,
               jv_tier, jv_readiness_score,
               enrichment_metadata, seniority, intent_signal
        FROM profiles
        WHERE {where}
        ORDER BY jv_readiness_score DESC NULLS LAST
    """
    if limit:
        sql += f" LIMIT {int(limit)}"

    cur.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def _batch_update_scores(updates: list[dict]) -> int:
    """Batch update jv_tier and jv_readiness_score."""
    if not updates:
        return 0

    conn = _get_conn()
    cur = conn.cursor()
    updated = 0

    try:
        for u in updates:
            cur.execute(
                """
                UPDATE profiles
                SET jv_tier = %s,
                    jv_readiness_score = %s,
                    updated_at = NOW()
                WHERE id = %s::uuid
                """,
                (u["tier"], u["score"], u["id"]),
            )
            updated += 1

        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error("Batch update failed: %s", e)
        raise
    finally:
        cur.close()
        conn.close()

    return updated


# ---------- Scoring functions (from jv_triage.py) ----------

# Import the actual scoring functions
def _import_triage():
    """Lazily import jv_triage scoring functions."""
    try:
        from scripts.sourcing.jv_triage import classify_tier, score_jv_readiness
        return classify_tier, score_jv_readiness
    except ImportError:
        # Fallback: add scripts/sourcing to path and retry
        sourcing_path = str(_project_root / "scripts" / "sourcing")
        if sourcing_path not in sys.path:
            sys.path.insert(0, sourcing_path)
        from jv_triage import classify_tier, score_jv_readiness
        return classify_tier, score_jv_readiness


# ---------- Main Layer 2 entry point ----------

class Layer2RescoreFilter:
    """Layer 2: Rescore profiles and filter by threshold for Layer 3."""

    def __init__(
        self,
        threshold: float = 50.0,
        tier_filter: set[str] | None = None,
        limit: int | None = None,
        dry_run: bool = False,
    ):
        self.threshold = threshold
        self.tier_filter = tier_filter
        self.limit = limit
        self.dry_run = dry_run

    def run(
        self,
        affected_ids: list[str] | None = None,
    ) -> Layer2Result:
        """Execute Layer 2 rescoring.

        Parameters
        ----------
        affected_ids:
            Profile IDs from Layer 1 that had data changes.
            If None, rescores all profiles matching tier_filter.
        """
        start = time.time()
        result = Layer2Result()

        classify_tier, score_jv_readiness = _import_triage()

        profiles = _fetch_profiles_for_rescore(
            profile_ids=affected_ids,
            tier_filter=self.tier_filter,
            limit=self.limit,
        )

        if not profiles:
            logger.info("Layer 2: no profiles to rescore")
            result.runtime_seconds = time.time() - start
            return result

        logger.info("Layer 2: rescoring %d profiles (threshold=%.1f)", len(profiles), self.threshold)

        # Track before scores
        before_scores = [p.get("jv_readiness_score") or 0 for p in profiles]
        result.avg_score_before = sum(before_scores) / len(before_scores) if before_scores else 0

        updates = []
        promotions = Counter()
        score_bands = Counter()

        for profile in profiles:
            old_tier = profile.get("jv_tier", "E")
            old_score = profile.get("jv_readiness_score") or 0

            new_tier = classify_tier(profile)
            new_score = score_jv_readiness(profile, new_tier)

            # Track tier changes
            if new_tier != old_tier:
                key = f"{old_tier}→{new_tier}"
                promotions[key] += 1

            # Track score distribution
            band = f"{int(new_score // 10) * 10}-{int(new_score // 10) * 10 + 10}"
            score_bands[band] += 1

            # Only update if something changed
            if new_tier != old_tier or abs(new_score - old_score) >= 0.5:
                updates.append({
                    "id": str(profile["id"]),
                    "tier": new_tier,
                    "score": round(new_score, 2),
                })

            # Qualified for Layer 3?
            if new_score >= self.threshold and new_tier != "X":
                result.qualified_ids.append(str(profile["id"]))

        result.profiles_rescored = len(profiles)
        result.tier_promotions = dict(promotions)
        result.score_distribution = dict(score_bands)
        result.qualified_count = len(result.qualified_ids)

        # Calculate after scores
        after_scores = []
        for profile in profiles:
            pid = str(profile["id"])
            for u in updates:
                if u["id"] == pid:
                    after_scores.append(u["score"])
                    break
            else:
                after_scores.append(profile.get("jv_readiness_score") or 0)
        result.avg_score_after = sum(after_scores) / len(after_scores) if after_scores else 0

        # Write updates
        if updates and not self.dry_run:
            written = _batch_update_scores(updates)
            logger.info("Layer 2: updated %d profiles in DB", written)
        elif updates:
            logger.info("Layer 2: DRY RUN — would update %d profiles", len(updates))

        result.runtime_seconds = time.time() - start

        logger.info(
            "Layer 2 complete: %d rescored, %d updated, %d qualified (>=%.0f), "
            "avg score %.1f→%.1f, promotions=%s, %.1fs",
            result.profiles_rescored,
            len(updates),
            result.qualified_count,
            self.threshold,
            result.avg_score_before,
            result.avg_score_after,
            dict(promotions),
            result.runtime_seconds,
        )

        return result
