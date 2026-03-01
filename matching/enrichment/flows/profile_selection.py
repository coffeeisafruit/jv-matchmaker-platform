"""
Profile selection task for the Prefect enrichment flow.

Extracts the tiered profile-selection logic from the monolithic
``automated_enrichment_pipeline_safe.py`` into a standalone Prefect @task.
Profiles are ranked across six tiers (0-5) so the pipeline always works on
the highest-ROI candidates first.

Tier 0 - Re-enrich previously enriched profiles missing new fields
Tier 1 - Has website + missing ``seeking`` + list_size > 10K  (highest ROI)
Tier 2 - Has website + missing any key field + list_size > 1K
Tier 3 - Has website + missing any key field
Tier 4 - No website + has LinkedIn  (deep research via web search)
Tier 5 - No website + no LinkedIn   (name-based search only)

Does **not** import Django -- uses raw psycopg2 against DATABASE_URL.
"""

from __future__ import annotations

import os
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor
from prefect import task, get_run_logger

# ---------------------------------------------------------------------------
# Source priority hierarchy (copied from monolith R2)
# Higher-priority sources must never be overwritten by lower ones.
# ---------------------------------------------------------------------------
SOURCE_PRIORITY: dict[str, int] = {
    "client_confirmed": 100,
    "client_ingest": 90,
    "manual_edit": 80,
    "exa_research": 50,
    "ai_research": 40,
    "apollo": 30,
    "unknown": 0,
}

# ---------------------------------------------------------------------------
# Columns selected from the profiles table
# ---------------------------------------------------------------------------
_SELECT_COLUMNS = (
    "id, name, email, company, website, linkedin, "
    "list_size, seeking, who_you_serve, what_you_do, offering, "
    "facebook, instagram, youtube, twitter, "
    "niche, tags, bio, "
    "enrichment_metadata"
)


# ---------------------------------------------------------------------------
# Database helper
# ---------------------------------------------------------------------------
def _get_db_connection() -> psycopg2.extensions.connection:
    """Create a new psycopg2 connection from the ``DATABASE_URL`` env var."""
    dsn = os.environ["DATABASE_URL"]
    return psycopg2.connect(dsn)


# ---------------------------------------------------------------------------
# SQL fragments shared by the tiered queries
# ---------------------------------------------------------------------------
_KEY_FIELDS_NULL = (
    "(seeking IS NULL OR seeking = '' "
    "OR who_you_serve IS NULL OR who_you_serve = '' "
    "OR what_you_do IS NULL OR what_you_do = '' "
    "OR offering IS NULL OR offering = '')"
)

_SEEKING_NULL = "(seeking IS NULL OR seeking = '')"

_NEW_FIELDS_NULL = (
    "(revenue_tier IS NULL OR revenue_tier = '' "
    "OR content_platforms IS NULL OR content_platforms::text IN ('{}', 'null', '') "
    "OR jv_history IS NULL OR jv_history::text IN ('[]', 'null', ''))"
)


def _build_tier_queries() -> list[tuple[int, str]]:
    """Return the list of ``(tier, sql)`` pairs used by tiered selection."""
    return [
        # Tier 0 -- previously enriched but missing new-schema fields
        (0, f"""
            SELECT {_SELECT_COLUMNS}
            FROM profiles
            WHERE name IS NOT NULL AND name != ''
              AND last_enriched_at IS NOT NULL
              AND {_NEW_FIELDS_NULL}
            ORDER BY list_size DESC NULLS LAST
        """),
        # Tier 1 -- website + no seeking + large list
        (1, f"""
            SELECT {_SELECT_COLUMNS}
            FROM profiles
            WHERE name IS NOT NULL AND name != ''
              AND website IS NOT NULL AND website != ''
              AND {_SEEKING_NULL}
              AND list_size > 10000
            ORDER BY list_size DESC NULLS LAST
        """),
        # Tier 2 -- website + missing key fields + medium list
        (2, f"""
            SELECT {_SELECT_COLUMNS}
            FROM profiles
            WHERE name IS NOT NULL AND name != ''
              AND website IS NOT NULL AND website != ''
              AND {_KEY_FIELDS_NULL}
              AND list_size > 1000
            ORDER BY list_size DESC NULLS LAST
        """),
        # Tier 3 -- website + missing key fields (any list size)
        (3, f"""
            SELECT {_SELECT_COLUMNS}
            FROM profiles
            WHERE name IS NOT NULL AND name != ''
              AND website IS NOT NULL AND website != ''
              AND {_KEY_FIELDS_NULL}
            ORDER BY list_size DESC NULLS LAST
        """),
        # Tier 4 -- no website, has LinkedIn
        (4, f"""
            SELECT {_SELECT_COLUMNS}
            FROM profiles
            WHERE name IS NOT NULL AND name != ''
              AND (website IS NULL OR website = '')
              AND linkedin IS NOT NULL AND linkedin != ''
            ORDER BY list_size DESC NULLS LAST
        """),
        # Tier 5 -- no website, no LinkedIn (name-only search)
        (5, f"""
            SELECT {_SELECT_COLUMNS}
            FROM profiles
            WHERE name IS NOT NULL AND name != ''
              AND (website IS NULL OR website = '')
              AND (linkedin IS NULL OR linkedin = '')
            ORDER BY list_size DESC NULLS LAST
        """),
    ]


# ---------------------------------------------------------------------------
# Main selection task
# ---------------------------------------------------------------------------
@task(name="select-profiles", retries=2, retry_delay_seconds=5)
def select_profiles(
    limit: int = 50,
    priority: str = "tiered",
    tier_filter: set[int] | None = None,
    refresh_mode: bool = False,
    stale_days: int = 30,
    profile_ids: list[str] | None = None,
    priority_ids: list[str] | None = None,
) -> list[dict]:
    """Select profiles for the enrichment pipeline.

    If priority_ids provided, process those first before tier-based selection.

    Parameters
    ----------
    limit:
        Maximum number of profiles to return.
    priority:
        Selection strategy.  ``"tiered"`` (default) walks tiers 0-5 in order
        and deduplicates by profile ID.
    tier_filter:
        If provided, only include profiles from these tiers.
    refresh_mode:
        When *True*, select stale profiles (``last_enriched_at`` older than
        *stale_days*), ordered by ``list_size DESC``.
    stale_days:
        Number of days after which a profile is considered stale.
    profile_ids:
        Explicit list of profile IDs to fetch (e.g. for acquisition-triggered
        enrichment).  When provided, all other filters are ignored.
    priority_ids:
        Optional list of profile IDs to process first (e.g. low-confidence
        or stale profiles flagged for priority re-enrichment).  These are
        included up to *limit*, then remaining slots are filled with the
        standard tier-based selection.

    Returns
    -------
    list[dict]
        Profile dicts, each augmented with a ``_tier`` field.
    """
    logger = get_run_logger()
    conn = _get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # ---- Explicit ID list (acquisition-triggered) --------------------
        if profile_ids is not None:
            logger.info(
                "Fetching %d explicit profile IDs", len(profile_ids)
            )
            cursor.execute(
                f"""
                SELECT {_SELECT_COLUMNS}
                FROM profiles
                WHERE id = ANY(%s)
                """,
                (profile_ids,),
            )
            profiles: list[dict] = []
            for row in cursor.fetchall():
                profile = dict(row)
                # Assign a tier heuristic for downstream use
                if profile.get("website"):
                    profile["_tier"] = 1
                elif profile.get("linkedin"):
                    profile["_tier"] = 4
                else:
                    profile["_tier"] = 5
                profiles.append(profile)
            logger.info("Fetched %d profiles by explicit IDs", len(profiles))
            return profiles

        # ---- Priority IDs first, then fill with tier-based selection -----
        profiles: list[dict] = []
        seen_ids: set[Any] = set()

        if priority_ids:
            # Fetch priority profiles first (up to limit)
            ids_to_fetch = priority_ids[:limit]
            logger.info(
                "Fetching %d priority profiles (from %d flagged)",
                len(ids_to_fetch),
                len(priority_ids),
            )
            cursor.execute(
                f"""
                SELECT {_SELECT_COLUMNS}
                FROM profiles
                WHERE id = ANY(%s)
                """,
                (ids_to_fetch,),
            )
            for row in cursor.fetchall():
                profile = dict(row)
                pid = profile["id"]
                if pid in seen_ids:
                    continue
                seen_ids.add(pid)
                # Assign tier heuristic
                if profile.get("website"):
                    profile["_tier"] = 1
                elif profile.get("linkedin"):
                    profile["_tier"] = 4
                else:
                    profile["_tier"] = 5
                profile["_priority"] = True
                profiles.append(profile)

            logger.info(
                "Loaded %d priority profiles; %d slots remaining",
                len(profiles),
                max(0, limit - len(profiles)),
            )

            # If priority profiles fill the limit, return immediately
            if len(profiles) >= limit:
                logger.info(
                    "Priority profiles filled all %d slots", limit
                )
                return profiles[:limit]

        # ---- Refresh mode (stale re-enrichment) -------------------------
        if refresh_mode:
            logger.info(
                "Refresh mode: selecting stale profiles (>%d days)",
                stale_days,
            )
            remaining = limit - len(profiles)
            cursor.execute(
                f"""
                SELECT {_SELECT_COLUMNS}
                FROM profiles
                WHERE name IS NOT NULL AND name != ''
                  AND last_enriched_at IS NOT NULL
                  AND last_enriched_at < NOW() - INTERVAL '%s days'
                ORDER BY list_size DESC NULLS LAST
                LIMIT %s
                """,
                (stale_days, remaining),
            )
            for row in cursor.fetchall():
                profile = dict(row)
                pid = profile["id"]
                if pid in seen_ids:
                    continue
                seen_ids.add(pid)
                if profile.get("website"):
                    profile["_tier"] = 1
                elif profile.get("linkedin"):
                    profile["_tier"] = 4
                else:
                    profile["_tier"] = 5
                if tier_filter and profile["_tier"] not in tier_filter:
                    continue
                profiles.append(profile)
                if len(profiles) >= limit:
                    break
            logger.info(
                "Refresh mode selected %d total profiles (%d priority + %d stale)",
                len(profiles),
                len([p for p in profiles if p.get("_priority")]),
                len([p for p in profiles if not p.get("_priority")]),
            )
            return profiles

        # ---- Tiered selection (default) ----------------------------------
        tier_queries = _build_tier_queries()

        for tier, query in tier_queries:
            if tier_filter and tier not in tier_filter:
                continue
            if len(profiles) >= limit:
                break

            cursor.execute(query)
            rows = cursor.fetchall()

            for row in rows:
                if len(profiles) >= limit:
                    break
                profile = dict(row)
                pid = profile["id"]
                if pid in seen_ids:
                    continue
                seen_ids.add(pid)
                profile["_tier"] = tier
                profiles.append(profile)

        # Log tier breakdown
        tier_counts: dict[int, int] = {}
        priority_count = 0
        for p in profiles:
            if p.get("_priority"):
                priority_count += 1
            t = p.get("_tier", 0)
            tier_counts[t] = tier_counts.get(t, 0) + 1
        if priority_count:
            logger.info("Priority profiles: %d", priority_count)
        for t in sorted(tier_counts):
            logger.info("Tier %d: %d profiles", t, tier_counts[t])
        logger.info("Total selected: %d profiles", len(profiles))

        return profiles

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Convenience task: fetch specific profiles by ID
# ---------------------------------------------------------------------------
@task(name="get-profiles-by-ids")
def get_profiles_by_ids(profile_ids: list[str]) -> list[dict]:
    """Fetch specific profiles by their IDs.

    Thin wrapper around :func:`select_profiles` for callers that already know
    which profiles to enrich (e.g. an acquisition webhook or manual re-run).

    Parameters
    ----------
    profile_ids:
        List of profile ``id`` values to fetch.

    Returns
    -------
    list[dict]
        Profile dicts, each augmented with a ``_tier`` field.
    """
    logger = get_run_logger()
    logger.info("get_profiles_by_ids called with %d IDs", len(profile_ids))
    conn = _get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            f"""
            SELECT {_SELECT_COLUMNS}
            FROM profiles
            WHERE id = ANY(%s::uuid[])
            """,
            (profile_ids,),
        )
        profiles: list[dict] = []
        for row in cursor.fetchall():
            profile = dict(row)
            if profile.get("website"):
                profile["_tier"] = 1
            elif profile.get("linkedin"):
                profile["_tier"] = 4
            else:
                profile["_tier"] = 5
            profiles.append(profile)
        logger.info("Fetched %d profiles by ID", len(profiles))
        return profiles
    finally:
        conn.close()
