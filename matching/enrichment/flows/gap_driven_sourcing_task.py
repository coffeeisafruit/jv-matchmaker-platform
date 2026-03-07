"""
Prefect @task: gap-driven sourcing -- reads the latest market intelligence
snapshot and triggers priority scrapers to fill identified gaps.

Matches gap data (unmet-demand keywords, missing roles, underserved niches)
against scraper metadata (TYPICAL_ROLES, TYPICAL_NICHES, TYPICAL_OFFERINGS)
to select which scrapers would most effectively close the gaps.

Usage (within a Prefect flow):
    from matching.enrichment.flows.gap_driven_sourcing_task import gap_driven_sourcing

    result = gap_driven_sourcing(max_scrapers=5, dry_run=False)
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor
from prefect import task, get_run_logger


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

LATEST_SNAPSHOT_QUERY = """
    SELECT id, computed_at, snapshot_data
    FROM niche_statistics_snapshots
    ORDER BY computed_at DESC
    LIMIT 1;
"""

# Quality floor: check historical ingestion quality per scraper
SCRAPER_QUALITY_QUERY = """
    SELECT source,
           COUNT(*) AS total,
           COUNT(*) FILTER (
               WHERE (seeking IS NOT NULL AND seeking != '')
                  OR (offering IS NOT NULL AND offering != '')
           ) AS with_data
    FROM profiles
    WHERE source LIKE 'scraper_%%'
    GROUP BY source;
"""


# ---------------------------------------------------------------------------
# Database helper (matches monthly_processing.py pattern)
# ---------------------------------------------------------------------------

def _get_db_connection() -> psycopg2.extensions.connection:
    """Create a new psycopg2 connection from ``DATABASE_URL``."""
    dsn = os.environ["DATABASE_URL"]
    return psycopg2.connect(dsn)


# ---------------------------------------------------------------------------
# Scraper scoring helpers
# ---------------------------------------------------------------------------

def _load_scraper_metadata() -> dict[str, dict[str, Any]]:
    """Load metadata from all registered scrapers.

    Returns a dict mapping scraper_name -> {
        "typical_roles": [...],
        "typical_niches": [...],
        "typical_offerings": [...],
    }
    """
    from scripts.sourcing.runner import SCRAPER_REGISTRY, _register_scrapers
    _register_scrapers()

    metadata: dict[str, dict[str, Any]] = {}
    for name, scraper_cls in SCRAPER_REGISTRY.items():
        metadata[name] = {
            "typical_roles": getattr(scraper_cls, "TYPICAL_ROLES", []),
            "typical_niches": getattr(scraper_cls, "TYPICAL_NICHES", []),
            "typical_offerings": getattr(scraper_cls, "TYPICAL_OFFERINGS", []),
        }
    return metadata


def _compute_scraper_scores(
    snapshot_data: dict,
    scraper_metadata: dict[str, dict[str, Any]],
    quality_map: dict[str, float],
    quality_floor: float = 0.30,
) -> list[tuple[str, float, list[str]]]:
    """Score each scraper by how well it matches identified gaps.

    Returns a list of (scraper_name, score, gaps_targeted) sorted by score
    descending.  Scrapers below the quality floor are excluded.
    """
    # Extract gap keywords (high-demand only)
    gap_keywords: set[str] = set()
    for gap in snapshot_data.get("supply_demand_gaps", []):
        if gap.get("gap_type") == "high_demand":
            gap_keywords.add(gap["keyword"].lower())

    # Extract missing roles across all niches
    missing_roles: set[str] = set()
    for rg in snapshot_data.get("role_gaps", []):
        for role in rg.get("missing_high_value_roles", []):
            missing_roles.add(role.lower())

    # Extract underserved niches (low health score < 40)
    underserved_niches: set[str] = set()
    for nh in snapshot_data.get("niche_health", []):
        if nh.get("health_score", 100) < 40:
            underserved_niches.add(nh["niche"].lower())

    scored: list[tuple[str, float, list[str]]] = []

    for name, meta in scraper_metadata.items():
        # Quality floor check
        quality = quality_map.get(f"scraper_{name}", quality_map.get(name, 1.0))
        if quality < quality_floor:
            continue

        score = 0.0
        gaps_targeted: list[str] = []

        # Score against gap keywords (check offerings and niches)
        typical_offerings = {o.lower() for o in meta.get("typical_offerings", [])}
        typical_niches = {n.lower() for n in meta.get("typical_niches", [])}
        typical_roles = {r.lower() for r in meta.get("typical_roles", [])}

        # Keyword overlap with offerings
        for kw in gap_keywords:
            for offering in typical_offerings:
                if kw in offering or offering in kw:
                    score += 3.0
                    gaps_targeted.append(f"demand:{kw}")
                    break

        # Niche overlap with gap keywords
        for kw in gap_keywords:
            for niche in typical_niches:
                if kw in niche or niche in kw:
                    score += 2.0
                    gaps_targeted.append(f"niche:{kw}")
                    break

        # Missing role coverage
        for role in missing_roles:
            for tr in typical_roles:
                if role in tr or tr in role:
                    score += 2.5
                    gaps_targeted.append(f"role:{role}")
                    break

        # Underserved niche coverage
        for niche in underserved_niches:
            for tn in typical_niches:
                if niche in tn or tn in niche:
                    score += 1.5
                    gaps_targeted.append(f"underserved:{niche}")
                    break

        # Quality bonus (higher quality data = more useful)
        score *= quality

        if score > 0:
            # Deduplicate targeted gaps
            gaps_targeted = sorted(set(gaps_targeted))
            scored.append((name, round(score, 2), gaps_targeted))

    scored.sort(key=lambda x: x[1], reverse=True)
    return scored


# ---------------------------------------------------------------------------
# AI scraper creation for uncovered gaps
# ---------------------------------------------------------------------------

def _try_ai_create_for_gaps(
    snapshot_data: dict,
    scraper_metadata: dict[str, dict[str, Any]],
    max_create: int,
    dry_run: bool,
    logger: logging.Logger,
) -> list[str]:
    """Attempt AI-powered scraper creation for gaps no existing scraper covers.

    Uses the scraper_generator module to identify uncovered gaps, find matching
    potential sources, and generate complete scrapers via Claude.

    Returns list of newly created scraper names.
    """
    try:
        from scripts.sourcing.scraper_generator import (
            find_uncovered_gaps,
            recommend_sources,
            ai_create_scraper,
        )
    except ImportError:
        logger.warning("scraper_generator not available, skipping AI creation")
        return []

    uncovered = find_uncovered_gaps(snapshot_data, scraper_metadata)
    total_uncovered = (
        len(uncovered.get("uncovered_keywords", []))
        + len(uncovered.get("uncovered_roles", []))
        + len(uncovered.get("uncovered_niches", []))
    )

    if total_uncovered == 0:
        logger.info("All gaps covered by existing scrapers, no AI creation needed")
        return []

    logger.info(
        "Found %d uncovered gaps (%d keywords, %d roles, %d niches)",
        total_uncovered,
        len(uncovered.get("uncovered_keywords", [])),
        len(uncovered.get("uncovered_roles", [])),
        len(uncovered.get("uncovered_niches", [])),
    )

    existing_names = set(scraper_metadata.keys())
    recommendations = recommend_sources(uncovered, exclude_existing=existing_names)

    if not recommendations:
        logger.info("No potential sources match the uncovered gaps")
        return []

    created: list[str] = []
    for source, score, gaps in recommendations[:max_create]:
        logger.info(
            "AI-creating scraper: %s (score=%.1f, gaps=%s)",
            source.name, score, gaps,
        )

        if dry_run:
            logger.info("[DRY RUN] Would AI-create scraper: %s", source.name)
            created.append(source.name)
            continue

        try:
            result = ai_create_scraper(source.name, dry_run=False)
            if result.get("success"):
                logger.info(
                    "AI-created scraper %s: method=%s, contacts=%d",
                    source.name,
                    result.get("method", "unknown"),
                    result.get("contacts_found", 0),
                )
                created.append(source.name)
            else:
                logger.warning(
                    "AI creation failed for %s: %s",
                    source.name,
                    result.get("error", "unknown"),
                )
        except Exception as exc:
            logger.error("AI creation error for %s: %s", source.name, exc)

    return created


# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------

@task(name="gap-driven-sourcing", retries=1, retry_delay_seconds=30)
def gap_driven_sourcing(
    max_scrapers: int = 5,
    max_contacts_per_scraper: int = 500,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Select and run scrapers that best fill identified market gaps.

    Reads the latest snapshot from ``niche_statistics_snapshots``, scores
    scrapers by gap coverage, applies a quality floor (skip scrapers where
    <30% of historical profiles have seeking/offering data), and runs the
    top *max_scrapers*.

    Parameters
    ----------
    max_scrapers:
        Maximum number of scrapers to run in this cycle.
    max_contacts_per_scraper:
        Cap on contacts per scraper (0 = unlimited). Default 500 to keep
        the monthly cycle runtime bounded.
    dry_run:
        If True, log the sourcing plan but do not run any scrapers.

    Returns
    -------
    dict with: scrapers_selected, scrapers_run, profiles_sourced,
    gaps_targeted.
    """
    logger = get_run_logger()

    # ── Load latest snapshot ──────────────────────────────────────────
    conn = _get_db_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(LATEST_SNAPSHOT_QUERY)
        row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        logger.warning("No market intelligence snapshot found. Run compute_market_intelligence first.")
        return {
            "scrapers_selected": [],
            "scrapers_run": 0,
            "profiles_sourced": 0,
            "gaps_targeted": [],
            "ai_created": [],
        }

    snapshot_data = row["snapshot_data"]
    if isinstance(snapshot_data, str):
        snapshot_data = json.loads(snapshot_data)

    logger.info(
        "Loaded snapshot id=%s computed_at=%s (%d gaps, %d role gaps)",
        row["id"],
        row["computed_at"],
        len(snapshot_data.get("supply_demand_gaps", [])),
        len(snapshot_data.get("role_gaps", [])),
    )

    # ── Load scraper quality history ──────────────────────────────────
    # NOTE: The `source` column doesn't exist in the profiles table.
    # Skip gracefully until a proper scraper-tracking column is added.
    quality_map: dict[str, float] = {}
    try:
        conn = _get_db_connection()
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(SCRAPER_QUALITY_QUERY)
            for qrow in cur.fetchall():
                source = qrow["source"]
                total = qrow["total"] or 0
                with_data = qrow["with_data"] or 0
                quality_map[source] = with_data / max(total, 1)
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("Scraper quality query skipped (column may not exist): %s", exc)

    logger.info(
        "Loaded quality data for %d scraper sources", len(quality_map)
    )

    # ── Load scraper metadata and score ───────────────────────────────
    scraper_metadata = _load_scraper_metadata()
    logger.info("Registered %d scrapers", len(scraper_metadata))

    scored = _compute_scraper_scores(
        snapshot_data, scraper_metadata, quality_map
    )

    selected = scored[:max_scrapers]

    # ── AI-generate scrapers for uncovered gaps ────────────────────────
    ai_created: list[str] = []
    remaining_slots = max_scrapers - len(selected)
    if remaining_slots > 0:
        ai_created = _try_ai_create_for_gaps(
            snapshot_data, scraper_metadata, remaining_slots, dry_run, logger,
        )
        # Add AI-created scrapers to selected list with score 0 (new)
        for name in ai_created:
            selected.append((name, 0.0, ["ai_generated"]))

    if not selected:
        logger.info("No scrapers matched the identified gaps.")
        return {
            "scrapers_selected": [],
            "scrapers_run": 0,
            "profiles_sourced": 0,
            "gaps_targeted": [],
            "ai_created": [],
        }

    # ── Log the plan ──────────────────────────────────────────────────
    all_gaps_targeted: list[str] = []
    scraper_names: list[str] = []
    for name, score, gaps in selected:
        scraper_names.append(name)
        all_gaps_targeted.extend(gaps)
        logger.info(
            "Selected scraper: %s (score=%.2f, gaps=%s)",
            name, score, gaps,
        )

    all_gaps_targeted = sorted(set(all_gaps_targeted))

    if dry_run:
        logger.info(
            "[DRY RUN] Would run %d scrapers: %s",
            len(selected),
            scraper_names,
        )
        return {
            "scrapers_selected": scraper_names,
            "scrapers_run": 0,
            "profiles_sourced": 0,
            "gaps_targeted": all_gaps_targeted,
            "ai_created": ai_created,
        }

    # ── Run selected scrapers ─────────────────────────────────────────
    from scripts.sourcing.runner import run_source, SCRAPER_REGISTRY, _register_scrapers
    _register_scrapers()

    scrapers_run = 0
    total_profiles = 0

    for name, score, gaps in selected:
        if name not in SCRAPER_REGISTRY:
            logger.warning(
                "Scraper '%s' not in registry, skipping", name
            )
            continue

        logger.info(
            "Running scraper: %s (score=%.2f, targeting %d gaps)",
            name, score, len(gaps),
        )

        try:
            stats = run_source(
                source_name=name,
                batch_size=100,
                max_pages=0,
                max_contacts=max_contacts_per_scraper,
                dry_run=False,
                resume=True,
            )
            contacts_found = stats.get("contacts_valid", 0) if stats else 0
            total_profiles += contacts_found
            scrapers_run += 1
            logger.info(
                "Scraper %s complete: %d valid contacts", name, contacts_found
            )
        except Exception as exc:
            logger.error("Scraper %s failed: %s", name, exc)

    summary = {
        "scrapers_selected": scraper_names,
        "scrapers_run": scrapers_run,
        "profiles_sourced": total_profiles,
        "gaps_targeted": all_gaps_targeted,
        "ai_created": ai_created,
    }
    logger.info("Gap-driven sourcing complete: %s", summary)
    return summary
