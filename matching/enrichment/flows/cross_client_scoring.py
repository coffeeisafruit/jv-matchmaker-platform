"""
Cross-client scoring task -- score new profiles against ALL active clients.

After new contacts are ingested and enriched, this module scores every
new profile against every active client using the full ISMC framework
(SupabaseMatchScoringService.score_pair).  High-quality matches are
persisted to the match_suggestions table and affected client reports
are flagged for regeneration.

Usage (from another flow):
    from matching.enrichment.flows.cross_client_scoring import (
        score_against_all_clients,
        flag_reports_for_update,
    )
    high_quality = score_against_all_clients(new_ids, score_threshold=70)
    flagged = flag_reports_for_update(high_quality)
"""

from __future__ import annotations

import os
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras
from prefect import task, get_run_logger

from matching.services import SupabaseMatchScoringService
from matching.models import SupabaseProfile, MemberReport


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class NewMatchResult:
    """A new high-quality match found."""

    client_id: str
    client_name: str
    prospect_id: str
    prospect_name: str
    harmonic_mean: float
    score_ab: float
    score_ba: float


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_connection() -> psycopg2.extensions.connection:
    """Open a psycopg2 connection, preferring direct over pgbouncer."""
    dsn = os.environ.get("DIRECT_DATABASE_URL") or os.environ["DATABASE_URL"]
    conn = psycopg2.connect(dsn)
    conn.cursor().execute("SET statement_timeout = 0")
    return conn


def _load_active_client_profiles(client_id_filter: str | None = None) -> list[SupabaseProfile]:
    """Return SupabaseProfile objects for all clients with active reports.

    A "client" is a profile that has at least one active MemberReport
    (is_active=True). Pass client_id_filter to restrict to a single client
    (used for parallel per-client scoring runs).
    """
    qs = (
        MemberReport.objects.filter(is_active=True)
        .exclude(supabase_profile__isnull=True)
        .values_list("supabase_profile_id", flat=True)
        .distinct()
    )
    profiles_qs = SupabaseProfile.objects.filter(id__in=qs)
    if client_id_filter:
        profiles_qs = profiles_qs.filter(id=client_id_filter)
    return list(profiles_qs)


def _load_profiles_by_ids(profile_ids: list[str], chunk_size: int = 1000) -> list[SupabaseProfile]:
    """Load SupabaseProfile objects in chunks to avoid statement timeouts."""
    results = []
    for i in range(0, len(profile_ids), chunk_size):
        chunk = profile_ids[i:i + chunk_size]
        results.extend(SupabaseProfile.objects.filter(id__in=chunk))
    return results


def _save_match_record(
    cur: psycopg2.extensions.cursor,
    client: SupabaseProfile,
    prospect: SupabaseProfile,
    scores: dict[str, Any],
) -> None:
    """Insert a match_suggestions row via psycopg2."""
    match_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO match_suggestions (
            id, profile_id, suggested_profile_id,
            match_score, score_ab, score_ba, harmonic_mean,
            source, status, suggested_at,
            match_context
        ) VALUES (
            %s, %s, %s,
            %s, %s, %s, %s,
            'contact_ingestion', 'pending', NOW(),
            %s
        )
        ON CONFLICT (profile_id, suggested_profile_id)
        DO UPDATE SET
            match_score   = EXCLUDED.match_score,
            score_ab      = EXCLUDED.score_ab,
            score_ba      = EXCLUDED.score_ba,
            harmonic_mean = EXCLUDED.harmonic_mean,
            match_context = EXCLUDED.match_context,
            suggested_at  = NOW()
        """,
        (
            match_id,
            str(client.id),
            str(prospect.id),
            scores["harmonic_mean"],
            scores["score_ab"],
            scores["score_ba"],
            scores["harmonic_mean"],
            psycopg2.extras.Json({
                "source": "contact_ingestion",
                "scored_at": datetime.now(timezone.utc).isoformat(),
            }),
        ),
    )


def _score_client_batch(
    client_id: str,
    prospect_ids: list[str],
    score_threshold: int,
    lightweight_threshold: int = 35,
) -> tuple[list[dict], list[dict]]:
    """Score one client against all prospects. Runs in a subprocess.

    Two-stage scoring for speed:
      Stage 1: lightweight score all pairs — skip those clearly below threshold.
      Stage 2: full score only on candidates that passed stage 1.

    Returns (all_rows, high_quality_rows) as plain dicts (picklable).
    """
    import django
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
    django.setup()

    from matching.services import SupabaseMatchScoringService
    from matching.models import SupabaseProfile

    client = SupabaseProfile.objects.get(id=client_id)
    # Load in chunks of 500 to keep memory per worker manageable
    prospects = []
    for i in range(0, len(prospect_ids), 500):
        prospects.extend(SupabaseProfile.objects.filter(id__in=prospect_ids[i:i+500]))
    scorer = SupabaseMatchScoringService()

    rows = []
    high_quality = []
    stage1_pass = 0

    for prospect in prospects:
        if str(prospect.id) == client_id:
            continue

        # Stage 1: fast pre-screen
        try:
            lw = scorer.score_pair_lightweight(client, prospect)
        except Exception:
            continue

        if lw["harmonic_mean"] < lightweight_threshold:
            continue  # Skip — clearly not a match
        stage1_pass += 1

        # Stage 2: full ISMC score
        try:
            scores = scorer.score_pair(client, prospect)
        except Exception:
            continue

        row = {
            "client_id": str(client.id),
            "client_name": client.name,
            "prospect_id": str(prospect.id),
            "prospect_name": prospect.name,
            "harmonic_mean": scores["harmonic_mean"],
            "score_ab": scores["score_ab"],
            "score_ba": scores["score_ba"],
        }
        rows.append(row)
        if scores["harmonic_mean"] >= score_threshold:
            high_quality.append(row)

    return rows, high_quality


def _vector_pre_filter(
    prospects: list,
    top_k: int = 200,
) -> list:
    """Pre-filter prospects by embedding similarity (not yet implemented).

    Activate after running: python3 manage.py backfill_embeddings
    Then flip pre_filter="vector" in monthly orchestrator / score_new_enrichments.
    """
    raise NotImplementedError(
        "Vector pre-filter requires embedding backfill. "
        "Run: python3 manage.py backfill_embeddings --tier B --tier C "
        "then set pre_filter='vector'."
    )


# ---------------------------------------------------------------------------
# Main scoring task
# ---------------------------------------------------------------------------

@task(name="score-against-all-clients")
def score_against_all_clients(
    profile_ids: list[str],
    score_threshold: int = 64,
    pre_filter: str = "none",
    pre_filter_top_k: int = 200,
    client_id_filter: str | None = None,
) -> list[NewMatchResult]:
    """Score new profiles against ALL active clients using full ISMC.

    Steps:
      1. Load all active client profiles (those with active MemberReports).
      2. Load the new profiles by IDs.
      3. For each (client, new_profile) pair, run
         SupabaseMatchScoringService.score_pair().
      4. Save match_suggestions records for all scores.
      5. Return list of NewMatchResult for high-quality matches
         (harmonic_mean >= threshold).

    Args:
        profile_ids: UUIDs of newly ingested/enriched profiles.
        score_threshold: Minimum harmonic_mean to qualify as high-quality.

    Returns:
        List of NewMatchResult for matches meeting the threshold.
    """
    logger = get_run_logger()

    clients = _load_active_client_profiles(client_id_filter=client_id_filter)
    if not clients:
        logger.warning("No active client profiles found; skipping scoring.")
        return []

    prospects = _load_profiles_by_ids(profile_ids)
    if not prospects:
        logger.warning("No prospect profiles found for IDs; skipping scoring.")
        return []

    # Pre-scoring enrichment filter: remove profiles with no usable text
    from matching.services import ProfileEnrichmentFilter
    eligible_prospects, candidates, ineligible = ProfileEnrichmentFilter.filter_scoreable_profiles(prospects)
    if candidates:
        logger.info(
            "Enrichment filter: %d candidates flagged for future enrichment",
            len(candidates),
        )
    if ineligible:
        logger.info(
            "Enrichment filter: %d ineligible profiles excluded (no scoreable text)",
            len(ineligible),
        )
    prospects = eligible_prospects

    if not prospects:
        logger.warning("No eligible prospects after enrichment filter; skipping scoring.")
        return []

    pair_count = len(prospects) * len(clients)
    logger.info(
        "Scoring %d prospects against %d active clients (%d pairs, pre_filter=%s)",
        len(prospects),
        len(clients),
        pair_count,
        pre_filter,
    )

    if pre_filter == "vector":
        prospects = _vector_pre_filter(prospects, pre_filter_top_k)

    high_quality: list[NewMatchResult] = []
    total_saved = 0
    COMMIT_EVERY = 500

    scorer = SupabaseMatchScoringService()
    logger.info("Scoring %d clients × %d prospects with 2-stage filter (lw_threshold=35)",
                len(clients), len(prospects))

    conn = _get_connection()
    try:
        conn.autocommit = False
        cur = conn.cursor()

        for client in clients:
            client_saved = 0
            for prospect in prospects:
                if str(client.id) == str(prospect.id):
                    continue

                # Stage 1: fast pre-screen
                try:
                    lw = scorer.score_pair_lightweight(client, prospect)
                except Exception:
                    continue
                if lw["harmonic_mean"] < 35:
                    continue

                # Stage 2: full ISMC
                try:
                    scores = scorer.score_pair(client, prospect)
                except Exception as exc:
                    logger.warning("score_pair failed %s<->%s: %s", client.name, prospect.name, exc)
                    continue

                _save_match_record(cur, client, prospect, scores)
                total_saved += 1
                client_saved += 1

                if scores["harmonic_mean"] >= score_threshold:
                    high_quality.append(NewMatchResult(
                        client_id=str(client.id),
                        client_name=client.name,
                        prospect_id=str(prospect.id),
                        prospect_name=prospect.name,
                        harmonic_mean=scores["harmonic_mean"],
                        score_ab=scores["score_ab"],
                        score_ba=scores["score_ba"],
                    ))

                if total_saved % COMMIT_EVERY == 0:
                    conn.commit()
                    logger.info("Progress: %d pairs saved, %d high-quality", total_saved, len(high_quality))

            conn.commit()
            logger.info("Client %s done: %d pairs saved", client.name, client_saved)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(
        "Scoring complete: %d pairs saved, %d high-quality (>=%d)",
        total_saved,
        len(high_quality),
        score_threshold,
    )
    return high_quality


# ---------------------------------------------------------------------------
# Report flagging task
# ---------------------------------------------------------------------------

@task(name="flag-reports-for-update")
def flag_reports_for_update(high_quality_matches: list[NewMatchResult]) -> int:
    """Flag client reports that gained new high-quality matches for regeneration.

    For each unique client in the high-quality matches list, mark their
    active MemberReport(s) as needing regeneration by writing a
    ``needs_regeneration`` flag into the report's JSON data field or
    updating the report record directly.

    Args:
        high_quality_matches: List of NewMatchResult from scoring.

    Returns:
        Count of reports flagged for regeneration.
    """
    logger = get_run_logger()

    if not high_quality_matches:
        logger.info("No high-quality matches; no reports to flag.")
        return 0

    # Unique client IDs that received new high-quality matches
    impacted_client_ids = list(
        {m.client_id for m in high_quality_matches}
    )

    logger.info(
        "Flagging reports for %d impacted clients", len(impacted_client_ids)
    )

    conn = _get_connection()
    try:
        conn.autocommit = False
        cur = conn.cursor()

        # Build a summary per client for the regeneration note
        client_new_matches: dict[str, list[str]] = {}
        for m in high_quality_matches:
            client_new_matches.setdefault(m.client_id, []).append(
                f"{m.prospect_name} ({m.harmonic_mean:.1f})"
            )

        flagged = 0
        now_iso = datetime.now(timezone.utc).isoformat()

        for client_id in impacted_client_ids:
            # Find the active MemberReport(s) for this client
            reports = MemberReport.objects.filter(
                supabase_profile_id=client_id,
                is_active=True,
            )
            for report in reports:
                # Update client_profile JSON to include regen flag
                profile_data = report.client_profile or {}
                profile_data["_needs_regeneration"] = True
                profile_data["_regen_reason"] = "new_high_quality_matches"
                profile_data["_regen_flagged_at"] = now_iso
                profile_data["_new_matches"] = client_new_matches.get(
                    client_id, []
                )

                cur.execute(
                    """
                    UPDATE matching_memberreport
                    SET client_profile = %s
                    WHERE id = %s
                    """,
                    (psycopg2.extras.Json(profile_data), report.id),
                )
                flagged += 1

        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("Flagged %d report(s) for regeneration", flagged)
    return flagged
