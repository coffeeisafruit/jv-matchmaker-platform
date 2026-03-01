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
    """Open a psycopg2 connection from DATABASE_URL."""
    return psycopg2.connect(os.environ["DATABASE_URL"])


def _load_active_client_profiles() -> list[SupabaseProfile]:
    """Return SupabaseProfile objects for all clients with active reports.

    A "client" is a profile that has at least one active MemberReport
    (is_active=True).
    """
    active_report_profile_ids = (
        MemberReport.objects.filter(is_active=True)
        .exclude(supabase_profile__isnull=True)
        .values_list("supabase_profile_id", flat=True)
        .distinct()
    )
    return list(
        SupabaseProfile.objects.filter(id__in=active_report_profile_ids)
    )


def _load_profiles_by_ids(profile_ids: list[str]) -> list[SupabaseProfile]:
    """Load SupabaseProfile objects by a list of UUID strings."""
    return list(SupabaseProfile.objects.filter(id__in=profile_ids))


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


# ---------------------------------------------------------------------------
# Main scoring task
# ---------------------------------------------------------------------------

@task(name="score-against-all-clients")
def score_against_all_clients(
    profile_ids: list[str],
    score_threshold: int = 70,
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

    clients = _load_active_client_profiles()
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

    logger.info(
        "Scoring %d prospects against %d active clients (%d pairs)",
        len(prospects),
        len(clients),
        len(prospects) * len(clients),
    )

    scorer = SupabaseMatchScoringService()
    high_quality: list[NewMatchResult] = []
    total_scored = 0
    total_saved = 0

    conn = _get_connection()
    try:
        conn.autocommit = False
        cur = conn.cursor()

        for client in clients:
            for prospect in prospects:
                # Skip self-matches
                if str(client.id) == str(prospect.id):
                    continue

                try:
                    scores = scorer.score_pair(client, prospect)
                except Exception as exc:
                    logger.warning(
                        "score_pair failed for %s <-> %s: %s",
                        client.name,
                        prospect.name,
                        exc,
                    )
                    continue

                total_scored += 1
                hm = scores["harmonic_mean"]

                # Save all match records regardless of score
                _save_match_record(cur, client, prospect, scores)
                total_saved += 1

                if hm >= score_threshold:
                    high_quality.append(
                        NewMatchResult(
                            client_id=str(client.id),
                            client_name=client.name,
                            prospect_id=str(prospect.id),
                            prospect_name=prospect.name,
                            harmonic_mean=hm,
                            score_ab=scores["score_ab"],
                            score_ba=scores["score_ba"],
                        )
                    )

        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(
        "Scoring complete: %d pairs scored, %d saved, %d high-quality (>=%d)",
        total_scored,
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
