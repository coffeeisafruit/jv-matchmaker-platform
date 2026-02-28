"""
Main enrichment pipeline — Prefect @flow.

Orchestrates the full enrichment pipeline by composing the individual
@task modules: profile selection → email discovery → AI research →
validation → consolidation → (optional) retry.

Supports two entry modes:
  1. Batch mode: select profiles by tier/priority (default)
  2. Targeted mode: enrich specific profile_ids (acquisition-triggered)

Usage (CLI):
    python -m matching.enrichment.flows.enrichment_flow --limit 50

Usage (Prefect):
    from matching.enrichment.flows.enrichment_flow import enrichment_flow
    enrichment_flow(limit=50, priority="tiered")
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from prefect import flow, get_run_logger
from prefect.futures import as_completed

from matching.enrichment.flows.profile_selection import (
    select_profiles,
    get_profiles_by_ids,
)
from matching.enrichment.flows.email_discovery import (
    discover_email,
    apollo_bulk_enrich,
)
from matching.enrichment.flows.ai_research_task import (
    ai_research_profile,
    AIResearchResult,
)
from matching.enrichment.flows.validation_task import (
    validate_batch,
    write_quarantine,
    ValidationResult,
)
from matching.enrichment.flows.consolidation_task import consolidate_to_db


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class EnrichmentFlowResult:
    """Summary of a single enrichment flow run."""

    profiles_selected: int = 0
    profiles_researched: int = 0
    emails_discovered: int = 0
    verified: int = 0
    unverified: int = 0
    quarantined: int = 0
    profiles_written: int = 0
    failed_writes: int = 0
    total_cost: float = 0.0
    runtime_seconds: float = 0.0
    quarantine_file: str = ""


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(
    name="enrichment-pipeline",
    description="Full enrichment pipeline: select → email → research → validate → write",
    retries=0,
    timeout_seconds=3600,  # 1 hour max
)
def enrichment_flow(
    limit: int = 50,
    priority: str = "tiered",
    tier_filter: set[int] | None = None,
    profile_ids: list[str] | None = None,
    refresh_mode: bool = False,
    stale_days: int = 30,
    max_apollo_credits: int = 0,
    cascade: bool = False,
    skip_social_reach: bool = False,
    exa_only: bool = False,
    cache_only: bool = False,
    enable_ai_verification: bool = False,
    concurrency: int = 5,
    dry_run: bool = False,
) -> EnrichmentFlowResult:
    """Run the full enrichment pipeline.

    Args:
        limit: Max profiles to process.
        priority: Selection strategy ('tiered', 'high-value', 'has-website').
        tier_filter: Only process these tiers (e.g. {0, 1, 2}).
        profile_ids: Explicit profile IDs to enrich (overrides selection).
        refresh_mode: Re-enrich stale profiles using source-priority checks.
        stale_days: Profiles older than this are considered stale.
        max_apollo_credits: Cap on Apollo API credits.
        cascade: Enable Exa → Apollo cascade for email discovery.
        skip_social_reach: Skip social reach signal extraction.
        exa_only: Only use Exa.ai (no crawl4ai/Claude fallback).
        cache_only: Only use cached results (no live API calls).
        enable_ai_verification: Run L3 AI verification (slower, optional).
        concurrency: Max parallel AI research tasks.
        dry_run: If True, skip DB writes.

    Returns:
        EnrichmentFlowResult with aggregated stats.
    """
    logger = get_run_logger()
    start_time = time.time()
    result = EnrichmentFlowResult()

    # ------------------------------------------------------------------
    # Step 1: Select profiles
    # ------------------------------------------------------------------
    if profile_ids:
        logger.info("Targeted enrichment: %d profile IDs", len(profile_ids))
        profiles = get_profiles_by_ids(profile_ids)
    else:
        logger.info(
            "Batch enrichment: limit=%d, priority=%s, refresh=%s",
            limit, priority, refresh_mode,
        )
        profiles = select_profiles(
            limit=limit,
            priority=priority,
            tier_filter=tier_filter,
            refresh_mode=refresh_mode,
            stale_days=stale_days,
            profile_ids=profile_ids,
        )

    result.profiles_selected = len(profiles)
    if not profiles:
        logger.info("No profiles to enrich — done")
        return result

    # Log tier breakdown
    tier_counts: dict[int, int] = {}
    for p in profiles:
        t = p.get("_tier", 0)
        tier_counts[t] = tier_counts.get(t, 0) + 1
    for t in sorted(tier_counts):
        logger.info("Tier %d: %d profiles", t, tier_counts[t])

    # ------------------------------------------------------------------
    # Step 2: Email discovery (parallel via task mapping)
    # ------------------------------------------------------------------
    logger.info("Starting email discovery for %d profiles", len(profiles))
    email_futures = discover_email.map(profiles)
    profiles_with_email = [f.result() for f in email_futures]

    emails_found = sum(1 for p in profiles_with_email if p.get("email"))
    result.emails_discovered = emails_found
    logger.info("Email discovery: %d/%d found", emails_found, len(profiles))

    # Optional: Apollo cascade for profiles still missing email
    if cascade and max_apollo_credits > 0:
        missing_email = [p for p in profiles_with_email if not p.get("email")]
        if missing_email:
            logger.info("Apollo cascade: %d profiles need email", len(missing_email))
            profiles_with_email_after = apollo_bulk_enrich(
                profiles_with_email, max_credits=max_apollo_credits
            )
            # Replace with Apollo-enriched versions
            profiles_with_email = profiles_with_email_after
            new_emails = sum(1 for p in profiles_with_email if p.get("email"))
            logger.info(
                "After Apollo: %d/%d have emails (+%d)",
                new_emails, len(profiles), new_emails - emails_found,
            )
            result.emails_discovered = new_emails

    # ------------------------------------------------------------------
    # Step 3: AI research (parallel, respecting concurrency limit)
    # ------------------------------------------------------------------
    logger.info(
        "Starting AI research for %d profiles (concurrency=%d)",
        len(profiles_with_email), concurrency,
    )

    # Submit all research tasks as futures for parallel execution
    research_futures = ai_research_profile.map(
        profiles_with_email,
        refresh_mode=refresh_mode,
        skip_social_reach=skip_social_reach,
        exa_only=exa_only,
        cache_only=cache_only,
    )

    research_results: list[AIResearchResult] = []
    for future in research_futures:
        try:
            res = future.result()
            research_results.append(res)
        except Exception as exc:
            logger.error("AI research task failed: %s", exc)

    succeeded = sum(1 for r in research_results if r.success)
    total_cost = sum(r.cost for r in research_results)
    result.profiles_researched = succeeded
    result.total_cost = total_cost
    logger.info(
        "AI research: %d/%d succeeded, cost=$%.3f",
        succeeded, len(research_results), total_cost,
    )

    # ------------------------------------------------------------------
    # Step 4: Merge research results back into profile dicts
    # ------------------------------------------------------------------
    enriched_results = _merge_research_into_profiles(
        profiles_with_email, research_results
    )
    logger.info("Merged %d enriched profiles for validation", len(enriched_results))

    # ------------------------------------------------------------------
    # Step 5: Validation gate
    # ------------------------------------------------------------------
    logger.info("Running validation gate")
    validations = validate_batch(
        enriched_results,
        enable_ai_verification=enable_ai_verification,
    )

    # Apply validation results: update emails, filter quarantined
    quarantined = []
    for vr, enriched in zip(validations, enriched_results):
        if vr.status == "quarantined":
            quarantined.append(vr)
            enriched["_quarantined"] = True
            enriched["email"] = None  # Don't write quarantined email
        elif vr.status == "verified":
            result.verified += 1
            if vr.email:
                enriched["email"] = vr.email  # Use fixed email
        else:  # unverified
            result.unverified += 1
            if vr.email:
                enriched["email"] = vr.email

    result.quarantined = len(quarantined)
    logger.info(
        "Validation: %d verified, %d unverified, %d quarantined",
        result.verified, result.unverified, result.quarantined,
    )

    # ------------------------------------------------------------------
    # Step 6: Write quarantine file
    # ------------------------------------------------------------------
    if quarantined:
        qfile = write_quarantine(quarantined, enriched_results)
        result.quarantine_file = qfile

    # ------------------------------------------------------------------
    # Step 7: Consolidate to database
    # ------------------------------------------------------------------
    if dry_run:
        logger.info("DRY RUN — skipping DB write")
    else:
        # Filter out quarantined profiles for the DB write
        writable = [r for r in enriched_results if not r.get("_quarantined")]
        if writable:
            logger.info("Writing %d profiles to database", len(writable))
            write_stats = consolidate_to_db(
                writable,
                validation_results=validations,
                refresh_mode=refresh_mode,
                stale_days=stale_days,
            )
            result.profiles_written = write_stats.get("profiles_updated", 0)
            result.failed_writes = write_stats.get("failed", 0)
        else:
            logger.info("No writable profiles after validation")

    # ------------------------------------------------------------------
    # Done
    # ------------------------------------------------------------------
    result.runtime_seconds = time.time() - start_time
    logger.info(
        "Enrichment complete: %d selected, %d researched, %d written, "
        "$%.3f cost, %.1fs runtime",
        result.profiles_selected,
        result.profiles_researched,
        result.profiles_written,
        result.total_cost,
        result.runtime_seconds,
    )
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _merge_research_into_profiles(
    profiles: list[dict],
    research_results: list[AIResearchResult],
) -> list[dict]:
    """Merge AI research results back into the profile dicts.

    Matches by profile_id. Fields from the research result are added
    to (or overwrite) the profile dict, following the same field layout
    as the monolithic pipeline's run() method.
    """
    # Build lookup by profile_id
    research_by_id: dict[str, AIResearchResult] = {
        r.profile_id: r for r in research_results if r.success and r.enriched_data
    }

    merged: list[dict] = []
    for profile in profiles:
        pid = str(profile.get("id", ""))
        result_dict = {
            "profile_id": pid,
            "name": profile.get("name", ""),
            "company": profile.get("company", ""),
            "email": profile.get("email"),
            "method": profile.get("email_method"),
            "list_size": profile.get("list_size", 0),
            "enriched_at": datetime.now().isoformat(),
            "_tier": profile.get("_tier", 0),
            "enrichment_metadata": profile.get("enrichment_metadata"),
        }

        # Carry forward scraped secondary data
        for key in ("_scraped_secondary_emails", "_scraped_phone", "_scraped_booking_link"):
            if profile.get(key):
                # Map to consolidated field names
                clean_key = key.replace("_scraped_", "")
                result_dict[clean_key] = profile[key]

        # Merge AI research enrichment
        research = research_by_id.get(pid)
        if research and research.enriched_data:
            enriched = research.enriched_data

            # Core text fields
            for field in ("what_you_do", "who_you_serve", "seeking", "offering",
                          "bio", "social_proof"):
                if enriched.get(field):
                    result_dict[field] = enriched[field]

            # New profile fields
            for field in ("signature_programs", "booking_link", "niche", "phone",
                          "current_projects", "company", "business_size"):
                if enriched.get(field):
                    result_dict[field] = enriched[field]

            # Categorization fields
            if enriched.get("tags"):
                result_dict["tags"] = enriched["tags"]
            for field in ("audience_type", "business_focus", "service_provided"):
                if enriched.get(field):
                    result_dict[field] = enriched[field]

            # Integer fields
            enriched_list_size = enriched.get("list_size")
            if enriched_list_size is not None:
                try:
                    ls = int(enriched_list_size)
                    if ls > 0:
                        result_dict["enriched_list_size"] = ls
                except (ValueError, TypeError):
                    pass

            # Extended signals
            for field in ("revenue_tier", "jv_history", "content_platforms"):
                if enriched.get(field):
                    result_dict[field] = enriched[field]
            if enriched.get("audience_engagement_score") is not None:
                result_dict["audience_engagement_score"] = enriched["audience_engagement_score"]

            enriched_social_reach = enriched.get("social_reach")
            if enriched_social_reach is not None:
                try:
                    sr = int(enriched_social_reach)
                    if sr > 0:
                        result_dict["social_reach"] = sr
                except (ValueError, TypeError):
                    pass

            # Exa-discovered email: primary if empty, secondary if different
            exa_email = (enriched.get("email") or "").strip()
            if exa_email and "@" in exa_email:
                current_email = (result_dict.get("email") or "").strip().lower()
                if not current_email:
                    result_dict["exa_email"] = exa_email
                elif exa_email.lower() != current_email:
                    result_dict.setdefault("secondary_emails", []).append(exa_email)

            # Discovered website/linkedin
            if enriched.get("website") and not profile.get("website"):
                result_dict["discovered_website"] = enriched["website"]
            if enriched.get("linkedin") and not profile.get("linkedin"):
                result_dict["discovered_linkedin"] = enriched["linkedin"]

            # Extraction metadata for verification gate
            if enriched.get("_extraction_metadata"):
                result_dict["_extraction_metadata"] = enriched["_extraction_metadata"]

        merged.append(result_dict)

    return merged


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import os

    if not os.environ.get("DATABASE_URL"):
        print("ERROR: DATABASE_URL environment variable is not set.")
        raise SystemExit(1)

    parser = argparse.ArgumentParser(description="Prefect enrichment pipeline")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--priority", type=str, default="tiered",
                        choices=["tiered", "high-value", "has-website", "all"])
    parser.add_argument("--tier", type=int, default=None)
    parser.add_argument("--tiers", type=str, default=None)
    parser.add_argument("--max-apollo-credits", type=int, default=0)
    parser.add_argument("--cascade", action="store_true")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--stale-days", type=int, default=30)
    parser.add_argument("--skip-social-reach", action="store_true")
    parser.add_argument("--exa-only", action="store_true")
    parser.add_argument("--cache-only", action="store_true")
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    tier_filter = None
    if args.tier is not None:
        tier_filter = {args.tier}
    elif args.tiers:
        tier_filter = {int(t.strip()) for t in args.tiers.split(",")}

    result = enrichment_flow(
        limit=args.limit,
        priority=args.priority,
        tier_filter=tier_filter,
        refresh_mode=args.refresh,
        stale_days=args.stale_days,
        max_apollo_credits=args.max_apollo_credits,
        cascade=args.cascade,
        skip_social_reach=args.skip_social_reach,
        exa_only=args.exa_only,
        cache_only=args.cache_only,
        concurrency=args.concurrency,
        dry_run=args.dry_run,
    )
    print(f"\nResult: {result}")
