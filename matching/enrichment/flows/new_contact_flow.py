"""
New-contact ingestion flow -- end-to-end pipeline.

Orchestrates the full lifecycle of new contacts arriving from any source:
  dedup -> save -> enrich -> score against all clients -> flag reports.

Supports CSV file input via CLI or programmatic invocation from other
flows / management commands.

Usage (CLI):
    python -m matching.enrichment.flows.new_contact_flow \\
        --file contacts.csv --source csv_import --dry-run

Usage (Prefect):
    from matching.enrichment.flows.new_contact_flow import new_contact_flow
    result = new_contact_flow(contacts=[...], source="csv_import")
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass, field
from typing import Any

import psycopg2
import psycopg2.extras
from prefect import flow, get_run_logger

from matching.enrichment.flows.contact_ingestion import (
    ingest_contacts,
    IngestionRecord,
)
from matching.enrichment.flows.enrichment_flow import enrichment_flow
from matching.enrichment.flows.cross_client_scoring import (
    score_against_all_clients,
    flag_reports_for_update,
    NewMatchResult,
)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class ContactIngestionResult:
    """Summary of a contact ingestion flow run."""

    total_received: int = 0
    new_contacts: int = 0
    duplicates: int = 0
    enriched: int = 0
    high_quality_matches: int = 0
    clients_impacted: int = 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_connection() -> psycopg2.extensions.connection:
    """Open a psycopg2 connection from DATABASE_URL."""
    return psycopg2.connect(os.environ["DATABASE_URL"])


# CSV column aliases -- map common header variations to canonical keys
_COLUMN_ALIASES: dict[str, str] = {
    "full_name": "name",
    "full name": "name",
    "first_name": "name",
    "first name": "name",
    "contact_name": "name",
    "contact name": "name",
    "email_address": "email",
    "email address": "email",
    "e-mail": "email",
    "company_name": "company",
    "company name": "company",
    "organisation": "company",
    "organization": "company",
    "org": "company",
    "website_url": "website",
    "website url": "website",
    "url": "website",
    "site": "website",
    "homepage": "website",
    "linkedin_url": "linkedin",
    "linkedin url": "linkedin",
    "linkedin_profile": "linkedin",
    "linkedin profile": "linkedin",
    "li_url": "linkedin",
}

# Canonical column names accepted by ingest_contacts
_CANONICAL_COLUMNS = {"name", "email", "company", "website", "linkedin", "phone", "bio"}


def _read_csv(file_path: str) -> list[dict]:
    """Read a CSV file and normalize column names to canonical keys.

    Handles:
      - BOM-prefixed files (utf-8-sig)
      - Common column name variations via _COLUMN_ALIASES
      - Strips whitespace from values
    """
    contacts: list[dict] = []
    with open(file_path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            return contacts

        # Build a mapping from CSV header -> canonical key
        col_map: dict[str, str] = {}
        for header in reader.fieldnames:
            normalized = header.strip().lower()
            if normalized in _CANONICAL_COLUMNS:
                col_map[header] = normalized
            elif normalized in _COLUMN_ALIASES:
                col_map[header] = _COLUMN_ALIASES[normalized]

        for row in reader:
            contact: dict[str, str] = {}
            for csv_col, canon_key in col_map.items():
                val = (row.get(csv_col) or "").strip()
                if val:
                    # For "name", merge first/last if we encounter both
                    if canon_key == "name" and "name" in contact:
                        contact["name"] = f"{contact['name']} {val}"
                    else:
                        contact[canon_key] = val
            if contact.get("name"):
                contacts.append(contact)

    return contacts


def _update_status_for_qualified(
    profile_ids: list[str],
    threshold: int = 70,
) -> int:
    """Set status='Qualified' for profiles that have matches >= threshold.

    Args:
        profile_ids: UUIDs of profiles to check.
        threshold: Minimum harmonic_mean to qualify.

    Returns:
        Count of profiles whose status was updated.
    """
    if not profile_ids:
        return 0

    conn = _get_connection()
    try:
        conn.autocommit = False
        cur = conn.cursor()

        # Find profiles that have at least one high-quality match
        cur.execute(
            """
            UPDATE profiles
            SET status = 'Qualified', updated_at = NOW()
            WHERE id = ANY(%s)
              AND status = 'Prospect'
              AND id IN (
                  SELECT DISTINCT suggested_profile_id
                  FROM match_suggestions
                  WHERE suggested_profile_id = ANY(%s)
                    AND harmonic_mean >= %s
              )
            """,
            (profile_ids, profile_ids, threshold),
        )
        updated = cur.rowcount
        conn.commit()
        return updated

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(
    name="new-contact-ingestion",
    description="Ingest new contacts from any source, enrich, and score against all clients",
    retries=0,
    timeout_seconds=3600,
)
def new_contact_flow(
    contacts: list[dict],
    source: str = "csv_import",
    ingested_by: str = "",
    source_file: str = "",
    skip_enrichment: bool = False,
    dry_run: bool = False,
) -> ContactIngestionResult:
    """Process new contacts: dedup -> save -> enrich -> score -> flag reports.

    Steps:
      1. ingest_contacts() -- dedup and save
      2. enrichment_flow(profile_ids=new_ids) -- full enrichment pipeline
      3. score_against_all_clients(new_ids) -- score vs ALL active clients
      4. flag_reports_for_update() -- flag impacted reports
      5. Update status to 'Qualified' for profiles with 70+ matches

    Args:
        contacts: List of contact dicts with keys like name, email,
            company, website, linkedin.
        source: Source label (csv_import, client_ingest, etc.).
        ingested_by: Identifier of the person/system that triggered ingestion.
        source_file: Original filename (for audit trail).
        skip_enrichment: If True, skip the enrichment step.
        dry_run: If True, ingest and report what *would* happen but skip
            enrichment, scoring, and status updates.

    Returns:
        ContactIngestionResult with aggregated stats.
    """
    logger = get_run_logger()
    result = ContactIngestionResult(total_received=len(contacts))

    if not contacts:
        logger.warning("No contacts provided; nothing to do.")
        return result

    # ------------------------------------------------------------------
    # Step 1: Dedup and save
    # ------------------------------------------------------------------
    logger.info("Step 1/5: Ingesting %d contacts", len(contacts))
    records: list[IngestionRecord] = ingest_contacts(
        contacts=contacts,
        source=source,
        ingested_by=ingested_by,
        source_file=source_file,
    )

    new_ids = [r.profile_id for r in records if r.is_new]
    result.new_contacts = len(new_ids)
    result.duplicates = sum(1 for r in records if not r.is_new)

    logger.info(
        "Ingestion: %d new, %d duplicates",
        result.new_contacts,
        result.duplicates,
    )

    if not new_ids:
        logger.info("All contacts were duplicates; pipeline complete.")
        return result

    if dry_run:
        logger.info("Dry-run mode: skipping enrichment, scoring, and updates.")
        return result

    # ------------------------------------------------------------------
    # Step 2: Enrich new profiles
    # ------------------------------------------------------------------
    if not skip_enrichment:
        logger.info("Step 2/5: Enriching %d new profiles", len(new_ids))
        enrichment_result = enrichment_flow(profile_ids=new_ids)
        result.enriched = enrichment_result.profiles_written
        logger.info("Enrichment complete: %d profiles written", result.enriched)
    else:
        logger.info("Step 2/5: Enrichment skipped (skip_enrichment=True)")
        result.enriched = 0

    # ------------------------------------------------------------------
    # Step 3: Score against all active clients
    # ------------------------------------------------------------------
    logger.info("Step 3/5: Scoring %d profiles against all clients", len(new_ids))
    high_quality: list[NewMatchResult] = score_against_all_clients(
        profile_ids=new_ids,
        score_threshold=70,
    )
    result.high_quality_matches = len(high_quality)
    logger.info("Scoring complete: %d high-quality matches", len(high_quality))

    # ------------------------------------------------------------------
    # Step 4: Flag impacted reports
    # ------------------------------------------------------------------
    logger.info("Step 4/5: Flagging impacted reports")
    flagged = flag_reports_for_update(high_quality)
    result.clients_impacted = flagged
    logger.info("Flagged %d report(s) for regeneration", flagged)

    # ------------------------------------------------------------------
    # Step 5: Update status for qualified profiles
    # ------------------------------------------------------------------
    logger.info("Step 5/5: Updating status for qualified profiles")
    qualified = _update_status_for_qualified(new_ids, threshold=70)
    logger.info("Updated %d profile(s) to Qualified status", qualified)

    logger.info(
        "Pipeline complete: received=%d new=%d dup=%d enriched=%d "
        "hq_matches=%d clients_impacted=%d",
        result.total_received,
        result.new_contacts,
        result.duplicates,
        result.enriched,
        result.high_quality_matches,
        result.clients_impacted,
    )
    return result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest new contacts from a CSV file, enrich, and score.",
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Path to CSV file with contact data.",
    )
    parser.add_argument(
        "--source",
        default="csv_import",
        help="Source label (default: csv_import).",
    )
    parser.add_argument(
        "--ingested-by",
        default="",
        help="Identifier for who triggered this import.",
    )
    parser.add_argument(
        "--skip-enrichment",
        action="store_true",
        help="Skip the enrichment step.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Ingest only; skip enrichment, scoring, and status updates.",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    """CLI entry point for the new-contact ingestion flow."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if not os.path.isfile(args.file):
        print(f"Error: file not found: {args.file}", file=sys.stderr)
        sys.exit(1)

    contacts = _read_csv(args.file)
    if not contacts:
        print("No valid contacts found in CSV.", file=sys.stderr)
        sys.exit(1)

    print(f"Loaded {len(contacts)} contacts from {args.file}")

    result = new_contact_flow(
        contacts=contacts,
        source=args.source,
        ingested_by=args.ingested_by,
        source_file=os.path.basename(args.file),
        skip_enrichment=args.skip_enrichment,
        dry_run=args.dry_run,
    )

    print(
        f"\nResults:\n"
        f"  Total received:       {result.total_received}\n"
        f"  New contacts:         {result.new_contacts}\n"
        f"  Duplicates:           {result.duplicates}\n"
        f"  Enriched:             {result.enriched}\n"
        f"  High-quality matches: {result.high_quality_matches}\n"
        f"  Clients impacted:     {result.clients_impacted}"
    )


if __name__ == "__main__":
    main()
