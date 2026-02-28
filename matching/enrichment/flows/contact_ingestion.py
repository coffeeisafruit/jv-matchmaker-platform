"""
Contact ingestion task -- deduplicates and saves new contacts.

Receives a list of contact dicts from any source (CSV, API, manual),
checks for duplicates against existing profiles, and inserts new ones
into the profiles table with proper enrichment metadata.

Dedup checks (in order):
  1. Email exact match
  2. Website domain match (strip www., protocol)
  3. LinkedIn URL match
  4. Name + company fuzzy match (case-insensitive)

Usage (from another flow):
    from matching.enrichment.flows.contact_ingestion import ingest_contacts
    records = ingest_contacts(contacts, source="csv_import")
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
from prefect import task, get_run_logger


# ---------------------------------------------------------------------------
# Source priority hierarchy (for reference / future use)
# ---------------------------------------------------------------------------
SOURCE_PRIORITY: dict[str, int] = {
    "client_confirmed": 100,
    "client_ingest": 90,
    "manual_edit": 80,
    "csv_import": 60,
    "exa_research": 50,
    "ai_research": 40,
    "apollo": 30,
    "unknown": 0,
}


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class IngestionRecord:
    """Result of ingesting a single contact."""

    profile_id: str
    name: str
    is_new: bool  # True if newly created, False if duplicate
    duplicate_of: str = ""  # ID of existing profile if duplicate
    source: str = ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_connection() -> psycopg2.extensions.connection:
    """Open a psycopg2 connection from DATABASE_URL."""
    return psycopg2.connect(os.environ["DATABASE_URL"])


def _normalize_domain(raw: str | None) -> str:
    """Extract a bare domain from a URL or domain string.

    Examples:
        https://www.example.com/page -> example.com
        www.example.com              -> example.com
        example.com                  -> example.com
    """
    if not raw:
        return ""
    raw = raw.strip().lower()
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    try:
        host = urlparse(raw).hostname or ""
    except Exception:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


def _normalize_linkedin(raw: str | None) -> str:
    """Extract the path portion of a LinkedIn URL for comparison.

    Returns the lowercase path stripped of trailing slashes, e.g.
    ``/in/john-doe``.
    """
    if not raw:
        return ""
    raw = raw.strip().lower()
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    try:
        path = urlparse(raw).path.rstrip("/")
    except Exception:
        return ""
    return path


def _load_existing_profiles(
    cur: psycopg2.extensions.cursor,
    emails: list[str],
    domains: list[str],
    linkedin_paths: list[str],
) -> dict[str, dict[str, Any]]:
    """Batch-load existing profiles that could be duplicates.

    Returns a dict keyed by profile id (str) -> row dict.
    """
    conditions: list[str] = []
    params: list[Any] = []

    if emails:
        conditions.append("LOWER(email) = ANY(%s)")
        params.append([e.lower() for e in emails if e])
    if domains:
        conditions.append(
            "LOWER(REGEXP_REPLACE("
            "REGEXP_REPLACE(website, '^https?://', ''), "
            "'^www\\.', ''"
            ")) = ANY(%s)"
        )
        params.append([d for d in domains if d])
    if linkedin_paths:
        conditions.append(
            "LOWER(RTRIM("
            "REGEXP_REPLACE(linkedin, '^https?://(www\\.)?linkedin\\.com', ''), "
            "'/'"
            ")) = ANY(%s)"
        )
        params.append([lp for lp in linkedin_paths if lp])

    if not conditions:
        return {}

    query = (
        "SELECT id, name, email, website, linkedin, company "
        "FROM profiles WHERE " + " OR ".join(conditions)
    )
    cur.execute(query, params)
    rows = cur.fetchall()
    return {str(r["id"]): r for r in rows}


def _find_duplicate(
    contact: dict,
    existing: dict[str, dict[str, Any]],
) -> str | None:
    """Check a single contact against pre-loaded existing profiles.

    Returns the existing profile id (str) if a duplicate is found,
    otherwise ``None``.
    """
    c_email = (contact.get("email") or "").strip().lower()
    c_domain = _normalize_domain(contact.get("website"))
    c_linkedin = _normalize_linkedin(contact.get("linkedin"))
    c_name = (contact.get("name") or "").strip().lower()
    c_company = (contact.get("company") or "").strip().lower()

    for pid, row in existing.items():
        # 1. Email exact match
        if c_email and (row.get("email") or "").strip().lower() == c_email:
            return pid
        # 2. Website domain match
        if c_domain and _normalize_domain(row.get("website")) == c_domain:
            return pid
        # 3. LinkedIn URL match
        if c_linkedin and _normalize_linkedin(row.get("linkedin")) == c_linkedin:
            return pid
        # 4. Name + company fuzzy match (case-insensitive exact)
        row_name = (row.get("name") or "").strip().lower()
        row_company = (row.get("company") or "").strip().lower()
        if (
            c_name
            and c_company
            and row_name == c_name
            and row_company == c_company
        ):
            return pid

    return None


# ---------------------------------------------------------------------------
# Main task
# ---------------------------------------------------------------------------

@task(name="ingest-contacts", retries=2, retry_delay_seconds=10)
def ingest_contacts(
    contacts: list[dict],
    source: str = "csv_import",
    ingested_by: str = "",
    source_file: str = "",
) -> list[IngestionRecord]:
    """Deduplicate and save new contacts to the database.

    Dedup checks (in order):
      1. Email exact match
      2. Website domain match (strip www., protocol)
      3. LinkedIn URL match
      4. Name fuzzy match (same name + same company)

    New profiles created with:
      - status = 'Prospect'
      - enrichment_metadata.ingestion_source = source
      - enrichment_metadata.ingested_at = now
      - enrichment_metadata.ingested_by = ingested_by (if provided)
      - enrichment_metadata.original_source_file = source_file (if provided)

    Returns list of IngestionRecord (one per input contact).
    """
    logger = get_run_logger()
    logger.info("Ingesting %d contacts (source=%s)", len(contacts), source)

    if not contacts:
        return []

    # Collect candidate keys for batch dedup lookup
    emails = [c.get("email", "") for c in contacts if c.get("email")]
    domains = [_normalize_domain(c.get("website")) for c in contacts]
    linkedin_paths = [_normalize_linkedin(c.get("linkedin")) for c in contacts]

    conn = _get_connection()
    try:
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        existing = _load_existing_profiles(cur, emails, domains, linkedin_paths)
        logger.info("Loaded %d existing candidate profiles for dedup", len(existing))

        now_iso = datetime.now(timezone.utc).isoformat()
        results: list[IngestionRecord] = []

        for contact in contacts:
            dup_id = _find_duplicate(contact, existing)

            if dup_id is not None:
                results.append(
                    IngestionRecord(
                        profile_id=dup_id,
                        name=contact.get("name", ""),
                        is_new=False,
                        duplicate_of=dup_id,
                        source=source,
                    )
                )
                continue

            # -- Insert new profile --
            new_id = str(uuid.uuid4())
            metadata: dict[str, Any] = {
                "ingestion_source": source,
                "ingested_at": now_iso,
                "source_priority": SOURCE_PRIORITY.get(source, 0),
            }
            if ingested_by:
                metadata["ingested_by"] = ingested_by
            if source_file:
                metadata["original_source_file"] = source_file

            cur.execute(
                """
                INSERT INTO profiles (
                    id, name, email, company, website, linkedin,
                    phone, bio, status, enrichment_metadata, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, 'Prospect', %s, NOW(), NOW()
                )
                """,
                (
                    new_id,
                    contact.get("name", ""),
                    contact.get("email"),
                    contact.get("company"),
                    contact.get("website"),
                    contact.get("linkedin"),
                    contact.get("phone"),
                    contact.get("bio"),
                    psycopg2.extras.Json(metadata),
                ),
            )

            # Add to existing set so later contacts in same batch dedup
            existing[new_id] = {
                "id": new_id,
                "name": contact.get("name", ""),
                "email": contact.get("email"),
                "website": contact.get("website"),
                "linkedin": contact.get("linkedin"),
                "company": contact.get("company"),
            }

            results.append(
                IngestionRecord(
                    profile_id=new_id,
                    name=contact.get("name", ""),
                    is_new=True,
                    source=source,
                )
            )

        conn.commit()
        new_count = sum(1 for r in results if r.is_new)
        dup_count = sum(1 for r in results if not r.is_new)
        logger.info(
            "Ingestion complete: %d new, %d duplicates", new_count, dup_count
        )
        return results

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
