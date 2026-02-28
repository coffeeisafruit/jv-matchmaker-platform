"""
Prefect @task: Save ALL discovered prospects to the database.

Design principle: every discovered prospect gets saved, even low-scoring
ones. This builds the prospect pool for future matching as enrichment
data improves.

Deduplication strategy:
  - Check existing profiles matching on email OR website OR linkedin
    OR (name similarity via trigram if pg_trgm is available)
  - New prospects are inserted with status='Prospect'
  - Existing profiles are left untouched (no overwrites)
  - Source tracking via enrichment_metadata
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
from prefect import task, get_run_logger


# ---------------------------------------------------------------------------
# Database helper
# ---------------------------------------------------------------------------

def _get_db_connection() -> psycopg2.extensions.connection:
    """Create a new psycopg2 connection from the ``DATABASE_URL`` env var."""
    dsn = os.environ["DATABASE_URL"]
    return psycopg2.connect(dsn)


# ---------------------------------------------------------------------------
# SQL templates
# ---------------------------------------------------------------------------

_DEDUP_BY_EMAIL_SQL = """
    SELECT id FROM profiles WHERE email = %s LIMIT 1
"""

_DEDUP_BY_WEBSITE_SQL = """
    SELECT id FROM profiles WHERE website = %s LIMIT 1
"""

_DEDUP_BY_LINKEDIN_SQL = """
    SELECT id FROM profiles WHERE linkedin = %s LIMIT 1
"""

_DEDUP_BY_NAME_SQL = """
    SELECT id FROM profiles
    WHERE lower(name) = lower(%s)
    LIMIT 1
"""

_INSERT_PROFILE_SQL = """
    INSERT INTO profiles (
        id, name, email, company, website, linkedin,
        niche, what_you_do, who_you_serve,
        status, enrichment_metadata,
        created_at, updated_at
    ) VALUES (
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s,
        %s, %s
    )
"""


# ---------------------------------------------------------------------------
# Deduplication logic
# ---------------------------------------------------------------------------

def _check_duplicate(
    cursor: Any,
    prospect: dict,
) -> str | None:
    """Check if a prospect already exists in the database.

    Checks by email, website, linkedin, and exact name match.
    Returns the existing profile ID if found, None otherwise.
    """
    # Check by email
    email = (prospect.get("email") or "").strip().lower()
    if email and "@" in email:
        cursor.execute(_DEDUP_BY_EMAIL_SQL, (email,))
        row = cursor.fetchone()
        if row:
            return str(row["id"])

    # Check by website (normalize)
    website = (prospect.get("website") or "").strip().lower()
    if website:
        # Normalize: remove trailing slash, protocol
        website_normalized = website.rstrip("/")
        for prefix in ("https://www.", "http://www.", "https://", "http://"):
            if website_normalized.startswith(prefix):
                website_normalized = website_normalized[len(prefix):]
                break

        # Try exact match first
        cursor.execute(_DEDUP_BY_WEBSITE_SQL, (prospect["website"],))
        row = cursor.fetchone()
        if row:
            return str(row["id"])

        # Try with common URL variants
        for variant in [
            f"https://{website_normalized}",
            f"https://www.{website_normalized}",
            f"http://{website_normalized}",
            website_normalized,
        ]:
            cursor.execute(_DEDUP_BY_WEBSITE_SQL, (variant,))
            row = cursor.fetchone()
            if row:
                return str(row["id"])

    # Check by LinkedIn
    linkedin = (prospect.get("linkedin") or "").strip()
    if linkedin:
        cursor.execute(_DEDUP_BY_LINKEDIN_SQL, (linkedin,))
        row = cursor.fetchone()
        if row:
            return str(row["id"])

    # Check by exact name (case-insensitive)
    name = (prospect.get("name") or "").strip()
    if name and len(name) > 3:
        cursor.execute(_DEDUP_BY_NAME_SQL, (name,))
        row = cursor.fetchone()
        if row:
            return str(row["id"])

    return None


# ---------------------------------------------------------------------------
# Main ingestion task
# ---------------------------------------------------------------------------

@task(name="ingest-prospects", retries=2, retry_delay_seconds=10)
def ingest_prospects(
    prospects: list[dict],
    source: str = "acquisition",
) -> dict:
    """Save ALL discovered prospects to the database.

    - Dedup by email, website, linkedin, and name
    - Set status='Prospect' for new profiles
    - Track ingestion source in enrichment_metadata
    - Returns dict with: new_ids, duplicate_count, total_saved

    Parameters
    ----------
    prospects:
        List of prospect dicts from discovery + prescoring.
    source:
        Source label for enrichment_metadata tracking.

    Returns
    -------
    dict with:
        - new_ids: list[str] -- UUIDs of newly created profiles
        - duplicate_ids: list[str] -- UUIDs of existing profiles that matched
        - duplicate_count: int
        - total_saved: int
        - errors: int
    """
    logger = get_run_logger()

    if not prospects:
        logger.info("No prospects to ingest")
        return {
            "new_ids": [],
            "duplicate_ids": [],
            "duplicate_count": 0,
            "total_saved": 0,
            "errors": 0,
        }

    conn = _get_db_connection()
    new_ids: list[str] = []
    duplicate_ids: list[str] = []
    error_count = 0
    now = datetime.now()

    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        for prospect in prospects:
            try:
                # Check for duplicates
                existing_id = _check_duplicate(cursor, prospect)

                if existing_id:
                    duplicate_ids.append(existing_id)
                    logger.debug(
                        "Duplicate found for '%s' -> %s",
                        prospect.get("name", "?"), existing_id,
                    )
                    continue

                # Generate new profile ID
                profile_id = str(uuid.uuid4())

                # Build enrichment metadata
                enrichment_metadata = {
                    "ingestion_source": source,
                    "ingested_at": now.isoformat(),
                    "discovery_source": prospect.get("source", "unknown"),
                    "discovery_query": prospect.get("source_query", ""),
                    "pre_score": prospect.get("_pre_score"),
                    "above_threshold": prospect.get("_above_threshold", False),
                    "pipeline_version": 1,
                }

                # Clean fields for insert
                name = (prospect.get("name") or "").strip()
                if not name:
                    logger.debug("Skipping prospect with no name")
                    continue

                email = (prospect.get("email") or "").strip() or None
                company = (prospect.get("company") or "").strip() or None
                website = (prospect.get("website") or "").strip() or None
                linkedin = (prospect.get("linkedin") or "").strip() or None
                niche = (prospect.get("niche") or "").strip() or None
                what_you_do = (prospect.get("what_you_do") or "").strip() or None
                who_you_serve = (prospect.get("who_you_serve") or "").strip() or None

                cursor.execute(
                    _INSERT_PROFILE_SQL,
                    (
                        profile_id,
                        name,
                        email,
                        company,
                        website,
                        linkedin,
                        niche,
                        what_you_do,
                        who_you_serve,
                        "Prospect",  # status
                        json.dumps(enrichment_metadata),
                        now,
                        now,
                    ),
                )

                new_ids.append(profile_id)

            except Exception as exc:
                logger.warning(
                    "Failed to ingest prospect '%s': %s",
                    prospect.get("name", "?"), exc,
                )
                error_count += 1
                # Roll back to a clean state for the next prospect
                conn.rollback()
                # Re-establish cursor after rollback
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                continue

        conn.commit()

    except Exception as exc:
        logger.error("Prospect ingestion failed: %s", exc)
        conn.rollback()
        error_count = len(prospects) - len(new_ids)
    finally:
        conn.close()

    result = {
        "new_ids": new_ids,
        "duplicate_ids": duplicate_ids,
        "duplicate_count": len(duplicate_ids),
        "total_saved": len(new_ids),
        "errors": error_count,
    }

    logger.info(
        "Ingestion complete: %d new, %d duplicates, %d errors (of %d prospects)",
        len(new_ids), len(duplicate_ids), error_count, len(prospects),
    )

    return result
