#!/usr/bin/env python3
"""
Bulk import the merged CSV into Supabase, bypassing Prefect.

Uses psycopg2 directly with the same dedup logic as contact_ingestion.py.

Usage:
    venv/bin/python scripts/sourcing/import_csv.py              # Import MERGED_ALL.csv
    venv/bin/python scripts/sourcing/import_csv.py --dry-run    # Preview only
    venv/bin/python scripts/sourcing/import_csv.py --file apple_podcasts_v2.csv  # Specific file
"""

from __future__ import annotations

import csv
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

# Load .env
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from dotenv import load_dotenv
    load_dotenv(project_root / ".env")
except ImportError:
    pass

import psycopg2
import psycopg2.extras


BATCH_SIZE = 200
OUTPUT_DIR = Path(__file__).parent / "output"
DEFAULT_FILE = "MERGED_ALL.csv"


# ---------------------------------------------------------------------------
# Helpers (copied from contact_ingestion.py to avoid Prefect import)
# ---------------------------------------------------------------------------

def _normalize_domain(raw: str | None) -> str:
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


def get_connection():
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("ERROR: DATABASE_URL not set in environment.")
        sys.exit(1)
    return psycopg2.connect(db_url)


def load_existing_profiles(cur):
    """Load all existing profiles for dedup."""
    cur.execute("SELECT id, name, email, website, linkedin, company FROM profiles")
    rows = cur.fetchall()
    return {str(r["id"]): r for r in rows}


def find_duplicate(contact: dict, existing: dict) -> str | None:
    c_email = (contact.get("email") or "").strip().lower()
    c_domain = _normalize_domain(contact.get("website"))
    c_linkedin = _normalize_linkedin(contact.get("linkedin"))
    c_name = (contact.get("name") or "").strip().lower()
    c_company = (contact.get("company") or "").strip().lower()

    for pid, row in existing.items():
        if c_email and (row.get("email") or "").strip().lower() == c_email:
            return pid
        if c_domain and _normalize_domain(row.get("website")) == c_domain:
            return pid
        if c_linkedin and _normalize_linkedin(row.get("linkedin")) == c_linkedin:
            return pid
        row_name = (row.get("name") or "").strip().lower()
        row_company = (row.get("company") or "").strip().lower()
        if c_name and c_company and row_name == c_name and row_company == c_company:
            return pid

    return None


# ---------------------------------------------------------------------------
# Main import
# ---------------------------------------------------------------------------

def import_csv(csv_path: Path, dry_run: bool = False) -> tuple[int, int]:
    """Import a CSV file into Supabase. Returns (total, new_count)."""
    rows = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            contact = {
                "name": (row.get("name") or "").strip(),
                "email": (row.get("email") or "").strip() or None,
                "company": (row.get("company") or "").strip() or None,
                "website": (row.get("website") or "").strip() or None,
                "linkedin": (row.get("linkedin") or "").strip() or None,
                "phone": (row.get("phone") or "").strip() or None,
                "bio": (row.get("bio") or "").strip() or None,
            }
            # Skip None values that are literally "None"
            for key in contact:
                if contact[key] == "None":
                    contact[key] = None
            if contact["name"] and len(contact["name"]) >= 2:
                rows.append(contact)

    total = len(rows)
    print(f"  Loaded {total:,} valid contacts from {csv_path.name}")

    if dry_run:
        print(f"  [DRY RUN] Would import {total:,} contacts")
        return total, 0

    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print("  Loading existing profiles for dedup...")
    existing = load_existing_profiles(cur)
    print(f"  Found {len(existing):,} existing profiles")

    now_iso = datetime.now(timezone.utc).isoformat()
    new_count = 0
    dup_count = 0
    source = f"scraper_bulk_import"

    for i, contact in enumerate(rows):
        dup_id = find_duplicate(contact, existing)

        if dup_id is not None:
            dup_count += 1
        else:
            new_id = str(uuid.uuid4())
            metadata = {
                "ingestion_source": source,
                "ingested_at": now_iso,
                "source_priority": 20,
                "original_source_file": csv_path.name,
            }

            cur.execute(
                """
                INSERT INTO profiles (
                    id, name, email, company, website, linkedin,
                    phone, bio, status, enrichment_metadata, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, 'Pending', %s, NOW(), NOW()
                )
                """,
                (
                    new_id,
                    contact["name"],
                    contact.get("email"),
                    contact.get("company"),
                    contact.get("website"),
                    contact.get("linkedin"),
                    contact.get("phone"),
                    contact.get("bio"),
                    psycopg2.extras.Json(metadata),
                ),
            )

            existing[new_id] = {
                "id": new_id,
                "name": contact["name"],
                "email": contact.get("email"),
                "website": contact.get("website"),
                "linkedin": contact.get("linkedin"),
                "company": contact.get("company"),
            }
            new_count += 1

        if (i + 1) % 1000 == 0:
            conn.commit()
            print(f"    Progress: {i+1:,}/{total:,} processed, {new_count:,} new, {dup_count:,} dups")

    conn.commit()
    cur.close()
    conn.close()

    return total, new_count


def main():
    dry_run = "--dry-run" in sys.argv

    target_file = DEFAULT_FILE
    for i, arg in enumerate(sys.argv[1:]):
        if arg == "--file" and i + 2 < len(sys.argv):
            target_file = sys.argv[i + 2]

    csv_path = OUTPUT_DIR / target_file
    if not csv_path.exists():
        print(f"ERROR: {csv_path} not found")
        sys.exit(1)

    print(f"\n{'=' * 60}")
    print("BULK IMPORT TO SUPABASE")
    print(f"{'=' * 60}")
    print(f"  File: {csv_path.name}")
    print(f"  Dry run: {dry_run}")
    print()

    total, new_count = import_csv(csv_path, dry_run=dry_run)

    print(f"\n{'=' * 60}")
    print("IMPORT COMPLETE")
    print(f"  Total processed: {total:,}")
    print(f"  New profiles:    {new_count:,}")
    print(f"  Duplicates:      {total - new_count:,}")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
