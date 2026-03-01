#!/usr/bin/env python3
"""
Bulk CSV-to-Supabase import pipeline.

Reads all CSVs from Filling Database/ and imports them into the profiles
table via the contact_ingestion pipeline with full deduplication.

Usage:
    python3 "Filling Database/supabase/import_all.py"
    python3 "Filling Database/supabase/import_all.py" --dry-run
    python3 "Filling Database/supabase/import_all.py" --file ../partners/sam_gov.csv
    python3 "Filling Database/supabase/import_all.py" --skip-ingestion
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Set Django settings module
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "jvmatch.settings")

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    print("Warning: python-dotenv not installed, skipping .env load")

# Import Django and ingestion pipeline
import django
django.setup()

from matching.enrichment.flows.contact_ingestion import ingest_contacts


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BATCH_SIZE = 500
STATE_FILE = Path(__file__).resolve().parent / ".import_state.json"

# CSV directories to scan
CSV_DIRS = [
    Path(__file__).resolve().parent.parent / "partners",
    Path(__file__).resolve().parent.parent / "chambers",
]


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class FileStats:
    """Statistics for a single CSV file import."""
    filename: str
    total_rows: int = 0
    new_count: int = 0
    dup_count: int = 0
    error_count: int = 0
    completed: bool = False
    errors: list[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


@dataclass
class ImportSummary:
    """Overall import summary."""
    total_files: int = 0
    total_rows: int = 0
    total_new: int = 0
    total_dups: int = 0
    total_errors: int = 0
    files_completed: int = 0
    files_failed: int = 0


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def load_state() -> dict[str, dict[str, Any]]:
    """Load import state from JSON file."""
    if not STATE_FILE.exists():
        return {}

    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: Failed to load state file: {e}")
        return {}


def save_state(state: dict[str, dict[str, Any]]) -> None:
    """Save import state to JSON file."""
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"Warning: Failed to save state file: {e}")


def update_file_state(state: dict, filename: str, stats: FileStats) -> None:
    """Update state for a specific file."""
    state[filename] = {
        "total_rows": stats.total_rows,
        "new_count": stats.new_count,
        "dup_count": stats.dup_count,
        "error_count": stats.error_count,
        "completed": stats.completed,
        "errors": stats.errors,
    }
    save_state(state)


# ---------------------------------------------------------------------------
# CSV processing
# ---------------------------------------------------------------------------

def derive_source_name(csv_path: Path) -> str:
    """Derive source name from CSV filename.

    Examples:
        sam_gov.csv -> scraper_sam_gov
        chamber_data.csv -> scraper_chamber_data
    """
    stem = csv_path.stem  # filename without extension
    return f"scraper_{stem}"


def read_csv_file(csv_path: Path) -> list[dict]:
    """Read a CSV file and return list of contact dicts.

    Expected headers: name,email,company,website,linkedin,phone,bio,source,source_url
    Missing columns will be filled with empty strings.
    """
    contacts = []

    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            # Validate headers (warn but don't fail on missing columns)
            expected = {"name", "email", "company", "website", "linkedin", "phone", "bio"}
            headers = set(reader.fieldnames or [])

            # Only require 'name' as mandatory
            if "name" not in headers:
                raise ValueError(f"CSV missing required 'name' column")

            missing = expected - headers
            if missing:
                print(f"  Info: Missing optional columns: {missing}")

            for row in reader:
                # Build contact dict with only the fields we need
                # Use .get() with default "" for all fields to handle missing columns
                contact = {
                    "name": row.get("name", "").strip(),
                    "email": row.get("email", "").strip(),
                    "company": row.get("company", "").strip(),
                    "website": row.get("website", "").strip(),
                    "linkedin": row.get("linkedin", "").strip(),
                    "phone": row.get("phone", "").strip(),
                    "bio": row.get("bio", "").strip(),
                }

                # Skip completely empty rows
                if not any(contact.values()):
                    continue

                contacts.append(contact)

        return contacts

    except Exception as e:
        print(f"  Error reading {csv_path.name}: {e}")
        raise


def process_csv_file(
    csv_path: Path,
    state: dict,
    dry_run: bool = False,
    skip_ingestion: bool = False,
) -> FileStats:
    """Process a single CSV file with batch ingestion.

    Returns FileStats with counts and status.
    """
    filename = csv_path.name
    stats = FileStats(filename=filename)

    # Check if already completed
    if filename in state and state[filename].get("completed"):
        print(f"\n[SKIP] {filename} - already completed")
        prev = state[filename]
        stats.total_rows = prev.get("total_rows", 0)
        stats.new_count = prev.get("new_count", 0)
        stats.dup_count = prev.get("dup_count", 0)
        stats.completed = True
        return stats

    print(f"\n[PROCESSING] {filename}")

    try:
        # Read CSV
        contacts = read_csv_file(csv_path)
        stats.total_rows = len(contacts)
        print(f"  Read {stats.total_rows} rows")

        if skip_ingestion:
            print(f"  Skipping ingestion (--skip-ingestion)")
            stats.completed = True
            return stats

        if dry_run:
            print(f"  Dry run - would import {stats.total_rows} contacts")
            stats.completed = True
            return stats

        # Derive source name
        source = derive_source_name(csv_path)

        # Process in batches
        total_batches = (len(contacts) + BATCH_SIZE - 1) // BATCH_SIZE

        for i in range(0, len(contacts), BATCH_SIZE):
            batch_num = i // BATCH_SIZE + 1
            batch = contacts[i:i + BATCH_SIZE]

            print(f"  Batch {batch_num}/{total_batches} ({len(batch)} contacts)...", end=" ", flush=True)

            try:
                # Call ingestion pipeline
                results = ingest_contacts(
                    contacts=batch,
                    source=source,
                    source_file=str(csv_path),
                )

                # Count results
                batch_new = sum(1 for r in results if r.is_new)
                batch_dup = sum(1 for r in results if not r.is_new)

                stats.new_count += batch_new
                stats.dup_count += batch_dup

                print(f"✓ ({batch_new} new, {batch_dup} dup)")

            except Exception as e:
                stats.error_count += len(batch)
                error_msg = f"Batch {batch_num} failed: {e}"
                stats.errors.append(error_msg)
                print(f"✗ {e}")
                continue

        # Mark as completed
        stats.completed = True
        print(f"  Total: {stats.new_count} new, {stats.dup_count} duplicates, {stats.error_count} errors")

    except Exception as e:
        stats.errors.append(f"File processing failed: {e}")
        print(f"  FAILED: {e}")

    # Update state
    update_file_state(state, filename, stats)

    return stats


# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------

def find_csv_files(single_file: str | None = None) -> list[Path]:
    """Find all CSV files to process."""
    if single_file:
        # Process single file
        file_path = Path(single_file)
        if not file_path.is_absolute():
            # Resolve relative to script location
            file_path = (Path(__file__).resolve().parent / single_file).resolve()

        if not file_path.exists():
            print(f"Error: File not found: {file_path}")
            sys.exit(1)

        return [file_path]

    # Scan all CSV directories
    csv_files = []
    for csv_dir in CSV_DIRS:
        if not csv_dir.exists():
            print(f"Warning: Directory not found: {csv_dir}")
            continue

        # Find all .csv files recursively
        csv_files.extend(csv_dir.rglob("*.csv"))

    return sorted(csv_files)


def print_summary(summary: ImportSummary, dry_run: bool = False, skip_ingestion: bool = False) -> None:
    """Print final summary report."""
    print("\n" + "=" * 70)
    if dry_run:
        print("DRY RUN SUMMARY")
    elif skip_ingestion:
        print("VALIDATION SUMMARY")
    else:
        print("IMPORT SUMMARY")
    print("=" * 70)

    print(f"Total files:       {summary.total_files}")
    print(f"Files completed:   {summary.files_completed}")

    if summary.files_failed > 0:
        print(f"Files failed:      {summary.files_failed}")

    print(f"\nTotal rows:        {summary.total_rows:,}")

    if not skip_ingestion:
        print(f"New profiles:      {summary.total_new:,}")
        print(f"Duplicates:        {summary.total_dups:,}")

    if summary.total_errors > 0:
        print(f"Errors:            {summary.total_errors:,}")

    print("=" * 70)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Bulk CSV-to-Supabase import pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Import all CSVs:
    python3 "Filling Database/supabase/import_all.py"

  Dry run (no DB writes):
    python3 "Filling Database/supabase/import_all.py" --dry-run

  Import single file:
    python3 "Filling Database/supabase/import_all.py" --file ../partners/sam_gov.csv

  Validate CSVs only:
    python3 "Filling Database/supabase/import_all.py" --skip-ingestion
        """
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview files without importing to database"
    )

    parser.add_argument(
        "--file",
        type=str,
        help="Import a single specific CSV file (relative to script or absolute path)"
    )

    parser.add_argument(
        "--skip-ingestion",
        action="store_true",
        help="Validate CSVs without database writes"
    )

    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Reset import state (start fresh)"
    )

    args = parser.parse_args()

    # Load or reset state
    if args.reset_state:
        state = {}
        save_state(state)
        print("State reset")
    else:
        state = load_state()

    # Find CSV files
    csv_files = find_csv_files(args.file)

    if not csv_files:
        print("No CSV files found")
        sys.exit(1)

    print(f"Found {len(csv_files)} CSV file(s) to process")

    if args.dry_run:
        print("\n*** DRY RUN MODE - No database writes will occur ***")
    elif args.skip_ingestion:
        print("\n*** VALIDATION MODE - Only validating CSVs ***")

    # Process files
    summary = ImportSummary(total_files=len(csv_files))

    for csv_path in csv_files:
        stats = process_csv_file(
            csv_path,
            state,
            dry_run=args.dry_run,
            skip_ingestion=args.skip_ingestion,
        )

        # Update summary
        summary.total_rows += stats.total_rows
        summary.total_new += stats.new_count
        summary.total_dups += stats.dup_count
        summary.total_errors += stats.error_count

        if stats.completed:
            summary.files_completed += 1
        else:
            summary.files_failed += 1

    # Print summary
    print_summary(summary, dry_run=args.dry_run, skip_ingestion=args.skip_ingestion)

    if not args.dry_run and not args.skip_ingestion:
        print(f"\nState saved to: {STATE_FILE}")


if __name__ == "__main__":
    main()
