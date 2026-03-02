#!/usr/bin/env python3
"""
Runner script for the association member directory scraper.

Reads the master CSV of professional associations, filters to those with
public directories, and runs the association_members scraper against each.

Features:
  - Resume support: tracks which associations have been processed
  - Per-association CSV output in Filling Database/partners/
  - Consolidated output file for all associations
  - Progress reporting and error tracking

Usage:
    # Run all associations with public directories
    python3 scripts/sourcing/run_associations.py

    # Limit to first 10 associations (for testing)
    python3 scripts/sourcing/run_associations.py --limit 10

    # Run a specific category
    python3 scripts/sourcing/run_associations.py --category "Professional Society"

    # Dry run (no CSV output, just discovery)
    python3 scripts/sourcing/run_associations.py --dry-run

    # Resume from where we left off
    python3 scripts/sourcing/run_associations.py --resume

    # Reset progress and start over
    python3 scripts/sourcing/run_associations.py --reset

    # Set max contacts per association
    python3 scripts/sourcing/run_associations.py --max-per-assoc 500

    # Show progress summary
    python3 scripts/sourcing/run_associations.py --status
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load .env if available
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

from scripts.sourcing.rate_limiter import RateLimiter
from scripts.sourcing.scrapers.association_members import Scraper as AssociationScraper


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

MASTER_CSV = PROJECT_ROOT / "Filling Database" / "directories" / "jv_directories_master.csv"
OUTPUT_DIR = PROJECT_ROOT / "Filling Database" / "partners"
CONSOLIDATED_CSV = OUTPUT_DIR / "association_members_all.csv"
STATE_FILE = PROJECT_ROOT / "scripts" / "sourcing" / "config" / "state" / "association_runner.json"

CSV_FIELDNAMES = [
    "name", "email", "company", "website", "linkedin", "phone", "bio",
    "source", "source_url", "association_name", "association_url",
    "category", "subcategory",
]


# ---------------------------------------------------------------------------
# State management for resume support
# ---------------------------------------------------------------------------

class RunnerState:
    """Tracks which associations have been processed for resume support."""

    def __init__(self, state_path: Path = STATE_FILE):
        self.state_path = state_path
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self._state = self._load()

    def _load(self) -> dict:
        if self.state_path.exists():
            try:
                return json.loads(self.state_path.read_text())
            except (json.JSONDecodeError, OSError):
                return self._default_state()
        return self._default_state()

    @staticmethod
    def _default_state() -> dict:
        return {
            "processed_urls": [],
            "total_contacts": 0,
            "total_associations_attempted": 0,
            "total_associations_with_contacts": 0,
            "errors": [],
            "started_at": None,
            "updated_at": None,
        }

    def save(self) -> None:
        self._state["updated_at"] = datetime.now().isoformat()
        self.state_path.write_text(json.dumps(self._state, indent=2))

    def is_processed(self, url: str) -> bool:
        return url in self._state["processed_urls"]

    def mark_processed(self, url: str, contacts_found: int) -> None:
        if url not in self._state["processed_urls"]:
            self._state["processed_urls"].append(url)
        self._state["total_associations_attempted"] += 1
        self._state["total_contacts"] += contacts_found
        if contacts_found > 0:
            self._state["total_associations_with_contacts"] += 1

    def add_error(self, url: str, error: str) -> None:
        self._state["errors"].append({
            "url": url,
            "error": error,
            "timestamp": datetime.now().isoformat(),
        })
        # Keep only last 500 errors to avoid unbounded growth
        if len(self._state["errors"]) > 500:
            self._state["errors"] = self._state["errors"][-500:]

    def set_started(self) -> None:
        if not self._state["started_at"]:
            self._state["started_at"] = datetime.now().isoformat()

    def reset(self) -> None:
        self._state = self._default_state()
        self.save()

    @property
    def processed_count(self) -> int:
        return len(self._state["processed_urls"])

    @property
    def total_contacts(self) -> int:
        return self._state["total_contacts"]

    @property
    def total_with_contacts(self) -> int:
        return self._state["total_associations_with_contacts"]

    @property
    def summary(self) -> dict:
        return {
            "processed": self.processed_count,
            "total_contacts": self.total_contacts,
            "with_contacts": self.total_with_contacts,
            "errors": len(self._state["errors"]),
            "started_at": self._state.get("started_at", "never"),
            "updated_at": self._state.get("updated_at", "never"),
        }


# ---------------------------------------------------------------------------
# CSV reading
# ---------------------------------------------------------------------------

def load_associations(
    csv_path: Path = MASTER_CSV,
    category_filter: str = "",
    subcategory_filter: str = "",
) -> list[dict]:
    """Load associations from the master CSV, filtering to public directories.

    Returns:
        List of dicts with keys: name, url, category, subcategory,
        description, estimated_members, has_public_directory.
    """
    if not csv_path.exists():
        print(f"ERROR: Master CSV not found at {csv_path}")
        sys.exit(1)

    associations = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Filter to public directories
            if (row.get("has_public_directory") or "").strip().lower() != "yes":
                continue

            # Category filter
            if category_filter:
                row_cat = (row.get("category") or "").strip().lower()
                if category_filter.lower() not in row_cat:
                    continue

            # Subcategory filter
            if subcategory_filter:
                row_subcat = (row.get("subcategory") or "").strip().lower()
                if subcategory_filter.lower() not in row_subcat:
                    continue

            # Validate URL
            url = (row.get("url") or "").strip()
            if not url or not url.startswith("http"):
                continue

            associations.append({
                "name": (row.get("name") or "").strip(),
                "url": url,
                "category": (row.get("category") or "").strip(),
                "subcategory": (row.get("subcategory") or "").strip(),
                "description": (row.get("description") or "").strip(),
                "estimated_members": (row.get("estimated_members") or "").strip(),
            })

    return associations


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_associations(
    associations: list[dict],
    resume: bool = True,
    dry_run: bool = False,
    max_per_assoc: int = 0,
    max_pages_per_assoc: int = 0,
    limit: int = 0,
    verbose: bool = False,
) -> None:
    """Run the association member scraper against each association.

    Args:
        associations: List of association dicts from load_associations().
        resume: If True, skip associations already processed.
        dry_run: If True, only discover directory links (no member scraping).
        max_per_assoc: Max contacts to extract per association (0 = unlimited).
        max_pages_per_assoc: Max pages to scrape per association (0 = unlimited).
        limit: Max number of associations to process (0 = all).
        verbose: Enable debug logging.
    """
    state = RunnerState()
    state.set_started()

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Set up consolidated CSV
    consolidated_file = None
    consolidated_writer = None
    if not dry_run:
        file_exists = CONSOLIDATED_CSV.exists() and CONSOLIDATED_CSV.stat().st_size > 0
        consolidated_file = open(CONSOLIDATED_CSV, "a", newline="", encoding="utf-8")
        consolidated_writer = csv.DictWriter(consolidated_file, fieldnames=CSV_FIELDNAMES)
        if not file_exists:
            consolidated_writer.writeheader()

    rate_limiter = RateLimiter()
    total_associations = len(associations)
    processed_this_run = 0
    contacts_this_run = 0

    print(f"\n{'=' * 70}")
    print("ASSOCIATION MEMBER DIRECTORY SCRAPER")
    print(f"{'=' * 70}")
    print(f"  Associations in CSV:       {total_associations}")
    print(f"  Already processed:         {state.processed_count}")
    remaining = total_associations - (state.processed_count if resume else 0)
    if limit:
        remaining = min(remaining, limit)
    print(f"  To process this run:       {remaining}")
    print(f"  Resume mode:               {resume}")
    print(f"  Dry run:                   {dry_run}")
    print(f"  Max contacts/association:  {max_per_assoc or 'unlimited'}")
    print(f"  Max pages/association:     {max_pages_per_assoc or 'unlimited'}")
    print(f"  Output directory:          {OUTPUT_DIR}")
    print(f"{'=' * 70}\n")

    try:
        for i, assoc in enumerate(associations):
            assoc_url = assoc["url"]
            assoc_name = assoc["name"]

            # Resume: skip already-processed associations
            if resume and state.is_processed(assoc_url):
                continue

            # Limit check
            if limit and processed_this_run >= limit:
                print(f"\nReached limit of {limit} associations.")
                break

            processed_this_run += 1
            print(f"\n[{processed_this_run}/{remaining}] {assoc_name}")
            print(f"  URL: {assoc_url}")
            print(f"  Category: {assoc.get('category', '')} / {assoc.get('subcategory', '')}")
            print(f"  Est. members: {assoc.get('estimated_members', 'unknown')}")

            if dry_run:
                # In dry-run mode, just discover directory links
                scraper = AssociationScraper(
                    association_urls=[assoc],
                    rate_limiter=rate_limiter,
                )
                html = scraper.fetch_page(assoc_url)
                if html:
                    dir_links = scraper._discover_directory_links(assoc_url, html)
                    if dir_links:
                        print(f"  FOUND {len(dir_links)} directory link(s):")
                        for dl in dir_links:
                            print(f"    -> {dl}")
                    else:
                        print("  No directory links found.")
                else:
                    print("  Could not fetch homepage.")
                state.mark_processed(assoc_url, 0)
                state.save()
                continue

            # Full scrape
            scraper = AssociationScraper(
                association_urls=[assoc],
                rate_limiter=rate_limiter,
            )

            assoc_contacts = 0
            try:
                for contact in scraper.run(
                    max_contacts=max_per_assoc,
                    max_pages=max_pages_per_assoc,
                ):
                    assoc_contacts += 1
                    contacts_this_run += 1

                    # Write to consolidated CSV
                    if consolidated_writer:
                        ingestion = contact.to_ingestion_dict()
                        row = {
                            "name": ingestion.get("name") or "",
                            "email": ingestion.get("email") or "",
                            "company": ingestion.get("company") or "",
                            "website": ingestion.get("website") or "",
                            "linkedin": ingestion.get("linkedin") or "",
                            "phone": ingestion.get("phone") or "",
                            "bio": ingestion.get("bio") or "",
                            "source": "association_members",
                            "source_url": contact.source_url or "",
                            "association_name": assoc_name,
                            "association_url": assoc_url,
                            "category": assoc.get("category", ""),
                            "subcategory": assoc.get("subcategory", ""),
                        }
                        consolidated_writer.writerow(row)

                    # Periodic flush
                    if assoc_contacts % 100 == 0 and consolidated_file:
                        consolidated_file.flush()

            except KeyboardInterrupt:
                print("\n\nInterrupted by user. Saving progress...")
                state.mark_processed(assoc_url, assoc_contacts)
                state.save()
                raise
            except Exception as exc:
                error_msg = f"{type(exc).__name__}: {exc}"
                print(f"  ERROR: {error_msg}")
                state.add_error(assoc_url, error_msg)
                logging.exception("Error scraping %s", assoc_url)

            state.mark_processed(assoc_url, assoc_contacts)
            state.save()

            print(f"  Contacts found: {assoc_contacts}")
            print(
                f"  Running total: {contacts_this_run} contacts "
                f"from {processed_this_run} associations"
            )

            # Flush consolidated CSV periodically
            if consolidated_file and processed_this_run % 5 == 0:
                consolidated_file.flush()

    except KeyboardInterrupt:
        print("\n\nInterrupted. Progress has been saved.")
    finally:
        if consolidated_file:
            consolidated_file.close()

    # Final summary
    print(f"\n{'=' * 70}")
    print("RUN COMPLETE")
    print(f"{'=' * 70}")
    print(f"  Associations processed this run: {processed_this_run}")
    print(f"  Contacts found this run:         {contacts_this_run}")
    print(f"  Total associations processed:    {state.processed_count}")
    print(f"  Total contacts (all runs):       {state.total_contacts}")
    print(f"  Associations with contacts:      {state.total_with_contacts}")
    if not dry_run:
        print(f"  Output file: {CONSOLIDATED_CSV}")
    print(f"{'=' * 70}\n")


def show_status() -> None:
    """Display current progress from state file."""
    state = RunnerState()
    summary = state.summary

    # Load associations to show remaining count
    associations = load_associations()
    total = len(associations)

    print(f"\n{'=' * 70}")
    print("ASSOCIATION SCRAPER STATUS")
    print(f"{'=' * 70}")
    print(f"  Total associations (has_public_directory=yes): {total}")
    print(f"  Processed:                                     {summary['processed']}")
    print(f"  Remaining:                                     {total - summary['processed']}")
    print(f"  Total contacts collected:                      {summary['total_contacts']}")
    print(f"  Associations that yielded contacts:            {summary['with_contacts']}")
    print(f"  Errors logged:                                 {summary['errors']}")
    print(f"  Started:                                       {summary['started_at']}")
    print(f"  Last updated:                                  {summary['updated_at']}")

    if CONSOLIDATED_CSV.exists():
        # Count lines in consolidated CSV (minus header)
        with open(CONSOLIDATED_CSV, "r", encoding="utf-8") as f:
            line_count = sum(1 for _ in f) - 1
        size_mb = CONSOLIDATED_CSV.stat().st_size / (1024 * 1024)
        print(f"  Consolidated CSV rows:                         {line_count}")
        print(f"  Consolidated CSV size:                         {size_mb:.1f} MB")

    print(f"{'=' * 70}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run association member directory scraper against master CSV"
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Max number of associations to process (0 = all)",
    )
    parser.add_argument(
        "--category", default="",
        help="Filter to associations in this category (e.g., 'Professional Society')",
    )
    parser.add_argument(
        "--subcategory", default="",
        help="Filter to associations in this subcategory (e.g., 'Healthcare')",
    )
    parser.add_argument(
        "--max-per-assoc", type=int, default=0,
        help="Max contacts per association (0 = unlimited)",
    )
    parser.add_argument(
        "--max-pages-per-assoc", type=int, default=0,
        help="Max pages to scrape per association (0 = unlimited)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Only discover directory links, do not scrape members",
    )
    parser.add_argument(
        "--resume", action="store_true", default=True,
        help="Resume from where we left off (default: True)",
    )
    parser.add_argument(
        "--no-resume", action="store_true",
        help="Start from scratch, ignoring previous progress",
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Reset all progress tracking and exit",
    )
    parser.add_argument(
        "--status", action="store_true",
        help="Show progress summary and exit",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    # Set up logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.reset:
        RunnerState().reset()
        print("Progress reset. All tracking data cleared.")
        return

    if args.status:
        show_status()
        return

    # Load and filter associations
    associations = load_associations(
        category_filter=args.category,
        subcategory_filter=args.subcategory,
    )

    if not associations:
        print("No associations found matching filters.")
        return

    print(f"Loaded {len(associations)} associations with public directories.")

    resume = args.resume and not args.no_resume

    run_associations(
        associations=associations,
        resume=resume,
        dry_run=args.dry_run,
        max_per_assoc=args.max_per_assoc,
        max_pages_per_assoc=args.max_pages_per_assoc,
        limit=args.limit,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
