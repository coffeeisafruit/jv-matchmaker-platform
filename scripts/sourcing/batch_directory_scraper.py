"""
Batch directory scraper — processes the partnership directory lists
and runs the generic scraper against each URL.

Reads from:
  - scripts/sourcing/config/partnership_directories.py (350+ directories)
  - Filling Database/directories/jv_directories_master.csv (2,526 directories)

Outputs CSVs to: Filling Database/partners/{source_name}.csv

Usage:
  # Scrape all directories (prioritized)
  python3 -m scripts.sourcing.batch_directory_scraper --all

  # Scrape specific categories
  python3 -m scripts.sourcing.batch_directory_scraper --category "Accelerator Alumni"

  # Scrape from CSV list only
  python3 -m scripts.sourcing.batch_directory_scraper --csv-only --max-sites 50

  # Scrape from partnership_directories.py only
  python3 -m scripts.sourcing.batch_directory_scraper --py-only

  # Resume from a specific site
  python3 -m scripts.sourcing.batch_directory_scraper --all --resume-from "Techstars Portfolio"

  # Dry run (list what would be scraped)
  python3 -m scripts.sourcing.batch_directory_scraper --all --dry-run
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

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.sourcing.generic_scraper import (
    GenericDirectoryScraper,
    export_contacts_to_csv,
)

logger = logging.getLogger("sourcing.batch")

# ---------------------------------------------------------------------------
# Priority tiers for directory categories
# ---------------------------------------------------------------------------

# Categories most likely to have scrapable public member directories
TIER_1_CATEGORIES = {
    "Accelerator Alumni",
    "VC Portfolio",
    "Incubator Directory",
    "No-Code Partner",
    "Tech Partner Ecosystem",
    "Integration Partner",
    "Fintech Partner",
    "AI/ML Partner",
    # From CSV
    "Professional Society",
    "Trade Association",
    "Coach Directory",
    "Speaker Directory",
    "Consultant Directory",
}

TIER_2_CATEGORIES = {
    "Startup Directory",
    "Co-Founder Matching",
    "SaaS Partnership Platform",
    "Blockchain/Web3",
    "Affiliate Network",
    "API Marketplace",
    "Open Source",
    "Developer Community",
    # From CSV
    "Certification Body",
    "Industry Association",
}

# Categories that are unlikely to yield contacts
SKIP_CATEGORIES = {
    "Discord Community",  # No scrapable profiles
}

# Specific sites known to be login-walled or heavily protected
SKIP_URLS = {
    "linkedin.com",
    "facebook.com",
    "twitter.com",
    "x.com",
}


def sanitize_filename(name: str) -> str:
    """Convert directory name to safe filename."""
    name = re.sub(r"[^\w\s-]", "", name)
    name = re.sub(r"\s+", "_", name.strip())
    return name.lower()[:80]


# ---------------------------------------------------------------------------
# Load directories from both sources
# ---------------------------------------------------------------------------

def load_partnership_directories() -> list[dict]:
    """Load directories from partnership_directories.py."""
    try:
        from scripts.sourcing.config.partnership_directories import PARTNERSHIP_DIRECTORIES
        dirs = []
        for d in PARTNERSHIP_DIRECTORIES:
            dirs.append({
                "name": d["name"],
                "url": d["url"],
                "category": d.get("category", "Unknown"),
                "description": d.get("description", ""),
                "source_file": "partnership_directories.py",
            })
        logger.info("Loaded %d directories from partnership_directories.py", len(dirs))
        return dirs
    except ImportError as e:
        logger.error("Could not import partnership_directories: %s", e)
        return []


def load_csv_directories() -> list[dict]:
    """Load directories from jv_directories_master.csv."""
    csv_path = PROJECT_ROOT / "Filling Database" / "directories" / "jv_directories_master.csv"
    if not csv_path.exists():
        logger.warning("CSV not found: %s", csv_path)
        return []

    dirs = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            dirs.append({
                "name": row.get("name", "").strip(),
                "url": row.get("url", "").strip(),
                "category": row.get("category", "Unknown").strip(),
                "description": row.get("description", "").strip(),
                "estimated_members": row.get("estimated_members", ""),
                "has_public_directory": row.get("has_public_directory", "").lower(),
                "source_file": "jv_directories_master.csv",
            })
    logger.info("Loaded %d directories from CSV", len(dirs))
    return dirs


def load_all_directories() -> list[dict]:
    """Load and merge directories from both sources, deduplicate by URL domain."""
    py_dirs = load_partnership_directories()
    csv_dirs = load_csv_directories()

    all_dirs = py_dirs + csv_dirs

    # Deduplicate by URL domain
    seen_domains = set()
    unique = []
    for d in all_dirs:
        from urllib.parse import urlparse
        domain = urlparse(d["url"]).netloc.lower().replace("www.", "")
        if domain and domain not in seen_domains:
            seen_domains.add(domain)
            unique.append(d)

    logger.info("Total unique directories: %d (from %d raw)", len(unique), len(all_dirs))
    return unique


# ---------------------------------------------------------------------------
# Prioritization
# ---------------------------------------------------------------------------

def prioritize_directories(dirs: list[dict]) -> list[dict]:
    """Sort directories by scraping priority."""

    def priority_key(d):
        cat = d["category"]
        # Tier 1 first
        if cat in TIER_1_CATEGORIES:
            tier = 0
        elif cat in TIER_2_CATEGORIES:
            tier = 1
        else:
            tier = 2

        # Within tier, prefer sites that claim public directories
        has_pub = 0 if d.get("has_public_directory") == "yes" else 1

        # Prefer sites with more estimated members
        members = 0
        try:
            members = -int(d.get("estimated_members", 0))
        except (ValueError, TypeError):
            pass

        return (tier, has_pub, members, d["name"])

    return sorted(dirs, key=priority_key)


def should_skip(d: dict) -> tuple[bool, str]:
    """Check if a directory should be skipped."""
    from urllib.parse import urlparse

    url = d["url"]
    domain = urlparse(url).netloc.lower()

    for skip in SKIP_URLS:
        if skip in domain:
            return True, f"Blocked domain: {skip}"

    if d["category"] in SKIP_CATEGORIES:
        return True, f"Skipped category: {d['category']}"

    return False, ""


# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------

PROGRESS_FILE = PROJECT_ROOT / "Filling Database" / "partners" / "_scraping_progress.json"

# Overridable at runtime for parallel chunk support
_active_progress_file: Path = PROGRESS_FILE


def load_progress(path: Path | None = None) -> dict:
    """Load scraping progress from file."""
    p = path or _active_progress_file
    if p.exists():
        with open(p, "r") as f:
            return json.load(f)
    return {"completed": {}, "skipped": {}, "failed": {}, "started_at": datetime.now().isoformat()}


def save_progress(progress: dict, path: Path | None = None):
    """Save scraping progress to file."""
    p = path or _active_progress_file
    os.makedirs(p.parent, exist_ok=True)
    with open(p, "w") as f:
        json.dump(progress, f, indent=2)


# ---------------------------------------------------------------------------
# Main batch scraper
# ---------------------------------------------------------------------------

def run_batch(
    dirs: list[dict],
    max_sites: int = 0,
    max_pages_per_site: int = 30,
    resume_from: str = "",
    dry_run: bool = False,
    use_crawl4ai: bool = True,
    chunk: int = 0,
    total_chunks: int = 0,
):
    """Run the generic scraper against a list of directories."""
    output_dir = PROJECT_ROOT / "Filling Database" / "partners"
    os.makedirs(output_dir, exist_ok=True)

    # For chunked parallel runs, also load main progress to skip already-done sites
    main_progress = load_progress(PROGRESS_FILE) if total_chunks > 0 else {}
    progress = load_progress()
    scraper = GenericDirectoryScraper(
        max_pages_per_site=max_pages_per_site,
        use_crawl4ai=use_crawl4ai,
    )

    past_resume = not bool(resume_from)
    sites_done = 0
    total_contacts = 0

    print(f"\n{'='*70}")
    print(f"BATCH DIRECTORY SCRAPER — {len(dirs)} directories queued")
    print(f"{'='*70}\n")

    for i, d in enumerate(dirs):
        name = d["name"]
        url = d["url"]
        category = d["category"]

        # Resume support
        if not past_resume:
            if name == resume_from or resume_from.lower() in name.lower():
                past_resume = True
            else:
                continue

        # Skip if already completed (check both chunk progress and main progress)
        if name in progress.get("completed", {}):
            logger.debug("Already completed: %s", name)
            continue
        if main_progress and name in main_progress.get("completed", {}):
            logger.debug("Already completed in main progress: %s", name)
            continue

        # Skip check
        skip, reason = should_skip(d)
        if skip:
            progress.setdefault("skipped", {})[name] = reason
            logger.debug("Skipping %s: %s", name, reason)
            continue

        if dry_run:
            print(f"  [{i+1}/{len(dirs)}] WOULD SCRAPE: {name} ({url}) [{category}]")
            continue

        print(f"\n  [{i+1}/{len(dirs)}] Scraping: {name}")
        print(f"    URL: {url}")
        print(f"    Category: {category}")

        try:
            contacts = scraper.scrape_site(url, name, category)

            filename = sanitize_filename(name)
            csv_path = output_dir / f"{filename}.csv"

            if contacts:
                # Filter to valid contacts only
                valid = [c for c in contacts if c.is_valid()]
                export_contacts_to_csv(valid, str(csv_path))
                total_contacts += len(valid)
                progress.setdefault("completed", {})[name] = {
                    "contacts": len(valid),
                    "csv": str(csv_path),
                    "scraped_at": datetime.now().isoformat(),
                }
                print(f"    ✓ {len(valid)} contacts → {csv_path.name}")
            else:
                progress.setdefault("completed", {})[name] = {
                    "contacts": 0,
                    "scraped_at": datetime.now().isoformat(),
                }
                print(f"    ○ No contacts found")

        except Exception as e:
            logger.error("Error scraping %s: %s", name, e, exc_info=True)
            progress.setdefault("failed", {})[name] = {
                "error": str(e),
                "at": datetime.now().isoformat(),
            }
            print(f"    ✗ Error: {e}")

        save_progress(progress)
        sites_done += 1

        if max_sites and sites_done >= max_sites:
            print(f"\n  Reached max_sites limit ({max_sites})")
            break

        # Rate limit between sites
        time.sleep(0.5)

    # Summary
    print(f"\n{'='*70}")
    print(f"BATCH COMPLETE")
    print(f"  Sites scraped: {sites_done}")
    print(f"  Total contacts: {total_contacts}")
    print(f"  Scraper stats: {scraper.stats}")
    print(f"  Progress saved to: {PROGRESS_FILE}")
    print(f"{'='*70}\n")

    return progress


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Batch directory scraper")
    parser.add_argument("--all", action="store_true", help="Scrape all directories")
    parser.add_argument("--py-only", action="store_true", help="Only from partnership_directories.py")
    parser.add_argument("--csv-only", action="store_true", help="Only from jv_directories_master.csv")
    parser.add_argument("--category", type=str, help="Filter by category name")
    parser.add_argument("--max-sites", type=int, default=0, help="Max sites to scrape")
    parser.add_argument("--max-pages", type=int, default=30, help="Max pages per site")
    parser.add_argument("--resume-from", type=str, default="", help="Resume from site name")
    parser.add_argument("--dry-run", action="store_true", help="List sites without scraping")
    parser.add_argument("--no-crawl4ai", action="store_true", help="Disable crawl4ai (requests only)")
    parser.add_argument("--tier", type=int, choices=[1, 2, 3], help="Only scrape specific tier")
    parser.add_argument("--chunk", type=int, default=0, help="Chunk index (0-based) for parallel runs")
    parser.add_argument("--total-chunks", type=int, default=0, help="Total number of chunks for parallel runs")
    parser.add_argument("--progress-file", type=str, default="", help="Custom progress file path (for parallel runs)")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    # Load directories
    if args.py_only:
        dirs = load_partnership_directories()
    elif args.csv_only:
        dirs = load_csv_directories()
    else:
        dirs = load_all_directories()

    # Filter by category
    if args.category:
        dirs = [d for d in dirs if args.category.lower() in d["category"].lower()]
        print(f"Filtered to {len(dirs)} directories in category '{args.category}'")

    # Filter by tier
    if args.tier:
        if args.tier == 1:
            dirs = [d for d in dirs if d["category"] in TIER_1_CATEGORIES]
        elif args.tier == 2:
            dirs = [d for d in dirs if d["category"] in TIER_2_CATEGORIES]
        else:
            dirs = [d for d in dirs if d["category"] not in TIER_1_CATEGORIES and d["category"] not in TIER_2_CATEGORIES]
        print(f"Filtered to {len(dirs)} Tier {args.tier} directories")

    # Prioritize
    dirs = prioritize_directories(dirs)

    if not dirs:
        print("No directories to scrape!")
        return

    # Parallel chunk support: split dirs into N chunks, process chunk i
    if args.total_chunks > 0:
        chunk_size = len(dirs) // args.total_chunks
        remainder = len(dirs) % args.total_chunks
        start = args.chunk * chunk_size + min(args.chunk, remainder)
        end = start + chunk_size + (1 if args.chunk < remainder else 0)
        print(f"Chunk {args.chunk + 1}/{args.total_chunks}: sites {start+1}-{end} of {len(dirs)}")
        dirs = dirs[start:end]

    # Custom progress file for parallel runs
    if args.progress_file:
        global _active_progress_file
        _active_progress_file = Path(args.progress_file)
        if not _active_progress_file.is_absolute():
            _active_progress_file = PROJECT_ROOT / _active_progress_file

    run_batch(
        dirs,
        max_sites=args.max_sites,
        max_pages_per_site=args.max_pages,
        resume_from=args.resume_from,
        dry_run=args.dry_run,
        use_crawl4ai=not args.no_crawl4ai,
        chunk=args.chunk,
        total_chunks=args.total_chunks,
    )


if __name__ == "__main__":
    main()
