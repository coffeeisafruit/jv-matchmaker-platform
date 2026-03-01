#!/usr/bin/env python3
"""
Merge and deduplicate all scraped CSV files into a single clean file.

Deduplication strategy:
1. Exact name match (case-insensitive) → merge data from both records
2. Name normalization (strip titles, suffixes, extra spaces)

Output: scripts/sourcing/output/MERGED_ALL.csv with unique contacts.

Usage:
    python3 scripts/sourcing/dedup_merge.py
    python3 scripts/sourcing/dedup_merge.py --stats  # Just show stats
"""

from __future__ import annotations

import csv
import re
import sys
from pathlib import Path
from collections import defaultdict


OUTPUT_DIR = Path(__file__).parent / "output"
MERGED_FILE = OUTPUT_DIR / "MERGED_ALL.csv"

# Skip these generic/invalid names
SKIP_NAMES = {
    "view", "new york", "los angeles", "chicago", "unknown",
    "various artists", "anonymous", "see more", "none",
    "n/a", "na", "test", "admin", "editor", "staff",
}


def normalize_name(name: str) -> str:
    """Normalize a name for dedup comparison."""
    name = name.strip().lower()
    # Remove common prefixes
    name = re.sub(r"^(dr\.?\s+|prof\.?\s+|mr\.?\s+|mrs\.?\s+|ms\.?\s+)", "", name)
    # Remove common suffixes
    name = re.sub(
        r",?\s*(phd|psyd|lcsw|lmft|lpc|lmhc|ma|ms|med|edd|md|ncc|bcc|cpc|acc|pcc|mcc|jr\.?|sr\.?|iii?|iv).*$",
        "", name, flags=re.I,
    )
    # Normalize whitespace
    name = re.sub(r"\s+", " ", name).strip()
    return name


def merge_records(existing: dict, new: dict) -> dict:
    """Merge two records, preferring non-empty fields from either."""
    merged = dict(existing)
    for key, val in new.items():
        if val and val.strip() and val.strip().lower() != "none":
            if not merged.get(key) or not merged[key].strip() or merged[key].strip().lower() == "none":
                merged[key] = val
    return merged


def load_all_csvs() -> list[dict]:
    """Load all CSV files from the output directory."""
    all_rows = []
    csv_files = sorted(OUTPUT_DIR.glob("*.csv"))

    for csv_path in csv_files:
        if csv_path.name == "MERGED_ALL.csv":
            continue
        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                count = 0
                for row in reader:
                    row["_source_file"] = csv_path.name
                    all_rows.append(row)
                    count += 1
                if count > 0:
                    print(f"  Loaded {count:>6,} from {csv_path.name}")
        except Exception as exc:
            print(f"  ERROR loading {csv_path.name}: {exc}")

    return all_rows


def deduplicate(rows: list[dict]) -> list[dict]:
    """Deduplicate by normalized name."""
    seen: dict[str, dict] = {}  # normalized_name -> best record
    skipped = 0

    for row in rows:
        name = row.get("name", "").strip()
        if not name or len(name) < 2:
            skipped += 1
            continue

        norm = normalize_name(name)
        if not norm or norm in SKIP_NAMES or len(norm) < 2:
            skipped += 1
            continue

        if norm in seen:
            # Merge fields
            seen[norm] = merge_records(seen[norm], row)
        else:
            seen[norm] = dict(row)

    print(f"\n  Raw rows: {len(rows):,}")
    print(f"  Skipped (invalid): {skipped:,}")
    print(f"  Unique contacts: {len(seen):,}")

    return list(seen.values())


def write_merged(contacts: list[dict]) -> None:
    """Write deduplicated contacts to merged CSV."""
    fieldnames = ["name", "email", "company", "website", "linkedin", "phone", "bio", "source", "source_url"]

    with open(MERGED_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for contact in contacts:
            writer.writerow(contact)

    print(f"\n  Written to: {MERGED_FILE}")
    print(f"  Total unique contacts: {len(contacts):,}")


def show_stats(contacts: list[dict]) -> None:
    """Show breakdown stats."""
    source_counts: dict[str, int] = defaultdict(int)
    has_email = 0
    has_website = 0
    has_linkedin = 0
    has_phone = 0
    has_bio = 0

    for c in contacts:
        source = c.get("source", c.get("_source_file", "unknown"))
        source_counts[source] += 1
        if c.get("email") and c["email"].strip() and c["email"].strip().lower() != "none":
            has_email += 1
        if c.get("website") and c["website"].strip() and c["website"].strip().lower() != "none":
            has_website += 1
        if c.get("linkedin") and c["linkedin"].strip() and c["linkedin"].strip().lower() != "none":
            has_linkedin += 1
        if c.get("phone") and c["phone"].strip() and c["phone"].strip().lower() != "none":
            has_phone += 1
        if c.get("bio") and c["bio"].strip() and c["bio"].strip().lower() != "none":
            has_bio += 1

    total = len(contacts)
    print(f"\n{'=' * 60}")
    print(f"CONTACT QUALITY BREAKDOWN ({total:,} unique)")
    print(f"{'=' * 60}")
    print(f"  Has email:     {has_email:>6,} ({100*has_email/total:.1f}%)")
    print(f"  Has website:   {has_website:>6,} ({100*has_website/total:.1f}%)")
    print(f"  Has LinkedIn:  {has_linkedin:>6,} ({100*has_linkedin/total:.1f}%)")
    print(f"  Has phone:     {has_phone:>6,} ({100*has_phone/total:.1f}%)")
    print(f"  Has bio:       {has_bio:>6,} ({100*has_bio/total:.1f}%)")
    print()

    print("  By source:")
    for source, count in sorted(source_counts.items(), key=lambda x: -x[1]):
        print(f"    {source:30s} {count:>6,}")
    print()


def main():
    stats_only = "--stats" in sys.argv

    print(f"\n{'=' * 60}")
    print("MERGE & DEDUPLICATE ALL SCRAPED CONTACTS")
    print(f"{'=' * 60}\n")

    rows = load_all_csvs()
    if not rows:
        print("No CSV data found.")
        return

    contacts = deduplicate(rows)
    show_stats(contacts)

    if not stats_only:
        write_merged(contacts)


if __name__ == "__main__":
    main()
