"""
Aggregate all scraped CSV files into a single master contacts CSV
and generate a summary report.

Usage:
    python3 -m scripts.sourcing.aggregate_results
    python3 -m scripts.sourcing.aggregate_results --dedup --output master.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import re
from collections import Counter, defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PARTNERS_DIR = PROJECT_ROOT / "Filling Database" / "partners"

# Standard fieldnames across all CSVs
STANDARD_FIELDS = [
    "name", "email", "company", "website", "linkedin",
    "phone", "bio", "source_platform", "source_url",
    "source_category", "scraped_at",
]

# Alternate column names we might encounter
FIELD_ALIASES = {
    "source": "source_platform",
    "source_name": "source_platform",
    "description": "bio",
    "url": "website",
    "organization": "company",
    "org": "company",
    "full_name": "name",
    "email_address": "email",
    "phone_number": "phone",
    "linkedin_url": "linkedin",
}


def normalize_row(row: dict, source_file: str) -> dict:
    """Normalize a CSV row to standard field names."""
    normalized = {}
    for key, value in row.items():
        key_lower = key.strip().lower()
        # Map aliased field names
        std_key = FIELD_ALIASES.get(key_lower, key_lower)
        if std_key in STANDARD_FIELDS:
            normalized[std_key] = (value or "").strip()

    # Fill missing fields
    for field in STANDARD_FIELDS:
        if field not in normalized:
            normalized[field] = ""

    # If source_platform is empty, use the CSV filename
    if not normalized["source_platform"]:
        normalized["source_platform"] = Path(source_file).stem

    return normalized


def is_valid_contact(row: dict) -> bool:
    """Check if a row represents a valid contact."""
    name = row.get("name", "").strip()
    if not name or len(name) < 2 or len(name) > 200:
        return False

    # Must have at least one contact signal
    has_signal = any([
        row.get("email", "").strip(),
        row.get("website", "").strip(),
        row.get("linkedin", "").strip(),
    ])
    return has_signal


def dedup_contacts(rows: list[dict]) -> list[dict]:
    """Deduplicate contacts by email, then by name+company."""
    seen_emails = set()
    seen_name_company = set()
    unique = []

    for row in rows:
        email = row.get("email", "").strip().lower()
        name = row.get("name", "").strip().lower()
        company = row.get("company", "").strip().lower()

        # Dedup by email (strongest signal)
        if email:
            if email in seen_emails:
                continue
            seen_emails.add(email)

        # Dedup by name+company combo
        key = f"{name}|{company}"
        if key in seen_name_company:
            continue
        seen_name_company.add(key)

        unique.append(row)

    return unique


def aggregate_csvs(
    partners_dir: Path = PARTNERS_DIR,
    output_file: str = "",
    deduplicate: bool = True,
) -> dict:
    """Aggregate all CSVs in the partners directory."""
    all_rows = []
    source_stats = Counter()
    category_stats = Counter()
    files_processed = 0
    files_empty = 0

    csv_files = sorted(partners_dir.glob("*.csv"))
    print(f"\nScanning {len(csv_files)} CSV files in {partners_dir}/\n")

    for csv_file in csv_files:
        if csv_file.name.startswith("_"):
            continue  # Skip progress/metadata files

        try:
            with open(csv_file, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                rows = []
                for row in reader:
                    normalized = normalize_row(row, str(csv_file))
                    if is_valid_contact(normalized):
                        rows.append(normalized)

                source_name = csv_file.stem
                source_stats[source_name] = len(rows)
                files_processed += 1

                if not rows:
                    files_empty += 1
                else:
                    for r in rows:
                        cat = r.get("source_category", "unknown")
                        category_stats[cat] += 1
                    all_rows.extend(rows)

                status = f"{len(rows):>6,} contacts" if rows else "  (empty)"
                print(f"  {csv_file.name:50s} {status}")

        except Exception as e:
            print(f"  {csv_file.name:50s}  ERROR: {e}")

    print(f"\n{'='*60}")
    print(f"AGGREGATION SUMMARY")
    print(f"{'='*60}")
    print(f"  CSV files processed:  {files_processed}")
    print(f"  Empty files:          {files_empty}")
    print(f"  Total raw contacts:   {len(all_rows):,}")

    if deduplicate:
        unique_rows = dedup_contacts(all_rows)
        print(f"  After deduplication:  {len(unique_rows):,}")
        print(f"  Duplicates removed:   {len(all_rows) - len(unique_rows):,}")
        all_rows = unique_rows

    # Stats by source
    print(f"\n  TOP SOURCES:")
    for source, count in source_stats.most_common(20):
        print(f"    {source:40s} {count:>8,}")

    # Stats by category
    if category_stats:
        print(f"\n  BY CATEGORY:")
        for cat, count in category_stats.most_common(20):
            if cat:
                print(f"    {cat:40s} {count:>8,}")

    # Contact quality stats
    with_email = sum(1 for r in all_rows if r.get("email"))
    with_website = sum(1 for r in all_rows if r.get("website"))
    with_linkedin = sum(1 for r in all_rows if r.get("linkedin"))
    with_phone = sum(1 for r in all_rows if r.get("phone"))
    with_bio = sum(1 for r in all_rows if r.get("bio"))

    print(f"\n  CONTACT QUALITY:")
    print(f"    With email:    {with_email:>8,} ({100*with_email/max(len(all_rows),1):.1f}%)")
    print(f"    With website:  {with_website:>8,} ({100*with_website/max(len(all_rows),1):.1f}%)")
    print(f"    With LinkedIn: {with_linkedin:>8,} ({100*with_linkedin/max(len(all_rows),1):.1f}%)")
    print(f"    With phone:    {with_phone:>8,} ({100*with_phone/max(len(all_rows),1):.1f}%)")
    print(f"    With bio:      {with_bio:>8,} ({100*with_bio/max(len(all_rows),1):.1f}%)")

    # Export master CSV
    if output_file:
        output_path = partners_dir / output_file
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=STANDARD_FIELDS)
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"\n  Master CSV: {output_path} ({len(all_rows):,} contacts)")

    print(f"{'='*60}\n")

    return {
        "total": len(all_rows),
        "with_email": with_email,
        "with_website": with_website,
        "sources": dict(source_stats),
    }


def main():
    parser = argparse.ArgumentParser(description="Aggregate scraped CSVs")
    parser.add_argument("--output", default="", help="Output master CSV filename")
    parser.add_argument("--dedup", action="store_true", help="Deduplicate contacts")
    parser.add_argument("--dir", default=str(PARTNERS_DIR), help="Directory to scan")
    args = parser.parse_args()

    aggregate_csvs(
        partners_dir=Path(args.dir),
        output_file=args.output,
        deduplicate=args.dedup,
    )


if __name__ == "__main__":
    main()
