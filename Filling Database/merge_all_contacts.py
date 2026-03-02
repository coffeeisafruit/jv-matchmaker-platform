#!/usr/bin/env python3
"""
Merge & Deduplicate All JV Contact CSVs

Reads all CSV files from partners/, chambers/, and existing output/,
deduplicates by normalized company name, and writes a single master CSV.

Usage:
    python3 "Filling Database/merge_all_contacts.py"
    python3 "Filling Database/merge_all_contacts.py" --stats
    python3 "Filling Database/merge_all_contacts.py" --output "Filling Database/MASTER_500K.csv"
"""

import argparse
import csv
import os
import sys
from collections import defaultdict
from pathlib import Path


# Standard CSV header (matches ScrapedContact output + JV partnership fields)
HEADER = [
    "name", "email", "company", "website", "linkedin", "phone", "bio",
    "pricing", "rating", "review_count", "tier", "categories",
    "location", "join_date", "product_focus", "revenue_indicator",
    "source", "source_url",
]

# Source priority for deduplication (higher = keep this one)
# When two records have the same normalized company name, keep the higher-priority one
SOURCE_PRIORITY = {
    "apple_podcasts": 90,       # High-quality: podcast hosts with audiences
    "youtube_api": 90,          # High-quality: YouTube creators
    "trustpilot": 90,           # Businesses with emails/phones/websites
    "noomii": 85,               # Coaches with profiles
    "psychology_today": 85,     # Therapists/coaches
    "tedx": 85,                 # Speakers
    "usaspending": 80,          # Federal contractors (NAICS-targeted)
    "usaspending_recipients": 75,  # Federal contractors (bulk)
    "sam_awards": 80,           # Contract award winners
    "fdic_banks": 70,           # Banks
    "sec_edgar": 70,            # Public companies
    "sec_edgar_search": 70,     # Public companies (search)
    "clutch_sitemap": 65,       # Agencies (name + URL only)
    "irs_business_leagues": 60, # Trade associations/chambers
    "irs_exempt": 40,           # Nonprofits (kept for later)
    "chambers": 50,             # Chambers of commerce
    "openlibrary": 45,          # Book authors
    "openlibrary_v2": 45,
    "google_books": 45,
    "muncheye": 80,             # JV-targeted launches
    "yc_companies": 75,         # YC startups
    "techstars_portfolio": 70,  # Techstars companies
    "fivehundred_global": 70,   # 500 Global portfolio
    "a16z_portfolio": 70,       # a16z portfolio
    "coaching_federation": 80,  # Certified coaches
    "producthunt": 70,          # Product Hunt startups
    "betalist": 65,             # Startup directory
    "startupgrind": 65,         # Startup community
    "f6s_startups": 65,         # F6S startups
    "bbb_sitemap": 65,          # BBB accredited businesses
    "epa_echo": 55,             # EPA regulated facilities
    "fda_devices": 70,          # FDA device manufacturers (has phones/contacts)
    "partnerstack_marketplace": 65,  # SaaS partner programs
    "shareasale_merchants": 60, # Affiliate merchants
    # SaaS partner ecosystems
    "shopify_partners": 75,       # Shopify agency partners with tiers/pricing
    "webflow_experts": 70,        # Webflow certified partners
    "atlassian_marketplace": 70,  # Atlassian marketplace vendors
    "aws_marketplace": 75,        # AWS ISVs with product/pricing data
    "microsoft_appsource": 75,    # Microsoft AppSource vendors
    "salesforce_appexchange": 75, # Salesforce AppExchange partners
    "chrome_extensions": 60,      # Chrome extension developers
    "slack_app_directory": 65,    # Slack app developers
    "stripe_partners": 70,        # Stripe consulting partners
    "zapier_partners": 65,        # Zapier integration partners
    "hubspot_partners": 70,       # HubSpot solutions partners
    "capterra_listings": 70,      # Capterra software vendors with reviews
    "g2_reviews": 70,             # G2 software vendors with reviews
    # Affiliate networks
    "cj_affiliates": 60,          # CJ affiliate merchants
    "impact_partners": 60,        # Impact.com partners
}

# Files to skip when reading from partners/ (merge outputs, not source data)
SKIP_FILES = {"_master_contacts.csv", "_scraping_progress.json", "irs_exempt.csv"}


def normalize_name(name: str) -> str:
    """Normalize company/contact name for deduplication."""
    if not name:
        return ""
    n = name.upper().strip()
    # Remove common suffixes
    for suffix in [" LLC", " L.L.C.", " INC", " INC.", " CORP", " CORP.",
                   " LTD", " LTD.", " CO", " CO.", " LP", " L.P.",
                   " PLLC", " PC", " P.C.", ",", "."]:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
    return n


def read_csv_file(filepath: str) -> list[dict]:
    """Read a CSV file and return rows as dicts."""
    rows = []
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Normalize field names to lowercase
                normalized = {}
                for key, value in row.items():
                    if key:
                        normalized[key.lower().strip()] = (value or "").strip()
                rows.append(normalized)
    except Exception as e:
        print(f"  WARNING: Error reading {filepath}: {e}")
    return rows


def collect_all_csvs(base_dir: str) -> dict[str, list[dict]]:
    """Collect all CSV files from the project."""
    all_data = {}
    base = Path(base_dir)

    # Partners directory
    partners_dir = base / "Filling Database" / "partners"
    if partners_dir.exists():
        for csv_file in sorted(partners_dir.glob("*.csv")):
            if csv_file.name in SKIP_FILES:
                continue
            rows = read_csv_file(str(csv_file))
            if rows:
                all_data[csv_file.stem] = rows

    # Chambers directory (all_chambers.csv)
    chambers_file = base / "Filling Database" / "chambers" / "all_chambers.csv"
    if chambers_file.exists():
        rows = read_csv_file(str(chambers_file))
        if rows:
            all_data["chambers"] = rows

    # Existing MERGED_ALL from previous scraping
    merged_file = base / "scripts" / "sourcing" / "output" / "MERGED_ALL.csv"
    if merged_file.exists():
        rows = read_csv_file(str(merged_file))
        if rows:
            all_data["merged_all"] = rows

    return all_data


def merge_and_dedup(all_data: dict[str, list[dict]]) -> list[dict]:
    """Merge all sources and deduplicate by normalized name."""
    # Index by normalized name, keeping highest-priority source
    seen = {}  # normalized_name -> (priority, row)
    total_input = 0
    duplicates = 0

    # JV partnership fields that should be merged (fill blanks from lower-priority sources)
    jv_fields = ["pricing", "rating", "review_count", "tier", "categories",
                 "location", "join_date", "product_focus", "revenue_indicator"]

    for source_name, rows in all_data.items():
        for row in rows:
            total_input += 1
            name = row.get("name", "")
            company = row.get("company", "")
            source = row.get("source", source_name)

            # Use company name for dedup if available, else contact name
            dedup_key = normalize_name(company or name)
            if not dedup_key or len(dedup_key) < 3:
                continue

            priority = SOURCE_PRIORITY.get(source, 50)

            if dedup_key in seen:
                existing_priority, existing_row = seen[dedup_key]
                if priority > existing_priority:
                    # Replace with higher-priority source, but keep JV fields from old
                    for jvf in jv_fields:
                        if not row.get(jvf) and existing_row.get(jvf):
                            row[jvf] = existing_row[jvf]
                    seen[dedup_key] = (priority, row)
                else:
                    # Fill in any missing JV fields from this lower-priority source
                    for jvf in jv_fields:
                        if not existing_row.get(jvf) and row.get(jvf):
                            existing_row[jvf] = row[jvf]
                duplicates += 1
            else:
                seen[dedup_key] = (priority, row)

    # Extract deduplicated rows
    result = [row for _, row in seen.values()]

    print(f"\n  Total input rows:   {total_input:,}")
    print(f"  Duplicates removed: {duplicates:,}")
    print(f"  Unique contacts:    {len(result):,}")

    return result


def write_master_csv(rows: list[dict], output_path: str):
    """Write merged data to a master CSV."""
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"\n  Written to: {output_path}")
    print(f"  File size:  {os.path.getsize(output_path) / 1024 / 1024:.1f} MB")


def print_stats(all_data: dict[str, list[dict]]):
    """Print detailed source statistics."""
    print("\n=== SOURCE BREAKDOWN ===\n")
    total = 0
    for source_name in sorted(all_data.keys(), key=lambda s: -len(all_data[s])):
        count = len(all_data[source_name])
        total += count
        print(f"  {source_name:35s} {count:>8,} rows")
    print(f"  {'TOTAL':35s} {total:>8,} rows")


def main():
    parser = argparse.ArgumentParser(description="Merge all JV contact CSVs")
    parser.add_argument("--output", default="Filling Database/MASTER_JV_CONTACTS.csv",
                        help="Output file path")
    parser.add_argument("--stats", action="store_true", help="Print detailed stats only")
    args = parser.parse_args()

    # Find project root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)

    print("=== JV Contact Merge & Dedup ===")
    print(f"  Project root: {project_root}")

    # Collect all CSVs
    print("\nCollecting CSV files...")
    all_data = collect_all_csvs(project_root)

    if not all_data:
        print("ERROR: No CSV files found!")
        sys.exit(1)

    # Print stats
    print_stats(all_data)

    if args.stats:
        return

    # Merge and deduplicate
    print("\nMerging and deduplicating...")
    merged = merge_and_dedup(all_data)

    # Write output
    output_path = os.path.join(project_root, args.output)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    write_master_csv(merged, output_path)

    # Category breakdown
    categories = defaultdict(int)
    for row in merged:
        source = row.get("source", "unknown")
        categories[source] += 1

    print("\n=== MERGED SOURCE BREAKDOWN ===\n")
    for source, count in sorted(categories.items(), key=lambda x: -x[1]):
        print(f"  {source:35s} {count:>8,}")
    print(f"  {'TOTAL':35s} {len(merged):>8,}")


if __name__ == "__main__":
    main()
