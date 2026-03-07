#!/usr/bin/env python3
"""
Import the cleaned master CSV into Supabase, writing 18 columns with rich
enrichment_metadata. Nothing scraped is lost.

Reads the output of aggregate_clean.py (MASTER_JV_CLEAN.csv by default) and
writes to the Supabase `profiles` table.

Usage:
    python3 scripts/sourcing/import_csv.py                    # Import MASTER_JV_CLEAN.csv
    python3 scripts/sourcing/import_csv.py --dry-run          # Preview with sample mapping
    python3 scripts/sourcing/import_csv.py --file other.csv   # Specific file
    python3 scripts/sourcing/import_csv.py --skip-existing    # Skip dedup (fresh DB)
"""

from __future__ import annotations

import csv
import json
import os
import re
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


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_FILE = project_root / "Filling Database" / "MASTER_JV_CLEAN.csv"
BATCH_SIZE = 500

# Unique batch ID for this import run — enables targeted rollback via:
#   DELETE FROM profiles WHERE enrichment_metadata->>'batch_id' = '<this value>';
_BATCH_ID = f"import_{datetime.now(timezone.utc).strftime('%Y-%m-%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

SOURCE_PRIORITY = {
    "apple_podcasts": 90, "youtube_api": 90, "trustpilot": 90,
    "noomii": 85, "psychology_today": 85, "tedx": 85,
    "usaspending": 80, "sam_awards": 80, "muncheye": 80, "muncheye_launches": 80,
    "coaching_federation": 80, "jvnotifypro": 80,
    "yc_companies": 75, "shopify_partners": 75, "aws_marketplace": 75,
    "microsoft_appsource": 75, "salesforce_appexchange": 75,
    "usaspending_recipients": 75, "speaking_com": 75,
    "sec_edgar": 70, "sec_edgar_search": 70, "fdic_banks": 70,
    "techstars_portfolio": 70, "fivehundred_global": 70, "a16z_portfolio": 70,
    "producthunt": 70, "fda_devices": 70, "webflow_experts": 70,
    "atlassian_marketplace": 70, "stripe_partners": 70, "hubspot_partners": 70,
    "capterra_listings": 70, "g2_reviews": 70, "expertfile": 70,
    "clutch_sitemap": 65, "betalist": 65, "startupgrind": 65, "f6s_startups": 65,
    "bbb_sitemap": 65, "slack_app_directory": 65, "zapier_partners": 65,
    "partnerstack_marketplace": 65, "sessionize": 65, "podcastindex": 65,
    "irs_business_leagues": 60, "chrome_extensions": 60,
    "shareasale_merchants": 60, "shareasale": 60,
    "cj_affiliates": 60, "impact_partners": 60,
    "epa_echo": 55, "chambers": 50,
    "openlibrary": 45, "openlibrary_v2": 45, "google_books": 45,
    "irs_exempt": 40,
}
DEFAULT_SOURCE_PRIORITY = 50

JV_SOURCES = {"muncheye", "muncheye_launches", "jvnotifypro"}
CONTENT_SOURCES = {"apple_podcasts", "youtube_api", "podcastindex"}

PLATFORM_DOMAINS = {
    "openlibrary.org", "books.google.com", "google.com",
    "podcasts.apple.com", "itunes.apple.com",
    "psychologytoday.com", "youtube.com", "www.youtube.com",
    "ted.com", "www.ted.com", "noomii.com", "www.noomii.com",
    "coachingfederation.org", "apps.coachingfederation.org",
    "sessionize.com", "doi.org", "speaking.com",
    "wikidata.org", "query.wikidata.org",
    "expertfile.com", "linkedin.com", "www.linkedin.com",
    "trustpilot.com", "www.trustpilot.com",
    "sec.gov", "www.sec.gov", "data.sec.gov",
    "usaspending.gov", "www.usaspending.gov",
    "banks.data.fdic.gov",
    "ui.awin.com", "www.awin.com",
    "ycombinator.com", "www.ycombinator.com",
    "shopify.com", "www.shopify.com",
}


# ---------------------------------------------------------------------------
# Helpers
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
    return host[4:] if host.startswith("www.") else host


def _clean(val: str | None) -> str | None:
    """Return None for empty/None-like strings, else stripped value."""
    if not val:
        return None
    v = val.replace("\x00", "").strip()
    if v.lower() in ("none", "null", ""):
        return None
    return v


def get_connection():
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("ERROR: DATABASE_URL not set in environment.")
        sys.exit(1)
    return psycopg2.connect(db_url)


# ---------------------------------------------------------------------------
# Row -> Supabase column mapping
# ---------------------------------------------------------------------------

def map_to_supabase(row: dict) -> dict:
    """Transform a CSV row into Supabase column values + enrichment_metadata."""
    source = (row.get("source") or "").strip()
    now_iso = datetime.now(timezone.utc).isoformat()
    priority = SOURCE_PRIORITY.get(source, DEFAULT_SOURCE_PRIORITY)

    # Direct columns
    name = _clean(row.get("name"))
    email = _clean(row.get("email"))
    company = _clean(row.get("company"))
    website = _clean(row.get("website"))
    linkedin = _clean(row.get("linkedin"))
    phone = _clean(row.get("phone"))
    bio = _clean(row.get("bio"))

    # categories -> tags (TEXT[]) + niche (TEXT)
    categories_raw = _clean(row.get("categories"))
    tags = None
    niche = None
    if categories_raw:
        tag_list = [t.strip().lower() for t in categories_raw.split(",") if t.strip()]
        seen = set()
        deduped = []
        for t in tag_list:
            if t not in seen:
                seen.add(t)
                deduped.append(t)
        tags = deduped[:7]
        niche = deduped[0] if deduped else None

    # product_focus -> business_focus
    business_focus = _clean(row.get("product_focus"))

    # revenue_indicator -> revenue_tier
    rev_raw = _clean(row.get("revenue_indicator"))
    revenue_tier = None
    revenue_raw_for_meta = rev_raw
    if rev_raw:
        m = re.match(r"^(enterprise|premium|established|emerging|micro):\s*(.+)", rev_raw)
        if m:
            revenue_tier = m.group(1)
            revenue_raw_for_meta = m.group(2).strip()

    # JV history (muncheye/jvnotifypro)
    jv_history = None
    if source in JV_SOURCES:
        jv_entry = _build_jv_history(row)
        if jv_entry:
            jv_history = [jv_entry]

    # Content platforms (podcasts/youtube)
    content_platforms = None
    if source in CONTENT_SOURCES:
        content_platforms = _build_content_platforms(row)

    # enrichment_metadata — field_meta for every populated field
    ingestion_source = f"scraper_{source}"
    populated = {
        "name": name, "email": email, "company": company, "website": website,
        "linkedin": linkedin, "phone": phone, "bio": bio,
        "niche": niche, "business_focus": business_focus, "revenue_tier": revenue_tier,
    }
    field_meta = {}
    for fname, fval in populated.items():
        if fval:
            field_meta[fname] = {
                "source": ingestion_source,
                "updated_at": now_iso,
                "pipeline_version": 1,
            }

    # scraper_data: catch-all for data without dedicated columns
    scraper_data = {}
    for key in ("pricing", "rating", "review_count", "tier", "location",
                "join_date", "source_url", "source_category", "scraped_at"):
        val = _clean(row.get(key))
        if val:
            scraper_data[key] = val

    if revenue_raw_for_meta:
        scraper_data["revenue_raw"] = revenue_raw_for_meta

    # Extract JV-specific fields from bio into scraper_data
    if source in JV_SOURCES and bio:
        for part in bio.split("|"):
            pl = part.strip().lower()
            if pl.startswith("commission:"):
                scraper_data["commission"] = part.split(":", 1)[1].strip()
            elif pl.startswith("jv page:"):
                scraper_data["jv_page_url"] = part.split(":", 1)[1].strip()
            elif pl.startswith("network:"):
                scraper_data["affiliate_network"] = part.split(":", 1)[1].strip()
            elif pl.startswith("launch:"):
                scraper_data["launch_date"] = part.split(":", 1)[1].strip()

    enrichment_metadata = {
        "ingestion_source": ingestion_source,
        "ingested_at": now_iso,
        "batch_id": _BATCH_ID,
        "source_priority": priority,
        "original_source": source,
        "field_meta": field_meta,
        "scraper_data": scraper_data,
    }

    return {
        "name": name,
        "email": email,
        "company": company,
        "website": website,
        "linkedin": linkedin,
        "phone": phone,
        "bio": bio,
        "tags": tags,
        "niche": niche,
        "business_focus": business_focus,
        "revenue_tier": revenue_tier,
        "jv_history": jv_history,
        "content_platforms": content_platforms,
        "enrichment_metadata": enrichment_metadata,
    }


def _build_jv_history(row: dict) -> dict | None:
    """Build a JV history entry from muncheye/jvnotifypro bio data."""
    bio = _clean(row.get("bio")) or ""
    source = row.get("source", "")
    source_url = _clean(row.get("source_url"))

    commission = ""
    network = ""
    for part in bio.split("|"):
        pl = part.strip().lower()
        if pl.startswith("commission:"):
            commission = part.split(":", 1)[1].strip()
        elif pl.startswith("network:"):
            network = part.split(":", 1)[1].strip()

    if not commission and not network:
        return None

    quote_parts = []
    if commission:
        quote_parts.append(f"Commission: {commission}")
    if network:
        quote_parts.append(f"Network: {network}")

    return {
        "partner_name": _clean(row.get("name")) or "",
        "format": "affiliate",
        "source_quote": " | ".join(quote_parts),
        "source": source,
        "source_url": source_url or "",
    }


def _build_content_platforms(row: dict) -> dict | None:
    """Build content_platforms JSONB from podcast/youtube data."""
    result = {}
    product_focus = _clean(row.get("product_focus"))
    source = row.get("source", "")

    if source == "apple_podcasts":
        if product_focus:
            result["podcast_name"] = product_focus
        rev = _clean(row.get("revenue_indicator")) or ""
        m = re.search(r"(\d+)\s*episodes?", rev, re.I)
        if m:
            result["episode_count"] = m.group(1)
        result["platform"] = "Apple Podcasts"

    elif source == "youtube_api":
        result["platform"] = "YouTube"
        if product_focus:
            result["channel_name"] = product_focus

    elif source == "podcastindex":
        result["platform"] = "Podcast Index"
        if product_focus:
            result["podcast_name"] = product_focus

    return result if result else None


# ---------------------------------------------------------------------------
# Dedup against existing DB (O(1) via hash indexes)
# ---------------------------------------------------------------------------

def load_existing_profiles(cur) -> dict:
    cur.execute("SELECT id, name, email, website, linkedin, company FROM profiles")
    return {str(r["id"]): r for r in cur.fetchall()}


def build_dedup_index(existing: dict) -> tuple[dict, dict, dict]:
    email_idx: dict[str, str] = {}
    name_co_idx: dict[str, str] = {}
    name_domain_idx: dict[str, str] = {}

    for pid, row in existing.items():
        email = (row.get("email") or "").strip().lower()
        if email and "@" in email:
            email_idx[email] = pid

        name = (row.get("name") or "").strip().lower()
        company = (row.get("company") or "").strip().upper()
        if name and company:
            name_co_idx[f"{name}|{company}"] = pid

        domain = _normalize_domain(row.get("website"))
        if name and domain and domain not in PLATFORM_DOMAINS:
            name_domain_idx[f"{name}|{domain}"] = pid

    return email_idx, name_co_idx, name_domain_idx


def find_duplicate(contact: dict, email_idx: dict, name_co_idx: dict,
                   name_domain_idx: dict) -> str | None:
    email = (contact.get("email") or "").strip().lower()
    if email and email in email_idx:
        return email_idx[email]

    name = (contact.get("name") or "").strip().lower()
    company = (contact.get("company") or "").strip().upper()
    if name and company and f"{name}|{company}" in name_co_idx:
        return name_co_idx[f"{name}|{company}"]

    domain = _normalize_domain(contact.get("website"))
    if name and domain and domain not in PLATFORM_DOMAINS:
        key = f"{name}|{domain}"
        if key in name_domain_idx:
            return name_domain_idx[key]

    return None


# ---------------------------------------------------------------------------
# Main import
# ---------------------------------------------------------------------------

INSERT_SQL = """
INSERT INTO profiles (
    id, name, email, company, website, linkedin,
    phone, bio, tags, niche, business_focus,
    revenue_tier, jv_history, content_platforms,
    enrichment_metadata, status, created_at, updated_at
) VALUES %s
"""

INSERT_TEMPLATE = (
    "(%(id)s, %(name)s, %(email)s, %(company)s, %(website)s, %(linkedin)s, "
    "%(phone)s, %(bio)s, %(tags)s, %(niche)s, %(business_focus)s, "
    "%(revenue_tier)s, %(jv_history)s, %(content_platforms)s, "
    "%(enrichment_metadata)s, 'Pending', NOW(), NOW())"
)


def _row_to_insert_dict(mapped: dict) -> dict:
    """Convert mapped row to a dict for execute_values template."""
    return {
        "id": str(uuid.uuid4()),
        "name": mapped["name"],
        "email": mapped["email"],
        "company": mapped["company"],
        "website": mapped["website"],
        "linkedin": mapped["linkedin"],
        "phone": mapped["phone"],
        "bio": mapped["bio"],
        "tags": mapped["tags"],
        "niche": mapped["niche"],
        "business_focus": mapped["business_focus"],
        "revenue_tier": mapped["revenue_tier"],
        "jv_history": psycopg2.extras.Json(mapped["jv_history"]) if mapped["jv_history"] else None,
        "content_platforms": psycopg2.extras.Json(mapped["content_platforms"]) if mapped["content_platforms"] else None,
        "enrichment_metadata": psycopg2.extras.Json(mapped["enrichment_metadata"]),
    }


def import_csv(csv_path: Path, dry_run: bool = False,
               skip_existing: bool = False) -> tuple[int, int]:
    """Import CSV into Supabase using batch inserts. Returns (total, new_count)."""

    # Count total lines for progress (without loading all into memory)
    print(f"  Counting rows in {csv_path.name}...")
    with open(csv_path, "r", encoding="utf-8") as f:
        total = sum(1 for _ in f) - 1  # subtract header
    print(f"  Total rows: {total:,}")

    if dry_run:
        # Load just first 5 for preview
        sample = []
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= 5:
                    break
                sample.append(row)
        _preview_mapping(sample)
        print(f"\n  [DRY RUN] Would import {total:,} contacts")
        return total, 0

    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    email_idx, name_co_idx, name_domain_idx = {}, {}, {}
    if not skip_existing:
        print("  Loading existing profiles for dedup...")
        existing = load_existing_profiles(cur)
        print(f"  Found {len(existing):,} existing profiles")
        email_idx, name_co_idx, name_domain_idx = build_dedup_index(existing)
    else:
        print("  Skipping dedup (--skip-existing)")

    new_count = 0
    dup_count = 0
    batch: list[dict] = []
    processed = 0
    import time
    t0 = time.time()

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("name") or "").strip()
            if not name or len(name) < 2 or name.lower() in ("none", "null"):
                processed += 1
                continue

            mapped = map_to_supabase(row)
            if not mapped.get("name"):
                processed += 1
                continue

            if not skip_existing:
                dup_id = find_duplicate(mapped, email_idx, name_co_idx, name_domain_idx)
                if dup_id is not None:
                    dup_count += 1
                    processed += 1
                    continue

            insert_dict = _row_to_insert_dict(mapped)
            batch.append(insert_dict)

            # Index for within-batch dedup
            if not skip_existing:
                email = (mapped["email"] or "").strip().lower()
                if email and "@" in email:
                    email_idx[email] = insert_dict["id"]
                nm = (mapped["name"] or "").strip().lower()
                co = (mapped["company"] or "").strip().upper()
                if nm and co:
                    name_co_idx[f"{nm}|{co}"] = insert_dict["id"]
                domain = _normalize_domain(mapped["website"])
                if nm and domain and domain not in PLATFORM_DOMAINS:
                    name_domain_idx[f"{nm}|{domain}"] = insert_dict["id"]

            new_count += 1
            processed += 1

            # Flush batch
            if len(batch) >= BATCH_SIZE:
                psycopg2.extras.execute_values(
                    cur, INSERT_SQL, batch, template=INSERT_TEMPLATE, page_size=BATCH_SIZE
                )
                conn.commit()
                batch = []

                if processed % 10000 == 0:
                    elapsed = time.time() - t0
                    rate = processed / elapsed if elapsed > 0 else 0
                    eta = (total - processed) / rate if rate > 0 else 0
                    print(
                        f"    {processed:>9,}/{total:,} | "
                        f"new={new_count:,} dups={dup_count:,} | "
                        f"{rate:.0f} rows/s | ETA {eta/60:.1f}min"
                    )

    # Final partial batch
    if batch:
        psycopg2.extras.execute_values(
            cur, INSERT_SQL, batch, template=INSERT_TEMPLATE, page_size=BATCH_SIZE
        )
        conn.commit()

    cur.close()
    conn.close()

    elapsed = time.time() - t0
    print(f"  Completed in {elapsed/60:.1f} minutes ({elapsed:.0f}s)")

    return total, new_count


def _preview_mapping(sample_rows: list[dict]) -> None:
    """Print sample column mappings for dry-run preview."""
    print(f"\n  {'─' * 60}")
    print(f"  SAMPLE COLUMN MAPPINGS (first {len(sample_rows)} rows)")
    print(f"  {'─' * 60}")

    for i, row in enumerate(sample_rows):
        mapped = map_to_supabase(row)
        print(f"\n  Row {i+1}: {mapped['name']}")
        print(f"    email:             {mapped['email']}")
        print(f"    company:           {mapped['company']}")
        print(f"    website:           {mapped['website']}")
        print(f"    tags:              {mapped['tags']}")
        print(f"    niche:             {mapped['niche']}")
        print(f"    business_focus:    {mapped['business_focus']}")
        print(f"    revenue_tier:      {mapped['revenue_tier']}")
        jv = json.dumps(mapped['jv_history']) if mapped['jv_history'] else 'null'
        cp = json.dumps(mapped['content_platforms']) if mapped['content_platforms'] else 'null'
        print(f"    jv_history:        {jv}")
        print(f"    content_platforms: {cp}")

        meta = mapped["enrichment_metadata"]
        print(f"    enrichment_metadata:")
        print(f"      source:          {meta['original_source']}")
        print(f"      priority:        {meta['source_priority']}")
        sd = json.dumps(meta['scraper_data'], indent=8)
        if len(sd) > 200:
            sd = sd[:200] + "..."
        print(f"      scraper_data:    {sd}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    dry_run = "--dry-run" in sys.argv
    skip_existing = "--skip-existing" in sys.argv

    csv_path = DEFAULT_FILE
    for i, arg in enumerate(sys.argv[1:]):
        if arg == "--file" and i + 2 < len(sys.argv):
            csv_path = Path(sys.argv[i + 2])

    if isinstance(csv_path, str):
        csv_path = Path(csv_path)

    if not csv_path.exists():
        print(f"ERROR: {csv_path} not found")
        print(f"  Run aggregate_clean.py first to generate {csv_path.name}")
        sys.exit(1)

    print(f"\n{'=' * 60}")
    print("SUPABASE IMPORT — 18-COLUMN ENRICHED")
    print(f"{'=' * 60}")
    print(f"  File:           {csv_path.name}")
    print(f"  Batch ID:       {_BATCH_ID}")
    print(f"  Dry run:        {dry_run}")
    print(f"  Skip dedup:     {skip_existing}")
    print(f"  Columns:        18 (up from 10)")
    print(f"  Rollback:       DELETE FROM profiles WHERE enrichment_metadata->>'batch_id' = '{_BATCH_ID}';")
    print()

    total, new_count = import_csv(csv_path, dry_run=dry_run, skip_existing=skip_existing)

    print(f"\n{'=' * 60}")
    print("IMPORT COMPLETE")
    print(f"  Total processed: {total:,}")
    print(f"  New profiles:    {new_count:,}")
    print(f"  Duplicates:      {total - new_count:,}")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
