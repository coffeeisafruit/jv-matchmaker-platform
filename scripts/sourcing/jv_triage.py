#!/usr/bin/env python3
"""
JV Triage — Classify 1.89M profiles into JV tiers and score readiness.

Source-based categorical triage first (expert-panel approved), then a weighted
JV readiness score. Tier X profiles are archived locally and deleted from
Supabase to leave a clean ~1.4M profile database.

Usage:
    python3 scripts/sourcing/jv_triage.py                   # Full run
    python3 scripts/sourcing/jv_triage.py --dry-run         # Report only, NO writes
    python3 scripts/sourcing/jv_triage.py --tier A,B        # Only process specific tiers
    python3 scripts/sourcing/jv_triage.py --top 100         # Show top N after scoring
    python3 scripts/sourcing/jv_triage.py --skip-delete     # Score all but keep Tier X
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# Environment setup
# ---------------------------------------------------------------------------
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
# Platform domains (imported concept from import_csv.py)
# ---------------------------------------------------------------------------
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


def _is_platform_url(url: str | None) -> bool:
    """Return True if the URL is a directory/platform page, not a real business site."""
    if not url:
        return True
    url = url.strip().lower()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        host = urlparse(url).hostname or ""
    except Exception:
        return True
    host = host[4:] if host.startswith("www.") else host
    return host in PLATFORM_DOMAINS or not host


# ═══════════════════════════════════════════════════════════════════════════
# TIER CLASSIFICATION — Source Lookup Tables
# ═══════════════════════════════════════════════════════════════════════════

TIER_A_SOURCES = {
    "muncheye", "muncheye_launches", "jvnotifypro",
}

TIER_B_SOURCES = {
    "apple_podcasts", "apple_podcasts_full", "podcastindex", "youtube_api",
    "coaching_federation", "noomii", "speaking_com", "espeakers", "nsaspeakers",
    "tedx", "sessionize", "lifecoach_directory", "psychology_today", "toastmasters",
}

TIER_C_SOURCES = {
    "clutch_sitemap", "clutch_agencies", "shopify_partners", "hubspot_partners",
    "stripe_partners", "salesforce_appexchange", "zapier_partners",
    "partnerstack_marketplace", "webflow_experts", "atlassian_marketplace",
    "microsoft_appsource", "aws_marketplace", "capterra_listings", "g2_reviews",
    "fiverr_pros", "upwork_agencies", "thumbtack_pros", "wordpress_plugins",
    "chrome_extensions", "slack_app_directory", "producthunt", "betalist",
    "startupgrind", "f6s_startups", "indie_hackers",
}

TIER_D_SOURCES = {
    "irs_business_leagues", "chambers", "bni_members", "eonetwork",
    "vistage_members", "score_mentors", "alignable", "meetup_organizers",
}

TIER_E_SOURCES = {
    "bbb_sitemap", "trustpilot", "glassdoor_companies", "dnb_listings",
    "google_maps_places", "yelp_businesses", "state_business_registrations",
}

TIER_X_SOURCES = {
    "epa_echo", "fda_devices", "fdic_banks", "census_business",
    "crossref", "wikidata", "openlibrary", "openlibrary_v2", "google_books",
    "sec_edgar", "sec_edgar_search",
    "usaspending", "usaspending_recipients", "sam_awards", "grants_gov",
    "gsa_sam", "sba_loans",
}

# IRS exempt: subsection '06' = business leagues (Tier D), others = Tier X
IRS_EXEMPT_SOURCE = "irs_exempt"

# ═══════════════════════════════════════════════════════════════════════════
# SOURCE JV AFFINITY SCORES (Component 1, 35% weight)
# ═══════════════════════════════════════════════════════════════════════════

SOURCE_JV_AFFINITY = {
    # Tier A — JV-native
    "muncheye": 100, "muncheye_launches": 100, "jvnotifypro": 100,
    # Affiliate networks
    "shareasale": 85, "shareasale_merchants": 85, "cj_affiliates": 85, "impact_partners": 85,
    # Coaches/speakers (audience owners)
    "coaching_federation": 75, "noomii": 75, "lifecoach_directory": 75,
    "speaking_com": 75, "espeakers": 75,
    # Content creators
    "apple_podcasts": 70, "apple_podcasts_full": 70, "podcastindex": 70,
    "youtube_api": 70, "sessionize": 70, "tedx": 70,
    # Startup/product platforms
    "producthunt": 60, "betalist": 60, "indie_hackers": 60,
    "startupgrind": 60, "f6s_startups": 60,
    # Service/tech partners
    "shopify_partners": 55, "hubspot_partners": 55, "stripe_partners": 55,
    "zapier_partners": 55, "partnerstack_marketplace": 55,
    "salesforce_appexchange": 55, "webflow_experts": 55,
    "atlassian_marketplace": 55, "microsoft_appsource": 55, "aws_marketplace": 55,
    # Service directories
    "clutch_sitemap": 50, "clutch_agencies": 50, "capterra_listings": 50,
    "g2_reviews": 50, "fiverr_pros": 50, "upwork_agencies": 50,
    "thumbtack_pros": 50, "wordpress_plugins": 50, "chrome_extensions": 50,
    "slack_app_directory": 50,
    # Network hubs
    "bni_members": 45, "eonetwork": 45, "vistage_members": 45,
    "irs_business_leagues": 45, "chambers": 45, "alignable": 45,
    "meetup_organizers": 45, "score_mentors": 45,
    # VC portfolios
    "yc_companies": 40, "techstars_portfolio": 40, "a16z_portfolio": 40,
    "accel_portfolio": 40, "benchmark_portfolio": 40, "bessemer_portfolio": 40,
    "founders_fund": 40, "general_catalyst": 40, "greylock_portfolio": 40,
    "index_ventures": 40, "lightspeed_portfolio": 40, "nea_portfolio": 40,
    "sequoia_portfolio": 40, "fivehundred_global": 40,
    # Speaker platforms
    "nsaspeakers": 70, "toastmasters": 45, "psychology_today": 70,
    # General business
    "bbb_sitemap": 30, "trustpilot": 30,
    "glassdoor_companies": 25, "dnb_listings": 25,
    "google_maps_places": 25, "yelp_businesses": 25,
    "state_business_registrations": 25,
    # IRS exempt c6 only
    "irs_exempt": 20,
    # Government/academic (Tier X but scored for promoted profiles)
    "epa_echo": 10, "fda_devices": 10, "fdic_banks": 10,
    "census_business": 10, "crossref": 10, "wikidata": 10,
    "openlibrary": 10, "openlibrary_v2": 10, "google_books": 10,
    "sec_edgar": 10, "sec_edgar_search": 10,
    "usaspending": 10, "usaspending_recipients": 10,
    "sam_awards": 10, "grants_gov": 10, "gsa_sam": 10, "sba_loans": 10,
}
DEFAULT_SOURCE_AFFINITY = 25


# ═══════════════════════════════════════════════════════════════════════════
# CLASSIFICATION + SCORING FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def classify_tier(row: dict) -> str:
    """Assign a JV tier (A/B/C/D/E/X) based on source + signal promotion rules."""
    meta = row.get("enrichment_metadata") or {}
    source = (meta.get("original_source") or "").strip().lower()
    scraper_data = meta.get("scraper_data") or {}

    # --- Determine base tier from source ---
    if source in TIER_A_SOURCES:
        base = "A"
    elif source in TIER_B_SOURCES:
        base = "B"
    elif source in TIER_C_SOURCES:
        base = "C"
    elif source in TIER_D_SOURCES:
        base = "D"
    elif source == IRS_EXEMPT_SOURCE:
        # Check subsection — '06' = business leagues (Tier D), others = Tier X
        subsection = str(scraper_data.get("subsection") or "").strip()
        base = "D" if subsection == "06" else "X"
    elif source in TIER_X_SOURCES:
        base = "X"
    elif source in TIER_E_SOURCES:
        base = "E"
    else:
        # Unknown source → default to E (general business)
        base = "E"

    # Also promote to B if ANY source has content_platforms
    if base not in ("A", "B") and row.get("content_platforms"):
        base = "B"

    # --- Promotion rules for Tier X ---
    if base == "X":
        jv_history = row.get("jv_history")
        if jv_history and (isinstance(jv_history, list) and len(jv_history) > 0
                          or isinstance(jv_history, str) and jv_history.strip()):
            return "A"  # Any X with jv_history → A

        if row.get("content_platforms"):
            return "B"  # Any X with content_platforms → B

        email = row.get("email")
        phone = row.get("phone")
        revenue_tier = row.get("revenue_tier")
        if email and phone and revenue_tier:
            return "E"  # Contactable business → E

    return base


def score_jv_readiness(row: dict, tier: str) -> float:
    """Compute 0-100 JV readiness score for tiers A-E. Returns 0.0 for Tier X."""
    if tier == "X":
        return 0.0

    meta = row.get("enrichment_metadata") or {}
    source = (meta.get("original_source") or "").strip().lower()
    scraper_data = meta.get("scraper_data") or {}

    # --- Component 1: Source JV Affinity (35%) ---
    c1 = SOURCE_JV_AFFINITY.get(source, DEFAULT_SOURCE_AFFINITY)

    # --- Component 2: JV Signals (25%) ---
    c2 = 0
    jv_history = row.get("jv_history")
    if jv_history:
        if isinstance(jv_history, list) and len(jv_history) > 0:
            c2 += 50
            if len(jv_history) >= 3:
                c2 += 20
        elif isinstance(jv_history, str) and jv_history.strip():
            c2 += 50

    bio = (row.get("bio") or "").lower()
    jv_keywords = ("commission", "affiliate", "jv", "joint venture")
    if any(kw in bio for kw in jv_keywords):
        c2 += 15

    if scraper_data.get("jv_page_url") or scraper_data.get("affiliate_network"):
        c2 += 15

    # NEW: seeking (Intent tier — AI-inferred, +10)
    seeking = (row.get("seeking") or "").strip()
    if len(seeking) > 10:
        c2 += 10

    # Seniority: decision-makers score higher (Apollo-derived)
    seniority = (row.get("seniority") or "").lower()
    if seniority in ("owner", "founder", "c_suite"):
        c2 += 15
    elif seniority in ("vp", "director"):
        c2 += 8

    # Intent signal from Apollo: active business development behavior
    if row.get("intent_signal"):
        c2 += 10

    c2 = min(c2, 100)

    # --- Component 3: Audience Signals (20%) ---
    c3 = 0
    if row.get("content_platforms"):
        c3 += 40

    list_size = row.get("list_size") or 0
    if isinstance(list_size, str):
        try:
            list_size = int(list_size)
        except ValueError:
            list_size = 0
    if list_size >= 10000:
        c3 += 30
    elif list_size >= 1000:
        c3 += 20
    elif list_size > 0:
        c3 += 10

    tags = row.get("tags") or []
    if isinstance(tags, str):
        tags = [tags]
    tags_lower = " ".join(t.lower() for t in tags if t)
    audience_tag_kw = ("podcast", "youtube", "newsletter", "course", "coaching",
                       "webinar", "masterclass", "membership")
    if any(kw in tags_lower for kw in audience_tag_kw):
        c3 += 15

    audience_bio_kw = ("subscriber", "follower", "listener", "student",
                       "member", "audience", "community")
    if any(kw in bio for kw in audience_bio_kw):
        c3 += 15

    # NEW: who_you_serve (Intent tier — AI-inferred, +8)
    who_you_serve = (row.get("who_you_serve") or "").strip()
    if len(who_you_serve) > 10:
        c3 += 8

    # NEW: audience_engagement_score (Computed tier — full points)
    aes = row.get("audience_engagement_score") or 0
    if isinstance(aes, str):
        try:
            aes = float(aes)
        except ValueError:
            aes = 0
    if aes > 0.5:
        c3 += 12
    elif aes > 0:
        c3 += 6

    c3 = min(c3, 100)

    # --- Component 4: Business Substance (12%) ---
    c4 = 0
    if row.get("company"):
        c4 += 20
    website = row.get("website")
    if website and not _is_platform_url(website):
        c4 += 25
    if row.get("revenue_tier"):
        c4 += 25
    if row.get("niche") or (tags and len(tags) > 0):
        c4 += 15
    if bio and len(bio) > 100:
        c4 += 15

    # NEW: what_you_do (Fit tier — observable, +15)
    what_you_do = (row.get("what_you_do") or "").strip()
    if len(what_you_do) > 10:
        c4 += 15

    # NEW: offering (Fit tier — observable, +12)
    offering = (row.get("offering") or "").strip()
    if len(offering) > 10:
        c4 += 12

    # NEW: signature_programs (Fit tier — observable, +10)
    sig_programs = (row.get("signature_programs") or "").strip()
    if len(sig_programs) > 5:
        c4 += 10

    c4 = min(c4, 100)

    # --- Component 5: Contactability (8%) ---
    c5 = 0
    if row.get("email"):
        c5 += 40
    if row.get("phone"):
        c5 += 20
    if row.get("linkedin"):
        c5 += 25
    if website:
        c5 += 15

    # NEW: booking_link (Fit tier — factual URL, +20)
    booking_link = (row.get("booking_link") or "").strip()
    if booking_link:
        c5 += 20

    # NEW: network_role (Computed tier — graph-derived, +10)
    network_role = (row.get("network_role") or "").strip().lower()
    if network_role in ("hub", "bridge"):
        c5 += 10

    # Email confidence: high confidence = more contactable (Apollo-derived)
    email_conf = row.get("email_confidence") or 0
    if isinstance(email_conf, str):
        try:
            email_conf = float(email_conf)
        except ValueError:
            email_conf = 0
    if email_conf >= 0.8:
        c5 += 10
    elif email_conf >= 0.5:
        c5 += 5

    c5 = min(c5, 100)

    score = (c1 * 0.35) + (c2 * 0.25) + (c3 * 0.20) + (c4 * 0.12) + (c5 * 0.08)
    return round(score, 1)


# ═══════════════════════════════════════════════════════════════════════════
# DATABASE OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════

def get_connection():
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("ERROR: DATABASE_URL not set.")
        sys.exit(1)
    return psycopg2.connect(db_url)


def ensure_columns(conn):
    """Add jv_tier, jv_readiness_score, and Apollo-derived columns if they don't exist."""
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS jv_tier VARCHAR(1);")
        cur.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS jv_readiness_score FLOAT;")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_profiles_jv_tier ON profiles (jv_tier);")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_profiles_jv_score "
            "ON profiles (jv_readiness_score DESC NULLS LAST);"
        )
        # Apollo-derived columns (promoted from enrichment_metadata.apollo_data)
        cur.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS seniority VARCHAR(30);")
        cur.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS email_confidence FLOAT;")
        cur.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS engagement_likelihood BOOLEAN;")
        cur.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS intent_signal BOOLEAN;")
        cur.execute("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS funding_stage VARCHAR(30);")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_profiles_seniority "
            "ON profiles (seniority) WHERE seniority IS NOT NULL;"
        )
    conn.commit()
    print("  Columns jv_tier + jv_readiness_score + Apollo fields ensured.")


FETCH_COLS = (
    "id, name, email, company, website, linkedin, phone, bio, tags, niche, "
    "revenue_tier, jv_history, content_platforms, list_size, enrichment_metadata, "
    "what_you_do, who_you_serve, seeking, offering, signature_programs, "
    "booking_link, audience_engagement_score, network_role, "
    "seniority, email_confidence, intent_signal"
)

COL_NAMES = [
    "id", "name", "email", "company", "website", "linkedin", "phone",
    "bio", "tags", "niche", "revenue_tier", "jv_history",
    "content_platforms", "list_size", "enrichment_metadata",
    "what_you_do", "who_you_serve", "seeking", "offering",
    "signature_programs", "booking_link", "audience_engagement_score",
    "network_role", "seniority", "email_confidence", "intent_signal",
]


def stream_profiles(conn, batch_size: int = 5000):
    """Yield all profiles in batches using a server-side cursor."""
    cur = conn.cursor(name="jv_triage_cursor")
    cur.itersize = batch_size
    cur.execute(f"SELECT {FETCH_COLS} FROM profiles")

    batch = []
    for db_row in cur:
        row = dict(zip(COL_NAMES, db_row))
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
    cur.close()


def batch_update_tiers(conn, updates: list[tuple], batch_size: int = 5000):
    """Batch UPDATE jv_tier + jv_readiness_score. updates = [(tier, score, id), ...]"""
    total = len(updates)
    written = 0
    with conn.cursor() as cur:
        for i in range(0, total, batch_size):
            chunk = updates[i : i + batch_size]
            psycopg2.extras.execute_values(
                cur,
                "UPDATE profiles SET jv_tier = data.tier, jv_readiness_score = data.score "
                "FROM (VALUES %s) AS data(tier, score, id) "
                "WHERE profiles.id = data.id",
                chunk,
                template="(%s, %s, %s::uuid)",
            )
            written += len(chunk)
            if written % 50000 < batch_size:
                print(f"    Updated {written:,} / {total:,} profiles...")
    conn.commit()
    print(f"    Updated {total:,} profiles total.")


def batch_delete(conn, ids: list, batch_size: int = 1000):
    """Delete Tier X profiles in batches."""
    total = len(ids)
    deleted = 0
    with conn.cursor() as cur:
        for i in range(0, total, batch_size):
            chunk = ids[i : i + batch_size]
            cur.execute(
                "DELETE FROM profiles WHERE id = ANY(%s::uuid[])",
                ([str(uid) for uid in chunk],),
            )
            deleted += len(chunk)
            if deleted % 10000 < batch_size:
                print(f"    Deleted {deleted:,} / {total:,}...")
    conn.commit()
    print(f"    Deleted {total:,} Tier X profiles.")


# ═══════════════════════════════════════════════════════════════════════════
# ARCHIVE TO CSV
# ═══════════════════════════════════════════════════════════════════════════

ARCHIVE_PATH = project_root / "Filling Database" / "ARCHIVED_NON_JV_PROFILES.csv"

ARCHIVE_COLS = [
    "id", "name", "email", "company", "website", "linkedin", "phone",
    "bio", "tags", "niche", "revenue_tier", "jv_history",
    "content_platforms", "list_size", "original_source", "exclusion_reason",
]


def archive_tier_x(tier_x_rows: list[dict]):
    """Write Tier X profiles to local CSV for safekeeping."""
    ARCHIVE_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(ARCHIVE_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ARCHIVE_COLS, extrasaction="ignore")
        writer.writeheader()
        for row in tier_x_rows:
            meta = row.get("enrichment_metadata") or {}
            archive_row = {
                "id": str(row["id"]),
                "name": row.get("name") or "",
                "email": row.get("email") or "",
                "company": row.get("company") or "",
                "website": row.get("website") or "",
                "linkedin": row.get("linkedin") or "",
                "phone": row.get("phone") or "",
                "bio": (row.get("bio") or "")[:500],  # Truncate for archive size
                "tags": json.dumps(row.get("tags") or []),
                "niche": row.get("niche") or "",
                "revenue_tier": row.get("revenue_tier") or "",
                "jv_history": json.dumps(row.get("jv_history") or []),
                "content_platforms": json.dumps(row.get("content_platforms") or {}),
                "list_size": row.get("list_size") or 0,
                "original_source": meta.get("original_source", ""),
                "exclusion_reason": f"Tier X source: {meta.get('original_source', 'unknown')}",
            }
            writer.writerow(archive_row)

    print(f"  Archived {len(tier_x_rows):,} Tier X profiles → {ARCHIVE_PATH}")


# ═══════════════════════════════════════════════════════════════════════════
# REPORT
# ═══════════════════════════════════════════════════════════════════════════

TIER_LABELS = {
    "A": "JV-Native",
    "B": "Audience Owners",
    "C": "Service Complementors",
    "D": "Network Hubs",
    "E": "General Business",
    "X": "Non-Candidate",
}


def print_report(tier_counts: Counter, score_buckets: Counter,
                 source_counts: dict, top_profiles: list,
                 tier_x_source_counts: Counter, total: int,
                 dry_run: bool):
    """Print the full triage report."""
    print("\n" + "=" * 64)
    print("JV TRIAGE REPORT" + ("  [DRY RUN]" if dry_run else ""))
    print("=" * 64)

    # Tier distribution
    print("\nTier Distribution:")
    for tier in ("A", "B", "C", "D", "E", "X"):
        count = tier_counts.get(tier, 0)
        pct = (count / total * 100) if total else 0
        label = TIER_LABELS[tier]
        suffix = " → ARCHIVE + DELETE" if tier == "X" and not dry_run else ""
        print(f"  {tier} — {label:25s} {count:>10,}  ({pct:5.1f}%){suffix}")
    print(f"  {'':27s} {'─' * 10}")
    print(f"  {'Total':27s} {total:>10,}")

    # Tier X source breakdown
    if tier_x_source_counts:
        print("\nTier X Breakdown by Source:")
        for src, cnt in tier_x_source_counts.most_common(20):
            print(f"    {src:35s} {cnt:>10,}")

    # Score distribution (Tiers A-E only)
    scored_total = sum(score_buckets.values())
    if scored_total:
        print(f"\nScore Distribution (Tiers A-E, n={scored_total:,}):")
        for lo, hi, label in [(90, 100, "top JV prospects"), (70, 89, "strong candidates"),
                               (50, 69, "moderate potential"), (30, 49, "low but possible"),
                               (0, 29, "minimal signals")]:
            key = f"{lo}-{hi}"
            count = score_buckets.get(key, 0)
            print(f"  {key:>7s}:  {count:>10,}  ({label})")

    # Top profiles
    if top_profiles:
        n = len(top_profiles)
        print(f"\nTop {n} Prospects:")
        print(f"  {'Name':40s} {'Source':25s} {'Tier':4s} {'Score':>6s}")
        print(f"  {'─' * 40} {'─' * 25} {'─' * 4} {'─' * 6}")
        for p in top_profiles:
            name = (p["name"] or "?")[:40]
            source = p["source"][:25]
            print(f"  {name:40s} {source:25s} {p['tier']:4s} {p['score']:>6.1f}")

    print("\n" + "=" * 64)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="JV Triage — classify & score profiles")
    parser.add_argument("--dry-run", action="store_true", help="Classify + report, NO writes/deletes")
    parser.add_argument("--tier", type=str, default=None, help="Only process specific tiers (comma-separated, e.g. A,B)")
    parser.add_argument("--top", type=int, default=50, help="Show top N profiles (default 50)")
    parser.add_argument("--skip-delete", action="store_true", help="Score all tiers but don't delete Tier X")
    args = parser.parse_args()

    tier_filter = None
    if args.tier:
        tier_filter = set(t.strip().upper() for t in args.tier.split(","))
        print(f"Filtering to tiers: {', '.join(sorted(tier_filter))}")

    conn = get_connection()
    print(f"Connected to database.")

    # Step 0: Ensure columns exist (skip in dry-run)
    if not args.dry_run:
        print("\nStep 0: Ensuring jv_tier + jv_readiness_score columns...")
        ensure_columns(conn)

    # Step 1: Stream all profiles, classify + score
    print("\nStep 1: Streaming profiles, classifying tiers + scoring...")
    t0 = time.time()

    tier_counts = Counter()
    score_buckets = Counter()
    tier_x_source_counts = Counter()
    source_tier_counts = defaultdict(Counter)  # source -> {tier: count}

    tier_x_rows = []        # Full rows for archive
    tier_x_ids = []         # UUIDs for deletion
    updates = []            # (tier, score, id) for batch UPDATE
    top_profiles = []       # For report

    total = 0
    for batch in stream_profiles(conn):
        for row in batch:
            total += 1
            tier = classify_tier(row)

            # Apply tier filter if specified
            if tier_filter and tier not in tier_filter:
                continue

            meta = row.get("enrichment_metadata") or {}
            source = meta.get("original_source", "unknown")
            tier_counts[tier] += 1
            source_tier_counts[source][tier] += 1

            if tier == "X":
                tier_x_source_counts[source] += 1
                tier_x_rows.append(row)
                tier_x_ids.append(row["id"])
                continue

            score = score_jv_readiness(row, tier)
            updates.append((tier, score, str(row["id"])))

            # Score bucket
            if score >= 90:
                score_buckets["90-100"] += 1
            elif score >= 70:
                score_buckets["70-89"] += 1
            elif score >= 50:
                score_buckets["50-69"] += 1
            elif score >= 30:
                score_buckets["30-49"] += 1
            else:
                score_buckets["0-29"] += 1

            # Track top N
            entry = {"name": row.get("name"), "source": source,
                     "tier": tier, "score": score}
            if len(top_profiles) < args.top:
                top_profiles.append(entry)
                if len(top_profiles) == args.top:
                    top_profiles.sort(key=lambda x: x["score"], reverse=True)
            elif score > top_profiles[-1]["score"]:
                top_profiles[-1] = entry
                top_profiles.sort(key=lambda x: x["score"], reverse=True)

        # Progress
        if total % 100000 < 5001:
            elapsed = time.time() - t0
            rate = total / elapsed if elapsed > 0 else 0
            print(f"  Processed {total:,} profiles ({rate:,.0f}/sec)...")

    elapsed = time.time() - t0
    print(f"  Classified {total:,} profiles in {elapsed:.1f}s")
    print(f"  Tiers A-E: {len(updates):,} to update | Tier X: {len(tier_x_ids):,} to archive+delete")

    # If tier_filter was used, show filtered count in report
    report_total = sum(tier_counts.values())

    # Print report
    print_report(
        tier_counts, score_buckets, source_tier_counts,
        top_profiles, tier_x_source_counts, report_total,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        print("\n[DRY RUN] No changes written to database.\n")
        conn.close()
        return

    # Step 2: Archive Tier X to CSV
    if tier_x_rows and not tier_filter:
        print(f"\nStep 2: Archiving {len(tier_x_rows):,} Tier X profiles to CSV...")
        archive_tier_x(tier_x_rows)
    else:
        print("\nStep 2: Skipped (no Tier X rows or tier filter active).")

    # Step 3: Delete Tier X from Supabase
    if tier_x_ids and not args.skip_delete and not tier_filter:
        print(f"\nStep 3: Deleting {len(tier_x_ids):,} Tier X profiles from Supabase...")
        batch_delete(conn, tier_x_ids)
    else:
        reason = "skip-delete flag" if args.skip_delete else "tier filter active or no Tier X"
        print(f"\nStep 3: Skipped ({reason}).")

    # Step 4: Batch UPDATE remaining profiles
    if updates:
        print(f"\nStep 4: Writing jv_tier + jv_readiness_score for {len(updates):,} profiles...")
        batch_update_tiers(conn, updates)
    else:
        print("\nStep 4: No updates to write.")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
