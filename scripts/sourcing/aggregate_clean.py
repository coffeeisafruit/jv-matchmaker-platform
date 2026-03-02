#!/usr/bin/env python3
"""
Aggregate, clean, and deduplicate all scraped CSV files into a single master CSV.

6-phase pipeline:
1. Discovery — find all CSVs across 3 directories
2. Ingest + Validate — normalize field names, validate records
3. Bio Intelligence Extraction — parse structured bio data into dedicated fields
4. Multi-Layer Dedup — email -> name+company -> name+domain
5. Revenue Tier Inference — parse financial data into tier labels
6. Output + Stats — write master CSV, print quality metrics

Usage:
    python3 scripts/sourcing/aggregate_clean.py                     # Default run
    python3 scripts/sourcing/aggregate_clean.py --include-irs       # Include 1.8M IRS orgs
    python3 scripts/sourcing/aggregate_clean.py --stats-only        # Stats only
    python3 scripts/sourcing/aggregate_clean.py --output path.csv   # Custom output
    python3 scripts/sourcing/aggregate_clean.py -v                  # Verbose
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_OUTPUT = PROJECT_ROOT / "Filling Database" / "MASTER_JV_CLEAN.csv"

OUTPUT_HEADER = [
    "name", "email", "company", "website", "linkedin", "phone", "bio",
    "pricing", "rating", "review_count", "tier", "categories",
    "location", "join_date", "product_focus", "revenue_indicator",
    "source", "source_url", "source_category", "scraped_at",
]

FIELD_ALIASES = {"source_platform": "source"}

SKIP_FILES = {
    "MERGED_ALL.csv", "JV_FILTERED.csv", "MASTER_JV_CLEAN.csv",
    "_master_contacts.csv", "_master_contacts_v2.csv",
    "_scraping_progress.json", "MASTER_JV_CONTACTS.csv",
    "irs_exempt.csv",  # skipped by default; --include-irs overrides
}
SKIP_PREFIXES = ("_progress_",)

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
    "epa_echo": 55,
    "chambers": 50,
    "openlibrary": 45, "openlibrary_v2": 45, "google_books": 45,
    "irs_exempt": 40,
}
DEFAULT_PRIORITY = 50

SKIP_NAMES = {
    "view", "new york", "los angeles", "chicago", "unknown",
    "various artists", "anonymous", "see more", "none",
    "n/a", "na", "test", "admin", "editor", "staff",
    "home", "about", "contact", "login", "sign up", "register",
    "privacy policy", "terms of service", "careers", "blog",
    "apple", "apple support",
}

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

JUNK_EMAIL_DOMAINS = {
    "example.com", "test.com", "mailinator.com", "tempmail.com",
    "guerrillamail.com", "yopmail.com", "sharklasers.com",
}

EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")


# ---------------------------------------------------------------------------
# Normalization (reused logic from dedup_merge.py, merge_all_contacts.py,
# import_csv.py — consolidated here to avoid import dependencies)
# ---------------------------------------------------------------------------

def normalize_person(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"^(dr\.?\s+|prof\.?\s+|mr\.?\s+|mrs\.?\s+|ms\.?\s+)", "", name)
    name = re.sub(
        r",?\s*(phd|psyd|lcsw|lmft|lpc|lmhc|ma|ms|med|edd|md|ncc|bcc|cpc|acc|pcc|mcc|jr\.?|sr\.?|iii?|iv).*$",
        "", name, flags=re.I,
    )
    return re.sub(r"\s+", " ", name).strip()


def normalize_company(name: str) -> str:
    if not name:
        return ""
    n = name.upper().strip()
    for suffix in [" LLC", " L.L.C.", " INC", " INC.", " CORP", " CORP.",
                   " LTD", " LTD.", " CO", " CO.", " LP", " L.P.",
                   " PLLC", " PC", " P.C.", ",", "."]:
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
    return n


def normalize_domain(raw: str | None) -> str:
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


def normalize_email(email: str | None) -> str:
    if not email:
        return ""
    email = email.strip().lower()
    if not EMAIL_RE.match(email):
        return ""
    domain = email.split("@")[1]
    if domain in JUNK_EMAIL_DOMAINS:
        return ""
    return email


# ---------------------------------------------------------------------------
# Bio intelligence extraction — source-specific parsers
# ---------------------------------------------------------------------------

def _val(row: dict, field: str) -> str:
    """Get a non-empty, non-None value from row."""
    v = (row.get(field) or "").strip()
    return "" if v.lower() == "none" else v


def _set_if_empty(row: dict, field: str, value: str) -> None:
    if not _val(row, field) and value:
        row[field] = value


def _append_field(row: dict, field: str, value: str, sep: str = ", ") -> None:
    if not value:
        return
    existing = _val(row, field)
    if existing:
        row[field] = f"{existing}{sep}{value}"
    else:
        row[field] = value


def extract_bio_fields(row: dict) -> dict:
    """Parse structured data from bio into empty extended fields."""
    bio = _val(row, "bio")
    if not bio:
        return row

    source = (row.get("source") or "").lower()
    parts = [p.strip() for p in bio.split("|")]

    if source in ("muncheye", "muncheye_launches", "jvnotifypro"):
        _parse_jv_launch_bio(row, parts)
    elif source == "trustpilot":
        _parse_trustpilot_bio(row, parts)
    elif source == "coaching_federation":
        _parse_coaching_bio(row, parts)
    elif source in ("usaspending", "usaspending_recipients"):
        _parse_usaspending_bio(row, parts)
    elif source == "fdic_banks":
        _parse_fdic_bio(row, parts)
    elif source in ("sec_edgar", "sec_edgar_search"):
        _parse_sec_edgar_bio(row, parts)
    elif source == "yc_companies":
        _parse_yc_bio(row, bio)
    elif source in ("apple_podcasts",):
        _parse_podcast_bio(row, parts)
    elif source in ("shareasale_merchants", "shareasale"):
        _parse_shareasale_bio(row, parts)
    elif source == "shopify_partners":
        _parse_shopify_bio(row, parts)
    elif source == "speaking_com":
        _parse_speaking_bio(row, parts)

    # Generic: location extraction fallback
    if not _val(row, "location"):
        for part in parts:
            if part.strip().lower().startswith("location:"):
                row["location"] = part.split(":", 1)[1].strip()
                break

    return row


def _parse_jv_launch_bio(row: dict, parts: list[str]) -> None:
    """MunchEye / JVNotifyPro: Launch, Price, Commission, Network, JV Page, Category."""
    in_categories = False
    for part in parts:
        pl = part.strip().lower()

        if pl.startswith("price:") or pl.startswith("priced"):
            price = part.split(":", 1)[1].strip() if ":" in part else part.strip()
            existing = _val(row, "pricing")
            if existing:
                row["pricing"] = f"{price} | {existing}"
            else:
                row["pricing"] = price
            in_categories = False

        elif pl.startswith("commission:"):
            commission = part.split(":", 1)[1].strip()
            _append_field(row, "pricing", f"Commission: {commission}", " | ")
            in_categories = False

        elif pl.startswith("launch:"):
            _set_if_empty(row, "join_date", part.split(":", 1)[1].strip())
            in_categories = False

        elif pl.startswith("network:"):
            _append_field(row, "categories", part.split(":", 1)[1].strip())
            in_categories = False

        elif pl.startswith("jv page:"):
            jv_url = part.split(":", 1)[1].strip()
            if not jv_url.startswith("http"):
                jv_url = "https://" + jv_url
            _set_if_empty(row, "website", jv_url)
            in_categories = False

        elif pl.startswith("niche:"):
            _append_field(row, "categories", part.split(":", 1)[1].strip())
            in_categories = False

        elif pl.startswith("category:"):
            _append_field(row, "categories", part.split(":", 1)[1].strip())
            in_categories = True

        elif in_categories:
            # Continue capturing category sub-items (short, no colon prefix)
            clean = part.strip()
            if len(clean) < 60 and not any(
                pl.startswith(k) for k in ("launch", "price", "commission",
                                           "network", "jv page", "description",
                                           "time:", "this ", "partners:")
            ):
                _append_field(row, "categories", clean)
            else:
                in_categories = False


def _parse_trustpilot_bio(row: dict, parts: list[str]) -> None:
    """Trustpilot: company | Location | Trust: X/5 | N reviews | Categories: ..."""
    for part in parts:
        pl = part.strip().lower()
        m = re.search(r"trust:\s*([\d.]+)/5", pl)
        if m:
            _set_if_empty(row, "rating", m.group(1))
            continue
        m = re.search(r"([\d,]+)\s*reviews?", pl)
        if m:
            _set_if_empty(row, "review_count", m.group(1).replace(",", ""))
            continue
        if pl.startswith("categories:"):
            _set_if_empty(row, "categories", part.split(":", 1)[1].strip())
            continue
        # Location: second part, has comma, not the company name
        if "," in part and not _val(row, "location"):
            loc = part.strip()
            if loc != _val(row, "name") and loc != _val(row, "company"):
                row["location"] = loc


def _parse_coaching_bio(row: dict, parts: list[str]) -> None:
    """ICF coaches: credential | location | pricing."""
    for part in parts:
        pl = part.strip().lower()
        m = re.search(r"icf\s+(pcc|mcc|acc)\s+credentialed", pl)
        if m:
            _set_if_empty(row, "tier", f"ICF {m.group(1).upper()}")
        m = re.search(r"\$[\d,]+-?\$?[\d,]*\s*per\s*hour", pl)
        if m:
            _set_if_empty(row, "pricing", m.group(0))
        if "," in part and "icf" not in pl and "$" not in pl:
            _set_if_empty(row, "location", part.strip())


def _parse_usaspending_bio(row: dict, parts: list[str]) -> None:
    """Federal contracts: $X | NAICS: Y."""
    for part in parts:
        pl = part.strip().lower()
        if "federal contract" in pl:
            _set_if_empty(row, "revenue_indicator", part.strip())
        if pl.startswith("naics:"):
            _set_if_empty(row, "categories", f"NAICS {part.split(':', 1)[1].strip()}")


def _parse_fdic_bio(row: dict, parts: list[str]) -> None:
    """Bank | City, ST | Assets: $X | Deposits: $Y | Est: DATE."""
    rev_parts = []
    for part in parts:
        pl = part.strip().lower()
        if "assets:" in pl or "deposits:" in pl:
            rev_parts.append(part.strip())
        elif "est:" in pl:
            _set_if_empty(row, "join_date", part.split(":", 1)[1].strip())
        elif "," in part and not _val(row, "location"):
            loc = part.strip()
            if loc != _val(row, "name") and loc != _val(row, "company"):
                row["location"] = loc
    if rev_parts:
        _set_if_empty(row, "revenue_indicator", " | ".join(rev_parts))


def _parse_sec_edgar_bio(row: dict, parts: list[str]) -> None:
    """COMPANY (TICKER) | Industry | CITY, ST | SIC: XXXX."""
    if len(parts) >= 2:
        industry = parts[1].strip()
        if industry and not industry.lower().startswith("sic:"):
            _set_if_empty(row, "categories", industry)
    for part in parts:
        pl = part.strip()
        if re.search(r"[A-Z]{2}\s*$", pl) and "," in pl:
            _set_if_empty(row, "location", pl)


def _parse_yc_bio(row: dict, bio: str) -> None:
    """YC bios use period-separated sentences, not pipes."""
    sl = bio.lower()
    m = re.search(r"industry:\s*(.+?)(?:\.|$)", sl)
    if m:
        _set_if_empty(row, "categories", m.group(1).strip().rstrip("."))
    m = re.search(r"stage:\s*(.+?)(?:\.|$)", sl)
    if m:
        stage = m.group(1).strip().rstrip(".")
        _append_field(row, "revenue_indicator", f"Stage: {stage}", " | ")
    m = re.search(r"team size:\s*(\d+)", sl)
    if m:
        _append_field(row, "revenue_indicator", f"Team size: {m.group(1)}", " | ")
    m = re.search(r"yc\s+(fall|winter|summer|spring)\s+\d{4}", sl)
    if m:
        _set_if_empty(row, "tier", m.group(0).strip())
    m = re.search(r"location:\s*(.+?)(?:\.|$)", sl)
    if m:
        _set_if_empty(row, "location", m.group(1).strip().rstrip("."))


def _parse_podcast_bio(row: dict, parts: list[str]) -> None:
    """Podcast: NAME | Genre: X | N episodes."""
    for part in parts:
        pl = part.strip().lower()
        m = re.search(r"podcast:\s*(.+)", pl)
        if m:
            _set_if_empty(row, "product_focus", m.group(1).strip())
        m = re.search(r"genre:\s*(.+)", pl)
        if m:
            _set_if_empty(row, "categories", m.group(1).strip())
        m = re.search(r"(\d+)\s*episodes?", pl)
        if m:
            _set_if_empty(row, "revenue_indicator", f"{m.group(1)} episodes")


def _parse_shareasale_bio(row: dict, parts: list[str]) -> None:
    """Description | Categories: X | Sector: Y | Awin member since: DATE."""
    for part in parts:
        pl = part.strip().lower()
        if pl.startswith("categories:"):
            _set_if_empty(row, "categories", part.split(":", 1)[1].strip())
        elif pl.startswith("sector:"):
            _append_field(row, "categories", part.split(":", 1)[1].strip())
        m = re.search(r"awin member since:\s*(.+)", pl)
        if m:
            _set_if_empty(row, "join_date", m.group(1).strip())


def _parse_shopify_bio(row: dict, parts: list[str]) -> None:
    """Tier info | Location: X | Starting at $Y."""
    for part in parts:
        pl = part.strip().lower()
        m = re.search(r"shopify\s+(select|plus|premium)\s+partner", pl)
        if m:
            _set_if_empty(row, "tier", f"Shopify {m.group(1).title()} Partner")
        if pl.startswith("location:"):
            _set_if_empty(row, "location", part.split(":", 1)[1].strip())
        m = re.search(r"starting\s+(?:at|from)\s+\$[\d,]+", pl)
        if m:
            _set_if_empty(row, "pricing", m.group(0).strip())


def _parse_speaking_bio(row: dict, parts: list[str]) -> None:
    """Topics: X, Y | Location: Z."""
    for part in parts:
        pl = part.strip().lower()
        if pl.startswith("topics:"):
            _set_if_empty(row, "categories", part.split(":", 1)[1].strip())
        if pl.startswith("location:"):
            _set_if_empty(row, "location", part.split(":", 1)[1].strip())


# ---------------------------------------------------------------------------
# Revenue tier inference
# ---------------------------------------------------------------------------

def infer_revenue_tier(raw: str) -> str:
    """Parse revenue_indicator and return 'tier: raw_value' format.
    Only infers from actual revenue/asset/contract/team data, NOT service pricing.
    """
    if not raw:
        return ""

    # Already processed (has "tier: " prefix)
    if re.match(r"^(enterprise|premium|established|emerging|micro):", raw):
        return raw

    # Dollar amounts with B/M/K suffix
    m = re.search(r"\$?([\d,.]+)\s*([BMK])", raw, re.I)
    if m:
        try:
            amount = float(m.group(1).replace(",", ""))
            mult = {"B": 1e9, "M": 1e6, "K": 1e3}[m.group(2).upper()]
            return f"{_amount_tier(amount * mult)}: {raw}"
        except (ValueError, KeyError):
            pass

    # Raw dollar amounts (e.g., "$7,436,133,032")
    m = re.search(r"\$?([\d,]{4,})", raw)
    if m:
        try:
            total = float(m.group(1).replace(",", ""))
            if total >= 100_000:
                return f"{_amount_tier(total)}: {raw}"
        except ValueError:
            pass

    # Team size
    m = re.search(r"team size:\s*(\d+)", raw, re.I)
    if m:
        try:
            return f"{_team_tier(int(m.group(1)))}: {raw}"
        except ValueError:
            pass

    # Episode count (podcast reach)
    m = re.search(r"(\d+)\s*episodes?", raw, re.I)
    if m:
        try:
            eps = int(m.group(1))
            tier = "established" if eps >= 500 else ("emerging" if eps >= 100 else "micro")
            return f"{tier}: {raw}"
        except ValueError:
            pass

    return ""


def _amount_tier(amount: float) -> str:
    if amount >= 100_000_000:
        return "enterprise"
    if amount >= 25_000_000:
        return "premium"
    if amount >= 5_000_000:
        return "established"
    if amount >= 1_000_000:
        return "emerging"
    return "micro"


def _team_tier(size: int) -> str:
    if size >= 500:
        return "enterprise"
    if size >= 100:
        return "premium"
    if size >= 50:
        return "established"
    if size >= 10:
        return "emerging"
    return "micro"


# ---------------------------------------------------------------------------
# DedupIndex — 3-layer O(1) lookup
# ---------------------------------------------------------------------------

class DedupIndex:
    def __init__(self):
        self.records: list[dict] = []
        self.priorities: list[int] = []
        self.email_idx: dict[str, int] = {}
        self.name_co_idx: dict[str, int] = {}
        self.name_domain_idx: dict[str, int] = {}
        self.stats = {"email": 0, "name_co": 0, "name_domain": 0}

    def add_or_merge(self, row: dict, priority: int) -> bool:
        """Returns True if this was a new record."""
        email = normalize_email(row.get("email"))
        name = normalize_person(row.get("name", ""))
        company = normalize_company(row.get("company", ""))
        domain = normalize_domain(row.get("website"))
        skip_domain = domain in PLATFORM_DOMAINS

        existing_idx = None

        # Layer 1: Email
        if email and email in self.email_idx:
            existing_idx = self.email_idx[email]
            self.stats["email"] += 1

        # Layer 2: Name + Company
        if existing_idx is None and name and company:
            key = f"{name}|{company}"
            if key in self.name_co_idx:
                existing_idx = self.name_co_idx[key]
                self.stats["name_co"] += 1

        # Layer 3: Name + Domain (skip platform domains)
        if existing_idx is None and name and domain and not skip_domain:
            key = f"{name}|{domain}"
            if key in self.name_domain_idx:
                existing_idx = self.name_domain_idx[key]
                self.stats["name_domain"] += 1

        if existing_idx is not None:
            self._merge(existing_idx, row, priority)
            return False

        # New record
        idx = len(self.records)
        self.records.append(dict(row))
        self.priorities.append(priority)
        self._index(idx, email, name, company, domain, skip_domain)
        return True

    def _index(self, idx: int, email: str, name: str, company: str,
               domain: str, skip_domain: bool) -> None:
        if email and email not in self.email_idx:
            self.email_idx[email] = idx
        if name and company:
            key = f"{name}|{company}"
            if key not in self.name_co_idx:
                self.name_co_idx[key] = idx
        if name and domain and not skip_domain:
            key = f"{name}|{domain}"
            if key not in self.name_domain_idx:
                self.name_domain_idx[key] = idx

    def _merge(self, idx: int, new_row: dict, new_priority: int) -> None:
        existing = self.records[idx]
        old_priority = self.priorities[idx]

        core = ("name", "email", "company", "website", "linkedin", "phone")
        supplementary = (
            "bio", "pricing", "rating", "review_count", "tier", "categories",
            "location", "join_date", "product_focus", "revenue_indicator",
            "source_url", "source_category", "scraped_at",
        )

        if new_priority > old_priority:
            for f in core:
                nv = (new_row.get(f) or "").strip()
                if nv and nv.lower() != "none":
                    existing[f] = nv
            existing["source"] = new_row.get("source", "")
            self.priorities[idx] = new_priority

        # Always fill blank supplementary fields from any source
        for f in supplementary:
            ev = (existing.get(f) or "").strip()
            nv = (new_row.get(f) or "").strip()
            if (not ev or ev.lower() == "none") and nv and nv.lower() != "none":
                existing[f] = nv

        # Fill blank core fields from any source
        for f in core:
            ev = (existing.get(f) or "").strip()
            nv = (new_row.get(f) or "").strip()
            if (not ev or ev.lower() == "none") and nv and nv.lower() != "none":
                existing[f] = nv

        # Re-index with newly acquired data
        email = normalize_email(existing.get("email"))
        name = normalize_person(existing.get("name", ""))
        company = normalize_company(existing.get("company", ""))
        domain = normalize_domain(existing.get("website"))
        self._index(idx, email, name, company, domain, domain in PLATFORM_DOMAINS)


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def discover_csvs(include_irs: bool = False) -> list[Path]:
    paths: list[Path] = []

    skip = set(SKIP_FILES)
    if include_irs:
        skip.discard("irs_exempt.csv")

    # 1. Filling Database/partners/
    partners_dir = PROJECT_ROOT / "Filling Database" / "partners"
    if partners_dir.exists():
        for p in sorted(partners_dir.glob("*.csv")):
            if p.name in skip:
                continue
            if any(p.name.startswith(px) for px in SKIP_PREFIXES):
                continue
            paths.append(p)

    # 2. Filling Database/chambers/ — prefer all_chambers.csv
    chambers_dir = PROJECT_ROOT / "Filling Database" / "chambers"
    if chambers_dir.exists():
        all_chambers = chambers_dir / "all_chambers.csv"
        if all_chambers.exists():
            paths.append(all_chambers)
        else:
            for p in sorted(chambers_dir.glob("*.csv")):
                paths.append(p)

    # 3. scripts/sourcing/output/
    output_dir = PROJECT_ROOT / "scripts" / "sourcing" / "output"
    if output_dir.exists():
        for p in sorted(output_dir.glob("*.csv")):
            if p.name in skip:
                continue
            paths.append(p)

    return paths


# ---------------------------------------------------------------------------
# Ingest + Validate
# ---------------------------------------------------------------------------

def _guess_source(path: Path) -> str:
    """Infer canonical source name from file path."""
    if path.parent.name == "chambers":
        return "chambers"
    return path.stem.lower()


def ingest_csv(path: Path) -> tuple[list[dict], str]:
    """Read CSV, normalize fields, validate rows. Returns (rows, source_name)."""
    rows: list[dict] = []
    source_name = _guess_source(path)

    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for raw_row in reader:
                row: dict[str, str] = {}
                for key, val in raw_row.items():
                    if not key:
                        continue
                    k = key.lower().strip()
                    k = FIELD_ALIASES.get(k, k)
                    row[k] = (val or "").strip()

                # Clean "None" strings
                for k in list(row.keys()):
                    if row[k] in ("None", "none", "null", "NULL"):
                        row[k] = ""

                # Canonical source: use CSV field if it's a known scraper ID
                csv_source = row.get("source", "")
                if csv_source not in SOURCE_PRIORITY:
                    row["source"] = source_name

                # Validate name
                name = row.get("name", "")
                if not name or len(name) < 2 or len(name) > 200:
                    continue
                if name.lower() in SKIP_NAMES:
                    continue
                if name.replace(" ", "").replace("-", "").isdigit():
                    continue
                if name.startswith(("http://", "https://")):
                    continue

                # Need at least one contact signal
                if not (normalize_email(row.get("email")) or
                        row.get("website", "").strip() or
                        row.get("linkedin", "").strip()):
                    continue

                rows.append(row)
    except Exception as e:
        print(f"  WARNING: {path.name}: {e}", file=sys.stderr)

    return rows, source_name


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(include_irs: bool = False, output_path: Path = DEFAULT_OUTPUT,
                 stats_only: bool = False, verbose: bool = False) -> None:

    # Phase 1: Discovery
    print(f"\n{'=' * 60}")
    print("PHASE 1: DISCOVERY")
    print(f"{'=' * 60}\n")

    csv_paths = discover_csvs(include_irs=include_irs)
    print(f"  Found {len(csv_paths):,} CSV files to process")

    # Phase 2+3: Ingest + Validate + Bio Extraction
    print(f"\n{'=' * 60}")
    print("PHASE 2+3: INGEST, VALIDATE, BIO EXTRACTION")
    print(f"{'=' * 60}\n")

    source_file_counts: dict[str, int] = defaultdict(int)
    total_valid = 0
    all_rows: list[tuple[dict, int]] = []

    for path in csv_paths:
        rows, source_name = ingest_csv(path)
        if not rows:
            continue

        # Priority from the canonical source
        source = rows[0].get("source", source_name)
        priority = SOURCE_PRIORITY.get(source, DEFAULT_PRIORITY)

        for row in rows:
            row = extract_bio_fields(row)
            all_rows.append((row, priority))

        source_file_counts[source] += len(rows)
        total_valid += len(rows)

        if verbose or len(rows) >= 100:
            print(f"  {path.name:55s} {len(rows):>6,} ({source}, P{priority})")

    print(f"\n  Total valid rows: {total_valid:,} from {len(source_file_counts)} sources")

    if total_valid == 0:
        print("  No valid data found. Exiting.")
        return

    # Phase 4: Multi-Layer Dedup
    print(f"\n{'=' * 60}")
    print("PHASE 4: MULTI-LAYER DEDUP")
    print(f"{'=' * 60}\n")

    dedup = DedupIndex()
    for row, priority in all_rows:
        dedup.add_or_merge(row, priority)

    unique = len(dedup.records)
    dups = total_valid - unique
    print(f"  Input rows:          {total_valid:,}")
    print(f"  Unique contacts:     {unique:,}")
    print(f"  Duplicates removed:  {dups:,}")
    print(f"    Email matches:       {dedup.stats['email']:,}")
    print(f"    Name+Company:        {dedup.stats['name_co']:,}")
    print(f"    Name+Domain:         {dedup.stats['name_domain']:,}")

    # Phase 5: Revenue Tier Inference
    print(f"\n{'=' * 60}")
    print("PHASE 5: REVENUE TIER INFERENCE")
    print(f"{'=' * 60}\n")

    tier_counts: dict[str, int] = defaultdict(int)
    for record in dedup.records:
        raw_rev = _val(record, "revenue_indicator")
        if raw_rev:
            tiered = infer_revenue_tier(raw_rev)
            if tiered:
                record["revenue_indicator"] = tiered
                tier_counts[tiered.split(":")[0].strip()] += 1

    print(f"  Revenue tiers assigned: {sum(tier_counts.values()):,}")
    for t in ("enterprise", "premium", "established", "emerging", "micro"):
        if t in tier_counts:
            print(f"    {t:15s} {tier_counts[t]:>6,}")

    # Phase 6: Stats + Output
    print(f"\n{'=' * 60}")
    print("PHASE 6: STATS & OUTPUT")
    print(f"{'=' * 60}")

    _print_field_stats(dedup.records)
    _print_source_breakdown(dedup.records)

    if stats_only:
        print("\n  [STATS ONLY] No file written.\n")
        return

    # Sort by priority desc, name asc
    pri_map = {id(r): p for r, p in zip(dedup.records, dedup.priorities)}
    sorted_recs = sorted(
        dedup.records,
        key=lambda r: (-pri_map.get(id(r), DEFAULT_PRIORITY), (r.get("name") or "").lower()),
    )

    os.makedirs(output_path.parent, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_HEADER, extrasaction="ignore")
        writer.writeheader()
        for rec in sorted_recs:
            writer.writerow(rec)

    size_mb = os.path.getsize(output_path) / 1024 / 1024
    print(f"\n  Written to: {output_path}")
    print(f"  Unique contacts: {len(sorted_recs):,}")
    print(f"  File size: {size_mb:.1f} MB\n")


def _print_field_stats(records: list[dict]) -> None:
    total = len(records)
    if not total:
        return

    fields = [
        "email", "company", "website", "linkedin", "phone", "bio",
        "pricing", "rating", "review_count", "tier", "categories",
        "location", "join_date", "product_focus", "revenue_indicator",
    ]
    counts = {f: 0 for f in fields}
    for r in records:
        for f in fields:
            v = (r.get(f) or "").strip()
            if v and v.lower() != "none":
                counts[f] += 1

    print(f"\n  FIELD POPULATION ({total:,} unique contacts)")
    print(f"  {'─' * 50}")
    for f in fields:
        c = counts[f]
        pct = 100 * c / total
        bar = "█" * int(pct / 5)
        print(f"    {f:20s} {c:>8,} ({pct:5.1f}%) {bar}")


def _print_source_breakdown(records: list[dict]) -> None:
    src: dict[str, int] = defaultdict(int)
    for r in records:
        src[r.get("source", "unknown")] += 1

    print(f"\n  SOURCE BREAKDOWN")
    print(f"  {'─' * 50}")
    for s, c in sorted(src.items(), key=lambda x: -x[1]):
        print(f"    {s:40s} {c:>8,}")
    print(f"    {'TOTAL':40s} {len(records):>8,}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Aggregate, clean, and deduplicate all scraped CSVs")
    parser.add_argument("--include-irs", action="store_true",
                        help="Include irs_exempt.csv (~1.8M orgs)")
    parser.add_argument("--stats-only", action="store_true",
                        help="Print stats without writing output file")
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT),
                        help="Output CSV file path")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Show every file as it's processed")
    args = parser.parse_args()

    run_pipeline(
        include_irs=args.include_irs,
        output_path=Path(args.output),
        stats_only=args.stats_only,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
