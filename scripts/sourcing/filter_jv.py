#!/usr/bin/env python3
"""
Filter MERGED_ALL.csv down to JV-relevant contacts only.

Strategy:
  1. AUTO-INCLUDE all contacts from inherently JV sources
     (coach directories, speaker platforms, podcasts, etc.)
  2. KEYWORD-FILTER contacts from broad sources (Crossref, OpenLibrary,
     Wikidata, Google Books) — only keep those whose bio signals
     JV relevance (coaching, business, marketing, self-help, etc.)
  3. EXCLUDE contacts that are clearly academic/institutional

Output: scripts/sourcing/output/JV_FILTERED.csv
"""

from __future__ import annotations

import csv
import re
import sys
from pathlib import Path
from collections import defaultdict

INPUT_FILE = Path(__file__).parent / "output" / "MERGED_ALL.csv"
OUTPUT_FILE = Path(__file__).parent / "output" / "JV_FILTERED.csv"

# ---------------------------------------------------------------------------
# Source classification
# ---------------------------------------------------------------------------

# These sources are inherently JV-relevant — keep ALL contacts
JV_SOURCES = {
    "sessionize", "noomii", "speaking_com", "apple_podcasts",
    "psychology_today", "expertfile", "tedx", "youtube_api",
    "shopify_partners", "clutch_agencies", "fivehundred_global",
    "yc_companies", "greylock_portfolio", "masschallenge_alumni",
    "axial", "openvc", "vc_sheet", "muncheye",
}

# Exclude entirely — academic citation databases, not JV practitioners
EXCLUDE_SOURCES = {"crossref"}

# Sources that need aggressive filtering — broad but contain some JV people
BROAD_SOURCES = {
    "openlibrary", "openlibrary_v2",
    "google_books", "wikidata",
}

# ---------------------------------------------------------------------------
# JV relevance keywords — STRONG signals only
# (Used for broad sources; JV_SOURCES are auto-included)
# ---------------------------------------------------------------------------

# STRONG JV signals — clearly about practitioners, not academic study OF the topic
STRONG_JV_KEYWORDS = [
    # Coaching (the person IS a coach, not studying coaching)
    r"\blife coach\b", r"\bbusiness coach\b", r"\bexecutive coach\b",
    r"\bhealth coach\b", r"\bwellness coach\b", r"\bcareer coach\b",
    r"\bcoaching (practice|business|clients|certification)\b",
    # Speaking
    r"\bkeynote speaker\b", r"\bmotivational speaker\b",
    r"\bprofessional speaker\b", r"\bpublic speaking\b",
    # Clear business/marketing practitioner
    r"\bcopywriting\b", r"\bcopywriter\b", r"\bdigital marketing\b",
    r"\baffiliate marketing\b", r"\bsales funnel\b", r"\blead gen\b",
    r"\bemail marketing\b", r"\bcontent marketing\b",
    r"\bsocial media marketing\b", r"\bSEO\b",
    # Course creators / info products
    r"\bcourse creator\b", r"\bonline course\b", r"\binfo ?product\b",
    r"\bmembership site\b",
    # Explicitly self-help / personal development books
    r"\bself[- ]help\b", r"\bpersonal development\b",
    r"\bself[- ]improvement\b",
    # Wellness practitioners (not academic study)
    r"\byoga teacher\b", r"\byoga instructor\b",
    r"\bpersonal trainer\b", r"\bfitness coach\b",
    r"\bnutritionist\b", r"\bdietitian\b",
    r"\bnaturopath\b", r"\bchiropractor\b",
    r"\bhealth practitioner\b", r"\bwellness practitioner\b",
    r"\bholistic healer\b", r"\benergy heal\b",
    r"\bbreathing\b", r"\bbreathwork\b",
    # Podcasting / content creation
    r"\bpodcast host\b", r"\bpodcaster\b",
    r"\bblogger\b", r"\binfluencer\b", r"\bcontent creator\b",
    # Finance (personal, not academic)
    r"\bfinancial freedom\b", r"\bpassive income\b",
    r"\bwealth building\b", r"\bmoney mindset\b",
    # JV-specific
    r"\bjoint venture\b", r"\bJV\b", r"\bmastermind\b",
    r"\bwebinar\b", r"\bsummit\b",
]

STRONG_JV_PATTERN = re.compile("|".join(STRONG_JV_KEYWORDS), re.IGNORECASE)

# MEDIUM JV signals — could be JV but only if NOT from an academic publisher
MEDIUM_JV_KEYWORDS = [
    r"\bcoach(ing)?\b", r"\bmentor(ing)?\b",
    r"\bspeaker\b", r"\bTEDx\b",
    r"\bentrepreneur\b", r"\bstartup\b", r"\bfounder\b",
    r"\bmarketing\b", r"\bconsultant\b", r"\badvisor\b",
    r"\bleadership\b", r"\bproductivity\b",
    r"\bmindset\b", r"\bmotivation(al)?\b",
    r"\bwellness\b", r"\bfitness\b", r"\byoga\b",
    r"\bmeditation\b", r"\bmindfulness\b",
    r"\btherapist\b", r"\bcounselor\b",
    r"\breal estate\b", r"\binvesting\b",
]

MEDIUM_JV_PATTERN = re.compile("|".join(MEDIUM_JV_KEYWORDS), re.IGNORECASE)

# ---------------------------------------------------------------------------
# Academic publisher names — if bio mentions these, it's academic work
# ---------------------------------------------------------------------------

ACADEMIC_PUBLISHERS = [
    # Major academic publishers
    "elsevier", "springer", "wiley", "taylor & francis",
    "academic press", "cambridge university press",
    "oxford university press", "ieee", "acm",
    "sage publications", "routledge", "palgrave",
    "mcgraw-hill", "pearson", "cengage",
    "world scientific", "de gruyter", "brill",
    "mit press", "вид",  # Ukrainian publisher prefix
    "verlag",  # German publisher
    "édition", "editora",  # French/Portuguese publisher
    "crc press", "nova science", "igi global",
    "informa", "mdpi", "frontiers",
    "royal society", "national academy",
    "american chemical", "american physical",
    "american mathematical", "american psychological association",
    "american sociological", "american psychiatric",
    # Catch-all academic patterns
    "university press", "university of",
    "policy press", "playwrights",
    "institute of", "society of", "society for",
    "association publish", "foundation press",
    "academy of", "academy press",
    # Medical / scientific publishers
    "lippincott", "karger", "thieme", "bentham",
    "humana press", "kluwer", "ios press",
    "wolters kluwer", "biomedcentral",
]


def _is_academic_publisher(bio: str) -> bool:
    """Check if the bio references an academic publisher."""
    bio_lower = bio.lower()
    return any(pub in bio_lower for pub in ACADEMIC_PUBLISHERS)


def _is_crossref_bio(bio: str) -> bool:
    """Check if bio follows Crossref pattern: Author: "TITLE" | Publisher: ..."""
    return bio.startswith("Author:") and "Publisher:" in bio


def _is_openlibrary_bio(bio: str) -> bool:
    """Check if bio follows Open Library pattern."""
    return "Published:" in bio or "Subjects:" in bio


def classify_contact(row: dict) -> str:
    """Classify a contact as 'jv' or 'skip'.

    Strategy:
      - JV sources: auto-include (coach dirs, speaker platforms, etc.)
      - Broad sources (Crossref, OpenLibrary, etc.):
        * STRONG JV keyword → include (even from academic publishers)
        * MEDIUM JV keyword + NOT academic publisher → include
        * Anything else → skip
      - Unknown sources: require STRONG JV signal
    """
    bio = (row.get("bio") or "").strip()
    company = (row.get("company") or "").strip()
    source = (row.get("source") or row.get("_source_file", "")).strip()
    combined = f"{bio} {company}"

    # Normalize source name
    source_clean = source.replace(".csv", "").lower()

    # 1. Exclude academic citation databases entirely
    if any(s in source_clean for s in EXCLUDE_SOURCES):
        return "skip"

    # 2. Auto-include from JV sources
    if any(s in source_clean for s in JV_SOURCES):
        return "jv"

    # 3. Broad sources — strict filtering
    is_broad = any(s in source_clean for s in BROAD_SOURCES)

    if is_broad:
        # Strong JV signal overrides even academic publisher
        if STRONG_JV_PATTERN.search(combined):
            return "jv"

        # Medium JV signal — only if NOT from academic publisher
        if MEDIUM_JV_PATTERN.search(combined):
            if not _is_academic_publisher(bio):
                return "jv"

        return "skip"

    # 4. Unknown / other sources — require strong signal
    if STRONG_JV_PATTERN.search(combined):
        return "jv"
    if MEDIUM_JV_PATTERN.search(combined) and not _is_academic_publisher(bio):
        return "jv"

    return "skip"


def main():
    stats_only = "--stats" in sys.argv

    print(f"\n{'=' * 60}")
    print("FILTER FOR JV-RELEVANT CONTACTS")
    print(f"{'=' * 60}\n")

    if not INPUT_FILE.exists():
        print(f"ERROR: {INPUT_FILE} not found. Run dedup_merge.py first.")
        return

    # Load all contacts
    all_contacts = []
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            all_contacts.append(row)

    print(f"  Loaded {len(all_contacts):,} contacts from MERGED_ALL.csv\n")

    # Classify each contact
    jv_contacts = []
    source_jv = defaultdict(int)
    source_skip = defaultdict(int)

    for row in all_contacts:
        source = (row.get("source") or row.get("_source_file", "unknown")).replace(".csv", "")
        result = classify_contact(row)
        if result == "jv":
            jv_contacts.append(row)
            source_jv[source] += 1
        else:
            source_skip[source] += 1

    # Stats
    total = len(all_contacts)
    kept = len(jv_contacts)
    removed = total - kept

    print(f"  Total:    {total:>8,}")
    print(f"  JV kept:  {kept:>8,} ({100*kept/total:.1f}%)")
    print(f"  Removed:  {removed:>8,} ({100*removed/total:.1f}%)")

    # Quality metrics
    has_email = sum(1 for c in jv_contacts if (c.get("email") or "").strip())
    has_website = sum(1 for c in jv_contacts if (c.get("website") or "").strip())
    has_linkedin = sum(1 for c in jv_contacts if (c.get("linkedin") or "").strip())
    has_bio = sum(1 for c in jv_contacts if len((c.get("bio") or "").strip()) > 20)

    print(f"\n  Quality of JV contacts:")
    print(f"    Has email:    {has_email:>6,} ({100*has_email/kept:.1f}%)")
    print(f"    Has website:  {has_website:>6,} ({100*has_website/kept:.1f}%)")
    print(f"    Has LinkedIn: {has_linkedin:>6,} ({100*has_linkedin/kept:.1f}%)")
    print(f"    Has bio:      {has_bio:>6,} ({100*has_bio/kept:.1f}%)")

    print(f"\n  By source (kept / removed):")
    all_sources = sorted(
        set(list(source_jv.keys()) + list(source_skip.keys())),
        key=lambda s: -(source_jv.get(s, 0) + source_skip.get(s, 0)),
    )
    for src in all_sources:
        jv = source_jv.get(src, 0)
        skip = source_skip.get(src, 0)
        total_src = jv + skip
        pct = 100 * jv / total_src if total_src else 0
        print(f"    {src:30s}  {jv:>6,} kept / {skip:>6,} removed  ({pct:.0f}% kept)")

    if not stats_only:
        # Write filtered output
        fieldnames = ["name", "email", "company", "website", "linkedin", "phone", "bio", "source", "source_url"]
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for contact in jv_contacts:
                writer.writerow(contact)

        print(f"\n  Written to: {OUTPUT_FILE}")
        print(f"  JV-filtered contacts: {kept:,}")


if __name__ == "__main__":
    main()
