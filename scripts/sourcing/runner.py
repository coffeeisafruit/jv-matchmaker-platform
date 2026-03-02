#!/usr/bin/env python3
"""
Batch orchestrator for multi-source JV candidate scraping.

Usage:
    python -m scripts.sourcing.runner --source speakerhub --batch-size 100
    python -m scripts.sourcing.runner --tier 1 --batch-size 100
    python -m scripts.sourcing.runner --source youtube_api --dry-run --max-pages 5
    python -m scripts.sourcing.runner --status
    python -m scripts.sourcing.runner --list
    python -m scripts.sourcing.runner --reset speakerhub
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

# Django setup
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

# Load .env
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), ".env"))
except ImportError:
    pass

from scripts.sourcing.base import BaseScraper, ScrapedContact
from scripts.sourcing.progress import ProgressTracker
from scripts.sourcing.rate_limiter import RateLimiter

# ---------------------------------------------------------------------------
# Scraper registry — import and register each scraper module
# ---------------------------------------------------------------------------

SCRAPER_REGISTRY: dict[str, type[BaseScraper]] = {}

# Scrapers excluded from gap-driven sourcing (low JV-relevant data yield)
GAP_SOURCING_EXCLUDED = {
    "bbb_sitemap", "irs_business_leagues", "census_business",
    "usaspending", "usaspending_recipients", "fdic_banks",
    "epa_echo", "fda_devices", "gsa_sam", "sam_awards",
    "wikidata", "crossref", "state_business_registrations",
}

# Tier assignments for --tier flag
# P1 = API-based (most reliable), P2 = server-rendered HTML, P3 = JS-rendered/blocked
TIERS: dict[int, list[str]] = {
    1: ["usaspending", "usaspending_recipients", "fdic_banks", "sam_awards", "sam_gov", "sbir_gov", "sec_edgar", "sec_edgar_search", "irs_exempt", "irs_business_leagues", "apple_podcasts", "youtube_api", "google_books", "openlibrary", "yc_companies", "trustpilot", "clutch_sitemap", "bbb_sitemap", "epa_echo", "fda_devices", "crossref", "wikidata", "podcastindex", "sessionize", "sba_loans", "grants_gov", "gsa_sam", "census_business", "wordpress_plugins", "atlassian_marketplace", "toastmasters"],
    2: ["chambers", "opencorporates", "muncheye", "noomii", "psychology_today", "tedx", "icf_coaching", "summit_speakers", "shopify_partners", "webflow_experts", "nsaspeakers", "espeakers", "speaking_com", "expertfile", "coaching_federation", "lifecoach_directory", "clarity_fm", "crunchbase_public", "producthunt", "betalist", "startupgrind", "f6s_startups", "techstars_portfolio", "fivehundred_global", "a16z_portfolio", "association_members", "thumbtack_pros", "state_business_registrations", "yelp_businesses", "google_maps_places", "linkedin_companies", "angel_list", "glassdoor_companies", "dnb_listings", "salesforce_appexchange", "microsoft_appsource", "aws_marketplace", "stripe_partners", "g2_reviews", "capterra_listings", "chrome_extensions", "slack_app_directory", "score_mentors", "clutch_agencies", "bni_members", "eonetwork", "vistage_members", "alignable", "sequoia_portfolio", "greylock_portfolio", "nea_portfolio", "index_ventures", "accel_portfolio", "benchmark_portfolio", "founders_fund", "lightspeed_portfolio", "general_catalyst", "bessemer_portfolio"],
    3: ["speakerhub", "udemy", "podchaser", "gumroad", "substack", "hubspot_partners", "zapier_partners", "partnerstack_marketplace", "cj_affiliates", "shareasale_merchants", "impact_partners", "indie_hackers", "upwork_agencies", "fiverr_pros", "meetup_organizers"],
    4: ["clickbank", "jvzoo", "warriorplus", "eventbrite", "medium"],
}


def _register_scrapers() -> None:
    """Import available scraper modules and populate the registry."""
    scraper_dir = Path(__file__).parent / "scrapers"
    for py_file in sorted(scraper_dir.glob("*.py")):
        if py_file.name.startswith("_"):
            continue
        module_name = py_file.stem
        try:
            mod = __import__(
                f"scripts.sourcing.scrapers.{module_name}",
                fromlist=["Scraper"],
            )
            if hasattr(mod, "Scraper"):
                SCRAPER_REGISTRY[module_name] = mod.Scraper
        except ImportError as exc:
            logging.debug("Could not import scraper %s: %s", module_name, exc)


def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )


# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------

def run_source(
    source_name: str,
    batch_size: int = 100,
    max_pages: int = 0,
    max_contacts: int = 0,
    dry_run: bool = False,
    export_csv: Optional[str] = None,
    resume: bool = True,
) -> dict:
    """Run a single source scraper, batching contacts into ingestion."""
    if source_name not in SCRAPER_REGISTRY:
        print(f"ERROR: Unknown source '{source_name}'. Available: {list(SCRAPER_REGISTRY.keys())}")
        return {}

    rate_limiter = RateLimiter()
    tracker = ProgressTracker()
    checkpoint = tracker.load(source_name) if resume else {}

    scraper_cls = SCRAPER_REGISTRY[source_name]
    scraper = scraper_cls(rate_limiter=rate_limiter)

    print(f"\n{'=' * 60}")
    print(f"SCRAPING: {source_name.upper()}")
    print(f"{'=' * 60}")
    print(f"  Batch size:    {batch_size}")
    print(f"  Max pages:     {max_pages or 'unlimited'}")
    print(f"  Max contacts:  {max_contacts or 'unlimited'}")
    print(f"  Dry run:       {dry_run}")
    print(f"  Resume from:   {checkpoint.get('last_url', 'beginning')}")
    print()

    batch: list[dict] = []
    batch_contacts: list[ScrapedContact] = []
    total_ingested = checkpoint.get("contacts_total", 0)
    total_new = 0
    last_url = ""

    # Optional CSV export
    csv_writer = None
    csv_file = None
    if export_csv:
        csv_file = open(export_csv, "a", newline="", encoding="utf-8")
        csv_writer = csv.DictWriter(
            csv_file,
            fieldnames=[
                "name", "email", "company", "website", "linkedin", "phone", "bio",
                "pricing", "rating", "review_count", "tier", "categories",
                "location", "join_date", "product_focus", "revenue_indicator",
                "source", "source_url",
            ],
        )
        if csv_file.tell() == 0:
            csv_writer.writeheader()

    try:
        for contact in scraper.run(
            max_pages=max_pages,
            max_contacts=max_contacts,
            checkpoint=checkpoint,
        ):
            ingestion_dict = contact.to_ingestion_dict()
            batch.append(ingestion_dict)
            batch_contacts.append(contact)
            last_url = contact.source_url

            if csv_writer:
                rd = contact.raw_data or {}
                cats = contact.categories or rd.get("categories") or rd.get("sectors") or ""
                if isinstance(cats, list):
                    cats = ", ".join(str(c) for c in cats)
                row = {
                    **ingestion_dict,
                    "pricing": contact.pricing or rd.get("pricing") or rd.get("starting_price") or "",
                    "rating": contact.rating or rd.get("rating") or "",
                    "review_count": contact.review_count or rd.get("review_count") or rd.get("reviews") or "",
                    "tier": contact.tier or rd.get("tier") or "",
                    "categories": cats,
                    "location": contact.location or rd.get("location") or "",
                    "join_date": contact.join_date or rd.get("join_date") or "",
                    "product_focus": contact.product_focus or rd.get("product_name") or rd.get("product_focus") or "",
                    "revenue_indicator": contact.revenue_indicator or rd.get("revenue_indicator") or "",
                    "source": source_name,
                    "source_url": contact.source_url,
                }
                csv_writer.writerow(row)

            if len(batch) >= batch_size:
                new_count = _flush_batch(batch, source_name, dry_run)
                total_new += new_count
                total_ingested += len(batch)
                print(
                    f"  Batch: {len(batch)} sent, {new_count} new | "
                    f"Running total: {total_ingested} ingested, {total_new} new"
                )
                batch = []
                batch_contacts = []
                tracker.update_checkpoint(source_name, last_url, total_ingested, new_count)

        # Final partial batch
        if batch:
            new_count = _flush_batch(batch, source_name, dry_run)
            total_new += new_count
            total_ingested += len(batch)
            print(
                f"  Final batch: {len(batch)} sent, {new_count} new | "
                f"Total: {total_ingested} ingested, {total_new} new"
            )
            tracker.update_checkpoint(source_name, last_url, total_ingested, new_count)

    finally:
        if csv_file:
            csv_file.close()

    print(f"\n{'=' * 60}")
    print(f"COMPLETE: {source_name.upper()}")
    print(f"{'=' * 60}")
    print(f"  Pages scraped:  {scraper.stats['pages_scraped']}")
    print(f"  Contacts found: {scraper.stats['contacts_found']}")
    print(f"  Contacts valid: {scraper.stats['contacts_valid']}")
    print(f"  Total ingested: {total_ingested}")
    print(f"  New profiles:   {total_new}")
    print(f"  Errors:         {scraper.stats['errors']}")
    print()

    return scraper.stats


def _flush_batch(batch: list[dict], source_name: str, dry_run: bool) -> int:
    """Send a batch to contact ingestion. Returns count of new profiles."""
    if dry_run:
        print(f"  [DRY RUN] Would ingest {len(batch)} contacts:")
        for c in batch[:3]:
            print(f"    - {c.get('name', '?')} | {c.get('website', '')} | {c.get('email', '')}")
        if len(batch) > 3:
            print(f"    ... and {len(batch) - 3} more")
        return 0

    try:
        import django
        django.setup()
        from matching.enrichment.flows.contact_ingestion import ingest_contacts

        results = ingest_contacts(
            contacts=batch,
            source=f"scraper_{source_name}",
            source_file=f"sourcing/{source_name}",
        )
        return sum(1 for r in results if r.is_new)
    except Exception as exc:
        logging.error("Ingestion failed: %s", exc)
        return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def show_status() -> None:
    """Print progress summary for all sources."""
    tracker = ProgressTracker()
    summary = tracker.get_summary()

    print(f"\n{'=' * 60}")
    print("SOURCING PROGRESS")
    print(f"{'=' * 60}\n")

    if not summary:
        print("  No scraping runs recorded yet.")
        print()
        return

    grand_total = 0
    grand_new = 0
    for source, data in summary.items():
        total = data["contacts_total"]
        new = data["contacts_new"]
        grand_total += total
        grand_new += new
        print(f"  {source:20s}  total={total:>6,}  new={new:>6,}  runs={data['runs']}  last={data['last_run'][:16]}")

    print(f"\n  {'GRAND TOTAL':20s}  total={grand_total:>6,}  new={grand_new:>6,}")
    print()


def show_available() -> None:
    """List all registered scrapers with tier info."""
    print(f"\n{'=' * 60}")
    print("AVAILABLE SCRAPERS")
    print(f"{'=' * 60}\n")

    tier_lookup = {}
    for tier, sources in TIERS.items():
        for s in sources:
            tier_lookup[s] = tier

    for name in sorted(SCRAPER_REGISTRY.keys()):
        tier = tier_lookup.get(name, "?")
        cls = SCRAPER_REGISTRY[name]
        print(f"  P{tier}  {name:20s}  ({cls.BASE_URL})")

    unregistered = set()
    for sources in TIERS.values():
        for s in sources:
            if s not in SCRAPER_REGISTRY:
                unregistered.add(s)
    if unregistered:
        print(f"\n  Not yet implemented: {', '.join(sorted(unregistered))}")
    print()


def _compute_scraper_priorities(gap_data: dict, max_scrapers: int = 5) -> list[tuple[str, float, list[str]]]:
    """Score scrapers against market gaps. Returns [(name, priority_score, gaps_filled)]."""
    supply_demand_gaps = gap_data.get("supply_demand_gaps", [])
    role_gaps = gap_data.get("role_gaps", [])

    # Build lookup: keyword -> gap_ratio for high-demand gaps only
    keyword_gaps = {}
    for gap in supply_demand_gaps:
        if gap.get("gap_type") == "high_demand":
            keyword_gaps[gap["keyword"]] = gap["gap_ratio"]

    # Build lookup: (niche, role) -> True for missing roles
    missing_roles = set()
    for rg in role_gaps:
        niche = rg.get("niche", "")
        for role in rg.get("missing_high_value_roles", []):
            missing_roles.add((niche, role))

    priorities = []
    for name, cls in SCRAPER_REGISTRY.items():
        if name in GAP_SOURCING_EXCLUDED:
            continue

        score = 0.0
        filled = []

        # Score against keyword gaps
        for offering_kw in getattr(cls, "TYPICAL_OFFERINGS", []):
            if offering_kw in keyword_gaps:
                score += keyword_gaps[offering_kw]
                filled.append(f"keyword:{offering_kw} (gap={keyword_gaps[offering_kw]:.1f})")

        # Score against role gaps
        for role in getattr(cls, "TYPICAL_ROLES", []):
            for niche in getattr(cls, "TYPICAL_NICHES", []):
                if (niche, role) in missing_roles:
                    score += 5.0  # Structural gap bonus
                    filled.append(f"role:{role} in {niche}")

        if score > 0:
            priorities.append((name, score, filled))

    priorities.sort(key=lambda x: x[1], reverse=True)
    return priorities[:max_scrapers]


def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-source JV candidate scraper")
    parser.add_argument("--source", help="Scraper name to run (e.g., speakerhub)")
    parser.add_argument("--tier", type=int, choices=[1, 2, 3, 4], help="Run all scrapers in a tier")
    parser.add_argument("--batch-size", type=int, default=100, help="Contacts per ingestion batch (default: 100)")
    parser.add_argument("--max-pages", type=int, default=0, help="Max pages to scrape (0 = unlimited)")
    parser.add_argument("--max-contacts", type=int, default=0, help="Max contacts to collect (0 = unlimited)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing to DB")
    parser.add_argument("--export-csv", help="Also write contacts to a CSV file")
    parser.add_argument("--no-resume", action="store_true", help="Start from scratch (ignore checkpoint)")
    parser.add_argument("--status", action="store_true", help="Show progress for all sources")
    parser.add_argument("--list", action="store_true", help="List available scrapers")
    parser.add_argument("--fill-gaps", type=int, nargs="?", const=5, metavar="N",
                        help="Auto-select top N scrapers based on market gap analysis (default: 5)")
    parser.add_argument("--reset", help="Reset checkpoint for a source")
    parser.add_argument("--verbose", "-v", action="store_true")

    args = parser.parse_args()
    _setup_logging(args.verbose)
    _register_scrapers()

    if args.status:
        show_status()
        return

    if args.list:
        show_available()
        return

    if args.reset:
        ProgressTracker().reset(args.reset)
        print(f"Reset checkpoint for: {args.reset}")
        return

    if args.tier:
        sources = TIERS.get(args.tier, [])
        available = [s for s in sources if s in SCRAPER_REGISTRY]
        if not available:
            print(f"No scrapers implemented yet for tier {args.tier}.")
            print(f"Expected: {sources}")
            return
        print(f"Running tier {args.tier} scrapers: {available}")
        for source in available:
            run_source(
                source,
                batch_size=args.batch_size,
                max_pages=args.max_pages,
                max_contacts=args.max_contacts,
                dry_run=args.dry_run,
                export_csv=args.export_csv,
                resume=not args.no_resume,
            )
        show_status()
        return

    if args.fill_gaps is not None:
        # Load latest gap analysis
        gap_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            "reports", "market_intelligence", "gap_report.json",
        )
        if not os.path.exists(gap_file):
            print(f"ERROR: No gap report found at {gap_file}")
            print("Run: python3 manage.py compute_market_intelligence")
            return

        with open(gap_file) as f:
            gap_data = json.load(f)

        priorities = _compute_scraper_priorities(gap_data, args.fill_gaps)
        if not priorities:
            print("No scrapers match current market gaps.")
            print("Ensure scrapers have TYPICAL_ROLES/NICHES/OFFERINGS metadata.")
            return

        print(f"\n{'=' * 60}")
        print(f"GAP-DRIVEN SOURCING PLAN")
        print(f"(based on {gap_data.get('enriched_profile_count', '?')} enriched profiles)")
        print(f"{'=' * 60}\n")
        for i, (name, score, filled) in enumerate(priorities, 1):
            print(f"  {i}. {name:25s} (priority={score:.1f})")
            for f_item in filled[:3]:
                print(f"     \u2514\u2500 fills: {f_item}")
        print()

        if args.dry_run:
            print("[DRY RUN] Would run the above scrapers.")
            return

        for name, score, filled in priorities:
            run_source(
                name,
                batch_size=args.batch_size,
                max_pages=args.max_pages,
                max_contacts=args.max_contacts,
                dry_run=args.dry_run,
                export_csv=args.export_csv,
                resume=not args.no_resume,
            )
        show_status()
        return

    if args.source:
        run_source(
            args.source,
            batch_size=args.batch_size,
            max_pages=args.max_pages,
            max_contacts=args.max_contacts,
            dry_run=args.dry_run,
            export_csv=args.export_csv,
            resume=not args.no_resume,
        )
        return

    parser.print_help()


if __name__ == "__main__":
    main()
