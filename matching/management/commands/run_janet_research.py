"""
Run Claude-based research pipeline for Janet's matches and update research_cache.

Usage:
  python manage.py run_janet_research                    # research the 6 that were hand-filled
  python manage.py run_janet_research --all              # research all 10 (overwrites cache)
  python manage.py run_janet_research --names "David Riklan,Cathy Smith"
"""

import csv
from pathlib import Path

from django.core.management.base import BaseCommand

from matching.enrichment.ai_research import (
    ProfileResearchCache,
    research_and_enrich_profile,
)

# Fallback URLs when main site times out or is sparse (name_lower -> url path or full url)
WEBSITE_FALLBACKS = {
    "david riklan": "https://selfgrowth.com/davidriklan_bio.html",
}

# The 6 that were filled from web fetch only (run Claude pipeline for these)
DEFAULT_NAMES = [
    "David Riklan",
    "Diane Forster",
    "Bob Sparkins",
    "Dr. Stephen J. Kosmyna",
    "Cathy Smith",
    "Julie Ann Mercouris",
]

MATCHES_CSV = Path(__file__).resolve().parents[3] / "Chelsea_clients" / "janet_actionable_matches.csv"


class Command(BaseCommand):
    help = "Run Claude-based profile research for Janet's matches and update research_cache"

    def add_arguments(self, parser):
        parser.add_argument(
            "--all",
            action="store_true",
            help="Research all 10 matches (otherwise only the 6 previously hand-filled)",
        )
        parser.add_argument(
            "--names",
            type=str,
            help="Comma-separated list of names to research (e.g. 'David Riklan,Cathy Smith')",
        )

    def handle(self, *args, **options):
        if options["names"]:
            names = [n.strip() for n in options["names"].split(",") if n.strip()]
        elif options["all"]:
            names = self._load_all_names_from_csv()
        else:
            names = DEFAULT_NAMES

        self.stdout.write(self.style.SUCCESS(f"\nClaude-based research for {len(names)} profile(s)\n"))
        self.stdout.write("=" * 60)

        csv_data = self._load_csv_lookup()

        cache = ProfileResearchCache()
        researched_count = 0

        for i, name in enumerate(names, 1):
            self.stdout.write(f"\n[{i}/{len(names)}] {name}")

            website = WEBSITE_FALLBACKS.get(name.lower()) or csv_data.get(name.lower(), "").strip()
            if not website:
                self.stdout.write(self.style.WARNING("  No website in CSV, skipping"))
                continue

            if not website.startswith("http"):
                website = "https://" + website

            existing_data = cache.get(name) or {}
            if not existing_data:
                row = self._row_for_name(name, csv_data)
                if row:
                    existing_data = {
                        "name": name,
                        "email": row.get("email", ""),
                        "company": row.get("company", ""),
                        "website": website,
                        "linkedin": row.get("linkedin", ""),
                        "niche": row.get("niche", ""),
                        "list_size": int(row.get("list_size") or 0),
                    }

            try:
                enriched, was_researched = research_and_enrich_profile(
                    name=name,
                    website=website,
                    existing_data=existing_data,
                    use_cache=True,
                    force_research=True,
                )
                if was_researched:
                    researched_count += 1
                    self.stdout.write(self.style.SUCCESS("  Researched and cached"))
                    if enriched.get("who_you_serve"):
                        self.stdout.write(f"  who_you_serve: {enriched['who_you_serve'][:70]}...")
                    if enriched.get("seeking"):
                        self.stdout.write(f"  seeking: {enriched['seeking'][:70]}...")
                else:
                    self.stdout.write(self.style.WARNING("  No new data (check website or API)"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"  Error: {e}"))

        self.stdout.write("\n" + "=" * 60)
        self.stdout.write(self.style.SUCCESS(f"Done. Researched {researched_count}/{len(names)} profiles.\n"))

    def _load_csv_lookup(self):
        """Return dict name_lower -> website from janet_actionable_matches.csv"""
        out = {}
        if not MATCHES_CSV.exists():
            return out
        with open(MATCHES_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = (row.get("name") or "").strip()
                if name:
                    out[name.lower()] = (row.get("website") or "").strip()
        return out

    def _load_all_names_from_csv(self):
        names = []
        if not MATCHES_CSV.exists():
            return names
        with open(MATCHES_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = (row.get("name") or "").strip()
                if name and len(names) < 10:
                    names.append(name)
        return names

    def _row_for_name(self, name, csv_data):
        """Get full row for name from CSV."""
        if not MATCHES_CSV.exists():
            return None
        with open(MATCHES_CSV, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if (row.get("name") or "").strip().lower() == name.lower():
                    return row
        return None
