"""
IRS 501(c)(6) Business Leagues Scraper

Focused scraper that ONLY extracts 501(c)(6) organizations from the IRS
Business Master File. These are business leagues, chambers of commerce,
real estate boards, and trade associations — all directly JV-relevant.

There are ~70,000+ 501(c)(6) orgs in the full IRS dataset.

Data source: https://www.irs.gov/pub/irs-soi/eo*.csv (4 regional files)
"""

import csv
import io
from typing import Iterator, Optional
from datetime import datetime

from scripts.sourcing.base import BaseScraper, ScrapedContact


class Scraper(BaseScraper):
    SOURCE_NAME = "irs_business_leagues"
    BASE_URL = "https://www.irs.gov"
    REQUESTS_PER_MINUTE = 2

    BMF_URLS = [
        "https://www.irs.gov/pub/irs-soi/eo1.csv",
        "https://www.irs.gov/pub/irs-soi/eo2.csv",
        "https://www.irs.gov/pub/irs-soi/eo3.csv",
        "https://www.irs.gov/pub/irs-soi/eo4.csv",
    ]

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_eins: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        return []

    def run(self, max_pages=0, max_contacts=0, checkpoint=None):
        """Download IRS data and extract ONLY 501(c)(6) business leagues."""
        self.logger.info("Starting IRS 501(c)(6) Business Leagues scraper")

        start_file = (checkpoint or {}).get("file_index", 0)

        for file_idx, csv_url in enumerate(self.BMF_URLS):
            if file_idx < start_file:
                continue

            self.logger.info("Downloading file %d/%d: %s",
                             file_idx + 1, len(self.BMF_URLS), csv_url)

            try:
                resp = self.session.get(csv_url, timeout=300)
                if resp.status_code != 200:
                    self.logger.error("HTTP %d for %s", resp.status_code, csv_url)
                    continue

                self.logger.info("Downloaded %.1f MB", len(resp.content) / 1024 / 1024)

                reader = csv.DictReader(io.StringIO(resp.text))
                for row in reader:
                    # ONLY 501(c)(6) — business leagues, chambers, trade associations
                    if row.get("SUBSECTION", "").strip() != "06":
                        continue

                    contact = self._parse_row(row, csv_url)
                    if contact:
                        self.stats["contacts_valid"] += 1
                        yield contact

                        if max_contacts and self.stats["contacts_valid"] >= max_contacts:
                            self.logger.info("Reached max_contacts=%d", max_contacts)
                            return

                self.logger.info("File %d done: %d valid 501(c)(6) orgs",
                                 file_idx + 1, self.stats["contacts_valid"])

            except Exception as e:
                self.logger.error("Error on %s: %s", csv_url, e)
                continue

        self.logger.info("Complete: %d 501(c)(6) business leagues found", self.stats["contacts_valid"])

    def _parse_row(self, row: dict, source_url: str) -> ScrapedContact | None:
        ein = row.get("EIN", "").strip()
        if not ein or ein in self._seen_eins:
            return None
        self._seen_eins.add(ein)

        name = row.get("NAME", "").strip()
        if not name or len(name) < 3:
            return None

        # Skip generic/invalid names
        if any(s in name.lower() for s in ["unknown", "invalid", "test", "n/a"]):
            return None

        city = row.get("CITY", "").strip()
        state = row.get("STATE", "").strip()
        street = row.get("STREET", "").strip()
        zip_code = row.get("ZIP", "").strip()

        if not city or not state:
            return None

        ntee = row.get("NTEE_CD", "").strip()

        # Build bio
        bio_parts = [name, f"{city}, {state}", "501(c)(6) Business League/Chamber"]
        if ntee:
            bio_parts.append(f"NTEE: {ntee}")

        # Financial data
        income = row.get("INCOME_AMT", "0").strip()
        revenue = row.get("REVENUE_AMT", "0").strip()
        try:
            income_int = int(income or 0)
            if income_int > 0:
                bio_parts.append(f"Income: ${income_int:,}")
        except (ValueError, TypeError):
            pass

        bio = " | ".join(bio_parts)
        website = f"https://www.guidestar.org/profile/{ein}"

        contact = ScrapedContact(
            name=name,
            company=name,
            email="",
            phone="",
            website=website,
            linkedin="",
            bio=bio,
            source_platform=self.SOURCE_NAME,
            source_url=source_url,
            source_category="business_leagues",
            scraped_at=datetime.now().isoformat(),
            raw_data={
                "ein": ein,
                "subsection": "06",
                "ntee_cd": ntee,
                "city": city,
                "state": state,
                "zip": zip_code,
                "income_amt": income,
                "revenue_amt": revenue,
            },
        )

        if not contact.is_valid():
            return None

        self.stats["contacts_found"] += 1
        return contact
