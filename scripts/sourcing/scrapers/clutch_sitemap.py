"""
Clutch.co Sitemap Scraper

Extracts company names and profile URLs from Clutch.co's sitemap XML.
Clutch has ~425,000 company profiles across 43 sitemap files.

While individual profile pages are Cloudflare-blocked, the sitemap XML
is publicly accessible and contains company names extractable from slugs.

These are real agencies: marketing, development, design, PR, etc.
Prime JV partnership material.
"""

import re
import xml.etree.ElementTree as ET
from typing import Iterator, Optional
from datetime import datetime

from scripts.sourcing.base import BaseScraper, ScrapedContact


class Scraper(BaseScraper):
    SOURCE_NAME = "clutch_sitemap"
    BASE_URL = "https://clutch.co"
    REQUESTS_PER_MINUTE = 5  # Be very polite

    SITEMAP_INDEX = "https://clutch.co/sitemap.xml"

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_slugs: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used — we override run()."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used — we override run()."""
        return []

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """
        Two-phase: fetch sitemap index → fetch each profile sitemap → extract URLs.
        """
        self.logger.info("Starting %s scraper", self.SOURCE_NAME)

        # Phase 1: Get sitemap index
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        try:
            resp = self.session.get(self.SITEMAP_INDEX, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            self.logger.error("Failed to fetch sitemap index: %s", e)
            return

        # Parse sitemap index for profile sitemaps
        profile_sitemaps = []
        try:
            root = ET.fromstring(resp.content)
            ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            for sitemap in root.findall("sm:sitemap", ns):
                loc = sitemap.find("sm:loc", ns)
                if loc is not None and "profile" in (loc.text or ""):
                    profile_sitemaps.append(loc.text)
        except ET.ParseError as e:
            self.logger.error("Failed to parse sitemap index: %s", e)
            return

        self.logger.info("Found %d profile sitemaps", len(profile_sitemaps))

        start_sitemap = (checkpoint or {}).get("sitemap_idx", 0)
        contacts_yielded = 0
        pages_done = 0

        # Phase 2: Fetch each profile sitemap
        for sm_idx, sm_url in enumerate(profile_sitemaps):
            if sm_idx < start_sitemap:
                continue

            self.logger.info("Processing sitemap %d/%d: %s",
                             sm_idx + 1, len(profile_sitemaps), sm_url)

            if self.rate_limiter:
                self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

            try:
                resp = self.session.get(sm_url, timeout=60)
                resp.raise_for_status()
                self.stats["pages_scraped"] += 1
            except Exception as e:
                self.stats["errors"] += 1
                self.logger.warning("Error fetching %s: %s", sm_url, e)
                continue

            # Parse profile URLs from sitemap
            try:
                root = ET.fromstring(resp.content)
                ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
                for url_elem in root.findall("sm:url", ns):
                    loc = url_elem.find("sm:loc", ns)
                    if loc is None or not loc.text:
                        continue

                    profile_url = loc.text.strip()
                    if "/profile/" not in profile_url:
                        continue

                    contact = self._parse_profile_url(profile_url)
                    if contact:
                        contacts_yielded += 1
                        yield contact

                        if max_contacts and contacts_yielded >= max_contacts:
                            self.logger.info("Reached max_contacts=%d", max_contacts)
                            return

            except ET.ParseError as e:
                self.stats["errors"] += 1
                self.logger.warning("Parse error on %s: %s", sm_url, e)
                continue

            pages_done += 1

            if pages_done % 5 == 0:
                self.logger.info(
                    "Progress: %d/%d sitemaps, %d valid contacts",
                    pages_done, len(profile_sitemaps),
                    self.stats["contacts_valid"]
                )

            if max_pages and pages_done >= max_pages:
                self.logger.info("Reached max_pages=%d", max_pages)
                break

        self.logger.info("Scraper complete: %s", self.stats)

    def _parse_profile_url(self, url: str) -> ScrapedContact | None:
        """Extract company name from Clutch profile URL."""
        # URL format: https://clutch.co/profile/company-name-slug
        match = re.search(r"/profile/([^/?#]+)", url)
        if not match:
            return None

        slug = match.group(1).strip()
        if not slug or len(slug) < 2:
            return None

        if slug in self._seen_slugs:
            return None
        self._seen_slugs.add(slug)

        # Convert slug to readable name
        # "nextec-group" → "Nextec Group"
        name = slug.replace("-", " ").title()

        # Skip very generic slugs
        if len(name) < 4:
            return None

        bio = f"{name} | Agency on Clutch.co"

        contact = ScrapedContact(
            name=name,
            email="",
            company=name,
            website=url,  # The Clutch profile URL itself
            linkedin="",
            phone="",
            bio=bio,
            source_platform=self.SOURCE_NAME,
            source_url=url,
            source_category="agencies",
            scraped_at=datetime.now().isoformat(),
            raw_data={
                "slug": slug,
                "profile_url": url,
            },
        )

        if not contact.is_valid():
            return None

        self.stats["contacts_found"] += 1
        self.stats["contacts_valid"] += 1
        return contact
