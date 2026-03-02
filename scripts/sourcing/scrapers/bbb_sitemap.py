"""
BBB.org (Better Business Bureau) Sitemap Scraper

Extracts business names, categories, and locations from BBB's sitemap XML.
BBB has ~540 sitemap files with ~10,000 URLs each = ~5.4M business profiles.

The sitemap is publicly accessible (referenced in robots.txt) and the URLs
contain rich structured data:

    /us/{state}/{city}/profile/{category}/{business-slug}-{bbb-id}

From the URL alone we can extract:
  - Country (us/ca)
  - State/Province
  - City
  - Business category (e.g. "home-builders", "insurance-services-office")
  - Business name (from slug)
  - BBB ID

These are real US and Canadian businesses across all industries.
High-volume source ideal for JV prospecting at scale.
"""

import re
import xml.etree.ElementTree as ET
from typing import Iterator, Optional
from datetime import datetime

from scripts.sourcing.base import BaseScraper, ScrapedContact


class Scraper(BaseScraper):
    SOURCE_NAME = "bbb_sitemap"
    BASE_URL = "https://www.bbb.org"
    REQUESTS_PER_MINUTE = 5  # Be very polite to BBB

    SITEMAP_INDEX = "https://www.bbb.org/sitemap-business-profiles-index.xml"

    # URL pattern: /{country}/{state}/{city}/profile/{category}/{slug}-{id}
    PROFILE_RE = re.compile(
        r"/(us|ca)/([^/]+)/([^/]+)/profile/([^/]+)/([^/?#]+)"
    )

    # BBB ID suffix pattern at end of slug: company-name-0011-12345
    BBB_ID_RE = re.compile(r"-(\d{4}-\d+)$")

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_slugs: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run()."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used -- we override run()."""
        return []

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """
        Two-phase: fetch sitemap index -> fetch each business profile sitemap -> extract URLs.
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

        # Parse sitemap index for business profile sitemaps
        profile_sitemaps = []
        try:
            root = ET.fromstring(resp.content)
            ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            for sitemap in root.findall("sm:sitemap", ns):
                loc = sitemap.find("sm:loc", ns)
                if loc is not None and loc.text:
                    profile_sitemaps.append(loc.text.strip())
        except ET.ParseError as e:
            self.logger.error("Failed to parse sitemap index: %s", e)
            return

        self.logger.info("Found %d business profile sitemaps", len(profile_sitemaps))

        start_sitemap = (checkpoint or {}).get("sitemap_idx", 0)
        contacts_yielded = 0
        pages_done = 0

        # Phase 2: Fetch each profile sitemap
        for sm_idx, sm_url in enumerate(profile_sitemaps):
            if sm_idx < start_sitemap:
                continue

            self.logger.info(
                "Processing sitemap %d/%d: %s",
                sm_idx + 1, len(profile_sitemaps), sm_url,
            )

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

            # Parse business profile URLs from sitemap
            try:
                root = ET.fromstring(resp.content)
                ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
                for url_elem in root.findall("sm:url", ns):
                    loc = url_elem.find("sm:loc", ns)
                    if loc is None or not loc.text:
                        continue

                    profile_url = loc.text.strip()
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

            if pages_done % 10 == 0:
                self.logger.info(
                    "Progress: %d/%d sitemaps, %d valid contacts",
                    pages_done, len(profile_sitemaps),
                    self.stats["contacts_valid"],
                )

            if max_pages and pages_done >= max_pages:
                self.logger.info("Reached max_pages=%d", max_pages)
                break

        self.logger.info("Scraper complete: %s", self.stats)

    def _parse_profile_url(self, url: str) -> ScrapedContact | None:
        """
        Extract business info from a BBB profile URL.

        URL format:
          https://www.bbb.org/us/md/baltimore/profile/fuel-oil/carroll-independent-fuel-llc-0011-10000060
        """
        match = self.PROFILE_RE.search(url)
        if not match:
            return None

        country = match.group(1)
        state = match.group(2)
        city = match.group(3)
        category_slug = match.group(4)
        business_slug = match.group(5)

        if not business_slug or len(business_slug) < 2:
            return None

        if business_slug in self._seen_slugs:
            return None
        self._seen_slugs.add(business_slug)

        # Strip the BBB ID suffix from the slug to get the business name
        # e.g. "carroll-independent-fuel-llc-0011-10000060" -> "carroll-independent-fuel-llc"
        bbb_id = ""
        id_match = self.BBB_ID_RE.search(business_slug)
        if id_match:
            bbb_id = id_match.group(1)
            name_slug = business_slug[: id_match.start()]
        else:
            name_slug = business_slug

        # Convert slug to readable name
        name = name_slug.replace("-", " ").strip().title()
        if not name or len(name) < 3:
            return None

        # Convert category slug to readable form
        category = category_slug.replace("-", " ").strip().title()

        # Convert location to readable form
        city_clean = city.replace("-", " ").strip().title()
        state_upper = state.upper()
        country_upper = country.upper()
        location = f"{city_clean}, {state_upper}"

        bio = f"{name} | {category} in {location} | BBB Business Profile"

        contact = ScrapedContact(
            name=name,
            email="",
            company=name,
            website=url,  # BBB profile URL
            linkedin="",
            phone="",
            bio=bio,
            source_platform=self.SOURCE_NAME,
            source_url=url,
            source_category=(category_slug or "").strip(),
            scraped_at=datetime.now().isoformat(),
            raw_data={
                "slug": business_slug,
                "profile_url": url,
                "bbb_id": (bbb_id or "").strip(),
                "country": (country_upper or "").strip(),
                "state": (state_upper or "").strip(),
                "city": (city_clean or "").strip(),
                "category": (category or "").strip(),
                "category_slug": (category_slug or "").strip(),
            },
        )

        if not contact.is_valid():
            return None

        self.stats["contacts_found"] += 1
        self.stats["contacts_valid"] += 1
        return contact
