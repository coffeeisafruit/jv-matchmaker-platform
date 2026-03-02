"""
Sequoia Capital Portfolio Scraper

Fetches portfolio companies from Sequoia Capital's website using their
FacetWP-powered REST API. The site is WordPress-based and uses FacetWP
for filtering/pagination with an AJAX endpoint.

Data includes:
- Company name, description, website link
- Investment stage (Pre-Seed/Seed, Early, Growth, IPO, Acquired)
- Category (AI, Consumer, Fintech, etc.)

Source: https://www.sequoiacap.com/our-companies/
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# FacetWP REST API endpoint
FACETWP_API_URL = "https://www.sequoiacap.com/wp-json/facetwp/v1/refresh"


class Scraper(BaseScraper):
    SOURCE_NAME = "sequoia_portfolio"
    BASE_URL = "https://www.sequoiacap.com/our-companies/"
    REQUESTS_PER_MINUTE = 10

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_names: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run() for FacetWP API pagination."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse the HTML portfolio page for company cards.

        Sequoia renders company cards as <a> tags with <h3> for name
        and <p> for description, linking to /companies/{slug}/.
        """
        soup = self.parse_html(html)
        contacts = []

        # Find all company card links
        company_links = soup.find_all("a", href=re.compile(r"/companies/[^/]+/?$"))

        for link in company_links:
            href = (link.get("href") or "").strip()
            if not href or "/companies/" not in href:
                continue

            # Extract slug from URL
            slug = href.rstrip("/").split("/")[-1]
            if not slug or slug == "companies":
                continue

            # Extract company name from h3 or other heading
            name_tag = link.find(["h3", "h4", "h2", "strong"])
            name = ""
            if name_tag:
                name = name_tag.get_text(strip=True)

            if not name:
                # Try getting text directly if no heading found
                texts = [t.strip() for t in link.stripped_strings]
                if texts:
                    name = texts[0]

            if not name:
                # Derive name from slug
                name = slug.replace("-", " ").title()

            # Skip duplicates
            name_lower = name.lower()
            if name_lower in self._seen_names:
                continue
            self._seen_names.add(name_lower)

            # Extract description from <p> tag
            desc_tag = link.find("p")
            description = ""
            if desc_tag:
                description = desc_tag.get_text(strip=True)

            # Extract image/logo URL
            img_tag = link.find("img")
            logo_url = ""
            if img_tag:
                logo_url = (img_tag.get("src") or img_tag.get("data-src") or "").strip()

            # Build source URL
            source_url = href
            if not source_url.startswith("http"):
                source_url = f"https://www.sequoiacap.com{href}"

            # Build bio
            bio_parts = []
            if description:
                bio_parts.append(description)
            bio_parts.append("Sequoia Capital portfolio company.")
            bio = " ".join(bio_parts)

            contact = ScrapedContact(
                name=name,
                email="",
                company=name,
                website=source_url,
                linkedin="",
                phone="",
                bio=bio,
                source_platform=self.SOURCE_NAME,
                source_url=source_url,
                source_category="vc_portfolio",
                raw_data={
                    "slug": slug,
                    "description": description,
                    "logo_url": logo_url,
                    "vc_firm": "Sequoia Capital",
                },
            )
            contacts.append(contact)

        return contacts

    def _fetch_facetwp_page(self, page: int = 1) -> Optional[str]:
        """Fetch a page of results from the FacetWP REST API.

        Returns the HTML content of the template, or None on failure.
        """
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        payload = {
            "action": "facetwp_refresh",
            "data": {
                "facets": {},
                "template": "companies",
                "paged": page,
                "http_params": {
                    "uri": "our-companies",
                    "url_vars": {},
                    "lang": "",
                },
                "frozen_facets": {},
                "soft_refresh": 0,
                "is_bfcache": 0,
                "first_load": 1 if page == 1 else 0,
                "extras": {},
            },
        }

        try:
            resp = self.session.post(
                FACETWP_API_URL,
                json=payload,
                timeout=30,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Referer": self.BASE_URL,
                },
            )
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1

            data = resp.json()
            return data.get("template", "")
        except Exception as exc:
            self.logger.warning("FacetWP API failed (page %d): %s", page, exc)
            self.stats["errors"] += 1
            return None

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Fetch Sequoia portfolio companies.

        Strategy:
        1. Try FacetWP REST API for paginated results
        2. Fall back to scraping the main HTML page
        """
        self.logger.info("Starting %s scraper", self.SOURCE_NAME)

        contacts_yielded = 0
        pages_done = 0

        # First, try the main HTML page to get all visible companies
        self.logger.info("Fetching main portfolio page...")
        html = self.fetch_page(self.BASE_URL)

        if html:
            contacts = self.scrape_page(self.BASE_URL, html)
            self.logger.info(
                "Main page: found %d companies", len(contacts)
            )

            for contact in contacts:
                contact.source_platform = self.SOURCE_NAME
                contact.source_url = contact.raw_data.get("slug", self.BASE_URL)
                contact.scraped_at = datetime.now().isoformat()
                contact.email = contact.clean_email()

                self.stats["contacts_found"] += 1

                if contact.is_valid():
                    self.stats["contacts_valid"] += 1
                    contacts_yielded += 1
                    yield contact

                    if max_contacts and contacts_yielded >= max_contacts:
                        self.logger.info("Reached max_contacts=%d", max_contacts)
                        return

            pages_done += 1

        # Try FacetWP API for additional pages
        page = 2
        empty_pages = 0

        while empty_pages < 2:
            if max_pages and pages_done >= max_pages:
                self.logger.info("Reached max_pages=%d", max_pages)
                break

            template_html = self._fetch_facetwp_page(page)
            if not template_html:
                empty_pages += 1
                page += 1
                continue

            contacts = self.scrape_page(f"facetwp_page_{page}", template_html)
            if not contacts:
                empty_pages += 1
                page += 1
                continue

            empty_pages = 0  # Reset on success

            for contact in contacts:
                contact.source_platform = self.SOURCE_NAME
                contact.scraped_at = datetime.now().isoformat()
                contact.email = contact.clean_email()

                self.stats["contacts_found"] += 1

                if contact.is_valid():
                    self.stats["contacts_valid"] += 1
                    contacts_yielded += 1
                    yield contact

                    if max_contacts and contacts_yielded >= max_contacts:
                        self.logger.info("Reached max_contacts=%d", max_contacts)
                        return

            pages_done += 1
            self.logger.info(
                "FacetWP page %d: %d companies, %d total valid",
                page, len(contacts), self.stats["contacts_valid"],
            )
            page += 1

        self.logger.info("Scraper complete: %s", self.stats)
