"""
Accel Portfolio Scraper

Fetches portfolio companies from Accel's website. The company listing page
is JS-rendered (Webflow + webpack bundles), so company data is not available
in the raw HTML. Instead, we parse the sitemap.xml to discover all company
page URLs, then extract company names from the URL slugs.

Optionally fetches individual company pages for descriptions and metadata
via og:description and og:title meta tags.

Data includes:
- Company name (from sitemap URL slug)
- Company page URL
- Description (from meta tags on detail pages, if fetched)

Source: https://www.accel.com/companies
Sitemap: https://www.accel.com/sitemap.xml
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


SITEMAP_URL = "https://www.accel.com/sitemap.xml"


class Scraper(BaseScraper):
    SOURCE_NAME = "accel_portfolio"
    BASE_URL = "https://www.accel.com/companies"
    REQUESTS_PER_MINUTE = 10

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_names: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run()."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a sitemap XML for company URLs.

        Extracts all URLs matching /companies/{slug} pattern from
        the sitemap XML.
        """
        contacts = []

        # Parse sitemap XML for company URLs
        soup = self.parse_html(html)
        loc_tags = soup.find_all("loc")

        for loc in loc_tags:
            page_url = loc.get_text(strip=True)
            if not page_url:
                continue

            # Only process company pages
            match = re.match(
                r"https?://www\.accel\.com/companies/([a-z0-9\-]+)/?$",
                page_url,
                re.I,
            )
            if not match:
                continue

            slug = match.group(1)
            if not slug or slug == "companies":
                continue

            # Convert slug to company name
            name = slug.replace("-", " ").title()

            # Handle common naming patterns
            # E.g., "1password" -> "1Password", "100ms" -> "100ms"
            if slug[0].isdigit():
                name = slug  # Keep original for numeric-starting names

            name_lower = name.lower()
            if name_lower in self._seen_names:
                continue
            self._seen_names.add(name_lower)

            source_url = page_url

            contact = ScrapedContact(
                name=name,
                email="",
                company=name,
                website=source_url,
                linkedin="",
                phone="",
                bio="Accel portfolio company.",
                source_platform=self.SOURCE_NAME,
                source_url=source_url,
                source_category="vc_portfolio",
                raw_data={
                    "slug": slug,
                    "vc_firm": "Accel",
                },
            )
            contacts.append(contact)

        return contacts

    def _enrich_from_detail_page(self, contact: ScrapedContact) -> None:
        """Optionally fetch a company detail page to get description."""
        html = self.fetch_page(contact.source_url)
        if not html:
            return

        soup = self.parse_html(html)

        # Get og:title for proper company name
        og_title = soup.find("meta", attrs={"property": "og:title"})
        if og_title:
            title = (og_title.get("content") or "").strip()
            # Often format is "Company Name | Accel"
            if title and "|" in title:
                title = title.split("|")[0].strip()
            if title and len(title) > 1:
                contact.name = title
                contact.company = title

        # Get og:description
        og_desc = soup.find("meta", attrs={"property": "og:description"})
        if og_desc:
            description = (og_desc.get("content") or "").strip()
            if description:
                contact.bio = f"{description} Accel portfolio company."
                contact.raw_data["description"] = description

        # Get actual website link if available
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and not og_desc:
            description = (meta_desc.get("content") or "").strip()
            if description:
                contact.bio = f"{description} Accel portfolio company."

        # Look for external website link
        for a_tag in soup.find_all("a", href=True):
            href = (a_tag.get("href") or "").strip()
            if href.startswith("http") and "accel.com" not in href:
                if not any(s in href for s in [
                    "twitter.com", "linkedin.com", "facebook.com",
                    "instagram.com", "youtube.com", "x.com",
                    "fonts.googleapis.com", "google.com", "gstatic.com",
                ]):
                    contact.website = href
                    break

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Fetch Accel portfolio companies from sitemap.

        Strategy:
        1. Parse sitemap.xml to discover all /companies/{slug} URLs
        2. Extract company names from URL slugs
        3. Optionally enrich with detail page metadata
        """
        self.logger.info("Starting %s scraper", self.SOURCE_NAME)

        contacts_yielded = 0

        # Fetch sitemap
        self.logger.info("Fetching sitemap from %s...", SITEMAP_URL)
        sitemap_html = self.fetch_page(SITEMAP_URL)
        if not sitemap_html:
            self.logger.error("Failed to fetch sitemap")
            return

        try:
            contacts = self.scrape_page(SITEMAP_URL, sitemap_html)
        except Exception as exc:
            self.logger.error("Sitemap parse error: %s", exc)
            self.stats["errors"] += 1
            return

        self.logger.info("Found %d companies in sitemap", len(contacts))

        # Resume from checkpoint
        start_from = (checkpoint or {}).get("last_url")
        past_checkpoint = start_from is None
        detail_pages_fetched = 0

        for contact in contacts:
            if not past_checkpoint:
                if contact.source_url == start_from:
                    past_checkpoint = True
                continue

            # Optionally enrich with detail page (rate-limited)
            # Only do this if max_pages is set (controls detail page fetching)
            if max_pages and detail_pages_fetched < max_pages:
                try:
                    self._enrich_from_detail_page(contact)
                    detail_pages_fetched += 1
                except Exception as exc:
                    self.logger.debug("Detail page error for %s: %s",
                                     contact.name, exc)

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

        self.logger.info("Scraper complete: %s", self.stats)
