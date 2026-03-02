"""
Index Ventures Portfolio Scraper

Fetches portfolio companies from Index Ventures' website. The site renders
a flat list of company names as anchor links to individual company pages.
Some entries include stock ticker information.

Data includes:
- Company name, stock ticker (if public)
- Link to company detail page
- Sector, region, stage (from detail pages or filters)

Source: https://www.indexventures.com/companies/
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


class Scraper(BaseScraper):
    SOURCE_NAME = "index_ventures"
    BASE_URL = "https://www.indexventures.com/companies/"
    REQUESTS_PER_MINUTE = 10

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_names: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run()."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Index Ventures companies page.

        Company entries appear as anchor links with pattern:
        /companies/{company-slug}/
        Some include ticker info like "NASDAQ: DIBS"
        """
        soup = self.parse_html(html)
        contacts = []

        # Find all company links
        company_links = soup.find_all("a", href=re.compile(r"/companies/[a-z0-9\-]+/?$"))

        for link in company_links:
            href = (link.get("href") or "").strip()
            slug = href.rstrip("/").split("/")[-1]

            if not slug or slug in ("companies", ""):
                continue

            # Get the full link text
            link_text = link.get_text(strip=True)
            if not link_text:
                continue

            # Parse company name and ticker
            # Format might be "Company Name" or "Company Name NASDAQ: TICK"
            name = link_text
            ticker = ""
            exchange = ""

            ticker_match = re.search(
                r"(NASDAQ|NYSE|LSE|XETRA|TSX|ASX):\s*([A-Z0-9.]+)\s*$",
                link_text,
            )
            if ticker_match:
                exchange = ticker_match.group(1)
                ticker = ticker_match.group(2)
                name = link_text[:ticker_match.start()].strip()

            if not name or len(name) < 2:
                continue

            # Skip navigation/filter items
            if name.lower() in ("companies", "all", "filter", "home",
                                "about", "team", "news"):
                continue

            name_lower = name.lower()
            if name_lower in self._seen_names:
                continue
            self._seen_names.add(name_lower)

            # Build source URL
            source_url = href
            if not source_url.startswith("http"):
                source_url = f"https://www.indexventures.com{href}"

            # Build bio
            bio_parts = ["Index Ventures portfolio company."]
            if ticker:
                bio_parts.append(f"Public: {exchange}: {ticker}.")
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
                    "ticker": ticker,
                    "exchange": exchange,
                    "vc_firm": "Index Ventures",
                },
            )
            contacts.append(contact)

        return contacts

    def _scrape_detail_page(self, source_url: str) -> dict:
        """Fetch a company detail page and extract additional data.

        Returns a dict with keys: website, description, sector, etc.
        """
        html = self.fetch_page(source_url)
        if not html:
            return {}

        soup = self.parse_html(html)
        result = {}

        # Look for external website link
        for a_tag in soup.find_all("a", href=True):
            href = (a_tag.get("href") or "").strip()
            if href.startswith("http") and "indexventures.com" not in href:
                # Skip social media links
                if not any(s in href for s in ["twitter.com", "linkedin.com",
                                                "facebook.com", "x.com"]):
                    result["website"] = href
                    break

        # Look for description
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            result["description"] = (meta_desc.get("content") or "").strip()

        # Look for og:description
        og_desc = soup.find("meta", attrs={"property": "og:description"})
        if og_desc and not result.get("description"):
            result["description"] = (og_desc.get("content") or "").strip()

        return result

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Fetch Index Ventures portfolio companies.

        Strategy:
        1. Fetch main companies listing page
        2. Extract all company names and links
        3. Optionally fetch detail pages for more info
        """
        self.logger.info("Starting %s scraper", self.SOURCE_NAME)

        contacts_yielded = 0

        # Fetch main companies page
        html = self.fetch_page(self.BASE_URL)
        if not html:
            self.logger.error("Failed to fetch companies page")
            return

        try:
            contacts = self.scrape_page(self.BASE_URL, html)
        except Exception as exc:
            self.logger.error("Parse error: %s", exc)
            self.stats["errors"] += 1
            return

        self.logger.info("Found %d companies on listing page", len(contacts))

        # Optionally enrich with detail pages
        detail_count = 0
        max_detail_pages = max_pages if max_pages else 0  # 0 = skip detail pages unless max_pages set

        for contact in contacts:
            # Try to get more info from detail page
            if max_detail_pages and detail_count < max_detail_pages:
                detail = self._scrape_detail_page(contact.source_url)
                if detail.get("website"):
                    contact.website = detail["website"]
                if detail.get("description"):
                    contact.bio = f"{detail['description']} {contact.bio}"
                detail_count += 1

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
