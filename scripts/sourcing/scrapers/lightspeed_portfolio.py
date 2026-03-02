"""
Lightspeed Venture Partners Portfolio Scraper

Fetches portfolio companies from Lightspeed's Next.js-based website.
The site embeds company data in __NEXT_DATA__ and also has a JavaScript
autocomplete array (window.companiesAutocomplete) with 700+ company names.

Data includes:
- Company name, founder names/titles
- Founded year, investment stage, status (Private/IPO/Acquired)
- Sector classification (AI, Healthcare, Fintech, etc.)
- Year backed since

Source: https://lsvp.com/portfolio/
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


class Scraper(BaseScraper):
    SOURCE_NAME = "lightspeed_portfolio"
    BASE_URL = "https://lsvp.com/portfolio/"
    REQUESTS_PER_MINUTE = 10

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_names: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run()."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Lightspeed portfolio page for company data.

        Extraction strategies:
        1. __NEXT_DATA__ JSON
        2. companiesAutocomplete JavaScript array
        3. HTML company cards
        """
        contacts = []

        # Strategy 1: __NEXT_DATA__
        next_data_contacts = self._parse_next_data(html)
        if next_data_contacts:
            return next_data_contacts

        # Strategy 2: companiesAutocomplete array
        autocomplete_contacts = self._parse_autocomplete(html)
        if autocomplete_contacts:
            return autocomplete_contacts

        # Strategy 3: HTML parsing
        soup = self.parse_html(html)

        # Look for company cards
        cards = soup.find_all(
            ["div", "article", "li"],
            class_=re.compile(r"portfolio|company|card", re.I),
        )

        if not cards:
            # Try link-based extraction
            cards = soup.find_all("a", href=re.compile(r"/portfolio/[a-z0-9\-]+/?$"))

        for card in cards:
            name = ""
            description = ""
            website = ""
            stage = ""
            status = ""
            founded = ""
            backed_since = ""

            # Extract name
            name_tag = card.find(["h2", "h3", "h4", "h5"])
            if name_tag:
                name = name_tag.get_text(strip=True)

            if not name:
                if hasattr(card, "get_text"):
                    name = card.get_text(strip=True)

            if not name or len(name) < 2:
                continue

            # Clean up name (remove extra whitespace)
            name = re.sub(r"\s+", " ", name).strip()

            # Skip navigation items
            if name.lower() in ("portfolio", "all", "filter", "search",
                                "load more", "view all"):
                continue

            name_lower = name.lower()
            if name_lower in self._seen_names:
                continue
            self._seen_names.add(name_lower)

            # Extract metadata from card text
            card_text = card.get_text(" ", strip=True) if hasattr(card, "get_text") else ""

            founded_match = re.search(r"Founded\s+(\d{4})", card_text, re.I)
            if founded_match:
                founded = founded_match.group(1)

            stage_match = re.search(r"(Seed|Series\s+[A-F]\+?|Growth)", card_text, re.I)
            if stage_match:
                stage = stage_match.group(1)

            backed_match = re.search(r"Backed\s+Since\s+(\d{4})", card_text, re.I)
            if backed_match:
                backed_since = backed_match.group(1)

            status_match = re.search(r"(Private|IPO|Acquired|SPAC|Public)", card_text, re.I)
            if status_match:
                status = status_match.group(1)

            # Extract external link
            for a_tag in card.find_all("a", href=True) if hasattr(card, "find_all") else []:
                href = (a_tag.get("href") or "").strip()
                if href.startswith("http") and "lsvp.com" not in href:
                    website = href
                    break

            source_url = self.BASE_URL

            bio_parts = ["Lightspeed Venture Partners portfolio company."]
            if stage:
                bio_parts.append(f"Stage: {stage}.")
            if status:
                bio_parts.append(f"Status: {status}.")
            if founded:
                bio_parts.append(f"Founded: {founded}.")
            if backed_since:
                bio_parts.append(f"Backed since: {backed_since}.")
            bio = " ".join(bio_parts)

            contact = ScrapedContact(
                name=name,
                email="",
                company=name,
                website=website or source_url,
                linkedin="",
                phone="",
                bio=bio,
                source_platform=self.SOURCE_NAME,
                source_url=source_url,
                source_category="vc_portfolio",
                raw_data={
                    "stage": stage,
                    "status": status,
                    "founded": founded,
                    "backed_since": backed_since,
                    "vc_firm": "Lightspeed Venture Partners",
                },
            )
            contacts.append(contact)

        return contacts

    def _parse_next_data(self, html: str) -> list[ScrapedContact]:
        """Extract company data from __NEXT_DATA__ script tag."""
        contacts = []

        match = re.search(
            r'<script\s+id="__NEXT_DATA__"\s+type="application/json">(.*?)</script>',
            html,
            re.DOTALL,
        )
        if not match:
            return contacts

        try:
            data = json.loads(match.group(1))
        except (json.JSONDecodeError, ValueError):
            return contacts

        # Navigate to portfolio data (Next.js structure varies)
        page_props = data.get("props", {}).get("pageProps", {})

        # Try common field names
        companies = (
            page_props.get("companies", [])
            or page_props.get("portfolio", [])
            or page_props.get("data", {}).get("companies", [])
            or page_props.get("initialData", {}).get("companies", [])
        )

        if not isinstance(companies, list):
            return contacts

        for item in companies:
            if not isinstance(item, dict):
                continue

            name = (
                item.get("title") or item.get("name") or item.get("company_name") or ""
            ).strip()

            if not name or len(name) < 2:
                continue

            name_lower = name.lower()
            if name_lower in self._seen_names:
                continue
            self._seen_names.add(name_lower)

            website = (item.get("website") or item.get("url") or item.get("company_url") or "").strip()
            description = (item.get("description") or item.get("tagline") or item.get("one_liner") or "").strip()
            stage = (item.get("stage") or item.get("investment_stage") or "").strip()
            status = (item.get("status") or item.get("company_status") or "").strip()
            sector = (item.get("sector") or item.get("industry") or item.get("category") or "").strip()
            founded = str(item.get("founded_year") or item.get("founded") or "").strip()
            slug = (item.get("slug") or "").strip()

            source_url = f"https://lsvp.com/portfolio/{slug}/" if slug else self.BASE_URL

            bio_parts = []
            if description:
                bio_parts.append(description)
            bio_parts.append("Lightspeed Venture Partners portfolio company.")
            if sector:
                bio_parts.append(f"Sector: {sector}.")
            if stage:
                bio_parts.append(f"Stage: {stage}.")
            if status:
                bio_parts.append(f"Status: {status}.")
            if founded:
                bio_parts.append(f"Founded: {founded}.")
            bio = " ".join(bio_parts)

            contact = ScrapedContact(
                name=name,
                email="",
                company=name,
                website=website or source_url,
                linkedin="",
                phone="",
                bio=bio,
                source_platform=self.SOURCE_NAME,
                source_url=source_url,
                source_category="vc_portfolio",
                raw_data={
                    "slug": slug,
                    "sector": sector,
                    "stage": stage,
                    "status": status,
                    "founded": founded,
                    "vc_firm": "Lightspeed Venture Partners",
                },
            )
            contacts.append(contact)

        return contacts

    def _parse_autocomplete(self, html: str) -> list[ScrapedContact]:
        """Extract company names from companiesAutocomplete JavaScript array."""
        contacts = []

        match = re.search(
            r"window\.companiesAutocomplete\s*=\s*(\[.*?\])\s*;",
            html,
            re.DOTALL,
        )
        if not match:
            # Try alternative pattern
            match = re.search(
                r"companiesAutocomplete\s*[:=]\s*(\[.*?\])",
                html,
                re.DOTALL,
            )
        if not match:
            return contacts

        try:
            names = json.loads(match.group(1))
        except (json.JSONDecodeError, ValueError):
            return contacts

        if not isinstance(names, list):
            return contacts

        for entry in names:
            # Entry might be a string (name) or a dict
            if isinstance(entry, str):
                name = entry.strip()
                slug = ""
            elif isinstance(entry, dict):
                name = (entry.get("label") or entry.get("name") or entry.get("value") or "").strip()
                slug = (entry.get("slug") or entry.get("value") or "").strip()
            else:
                continue

            if not name or len(name) < 2:
                continue

            name_lower = name.lower()
            if name_lower in self._seen_names:
                continue
            self._seen_names.add(name_lower)

            source_url = self.BASE_URL

            contact = ScrapedContact(
                name=name,
                email="",
                company=name,
                website=source_url,
                linkedin="",
                phone="",
                bio="Lightspeed Venture Partners portfolio company.",
                source_platform=self.SOURCE_NAME,
                source_url=source_url,
                source_category="vc_portfolio",
                raw_data={
                    "slug": slug,
                    "vc_firm": "Lightspeed Venture Partners",
                },
            )
            contacts.append(contact)

        return contacts

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Fetch Lightspeed portfolio companies."""
        self.logger.info("Starting %s scraper", self.SOURCE_NAME)

        contacts_yielded = 0

        html = self.fetch_page(self.BASE_URL)
        if not html:
            self.logger.error("Failed to fetch portfolio page")
            return

        try:
            contacts = self.scrape_page(self.BASE_URL, html)
        except Exception as exc:
            self.logger.error("Parse error: %s", exc)
            self.stats["errors"] += 1
            return

        self.logger.info("Found %d companies", len(contacts))

        for contact in contacts:
            contact.source_platform = self.SOURCE_NAME
            contact.source_url = self.BASE_URL
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
