"""
Bessemer Venture Partners Portfolio Scraper

Fetches portfolio companies from BVP's WordPress-based website.
The site uses a dynamic filtering system with a /search.json.php endpoint
and renders company cards with metadata about founding year, partnership year,
categories (roadmaps), investor names, and exit status.

Data includes:
- Company name, founding year, partnership year
- Category tags (roadmaps): enterprise, consumer, AI/ML, healthcare
- Associated BVP investor names
- Exit status (IPO ticker, acquisition details)

Source: https://www.bvp.com/portfolio
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# BVP search endpoint for portfolio data
BVP_SEARCH_URL = "https://www.bvp.com/search.json.php"


class Scraper(BaseScraper):
    SOURCE_NAME = "bessemer_portfolio"
    BASE_URL = "https://www.bvp.com/portfolio"
    REQUESTS_PER_MINUTE = 10

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_names: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run()."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse BVP portfolio page for company data.

        BVP renders company cards in WordPress with rich metadata.
        """
        soup = self.parse_html(html)
        contacts = []

        # Strategy 1: Look for portfolio company cards
        cards = soup.find_all(
            ["div", "article", "a"],
            class_=re.compile(r"portfolio.*card|company.*card|portfolio.*item", re.I),
        )

        if not cards:
            # Try WordPress post/entry patterns
            cards = soup.find_all("article", class_=re.compile(r"post|entry", re.I))

        if not cards:
            # Try looking for links to /portfolio/ subpages
            cards = soup.find_all("a", href=re.compile(r"/portfolio/[a-z0-9\-]+/?$"))

        if not cards:
            # Try broader: any div that contains company-like content
            # Look for containers with company names and metadata
            for container in soup.find_all(["div", "section"]):
                headings = container.find_all(["h2", "h3", "h4"])
                if len(headings) >= 3:  # Multiple companies in this container
                    cards = headings
                    break

        for card in cards:
            name = ""
            website = ""
            description = ""
            founded = ""
            partnered = ""
            categories = []
            investors = []
            ticker = ""
            exit_info = ""

            # Extract company name
            if card.name in ("h2", "h3", "h4"):
                name = card.get_text(strip=True)
            else:
                name_tag = card.find(["h2", "h3", "h4", "h5", "strong"])
                if name_tag:
                    name = name_tag.get_text(strip=True)

            if not name:
                if card.name == "a":
                    name = card.get_text(strip=True)
                continue

            if not name or len(name) < 2:
                continue

            # Skip navigation/filter items and metadata labels
            if name.lower() in ("portfolio", "portfolio companies", "all",
                                "filter", "search", "load more", "enterprise",
                                "consumer", "healthcare", "ai", "featured",
                                "investors", "roadmaps", "view all",
                                "see all", "navigation", "menu", "founded",
                                "partnered", "roadmap", "status", "sector",
                                "industry", "stage", "year", "about",
                                "team", "blog", "contact", "careers",
                                "home", "news", "insights"):
                continue

            name_lower = name.lower()
            if name_lower in self._seen_names:
                continue
            self._seen_names.add(name_lower)

            # Extract href
            if card.name == "a":
                href = (card.get("href") or "").strip()
            else:
                a_tag = card.find("a", href=True)
                href = (a_tag.get("href") or "").strip() if a_tag else ""

            if href:
                if href.startswith("/"):
                    website = f"https://www.bvp.com{href}"
                elif href.startswith("http"):
                    website = href

            # Extract metadata from text
            card_text = card.get_text(" ", strip=True) if hasattr(card, "get_text") else ""

            founded_match = re.search(r"Founded\s*:?\s*(\d{4})", card_text, re.I)
            if founded_match:
                founded = founded_match.group(1)

            partnered_match = re.search(r"Partner(?:ed|ship)\s*:?\s*(\d{4})", card_text, re.I)
            if partnered_match:
                partnered = partnered_match.group(1)

            ticker_match = re.search(r"(NASDAQ|NYSE|LSE):\s*([A-Z0-9.]+)", card_text)
            if ticker_match:
                ticker = f"{ticker_match.group(1)}: {ticker_match.group(2)}"

            acquired_match = re.search(r"Acquired by\s+(\w[\w\s]*?)(?:\.|$)", card_text, re.I)
            if acquired_match:
                exit_info = f"Acquired by {acquired_match.group(1).strip()}"

            # Build source URL
            source_url = website or self.BASE_URL

            # Build bio
            bio_parts = ["Bessemer Venture Partners portfolio company."]
            if description:
                bio_parts.insert(0, description)
            if founded:
                bio_parts.append(f"Founded: {founded}.")
            if partnered:
                bio_parts.append(f"BVP partnership: {partnered}.")
            if ticker:
                bio_parts.append(f"Public: {ticker}.")
            if exit_info:
                bio_parts.append(f"{exit_info}.")
            bio = " ".join(bio_parts)

            contact = ScrapedContact(
                name=name,
                email="",
                company=name,
                website=website or self.BASE_URL,
                linkedin="",
                phone="",
                bio=bio,
                source_platform=self.SOURCE_NAME,
                source_url=source_url,
                source_category="vc_portfolio",
                raw_data={
                    "founded": founded,
                    "partnered": partnered,
                    "ticker": ticker,
                    "exit_info": exit_info,
                    "categories": categories,
                    "investors": investors,
                    "vc_firm": "Bessemer Venture Partners",
                },
            )
            contacts.append(contact)

        return contacts

    def _fetch_search_api(self, query: str = "", category: str = "") -> list[dict]:
        """Try BVP's search.json.php endpoint for portfolio data."""
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        params = {
            "type": "portfolio",
        }
        if query:
            params["q"] = query
        if category:
            params["category"] = category

        try:
            resp = self.session.get(BVP_SEARCH_URL, params=params, timeout=30)
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1
            data = resp.json()

            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return data.get("results", data.get("companies", data.get("items", [])))
            return []
        except Exception as exc:
            self.logger.debug("BVP search API failed: %s", exc)
            self.stats["errors"] += 1
            return []

    def _search_item_to_contact(self, item: dict) -> Optional[ScrapedContact]:
        """Convert a search API result to ScrapedContact."""
        if not isinstance(item, dict):
            return None

        name = (
            item.get("title") or item.get("name") or item.get("company_name") or ""
        ).strip()

        # Clean HTML
        name = re.sub(r"<[^>]+>", "", name).strip()
        name = (
            name
            .replace("&amp;", "&")
            .replace("&#8217;", "'")
            .replace("&quot;", '"')
        )

        if not name or len(name) < 2:
            return None

        name_lower = name.lower()
        if name_lower in self._seen_names:
            return None
        self._seen_names.add(name_lower)

        website = (item.get("url") or item.get("link") or item.get("website") or "").strip()
        description = (item.get("description") or item.get("excerpt") or "").strip()
        description = re.sub(r"<[^>]+>", "", description).strip()

        slug = (item.get("slug") or "").strip()
        source_url = website or (f"https://www.bvp.com/portfolio/{slug}" if slug else self.BASE_URL)

        bio_parts = []
        if description:
            bio_parts.append(description[:500])
        bio_parts.append("Bessemer Venture Partners portfolio company.")
        bio = " ".join(bio_parts)

        return ScrapedContact(
            name=name,
            email="",
            company=name,
            website=website or self.BASE_URL,
            linkedin="",
            phone="",
            bio=bio,
            source_platform=self.SOURCE_NAME,
            source_url=source_url,
            source_category="vc_portfolio",
            raw_data={
                "slug": slug,
                "vc_firm": "Bessemer Venture Partners",
            },
        )

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Fetch Bessemer portfolio companies.

        Strategy:
        1. Try search.json.php API endpoint
        2. Fall back to HTML page scraping
        """
        self.logger.info("Starting %s scraper", self.SOURCE_NAME)

        contacts_yielded = 0

        # Strategy 1: Try search API
        self.logger.info("Trying BVP search API...")
        api_results = self._fetch_search_api()
        api_contacts = []

        if api_results:
            for item in api_results:
                contact = self._search_item_to_contact(item)
                if contact:
                    api_contacts.append(contact)

        if api_contacts:
            self.logger.info("Search API returned %d companies", len(api_contacts))

            for contact in api_contacts:
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

        # Strategy 2: Scrape HTML page
        self.logger.info("Scraping HTML portfolio page...")
        html = self.fetch_page(self.BASE_URL)
        if not html:
            if not api_contacts:
                self.logger.error("Failed to fetch portfolio page and API returned no results")
            return

        try:
            html_contacts = self.scrape_page(self.BASE_URL, html)
        except Exception as exc:
            self.logger.error("Parse error: %s", exc)
            self.stats["errors"] += 1
            return

        self.logger.info("HTML page: found %d companies", len(html_contacts))

        for contact in html_contacts:
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
