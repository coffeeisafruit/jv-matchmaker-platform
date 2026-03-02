"""
General Catalyst Portfolio Scraper

Fetches portfolio companies from General Catalyst's Webflow-based website.
The site uses Webflow CMS collections with client-side filtering
(fs-cmsfilter) for sector, investor, location, and status.

Data includes:
- Company name, logo
- Sector (AI, Enterprise, Fintech, Defense, Healthcare, Consumer, etc.)
- Lead investor names
- Location (Asia, Europe, North America, etc.)
- Status (Active, IPO, Acquired, Creation, CVF, Seed)

Source: https://www.generalcatalyst.com/portfolio
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


class Scraper(BaseScraper):
    SOURCE_NAME = "general_catalyst"
    BASE_URL = "https://www.generalcatalyst.com/portfolio"
    REQUESTS_PER_MINUTE = 10

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_names: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run()."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse General Catalyst portfolio page.

        Webflow CMS renders company data as collection items with
        fs-cmsfilter attributes for filtering metadata.
        """
        soup = self.parse_html(html)
        contacts = []

        # Strategy 1: Look for Webflow CMS collection items
        items = soup.find_all(
            ["div", "a"],
            class_=re.compile(r"w-dyn-item|collection-item", re.I),
        )

        if not items:
            # Try role=listitem (Webflow pattern)
            items = soup.find_all("div", role="listitem")

        if not items:
            # Try company card patterns
            items = soup.find_all(
                ["div", "a"],
                class_=re.compile(r"portfolio.*card|company.*card|card.*portfolio", re.I),
            )

        if not items:
            # Try links to company pages
            items = soup.find_all("a", href=re.compile(r"/companies/[a-z0-9\-]+/?$"))

        for item in items:
            name = ""
            website = ""
            description = ""
            sector = ""
            location = ""
            status = ""
            investor = ""

            # Extract company name from headings
            name_tag = item.find(["h2", "h3", "h4", "h5", "h6"])
            if name_tag:
                name = name_tag.get_text(strip=True)

            if not name:
                # Try image alt text
                img = item.find("img")
                if img:
                    alt = (img.get("alt") or "").strip()
                    if alt and len(alt) > 1 and alt.lower() not in ("logo", "image", "icon", ""):
                        name = alt

            if not name:
                # Try link text
                a_tag = item.find("a")
                if a_tag:
                    name = a_tag.get_text(strip=True)

            if not name or len(name) < 2:
                continue

            # Skip filter/UI elements and accessibility descriptions
            if name.lower() in ("portfolio", "all", "filter", "clear",
                                "load more", "show more", "view all",
                                "see all", "active", "ipo", "acquired",
                                "home", "about", "team", "news", "contact",
                                "companies", "careers", "insights"):
                continue

            # Skip names that look like UI descriptions or alt text
            if any(phrase in name.lower() for phrase in (
                "plus icon", "icon that", "filter that", "arrow",
                "close button", "menu", "toggle", "indicates",
                "button", "checkbox", "dropdown",
            )):
                continue

            name_lower = name.lower()
            if name_lower in self._seen_names:
                continue
            self._seen_names.add(name_lower)

            # Extract fs-cmsfilter metadata (Webflow filtering attributes)
            item_text = item.get_text(" ", strip=True) if hasattr(item, "get_text") else ""

            # Look for sector tags
            sector_tags = item.find_all(attrs={"fs-cmsfilter-field": "sector"})
            if sector_tags:
                sector = ", ".join(t.get_text(strip=True) for t in sector_tags)

            # Look for location tags
            location_tags = item.find_all(attrs={"fs-cmsfilter-field": "location"})
            if location_tags:
                location = ", ".join(t.get_text(strip=True) for t in location_tags)

            # Look for status tags
            status_tags = item.find_all(attrs={"fs-cmsfilter-field": "status"})
            if status_tags:
                status = ", ".join(t.get_text(strip=True) for t in status_tags)

            # Look for investor tags
            investor_tags = item.find_all(attrs={"fs-cmsfilter-field": "investor"})
            if investor_tags:
                investor = ", ".join(t.get_text(strip=True) for t in investor_tags)

            # Try to get href from link
            if hasattr(item, "name") and item.name == "a":
                href = (item.get("href") or "").strip()
            else:
                a_tag = item.find("a", href=True)
                href = (a_tag.get("href") or "").strip() if a_tag else ""

            if href:
                if href.startswith("/"):
                    href = f"https://www.generalcatalyst.com{href}"
                website = href

            # Extract external website link
            for a_tag in item.find_all("a", href=True) if hasattr(item, "find_all") else []:
                link_href = (a_tag.get("href") or "").strip()
                if link_href.startswith("http") and "generalcatalyst.com" not in link_href:
                    website = link_href
                    break

            source_url = website or self.BASE_URL

            bio_parts = ["General Catalyst portfolio company."]
            if sector:
                bio_parts.append(f"Sector: {sector}.")
            if location:
                bio_parts.append(f"Location: {location}.")
            if status:
                bio_parts.append(f"Status: {status}.")
            if investor:
                bio_parts.append(f"Lead investor: {investor}.")
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
                    "sector": sector,
                    "location": location,
                    "status": status,
                    "investor": investor,
                    "vc_firm": "General Catalyst",
                },
            )
            contacts.append(contact)

        # Fallback: extract company names from any pattern
        if not contacts:
            contacts = self._extract_from_text(soup)

        return contacts

    def _extract_from_text(self, soup) -> list[ScrapedContact]:
        """Fallback extraction from general page structure."""
        contacts = []

        # Look for all links that might be company pages
        for a_tag in soup.find_all("a", href=True):
            href = (a_tag.get("href") or "").strip()
            text = a_tag.get_text(strip=True)

            if not text or len(text) < 2 or len(text) > 80:
                continue

            # Check if this looks like a company link
            is_company = (
                "/companies/" in href
                or "/portfolio/" in href
                or (href.startswith("http") and "generalcatalyst.com" not in href
                    and not any(s in href for s in [
                        "twitter.com", "linkedin.com", "facebook.com",
                        "instagram.com", "youtube.com", "x.com",
                    ]))
            )

            if not is_company:
                continue

            if text.lower() in ("home", "about", "team", "news", "contact",
                                "portfolio", "privacy", "terms", "careers"):
                continue

            name_lower = text.lower()
            if name_lower in self._seen_names:
                continue
            self._seen_names.add(name_lower)

            website = href if href.startswith("http") else self.BASE_URL

            contact = ScrapedContact(
                name=text,
                email="",
                company=text,
                website=website,
                linkedin="",
                phone="",
                bio="General Catalyst portfolio company.",
                source_platform=self.SOURCE_NAME,
                source_url=self.BASE_URL,
                source_category="vc_portfolio",
                raw_data={"vc_firm": "General Catalyst"},
            )
            contacts.append(contact)

        return contacts

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Fetch General Catalyst portfolio companies.

        The Webflow site may paginate via ?page=N or load all at once.
        We try the main page first, then attempt pagination.
        """
        self.logger.info("Starting %s scraper", self.SOURCE_NAME)

        contacts_yielded = 0
        pages_done = 0

        # Fetch main portfolio page
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

        self.logger.info("Main page: found %d companies", len(contacts))
        pages_done += 1

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

        # Try pagination (Webflow CMS often paginates at /portfolio?page=N)
        page = 2
        empty_pages = 0
        max_pagination = 20  # Safety limit

        while empty_pages < 2 and page <= max_pagination:
            if max_pages and pages_done >= max_pages:
                break

            page_url = f"{self.BASE_URL}?page={page}"
            page_html = self.fetch_page(page_url)

            if not page_html:
                empty_pages += 1
                page += 1
                continue

            try:
                page_contacts = self.scrape_page(page_url, page_html)
            except Exception as exc:
                self.logger.debug("Parse error on page %d: %s", page, exc)
                page += 1
                continue

            if not page_contacts:
                empty_pages += 1
                page += 1
                continue

            empty_pages = 0
            pages_done += 1

            for contact in page_contacts:
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

            self.logger.info(
                "Page %d: %d new companies, %d total valid",
                page, len(page_contacts), self.stats["contacts_valid"],
            )
            page += 1

        self.logger.info("Scraper complete: %s", self.stats)
