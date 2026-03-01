"""
JVZoo marketplace scraper.

JVZoo is a major JV/affiliate marketplace. Product pages list vendor info
and JV/affiliate signup pages. These are people actively doing JV launches.

Estimated yield: 1,000-2,000 vendors
"""

from __future__ import annotations

import re
from typing import Iterator
from urllib.parse import urljoin

from scripts.sourcing.base import BaseScraper, ScrapedContact


# JVZoo marketplace categories
CATEGORIES = [
    "internet-marketing",
    "e-business",
    "health",
    "self-help",
    "education",
    "software",
    "green",
    "business",
]

MAX_PAGES = 30


class Scraper(BaseScraper):
    SOURCE_NAME = "jvzoo"
    BASE_URL = "https://www.jvzoo.com"
    REQUESTS_PER_MINUTE = 5

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_vendors: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield JVZoo marketplace pages."""
        # Main marketplace listing
        for page in range(1, MAX_PAGES + 1):
            yield f"{self.BASE_URL}/products/list?page={page}"

        # Category listings
        for cat in CATEGORIES:
            for page in range(1, MAX_PAGES + 1):
                yield f"{self.BASE_URL}/products/list/{cat}?page={page}"

        # Top sellers / featured
        yield f"{self.BASE_URL}/products/top"
        yield f"{self.BASE_URL}/products/featured"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse JVZoo product listing page."""
        soup = self.parse_html(html)
        contacts = []

        # Find product cards/rows
        for card in soup.find_all(class_=re.compile(r"product|listing|item|result", re.I)):
            contact = self._parse_product_card(card)
            if contact:
                contacts.append(contact)

        # Also try generic table rows
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 2:
                contact = self._parse_row(cells)
                if contact:
                    contacts.append(contact)

        # Fallback: scan links
        if not contacts:
            contacts = self._scan_links(soup)

        return contacts

    def _parse_product_card(self, card) -> ScrapedContact | None:
        """Parse a product card for vendor info."""
        # Product name
        title_el = card.find(["h2", "h3", "h4", "h5"]) or card.find(class_=re.compile(r"title|name", re.I))
        title = title_el.get_text(strip=True) if title_el else ""

        # Vendor name
        vendor = ""
        vendor_el = card.find(class_=re.compile(r"vendor|seller|creator|by", re.I))
        if vendor_el:
            vendor = vendor_el.get_text(strip=True)
            vendor = re.sub(r"^(by|from|vendor:?)\s*", "", vendor, flags=re.I).strip()

        if not vendor:
            text = card.get_text()
            by_match = re.search(r"(?:by|from|vendor:?)\s+([A-Z][a-zA-Z\s.]+?)(?:\s*[|,\n]|$)", text)
            if by_match:
                vendor = by_match.group(1).strip()

        name = vendor or title
        if not name or len(name) < 2 or name.lower() in self._seen_vendors:
            return None
        self._seen_vendors.add(name.lower())

        # JV page / product website
        website = ""
        jv_page = ""
        for a in card.find_all("a", href=True):
            href = a["href"]
            href_lower = href.lower()
            link_text = a.get_text(strip=True).lower()
            if "jv" in link_text or "affiliate" in link_text or "/jv" in href_lower:
                jv_page = href if href.startswith("http") else urljoin(self.BASE_URL, href)
            elif href.startswith("http") and "jvzoo.com" not in href_lower and not website:
                website = href

        bio = f"JVZoo vendor: {title}" if title and title != name else "JVZoo marketplace vendor"

        return ScrapedContact(
            name=name,
            website=jv_page or website,
            bio=bio,
            source_category="jv_marketplace",
        )

    def _parse_row(self, cells) -> ScrapedContact | None:
        """Parse table cells for product/vendor info."""
        texts = [c.get_text(strip=True) for c in cells]
        name = texts[0] if texts else ""

        if not name or len(name) < 2 or name.lower() in self._seen_vendors:
            return None
        self._seen_vendors.add(name.lower())

        website = ""
        for cell in cells:
            for a in cell.find_all("a", href=True):
                href = a["href"]
                if href.startswith("http") and "jvzoo.com" not in href.lower():
                    website = href
                    break
            if website:
                break

        return ScrapedContact(
            name=name,
            website=website,
            bio="JVZoo marketplace vendor",
            source_category="jv_marketplace",
        )

    def _scan_links(self, soup) -> list[ScrapedContact]:
        """Fallback: extract vendor info from page links."""
        contacts = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            href_lower = href.lower()

            if any(kw in href_lower for kw in ["/jv", "/affiliate", "vendor="]):
                if not text or len(text) < 2 or len(text) > 80:
                    continue
                if text.lower() in self._seen_vendors:
                    continue
                self._seen_vendors.add(text.lower())
                contacts.append(ScrapedContact(
                    name=text,
                    website=href if href.startswith("http") else urljoin(self.BASE_URL, href),
                    bio="JVZoo marketplace vendor",
                    source_category="jv_marketplace",
                ))
        return contacts
