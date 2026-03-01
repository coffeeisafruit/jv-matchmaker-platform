"""
WarriorPlus marketplace scraper.

WarriorPlus is a digital product marketplace focused on internet marketing,
similar to JVZoo. Vendors sell courses, software, and info products.

Estimated yield: 800-1,500 vendors
"""

from __future__ import annotations

import re
from typing import Iterator
from urllib.parse import urljoin

from scripts.sourcing.base import BaseScraper, ScrapedContact


CATEGORIES = [
    "internet-marketing",
    "make-money-online",
    "seo-traffic",
    "social-media",
    "wordpress",
    "software",
    "ecommerce",
    "health-fitness",
    "self-help",
    "video",
    "copywriting",
    "coaching",
]

MAX_PAGES = 25


class Scraper(BaseScraper):
    SOURCE_NAME = "warriorplus"
    BASE_URL = "https://warriorplus.com"
    REQUESTS_PER_MINUTE = 5

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_vendors: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield WarriorPlus marketplace pages."""
        for cat in CATEGORIES:
            for page in range(1, MAX_PAGES + 1):
                yield f"{self.BASE_URL}/marketplace/{cat}?page={page}"

        # Top/best sellers
        for page in range(1, MAX_PAGES + 1):
            yield f"{self.BASE_URL}/marketplace/best-sellers?page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse WarriorPlus product listings."""
        soup = self.parse_html(html)
        contacts = []

        # Product cards
        for card in soup.find_all(class_=re.compile(r"product|listing|offer|item", re.I)):
            contact = self._parse_card(card)
            if contact:
                contacts.append(contact)

        # Table rows
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 2:
                contact = self._parse_cells(cells)
                if contact:
                    contacts.append(contact)

        # Links fallback
        if not contacts:
            for a in soup.find_all("a", href=True):
                href = a["href"]
                text = a.get_text(strip=True)
                if "/o/" in href or "/vendor/" in href:
                    if text and len(text) > 2 and len(text) < 80:
                        if text.lower() not in self._seen_vendors:
                            self._seen_vendors.add(text.lower())
                            full_url = href if href.startswith("http") else urljoin(self.BASE_URL, href)
                            contacts.append(ScrapedContact(
                                name=text,
                                website=full_url,
                                bio="WarriorPlus vendor",
                                source_category="digital_marketplace",
                            ))

        return contacts

    def _parse_card(self, card) -> ScrapedContact | None:
        title_el = card.find(["h2", "h3", "h4", "h5"]) or card.find(class_=re.compile(r"title|name", re.I))
        title = title_el.get_text(strip=True) if title_el else ""

        vendor = ""
        vendor_el = card.find(class_=re.compile(r"vendor|seller|author|by", re.I))
        if vendor_el:
            vendor = vendor_el.get_text(strip=True)
            vendor = re.sub(r"^(by|from|vendor:?)\s*", "", vendor, flags=re.I).strip()

        if not vendor:
            text = card.get_text()
            match = re.search(r"(?:by|from)\s+([A-Z][a-zA-Z\s.]+?)(?:\s*[|,\n]|$)", text)
            if match:
                vendor = match.group(1).strip()

        name = vendor or title
        if not name or len(name) < 2 or name.lower() in self._seen_vendors:
            return None
        self._seen_vendors.add(name.lower())

        website = ""
        for a in card.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http") and "warriorplus.com" not in href.lower():
                website = href
                break

        if not website:
            for a in card.find_all("a", href=True):
                href = a["href"]
                if "/o/" in href or "/jv" in href.lower():
                    website = href if href.startswith("http") else urljoin(self.BASE_URL, href)
                    break

        bio = f"WarriorPlus product: {title}" if title and title != name else "WarriorPlus vendor"

        return ScrapedContact(
            name=name,
            website=website,
            bio=bio,
            source_category="digital_marketplace",
        )

    def _parse_cells(self, cells) -> ScrapedContact | None:
        texts = [c.get_text(strip=True) for c in cells]
        name = texts[0] if texts else ""

        if not name or len(name) < 2 or name.lower() in self._seen_vendors:
            return None
        self._seen_vendors.add(name.lower())

        website = ""
        for cell in cells:
            for a in cell.find_all("a", href=True):
                href = a["href"]
                if href.startswith("http") and "warriorplus.com" not in href.lower():
                    website = href
                    break

        return ScrapedContact(
            name=name,
            website=website,
            bio="WarriorPlus vendor",
            source_category="digital_marketplace",
        )
