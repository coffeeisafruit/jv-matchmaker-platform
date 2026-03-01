"""
ClickBank marketplace scraper.

ClickBank's marketplace at clickbank.com/marketplace lists digital products
by category. Vendor info and affiliate/pitch pages can be extracted.

Focus: self-help, business/investing, health/fitness, e-business.

Estimated yield: 1,500-2,500 vendors
"""

from __future__ import annotations

import re
from typing import Iterator
from urllib.parse import urljoin, urlencode

from scripts.sourcing.base import BaseScraper, ScrapedContact


# ClickBank category IDs (from marketplace URL params)
CATEGORIES = [
    ("self-help", "selfhelp"),
    ("business-investing", "business"),
    ("health-fitness", "health"),
    ("e-business-e-marketing", "ebusiness"),
    ("education", "education"),
    ("spirituality-new-age", "spirituality"),
    ("parenting-families", "parenting"),
    ("green-products", "green"),
    ("computing-internet", "computing"),
]

MAX_PAGES = 30


class Scraper(BaseScraper):
    SOURCE_NAME = "clickbank"
    BASE_URL = "https://www.clickbank.com"
    REQUESTS_PER_MINUTE = 6

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_vendors: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield ClickBank marketplace category pages."""
        for cat_name, cat_slug in CATEGORIES:
            for page in range(1, MAX_PAGES + 1):
                yield f"{self.BASE_URL}/marketplace/{cat_slug}?page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse ClickBank marketplace listing page."""
        soup = self.parse_html(html)
        contacts = []

        # ClickBank product listings contain vendor info
        # Each product has: title, description, vendor nickname, stats, pitch page URL

        # Find product entries
        for entry in soup.find_all(class_=re.compile(r"product|listing|result|item", re.I)):
            contact = self._parse_product_entry(entry)
            if contact:
                contacts.append(contact)

        # Also try table rows
        for row in soup.find_all("tr"):
            contact = self._parse_product_row(row)
            if contact:
                contacts.append(contact)

        # Fallback: scan for pitch page links
        if not contacts:
            contacts = self._extract_vendor_links(soup)

        return contacts

    def _parse_product_entry(self, entry) -> ScrapedContact | None:
        """Parse a product listing div for vendor info."""
        text = entry.get_text(strip=True)

        # Product title
        title_el = entry.find(["h2", "h3", "h4"]) or entry.find(class_=re.compile(r"title|name", re.I))
        title = title_el.get_text(strip=True) if title_el else ""

        # Vendor name
        vendor_name = ""
        vendor_el = entry.find(class_=re.compile(r"vendor|seller|author", re.I))
        if vendor_el:
            vendor_name = vendor_el.get_text(strip=True)

        # Try "by X" pattern
        if not vendor_name:
            by_match = re.search(r"by\s+([A-Za-z][A-Za-z\s.]+)", text)
            if by_match:
                vendor_name = by_match.group(1).strip()

        # Vendor nickname (ClickBank ID)
        vendor_id = ""
        vendor_link = entry.find("a", href=re.compile(r"vendor|hop\.clickbank|affiliate", re.I))
        if vendor_link:
            href = vendor_link["href"]
            id_match = re.search(r"vendor=([a-zA-Z0-9]+)", href)
            if id_match:
                vendor_id = id_match.group(1)

        name = vendor_name or vendor_id or title
        if not name or len(name) < 2:
            return None

        name_key = name.lower().strip()
        if name_key in self._seen_vendors:
            return None
        self._seen_vendors.add(name_key)

        # Pitch page / product website
        website = ""
        for a in entry.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http") and "clickbank.com" not in href.lower():
                website = href
                break

        # Description
        desc_el = entry.find(class_=re.compile(r"desc|summary|blurb", re.I))
        desc = desc_el.get_text(strip=True)[:500] if desc_el else ""

        bio = f"ClickBank product: {title}" if title else "ClickBank vendor"
        if desc:
            bio = f"{bio} | {desc}"

        return ScrapedContact(
            name=name,
            website=website,
            bio=bio,
            source_category="affiliate_marketing",
        )

    def _parse_product_row(self, row) -> ScrapedContact | None:
        """Parse a table row for product/vendor info."""
        cells = row.find_all("td")
        if len(cells) < 2:
            return None

        texts = [c.get_text(strip=True) for c in cells]
        full_text = " ".join(texts)

        # Find vendor name
        name = ""
        for a in row.find_all("a"):
            text = a.get_text(strip=True)
            if text and len(text) > 2 and len(text) < 80:
                name = text
                break

        if not name:
            name = texts[0] if texts else ""

        if not name or name.lower() in self._seen_vendors:
            return None
        self._seen_vendors.add(name.lower())

        website = ""
        for a in row.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http") and "clickbank.com" not in href.lower():
                website = href
                break

        return ScrapedContact(
            name=name,
            website=website,
            bio="ClickBank vendor",
            source_category="affiliate_marketing",
        )

    def _extract_vendor_links(self, soup) -> list[ScrapedContact]:
        """Fallback: extract vendor info from all links on page."""
        contacts = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)

            # Look for hop links (affiliate pitch pages)
            if "hop.clickbank" in href.lower() or "vendor=" in href.lower():
                vendor_match = re.search(r"vendor=([a-zA-Z0-9]+)", href)
                vendor_id = vendor_match.group(1) if vendor_match else ""
                name = text if len(text) > 2 and len(text) < 80 else vendor_id

                if not name or name.lower() in self._seen_vendors:
                    continue
                self._seen_vendors.add(name.lower())

                contacts.append(ScrapedContact(
                    name=name,
                    website=href,
                    bio="ClickBank vendor",
                    source_category="affiliate_marketing",
                ))

        return contacts
