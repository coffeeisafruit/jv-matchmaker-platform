"""
Muncheye.com launch calendar scraper.

Muncheye lists upcoming and past product launches with vendor info and
JV page links. These are people ACTIVELY seeking JV partners — the
highest-signal source in the pipeline.

Estimated yield: 500-1,000 unique vendors (highly targeted)
"""

from __future__ import annotations

import re
from typing import Iterator
from urllib.parse import urljoin

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Muncheye pages to scrape
LAUNCH_PAGES = [
    "/upcoming/",        # Upcoming launches
    "/just-launched/",   # Recently launched
    "/internet-marketing/",
    "/affiliate-marketing/",
    "/health-fitness/",
    "/self-help/",
    "/e-business-e-marketing/",
    "/software/",
    "/social-media/",
    "/seo/",
    "/coaching/",
]

# How many archive pages to go back per category
MAX_ARCHIVE_PAGES = 20


class Scraper(BaseScraper):
    SOURCE_NAME = "muncheye"
    BASE_URL = "https://www.muncheye.com"
    REQUESTS_PER_MINUTE = 6
    RESPECT_ROBOTS_TXT = False  # Parser incorrectly blocks valid paths

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_vendors: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield launch listing page URLs."""
        # Main pages
        for page_path in LAUNCH_PAGES:
            yield f"{self.BASE_URL}{page_path}"

        # Archive pagination for each category
        for page_path in LAUNCH_PAGES:
            for page_num in range(2, MAX_ARCHIVE_PAGES + 1):
                yield f"{self.BASE_URL}{page_path}page/{page_num}/"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a launch listing page for product/vendor entries."""
        soup = self.parse_html(html)
        contacts = []

        # Muncheye uses div.product_info with table structure
        # Structure: <div class="product_info"><table><tr><td><b>Vendor:</b></td><td>...</td></tr>...
        product_info_divs = soup.find_all("div", class_="product_info")

        for info_div in product_info_divs:
            contact = self._parse_product_info(info_div)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_product_info(self, info_div) -> ScrapedContact | None:
        """Parse a Muncheye product_info div with table structure."""
        # Find the table inside the div
        table = info_div.find("table")
        if not table:
            return None

        # Extract data from table rows
        vendor_name = ""
        product_name = ""
        jv_page = ""
        launch_date = ""

        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue

            label_cell = cells[0]
            value_cell = cells[1]

            label = label_cell.get_text(strip=True).lower()

            if "vendor" in label:
                # Extract vendor name from link or text
                vendor_link = value_cell.find("a")
                if vendor_link:
                    vendor_name = vendor_link.get_text(strip=True)
                else:
                    vendor_name = value_cell.get_text(strip=True)

            elif "product" in label:
                product_name = value_cell.get_text(strip=True)

            elif "jv page" in label or "jv link" in label:
                # Extract JV page URL
                jv_link = value_cell.find("a", href=True)
                if jv_link:
                    jv_page = jv_link["href"]
                else:
                    # Sometimes it's just plain text
                    text = value_cell.get_text(strip=True)
                    if text.startswith("http"):
                        jv_page = text

            elif "launch date" in label:
                launch_date = value_cell.get_text(strip=True)

        # Must have vendor name
        if not vendor_name or len(vendor_name) < 2:
            return None

        # Deduplicate by vendor name (case-insensitive)
        vendor_key = vendor_name.lower().strip()
        if vendor_key in self._seen_vendors:
            return None
        self._seen_vendors.add(vendor_key)

        # Build bio
        bio_parts = []
        if product_name:
            bio_parts.append(f"Product: {product_name}")
        if launch_date:
            bio_parts.append(f"Launch date: {launch_date}")
        bio = " | ".join(bio_parts) if bio_parts else "Active on Muncheye launch calendar"

        return ScrapedContact(
            name=vendor_name,
            website=jv_page if jv_page else "",
            bio=bio,
            source_url=f"{self.BASE_URL}",  # Don't have individual product URLs
            source_category="jv_launches",
        )
