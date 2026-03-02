"""
Muncheye.com launch calendar scraper.

Muncheye lists upcoming and past product launches with vendor info and
JV page links. These are people ACTIVELY seeking JV partners — the
highest-signal source in the pipeline.

Parses two formats:
  1. div.item — the main listing format on /just-launched/ and homepage
     (vendor:product, price, commission, network, launch date)
  2. div.product_info — detail-page style table found on some category pages
     (vendor, product, JV page URL, launch date, etc.)

Estimated yield: 2,000-5,000 unique vendors (highly targeted)
"""

from __future__ import annotations

import re
from typing import Iterator
from urllib.parse import urljoin

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Muncheye pages to scrape — ordered by data richness
# /just-launched/ has 600+ items in a single page (no pagination).
# Many old category URLs now 404; only include confirmed-working pages.
LAUNCH_PAGES = [
    "/just-launched/",   # 600+ items — the main gold mine
    "/upcoming/",        # Upcoming launches (product_info format)
    "/",                 # Homepage — upcoming + recent
    "/internet-marketing/",
    "/affiliate-marketing/",
    "/self-help/",
    "/software/",
    "/social-media/",
    "/seo/",
]

# How many archive pages to go back per category (many 404, keep low)
MAX_ARCHIVE_PAGES = 3


class Scraper(BaseScraper):
    SOURCE_NAME = "muncheye"
    BASE_URL = "https://www.muncheye.com"
    REQUESTS_PER_MINUTE = 20
    RESPECT_ROBOTS_TXT = False  # Parser incorrectly blocks valid paths

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_vendors: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield launch listing page URLs."""
        # Main pages first
        for page_path in LAUNCH_PAGES:
            yield f"{self.BASE_URL}{page_path}"

        # Archive pagination for each category
        for page_path in LAUNCH_PAGES:
            for page_num in range(2, MAX_ARCHIVE_PAGES + 1):
                yield f"{self.BASE_URL}{page_path}page/{page_num}/"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a launch listing page for product/vendor entries.

        Handles both div.item (listing cards) and div.product_info (tables).
        """
        soup = self.parse_html(html)
        contacts = []

        # Format 1: div.item — the main listing format with rich data
        for item in soup.find_all("div", class_="item"):
            contact = self._parse_item(item, url)
            if contact:
                contacts.append(contact)

        # Format 2: div.product_info — table format on detail/category pages
        for info_div in soup.find_all("div", class_="product_info"):
            contact = self._parse_product_info(info_div)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_item(self, item, page_url: str) -> ScrapedContact | None:
        """Parse a div.item listing card.

        These contain: vendor:product link text, price, commission %,
        affiliate network, and launch date — all key JV partnership data.
        """
        link_tag = item.find("a", href=True)
        if not link_tag:
            return None

        link_text = link_tag.get_text(strip=True)
        href = link_tag.get("href", "")

        # Parse "Vendor: Product" format
        vendor_name = ""
        product_name = ""
        if ":" in link_text:
            parts = link_text.split(":", 1)
            vendor_name = parts[0].strip()
            product_name = parts[1].strip()
        else:
            vendor_name = link_text.strip()

        if not vendor_name or len(vendor_name) < 2:
            return None

        # Dedup by vendor+product (case-insensitive)
        dedup_key = f"{vendor_name}:{product_name}".lower().strip()
        if dedup_key in self._seen_vendors:
            return None
        self._seen_vendors.add(dedup_key)

        # Price and commission from span.item_details ("$47 at 50%")
        price = ""
        commission = ""
        details_span = item.find("span", class_="item_details")
        if details_span:
            details_text = details_span.get_text(strip=True)
            price_match = re.search(r"\$?([\d,.]+)", details_text)
            if price_match:
                price = f"${price_match.group(1)}"
            comm_match = re.search(r"(\d+)%", details_text)
            if comm_match:
                commission = f"{comm_match.group(1)}%"

        # Affiliate network from brand image title attribute
        network = ""
        brand_img = item.find("img", class_="brand")
        if brand_img:
            network = brand_img.get("title", "")

        # Launch date from schema.org meta tag
        launch_date = ""
        release_meta = item.find("meta", attrs={"itemprop": "releaseDate"})
        if release_meta:
            launch_date = release_meta.get("content", "")

        # Build detail URL
        detail_url = ""
        if href:
            if href.startswith("http"):
                detail_url = href
            elif href.startswith("/"):
                detail_url = f"{self.BASE_URL}{href}"
            else:
                detail_url = f"{self.BASE_URL}/{href}"

        # Build rich bio capturing all JV partnership data
        bio_parts = []
        if product_name:
            bio_parts.append(f"Product: {product_name}")
        if launch_date:
            bio_parts.append(f"Launch: {launch_date}")
        if price:
            bio_parts.append(f"Price: {price}")
        if commission:
            bio_parts.append(f"Commission: {commission}")
        if network:
            bio_parts.append(f"Network: {network}")
        bio = " | ".join(bio_parts) if bio_parts else "Listed on MunchEye launch calendar"

        return ScrapedContact(
            name=vendor_name,
            company=product_name,
            website=detail_url or page_url,  # MunchEye detail page as website
            bio=bio,
            source_url=detail_url or page_url,
            source_category="jv_launches",
        )

    def _parse_product_info(self, info_div) -> ScrapedContact | None:
        """Parse a Muncheye product_info div with table structure."""
        table = info_div.find("table")
        if not table:
            return None

        vendor_name = ""
        product_name = ""
        jv_page = ""
        launch_date = ""
        price = ""
        commission = ""
        network = ""

        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue

            label = cells[0].get_text(strip=True).rstrip(":").lower()
            value_cell = cells[1]
            value_text = value_cell.get_text(strip=True)

            if "vendor" in label:
                vendor_link = value_cell.find("a")
                vendor_name = vendor_link.get_text(strip=True) if vendor_link else value_text

            elif "product" in label:
                product_name = value_text

            elif "jv page" in label or "jv link" in label:
                jv_link = value_cell.find("a", href=True)
                if jv_link:
                    jv_page = jv_link["href"]
                elif value_text.startswith("http"):
                    jv_page = value_text

            elif "launch date" in label:
                launch_date = value_text

            elif "price" in label:
                price = value_text

            elif "commission" in label:
                commission = value_text

            elif "network" in label:
                net_link = value_cell.find("a")
                network = net_link.get_text(strip=True) if net_link else value_text

        if not vendor_name or len(vendor_name) < 2:
            return None

        # Dedup
        dedup_key = f"{vendor_name}:{product_name}".lower().strip()
        if dedup_key in self._seen_vendors:
            return None
        self._seen_vendors.add(dedup_key)

        # Build rich bio
        bio_parts = []
        if product_name:
            bio_parts.append(f"Product: {product_name}")
        if launch_date:
            bio_parts.append(f"Launch: {launch_date}")
        if price:
            bio_parts.append(f"Price: {price}")
        if commission:
            bio_parts.append(f"Commission: {commission}")
        if network:
            bio_parts.append(f"Network: {network}")
        if jv_page:
            bio_parts.append(f"JV Page: {jv_page}")
        bio = " | ".join(bio_parts) if bio_parts else "Active on Muncheye launch calendar"

        return ScrapedContact(
            name=vendor_name,
            company=product_name,
            website=jv_page if jv_page else "",
            bio=bio,
            source_url=self.BASE_URL,
            source_category="jv_launches",
        )
