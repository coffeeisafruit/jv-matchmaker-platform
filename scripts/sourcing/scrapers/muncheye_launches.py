"""
MunchEye deep launch data scraper.

Unlike the basic muncheye scraper that only parses listing pages, this scraper
follows every launch link to its detail page and extracts the full structured
data: vendor, product, launch date/time, front-end price, commission %,
JV page URL, affiliate network, and niche.

The JV Page URL is the most valuable field -- it links to the vendor's
affiliate/partnership signup page, which is exactly what a JV matchmaker needs.

Strategy:
  1. Fetch a listing page (homepage or archive page via ?startDate=)
  2. Parse all div.item entries to discover launch detail links
  3. Visit each detail link one by one, yielding contacts as we go
  4. Move to the next listing page and repeat

This uses a custom run() override so that max_contacts stops immediately
rather than waiting for an entire listing page of detail fetches to finish.

Estimated yield: 600+ launches from homepage + thousands from archives
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# How many archive start-date pages to traverse (each covers ~6 months)
MAX_ARCHIVE_PAGES = 10

# How many paginated sub-pages within each start-date view
MAX_SUB_PAGES = 5

# Non-launch slugs to skip
SKIP_SLUGS = frozenset({
    "submit-launch", "events", "evergreens", "page",
    "category", "author", "niche", "affiliate",
    "wp-content", "feed", "xmlrpc", "",
})


class Scraper(BaseScraper):
    SOURCE_NAME = "muncheye_launches"
    BASE_URL = "https://www.muncheye.com"
    REQUESTS_PER_MINUTE = 60  # 1 req/sec
    RESPECT_ROBOTS_TXT = False

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_slugs: set[str] = set()

    # ------------------------------------------------------------------
    # URL generation (listing pages only)
    # ------------------------------------------------------------------

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield listing page URLs.

        The homepage shows upcoming + recent launches.  The ``?startDate=``
        parameter pages backwards through history, and ``page/N`` paginates
        within each date window.
        """
        # Current / upcoming launches (homepage)
        yield f"{self.BASE_URL}/"

        # Walk backwards through history in ~6-month steps
        now = datetime.now()
        for i in range(MAX_ARCHIVE_PAGES):
            start = now - timedelta(days=180 * (i + 1))
            start_str = start.strftime("%Y-%m-%d")
            yield f"{self.BASE_URL}/?startDate={start_str}"
            for page_num in range(2, MAX_SUB_PAGES + 1):
                yield f"{self.BASE_URL}/page/{page_num}?startDate={start_str}"

    # ------------------------------------------------------------------
    # scrape_page is required by the ABC but unused in our custom run()
    # ------------------------------------------------------------------

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used -- see run() override. Required by BaseScraper ABC."""
        return []

    # ------------------------------------------------------------------
    # Custom run() -- yields contacts one at a time from detail pages
    # ------------------------------------------------------------------

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Fetch listing pages, discover launch links, visit each detail page.

        Yields one ScrapedContact per detail page so that max_contacts can
        stop early without fetching hundreds of unnecessary pages.
        """
        contacts_yielded = 0
        listing_pages_done = 0
        start_from = (checkpoint or {}).get("last_url")
        past_checkpoint = start_from is None

        self.logger.info(
            "Starting %s scraper (max_pages=%s, max_contacts=%s, checkpoint=%s)",
            self.SOURCE_NAME,
            max_pages or "unlimited",
            max_contacts or "unlimited",
            start_from or "none",
        )

        for listing_url in self.generate_urls():
            # Checkpoint handling
            if not past_checkpoint:
                if listing_url == start_from:
                    past_checkpoint = True
                continue

            # Fetch listing page
            listing_html = self.fetch_page(listing_url)
            if not listing_html:
                continue

            # Parse listing page to discover launch detail links
            launch_items = self._parse_listing(listing_url, listing_html)
            self.logger.info(
                "Found %d new launch items on %s", len(launch_items), listing_url
            )

            # Visit each detail page and yield contacts
            for detail_url, listing_data in launch_items:
                contact = self._fetch_detail_page(detail_url, listing_data)
                if not contact:
                    continue

                # Apply same post-processing as BaseScraper.run()
                contact.source_platform = self.SOURCE_NAME
                contact.source_url = detail_url
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

            listing_pages_done += 1
            if listing_pages_done % 5 == 0:
                self.logger.info(
                    "Progress: %d listing pages, %d valid contacts",
                    listing_pages_done, self.stats["contacts_valid"],
                )

            if max_pages and listing_pages_done >= max_pages:
                self.logger.info("Reached max_pages=%d", max_pages)
                break

        self.logger.info("Scraper complete: %s", self.stats)

    # ------------------------------------------------------------------
    # Listing page parser: extract launch links + preview data
    # ------------------------------------------------------------------

    def _parse_listing(
        self, listing_url: str, html: str
    ) -> list[tuple[str, dict]]:
        """Parse a listing page and return (detail_url, listing_data) pairs.

        Only returns items with slugs not yet seen (deduplication).
        """
        soup = self.parse_html(html)
        results: list[tuple[str, dict]] = []

        for item in soup.find_all("div", class_="item"):
            link_tag = item.find("a", href=True)
            if not link_tag:
                continue

            href = link_tag.get("href", "")

            # Skip external links
            if href.startswith("http") and "muncheye.com" not in href:
                continue

            # Normalize to absolute URL
            if href.startswith("/"):
                detail_url = f"{self.BASE_URL}{href}"
            elif not href.startswith("http"):
                detail_url = f"{self.BASE_URL}/{href}"
            else:
                detail_url = href

            # Extract slug for dedup
            slug = href.rstrip("/").split("/")[-1].split("?")[0].lower()
            if slug in SKIP_SLUGS or slug in self._seen_slugs:
                continue
            self._seen_slugs.add(slug)

            # Pre-extract data from listing (fallback)
            listing_data = self._extract_listing_data(item)
            results.append((detail_url, listing_data))

        return results

    def _extract_listing_data(self, item) -> dict:
        """Pull data available directly from the listing item div.

        Serves as fallback when the detail page fetch fails.
        """
        data: dict = {
            "vendor": "",
            "product": "",
            "price": "",
            "commission": "",
            "network": "",
            "launch_date": "",
        }

        # Link text is "Vendor: Product"
        link_tag = item.find("a", href=True)
        if link_tag:
            text = link_tag.get_text(strip=True)
            if ":" in text:
                parts = text.split(":", 1)
                data["vendor"] = parts[0].strip()
                data["product"] = parts[1].strip()
            else:
                data["product"] = text

        # Price and commission from span.item_details ("47 at 50%")
        details_span = item.find("span", class_="item_details")
        if details_span:
            details_text = details_span.get_text(strip=True)
            price_match = re.search(r"\$?([\d,.]+)", details_text)
            if price_match:
                data["price"] = f"${price_match.group(1)}"
            comm_match = re.search(r"(\d+)%", details_text)
            if comm_match:
                data["commission"] = f"{comm_match.group(1)}%"

        # Network from brand image title attribute
        brand_img = item.find("img", class_="brand")
        if brand_img:
            data["network"] = brand_img.get("title", "")

        # Release date from schema.org meta tag
        release_meta = item.find("meta", attrs={"itemprop": "releaseDate"})
        if release_meta:
            data["launch_date"] = release_meta.get("content", "")

        return data

    # ------------------------------------------------------------------
    # Detail page fetch and parse
    # ------------------------------------------------------------------

    def _fetch_detail_page(
        self, detail_url: str, listing_data: dict
    ) -> Optional[ScrapedContact]:
        """Fetch a launch detail page and extract structured fields."""
        html = self.fetch_page(detail_url)
        if not html:
            return self._contact_from_listing_data(listing_data, detail_url)

        soup = self.parse_html(html)
        product_info = soup.find("div", class_="product_info")

        if not product_info:
            return self._contact_from_listing_data(listing_data, detail_url)

        table = product_info.find("table")
        if not table:
            return self._contact_from_listing_data(listing_data, detail_url)

        # Parse the structured table
        fields: dict[str, str] = {}
        jv_page_url = ""

        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue

            label = cells[0].get_text(strip=True).rstrip(":").lower()
            value_cell = cells[1]
            value_text = value_cell.get_text(strip=True)

            if "vendor" in label:
                fields["vendor"] = value_text
            elif "product" in label:
                fields["product"] = value_text
            elif "launch date" in label:
                fields["launch_date"] = value_text
            elif "launch time" in label:
                fields["launch_time"] = value_text
            elif "front-end price" in label or "price" in label:
                fields["price"] = value_text
            elif "commission" in label:
                fields["commission"] = value_text
            elif "jv page" in label or "jv link" in label:
                jv_link = value_cell.find("a", href=True)
                if jv_link:
                    jv_page_url = jv_link["href"]
                elif value_text.startswith("http"):
                    jv_page_url = value_text
                fields["jv_page"] = jv_page_url or value_text
            elif "affiliate network" in label or "network" in label:
                net_link = value_cell.find("a")
                fields["network"] = (
                    net_link.get_text(strip=True) if net_link else value_text
                )
            elif "niche" in label:
                niche_link = value_cell.find("a")
                fields["niche"] = (
                    niche_link.get_text(strip=True) if niche_link else value_text
                )

        # Merge: detail page fields take priority over listing data
        vendor = fields.get("vendor") or listing_data.get("vendor", "")
        product = fields.get("product") or listing_data.get("product", "")
        launch_date = fields.get("launch_date") or listing_data.get("launch_date", "")
        launch_time = fields.get("launch_time", "")
        price = fields.get("price") or listing_data.get("price", "")
        commission = fields.get("commission") or listing_data.get("commission", "")
        network = fields.get("network") or listing_data.get("network", "")
        niche = fields.get("niche", "")
        jv_page = jv_page_url or ""

        # Normalize price -- add $ if missing
        if price and not price.startswith("$") and re.match(r"[\d,.]", price):
            price = f"${price}"

        if not vendor:
            return None

        # Extract product description from paragraphs after product_info
        description = self._extract_description(product_info)

        # Build rich bio string
        bio_parts = []
        if launch_date:
            bio_parts.append(f"Launch: {launch_date}")
        if launch_time:
            bio_parts.append(f"Time: {launch_time}")
        if price:
            bio_parts.append(f"Price: {price}")
        if commission:
            bio_parts.append(f"Commission: {commission}")
        if network:
            bio_parts.append(f"Network: {network}")
        if niche:
            bio_parts.append(f"Niche: {niche}")
        if jv_page:
            bio_parts.append(f"JV Page: {jv_page}")
        if description:
            bio_parts.append(f"Description: {description}")

        bio = " | ".join(bio_parts) if bio_parts else "Listed on MunchEye launch calendar"

        return ScrapedContact(
            name=vendor,
            company=product,
            website=jv_page,
            email="",
            linkedin="",
            phone="",
            bio=bio,
            source_platform="muncheye_launches",
            source_url=detail_url,
            source_category="jv_launches",
        )

    def _extract_description(self, product_info) -> str:
        """Extract a brief product description from content after the info table."""
        desc_parts = []
        for sibling in product_info.find_next_siblings():
            if sibling.name in ("p", "div"):
                text = sibling.get_text(strip=True)
                if text and len(text) > 20:
                    # Skip boilerplate / JV page link lines
                    if text.startswith("http") or "checkout the JV PAGE" in text:
                        continue
                    # Skip social share / footer content
                    if "Share this" in text or "Related Posts" in text:
                        break
                    desc_parts.append(text)
                    if len(" ".join(desc_parts)) > 300:
                        break
            # Stop at boundary elements
            if sibling.name in ("hr", "footer", "section"):
                break
            if len(desc_parts) >= 3:
                break

        full_desc = " ".join(desc_parts)
        if len(full_desc) > 500:
            full_desc = full_desc[:497] + "..."
        return full_desc

    def _contact_from_listing_data(
        self, listing_data: dict, detail_url: str
    ) -> Optional[ScrapedContact]:
        """Build a contact from listing-only data when detail page is unavailable."""
        vendor = listing_data.get("vendor", "")
        if not vendor:
            return None

        product = listing_data.get("product", "")
        price = listing_data.get("price", "")
        commission = listing_data.get("commission", "")
        network = listing_data.get("network", "")
        launch_date = listing_data.get("launch_date", "")

        bio_parts = []
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
            name=vendor,
            company=product,
            website="",  # No JV page from listing alone
            email="",
            linkedin="",
            phone="",
            bio=bio,
            source_platform="muncheye_launches",
            source_url=detail_url,
            source_category="jv_launches",
        )
