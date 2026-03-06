"""
Gumroad creator directory scraper.

Scrapes Gumroad's discover pages by taxonomy category. Gumroad uses
Inertia.js which embeds full page data as JSON in a `data-page` HTML
attribute — no browser/JS rendering needed, plain requests works.

Focus taxonomies: self-improvement, business-and-money, education,
writing-and-publishing, fitness-and-health.

Estimated yield: 5,000-10,000 unique creators across all categories.
"""

from __future__ import annotations

import html
import json
import logging
import re
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# JV-relevant taxonomy slugs on Gumroad discover
TAXONOMY_CATEGORIES = [
    "self-improvement",
    "business-and-money",
    "education",
    "writing-and-publishing",
    "fitness-and-health",
]

PRODUCTS_PER_PAGE = 36

logger = logging.getLogger(__name__)


class Scraper(BaseScraper):
    """Gumroad discover scraper using Inertia.js data extraction."""

    SOURCE_NAME = "gumroad"
    BASE_URL = "https://gumroad.com"
    REQUESTS_PER_MINUTE = 20

    TYPICAL_ROLES = ["creator", "course_creator", "coach", "author"]
    TYPICAL_NICHES = [
        "self-improvement", "business", "education",
        "writing", "fitness", "health",
    ]
    TYPICAL_OFFERINGS = [
        "digital product", "online course", "ebook",
        "template", "coaching", "workshop",
    ]

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_seller_ids: set[str] = set()

    # ------------------------------------------------------------------
    # Abstract method implementations
    # ------------------------------------------------------------------

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used — run() handles pagination directly."""
        return iter([])

    def scrape_page(self, url: str, html_text: str) -> list[ScrapedContact]:
        """Not used — run() handles extraction directly."""
        return []

    # ------------------------------------------------------------------
    # Inertia.js data extraction
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_inertia_data(html_text: str) -> Optional[dict]:
        """Extract the Inertia.js JSON payload from the data-page attribute.

        Gumroad embeds all page data in an HTML element like:
            <div id="app" data-page="{&quot;props&quot;: ...}">

        Returns the parsed dict, or None on failure.
        """
        match = re.search(r'data-page="([^"]+)"', html_text)
        if not match:
            return None
        raw = match.group(1)
        # Decode HTML entities: &quot; -> ", &amp; -> &, etc.
        decoded = html.unescape(raw)
        try:
            return json.loads(decoded)
        except (json.JSONDecodeError, ValueError):
            return None

    # ------------------------------------------------------------------
    # Main run loop (overrides BaseScraper.run)
    # ------------------------------------------------------------------

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Iterate taxonomy categories, paginate, extract unique sellers.

        Args:
            max_pages: Max number of discover pages to fetch (0 = unlimited).
            max_contacts: Max contacts to yield (0 = unlimited).
            checkpoint: Optional dict with 'taxonomy' and 'offset' to resume.

        Yields:
            ScrapedContact for each unique Gumroad seller.
        """
        pages_fetched = 0
        contacts_yielded = 0

        # Resume support
        resume_taxonomy = (checkpoint or {}).get("taxonomy")
        resume_offset = (checkpoint or {}).get("offset", 0)
        past_checkpoint = resume_taxonomy is None

        self.logger.info(
            "Starting Gumroad scraper (max_pages=%s, max_contacts=%s, checkpoint=%s)",
            max_pages or "unlimited",
            max_contacts or "unlimited",
            checkpoint or "none",
        )

        for taxonomy in TAXONOMY_CATEGORIES:
            # Handle checkpoint resume
            if not past_checkpoint:
                if taxonomy == resume_taxonomy:
                    past_checkpoint = True
                else:
                    continue

            offset = resume_offset if (taxonomy == resume_taxonomy) else 0
            resume_offset = 0  # Only apply to the first taxonomy after resume

            self.logger.info("Scraping taxonomy: %s (starting at offset %d)", taxonomy, offset)

            while True:
                url = f"{self.BASE_URL}/discover?taxonomy={taxonomy}&from={offset}"
                html_text = self.fetch_page(url)
                if not html_text:
                    self.logger.warning("Failed to fetch %s, skipping", url)
                    break

                pages_fetched += 1

                data = self._parse_inertia_data(html_text)
                if not data:
                    self.logger.warning("No Inertia.js data found on %s", url)
                    break

                search_results = data.get("props", {}).get("search_results")
                if not search_results:
                    self.logger.warning("No search_results in Inertia data for %s", url)
                    break

                total = search_results.get("total", 0)
                products = search_results.get("products", [])

                if not products:
                    self.logger.debug("No products on page %s, moving to next taxonomy", url)
                    break

                for product in products:
                    seller = product.get("seller") or {}
                    seller_id = str(seller.get("id", ""))

                    if not seller_id or seller_id in self._seen_seller_ids:
                        continue
                    self._seen_seller_ids.add(seller_id)

                    contact = self._seller_to_contact(seller, product, taxonomy)
                    if not contact or not contact.is_valid():
                        continue

                    contact.source_platform = self.SOURCE_NAME
                    contact.source_url = url
                    contact.scraped_at = datetime.now().isoformat()
                    contact.email = contact.clean_email()

                    self.stats["contacts_found"] += 1
                    self.stats["contacts_valid"] += 1
                    contacts_yielded += 1
                    yield contact

                    if max_contacts and contacts_yielded >= max_contacts:
                        self.logger.info("Reached max_contacts=%d", max_contacts)
                        return

                # Log progress
                if pages_fetched % 10 == 0:
                    self.logger.info(
                        "Progress: %d pages fetched, %d unique sellers yielded, "
                        "%d total sellers seen",
                        pages_fetched, contacts_yielded, len(self._seen_seller_ids),
                    )

                if max_pages and pages_fetched >= max_pages:
                    self.logger.info("Reached max_pages=%d", max_pages)
                    return

                # Advance pagination
                offset += PRODUCTS_PER_PAGE
                if offset >= total:
                    self.logger.info(
                        "Finished taxonomy %s: %d total products", taxonomy, total
                    )
                    break

        self.logger.info("Gumroad scraper complete: %s", self.stats)

    # ------------------------------------------------------------------
    # Seller -> ScrapedContact mapping
    # ------------------------------------------------------------------

    def _seller_to_contact(
        self,
        seller: dict,
        product: dict,
        taxonomy: str,
    ) -> Optional[ScrapedContact]:
        """Convert a Gumroad seller + product record to a ScrapedContact.

        Args:
            seller: The seller dict from the product listing.
            product: The full product dict (for ratings, price, etc.).
            taxonomy: The taxonomy slug this product was found under.

        Returns:
            ScrapedContact or None if seller name is missing.
        """
        name = (seller.get("name") or "").strip()
        if not name or len(name) < 2:
            return None

        profile_url = (seller.get("profile_url") or "").strip()
        avatar_url = (seller.get("avatar_url") or "").strip()

        # Build bio from product info
        bio_parts = []
        product_name = (product.get("name") or "").strip()
        if product_name:
            bio_parts.append(f"Gumroad product: {product_name}")

        product_desc = (product.get("description") or "").strip()
        if product_desc:
            bio_parts.append(product_desc[:500])

        if seller.get("is_verified"):
            bio_parts.append("Verified Gumroad creator")

        bio = " | ".join(bio_parts)

        # Ratings
        ratings = product.get("ratings") or {}
        rating_avg = str(ratings.get("average", "")) if ratings.get("average") else ""
        rating_count = str(ratings.get("count", "")) if ratings.get("count") else ""

        # Price
        price_cents = product.get("price_cents")
        pricing = ""
        if price_cents is not None and price_cents > 0:
            pricing = f"${price_cents / 100:.2f}"

        # Revenue indicator from ratings count
        revenue_indicator = ""
        count = ratings.get("count", 0) or 0
        if count >= 500:
            revenue_indicator = "500+ sales (high volume)"
        elif count >= 100:
            revenue_indicator = "100+ sales (established)"
        elif count >= 20:
            revenue_indicator = "20+ sales (growing)"

        return ScrapedContact(
            name=name,
            website=profile_url or "",
            company=name,
            bio=bio[:2000],
            rating=rating_avg,
            review_count=rating_count,
            pricing=pricing,
            categories=taxonomy.replace("-", " "),
            source_category="digital_creators",
            product_focus=(product.get("native_type") or "digital"),
            revenue_indicator=revenue_indicator,
        )
