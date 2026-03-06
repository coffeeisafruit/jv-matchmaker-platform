"""
WarriorPlus marketplace scraper via AJAX JSON API.

WarriorPlus uses a Vue.js SPA backed by a PHP AJAX endpoint at
/include/ajax/fetch-marketplace.php that returns JSON product listings.

The endpoint accepts query (q), vendor, and other filter params. It returns
25 results per request. Pagination params exist in the UI but do not function
for anonymous users. To maximize coverage, we issue searches across many
keywords and aggregate unique vendors.

Total marketplace size: ~4,800+ products.
Expected unique yield per run: 1,000-2,500 vendors (depending on keyword coverage).

Focus: internet marketing, PLR, AI tools, traffic, email marketing, courses.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Iterator, Optional
from urllib.parse import urlencode

from scripts.sourcing.base import BaseScraper, ScrapedContact


# AJAX endpoint that backs the Vue.js marketplace table
MARKETPLACE_AJAX_URL = "https://warriorplus.com/include/ajax/fetch-marketplace.php"

# Comprehensive keyword list for search-based coverage.
# Each search returns up to 25 unique products, so we need many keywords
# to cover the ~4,800 product catalog.
SEARCH_KEYWORDS = [
    # Core internet marketing
    "marketing", "affiliate", "traffic", "email", "seo",
    "social media", "copywriting", "funnel", "landing page", "lead",
    "conversion", "autoresponder", "list building", "opt-in", "squeeze",

    # Product types
    "plr", "course", "ebook", "software", "plugin", "tool",
    "template", "swipe", "done for you", "dfy", "agency",

    # AI and modern tools
    "ai", "chatgpt", "artificial intelligence", "automation", "bot",
    "gpt", "machine learning", "neural",

    # Content and platforms
    "youtube", "tiktok", "instagram", "facebook", "pinterest",
    "blog", "podcast", "video", "content", "wordpress",
    "shopify", "amazon", "etsy", "print on demand",

    # Monetization
    "passive income", "make money", "income", "profit", "commission",
    "recurring", "membership", "subscription", "coaching", "consulting",

    # Niches
    "health", "fitness", "weight loss", "diet", "keto",
    "crypto", "forex", "trading", "bitcoin", "stock",
    "real estate", "investing", "wealth",
    "dating", "relationship", "self help", "motivation",
    "spirituality", "manifestation", "meditation", "mindset",

    # Business
    "freelance", "outsource", "ecommerce", "dropshipping",
    "local business", "saas", "startup",
    "graphic", "design", "logo", "branding",
    "niche", "authority", "brand", "launch",

    # Technical
    "hosting", "domain", "website", "web", "app",
    "mobile", "chrome extension", "api",

    # Common product words
    "bundle", "pack", "kit", "system", "method",
    "secret", "formula", "blueprint", "masterclass",
    "pro", "elite", "premium", "ultimate", "mega",
    "instant", "easy", "simple", "fast", "quick",
    "complete", "guide", "tutorial", "training",
    "hack", "shortcut", "loophole",
    "viral", "trending", "new", "2026", "2025",
]

# Vendor-name prefix searches for additional coverage
VENDOR_PREFIXES = list("abcdefghijklmnopqrstuvwxyz")


class Scraper(BaseScraper):
    """WarriorPlus marketplace scraper using the AJAX JSON API.

    Overrides run() to fetch from the JSON endpoint instead of scraping HTML.
    Uses keyword-based search to work around the 25-result pagination limit.
    """

    SOURCE_NAME = "warriorplus"
    BASE_URL = "https://warriorplus.com"
    REQUESTS_PER_MINUTE = 10  # The AJAX endpoint is lightweight

    TYPICAL_ROLES = ["vendor", "product_creator", "affiliate_marketer"]
    TYPICAL_NICHES = [
        "internet_marketing", "digital_products", "make_money_online",
        "plr", "software",
    ]
    TYPICAL_OFFERINGS = [
        "digital_products", "software", "PLR", "courses",
        "done_for_you", "tools", "templates",
    ]

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_vendors: set[str] = set()
        self._seen_product_ids: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- AJAX endpoint called directly in run()."""
        yield MARKETPLACE_AJAX_URL

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used -- data parsed from JSON in run()."""
        return []

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Fetch WarriorPlus marketplace data via AJAX endpoint.

        Strategy:
        1. Fetch the default listing (newest products, 25 results)
        2. Search by each keyword to get different result sets
        3. Search by vendor name prefixes for additional coverage
        4. Deduplicate by vendor name across all results
        """
        self.logger.info(
            "Starting %s scraper (max_contacts=%s)",
            self.SOURCE_NAME, max_contacts or "unlimited",
        )

        contacts_yielded = 0
        searches_done = 0
        max_searches = max_pages if max_pages else 0  # 0 = unlimited

        # Phase 1: Default listing (no search)
        results = self._fetch_marketplace()
        if results:
            for contact in self._process_results(results):
                contacts_yielded += 1
                yield contact
                if max_contacts and contacts_yielded >= max_contacts:
                    self.logger.info("Reached max_contacts=%d", max_contacts)
                    return
        searches_done += 1

        # Phase 2: Keyword searches
        for keyword in SEARCH_KEYWORDS:
            if max_searches and searches_done >= max_searches:
                break

            results = self._fetch_marketplace(query=keyword)
            if results:
                for contact in self._process_results(results):
                    contacts_yielded += 1
                    yield contact
                    if max_contacts and contacts_yielded >= max_contacts:
                        self.logger.info("Reached max_contacts=%d", max_contacts)
                        return

            searches_done += 1

            if searches_done % 20 == 0:
                self.logger.info(
                    "Progress: %d searches, %d unique vendors found",
                    searches_done, self.stats["contacts_valid"],
                )

        # Phase 3: Vendor name searches (a-z prefix)
        for prefix in VENDOR_PREFIXES:
            if max_searches and searches_done >= max_searches:
                break

            results = self._fetch_marketplace(vendor=prefix)
            if results:
                for contact in self._process_results(results):
                    contacts_yielded += 1
                    yield contact
                    if max_contacts and contacts_yielded >= max_contacts:
                        self.logger.info("Reached max_contacts=%d", max_contacts)
                        return

            searches_done += 1

        self.logger.info(
            "Scraper complete after %d searches: %s",
            searches_done, self.stats,
        )

    def _fetch_marketplace(
        self,
        query: str = "",
        vendor: str = "",
        search_desc: int = 0,
    ) -> list[dict]:
        """Fetch products from the WarriorPlus AJAX endpoint.

        Args:
            query: Keyword search term (searches product titles)
            vendor: Vendor name search
            search_desc: Whether to search descriptions (0 or 1)

        Returns:
            List of product dicts from the 'data' key.
        """
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        params = {"limit": "25"}
        if query:
            params["q"] = query
        if vendor:
            params["vendor"] = vendor
        if search_desc:
            params["searchdesc"] = str(search_desc)

        url = f"{MARKETPLACE_AJAX_URL}?{urlencode(params)}"

        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            self.stats["pages_scraped"] += 1

            products = data.get("data", [])
            total = data.get("total", 0)

            if query or vendor:
                search_term = query or f"vendor:{vendor}"
                self.logger.debug(
                    "Search '%s': %d results (total %d)",
                    search_term, len(products), total,
                )

            return products

        except Exception as exc:
            self.logger.warning("Fetch failed for %s: %s", url, exc)
            self.stats["errors"] += 1
            return []

    def _process_results(
        self, results: list[dict]
    ) -> Iterator[ScrapedContact]:
        """Parse a batch of marketplace results into ScrapedContacts.

        Deduplicates by vendor name and product ID.
        """
        for product in results:
            contact = self._parse_product(product)
            if contact and contact.is_valid():
                self.stats["contacts_valid"] += 1
                yield contact
            self.stats["contacts_found"] += 1

    def _parse_product(self, product: dict) -> ScrapedContact | None:
        """Parse a single product dict from the AJAX response.

        Product structure:
        {
            "id": "90343",
            "offer_name": "AI Affiliate Content Machine...",
            "offer_date": "2026-03-05",
            "offer_link": "/o/view/rjzgq1/marketplace_home",
            "vendor_name": "Lord5",
            "vendor_link": "/member/Lord5",
            "is_favorite": false,
            "rowclass": "",
            "alert": false,
            "purchase_date": null,
            "dotd": false  # deal of the day
        }
        """
        product_id = str(product.get("id") or "").strip()
        offer_name = (product.get("offer_name") or "").strip()
        offer_date = (product.get("offer_date") or "").strip()
        offer_link = (product.get("offer_link") or "").strip()
        vendor_name = (product.get("vendor_name") or "").strip()
        vendor_link = (product.get("vendor_link") or "").strip()
        is_dotd = product.get("dotd", False)

        if not vendor_name:
            return None

        # Dedup by vendor name
        vendor_key = vendor_name.lower()
        if vendor_key in self._seen_vendors:
            return None
        self._seen_vendors.add(vendor_key)

        # Also track product IDs for stats
        if product_id:
            self._seen_product_ids.add(product_id)

        # Build full URLs from relative paths
        website = ""
        if offer_link:
            website = (
                offer_link
                if offer_link.startswith("http")
                else f"{self.BASE_URL}{offer_link}"
            )

        vendor_url = ""
        if vendor_link:
            vendor_url = (
                vendor_link
                if vendor_link.startswith("http")
                else f"{self.BASE_URL}{vendor_link}"
            )

        # Build bio
        bio_parts = [f"WarriorPlus vendor ({vendor_name})"]
        if offer_name:
            bio_parts.append(f"Product: {offer_name}")
        if offer_date:
            bio_parts.append(f"Listed: {offer_date}")
        if is_dotd:
            bio_parts.append("Featured Deal of the Day")
        bio = " | ".join(bio_parts)

        contact = ScrapedContact(
            name=vendor_name,
            website=website or vendor_url,
            bio=bio,
            source_category="digital_marketplace",
            source_platform=self.SOURCE_NAME,
            source_url=f"{self.BASE_URL}/marketplace",
            scraped_at=datetime.now().isoformat(),
            product_focus=offer_name[:200] if offer_name else "",
            join_date=offer_date,
            raw_data={
                "product_id": product_id,
                "offer_name": offer_name,
                "offer_date": offer_date,
                "offer_link": offer_link,
                "vendor_name": vendor_name,
                "vendor_link": vendor_link,
                "is_dotd": is_dotd,
            },
        )

        # Clean email before returning
        contact.email = contact.clean_email()

        return contact
