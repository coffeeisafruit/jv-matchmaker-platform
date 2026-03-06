"""
ClickBank marketplace scraper via GraphQL API.

ClickBank's marketplace at accounts.clickbank.com/marketplace.htm is a React
SPA backed by a public GraphQL endpoint at /graphql. This scraper queries that
endpoint directly, bypassing the need for JS rendering.

Returns ALL active products in a single query (typically ~1,400 vendors).
Each hit includes: vendor site ID, product title, description, product URL,
affiliate tools URL, support email, category, gravity, avg $/sale, and more.

Focus: digital products in self-help, health, e-business, education, etc.

Estimated yield: 1,200-1,500 unique vendors
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# GraphQL query that mirrors the React marketplace UI fields
MARKETPLACE_QUERY = """
query ($parameters: MarketplaceSearchParameters!) {
    marketplaceSearch(parameters: $parameters) {
        totalHits
        offset
        hits {
            site
            title
            description
            url
            affiliateToolsUrl
            affiliateSupportEmail
            marketplaceStats {
                category
                subCategory
                gravity
                initialDollarsPerSale
                averageDollarsPerSale
                activateDate
                rebill
                standard
                conversionRate
            }
        }
    }
}
""".strip()

# Categories we prioritize for JV-relevant vendors
JV_PRIORITY_CATEGORIES = {
    "Self-Help",
    "E-Business & E-Marketing",
    "Health & Fitness",
    "Education",
    "Spirituality, New Age & Alternative Beliefs",
    "Business/Investing",
}

# ClickBank category slugs for search filtering
CATEGORY_SLUGS = [
    "selfhelp",
    "ebusiness",
    "health",
    "education",
    "spirituality",
    "business",
    "parenting",
    "green",
    "computing",
    "home",
    "languages",
    "travel",
    "cooking",
    "games",
    "sports",
    "arts",
    "betting",
    "politics",
]


class Scraper(BaseScraper):
    """ClickBank marketplace scraper using the GraphQL API.

    Overrides run() to use JSON API calls instead of HTML scraping.
    The generate_urls() and scrape_page() methods are implemented as
    no-ops to satisfy the ABC, but the actual work is in run().
    """

    SOURCE_NAME = "clickbank"
    BASE_URL = "https://accounts.clickbank.com"
    GRAPHQL_URL = "https://accounts.clickbank.com/graphql"
    REQUESTS_PER_MINUTE = 6  # Conservative rate limit

    TYPICAL_ROLES = ["vendor", "product_creator", "affiliate_marketer"]
    TYPICAL_NICHES = ["digital_products", "info_products", "online_courses"]
    TYPICAL_OFFERINGS = [
        "digital_products", "courses", "ebooks", "supplements",
        "software", "coaching", "membership",
    ]

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_vendors: set[str] = set()
        # Override Accept header for GraphQL
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": "https://accounts.clickbank.com",
            "Referer": "https://accounts.clickbank.com/marketplace.htm",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        })

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- GraphQL API fetched via run() override."""
        yield self.GRAPHQL_URL

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used -- data parsed from JSON in run()."""
        return []

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Fetch all ClickBank marketplace products via GraphQL.

        The API returns all products in a single response (typically ~1,400),
        so no pagination is needed. We optionally filter by category for
        multiple queries to catch any category-specific results.
        """
        self.logger.info(
            "Starting %s scraper (max_contacts=%s)",
            self.SOURCE_NAME, max_contacts or "unlimited",
        )

        contacts_yielded = 0

        # Primary query: all products sorted by rank
        hits = self._fetch_marketplace(sort_field="rank")
        if hits:
            self.logger.info(
                "Fetched %d products from ClickBank marketplace", len(hits)
            )
            for hit in hits:
                contact = self._parse_hit(hit)
                if contact and contact.is_valid():
                    self.stats["contacts_valid"] += 1
                    contacts_yielded += 1
                    yield contact

                    if max_contacts and contacts_yielded >= max_contacts:
                        self.logger.info("Reached max_contacts=%d", max_contacts)
                        return

                self.stats["contacts_found"] += 1

        # Secondary queries by category to catch any products missed
        for cat_slug in CATEGORY_SLUGS:
            if self.rate_limiter:
                self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

            cat_hits = self._fetch_marketplace(
                sort_field="rank", category=cat_slug
            )
            if cat_hits:
                for hit in cat_hits:
                    contact = self._parse_hit(hit)
                    if contact and contact.is_valid():
                        self.stats["contacts_valid"] += 1
                        contacts_yielded += 1
                        yield contact

                        if max_contacts and contacts_yielded >= max_contacts:
                            self.logger.info(
                                "Reached max_contacts=%d", max_contacts
                            )
                            return

                    self.stats["contacts_found"] += 1

        self.logger.info("Scraper complete: %s", self.stats)

    def _fetch_marketplace(
        self,
        sort_field: str = "rank",
        offset: int = 0,
        category: str = "",
        include_keywords: str = "",
    ) -> list[dict]:
        """Execute a GraphQL marketplace search query.

        Args:
            sort_field: Sort field (rank, gravity, popularity, etc.)
            offset: Result offset for pagination
            category: Optional category slug to filter by
            include_keywords: Optional keyword filter

        Returns:
            List of hit dicts from the API response.
        """
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        params: dict = {"sortField": sort_field, "offset": offset}
        if category:
            params["category"] = category
        if include_keywords:
            params["includeKeywords"] = include_keywords

        payload = {
            "query": MARKETPLACE_QUERY,
            "variables": {"parameters": params},
        }

        try:
            resp = self.session.post(
                self.GRAPHQL_URL,
                json=payload,
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
            self.stats["pages_scraped"] += 1

            if "errors" in data:
                self.logger.warning(
                    "GraphQL errors: %s", data["errors"]
                )
                return []

            search_result = data.get("data", {}).get("marketplaceSearch", {})
            total = search_result.get("totalHits", 0)
            hits = search_result.get("hits", [])

            if category:
                self.logger.info(
                    "Category '%s': %d hits (total %d)",
                    category, len(hits), total,
                )
            else:
                self.logger.info(
                    "All products: %d hits (total %d)", len(hits), total
                )

            return hits

        except Exception as exc:
            self.logger.warning("GraphQL fetch failed: %s", exc)
            self.stats["errors"] += 1
            return []

    def _parse_hit(self, hit: dict) -> ScrapedContact | None:
        """Parse a single GraphQL hit into a ScrapedContact.

        Args:
            hit: Dict from the marketplaceSearch hits array.

        Returns:
            ScrapedContact or None if invalid/duplicate.
        """
        site_id = (hit.get("site") or "").strip()
        title = (hit.get("title") or "").strip()
        description = (hit.get("description") or "").strip()
        product_url = (hit.get("url") or "").strip()
        affiliate_tools_url = (hit.get("affiliateToolsUrl") or "").strip()
        support_email = (hit.get("affiliateSupportEmail") or "").strip()

        stats = hit.get("marketplaceStats") or {}
        category = (stats.get("category") or "").strip()
        sub_category = (stats.get("subCategory") or "").strip()
        gravity = stats.get("gravity", 0) or 0
        avg_sale = stats.get("averageDollarsPerSale", 0) or 0
        initial_sale = stats.get("initialDollarsPerSale", 0) or 0
        activate_date = (stats.get("activateDate") or "").strip()
        has_rebill = stats.get("rebill", False)
        conversion_rate = stats.get("conversionRate", -1)

        # Use site_id as canonical name (ClickBank vendor nickname)
        if not site_id:
            return None

        vendor_key = site_id.lower()
        if vendor_key in self._seen_vendors:
            return None
        self._seen_vendors.add(vendor_key)

        # Build a descriptive name from the vendor ID
        # ClickBank vendor IDs are often abbreviated (e.g., "MIKEGEARY1")
        name = site_id

        # Website: prefer vendor's product URL, then affiliate tools page
        website = product_url or affiliate_tools_url or ""

        # If no external URL, construct the ClickBank hop link
        if not website:
            website = f"https://hop.clickbank.net/?vendor={site_id}"

        # Build bio from available data
        bio_parts = [f"ClickBank vendor ({site_id})"]
        if title:
            bio_parts.append(f"Product: {title}")
        if category:
            cat_str = f"{category}/{sub_category}" if sub_category else category
            bio_parts.append(f"Category: {cat_str}")
        if gravity > 0:
            bio_parts.append(f"Gravity: {gravity:.1f}")
        if avg_sale > 0:
            bio_parts.append(f"Avg $/sale: ${avg_sale:.2f}")
        if has_rebill:
            bio_parts.append("Recurring billing")
        if description:
            bio_parts.append(description[:300])
        bio = " | ".join(bio_parts)

        # Revenue indicator from earnings data
        revenue_indicator = ""
        if avg_sale > 0 and gravity > 0:
            est_monthly = avg_sale * gravity * 0.5  # rough estimate
            if est_monthly > 50000:
                revenue_indicator = "high_volume"
            elif est_monthly > 10000:
                revenue_indicator = "mid_volume"
            elif est_monthly > 1000:
                revenue_indicator = "moderate_volume"
            else:
                revenue_indicator = "low_volume"

        # Categories string
        categories = category
        if sub_category and sub_category != "General":
            categories = f"{category}, {sub_category}"

        return ScrapedContact(
            name=name,
            email=support_email,
            website=website,
            bio=bio,
            source_category="affiliate_marketplace",
            categories=categories,
            rating=f"{gravity:.1f}" if gravity > 0 else "",
            pricing=f"${avg_sale:.2f}/sale" if avg_sale > 0 else "",
            revenue_indicator=revenue_indicator,
            join_date=activate_date,
            product_focus=title[:200] if title else "",
            raw_data={
                "site_id": site_id,
                "title": title,
                "gravity": gravity,
                "avg_sale": avg_sale,
                "initial_sale": initial_sale,
                "has_rebill": has_rebill,
                "conversion_rate": conversion_rate,
                "affiliate_tools_url": affiliate_tools_url,
                "category": category,
                "sub_category": sub_category,
            },
        )
