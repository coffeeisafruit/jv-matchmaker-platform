"""
Salesforce AppExchange scraper.

Salesforce AppExchange is the largest enterprise SaaS marketplace with
5,000+ apps and consulting partners. The listing pages use a JSON API
at appexchange.salesforce.com for search results.

Uses the AppExchange search API which returns structured JSON with:
- App/partner name, description, company info
- Website, ratings, review count
- Category tags, pricing info
- Installation count

Estimated yield: 3,000-5,000 SaaS companies and consulting partners
"""

from __future__ import annotations

import json
import re
from typing import Iterator, Optional
from urllib.parse import urlencode, quote_plus

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Search categories covering the full AppExchange taxonomy
SEARCH_CATEGORIES = [
    "Sales", "Service", "Marketing", "Commerce", "Analytics",
    "Integration", "Productivity", "Finance", "HR", "IT",
    "Collaboration", "Data", "Security", "Communication",
    "ERP", "Project Management", "Document Management",
    "Customer Service", "CRM", "Automation",
    "AI", "Machine Learning", "DevOps", "Healthcare",
    "Education", "Real Estate", "Manufacturing", "Nonprofit",
]

# General search terms for broader coverage
SEARCH_TERMS = [
    "app", "integration", "connector", "automation",
    "dashboard", "reporting", "management", "platform",
    "workflow", "email", "payment", "accounting",
    "scheduling", "survey", "support", "chat",
]


class Scraper(BaseScraper):
    """Salesforce AppExchange scraper.

    Uses the AppExchange search/listing pages to extract app and
    partner company data. Tries the JSON API first, falls back to
    HTML parsing of listing pages.
    """

    SOURCE_NAME = "salesforce_appexchange"
    BASE_URL = "https://appexchange.salesforce.com"
    REQUESTS_PER_MINUTE = 10

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_slugs: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield AppExchange listing page URLs.

        Uses the public search/browse pages which render server-side
        with embedded JSON data for each listing.
        """
        # Browse by category pages
        for category in SEARCH_CATEGORIES:
            for page_num in range(1, 11):  # Up to 10 pages per category
                yield (
                    f"{self.BASE_URL}/appxSearchKeywordResults"
                    f"?searchKeyword={quote_plus(category)}"
                    f"&pageNumber={page_num}"
                )

        # General search terms for additional coverage
        for term in SEARCH_TERMS:
            for page_num in range(1, 6):
                yield (
                    f"{self.BASE_URL}/appxSearchKeywordResults"
                    f"?searchKeyword={quote_plus(term)}"
                    f"&pageNumber={page_num}"
                )

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse AppExchange search results page.

        Tries to extract embedded JSON data first, then falls back
        to HTML parsing of listing cards.
        """
        contacts = []

        # Try to find embedded JSON data (React/Lightning hydration)
        json_data = self._extract_embedded_json(html)
        if json_data:
            contacts = self._parse_json_results(json_data, url)
            if contacts:
                return contacts

        # Fallback: parse HTML listing cards
        return self._parse_html_listings(html, url)

    def _extract_embedded_json(self, html: str) -> Optional[list]:
        """Try to extract embedded listing data from the page."""
        # Look for JSON data in script tags or data attributes
        patterns = [
            r'window\.__PRELOADED_STATE__\s*=\s*({.*?});',
            r'<script[^>]*>var\s+appxData\s*=\s*({.*?});</script>',
            r'"listings"\s*:\s*(\[.*?\])\s*[,}]',
            r'"results"\s*:\s*(\[.*?\])\s*[,}]',
        ]
        for pattern in patterns:
            match = re.search(pattern, html, re.DOTALL)
            if match:
                try:
                    data = json.loads(match.group(1))
                    if isinstance(data, dict):
                        # Look for listings array within the data
                        for key in ("listings", "results", "apps", "items"):
                            if key in data and isinstance(data[key], list):
                                return data[key]
                        return [data]
                    return data if isinstance(data, list) else None
                except json.JSONDecodeError:
                    continue
        return None

    def _parse_json_results(self, results: list, url: str) -> list[ScrapedContact]:
        """Parse JSON listing objects into ScrapedContacts."""
        contacts = []
        for item in results:
            if not isinstance(item, dict):
                continue
            contact = self._parse_listing_dict(item, url)
            if contact:
                contacts.append(contact)
        return contacts

    def _parse_listing_dict(self, item: dict, url: str) -> Optional[ScrapedContact]:
        """Parse a single listing dict into a ScrapedContact."""
        name = (item.get("name") or item.get("title") or item.get("appName") or "").strip()
        if not name:
            return None

        slug = (item.get("slug") or item.get("urlKey") or item.get("id") or "").strip()
        if slug in self._seen_slugs:
            return None
        self._seen_slugs.add(slug or name.lower().replace(" ", "-"))

        company = (item.get("companyName") or item.get("company") or item.get("providerName") or name).strip()
        website = (item.get("website") or item.get("companyUrl") or item.get("appUrl") or "").strip()
        description = (item.get("description") or item.get("summary") or item.get("tagLine") or "").strip()

        # Rating and reviews
        rating = item.get("rating") or item.get("averageRating") or 0
        review_count = item.get("reviewCount") or item.get("numReviews") or 0

        # Categories
        categories = item.get("categories") or item.get("tags") or []
        if isinstance(categories, list):
            cat_names = []
            for c in categories:
                if isinstance(c, str):
                    cat_names.append(c)
                elif isinstance(c, dict):
                    cat_names.append((c.get("name") or c.get("label") or "").strip())
            categories = [c for c in cat_names if c]

        # Pricing
        pricing = (item.get("pricing") or item.get("priceType") or "").strip()

        listing_url = f"{self.BASE_URL}/appxListingDetail?listingId={slug}" if slug else url

        # Build bio
        bio_parts = []
        if description:
            bio_parts.append(description[:500])
        if categories:
            bio_parts.append(f"Categories: {', '.join(categories[:5])}")
        if rating:
            bio_parts.append(f"Rating: {rating}/5")
        if review_count:
            bio_parts.append(f"{review_count} reviews")
        if pricing:
            bio_parts.append(f"Pricing: {pricing}")
        if not bio_parts:
            bio_parts.append("Salesforce AppExchange listing")

        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=company,
            company=company,
            website=website,
            bio=bio,
            source_url=listing_url,
            source_category="saas_marketplace",
            raw_data={
                "app_name": name,
                "slug": slug,
                "categories": categories,
                "rating": rating,
                "review_count": review_count,
                "pricing": pricing,
                "platform": "salesforce_appexchange",
            },
        )

    def _parse_html_listings(self, html: str, url: str) -> list[ScrapedContact]:
        """Fallback HTML parsing for AppExchange listing cards."""
        soup = self.parse_html(html)
        contacts = []

        # Look for listing cards with various possible selectors
        cards = (
            soup.select(".appx-listing-card")
            or soup.select("[class*='ListingCard']")
            or soup.select("[class*='listing-tile']")
            or soup.select("article[class*='app']")
            or soup.select(".search-result-item")
        )

        for card in cards:
            name_el = (
                card.select_one("h2")
                or card.select_one("h3")
                or card.select_one("[class*='title']")
                or card.select_one("[class*='name']")
            )
            if not name_el:
                continue

            name = name_el.get_text(strip=True)
            if not name or len(name) < 2:
                continue

            if name in self._seen_slugs:
                continue
            self._seen_slugs.add(name)

            # Company/provider name
            company_el = card.select_one("[class*='company']") or card.select_one("[class*='provider']")
            company = company_el.get_text(strip=True) if company_el else name

            # Link to listing detail
            link_el = card.select_one("a[href]")
            listing_url = ""
            if link_el:
                href = link_el.get("href", "")
                if href.startswith("/"):
                    listing_url = f"{self.BASE_URL}{href}"
                elif href.startswith("http"):
                    listing_url = href

            # Description
            desc_el = (
                card.select_one("[class*='description']")
                or card.select_one("[class*='tagline']")
                or card.select_one("p")
            )
            description = desc_el.get_text(strip=True) if desc_el else ""

            # Rating
            rating_el = card.select_one("[class*='rating']")
            rating_text = rating_el.get_text(strip=True) if rating_el else ""

            bio_parts = []
            if description:
                bio_parts.append(description[:500])
            if rating_text:
                bio_parts.append(f"Rating: {rating_text}")
            if not bio_parts:
                bio_parts.append("Salesforce AppExchange app")
            bio = " | ".join(bio_parts)

            contacts.append(ScrapedContact(
                name=company,
                company=company,
                website=listing_url or url,
                bio=bio,
                source_url=listing_url or url,
                source_category="saas_marketplace",
                raw_data={
                    "app_name": name,
                    "platform": "salesforce_appexchange",
                },
            ))

        return contacts
