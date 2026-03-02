"""
Microsoft AppSource scraper.

Microsoft AppSource (appsource.microsoft.com) is the marketplace for
business apps that integrate with Microsoft 365, Dynamics 365, Power BI,
Azure, and other Microsoft platforms.

The marketplace has a public API used by the frontend for search and
browse operations. Apps include:
- App/solution name and publisher
- Description, categories, industries
- Rating and review count
- Pricing model
- Integration products (Office 365, Teams, Dynamics, etc.)

Estimated yield: 3,000-5,000 SaaS companies and ISVs
"""

from __future__ import annotations

import json
import re
from typing import Iterator, Optional
from urllib.parse import urlencode, quote_plus

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Microsoft product categories for search
PRODUCT_CATEGORIES = [
    "dynamics-365", "microsoft-365", "power-bi",
    "power-apps", "power-automate", "azure",
    "teams", "sharepoint", "outlook",
    "excel", "word", "visio",
]

# Search terms for broader coverage
SEARCH_TERMS = [
    "CRM", "ERP", "analytics", "reporting", "automation",
    "integration", "marketing", "sales", "HR", "finance",
    "project management", "collaboration", "communication",
    "security", "compliance", "productivity", "AI",
    "machine learning", "IoT", "supply chain",
    "customer service", "helpdesk", "accounting",
    "inventory", "ecommerce", "document management",
    "business intelligence", "data visualization",
    "workflow", "forms", "survey", "email marketing",
]


class Scraper(BaseScraper):
    """Microsoft AppSource scraper.

    Uses the AppSource search API and browse pages to extract
    app publisher/company data.
    """

    SOURCE_NAME = "microsoft_appsource"
    BASE_URL = "https://appsource.microsoft.com"
    # The storefront API used by the frontend
    API_URL = "https://appsource.microsoft.com/en-US/marketplace/apps"
    REQUESTS_PER_MINUTE = 10

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_ids: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield AppSource browse and search page URLs."""
        # Browse by product
        for product in PRODUCT_CATEGORIES:
            for page in range(1, 11):
                yield (
                    f"{self.BASE_URL}/en-US/marketplace/apps"
                    f"?product={product}&page={page}"
                )

        # Search pages
        for term in SEARCH_TERMS:
            for page in range(1, 6):
                yield (
                    f"{self.BASE_URL}/en-US/marketplace/apps"
                    f"?search={quote_plus(term)}&page={page}"
                )

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse AppSource page for app listings."""
        contacts = []

        # Try embedded JSON data first
        json_contacts = self._try_json_extraction(html, url)
        if json_contacts:
            return json_contacts

        # Parse HTML
        return self._parse_html(html, url)

    def _try_json_extraction(self, html: str, url: str) -> list[ScrapedContact]:
        """Try to extract app data from embedded JSON."""
        contacts = []

        # Look for __NEXT_DATA__
        match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html, re.DOTALL,
        )
        if match:
            try:
                data = json.loads(match.group(1))
                props = data.get("props", {}).get("pageProps", {})
                apps = (
                    props.get("searchResults")
                    or props.get("apps")
                    or props.get("results")
                    or []
                )
                for app in apps:
                    contact = self._parse_app_dict(app, url)
                    if contact:
                        contacts.append(contact)
                if contacts:
                    return contacts
            except json.JSONDecodeError:
                pass

        # Look for embedded hydration data
        for pattern in [
            r'window\.__INITIAL_STATE__\s*=\s*({.*?});\s*</script>',
            r'window\.__data\s*=\s*({.*?});\s*</script>',
            r'"apps"\s*:\s*(\[{.*?}\])',
            r'"searchResults"\s*:\s*(\[{.*?}\])',
        ]:
            match = re.search(pattern, html, re.DOTALL)
            if match:
                try:
                    data = json.loads(match.group(1))
                    apps = []
                    if isinstance(data, dict):
                        apps = (
                            data.get("apps")
                            or data.get("searchResults")
                            or data.get("results")
                            or data.get("items")
                            or []
                        )
                    elif isinstance(data, list):
                        apps = data

                    for app in apps:
                        contact = self._parse_app_dict(app, url)
                        if contact:
                            contacts.append(contact)
                    if contacts:
                        return contacts
                except json.JSONDecodeError:
                    continue

        return contacts

    def _parse_app_dict(self, app: dict, url: str) -> Optional[ScrapedContact]:
        """Parse a single app dict into a ScrapedContact."""
        if not isinstance(app, dict):
            return None

        # App name
        name = (
            app.get("displayName")
            or app.get("title")
            or app.get("name")
            or ""
        ).strip()
        if not name or len(name) < 2:
            return None

        app_id = (
            app.get("id")
            or app.get("appId")
            or app.get("legacyId")
            or name.lower()
        ).strip()
        if app_id in self._seen_ids:
            return None
        self._seen_ids.add(app_id)

        # Publisher info
        publisher = app.get("publisher") or app.get("publisherDetails") or {}
        if isinstance(publisher, str):
            publisher_name = publisher
            publisher_url = ""
        elif isinstance(publisher, dict):
            publisher_name = (
                publisher.get("displayName")
                or publisher.get("name")
                or publisher.get("publisherName")
                or ""
            ).strip()
            publisher_url = (publisher.get("website") or publisher.get("url") or "").strip()
        else:
            publisher_name = ""
            publisher_url = ""

        company = publisher_name or name
        website = publisher_url

        description = (
            app.get("description")
            or app.get("shortDescription")
            or app.get("summary")
            or ""
        ).strip()

        # Products/platforms
        products = app.get("products") or app.get("applicableProducts") or []
        if isinstance(products, list):
            product_names = []
            for p in products:
                if isinstance(p, str):
                    product_names.append(p)
                elif isinstance(p, dict):
                    product_names.append((p.get("displayName") or p.get("name") or "").strip())
            products = [p for p in product_names if p]

        # Categories
        categories = app.get("categories") or app.get("industryCategories") or []
        if isinstance(categories, list):
            cat_names = []
            for c in categories:
                if isinstance(c, str):
                    cat_names.append(c)
                elif isinstance(c, dict):
                    cat_names.append((c.get("displayName") or c.get("name") or "").strip())
            categories = [c for c in cat_names if c]

        # Rating
        rating = app.get("rating") or app.get("averageRating") or 0
        review_count = app.get("ratingCount") or app.get("reviewCount") or 0

        # Pricing
        pricing = (app.get("pricingModel") or app.get("pricing") or "").strip()

        # Detail URL
        slug = (app.get("slug") or app.get("urlSlug") or "").strip()
        listing_url = ""
        if slug:
            listing_url = f"{self.BASE_URL}/en-US/product/office/{slug}"
        elif app_id and app_id != name.lower():
            listing_url = f"{self.BASE_URL}/en-US/product/office/{app_id}"

        if not website:
            website = listing_url or url

        # Build bio
        bio_parts = []
        if description:
            bio_parts.append(description[:500])
        if products:
            bio_parts.append(f"Works with: {', '.join(products[:5])}")
        if categories:
            bio_parts.append(f"Categories: {', '.join(categories[:5])}")
        if rating:
            bio_parts.append(f"Rating: {rating}/5")
        if review_count:
            bio_parts.append(f"{review_count} reviews")
        if pricing:
            bio_parts.append(f"Pricing: {pricing}")
        if not bio_parts:
            bio_parts.append("Microsoft AppSource listing")

        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=company,
            company=company,
            website=website,
            bio=bio,
            source_url=listing_url or url,
            source_category="saas_marketplace",
            raw_data={
                "app_name": name,
                "app_id": app_id,
                "products": products,
                "categories": categories,
                "rating": rating,
                "review_count": review_count,
                "pricing": pricing,
                "platform": "microsoft_appsource",
            },
        )

    def _parse_html(self, html: str, url: str) -> list[ScrapedContact]:
        """Fallback HTML parsing for AppSource listings."""
        soup = self.parse_html(html)
        contacts = []

        # App cards
        cards = (
            soup.select("[class*='AppCard']")
            or soup.select("[class*='app-card']")
            or soup.select("[class*='tileCont']")
            or soup.select("[class*='gallery-item']")
            or soup.select("[class*='SearchResult']")
            or soup.select("article")
        )

        for card in cards:
            name_el = (
                card.select_one("h3")
                or card.select_one("h2")
                or card.select_one("[class*='title']")
                or card.select_one("[class*='name']")
            )
            if not name_el:
                continue

            name = name_el.get_text(strip=True)
            if not name or len(name) < 2:
                continue
            if name in self._seen_ids:
                continue
            self._seen_ids.add(name)

            # Publisher
            pub_el = (
                card.select_one("[class*='publisher']")
                or card.select_one("[class*='author']")
                or card.select_one("[class*='company']")
            )
            company = pub_el.get_text(strip=True) if pub_el else name

            # Link
            link_el = card.select_one("a[href]")
            listing_url = ""
            if link_el:
                href = link_el.get("href", "")
                if href.startswith("/"):
                    listing_url = f"{self.BASE_URL}{href}"
                elif href.startswith("http"):
                    listing_url = href

            # Description
            desc_el = card.select_one("[class*='desc']") or card.select_one("p")
            description = desc_el.get_text(strip=True) if desc_el else ""

            bio_parts = []
            if description:
                bio_parts.append(description[:500])
            if not bio_parts:
                bio_parts.append("Microsoft AppSource app")
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
                    "platform": "microsoft_appsource",
                },
            ))

        return contacts
