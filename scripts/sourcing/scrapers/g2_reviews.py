"""
G2 Reviews scraper.

G2 (g2.com) is one of the largest B2B software review platforms with
100,000+ software products reviewed. The site organizes software by
categories, each containing dozens to hundreds of products with
company info, ratings, and reviews.

Uses the public category browse pages which are server-rendered HTML.
Each category page lists software products with:
- Product name and company name
- Star rating and review count
- Short description
- Website link
- Company size, industry focus

Category pages are at: https://www.g2.com/categories/{slug}

Estimated yield: 5,000-10,000+ SaaS companies
"""

from __future__ import annotations

import json
import re
from typing import Iterator, Optional
from urllib.parse import quote_plus

from scripts.sourcing.base import BaseScraper, ScrapedContact


# G2 software categories (top-level and popular subcategories)
CATEGORIES = [
    # Sales & Marketing
    "crm", "marketing-automation", "email-marketing",
    "social-media-management", "seo-tools", "content-marketing",
    "lead-generation", "sales-enablement", "sales-intelligence",
    "account-based-marketing", "ad-tech", "affiliate-marketing",
    "conversion-rate-optimization", "customer-data-platform",
    "demand-gen", "digital-analytics", "display-advertising",
    "influencer-marketing", "marketing-analytics",
    "public-relations", "video-marketing",
    # Customer Service
    "help-desk", "live-chat", "customer-success",
    "customer-experience", "survey", "feedback-analytics",
    "knowledge-management", "chatbots",
    # Productivity & Collaboration
    "project-management", "team-collaboration",
    "video-conferencing", "document-management",
    "workflow-management", "task-management",
    "note-taking", "online-whiteboard",
    # HR & Finance
    "hr-management", "applicant-tracking-systems",
    "payroll", "employee-engagement", "learning-management-system",
    "performance-management", "accounting",
    "expense-management", "billing-and-invoicing",
    # IT & Development
    "it-service-management", "cloud-infrastructure",
    "application-performance-monitoring", "ci-cd",
    "code-review", "api-management", "low-code-development",
    "no-code-development", "website-builder",
    # Data & Analytics
    "business-intelligence", "data-analytics",
    "data-visualization", "etl-tools",
    "data-warehouse", "big-data",
    # Security
    "endpoint-security", "identity-management",
    "siem", "vulnerability-management",
    "cloud-security", "email-security",
    # E-commerce
    "e-commerce-platforms", "shopping-cart",
    "payment-processing", "subscription-management",
    "order-management",
    # Other
    "backup", "digital-asset-management",
    "electronic-signature", "form-builder",
    "proposal-management", "event-management",
    "webinar", "appointment-scheduling",
    "contract-management", "compliance-management",
]


class Scraper(BaseScraper):
    """G2 Reviews category scraper.

    Browses G2 category pages to extract software product
    and company information.
    """

    SOURCE_NAME = "g2_reviews"
    BASE_URL = "https://www.g2.com"
    REQUESTS_PER_MINUTE = 6  # G2 has aggressive rate limiting

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_products: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield G2 category page URLs with pagination."""
        for category in CATEGORIES:
            # Each category page, up to 5 pages
            for page in range(1, 6):
                if page == 1:
                    yield f"{self.BASE_URL}/categories/{category}"
                else:
                    yield f"{self.BASE_URL}/categories/{category}?page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse G2 category page for product listings."""
        contacts = []

        # Try JSON-LD structured data first (G2 includes it)
        json_ld_contacts = self._parse_json_ld(html, url)
        if json_ld_contacts:
            contacts.extend(json_ld_contacts)

        # Try embedded __NEXT_DATA__
        next_data_contacts = self._try_next_data(html, url)
        if next_data_contacts:
            # Merge without duplicates
            for c in next_data_contacts:
                if c.name not in self._seen_products:
                    contacts.append(c)

        # HTML fallback
        if not contacts:
            contacts = self._parse_html_listings(html, url)

        return contacts

    def _parse_json_ld(self, html: str, url: str) -> list[ScrapedContact]:
        """Extract product data from JSON-LD structured data."""
        contacts = []

        # Find all JSON-LD script tags
        for match in re.finditer(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
            html, re.DOTALL,
        ):
            try:
                data = json.loads(match.group(1))
            except json.JSONDecodeError:
                continue

            # Handle both single objects and arrays
            items = data if isinstance(data, list) else [data]

            for item in items:
                if not isinstance(item, dict):
                    continue

                item_type = item.get("@type", "")
                if item_type not in ("SoftwareApplication", "Product", "WebApplication"):
                    # Check for ItemList containing products
                    if item_type == "ItemList":
                        for list_item in item.get("itemListElement", []):
                            if isinstance(list_item, dict):
                                inner = list_item.get("item", list_item)
                                contact = self._json_ld_to_contact(inner, url)
                                if contact:
                                    contacts.append(contact)
                    continue

                contact = self._json_ld_to_contact(item, url)
                if contact:
                    contacts.append(contact)

        return contacts

    def _json_ld_to_contact(self, item: dict, url: str) -> Optional[ScrapedContact]:
        """Convert a JSON-LD product item to a ScrapedContact."""
        name = (item.get("name") or "").strip()
        if not name or len(name) < 2:
            return None
        if name in self._seen_products:
            return None
        self._seen_products.add(name)

        # Company/brand
        brand = item.get("brand") or item.get("manufacturer") or {}
        if isinstance(brand, str):
            company = brand
        elif isinstance(brand, dict):
            company = (brand.get("name") or "").strip()
        else:
            company = name

        website = (item.get("url") or "").strip()
        description = (item.get("description") or "").strip()

        # Rating
        aggregate_rating = item.get("aggregateRating") or {}
        rating = aggregate_rating.get("ratingValue") or 0
        review_count = aggregate_rating.get("reviewCount") or 0

        # Category
        category = (item.get("applicationCategory") or item.get("category") or "").strip()

        # Build bio
        bio_parts = []
        if description:
            bio_parts.append(description[:500])
        if category:
            bio_parts.append(f"Category: {category}")
        if rating:
            bio_parts.append(f"G2 Rating: {rating}/5")
        if review_count:
            bio_parts.append(f"{review_count} reviews")
        if not bio_parts:
            bio_parts.append("G2 software listing")

        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=company or name,
            company=company or name,
            website=website or url,
            bio=bio,
            source_url=website or url,
            source_category="saas_reviews",
            raw_data={
                "product_name": name,
                "category": category,
                "rating": rating,
                "review_count": review_count,
                "platform": "g2_reviews",
            },
        )

    def _try_next_data(self, html: str, url: str) -> list[ScrapedContact]:
        """Try to extract data from __NEXT_DATA__ or similar."""
        contacts = []
        match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html, re.DOTALL,
        )
        if not match:
            return contacts

        try:
            data = json.loads(match.group(1))
            props = data.get("props", {}).get("pageProps", {})
            products = (
                props.get("products")
                or props.get("results")
                or props.get("items")
                or []
            )
            for product in products:
                if not isinstance(product, dict):
                    continue
                contact = self._product_dict_to_contact(product, url)
                if contact:
                    contacts.append(contact)
        except json.JSONDecodeError:
            pass

        return contacts

    def _product_dict_to_contact(self, product: dict, url: str) -> Optional[ScrapedContact]:
        """Parse a product dict from API/embedded data."""
        name = (
            product.get("name")
            or product.get("productName")
            or product.get("title")
            or ""
        ).strip()
        if not name or len(name) < 2:
            return None
        if name in self._seen_products:
            return None
        self._seen_products.add(name)

        company = (
            product.get("vendorName")
            or product.get("vendor")
            or product.get("company")
            or name
        ).strip()

        website = (product.get("website") or product.get("url") or "").strip()
        description = (
            product.get("description")
            or product.get("shortDescription")
            or ""
        ).strip()

        slug = (product.get("slug") or "").strip()
        listing_url = f"{self.BASE_URL}/products/{slug}/reviews" if slug else url

        rating = product.get("rating") or product.get("averageRating") or 0
        review_count = product.get("reviewCount") or product.get("numReviews") or 0
        category = (product.get("category") or product.get("categoryName") or "").strip()

        bio_parts = []
        if description:
            bio_parts.append(description[:500])
        if category:
            bio_parts.append(f"Category: {category}")
        if rating:
            bio_parts.append(f"G2 Rating: {rating}/5")
        if review_count:
            bio_parts.append(f"{review_count} reviews")
        if not bio_parts:
            bio_parts.append("G2 software listing")
        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=company,
            company=company,
            website=website or listing_url,
            bio=bio,
            source_url=listing_url,
            source_category="saas_reviews",
            raw_data={
                "product_name": name,
                "slug": slug,
                "category": category,
                "rating": rating,
                "review_count": review_count,
                "platform": "g2_reviews",
            },
        )

    def _parse_html_listings(self, html: str, url: str) -> list[ScrapedContact]:
        """Fallback HTML parsing for G2 category pages."""
        soup = self.parse_html(html)
        contacts = []

        # G2 product cards
        cards = (
            soup.select("[class*='product-card']")
            or soup.select("[class*='ProductCard']")
            or soup.select("[id*='product-card']")
            or soup.select("div[data-product-id]")
            or soup.select(".product-listing")
            or soup.select("[itemtype*='SoftwareApplication']")
        )

        # If no product cards found, try other patterns
        if not cards:
            # Look for grid items
            cards = soup.select("[class*='grid'] [class*='card']")

        for card in cards:
            name_el = (
                card.select_one("[itemprop='name']")
                or card.select_one("h3")
                or card.select_one("[class*='product-name']")
                or card.select_one("[class*='ProductName']")
                or card.select_one("a[class*='name']")
            )
            if not name_el:
                continue

            name = name_el.get_text(strip=True)
            if not name or len(name) < 2:
                continue
            if name in self._seen_products:
                continue
            self._seen_products.add(name)

            # Company name (often the same as product name on G2)
            vendor_el = card.select_one("[class*='vendor']") or card.select_one("[class*='company']")
            company = vendor_el.get_text(strip=True) if vendor_el else name

            # Link
            link_el = card.select_one("a[href*='/products/']")
            listing_url = ""
            if link_el:
                href = link_el.get("href", "")
                if href.startswith("/"):
                    listing_url = f"{self.BASE_URL}{href}"
                elif href.startswith("http"):
                    listing_url = href

            # Rating
            rating_el = card.select_one("[class*='rating']") or card.select_one("[itemprop='ratingValue']")
            rating = ""
            if rating_el:
                rating = rating_el.get_text(strip=True)
                # Also check content attribute
                if not rating:
                    rating = rating_el.get("content", "")

            # Review count
            review_el = card.select_one("[class*='review-count']") or card.select_one("[itemprop='reviewCount']")
            reviews = ""
            if review_el:
                reviews = review_el.get_text(strip=True)
                if not reviews:
                    reviews = review_el.get("content", "")

            # Description
            desc_el = card.select_one("[class*='description']") or card.select_one("p")
            description = desc_el.get_text(strip=True) if desc_el else ""

            bio_parts = []
            if description:
                bio_parts.append(description[:500])
            if rating:
                bio_parts.append(f"G2 Rating: {rating}")
            if reviews:
                bio_parts.append(f"{reviews} reviews")
            if not bio_parts:
                bio_parts.append("G2 software listing")
            bio = " | ".join(bio_parts)

            contacts.append(ScrapedContact(
                name=company,
                company=company,
                website=listing_url or url,
                bio=bio,
                source_url=listing_url or url,
                source_category="saas_reviews",
                raw_data={
                    "product_name": name,
                    "platform": "g2_reviews",
                },
            ))

        return contacts
