"""
Capterra Listings scraper.

Capterra (capterra.com) is a major B2B software review and comparison
platform owned by Gartner, listing 100,000+ software products across
800+ categories.

Uses the public browse/category pages which are server-rendered.
Each category page lists software products with:
- Software name and vendor/company
- Star rating and review count
- Short description
- Key features
- Pricing info

Category URLs: https://www.capterra.com/CATEGORY-slug/software/

Estimated yield: 5,000-10,000+ SaaS companies
"""

from __future__ import annotations

import json
import re
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Capterra top-level software categories
CATEGORIES = [
    # Business & Productivity
    "project-management", "crm", "accounting",
    "hr", "collaboration", "document-management",
    "business-intelligence", "workflow-management",
    "time-tracking", "expense-management",
    "inventory-management", "business-process-management",
    "enterprise-resource-planning", "field-service-management",
    # Sales & Marketing
    "marketing-automation", "email-marketing",
    "social-media-management", "seo", "lead-generation",
    "sales-enablement", "content-management",
    "digital-asset-management", "affiliate-management",
    "customer-engagement", "campaign-management",
    "landing-page", "ab-testing",
    # Customer Service
    "help-desk", "live-chat", "customer-success",
    "knowledge-management", "chatbot",
    "customer-feedback", "customer-experience",
    # IT & Dev
    "it-service-management", "application-development",
    "devops", "api-management", "low-code-development",
    "database-management", "cloud-management",
    "it-asset-management", "network-monitoring",
    # HR
    "applicant-tracking", "payroll", "employee-engagement",
    "learning-management-system", "performance-management",
    "onboarding", "scheduling", "workforce-management",
    # Finance
    "billing-and-invoicing", "payment-processing",
    "subscription-management", "budgeting",
    "financial-reporting", "tax-preparation",
    # Healthcare
    "electronic-health-records", "medical-practice-management",
    "telemedicine", "patient-engagement",
    # E-commerce
    "e-commerce", "shopping-cart", "order-management",
    "point-of-sale", "multichannel-retail",
    # Security
    "cybersecurity", "identity-management",
    "endpoint-security", "email-security",
    # Other
    "event-management", "survey",
    "electronic-signature", "form-builder",
    "video-conferencing", "webinar",
    "appointment-scheduling", "contract-management",
    "proposal-management", "construction-management",
    "real-estate", "legal-management",
    "church-management", "association-management",
    "property-management", "fleet-management",
]


class Scraper(BaseScraper):
    """Capterra software directory scraper.

    Browses category pages to extract software product
    and vendor company information.
    """

    SOURCE_NAME = "capterra_listings"
    BASE_URL = "https://www.capterra.com"
    REQUESTS_PER_MINUTE = 6  # Capterra has rate limiting

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_products: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield Capterra category page URLs."""
        for category in CATEGORIES:
            # First page
            yield f"{self.BASE_URL}/{category}-software/"
            # Additional pages
            for page in range(2, 6):
                yield f"{self.BASE_URL}/{category}-software/?page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Capterra category page for product listings."""
        contacts = []

        # Try JSON-LD structured data
        json_ld_contacts = self._parse_json_ld(html, url)
        if json_ld_contacts:
            return json_ld_contacts

        # Try embedded JSON
        json_contacts = self._try_embedded_json(html, url)
        if json_contacts:
            return json_contacts

        # HTML parsing fallback
        return self._parse_html_listings(html, url)

    def _parse_json_ld(self, html: str, url: str) -> list[ScrapedContact]:
        """Extract product data from JSON-LD structured data."""
        contacts = []
        for match in re.finditer(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
            html, re.DOTALL,
        ):
            try:
                data = json.loads(match.group(1))
            except json.JSONDecodeError:
                continue

            items = data if isinstance(data, list) else [data]
            for item in items:
                if not isinstance(item, dict):
                    continue

                item_type = item.get("@type", "")
                if item_type == "ItemList":
                    for elem in item.get("itemListElement", []):
                        inner = elem.get("item", elem) if isinstance(elem, dict) else elem
                        if isinstance(inner, dict):
                            contact = self._json_ld_to_contact(inner, url)
                            if contact:
                                contacts.append(contact)
                elif item_type in ("SoftwareApplication", "Product", "WebApplication"):
                    contact = self._json_ld_to_contact(item, url)
                    if contact:
                        contacts.append(contact)

        return contacts

    def _json_ld_to_contact(self, item: dict, url: str) -> Optional[ScrapedContact]:
        """Convert JSON-LD to ScrapedContact."""
        name = (item.get("name") or "").strip()
        if not name or len(name) < 2:
            return None
        if name in self._seen_products:
            return None
        self._seen_products.add(name)

        brand = item.get("brand") or item.get("manufacturer") or {}
        if isinstance(brand, str):
            company = brand
        elif isinstance(brand, dict):
            company = (brand.get("name") or "").strip()
        else:
            company = name

        website = (item.get("url") or "").strip()
        description = (item.get("description") or "").strip()

        aggregate = item.get("aggregateRating") or {}
        rating = aggregate.get("ratingValue") or 0
        review_count = aggregate.get("reviewCount") or 0

        category = (item.get("applicationCategory") or "").strip()

        bio_parts = []
        if description:
            bio_parts.append(description[:500])
        if category:
            bio_parts.append(f"Category: {category}")
        if rating:
            bio_parts.append(f"Capterra Rating: {rating}/5")
        if review_count:
            bio_parts.append(f"{review_count} reviews")
        if not bio_parts:
            bio_parts.append("Capterra software listing")
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
                "platform": "capterra",
            },
        )

    def _try_embedded_json(self, html: str, url: str) -> list[ScrapedContact]:
        """Try to extract data from embedded JSON/Next.js data."""
        contacts = []

        # __NEXT_DATA__
        match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html, re.DOTALL,
        )
        if match:
            try:
                data = json.loads(match.group(1))
                props = data.get("props", {}).get("pageProps", {})
                products = (
                    props.get("products")
                    or props.get("listings")
                    or props.get("results")
                    or []
                )
                for product in products:
                    if isinstance(product, dict):
                        contact = self._product_to_contact(product, url)
                        if contact:
                            contacts.append(contact)
                if contacts:
                    return contacts
            except json.JSONDecodeError:
                pass

        # Window data
        for pattern in [
            r'window\.__INITIAL_STATE__\s*=\s*({.*?});\s*</script>',
            r'window\.INITIAL_DATA\s*=\s*({.*?});\s*</script>',
        ]:
            match = re.search(pattern, html, re.DOTALL)
            if match:
                try:
                    data = json.loads(match.group(1))
                    products = (
                        data.get("products")
                        or data.get("listings")
                        or data.get("results")
                        or []
                    )
                    for product in products:
                        if isinstance(product, dict):
                            contact = self._product_to_contact(product, url)
                            if contact:
                                contacts.append(contact)
                    if contacts:
                        return contacts
                except json.JSONDecodeError:
                    continue

        return contacts

    def _product_to_contact(self, product: dict, url: str) -> Optional[ScrapedContact]:
        """Parse a product dict."""
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

        website = (
            product.get("websiteUrl")
            or product.get("website")
            or product.get("url")
            or ""
        ).strip()

        description = (
            product.get("shortDescription")
            or product.get("description")
            or ""
        ).strip()

        rating = product.get("overallRating") or product.get("rating") or 0
        review_count = product.get("reviewCount") or product.get("numReviews") or 0
        category = (product.get("category") or product.get("categoryName") or "").strip()

        slug = (product.get("slug") or product.get("permalink") or "").strip()
        listing_url = f"{self.BASE_URL}/software/{slug}" if slug else url

        bio_parts = []
        if description:
            bio_parts.append(description[:500])
        if category:
            bio_parts.append(f"Category: {category}")
        if rating:
            bio_parts.append(f"Capterra Rating: {rating}/5")
        if review_count:
            bio_parts.append(f"{review_count} reviews")
        if not bio_parts:
            bio_parts.append("Capterra software listing")
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
                "platform": "capterra",
            },
        )

    def _parse_html_listings(self, html: str, url: str) -> list[ScrapedContact]:
        """Fallback HTML parsing."""
        soup = self.parse_html(html)
        contacts = []

        # Product cards
        cards = (
            soup.select("[data-testid*='product-card']")
            or soup.select("[class*='ProductCard']")
            or soup.select("[class*='product-card']")
            or soup.select("[class*='listing-card']")
            or soup.select("[class*='ListingCard']")
            or soup.select("div[data-product-id]")
        )

        if not cards:
            # Try broader selectors
            cards = soup.select("[class*='card'][class*='product']")
            if not cards:
                cards = soup.select(".sb.listing")

        for card in cards:
            name_el = (
                card.select_one("[class*='product-name']")
                or card.select_one("[class*='ProductName']")
                or card.select_one("h2")
                or card.select_one("h3")
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

            # Vendor
            vendor_el = (
                card.select_one("[class*='vendor']")
                or card.select_one("[class*='company']")
                or card.select_one("[class*='developer']")
            )
            company = vendor_el.get_text(strip=True) if vendor_el else name

            # Link
            link_el = card.select_one("a[href*='/software/']") or card.select_one("a[href]")
            listing_url = ""
            if link_el:
                href = link_el.get("href", "")
                if href.startswith("/"):
                    listing_url = f"{self.BASE_URL}{href}"
                elif href.startswith("http"):
                    listing_url = href

            # Rating
            rating_el = card.select_one("[class*='rating']")
            rating = rating_el.get_text(strip=True) if rating_el else ""

            # Description
            desc_el = card.select_one("[class*='description']") or card.select_one("p")
            description = desc_el.get_text(strip=True) if desc_el else ""

            bio_parts = []
            if description:
                bio_parts.append(description[:500])
            if rating:
                bio_parts.append(f"Capterra Rating: {rating}")
            if not bio_parts:
                bio_parts.append("Capterra software listing")
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
                    "platform": "capterra",
                },
            ))

        return contacts
