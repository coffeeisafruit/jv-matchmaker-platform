"""
Stripe Partners / App Marketplace scraper.

Stripe's partner/app marketplace at stripe.com/partners/apps lists
integrations and apps built on the Stripe platform for payments,
billing, and financial infrastructure.

The marketplace pages are rendered server-side with app data.
Each listing includes:
- App/partner name and company
- Description and tagline
- Category (payments, billing, tax, fraud, etc.)
- Website link
- Integration type (verified, certified)

Estimated yield: 500-1,500 fintech/SaaS companies
"""

from __future__ import annotations

import json
import re
from typing import Iterator, Optional
from urllib.parse import quote_plus

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Stripe marketplace categories
CATEGORIES = [
    "payments", "billing", "connect", "tax",
    "fraud-prevention", "identity-verification",
    "analytics", "accounting", "marketing",
    "customer-support", "subscriptions", "invoicing",
    "reporting", "data", "automation",
    "developer-tools", "extensions",
]

# Search terms for broader coverage
SEARCH_TERMS = [
    "payment", "checkout", "subscription", "invoice",
    "billing", "accounting", "tax", "fraud",
    "identity", "compliance", "reporting",
    "analytics", "marketing", "CRM", "ERP",
    "ecommerce", "marketplace", "platform",
    "SaaS", "fintech", "banking",
]


class Scraper(BaseScraper):
    """Stripe Partner / App Marketplace scraper.

    Browses the Stripe partner directory and app marketplace
    to extract partner company information.
    """

    SOURCE_NAME = "stripe_partners"
    BASE_URL = "https://stripe.com"
    MARKETPLACE_URL = "https://marketplace.stripe.com"
    REQUESTS_PER_MINUTE = 8

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_ids: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield Stripe marketplace and partner page URLs."""
        # Main marketplace pages
        yield f"{self.MARKETPLACE_URL}/"

        # Browse by category
        for category in CATEGORIES:
            yield f"{self.MARKETPLACE_URL}/categories/{category}"
            yield f"{self.BASE_URL}/partners/apps/{category}"

        # Search terms
        for term in SEARCH_TERMS:
            yield f"{self.MARKETPLACE_URL}/search?q={quote_plus(term)}"

        # Partner directory pages
        yield f"{self.BASE_URL}/partners/apps"
        yield f"{self.BASE_URL}/partners/directory"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Stripe marketplace/partner page for listings."""
        contacts = []

        # Try embedded JSON data
        json_contacts = self._try_embedded_json(html, url)
        if json_contacts:
            return json_contacts

        # HTML parsing
        return self._parse_html_listings(html, url)

    def _try_embedded_json(self, html: str, url: str) -> list[ScrapedContact]:
        """Try to extract app data from embedded JSON."""
        contacts = []

        # Look for __NEXT_DATA__ (Next.js)
        match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html, re.DOTALL,
        )
        if match:
            try:
                data = json.loads(match.group(1))
                props = data.get("props", {}).get("pageProps", {})

                # Look for apps/listings in various keys
                apps = (
                    props.get("apps")
                    or props.get("listings")
                    or props.get("partners")
                    or props.get("results")
                    or props.get("integrations")
                    or []
                )

                # Also check nested structures
                if not apps:
                    for key in props:
                        val = props[key]
                        if isinstance(val, list) and val and isinstance(val[0], dict):
                            if any(k in val[0] for k in ("name", "title", "appName")):
                                apps = val
                                break

                for app in apps:
                    if isinstance(app, dict):
                        contact = self._app_to_contact(app, url)
                        if contact:
                            contacts.append(contact)

                if contacts:
                    return contacts
            except json.JSONDecodeError:
                pass

        # Other embedded data patterns
        for pattern in [
            r'window\.__PRELOADED_STATE__\s*=\s*({.*?});\s*</script>',
            r'window\.__INITIAL_STATE__\s*=\s*({.*?});\s*</script>',
            r'"apps"\s*:\s*(\[{.*?}\])',
            r'"partners"\s*:\s*(\[{.*?}\])',
        ]:
            match = re.search(pattern, html, re.DOTALL)
            if match:
                try:
                    data = json.loads(match.group(1))
                    apps = []
                    if isinstance(data, dict):
                        apps = (
                            data.get("apps")
                            or data.get("partners")
                            or data.get("listings")
                            or []
                        )
                    elif isinstance(data, list):
                        apps = data

                    for app in apps:
                        if isinstance(app, dict):
                            contact = self._app_to_contact(app, url)
                            if contact:
                                contacts.append(contact)
                    if contacts:
                        return contacts
                except json.JSONDecodeError:
                    continue

        return contacts

    def _app_to_contact(self, app: dict, url: str) -> Optional[ScrapedContact]:
        """Convert an app/partner dict to ScrapedContact."""
        name = (
            app.get("name")
            or app.get("title")
            or app.get("appName")
            or app.get("partnerName")
            or ""
        ).strip()
        if not name or len(name) < 2:
            return None

        app_id = (
            app.get("id")
            or app.get("slug")
            or app.get("appId")
            or name.lower().replace(" ", "-")
        ).strip()
        if app_id in self._seen_ids:
            return None
        self._seen_ids.add(app_id)

        # Company info
        company = (
            app.get("company")
            or app.get("developerName")
            or app.get("publisher")
            or app.get("vendor")
            or name
        ).strip()

        website = (
            app.get("website")
            or app.get("url")
            or app.get("companyUrl")
            or app.get("developerUrl")
            or ""
        ).strip()

        description = (
            app.get("description")
            or app.get("shortDescription")
            or app.get("tagline")
            or app.get("summary")
            or ""
        ).strip()

        # Category
        category = (
            app.get("category")
            or app.get("categoryName")
            or ""
        ).strip()

        categories = app.get("categories") or app.get("tags") or []
        if isinstance(categories, list):
            cat_names = []
            for c in categories:
                if isinstance(c, str):
                    cat_names.append(c)
                elif isinstance(c, dict):
                    cat_names.append((c.get("name") or c.get("label") or "").strip())
            categories = [c for c in cat_names if c]
        if category and category not in categories:
            categories.insert(0, category)

        # Status
        verified = app.get("verified") or app.get("isVerified") or False
        certified = app.get("certified") or app.get("isCertified") or False

        # Rating
        rating = app.get("rating") or app.get("averageRating") or 0
        review_count = app.get("reviewCount") or app.get("numReviews") or 0

        # Listing URL
        slug = (app.get("slug") or "").strip()
        listing_url = ""
        if slug:
            listing_url = f"{self.MARKETPLACE_URL}/apps/{slug}"
        elif app_id:
            listing_url = f"{self.MARKETPLACE_URL}/apps/{app_id}"

        if not website:
            website = listing_url or url

        # Build bio
        bio_parts = []
        if description:
            bio_parts.append(description[:500])
        if categories:
            bio_parts.append(f"Categories: {', '.join(categories[:5])}")
        if verified:
            bio_parts.append("Stripe Verified")
        if certified:
            bio_parts.append("Stripe Certified")
        if rating:
            bio_parts.append(f"Rating: {rating}/5")
        if review_count:
            bio_parts.append(f"{review_count} reviews")
        if not bio_parts:
            bio_parts.append("Stripe marketplace app")

        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=company,
            company=company,
            website=website,
            bio=bio,
            source_url=listing_url or url,
            source_category="fintech_marketplace",
            raw_data={
                "app_name": name,
                "app_id": app_id,
                "slug": slug,
                "categories": categories,
                "verified": verified,
                "certified": certified,
                "rating": rating,
                "review_count": review_count,
                "platform": "stripe_partners",
            },
        )

    def _parse_html_listings(self, html: str, url: str) -> list[ScrapedContact]:
        """Fallback HTML parsing for Stripe marketplace pages."""
        soup = self.parse_html(html)
        contacts = []

        # App cards
        cards = (
            soup.select("[class*='AppCard']")
            or soup.select("[class*='app-card']")
            or soup.select("[class*='PartnerCard']")
            or soup.select("[class*='partner-card']")
            or soup.select("[class*='IntegrationCard']")
            or soup.select("[class*='ListingCard']")
            or soup.select("article[class*='app']")
            or soup.select("a[href*='/apps/']")
        )

        # If we got raw links, wrap them in parent
        if cards and cards[0].name == 'a':
            wrapped = []
            for c in cards:
                parent = c.parent
                if parent and parent.name in ('div', 'li', 'article', 'section'):
                    wrapped.append(parent)
            cards = wrapped if wrapped else cards

        for card in cards:
            name_el = (
                card.select_one("h3")
                or card.select_one("h2")
                or card.select_one("[class*='name']")
                or card.select_one("[class*='title']")
                or card.select_one("strong")
            )

            if not name_el:
                # If card is an anchor, try its text
                if card.name == 'a':
                    name_text = card.get_text(strip=True)
                    if name_text and len(name_text) > 2:
                        name_el = card
                if not name_el:
                    continue

            name = name_el.get_text(strip=True)
            if not name or len(name) < 2:
                continue
            if name in self._seen_ids:
                continue
            self._seen_ids.add(name)

            # Company/developer
            dev_el = (
                card.select_one("[class*='company']")
                or card.select_one("[class*='developer']")
                or card.select_one("[class*='vendor']")
                or card.select_one("[class*='publisher']")
            )
            company = dev_el.get_text(strip=True) if dev_el else name

            # Link
            link_el = card.select_one("a[href]") if card.name != 'a' else card
            listing_url = ""
            if link_el:
                href = link_el.get("href", "")
                if href.startswith("/"):
                    # Determine base
                    if "marketplace" in url:
                        listing_url = f"{self.MARKETPLACE_URL}{href}"
                    else:
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

            # Category badge
            cat_el = card.select_one("[class*='category']") or card.select_one("[class*='tag']")
            category = cat_el.get_text(strip=True) if cat_el else ""

            bio_parts = []
            if description:
                bio_parts.append(description[:500])
            if category:
                bio_parts.append(f"Category: {category}")
            if not bio_parts:
                bio_parts.append("Stripe partner app")
            bio = " | ".join(bio_parts)

            contacts.append(ScrapedContact(
                name=company,
                company=company,
                website=listing_url or url,
                bio=bio,
                source_url=listing_url or url,
                source_category="fintech_marketplace",
                raw_data={
                    "app_name": name,
                    "platform": "stripe_partners",
                },
            ))

        return contacts
