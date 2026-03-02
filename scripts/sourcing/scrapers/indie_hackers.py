"""
Indie Hackers Product/Company Listings Scraper

Scrapes product and founder data from https://www.indiehackers.com/products.

Indie Hackers is a community of founders sharing their products with
transparent revenue data. The site is a JS-rendered SPA backed by Firebase.
Direct HTML scraping yields only a loading screen.

Strategy:
  Indie Hackers stores product data in Firebase Realtime Database. We query
  the Firebase REST API directly to get product listings as JSON, which is
  far more efficient than trying to render JS.

  Firebase endpoints:
    - https://indie-hackers.firebaseio.com/products.json — All products
    - https://indie-hackers.firebaseio.com/products/{id}.json — Single product

  We also scrape the server-rendered product interview pages at
  /product/{slug} which contain useful detail and are HTML-accessible.

Estimated yield: 2,000-5,000 products with founder info
"""

from __future__ import annotations

import json
import re
from typing import Iterator, Optional
from datetime import datetime

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Categories on Indie Hackers
CATEGORIES = [
    "saas",
    "marketing",
    "fintech",
    "e-commerce",
    "education",
    "productivity",
    "developer-tools",
    "design",
    "analytics",
    "social-media",
    "health",
    "ai",
    "content",
    "community",
    "marketplace",
    "consulting",
    "agency",
    "newsletter",
    "podcasting",
    "mobile-apps",
]

# Firebase query parameters for pagination
FIREBASE_PAGE_SIZE = 200


class Scraper(BaseScraper):
    SOURCE_NAME = "indie_hackers"
    BASE_URL = "https://www.indiehackers.com"
    FIREBASE_URL = "https://indie-hackers.firebaseio.com"
    REQUESTS_PER_MINUTE = 8

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_ids: set[str] = set()
        self._seen_names: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield URLs for Indie Hackers product data.

        We use a mix of:
          1. Firebase REST API for bulk JSON data
          2. HTML product pages for individual profiles
        """
        # Primary: Firebase products endpoint with pagination
        # orderBy + limitToFirst for paginated access
        yield (
            f"{self.FIREBASE_URL}/products.json"
            f"?orderBy=\"$key\"&limitToFirst={FIREBASE_PAGE_SIZE}"
        )

        # Category/topic-based product listing pages (HTML fallback)
        for category in CATEGORIES:
            yield f"{self.BASE_URL}/products/{category}"

        # Top products page
        yield f"{self.BASE_URL}/products?sorting=highest-revenue"
        yield f"{self.BASE_URL}/products?sorting=most-popular"
        yield f"{self.BASE_URL}/products?sorting=newest"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse product data from either Firebase JSON or HTML pages."""
        # Check if this is a Firebase JSON response
        if "firebaseio.com" in url:
            return self._parse_firebase_json(url, html)

        # Otherwise parse as HTML
        return self._parse_html_page(url, html)

    def _parse_firebase_json(self, url: str, raw: str) -> list[ScrapedContact]:
        """Parse Firebase products JSON response."""
        contacts = []
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            self.logger.warning("Failed to parse Firebase JSON from %s", url)
            return contacts

        if not isinstance(data, dict):
            return contacts

        for product_id, product in data.items():
            if not isinstance(product, dict):
                continue

            contact = self._product_to_contact(product_id, product)
            if contact:
                contacts.append(contact)

        # If we got a full page, queue the next page
        if len(data) >= FIREBASE_PAGE_SIZE:
            last_key = list(data.keys())[-1]
            next_url = (
                f"{self.FIREBASE_URL}/products.json"
                f"?orderBy=\"$key\"&startAfter=\"{last_key}\""
                f"&limitToFirst={FIREBASE_PAGE_SIZE}"
            )
            next_raw = self.fetch_page(next_url)
            if next_raw:
                contacts.extend(self._parse_firebase_json(next_url, next_raw))

        return contacts

    def _product_to_contact(self, product_id: str, product: dict) -> ScrapedContact | None:
        """Convert a Firebase product dict to a ScrapedContact."""
        if product_id in self._seen_ids:
            return None
        self._seen_ids.add(product_id)

        name = (product.get("name") or "").strip()
        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_names:
            return None
        self._seen_names.add(name_key)

        tagline = (product.get("tagline") or product.get("oneLiner") or "").strip()
        description = (product.get("description") or "").strip()
        website = (product.get("website") or product.get("url") or "").strip()
        revenue = (product.get("revenue") or product.get("monthlyRevenue") or "").strip()
        slug = (product.get("slug") or product.get("id") or product_id).strip()

        # Founder info
        founder_name = ""
        founder_data = product.get("founder") or product.get("user") or {}
        if isinstance(founder_data, dict):
            founder_name = (founder_data.get("name") or founder_data.get("username") or "").strip()
        elif isinstance(founder_data, str):
            founder_name = founder_data.strip()

        # If no external website, use the IH product page
        if not website:
            website = f"{self.BASE_URL}/product/{slug}"

        # Build bio
        bio_parts = []
        if tagline:
            bio_parts.append(tagline)
        if description and description != tagline:
            bio_parts.append(description[:500])
        if revenue:
            bio_parts.append(f"Revenue: {revenue}")
        if founder_name:
            bio_parts.append(f"Founder: {founder_name}")
        bio = " | ".join(bio_parts) if bio_parts else f"Product on Indie Hackers"

        # Use founder name as the contact name if available, else company name
        contact_name = founder_name if founder_name else name

        return ScrapedContact(
            name=contact_name,
            company=name,
            website=website,
            bio=bio,
            source_url=f"{self.BASE_URL}/product/{slug}",
            source_category="startups",
            raw_data={
                "product_id": product_id,
                "slug": slug,
                "revenue": revenue,
                "founder": founder_name,
            },
        )

    def _parse_html_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse an Indie Hackers HTML page (may be partially rendered)."""
        contacts = []
        soup = self.parse_html(html)

        # Look for product links in the HTML
        product_links = set()
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            match = re.match(r"^/product/([a-zA-Z0-9_\-]+)$", href)
            if match:
                slug = match.group(1)
                if slug and slug not in self._seen_ids:
                    product_links.add(slug)

        # Fetch individual product pages
        for slug in product_links:
            product_url = f"{self.BASE_URL}/product/{slug}"
            product_html = self.fetch_page(product_url)
            if product_html:
                contact = self._parse_product_page(product_url, product_html, slug)
                if contact:
                    contacts.append(contact)

        # Also try to extract from JSON-LD or embedded script data
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                ld_data = json.loads(script.string or "")
                contact = self._parse_json_ld(ld_data)
                if contact:
                    contacts.append(contact)
            except (json.JSONDecodeError, ValueError):
                continue

        # Check for embedded __NEXT_DATA__ or similar SPA state
        for script in soup.find_all("script"):
            text = script.string or ""
            if "__NEXT_DATA__" in text or "window.__data" in text or "window.__INITIAL_STATE__" in text:
                json_match = re.search(r'(?:__NEXT_DATA__|__data|__INITIAL_STATE__)\s*=\s*({.+?});', text, re.DOTALL)
                if json_match:
                    try:
                        embedded = json.loads(json_match.group(1))
                        contacts.extend(self._parse_embedded_state(embedded))
                    except (json.JSONDecodeError, ValueError):
                        continue

        return contacts

    def _parse_product_page(self, url: str, html: str, slug: str) -> ScrapedContact | None:
        """Parse an individual product page."""
        if slug in self._seen_ids:
            return None
        self._seen_ids.add(slug)

        soup = self.parse_html(html)

        # Name from title or h1
        name = ""
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        if not name:
            og_title = soup.find("meta", property="og:title")
            if og_title:
                name = (og_title.get("content", "") or "").split("|")[0].split("-")[0].strip()

        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_names:
            return None
        self._seen_names.add(name_key)

        # Description
        bio = ""
        og_desc = soup.find("meta", property="og:description")
        if og_desc:
            bio = (og_desc.get("content", "") or "")[:1000]

        # Website
        website = ""
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            text = a_tag.get_text(strip=True).lower()
            if (
                ("website" in text or "visit" in text)
                and href.startswith("http")
                and "indiehackers.com" not in href.lower()
            ):
                website = href
                break

        if not website:
            website = url

        # Email
        emails = self.extract_emails(html)
        email = emails[0] if emails else ""

        # LinkedIn
        linkedin = self.extract_linkedin(html)

        return ScrapedContact(
            name=name,
            email=email,
            company=name,
            website=website,
            linkedin=linkedin,
            bio=bio or f"Product on Indie Hackers",
            source_url=url,
            source_category="startups",
            raw_data={"slug": slug},
        )

    def _parse_json_ld(self, data: dict) -> ScrapedContact | None:
        """Extract contact from JSON-LD structured data."""
        if not isinstance(data, dict):
            return None

        ld_type = data.get("@type", "")
        if ld_type not in ("Product", "SoftwareApplication", "WebApplication", "Organization"):
            return None

        name = (data.get("name") or "").strip()
        if not name or len(name) < 2 or name.lower() in self._seen_names:
            return None
        self._seen_names.add(name.lower())

        description = (data.get("description") or "").strip()[:1000]
        website = (data.get("url") or "").strip()

        return ScrapedContact(
            name=name,
            company=name,
            website=website,
            bio=description or f"Product on Indie Hackers",
            source_category="startups",
        )

    def _parse_embedded_state(self, data: dict) -> list[ScrapedContact]:
        """Parse products from embedded SPA state."""
        contacts = []
        # Recursively look for product-like objects
        self._find_products(data, contacts, depth=0)
        return contacts

    def _find_products(self, obj, results: list, depth: int = 0):
        """Recursively find product data in nested state."""
        if depth > 8:
            return

        if isinstance(obj, dict):
            # Check if this dict looks like a product
            if (
                obj.get("name")
                and (obj.get("website") or obj.get("url") or obj.get("tagline"))
                and isinstance(obj.get("name"), str)
                and len(obj["name"]) > 2
            ):
                name = obj["name"].strip()
                if name.lower() not in self._seen_names:
                    self._seen_names.add(name.lower())
                    tagline = (obj.get("tagline") or obj.get("description") or "").strip()
                    website = (obj.get("website") or obj.get("url") or "").strip()
                    revenue = (obj.get("revenue") or obj.get("monthlyRevenue") or "").strip()

                    bio_parts = []
                    if tagline:
                        bio_parts.append(tagline[:500])
                    if revenue:
                        bio_parts.append(f"Revenue: {revenue}")

                    results.append(ScrapedContact(
                        name=name,
                        company=name,
                        website=website,
                        bio=" | ".join(bio_parts) if bio_parts else f"Product on Indie Hackers",
                        source_category="startups",
                    ))
            else:
                for value in obj.values():
                    self._find_products(value, results, depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                self._find_products(item, results, depth + 1)
