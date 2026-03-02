"""
Fiverr Pro service provider scraper.

Fiverr Pro is the curated tier of Fiverr with vetted professionals
across categories like programming, design, marketing, and business.

Strategy: Paginate through Fiverr Pro category pages and extract
provider profiles. Fiverr uses some JS rendering but the initial
HTML often contains enough data for extraction.

Note: Fiverr may use JS rendering. If blocked, this will need
crawl4ai/playwright. Marking as Tier 2 optimistically.

Estimated yield: 2,000-8,000 pro sellers
"""

from __future__ import annotations

import json
import re
from typing import Iterator

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Fiverr Pro category slugs
CATEGORIES = [
    "pro/categories/programming-tech",
    "pro/categories/graphics-design",
    "pro/categories/digital-marketing",
    "pro/categories/writing-translation",
    "pro/categories/video-animation",
    "pro/categories/music-audio",
    "pro/categories/business",
    "pro/categories/data",
    "pro/categories/photography",
    "pro/categories/ai-services",
]

# Sub-categories for broader coverage
SUB_CATEGORIES = [
    "pro/categories/programming-tech/website-development",
    "pro/categories/programming-tech/mobile-app-development",
    "pro/categories/programming-tech/software-development",
    "pro/categories/programming-tech/ai-coding",
    "pro/categories/graphics-design/logo-design",
    "pro/categories/graphics-design/brand-style-guides",
    "pro/categories/graphics-design/web-ui-ux-design",
    "pro/categories/digital-marketing/social-media-marketing",
    "pro/categories/digital-marketing/seo",
    "pro/categories/digital-marketing/sem",
    "pro/categories/digital-marketing/content-marketing",
    "pro/categories/business/business-consulting",
    "pro/categories/business/financial-consulting",
    "pro/categories/business/legal-consulting",
    "pro/categories/business/hr-consulting",
    "pro/categories/business/presentations",
    "pro/categories/data/data-science",
    "pro/categories/data/data-analytics",
    "pro/categories/data/data-visualization",
]

MAX_PAGES = 15


class Scraper(BaseScraper):
    SOURCE_NAME = "fiverr_pros"
    BASE_URL = "https://www.fiverr.com"
    REQUESTS_PER_MINUTE = 4  # Conservative for Fiverr

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_sellers: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield Fiverr Pro category page URLs."""
        # Main pro page
        yield f"{self.BASE_URL}/pro"

        # Category pages with pagination
        all_categories = CATEGORIES + SUB_CATEGORIES
        for category in all_categories:
            for page in range(1, MAX_PAGES + 1):
                if page == 1:
                    yield f"{self.BASE_URL}/{category}"
                else:
                    yield f"{self.BASE_URL}/{category}?page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a Fiverr Pro listing page."""
        soup = self.parse_html(html)
        contacts = []

        # Try to extract data from __NEXT_DATA__ JSON (Next.js app)
        next_data = soup.find("script", id="__NEXT_DATA__")
        if next_data and next_data.string:
            try:
                data = json.loads(next_data.string)
                contacts = self._parse_next_data(data, url)
                if contacts:
                    return contacts
            except (json.JSONDecodeError, KeyError):
                pass

        # Try JSON-LD structured data
        for script in soup.find_all("script", type="application/ld+json"):
            if script.string:
                try:
                    data = json.loads(script.string)
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        contact = self._parse_jsonld_seller(item, url)
                        if contact:
                            contacts.append(contact)
                except (json.JSONDecodeError, KeyError):
                    pass

        if contacts:
            return contacts

        # Parse seller cards from HTML
        for card in soup.find_all(class_=re.compile(
            r"seller|gig-card|listing|pro-card|provider", re.I
        )):
            contact = self._parse_seller_card(card, url)
            if contact:
                contacts.append(contact)

        # Look for seller profile links
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            # Fiverr seller profiles: /sellers/{username} or /{username}
            if re.match(r"^/sellers?/[a-zA-Z0-9_]+/?$", href):
                seller_url = f"{self.BASE_URL}{href}"
                username = href.rstrip("/").split("/")[-1]
                if username not in self._seen_sellers:
                    self._seen_sellers.add(username)
                    seller_html = self.fetch_page(seller_url)
                    if seller_html:
                        contact = self._parse_seller_profile(seller_url, seller_html)
                        if contact:
                            contacts.append(contact)

        return contacts

    def _parse_next_data(self, data: dict, source_url: str) -> list[ScrapedContact]:
        """Extract seller data from Next.js __NEXT_DATA__."""
        contacts = []
        props = data.get("props", {}).get("pageProps", {})

        # Navigate to gig/seller listings in the data tree
        listings = (
            props.get("listings", [])
            or props.get("gigs", [])
            or props.get("results", [])
            or props.get("sellers", [])
        )

        for item in listings:
            seller = item.get("seller", item)
            username = (seller.get("username") or seller.get("seller_name") or "").strip()
            display_name = (seller.get("display_name") or seller.get("name") or "").strip()
            name = display_name or username

            if not name or len(name) < 2:
                continue

            if username:
                if username.lower() in self._seen_sellers:
                    continue
                self._seen_sellers.add(username.lower())

            country = (seller.get("country") or "").strip()
            level = (seller.get("level") or seller.get("seller_level") or "").strip()
            title = (item.get("title") or item.get("gig_title") or "").strip()

            bio_parts = ["Fiverr Pro"]
            if level:
                bio_parts.append(f"Level: {level}")
            if country:
                bio_parts.append(country)
            if title:
                bio_parts.append(title[:200])

            profile_url = f"{self.BASE_URL}/{username}" if username else source_url

            contacts.append(ScrapedContact(
                name=name,
                email="",
                company="",
                website=profile_url,
                linkedin="",
                phone="",
                bio=" | ".join(bio_parts),
                source_url=source_url,
                source_category="freelance_pro",
            ))

        return contacts

    def _parse_jsonld_seller(self, data: dict, source_url: str) -> ScrapedContact | None:
        """Parse JSON-LD seller data."""
        data_type = data.get("@type", "")
        if data_type not in ("Person", "Organization", "Service", "Product", "Offer"):
            return None

        name = (data.get("name") or "").strip()
        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_sellers:
            return None
        self._seen_sellers.add(name_key)

        description = (data.get("description") or "")[:500]
        url = (data.get("url") or "").strip()

        return ScrapedContact(
            name=name,
            email="",
            company="",
            website=url or source_url,
            linkedin="",
            phone="",
            bio=f"Fiverr Pro | {description}" if description else "Fiverr Pro Seller",
            source_url=source_url,
            source_category="freelance_pro",
        )

    def _parse_seller_card(self, card, source_url: str) -> ScrapedContact | None:
        """Parse a seller card element."""
        name = ""

        for tag in ["h3", "h2", "h4", "strong", "span"]:
            el = card.find(tag, class_=re.compile(r"name|seller|username", re.I))
            if el:
                name = el.get_text(strip=True)
                if name and len(name) > 1:
                    break
                name = ""

        if not name:
            for tag in ["h3", "h2", "h4"]:
                el = card.find(tag)
                if el:
                    name = el.get_text(strip=True)
                    if name and len(name) > 1 and len(name) < 100:
                        break
                    name = ""

        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_sellers:
            return None
        self._seen_sellers.add(name_key)

        # Profile link
        profile_url = ""
        for a in card.find_all("a", href=True):
            href = a.get("href", "")
            if "/seller" in href or re.match(r"^/[a-zA-Z0-9_]+$", href):
                profile_url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                break

        card_text = card.get_text(separator=" | ", strip=True)
        bio = f"Fiverr Pro | {card_text[:400]}" if card_text else "Fiverr Pro Seller"

        return ScrapedContact(
            name=name,
            email="",
            company="",
            website=profile_url or source_url,
            linkedin="",
            phone="",
            bio=bio,
            source_url=source_url,
            source_category="freelance_pro",
        )

    def _parse_seller_profile(self, url: str, html: str) -> ScrapedContact | None:
        """Parse a Fiverr seller profile page."""
        soup = self.parse_html(html)

        name = ""
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        if not name:
            og = soup.find("meta", property="og:title")
            if og:
                name = (og.get("content") or "").split("|")[0].split("-")[0].strip()

        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_sellers:
            return None
        self._seen_sellers.add(name_key)

        bio = ""
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            bio = (meta_desc.get("content") or "")[:500]

        return ScrapedContact(
            name=name,
            email="",
            company="",
            website=url,
            linkedin="",
            phone="",
            bio=f"Fiverr Pro | {bio}" if bio else "Fiverr Pro Seller",
            source_url=url,
            source_category="freelance_pro",
        )
