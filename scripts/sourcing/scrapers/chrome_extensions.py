"""
Chrome Web Store Extensions scraper.

The Chrome Web Store hosts 100,000+ browser extensions and apps.
While there's no official public API, the store has internal APIs
used by the frontend for search and category browsing.

The Chrome Web Store search uses an API endpoint that returns
structured data about extensions including:
- Extension name and developer/company
- Description, category
- User count, rating, review count
- Developer website
- Featured/editor picks status

We use the search endpoint at:
  https://chrome.google.com/webstore/ajax/item

And also the sitemap at:
  https://chrome.google.com/webstore/sitemap

Estimated yield: 5,000-10,000+ extension developers/companies
"""

from __future__ import annotations

import json
import re
from typing import Iterator, Optional
from urllib.parse import urlencode, quote_plus

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Business-relevant search categories for Chrome extensions
SEARCH_TERMS = [
    # Productivity
    "productivity", "project management", "task manager",
    "time tracking", "calendar", "scheduling", "note taking",
    "todo list", "bookmark manager", "tab manager",
    # Business
    "crm", "sales", "marketing", "lead generation",
    "email tracking", "linkedin", "prospecting",
    "business", "enterprise", "startup",
    # Communication
    "email", "chat", "messaging", "video conference",
    "screen recording", "screenshot", "collaboration",
    # Development
    "developer tools", "web development", "api",
    "debugging", "testing", "code editor", "github",
    "json", "css", "javascript",
    # Marketing & SEO
    "seo", "analytics", "social media", "content marketing",
    "keyword research", "backlink", "page speed",
    "google analytics", "advertising",
    # Security & Privacy
    "security", "password manager", "vpn", "privacy",
    "ad blocker", "encryption",
    # E-commerce
    "ecommerce", "shopping", "price tracker", "coupon",
    "amazon", "dropshipping",
    # Design
    "design", "color picker", "screenshot", "image editor",
    "font", "wireframe", "mockup",
    # Writing
    "grammar", "writing", "translation", "spell check",
    "ai writer", "copywriting",
    # Finance
    "finance", "cryptocurrency", "stock market", "banking",
    "expense tracker", "invoice",
]

# Chrome Web Store categories
CATEGORIES = [
    "ext/10-blogging",
    "ext/15-by-google",
    "ext/12-shopping",
    "ext/11-web-development",
    "ext/1-communication",
    "ext/7-productivity",
    "ext/38-search-tools",
    "ext/13-sports",
    "ext/22-accessibility",
    "ext/6-news",
    "ext/14-fun",
    "ext/28-photos",
]


class Scraper(BaseScraper):
    """Chrome Web Store extension scraper.

    Searches by business-relevant terms and browses categories
    to extract extension developer/company information.
    """

    SOURCE_NAME = "chrome_extensions"
    BASE_URL = "https://chromewebstore.google.com"
    REQUESTS_PER_MINUTE = 8

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_ids: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield Chrome Web Store search and category URLs."""
        # Search by terms
        for term in SEARCH_TERMS:
            yield f"{self.BASE_URL}/search/{quote_plus(term)}"

        # Browse categories
        for category in CATEGORIES:
            yield f"{self.BASE_URL}/category/{category}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Chrome Web Store page for extension listings."""
        contacts = []

        # Try embedded JSON/structured data
        json_contacts = self._try_embedded_data(html, url)
        if json_contacts:
            return json_contacts

        # HTML parsing
        return self._parse_html_listings(html, url)

    def _try_embedded_data(self, html: str, url: str) -> list[ScrapedContact]:
        """Try to extract extension data from embedded page data."""
        contacts = []

        # The Chrome Web Store embeds extension data in script tags
        # Look for AF_initDataCallback or similar data patterns
        for pattern in [
            r'AF_initDataCallback\(\{[^}]*data:\s*(\[.*?\])\s*\}\)',
            r'window\.__PRELOADED_STATE__\s*=\s*({.*?});\s*</script>',
            r'<script[^>]*>var\s+_gaq\s.*?itemData\s*=\s*(\[.*?\]);',
        ]:
            for match in re.finditer(pattern, html, re.DOTALL):
                try:
                    data = json.loads(match.group(1))
                    parsed = self._parse_af_data(data, url)
                    if parsed:
                        contacts.extend(parsed)
                except json.JSONDecodeError:
                    continue

        if contacts:
            return contacts

        # Try JSON-LD
        for match in re.finditer(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
            html, re.DOTALL,
        ):
            try:
                data = json.loads(match.group(1))
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if isinstance(item, dict):
                        contact = self._json_ld_to_contact(item, url)
                        if contact:
                            contacts.append(contact)
            except json.JSONDecodeError:
                continue

        return contacts

    def _parse_af_data(self, data, url: str) -> list[ScrapedContact]:
        """Parse AF_initDataCallback data structure."""
        contacts = []

        if not isinstance(data, list):
            return contacts

        # The data structure varies; look for arrays containing extension info
        def extract_extensions(arr, depth=0):
            if depth > 5 or not isinstance(arr, list):
                return
            for item in arr:
                if isinstance(item, list) and len(item) >= 5:
                    # Check if this looks like an extension entry
                    # Typical structure: [id, name, ?, ?, description, ...]
                    possible_id = item[0] if len(item) > 0 else None
                    possible_name = item[1] if len(item) > 1 else None

                    if (
                        isinstance(possible_id, str)
                        and len(possible_id) == 32
                        and isinstance(possible_name, str)
                        and len(possible_name) > 1
                    ):
                        contact = self._array_to_contact(item, url)
                        if contact:
                            contacts.append(contact)
                    else:
                        extract_extensions(item, depth + 1)
                elif isinstance(item, list):
                    extract_extensions(item, depth + 1)

        extract_extensions(data)
        return contacts

    def _array_to_contact(self, arr: list, url: str) -> Optional[ScrapedContact]:
        """Convert an extension data array to ScrapedContact."""
        if len(arr) < 5:
            return None

        ext_id = str(arr[0]) if arr[0] else ""
        name = str(arr[1]) if arr[1] else ""

        if not name or len(name) < 2:
            return None
        if ext_id in self._seen_ids:
            return None
        self._seen_ids.add(ext_id or name)

        # Description is usually at index 6 or 4
        description = ""
        for idx in [6, 4, 5]:
            if idx < len(arr) and isinstance(arr[idx], str) and len(arr[idx]) > 10:
                description = arr[idx]
                break

        # Developer/author - look for it in various positions
        developer = ""
        for idx in [2, 3]:
            if idx < len(arr) and isinstance(arr[idx], str) and 2 < len(arr[idx]) < 100:
                developer = arr[idx]
                break

        company = developer or name
        website = f"{self.BASE_URL}/detail/{ext_id}" if ext_id else url

        # User count
        user_count = ""
        for idx in [7, 8, 9, 10]:
            if idx < len(arr):
                val = arr[idx]
                if isinstance(val, (int, float)) and val > 0:
                    user_count = f"{int(val):,} users"
                    break
                elif isinstance(val, str) and "user" in val.lower():
                    user_count = val
                    break

        bio_parts = []
        if description:
            bio_parts.append(description[:500])
        if developer and developer != name:
            bio_parts.append(f"By {developer}")
        if user_count:
            bio_parts.append(user_count)
        if not bio_parts:
            bio_parts.append("Chrome Web Store extension")

        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=company,
            company=company,
            website=website,
            bio=bio,
            source_url=website,
            source_category="browser_extensions",
            raw_data={
                "extension_name": name,
                "extension_id": ext_id,
                "platform": "chrome_web_store",
            },
        )

    def _json_ld_to_contact(self, item: dict, url: str) -> Optional[ScrapedContact]:
        """Convert JSON-LD to ScrapedContact."""
        name = (item.get("name") or "").strip()
        if not name or len(name) < 2:
            return None
        if name in self._seen_ids:
            return None
        self._seen_ids.add(name)

        author = item.get("author") or {}
        company = ""
        if isinstance(author, str):
            company = author
        elif isinstance(author, dict):
            company = (author.get("name") or "").strip()
        company = company or name

        website = (item.get("url") or "").strip()
        description = (item.get("description") or "").strip()

        aggregate = item.get("aggregateRating") or {}
        rating = aggregate.get("ratingValue") or 0
        review_count = aggregate.get("ratingCount") or 0

        bio_parts = []
        if description:
            bio_parts.append(description[:500])
        if rating:
            bio_parts.append(f"Rating: {rating}/5")
        if review_count:
            bio_parts.append(f"{review_count} ratings")
        if not bio_parts:
            bio_parts.append("Chrome Web Store extension")
        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=company,
            company=company,
            website=website or url,
            bio=bio,
            source_url=website or url,
            source_category="browser_extensions",
            raw_data={
                "extension_name": name,
                "rating": rating,
                "review_count": review_count,
                "platform": "chrome_web_store",
            },
        )

    def _parse_html_listings(self, html: str, url: str) -> list[ScrapedContact]:
        """Fallback HTML parsing for Chrome Web Store pages."""
        soup = self.parse_html(html)
        contacts = []

        # Extension cards - Chrome Web Store uses various class names
        cards = (
            soup.select("[class*='a-na-d-K']")           # Extension cards
            or soup.select("[class*='webstore-']")
            or soup.select("[class*='extension-card']")
            or soup.select("[class*='ItemCard']")
            or soup.select("a[href*='/detail/']")
        )

        # If we got raw links, wrap them
        if cards and cards[0].name == 'a':
            cards = [c.parent for c in cards if c.parent and c.parent.name in ('div', 'li', 'article')]

        for card in cards:
            name_el = (
                card.select_one("h2")
                or card.select_one("h3")
                or card.select_one("[class*='name']")
                or card.select_one("[class*='title']")
                or card.select_one("[role='heading']")
            )
            if not name_el:
                continue

            name = name_el.get_text(strip=True)
            if not name or len(name) < 2:
                continue
            if name in self._seen_ids:
                continue
            self._seen_ids.add(name)

            # Developer
            dev_el = (
                card.select_one("[class*='developer']")
                or card.select_one("[class*='author']")
                or card.select_one("[class*='publisher']")
            )
            company = dev_el.get_text(strip=True) if dev_el else name

            # Link
            link_el = card.select_one("a[href*='/detail/']")
            listing_url = ""
            if link_el:
                href = link_el.get("href", "")
                if href.startswith("/"):
                    listing_url = f"{self.BASE_URL}{href}"
                elif href.startswith("http"):
                    listing_url = href

            # Description
            desc_el = card.select_one("[class*='description']") or card.select_one("p")
            description = desc_el.get_text(strip=True) if desc_el else ""

            # User count
            users_el = card.select_one("[class*='users']") or card.select_one("[class*='installs']")
            users = users_el.get_text(strip=True) if users_el else ""

            bio_parts = []
            if description:
                bio_parts.append(description[:500])
            if users:
                bio_parts.append(users)
            if not bio_parts:
                bio_parts.append("Chrome extension")
            bio = " | ".join(bio_parts)

            contacts.append(ScrapedContact(
                name=company,
                company=company,
                website=listing_url or url,
                bio=bio,
                source_url=listing_url or url,
                source_category="browser_extensions",
                raw_data={
                    "extension_name": name,
                    "platform": "chrome_web_store",
                },
            ))

        return contacts
