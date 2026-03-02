"""
Slack App Directory scraper.

The Slack App Directory (slack.com/apps) lists 2,500+ integrations
built on the Slack platform. The directory organizes apps by category
and provides company/developer info for each listing.

Uses the public browse pages which are server-rendered HTML with
app cards containing:
- App name, developer/company name
- Description and tagline
- Category tags
- Website links (via detail pages)

Also tries the API-like endpoints that the directory frontend uses
for fetching app data.

Estimated yield: 2,000-3,000 SaaS companies
"""

from __future__ import annotations

import json
import re
from typing import Iterator, Optional
from urllib.parse import urlencode, quote_plus

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Slack app directory categories (from the browse sidebar)
CATEGORIES = [
    "analytics", "communication", "customer-support", "design",
    "developer-tools", "file-management", "finance", "health-wellness",
    "hr", "marketing", "office-management", "productivity",
    "project-management", "sales", "security-compliance",
    "social-fun", "travel",
]

# Additional search terms for broader coverage
SEARCH_TERMS = [
    "CRM", "automation", "workflow", "notifications", "bot",
    "calendar", "email", "survey", "tracking", "dashboard",
    "reporting", "integration", "sync", "backup", "compliance",
    "onboarding", "feedback", "scheduling", "billing", "invoicing",
    "helpdesk", "ticketing", "knowledge base", "documentation",
    "video", "meeting", "standup", "retrospective", "OKR",
]


class Scraper(BaseScraper):
    """Slack App Directory scraper.

    Browses category pages and search results to extract app
    and developer company information.
    """

    SOURCE_NAME = "slack_app_directory"
    BASE_URL = "https://slack.com"
    REQUESTS_PER_MINUTE = 8

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_apps: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield Slack app directory category and search URLs."""
        # Category browse pages
        for category in CATEGORIES:
            # Each category can have multiple pages
            for page in range(1, 11):
                yield f"{self.BASE_URL}/apps/category/{category}?page={page}"

        # Search pages for additional coverage
        for term in SEARCH_TERMS:
            yield f"{self.BASE_URL}/apps/search?q={quote_plus(term)}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Slack app directory page for app listings."""
        contacts = []

        # Try embedded JSON data first
        json_contacts = self._try_json_extraction(html, url)
        if json_contacts:
            return json_contacts

        # Parse HTML listing cards
        soup = self.parse_html(html)

        # Look for app cards - Slack uses various CSS class patterns
        cards = (
            soup.select("[class*='app_card']")
            or soup.select("[class*='AppCard']")
            or soup.select("[class*='app-card']")
            or soup.select("[data-app-id]")
            or soup.select(".app_row")
            or soup.select("[class*='search_result']")
        )

        # If no cards found, try broader selectors
        if not cards:
            cards = soup.select("a[href*='/apps/']")
            cards = [c.parent for c in cards if c.parent and c.parent.name in ('div', 'li', 'article')]

        for card in cards:
            contact = self._parse_card(card, url)
            if contact:
                contacts.append(contact)

        return contacts

    def _try_json_extraction(self, html: str, url: str) -> list[ScrapedContact]:
        """Try to extract app data from embedded JSON/script tags."""
        contacts = []

        # Look for __NEXT_DATA__ (Next.js)
        match = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
        if match:
            try:
                data = json.loads(match.group(1))
                props = data.get("props", {}).get("pageProps", {})
                apps = props.get("apps") or props.get("results") or props.get("items") or []
                for app in apps:
                    contact = self._parse_app_json(app, url)
                    if contact:
                        contacts.append(contact)
                if contacts:
                    return contacts
            except json.JSONDecodeError:
                pass

        # Look for window.__data or similar
        for pattern in [
            r'window\.__data\s*=\s*({.*?});',
            r'window\.__INITIAL_DATA__\s*=\s*({.*?});',
            r'"apps"\s*:\s*(\[.*?\])\s*[,}]',
        ]:
            match = re.search(pattern, html, re.DOTALL)
            if match:
                try:
                    data = json.loads(match.group(1))
                    if isinstance(data, dict):
                        apps = data.get("apps") or data.get("results") or []
                    elif isinstance(data, list):
                        apps = data
                    else:
                        continue
                    for app in apps:
                        contact = self._parse_app_json(app, url)
                        if contact:
                            contacts.append(contact)
                    if contacts:
                        return contacts
                except json.JSONDecodeError:
                    continue

        return contacts

    def _parse_app_json(self, app: dict, url: str) -> Optional[ScrapedContact]:
        """Parse an app dict from JSON data."""
        if not isinstance(app, dict):
            return None

        name = (app.get("name") or app.get("app_name") or "").strip()
        if not name or len(name) < 2:
            return None

        app_id = (app.get("id") or app.get("app_id") or name.lower()).strip()
        if app_id in self._seen_apps:
            return None
        self._seen_apps.add(app_id)

        company = (app.get("developer_name") or app.get("company") or app.get("developer") or name).strip()
        website = (app.get("website") or app.get("developer_url") or app.get("app_url") or "").strip()
        description = (app.get("description") or app.get("short_description") or app.get("tagline") or "").strip()

        # Categories
        categories = app.get("categories") or app.get("tags") or []
        if isinstance(categories, list):
            cat_names = []
            for c in categories:
                if isinstance(c, str):
                    cat_names.append(c)
                elif isinstance(c, dict):
                    cat_names.append((c.get("name") or c.get("label") or "").strip())
            categories = [c for c in cat_names if c]

        slug = (app.get("slug") or app.get("app_directory_slug") or "").strip()
        listing_url = f"{self.BASE_URL}/apps/{slug}" if slug else url

        # Build bio
        bio_parts = []
        if description:
            bio_parts.append(description[:500])
        if company and company != name:
            bio_parts.append(f"By {company}")
        if categories:
            bio_parts.append(f"Categories: {', '.join(categories[:5])}")
        if not bio_parts:
            bio_parts.append("Slack App Directory listing")

        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=company,
            company=company,
            website=website or listing_url,
            bio=bio,
            source_url=listing_url,
            source_category="saas_marketplace",
            raw_data={
                "app_name": name,
                "app_id": app_id,
                "slug": slug,
                "categories": categories,
                "platform": "slack_app_directory",
            },
        )

    def _parse_card(self, card, url: str) -> Optional[ScrapedContact]:
        """Parse an HTML app card element."""
        # App name
        name_el = (
            card.select_one("h3")
            or card.select_one("h2")
            or card.select_one("[class*='name']")
            or card.select_one("[class*='title']")
            or card.select_one("strong")
        )
        if not name_el:
            return None

        name = name_el.get_text(strip=True)
        if not name or len(name) < 2:
            return None

        if name in self._seen_apps:
            return None
        self._seen_apps.add(name)

        # Developer/company
        dev_el = (
            card.select_one("[class*='developer']")
            or card.select_one("[class*='author']")
            or card.select_one("[class*='company']")
            or card.select_one("[class*='subtitle']")
        )
        company = dev_el.get_text(strip=True) if dev_el else name
        # Clean up "by CompanyName" prefix
        if company.lower().startswith("by "):
            company = company[3:].strip()

        # Listing URL
        link_el = card.select_one("a[href*='/apps/']")
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

        bio_parts = []
        if description:
            bio_parts.append(description[:500])
        if company and company != name:
            bio_parts.append(f"By {company}")
        if not bio_parts:
            bio_parts.append("Slack app")
        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=company,
            company=company,
            website=listing_url or url,
            bio=bio,
            source_url=listing_url or url,
            source_category="saas_marketplace",
            raw_data={
                "app_name": name,
                "platform": "slack_app_directory",
            },
        )
