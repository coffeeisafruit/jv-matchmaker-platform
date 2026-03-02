"""
WordPress Plugins Directory scraper.

Uses the official WordPress.org Plugin API at:
  https://api.wordpress.org/plugins/info/1.2/?action=query_plugins

This is a well-documented, free, public API that returns structured
JSON with full plugin and author details.

Data includes:
- Plugin name, slug, version
- Author name, author profile URL
- Description, short description
- Active installs, download count
- Rating, number of ratings
- Tags/categories
- Homepage URL (often the company website)

Estimated yield: 10,000-50,000+ plugin authors/companies
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Iterator, Optional
from urllib.parse import urlencode

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Search terms for business-relevant plugins
SEARCH_TERMS = [
    "business", "ecommerce", "woocommerce", "marketing",
    "seo", "analytics", "crm", "email", "forms",
    "payment", "booking", "scheduling", "membership",
    "subscription", "invoicing", "accounting", "hr",
    "project management", "social media", "automation",
    "integration", "api", "dashboard", "reporting",
    "security", "backup", "cache", "performance",
    "contact form", "popup", "landing page", "slider",
    "gallery", "video", "survey", "quiz",
    "chat", "support", "helpdesk", "ticketing",
    "affiliate", "referral", "loyalty", "rewards",
    "shipping", "inventory", "warehouse", "pos",
    "lms", "course", "learning", "education",
    "event", "calendar", "appointment", "restaurant",
    "real estate", "hotel", "property", "listing",
    "directory", "classified", "job board", "recruitment",
    "donation", "fundraising", "nonprofit", "church",
]

# Browse by tag for additional coverage
BROWSE_TAGS = [
    "woocommerce", "widget", "admin", "post",
    "page", "sidebar", "social", "shortcode",
    "editor", "image", "menu", "login",
    "custom-post-type", "comments", "media",
    "spam", "google", "twitter", "facebook",
    "elementor", "gutenberg", "block",
]


class Scraper(BaseScraper):
    """WordPress.org Plugin Directory scraper.

    Uses the official Plugin API for structured JSON data.
    Searches by term and browses by tag to maximize coverage.
    """

    SOURCE_NAME = "wordpress_plugins"
    BASE_URL = "https://wordpress.org"
    API_URL = "https://api.wordpress.org/plugins/info/1.2/"
    REQUESTS_PER_MINUTE = 20  # WP API is generous

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_slugs: set[str] = set()
        self._seen_authors: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run() for API-based fetching."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used -- parsing is done inline in run()."""
        return []

    def _fetch_plugins_page(
        self,
        search: str = "",
        tag: str = "",
        page: int = 1,
        per_page: int = 100,
    ) -> Optional[dict]:
        """Fetch a page of plugins from the WordPress.org API."""
        params = {
            "action": "query_plugins",
            "request[per_page]": per_page,
            "request[page]": page,
            "request[fields][description]": "1",
            "request[fields][short_description]": "1",
            "request[fields][author]": "1",
            "request[fields][author_profile]": "1",
            "request[fields][homepage]": "1",
            "request[fields][rating]": "1",
            "request[fields][num_ratings]": "1",
            "request[fields][active_installs]": "1",
            "request[fields][downloaded]": "1",
            "request[fields][tags]": "1",
            "request[fields][requires]": "0",
            "request[fields][tested]": "0",
            "request[fields][compatibility]": "0",
            "request[fields][sections]": "0",
            "request[fields][screenshots]": "0",
            "request[fields][banners]": "0",
            "request[fields][icons]": "0",
        }

        if search:
            params["request[search]"] = search
        elif tag:
            params["request[tag]"] = tag
        else:
            # Browse popular
            params["request[browse]"] = "popular"

        url = f"{self.API_URL}?{urlencode(params)}"

        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        try:
            resp = self.session.get(url, timeout=30, headers={
                "Accept": "application/json",
            })
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1
            return resp.json()
        except Exception as exc:
            self.logger.warning("API fetch failed: %s", exc)
            self.stats["errors"] += 1
            return None

    def _plugin_to_contact(self, plugin: dict) -> Optional[ScrapedContact]:
        """Convert a plugin dict to a ScrapedContact."""
        name = (plugin.get("name") or "").strip()
        # Clean HTML entities from name
        if "&" in name:
            from html import unescape
            name = unescape(name)

        if not name or len(name) < 2:
            return None

        slug = (plugin.get("slug") or "").strip()
        if slug in self._seen_slugs:
            return None
        self._seen_slugs.add(slug)

        # Author info
        author = (plugin.get("author") or "").strip()
        # Author field often has HTML: <a href="...">Author Name</a>
        if "<" in author:
            # Extract text from HTML
            import re
            author_text = re.sub(r'<[^>]+>', '', author).strip()
            if author_text:
                author = author_text

        author_profile = (plugin.get("author_profile") or "").strip()

        # Homepage (company website)
        homepage = (plugin.get("homepage") or "").strip()

        # Description
        short_desc = (plugin.get("short_description") or "").strip()
        # Clean HTML
        if "<" in short_desc:
            import re
            short_desc = re.sub(r'<[^>]+>', '', short_desc).strip()

        # Stats
        active_installs = plugin.get("active_installs") or 0
        downloaded = plugin.get("downloaded") or 0
        rating = plugin.get("rating") or 0  # Out of 100
        num_ratings = plugin.get("num_ratings") or 0

        # Tags
        tags = plugin.get("tags") or {}
        if isinstance(tags, dict):
            tag_names = list(tags.values())
        elif isinstance(tags, list):
            tag_names = [t if isinstance(t, str) else (t.get("name") or "") for t in tags]
        else:
            tag_names = []

        # Company name - use author as company, plugin name as product
        company = author if author else name

        # Website - prefer homepage, then author profile
        website = homepage or author_profile or ""
        if website and not website.startswith("http"):
            website = f"https://{website}"

        plugin_url = f"https://wordpress.org/plugins/{slug}/" if slug else ""

        # Normalize rating to /5
        rating_5 = round(rating / 20, 1) if rating else 0

        # Build bio
        bio_parts = []
        if short_desc:
            bio_parts.append(short_desc[:500])
        if active_installs:
            bio_parts.append(f"{active_installs:,}+ active installs")
        if rating_5:
            bio_parts.append(f"Rating: {rating_5}/5 ({num_ratings} ratings)")
        if tag_names:
            bio_parts.append(f"Tags: {', '.join(tag_names[:5])}")
        if not bio_parts:
            bio_parts.append("WordPress plugin")

        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=company,
            company=company,
            website=website or plugin_url,
            bio=bio,
            source_url=plugin_url,
            source_category="wordpress_plugins",
            raw_data={
                "plugin_name": name,
                "slug": slug,
                "active_installs": active_installs,
                "downloaded": downloaded,
                "rating": rating_5,
                "num_ratings": num_ratings,
                "tags": tag_names,
                "author_profile": author_profile,
                "homepage": homepage,
                "platform": "wordpress_plugins",
            },
        )

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Fetch plugins via the WordPress.org API.

        Searches by terms and browses by tags to maximize coverage.
        """
        self.logger.info("Starting %s scraper", self.SOURCE_NAME)

        contacts_yielded = 0
        pages_done = 0

        # Resume from checkpoint
        start_from_term = ""
        start_from_page = 1
        if checkpoint:
            start_from_term = checkpoint.get("last_term", "")
            start_from_page = checkpoint.get("last_page", 1)

        past_checkpoint = not start_from_term

        # Phase 1: Search by terms
        for term in SEARCH_TERMS:
            if not past_checkpoint:
                if term == start_from_term:
                    past_checkpoint = True
                else:
                    continue

            for page in range(1, 11):  # Up to 10 pages per search term
                if not past_checkpoint and page < start_from_page:
                    continue
                past_checkpoint = True

                data = self._fetch_plugins_page(search=term, page=page)
                if not data:
                    break

                plugins = data.get("plugins") or []
                if not plugins:
                    break

                total_pages = data.get("info", {}).get("pages") or 1

                for plugin in plugins:
                    if not isinstance(plugin, dict):
                        continue

                    contact = self._plugin_to_contact(plugin)
                    if not contact:
                        continue

                    contact.source_platform = self.SOURCE_NAME
                    contact.scraped_at = datetime.now().isoformat()
                    contact.email = contact.clean_email()

                    self.stats["contacts_found"] += 1

                    if contact.is_valid():
                        self.stats["contacts_valid"] += 1
                        contacts_yielded += 1
                        yield contact

                        if max_contacts and contacts_yielded >= max_contacts:
                            self.logger.info("Reached max_contacts=%d", max_contacts)
                            return

                pages_done += 1
                if pages_done % 10 == 0:
                    self.logger.info(
                        "Progress: %d pages, %d valid contacts (search: '%s')",
                        pages_done, self.stats["contacts_valid"], term,
                    )

                if max_pages and pages_done >= max_pages:
                    self.logger.info("Reached max_pages=%d", max_pages)
                    self.logger.info("Scraper complete: %s", self.stats)
                    return

                if page >= total_pages:
                    break

        # Phase 2: Browse by tag
        for tag in BROWSE_TAGS:
            for page in range(1, 6):
                data = self._fetch_plugins_page(tag=tag, page=page)
                if not data:
                    break

                plugins = data.get("plugins") or []
                if not plugins:
                    break

                total_pages = data.get("info", {}).get("pages") or 1

                for plugin in plugins:
                    if not isinstance(plugin, dict):
                        continue

                    contact = self._plugin_to_contact(plugin)
                    if not contact:
                        continue

                    contact.source_platform = self.SOURCE_NAME
                    contact.scraped_at = datetime.now().isoformat()
                    contact.email = contact.clean_email()

                    self.stats["contacts_found"] += 1

                    if contact.is_valid():
                        self.stats["contacts_valid"] += 1
                        contacts_yielded += 1
                        yield contact

                        if max_contacts and contacts_yielded >= max_contacts:
                            self.logger.info("Reached max_contacts=%d", max_contacts)
                            return

                pages_done += 1

                if max_pages and pages_done >= max_pages:
                    self.logger.info("Reached max_pages=%d", max_pages)
                    self.logger.info("Scraper complete: %s", self.stats)
                    return

                if page >= total_pages:
                    break

        self.logger.info("Scraper complete: %s", self.stats)
