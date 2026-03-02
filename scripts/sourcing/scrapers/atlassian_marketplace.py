"""
Atlassian Marketplace scraper.

The Atlassian Marketplace (marketplace.atlassian.com) hosts 6,000+
apps for Jira, Confluence, Bitbucket, and Trello.

Uses the public REST API at:
  https://marketplace.atlassian.com/rest/2/addons

The API returns structured JSON with pagination and supports filtering
by hosting type, category, and text search. No authentication required.

Data includes:
- App name, vendor/company name, vendor website
- Description, tagline, categories
- Download/install counts, star ratings
- Hosting type (Cloud, Server, Data Center)

Estimated yield: 5,000-6,000 SaaS vendor companies
"""

from __future__ import annotations

import json
from typing import Iterator, Optional
from urllib.parse import urlencode

from scripts.sourcing.base import BaseScraper, ScrapedContact


class Scraper(BaseScraper):
    """Atlassian Marketplace scraper using the public REST API.

    Paginates through all addons, extracting vendor company data
    from each listing.
    """

    SOURCE_NAME = "atlassian_marketplace"
    BASE_URL = "https://marketplace.atlassian.com"
    API_URL = "https://marketplace.atlassian.com/rest/2/addons"
    REQUESTS_PER_MINUTE = 15
    PAGE_SIZE = 50  # API supports up to 50

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_keys: set[str] = set()
        self._seen_vendors: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run() for API-based pagination."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used -- parsing is done inline in run()."""
        return []

    def _fetch_addons_page(self, offset: int = 0) -> Optional[dict]:
        """Fetch a single page of addons from the REST API."""
        params = {
            "offset": offset,
            "limit": self.PAGE_SIZE,
            "hosting": "cloud",
        }
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
            self.logger.warning("API fetch failed at offset %d: %s", offset, exc)
            self.stats["errors"] += 1
            return None

    def _fetch_addon_detail(self, addon_key: str) -> Optional[dict]:
        """Fetch detailed info for a single addon."""
        url = f"{self.API_URL}/{addon_key}"

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
            self.logger.debug("Detail fetch failed for %s: %s", addon_key, exc)
            self.stats["errors"] += 1
            return None

    def _addon_to_contact(self, addon: dict) -> Optional[ScrapedContact]:
        """Convert an addon dict to a ScrapedContact."""
        name = (addon.get("name") or "").strip()
        if not name:
            return None

        addon_key = (addon.get("key") or "").strip()
        if addon_key in self._seen_keys:
            return None
        self._seen_keys.add(addon_key)

        # Vendor info
        vendor = addon.get("vendor") or addon.get("_embedded", {}).get("vendor") or {}
        vendor_name = (vendor.get("name") or "").strip()
        vendor_url = ""

        # Try vendor links
        vendor_links = vendor.get("_links") or {}
        if isinstance(vendor_links, dict):
            alternate = vendor_links.get("alternate") or {}
            if isinstance(alternate, dict):
                vendor_url = (alternate.get("href") or "").strip()
            elif isinstance(alternate, list) and alternate:
                vendor_url = (alternate[0].get("href") or "").strip()

        # App info
        summary = (addon.get("summary") or addon.get("tagLine") or "").strip()

        # Links
        links = addon.get("_links") or {}
        app_url = ""
        alternate = links.get("alternate") or {}
        if isinstance(alternate, dict):
            app_url = (alternate.get("href") or "").strip()

        # Categories
        categories = addon.get("categories") or []
        cat_names = []
        for cat in categories:
            if isinstance(cat, str):
                cat_names.append(cat)
            elif isinstance(cat, dict):
                cat_names.append((cat.get("name") or cat.get("key") or "").strip())
        cat_names = [c for c in cat_names if c]

        # Hosting
        hosting = addon.get("hosting") or {}
        hosting_type = ""
        if isinstance(hosting, dict):
            hosting_type = (hosting.get("cloud") or hosting.get("server") or "").strip() if isinstance(hosting.get("cloud"), str) else ""
        elif isinstance(hosting, str):
            hosting_type = hosting

        # Distribution info
        distribution = addon.get("distribution") or {}
        downloads = distribution.get("totalDownloads") or distribution.get("downloads") or 0
        installs = distribution.get("totalInstalls") or distribution.get("installs") or 0

        # Use vendor name as the company
        company = vendor_name or name
        # Use vendor company dedup if we already have this vendor
        vendor_key = vendor_name.lower() if vendor_name else ""
        if vendor_key and vendor_key in self._seen_vendors:
            # Still yield since different apps might have more info
            pass
        if vendor_key:
            self._seen_vendors.add(vendor_key)

        listing_url = app_url or (f"{self.BASE_URL}/apps/{addon_key}" if addon_key else "")
        website = vendor_url or listing_url

        # Build bio
        bio_parts = []
        if summary:
            bio_parts.append(summary[:500])
        if cat_names:
            bio_parts.append(f"Categories: {', '.join(cat_names[:5])}")
        if downloads:
            bio_parts.append(f"{downloads:,} downloads")
        elif installs:
            bio_parts.append(f"{installs:,} installs")
        if hosting_type:
            bio_parts.append(f"Hosting: {hosting_type}")
        if not bio_parts:
            bio_parts.append("Atlassian Marketplace app")

        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=company,
            company=company,
            website=website,
            bio=bio,
            source_url=listing_url,
            source_category="saas_marketplace",
            raw_data={
                "addon_key": addon_key,
                "app_name": name,
                "vendor_name": vendor_name,
                "categories": cat_names,
                "downloads": downloads,
                "installs": installs,
                "hosting_type": hosting_type,
                "platform": "atlassian_marketplace",
            },
        )

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Paginate through the Atlassian Marketplace REST API.

        Fetches addons in batches of 50, extracting vendor company data.
        """
        from datetime import datetime

        self.logger.info("Starting %s scraper", self.SOURCE_NAME)

        # Resume from checkpoint
        start_offset = 0
        if checkpoint:
            start_offset = checkpoint.get("last_offset", 0)
            self.logger.info("Resuming from offset %d", start_offset)

        offset = start_offset
        contacts_yielded = 0
        pages_done = 0
        empty_pages = 0

        while True:
            data = self._fetch_addons_page(offset=offset)
            if not data:
                empty_pages += 1
                if empty_pages >= 3:
                    self.logger.info("Too many consecutive failures, stopping")
                    break
                offset += self.PAGE_SIZE
                continue

            empty_pages = 0

            # Handle different API response formats
            addons = []
            if isinstance(data, dict):
                # Could be _embedded format or direct list
                embedded = data.get("_embedded") or {}
                addons = embedded.get("addons") or data.get("addons") or data.get("plugins") or []
                if not addons and "results" in data:
                    addons = data["results"]
            elif isinstance(data, list):
                addons = data

            if not addons:
                self.logger.info("No more addons found at offset %d", offset)
                break

            self.logger.info("Fetched %d addons at offset %d", len(addons), offset)

            for addon in addons:
                if not isinstance(addon, dict):
                    continue

                contact = self._addon_to_contact(addon)
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
            offset += self.PAGE_SIZE

            if pages_done % 10 == 0:
                self.logger.info(
                    "Progress: %d pages, %d valid contacts",
                    pages_done, self.stats["contacts_valid"],
                )

            if max_pages and pages_done >= max_pages:
                self.logger.info("Reached max_pages=%d", max_pages)
                break

            # Check if we've reached the end
            total = 0
            if isinstance(data, dict):
                total = data.get("count") or data.get("total") or data.get("totalSize") or 0
            if total and offset >= total:
                self.logger.info("Reached end of results (total=%d)", total)
                break

        self.logger.info("Scraper complete: %s", self.stats)
