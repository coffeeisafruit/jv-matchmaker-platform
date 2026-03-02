"""
Founders Fund Portfolio Scraper

Fetches portfolio companies from Founders Fund's WordPress-based website.
The site exposes data in multiple ways:
1. WordPress REST API: /wp-json/wp/v2/company
2. Embedded window.__data JavaScript object
3. HTML company cards on the portfolio page

Data includes:
- Company name, description, industry
- Founder names
- Website and social links

Source: https://foundersfund.com/portfolio/
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# WordPress REST API endpoint for company post type
WP_API_URL = "https://foundersfund.com/wp-json/wp/v2/company"
WP_API_PER_PAGE = 100


class Scraper(BaseScraper):
    SOURCE_NAME = "founders_fund"
    BASE_URL = "https://foundersfund.com/portfolio/"
    REQUESTS_PER_MINUTE = 10

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_names: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run()."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Founders Fund portfolio page for company data.

        Tries multiple extraction strategies:
        1. window.__data JSON object
        2. HTML company cards
        3. Links to /company/ pages
        """
        contacts = []

        # Strategy 1: Extract from window.__data
        data_match = re.search(r"window\.__data\s*=\s*({.*?})\s*;", html, re.DOTALL)
        if data_match:
            try:
                data = json.loads(data_match.group(1))
                companies = data.get("companies", data.get("portfolio", []))
                if isinstance(companies, list):
                    for item in companies:
                        contact = self._item_to_contact(item)
                        if contact:
                            contacts.append(contact)
                    if contacts:
                        return contacts
            except (json.JSONDecodeError, ValueError):
                pass

        # Strategy 2: Parse HTML
        soup = self.parse_html(html)

        # Look for company cards or links
        company_links = soup.find_all("a", href=re.compile(r"/portfolio/[a-z0-9\-]+/?$"))
        if not company_links:
            company_links = soup.find_all("a", href=re.compile(r"/company/[a-z0-9\-]+/?$"))

        for link in company_links:
            href = (link.get("href") or "").strip()
            slug = href.rstrip("/").split("/")[-1]

            if not slug or slug in ("portfolio", "company", ""):
                continue

            # Get company name
            name = ""
            name_tag = link.find(["h2", "h3", "h4", "h5", "span"])
            if name_tag:
                name = name_tag.get_text(strip=True)
            if not name:
                name = link.get_text(strip=True)
            if not name:
                name = slug.replace("-", " ").title()

            if not name or len(name) < 2:
                continue

            # Skip navigation
            if name.lower() in ("portfolio", "all", "filter", "more",
                                "view all", "see more"):
                continue

            name_lower = name.lower()
            if name_lower in self._seen_names:
                continue
            self._seen_names.add(name_lower)

            # Extract description
            description = ""
            parent = link.parent
            if parent:
                desc_tag = parent.find("p")
                if desc_tag:
                    description = desc_tag.get_text(strip=True)

            # Extract image/logo
            img = link.find("img")
            logo_url = ""
            if img:
                logo_url = (img.get("src") or "").strip()

            source_url = href
            if not source_url.startswith("http"):
                source_url = f"https://foundersfund.com{href}"

            bio_parts = []
            if description:
                bio_parts.append(description)
            bio_parts.append("Founders Fund portfolio company.")
            bio = " ".join(bio_parts)

            contact = ScrapedContact(
                name=name,
                email="",
                company=name,
                website=source_url,
                linkedin="",
                phone="",
                bio=bio,
                source_platform=self.SOURCE_NAME,
                source_url=source_url,
                source_category="vc_portfolio",
                raw_data={
                    "slug": slug,
                    "description": description,
                    "logo_url": logo_url,
                    "vc_firm": "Founders Fund",
                },
            )
            contacts.append(contact)

        return contacts

    def _item_to_contact(self, item: dict) -> Optional[ScrapedContact]:
        """Convert a JSON company object to ScrapedContact."""
        if not isinstance(item, dict):
            return None

        name = (
            item.get("title") or item.get("name") or item.get("company_name") or ""
        ).strip()

        if not name or len(name) < 2:
            return None

        name_lower = name.lower()
        if name_lower in self._seen_names:
            return None
        self._seen_names.add(name_lower)

        # Handle rendered title (WordPress format)
        if isinstance(name, dict):
            name = (name.get("rendered") or "").strip()

        # Clean HTML from title
        name = re.sub(r"<[^>]+>", "", name).strip()
        if not name:
            return None

        description = (item.get("content") or item.get("description") or "").strip()
        # Clean HTML from description
        description = re.sub(r"<[^>]+>", "", description).strip()

        website = (item.get("url") or item.get("website") or "").strip()
        industry = (item.get("industry") or "").strip()
        slug = (item.get("slug") or "").strip()

        # Extract founders
        founders = item.get("founders", [])
        founder_names = []
        if isinstance(founders, list):
            for f in founders:
                if isinstance(f, dict):
                    fname = (f.get("name") or f.get("full_name") or "").strip()
                    if fname:
                        founder_names.append(fname)
                elif isinstance(f, str):
                    founder_names.append(f.strip())

        source_url = f"https://foundersfund.com/portfolio/{slug}/" if slug else self.BASE_URL

        bio_parts = []
        if description:
            bio_parts.append(description[:500])
        bio_parts.append("Founders Fund portfolio company.")
        if industry:
            bio_parts.append(f"Industry: {industry}.")
        if founder_names:
            bio_parts.append(f"Founders: {', '.join(founder_names)}.")
        bio = " ".join(bio_parts)

        return ScrapedContact(
            name=name,
            email="",
            company=name,
            website=website or source_url,
            linkedin="",
            phone="",
            bio=bio,
            source_platform=self.SOURCE_NAME,
            source_url=source_url,
            source_category="vc_portfolio",
            raw_data={
                "slug": slug,
                "industry": industry,
                "founders": founder_names,
                "vc_firm": "Founders Fund",
            },
        )

    def _fetch_wp_api(self, page: int = 1) -> list[dict]:
        """Fetch companies from WordPress REST API."""
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        params = {
            "per_page": WP_API_PER_PAGE,
            "page": page,
            "_fields": "id,title,slug,content,acf,meta,link",
        }

        try:
            resp = self.session.get(WP_API_URL, params=params, timeout=30)
            if resp.status_code == 400:
                # Try without _fields filter
                del params["_fields"]
                resp = self.session.get(WP_API_URL, params=params, timeout=30)
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1
            return resp.json()
        except Exception as exc:
            self.logger.debug("WP API failed (page %d): %s", page, exc)
            self.stats["errors"] += 1
            return []

    def _wp_item_to_contact(self, item: dict) -> Optional[ScrapedContact]:
        """Convert a WordPress REST API company object to ScrapedContact."""
        if not isinstance(item, dict):
            return None

        # WordPress wraps title in {rendered: "..."}
        title_obj = item.get("title", {})
        if isinstance(title_obj, dict):
            name = (title_obj.get("rendered") or "").strip()
        else:
            name = str(title_obj).strip()

        # Clean HTML entities
        name = (
            name
            .replace("&amp;", "&")
            .replace("&#8217;", "'")
            .replace("&#8216;", "'")
            .replace("&quot;", '"')
            .replace("&#038;", "&")
        )
        name = re.sub(r"<[^>]+>", "", name).strip()

        if not name or len(name) < 2:
            return None

        name_lower = name.lower()
        if name_lower in self._seen_names:
            return None
        self._seen_names.add(name_lower)

        # Extract content/description
        content_obj = item.get("content", {})
        if isinstance(content_obj, dict):
            content = (content_obj.get("rendered") or "").strip()
        else:
            content = str(content_obj).strip()
        content = re.sub(r"<[^>]+>", "", content).strip()

        slug = (item.get("slug") or "").strip()
        link = (item.get("link") or "").strip()
        source_url = link or (f"https://foundersfund.com/portfolio/{slug}/" if slug else self.BASE_URL)

        # Try ACF fields for additional data
        acf = item.get("acf", {}) or {}
        website = (acf.get("website") or acf.get("company_url") or "").strip()
        industry = (acf.get("industry") or "").strip()

        bio_parts = []
        if content:
            bio_parts.append(content[:500])
        bio_parts.append("Founders Fund portfolio company.")
        if industry:
            bio_parts.append(f"Industry: {industry}.")
        bio = " ".join(bio_parts)

        return ScrapedContact(
            name=name,
            email="",
            company=name,
            website=website or source_url,
            linkedin="",
            phone="",
            bio=bio,
            source_platform=self.SOURCE_NAME,
            source_url=source_url,
            source_category="vc_portfolio",
            raw_data={
                "slug": slug,
                "wp_id": item.get("id", ""),
                "industry": industry,
                "vc_firm": "Founders Fund",
            },
        )

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Fetch Founders Fund portfolio companies.

        Strategy:
        1. Try WordPress REST API first (most structured)
        2. Fall back to scraping the portfolio HTML page
        """
        self.logger.info("Starting %s scraper", self.SOURCE_NAME)

        contacts_yielded = 0
        pages_done = 0

        # Strategy 1: WordPress REST API
        self.logger.info("Trying WordPress REST API...")
        page = 1
        api_success = False

        while True:
            if max_pages and pages_done >= max_pages:
                break

            items = self._fetch_wp_api(page)
            if not items:
                break

            api_success = True
            pages_done += 1

            for item in items:
                contact = self._wp_item_to_contact(item)
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

            self.logger.info(
                "WP API page %d: %d items, %d total valid",
                page, len(items), self.stats["contacts_valid"],
            )

            if len(items) < WP_API_PER_PAGE:
                break

            page += 1

        if api_success:
            self.logger.info("WP API fetch complete: %s", self.stats)
            return

        # Strategy 2: Scrape HTML page
        self.logger.info("WP API unavailable, scraping HTML page...")
        html = self.fetch_page(self.BASE_URL)
        if not html:
            self.logger.error("Failed to fetch portfolio page")
            return

        try:
            contacts = self.scrape_page(self.BASE_URL, html)
        except Exception as exc:
            self.logger.error("Parse error: %s", exc)
            self.stats["errors"] += 1
            return

        self.logger.info("Found %d companies from HTML page", len(contacts))

        for contact in contacts:
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

        self.logger.info("Scraper complete: %s", self.stats)
