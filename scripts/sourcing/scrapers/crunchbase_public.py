"""
Crunchbase Public Company Listings Scraper

Scrapes company data from https://www.crunchbase.com/discover/organization.companies.

Strategy:
  Crunchbase embeds organization data in an Angular app-state JSON blob inside
  a <script id="ng-state"> or <script id="client-app-state"> tag. We parse
  that embedded JSON to extract structured company data without needing the
  paid API.

  We iterate through the sitemap and category/industry pages to discover
  organizations, then fetch individual org pages to extract the embedded
  data (name, short description, website, location, founders, funding).

  Crunchbase aggressively rate-limits and blocks scrapers, so we use
  conservative timing and proper headers.

Estimated yield: 2,000-10,000 companies (depending on rate-limit tolerance)
"""

from __future__ import annotations

import json
import re
from typing import Iterator, Optional
from datetime import datetime

from urllib.parse import urljoin

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Industry verticals to browse on Crunchbase
INDUSTRIES = [
    "artificial-intelligence",
    "fintech",
    "e-commerce",
    "health-care",
    "information-technology",
    "software",
    "internet-services",
    "mobile",
    "analytics",
    "biotechnology",
    "clean-technology",
    "cloud-computing",
    "cyber-security",
    "data-and-analytics",
    "design",
    "education",
    "enterprise-software",
    "food-and-beverage",
    "gaming",
    "hardware",
    "logistics",
    "manufacturing",
    "marketing-and-advertising",
    "media-and-entertainment",
    "real-estate",
    "robotics",
    "saas",
    "sales-and-marketing",
    "social-media",
    "sustainability",
    "transportation",
    "travel-and-tourism",
]

# Funding stages to filter by
FUNDING_STAGES = [
    "seed",
    "early-stage-venture",
    "late-stage-venture",
    "private-equity",
]

MAX_PAGES_PER_CATEGORY = 10


class Scraper(BaseScraper):
    SOURCE_NAME = "crunchbase_public"
    BASE_URL = "https://www.crunchbase.com"
    REQUESTS_PER_MINUTE = 4  # Very conservative — CB is aggressive with blocks

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_slugs: set[str] = set()
        # Crunchbase requires specific headers to not get blocked
        self.session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        })

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield Crunchbase organization listing URLs."""
        # Main discover page
        yield f"{self.BASE_URL}/discover/organization.companies"

        # Industry-specific listings with pagination
        for industry in INDUSTRIES:
            for page in range(1, MAX_PAGES_PER_CATEGORY + 1):
                yield (
                    f"{self.BASE_URL}/discover/organization.companies"
                    f"/field/industries/{industry}"
                    f"?page={page}"
                )

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a Crunchbase listing or org page for company data."""
        contacts = []

        # Try to extract embedded app-state JSON data
        app_data = self._extract_app_state(html)
        if app_data:
            contacts.extend(self._parse_app_state_listings(app_data))

        # Also look for organization links to fetch individually
        soup = self.parse_html(html)
        org_links = self._extract_org_links(soup)

        for org_url in org_links:
            org_html = self.fetch_page(org_url)
            if not org_html:
                continue
            contact = self._parse_org_page(org_url, org_html)
            if contact:
                contacts.append(contact)

        return contacts

    def _extract_app_state(self, html: str) -> dict | None:
        """Extract Angular app-state JSON from script tags."""
        # Crunchbase embeds data in <script id="ng-state"> or similar
        patterns = [
            r'<script[^>]*id="ng-state"[^>]*>(.*?)</script>',
            r'<script[^>]*id="client-app-state"[^>]*>(.*?)</script>',
            r'<script[^>]*type="application/json"[^>]*id="[^"]*state[^"]*"[^>]*>(.*?)</script>',
        ]
        for pattern in patterns:
            match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
            if match:
                raw = match.group(1)
                # Angular HTML-encodes the JSON
                raw = (
                    raw.replace("&q;", '"')
                    .replace("&a;", "&")
                    .replace("&l;", "<")
                    .replace("&g;", ">")
                    .replace("&quot;", '"')
                    .replace("&#39;", "'")
                    .replace("&amp;", "&")
                )
                try:
                    return json.loads(raw)
                except (json.JSONDecodeError, ValueError):
                    continue
        return None

    def _parse_app_state_listings(self, data: dict) -> list[ScrapedContact]:
        """Parse org listings from the embedded app-state JSON."""
        contacts = []

        # Navigate the nested structure to find organization entities
        # Crunchbase stores entities in various nested paths
        entities = []
        self._find_entities(data, entities)

        for entity in entities:
            contact = self._entity_to_contact(entity)
            if contact:
                contacts.append(contact)

        return contacts

    def _find_entities(self, obj, results: list, depth: int = 0):
        """Recursively find organization entity dicts in nested JSON."""
        if depth > 10:
            return
        if isinstance(obj, dict):
            # Check if this dict looks like an organization entity
            if (
                obj.get("type") == "organization"
                or obj.get("entity_type") == "organization"
                or ("properties" in obj and "name" in obj.get("properties", {}))
            ):
                results.append(obj)
            else:
                for value in obj.values():
                    self._find_entities(value, results, depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                self._find_entities(item, results, depth + 1)

    def _entity_to_contact(self, entity: dict) -> ScrapedContact | None:
        """Convert a Crunchbase entity dict to a ScrapedContact."""
        props = entity.get("properties", entity)

        name = (props.get("name") or props.get("title") or "").strip()
        if not name or len(name) < 2:
            return None

        slug = (
            props.get("identifier", {}).get("permalink", "")
            or props.get("permalink", "")
            or props.get("slug", "")
        ).strip()

        if slug:
            if slug in self._seen_slugs:
                return None
            self._seen_slugs.add(slug)
        else:
            key = name.lower()
            if key in self._seen_slugs:
                return None
            self._seen_slugs.add(key)

        short_desc = (props.get("short_description") or "").strip()
        description = (props.get("description") or "").strip()
        website = (props.get("homepage_url") or props.get("website_url") or "").strip()
        location = (
            props.get("location_identifiers", [{}])[0].get("value", "")
            if isinstance(props.get("location_identifiers"), list)
            and props.get("location_identifiers")
            else (props.get("city_name") or props.get("location", "") or "")
        ).strip() if props.get("location_identifiers") or props.get("city_name") or props.get("location") else ""

        num_employees = (props.get("num_employees_enum") or "").strip()
        founded_on = (props.get("founded_on") or "").strip()
        funding_total = props.get("funding_total", {})
        funding_str = ""
        if isinstance(funding_total, dict) and funding_total.get("value"):
            currency = funding_total.get("currency", "USD")
            amount = funding_total.get("value", 0)
            if amount >= 1_000_000:
                funding_str = f"${amount / 1_000_000:.1f}M {currency}"
            elif amount >= 1_000:
                funding_str = f"${amount / 1_000:.0f}K {currency}"

        # Build bio
        bio_parts = []
        if short_desc:
            bio_parts.append(short_desc)
        elif description:
            bio_parts.append(description[:500])
        if location:
            bio_parts.append(f"Location: {location}")
        if founded_on:
            bio_parts.append(f"Founded: {founded_on}")
        if funding_str:
            bio_parts.append(f"Funding: {funding_str}")
        if num_employees:
            bio_parts.append(f"Employees: {num_employees}")
        bio = " | ".join(bio_parts) if bio_parts else f"Company listed on Crunchbase"

        # Use Crunchbase profile URL if no website
        if not website and slug:
            website = f"{self.BASE_URL}/organization/{slug}"

        return ScrapedContact(
            name=name,
            company=name,
            website=website,
            bio=bio,
            source_category="startups",
            raw_data={
                "crunchbase_slug": slug,
                "founded_on": founded_on,
                "num_employees": num_employees,
                "funding": funding_str,
                "location": location,
            },
        )

    def _extract_org_links(self, soup) -> list[str]:
        """Extract organization profile links from a listing page."""
        links = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            if re.match(r"^/organization/[a-zA-Z0-9_\-]+/?$", href):
                slug = href.strip("/").split("/")[-1]
                if slug and slug not in self._seen_slugs:
                    self._seen_slugs.add(slug)
                    links.append(urljoin(self.BASE_URL, href))
        return links[:20]  # Cap per page

    def _parse_org_page(self, url: str, html: str) -> ScrapedContact | None:
        """Parse an individual Crunchbase organization page."""
        # Try app-state first
        app_data = self._extract_app_state(html)
        if app_data:
            entities = []
            self._find_entities(app_data, entities)
            if entities:
                return self._entity_to_contact(entities[0])

        # Fallback to HTML parsing
        soup = self.parse_html(html)

        name = ""
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        if not name:
            og_title = soup.find("meta", property="og:title")
            if og_title:
                raw_title = (og_title.get("content", "") or "")
                name = raw_title.split("-")[0].split("|")[0].strip()

        if not name or len(name) < 2:
            return None

        slug = url.rstrip("/").split("/")[-1]
        if slug in self._seen_slugs:
            return None
        self._seen_slugs.add(slug)

        bio = ""
        og_desc = soup.find("meta", property="og:description")
        if og_desc:
            bio = (og_desc.get("content", "") or "")[:1000]

        # Look for website link
        website = ""
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            if (
                href.startswith("http")
                and "crunchbase.com" not in href.lower()
                and "facebook.com" not in href.lower()
                and "twitter.com" not in href.lower()
                and "linkedin.com" not in href.lower()
            ):
                website = href
                break

        if not website:
            website = url

        linkedin = self.extract_linkedin(html)

        return ScrapedContact(
            name=name,
            company=name,
            website=website,
            linkedin=linkedin,
            bio=bio or f"Company listed on Crunchbase",
            source_url=url,
            source_category="startups",
            raw_data={"crunchbase_slug": slug},
        )
