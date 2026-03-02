"""
HubSpot Solutions Marketplace partner scraper.

Scrapes https://ecosystem.hubspot.com/marketplace/solutions for
HubSpot certified partners (agencies, consultants, service providers).

The HubSpot marketplace is a fully client-rendered React SPA backed by
Apollo GraphQL. There is no server-rendered HTML and no public API
documentation for listing providers. This scraper uses two strategies:

Strategy 1 (Primary): Discover and call the internal marketplace API
  that powers the SPA. The frontend uses Apollo GraphQL to fetch listings
  from HubSpot's backend. We attempt common API endpoint patterns.

Strategy 2 (Fallback): Use the undocumented marketplace search/listing
  API endpoints at ecosystem.hubspot.com that return JSON. These are
  the same endpoints the React frontend calls via XHR.

Strategy 3 (Last resort): Scrape partner profile URLs discovered
  through category browsing pages. Individual provider profiles at
  ``ecosystem.hubspot.com/marketplace/solutions/{provider-slug}``
  may contain server-rendered metadata.

Note: This scraper may require periodic updates as HubSpot changes
their internal API. If all strategies fail, consider using the
``crawl4ai`` browser automation tool for JS rendering.

Estimated yield: 700-8,000+ solution providers
"""

from __future__ import annotations

import json
import re
import time
from typing import Iterator, Optional
from datetime import datetime

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Service categories available in the marketplace
SOLUTION_CATEGORIES = [
    "marketing",
    "sales",
    "service",
    "cms",
    "operations",
    "crm",
    "inbound-marketing",
    "seo",
    "content-marketing",
    "email-marketing",
    "social-media",
    "web-design",
    "web-development",
    "lead-generation",
    "marketing-automation",
    "sales-enablement",
    "crm-implementation",
    "hubspot-onboarding",
    "data-migration",
    "integration",
    "custom-development",
    "training",
    "consulting",
    "revops",
]

# Known internal API endpoint patterns to try
API_PATTERNS = [
    # GraphQL endpoint (Apollo-based frontend)
    "https://ecosystem.hubspot.com/marketplace/api/graphql",
    "https://ecosystem.hubspot.com/api/graphql",
    "https://ecosystem.hubspot.com/graphql",
    # REST-like endpoints
    "https://ecosystem.hubspot.com/marketplace/api/v1/solutions/search",
    "https://ecosystem.hubspot.com/marketplace/api/v1/marketplace/solutions",
    "https://ecosystem.hubspot.com/marketplace/api/v2/solutions",
    "https://ecosystem.hubspot.com/api/marketplace/v1/solutions",
    "https://ecosystem.hubspot.com/api/v1/marketplace-solutions/search",
    # HubSpot API patterns (from infrastructure analysis)
    "https://api.hubspot.com/marketplace/v1/solutions/search",
    "https://api.hubspot.com/ecosystem/v1/solutions",
]

# GraphQL query template for solution providers
GRAPHQL_QUERY = """
query SolutionsSearch($offset: Int, $limit: Int, $type: String) {
  solutions(offset: $offset, limit: $limit, type: $type) {
    total
    results {
      id
      name
      slug
      description
      website
      location {
        city
        state
        country
      }
      categories
      tier
      rating
      reviewCount
      logo
    }
  }
}
"""

# Maximum pages per category approach
MAX_PAGES = 50
RESULTS_PER_PAGE = 20


class Scraper(BaseScraper):
    SOURCE_NAME = "hubspot_partners"
    BASE_URL = "https://ecosystem.hubspot.com/marketplace/solutions"
    REQUESTS_PER_MINUTE = 8

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_slugs: set[str] = set()
        self._api_endpoint: Optional[str] = None
        self._api_method: Optional[str] = None  # "graphql" or "rest"
        # Add JSON accept header for API calls
        self.session.headers.update({
            "Accept": "application/json, text/html, */*",
            "Content-Type": "application/json",
            "Referer": "https://ecosystem.hubspot.com/marketplace/solutions",
            "Origin": "https://ecosystem.hubspot.com",
        })

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used — we override run() for multi-strategy approach."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse solutions provider data from various response formats."""
        contacts = []

        # Try parsing as JSON first (API response)
        try:
            data = json.loads(html)
            contacts = self._parse_api_response(data, url)
            if contacts:
                return contacts
        except (json.JSONDecodeError, TypeError):
            pass

        # Fall back to HTML parsing
        contacts = self._parse_html_page(html, url)
        return contacts

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Multi-strategy scraping approach.

        Tries internal API discovery first, then falls back to
        page-by-page browsing with HTML parsing.
        """
        self.logger.info("Starting %s scraper", self.SOURCE_NAME)
        contacts_yielded = 0
        pages_done = 0

        # Strategy 1: Try to discover and use the internal API
        self.logger.info("Strategy 1: Attempting API endpoint discovery...")
        api_found = self._discover_api()

        if api_found:
            self.logger.info(
                "API discovered at %s (method: %s)",
                self._api_endpoint, self._api_method,
            )
            for contact in self._scrape_via_api(max_contacts):
                contact.source_platform = self.SOURCE_NAME
                contact.scraped_at = datetime.now().isoformat()
                contact.email = contact.clean_email()

                if contact.is_valid():
                    self.stats["contacts_valid"] += 1
                    contacts_yielded += 1
                    yield contact

                    if max_contacts and contacts_yielded >= max_contacts:
                        self.logger.info("Reached max_contacts=%d", max_contacts)
                        return

                self.stats["contacts_found"] += 1

            if contacts_yielded > 0:
                self.logger.info(
                    "API strategy yielded %d contacts", contacts_yielded,
                )
                self.logger.info("Scraper complete: %s", self.stats)
                return

        # Strategy 2: Browse category pages and parse HTML/meta
        self.logger.info("Strategy 2: Browsing marketplace category pages...")

        # Resume from checkpoint
        start_from = (checkpoint or {}).get("last_url")
        past_checkpoint = start_from is None

        for url in self._generate_browse_urls():
            if not past_checkpoint:
                if url == start_from:
                    past_checkpoint = True
                continue

            html = self.fetch_page(url)
            if not html:
                continue

            try:
                contacts = self.scrape_page(url, html)
            except Exception as exc:
                self.logger.error("Parse error on %s: %s", url, exc)
                self.stats["errors"] += 1
                continue

            for contact in contacts:
                contact.source_platform = self.SOURCE_NAME
                contact.source_url = url
                contact.scraped_at = datetime.now().isoformat()
                contact.email = contact.clean_email()

                if contact.is_valid():
                    self.stats["contacts_valid"] += 1
                    contacts_yielded += 1
                    yield contact

                    if max_contacts and contacts_yielded >= max_contacts:
                        self.logger.info("Reached max_contacts=%d", max_contacts)
                        return

                self.stats["contacts_found"] += 1

            pages_done += 1
            if pages_done % 10 == 0:
                self.logger.info(
                    "Progress: %d pages, %d valid contacts",
                    pages_done, self.stats["contacts_valid"],
                )

            if max_pages and pages_done >= max_pages:
                self.logger.info("Reached max_pages=%d", max_pages)
                break

        # Strategy 3: Scrape individual provider profile pages
        # (if we found provider slugs from category pages)
        if contacts_yielded == 0:
            self.logger.info("Strategy 3: Attempting individual provider profiles...")
            for contact in self._scrape_provider_profiles(max_contacts):
                contact.source_platform = self.SOURCE_NAME
                contact.scraped_at = datetime.now().isoformat()
                contact.email = contact.clean_email()

                if contact.is_valid():
                    self.stats["contacts_valid"] += 1
                    contacts_yielded += 1
                    yield contact

                    if max_contacts and contacts_yielded >= max_contacts:
                        return

                self.stats["contacts_found"] += 1

        self.logger.info(
            "Scraper complete: %d contacts yielded. Stats: %s",
            contacts_yielded, self.stats,
        )

    def _discover_api(self) -> bool:
        """Try known API endpoint patterns to find a working one."""
        for endpoint in API_PATTERNS:
            try:
                if "graphql" in endpoint.lower():
                    # Try GraphQL
                    payload = {
                        "query": GRAPHQL_QUERY,
                        "variables": {"offset": 0, "limit": 5, "type": "service"},
                    }
                    if self.rate_limiter:
                        self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

                    resp = self.session.post(
                        endpoint,
                        json=payload,
                        timeout=15,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        if "data" in data and not data.get("errors"):
                            self._api_endpoint = endpoint
                            self._api_method = "graphql"
                            return True
                else:
                    # Try REST
                    if self.rate_limiter:
                        self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

                    resp = self.session.get(
                        endpoint,
                        params={"offset": 0, "limit": 5, "type": "service"},
                        timeout=15,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        if isinstance(data, (dict, list)):
                            # Check if it looks like a valid listing response
                            if isinstance(data, list) and len(data) > 0:
                                self._api_endpoint = endpoint
                                self._api_method = "rest"
                                return True
                            elif isinstance(data, dict) and (
                                "results" in data
                                or "items" in data
                                or "solutions" in data
                                or "data" in data
                            ):
                                self._api_endpoint = endpoint
                                self._api_method = "rest"
                                return True

            except Exception as exc:
                self.logger.debug("API probe failed for %s: %s", endpoint, exc)
                continue

        self.logger.info("No working API endpoint found — falling back to HTML scraping")
        return False

    def _scrape_via_api(self, max_contacts: int = 0) -> Iterator[ScrapedContact]:
        """Fetch solutions via discovered API endpoint."""
        offset = 0
        total_fetched = 0

        while True:
            try:
                if self._api_method == "graphql":
                    payload = {
                        "query": GRAPHQL_QUERY,
                        "variables": {
                            "offset": offset,
                            "limit": RESULTS_PER_PAGE,
                            "type": "service",
                        },
                    }
                    if self.rate_limiter:
                        self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

                    resp = self.session.post(
                        self._api_endpoint,
                        json=payload,
                        timeout=30,
                    )
                    data = resp.json()
                    solutions = (
                        data.get("data", {}).get("solutions", {}).get("results", [])
                    )
                    total = data.get("data", {}).get("solutions", {}).get("total", 0)

                else:
                    # REST approach
                    if self.rate_limiter:
                        self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

                    resp = self.session.get(
                        self._api_endpoint,
                        params={
                            "offset": offset,
                            "limit": RESULTS_PER_PAGE,
                            "type": "service",
                        },
                        timeout=30,
                    )
                    data = resp.json()
                    solutions = (
                        data.get("results", [])
                        or data.get("items", [])
                        or data.get("solutions", [])
                        or (data.get("data", {}) if isinstance(data.get("data"), list) else [])
                    )
                    total = data.get("total", data.get("count", 0))

                self.stats["pages_scraped"] += 1

                if not solutions:
                    break

                for sol in solutions:
                    contact = self._solution_to_contact(sol)
                    if contact:
                        yield contact
                        total_fetched += 1

                offset += RESULTS_PER_PAGE

                if total and offset >= total:
                    break

                if max_contacts and total_fetched >= max_contacts:
                    break

            except Exception as exc:
                self.logger.error("API fetch error at offset %d: %s", offset, exc)
                self.stats["errors"] += 1
                break

    def _solution_to_contact(self, sol: dict) -> Optional[ScrapedContact]:
        """Convert an API solution object to ScrapedContact."""
        name = (sol.get("name") or sol.get("title") or "").strip()
        if not name or len(name) < 2:
            return None

        slug = (sol.get("slug") or sol.get("id") or "").strip()
        if slug in self._seen_slugs:
            return None
        if slug:
            self._seen_slugs.add(slug)

        description = (sol.get("description") or sol.get("bio") or "").strip()
        website = (sol.get("website") or sol.get("url") or "").strip()

        location = sol.get("location", {})
        if isinstance(location, dict):
            city = (location.get("city") or "").strip()
            state = (location.get("state") or "").strip()
            country = (location.get("country") or "").strip()
        else:
            city = state = country = ""
            if isinstance(location, str):
                city = location

        categories = sol.get("categories", [])
        if isinstance(categories, list):
            categories_str = ", ".join(str(c) for c in categories[:5])
        else:
            categories_str = str(categories) if categories else ""

        tier = (sol.get("tier") or sol.get("partnerTier") or "").strip()
        rating = sol.get("rating", "")
        review_count = sol.get("reviewCount", sol.get("reviews", ""))

        # Build bio
        bio_parts = []
        if description:
            bio_parts.append(description[:500])
        if tier:
            bio_parts.append(f"HubSpot {tier} Partner")
        loc_str = ", ".join(filter(None, [city, state, country]))
        if loc_str:
            bio_parts.append(f"Location: {loc_str}")
        if rating and review_count:
            bio_parts.append(f"Rating: {rating}/5 ({review_count} reviews)")
        if categories_str:
            bio_parts.append(f"Services: {categories_str}")
        bio = " | ".join(bio_parts) if bio_parts else f"{name} - HubSpot Partner"

        profile_url = f"{self.BASE_URL}/{slug}" if slug else self.BASE_URL

        return ScrapedContact(
            name=name,
            company=name,
            website=website,
            bio=bio[:2000],
            source_url=profile_url,
            source_category="hubspot_partner",
            raw_data={
                "slug": slug,
                "tier": tier,
                "rating": str(rating),
                "review_count": str(review_count),
                "city": city,
                "state": state,
                "country": country,
                "categories": categories_str,
            },
        )

    def _generate_browse_urls(self) -> Iterator[str]:
        """Generate category browsing URLs for HTML parsing."""
        # Try the main solutions listing with pagination
        for page in range(1, MAX_PAGES + 1):
            yield f"{self.BASE_URL}/all?page={page}&type=service&ecosort=recommended"

        # Also try category-specific pages
        for category in SOLUTION_CATEGORIES:
            yield f"{self.BASE_URL}/{category}"
            for page in range(2, 11):
                yield f"{self.BASE_URL}/{category}?page={page}"

    def _parse_api_response(self, data: dict, url: str) -> list[ScrapedContact]:
        """Parse a JSON API response into contacts."""
        contacts = []

        # Handle various response shapes
        results = (
            data.get("results", [])
            or data.get("items", [])
            or data.get("solutions", [])
            or data.get("data", {}).get("solutions", {}).get("results", [])
            if isinstance(data.get("data"), dict)
            else []
        )

        for item in results:
            contact = self._solution_to_contact(item)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_html_page(self, html: str, url: str) -> list[ScrapedContact]:
        """Parse provider data from HTML (limited due to SPA rendering)."""
        soup = self.parse_html(html)
        contacts = []

        # Extract from meta tags (og: and standard meta)
        # Even SPAs often set meta tags for SEO
        og_title = soup.find("meta", property="og:title")
        og_description = soup.find("meta", property="og:description")
        meta_description = soup.find("meta", attrs={"name": "description"})

        # Look for JSON-LD data
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                ld_data = json.loads(script.string or "")
                if isinstance(ld_data, dict):
                    ld_type = ld_data.get("@type", "")
                    if ld_type in ("Organization", "LocalBusiness", "ProfessionalService"):
                        contact = self._parse_jsonld_org(ld_data, url)
                        if contact:
                            contacts.append(contact)
                    elif ld_type == "ItemList":
                        for item in ld_data.get("itemListElement", []):
                            org = item.get("item", {})
                            contact = self._parse_jsonld_org(org, url)
                            if contact:
                                contacts.append(contact)
            except (json.JSONDecodeError, TypeError):
                continue

        # Look for any links to provider profiles within the HTML
        # Pattern: /marketplace/solutions/{slug}
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            match = re.search(r"/marketplace/solutions/([\w\-]+)/?$", href)
            if match:
                slug = match.group(1)
                name_slug = a.get_text(strip=True).lower().replace(" ", "-")
                if (
                    slug not in self._seen_slugs
                    and name_slug not in self._seen_slugs
                    and slug not in (
                        "all", "marketing", "sales", "service", "cms",
                        "operations", "crm",
                    )
                ):
                    self._seen_slugs.add(slug)
                    name = a.get_text(strip=True)
                    if name and len(name) > 2 and len(name) < 150:
                        contacts.append(ScrapedContact(
                            name=name,
                            company=name,
                            website=f"https://ecosystem.hubspot.com/marketplace/solutions/{slug}",
                            bio=f"{name} - HubSpot Solutions Partner",
                            source_url=url,
                            source_category="hubspot_partner",
                            raw_data={"slug": slug},
                        ))

        return contacts

    def _parse_jsonld_org(self, data: dict, url: str) -> Optional[ScrapedContact]:
        """Parse a JSON-LD Organization into a ScrapedContact."""
        name = (data.get("name") or "").strip()
        if not name or len(name) < 2:
            return None

        slug = name.lower().replace(" ", "-")
        if slug in self._seen_slugs:
            return None
        self._seen_slugs.add(slug)

        description = (data.get("description") or "").strip()
        website = (data.get("url") or data.get("website") or "").strip()

        address = data.get("address", {})
        if isinstance(address, dict):
            city = (address.get("addressLocality") or "").strip()
            country = (address.get("addressCountry") or "").strip()
        else:
            city = country = ""

        phone = (data.get("telephone") or "").strip()
        email = (data.get("email") or "").strip()

        bio_parts = [description[:500]] if description else [name]
        if city and country:
            bio_parts.append(f"Location: {city}, {country}")
        bio_parts.append("HubSpot Solutions Partner")

        return ScrapedContact(
            name=name,
            email=email,
            company=name,
            website=website,
            phone=phone,
            bio=" | ".join(bio_parts),
            source_url=url,
            source_category="hubspot_partner",
            raw_data={"city": city, "country": country},
        )

    def _scrape_provider_profiles(self, max_contacts: int = 0) -> Iterator[ScrapedContact]:
        """Last resort: try to scrape individual provider profile pages.

        Uses known slugs from the marketplace (if any were discovered)
        or attempts to find them through the marketplace HTML.
        """
        profiles_tried = 0

        for slug in list(self._seen_slugs):
            if profiles_tried >= 50:  # Limit profile fetches
                break

            profile_url = f"{self.BASE_URL}/{slug}"
            html = self.fetch_page(profile_url)
            if not html:
                continue

            profiles_tried += 1
            contacts = self._parse_html_page(html, profile_url)

            for contact in contacts:
                yield contact

            if max_contacts and self.stats["contacts_valid"] >= max_contacts:
                break
