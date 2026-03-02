"""
Wellfound (formerly AngelList) Startup Directory Scraper

Scrapes the Wellfound startup directory at https://wellfound.com/companies
to collect startup company profiles.

Strategy:
  - Wellfound uses Next.js with server-rendered pages
  - Company listing pages at /role/{role} and /location/{location}
  - Also has sitemap at /sitemap.xml with company page URLs
  - Extract __NEXT_DATA__ JSON from pages for structured data
  - Individual company pages at /company/{slug} have full details

Data extracted:
  - Company name, website, description, location
  - Industry/market, company size, stage
  - Job count (as a proxy for company activity)

Estimated yield: 20,000-100,000 startups.

Note: Wellfound may block aggressive scraping. Pages are partially
JS-rendered, but __NEXT_DATA__ provides most structured data.
If __NEXT_DATA__ is empty, fall back to Open Graph / meta tags.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Wellfound market/industry categories for browsing
MARKET_CATEGORIES = [
    "saas",
    "fintech",
    "health-care",
    "e-commerce",
    "edtech",
    "artificial-intelligence",
    "machine-learning",
    "cybersecurity",
    "blockchain",
    "real-estate",
    "marketing",
    "advertising",
    "analytics",
    "business-intelligence",
    "consulting",
    "design",
    "developer-tools",
    "enterprise-software",
    "human-resources",
    "insurance",
    "legal",
    "logistics",
    "media",
    "mobile",
    "payments",
    "productivity-tools",
    "recruiting",
    "sales-and-marketing",
    "social-media",
    "travel",
    "video",
    "wellness",
    "clean-technology",
    "food-and-beverage",
    "gaming",
    "government",
    "hardware",
    "internet-of-things",
    "manufacturing",
    "non-profit",
    "pet",
    "professional-services",
    "robotics",
    "sustainability",
]

# US locations for geographic browsing
LOCATIONS = [
    "san-francisco",
    "new-york",
    "los-angeles",
    "chicago",
    "boston",
    "seattle",
    "austin",
    "denver",
    "atlanta",
    "miami",
    "washington-dc",
    "san-diego",
    "portland",
    "nashville",
    "dallas",
    "houston",
    "philadelphia",
    "phoenix",
    "minneapolis",
    "salt-lake-city",
    "raleigh",
    "detroit",
    "pittsburgh",
    "cleveland",
    "charlotte",
    "remote",
]

# Max pages per category/location listing
MAX_PAGES_PER_LISTING = 50


class Scraper(BaseScraper):
    SOURCE_NAME = "angel_list"
    BASE_URL = "https://wellfound.com"
    REQUESTS_PER_MINUTE = 8

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_slugs: set[str] = set()
        self._seen_names: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run()."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used -- we override run()."""
        return []

    def _extract_next_data(self, html: str) -> Optional[dict]:
        """Extract __NEXT_DATA__ JSON from a Wellfound page."""
        match = re.search(
            r'<script\s+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html,
            re.DOTALL,
        )
        if not match:
            return None
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            return None

    def _parse_listing_page(self, html: str, source_url: str) -> list[ScrapedContact]:
        """Parse a Wellfound listing page for startup entries."""
        contacts = []

        next_data = self._extract_next_data(html)
        if next_data:
            contacts = self._parse_next_data_listings(next_data, source_url)
            if contacts:
                return contacts

        # Fallback: parse HTML structure
        contacts = self._parse_html_listings(html, source_url)
        return contacts

    def _parse_next_data_listings(self, next_data: dict, source_url: str) -> list[ScrapedContact]:
        """Extract company listings from __NEXT_DATA__."""
        contacts = []

        props = next_data.get("props", {})
        page_props = props.get("pageProps", {})

        # Wellfound structures vary; look for common patterns
        # Pattern 1: companies list directly in pageProps
        companies = page_props.get("companies") or []
        if not companies:
            # Pattern 2: nested in a results/data key
            companies = page_props.get("results", {}).get("companies", []) if isinstance(page_props.get("results"), dict) else []
        if not companies:
            # Pattern 3: startupResults
            startup_results = page_props.get("startupResults") or {}
            companies = startup_results.get("startups", []) if isinstance(startup_results, dict) else []
        if not companies:
            # Pattern 4: Apollo state / dehydrated state
            apollo_state = next_data.get("props", {}).get("apolloState", {})
            if apollo_state:
                for key, value in apollo_state.items():
                    if key.startswith("Startup:") and isinstance(value, dict):
                        companies.append(value)
            # Also check dehydratedState
            dehydrated = page_props.get("dehydratedState") or {}
            queries = dehydrated.get("queries") or []
            for q in queries:
                state = q.get("state") or {}
                data = state.get("data") or {}
                # Look for any list of company-like objects
                for k, v in data.items():
                    if isinstance(v, list) and v and isinstance(v[0], dict) and v[0].get("name"):
                        companies.extend(v)

        for company in companies:
            contact = self._parse_company_data(company, source_url)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_company_data(self, company: dict, source_url: str) -> Optional[ScrapedContact]:
        """Parse a single company data object into ScrapedContact."""
        name = (company.get("name") or company.get("companyName") or "").strip()
        if not name or len(name) < 2:
            return None

        slug = (company.get("slug") or company.get("id") or "").strip()

        # Deduplicate
        dedup_key = slug if slug else name.lower().replace(" ", "")
        if dedup_key in self._seen_slugs:
            return None
        self._seen_slugs.add(dedup_key)

        website = (company.get("companyUrl") or company.get("website") or company.get("company_url") or "").strip()
        description = (
            company.get("highConcept") or
            company.get("high_concept") or
            company.get("description") or
            company.get("one_liner") or
            company.get("tagline") or
            ""
        ).strip()
        long_desc = (company.get("productDescription") or company.get("product_description") or "").strip()

        # Location
        location_parts = []
        locations = company.get("locations") or company.get("location_tags") or []
        if isinstance(locations, list):
            for loc in locations:
                if isinstance(loc, dict):
                    location_parts.append((loc.get("displayName") or loc.get("name") or "").strip())
                elif isinstance(loc, str):
                    location_parts.append(loc.strip())
        location_str = (company.get("location") or "").strip()
        if location_str and not location_parts:
            location_parts.append(location_str)
        location = ", ".join(p for p in location_parts if p)

        # Industry / market
        markets = company.get("markets") or company.get("market_tags") or []
        market_names = []
        if isinstance(markets, list):
            for m in markets:
                if isinstance(m, dict):
                    market_names.append((m.get("displayName") or m.get("name") or "").strip())
                elif isinstance(m, str):
                    market_names.append(m.strip())

        # Company size
        company_size = (
            company.get("companySize") or
            company.get("company_size") or
            company.get("teamSize") or
            ""
        )
        if isinstance(company_size, int):
            company_size = str(company_size)
        company_size = str(company_size).strip()

        # Stage
        stage = (company.get("stage") or "").strip()

        # Logo
        logo = (company.get("logoUrl") or company.get("logo_url") or "").strip()

        # Build wellfound URL
        wellfound_url = f"https://wellfound.com/company/{slug}" if slug else ""

        # If no website, use Wellfound profile as website
        if not website:
            website = wellfound_url

        # Bio
        bio_parts = [name]
        if description:
            bio_parts.append(description)
        if location:
            bio_parts.append(f"Location: {location}")
        if market_names:
            bio_parts.append(f"Markets: {', '.join(market_names[:4])}")
        if stage:
            bio_parts.append(f"Stage: {stage}")
        if company_size:
            bio_parts.append(f"Size: {company_size}")
        if long_desc:
            remaining = 2000 - len(" | ".join(bio_parts)) - 3
            if remaining > 50:
                bio_parts.append(long_desc[:remaining])
        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=name,
            email="",
            company=name,
            website=website,
            linkedin="",
            phone="",
            bio=bio,
            source_category="startups",
            raw_data={
                "wellfound_slug": slug,
                "wellfound_url": wellfound_url,
                "description": description,
                "location": location,
                "markets": market_names,
                "stage": stage,
                "company_size": company_size,
            },
        )

    def _parse_html_listings(self, html: str, source_url: str) -> list[ScrapedContact]:
        """Fallback HTML parser for Wellfound listing pages."""
        soup = self.parse_html(html)
        contacts = []

        # Look for company links with /company/ pattern
        for link in soup.find_all("a", href=re.compile(r"/company/[a-zA-Z0-9\-]+")):
            href = link.get("href", "")
            slug_match = re.search(r"/company/([a-zA-Z0-9\-]+)", href)
            if not slug_match:
                continue

            slug = slug_match.group(1).lower()
            if slug in self._seen_slugs:
                continue

            name = link.get_text(strip=True)
            if not name or len(name) < 2 or len(name) > 200:
                continue

            # Skip navigation-like text
            if name.lower() in ("view company", "see more", "apply", "follow"):
                continue

            self._seen_slugs.add(slug)

            wellfound_url = f"https://wellfound.com/company/{slug}"

            # Try to get description from nearby elements
            description = ""
            parent = link.find_parent(["div", "li", "article"])
            if parent:
                # Look for a text element that's not the link itself
                for child in parent.find_all(["p", "span", "div"]):
                    text = child.get_text(strip=True)
                    if text and text != name and len(text) > 20 and len(text) < 500:
                        description = text
                        break

            bio_parts = [name]
            if description:
                bio_parts.append(description[:200])
            bio = " | ".join(bio_parts)

            contacts.append(ScrapedContact(
                name=name,
                email="",
                company=name,
                website=wellfound_url,
                linkedin="",
                phone="",
                bio=bio,
                source_category="startups",
                raw_data={
                    "wellfound_slug": slug,
                    "wellfound_url": wellfound_url,
                },
            ))

        return contacts

    def _fetch_company_page(self, slug: str) -> Optional[dict]:
        """Fetch an individual company page for more detailed data."""
        url = f"{self.BASE_URL}/company/{slug}"

        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1
        except Exception as exc:
            self.logger.debug("Failed to fetch company page %s: %s", slug, exc)
            self.stats["errors"] += 1
            return None

        html = resp.text
        next_data = self._extract_next_data(html)
        if next_data:
            props = next_data.get("props", {}).get("pageProps", {})
            company = props.get("company") or props.get("startup") or {}
            if company:
                return company

        # Fallback: extract from meta tags
        soup = self.parse_html(html)
        data = {}

        og_title = soup.find("meta", property="og:title")
        if og_title:
            data["name"] = re.sub(r"\s*[-|].*$", "", (og_title.get("content") or "")).strip()

        og_desc = soup.find("meta", property="og:description")
        if og_desc:
            data["description"] = (og_desc.get("content") or "").strip()

        og_url = soup.find("meta", property="og:url")
        if og_url:
            data["companyUrl"] = (og_url.get("content") or "").strip()

        return data if data.get("name") else None

    # ------------------------------------------------------------------
    # Sitemap-based discovery
    # ------------------------------------------------------------------

    def _fetch_sitemap_slugs(self) -> list[str]:
        """Attempt to fetch company slugs from Wellfound's sitemap."""
        slugs = []

        sitemap_urls = [
            f"{self.BASE_URL}/sitemap.xml",
            f"{self.BASE_URL}/sitemaps/companies-1.xml",
            f"{self.BASE_URL}/sitemaps/companies.xml",
            f"{self.BASE_URL}/sitemap-companies.xml",
        ]

        for sitemap_url in sitemap_urls:
            if self.rate_limiter:
                self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

            html = self.fetch_page(sitemap_url)
            if not html:
                continue

            # Extract company URLs from sitemap XML
            for match in re.finditer(r"<loc>https?://wellfound\.com/company/([a-zA-Z0-9\-]+)</loc>", html):
                slug = match.group(1).lower()
                if slug not in self._seen_slugs:
                    slugs.append(slug)

            if slugs:
                self.logger.info("Found %d company slugs from sitemap %s", len(slugs), sitemap_url)
                break

            # Check for nested sitemap references
            for match in re.finditer(r"<loc>(https?://wellfound\.com/sitemaps/[^<]+)</loc>", html):
                nested_url = match.group(1)
                nested_html = self.fetch_page(nested_url)
                if nested_html:
                    for company_match in re.finditer(
                        r"<loc>https?://wellfound\.com/company/([a-zA-Z0-9\-]+)</loc>",
                        nested_html,
                    ):
                        slug = company_match.group(1).lower()
                        if slug not in self._seen_slugs:
                            slugs.append(slug)

            if slugs:
                self.logger.info("Found %d company slugs from nested sitemaps", len(slugs))
                break

        return slugs

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Scrape Wellfound startup directory.

        Three-phase approach:
        1. Try sitemap for bulk company slug discovery
        2. Browse market category listing pages
        3. Browse location listing pages

        For each discovered company, yield a ScrapedContact.
        """
        self.logger.info(
            "Starting %s scraper — %d markets, %d locations",
            self.SOURCE_NAME, len(MARKET_CATEGORIES), len(LOCATIONS),
        )

        contacts_yielded = 0
        pages_done = 0
        start_phase = (checkpoint or {}).get("phase", 0)
        start_idx = (checkpoint or {}).get("idx", 0)

        # Phase 1: Sitemap discovery
        if start_phase <= 0:
            self.logger.info("Phase 1: Sitemap discovery")
            sitemap_slugs = self._fetch_sitemap_slugs()

            for idx, slug in enumerate(sitemap_slugs):
                if start_phase == 0 and idx < start_idx:
                    continue

                if slug in self._seen_slugs:
                    continue

                company_data = self._fetch_company_page(slug)
                if not company_data:
                    continue

                contact = self._parse_company_data(company_data, f"{self.BASE_URL}/company/{slug}")
                if contact and contact.is_valid():
                    contact.source_platform = self.SOURCE_NAME
                    contact.source_url = f"{self.BASE_URL}/company/{slug}"
                    contact.scraped_at = datetime.now().isoformat()
                    contact.email = contact.clean_email()
                    self.stats["contacts_found"] += 1
                    self.stats["contacts_valid"] += 1
                    contacts_yielded += 1
                    yield contact

                    if max_contacts and contacts_yielded >= max_contacts:
                        self.logger.info("Reached max_contacts=%d", max_contacts)
                        return

                pages_done += 1
                if max_pages and pages_done >= max_pages:
                    self.logger.info("Reached max_pages=%d", max_pages)
                    return

                if pages_done % 50 == 0:
                    self.logger.info(
                        "Phase 1 progress: %d/%d slugs, %d valid contacts",
                        idx + 1, len(sitemap_slugs), self.stats["contacts_valid"],
                    )

        # Phase 2: Browse market categories
        if start_phase <= 1:
            self.logger.info("Phase 2: Market category browsing")
            for cat_idx, category in enumerate(MARKET_CATEGORIES):
                if start_phase == 1 and cat_idx < start_idx:
                    continue

                self.logger.info(
                    "Market: %s (%d/%d)",
                    category, cat_idx + 1, len(MARKET_CATEGORIES),
                )

                consecutive_empty = 0
                for page_num in range(1, MAX_PAGES_PER_LISTING + 1):
                    url = f"{self.BASE_URL}/role/r/software-engineer/l/{category}" if category in LOCATIONS else f"{self.BASE_URL}/companies?market={category}&page={page_num}"

                    # Try multiple URL patterns that Wellfound uses
                    listing_url = f"{self.BASE_URL}/companies?market={category}&page={page_num}"

                    html = self.fetch_page(listing_url)
                    if not html:
                        break

                    contacts = self._parse_listing_page(html, listing_url)

                    if not contacts:
                        consecutive_empty += 1
                        if consecutive_empty >= 2:
                            break
                        continue
                    else:
                        consecutive_empty = 0

                    for contact in contacts:
                        if contact.is_valid():
                            contact.source_platform = self.SOURCE_NAME
                            contact.source_url = listing_url
                            contact.scraped_at = datetime.now().isoformat()
                            contact.email = contact.clean_email()
                            self.stats["contacts_found"] += 1
                            self.stats["contacts_valid"] += 1
                            contacts_yielded += 1
                            yield contact

                            if max_contacts and contacts_yielded >= max_contacts:
                                self.logger.info("Reached max_contacts=%d", max_contacts)
                                return

                    pages_done += 1
                    if max_pages and pages_done >= max_pages:
                        self.logger.info("Reached max_pages=%d", max_pages)
                        return

        # Phase 3: Browse locations
        if start_phase <= 2:
            self.logger.info("Phase 3: Location browsing")
            for loc_idx, location in enumerate(LOCATIONS):
                if start_phase == 2 and loc_idx < start_idx:
                    continue

                self.logger.info(
                    "Location: %s (%d/%d)",
                    location, loc_idx + 1, len(LOCATIONS),
                )

                consecutive_empty = 0
                for page_num in range(1, MAX_PAGES_PER_LISTING + 1):
                    listing_url = f"{self.BASE_URL}/companies?location={location}&page={page_num}"

                    html = self.fetch_page(listing_url)
                    if not html:
                        break

                    contacts = self._parse_listing_page(html, listing_url)

                    if not contacts:
                        consecutive_empty += 1
                        if consecutive_empty >= 2:
                            break
                        continue
                    else:
                        consecutive_empty = 0

                    for contact in contacts:
                        if contact.is_valid():
                            contact.source_platform = self.SOURCE_NAME
                            contact.source_url = listing_url
                            contact.scraped_at = datetime.now().isoformat()
                            contact.email = contact.clean_email()
                            self.stats["contacts_found"] += 1
                            self.stats["contacts_valid"] += 1
                            contacts_yielded += 1
                            yield contact

                            if max_contacts and contacts_yielded >= max_contacts:
                                self.logger.info("Reached max_contacts=%d", max_contacts)
                                return

                    pages_done += 1
                    if max_pages and pages_done >= max_pages:
                        self.logger.info("Reached max_pages=%d", max_pages)
                        return

                if pages_done % 10 == 0 and pages_done > 0:
                    self.logger.info(
                        "Progress: %d pages, %d valid contacts",
                        pages_done, self.stats["contacts_valid"],
                    )

        self.logger.info("Scraper complete: %s", self.stats)
