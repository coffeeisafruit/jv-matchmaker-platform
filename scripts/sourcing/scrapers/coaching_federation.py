"""
ICF Coaching Federation Credentialed Coach Finder scraper.

Uses ICF's Azure-hosted JSON search API at:
  https://icf-ccf.azurewebsites.net/api/search

The API accepts POST requests with filter criteria and returns
paginated results with coach name, credential, location, headline,
description, and rate. Total pool: ~37,000 credentialed coaches.

Unlike the existing icf_coaching.py scraper which guesses at HTML
patterns, this scraper uses the actual search API discovered by
analyzing the CCF JavaScript (ccf.search.min.js).

Coach profile pages are at:
  apps.coachingfederation.org/eweb/CCFDynamicPage.aspx?
    webcode=ccfcoachprofileview&coachcstkey={key}

NOTE: The API does not expose email or website directly. Coach
names plus credential/location provide enough signal for enrichment
pipeline to find them via Apollo/LinkedIn.

Estimated yield: 5,000-37,000 coaches
"""

from __future__ import annotations

import json
import uuid
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Search API endpoint (discovered from ccf.search.min.js)
SEARCH_API_URL = "https://icf-ccf.azurewebsites.net/api/search"

# Profile page base URL
PROFILE_BASE = (
    "https://apps.coachingfederation.org/eweb/CCFDynamicPage.aspx"
    "?webcode=ccfcoachprofileview&coachcstkey="
)

# Credential types to search
CREDENTIALS = ["ACC", "PCC", "MCC"]

# Countries to search (largest English-speaking coach populations)
COUNTRIES = [
    "United States",
    "Canada",
    "United Kingdom",
    "Australia",
    "India",
    "Singapore",
    "South Africa",
    "Ireland",
    "New Zealand",
    "Germany",
    "Netherlands",
    "United Arab Emirates",
    "France",
    "Switzerland",
    "Spain",
    "Brazil",
    "Japan",
    "China",
    "Mexico",
    "Nigeria",
]

# Coaching themes/keywords to broaden coverage
KEYWORDS = [
    "",           # Empty keyword = all results
    "business",
    "executive",
    "leadership",
    "career",
    "life",
    "health",
    "wellness",
    "team",
    "performance",
    "communication",
    "entrepreneurship",
    "transition",
    "mindfulness",
    "financial",
    "sales",
    "marketing",
    "women",
    "diversity",
    "conflict",
]

# Results per page (API max appears to be 75)
RESULTS_PER_PAGE = 75

# Maximum pages per search combination
MAX_PAGES_PER_SEARCH = 100


class Scraper(BaseScraper):
    SOURCE_NAME = "coaching_federation"
    BASE_URL = "https://apps.coachingfederation.org"
    REQUESTS_PER_MINUTE = 8  # JSON API can handle more

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_keys: set[str] = set()
        self._seen_names: set[str] = set()
        # Configure session for JSON API
        self.session.headers.update({
            "Content-Type": "application/json; charset=UTF-8",
            "Accept": "*/*",
            "Origin": "https://apps.coachingfederation.org",
            "Referer": (
                "https://apps.coachingfederation.org/eweb/"
                "CCFDynamicPage.aspx?webcode=ccfsearch&site=icfapp"
            ),
        })

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield synthetic URL identifiers for search API calls.

        Since the API uses POST, we encode the search parameters
        into a pseudo-URL that run() will recognize and handle.
        Each URL encodes: keyword, page number.
        """
        # Primary strategy: paginate through all results with no filters
        for page in range(1, MAX_PAGES_PER_SEARCH + 1):
            yield f"__api__:keyword=&page={page}"

        # Secondary strategy: search by keyword for broader coverage
        for keyword in KEYWORDS:
            if not keyword:
                continue  # Already covered above
            for page in range(1, 20):  # Keyword results tend to be smaller
                yield f"__api__:keyword={keyword}&page={page}"

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Override run() to use JSON API instead of HTML fetching.

        The base class run() calls fetch_page() which expects HTML.
        We need to use POST requests to the search API instead.
        """
        from datetime import datetime

        pages_done = 0
        contacts_yielded = 0
        start_from = (checkpoint or {}).get("last_url")
        past_checkpoint = start_from is None

        self.logger.info(
            "Starting %s scraper (max_pages=%s, max_contacts=%s)",
            self.SOURCE_NAME,
            max_pages or "unlimited",
            max_contacts or "unlimited",
        )

        for url in self.generate_urls():
            if not past_checkpoint:
                if url == start_from:
                    past_checkpoint = True
                continue

            # Parse synthetic URL
            params = self._parse_api_url(url)
            keyword = params.get("keyword", "")
            page = int(params.get("page", 1))

            # Make API call
            results = self._search_api(keyword=keyword, page=page)
            if results is None:
                continue

            coaches = results.get("results", [])
            total_count = results.get("resultCount", 0)

            if not coaches:
                # No more results for this keyword, skip remaining pages
                self.logger.debug(
                    "No results for keyword='%s' page=%d (total=%d)",
                    keyword, page, total_count,
                )
                continue

            for coach_data in coaches:
                contact = self._parse_coach(coach_data)
                if not contact:
                    continue

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
                    "Progress: %d pages, %d valid contacts, %d total seen",
                    pages_done, self.stats["contacts_valid"], len(self._seen_keys),
                )

            if max_pages and pages_done >= max_pages:
                self.logger.info("Reached max_pages=%d", max_pages)
                break

            # Check if we've exhausted this keyword's results
            skip = (page - 1) * RESULTS_PER_PAGE
            if skip + len(coaches) >= total_count:
                self.logger.debug(
                    "Exhausted results for keyword='%s' at page %d (%d/%d)",
                    keyword, page, skip + len(coaches), total_count,
                )

        self.logger.info("Scraper complete: %s", self.stats)

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used - this scraper overrides run() for API access."""
        return []

    def _parse_api_url(self, url: str) -> dict:
        """Parse synthetic API URL into parameters."""
        params = {}
        if url.startswith("__api__:"):
            param_str = url[len("__api__:"):]
            for part in param_str.split("&"):
                if "=" in part:
                    key, value = part.split("=", 1)
                    params[key] = value
        return params

    def _search_api(
        self,
        keyword: str = "",
        page: int = 1,
        take: int = RESULTS_PER_PAGE,
    ) -> Optional[dict]:
        """Call the ICF Credentialed Coach Finder search API."""
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        skip = (page - 1) * take
        request_id = str(uuid.uuid4())

        payload = {
            "requestId": request_id,
            "continuationToken": "",
            "skip": skip,
            "take": take,
            "sort": "lastName",
            "sortDirection": "asc",
            "keywords": keyword,
            "filters": {
                "keywords": keyword,
                "credentials": [],
                "services": {
                    "coachingThemes": [],
                    "coachingMethods": {
                        "methods": [],
                        "relocate": False,
                    },
                    "standardRate": {
                        "proBono": False,
                        "nonProfitDiscount": False,
                        "feeRanges": [],
                    },
                },
                "experience": {
                    "haveCoached": {
                        "clientType": "",
                        "organizationalClientTypes": [],
                    },
                    "coachedOrganizations": {
                        "global": False,
                        "nonProfit": False,
                        "industrySector": "",
                    },
                    "heldPositions": [],
                },
                "demographics": {
                    "gender": "",
                    "ageRange": "",
                    "fluentLanguages": [],
                    "locations": {
                        "countries": [],
                        "states": [],
                    },
                },
                "additional": {
                    "canProvide": [],
                    "designations": [],
                },
            },
        }

        try:
            resp = self.session.post(
                SEARCH_API_URL,
                json=payload,
                timeout=30,
            )
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1
            return resp.json()
        except Exception as exc:
            self.logger.warning("API call failed (keyword='%s', page=%d): %s", keyword, page, exc)
            self.stats["errors"] += 1
            return None

    def _parse_coach(self, data: dict) -> ScrapedContact | None:
        """Parse a single coach record from the API response."""
        key = (data.get("key") or "").strip()
        full_name = (data.get("fullName") or "").strip()

        if not full_name:
            return None

        # Deduplicate by key (GUID)
        if key and key in self._seen_keys:
            return None
        if key:
            self._seen_keys.add(key)

        # Also deduplicate by name (in case keys differ across searches)
        name_lower = full_name.lower()
        if name_lower in self._seen_names:
            return None
        self._seen_names.add(name_lower)

        # Clean name - remove credential suffixes like ", PCC"
        # e.g. "John Smith, PCC" -> "John Smith"
        # But keep complex names like "Dr. Jane Doe, MBA, PCC"
        name = full_name

        credential = (data.get("credential") or "").strip()
        headline = (data.get("headline") or "").strip()
        description = (data.get("description") or "").strip()
        location = (data.get("location") or "").strip()
        rate = (data.get("standardRate") or "").strip()

        # Build bio from available fields
        bio_parts = []
        if credential:
            bio_parts.append(f"ICF {credential} Credentialed Coach")
        else:
            bio_parts.append("ICF Credentialed Coach")
        if location:
            bio_parts.append(location)
        if rate and rate != "Unspecified":
            bio_parts.append(rate)
        if headline:
            bio_parts.append(headline[:300])
        elif description:
            # Use first 300 chars of description
            desc_clean = description.replace("\r\n", " ").replace("\n", " ").strip()
            bio_parts.append(desc_clean[:300])

        bio = " | ".join(bio_parts)

        # Build profile URL
        profile_url = f"{PROFILE_BASE}{key}" if key else ""

        return ScrapedContact(
            name=name,
            email="",  # Not exposed by API
            company="",
            website=profile_url,  # ICF profile page as website
            linkedin="",  # Not exposed by API
            phone="",
            bio=bio,
            source_category="coaching",
            raw_data={
                "icf_key": key,
                "credential": credential,
                "location": location,
                "rate": rate,
                "has_enhanced_profile": data.get("hasEnhancedProfile", False),
            },
        )
