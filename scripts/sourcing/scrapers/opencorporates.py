"""
OpenCorporates company scraper.

OpenCorporates is the largest open database of companies in the world with 200M+ companies.
Searches for JV-relevant company types across US jurisdictions.

API: https://api.opencorporates.com/v0.4/companies/search
Requires: OPENCORPORATES_API_KEY in .env
Rate limit: ~500 requests/day for free tier (10 req/min)

Usage:
    python3 -m scripts.sourcing.runner --source opencorporates --dry-run --max-pages 10
    python3 -m scripts.sourcing.runner --source opencorporates --max-pages 100
"""

import json
import os
from typing import Iterator
from scripts.sourcing.base import BaseScraper, ScrapedContact


class Scraper(BaseScraper):
    SOURCE_NAME = "opencorporates"
    BASE_URL = "https://api.opencorporates.com/v0.4/companies/search"
    REQUESTS_PER_MINUTE = 10  # Conservative for free tier (500/day limit)

    # JV-relevant search queries
    SEARCH_QUERIES = [
        "consulting", "coaching", "training", "marketing agency",
        "advertising", "public relations", "management consulting",
        "business development", "strategic advisory", "venture",
        "joint venture", "partnership", "investment group",
        "accelerator", "incubator", "venture capital",
        "digital marketing", "brand agency", "media company",
        "technology consulting", "IT consulting", "software development",
        "financial advisory", "wealth management", "insurance agency",
        "real estate investment", "property management",
        "franchise", "staffing agency", "recruiting",
        "health coaching", "wellness", "fitness",
        "legal services", "accounting firm", "CPA",
    ]

    # Top 10 US jurisdictions by business volume
    US_JURISDICTIONS = [
        "us_ca",  # California
        "us_ny",  # New York
        "us_tx",  # Texas
        "us_fl",  # Florida
        "us_il",  # Illinois
        "us_pa",  # Pennsylvania
        "us_oh",  # Ohio
        "us_ga",  # Georgia
        "us_nc",  # North Carolina
        "us_mi",  # Michigan
    ]

    # All US jurisdictions (for future expansion)
    ALL_US_JURISDICTIONS = [
        "us_ca", "us_ny", "us_tx", "us_fl", "us_il", "us_pa", "us_oh",
        "us_ga", "us_nc", "us_mi", "us_nj", "us_va", "us_wa", "us_az",
        "us_ma", "us_tn", "us_in", "us_mo", "us_md", "us_wi",
        "us_co", "us_mn", "us_sc", "us_al", "us_la", "us_ky",
        "us_or", "us_ok", "us_ct", "us_ut", "us_ia", "us_nv",
        "us_ar", "us_ms", "us_ks", "us_nm", "us_ne", "us_id",
        "us_wv", "us_hi", "us_nh", "us_me", "us_mt", "us_ri",
        "us_de", "us_sd", "us_nd", "us_ak", "us_vt", "us_wy", "us_dc",
    ]

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_numbers: set[str] = set()  # Track company_number + jurisdiction
        self._api_token = os.getenv("OPENCORPORATES_API_KEY")
        if not self._api_token:
            self.logger.warning(
                "OPENCORPORATES_API_KEY not found in environment. "
                "Get a free key from https://opencorporates.com/api_accounts/new"
            )

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """
        Generate paginated search URLs for each query × jurisdiction combination.

        Strategy:
        - 35 queries × 10 jurisdictions × 2 pages = ~700 URLs
        - Stays within 500 requests/day limit with some buffer
        - Can be expanded to more jurisdictions or pages as needed
        """
        if not self._api_token:
            self.logger.error("Cannot generate URLs without OPENCORPORATES_API_KEY")
            return

        max_pages_per_query = 2  # Conservative to stay within daily limit
        per_page = 30  # Max for free tier

        for query in self.SEARCH_QUERIES:
            for jurisdiction in self.US_JURISDICTIONS:
                for page in range(1, max_pages_per_query + 1):
                    url = (
                        f"{self.BASE_URL}"
                        f"?q={query.replace(' ', '+')}"
                        f"&jurisdiction_code={jurisdiction}"
                        f"&current_status=Active"
                        f"&page={page}"
                        f"&per_page={per_page}"
                        f"&api_token={self._api_token}"
                    )
                    yield url

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """
        Parse JSON response and extract company data.

        The 'html' parameter actually contains JSON text from the API response.
        """
        contacts = []

        try:
            data = json.loads(html)
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON response: {e}")
            return contacts

        # Check for API errors
        if "error" in data:
            error_msg = data["error"].get("message", "Unknown error")
            self.logger.error(f"API error: {error_msg}")
            return contacts

        # Extract companies from results
        results = data.get("results", {})
        companies = results.get("companies", [])

        if not companies:
            self.logger.debug(f"No companies found in response")
            return contacts

        for item in companies:
            company_data = item.get("company", {})

            # Extract key fields
            company_name = company_data.get("name", "").strip()
            company_number = company_data.get("company_number", "").strip()
            jurisdiction = company_data.get("jurisdiction_code", "").strip()
            incorporation_date = company_data.get("incorporation_date", "")
            company_type = company_data.get("company_type", "")
            status = company_data.get("current_status", "")
            registry_url = company_data.get("registry_url", "")
            registered_address = company_data.get("registered_address_in_full", "").strip()

            # Skip if missing critical data
            if not company_name or not company_number:
                continue

            # Deduplicate by company_number + jurisdiction
            unique_id = f"{jurisdiction}:{company_number}"
            if unique_id in self._seen_numbers:
                continue
            self._seen_numbers.add(unique_id)

            # Skip inactive companies (belt and suspenders - we filter in URL too)
            if status and status.lower() != "active":
                continue

            # Clean company name
            name = self._clean_name(company_name)
            if not name:
                continue

            # Build bio with available metadata
            bio_parts = [name]
            if registered_address:
                bio_parts.append(registered_address)
            if company_type:
                bio_parts.append(company_type)
            if incorporation_date:
                bio_parts.append(f"Inc: {incorporation_date}")
            bio = " | ".join(bio_parts)

            # Create contact
            contact = ScrapedContact(
                name=name,
                company=company_name,
                website=registry_url or "",
                bio=bio,
                source_category="business_registrations",
            )
            contacts.append(contact)

        self.logger.info(
            f"Extracted {len(contacts)} companies from page "
            f"(total unique: {len(self._seen_numbers)})"
        )

        return contacts

    def _clean_name(self, name: str) -> str:
        """
        Clean company name:
        - Remove common entity suffixes (LLC, Inc, Corp, etc.)
        - Strip whitespace
        - Return empty string if invalid
        """
        if not name:
            return ""

        # Common entity suffixes to remove
        suffixes = [
            " LLC", " L.L.C.", " L.L.C", " LLP", " L.L.P.",
            " INC", " INC.", " INCORPORATED",
            " CORP", " CORP.", " CORPORATION",
            " LTD", " LTD.", " LIMITED",
            " CO", " CO.", " COMPANY",
            " PC", " P.C.", " PROFESSIONAL CORPORATION",
            " PLLC", " P.L.L.C.",
        ]

        cleaned = name.strip()
        upper = cleaned.upper()

        for suffix in suffixes:
            if upper.endswith(suffix):
                cleaned = cleaned[:-len(suffix)].strip()
                break

        # Remove trailing periods and commas
        cleaned = cleaned.rstrip(".,")

        # Skip if too short or generic
        if len(cleaned) < 3:
            return ""

        # Skip generic names
        generic_terms = ["COMPANY", "CORPORATION", "LLC", "INC", "LTD"]
        if cleaned.upper() in generic_terms:
            return ""

        return cleaned
