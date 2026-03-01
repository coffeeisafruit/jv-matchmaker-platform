"""
SBIR.gov API Scraper

Scrapes Small Business Innovation Research (SBIR) award recipients from the public API.
Targets companies that have received federal R&D funding, which indicates innovation
capacity and potential for joint venture partnerships.

API Documentation: https://www.sbir.gov/api
Base endpoint: https://api.www.sbir.gov/public/api/awards
"""

import json
from typing import Iterator
from datetime import datetime

from scripts.sourcing.base import BaseScraper, ScrapedContact


class Scraper(BaseScraper):
    SOURCE_NAME = "sbir_gov"
    BASE_URL = "https://api.www.sbir.gov"
    REQUESTS_PER_MINUTE = 15

    # Federal agencies that fund SBIR/STTR programs
    AGENCIES = ["DOD", "HHS", "NSF", "DOE", "NASA", "USDA", "EPA", "DOC", "DOT", "ED", "DHS"]

    # Years to scrape (2020-2026)
    YEARS = list(range(2020, 2027))

    # Pagination settings
    ROWS_PER_PAGE = 100
    MAX_PAGES_PER_COMBO = 50  # 5000 results max per agency-year

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_firms: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """
        Generate API URLs for all agency-year combinations with pagination.

        Yields URLs in format:
        https://api.www.sbir.gov/public/api/awards?agency=DOD&year=2024&rows=100&start=0
        """
        for agency in self.AGENCIES:
            for year in self.YEARS:
                for page in range(self.MAX_PAGES_PER_COMBO):
                    start = page * self.ROWS_PER_PAGE
                    url = (
                        f"{self.BASE_URL}/public/api/awards"
                        f"?agency={agency}"
                        f"&year={year}"
                        f"&rows={self.ROWS_PER_PAGE}"
                        f"&start={start}"
                    )
                    yield url

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """
        Parse SBIR API JSON response and extract contacts.

        Attempts to extract both POC (Point of Contact) and PI (Principal Investigator)
        as separate contacts when they differ.
        """
        contacts = []

        # Try to parse JSON response
        try:
            data = json.loads(html)
        except json.JSONDecodeError:
            self.logger.warning(f"Invalid JSON response from {url}")
            return []

        # Handle case where API returns error object instead of array
        if not isinstance(data, list):
            self.logger.warning(f"Expected array, got {type(data).__name__} from {url}")
            return []

        # If empty array, we've reached the end of results for this combo
        if len(data) == 0:
            return []

        for award in data:
            firm = award.get("firm", "").strip()
            if not firm:
                continue

            # Deduplicate by normalized firm name
            firm_key = firm.lower().strip()
            if firm_key in self._seen_firms:
                continue
            self._seen_firms.add(firm_key)

            # Extract common fields
            company_url = award.get("company_url", "").strip()
            city = award.get("city", "").strip()
            state = award.get("state", "").strip()
            agency = award.get("agency", "").strip()
            program = award.get("program", "").strip()
            phase = award.get("phase", "").strip()
            award_amount = award.get("award_amount", 0)
            award_year = award.get("award_year", "")
            num_employees = award.get("number_employees", "")
            keywords = award.get("research_keywords", "").strip()

            # Build bio string
            bio_parts = [firm]
            if city and state:
                bio_parts.append(f"{city}, {state}")
            if agency and program and phase:
                bio_parts.append(f"{agency} {program} {phase}")
            if award_amount:
                bio_parts.append(f"${award_amount:,} ({award_year})")
            if num_employees:
                bio_parts.append(f"{num_employees} employees")
            if keywords:
                bio_parts.append(f"Keywords: {keywords}")
            bio = " | ".join(bio_parts)

            # Store raw data
            raw_data = {
                "agency": agency,
                "program": program,
                "phase": phase,
                "award_amount": award_amount,
                "award_year": award_year,
                "hubzone_owned": award.get("hubzone_owned", ""),
                "women_owned": award.get("women_owned", ""),
                "socially_economically_disadvantaged": award.get("socially_economically_disadvantaged", ""),
                "duns_number": award.get("duns_number", ""),
                "branch": award.get("branch", ""),
                "award_title": award.get("award_title", ""),
                "research_keywords": keywords,
                "number_employees": num_employees,
            }

            # Extract POC (Point of Contact)
            poc_name = award.get("poc_name", "").strip()
            poc_email = award.get("poc_email", "").strip()
            poc_phone = award.get("poc_phone", "").strip()
            poc_title = award.get("poc_title", "").strip()

            # Extract PI (Principal Investigator)
            pi_name = award.get("pi_name", "").strip()
            pi_email = award.get("pi_email", "").strip()
            pi_phone = award.get("pi_phone", "").strip()

            # Determine if POC and PI are different people
            poc_different = poc_name and poc_name.lower() != pi_name.lower()
            pi_different = pi_name and pi_name.lower() != poc_name.lower()

            # Create POC contact if we have name or email
            if poc_name or poc_email:
                poc_contact = ScrapedContact(
                    name=poc_name or firm,
                    email=poc_email,
                    phone=poc_phone,
                    company=firm,
                    website=company_url,
                    linkedin="",
                    bio=f"{poc_title} | {bio}" if poc_title else bio,
                    source_platform=self.SOURCE_NAME,
                    source_url=url,
                    source_category="sbir_awardees",
                    scraped_at=datetime.now().isoformat(),
                    raw_data=raw_data.copy()
                )
                contacts.append(poc_contact)

            # Create PI contact if different from POC and has email
            if pi_different and pi_email:
                pi_contact = ScrapedContact(
                    name=pi_name,
                    email=pi_email,
                    phone=pi_phone,
                    company=firm,
                    website=company_url,
                    linkedin="",
                    bio=f"Principal Investigator | {bio}",
                    source_platform=self.SOURCE_NAME,
                    source_url=url,
                    source_category="sbir_awardees",
                    scraped_at=datetime.now().isoformat(),
                    raw_data=raw_data.copy()
                )
                contacts.append(pi_contact)

            # If we have neither POC nor PI, create a company-level contact
            if not contacts and firm:
                fallback_contact = ScrapedContact(
                    name=firm,
                    email="",
                    phone="",
                    company=firm,
                    website=company_url,
                    linkedin="",
                    bio=bio,
                    source_platform=self.SOURCE_NAME,
                    source_url=url,
                    source_category="sbir_awardees",
                    scraped_at=datetime.now().isoformat(),
                    raw_data=raw_data
                )
                contacts.append(fallback_contact)

        return contacts
