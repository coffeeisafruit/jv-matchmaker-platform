"""
USAspending.gov Federal Contractor Scraper

Scrapes government contractor data from USAspending.gov API.
These are real businesses that receive federal contracts — many actively
seek JV (Joint Venture) partners for government work.

API: https://api.usaspending.gov/api/v2/
No API key required. Rate limit: be reasonable (10 req/min).

Two-phase approach:
1. spending_by_category/recipient/ — find companies by NAICS code
2. recipient/{hash}/ — enrich with address, website, business type

Overrides run() because the default generate_urls→fetch_page→scrape_page
loop doesn't work for POST-based API pagination.
"""

import json
import time
from typing import Iterator, Optional
from datetime import datetime

from scripts.sourcing.base import BaseScraper, ScrapedContact


# JV-relevant NAICS codes for professional/business services
JV_NAICS_CODES = [
    # Management Consulting
    "541611",  # Administrative Management Consulting
    "541612",  # Human Resources Consulting
    "541613",  # Marketing Consulting
    "541614",  # Process/Physical Distribution/Logistics Consulting
    "541618",  # Other Management Consulting
    # Computer/IT Services
    "541511",  # Custom Computer Programming
    "541512",  # Computer Systems Design
    "541519",  # Other Computer Related Services
    # Engineering & Scientific
    "541330",  # Engineering Services
    "541380",  # Testing Laboratories
    "541620",  # Environmental Consulting
    "541690",  # Other Scientific/Technical Consulting
    "541715",  # R&D in Physical, Engineering, Life Sciences
    # Marketing & Communications
    "541810",  # Advertising Agencies
    "541820",  # Public Relations Agencies
    "541830",  # Media Buying Agencies
    "541910",  # Marketing Research and Public Opinion Polling
    "541921",  # Photography Studios
    "541922",  # Commercial Photography
    # Professional Training & Education
    "541990",  # All Other Professional/Scientific/Technical Services
    "611430",  # Professional and Management Development Training
    "611710",  # Educational Support Services
    # Financial Services
    "523110",  # Investment Banking
    "523120",  # Securities Brokerage
    "523910",  # Miscellaneous Intermediation
    "523930",  # Investment Advice
    "524210",  # Insurance Agencies
    "541211",  # Offices of CPAs
    "541214",  # Payroll Services
    "541219",  # Other Accounting Services
    # Staffing & HR
    "561310",  # Employment Placement Agencies
    "561311",  # Employment Placement Agencies
    "561320",  # Temporary Help Services
    "561330",  # Professional Employer Organizations
    # Facilities & Business Support
    "561210",  # Facilities Support Services
    "561499",  # All Other Business Support Services
    "561110",  # Office Administrative Services
    # Health & Wellness
    "621111",  # Offices of Physicians
    "621320",  # Offices of Optometrists
    "621399",  # Offices of Misc Health Practitioners
    "621610",  # Home Health Care Services
    "624190",  # Other Individual and Family Services
]


class Scraper(BaseScraper):
    SOURCE_NAME = "usaspending"
    BASE_URL = "https://api.usaspending.gov"
    REQUESTS_PER_MINUTE = 10  # Be reasonable with free API

    # Pagination limits
    MAX_RESULTS_PER_NAICS = 10000  # API may cap at 10K per query
    PAGE_SIZE = 100  # Max per page for spending_by_category

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self.session.headers["Content-Type"] = "application/json"
        self.session.headers["Accept"] = "application/json"
        self._seen_ueis: set[str] = set()
        self._seen_names: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used — we override run() for POST-based API."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used — we override run() for POST-based API."""
        return []

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """
        Two-phase scrape:
        1. Find companies by NAICS via spending_by_category/recipient/
        2. Enrich each with recipient profile for address/website

        Yields ScrapedContact objects.
        """
        self.logger.info("Starting %s scraper — %d NAICS codes to process",
                         self.SOURCE_NAME, len(JV_NAICS_CODES))

        # Resume from checkpoint
        start_naics_idx = (checkpoint or {}).get("naics_idx", 0)
        contacts_yielded = 0
        pages_done = 0

        for naics_idx, naics_code in enumerate(JV_NAICS_CODES):
            if naics_idx < start_naics_idx:
                continue

            self.logger.info("Processing NAICS %s (%d/%d)",
                             naics_code, naics_idx + 1, len(JV_NAICS_CODES))

            # Paginate through recipients for this NAICS
            page = 1
            empty_pages = 0
            while True:
                if self.rate_limiter:
                    self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

                payload = {
                    "filters": {
                        "time_period": [
                            {"start_date": "2020-01-01", "end_date": "2026-12-31"}
                        ],
                        "award_type_codes": ["A", "B", "C", "D"],  # All contract types
                        "naics_codes": {"require": [naics_code]},
                    },
                    "category": "recipient",
                    "page": page,
                    "limit": self.PAGE_SIZE,
                }

                try:
                    resp = self.session.post(
                        f"{self.BASE_URL}/api/v2/search/spending_by_category/recipient/",
                        json=payload,
                        timeout=60,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    self.stats["pages_scraped"] += 1
                except Exception as e:
                    self.stats["errors"] += 1
                    self.logger.warning("API error on NAICS %s page %d: %s",
                                        naics_code, page, e)
                    break

                results = data.get("results", [])
                if not results:
                    break

                for item in results:
                    contact = self._parse_recipient(item, naics_code)
                    if contact:
                        contacts_yielded += 1
                        yield contact

                        if max_contacts and contacts_yielded >= max_contacts:
                            self.logger.info("Reached max_contacts=%d", max_contacts)
                            return

                pages_done += 1

                # Check if there are more pages
                page_metadata = data.get("page_metadata", {})
                has_next = page_metadata.get("hasNext", False)

                if not has_next:
                    break

                page += 1

                # Safety: don't go past max pages per NAICS
                if page > self.MAX_RESULTS_PER_NAICS // self.PAGE_SIZE:
                    self.logger.info("Hit max pages for NAICS %s", naics_code)
                    break

                if max_pages and pages_done >= max_pages:
                    self.logger.info("Reached max_pages=%d", max_pages)
                    return

            self.logger.info(
                "NAICS %s done: %d valid contacts so far",
                naics_code, self.stats["contacts_valid"]
            )

        self.logger.info("Scraper complete: %s", self.stats)

    def _parse_recipient(self, item: dict, naics_code: str) -> ScrapedContact | None:
        """Parse a spending_by_category/recipient result into ScrapedContact."""
        name = (item.get("name") or "").strip()
        if not name or len(name) < 3:
            return None

        # Skip government agencies
        name_upper = name.upper()
        gov_indicators = [
            "DEPARTMENT OF", "UNITED STATES", "U.S. ", "US ARMY", "US NAVY",
            "US AIR FORCE", "FEDERAL", "STATE OF ", "COUNTY OF ", "CITY OF ",
            "GOVERNMENT", "MUNICIPALITY", "TRIBAL", "NATION OF",
        ]
        if any(ind in name_upper for ind in gov_indicators):
            return None

        # Deduplicate by UEI or normalized name
        uei = (item.get("uei") or "").strip()
        if uei:
            if uei in self._seen_ueis:
                return None
            self._seen_ueis.add(uei)
        else:
            name_key = name_upper.strip()
            if name_key in self._seen_names:
                return None
            self._seen_names.add(name_key)

        # Extract data
        amount = item.get("amount", 0) or 0
        recipient_id = (item.get("recipient_id") or "").strip()
        duns = (item.get("code") or "").strip()

        # Build bio
        bio_parts = [name]
        if amount and amount > 0:
            bio_parts.append(f"Federal contracts: ${amount:,.0f}")
        bio_parts.append(f"NAICS: {naics_code}")
        bio = " | ".join(bio_parts)

        # Use USAspending profile as website
        if recipient_id:
            website = f"https://www.usaspending.gov/recipient/{recipient_id}/latest"
        else:
            website = "https://www.usaspending.gov"

        contact = ScrapedContact(
            name=name,
            email="",
            company=name,
            website=website,
            linkedin="",
            phone="",
            bio=bio,
            source_platform=self.SOURCE_NAME,
            source_url=f"{self.BASE_URL}/api/v2/search/spending_by_category/recipient/",
            source_category="federal_contractors",
            scraped_at=datetime.now().isoformat(),
            raw_data={
                "uei": uei,
                "duns": duns,
                "recipient_id": recipient_id,
                "amount": amount,
                "naics_code": naics_code,
            },
        )

        if not contact.is_valid():
            return None

        self.stats["contacts_found"] += 1
        self.stats["contacts_valid"] += 1
        return contact
