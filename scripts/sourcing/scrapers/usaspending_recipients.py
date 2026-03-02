"""
USAspending.gov Recipient List Scraper

Unlike usaspending.py which searches by NAICS code, this scraper
paginates through ALL federal award recipients sorted by total amount.
The endpoint has 18.2M+ recipients — we filter for actual businesses
(not government agencies) and yield company contacts.

API: https://api.usaspending.gov/api/v2/recipient/
No API key required.

This is the highest-volume scraper: even filtering aggressively,
there are 500K+ business recipients with federal contracts.
"""

import json
from typing import Iterator, Optional
from datetime import datetime

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Words that indicate a government entity (skip these)
GOV_INDICATORS = {
    "DEPARTMENT OF", "UNITED STATES", "U.S. GOVERNMENT",
    "US ARMY", "US NAVY", "US AIR FORCE", "US MARINE",
    "FEDERAL BUREAU", "FEDERAL AGENCY", "FEDERAL RESERVE",
    "STATE OF ", "COUNTY OF ", "CITY OF ", "TOWN OF ", "VILLAGE OF ",
    "MUNICIPALITY", "TRIBAL", " NATION", "GOVERNMENT OF",
    "SCHOOL DISTRICT", "PUBLIC SCHOOL", "BOARD OF EDUCATION",
    "FIRE DISTRICT", "WATER DISTRICT", "SANITATION DISTRICT",
    "HOUSING AUTHORITY", "TRANSIT AUTHORITY", "PORT AUTHORITY",
    "UNIVERSITY OF", "COLLEGE OF", "COMMUNITY COLLEGE",
    " HOSPITAL DISTRICT", "HEALTH DISTRICT",
    "COMMONWEALTH OF", "TERRITORY OF", "REPUBLIC OF",
    "HEALTH & HUMAN", "HEALTH AND HUMAN", "DEPT OF",
    "HUMAN SERVICES", "SOCIAL SERVICES",
    "COMMN ", "COMMISSION OF", "BUREAU OF",
    "OFFICE OF ", "ADMIN OF ", "ADMINISTRATION OF",
    "JUDICIARY", "SUPREME COURT",
    "POLICE DEPT", "SHERIFF", "CORRECTIONS",
    "NATIONAL GUARD", "COAST GUARD",
    "PUBLIC HEALTH", "PUBLIC WORKS",
}

# Words that indicate individual (not a business)
INDIVIDUAL_INDICATORS = {
    "MULTIPLE RECIPIENTS", "REDACTED", "INDIVIDUAL",
    "MISCELLANEOUS FOREIGN", "CLASSIFIED",
}


class Scraper(BaseScraper):
    SOURCE_NAME = "usaspending_recipients"
    BASE_URL = "https://api.usaspending.gov"
    REQUESTS_PER_MINUTE = 10

    PAGE_SIZE = 100
    # Skip recipients below this threshold (filters noise)
    MIN_AMOUNT = 10000  # $10K minimum federal contracts

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self.session.headers["Content-Type"] = "application/json"
        self.session.headers["Accept"] = "application/json"
        self._seen_ids: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used — we override run()."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used — we override run()."""
        return []

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """
        Paginate through all recipients sorted by total award amount (desc).
        Filter for businesses, skip government entities.
        """
        self.logger.info("Starting %s scraper — paginating all recipients",
                         self.SOURCE_NAME)

        page = (checkpoint or {}).get("page", 1)
        contacts_yielded = 0
        pages_done = 0
        consecutive_empty = 0

        while True:
            if self.rate_limiter:
                self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

            payload = {
                "order": "desc",
                "sort": "amount",
                "page": page,
                "limit": self.PAGE_SIZE,
            }

            try:
                resp = self.session.post(
                    f"{self.BASE_URL}/api/v2/recipient/",
                    json=payload,
                    timeout=60,
                )
                resp.raise_for_status()
                data = resp.json()
                self.stats["pages_scraped"] += 1
            except Exception as e:
                self.stats["errors"] += 1
                self.logger.warning("API error on page %d: %s", page, e)
                # Retry logic: skip one page on error
                page += 1
                if self.stats["errors"] > 10:
                    self.logger.error("Too many errors, stopping")
                    break
                continue

            results = data.get("results", [])
            if not results:
                break

            page_contacts = 0
            for item in results:
                contact = self._parse_recipient(item)
                if contact:
                    contacts_yielded += 1
                    page_contacts += 1
                    yield contact

                    if max_contacts and contacts_yielded >= max_contacts:
                        self.logger.info("Reached max_contacts=%d", max_contacts)
                        return

            pages_done += 1
            page += 1

            if page_contacts == 0:
                consecutive_empty += 1
            else:
                consecutive_empty = 0

            # If we hit 5 pages with 0 valid contacts, the amounts dropped below threshold
            if consecutive_empty >= 5:
                self.logger.info("5 consecutive empty pages — amounts likely below threshold")
                break

            if pages_done % 50 == 0:
                self.logger.info(
                    "Progress: page %d, %d valid contacts, %d seen",
                    page, self.stats["contacts_valid"], len(self._seen_ids)
                )

            if max_pages and pages_done >= max_pages:
                self.logger.info("Reached max_pages=%d", max_pages)
                break

            # Check if there are more pages
            page_meta = data.get("page_metadata", {})
            if not page_meta.get("hasNext", False):
                break

        self.logger.info("Scraper complete: %s", self.stats)

    def _parse_recipient(self, item: dict) -> ScrapedContact | None:
        """Parse a recipient into a ScrapedContact."""
        name = (item.get("name") or "").strip()
        if not name or len(name) < 3:
            return None

        # Skip below minimum amount
        amount = item.get("amount", 0) or 0
        if amount < self.MIN_AMOUNT:
            return None

        # Skip government entities
        name_upper = name.upper()
        for indicator in GOV_INDICATORS:
            if indicator in name_upper:
                return None

        # Skip individuals/redacted
        for indicator in INDIVIDUAL_INDICATORS:
            if indicator in name_upper:
                return None

        # Deduplicate
        recipient_id = (item.get("id") or "").strip()
        uei = (item.get("uei") or "").strip()
        dedup_key = recipient_id or uei or name_upper
        if dedup_key in self._seen_ids:
            return None
        self._seen_ids.add(dedup_key)

        duns = (item.get("duns") or "").strip()
        recipient_level = (item.get("recipient_level") or "").strip()

        # Build bio
        bio_parts = [name]
        if amount > 1_000_000_000:
            bio_parts.append(f"Federal contracts: ${amount/1e9:,.1f}B")
        elif amount > 1_000_000:
            bio_parts.append(f"Federal contracts: ${amount/1e6:,.1f}M")
        elif amount > 0:
            bio_parts.append(f"Federal contracts: ${amount:,.0f}")
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
            source_url=f"{self.BASE_URL}/api/v2/recipient/",
            source_category="federal_contractors",
            scraped_at=datetime.now().isoformat(),
            raw_data={
                "recipient_id": recipient_id,
                "uei": uei,
                "duns": duns,
                "amount": amount,
                "recipient_level": recipient_level,
            },
        )

        if not contact.is_valid():
            return None

        self.stats["contacts_found"] += 1
        self.stats["contacts_valid"] += 1
        return contact
