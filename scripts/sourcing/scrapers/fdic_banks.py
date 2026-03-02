"""
FDIC BankFind Scraper

Scrapes all FDIC-insured financial institutions from the BankFind API.
Banks and credit unions are potential JV partners for financial services,
real estate, and professional services firms.

API: https://api.fdic.gov/banks/
No API key required.

Uses institutions endpoint with pagination.
"""

import json
from typing import Iterator, Optional
from datetime import datetime

from scripts.sourcing.base import BaseScraper, ScrapedContact


class Scraper(BaseScraper):
    SOURCE_NAME = "fdic_banks"
    BASE_URL = "https://banks.data.fdic.gov"
    REQUESTS_PER_MINUTE = 10

    # Fields to request
    FIELDS = "NAME,CITY,STALP,WEBADDR,CERT,ASSET,DEP,NETINC,SPECGRP,ESTYMD,ZIP,ADDRESS,COUNTY"

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_certs: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used — we override run() for API pagination."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used — we override run() for API pagination."""
        return []

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Paginate through all FDIC-insured institutions."""
        self.logger.info("Starting %s scraper", self.SOURCE_NAME)

        offset = (checkpoint or {}).get("offset", 0)
        page_size = 100
        contacts_yielded = 0
        pages_done = 0

        while True:
            if self.rate_limiter:
                self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

            url = (
                f"{self.BASE_URL}/api/institutions"
                f"?filters=ACTIVE:1"
                f"&fields={self.FIELDS}"
                f"&sort_by=ASSET&sort_order=DESC"
                f"&limit={page_size}&offset={offset}"
            )

            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                self.stats["pages_scraped"] += 1
            except Exception as e:
                self.stats["errors"] += 1
                self.logger.warning("API error at offset %d: %s", offset, e)
                break

            results = data.get("data", [])
            if not results:
                break

            for item in results:
                props = item.get("data", {})
                contact = self._parse_institution(props)
                if contact:
                    contacts_yielded += 1
                    yield contact

                    if max_contacts and contacts_yielded >= max_contacts:
                        self.logger.info("Reached max_contacts=%d", max_contacts)
                        return

            pages_done += 1
            offset += page_size

            if pages_done % 10 == 0:
                self.logger.info(
                    "Progress: offset %d, %d valid contacts",
                    offset, self.stats["contacts_valid"]
                )

            if max_pages and pages_done >= max_pages:
                self.logger.info("Reached max_pages=%d", max_pages)
                break

            # Check if we've reached the end
            total = data.get("totals", {}).get("count", 0)
            if offset >= total:
                break

        self.logger.info("Scraper complete: %s", self.stats)

    def _parse_institution(self, props: dict) -> ScrapedContact | None:
        """Parse a single FDIC institution into ScrapedContact."""
        name = (props.get("NAME") or "").strip()
        if not name or len(name) < 3:
            return None

        cert = str(props.get("CERT") or "").strip()
        if cert in self._seen_certs:
            return None
        self._seen_certs.add(cert)

        city = (props.get("CITY") or "").strip()
        state = (props.get("STALP") or "").strip()
        website = (props.get("WEBADDR") or "").strip()
        zip_code = (props.get("ZIP") or "").strip()
        assets = props.get("ASSET", 0) or 0
        deposits = props.get("DEP", 0) or 0
        net_income = props.get("NETINC", 0) or 0
        spec_group = str(props.get("SPECGRP") or "").strip()
        established = str(props.get("ESTYMD") or "").strip()

        # Ensure website has protocol
        if website and not website.startswith("http"):
            website = f"https://{website}"

        # If no website, use FDIC profile
        if not website and cert:
            website = f"https://www.fdic.gov/resources/bankers/bank-find/details/?CERT={cert}"

        # Build bio
        bio_parts = [name]
        if city and state:
            bio_parts.append(f"{city}, {state}")
        if assets:
            # FDIC assets are in thousands
            assets_millions = assets / 1000
            if assets_millions >= 1000:
                bio_parts.append(f"Assets: ${assets_millions/1000:,.1f}B")
            else:
                bio_parts.append(f"Assets: ${assets_millions:,.0f}M")
        if deposits:
            dep_millions = deposits / 1000
            if dep_millions >= 1000:
                bio_parts.append(f"Deposits: ${dep_millions/1000:,.1f}B")
            else:
                bio_parts.append(f"Deposits: ${dep_millions:,.0f}M")
        if established:
            bio_parts.append(f"Est: {established}")
        bio = " | ".join(bio_parts)

        contact = ScrapedContact(
            name=name,
            email="",
            company=name,
            website=website,
            linkedin="",
            phone="",
            bio=bio,
            source_platform=self.SOURCE_NAME,
            source_url=f"{self.BASE_URL}/api/institutions",
            source_category="financial_institutions",
            scraped_at=datetime.now().isoformat(),
            raw_data={
                "cert": cert,
                "city": city,
                "state": state,
                "zip": zip_code,
                "assets_thousands": assets,
                "deposits_thousands": deposits,
                "net_income_thousands": net_income,
                "specialty_group": spec_group,
                "established": established,
            },
        )

        if not contact.is_valid():
            return None

        self.stats["contacts_found"] += 1
        self.stats["contacts_valid"] += 1
        return contact
