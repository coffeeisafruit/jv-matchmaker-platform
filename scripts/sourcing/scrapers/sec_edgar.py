"""
SEC EDGAR Company Scraper

Fetches public company data from SEC EDGAR database.
Source: https://www.sec.gov/edgar/sec-api-documentation

Data includes:
- Company name, phone, website
- SIC code and industry description
- Business address
- CIK, ticker symbols, exchange listings

Uses a two-phase approach:
1. Download company_tickers.json for the full CIK list
2. Fetch each company's submissions JSON

Overrides run() because the default generate_urls→fetch_page→scrape_page
loop doesn't work for the two-phase pattern.
"""

import json
from typing import Iterator, Optional
from datetime import datetime

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Professional services SIC codes relevant to JV partnerships
JV_SIC_CODES = {
    "7311", "7312", "7313", "7319",  # Advertising services
    "7361", "7363",                  # Staffing / temp services
    "7371", "7372", "7374", "7379",  # Computer services / software
    "8111",                          # Legal services
    "8711", "8712", "8713", "8721",  # Engineering / accounting
    "8731", "8732", "8733", "8734",  # R&D / testing labs
    "8741", "8742", "8743", "8744",  # Management / PR consulting
    "8748",                          # Misc business consulting
    "6159", "6199",                  # Financial services
}


class Scraper(BaseScraper):
    SOURCE_NAME = "sec_edgar"
    BASE_URL = "https://data.sec.gov"
    REQUESTS_PER_MINUTE = 8  # SEC asks max 10 req/sec, stay conservative

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        # SEC requires User-Agent header with contact info
        self.session.headers["User-Agent"] = "JVMatchmaker/1.0 (help@jvmatches.com)"
        self._seen_ciks: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used — we override run() for two-phase fetch."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse company submission JSON into ScrapedContact."""
        try:
            data = json.loads(html)
        except json.JSONDecodeError:
            self.logger.warning("Failed to parse JSON from %s", url)
            return []

        cik = str(data.get("cik") or "")
        name = (data.get("name") or "").strip()
        phone = (data.get("phone") or "").strip()
        website = (data.get("website") or "").strip()
        sic = str(data.get("sic") or "").strip()
        sic_description = (data.get("sicDescription") or "").strip()
        tickers = data.get("tickers") or []
        exchanges = data.get("exchanges") or []
        state_of_incorporation = data.get("stateOfIncorporation") or ""
        ein = data.get("ein") or ""

        addresses = data.get("addresses") or {}
        business_address = addresses.get("business") or {}
        city = (business_address.get("city") or "").strip()
        state = (business_address.get("stateOrCountry") or "").strip()
        street1 = (business_address.get("street1") or "").strip()
        street2 = (business_address.get("street2") or "").strip()
        zipcode = (business_address.get("zipCode") or "").strip()

        if not name:
            return []

        if cik in self._seen_ciks:
            return []
        self._seen_ciks.add(cik)

        # Build bio
        bio_parts = [name]
        if tickers:
            bio_parts.append(f"({', '.join(tickers)})")
        if sic_description:
            bio_parts.append(f"| {sic_description}")
        if city and state:
            bio_parts.append(f"| {city}, {state}")
        if sic:
            bio_parts.append(f"| SIC: {sic}")
        bio = " ".join(bio_parts)

        full_address = ""
        if street1:
            full_address = street1
            if street2:
                full_address += f", {street2}"
            if city:
                full_address += f", {city}"
            if state:
                full_address += f", {state}"
            if zipcode:
                full_address += f" {zipcode}"

        # If no website from EDGAR, use SEC filing page as discovery URL
        if not website and cik:
            website = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}"

        contact = ScrapedContact(
            name=name,
            email="",
            company=name,
            website=website,
            phone=phone,
            bio=bio,
            source_category="public_companies",
            raw_data={
                "cik": cik,
                "sic": sic,
                "sic_description": sic_description,
                "tickers": tickers,
                "exchanges": exchanges,
                "state_of_incorporation": state_of_incorporation,
                "ein": ein,
                "address": full_address,
                "city": city,
                "state": state,
                "zipcode": zipcode,
            },
        )

        return [contact]

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Two-phase fetch: company_tickers.json → individual submissions.

        Yields ScrapedContact objects compatible with the runner.
        """
        self.logger.info("Starting %s scraper", self.SOURCE_NAME)

        # Phase 1: Get company list
        tickers_url = "https://www.sec.gov/files/company_tickers.json"
        self.logger.info("Fetching company list from %s", tickers_url)

        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        try:
            response = self.session.get(tickers_url, timeout=30)
            response.raise_for_status()
            tickers_data = response.json()
        except Exception as e:
            self.logger.error("Failed to fetch company tickers: %s", e)
            return

        companies = []
        for entry in tickers_data.values():
            cik = entry.get("cik_str")
            if cik:
                companies.append({
                    "cik": cik,
                    "ticker": entry.get("ticker", ""),
                    "title": entry.get("title", ""),
                })

        total_companies = len(companies)
        self.logger.info("Found %d companies to process", total_companies)

        # Resume from checkpoint
        start_from = (checkpoint or {}).get("last_url")
        past_checkpoint = start_from is None
        contacts_yielded = 0
        pages_done = 0

        # Phase 2: Fetch each company's submission data
        for idx, company in enumerate(companies):
            cik = company["cik"]
            cik_padded = str(cik).zfill(10)
            submission_url = f"{self.BASE_URL}/submissions/CIK{cik_padded}.json"

            # Skip until past checkpoint
            if not past_checkpoint:
                if submission_url == start_from:
                    past_checkpoint = True
                continue

            # Rate limiting
            if self.rate_limiter:
                self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

            try:
                response = self.session.get(submission_url, timeout=30)
                response.raise_for_status()
                html = response.text
                self.stats["pages_scraped"] += 1
            except Exception as e:
                self.stats["errors"] += 1
                self.logger.warning("Error fetching %s: %s", submission_url, e)
                continue

            # Parse contacts
            try:
                contacts = self.scrape_page(submission_url, html)
            except Exception as e:
                self.stats["errors"] += 1
                self.logger.error("Parse error on %s: %s", submission_url, e)
                continue

            for contact in contacts:
                contact.source_platform = self.SOURCE_NAME
                contact.source_url = submission_url
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
            if pages_done % 100 == 0:
                self.logger.info(
                    "Progress: %d/%d companies, %d valid contacts",
                    pages_done, total_companies, self.stats["contacts_valid"],
                )

            if max_pages and pages_done >= max_pages:
                self.logger.info("Reached max_pages=%d", max_pages)
                break

        self.logger.info("Scraper complete: %s", self.stats)
