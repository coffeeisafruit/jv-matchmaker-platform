"""
SBA PPP Loan Data Scraper

Fetches Paycheck Protection Program (PPP) loan data from SBA's FOIA dataset.
Contains millions of business records with names, addresses, loan amounts,
NAICS codes, and job retention numbers.

Data source: https://data.sba.gov/dataset/ppp-foia
The SBA publishes bulk CSV files organized by state. We download and parse
these CSVs to extract business contacts.

The SBA data API endpoint provides a CKAN-based catalog. We query the
package metadata to discover CSV resource URLs, then stream-download each.
"""

import csv
import io
import json
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# NAICS prefixes most relevant to JV partnerships
JV_RELEVANT_NAICS_PREFIXES = {
    "5411",  # Legal services
    "5412",  # Accounting/bookkeeping
    "5413",  # Architectural/engineering
    "5414",  # Specialized design
    "5415",  # Computer systems design
    "5416",  # Management/scientific/technical consulting
    "5417",  # Scientific R&D
    "5418",  # Advertising/PR
    "5419",  # Other professional services
    "5511",  # Management of companies
    "5611",  # Office administrative services
    "5612",  # Facilities support services
    "5613",  # Employment services
    "5614",  # Business support services
    "5615",  # Travel arrangement
    "5616",  # Investigation/security
    "5617",  # Services to buildings
    "5619",  # Other support services
    "6114",  # Business schools / training
    "6117",  # Educational support services
    "5221",  # Depository credit intermediation
    "5222",  # Nondepository credit
    "5231",  # Securities/commodity contracts
    "5239",  # Other financial investment
    "5241",  # Insurance carriers
    "5242",  # Insurance agencies
    "2361",  # Residential building construction
    "2362",  # Nonresidential building construction
    "2371",  # Utility system construction
    "2372",  # Land subdivision
    "2373",  # Highway/street/bridge construction
    "2379",  # Other heavy construction
    "2381",  # Foundation/structure contractors
    "2382",  # Building equipment contractors
    "2383",  # Building finishing contractors
    "2389",  # Other specialty trade contractors
}


class Scraper(BaseScraper):
    """SBA PPP Loan data scraper using CKAN data API."""

    SOURCE_NAME = "sba_loans"
    BASE_URL = "https://data.sba.gov"
    REQUESTS_PER_MINUTE = 5  # Be gentle with SBA servers

    # Direct CSV URLs for PPP FOIA data (public bulk downloads)
    # These are the known dataset resource URLs from data.sba.gov
    CKAN_PACKAGE_URL = "https://data.sba.gov/api/3/action/package_show?id=paycheck-protection-program-ppp-data"

    # Fallback: known direct download URLs for PPP data
    FALLBACK_CSV_URLS = [
        "https://data.sba.gov/dataset/ppp-foia/resource/aab8e9f9-36d1-42e1-b3ba-e59c79f1d7f0/download/public_150k_plus_230930.csv",
        "https://data.sba.gov/dataset/ppp-foia/resource/cdc0eb79-8983-4076-b5ea-1bc710e7e4a5/download/public_up_to_150k_230930.csv",
    ]

    # Minimum loan amount to filter for substantial businesses
    MIN_LOAN_AMOUNT = 50000

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_names: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run() to handle CSV bulk downloads."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used -- we override run() to handle CSV bulk downloads."""
        return []

    def _discover_csv_urls(self) -> list[str]:
        """Query CKAN API to discover CSV resource download URLs."""
        self.logger.info("Querying CKAN API for PPP dataset resources...")

        try:
            data = self.fetch_json(self.CKAN_PACKAGE_URL, timeout=60)
            if not data or not data.get("success"):
                self.logger.warning("CKAN API returned no data, using fallback URLs")
                return self.FALLBACK_CSV_URLS

            result = data.get("result", {})
            resources = result.get("resources", [])

            csv_urls = []
            for resource in resources:
                url = (resource.get("url") or "").strip()
                fmt = (resource.get("format") or "").upper()
                name = (resource.get("name") or "").lower()

                # Only include CSV resources that look like PPP loan data
                if url and (fmt == "CSV" or url.endswith(".csv")):
                    csv_urls.append(url)
                    self.logger.info("Found CSV resource: %s (%s)", name, url[:80])

            if csv_urls:
                return csv_urls

        except Exception as e:
            self.logger.warning("CKAN API discovery failed: %s", e)

        self.logger.info("Using %d fallback CSV URLs", len(self.FALLBACK_CSV_URLS))
        return self.FALLBACK_CSV_URLS

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Download and parse SBA PPP loan CSV files.

        Each CSV contains columns like:
        - BorrowerName, BorrowerAddress, BorrowerCity, BorrowerState, BorrowerZip
        - CurrentApprovalAmount, ForgivenessAmount, NAICSCode
        - BusinessType, JobsReported, LoanStatus
        """
        self.logger.info("Starting SBA PPP Loan scraper")

        # Resume support
        start_file_idx = (checkpoint or {}).get("file_index", 0)
        start_row_idx = (checkpoint or {}).get("row_index", 0)
        contacts_yielded = 0

        csv_urls = self._discover_csv_urls()
        if not csv_urls:
            self.logger.error("No CSV URLs found")
            return

        self.logger.info("Processing %d CSV files", len(csv_urls))

        for file_idx, csv_url in enumerate(csv_urls):
            if file_idx < start_file_idx:
                self.logger.info("Skipping file %d (before checkpoint)", file_idx)
                continue

            self.logger.info(
                "Downloading file %d/%d: %s",
                file_idx + 1, len(csv_urls), csv_url[:100],
            )

            if self.rate_limiter:
                self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

            try:
                resp = self.session.get(csv_url, timeout=600, stream=True)
                resp.raise_for_status()

                # Stream the response to handle large files
                content = resp.text
                self.logger.info(
                    "Downloaded file %d (%.2f MB)",
                    file_idx + 1, len(resp.content) / 1024 / 1024,
                )
                self.stats["pages_scraped"] += 1

            except Exception as e:
                self.logger.error("Failed to download %s: %s", csv_url[:80], e)
                self.stats["errors"] += 1
                continue

            # Parse CSV — clean up embedded newlines in unquoted fields
            try:
                reader = csv.DictReader(io.StringIO(content.replace('\r', '')), restkey='_extra')
            except Exception as e:
                self.logger.error("Failed to parse CSV from %s: %s", csv_url[:80], e)
                self.stats["errors"] += 1
                continue

            for row_idx, row in enumerate(reader):
                # Skip rows before checkpoint (only for resume file)
                if file_idx == start_file_idx and row_idx < start_row_idx:
                    continue

                contact = self._parse_row(row, csv_url)
                if contact:
                    self.stats["contacts_valid"] += 1
                    contacts_yielded += 1
                    yield contact

                    if max_contacts and contacts_yielded >= max_contacts:
                        self.logger.info("Reached max_contacts=%d", max_contacts)
                        return

                self.stats["contacts_found"] += 1

                # Log progress every 10K rows
                if row_idx > 0 and row_idx % 10000 == 0:
                    self.logger.info(
                        "File %d progress: %d rows, %d valid contacts",
                        file_idx + 1, row_idx, self.stats["contacts_valid"],
                    )

            self.logger.info(
                "Completed file %d/%d: %d valid contacts so far",
                file_idx + 1, len(csv_urls), self.stats["contacts_valid"],
            )

        self.logger.info("SBA PPP scraper complete: %s", self.stats)

    def _parse_row(self, row: dict, source_url: str) -> Optional[ScrapedContact]:
        """Parse a single PPP loan CSV row into a ScrapedContact."""
        # Column names vary slightly between PPP datasets; handle common variants
        name = (
            row.get("BorrowerName", "")
            or row.get("borrower_name", "")
            or row.get("BorrowerName ", "")  # Some CSVs have trailing space
            or ""
        ).strip()

        if not name or len(name) < 3:
            return None

        # Skip generic/invalid names
        name_upper = name.upper()
        skip_patterns = [
            "N/A", "UNKNOWN", "TEST", "NONE", "NULL",
            "SELF EMPLOYED", "SOLE PROPRIETOR",
        ]
        if any(pat in name_upper for pat in skip_patterns):
            return None

        # Deduplicate by normalized name
        name_key = name_upper.strip()
        if name_key in self._seen_names:
            return None
        self._seen_names.add(name_key)

        # Extract loan amount
        loan_amount_str = (
            row.get("CurrentApprovalAmount", "")
            or row.get("current_approval_amount", "")
            or row.get("InitialApprovalAmount", "")
            or row.get("initial_approval_amount", "")
            or "0"
        ).strip()

        try:
            loan_amount = float(loan_amount_str.replace(",", "").replace("$", ""))
        except (ValueError, TypeError):
            loan_amount = 0

        # Filter: only businesses with substantial loans (likely real businesses)
        if loan_amount < self.MIN_LOAN_AMOUNT:
            return None

        # Extract NAICS code
        naics = (
            row.get("NAICSCode", "")
            or row.get("naics_code", "")
            or ""
        ).strip()

        # Extract address
        address = (
            row.get("BorrowerAddress", "")
            or row.get("borrower_address", "")
            or ""
        ).strip()
        city = (
            row.get("BorrowerCity", "")
            or row.get("borrower_city", "")
            or ""
        ).strip()
        state = (
            row.get("BorrowerState", "")
            or row.get("borrower_state", "")
            or ""
        ).strip()
        zipcode = (
            row.get("BorrowerZip", "")
            or row.get("borrower_zip", "")
            or ""
        ).strip()

        # Extract other fields
        business_type = (
            row.get("BusinessType", "")
            or row.get("business_type", "")
            or ""
        ).strip()
        jobs_reported = (
            row.get("JobsReported", "")
            or row.get("jobs_reported", "")
            or ""
        ).strip()
        forgiveness_amount_str = (
            row.get("ForgivenessAmount", "")
            or row.get("forgiveness_amount", "")
            or "0"
        ).strip()
        loan_status = (
            row.get("LoanStatus", "")
            or row.get("loan_status", "")
            or ""
        ).strip()
        lender = (
            row.get("ServicingLenderName", "")
            or row.get("OriginatingLenderName", "")
            or row.get("originating_lender", "")
            or ""
        ).strip()

        try:
            forgiveness_amount = float(forgiveness_amount_str.replace(",", "").replace("$", ""))
        except (ValueError, TypeError):
            forgiveness_amount = 0

        # Must have at least city and state for useful contact
        if not city or not state:
            return None

        # Build bio
        bio_parts = [name]
        if city and state:
            bio_parts.append(f"{city}, {state}")
        if business_type:
            bio_parts.append(business_type)
        if loan_amount > 0:
            bio_parts.append(f"PPP Loan: ${loan_amount:,.0f}")
        if naics:
            bio_parts.append(f"NAICS: {naics}")
        if jobs_reported:
            bio_parts.append(f"Jobs: {jobs_reported}")
        bio = " | ".join(bio_parts)

        # Build full address
        full_address = ""
        if address:
            full_address = address
            if city:
                full_address += f", {city}"
            if state:
                full_address += f", {state}"
            if zipcode:
                full_address += f" {zipcode}"

        # Use SBA data page as website for is_valid() check
        website = "https://data.sba.gov/dataset/ppp-foia"

        # Determine source category
        if naics and any(naics.startswith(prefix) for prefix in JV_RELEVANT_NAICS_PREFIXES):
            source_category = "professional_services"
        elif naics and naics.startswith("23"):
            source_category = "construction"
        else:
            source_category = "ppp_loan_recipients"

        contact = ScrapedContact(
            name=name,
            email="",
            company=name,
            website=website,
            linkedin="",
            phone="",
            bio=bio,
            source_platform=self.SOURCE_NAME,
            source_url=source_url,
            source_category=source_category,
            scraped_at=datetime.now().isoformat(),
            raw_data={
                "loan_amount": loan_amount,
                "forgiveness_amount": forgiveness_amount,
                "naics_code": naics,
                "business_type": business_type,
                "jobs_reported": jobs_reported,
                "loan_status": loan_status,
                "lender": lender,
                "address": full_address,
                "city": city,
                "state": state,
                "zip": zipcode,
            },
        )

        if not contact.is_valid():
            return None

        return contact
