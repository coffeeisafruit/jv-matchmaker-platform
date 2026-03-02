"""
Census Bureau County Business Patterns (CBP) Scraper

Uses the Census Bureau's API to fetch County Business Patterns data and
Business Dynamics Statistics. This provides aggregate business counts by
industry (NAICS), geography (state/county/zip), and establishment size.

API: https://api.census.gov/data/
Documentation: https://www.census.gov/data/developers/data-sets.html

The CBP dataset provides:
- Number of establishments by NAICS code, by county
- Employment size class breakdowns
- Annual payroll data
- Year-over-year business dynamics (openings, closings, expansions)

No API key required for basic access (limited to ~500 calls/day).
For higher volume, set CENSUS_API_KEY environment variable.
Free key at: https://api.census.gov/data/key_signup.html

This scraper generates contacts representing business clusters --
geographic areas with high concentrations of specific industries --
which are valuable for targeting JV partner outreach.
"""

import json
import os
import time
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# NAICS codes for JV-relevant industries (2-digit and select 3-digit)
JV_NAICS_CODES = {
    # Professional and Business Services
    "54": "Professional, Scientific, and Technical Services",
    "541": "Professional, Scientific, and Technical Services",
    "5411": "Legal Services",
    "5412": "Accounting, Tax, Bookkeeping, Payroll",
    "5413": "Architectural, Engineering Services",
    "5414": "Specialized Design Services",
    "5415": "Computer Systems Design",
    "5416": "Management, Scientific, Technical Consulting",
    "5417": "Scientific Research and Development",
    "5418": "Advertising, PR, and Related Services",
    "5419": "Other Professional Services",
    # Administrative Services
    "56": "Administrative and Support Services",
    "5611": "Office Administrative Services",
    "5612": "Facilities Support Services",
    "5613": "Employment Services",
    "5614": "Business Support Services",
    "5615": "Travel Arrangement and Reservation",
    "5616": "Investigation and Security Services",
    "5617": "Services to Buildings and Dwellings",
    # Construction
    "23": "Construction",
    "236": "Construction of Buildings",
    "237": "Heavy and Civil Engineering",
    "238": "Specialty Trade Contractors",
    # Finance and Insurance
    "52": "Finance and Insurance",
    "523": "Securities, Commodity Contracts",
    "524": "Insurance Carriers and Related",
    # Information
    "51": "Information",
    "511": "Publishing Industries",
    "518": "Data Processing, Hosting",
    # Education
    "61": "Educational Services",
    "611": "Educational Services",
    # Health Care
    "62": "Health Care and Social Assistance",
    "621": "Ambulatory Health Care",
    # Real Estate
    "53": "Real Estate and Rental and Leasing",
    "531": "Real Estate",
}

# US State FIPS codes for geographic iteration
STATE_FIPS = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY",
}

# Major states to prioritize (largest business populations)
PRIORITY_STATES = [
    "06", "48", "12", "36", "17",  # CA, TX, FL, NY, IL
    "42", "39", "13", "37", "26",  # PA, OH, GA, NC, MI
    "34", "51", "53", "25", "04",  # NJ, VA, WA, MA, AZ
    "18", "47", "29", "24", "55",  # IN, TN, MO, MD, WI
    "08", "27", "21", "41", "22",  # CO, MN, KY, OR, LA
    "01", "45", "09", "40", "19",  # AL, SC, CT, OK, IA
    "49", "32", "05", "20", "28",  # UT, NV, AR, KS, MS
    "31", "35", "16", "15", "23",  # NE, NM, ID, HI, ME
    "33", "44", "11", "10", "30",  # NH, RI, DC, DE, MT
    "46", "38", "50", "56", "02",  # SD, ND, VT, WY, AK
]


class Scraper(BaseScraper):
    """Census Bureau County Business Patterns API scraper."""

    SOURCE_NAME = "census_business"
    BASE_URL = "https://api.census.gov"
    REQUESTS_PER_MINUTE = 15  # Census API is relatively permissive

    # Most recent CBP data year available
    DATA_YEAR = "2021"  # Latest complete CBP release

    # Minimum establishment count to generate a contact
    MIN_ESTABLISHMENTS = 10

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self.api_key = os.environ.get("CENSUS_API_KEY", "")
        self._seen_keys: set[str] = set()

        if not self.api_key:
            self.logger.info(
                "CENSUS_API_KEY not set. Using keyless access (lower rate limit). "
                "Get a free key at https://api.census.gov/data/key_signup.html"
            )

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run() for multi-query API access."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used -- we override run() for multi-query API access."""
        return []

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Query Census CBP API for business patterns by state and NAICS.

        For each state + NAICS code combination, retrieves county-level
        business establishment counts. Generates contacts for counties
        with significant industry concentration.
        """
        self.logger.info(
            "Starting Census Bureau CBP scraper (year=%s, %d states, %d NAICS codes)",
            self.DATA_YEAR, len(PRIORITY_STATES), len(JV_NAICS_CODES),
        )

        contacts_yielded = 0
        pages_done = 0

        # Resume from checkpoint
        start_state_idx = (checkpoint or {}).get("state_idx", 0)
        start_naics_idx = (checkpoint or {}).get("naics_idx", 0)

        # Select NAICS codes to query (use broader codes to reduce API calls)
        # Focus on 2-digit and 3-digit codes for county-level aggregation
        naics_to_query = [
            code for code in JV_NAICS_CODES.keys()
            if len(code) <= 3
        ]

        for state_idx, state_fips in enumerate(PRIORITY_STATES):
            if state_idx < start_state_idx:
                continue

            state_abbrev = STATE_FIPS.get(state_fips, state_fips)
            self.logger.info(
                "Processing state %s (%d/%d)",
                state_abbrev, state_idx + 1, len(PRIORITY_STATES),
            )

            for naics_idx, naics_code in enumerate(naics_to_query):
                if state_idx == start_state_idx and naics_idx < start_naics_idx:
                    continue

                if self.rate_limiter:
                    self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

                # Build CBP API URL
                # Variables: ESTAB (establishments), PAYANN (annual payroll),
                #            EMP (employees), NAICS2017 (industry code)
                api_url = (
                    f"https://api.census.gov/data/{self.DATA_YEAR}/cbp"
                    f"?get=ESTAB,PAYANN,EMP,NAICS2017,NAICS2017_LABEL,NAME"
                    f"&for=county:*"
                    f"&in=state:{state_fips}"
                    f"&NAICS2017={naics_code}"
                )

                if self.api_key:
                    api_url += f"&key={self.api_key}"

                try:
                    resp = self.session.get(api_url, timeout=60)

                    if resp.status_code == 204:
                        # No data for this combination
                        continue

                    if resp.status_code == 429:
                        self.logger.warning("Rate limited, waiting 60 seconds")
                        time.sleep(60)
                        continue

                    resp.raise_for_status()

                    # Census API returns a list of lists (first row = headers)
                    data = resp.json()
                    self.stats["pages_scraped"] += 1

                except Exception as e:
                    self.logger.warning(
                        "API error for state %s NAICS %s: %s",
                        state_abbrev, naics_code, e,
                    )
                    self.stats["errors"] += 1
                    continue

                if not data or len(data) < 2:
                    continue

                # Parse header row and data rows
                headers = data[0]
                rows = data[1:]

                pages_done += 1

                for row in rows:
                    contact = self._parse_cbp_row(
                        headers, row, state_abbrev, state_fips
                    )
                    if contact:
                        contacts_yielded += 1
                        yield contact

                        if max_contacts and contacts_yielded >= max_contacts:
                            self.logger.info("Reached max_contacts=%d", max_contacts)
                            return

                if pages_done % 50 == 0:
                    self.logger.info(
                        "Progress: %d API calls, %d valid contacts",
                        pages_done, self.stats["contacts_valid"],
                    )

                if max_pages and pages_done >= max_pages:
                    self.logger.info("Reached max_pages=%d", max_pages)
                    return

            self.logger.info(
                "State %s complete: %d valid contacts so far",
                state_abbrev, self.stats["contacts_valid"],
            )

        self.logger.info("Census CBP scraper complete: %s", self.stats)

    def _parse_cbp_row(
        self,
        headers: list,
        row: list,
        state_abbrev: str,
        state_fips: str,
    ) -> Optional[ScrapedContact]:
        """Parse a single CBP API response row into a ScrapedContact.

        Each row represents a county + NAICS code combination with
        aggregate business statistics.
        """
        # Map headers to values
        record = {}
        for i, header in enumerate(headers):
            record[header] = row[i] if i < len(row) else ""

        # Extract fields
        estab_str = (record.get("ESTAB") or "0").strip()
        payann_str = (record.get("PAYANN") or "0").strip()
        emp_str = (record.get("EMP") or "0").strip()
        naics_code = (record.get("NAICS2017") or "").strip()
        naics_label = (record.get("NAICS2017_LABEL") or "").strip()
        county_name = (record.get("NAME") or "").strip()
        county_fips = (record.get("county") or "").strip()

        # Parse numeric values
        try:
            establishments = int(estab_str)
        except (ValueError, TypeError):
            establishments = 0

        try:
            annual_payroll = int(payann_str) * 1000  # CBP payroll in thousands
        except (ValueError, TypeError):
            annual_payroll = 0

        try:
            employees = int(emp_str)
        except (ValueError, TypeError):
            employees = 0

        # Filter: need meaningful establishment count
        if establishments < self.MIN_ESTABLISHMENTS:
            return None

        # Skip if NAICS is "00" (total for all industries)
        if naics_code in ("00", "0", ""):
            return None

        # Deduplicate by state+county+naics
        dedup_key = f"{state_fips}_{county_fips}_{naics_code}"
        if dedup_key in self._seen_keys:
            return None
        self._seen_keys.add(dedup_key)

        # Build a descriptive name for this business cluster
        # e.g., "Professional Services Cluster - Los Angeles County, CA"
        industry_name = naics_label or JV_NAICS_CODES.get(naics_code, f"NAICS {naics_code}")

        # Clean up county name (remove ", State" suffix if present)
        if "," in county_name:
            county_name = county_name.split(",")[0].strip()

        cluster_name = f"{industry_name} - {county_name}, {state_abbrev}"

        # Build bio with statistics
        bio_parts = [cluster_name]
        bio_parts.append(f"{establishments:,} establishments")
        if employees > 0:
            bio_parts.append(f"{employees:,} employees")
        if annual_payroll > 0:
            bio_parts.append(f"Annual payroll: ${annual_payroll:,.0f}")
        bio_parts.append(f"NAICS: {naics_code}")
        bio = " | ".join(bio_parts)

        # Use Census data explorer as the website
        website = (
            f"https://data.census.gov/table/CBP{self.DATA_YEAR}.CB{self.DATA_YEAR[2:]}00CBP"
            f"?g=050XX00US{state_fips}{county_fips}"
            f"&n={naics_code}"
        )

        # Determine source category
        if naics_code.startswith("23"):
            source_category = "census_construction"
        elif naics_code.startswith("54"):
            source_category = "census_professional_services"
        elif naics_code.startswith("52"):
            source_category = "census_finance"
        elif naics_code.startswith("56"):
            source_category = "census_admin_services"
        else:
            source_category = "census_business_patterns"

        contact = ScrapedContact(
            name=cluster_name,
            email="",
            company=f"{county_name}, {state_abbrev} - {industry_name}",
            website=website,
            linkedin="",
            phone="",
            bio=bio,
            source_platform=self.SOURCE_NAME,
            source_url=f"https://api.census.gov/data/{self.DATA_YEAR}/cbp",
            source_category=source_category,
            scraped_at=datetime.now().isoformat(),
            raw_data={
                "state": state_abbrev,
                "state_fips": state_fips,
                "county_fips": county_fips,
                "county_name": county_name,
                "naics_code": naics_code,
                "naics_label": naics_label,
                "establishments": establishments,
                "employees": employees,
                "annual_payroll": annual_payroll,
                "data_year": self.DATA_YEAR,
            },
        )

        if not contact.is_valid():
            return None

        self.stats["contacts_found"] += 1
        self.stats["contacts_valid"] += 1
        return contact
