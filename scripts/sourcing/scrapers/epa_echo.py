"""
EPA ECHO (Enforcement and Compliance History Online) Scraper

Scrapes business facilities with environmental permits from the EPA ECHO API.
These are real operating businesses — manufacturers, processors, waste handlers,
construction companies, food producers, chemical plants, etc. — all potential
JV partners for environmental, engineering, construction, and industrial services.

API: https://echodata.epa.gov/echo/echo_rest_services
No API key required. Two-step process:
  1. get_facility_info — create a search query, returns QueryID + row count
  2. get_qid — paginate through facility results using the QueryID

Total active facilities: ~800K–1.2M across all US states.
Each record includes: company name, full address, SIC codes, compliance status,
EPA region, latitude, and inspection history.

Rate limit: be reasonable (6 req/min — the API can be slow).
"""

import re
from typing import Iterator, Optional
from datetime import datetime

from scripts.sourcing.base import BaseScraper, ScrapedContact


# All US state codes for iteration
US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC", "PR", "VI", "GU", "AS", "MP",
]

# SIC code prefix descriptions (2-digit) for bio enrichment
SIC_DESCRIPTIONS = {
    "01": "Agricultural Crops", "02": "Agricultural Livestock",
    "07": "Agricultural Services", "08": "Forestry",
    "09": "Fishing/Hunting/Trapping", "10": "Metal Mining",
    "12": "Coal Mining", "13": "Oil & Gas Extraction",
    "14": "Nonmetallic Minerals Mining", "15": "General Building Contractors",
    "16": "Heavy Construction", "17": "Special Trade Contractors",
    "20": "Food Products", "21": "Tobacco Products",
    "22": "Textile Mill Products", "23": "Apparel",
    "24": "Lumber & Wood Products", "25": "Furniture & Fixtures",
    "26": "Paper Products", "27": "Printing & Publishing",
    "28": "Chemicals", "29": "Petroleum Refining",
    "30": "Rubber & Plastics", "31": "Leather Products",
    "32": "Stone/Clay/Glass Products", "33": "Primary Metal Industries",
    "34": "Fabricated Metal Products", "35": "Industrial Machinery",
    "36": "Electronic Equipment", "37": "Transportation Equipment",
    "38": "Instruments & Related", "39": "Misc Manufacturing",
    "40": "Railroad Transportation", "41": "Local Transit",
    "42": "Trucking & Warehousing", "43": "US Postal Service",
    "44": "Water Transportation", "45": "Air Transportation",
    "46": "Pipelines", "47": "Transportation Services",
    "48": "Communications", "49": "Electric/Gas/Sanitary Services",
    "50": "Durable Goods Wholesale", "51": "Nondurable Goods Wholesale",
    "52": "Building Materials Retail", "53": "General Merchandise Stores",
    "54": "Food Stores", "55": "Auto Dealers & Gas Stations",
    "56": "Apparel & Accessory Stores", "57": "Furniture Stores",
    "58": "Eating & Drinking Places", "59": "Misc Retail",
    "60": "Banking", "61": "Credit Institutions",
    "62": "Securities & Commodities", "63": "Insurance Carriers",
    "64": "Insurance Agents", "65": "Real Estate",
    "70": "Hotels & Lodging", "72": "Personal Services",
    "73": "Business Services", "75": "Auto Repair & Services",
    "76": "Misc Repair Services", "78": "Motion Pictures",
    "79": "Amusement & Recreation", "80": "Health Services",
    "81": "Legal Services", "82": "Educational Services",
    "83": "Social Services", "84": "Museums & Galleries",
    "86": "Membership Organizations", "87": "Engineering & Management",
    "89": "Services NEC", "91": "Executive/Legislative",
    "92": "Justice/Public Order", "93": "Finance/Taxation",
    "94": "Administration Human Resources", "95": "Environmental Quality",
    "96": "Administration Economic Programs", "97": "National Security",
    "99": "Nonclassifiable",
}

# Common street suffixes for detecting address-as-name entries
_STREET_SUFFIXES = (
    r"(?:ST|STREET|AVE|AVENUE|BLVD|BOULEVARD|DR|DRIVE|RD|ROAD|"
    r"LN|LANE|CT|COURT|WAY|PL|PLACE|PKWY|PARKWAY|CIR|CIRCLE|"
    r"TRL|TRAIL|TERR?|TERRACE|CREST|LOOP|RUN|PASS|PATH|RIDGE|"
    r"PIKE|HWY|HIGHWAY|SR|CR|RTE|ROUTE|SITE)"
)

# Regex: name that starts with digits followed by street-like words
_ADDR_NAME_RE = re.compile(
    rf"^\d+\s+.*\b{_STREET_SUFFIXES}\b",
    re.IGNORECASE,
)

# Regex: alphanumeric code/ID (e.g., "0A-007HH1U03")
_CODE_NAME_RE = re.compile(r"^[0-9A-Z][\d\-A-Z]+$")

# Regex: purely numeric (with spaces/dashes)
_NUMERIC_RE = re.compile(r"^[\d\s\-\.]+$")


class Scraper(BaseScraper):
    SOURCE_NAME = "epa_echo"
    BASE_URL = "https://echodata.epa.gov/echo"
    REQUESTS_PER_MINUTE = 6  # API can be slow; be respectful

    PAGE_SIZE = 100  # Results per page from get_qid

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self.session.headers["Accept"] = "application/json"
        self._seen_registry_ids: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used — we override run() for the 2-step ECHO API."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used — we override run() for the 2-step ECHO API."""
        return []

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """
        For each US state:
          1. Call get_facility_info to create a query (returns QueryID)
          2. Paginate through get_qid to retrieve facility records
          3. Parse each facility into a ScrapedContact

        Yields ScrapedContact objects.
        """
        self.logger.info(
            "Starting %s scraper — %d states to process",
            self.SOURCE_NAME, len(US_STATES),
        )

        # Resume from checkpoint
        start_state_idx = (checkpoint or {}).get("state_idx", 0)
        contacts_yielded = 0
        pages_done = 0

        for state_idx, state_code in enumerate(US_STATES):
            if state_idx < start_state_idx:
                continue

            self.logger.info(
                "Processing state %s (%d/%d)",
                state_code, state_idx + 1, len(US_STATES),
            )

            # Step 1: Create query for this state (active facilities only)
            query_id, total_rows = self._create_query(state_code)
            if not query_id:
                self.logger.warning(
                    "Failed to create query for %s", state_code
                )
                continue

            self.logger.info(
                "State %s: QueryID=%s, %s facilities",
                state_code, query_id, total_rows,
            )

            # Step 2: Paginate through results
            page_no = 1
            while True:
                if self.rate_limiter:
                    self.rate_limiter.wait(
                        self.SOURCE_NAME, self.REQUESTS_PER_MINUTE
                    )

                url = (
                    f"{self.BASE_URL}/echo_rest_services.get_qid"
                    f"?output=JSON&qid={query_id}"
                    f"&pageno={page_no}&pagesize={self.PAGE_SIZE}"
                )

                try:
                    resp = self.session.get(url, timeout=60)
                    resp.raise_for_status()
                    data = resp.json()
                    self.stats["pages_scraped"] += 1
                except Exception as e:
                    self.stats["errors"] += 1
                    self.logger.warning(
                        "get_qid error for %s page %d: %s",
                        state_code, page_no, e,
                    )
                    break

                facilities = (
                    data.get("Results", {}).get("Facilities", [])
                )
                if not facilities:
                    break

                for fac in facilities:
                    contact = self._parse_facility(fac, state_code)
                    if contact:
                        contacts_yielded += 1
                        yield contact

                        if max_contacts and contacts_yielded >= max_contacts:
                            self.logger.info(
                                "Reached max_contacts=%d", max_contacts
                            )
                            return

                pages_done += 1
                page_no += 1

                if pages_done % 20 == 0:
                    self.logger.info(
                        "Progress: %d pages, %d valid contacts",
                        pages_done, self.stats["contacts_valid"],
                    )

                if max_pages and pages_done >= max_pages:
                    self.logger.info("Reached max_pages=%d", max_pages)
                    return

                # Check if we've fetched all rows
                if page_no * self.PAGE_SIZE > int(total_rows) + self.PAGE_SIZE:
                    break

            self.logger.info(
                "State %s done: %d valid contacts so far",
                state_code, self.stats["contacts_valid"],
            )

        self.logger.info("Scraper complete: %s", self.stats)

    def _create_query(self, state_code: str) -> tuple[str, str]:
        """
        Step 1: Call get_facility_info to create a search query.
        Returns (query_id, total_rows) or ("", "0") on failure.
        """
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        url = (
            f"{self.BASE_URL}/echo_rest_services.get_facility_info"
            f"?output=JSON&p_st={state_code}&p_act=Y"
            f"&responseset={self.PAGE_SIZE}"
        )

        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            self.stats["errors"] += 1
            self.logger.warning(
                "get_facility_info error for %s: %s", state_code, e
            )
            return ("", "0")

        results = data.get("Results", {})

        # Check for error
        if "Error" in results:
            self.logger.warning(
                "ECHO API error for %s: %s",
                state_code,
                results["Error"].get("ErrorMessage", "Unknown"),
            )
            return ("", "0")

        query_id = (results.get("QueryID") or "").strip()
        total_rows = (results.get("QueryRows") or "0").strip()

        return (query_id, total_rows)

    @staticmethod
    def _is_business_name(name: str) -> bool:
        """
        Filter out facility names that are really addresses, parcel IDs,
        or project codes rather than business names.
        """
        # Very short names are usually codes
        if len(name) < 4:
            return False

        # Purely numeric or numeric with separators
        if _NUMERIC_RE.match(name):
            return False

        # Starts with "0 " — typically a parcel/address entry
        if name.startswith("0 "):
            return False

        # Looks like a street address (digits + street suffix)
        if _ADDR_NAME_RE.match(name):
            return False

        # Looks like an alphanumeric code/ID
        if _CODE_NAME_RE.match(name) and len(name) < 20:
            return False

        return True

    def _parse_facility(
        self, fac: dict, state_code: str
    ) -> ScrapedContact | None:
        """Parse a single ECHO facility record into a ScrapedContact."""
        name = (fac.get("FacName") or "").strip()
        if not name or len(name) < 3:
            return None

        # Skip entries with unknown/placeholder names
        name_upper = name.upper()
        skip_names = {
            "UNKNOWN", "N/A", "NA", "NONE", "TEST", "DELETED",
            "CLOSED", "NOT APPLICABLE", "TBD", "INACTIVE",
            "NO NAME", "UNNAMED", "TEMP", "TEMPORARY",
        }
        if name_upper in skip_names:
            return None

        # Skip names that are really addresses or project codes
        if not self._is_business_name(name):
            return None

        # Skip federal facilities
        if (fac.get("FacFederalFlg") or "").strip().upper() == "Y":
            return None

        # Deduplicate by RegistryID
        registry_id = (fac.get("RegistryID") or "").strip()
        if registry_id:
            if registry_id in self._seen_registry_ids:
                return None
            self._seen_registry_ids.add(registry_id)

        # Extract address fields
        street = (fac.get("FacStreet") or "").strip()
        city = (fac.get("FacCity") or "").strip()
        state = (fac.get("FacState") or state_code).strip()
        zip_code = (fac.get("FacZip") or "").strip()
        county = (fac.get("FacCounty") or "").strip()
        lat = (fac.get("FacLat") or "").strip()

        # Skip entries with unknown city AND unknown address
        city_upper = city.upper()
        street_upper = street.upper()
        unknown_values = {
            "UNKNOWN", "(UNKNOWN CITY)", "UNKNOWN CITY",
            "(UNKNOWN ADDRESS)", "UNKNOWN ADDRESS", "N/A", "",
        }
        if city_upper in unknown_values and street_upper in unknown_values:
            return None

        # Extract SIC codes and build industry description
        sic_codes_raw = (fac.get("FacSICCodes") or "").strip()
        sic_codes = [
            s.strip() for s in sic_codes_raw.split() if s.strip()
        ]
        naics_codes_raw = (fac.get("FacNAICSCodes") or "").strip()

        # Get industry from first SIC code
        industry = ""
        if sic_codes:
            prefix = sic_codes[0][:2]
            industry = SIC_DESCRIPTIONS.get(prefix, "")

        # Compliance status
        compliance = (fac.get("FacComplianceStatus") or "").strip()

        # Build ECHO profile URL as website
        website = ""
        if registry_id:
            website = (
                f"https://echo.epa.gov/detailed-facility-report"
                f"?fid={registry_id}"
            )

        # Build bio
        bio_parts = [name]
        if city and city_upper not in unknown_values:
            bio_parts.append(f"{city}, {state}")
        if industry:
            bio_parts.append(industry)
        if sic_codes:
            bio_parts.append(f"SIC: {', '.join(sic_codes[:3])}")
        if compliance and compliance != "No Violation Identified":
            bio_parts.append(f"Compliance: {compliance}")
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
            source_url=(
                f"{self.BASE_URL}/echo_rest_services.get_facility_info"
                f"?p_st={state}"
            ),
            source_category="environmental_facilities",
            scraped_at=datetime.now().isoformat(),
            raw_data={
                "registry_id": registry_id,
                "street": street,
                "city": city,
                "state": state,
                "zip": zip_code,
                "county": county,
                "latitude": lat,
                "sic_codes": sic_codes,
                "naics_codes": naics_codes_raw,
                "compliance_status": compliance,
                "active": (fac.get("FacActiveFlag") or "").strip(),
                "federal_facility": (
                    fac.get("FacFederalFlg") or ""
                ).strip(),
                "caa_flag": (fac.get("AIRFlag") or "").strip(),
                "tri_flag": (fac.get("TRIFlag") or "").strip(),
            },
        )

        if not contact.is_valid():
            return None

        self.stats["contacts_found"] += 1
        self.stats["contacts_valid"] += 1
        return contact
