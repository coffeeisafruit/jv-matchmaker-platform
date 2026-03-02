"""
GSA SAM.gov Entity Registration Scraper

Fetches federal contractor entity registration data from the SAM.gov
Entity Management API (v3). Contains all businesses registered to do
business with the federal government.

API: https://api.sam.gov/entity-information/v3/entities
Documentation: https://open.gsa.gov/api/entity-api/

Requires SAM_GOV_API_KEY environment variable.
Free API key available at: https://open.gsa.gov/api/entity-api/

Extracts: legal business name, UEI, CAGE code, website, physical address,
NAICS codes, business type, socio-economic categories, POC info.

This differs from the existing sam_gov.py scraper by:
- Using broader NAICS coverage (all business categories, not just services)
- Extracting more entity detail fields (CAGE, socio-economic status, etc.)
- Supporting bulk pagination through the full registry
- Including entity size and revenue data when available
"""

import json
import os
import time
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Broad NAICS prefixes covering major JV-relevant industries
# We use 2-digit prefixes to cast a wide net across the federal registry
NAICS_PREFIXES = [
    "23",    # Construction
    "31",    # Manufacturing (food, textile, apparel)
    "32",    # Manufacturing (wood, paper, chemical, plastics)
    "33",    # Manufacturing (metals, machinery, electronics)
    "42",    # Wholesale trade
    "48",    # Transportation
    "49",    # Warehousing
    "51",    # Information
    "52",    # Finance and Insurance
    "53",    # Real Estate
    "54",    # Professional, Scientific, Technical Services
    "55",    # Management of Companies
    "56",    # Administrative and Waste Services
    "61",    # Educational Services
    "62",    # Health Care and Social Assistance
    "71",    # Arts, Entertainment, Recreation
    "81",    # Other Services
]

# Specific 6-digit NAICS codes to query (most JV-relevant)
PRIORITY_NAICS_CODES = [
    # Construction
    "236220",  # Commercial building construction
    "237310",  # Highway and street construction
    "238210",  # Electrical contractors
    "238220",  # Plumbing/HVAC contractors
    # IT and Professional Services
    "541511",  # Custom computer programming
    "541512",  # Computer systems design
    "541519",  # Other computer services
    "541611",  # Administrative management consulting
    "541612",  # HR consulting
    "541613",  # Marketing consulting
    "541614",  # Logistics consulting
    "541618",  # Other management consulting
    "541620",  # Environmental consulting
    "541690",  # Other scientific/technical consulting
    "541715",  # R&D services
    "541990",  # All other professional services
    # Engineering
    "541310",  # Architectural services
    "541320",  # Landscape architecture
    "541330",  # Engineering services
    "541340",  # Drafting services
    "541350",  # Building inspection
    "541360",  # Geophysical surveying
    "541370",  # Surveying and mapping
    "541380",  # Testing laboratories
    # Business Services
    "561110",  # Office administrative services
    "561210",  # Facilities support services
    "561310",  # Employment placement
    "561320",  # Temporary help services
    "561330",  # Professional employer organizations
    "561410",  # Document preparation services
    "561499",  # Other business support services
    "561612",  # Security guards
    "561621",  # Security systems services
    "561710",  # Exterminating and pest control
    "561720",  # Janitorial services
    "561730",  # Landscaping services
    "561790",  # Other services to buildings
    # Marketing and Advertising
    "541810",  # Advertising agencies
    "541820",  # Public relations
    "541830",  # Media buying
    "541910",  # Marketing research
    # Training
    "611430",  # Professional development training
    "611710",  # Educational support services
]


class Scraper(BaseScraper):
    """SAM.gov Entity Registration API scraper (broad coverage)."""

    SOURCE_NAME = "gsa_sam"
    BASE_URL = "https://api.sam.gov"
    REQUESTS_PER_MINUTE = 8  # SAM API rate limits are fairly strict

    API_ENDPOINT = "https://api.sam.gov/entity-information/v3/entities"
    PAGE_SIZE = 100  # Max entities per page

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_ueis: set[str] = set()
        self._seen_names: set[str] = set()

        self.api_key = os.environ.get("SAM_GOV_API_KEY", "")
        if not self.api_key:
            self.logger.warning(
                "SAM_GOV_API_KEY not set. Get a free key at "
                "https://open.gsa.gov/api/entity-api/"
            )

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run() for API-based pagination."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used -- we override run() for API-based pagination."""
        return []

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Query SAM.gov Entity API by NAICS codes.

        Paginates through all active registrations for each NAICS code.
        Extracts entity details including business name, address, POC,
        CAGE code, socio-economic status, and business type.
        """
        if not self.api_key:
            self.logger.error("Cannot run without SAM_GOV_API_KEY")
            return

        self.logger.info(
            "Starting GSA SAM.gov entity scraper with %d NAICS codes",
            len(PRIORITY_NAICS_CODES),
        )

        contacts_yielded = 0
        pages_done = 0

        # Resume from checkpoint
        start_naics_idx = (checkpoint or {}).get("naics_idx", 0)

        for naics_idx, naics_code in enumerate(PRIORITY_NAICS_CODES):
            if naics_idx < start_naics_idx:
                continue

            self.logger.info(
                "Processing NAICS %s (%d/%d)",
                naics_code, naics_idx + 1, len(PRIORITY_NAICS_CODES),
            )

            page = 0
            consecutive_empty = 0

            while True:
                if self.rate_limiter:
                    self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

                # Build API query parameters
                params = {
                    "api_key": self.api_key,
                    "registrationStatus": "A",  # Active only
                    "naicsCode": naics_code,
                    "includeSections": (
                        "entityRegistration,coreData,"
                        "pointsOfContact,assertions"
                    ),
                    "page": page,
                    "size": self.PAGE_SIZE,
                }

                try:
                    resp = self.session.get(
                        self.API_ENDPOINT,
                        params=params,
                        timeout=60,
                    )

                    # Handle rate limiting
                    if resp.status_code == 429:
                        retry_after = int(resp.headers.get("Retry-After", 60))
                        self.logger.warning(
                            "Rate limited, waiting %d seconds", retry_after
                        )
                        time.sleep(retry_after)
                        continue

                    resp.raise_for_status()
                    data = resp.json()
                    self.stats["pages_scraped"] += 1
                except Exception as e:
                    self.logger.warning(
                        "API error for NAICS %s page %d: %s",
                        naics_code, page, e,
                    )
                    self.stats["errors"] += 1
                    # If we get a 403/404, skip this NAICS entirely
                    if hasattr(e, "response") and hasattr(e.response, "status_code"):
                        if e.response.status_code in (403, 404):
                            break
                    consecutive_empty += 1
                    if consecutive_empty >= 3:
                        break
                    continue

                # Parse entity data
                total_records = data.get("totalRecords", 0)
                entities = data.get("entityData", [])

                if not entities:
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        break
                    page += 1
                    continue

                consecutive_empty = 0

                for entity in entities:
                    contact = self._parse_entity(entity, naics_code)
                    if contact:
                        contacts_yielded += 1
                        yield contact

                        if max_contacts and contacts_yielded >= max_contacts:
                            self.logger.info("Reached max_contacts=%d", max_contacts)
                            return

                pages_done += 1

                if max_pages and pages_done >= max_pages:
                    self.logger.info("Reached max_pages=%d", max_pages)
                    return

                # Check if more pages available
                if (page + 1) * self.PAGE_SIZE >= total_records:
                    break

                page += 1

                # Safety: cap at 100 pages per NAICS
                if page >= 100:
                    self.logger.info(
                        "Hit page cap for NAICS %s (total: %d)", naics_code, total_records
                    )
                    break

            self.logger.info(
                "NAICS %s done: %d valid contacts so far",
                naics_code, self.stats["contacts_valid"],
            )

        self.logger.info("GSA SAM.gov scraper complete: %s", self.stats)

    def _parse_entity(
        self, entity: dict, query_naics: str
    ) -> Optional[ScrapedContact]:
        """Parse a SAM.gov entity record into a ScrapedContact."""
        entity_reg = entity.get("entityRegistration", {})
        core_data = entity.get("coreData", {})
        pocs = entity.get("pointsOfContact", {})
        assertions = entity.get("assertions", {})

        # UEI for deduplication
        uei = (entity_reg.get("ueiSAM") or "").strip()
        if not uei:
            return None
        if uei in self._seen_ueis:
            return None
        self._seen_ueis.add(uei)

        # Business name
        company = (entity_reg.get("legalBusinessName") or "").strip()
        if not company or len(company) < 3:
            return None

        # Deduplicate by normalized name too
        name_key = company.upper().strip()
        if name_key in self._seen_names:
            return None
        self._seen_names.add(name_key)

        # Skip obvious government entities
        gov_indicators = [
            "DEPARTMENT OF", "UNITED STATES", "U.S. ",
            "FEDERAL AGENCY", "STATE OF ", "COUNTY OF ", "CITY OF ",
            "GOVERNMENT OF", "MUNICIPALITY", "TRIBAL GOVERNMENT",
        ]
        if any(ind in name_key for ind in gov_indicators):
            return None

        # Extract fields
        dba_name = (entity_reg.get("dbaName") or "").strip()
        cage_code = (entity_reg.get("cageCode") or "").strip()
        website = (entity_reg.get("entityURL") or "").strip()
        registration_status = (entity_reg.get("registrationStatus") or "").strip()
        entity_start_date = (entity_reg.get("entityStartDate") or "").strip()
        fiscal_year_end = (entity_reg.get("fiscalYearEndCloseDate") or "").strip()
        submission_date = (entity_reg.get("submissionDate") or "").strip()

        # Physical address
        phys_addr = core_data.get("physicalAddress", {})
        address_line1 = (phys_addr.get("addressLine1") or "").strip()
        address_line2 = (phys_addr.get("addressLine2") or "").strip()
        city = (phys_addr.get("city") or "").strip()
        state = (phys_addr.get("stateOrProvinceCode") or "").strip()
        zipcode = (phys_addr.get("zipCode") or "").strip()
        country = (phys_addr.get("countryCode") or "").strip()

        # General info
        general_info = core_data.get("generalInformation", {})
        entity_structure = (general_info.get("entityStructureDesc") or "").strip()
        entity_type = (general_info.get("entityTypeDesc") or "").strip()
        org_structure = (general_info.get("organizationStructureDesc") or "").strip()
        state_of_inc = (general_info.get("stateOfIncorporationCode") or "").strip()

        # NAICS codes from the entity
        naics_list = core_data.get("naicsList", [])
        if isinstance(naics_list, list):
            naics_codes = [
                (n.get("naicsCode") or "") for n in naics_list if n.get("naicsCode")
            ]
            primary_naics = next(
                ((n.get("naicsCode") or "")
                 for n in naics_list if n.get("primaryNaics")),
                "",
            )
        else:
            naics_codes = []
            primary_naics = ""

        # Business types / socio-economic
        goods_and_services = assertions.get("goodsAndServices", {})
        sba_business_types = assertions.get("sbaBusinessTypeList", [])
        if isinstance(sba_business_types, list):
            sba_types = [
                (t.get("sbaBusinessTypeDesc") or "") for t in sba_business_types
            ]
        else:
            sba_types = []

        # Points of contact
        gov_poc = pocs.get("governmentBusinessPOC") or {}
        elec_poc = pocs.get("electronicBusinessPOC") or {}
        poc = gov_poc if gov_poc.get("lastName") else elec_poc

        poc_first = (poc.get("firstName") or "").strip()
        poc_last = (poc.get("lastName") or "").strip()
        poc_email = (poc.get("email") or "").strip()
        poc_phone = (poc.get("usPhone") or "").strip()
        poc_title = (poc.get("title") or "").strip()

        poc_name = f"{poc_first} {poc_last}".strip()

        # Build full address
        address_parts = []
        if address_line1:
            address_parts.append(address_line1)
        if address_line2:
            address_parts.append(address_line2)
        if city:
            address_parts.append(city)
        if state:
            if zipcode:
                address_parts.append(f"{state} {zipcode}")
            else:
                address_parts.append(state)
        full_address = ", ".join(address_parts)

        # Build bio
        bio_parts = [company]
        if dba_name and dba_name != company:
            bio_parts.append(f"DBA: {dba_name}")
        if city and state:
            bio_parts.append(f"{city}, {state}")
        if entity_structure:
            bio_parts.append(entity_structure)
        if cage_code:
            bio_parts.append(f"CAGE: {cage_code}")
        if primary_naics or naics_codes:
            naics_display = primary_naics or (naics_codes[0] if naics_codes else "")
            bio_parts.append(f"NAICS: {naics_display}")
        if sba_types:
            bio_parts.append(f"SBA: {', '.join(sba_types[:2])}")
        bio = " | ".join(bio_parts)

        # Use SAM.gov as website fallback
        if not website:
            website = f"https://sam.gov/entity/{uei}/coreData"

        # Determine source category
        if any(n.startswith("23") for n in naics_codes):
            source_category = "federal_contractors_construction"
        elif any(n.startswith("54") for n in naics_codes):
            source_category = "federal_contractors_professional"
        elif any(n.startswith("56") for n in naics_codes):
            source_category = "federal_contractors_admin"
        else:
            source_category = "federal_contractors"

        # Use POC name if available, otherwise company name
        contact_name = poc_name if poc_name and len(poc_name) >= 3 else company

        contact = ScrapedContact(
            name=contact_name,
            email=poc_email,
            company=company,
            website=website,
            linkedin="",
            phone=poc_phone,
            bio=bio,
            source_platform=self.SOURCE_NAME,
            source_url=f"https://sam.gov/entity/{uei}/coreData",
            source_category=source_category,
            scraped_at=datetime.now().isoformat(),
            raw_data={
                "uei": uei,
                "cage_code": cage_code,
                "dba_name": dba_name,
                "entity_structure": entity_structure,
                "entity_type": entity_type,
                "org_structure": org_structure,
                "state_of_inc": state_of_inc,
                "registration_status": registration_status,
                "entity_start_date": entity_start_date,
                "address": full_address,
                "city": city,
                "state": state,
                "zip": zipcode,
                "country": country,
                "naics_codes": naics_codes,
                "primary_naics": primary_naics,
                "sba_business_types": sba_types,
                "poc_name": poc_name,
                "poc_title": poc_title,
                "poc_email": poc_email,
                "poc_phone": poc_phone,
                "query_naics": query_naics,
            },
        )

        if not contact.is_valid():
            return None

        self.stats["contacts_found"] += 1
        self.stats["contacts_valid"] += 1
        return contact
