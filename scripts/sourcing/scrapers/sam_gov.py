"""
SAM.gov Entity Management API scraper.

Scrapes active government contractors from SAM.gov (System for Award Management)
focusing on professional services, consulting, coaching, and marketing firms.

API Documentation: https://open.gsa.gov/api/entity-api/
"""

import json
import logging
import os
from typing import Iterator

from scripts.sourcing.base import BaseScraper, ScrapedContact

logger = logging.getLogger(__name__)


class Scraper(BaseScraper):
    """SAM.gov Entity Management API scraper."""

    SOURCE_NAME = "sam_gov"
    BASE_URL = "https://api.sam.gov"
    REQUESTS_PER_MINUTE = 10  # Conservative for API rate limits

    # JV-relevant NAICS codes: professional services, consulting, coaching, marketing
    NAICS_CODES = [
        "541611",  # Administrative Management Consulting
        "541612",  # Human Resources Consulting
        "541613",  # Marketing Consulting
        "541614",  # Process/Logistics Consulting
        "541618",  # Other Management Consulting
        "541690",  # Other Scientific/Technical Consulting
        "541810",  # Advertising Agencies
        "541820",  # Public Relations Agencies
        "541830",  # Media Buying Agencies
        "541840",  # Media Representatives
        "541850",  # Outdoor Advertising
        "541860",  # Direct Mail Advertising
        "541870",  # Advertising Material Distribution
        "541910",  # Marketing Research
        "541990",  # All Other Professional/Scientific/Technical Services
        "611430",  # Professional Development Training
        "611710",  # Educational Support Services
        "624190",  # Other Individual/Family Services (coaching)
        "812990",  # All Other Personal Services
    ]

    def __init__(self, **kwargs):
        """Initialize scraper and check for API key."""
        super().__init__(**kwargs)
        self._seen_ueis: set[str] = set()
        self.api_key = os.environ.get("SAM_GOV_API_KEY", "")

        if not self.api_key:
            logger.warning(
                "SAM_GOV_API_KEY not found in environment. "
                "API requests will fail. Get a free key at https://open.gsa.gov/api/entity-api/"
            )

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """
        Generate API query URLs.

        For each NAICS code, generate up to 100 pages (10,000 results max per NAICS).
        The scrape_page method will return empty list when no more results exist.

        Yields:
            API endpoint URLs with query parameters
        """
        if not self.api_key:
            logger.error("Cannot generate URLs without SAM_GOV_API_KEY")
            return

        base_endpoint = f"{self.BASE_URL}/entity-information/v3/entities"

        for naics_code in self.NAICS_CODES:
            logger.info(f"Generating URLs for NAICS code: {naics_code}")

            # Generate up to 100 pages (100 results per page = 10,000 max)
            for page in range(100):
                params = [
                    f"api_key={self.api_key}",
                    "registrationStatus=A",  # Active only
                    f"naicsCode={naics_code}",
                    "includeSections=entityRegistration,coreData,pointsOfContact",
                    f"page={page}",
                    "size=100",  # Max results per page
                ]

                url = f"{base_endpoint}?{'&'.join(params)}"
                yield url

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """
        Parse API JSON response into contacts.

        Args:
            url: The API URL that was fetched
            html: JSON response text from the API

        Returns:
            List of ScrapedContact objects
        """
        contacts = []

        try:
            data = json.loads(html)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from {url}: {e}")
            return contacts

        total_records = data.get("totalRecords", 0)
        entity_data = data.get("entityData", [])

        if total_records == 0 or not entity_data:
            # No more results for this NAICS code
            return contacts

        logger.info(f"Processing {len(entity_data)} entities (total available: {total_records})")

        for entity in entity_data:
            try:
                contact = self._parse_entity(entity, url)
                if contact:
                    contacts.append(contact)
            except Exception as e:
                logger.error(f"Error parsing entity: {e}", exc_info=True)
                continue

        return contacts

    def _parse_entity(self, entity: dict, source_url: str) -> ScrapedContact | None:
        """
        Parse a single entity into a ScrapedContact.

        Args:
            entity: Entity data from API response
            source_url: The API URL that returned this entity

        Returns:
            ScrapedContact or None if UEI already seen or required data missing
        """
        entity_reg = entity.get("entityRegistration", {})
        core_data = entity.get("coreData", {})
        pocs = entity.get("pointsOfContact", {})

        # Get UEI (unique entity identifier) for deduplication
        uei = entity_reg.get("ueiSAM", "")
        if not uei:
            logger.debug("Skipping entity without UEI")
            return None

        if uei in self._seen_ueis:
            logger.debug(f"Skipping duplicate UEI: {uei}")
            return None

        self._seen_ueis.add(uei)

        # Get company info
        company = entity_reg.get("legalBusinessName", "").strip()
        if not company:
            logger.debug(f"Skipping entity {uei} without legal business name")
            return None

        website = entity_reg.get("entityURL", "").strip()
        registration_status = entity_reg.get("registrationStatus", "")

        # Get point of contact (prefer governmentBusinessPOC, fallback to electronicBusinessPOC)
        gov_poc = pocs.get("governmentBusinessPOC") or {}
        elec_poc = pocs.get("electronicBusinessPOC") or {}

        # Use governmentBusinessPOC if available, otherwise electronicBusinessPOC
        poc = gov_poc if gov_poc.get("email") else elec_poc

        first_name = poc.get("firstName", "").strip()
        last_name = poc.get("lastName", "").strip()
        email = poc.get("email", "").strip()
        phone = poc.get("usPhone", "").strip()

        # Build contact name
        name_parts = [first_name, last_name]
        name = " ".join(p for p in name_parts if p)

        # Get address
        physical_address = core_data.get("physicalAddress", {})
        address_line1 = physical_address.get("addressLine1", "").strip()
        city = physical_address.get("city", "").strip()
        state = physical_address.get("stateOrProvinceCode", "").strip()
        zip_code = physical_address.get("zipCode", "").strip()

        # Get entity structure info
        general_info = core_data.get("generalInformation", {})
        entity_structure = general_info.get("entityStructureDesc", "").strip()
        org_structure = general_info.get("organizationStructureDesc", "").strip()

        # Get NAICS codes
        naics_list = core_data.get("naicsList", [])
        naics_codes = [n.get("naicsCode", "") for n in naics_list if n.get("naicsCode")]

        # Build bio
        bio_parts = [company]
        if city and state:
            bio_parts.append(f"{city}, {state}")
        if naics_codes:
            bio_parts.append(f"NAICS: {', '.join(naics_codes[:3])}")  # First 3 NAICS codes
        if entity_structure:
            bio_parts.append(entity_structure)

        bio = " | ".join(bio_parts)

        # Build raw_data with additional metadata
        raw_data = {
            "ueiSAM": uei,
            "registrationStatus": registration_status,
            "entityStructureDesc": entity_structure,
            "organizationStructureDesc": org_structure,
            "naicsCodes": naics_codes,
            "fullAddress": {
                "addressLine1": address_line1,
                "city": city,
                "state": state,
                "zipCode": zip_code,
            },
            "pocType": "governmentBusinessPOC" if gov_poc.get("email") else "electronicBusinessPOC",
        }

        # Create contact — use company as name fallback
        contact = ScrapedContact(
            name=name or company,
            email=email or "",
            company=company,
            website=website or "",
            linkedin="",
            phone=phone or "",
            bio=bio,
            source_platform=self.SOURCE_NAME,
            source_url=source_url,
            source_category="government_contractors",
            raw_data=raw_data,
        )

        return contact
