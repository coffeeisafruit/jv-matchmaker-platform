"""
FDA Medical Device Registration Scraper

Scrapes registered medical device establishments from the openFDA API.
These are real businesses — manufacturers, contract manufacturers, importers,
distributors, repackagers — all potential JV partners for healthcare,
biotech, engineering, and professional services.

API: https://api.fda.gov/device/registrationlisting.json
No API key required (but limited to 240 requests/minute without one).
With API key: 120K requests/day.

Total US registrations: ~157K active establishments.
Each record includes: company name, full address, owner/operator name,
official correspondent (contact person name + phone), product codes,
and establishment type.

The API caps `skip` at 25000, so we paginate by state to get all results
(no single state exceeds 25K registrations; CA is largest at ~24K).
"""

from typing import Iterator, Optional
from datetime import datetime

from scripts.sourcing.base import BaseScraper, ScrapedContact


# All US state codes — ordered largest first for fastest initial output
US_STATES = [
    "CA", "FL", "TX", "NY", "NJ", "PA", "IL", "MA", "OH", "MN",
    "MI", "GA", "NC", "WI", "CT", "IN", "MO", "WA", "TN", "CO",
    "VA", "AZ", "MD", "OR", "SC", "UT", "AL", "KY", "IA", "OK",
    "LA", "KS", "NE", "NV", "NH", "NM", "AR", "MS", "RI", "ID",
    "ME", "MT", "HI", "WV", "SD", "ND", "DE", "VT", "WY", "AK",
    "DC", "PR",
]

# Establishment types that indicate actual businesses (not just spec holders)
BUSINESS_ESTABLISHMENT_TYPES = {
    "Manufacture Medical Device",
    "Manufacture Medical Device for Another Party (Contract Manufacturer)",
    "Contract Sterilizer",
    "Repackager",
    "Relabeler",
    "Specification Developer",
    "Remanufacturer",
    "Reprocessor of Single Use Devices",
    "Manufacture Medicated Device",
    "Contract Manufacturer",
}


class Scraper(BaseScraper):
    SOURCE_NAME = "fda_devices"
    BASE_URL = "https://api.fda.gov"
    REQUESTS_PER_MINUTE = 30  # openFDA is generous; 240/min without key

    PAGE_SIZE = 100  # Max allowed by openFDA
    MAX_SKIP = 25000  # openFDA hard limit on skip parameter

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self.session.headers["Accept"] = "application/json"
        self._seen_registration_numbers: set[str] = set()
        self._seen_owner_operators: set[str] = set()

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
        """
        Paginate through FDA device registrations by US state.
        The openFDA API caps skip at 25000, so we iterate state-by-state
        (no state exceeds 25K registrations).

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

            skip = 0
            state_contacts = 0

            while skip < self.MAX_SKIP:
                if self.rate_limiter:
                    self.rate_limiter.wait(
                        self.SOURCE_NAME, self.REQUESTS_PER_MINUTE
                    )

                search_query = (
                    f"registration.iso_country_code:US"
                    f"+AND+registration.status_code:1"
                    f"+AND+registration.state_code:{state_code}"
                )
                url = (
                    f"{self.BASE_URL}/device/registrationlisting.json"
                    f"?search={search_query}"
                    f"&skip={skip}&limit={self.PAGE_SIZE}"
                )

                try:
                    resp = self.session.get(url, timeout=30)

                    # Handle 404 (no results for this state/skip)
                    if resp.status_code == 404:
                        break

                    resp.raise_for_status()
                    data = resp.json()
                    self.stats["pages_scraped"] += 1
                except Exception as e:
                    self.stats["errors"] += 1
                    self.logger.warning(
                        "API error for %s skip=%d: %s",
                        state_code, skip, e,
                    )
                    # On error, try to continue with next page
                    skip += self.PAGE_SIZE
                    if self.stats["errors"] > 20:
                        self.logger.error("Too many errors, stopping")
                        return
                    continue

                # Check for API error response
                if "error" in data:
                    self.logger.warning(
                        "API returned error for %s: %s",
                        state_code, data["error"],
                    )
                    break

                results = data.get("results", [])
                if not results:
                    break

                for item in results:
                    contact = self._parse_registration(item, state_code)
                    if contact:
                        contacts_yielded += 1
                        state_contacts += 1
                        yield contact

                        if max_contacts and contacts_yielded >= max_contacts:
                            self.logger.info(
                                "Reached max_contacts=%d", max_contacts
                            )
                            return

                pages_done += 1
                skip += self.PAGE_SIZE

                if pages_done % 20 == 0:
                    self.logger.info(
                        "Progress: %d pages, %d valid contacts",
                        pages_done, self.stats["contacts_valid"],
                    )

                if max_pages and pages_done >= max_pages:
                    self.logger.info("Reached max_pages=%d", max_pages)
                    return

                # Check if we've hit the total
                total = data.get("meta", {}).get("results", {}).get(
                    "total", 0
                )
                if skip >= total:
                    break

            self.logger.info(
                "State %s done: %d contacts (%d total so far)",
                state_code, state_contacts, self.stats["contacts_valid"],
            )

        self.logger.info("Scraper complete: %s", self.stats)

    def _parse_registration(
        self, item: dict, state_code: str
    ) -> ScrapedContact | None:
        """Parse a single FDA device registration into ScrapedContact."""
        registration = item.get("registration", {})
        if not registration:
            return None

        # Company name from registration
        reg_name = (registration.get("name") or "").strip()
        if not reg_name or len(reg_name) < 3:
            return None

        # Skip non-active
        status = (registration.get("status_code") or "").strip()
        if status != "1":
            return None

        # Registration number for dedup
        reg_number = (
            registration.get("registration_number") or ""
        ).strip()
        if reg_number:
            if reg_number in self._seen_registration_numbers:
                return None
            self._seen_registration_numbers.add(reg_number)

        # Owner/operator info (often the actual business entity)
        owner = registration.get("owner_operator", {}) or {}
        firm_name = (owner.get("firm_name") or "").strip()
        owner_number = (
            owner.get("owner_operator_number") or ""
        ).strip()

        # Deduplicate by owner_operator_number too
        if owner_number:
            if owner_number in self._seen_owner_operators:
                return None
            self._seen_owner_operators.add(owner_number)

        # Use firm_name as the primary name if available (it's cleaner)
        company_name = firm_name or reg_name

        # Skip names that look like government/military
        name_upper = company_name.upper()
        gov_indicators = [
            "DEPARTMENT OF", "US ARMY", "US NAVY", "US AIR FORCE",
            "VETERANS AFFAIRS", "NATIONAL INSTITUTES", "WALTER REED",
            "FEDERAL", "GOVERNMENT",
        ]
        if any(ind in name_upper for ind in gov_indicators):
            return None

        # Address from registration
        address_1 = (registration.get("address_line_1") or "").strip()
        address_2 = (registration.get("address_line_2") or "").strip()
        city = (registration.get("city") or "").strip()
        state = (registration.get("state_code") or state_code).strip()
        zip_code = (registration.get("zip_code") or "").strip()

        # Official correspondent (contact person)
        correspondent = (
            owner.get("official_correspondent", {}) or {}
        )
        contact_first = (
            correspondent.get("first_name") or ""
        ).strip()
        contact_last = (correspondent.get("last_name") or "").strip()
        contact_name = f"{contact_first} {contact_last}".strip()
        phone_raw = (
            correspondent.get("phone_number") or ""
        ).strip()

        # Clean phone number (format is sometimes "x-724-7469390-7004"
        # or "1-818-3658740-x")
        phone = self._clean_phone(phone_raw)

        # Owner contact address (may differ from registration address)
        owner_address = owner.get("contact_address", {}) or {}
        owner_city = (owner_address.get("city") or "").strip()
        owner_state = (owner_address.get("state_code") or "").strip()

        # Establishment types
        est_types = item.get("establishment_type", []) or []
        est_type_str = ", ".join(est_types[:2]) if est_types else ""

        # Products (get the first few product names for bio)
        products = item.get("products", []) or []
        product_names = []
        for p in products[:3]:
            openfda = p.get("openfda", {}) or {}
            device_name = (
                openfda.get("device_name") or ""
            ).strip()
            if device_name and device_name not in product_names:
                product_names.append(device_name)

        # Build bio
        bio_parts = [company_name]
        if city and state:
            bio_parts.append(f"{city}, {state}")
        if est_type_str:
            bio_parts.append(est_type_str)
        if product_names:
            bio_parts.append(f"Products: {'; '.join(product_names[:2])}")
        if contact_name:
            bio_parts.append(f"Contact: {contact_name}")
        bio = " | ".join(bio_parts)

        # Build website from FDA profile
        fei_number = (registration.get("fei_number") or "").strip()
        website = ""
        if reg_number:
            website = (
                f"https://www.accessdata.fda.gov/scripts/cdrh/"
                f"cfdocs/cfRL/rl.cfm?rid={reg_number}"
            )

        contact = ScrapedContact(
            name=company_name,
            email="",
            company=company_name,
            website=website,
            linkedin="",
            phone=phone,
            bio=bio,
            source_platform=self.SOURCE_NAME,
            source_url=(
                f"{self.BASE_URL}/device/registrationlisting.json"
            ),
            source_category="medical_device_manufacturers",
            scraped_at=datetime.now().isoformat(),
            raw_data={
                "registration_number": reg_number,
                "fei_number": fei_number,
                "owner_operator_number": owner_number,
                "registration_name": reg_name,
                "firm_name": firm_name,
                "address_1": address_1,
                "address_2": address_2,
                "city": city,
                "state": state,
                "zip_code": zip_code,
                "owner_city": owner_city,
                "owner_state": owner_state,
                "contact_first_name": contact_first,
                "contact_last_name": contact_last,
                "phone_raw": phone_raw,
                "establishment_types": est_types,
                "product_count": len(products),
                "product_names": product_names[:5],
            },
        )

        if not contact.is_valid():
            return None

        self.stats["contacts_found"] += 1
        self.stats["contacts_valid"] += 1
        return contact

    @staticmethod
    def _clean_phone(raw: str) -> str:
        """
        Clean FDA phone numbers.
        Formats seen: "x-724-7469390-7004", "1-818-3658740-x",
        "1-800-5551234", "49-7142-705-192" (foreign).
        """
        if not raw:
            return ""

        # Remove leading/trailing 'x' parts (extensions)
        parts = raw.split("-")
        # Filter out 'x' and empty parts
        digits_parts = [
            p for p in parts
            if p.strip().lower() != "x" and p.strip()
        ]

        if not digits_parts:
            return ""

        # Rejoin
        phone = "-".join(digits_parts)

        # Check if it looks like a US phone (starts with 1 or has 10 digits)
        digits_only = "".join(c for c in phone if c.isdigit())

        if len(digits_only) < 7:
            return ""
        if len(digits_only) > 15:
            return ""

        # Format as standard phone
        if len(digits_only) == 10:
            return f"({digits_only[:3]}) {digits_only[3:6]}-{digits_only[6:]}"
        elif len(digits_only) == 11 and digits_only[0] == "1":
            d = digits_only[1:]
            return f"({d[:3]}) {d[3:6]}-{d[6:]}"
        else:
            # Return as-is with dashes
            return phone
