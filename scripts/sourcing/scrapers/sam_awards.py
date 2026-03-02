"""
SAM.gov Contract Awards Scraper

Scrapes federal contract award winners from SAM.gov frontend search API.
Companies winning government contracts are prime JV candidates — many
are required to partner with small businesses.

API: https://sam.gov/api/prod/sgs/v1/search/
No API key required. Requires Accept: application/hal+json header.
"""

import json
from typing import Iterator, Optional
from datetime import datetime

from scripts.sourcing.base import BaseScraper, ScrapedContact


# JV-relevant NAICS codes (subset focused on highest-volume categories)
AWARD_NAICS = [
    "541611", "541612", "541613", "541618",  # Consulting
    "541511", "541512", "541519",  # IT Services
    "541330", "541690",  # Engineering, Scientific Consulting
    "541810", "541820", "541910",  # Marketing, PR, Research
    "611430",  # Professional Development
    "541990",  # Other Professional Services
    "561310", "561320",  # Staffing
]


class Scraper(BaseScraper):
    SOURCE_NAME = "sam_awards"
    BASE_URL = "https://sam.gov"
    REQUESTS_PER_MINUTE = 8  # Be conservative

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self.session.headers["Accept"] = "application/hal+json"
        self._seen_ueis: set[str] = set()
        self._seen_names: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used — we override run() for paginated API search."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used — we override run() for paginated API search."""
        return []

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Search SAM.gov for contract awards by NAICS code."""
        self.logger.info("Starting %s scraper — %d NAICS codes",
                         self.SOURCE_NAME, len(AWARD_NAICS))

        start_naics_idx = (checkpoint or {}).get("naics_idx", 0)
        contacts_yielded = 0
        pages_done = 0

        for naics_idx, naics_code in enumerate(AWARD_NAICS):
            if naics_idx < start_naics_idx:
                continue

            self.logger.info("Searching awards for NAICS %s (%d/%d)",
                             naics_code, naics_idx + 1, len(AWARD_NAICS))

            page = 0
            while True:
                if self.rate_limiter:
                    self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

                url = (
                    f"{self.BASE_URL}/api/prod/sgs/v1/search/"
                    f"?index=opp"
                    f"&q={naics_code}"
                    f"&page={page}"
                    f"&size=25"
                    f"&sort=-modifiedDate"
                    f"&mode=search"
                    f"&notice_type=a"
                )

                try:
                    resp = self.session.get(url, timeout=30)
                    resp.raise_for_status()
                    data = resp.json()
                    self.stats["pages_scraped"] += 1
                except Exception as e:
                    self.stats["errors"] += 1
                    self.logger.warning("SAM.gov error for NAICS %s page %d: %s",
                                        naics_code, page, e)
                    break

                # Extract embedded results
                embedded = data.get("_embedded", {})
                results = embedded.get("results", [])

                if not results:
                    break

                for item in results:
                    contacts = self._parse_opportunity(item, naics_code)
                    for contact in contacts:
                        contacts_yielded += 1
                        yield contact

                        if max_contacts and contacts_yielded >= max_contacts:
                            self.logger.info("Reached max_contacts=%d", max_contacts)
                            return

                pages_done += 1
                page += 1

                # Check pagination
                total_pages = data.get("page", {}).get("totalPages", 0)
                if page >= total_pages:
                    break

                # Cap at 40 pages per NAICS (1000 results)
                if page >= 40:
                    break

                if max_pages and pages_done >= max_pages:
                    self.logger.info("Reached max_pages=%d", max_pages)
                    return

            self.logger.info("NAICS %s done: %d valid contacts so far",
                             naics_code, self.stats["contacts_valid"])

        self.logger.info("Scraper complete: %s", self.stats)

    def _parse_opportunity(self, item: dict, naics_code: str) -> list[ScrapedContact]:
        """Parse a SAM.gov opportunity for awardee info.

        Captures full JV-relevant context: awarding agency chain,
        contract type, dates, descriptions, and awardee details.
        """
        contacts = []

        # Extract award info
        award = item.get("award", {}) or {}
        awardee = award.get("awardee", {}) or {}

        awardee_name = (awardee.get("name") or "").strip()
        awardee_uei = (awardee.get("ueiSAM") or "").strip()

        # Contract metadata
        title = (item.get("title") or "").strip()
        sol_number = (item.get("solicitationNumber") or "").strip()
        publish_date = (item.get("publishDate") or "").strip()
        response_date = (item.get("responseDate") or "").strip()
        modified_date = (item.get("modifiedDate") or "").strip()
        is_active = item.get("isActive", False)
        modifications_count = (item.get("modifications", {}) or {}).get("count", 0)

        # Contract type (Award Notice, Solicitation, etc.)
        type_info = item.get("type", {}) or {}
        notice_type_code = (type_info.get("code") or "").strip()
        notice_type_value = (type_info.get("value") or "").strip()

        # Description (often contains scope of work details)
        descriptions = item.get("descriptions", []) or []
        description_text = ""
        if descriptions:
            description_text = (descriptions[0].get("content") or "").strip()
            # Strip HTML tags for cleaner storage
            import re
            description_text = re.sub(r"<[^>]+>", " ", description_text)
            description_text = re.sub(r"\s+", " ", description_text).strip()[:500]

        # Organization hierarchy (department → agency → office)
        org_hierarchy = item.get("organizationHierarchy", []) or []
        awarding_department = ""
        awarding_agency = ""
        awarding_office = ""
        awarding_office_address = {}
        for org in org_hierarchy:
            level = org.get("level", 0)
            org_name = (org.get("name") or "").strip()
            if level == 1:
                awarding_department = org_name
            elif level == 2:
                awarding_agency = org_name
            elif level == 3:
                awarding_office = org_name
                awarding_office_address = org.get("address", {}) or {}

        if awardee_name:
            # Skip if already seen
            if awardee_uei:
                if awardee_uei in self._seen_ueis:
                    return contacts
                self._seen_ueis.add(awardee_uei)
            else:
                name_key = awardee_name.upper()
                if name_key in self._seen_names:
                    return contacts
                self._seen_names.add(name_key)

            # Build rich bio with JV context
            bio_parts = [awardee_name]
            if title:
                bio_parts.append(f"Won: {title[:120]}")
            if awarding_agency:
                bio_parts.append(f"Agency: {awarding_agency}")
            elif awarding_department:
                bio_parts.append(f"Dept: {awarding_department}")
            if notice_type_value:
                bio_parts.append(f"Type: {notice_type_value}")
            bio_parts.append(f"NAICS: {naics_code}")
            if publish_date:
                bio_parts.append(f"Date: {publish_date[:10]}")
            bio = " | ".join(bio_parts)

            # Use SAM.gov entity page as website
            if awardee_uei:
                website = f"https://sam.gov/entity/{awardee_uei}/coreData"
            else:
                website = "https://sam.gov"

            contact = ScrapedContact(
                name=awardee_name,
                email="",
                company=awardee_name,
                website=website,
                linkedin="",
                phone="",
                bio=bio,
                source_platform=self.SOURCE_NAME,
                source_url=f"https://sam.gov/opp/{sol_number}" if sol_number else "https://sam.gov",
                source_category="contract_awardees",
                scraped_at=datetime.now().isoformat(),
                raw_data={
                    "uei": awardee_uei,
                    "title": title,
                    "solicitation_number": sol_number,
                    "naics_code": naics_code,
                    "notice_type": notice_type_value,
                    "notice_type_code": notice_type_code,
                    "publish_date": publish_date,
                    "response_date": response_date,
                    "modified_date": modified_date,
                    "is_active": is_active,
                    "modifications_count": modifications_count,
                    "description": description_text,
                    "awarding_department": awarding_department,
                    "awarding_agency": awarding_agency,
                    "awarding_office": awarding_office,
                    "awarding_office_city": (awarding_office_address.get("city") or ""),
                    "awarding_office_state": (awarding_office_address.get("state") or ""),
                },
            )

            if contact.is_valid():
                self.stats["contacts_found"] += 1
                self.stats["contacts_valid"] += 1
                contacts.append(contact)

        return contacts
