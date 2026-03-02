"""
Grants.gov Grant Opportunities Scraper

Fetches grant opportunity data from the Grants.gov REST API.
Contains thousands of federal grant postings with agency names,
applicant types, funding amounts, and descriptions.

API: https://www.grants.gov/grantsws/rest/opportunities/search/
No API key required. Rate limit: be reasonable.

The API returns grant *opportunities* (postings), not recipients.
However, these contain the sponsoring agency and eligible organization
types, and the organizations that post/manage grants are high-value
JV partner targets (government agencies, large nonprofits, universities).

We also extract applicant organization information when available.
"""

import json
import time
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Keywords for JV-relevant grant categories
GRANT_SEARCH_KEYWORDS = [
    "business development",
    "small business",
    "joint venture",
    "technology transfer",
    "workforce development",
    "construction",
    "professional services",
    "consulting",
    "training",
    "economic development",
    "entrepreneurship",
    "innovation",
    "research and development",
    "infrastructure",
    "environmental services",
    "information technology",
    "cybersecurity",
    "healthcare services",
    "community development",
    "capacity building",
]

# CFDA categories relevant to JV partnerships
JV_RELEVANT_CATEGORIES = {
    "BC",  # Business and Commerce
    "CD",  # Community Development
    "ED",  # Education
    "ELT", # Employment, Labor, and Training
    "E",   # Energy
    "ENV", # Environment
    "HL",  # Health
    "HO",  # Housing
    "ISS", # Information and Statistics
    "IS",  # Income Security and Social Services
    "ST",  # Science and Technology
    "T",   # Transportation
}

# Eligible applicant types that indicate potential JV partners
RELEVANT_APPLICANT_TYPES = {
    "25",  # Others (see text)
    "21",  # Nonprofits with 501(c)(3)
    "22",  # Nonprofits without 501(c)(3)
    "23",  # Small businesses
    "12",  # For-profit organizations other than small businesses
    "08",  # Independent school districts
    "11",  # Native American tribal organizations
    "20",  # Private institutions of higher education
    "99",  # Unrestricted
}


class Scraper(BaseScraper):
    """Grants.gov grant opportunities API scraper."""

    SOURCE_NAME = "grants_gov"
    BASE_URL = "https://www.grants.gov"
    REQUESTS_PER_MINUTE = 10  # Conservative for public API

    # API endpoints (search2 replaced the old /grantsws/rest/ path in March 2025)
    SEARCH_API = "https://api.grants.gov/v1/api/search2"
    DETAIL_API = "https://api.grants.gov/v1/api/fetchOpportunity"

    PAGE_SIZE = 25  # API max per page

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        self._seen_ids: set[str] = set()
        self._seen_agencies: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run() for POST-based API."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used -- we override run() for POST-based API."""
        return []

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Search Grants.gov API for grant opportunities.

        Iterates through JV-relevant keywords, paginating through results.
        Extracts agency/organization contacts from each grant posting.
        """
        self.logger.info(
            "Starting Grants.gov scraper with %d search keywords",
            len(GRANT_SEARCH_KEYWORDS),
        )

        contacts_yielded = 0
        pages_done = 0

        # Resume from checkpoint
        start_kw_idx = (checkpoint or {}).get("keyword_idx", 0)
        start_page = (checkpoint or {}).get("page", 0)

        for kw_idx, keyword in enumerate(GRANT_SEARCH_KEYWORDS):
            if kw_idx < start_kw_idx:
                continue

            self.logger.info(
                "Searching for '%s' (%d/%d keywords)",
                keyword, kw_idx + 1, len(GRANT_SEARCH_KEYWORDS),
            )

            page = start_page if kw_idx == start_kw_idx else 0
            consecutive_empty = 0

            while True:
                if self.rate_limiter:
                    self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

                # Build search payload
                payload = {
                    "keyword": keyword,
                    "oppStatuses": "forecasted|posted",
                    "sortBy": "openDate|desc",
                    "rows": self.PAGE_SIZE,
                    "startRecordNum": page * self.PAGE_SIZE,
                }

                try:
                    resp = self.session.post(
                        self.SEARCH_API,
                        json=payload,
                        timeout=60,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    self.stats["pages_scraped"] += 1
                except Exception as e:
                    self.logger.warning(
                        "API error for '%s' page %d: %s",
                        keyword, page, e,
                    )
                    self.stats["errors"] += 1
                    break

                # Parse response (search2 nests results under "data")
                inner = data.get("data", data)
                opp_data = inner.get("oppHits", [])
                total_count = inner.get("hitCount", 0)

                if not opp_data:
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        break
                    page += 1
                    continue

                consecutive_empty = 0
                self.logger.info(
                    "Page %d: %d results (total available: %d)",
                    page, len(opp_data), total_count,
                )

                for opp in opp_data:
                    contacts = self._parse_opportunity(opp, keyword)
                    for contact in contacts:
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
                if (page + 1) * self.PAGE_SIZE >= total_count:
                    break

                page += 1

                # Safety: cap pages per keyword
                if page > 100:
                    self.logger.info("Hit page cap for keyword '%s'", keyword)
                    break

            self.logger.info(
                "Keyword '%s' done: %d valid contacts so far",
                keyword, self.stats["contacts_valid"],
            )

        self.logger.info("Grants.gov scraper complete: %s", self.stats)

    def _parse_opportunity(
        self, opp: dict, keyword: str
    ) -> list[ScrapedContact]:
        """Parse a grant opportunity into ScrapedContact objects.

        Each opportunity can yield:
        1. The grant-posting agency as a contact
        2. The opportunity itself as a discoverable entity
        """
        contacts = []

        opp_id = str(opp.get("id") or opp.get("oppId") or opp.get("number") or "")
        if not opp_id or opp_id in self._seen_ids:
            return contacts
        self._seen_ids.add(opp_id)

        # Extract opportunity fields
        title = (opp.get("title") or opp.get("oppTitle") or "").strip()
        agency_name = (opp.get("agency") or opp.get("agencyName") or "").strip()
        opp_number = (opp.get("number") or opp.get("oppNumber") or opp.get("fundingNumber") or "").strip()
        open_date = (opp.get("openDate") or opp.get("postDate") or "").strip()
        close_date = (opp.get("closeDate") or opp.get("archiveDate") or "").strip()
        award_ceiling = opp.get("awardCeiling") or opp.get("ceiling") or 0
        award_floor = opp.get("awardFloor") or opp.get("floor") or 0
        description = (opp.get("description") or opp.get("synopsis") or "").strip()
        cfda_list = opp.get("cfdaList", [])
        cfda_number = (cfda_list[0] if isinstance(cfda_list, list) and cfda_list
                       else opp.get("cfdaNumber") or opp.get("cfda") or "").strip()
        category = (opp.get("category") or opp.get("fundingCategory") or "").strip()
        opp_status = (opp.get("oppStatus") or opp.get("status") or "").strip()

        # Parse award amounts
        try:
            if isinstance(award_ceiling, str):
                award_ceiling = float(award_ceiling.replace(",", "").replace("$", ""))
            else:
                award_ceiling = float(award_ceiling or 0)
        except (ValueError, TypeError):
            award_ceiling = 0

        try:
            if isinstance(award_floor, str):
                award_floor = float(award_floor.replace(",", "").replace("$", ""))
            else:
                award_floor = float(award_floor or 0)
        except (ValueError, TypeError):
            award_floor = 0

        # Create contact for the agency posting the grant
        if agency_name and len(agency_name) >= 3:
            agency_key = agency_name.upper().strip()
            if agency_key not in self._seen_agencies:
                self._seen_agencies.add(agency_key)

                agency_bio_parts = [agency_name]
                if title:
                    agency_bio_parts.append(f"Grant: {title[:100]}")
                if award_ceiling > 0:
                    agency_bio_parts.append(f"Award ceiling: ${award_ceiling:,.0f}")
                if category:
                    agency_bio_parts.append(f"Category: {category}")
                agency_bio = " | ".join(agency_bio_parts)

                agency_contact = ScrapedContact(
                    name=agency_name,
                    email="",
                    company=agency_name,
                    website=f"https://www.grants.gov/search-results-detail/{opp_id}",
                    linkedin="",
                    phone="",
                    bio=agency_bio,
                    source_platform=self.SOURCE_NAME,
                    source_url=f"https://www.grants.gov/search-results-detail/{opp_id}",
                    source_category="grant_agencies",
                    scraped_at=datetime.now().isoformat(),
                    raw_data={
                        "opportunity_id": opp_id,
                        "opportunity_number": opp_number,
                        "opportunity_title": title,
                        "agency_name": agency_name,
                        "award_ceiling": award_ceiling,
                        "award_floor": award_floor,
                        "open_date": open_date,
                        "close_date": close_date,
                        "cfda_number": cfda_number,
                        "category": category,
                        "status": opp_status,
                        "search_keyword": keyword,
                    },
                )

                if agency_contact.is_valid():
                    self.stats["contacts_found"] += 1
                    self.stats["contacts_valid"] += 1
                    contacts.append(agency_contact)

        # Create contact for the grant opportunity itself (as a project lead)
        if title and len(title) >= 5:
            # Use opportunity as a named entity
            opp_name = title[:150]

            opp_bio_parts = [opp_name]
            if agency_name:
                opp_bio_parts.append(f"Agency: {agency_name}")
            if award_ceiling > 0:
                opp_bio_parts.append(f"Ceiling: ${award_ceiling:,.0f}")
            if award_floor > 0:
                opp_bio_parts.append(f"Floor: ${award_floor:,.0f}")
            if opp_number:
                opp_bio_parts.append(f"Opp#: {opp_number}")
            if open_date:
                opp_bio_parts.append(f"Posted: {open_date}")
            if close_date:
                opp_bio_parts.append(f"Close: {close_date}")
            if description:
                opp_bio_parts.append(description[:300])
            opp_bio = " | ".join(opp_bio_parts)

            opp_contact = ScrapedContact(
                name=opp_name,
                email="",
                company=agency_name,
                website=f"https://www.grants.gov/search-results-detail/{opp_id}",
                linkedin="",
                phone="",
                bio=opp_bio,
                source_platform=self.SOURCE_NAME,
                source_url=f"https://www.grants.gov/search-results-detail/{opp_id}",
                source_category="grant_opportunities",
                scraped_at=datetime.now().isoformat(),
                raw_data={
                    "opportunity_id": opp_id,
                    "opportunity_number": opp_number,
                    "agency_name": agency_name,
                    "award_ceiling": award_ceiling,
                    "award_floor": award_floor,
                    "open_date": open_date,
                    "close_date": close_date,
                    "cfda_number": cfda_number,
                    "category": category,
                    "status": opp_status,
                    "search_keyword": keyword,
                },
            )

            if opp_contact.is_valid():
                self.stats["contacts_found"] += 1
                self.stats["contacts_valid"] += 1
                contacts.append(opp_contact)

        return contacts
