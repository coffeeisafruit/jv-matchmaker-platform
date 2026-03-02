"""
State Secretary of State Business Registration Scraper

Scrapes business registration data from state-level Secretary of State databases.
Targets the most accessible states with public search APIs or HTML-based search:

- California (bizfileonline.sos.ca.gov) — POST-based search API
- New York (apps.dos.ny.gov/publicInquiry/) — HTML form-based search
- Florida (search.sunbiz.org) — HTML-based search
- Texas (mycpa.cpa.state.tx.us) — Taxpayer search

Extracts: business name, filing date, status, agent name, address, entity type.

Note: Delaware (icis.corp.delaware.gov) requires CAPTCHA and is not feasible
to scrape programmatically. We skip it and focus on states with open data.

Overrides run() because each state uses a different data access pattern
and we cycle through them sequentially.
"""

import json
import re
import time
import urllib.parse
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Common business name prefixes to search across all states
# Using common industry terms to discover businesses
SEARCH_TERMS = [
    # Professional services
    "consulting", "advisors", "advisory", "partners", "associates",
    "solutions", "services", "management", "enterprises", "holdings",
    "capital", "ventures", "investments", "development", "technologies",
    "engineering", "construction", "builders", "contractors", "realty",
    "properties", "logistics", "marketing", "media", "digital",
    "healthcare", "medical", "financial", "insurance", "legal",
    "design", "creative", "analytics", "data", "security",
    "energy", "environmental", "staffing", "training", "education",
    # Common LLC/Corp suffixes combined with industry terms
    "group", "global", "international", "national", "american",
]

# Entity types to include (filter out sole proprietors and trusts)
VALID_ENTITY_TYPES = {
    "LLC", "CORPORATION", "CORP", "INC", "LP", "LLP", "PLLC",
    "LIMITED LIABILITY COMPANY", "LIMITED PARTNERSHIP",
    "LIMITED LIABILITY PARTNERSHIP", "PROFESSIONAL CORPORATION",
    "DOMESTIC", "FOREIGN", "S CORPORATION", "C CORPORATION",
}


class Scraper(BaseScraper):
    """Multi-state Secretary of State business registration scraper."""

    SOURCE_NAME = "state_business_registrations"
    BASE_URL = "https://bizfileonline.sos.ca.gov"
    REQUESTS_PER_MINUTE = 6  # Conservative to avoid rate limiting

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_names: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run() for multi-state scraping."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used -- we override run() for multi-state scraping."""
        return []

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Run business registration scrapers across multiple states.

        Cycles through each state's search system, querying with
        common business name terms to discover registered entities.
        """
        self.logger.info("Starting multi-state business registration scraper")

        contacts_yielded = 0
        pages_done = 0

        # Resume from checkpoint
        start_state_idx = (checkpoint or {}).get("state_idx", 0)
        start_term_idx = (checkpoint or {}).get("term_idx", 0)

        state_scrapers = [
            ("California", self._scrape_california),
            ("New York", self._scrape_new_york),
            ("Florida", self._scrape_florida),
            ("Texas", self._scrape_texas),
        ]

        for state_idx, (state_name, scrape_fn) in enumerate(state_scrapers):
            if state_idx < start_state_idx:
                continue

            self.logger.info("Processing state: %s", state_name)

            for term_idx, term in enumerate(SEARCH_TERMS):
                if state_idx == start_state_idx and term_idx < start_term_idx:
                    continue

                self.logger.info(
                    "Searching %s for '%s' (%d/%d terms)",
                    state_name, term, term_idx + 1, len(SEARCH_TERMS),
                )

                try:
                    results = scrape_fn(term)
                except Exception as e:
                    self.logger.warning(
                        "Error searching %s for '%s': %s",
                        state_name, term, e,
                    )
                    self.stats["errors"] += 1
                    continue

                pages_done += 1

                for contact in results:
                    contact.source_platform = self.SOURCE_NAME
                    contact.scraped_at = datetime.now().isoformat()
                    contact.email = contact.clean_email()

                    if contact.is_valid():
                        # Deduplicate across states
                        name_key = contact.name.upper().strip()
                        if name_key in self._seen_names:
                            continue
                        self._seen_names.add(name_key)

                        self.stats["contacts_valid"] += 1
                        contacts_yielded += 1
                        yield contact

                        if max_contacts and contacts_yielded >= max_contacts:
                            self.logger.info("Reached max_contacts=%d", max_contacts)
                            return

                    self.stats["contacts_found"] += 1

                if pages_done % 10 == 0:
                    self.logger.info(
                        "Progress: %d searches done, %d valid contacts",
                        pages_done, self.stats["contacts_valid"],
                    )

                if max_pages and pages_done >= max_pages:
                    self.logger.info("Reached max_pages=%d", max_pages)
                    return

            self.logger.info(
                "Completed %s: %d valid contacts so far",
                state_name, self.stats["contacts_valid"],
            )

        self.logger.info("State business registration scraper complete: %s", self.stats)

    # ------------------------------------------------------------------
    # California (bizfileonline.sos.ca.gov)
    # ------------------------------------------------------------------

    def _scrape_california(self, search_term: str) -> list[ScrapedContact]:
        """Search California Secretary of State business database.

        Uses the public search API at bizfileonline.sos.ca.gov.
        """
        contacts = []

        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        search_url = "https://bizfileonline.sos.ca.gov/api/Records/businesssearch"
        payload = {
            "SearchType": "CORP",
            "SearchCriteria": search_term,
            "SearchSubType": "Keyword",
        }

        try:
            resp = self.session.post(
                search_url,
                json=payload,
                timeout=30,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            self.stats["pages_scraped"] += 1
        except Exception as e:
            self.logger.warning("California search failed for '%s': %s", search_term, e)
            self.stats["errors"] += 1
            return contacts

        # Response contains a list of business records
        rows = data if isinstance(data, list) else data.get("rows", [])

        for record in rows[:100]:  # Cap at 100 per search term
            contact = self._parse_california_record(record)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_california_record(self, record: dict) -> Optional[ScrapedContact]:
        """Parse a California business search result."""
        name = (record.get("TITLE") or record.get("EntityName") or "").strip()
        if not name or len(name) < 3:
            return None

        entity_number = (record.get("EntityNumber") or record.get("ENTITYID") or "").strip()
        status = (record.get("Status") or record.get("STATUS") or "").strip()
        filing_date = (record.get("FormationDate") or record.get("FILINGDATE") or "").strip()
        entity_type = (record.get("EntityType") or record.get("ENTITYTYPE") or "").strip()
        agent_name = (record.get("AgentName") or record.get("AGENT") or "").strip()
        address = (record.get("Address") or "").strip()
        city = (record.get("City") or "").strip()
        state = "CA"

        # Build bio
        bio_parts = [name]
        if city:
            bio_parts.append(f"{city}, CA")
        if entity_type:
            bio_parts.append(entity_type)
        if status:
            bio_parts.append(f"Status: {status}")
        if filing_date:
            bio_parts.append(f"Filed: {filing_date}")
        if agent_name:
            bio_parts.append(f"Agent: {agent_name}")
        bio = " | ".join(bio_parts)

        # Use SOS detail page as website
        if entity_number:
            website = f"https://bizfileonline.sos.ca.gov/search/business?id={entity_number}"
        else:
            website = "https://bizfileonline.sos.ca.gov"

        return ScrapedContact(
            name=name,
            email="",
            company=name,
            website=website,
            linkedin="",
            phone="",
            bio=bio,
            source_url="https://bizfileonline.sos.ca.gov/search/business",
            source_category="state_registrations_ca",
            raw_data={
                "entity_number": entity_number,
                "entity_type": entity_type,
                "status": status,
                "filing_date": filing_date,
                "agent_name": agent_name,
                "address": address,
                "city": city,
                "state": state,
            },
        )

    # ------------------------------------------------------------------
    # New York (apps.dos.ny.gov/publicInquiry/)
    # ------------------------------------------------------------------

    def _scrape_new_york(self, search_term: str) -> list[ScrapedContact]:
        """Search New York Department of State Corporation and Business Entity Database.

        Uses the public search interface at apps.dos.ny.gov.
        """
        contacts = []

        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        # NY DOS uses a form-based search
        search_url = "https://apps.dos.ny.gov/publicInquiry/api/entity/search"
        params = {
            "name": search_term,
            "type": "",  # All types
            "jurisdiction": "",
            "formationDateFrom": "",
            "formationDateTo": "",
            "status": "Active",
        }

        try:
            resp = self.session.get(
                search_url,
                params=params,
                timeout=30,
                headers={"Accept": "application/json"},
            )
            resp.raise_for_status()

            # Try JSON first
            try:
                data = resp.json()
                records = data if isinstance(data, list) else data.get("results", [])
            except (json.JSONDecodeError, ValueError):
                # If JSON fails, try HTML parsing
                records = self._parse_ny_html(resp.text, search_term)

            self.stats["pages_scraped"] += 1
        except Exception as e:
            self.logger.warning("New York search failed for '%s': %s", search_term, e)
            self.stats["errors"] += 1
            return contacts

        for record in records[:100]:
            contact = self._parse_new_york_record(record)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_ny_html(self, html: str, search_term: str) -> list[dict]:
        """Fallback: parse NY DOS HTML search results into record dicts."""
        records = []
        try:
            soup = self.parse_html(html)
            # Look for result table rows
            rows = soup.select("table.table tbody tr") or soup.select("table tbody tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 3:
                    name = cells[0].get_text(strip=True)
                    entity_type = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                    status = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                    filing_date = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                    county = cells[4].get_text(strip=True) if len(cells) > 4 else ""

                    # Extract entity ID from link if available
                    link = cells[0].find("a")
                    entity_id = ""
                    if link and link.get("href"):
                        href = link["href"]
                        match = re.search(r"id[=/](\d+)", href)
                        if match:
                            entity_id = match.group(1)

                    records.append({
                        "EntityName": name,
                        "EntityType": entity_type,
                        "Status": status,
                        "FilingDate": filing_date,
                        "County": county,
                        "EntityId": entity_id,
                    })
        except Exception as e:
            self.logger.warning("Failed to parse NY HTML: %s", e)

        return records

    def _parse_new_york_record(self, record: dict) -> Optional[ScrapedContact]:
        """Parse a New York business search result."""
        name = (record.get("EntityName") or record.get("name") or "").strip()
        if not name or len(name) < 3:
            return None

        entity_id = (record.get("EntityId") or record.get("id") or "").strip()
        entity_type = (record.get("EntityType") or record.get("type") or "").strip()
        status = (record.get("Status") or record.get("status") or "").strip()
        filing_date = (record.get("FilingDate") or record.get("formationDate") or "").strip()
        county = (record.get("County") or record.get("county") or "").strip()
        agent_name = (record.get("AgentName") or record.get("processAgent") or "").strip()

        bio_parts = [name]
        if county:
            bio_parts.append(f"{county}, NY")
        if entity_type:
            bio_parts.append(entity_type)
        if status:
            bio_parts.append(f"Status: {status}")
        if filing_date:
            bio_parts.append(f"Filed: {filing_date}")
        bio = " | ".join(bio_parts)

        if entity_id:
            website = f"https://apps.dos.ny.gov/publicInquiry/entityDisplay?id={entity_id}"
        else:
            website = "https://apps.dos.ny.gov/publicInquiry/"

        return ScrapedContact(
            name=name,
            email="",
            company=name,
            website=website,
            linkedin="",
            phone="",
            bio=bio,
            source_url="https://apps.dos.ny.gov/publicInquiry/",
            source_category="state_registrations_ny",
            raw_data={
                "entity_id": entity_id,
                "entity_type": entity_type,
                "status": status,
                "filing_date": filing_date,
                "county": county,
                "agent_name": agent_name,
                "state": "NY",
            },
        )

    # ------------------------------------------------------------------
    # Florida (search.sunbiz.org)
    # ------------------------------------------------------------------

    def _scrape_florida(self, search_term: str) -> list[ScrapedContact]:
        """Search Florida Division of Corporations (Sunbiz).

        Uses search.sunbiz.org HTML interface.
        """
        contacts = []

        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        encoded_term = urllib.parse.quote(search_term)
        search_url = (
            f"https://search.sunbiz.org/Inquiry/CorporationSearch/"
            f"SearchByName?searchNameOrder={encoded_term}"
            f"&searchTerm={encoded_term}&listNameOrder="
        )

        html = self.fetch_page(search_url, timeout=30)
        if not html:
            return contacts

        try:
            soup = self.parse_html(html)
            # Sunbiz returns results in a table
            result_table = soup.find("table", class_="searchResultTable") or soup.find("table")
            if not result_table:
                return contacts

            rows = result_table.find_all("tr")[1:]  # Skip header row
            for row in rows[:100]:
                cells = row.find_all("td")
                if len(cells) < 3:
                    continue

                # Extract entity name and link
                name_cell = cells[0]
                name_link = name_cell.find("a")
                name = name_cell.get_text(strip=True)
                detail_url = ""
                if name_link and name_link.get("href"):
                    detail_url = name_link["href"]
                    if not detail_url.startswith("http"):
                        detail_url = f"https://search.sunbiz.org{detail_url}"

                entity_type = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                status = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                filing_date = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                state_country = cells[4].get_text(strip=True) if len(cells) > 4 else ""

                if not name or len(name) < 3:
                    continue

                bio_parts = [name, "FL"]
                if entity_type:
                    bio_parts.append(entity_type)
                if status:
                    bio_parts.append(f"Status: {status}")
                if filing_date:
                    bio_parts.append(f"Filed: {filing_date}")
                bio = " | ".join(bio_parts)

                website = detail_url or "https://search.sunbiz.org"

                contact = ScrapedContact(
                    name=name,
                    email="",
                    company=name,
                    website=website,
                    linkedin="",
                    phone="",
                    bio=bio,
                    source_url="https://search.sunbiz.org",
                    source_category="state_registrations_fl",
                    raw_data={
                        "entity_type": entity_type,
                        "status": status,
                        "filing_date": filing_date,
                        "state_country": state_country,
                        "detail_url": detail_url,
                        "state": "FL",
                    },
                )

                if contact.is_valid():
                    contacts.append(contact)

        except Exception as e:
            self.logger.warning("Florida parsing failed for '%s': %s", search_term, e)
            self.stats["errors"] += 1

        return contacts

    # ------------------------------------------------------------------
    # Texas (mycpa.cpa.state.tx.us or SOS direct.sos.state.tx.us)
    # ------------------------------------------------------------------

    def _scrape_texas(self, search_term: str) -> list[ScrapedContact]:
        """Search Texas Secretary of State SOSDirect database.

        Uses the public search at direct.sos.state.tx.us.
        """
        contacts = []

        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        # Texas SOS ALVARA search (public, no login required)
        encoded_term = urllib.parse.quote(search_term)
        search_url = (
            f"https://mycpa.cpa.state.tx.us/coa/coaSearchBtn"
            f"?search_nm={encoded_term}&search_type=contains"
            f"&submit=Search"
        )

        html = self.fetch_page(search_url, timeout=30)
        if not html:
            return contacts

        try:
            soup = self.parse_html(html)
            # Look for result tables
            tables = soup.find_all("table")

            for table in tables:
                rows = table.find_all("tr")
                for row in rows[1:100]:  # Skip header, cap at 100
                    cells = row.find_all("td")
                    if len(cells) < 2:
                        continue

                    name = cells[0].get_text(strip=True)
                    if not name or len(name) < 3:
                        continue

                    # Extract detail link
                    link = cells[0].find("a")
                    detail_url = ""
                    if link and link.get("href"):
                        href = link["href"]
                        if not href.startswith("http"):
                            detail_url = f"https://mycpa.cpa.state.tx.us{href}"
                        else:
                            detail_url = href

                    taxpayer_id = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                    city = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                    status = cells[3].get_text(strip=True) if len(cells) > 3 else ""

                    bio_parts = [name]
                    if city:
                        bio_parts.append(f"{city}, TX")
                    if status:
                        bio_parts.append(f"Status: {status}")
                    bio = " | ".join(bio_parts)

                    website = detail_url or "https://mycpa.cpa.state.tx.us"

                    contact = ScrapedContact(
                        name=name,
                        email="",
                        company=name,
                        website=website,
                        linkedin="",
                        phone="",
                        bio=bio,
                        source_url="https://mycpa.cpa.state.tx.us",
                        source_category="state_registrations_tx",
                        raw_data={
                            "taxpayer_id": taxpayer_id,
                            "city": city,
                            "status": status,
                            "detail_url": detail_url,
                            "state": "TX",
                        },
                    )

                    if contact.is_valid():
                        contacts.append(contact)

        except Exception as e:
            self.logger.warning("Texas parsing failed for '%s': %s", search_term, e)
            self.stats["errors"] += 1

        return contacts
