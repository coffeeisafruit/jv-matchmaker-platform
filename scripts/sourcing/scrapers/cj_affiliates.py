"""
CJ Affiliate (Commission Junction) advertiser scraper.

CJ does NOT have a public advertiser directory — their advertiser lookup
requires an authenticated API call with a CJ Developer API key.

This scraper uses the CJ Advertiser Lookup REST API:
  https://developers.cj.com/docs/rest-apis/advertiser-lookup

Requirements:
  - CJ_API_KEY environment variable (Publisher's personal access token)
  - Must be an approved CJ publisher to obtain a token
  - API returns joined and non-joined advertisers

If no API key is set, the scraper will log a warning and yield nothing.

Estimated yield: 3,000-5,000 advertisers (with valid API key)
"""

from __future__ import annotations

import os
import re
from typing import Iterator, Optional
from xml.etree import ElementTree

from scripts.sourcing.base import BaseScraper, ScrapedContact


# CJ Advertiser Lookup API endpoint
CJ_API_BASE = "https://advertiser-lookup.api.cj.com/v2/advertiser-lookup"

# Number of advertisers per API page (max 100)
PAGE_SIZE = 100

# CJ advertiser categories to search — these map to CJ's internal category IDs.
# Empty string means "all categories".
CJ_KEYWORDS = [
    "",                    # All advertisers (broad sweep)
    "software",
    "saas",
    "marketing",
    "business services",
    "education",
    "health",
    "finance",
    "travel",
    "retail",
    "technology",
    "consulting",
    "coaching",
    "ecommerce",
    "real estate",
    "insurance",
    "legal",
]


class Scraper(BaseScraper):
    """CJ Affiliate advertiser scraper via REST API.

    NOTE: Requires CJ_API_KEY environment variable. Without it,
    the scraper exits cleanly with zero results.
    """

    SOURCE_NAME = "cj_affiliates"
    BASE_URL = "https://www.cj.com"
    REQUESTS_PER_MINUTE = 10  # CJ rate limit is ~25 req/min; stay conservative
    TYPICAL_ROLES = ["Affiliate/Promoter", "Product Creator"]
    TYPICAL_NICHES = ["affiliate_marketing", "ecommerce"]
    TYPICAL_OFFERINGS = ["affiliate", "products", "promotions"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.api_key = os.environ.get("CJ_API_KEY", "")
        self._seen_advertiser_ids: set[str] = set()
        if not self.api_key:
            self.logger.warning(
                "CJ_API_KEY not set. CJ Affiliate scraper requires a publisher "
                "API token from https://developers.cj.com. Scraper will yield 0 results."
            )
        # CJ API requires specific headers
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/xml",  # CJ API returns XML
        })

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used — we override run() for paginated API calls."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse CJ Advertiser Lookup XML response.

        CJ returns XML like:
        <cj-api>
          <advertisers total-matched="5000">
            <advertiser>
              <advertiser-id>1234</advertiser-id>
              <advertiser-name>Acme Corp</advertiser-name>
              <program-url>https://www.acme.com</program-url>
              <network-rank>5</network-rank>
              <seven-day-epc>0.05</seven-day-epc>
              <three-month-epc>0.04</three-month-epc>
              <relationship-status>joined</relationship-status>
              <mobile-tracking-certified>true</mobile-tracking-certified>
              <actions>
                <action>
                  <name>Sale</name>
                  <type>sale</type>
                  <commission>
                    <default>10%</default>
                  </commission>
                </action>
              </actions>
            </advertiser>
          </advertisers>
        </cj-api>
        """
        contacts = []
        try:
            root = ElementTree.fromstring(html)
        except ElementTree.ParseError as e:
            self.logger.warning("Failed to parse XML from %s: %s", url, e)
            return []

        # Find all advertiser elements
        advertisers_elem = root.find("advertisers")
        if advertisers_elem is None:
            # Try without namespace
            for elem in root.iter():
                if "advertiser" in elem.tag.lower() and elem.tag.lower().endswith("advertisers"):
                    advertisers_elem = elem
                    break

        if advertisers_elem is None:
            self.logger.debug("No advertisers element found in response from %s", url)
            return []

        for adv in advertisers_elem.findall("advertiser"):
            contact = self._parse_advertiser(adv)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_advertiser(self, adv_elem) -> Optional[ScrapedContact]:
        """Parse a single advertiser XML element into a ScrapedContact."""

        def get_text(tag: str) -> str:
            """Safely extract text from an XML element."""
            elem = adv_elem.find(tag)
            if elem is not None and elem.text:
                return elem.text.strip()
            return ""

        advertiser_id = get_text("advertiser-id")
        name = get_text("advertiser-name")
        program_url = get_text("program-url")
        network_rank = get_text("network-rank")
        seven_day_epc = get_text("seven-day-epc")
        three_month_epc = get_text("three-month-epc")
        relationship_status = get_text("relationship-status")
        mobile_certified = get_text("mobile-tracking-certified")

        if not name:
            return None

        # Deduplicate by advertiser ID
        if advertiser_id:
            if advertiser_id in self._seen_advertiser_ids:
                return None
            self._seen_advertiser_ids.add(advertiser_id)

        # Build bio with CJ-specific metadata
        bio_parts = [f"CJ Affiliate advertiser"]
        if network_rank and network_rank != "0":
            bio_parts.append(f"Network rank: {network_rank}")
        if seven_day_epc and seven_day_epc != "0.0":
            bio_parts.append(f"7-day EPC: ${seven_day_epc}")
        if three_month_epc and three_month_epc != "0.0":
            bio_parts.append(f"3-month EPC: ${three_month_epc}")
        if relationship_status:
            bio_parts.append(f"Status: {relationship_status}")
        if mobile_certified == "true":
            bio_parts.append("Mobile tracking certified")

        # Extract commission info from actions
        actions_elem = adv_elem.find("actions")
        if actions_elem is not None:
            for action in actions_elem.findall("action"):
                action_name_elem = action.find("name")
                commission_elem = action.find("commission")
                if action_name_elem is not None and commission_elem is not None:
                    action_name = (action_name_elem.text or "").strip()
                    default_elem = commission_elem.find("default")
                    if default_elem is not None and default_elem.text:
                        commission = default_elem.text.strip()
                        bio_parts.append(f"Commission ({action_name}): {commission}")

        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=name,
            company=name,
            website=program_url,
            bio=bio,
            source_category="affiliate_network",
            raw_data={
                "advertiser_id": advertiser_id,
                "network_rank": network_rank,
                "seven_day_epc": seven_day_epc,
                "three_month_epc": three_month_epc,
                "relationship_status": relationship_status,
                "platform": "cj_affiliate",
            },
        )

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Paginated API fetch of CJ advertisers.

        Iterates through keyword searches to discover advertisers.
        Each keyword search is paginated (100 results per page).
        """
        if not self.api_key:
            self.logger.error(
                "CJ_API_KEY not set — cannot access CJ Advertiser Lookup API. "
                "Get a key at https://developers.cj.com"
            )
            return

        self.logger.info("Starting %s scraper with API key", self.SOURCE_NAME)

        contacts_yielded = 0
        pages_done = 0
        start_keyword = (checkpoint or {}).get("last_keyword")
        past_checkpoint = start_keyword is None

        for keyword in CJ_KEYWORDS:
            if not past_checkpoint:
                if keyword == start_keyword:
                    past_checkpoint = True
                continue

            page_number = 1
            while True:
                # Build API URL with pagination
                params = {
                    "records-per-page": str(PAGE_SIZE),
                    "page-number": str(page_number),
                }
                if keyword:
                    params["keywords"] = keyword

                # Construct URL
                query_parts = [f"{k}={v}" for k, v in params.items()]
                url = f"{CJ_API_BASE}?{'&'.join(query_parts)}"

                # Rate limiting
                if self.rate_limiter:
                    self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

                try:
                    resp = self.session.get(url, timeout=30)
                    if resp.status_code == 401:
                        self.logger.error(
                            "CJ API returned 401 Unauthorized. Check CJ_API_KEY."
                        )
                        return
                    if resp.status_code == 403:
                        self.logger.error(
                            "CJ API returned 403 Forbidden. Your API key may lack "
                            "advertiser-lookup permissions."
                        )
                        return
                    resp.raise_for_status()
                    xml_text = resp.text
                    self.stats["pages_scraped"] += 1
                except Exception as e:
                    self.stats["errors"] += 1
                    self.logger.warning("Error fetching %s: %s", url, e)
                    break

                # Parse contacts from response
                try:
                    contacts = self.scrape_page(url, xml_text)
                except Exception as e:
                    self.stats["errors"] += 1
                    self.logger.error("Parse error on %s: %s", url, e)
                    break

                if not contacts:
                    # No more results for this keyword
                    break

                from datetime import datetime

                for contact in contacts:
                    contact.source_platform = self.SOURCE_NAME
                    contact.source_url = url
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
                if pages_done % 10 == 0:
                    self.logger.info(
                        "Progress: %d pages, %d valid contacts (keyword: '%s')",
                        pages_done, self.stats["contacts_valid"], keyword,
                    )

                if max_pages and pages_done >= max_pages:
                    self.logger.info("Reached max_pages=%d", max_pages)
                    return

                # If we got fewer results than PAGE_SIZE, no more pages
                if len(contacts) < PAGE_SIZE:
                    break

                page_number += 1

            self.logger.info(
                "Finished keyword '%s': %d valid contacts so far",
                keyword, self.stats["contacts_valid"],
            )

        self.logger.info("Scraper complete: %s", self.stats)
