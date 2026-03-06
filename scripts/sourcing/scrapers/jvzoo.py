"""
JVZoo marketplace scraper via embedded calendar data.

JVZoo's marketplace at jvzoo.com/marketplace is a server-rendered page with
a FullCalendar component. Product launch events are embedded as JSON in the
page source, containing vendor names, product IDs, commission rates, and
affiliate info URLs.

The calendar typically contains ~85 active product launches. Each affiliate
info page (partially accessible without login) provides additional context.

JVZoo is THE premier JV/affiliate marketplace -- people here are literally
doing JV launches. Even modest yields are extremely high-value for ISMC scoring.

Estimated yield: 60-100 unique vendors per scrape (refreshes as new launches
are added). Run weekly to accumulate over time.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import Iterator, Optional
from urllib.parse import urljoin

from scripts.sourcing.base import BaseScraper, ScrapedContact


class Scraper(BaseScraper):
    """JVZoo marketplace scraper.

    Extracts vendor/product data from the embedded FullCalendar JSON on the
    marketplace page, then optionally enriches from affiliate info pages.

    Overrides run() to use JSON extraction instead of HTML scraping.
    """

    SOURCE_NAME = "jvzoo"
    BASE_URL = "https://www.jvzoo.com"
    REQUESTS_PER_MINUTE = 5

    TYPICAL_ROLES = ["vendor", "product_creator", "jv_partner"]
    TYPICAL_NICHES = [
        "internet_marketing", "digital_products", "info_products",
        "software", "make_money_online",
    ]
    TYPICAL_OFFERINGS = [
        "digital_products", "software", "courses", "tools",
        "done_for_you", "PLR", "agency",
    ]

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_vendors: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield the marketplace page URL."""
        yield f"{self.BASE_URL}/marketplace"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used -- data extracted from JSON in run()."""
        return []

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Extract vendor data from JVZoo marketplace calendar.

        Steps:
        1. Fetch the marketplace page
        2. Extract the FullCalendar events JSON from the page source
        3. Parse each event into a ScrapedContact
        4. Optionally fetch affiliate info pages for additional data
        """
        self.logger.info(
            "Starting %s scraper (max_contacts=%s)",
            self.SOURCE_NAME, max_contacts or "unlimited",
        )

        contacts_yielded = 0

        # Fetch the marketplace page
        html = self.fetch_page(f"{self.BASE_URL}/marketplace")
        if not html:
            self.logger.error("Failed to fetch JVZoo marketplace page")
            return

        # Extract calendar events JSON
        events = self._extract_calendar_events(html)
        if not events:
            self.logger.warning("No calendar events found on marketplace page")
            return

        self.logger.info("Found %d calendar events on JVZoo marketplace", len(events))

        # Parse each event into a contact
        for event in events:
            contact = self._parse_event(event)
            if not contact:
                continue

            # Optionally enrich from affiliate info page
            affiliate_url = event.get("url", "")
            if affiliate_url:
                self._enrich_from_affiliate_page(contact, affiliate_url)

            if contact.is_valid():
                contact.source_platform = self.SOURCE_NAME
                contact.source_url = f"{self.BASE_URL}/marketplace"
                contact.scraped_at = datetime.now().isoformat()
                contact.email = contact.clean_email()

                self.stats["contacts_valid"] += 1
                contacts_yielded += 1
                yield contact

                if max_contacts and contacts_yielded >= max_contacts:
                    self.logger.info("Reached max_contacts=%d", max_contacts)
                    return

            self.stats["contacts_found"] += 1

        self.logger.info("Scraper complete: %s", self.stats)

    def _extract_calendar_events(self, html: str) -> list[dict]:
        """Extract the FullCalendar events JSON array from page source.

        The calendar initialization code contains an events array like:
            events: [{...}, {...}, ...]

        We find this array and parse it as JSON.
        """
        # Flatten to single line for regex
        flat_html = html.replace("\n", " ")

        # Find "events: [" and extract the array using bracket counting
        idx = flat_html.find("events: [")
        if idx < 0:
            # Try alternate patterns
            idx = flat_html.find("events:[")
            if idx < 0:
                self.logger.warning("Could not find events array in page")
                return []

        # Position at the start of the array
        array_start = flat_html.index("[", idx)
        depth = 0
        array_end = array_start

        for i, char in enumerate(flat_html[array_start:array_start + 500000]):
            if char == "[":
                depth += 1
            elif char == "]":
                depth -= 1
            if depth == 0:
                array_end = array_start + i + 1
                break

        if depth != 0:
            self.logger.warning("Unbalanced brackets in events array")
            return []

        events_json = flat_html[array_start:array_end]

        try:
            events = json.loads(events_json)
            return events if isinstance(events, list) else []
        except json.JSONDecodeError as exc:
            self.logger.warning("Failed to parse events JSON: %s", exc)
            return []

    def _parse_event(self, event: dict) -> ScrapedContact | None:
        """Parse a calendar event dict into a ScrapedContact.

        Event structure:
        {
            "id": "435363",
            "product_name": "My Product",
            "title": "My Product",
            "start": "2026-03-05 10:00:00",
            "url": "https://www.jvzoo.com/affiliate/affiliateinfo/index/435363",
            "commissions": "50",
            "user_name": "John Doe",
            "user_id": "3511491",
            "funnel_id": null
        }
        """
        user_name = (event.get("user_name") or "").strip()
        product_name = (event.get("product_name") or "").strip()
        title = (event.get("title") or "").strip()
        product_id = str(event.get("id") or "").strip()
        user_id = str(event.get("user_id") or "").strip()
        commissions = str(event.get("commissions") or "").strip()
        launch_date = (event.get("start") or "").strip()
        affiliate_url = (event.get("url") or "").strip()
        funnel_id = event.get("funnel_id")

        # Must have at least a user name or product name
        if not user_name and not product_name:
            return None

        # Use vendor name as the contact name
        name = user_name or title or product_name
        if not name or len(name) < 2:
            return None

        # Dedup by vendor name (case-insensitive)
        vendor_key = name.lower()
        if vendor_key in self._seen_vendors:
            return None
        self._seen_vendors.add(vendor_key)

        # Build the affiliate info URL
        if not affiliate_url and product_id:
            affiliate_url = (
                f"{self.BASE_URL}/affiliate/affiliateinfo/index/{product_id}"
            )

        # Build bio
        bio_parts = ["JVZoo product launcher"]
        if product_name and product_name != name:
            bio_parts.append(f"Product: {product_name}")
        if commissions:
            bio_parts.append(f"Commission: {commissions}%")
        if launch_date:
            date_str = launch_date.split(" ")[0] if " " in launch_date else launch_date
            bio_parts.append(f"Launch: {date_str}")
        if funnel_id:
            bio_parts.append("Has sales funnel")
        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=name,
            website=affiliate_url,
            bio=bio,
            source_category="jv_marketplace",
            product_focus=product_name[:200] if product_name else "",
            join_date=launch_date.split(" ")[0] if launch_date else "",
            pricing=f"{commissions}% commission" if commissions else "",
            raw_data={
                "product_id": product_id,
                "user_id": user_id,
                "product_name": product_name,
                "commissions": commissions,
                "launch_date": launch_date,
                "funnel_id": funnel_id,
                "affiliate_url": affiliate_url,
            },
        )

    def _enrich_from_affiliate_page(
        self, contact: ScrapedContact, affiliate_url: str
    ) -> None:
        """Attempt to enrich a contact from their JVZoo affiliate info page.

        The affiliate info page may contain:
        - Sale price
        - Product description
        - Number of sales
        - Refund rate
        - JV page link (external)
        - Vendor website / Facebook profile

        Many fields require login to access, so we extract what's available.
        Uses a short timeout since these pages can be slow/unreliable.
        """
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        try:
            resp = self.session.get(affiliate_url, timeout=15)
            resp.raise_for_status()
            html = resp.text
            self.stats["pages_scraped"] += 1
        except Exception:
            return

        try:
            soup = self.parse_html(html)

            # Look for "Product of the Day" badge or description
            desc_divs = soup.find_all(
                "div", class_=re.compile(r"my-auto|description|subtitle", re.I)
            )
            for div in desc_divs:
                text = div.get_text(strip=True)
                if len(text) > 20 and len(text) < 500:
                    # Append to bio if meaningful
                    if text not in contact.bio:
                        contact.bio = f"{contact.bio} | {text}"
                    break

            # Look for external links: JV pages, vendor websites, social
            jvzoo_support = "jvzoosupport.com"
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                href_lower = href.lower()
                text = a_tag.get_text(strip=True).lower()

                # Skip JVZoo internal links and support
                if "jvzoo.com" in href_lower or jvzoo_support in href_lower:
                    continue

                if not href.startswith("http"):
                    continue

                # Prefer JV page links, then any external link
                if "jv" in text or "affiliate" in text or "/jv" in href_lower:
                    contact.website = href
                    break
                elif "facebook.com" in href_lower:
                    if not contact.linkedin:  # Store FB in linkedin field
                        contact.linkedin = href
                elif not contact.website or "affiliateinfo" in contact.website:
                    # Replace the JVZoo affiliate URL with a real website
                    contact.website = href

            # Look for sale price text
            price_match = re.search(
                r"\$\s*(\d+(?:\.\d{2})?)", soup.get_text()
            )
            if price_match and not contact.pricing:
                contact.pricing = f"${price_match.group(1)}"

        except Exception as exc:
            self.logger.debug(
                "Error enriching from affiliate page %s: %s",
                affiliate_url, exc,
            )
