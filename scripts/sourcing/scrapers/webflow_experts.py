"""
Webflow Certified Partners directory scraper.

Scrapes https://webflow.com/certified-partners/browse for certified
Webflow agencies and freelancers.

The directory uses server-rendered HTML with JSON-LD structured data
(schema.org ItemList) embedded in each page. Pagination via query
parameter ``5b5090bd_page=N`` with ~10 partners per page.

Profile URLs follow the pattern: https://webflow.com/@{handle}

Estimated yield: 1,500+ certified partners
"""

from __future__ import annotations

import json
import re
from typing import Iterator
from urllib.parse import urljoin, urlencode, parse_qs, urlparse

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Maximum pages to iterate (directory reports ~158 pages as of 2026-03)
MAX_PAGES = 200

# Pagination query param (Webflow CMS collection list identifier)
PAGE_PARAM = "5b5090bd_page"


class Scraper(BaseScraper):
    SOURCE_NAME = "webflow_experts"
    BASE_URL = "https://webflow.com/certified-partners/browse"
    REQUESTS_PER_MINUTE = 6

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_handles: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield paginated browse URLs for the certified partners directory."""
        # Page 1 has no query param, subsequent pages use the CMS param
        yield self.BASE_URL
        for page in range(2, MAX_PAGES + 1):
            yield f"{self.BASE_URL}?{PAGE_PARAM}={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse partner listings from the browse page.

        Uses two strategies:
        1. JSON-LD structured data (most reliable, contains name/location/price)
        2. HTML card parsing (fallback, extracts additional fields)
        """
        soup = self.parse_html(html)
        contacts = []

        # Strategy 1: Extract from JSON-LD ItemList
        jsonld_contacts = self._parse_jsonld(soup, url)
        if jsonld_contacts:
            contacts.extend(jsonld_contacts)

        # Strategy 2: Parse HTML cards for any partners not in JSON-LD
        html_contacts = self._parse_html_cards(soup, url)
        for contact in html_contacts:
            handle = self._extract_handle(contact.source_url)
            if handle and handle not in self._seen_handles:
                contacts.append(contact)

        # Detect empty page (end of pagination)
        if not contacts:
            self.logger.info("Empty page at %s — end of directory", url)

        return contacts

    def _parse_jsonld(self, soup, page_url: str) -> list[ScrapedContact]:
        """Extract partners from JSON-LD schema.org ItemList."""
        contacts = []

        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
            except (json.JSONDecodeError, TypeError):
                continue

            if data.get("@type") != "ItemList":
                continue

            items = data.get("itemListElement", [])
            for item in items:
                partner = item.get("item", {})
                if not partner:
                    continue

                name = (partner.get("name") or "").strip()
                if not name or len(name) < 2:
                    continue

                # Address
                address = partner.get("address", {})
                city = (address.get("addressLocality") or "").strip()
                country = (address.get("addressCountry") or "").strip()

                # Price
                price_range = (partner.get("priceRange") or "").strip()

                # Type (Organization or Person)
                entity_type = partner.get("@type", "")

                # Build bio
                description = (partner.get("description") or "").strip()
                bio_parts = []
                if description:
                    bio_parts.append(description)
                if city and country:
                    bio_parts.append(f"Location: {city}, {country}")
                elif country:
                    bio_parts.append(f"Location: {country}")
                if price_range:
                    bio_parts.append(f"Starting at {price_range}")
                if entity_type:
                    bio_parts.append(f"Type: {entity_type}")
                bio = " | ".join(bio_parts)

                contact = ScrapedContact(
                    name=name,
                    company=name if entity_type == "Organization" else "",
                    website="",  # filled from HTML cards
                    bio=bio,
                    source_url=page_url,
                    source_category="webflow_certified_partner",
                    raw_data={
                        "city": city,
                        "country": country,
                        "price_range": price_range,
                        "entity_type": entity_type,
                    },
                )

                # Track by name to avoid duplicates
                self._seen_handles.add(name.lower().strip())
                contacts.append(contact)

        return contacts

    def _parse_html_cards(self, soup, page_url: str) -> list[ScrapedContact]:
        """Fallback: parse partner cards from the HTML structure."""
        contacts = []

        # Look for partner card links pointing to @handle profiles
        profile_links = {}
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            # Match /@handle pattern
            if re.match(r"^https?://webflow\.com/@[\w\-]+$", href):
                handle = self._extract_handle(href)
                if handle and handle not in self._seen_handles:
                    # Get the parent card container
                    card = a.find_parent(
                        ["div", "article", "li"],
                        class_=re.compile(r"partner|card|item|collection", re.I),
                    )
                    if not card:
                        card = a.parent
                    profile_links[handle] = {
                        "url": href,
                        "card": card,
                        "link_text": a.get_text(strip=True),
                    }

        for handle, info in profile_links.items():
            if handle.lower() in self._seen_handles:
                continue

            name = info["link_text"] or handle
            card = info["card"]

            # Extract data from the card container
            bio = ""
            city = ""
            country = ""
            services = ""
            price = ""
            partner_type = ""

            if card:
                card_text = card.get_text(separator="\n", strip=True)
                lines = [l.strip() for l in card_text.split("\n") if l.strip()]

                # Look for location pattern (e.g., "Philadelphia, United States")
                for line in lines:
                    if "," in line and len(line) < 80:
                        parts = line.split(",", 1)
                        if len(parts) == 2 and len(parts[0].strip()) > 1:
                            possible_city = parts[0].strip()
                            possible_country = parts[1].strip()
                            # Heuristic: locations have title-cased words
                            if possible_city[0].isupper() and possible_country[0].isupper():
                                city = possible_city
                                country = possible_country
                                break

                # Look for price pattern
                for line in lines:
                    price_match = re.search(r"\$[\d,]+(?:\s*USD)?", line)
                    if price_match:
                        price = price_match.group(0)
                        break

                # Look for type (Agency/Freelancer)
                for line in lines:
                    if line.lower() in ("agency", "freelancer"):
                        partner_type = line
                        break

                # Description - longest text block that isn't just a label
                desc_candidates = [l for l in lines if len(l) > 40 and l != name]
                if desc_candidates:
                    bio = desc_candidates[0][:500]

            # Build bio if not found from card text
            if not bio:
                bio_parts = [name]
                if city:
                    bio_parts.append(f"{city}, {country}")
                if partner_type:
                    bio_parts.append(f"Webflow {partner_type}")
                bio_parts.append("Webflow Certified Partner")
                bio = " | ".join(bio_parts)

            contact = ScrapedContact(
                name=name,
                company=name if partner_type == "Agency" else "",
                website="",
                bio=bio,
                source_url=info["url"],
                source_category="webflow_certified_partner",
                raw_data={
                    "city": city,
                    "country": country,
                    "price": price,
                    "partner_type": partner_type,
                    "handle": handle,
                    "profile_url": info["url"],
                },
            )

            self._seen_handles.add(handle.lower())
            contacts.append(contact)

        return contacts

    def _extract_handle(self, url: str) -> str:
        """Extract the @handle from a Webflow profile URL."""
        match = re.search(r"webflow\.com/@([\w\-]+)", url)
        return match.group(1) if match else ""

    def _enrich_from_profile(self, contact: ScrapedContact, profile_url: str) -> None:
        """Optionally fetch the profile page for additional data.

        Not called by default to conserve rate limits. Can be enabled
        for targeted enrichment of high-value partners.
        """
        html = self.fetch_page(profile_url)
        if not html:
            return

        soup = self.parse_html(html)

        # Look for external website links
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            href_lower = href.lower()
            if (
                href.startswith("http")
                and "webflow.com" not in href_lower
                and "facebook.com" not in href_lower
                and "twitter.com" not in href_lower
                and "instagram.com" not in href_lower
                and "youtube.com" not in href_lower
                and "linkedin.com" not in href_lower
            ):
                if not contact.website:
                    contact.website = href

            if "linkedin.com/in/" in href_lower or "linkedin.com/company/" in href_lower:
                if not contact.linkedin:
                    contact.linkedin = href

        # Look for email
        emails = self.extract_emails(html)
        if emails and not contact.email:
            contact.email = emails[0]
