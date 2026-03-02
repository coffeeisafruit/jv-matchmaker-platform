"""
PartnerStack Marketplace SaaS partner program scraper.

PartnerStack has a public program directory at:
  https://market.partnerstack.com/

The marketplace page embeds ALL program data (~430+ companies) in a
single `window.__INITIAL_STATE__` JavaScript object, so only one page
fetch is needed to get the complete dataset.

Each company entry includes:
  - Company name, slug, description, website
  - Commission/offer details (bounty amounts, revenue share, etc.)
  - Category tags (marketplace, product type, partner type)
  - Partnership count (number of active partners)
  - Terms of service URL

This is a high-quality source for B2B SaaS companies actively seeking
affiliate/referral/reseller partners — strong JV signal.

Estimated yield: 400-500 SaaS partner programs
"""

from __future__ import annotations

import json
import re
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


class Scraper(BaseScraper):
    """PartnerStack marketplace scraper.

    Fetches the main marketplace page and extracts all company data
    from the embedded window.__INITIAL_STATE__ JSON object.
    """

    SOURCE_NAME = "partnerstack_marketplace"
    BASE_URL = "https://market.partnerstack.com"
    REQUESTS_PER_MINUTE = 5

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_slugs: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield the single marketplace URL.

        All ~430+ companies are embedded in the main page's
        __INITIAL_STATE__ object, so only one page is needed.
        """
        yield self.BASE_URL + "/"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Extract company data from the embedded __INITIAL_STATE__ JSON.

        The page contains a script tag with:
          window.__INITIAL_STATE__ = { company: { companies: { ... } } }

        Each company is keyed by slug and contains rich structured data.
        """
        # Extract the __INITIAL_STATE__ JSON
        data = self._extract_initial_state(html)
        if not data:
            self.logger.warning("Could not extract __INITIAL_STATE__ from %s", url)
            return []

        companies_dict = (
            data.get("company", {}).get("companies", {})
        )

        if not companies_dict:
            self.logger.warning("No companies found in __INITIAL_STATE__")
            return []

        self.logger.info("Found %d companies in marketplace data", len(companies_dict))

        contacts = []
        for slug, company in companies_dict.items():
            contact = self._parse_company(slug, company)
            if contact:
                contacts.append(contact)

        return contacts

    def _extract_initial_state(self, html: str) -> Optional[dict]:
        """Parse the window.__INITIAL_STATE__ JSON from the HTML source.

        Uses brace-depth counting to find the complete JSON object,
        since the data can be very large (~4MB) and may contain
        unescaped characters that confuse simple regex.
        """
        marker = "window.__INITIAL_STATE__ = "
        start_idx = html.find(marker)
        if start_idx < 0:
            return None

        start_idx += len(marker)

        # Find matching closing brace by counting depth
        depth = 0
        end_idx = start_idx
        in_string = False
        escape_next = False

        for i in range(start_idx, min(start_idx + 10_000_000, len(html))):
            char = html[i]

            if escape_next:
                escape_next = False
                continue

            if char == "\\":
                if in_string:
                    escape_next = True
                continue

            if char == '"' and not escape_next:
                in_string = not in_string
                continue

            if in_string:
                continue

            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    end_idx = i + 1
                    break

        if depth != 0:
            self.logger.warning("Failed to find complete JSON (depth=%d)", depth)
            return None

        raw_json = html[start_idx:end_idx]

        try:
            return json.loads(raw_json)
        except json.JSONDecodeError as e:
            self.logger.warning("JSON parse error: %s", e)
            return None

    def _parse_company(self, slug: str, company: dict) -> Optional[ScrapedContact]:
        """Parse a single company dict into a ScrapedContact."""
        name = (company.get("name") or "").strip()
        if not name:
            return None

        # Skip archived, test, or restricted companies
        if company.get("archived") or company.get("test"):
            return None
        if company.get("restrict_marketplace_display"):
            return None

        # Deduplicate
        if slug in self._seen_slugs:
            return None
        self._seen_slugs.add(slug)

        # Website — normalize to full URL
        raw_website = (company.get("website") or "").strip()
        website = ""
        if raw_website:
            if not raw_website.startswith("http"):
                website = f"https://{raw_website}"
            else:
                website = raw_website

        # Description
        description = (company.get("description") or "").strip()
        description_product = (company.get("description_product") or "").strip()
        full_description = description
        if description_product and description_product != description:
            full_description = f"{description} {description_product}".strip()

        # Extract marketplace category tags
        tags = company.get("tags") or []
        marketplace_tags = sorted(set(
            t["name"]
            for t in tags
            if t.get("collection") == "marketplace" and t.get("name")
        ))
        product_tags = sorted(set(
            t["name"]
            for t in tags
            if t.get("collection") == "product" and t.get("name")
        ))
        partner_types = sorted(set(
            t["name"]
            for t in tags
            if t.get("collection") == "product.partner_type" and t.get("name")
        ))

        # Extract offer/commission information
        offers = company.get("base_offers") or []
        offer_descriptions = []
        for offer in offers:
            body = (offer.get("body") or "").strip()
            if body:
                offer_descriptions.append(body)

        # Partnership count
        partnership_count = company.get("partnership_count") or 0

        # Promotions
        promotions = company.get("promotions") or []

        # Build bio
        bio_parts = []
        if full_description:
            bio_parts.append(full_description[:600])
        if offer_descriptions:
            bio_parts.append(f"Offers: {'; '.join(offer_descriptions[:3])}")
        if marketplace_tags:
            bio_parts.append(f"Categories: {', '.join(marketplace_tags)}")
        if partner_types:
            bio_parts.append(f"Partner types: {', '.join(partner_types)}")
        if partnership_count:
            bio_parts.append(f"{partnership_count:,} active partners")
        if promotions:
            bio_parts.append(f"Promotions: {', '.join(promotions)}")
        if not bio_parts:
            bio_parts.append("PartnerStack marketplace program")

        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=name,
            company=name,
            website=website,
            bio=bio,
            source_url=f"{self.BASE_URL}/{slug}",
            source_category="saas_partner_program",
            raw_data={
                "slug": slug,
                "marketplace_tags": marketplace_tags,
                "product_tags": product_tags,
                "partner_types": partner_types,
                "offer_descriptions": offer_descriptions,
                "partnership_count": partnership_count,
                "promotions": promotions,
                "platform": "partnerstack",
            },
        )
