"""
ShareASale / Awin merchant directory scraper.

ShareASale was acquired by Awin and now redirects to awin.com.
The Awin Advertiser Directory is publicly searchable via an Algolia
search index embedded in their frontend at:
  https://www.awin.com/us/search/advertiser-directory

This scraper queries the Algolia search API directly using the
public search-only credentials from Awin's frontend JavaScript.
No authentication or API key required — these are public search keys
designed to be used in the browser.

Data available per merchant:
  - Company name, description, sector/category
  - Awin merchant profile URL
  - Logo image URL
  - Join date on the network

Regions available: us, gb, de, fr, nl, it, es, ca, au, nordics, etc.
US index alone has ~8,000 merchants.

Estimated yield: 8,000+ merchants (US only), 24,000+ (all regions)
"""

from __future__ import annotations

import json
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Awin Algolia public search credentials (from their frontend JS)
ALGOLIA_APP_ID = "C1W1Y0AAMV"
ALGOLIA_SEARCH_KEY = "e2f2e353dd25e86d66044683f3711e6a"
ALGOLIA_HOST = f"https://{ALGOLIA_APP_ID}-dsn.algolia.net"

# Index name pattern: awin-website-automatic_advertiser-directory_{region}
INDEX_BASE = "awin-website-automatic_advertiser-directory"

# Regions to scrape (ordered by size / relevance for JV matching)
# Set to just ["us"] for US-only, or expand for international coverage.
REGIONS = [
    "us",   # ~8,000 merchants
    "gb",   # ~3,700
    "de",   # ~2,700
    "ca",   # ~226
    "au",   # ~143
]

# Algolia max hits per page
HITS_PER_PAGE = 100  # Algolia allows up to 1000, but 100 is safe


class Scraper(BaseScraper):
    """Awin/ShareASale advertiser directory scraper via Algolia API.

    Uses the public Algolia search credentials embedded in Awin's
    frontend to query their advertiser directory index.
    """

    SOURCE_NAME = "shareasale_merchants"
    BASE_URL = "https://www.awin.com"
    REQUESTS_PER_MINUTE = 15  # Algolia is generous with rate limits
    TYPICAL_ROLES = ["Affiliate/Promoter", "Product Creator"]
    TYPICAL_NICHES = ["affiliate_marketing", "ecommerce"]
    TYPICAL_OFFERINGS = ["affiliate", "products", "commissions"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_ids: set[str] = set()
        # Set up Algolia headers
        self.session.headers.update({
            "X-Algolia-Application-Id": ALGOLIA_APP_ID,
            "X-Algolia-API-Key": ALGOLIA_SEARCH_KEY,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used — we override run() for Algolia API pagination."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Algolia JSON response into ScrapedContacts.

        Each hit in the Algolia response looks like:
        {
          "id": "124452",
          "name": "Company Name",
          "description": "Company description...",
          "joinDate": "2026-02-24 19:41:28",
          "image": "https://ui.awin.com/images/upload/merchant/profile/124452.png",
          "link": "https://ui.awin.com/merchant-profile/124452",
          "url": "https://ui.awin.com/merchant-profile/124452",
          "sectors": [{"sectorName": "Health & Beauty", "parentSectorName": "Retail & Shopping"}],
          "parentSectorName": "Retail & Shopping",
          "objectID": "124452"
        }
        """
        contacts = []
        try:
            data = json.loads(html)
        except json.JSONDecodeError as e:
            self.logger.warning("Failed to parse JSON from %s: %s", url, e)
            return []

        hits = data.get("hits", [])
        for hit in hits:
            contact = self._parse_hit(hit)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_hit(self, hit: dict) -> Optional[ScrapedContact]:
        """Parse a single Algolia hit into a ScrapedContact."""
        merchant_id = (hit.get("id") or hit.get("objectID") or "")
        name = (hit.get("name") or "").strip()
        description = (hit.get("description") or "").strip()
        profile_url = (hit.get("link") or hit.get("url") or "").strip()
        join_date = (hit.get("joinDate") or "").strip()

        if not name:
            return None

        # Deduplicate by merchant ID
        dedup_key = merchant_id if merchant_id else name.lower()
        if dedup_key in self._seen_ids:
            return None
        self._seen_ids.add(dedup_key)

        # Extract sector info
        sectors = hit.get("sectors") or []
        sector_names = []
        parent_sectors = set()
        for sector in sectors:
            sector_name = (sector.get("sectorName") or "").strip()
            parent_name = (sector.get("parentSectorName") or "").strip()
            if sector_name:
                sector_names.append(sector_name)
            if parent_name:
                parent_sectors.add(parent_name)

        # Also check top-level parentSectorName
        top_parent = (hit.get("parentSectorName") or "").strip()
        if top_parent:
            parent_sectors.add(top_parent)

        # Build bio
        bio_parts = []
        if description:
            bio_parts.append(description[:500])
        if sector_names:
            bio_parts.append(f"Categories: {', '.join(sector_names)}")
        if parent_sectors:
            bio_parts.append(f"Sector: {', '.join(sorted(parent_sectors))}")
        if join_date:
            bio_parts.append(f"Awin member since: {join_date[:10]}")
        if not bio_parts:
            bio_parts.append("Awin/ShareASale network merchant")

        bio = " | ".join(bio_parts)

        # Use profile URL as website (the merchant's actual website
        # isn't exposed in the Algolia data, but the profile page is)
        website = profile_url

        return ScrapedContact(
            name=name,
            company=name,
            website=website,
            bio=bio,
            source_category="affiliate_network",
            raw_data={
                "merchant_id": merchant_id,
                "sectors": sector_names,
                "parent_sectors": list(parent_sectors),
                "join_date": join_date,
                "image_url": (hit.get("image") or ""),
                "platform": "awin_shareasale",
            },
        )

    def _query_algolia(self, index_name: str, page: int) -> Optional[dict]:
        """Execute an Algolia search query and return the JSON response."""
        url = f"{ALGOLIA_HOST}/1/indexes/{index_name}/query"

        payload = json.dumps({
            "query": "",
            "hitsPerPage": HITS_PER_PAGE,
            "page": page,
        })

        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        try:
            import urllib.request

            req = urllib.request.Request(
                url,
                data=payload.encode("utf-8"),
                headers={
                    "X-Algolia-Application-Id": ALGOLIA_APP_ID,
                    "X-Algolia-API-Key": ALGOLIA_SEARCH_KEY,
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            resp = urllib.request.urlopen(req, timeout=30)
            self.stats["pages_scraped"] += 1
            return json.loads(resp.read())
        except Exception as e:
            self.stats["errors"] += 1
            self.logger.warning(
                "Algolia query failed (index=%s, page=%d): %s",
                index_name, page, e,
            )
            return None

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Paginate through Awin's Algolia advertiser directory.

        Iterates through configured regions, paginating each index
        until all merchants are retrieved.
        """
        from datetime import datetime

        self.logger.info(
            "Starting %s scraper (regions: %s)",
            self.SOURCE_NAME, ", ".join(REGIONS),
        )

        contacts_yielded = 0
        pages_done = 0
        start_region = (checkpoint or {}).get("last_region")
        start_page = (checkpoint or {}).get("last_page", 0)
        past_checkpoint = start_region is None

        for region in REGIONS:
            index_name = f"{INDEX_BASE}_{region}"

            if not past_checkpoint:
                if region == start_region:
                    past_checkpoint = True
                    # Resume from the page after the last completed one
                    start_page_for_region = start_page + 1
                else:
                    continue
            else:
                start_page_for_region = 0

            self.logger.info("Scraping region '%s' (index: %s)", region, index_name)

            # First query to get total count
            page = start_page_for_region
            while True:
                result = self._query_algolia(index_name, page)
                if result is None:
                    break

                total_hits = result.get("nbHits", 0)
                total_pages = result.get("nbPages", 0)

                if page == start_page_for_region:
                    self.logger.info(
                        "Region '%s': %d total merchants, %d pages",
                        region, total_hits, total_pages,
                    )

                # Parse the hits
                hits_json = json.dumps(result)
                try:
                    contacts = self.scrape_page(
                        f"algolia://{index_name}?page={page}",
                        hits_json,
                    )
                except Exception as e:
                    self.stats["errors"] += 1
                    self.logger.error("Parse error on page %d: %s", page, e)
                    page += 1
                    continue

                for contact in contacts:
                    contact.source_platform = self.SOURCE_NAME
                    contact.source_url = f"https://www.awin.com/us/search/advertiser-directory"
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
                if pages_done % 20 == 0:
                    self.logger.info(
                        "Progress: %d pages, %d valid contacts (region: %s, page: %d/%d)",
                        pages_done, self.stats["contacts_valid"],
                        region, page + 1, total_pages,
                    )

                if max_pages and pages_done >= max_pages:
                    self.logger.info("Reached max_pages=%d", max_pages)
                    return

                # Move to next page
                page += 1
                if page >= total_pages:
                    break

            self.logger.info(
                "Finished region '%s': %d valid contacts so far",
                region, self.stats["contacts_valid"],
            )

        self.logger.info("Scraper complete: %s", self.stats)
