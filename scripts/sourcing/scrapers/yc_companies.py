"""
Y Combinator Company Directory Scraper

Fetches all YC-backed companies from the Y Combinator directory using
their Algolia-powered search API, then enriches each company with founder
details from the individual company pages (Inertia.js server-rendered data).

Data includes:
- Company name, website, one-liner description, long description
- YC batch, industry, status, stage, team size, location
- Founder names, titles, LinkedIn URLs, bios

Two-phase approach:
1. Algolia API to get all ~5,700+ companies (split by batch to bypass
   Algolia's 1,000-record search limit)
2. Individual company page fetch for founder details (Inertia.js props)

Overrides run() because the default generate_urls -> fetch_page -> scrape_page
loop does not fit the API-first pattern.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Iterator, Optional
from urllib.parse import quote

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Algolia config extracted from https://www.ycombinator.com/companies
ALGOLIA_APP_ID = "45BWZJ1SGC"
ALGOLIA_API_KEY = (
    "ZjA3NWMwMmNhMzEwZmMxOThkZDlkMjFmNDAwNTNjNjdkZjdhNWJkOWRjMThiODQw"
    "MjUyZTVkYjA4YjFlMmU2YnJlc3RyaWN0SW5kaWNlcz0lNUIlMjJZQ0NvbXBhbnlf"
    "cHJvZHVjdGlvbiUyMiUyQyUyMllDQ29tcGFueV9CeV9MYXVuY2hfRGF0ZV9wcm9k"
    "dWN0aW9uJTIyJTVEJnRhZ0ZpbHRlcnM9JTVCJTIyeWNkY19wdWJsaWMlMjIlNUQm"
    "YW5hbHl0aWNzVGFncz0lNUIlMjJ5Y2RjJTIyJTVE"
)
ALGOLIA_INDEX = "YCCompany_production"
ALGOLIA_SEARCH_URL = f"https://{ALGOLIA_APP_ID.lower()}-dsn.algolia.net/1/indexes/*/queries"

# Max hits Algolia returns per search query (hard platform limit)
ALGOLIA_MAX_HITS = 1000


class Scraper(BaseScraper):
    SOURCE_NAME = "yc_companies"
    BASE_URL = "https://www.ycombinator.com"
    REQUESTS_PER_MINUTE = 30  # Algolia can handle high throughput; be moderate for detail pages
    TYPICAL_ROLES = ["Product Creator", "Service Provider"]
    TYPICAL_NICHES = ["saas_software", "ai_machine_learning"]
    TYPICAL_OFFERINGS = ["software", "platform", "technology"]

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_ids: set[int] = set()

    # ------------------------------------------------------------------
    # Abstract method stubs (not used; we override run())
    # ------------------------------------------------------------------

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run() for API-based fetching."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used -- parsing is done inline in run()."""
        return []

    # ------------------------------------------------------------------
    # Algolia API helpers
    # ------------------------------------------------------------------

    def _algolia_search(
        self,
        query: str = "",
        page: int = 0,
        hits_per_page: int = ALGOLIA_MAX_HITS,
        facet_filters: str = "",
    ) -> Optional[dict]:
        """Execute a single Algolia search query and return the results dict."""
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        params_parts = [
            f"hitsPerPage={hits_per_page}",
            f"page={page}",
            f"query={quote(query)}",
        ]
        if facet_filters:
            params_parts.append(f"facetFilters={quote(facet_filters)}")

        payload = {
            "requests": [
                {
                    "indexName": ALGOLIA_INDEX,
                    "params": "&".join(params_parts),
                }
            ]
        }

        headers = {
            "x-algolia-application-id": ALGOLIA_APP_ID,
            "x-algolia-api-key": ALGOLIA_API_KEY,
            "content-type": "application/json",
        }

        try:
            resp = self.session.post(
                ALGOLIA_SEARCH_URL,
                headers=headers,
                json=payload,
                timeout=30,
            )
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1
            data = resp.json()
            results = data.get("results", [])
            return results[0] if results else None
        except Exception as exc:
            self.logger.warning("Algolia search failed: %s", exc)
            self.stats["errors"] += 1
            return None

    def _get_all_batches(self) -> list[str]:
        """Fetch the list of all YC batch names via Algolia facet query."""
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        payload = {
            "requests": [
                {
                    "indexName": ALGOLIA_INDEX,
                    "params": "hitsPerPage=0&page=0&query=&facets=%5B%22batch%22%5D",
                }
            ]
        }

        headers = {
            "x-algolia-application-id": ALGOLIA_APP_ID,
            "x-algolia-api-key": ALGOLIA_API_KEY,
            "content-type": "application/json",
        }

        try:
            resp = self.session.post(
                ALGOLIA_SEARCH_URL,
                headers=headers,
                json=payload,
                timeout=30,
            )
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1
            data = resp.json()
            results = data.get("results", [])
            if not results:
                return []
            facets = results[0].get("facets", {})
            batch_facet = facets.get("batch", {})
            # Sort batches chronologically
            return sorted(batch_facet.keys())
        except Exception as exc:
            self.logger.warning("Failed to fetch batch facets: %s", exc)
            self.stats["errors"] += 1
            return []

    def _fetch_batch_companies(self, batch_name: str) -> list[dict]:
        """Fetch all companies for a specific YC batch via Algolia.

        Since each batch has fewer than 1,000 companies, a single query
        suffices. Falls back to multi-page fetching if needed.
        """
        all_hits: list[dict] = []
        page = 0

        while True:
            result = self._algolia_search(
                page=page,
                hits_per_page=ALGOLIA_MAX_HITS,
                facet_filters=json.dumps(["batch:" + batch_name]),
            )
            if not result:
                break

            hits = result.get("hits", [])
            all_hits.extend(hits)

            nb_pages = result.get("nbPages", 0)
            if page + 1 >= nb_pages or not hits:
                break
            page += 1

        return all_hits

    # ------------------------------------------------------------------
    # Company detail page parser
    # ------------------------------------------------------------------

    def _fetch_founders(self, slug: str) -> list[dict]:
        """Fetch founder info from a company's detail page.

        YC uses Inertia.js which embeds structured JSON in a data-page
        attribute. We parse that to extract founder details.

        Returns a list of dicts with keys: full_name, title, linkedin_url,
        founder_bio, twitter_url.
        """
        url = f"{self.BASE_URL}/companies/{slug}"

        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1
        except Exception as exc:
            self.logger.debug("Failed to fetch detail page for %s: %s", slug, exc)
            self.stats["errors"] += 1
            return []

        html = resp.text

        # Extract Inertia.js data-page attribute
        match = re.search(r'data-page="([^"]+)"', html)
        if not match:
            return []

        try:
            encoded = match.group(1)
            decoded = (
                encoded
                .replace("&quot;", '"')
                .replace("&amp;", "&")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&#39;", "'")
            )
            page_data = json.loads(decoded)
        except (json.JSONDecodeError, ValueError):
            return []

        props = page_data.get("props", {})
        company = props.get("company", {})
        if not isinstance(company, dict):
            return []

        founders_raw = company.get("founders", [])
        if not isinstance(founders_raw, list):
            return []

        founders = []
        for f in founders_raw:
            if not isinstance(f, dict):
                continue
            founders.append({
                "full_name": (f.get("full_name") or "").strip(),
                "title": (f.get("title") or "").strip(),
                "linkedin_url": (f.get("linkedin_url") or "").strip(),
                "founder_bio": (f.get("founder_bio") or "").strip(),
                "twitter_url": (f.get("twitter_url") or "").strip(),
            })

        return founders

    # ------------------------------------------------------------------
    # Hit -> ScrapedContact conversion
    # ------------------------------------------------------------------

    def _hit_to_contacts(
        self,
        hit: dict,
        founders: list[dict],
    ) -> list[ScrapedContact]:
        """Convert an Algolia hit + founder data into ScrapedContact objects.

        Yields one contact per company (with founder info in the bio),
        plus one contact per founder who has a LinkedIn URL.
        """
        company_id = hit.get("id")
        if company_id in self._seen_ids:
            return []
        self._seen_ids.add(company_id)

        name = (hit.get("name") or "").strip()
        if not name:
            return []

        slug = (hit.get("slug") or "").strip()
        website = (hit.get("website") or "").strip()
        one_liner = (hit.get("one_liner") or "").strip()
        long_desc = (hit.get("long_description") or "").strip()
        batch = (hit.get("batch") or "").strip()
        industry = (hit.get("industry") or "").strip()
        subindustry = (hit.get("subindustry") or "").strip()
        status = (hit.get("status") or "").strip()
        stage = (hit.get("stage") or "").strip()
        team_size = hit.get("team_size") or 0
        location = (hit.get("all_locations") or "").strip()
        tags = hit.get("tags") or []
        regions = hit.get("regions") or []
        is_hiring = hit.get("isHiring", False)
        top_company = hit.get("top_company", False)

        yc_url = f"{self.BASE_URL}/companies/{slug}" if slug else ""

        # Build a rich bio
        bio_parts = []
        if one_liner:
            bio_parts.append(one_liner)
        if batch:
            bio_parts.append(f"YC {batch}.")
        if industry:
            bio_parts.append(f"Industry: {subindustry or industry}.")
        if status and status != "Active":
            bio_parts.append(f"Status: {status}.")
        if stage:
            bio_parts.append(f"Stage: {stage}.")
        if team_size:
            bio_parts.append(f"Team size: {team_size}.")
        if location:
            bio_parts.append(f"Location: {location}.")
        if tags:
            bio_parts.append(f"Tags: {', '.join(tags)}.")

        # Add founder names to bio
        founder_names = [f["full_name"] for f in founders if f.get("full_name")]
        if founder_names:
            bio_parts.append(f"Founders: {', '.join(founder_names)}.")

        # Append truncated long description
        if long_desc:
            remaining = 2000 - len(" ".join(bio_parts)) - 2
            if remaining > 100:
                desc_snippet = long_desc[:remaining]
                bio_parts.append(desc_snippet)

        bio = " ".join(bio_parts)

        contacts: list[ScrapedContact] = []

        # Company-level contact
        company_contact = ScrapedContact(
            name=name,
            email="",
            company=name,
            website=website or yc_url,
            linkedin="",
            phone="",
            bio=bio,
            source_platform=self.SOURCE_NAME,
            source_url=yc_url,
            source_category="yc_startups",
            raw_data={
                "yc_id": company_id,
                "slug": slug,
                "batch": batch,
                "industry": industry,
                "subindustry": subindustry,
                "status": status,
                "stage": stage,
                "team_size": team_size,
                "location": location,
                "tags": tags,
                "regions": regions,
                "is_hiring": is_hiring,
                "top_company": top_company,
                "founder_count": len(founders),
            },
        )
        contacts.append(company_contact)

        # Individual founder contacts (only those with LinkedIn)
        for f in founders:
            if not f.get("full_name"):
                continue
            if not f.get("linkedin_url"):
                continue

            founder_bio_parts = []
            if f.get("title"):
                founder_bio_parts.append(f"{f['title']} at {name}.")
            else:
                founder_bio_parts.append(f"Founder at {name}.")
            if batch:
                founder_bio_parts.append(f"YC {batch}.")
            if one_liner:
                founder_bio_parts.append(one_liner)
            if f.get("founder_bio"):
                founder_bio_parts.append(f["founder_bio"])

            founder_contact = ScrapedContact(
                name=f["full_name"],
                email="",
                company=name,
                website=website or yc_url,
                linkedin=f.get("linkedin_url", ""),
                phone="",
                bio=" ".join(founder_bio_parts),
                source_platform=self.SOURCE_NAME,
                source_url=yc_url,
                source_category="yc_founders",
                raw_data={
                    "company_slug": slug,
                    "title": f.get("title", ""),
                    "twitter_url": f.get("twitter_url", ""),
                },
            )
            contacts.append(founder_contact)

        return contacts

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Two-phase fetch: Algolia API for company list, detail pages for founders.

        Phase 1: Query Algolia by batch to get all companies (bypasses 1000-hit limit).
        Phase 2: For each company, optionally fetch the detail page for founder info.

        Yields ScrapedContact objects for both companies and individual founders.
        """
        self.logger.info("Starting %s scraper", self.SOURCE_NAME)

        # Phase 1: Get all batch names
        self.logger.info("Fetching YC batch list from Algolia...")
        batches = self._get_all_batches()
        if not batches:
            self.logger.error("Could not retrieve batch list. Falling back to single query.")
            # Fallback: single query for up to 1000 companies
            batches = ["__ALL__"]

        self.logger.info("Found %d batches to process", len(batches))

        # Collect all company hits across batches
        all_companies: list[dict] = []
        for batch_name in batches:
            if batch_name == "__ALL__":
                result = self._algolia_search(hits_per_page=ALGOLIA_MAX_HITS)
                hits = result.get("hits", []) if result else []
            else:
                hits = self._fetch_batch_companies(batch_name)

            new_hits = [h for h in hits if h.get("id") not in self._seen_ids]
            all_companies.extend(new_hits)
            # Track IDs to avoid dups across batch queries
            for h in new_hits:
                self._seen_ids.add(h.get("id"))

            self.logger.info(
                "Batch '%s': %d companies (%d new, %d total so far)",
                batch_name, len(hits), len(new_hits), len(all_companies),
            )

        # Reset seen IDs so _hit_to_contacts can re-check
        self._seen_ids.clear()

        total_companies = len(all_companies)
        self.logger.info("Total companies to process: %d", total_companies)

        # Resume from checkpoint
        start_from = (checkpoint or {}).get("last_url")
        past_checkpoint = start_from is None
        contacts_yielded = 0
        companies_done = 0

        # Phase 2: Process each company
        for idx, hit in enumerate(all_companies):
            slug = (hit.get("slug") or "").strip()
            yc_url = f"{self.BASE_URL}/companies/{slug}" if slug else ""

            # Skip until past checkpoint
            if not past_checkpoint:
                if yc_url == start_from:
                    past_checkpoint = True
                continue

            # Fetch founder details from detail page
            founders = []
            if slug:
                founders = self._fetch_founders(slug)

            # Convert to contacts
            try:
                contacts = self._hit_to_contacts(hit, founders)
            except Exception as exc:
                self.logger.error("Parse error for %s: %s", slug, exc)
                self.stats["errors"] += 1
                continue

            for contact in contacts:
                contact.source_platform = self.SOURCE_NAME
                contact.source_url = yc_url
                contact.scraped_at = datetime.now().isoformat()
                contact.email = contact.clean_email()

                self.stats["contacts_found"] += 1

                if contact.is_valid():
                    self.stats["contacts_valid"] += 1
                    contacts_yielded += 1
                    yield contact

                    if max_contacts and contacts_yielded >= max_contacts:
                        self.logger.info("Reached max_contacts=%d", max_contacts)
                        return

            companies_done += 1
            if companies_done % 50 == 0:
                self.logger.info(
                    "Progress: %d/%d companies, %d valid contacts yielded",
                    companies_done, total_companies, self.stats["contacts_valid"],
                )

            if max_pages and companies_done >= max_pages:
                self.logger.info("Reached max_pages=%d (companies processed)", max_pages)
                break

        self.logger.info("Scraper complete: %s", self.stats)
