"""
SEC EDGAR Full-Text Search Scraper

Uses the EDGAR full-text search API to find companies by business description.
Searches for JV-relevant terms like "consulting", "marketing", "coaching", etc.
within 10-K filings to find companies in professional services.

API: https://efts.sec.gov/LATEST/search-index
No API key required, needs User-Agent header.
"""

import json
from typing import Iterator, Optional
from datetime import datetime

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Search terms for JV-relevant companies
SEARCH_TERMS = [
    '"management consulting"',
    '"marketing agency"',
    '"advertising agency"',
    '"public relations"',
    '"business consulting"',
    '"technology consulting"',
    '"IT consulting"',
    '"financial advisory"',
    '"human resources consulting"',
    '"training and development"',
    '"professional services"',
    '"staffing agency"',
    '"digital marketing"',
    '"software development"',
    '"coaching services"',
    '"strategic consulting"',
    '"investment advisory"',
    '"accounting firm"',
    '"engineering services"',
    '"healthcare consulting"',
    '"real estate services"',
    '"insurance agency"',
    '"franchise"',
    '"joint venture"',
    '"partnership"',
]


class Scraper(BaseScraper):
    SOURCE_NAME = "sec_edgar_search"
    BASE_URL = "https://efts.sec.gov"
    REQUESTS_PER_MINUTE = 8  # SEC rate limit

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self.session.headers["User-Agent"] = "JVMatchmaker/1.0 (help@jvmatches.com)"
        self.session.headers["Accept"] = "application/json"
        self._seen_ciks: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used — override run()."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used — override run()."""
        return []

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Search EDGAR for companies by business keywords."""
        self.logger.info("Starting %s scraper — %d search terms",
                         self.SOURCE_NAME, len(SEARCH_TERMS))

        start_term_idx = (checkpoint or {}).get("term_idx", 0)
        contacts_yielded = 0
        pages_done = 0

        for term_idx, term in enumerate(SEARCH_TERMS):
            if term_idx < start_term_idx:
                continue

            self.logger.info("Searching: %s (%d/%d)",
                             term, term_idx + 1, len(SEARCH_TERMS))

            offset = 0
            page_size = 50

            while True:
                if self.rate_limiter:
                    self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

                url = (
                    f"{self.BASE_URL}/LATEST/search-index"
                    f"?q={term}"
                    f"&dateRange=custom&startdt=2020-01-01&enddt=2026-12-31"
                    f"&forms=10-K"
                    f"&from={offset}&size={page_size}"
                )

                try:
                    resp = self.session.get(url, timeout=30)
                    resp.raise_for_status()
                    data = resp.json()
                    self.stats["pages_scraped"] += 1
                except Exception as e:
                    self.stats["errors"] += 1
                    self.logger.warning("Search error for %s offset %d: %s",
                                        term, offset, e)
                    break

                hits = data.get("hits", {})
                total = hits.get("total", {}).get("value", 0)
                results = hits.get("hits", [])

                if not results:
                    break

                for item in results:
                    contact = self._parse_hit(item, term)
                    if contact:
                        contacts_yielded += 1
                        yield contact

                        if max_contacts and contacts_yielded >= max_contacts:
                            self.logger.info("Reached max_contacts=%d", max_contacts)
                            return

                pages_done += 1
                offset += page_size

                if offset >= total or offset >= 500:  # Cap at 500 per term
                    break

                if max_pages and pages_done >= max_pages:
                    self.logger.info("Reached max_pages=%d", max_pages)
                    return

            self.logger.info("Term '%s' done: %d total found, %d valid so far",
                             term, total if 'total' in dir() else 0,
                             self.stats["contacts_valid"])

        self.logger.info("Scraper complete: %s", self.stats)

    def _parse_hit(self, item: dict, search_term: str) -> ScrapedContact | None:
        """Parse a search hit into a ScrapedContact."""
        source = item.get("_source", {})

        # Get CIK(s) and display name(s)
        ciks = source.get("ciks", [])
        display_names = source.get("display_names", [])
        sics = source.get("sics", [])
        biz_locations = source.get("biz_locations", [])

        if not ciks or not display_names:
            return None

        cik = ciks[0]
        if cik in self._seen_ciks:
            return None
        self._seen_ciks.add(cik)

        # Parse display name — format: "Company Name (TICKER) (CIK 0001234567)"
        raw_name = display_names[0]
        # Strip CIK suffix
        name = raw_name.split("(CIK")[0].strip()
        # Extract ticker if present
        ticker = ""
        if "(" in name and ")" in name:
            paren_start = name.rindex("(")
            ticker = name[paren_start + 1:name.rindex(")")].strip()
            name = name[:paren_start].strip()

        if not name or len(name) < 3:
            return None

        location = biz_locations[0] if biz_locations else ""
        sic = sics[0] if sics else ""

        # Build bio
        bio_parts = [name]
        if ticker:
            bio_parts.append(f"({ticker})")
        if location:
            bio_parts.append(location)
        if sic:
            bio_parts.append(f"SIC: {sic}")
        bio_parts.append(f"Found via: {search_term}")
        bio = " | ".join(bio_parts)

        # Use SEC filing page
        cik_padded = cik.zfill(10)
        website = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik_padded}"

        contact = ScrapedContact(
            name=name,
            email="",
            company=name,
            website=website,
            linkedin="",
            phone="",
            bio=bio,
            source_platform=self.SOURCE_NAME,
            source_url=f"{self.BASE_URL}/LATEST/search-index",
            source_category="public_companies",
            scraped_at=datetime.now().isoformat(),
            raw_data={
                "cik": cik,
                "ticker": ticker,
                "sic": sic,
                "location": location,
                "search_term": search_term,
            },
        )

        if not contact.is_valid():
            return None

        self.stats["contacts_found"] += 1
        self.stats["contacts_valid"] += 1
        return contact
