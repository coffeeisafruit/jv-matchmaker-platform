"""
Techstars Portfolio Scraper

Fetches portfolio company data from Techstars via their Typesense search API.
Source: https://www.techstars.com/portfolio

The Techstars portfolio page is a Next.js app that loads company data from
a Typesense search index. The public API key and cluster URL are embedded
in the client-side JavaScript bundle.

Data includes:
- Company name, website, description
- Location (city, state, country)
- Accelerator program and year
- LinkedIn, Twitter, Facebook, Crunchbase URLs
- Industry verticals

Overrides run() because data is fetched via Typesense search API, not
by crawling HTML pages.
"""

import json
from typing import Iterator, Optional
from datetime import datetime

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Typesense cluster credentials (public, embedded in client-side JS bundle)
TYPESENSE_URL = "https://8gbms7c94riane0lp-1.a1.typesense.net"
TYPESENSE_API_KEY = "0QKFSu4mIDX9UalfCNQN4qjg2xmukDE0"
TYPESENSE_COLLECTION = "companies"

# Number of results per page (Typesense max is 250)
PAGE_SIZE = 250


class Scraper(BaseScraper):
    SOURCE_NAME = "techstars_portfolio"
    BASE_URL = "https://www.techstars.com/portfolio"
    REQUESTS_PER_MINUTE = 10

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.session.headers.update({
            "X-TYPESENSE-API-KEY": TYPESENSE_API_KEY,
            "Accept": "application/json",
        })

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run() for Typesense API pagination."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a Typesense search response page into ScrapedContacts."""
        try:
            data = json.loads(html)
        except json.JSONDecodeError:
            self.logger.warning("Failed to parse JSON from %s", url)
            return []

        contacts = []
        hits = data.get("hits", [])

        for hit in hits:
            doc = hit.get("document", {})
            company_name = (doc.get("company_name") or "").strip()
            if not company_name:
                continue

            website = (doc.get("website") or "").strip()
            description = (doc.get("brief_description") or "").strip()
            linkedin = (doc.get("linkedin_url") or "").strip()
            city = (doc.get("city") or "").strip()
            state = (doc.get("state_province") or "").strip()
            country = (doc.get("country") or "").strip()
            year = doc.get("first_session_year") or ""
            latest_class_for = doc.get("latest_class_for") or []
            if isinstance(latest_class_for, list):
                accelerator = latest_class_for[0].strip() if latest_class_for else ""
            else:
                accelerator = str(latest_class_for).strip()
            twitter = (doc.get("twitter_url") or "").strip()
            facebook = (doc.get("facebook_url") or "").strip()
            crunchbase = (doc.get("crunchbase_url") or "").strip()
            logo_url = (doc.get("logo_url") or "").strip()
            verticals = doc.get("industry_vertical") or []
            programs = doc.get("program_names") or []
            is_exit = doc.get("is_exit", False)
            is_1b = doc.get("is_1b", False)

            # Normalize website URL
            if website and not website.startswith("http"):
                website = "https://" + website

            # Normalize LinkedIn URL
            if linkedin and not linkedin.startswith("http"):
                linkedin = "https://" + linkedin

            # Build location string
            location_parts = [p for p in [city, state, country] if p]
            location = ", ".join(location_parts)

            # Build bio
            bio_parts = []
            if description:
                bio_parts.append(description)
            if accelerator and year:
                bio_parts.append(f"Techstars {accelerator} ({year})")
            elif accelerator:
                bio_parts.append(f"Techstars {accelerator}")
            if location:
                bio_parts.append(f"Location: {location}")
            if verticals:
                bio_parts.append(f"Verticals: {', '.join(verticals)}")
            if is_exit:
                bio_parts.append("Status: Exit (Acquired/IPO)")
            if is_1b:
                bio_parts.append("Valuation: $1B+")
            bio = " | ".join(bio_parts)

            contact = ScrapedContact(
                name=company_name,
                email="",
                company=company_name,
                website=website,
                linkedin=linkedin,
                phone="",
                bio=bio,
                source_category="accelerator_portfolio",
                raw_data={
                    "company_id": doc.get("company_id", ""),
                    "city": city,
                    "state": state,
                    "country": country,
                    "year": str(year),
                    "accelerator": accelerator,
                    "programs": programs,
                    "verticals": verticals,
                    "twitter": twitter,
                    "facebook": facebook,
                    "crunchbase": crunchbase,
                    "logo_url": logo_url,
                    "is_exit": is_exit,
                    "is_1b": is_1b,
                },
            )
            contacts.append(contact)

        return contacts

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Fetch all Techstars portfolio companies via Typesense search API.

        Paginates through the full index using sort_by for stable ordering.
        Yields ScrapedContact objects compatible with the runner.
        """
        self.logger.info("Starting %s scraper", self.SOURCE_NAME)

        page = 1
        pages_done = 0
        contacts_yielded = 0

        # Resume from checkpoint
        if checkpoint and "page" in checkpoint:
            page = int(checkpoint["page"])
            self.logger.info("Resuming from page %d", page)

        while True:
            search_url = (
                f"{TYPESENSE_URL}/collections/{TYPESENSE_COLLECTION}"
                f"/documents/search"
            )
            params = {
                "q": "*",
                "query_by": "company_name",
                "sort_by": "website_order:asc",
                "per_page": PAGE_SIZE,
                "page": page,
            }

            if self.rate_limiter:
                self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

            try:
                resp = self.session.get(search_url, params=params, timeout=30)
                resp.raise_for_status()
                html = resp.text
                self.stats["pages_scraped"] += 1
            except Exception as exc:
                self.stats["errors"] += 1
                self.logger.error(
                    "Typesense search failed (page %d): %s", page, exc
                )
                break

            try:
                contacts = self.scrape_page(search_url, html)
            except Exception as exc:
                self.stats["errors"] += 1
                self.logger.error("Parse error on page %d: %s", page, exc)
                break

            if not contacts:
                self.logger.info("No more results at page %d", page)
                break

            for contact in contacts:
                contact.source_platform = self.SOURCE_NAME
                contact.source_url = self.BASE_URL
                contact.scraped_at = datetime.now().isoformat()
                contact.email = contact.clean_email()

                if contact.is_valid():
                    self.stats["contacts_valid"] += 1
                    contacts_yielded += 1
                    yield contact

                    if max_contacts and contacts_yielded >= max_contacts:
                        self.logger.info(
                            "Reached max_contacts=%d", max_contacts
                        )
                        return

                self.stats["contacts_found"] += 1

            pages_done += 1
            self.logger.info(
                "Page %d: %d companies fetched, %d valid total",
                page, len(contacts), self.stats["contacts_valid"],
            )

            if max_pages and pages_done >= max_pages:
                self.logger.info("Reached max_pages=%d", max_pages)
                break

            # Check if we've fetched all results
            try:
                response_data = json.loads(html)
                total_found = response_data.get("found", 0)
                if page * PAGE_SIZE >= total_found:
                    self.logger.info(
                        "All %d companies fetched", total_found
                    )
                    break
            except (json.JSONDecodeError, TypeError):
                pass

            page += 1

        self.logger.info("Scraper complete: %s", self.stats)
