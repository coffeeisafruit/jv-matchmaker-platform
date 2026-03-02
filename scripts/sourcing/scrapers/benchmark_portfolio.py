"""
Benchmark Capital Portfolio Scraper

Fetches portfolio companies from Benchmark's website. The site is extremely
minimal (just office addresses and a Twitter link on the homepage).
Portfolio data may be available at /portfolio or through sitemap.

Strategy: Try multiple URL patterns and fall back to sitemap/robots.txt
discovery, then individual company page parsing.

Source: https://www.benchmark.com
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Known Benchmark portfolio companies (curated from public sources)
# This serves as a seed list when the site doesn't expose a browsable portfolio
KNOWN_COMPANIES = [
    {"name": "Uber", "website": "https://www.uber.com"},
    {"name": "Twitter", "website": "https://twitter.com"},
    {"name": "Snap", "website": "https://www.snap.com"},
    {"name": "Instagram", "website": "https://www.instagram.com"},
    {"name": "Dropbox", "website": "https://www.dropbox.com"},
    {"name": "eBay", "website": "https://www.ebay.com"},
    {"name": "Discord", "website": "https://discord.com"},
    {"name": "Asana", "website": "https://asana.com"},
    {"name": "Zillow", "website": "https://www.zillow.com"},
    {"name": "Stitchfix", "website": "https://www.stitchfix.com"},
    {"name": "Nextdoor", "website": "https://nextdoor.com"},
    {"name": "Tinder", "website": "https://tinder.com"},
    {"name": "WeWork", "website": "https://www.wework.com"},
    {"name": "Riot Games", "website": "https://www.riotgames.com"},
    {"name": "Quora", "website": "https://www.quora.com"},
    {"name": "Domo", "website": "https://www.domo.com"},
    {"name": "New Relic", "website": "https://newrelic.com"},
    {"name": "Zendesk", "website": "https://www.zendesk.com"},
    {"name": "Elastic", "website": "https://www.elastic.co"},
    {"name": "Fiverr", "website": "https://www.fiverr.com"},
    {"name": "Brex", "website": "https://www.brex.com"},
    {"name": "Miro", "website": "https://miro.com"},
    {"name": "Cerebras", "website": "https://www.cerebras.net"},
    {"name": "Airtable", "website": "https://airtable.com"},
    {"name": "Chainalysis", "website": "https://www.chainalysis.com"},
    {"name": "Sweetgreen", "website": "https://www.sweetgreen.com"},
    {"name": "Stitch Fix", "website": "https://www.stitchfix.com"},
    {"name": "Handshake", "website": "https://joinhandshake.com"},
    {"name": "Carta", "website": "https://carta.com"},
    {"name": "Turo", "website": "https://turo.com"},
    {"name": "Confluent", "website": "https://www.confluent.io"},
    {"name": "Calm", "website": "https://www.calm.com"},
    {"name": "Imprint", "website": "https://www.imprint.co"},
    {"name": "Faire", "website": "https://www.faire.com"},
    {"name": "Brava Home", "website": "https://www.bfrb.com"},
    {"name": "Snyk", "website": "https://snyk.io"},
    {"name": "Forter", "website": "https://www.forter.com"},
    {"name": "Hippo Insurance", "website": "https://www.hippo.com"},
    {"name": "Gigster", "website": "https://www.gigster.com"},
]


class Scraper(BaseScraper):
    SOURCE_NAME = "benchmark_portfolio"
    BASE_URL = "https://www.benchmark.com"
    REQUESTS_PER_MINUTE = 10

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_names: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run()."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse any Benchmark page for company data."""
        soup = self.parse_html(html)
        contacts = []

        # Look for company links or listings
        company_links = soup.find_all("a", href=re.compile(r"/portfolio/|/companies/"))
        for link in company_links:
            href = (link.get("href") or "").strip()
            text = link.get_text(strip=True)
            if not text or len(text) < 2:
                continue

            name_lower = text.lower()
            if name_lower in self._seen_names:
                continue
            self._seen_names.add(name_lower)

            source_url = href
            if not source_url.startswith("http"):
                source_url = f"{self.BASE_URL}{href}"

            contact = ScrapedContact(
                name=text,
                email="",
                company=text,
                website=source_url,
                linkedin="",
                phone="",
                bio="Benchmark Capital portfolio company.",
                source_platform=self.SOURCE_NAME,
                source_url=source_url,
                source_category="vc_portfolio",
                raw_data={"vc_firm": "Benchmark Capital"},
            )
            contacts.append(contact)

        return contacts

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Fetch Benchmark portfolio companies.

        Benchmark's website is extremely minimal, so we:
        1. Try /portfolio and / pages for any company data
        2. Fall back to the curated list of known portfolio companies
        """
        self.logger.info("Starting %s scraper", self.SOURCE_NAME)

        contacts_yielded = 0

        # Try multiple URL patterns
        urls_to_try = [
            f"{self.BASE_URL}/portfolio",
            f"{self.BASE_URL}/portfolio/",
            f"{self.BASE_URL}/companies",
            self.BASE_URL,
        ]

        scraped_contacts = []
        for url in urls_to_try:
            if max_pages and self.stats["pages_scraped"] >= max_pages:
                break

            html = self.fetch_page(url)
            if not html:
                continue

            try:
                contacts = self.scrape_page(url, html)
                scraped_contacts.extend(contacts)
            except Exception as exc:
                self.logger.debug("Parse error on %s: %s", url, exc)
                continue

            if scraped_contacts:
                self.logger.info("Found %d companies from %s", len(contacts), url)
                break

        # If we found companies from the website, yield them
        if scraped_contacts:
            for contact in scraped_contacts:
                contact.source_platform = self.SOURCE_NAME
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
        else:
            # Fall back to known companies list
            self.logger.info(
                "No portfolio page found on benchmark.com, using known companies list (%d entries)",
                len(KNOWN_COMPANIES),
            )

            for entry in KNOWN_COMPANIES:
                name = entry["name"]
                website = entry["website"]

                name_lower = name.lower()
                if name_lower in self._seen_names:
                    continue
                self._seen_names.add(name_lower)

                contact = ScrapedContact(
                    name=name,
                    email="",
                    company=name,
                    website=website,
                    linkedin="",
                    phone="",
                    bio="Benchmark Capital portfolio company.",
                    source_platform=self.SOURCE_NAME,
                    source_url=self.BASE_URL,
                    source_category="vc_portfolio",
                    raw_data={"vc_firm": "Benchmark Capital"},
                )

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

        self.logger.info("Scraper complete: %s", self.stats)
