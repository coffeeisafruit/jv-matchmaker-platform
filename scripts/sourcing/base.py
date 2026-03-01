"""
Base scraper class and standardized contact output.

All scrapers inherit from BaseScraper and output ScrapedContact objects
that map directly to the ingest_contacts() dict format.
"""

from __future__ import annotations

import logging
import os
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterator, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ---------------------------------------------------------------------------
# Regex patterns (reused from matching/enrichment/contact_scraper.py)
# ---------------------------------------------------------------------------

EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
)
LINKEDIN_RE = re.compile(
    r"https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9_\-]+/?",
)
WEBSITE_RE = re.compile(
    r"https?://[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}(?:/[^\s\"'<>]*)?",
)

# Common junk emails to skip
JUNK_EMAIL_DOMAINS = {
    "example.com", "sentry.io", "wixpress.com", "wordpress.com",
    "squarespace.com", "mailchimp.com", "gravatar.com",
}


# ---------------------------------------------------------------------------
# ScrapedContact dataclass
# ---------------------------------------------------------------------------

@dataclass
class ScrapedContact:
    """Standardized output from any scraper.

    Maps directly to the dict format expected by
    matching.enrichment.flows.contact_ingestion.ingest_contacts().
    """

    name: str
    email: str = ""
    company: str = ""
    website: str = ""
    linkedin: str = ""
    phone: str = ""
    bio: str = ""

    # Metadata (tracking only, not sent to ingestion)
    source_platform: str = ""
    source_url: str = ""
    source_category: str = ""
    scraped_at: str = ""
    raw_data: dict = field(default_factory=dict)

    def to_ingestion_dict(self) -> dict:
        """Convert to the dict format expected by ingest_contacts()."""
        return {
            "name": self.name.strip(),
            "email": self.email.strip() or None,
            "company": self.company.strip() or None,
            "website": self.website.strip() or None,
            "linkedin": self.linkedin.strip() or None,
            "phone": self.phone.strip() or None,
            "bio": (self.bio.strip()[:2000] if self.bio else None),
        }

    def is_valid(self) -> bool:
        """Must have a name and at least one contact signal."""
        if not self.name or len(self.name.strip()) < 2:
            return False
        if len(self.name.strip()) > 200:
            return False
        return bool(self.email or self.website or self.linkedin)

    def clean_email(self) -> str:
        """Return email only if it looks real."""
        if not self.email:
            return ""
        email = self.email.strip().lower()
        domain = email.split("@")[-1] if "@" in email else ""
        if domain in JUNK_EMAIL_DOMAINS:
            return ""
        if not EMAIL_RE.fullmatch(email):
            return ""
        return email


# ---------------------------------------------------------------------------
# BaseScraper ABC
# ---------------------------------------------------------------------------

class BaseScraper(ABC):
    """Abstract base for all directory/platform scrapers.

    Subclasses must implement:
      - generate_urls(**kwargs) -> Iterator[str]
      - scrape_page(url, html) -> list[ScrapedContact]

    The base class provides:
      - HTTP session with retries and browser-like headers
      - BeautifulSoup parsing helper
      - Rate limiting integration
      - Stats tracking
    """

    SOURCE_NAME: str = ""
    BASE_URL: str = ""
    REQUESTS_PER_MINUTE: int = 10
    RESPECT_ROBOTS_TXT: bool = False  # Disabled — all sources are public directories

    def __init__(self, rate_limiter=None):
        self.session = self._build_session()
        self.rate_limiter = rate_limiter
        self.logger = logging.getLogger(f"sourcing.{self.SOURCE_NAME}")
        self.stats = {
            "pages_scraped": 0,
            "contacts_found": 0,
            "contacts_valid": 0,
            "errors": 0,
        }

    def _build_session(self) -> requests.Session:
        """Build requests session with retry and browser-like headers."""
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,"
                "application/xml;q=0.9,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        })
        return session

    def fetch_page(self, url: str, timeout: int = 30) -> Optional[str]:
        """Fetch URL with rate limiting. Returns HTML or None."""
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)
        try:
            resp = self.session.get(url, timeout=timeout)
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1
            return resp.text
        except requests.RequestException as exc:
            self.logger.warning("Fetch failed for %s: %s", url, exc)
            self.stats["errors"] += 1
            return None

    def fetch_json(self, url: str, timeout: int = 30) -> Optional[dict]:
        """Fetch URL expecting JSON response."""
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)
        try:
            resp = self.session.get(url, timeout=timeout)
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            self.logger.warning("JSON fetch failed for %s: %s", url, exc)
            self.stats["errors"] += 1
            return None

    @staticmethod
    def parse_html(html: str):
        """Return BeautifulSoup object."""
        from bs4 import BeautifulSoup
        return BeautifulSoup(html, "html.parser")

    @staticmethod
    def extract_emails(text: str) -> list[str]:
        """Extract email addresses from text."""
        return list(set(EMAIL_RE.findall(text)))

    @staticmethod
    def extract_linkedin(text: str) -> str:
        """Extract first LinkedIn profile URL from text."""
        matches = LINKEDIN_RE.findall(text)
        return matches[0] if matches else ""

    @abstractmethod
    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield URLs to scrape (categories, pages, etc.)."""
        ...

    @abstractmethod
    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a single page and return extracted contacts."""
        ...

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Main loop: generate URLs -> fetch -> parse -> yield contacts.

        Yields ScrapedContact objects for memory efficiency.
        Supports resuming from a checkpoint dict.
        """
        pages_done = 0
        contacts_yielded = 0
        start_from = (checkpoint or {}).get("last_url")
        past_checkpoint = start_from is None

        self.logger.info(
            "Starting %s scraper (max_pages=%s, checkpoint=%s)",
            self.SOURCE_NAME, max_pages or "unlimited", start_from or "none",
        )

        for url in self.generate_urls():
            if not past_checkpoint:
                if url == start_from:
                    past_checkpoint = True
                continue

            # Respect robots.txt
            if self.RESPECT_ROBOTS_TXT and self.rate_limiter:
                if not self.rate_limiter.is_allowed(url):
                    self.logger.debug("Blocked by robots.txt: %s", url)
                    continue

            html = self.fetch_page(url)
            if not html:
                continue

            try:
                contacts = self.scrape_page(url, html)
            except Exception as exc:
                self.logger.error("Parse error on %s: %s", url, exc)
                self.stats["errors"] += 1
                continue

            for contact in contacts:
                contact.source_platform = self.SOURCE_NAME
                contact.source_url = url
                contact.scraped_at = datetime.now().isoformat()
                # Clean email
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
                    "Progress: %d pages, %d valid contacts",
                    pages_done, self.stats["contacts_valid"],
                )

            if max_pages and pages_done >= max_pages:
                self.logger.info("Reached max_pages=%d", max_pages)
                break

        self.logger.info("Scraper complete: %s", self.stats)
