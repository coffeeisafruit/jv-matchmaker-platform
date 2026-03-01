"""
Eventbrite event organizer scraper.

Eventbrite has public event listings with organizer profiles.
Focus on business/coaching/personal development event organizers.

Estimated yield: 1,000-2,000 organizers
"""

from __future__ import annotations

import re
from typing import Iterator
from urllib.parse import urljoin, quote_plus

from scripts.sourcing.base import BaseScraper, ScrapedContact


SEARCH_QUERIES = [
    "business coaching",
    "life coaching workshop",
    "personal development seminar",
    "entrepreneurship",
    "marketing workshop",
    "leadership training",
    "mindset workshop",
    "health wellness retreat",
    "networking event business",
    "women in business",
    "sales training",
    "digital marketing",
    "course creator workshop",
    "mastermind group",
    "speaker event",
    "consulting workshop",
    "financial coaching",
    "career coaching",
]

MAX_PAGES = 15


class Scraper(BaseScraper):
    SOURCE_NAME = "eventbrite"
    BASE_URL = "https://www.eventbrite.com"
    REQUESTS_PER_MINUTE = 6

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_organizers: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield Eventbrite search result pages."""
        for query in SEARCH_QUERIES:
            for page in range(1, MAX_PAGES + 1):
                encoded = quote_plus(query)
                yield f"{self.BASE_URL}/d/online/{encoded}/?page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Eventbrite search results for organizer info."""
        soup = self.parse_html(html)
        contacts = []

        # Find event cards
        organizer_links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/o/" in href:
                full_url = href if href.startswith("http") else urljoin(self.BASE_URL, href)
                slug = href.rstrip("/").split("/")[-1]
                if slug and slug not in self._seen_organizers:
                    self._seen_organizers.add(slug)
                    organizer_links.add(full_url)

        # Also look for organizer names in event cards
        for card in soup.find_all(class_=re.compile(r"event-card|listing|result", re.I)):
            org_el = card.find(class_=re.compile(r"organizer|host|by", re.I))
            if org_el:
                # Find link to organizer page
                org_link = org_el.find("a", href=True)
                if org_link and "/o/" in org_link["href"]:
                    href = org_link["href"]
                    full_url = href if href.startswith("http") else urljoin(self.BASE_URL, href)
                    slug = href.rstrip("/").split("/")[-1]
                    if slug and slug not in self._seen_organizers:
                        self._seen_organizers.add(slug)
                        organizer_links.add(full_url)

        # Fetch organizer pages
        for org_url in organizer_links:
            org_html = self.fetch_page(org_url)
            if not org_html:
                continue
            contact = self._parse_organizer_page(org_url, org_html)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_organizer_page(self, url: str, html: str) -> ScrapedContact | None:
        """Extract organizer details from their profile page."""
        soup = self.parse_html(html)

        # Name
        name = ""
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        if not name:
            og = soup.find("meta", property="og:title")
            if og:
                name = og.get("content", "").split("|")[0].split("-")[0].strip()

        if not name or len(name) < 2:
            return None

        # Bio
        bio = ""
        desc_el = soup.find(class_=re.compile(r"description|about|bio|organizer-description", re.I))
        if desc_el:
            bio = desc_el.get_text(strip=True)[:800]

        if not bio:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc:
                bio = meta_desc.get("content", "")[:500]

        # Event count / followers
        stats = []
        for stat_el in soup.find_all(class_=re.compile(r"stat|count|followers|events", re.I)):
            text = stat_el.get_text(strip=True)
            if text and len(text) < 50:
                stats.append(text)
        if stats:
            bio = f"{bio} | {', '.join(stats[:3])}" if bio else ", ".join(stats[:3])

        # External links
        website = ""
        linkedin = ""
        for a in soup.find_all("a", href=True):
            href = a["href"]
            href_lower = href.lower()
            if "linkedin.com/in/" in href_lower and not linkedin:
                linkedin = href
            elif (
                href.startswith("http")
                and "eventbrite.com" not in href_lower
                and "facebook.com" not in href_lower
                and "twitter.com" not in href_lower
                and "instagram.com" not in href_lower
                and not website
            ):
                website = href

        if not website:
            website = url

        emails = self.extract_emails(html)
        email = emails[0] if emails else ""

        return ScrapedContact(
            name=name,
            email=email,
            website=website,
            linkedin=linkedin,
            bio=bio,
            source_url=url,
            source_category="event_organizers",
        )
