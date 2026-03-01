"""
Virtual summit speaker list scraper.

This is a meta-scraper that finds and scrapes virtual summit speaker
pages. Summits typically list 20-50 speakers with headshots, bios,
and website links — high-quality JV candidates.

Discovery method: DuckDuckGo search for summit speaker pages,
then parse each one for speaker profiles.

Estimated yield: 500-1,500 speakers (highly qualified)
"""

from __future__ import annotations

import re
from typing import Iterator
from urllib.parse import urljoin, quote_plus

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Search queries to find summit speaker pages
SUMMIT_SEARCH_QUERIES = [
    "virtual summit speakers business coaching",
    "online summit speakers personal development",
    "virtual summit speakers health wellness",
    "business summit speaker lineup",
    "coaching summit speakers",
    "entrepreneurship summit speakers",
    "marketing summit speaker list",
    "leadership summit speakers",
    "womens business summit speakers",
    "digital marketing summit speakers",
    "mindset summit speakers",
    "wellness summit speaker lineup",
    "course creator summit speakers",
    "affiliate summit speakers",
    "podcast summit speakers",
    "author summit speakers",
    "transformation summit speakers",
    "success summit speakers lineup",
    "growth summit speakers",
    "expert summit speakers",
]

# Known summit domains to scrape (curated list)
KNOWN_SUMMIT_URLS = [
    # Add summit speaker page URLs as you discover them
    # These are placeholder patterns — real URLs would be added manually
    # or discovered via search
]


class Scraper(BaseScraper):
    SOURCE_NAME = "summit_speakers"
    BASE_URL = "https://duckduckgo.com"
    REQUESTS_PER_MINUTE = 4
    RESPECT_ROBOTS_TXT = False  # We're making search queries, not crawling

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_speakers: set[str] = set()
        self._summit_urls: list[str] = list(KNOWN_SUMMIT_URLS)

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield summit speaker page URLs found via search."""
        # First yield known summit URLs
        for url in self._summit_urls:
            yield url

        # Then use DuckDuckGo HTML search to discover more
        for query in SUMMIT_SEARCH_QUERIES:
            encoded = quote_plus(query)
            yield f"https://html.duckduckgo.com/html/?q={encoded}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse either a search results page or a summit speaker page."""
        if "duckduckgo.com" in url:
            return self._parse_search_results(url, html)
        else:
            return self._parse_summit_page(url, html)

    def _parse_search_results(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse DuckDuckGo results to find summit pages, then scrape them."""
        soup = self.parse_html(html)
        contacts = []

        # Find result links
        summit_urls = []
        for a in soup.find_all("a", class_="result__a", href=True):
            href = a["href"]
            if href.startswith("http"):
                # Check if it looks like a summit speaker page
                href_lower = href.lower()
                text = a.get_text(strip=True).lower()
                if any(kw in href_lower or kw in text for kw in [
                    "summit", "speaker", "lineup", "presenter",
                ]):
                    summit_urls.append(href)

        # Also check regular links
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if not href.startswith("http") or "duckduckgo" in href:
                continue
            text = a.get_text(strip=True).lower()
            if "speaker" in text and "summit" in text:
                summit_urls.append(href)

        # Scrape discovered summit pages
        for summit_url in summit_urls[:10]:  # Limit per search to avoid overloading
            page_html = self.fetch_page(summit_url)
            if not page_html:
                continue
            page_contacts = self._parse_summit_page(summit_url, page_html)
            contacts.extend(page_contacts)

        return contacts

    def _parse_summit_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a summit speaker page for individual speaker info."""
        soup = self.parse_html(html)
        contacts = []

        # Summit speaker pages typically have speaker cards with:
        # - Photo, name, title/bio, website link

        # Pattern 1: Speaker cards with headings
        for card in soup.find_all(class_=re.compile(r"speaker|presenter|expert|panelist|guest", re.I)):
            contact = self._extract_speaker_from_element(card, url)
            if contact:
                contacts.append(contact)

        # Pattern 2: Sections with headings + paragraphs
        if not contacts:
            for heading in soup.find_all(["h2", "h3", "h4"]):
                heading_text = heading.get_text(strip=True)
                # Skip navigation/generic headings
                if any(kw in heading_text.lower() for kw in [
                    "speaker", "presenter", "expert", "meet", "featured",
                ]):
                    # The content after this heading contains speakers
                    parent = heading.find_parent(["div", "section"])
                    if parent:
                        for child_card in parent.find_all(["div", "article", "li"]):
                            contact = self._extract_speaker_from_element(child_card, url)
                            if contact:
                                contacts.append(contact)

        # Pattern 3: Image + name pairs
        if not contacts:
            for img in soup.find_all("img"):
                alt = (img.get("alt") or "").strip()
                if alt and len(alt) > 3 and len(alt) < 60 and " " in alt:
                    # Looks like a person name in alt text
                    if alt.lower() not in self._seen_speakers:
                        parent = img.find_parent(["div", "figure", "a"])
                        if parent:
                            contact = self._extract_speaker_from_element(parent, url)
                            if contact:
                                contacts.append(contact)

        return contacts

    def _extract_speaker_from_element(self, element, page_url: str) -> ScrapedContact | None:
        """Extract a single speaker from a DOM element."""
        # Name — from heading, strong, or img alt
        name = ""
        name_el = element.find(["h2", "h3", "h4", "h5", "strong"])
        if name_el:
            name = name_el.get_text(strip=True)

        if not name:
            img = element.find("img", alt=True)
            if img:
                alt = img["alt"].strip()
                if " " in alt and len(alt) < 60:
                    name = alt

        if not name or len(name) < 3 or len(name) > 80:
            return None

        # Clean up name
        name = re.sub(r"\s*[-–|:]\s*.*$", "", name).strip()

        if name.lower() in self._seen_speakers:
            return None
        self._seen_speakers.add(name.lower())

        # Bio
        bio = ""
        for p in element.find_all("p"):
            text = p.get_text(strip=True)
            if text and len(text) > 20:
                bio = text[:500]
                break

        # Links
        website = ""
        linkedin = ""
        for a in element.find_all("a", href=True):
            href = a["href"]
            href_lower = href.lower()
            if "linkedin.com/in/" in href_lower and not linkedin:
                linkedin = href
            elif (
                href.startswith("http")
                and not any(domain in href_lower for domain in [
                    "facebook.com", "twitter.com", "instagram.com",
                    "youtube.com", "tiktok.com",
                ])
                and not website
            ):
                website = href

        emails = self.extract_emails(element.get_text())
        email = emails[0] if emails else ""

        return ScrapedContact(
            name=name,
            email=email,
            website=website,
            linkedin=linkedin,
            bio=bio or f"Summit speaker (from {page_url})",
            source_url=page_url,
            source_category="speakers",
        )
