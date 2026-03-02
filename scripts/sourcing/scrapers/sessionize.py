"""
Sessionize speakers directory scraper.

Sessionize is a conference session management platform with 74K+
speaker profiles publicly browsable. The speakers directory at
sessionize.com/speakers-directory has paginated, server-rendered HTML.

Each speaker profile includes name, bio, tagline, and social links
(Twitter, blog, company website, LinkedIn).

Estimated yield: 5,000-15,000 unique speakers
"""

from __future__ import annotations

import json
import re
from typing import Iterator
from urllib.parse import urljoin, urlencode

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Search terms to use with Sessionize's search
SEARCH_QUERIES = [
    # Business / leadership
    "business", "leadership", "management", "strategy",
    "entrepreneur", "startup", "innovation", "digital transformation",
    "agile", "product management", "marketing",
    # Personal development / coaching
    "coaching", "personal development", "motivation",
    "mindset", "wellness", "health", "productivity",
    "career development", "mentoring",
    # Technology (many tech speakers also do coaching/consulting)
    "artificial intelligence", "machine learning", "data science",
    "cloud", "DevOps", "cybersecurity", "blockchain",
    # Soft skills
    "communication", "public speaking", "storytelling",
    "team building", "diversity inclusion",
    "remote work", "collaboration",
    # Education
    "education", "training", "learning",
    "workshop", "facilitation",
]

MAX_PAGES_PER_QUERY = 10


class Scraper(BaseScraper):
    SOURCE_NAME = "sessionize"
    BASE_URL = "https://sessionize.com"
    REQUESTS_PER_MINUTE = 8
    TYPICAL_ROLES = ["Thought Leader", "Educator"]
    TYPICAL_NICHES = ["speaking", "corporate_training", "saas_software"]
    TYPICAL_OFFERINGS = ["speaking", "workshops", "training"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_names: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield Sessionize speaker search URLs."""
        for query in SEARCH_QUERIES:
            for page in range(1, MAX_PAGES_PER_QUERY + 1):
                yield f"{self.BASE_URL}/speakers-directory?q={query}&page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Sessionize speakers directory page."""
        soup = self.parse_html(html)
        contacts = []

        # Sessionize uses div.c-entry.c-entry--speaker for speaker cards
        speaker_cards = soup.find_all("div", class_="c-entry--speaker")

        for card in speaker_cards:
            contact = self._parse_speaker_card(card)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_speaker_card(self, card) -> ScrapedContact | None:
        """Parse a Sessionize c-entry--speaker card.

        Structure:
          div.c-entry--speaker
            div.c-entry__intro
              h3.c-entry__title > a[href="/slug"] > "Name"
              p.c-entry__tagline > "Title at Company"
            ul.c-entry__meta
              li > span.c-entry__meta-value > "Location"
              li > span.c-entry__meta-value > "X sessions"
            div.c-entry__description > p > "Bio text"
        """
        # Name from h3.c-entry__title > a
        name = ""
        profile_slug = ""
        title_el = card.find("h3", class_="c-entry__title")
        if title_el:
            link = title_el.find("a")
            if link:
                name = link.get_text(strip=True)
                href = link.get("href", "")
                if href:
                    profile_slug = href

        # Remove "Favorite" button text that may leak into name
        if "Favorite" in name:
            name = name.replace("Favorite", "").strip()

        if not name or len(name) < 3 or len(name) > 100:
            return None

        name_key = name.lower().strip()
        if name_key in self._seen_names:
            return None
        self._seen_names.add(name_key)

        # Tagline (title/company)
        tagline = ""
        tagline_el = card.find("p", class_="c-entry__tagline")
        if tagline_el:
            tagline = tagline_el.get_text(strip=True)

        # Bio from description
        bio = ""
        desc_el = card.find("div", class_="c-entry__description")
        if desc_el:
            bio = desc_el.get_text(strip=True)[:500]

        if not bio and tagline:
            bio = tagline
        elif not bio:
            bio = "Conference speaker"

        # Profile URL
        profile_url = ""
        if profile_slug:
            profile_url = f"{self.BASE_URL}{profile_slug}" if profile_slug.startswith("/") else profile_slug

        return ScrapedContact(
            name=name,
            company=tagline[:100] if tagline else "",
            website=profile_url,
            bio=bio,
            source_category="speaker",
        )

    def _parse_json_ld_person(self, data: dict) -> ScrapedContact | None:
        """Parse a JSON-LD Person object."""
        if not isinstance(data, dict):
            return None
        if data.get("@type") not in ("Person", "Speaker"):
            return None

        name = data.get("name", "")
        if not name or len(name) < 3:
            return None

        name_key = name.lower().strip()
        if name_key in self._seen_names:
            return None
        self._seen_names.add(name_key)

        website = data.get("url", "") or data.get("sameAs", "")
        if isinstance(website, list):
            website = website[0] if website else ""
        bio = data.get("description", "")[:500] or "Conference speaker"

        return ScrapedContact(
            name=name,
            website=website,
            bio=bio,
            source_category="speaker",
        )

    def _parse_next_data(self, soup) -> list[ScrapedContact]:
        """Try to extract speaker data from __NEXT_DATA__ script tag."""
        contacts = []
        script = soup.find("script", id="__NEXT_DATA__")
        if not script or not script.string:
            return contacts

        try:
            data = json.loads(script.string)
            # Navigate common Next.js data structures
            props = data.get("props", {}).get("pageProps", {})
            speakers = props.get("speakers", []) or props.get("items", []) or props.get("results", [])

            for speaker in speakers:
                if not isinstance(speaker, dict):
                    continue
                name = speaker.get("name", "") or speaker.get("fullName", "")
                if not name or len(name) < 3:
                    continue

                name_key = name.lower().strip()
                if name_key in self._seen_names:
                    continue
                self._seen_names.add(name_key)

                bio = speaker.get("bio", "") or speaker.get("tagLine", "") or speaker.get("description", "")
                website = speaker.get("website", "") or speaker.get("url", "")
                linkedin = ""

                # Check social links
                for link in speaker.get("links", []) or speaker.get("socialLinks", []):
                    if isinstance(link, dict):
                        link_url = link.get("url", "") or link.get("href", "")
                        if "linkedin.com/in/" in link_url.lower():
                            linkedin = link_url
                        elif not website and link_url.startswith("http"):
                            website = link_url
                    elif isinstance(link, str):
                        if "linkedin.com/in/" in link.lower():
                            linkedin = link
                        elif not website and link.startswith("http"):
                            website = link

                contacts.append(ScrapedContact(
                    name=name,
                    website=website,
                    linkedin=linkedin,
                    bio=bio[:500] if bio else "Conference speaker",
                    source_category="speaker",
                ))

        except (json.JSONDecodeError, TypeError, KeyError):
            pass

        return contacts
