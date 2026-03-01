"""
TEDx talks API scraper for speakers.

TED.com has a public search API that returns speaker info, talk titles,
and speaker profiles. TEDx speakers are high-quality JV candidates —
they're established experts who are actively building their platform.

Uses TED's internal API (same as their website uses).

Estimated yield: 2,000-4,000 speakers
"""

from __future__ import annotations

import json
import re
from typing import Iterator
from urllib.parse import urlencode, quote_plus

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Search queries targeting business/personal development TEDx talks
SEARCH_QUERIES = [
    "leadership", "entrepreneurship", "business",
    "innovation", "motivation", "success",
    "personal development", "coaching", "mindset",
    "productivity", "creativity", "communication",
    "marketing", "sales", "negotiation",
    "health", "wellness", "fitness",
    "education", "teaching", "learning",
    "finance", "investing", "money",
    "relationships", "happiness", "resilience",
    "meditation", "mindfulness", "psychology",
    "self improvement", "habits", "growth",
    "women leadership", "diversity", "inclusion",
    "technology", "startup", "digital",
    "social media", "storytelling", "branding",
    "purpose", "passion", "impact",
]

RESULTS_PER_PAGE = 30
MAX_PAGES_PER_QUERY = 5


class Scraper(BaseScraper):
    SOURCE_NAME = "tedx"
    BASE_URL = "https://www.ted.com"
    REQUESTS_PER_MINUTE = 10

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_speakers: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield TED search URLs."""
        # TED's internal search API
        for query in SEARCH_QUERIES:
            for page in range(1, MAX_PAGES_PER_QUERY + 1):
                offset = (page - 1) * RESULTS_PER_PAGE
                params = urlencode({
                    "q": query,
                    "page": page,
                    "per_page": RESULTS_PER_PAGE,
                })
                # Try the TED talks search page (server-rendered)
                yield f"{self.BASE_URL}/talks?sort=relevance&q={quote_plus(query)}&page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse TED search results page."""
        soup = self.parse_html(html)
        contacts = []

        # Try JSON-LD structured data first
        json_ld_contacts = self._parse_json_ld(soup)
        if json_ld_contacts:
            return json_ld_contacts

        # Try embedded JSON data (TED uses Next.js/React with __NEXT_DATA__)
        for script in soup.find_all("script", id="__NEXT_DATA__"):
            try:
                data = json.loads(script.string or "")
                contacts = self._parse_next_data(data)
                if contacts:
                    return contacts
            except (json.JSONDecodeError, TypeError):
                pass

        # Fallback: parse HTML talk cards
        for card in soup.find_all(["div", "article"], class_=re.compile(r"talk|result|media", re.I)):
            contact = self._parse_talk_card(card)
            if contact:
                contacts.append(contact)

        # Secondary fallback: look for speaker links
        if not contacts:
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/speakers/" in href or "/talks/" in href:
                    text = a.get_text(strip=True)
                    if text and len(text) > 2 and len(text) < 80:
                        if text.lower() not in self._seen_speakers:
                            self._seen_speakers.add(text.lower())
                            contacts.append(ScrapedContact(
                                name=text,
                                website=f"https://www.ted.com{href}" if href.startswith("/") else href,
                                bio="TED/TEDx speaker",
                                source_category="speakers",
                            ))

        return contacts

    def _parse_json_ld(self, soup) -> list[ScrapedContact]:
        """Extract speaker data from JSON-LD."""
        contacts = []
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    # VideoObject or similar
                    creator = item.get("creator") or item.get("author")
                    if isinstance(creator, dict):
                        name = creator.get("name", "")
                        if name and name.lower() not in self._seen_speakers:
                            self._seen_speakers.add(name.lower())
                            contacts.append(ScrapedContact(
                                name=name,
                                website=creator.get("url", ""),
                                bio=f"TED/TEDx speaker | Talk: {item.get('name', '')}",
                                source_category="speakers",
                            ))
            except (json.JSONDecodeError, TypeError):
                continue
        return contacts

    def _parse_next_data(self, data: dict) -> list[ScrapedContact]:
        """Parse Next.js __NEXT_DATA__ for talk/speaker info."""
        contacts = []

        def _walk(obj, depth=0):
            if depth > 10:
                return
            if isinstance(obj, dict):
                # Look for speaker-like objects
                name = obj.get("speaker_name") or obj.get("presenterDisplayName") or obj.get("speaker")
                if name and isinstance(name, str) and name.lower() not in self._seen_speakers:
                    self._seen_speakers.add(name.lower())
                    talk_title = obj.get("title") or obj.get("talk_title") or ""
                    slug = obj.get("slug") or obj.get("speaker_slug") or ""
                    website = f"https://www.ted.com/speakers/{slug}" if slug else ""
                    contacts.append(ScrapedContact(
                        name=name,
                        website=website,
                        bio=f"TED/TEDx speaker | Talk: {talk_title}" if talk_title else "TED/TEDx speaker",
                        source_category="speakers",
                    ))
                for v in obj.values():
                    _walk(v, depth + 1)
            elif isinstance(obj, list):
                for v in obj:
                    _walk(v, depth + 1)

        _walk(data)
        return contacts

    def _parse_talk_card(self, card) -> ScrapedContact | None:
        """Parse a talk card HTML element."""
        # Speaker name
        speaker_el = card.find(class_=re.compile(r"speaker|presenter|author", re.I))
        name = speaker_el.get_text(strip=True) if speaker_el else ""

        if not name:
            # Try meta or data attributes
            for a in card.find_all("a", href=True):
                if "/speakers/" in a["href"]:
                    name = a.get_text(strip=True)
                    break

        if not name or len(name) < 2 or name.lower() in self._seen_speakers:
            return None
        self._seen_speakers.add(name.lower())

        # Talk title
        title_el = card.find(["h3", "h4"]) or card.find(class_=re.compile(r"title|heading", re.I))
        title = title_el.get_text(strip=True) if title_el else ""

        # Speaker profile URL
        website = ""
        for a in card.find_all("a", href=True):
            href = a["href"]
            if "/speakers/" in href:
                website = f"https://www.ted.com{href}" if href.startswith("/") else href
                break
            elif "/talks/" in href:
                website = f"https://www.ted.com{href}" if href.startswith("/") else href

        return ScrapedContact(
            name=name,
            website=website,
            bio=f"TED/TEDx speaker | Talk: {title}" if title else "TED/TEDx speaker",
            source_category="speakers",
        )
