"""
Psychology Today therapist/coach directory scraper.

Psychology Today has 100,000+ therapist profiles in a server-rendered
directory. Many therapists also offer coaching services and are
excellent JV candidates for wellness/personal development products.

The directory is at: https://www.psychologytoday.com/us/therapists
Profiles contain: name, specialty, website, phone, bio, approach.

Estimated yield: 5,000-10,000 profiles
"""

from __future__ import annotations

import re
from typing import Iterator
from urllib.parse import urljoin

from scripts.sourcing.base import BaseScraper, ScrapedContact


# (state_abbrev, city_slug) pairs for URL generation
# Format: /us/therapists/{state}/{city}
LOCATIONS = [
    # Major metro areas (highest density)
    ("ny", "new-york"),
    ("ca", "los-angeles"),
    ("il", "chicago"),
    ("tx", "houston"),
    ("az", "phoenix"),
    ("pa", "philadelphia"),
    ("tx", "san-antonio"),
    ("ca", "san-diego"),
    ("tx", "dallas"),
    ("tx", "austin"),
    ("ca", "san-francisco"),
    ("wa", "seattle"),
    ("co", "denver"),
    ("ma", "boston"),
    ("tn", "nashville"),
    ("or", "portland"),
    ("ga", "atlanta"),
    ("fl", "miami"),
    ("mn", "minneapolis"),
    ("nc", "charlotte"),
    ("nc", "raleigh"),
    ("fl", "tampa"),
    ("fl", "orlando"),
    ("ca", "sacramento"),
    ("nv", "las-vegas"),
    ("ca", "san-jose"),
    ("oh", "columbus"),
    ("in", "indianapolis"),
    ("mi", "detroit"),
    ("ut", "salt-lake-city"),
    # Secondary cities
    ("mo", "kansas-city"),
    ("wi", "milwaukee"),
    ("ok", "oklahoma-city"),
    ("va", "richmond"),
    ("pa", "pittsburgh"),
    ("oh", "cincinnati"),
    ("mo", "saint-louis"),
    ("oh", "cleveland"),
    ("la", "new-orleans"),
    ("hi", "honolulu"),
    ("id", "boise"),
    ("az", "tucson"),
    ("nm", "albuquerque"),
    ("ne", "omaha"),
    ("ky", "louisville"),
    ("fl", "jacksonville"),
    ("al", "birmingham"),
    ("tn", "memphis"),
    ("md", "baltimore"),
    ("dc", "washington"),
]

# Specialty filters to target coaching-adjacent therapists
SPECIALTIES = [
    "life-coaching",
    "career-counseling",
    "relationship-issues",
    "self-esteem",
    "stress",
    "anxiety",
    "life-transitions",
    "women",
    "men",
    "spirituality",
    "weight-loss",
]

MAX_PAGES = 20  # 20 results per page


class Scraper(BaseScraper):
    SOURCE_NAME = "psychology_today"
    BASE_URL = "https://www.psychologytoday.com"
    REQUESTS_PER_MINUTE = 8  # Be polite

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_names: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield paginated therapist directory URLs."""
        for state, city in LOCATIONS:
            for page in range(1, MAX_PAGES + 1):
                if page == 1:
                    yield f"{self.BASE_URL}/us/therapists/{state}/{city}"
                else:
                    yield f"{self.BASE_URL}/us/therapists/{state}/{city}?page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse therapist listing page."""
        soup = self.parse_html(html)
        contacts = []

        # Psychology Today uses div.results-row or similar for each profile
        profile_cards = soup.find_all(
            "div", class_=re.compile(r"result-row|profile-card|results-row|therapist-card", re.I)
        )

        if not profile_cards:
            # Fallback: look for profile links
            profile_cards = soup.find_all("div", class_=re.compile(r"result", re.I))

        for card in profile_cards:
            contact = self._parse_card(card)
            if contact:
                contacts.append(contact)

        # Also try parsing structured data (JSON-LD)
        if not contacts:
            contacts = self._parse_json_ld(soup)

        return contacts

    def _parse_card(self, card) -> ScrapedContact | None:
        """Parse a therapist result card (div.results-row)."""
        # Name from a.profile-title (Psychology Today's specific class)
        name = ""
        profile_link = ""
        title_el = card.find("a", class_="profile-title")
        if title_el:
            name = title_el.get_text(strip=True)
            href = title_el.get("href", "")
            if href:
                profile_link = href if href.startswith("http") else urljoin(self.BASE_URL, href)

        if not name or len(name) < 3:
            return None

        # Clean name: remove "Dr. " prefix and credentials suffix
        name = re.sub(r"^(Dr\.\s+|Prof\.\s+)", "", name)
        name = re.sub(
            r",?\s*(PhD|PsyD|LCSW|LMFT|LPC|LMHC|MA|MS|MEd|EdD|MD|NCC|BCC|CPC|ACC|PCC|MCC|LCPC|LICSW|LPCC|RN|NP|DO|CADC|CAC|LAC|LADC|CASAC|CSAT|CGP|BCBA|DBT|EMDR).*$",
            "", name, flags=re.I,
        )
        name = name.strip().rstrip(",").strip()

        # Skip non-name entries
        if len(name) < 3 or len(name) > 80:
            return None
        if name.lower() in {"view", "new york", "los angeles", "chicago", "see more"}:
            return None

        name_key = name.lower()
        if name_key in self._seen_names:
            return None
        self._seen_names.add(name_key)

        # Phone from span.results-row-phone
        phone = ""
        phone_el = card.find("span", class_="results-row-phone")
        if phone_el:
            phone = phone_el.get_text(strip=True)

        # Bio from the card's text content (after name/address)
        bio = ""
        for span in card.find_all("span"):
            text = span.get_text(strip=True)
            # Bio is usually the longest span text that isn't the name, address, or phone
            if (len(text) > 50 and text != name
                    and not text.startswith("(") and "," not in text[:10]):
                bio = text[:500]
                break

        if not bio:
            bio = "Licensed therapist/coach"

        # Website: external links (non-PT)
        website = ""
        for a in card.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True).lower()
            if "website" in text and href.startswith("http"):
                website = href
                break
            elif href.startswith("http") and "psychologytoday.com" not in href:
                website = href
                break

        return ScrapedContact(
            name=name,
            website=website or profile_link,
            phone=phone,
            bio=bio,
            source_category="therapist_coach",
        )

    def _parse_json_ld(self, soup) -> list[ScrapedContact]:
        """Try to extract therapist data from JSON-LD structured data."""
        import json
        contacts = []

        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict):
                    items = data.get("itemListElement", [data])
                else:
                    continue

                for item in items:
                    if isinstance(item, dict) and item.get("item"):
                        item = item["item"]
                    if not isinstance(item, dict):
                        continue

                    name = item.get("name", "")
                    if not name or name.lower() in self._seen_names:
                        continue
                    self._seen_names.add(name.lower())

                    contacts.append(ScrapedContact(
                        name=name,
                        website=item.get("url", ""),
                        phone=item.get("telephone", ""),
                        bio=item.get("description", "")[:500] or "Licensed therapist/coach",
                        source_category="therapist_coach",
                    ))
            except (json.JSONDecodeError, TypeError):
                continue

        return contacts
