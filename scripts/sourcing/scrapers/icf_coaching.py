"""
ICF (International Coaching Federation) coach directory scraper.

ICF has 50,000+ credentialed coaches globally. Their "Find a Coach"
directory at coachingfederation.org is publicly searchable.

The directory uses a search form that returns paginated results.
We search by credential type and specialty.

Estimated yield: 2,000-3,000 coaches
"""

from __future__ import annotations

import re
import json
from typing import Iterator
from urllib.parse import urljoin, urlencode

from scripts.sourcing.base import BaseScraper, ScrapedContact


# ICF credential types
CREDENTIALS = ["ACC", "PCC", "MCC"]  # Associate, Professional, Master

# Search specialties
SPECIALTIES = [
    "Business",
    "Executive",
    "Leadership",
    "Career",
    "Life",
    "Health",
    "Wellness",
    "Personal Development",
    "Small Business",
    "Entrepreneurship",
    "Team",
    "Performance",
    "Communication",
    "Relationship",
    "Financial",
    "Transition",
    "Women",
    "Mindfulness",
]

# Countries with most English-speaking coaches
COUNTRIES = [
    "United States",
    "Canada",
    "United Kingdom",
    "Australia",
    "India",
]

MAX_PAGES = 40


class Scraper(BaseScraper):
    SOURCE_NAME = "icf_coaching"
    BASE_URL = "https://apps.coachingfederation.org"
    REQUESTS_PER_MINUTE = 5  # Conservative for .org site

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_names: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield search result URLs for different credential/specialty combos."""
        # The ICF directory often has a JSON API or search form
        # We'll try common search URL patterns
        for country in COUNTRIES:
            for specialty in SPECIALTIES:
                for page in range(1, MAX_PAGES + 1):
                    params = urlencode({
                        "Country": country,
                        "Specialty": specialty,
                        "Page": page,
                    })
                    yield f"{self.BASE_URL}/eweb/CCFDynamicPage.aspx?{params}"

        # Also try the newer API-based directory if available
        for country in COUNTRIES:
            for specialty in SPECIALTIES:
                for page in range(1, MAX_PAGES + 1):
                    params = urlencode({
                        "country": country,
                        "specialty": specialty,
                        "page": page,
                        "pageSize": 50,
                    })
                    yield f"https://www.coachingfederation.org/find-a-coach?{params}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse ICF directory search results."""
        soup = self.parse_html(html)
        contacts = []

        # Try to find coach cards/entries
        # ICF directory uses various HTML structures

        # Pattern 1: Table rows
        for row in soup.find_all("tr"):
            contact = self._parse_table_row(row)
            if contact:
                contacts.append(contact)

        # Pattern 2: Card/list items
        for card in soup.find_all(class_=re.compile(r"coach|member|profile|result|card", re.I)):
            contact = self._parse_card(card)
            if contact:
                contacts.append(contact)

        # Pattern 3: JSON data embedded in page
        for script in soup.find_all("script"):
            script_text = script.string or ""
            if "coach" in script_text.lower() and "{" in script_text:
                embedded_contacts = self._parse_embedded_json(script_text)
                contacts.extend(embedded_contacts)

        return contacts

    def _parse_table_row(self, row) -> ScrapedContact | None:
        """Parse a table row for coach info."""
        cells = row.find_all("td")
        if len(cells) < 2:
            return None

        # Extract text
        texts = [c.get_text(strip=True) for c in cells]
        full_text = " ".join(texts)

        # Look for name (usually first cell or a link)
        name = ""
        for a in row.find_all("a", href=True):
            text = a.get_text(strip=True)
            if text and len(text) > 2 and len(text) < 80:
                name = text
                break

        if not name and texts:
            name = texts[0]

        if not name or len(name) < 2 or name.lower() in self._seen_names:
            return None
        self._seen_names.add(name.lower())

        # Website and LinkedIn from links
        website = ""
        linkedin = ""
        for a in row.find_all("a", href=True):
            href = a["href"]
            href_lower = href.lower()
            if "linkedin.com/in/" in href_lower:
                linkedin = href
            elif href.startswith("http") and "coachingfederation" not in href_lower:
                website = href

        # Bio from remaining cell text
        bio_parts = [t for t in texts[1:] if t and len(t) > 5]
        bio = " | ".join(bio_parts[:3])

        emails = self.extract_emails(full_text)

        return ScrapedContact(
            name=name,
            email=emails[0] if emails else "",
            website=website,
            linkedin=linkedin,
            bio=f"ICF Credentialed Coach | {bio}" if bio else "ICF Credentialed Coach",
            source_category="coaching",
        )

    def _parse_card(self, card) -> ScrapedContact | None:
        """Parse a card element for coach info."""
        # Name
        name = ""
        name_el = card.find(["h2", "h3", "h4"]) or card.find(class_=re.compile(r"name|title", re.I))
        if name_el:
            name = name_el.get_text(strip=True)

        if not name:
            # First link text
            a = card.find("a")
            if a:
                name = a.get_text(strip=True)

        if not name or len(name) < 2 or name.lower() in self._seen_names:
            return None
        self._seen_names.add(name.lower())

        # Credential
        credential = ""
        for text in card.stripped_strings:
            if text.upper() in {"ACC", "PCC", "MCC"}:
                credential = text.upper()
                break

        # Specialty / bio
        bio = ""
        desc_el = card.find(class_=re.compile(r"description|bio|specialty|focus|about", re.I))
        if desc_el:
            bio = desc_el.get_text(strip=True)[:500]

        if credential:
            bio = f"ICF {credential} | {bio}" if bio else f"ICF {credential} Credentialed Coach"
        else:
            bio = f"ICF Credentialed Coach | {bio}" if bio else "ICF Credentialed Coach"

        # Links
        website = ""
        linkedin = ""
        for a in card.find_all("a", href=True):
            href = a["href"]
            href_lower = href.lower()
            if "linkedin.com/in/" in href_lower:
                linkedin = href
            elif href.startswith("http") and "coachingfederation" not in href_lower:
                website = href

        full_text = card.get_text()
        emails = self.extract_emails(full_text)

        return ScrapedContact(
            name=name,
            email=emails[0] if emails else "",
            website=website,
            linkedin=linkedin,
            bio=bio,
            source_category="coaching",
        )

    def _parse_embedded_json(self, script_text: str) -> list[ScrapedContact]:
        """Try to extract coach data from embedded JSON."""
        contacts = []
        # Look for JSON arrays/objects containing coach data
        json_matches = re.findall(r'\[{.*?}\]', script_text, re.DOTALL)
        for match_str in json_matches:
            try:
                data = json.loads(match_str)
                if not isinstance(data, list):
                    continue
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    name = (
                        item.get("name")
                        or item.get("fullName")
                        or f"{item.get('firstName', '')} {item.get('lastName', '')}".strip()
                    )
                    if not name or name.lower() in self._seen_names:
                        continue
                    self._seen_names.add(name.lower())

                    contacts.append(ScrapedContact(
                        name=name,
                        email=item.get("email", ""),
                        website=item.get("website", item.get("url", "")),
                        linkedin=item.get("linkedin", ""),
                        bio=f"ICF Coach | {item.get('specialty', '')}" if item.get("specialty") else "ICF Credentialed Coach",
                        source_category="coaching",
                    ))
            except (json.JSONDecodeError, TypeError):
                continue

        return contacts
