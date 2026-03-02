"""
Upwork agency directory scraper.

Upwork lists thousands of freelancer agencies with public profiles
at https://www.upwork.com/agencies/. Each agency has a profile page
with company name, services, location, and sometimes website.

Strategy: Paginate through agency search results by category and
parse the listing pages. Individual profiles may require JS rendering
so we extract as much as possible from the listing pages.

Note: Upwork may use JS rendering for some content. This scraper
targets the server-rendered portions. If blocked, mark as Tier 3.

Estimated yield: 5,000-15,000 agencies
"""

from __future__ import annotations

import json
import re
from typing import Iterator

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Upwork agency categories
CATEGORIES = [
    "web-development",
    "mobile-development",
    "design-creative",
    "writing",
    "admin-support",
    "customer-service",
    "data-science-analytics",
    "engineering-architecture",
    "it-networking",
    "sales-marketing",
    "accounting-consulting",
    "legal",
    "translation",
]

# Search by location for broader coverage
LOCATIONS = [
    "united-states",
    "united-kingdom",
    "canada",
    "australia",
    "india",
    "germany",
    "netherlands",
    "france",
    "brazil",
    "philippines",
    "ukraine",
    "poland",
    "argentina",
    "pakistan",
    "egypt",
]

MAX_PAGES = 20


class Scraper(BaseScraper):
    SOURCE_NAME = "upwork_agencies"
    BASE_URL = "https://www.upwork.com"
    REQUESTS_PER_MINUTE = 4  # Careful with Upwork rate limits

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_slugs: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield agency listing page URLs by category and location."""
        # Main agency listing
        yield f"{self.BASE_URL}/agencies/"

        # By category
        for category in CATEGORIES:
            for page in range(1, MAX_PAGES + 1):
                if page == 1:
                    yield f"{self.BASE_URL}/agencies/{category}/"
                else:
                    yield f"{self.BASE_URL}/agencies/{category}/?page={page}"

        # By location for wider coverage
        for location in LOCATIONS:
            for page in range(1, 10):
                if page == 1:
                    yield f"{self.BASE_URL}/agencies/?loc={location}"
                else:
                    yield f"{self.BASE_URL}/agencies/?loc={location}&page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse an Upwork agency listing page."""
        soup = self.parse_html(html)
        contacts = []

        # Try to find JSON-LD structured data first
        for script in soup.find_all("script", type="application/ld+json"):
            script_text = script.string
            if script_text:
                try:
                    data = json.loads(script_text)
                    if isinstance(data, list):
                        for item in data:
                            contact = self._parse_jsonld(item, url)
                            if contact:
                                contacts.append(contact)
                    elif isinstance(data, dict):
                        contact = self._parse_jsonld(data, url)
                        if contact:
                            contacts.append(contact)
                except (json.JSONDecodeError, KeyError):
                    pass

        if contacts:
            return contacts

        # Parse agency cards from listing pages
        # Upwork uses various card patterns
        for card in soup.find_all(attrs={"data-test": re.compile(r"agency", re.I)}):
            contact = self._parse_card(card, url)
            if contact:
                contacts.append(contact)

        # Fallback: look for agency profile links and extract from cards
        if not contacts:
            for card in soup.find_all(class_=re.compile(
                r"agency|company|profile|listing|card|result", re.I
            )):
                contact = self._parse_card(card, url)
                if contact:
                    contacts.append(contact)

        # Also look for agency profile links to fetch
        profile_links = set()
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if re.match(r"/agencies/~[a-zA-Z0-9]+/?$", href):
                full_url = f"{self.BASE_URL}{href}"
                slug = href.rstrip("/").split("/")[-1]
                if slug not in self._seen_slugs:
                    self._seen_slugs.add(slug)
                    profile_links.add(full_url)

        for profile_url in profile_links:
            profile_html = self.fetch_page(profile_url)
            if profile_html:
                contact = self._parse_profile(profile_url, profile_html)
                if contact:
                    contacts.append(contact)

        return contacts

    def _parse_jsonld(self, data: dict, source_url: str) -> ScrapedContact | None:
        """Parse JSON-LD structured data."""
        if data.get("@type") not in ("Organization", "LocalBusiness", "ProfessionalService"):
            return None

        name = (data.get("name") or "").strip()
        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_slugs:
            return None
        self._seen_slugs.add(name_key)

        website = (data.get("url") or "").strip()
        description = (data.get("description") or "").strip()[:500]

        address = data.get("address", {})
        location = ""
        if isinstance(address, dict):
            parts = [
                (address.get("addressLocality") or "").strip(),
                (address.get("addressRegion") or "").strip(),
                (address.get("addressCountry") or "").strip(),
            ]
            location = ", ".join(p for p in parts if p)

        bio_parts = ["Upwork Agency"]
        if location:
            bio_parts.append(location)
        if description:
            bio_parts.append(description[:300])
        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=name,
            email="",
            company=name,
            website=website,
            linkedin="",
            phone="",
            bio=bio,
            source_url=source_url,
            source_category="agency",
        )

    def _parse_card(self, card, source_url: str) -> ScrapedContact | None:
        """Parse an agency card element."""
        name = ""

        for tag in ["h3", "h2", "h4", "strong"]:
            el = card.find(tag)
            if el:
                name = el.get_text(strip=True)
                if name and len(name) > 1 and len(name) < 150:
                    break
                name = ""

        if not name:
            # Try data attributes
            name = (card.get("data-agency-name") or "").strip()

        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_slugs:
            return None
        self._seen_slugs.add(name_key)

        # Extract profile URL
        profile_url = ""
        for a in card.find_all("a", href=True):
            href = a.get("href", "")
            if "/agencies/" in href:
                profile_url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                break

        # Bio from card text
        card_text = card.get_text(separator=" | ", strip=True)
        bio = f"Upwork Agency | {card_text[:400]}" if card_text else "Upwork Agency"

        return ScrapedContact(
            name=name,
            email="",
            company=name,
            website=profile_url or source_url,
            linkedin="",
            phone="",
            bio=bio,
            source_url=source_url,
            source_category="agency",
        )

    def _parse_profile(self, url: str, html: str) -> ScrapedContact | None:
        """Parse a full Upwork agency profile page."""
        soup = self.parse_html(html)

        name = ""
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        if not name:
            og = soup.find("meta", property="og:title")
            if og:
                name = (og.get("content") or "").split("|")[0].split("-")[0].strip()

        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_slugs:
            return None
        self._seen_slugs.add(name_key)

        bio = ""
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            bio = (meta_desc.get("content") or "")[:500]

        # External website link
        website = ""
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if (href.startswith("http")
                    and "upwork.com" not in href.lower()
                    and "linkedin.com" not in href.lower()
                    and "facebook.com" not in href.lower()
                    and "twitter.com" not in href.lower()):
                website = href
                break

        linkedin = self.extract_linkedin(html)
        emails = self.extract_emails(html)
        email = emails[0] if emails else ""

        bio_parts = ["Upwork Agency"]
        if bio:
            bio_parts.append(bio[:400])
        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=name,
            email=email,
            company=name,
            website=website or url,
            linkedin=linkedin,
            phone="",
            bio=bio,
            source_url=url,
            source_category="agency",
        )
