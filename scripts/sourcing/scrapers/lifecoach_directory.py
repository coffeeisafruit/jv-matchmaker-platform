"""
Life Coach Directory (lifecoach-directory.org.uk) scraper.

UK-based life coaching directory with public coach profiles
organized by region, city, and coaching specialty.

URL patterns:
  - /region-{region}.html  (region listings)
  - /city/{city}           (city listings)
  - /service/{topic}-coaching.html (topic listings)
  - /lifecoaches/{slug}    (individual profiles)

NOTE: This site uses Cloudflare-style bot protection and may
return 403 on automated requests. The scraper uses conservative
rate limiting and rotates through different entry points.
If 403s persist, consider using crawl4ai/playwright.

Estimated yield: 500-2,000 coaches (if accessible)
"""

from __future__ import annotations

import re
from typing import Iterator
from urllib.parse import urljoin

from scripts.sourcing.base import BaseScraper, ScrapedContact


# UK regions used in the directory
REGIONS = [
    "east-anglia",
    "east-midlands",
    "london",
    "north-east",
    "north-west",
    "northern-ireland",
    "scotland",
    "south-east",
    "south-west",
    "wales",
    "west-midlands",
    "yorkshire",
    "east-london",
    "west-london",
    "north-london",
    "south-london",
    "central-london",
]

# Major UK cities
CITIES = [
    "london", "manchester", "birmingham", "leeds", "glasgow",
    "liverpool", "edinburgh", "bristol", "sheffield", "newcastle",
    "nottingham", "cardiff", "belfast", "leicester", "coventry",
    "brighton", "reading", "cambridge", "oxford", "bath",
    "york", "exeter", "norwich", "southampton", "plymouth",
    "aberdeen", "dundee", "swansea", "derby", "wolverhampton",
    "stoke-on-trent", "sunderland", "portsmouth", "luton", "preston",
]

# Coaching specialties listed on the site
SPECIALTIES = [
    "health-coaching",
    "life-coaching",
    "career-coaching",
    "business-coaching",
    "executive-coaching",
    "confidence-coaching",
    "personal-development",
    "relationship-coaching",
    "stress-management",
    "wellness-coaching",
    "mindfulness-coaching",
    "performance-coaching",
    "leadership-coaching",
    "retirement-coaching",
    "redundancy-coaching",
    "work-life-balance",
    "bereavement-coaching",
    "anxiety-coaching",
    "weight-management",
    "addiction-coaching",
    "parent-coaching",
    "dating-coaching",
    "spiritual-coaching",
    "creativity-coaching",
    "motivation-coaching",
    "interview-coaching",
]

MAX_PAGES_PER_LISTING = 20


class Scraper(BaseScraper):
    SOURCE_NAME = "lifecoach_directory"
    BASE_URL = "https://www.lifecoach-directory.org.uk"
    REQUESTS_PER_MINUTE = 4  # Very conservative for UK .org site

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_slugs: set[str] = set()
        # Add additional headers to reduce 403 likelihood
        self.session.headers.update({
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "Accept-Language": "en-GB,en;q=0.9,en-US;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        })

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield listing page URLs across regions, cities, and specialties."""
        # Strategy 1: Region pages
        for region in REGIONS:
            yield f"{self.BASE_URL}/region-{region}.html"

        # Strategy 2: City pages (more granular)
        for city in CITIES:
            yield f"{self.BASE_URL}/city/{city}"

        # Strategy 3: Specialty/service pages with pagination
        for specialty in SPECIALTIES:
            for page in range(1, MAX_PAGES_PER_LISTING + 1):
                if page == 1:
                    yield f"{self.BASE_URL}/service/{specialty}.html"
                else:
                    yield f"{self.BASE_URL}/service/{specialty}.html?page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse listing page and extract coach profiles."""
        soup = self.parse_html(html)
        contacts = []

        # Collect profile links from listing page
        profile_links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Profile URLs: /lifecoaches/{slug}
            match = re.search(r"/lifecoaches/([a-z0-9\-]+)$", href)
            if match:
                slug = match.group(1)
                if slug not in self._seen_slugs:
                    self._seen_slugs.add(slug)
                    full_url = urljoin(self.BASE_URL, href)
                    profile_links.add(full_url)

        if not profile_links:
            # Fallback: try extracting data directly from listing cards
            contacts.extend(self._parse_listing_cards(soup, url))
            return contacts

        # Fetch and parse each profile page
        for profile_url in profile_links:
            profile_html = self.fetch_page(profile_url)
            if not profile_html:
                continue
            contact = self._parse_profile(profile_url, profile_html)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_listing_cards(self, soup, url: str) -> list[ScrapedContact]:
        """Extract coach data from listing card elements."""
        contacts = []

        # Look for coach cards/listing items
        # Common patterns: div with class containing 'coach', 'listing', 'profile', 'result'
        card_selectors = [
            soup.find_all(class_=re.compile(r"coach|listing|profile|result|therapist", re.I)),
            soup.find_all("article"),
            soup.find_all("li", class_=re.compile(r"coach|listing|member", re.I)),
        ]

        for cards in card_selectors:
            for card in cards:
                contact = self._extract_card_data(card)
                if contact:
                    contacts.append(contact)
            if contacts:
                break

        return contacts

    def _extract_card_data(self, card) -> ScrapedContact | None:
        """Extract contact data from a listing card element."""
        # Name from heading or strong link
        name = ""
        name_el = card.find(["h2", "h3", "h4"]) or card.find("strong")
        if name_el:
            name = name_el.get_text(strip=True)
        if not name:
            a = card.find("a")
            if a:
                name = a.get_text(strip=True)

        if not name or len(name) < 2:
            return None

        # Clean name - remove trailing location or specialty text
        name = re.sub(r"\s*[-,|]\s*(London|Manchester|Birmingham|UK|England).*$", "", name, flags=re.I)
        name = name.strip()

        if not name or len(name) < 2:
            return None

        # Deduplicate by normalized name
        norm = name.lower().strip()
        if norm in self._seen_slugs:
            return None
        self._seen_slugs.add(norm)

        # Bio / description
        bio = ""
        desc_el = card.find(class_=re.compile(r"desc|bio|about|summary|excerpt|text", re.I))
        if desc_el:
            bio = desc_el.get_text(strip=True)[:500]
        if not bio:
            # Use all paragraph text
            paragraphs = card.find_all("p")
            bio_parts = [p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20]
            bio = " ".join(bio_parts)[:500]

        # Website and LinkedIn
        website = ""
        linkedin = ""
        for a in card.find_all("a", href=True):
            href = a["href"]
            href_lower = href.lower()
            if "linkedin.com/in/" in href_lower and not linkedin:
                linkedin = href
            elif (
                href.startswith("http")
                and "lifecoach-directory" not in href_lower
                and "facebook.com" not in href_lower
                and "twitter.com" not in href_lower
                and "instagram.com" not in href_lower
                and not website
            ):
                website = href

        # Email
        card_text = card.get_text()
        emails = self.extract_emails(card_text)
        email = emails[0] if emails else ""

        # Phone
        phone = ""
        phone_match = re.search(
            r"(?:0\d{2,4}[\s\-]?\d{3,4}[\s\-]?\d{3,4}|"
            r"\+44[\s\-]?\d{2,4}[\s\-]?\d{3,4}[\s\-]?\d{3,4})",
            card_text,
        )
        if phone_match:
            phone = phone_match.group(0).strip()

        # Location from card
        location = ""
        loc_el = card.find(class_=re.compile(r"location|address|area|region|town", re.I))
        if loc_el:
            location = loc_el.get_text(strip=True)

        if bio:
            bio = f"Life Coach (UK) | {location} | {bio}" if location else f"Life Coach (UK) | {bio}"
        else:
            bio = f"Life Coach (UK) | {location}" if location else "Life Coach (UK)"

        return ScrapedContact(
            name=name,
            email=email,
            website=website,
            linkedin=linkedin,
            phone=phone,
            bio=bio,
            source_category="coaching",
        )

    def _parse_profile(self, url: str, html: str) -> ScrapedContact | None:
        """Extract contact info from an individual coach profile page."""
        soup = self.parse_html(html)

        # Name - try h1 first
        name = ""
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        # Fallback: og:title or title tag
        if not name:
            og = soup.find("meta", property="og:title")
            if og:
                name = og.get("content", "").split("|")[0].split("-")[0].strip()
        if not name:
            title = soup.find("title")
            if title:
                name = title.get_text().split("|")[0].split("-")[0].strip()

        if not name or len(name) < 2:
            return None

        # Clean name - remove titles like "Life Coach" suffix
        name = re.sub(
            r"\s*[-,|]\s*(?:Life|Business|Executive|Career|Health|Wellness)\s+Coach.*$",
            "", name, flags=re.I,
        )
        name = re.sub(r"\s*,\s*(London|Manchester|Birmingham|UK|England).*$", "", name, flags=re.I)
        name = name.strip()

        if not name or len(name) < 2:
            return None

        # Bio - meta description or content area
        bio = ""
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            bio = meta_desc.get("content", "")[:500]

        if not bio:
            # Look for about/bio section
            for section in soup.find_all(class_=re.compile(r"about|bio|description|profile", re.I)):
                text = section.get_text(strip=True)
                if len(text) > 50:
                    bio = text[:500]
                    break

        # JSON-LD structured data
        for script in soup.find_all("script", type="application/ld+json"):
            script_text = script.string or ""
            if '"@type"' in script_text and ("Person" in script_text or "LocalBusiness" in script_text):
                try:
                    import json
                    ld_data = json.loads(script_text)
                    if not bio and ld_data.get("description"):
                        bio = ld_data["description"][:500]
                    if ld_data.get("url"):
                        pass  # Could use for website
                    if ld_data.get("telephone"):
                        phone_ld = ld_data["telephone"]
                except (json.JSONDecodeError, TypeError):
                    pass

        # Website and LinkedIn
        website = ""
        linkedin = ""
        for a in soup.find_all("a", href=True):
            href = a["href"]
            href_lower = href.lower()
            if "linkedin.com/in/" in href_lower and not linkedin:
                linkedin = href
            elif (
                href.startswith("http")
                and "lifecoach-directory" not in href_lower
                and "facebook.com" not in href_lower
                and "twitter.com" not in href_lower
                and "instagram.com" not in href_lower
                and "youtube.com" not in href_lower
                and "mailto:" not in href_lower
                and not website
            ):
                website = href

        # Email
        emails = self.extract_emails(html)
        email = emails[0] if emails else ""

        # Phone
        phone = ""
        phone_match = re.search(
            r"(?:0\d{2,4}[\s\-]?\d{3,4}[\s\-]?\d{3,4}|"
            r"\+44[\s\-]?\d{2,4}[\s\-]?\d{3,4}[\s\-]?\d{3,4})",
            html,
        )
        if phone_match:
            phone = phone_match.group(0).strip()

        # Location
        location = ""
        loc_el = soup.find(class_=re.compile(r"location|address|area", re.I))
        if loc_el:
            location = loc_el.get_text(strip=True)

        if bio:
            bio = f"Life Coach (UK) | {location} | {bio}" if location else f"Life Coach (UK) | {bio}"
        else:
            bio = f"Life Coach (UK) | {location}" if location else "Life Coach (UK)"

        return ScrapedContact(
            name=name,
            email=email,
            website=website,
            linkedin=linkedin,
            phone=phone,
            bio=bio,
            source_url=url,
            source_category="coaching",
        )
