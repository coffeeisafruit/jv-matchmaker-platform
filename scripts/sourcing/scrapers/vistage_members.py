"""
Vistage peer advisory group scraper.

Vistage is the world's largest CEO coaching and peer advisory
organization with 45,000+ members. Their website lists Chairs
(facilitators/coaches) and group information by location.

Strategy: Scrape the Vistage Chair directory at
  vistage.com/find-a-chair
which lists Vistage Chairs (executive coaches) by location.
Also scrape the general resources/events for organizational contacts.

Chairs are high-value JV targets: executive coaches who serve
CEOs and senior leaders.

Estimated yield: 500-2,000 chairs + organizational contacts
"""

from __future__ import annotations

import json
import re
from typing import Iterator

from scripts.sourcing.base import BaseScraper, ScrapedContact


# US states for geographic search
US_STATES = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California",
    "Colorado", "Connecticut", "Delaware", "Florida", "Georgia",
    "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa",
    "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland",
    "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri",
    "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey",
    "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
    "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina",
    "South Dakota", "Tennessee", "Texas", "Utah", "Vermont",
    "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming",
]

# Metro areas for zip-based searches
METRO_ZIPS = [
    "10001", "90001", "60601", "77001", "85001", "19101",
    "75201", "95101", "78701", "94101", "98101", "80201",
    "20001", "37201", "33101", "97201", "30301", "02101",
    "44101", "55401", "84101", "27601", "32801", "63101",
    "48201", "46201", "28201", "45201", "15201", "40201",
]


class Scraper(BaseScraper):
    SOURCE_NAME = "vistage_members"
    BASE_URL = "https://www.vistage.com"
    REQUESTS_PER_MINUTE = 5

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_chairs: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield Vistage directory URLs."""
        # Main find-a-chair pages
        yield f"{self.BASE_URL}/find-a-chair"
        yield f"{self.BASE_URL}/find-a-chair/"

        # State-based searches
        for state in US_STATES:
            state_slug = state.lower().replace(" ", "-")
            yield f"{self.BASE_URL}/find-a-chair/{state_slug}"
            yield f"{self.BASE_URL}/find-a-chair?state={state_slug}"

        # Zip-based searches
        for zip_code in METRO_ZIPS:
            yield f"{self.BASE_URL}/find-a-chair?zip={zip_code}"

        # Chair listing pages
        yield f"{self.BASE_URL}/chairs"
        yield f"{self.BASE_URL}/our-chairs"

        # Speakers and events (often list members/chairs)
        yield f"{self.BASE_URL}/speakers"
        yield f"{self.BASE_URL}/events"

        # Sitemap for additional discovery
        yield f"{self.BASE_URL}/sitemap.xml"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Vistage pages for chair and member data."""
        if url.endswith("sitemap.xml"):
            return self._parse_sitemap(html)

        soup = self.parse_html(html)
        contacts = []

        # Try JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            if script.string:
                try:
                    data = json.loads(script.string)
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        contact = self._parse_jsonld(item, url)
                        if contact:
                            contacts.append(contact)
                except (json.JSONDecodeError, KeyError):
                    pass

        # Try Next.js data
        next_data = soup.find("script", id="__NEXT_DATA__")
        if next_data and next_data.string:
            try:
                data = json.loads(next_data.string)
                props = data.get("props", {}).get("pageProps", {})
                chairs = (
                    props.get("chairs", [])
                    or props.get("results", [])
                    or props.get("data", {}).get("chairs", [])
                )
                for chair in chairs:
                    contact = self._parse_chair_data(chair, url)
                    if contact:
                        contacts.append(contact)
            except (json.JSONDecodeError, KeyError):
                pass

        # Search for embedded JSON data in scripts
        for script in soup.find_all("script"):
            text = script.string or ""
            if ("chair" in text.lower() and "{" in text) or "searchResults" in text:
                try:
                    # Find JSON objects with chair data
                    for match in re.finditer(r'\{[^{}]*"name"[^{}]*"location"[^{}]*\}', text):
                        try:
                            chair_data = json.loads(match.group(0))
                            contact = self._parse_chair_data(chair_data, url)
                            if contact:
                                contacts.append(contact)
                        except json.JSONDecodeError:
                            pass
                except Exception:
                    pass

        # Parse chair cards from HTML
        for card in soup.find_all(class_=re.compile(
            r"chair|coach|advisor|member|profile|card|listing|result", re.I
        )):
            contact = self._parse_chair_card(card, url)
            if contact:
                contacts.append(contact)

        # Follow chair profile links
        profile_links = set()
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            text = a.get_text(strip=True)
            # Chair profile URLs
            if ("/chair/" in href or "/chairs/" in href or "/profile/" in href) and text:
                full_url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                slug = full_url.rstrip("/").split("/")[-1]
                if slug and slug not in self._seen_chairs and slug not in ("chairs", "find-a-chair"):
                    self._seen_chairs.add(slug)
                    profile_links.add(full_url)

        for profile_url in profile_links:
            profile_html = self.fetch_page(profile_url)
            if profile_html:
                profile_contacts = self._parse_chair_profile(profile_url, profile_html)
                contacts.extend(profile_contacts)

        return contacts

    def _parse_sitemap(self, xml_text: str) -> list[ScrapedContact]:
        """Extract chair/profile URLs from sitemap."""
        contacts = []
        urls = re.findall(r"<loc>(https?://[^<]+)</loc>", xml_text)

        chair_urls = [
            u for u in urls
            if "/chair/" in u or "/chairs/" in u or "/profile/" in u
        ]

        self.logger.info("Sitemap: found %d chair/profile URLs", len(chair_urls))

        for chair_url in chair_urls[:500]:
            slug = chair_url.rstrip("/").split("/")[-1]
            if slug in self._seen_chairs:
                continue
            self._seen_chairs.add(slug)

            html = self.fetch_page(chair_url)
            if html:
                page_contacts = self._parse_chair_profile(chair_url, html)
                contacts.extend(page_contacts)

        return contacts

    def _parse_jsonld(self, data: dict, source_url: str) -> ScrapedContact | None:
        """Parse JSON-LD data."""
        data_type = data.get("@type", "")
        if data_type not in ("Person", "Organization", "ProfessionalService"):
            return None

        name = (data.get("name") or "").strip()
        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_chairs:
            return None
        self._seen_chairs.add(name_key)

        website = (data.get("url") or "").strip()
        description = (data.get("description") or "")[:500]
        email = (data.get("email") or "").strip()
        phone = (data.get("telephone") or "").strip()

        address = data.get("address", {})
        location = ""
        if isinstance(address, dict):
            parts = [
                (address.get("addressLocality") or "").strip(),
                (address.get("addressRegion") or "").strip(),
            ]
            location = ", ".join(p for p in parts if p)

        bio_parts = ["Vistage Chair"]
        if location:
            bio_parts.append(location)
        if description:
            bio_parts.append(description[:300])

        return ScrapedContact(
            name=name,
            email=email,
            company="Vistage",
            website=website or source_url,
            linkedin="",
            phone=phone,
            bio=" | ".join(bio_parts),
            source_url=source_url,
            source_category="executive_coaching",
        )

    def _parse_chair_data(self, data: dict, source_url: str) -> ScrapedContact | None:
        """Parse chair info from JSON data."""
        name = (
            data.get("name")
            or data.get("fullName")
            or data.get("chairName")
            or ""
        ).strip()

        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_chairs:
            return None
        self._seen_chairs.add(name_key)

        location = (data.get("location") or "").strip()
        city = (data.get("city") or "").strip()
        state = (data.get("state") or "").strip()
        if not location and (city or state):
            parts = [p for p in [city, state] if p]
            location = ", ".join(parts)

        title = (data.get("title") or data.get("headline") or "").strip()
        bio_text = (data.get("bio") or data.get("description") or "").strip()
        specialties = (data.get("specialties") or data.get("expertise") or "").strip()

        email = (data.get("email") or "").strip()
        phone = (data.get("phone") or "").strip()
        website = (data.get("website") or data.get("url") or "").strip()
        linkedin = (data.get("linkedin") or data.get("linkedinUrl") or "").strip()

        if website and not website.startswith("http"):
            website = f"https://{website}"

        bio_parts = ["Vistage Chair | Executive Peer Advisory"]
        if title:
            bio_parts.append(title[:100])
        if location:
            bio_parts.append(location)
        if specialties:
            bio_parts.append(specialties[:200])
        elif bio_text:
            clean_bio = re.sub(r"<[^>]+>", " ", bio_text).strip()[:200]
            bio_parts.append(clean_bio)

        return ScrapedContact(
            name=name,
            email=email,
            company="Vistage",
            website=website or source_url,
            linkedin=linkedin,
            phone=phone,
            bio=" | ".join(bio_parts),
            source_url=source_url,
            source_category="executive_coaching",
        )

    def _parse_chair_card(self, card, source_url: str) -> ScrapedContact | None:
        """Parse a chair card element."""
        name = ""
        website = ""

        for tag in ["h2", "h3", "h4", "strong"]:
            el = card.find(tag)
            if el:
                link = el.find("a", href=True)
                if link:
                    name = link.get_text(strip=True)
                    href = link.get("href", "")
                    website = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                else:
                    name = el.get_text(strip=True)
                if name and len(name) > 2 and len(name) < 100:
                    break
                name = ""

        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_chairs:
            return None
        self._seen_chairs.add(name_key)

        # Location
        location = ""
        loc_el = card.find(class_=re.compile(r"location|city|region|address", re.I))
        if loc_el:
            location = loc_el.get_text(strip=True)[:100]

        # Title/specialty
        title = ""
        title_el = card.find(class_=re.compile(r"title|specialty|role|description", re.I))
        if title_el:
            title = title_el.get_text(strip=True)[:200]

        linkedin = self.extract_linkedin(str(card))
        emails = self.extract_emails(str(card))
        email = emails[0] if emails else ""

        bio_parts = ["Vistage Chair"]
        if location:
            bio_parts.append(location)
        if title:
            bio_parts.append(title)

        return ScrapedContact(
            name=name,
            email=email,
            company="Vistage",
            website=website or source_url,
            linkedin=linkedin,
            phone="",
            bio=" | ".join(bio_parts),
            source_url=source_url,
            source_category="executive_coaching",
        )

    def _parse_chair_profile(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a Vistage chair profile page."""
        soup = self.parse_html(html)
        contacts = []

        name = ""
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        if not name:
            og = soup.find("meta", property="og:title")
            if og:
                name = (og.get("content") or "").split("|")[0].split("-")[0].strip()

        if not name or len(name) < 2:
            return []

        # Clean name
        name = re.sub(r"\s*[|–-]\s*Vistage.*$", "", name, flags=re.I).strip()

        name_key = name.lower()
        if name_key in self._seen_chairs:
            return []
        self._seen_chairs.add(name_key)

        bio = ""
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            bio = (meta_desc.get("content") or "")[:500]

        # External website
        website = ""
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            text = a.get_text(strip=True).lower()
            if ("website" in text or "visit" in text) and href.startswith("http"):
                if "vistage.com" not in href.lower():
                    website = href
                    break

        if not website:
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if (href.startswith("http")
                        and "vistage.com" not in href.lower()
                        and "facebook.com" not in href.lower()
                        and "twitter.com" not in href.lower()
                        and "instagram.com" not in href.lower()
                        and "linkedin.com" not in href.lower()):
                    website = href
                    break

        emails = self.extract_emails(html)
        email = emails[0] if emails else ""
        linkedin = self.extract_linkedin(html)

        phone = ""
        phone_match = re.search(
            r"(?:1[-.\s]?)?(?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4}", html
        )
        if phone_match:
            phone = phone_match.group(0)

        contacts.append(ScrapedContact(
            name=name,
            email=email,
            company="Vistage",
            website=website or url,
            linkedin=linkedin,
            phone=phone,
            bio=f"Vistage Chair | {bio}" if bio else "Vistage Chair | CEO Peer Advisory Group",
            source_url=url,
            source_category="executive_coaching",
        ))

        return contacts
