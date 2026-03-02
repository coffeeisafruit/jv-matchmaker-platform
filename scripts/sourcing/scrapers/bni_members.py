"""
BNI (Business Network International) chapter directory scraper.

BNI is the world's largest business networking and referral
organization with ~10,000 chapters in 70+ countries. The chapter
finder at bni.com/find-a-chapter lists chapters with location,
meeting details, and sometimes member information.

Strategy: Use BNI's chapter search API/pages to discover chapters
by location, then scrape chapter pages for member listings and
chapter contact info.

Estimated yield: 3,000-10,000 chapters and members
"""

from __future__ import annotations

import json
import re
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# US states and major countries for chapter search
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
    "District of Columbia",
]

COUNTRIES = [
    "United States", "Canada", "United Kingdom", "Australia",
    "India", "Germany", "France", "Brazil", "Mexico",
    "South Africa", "Italy", "Spain", "Netherlands",
    "Singapore", "Japan", "Philippines",
]

# BNI region URLs for chapter discovery
REGION_SLUGS = [
    "us-northeast", "us-southeast", "us-midwest", "us-west",
    "us-southwest", "us-midatlantic", "us-northwest",
    "canada", "united-kingdom", "australia", "india",
]


class Scraper(BaseScraper):
    SOURCE_NAME = "bni_members"
    BASE_URL = "https://www.bni.com"
    REQUESTS_PER_MINUTE = 5

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_chapters: set[str] = set()
        self._seen_members: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield BNI chapter finder URLs."""
        # Main find-a-chapter page
        yield f"{self.BASE_URL}/find-a-chapter"

        # Region-based search
        for region in REGION_SLUGS:
            yield f"{self.BASE_URL}/find-a-chapter/{region}"

        # State-based search (US)
        for state in US_STATES:
            state_slug = state.lower().replace(" ", "-")
            yield f"{self.BASE_URL}/find-a-chapter?q={state_slug}"

        # Country pages
        for country in COUNTRIES:
            country_slug = country.lower().replace(" ", "-")
            yield f"{self.BASE_URL}/regions/{country_slug}"

        # Sitemap for chapter pages
        yield f"{self.BASE_URL}/sitemap.xml"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse BNI pages for chapter and member data."""
        if url.endswith("sitemap.xml"):
            return self._parse_sitemap(html)

        soup = self.parse_html(html)
        contacts = []

        # Try to extract from Next.js / React data
        for script in soup.find_all("script"):
            text = script.string or ""
            if "__NEXT_DATA__" in text or "chapters" in text.lower():
                try:
                    # Find JSON data in script
                    json_match = re.search(r'\{.*"chapters".*\}', text)
                    if json_match:
                        data = json.loads(json_match.group(0))
                        chapters = data.get("chapters", [])
                        for chapter in chapters:
                            contact = self._parse_chapter_data(chapter, url)
                            if contact:
                                contacts.append(contact)
                except (json.JSONDecodeError, KeyError):
                    pass

        # Try __NEXT_DATA__ specifically
        next_data = soup.find("script", id="__NEXT_DATA__")
        if next_data and next_data.string:
            try:
                data = json.loads(next_data.string)
                props = data.get("props", {}).get("pageProps", {})
                chapters = (
                    props.get("chapters", [])
                    or props.get("results", [])
                    or props.get("data", {}).get("chapters", [])
                )
                for chapter in chapters:
                    contact = self._parse_chapter_data(chapter, url)
                    if contact:
                        contacts.append(contact)
            except (json.JSONDecodeError, KeyError):
                pass

        # Parse chapter cards from HTML
        for card in soup.find_all(class_=re.compile(
            r"chapter|group|result|card|listing", re.I
        )):
            contact = self._parse_chapter_card(card, url)
            if contact:
                contacts.append(contact)

        # Look for chapter links to follow
        chapter_links = set()
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            # BNI chapter URLs: /find-a-chapter/{region}/{chapter-name}
            if ("/chapter/" in href or "/find-a-chapter/" in href) and href.count("/") >= 3:
                full_url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                slug = full_url.rstrip("/").split("/")[-1]
                if slug not in self._seen_chapters and slug != "find-a-chapter":
                    self._seen_chapters.add(slug)
                    chapter_links.add(full_url)

        for chapter_url in chapter_links:
            chapter_html = self.fetch_page(chapter_url)
            if chapter_html:
                chapter_contacts = self._parse_chapter_page(chapter_url, chapter_html)
                contacts.extend(chapter_contacts)

        return contacts

    def _parse_sitemap(self, xml_text: str) -> list[ScrapedContact]:
        """Extract chapter URLs from sitemap."""
        contacts = []
        urls = re.findall(r"<loc>(https?://[^<]+)</loc>", xml_text)

        chapter_urls = [
            u for u in urls
            if "/chapter/" in u or ("/find-a-chapter/" in u and u.count("/") >= 5)
        ]

        self.logger.info("Sitemap: found %d chapter URLs", len(chapter_urls))

        for chapter_url in chapter_urls[:500]:
            slug = chapter_url.rstrip("/").split("/")[-1]
            if slug in self._seen_chapters:
                continue
            self._seen_chapters.add(slug)

            chapter_html = self.fetch_page(chapter_url)
            if chapter_html:
                chapter_contacts = self._parse_chapter_page(chapter_url, chapter_html)
                contacts.extend(chapter_contacts)

        return contacts

    def _parse_chapter_data(self, data: dict, source_url: str) -> ScrapedContact | None:
        """Parse chapter info from JSON data."""
        name = (data.get("name") or data.get("chapterName") or "").strip()
        if not name:
            return None

        name_key = name.lower()
        if name_key in self._seen_chapters:
            return None
        self._seen_chapters.add(name_key)

        location = ""
        city = (data.get("city") or "").strip()
        state = (data.get("state") or data.get("region") or "").strip()
        country = (data.get("country") or "").strip()
        if city and state:
            location = f"{city}, {state}"
        elif city:
            location = city
        elif state:
            location = state

        meeting_info = (data.get("meetingTime") or data.get("meeting") or "").strip()
        member_count = data.get("memberCount") or data.get("members") or ""

        chapter_url = (data.get("url") or data.get("chapterUrl") or "").strip()
        if chapter_url and not chapter_url.startswith("http"):
            chapter_url = f"{self.BASE_URL}{chapter_url}"

        bio_parts = ["BNI Chapter"]
        if location:
            bio_parts.append(location)
        if member_count:
            bio_parts.append(f"{member_count} members")
        if meeting_info:
            bio_parts.append(meeting_info[:100])
        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=f"BNI {name}" if not name.upper().startswith("BNI") else name,
            email="",
            company="BNI",
            website=chapter_url or source_url,
            linkedin="",
            phone="",
            bio=bio,
            source_url=source_url,
            source_category="networking",
        )

    def _parse_chapter_card(self, card, source_url: str) -> ScrapedContact | None:
        """Parse a chapter card element."""
        name = ""
        website = ""

        for tag in ["h2", "h3", "h4", "strong", "a"]:
            el = card.find(tag)
            if el:
                name = el.get_text(strip=True)
                if isinstance(el, type(card.find("a"))) and el.get("href"):
                    href = el.get("href", "")
                    website = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                if name and len(name) > 2 and len(name) < 150:
                    break
                name = ""

        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_chapters:
            return None
        self._seen_chapters.add(name_key)

        # Location
        location = ""
        loc_el = card.find(class_=re.compile(r"location|address|city|region", re.I))
        if loc_el:
            location = loc_el.get_text(strip=True)[:100]

        bio_parts = ["BNI Chapter"]
        if location:
            bio_parts.append(location)
        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=f"BNI {name}" if not name.upper().startswith("BNI") else name,
            email="",
            company="BNI",
            website=website or source_url,
            linkedin="",
            phone="",
            bio=bio,
            source_url=source_url,
            source_category="networking",
        )

    def _parse_chapter_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a BNI chapter page for chapter info and member listings."""
        soup = self.parse_html(html)
        contacts = []

        # Chapter name
        chapter_name = ""
        h1 = soup.find("h1")
        if h1:
            chapter_name = h1.get_text(strip=True)

        if not chapter_name:
            og = soup.find("meta", property="og:title")
            if og:
                chapter_name = (og.get("content") or "").split("|")[0].strip()

        if chapter_name:
            name_key = chapter_name.lower()
            if name_key not in self._seen_chapters:
                self._seen_chapters.add(name_key)

                emails = self.extract_emails(html)
                email = emails[0] if emails else ""

                phone = ""
                phone_match = re.search(
                    r"(?:1[-.\s]?)?(?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4}", html
                )
                if phone_match:
                    phone = phone_match.group(0)

                bio = ""
                meta_desc = soup.find("meta", attrs={"name": "description"})
                if meta_desc:
                    bio = (meta_desc.get("content") or "")[:500]

                display_name = chapter_name
                if not display_name.upper().startswith("BNI"):
                    display_name = f"BNI {chapter_name}"

                contacts.append(ScrapedContact(
                    name=display_name,
                    email=email,
                    company="BNI",
                    website=url,
                    linkedin="",
                    phone=phone,
                    bio=f"BNI Chapter | {bio}" if bio else "BNI Business Networking Chapter",
                    source_url=url,
                    source_category="networking",
                ))

        # Look for member listings on the chapter page
        for member_el in soup.find_all(class_=re.compile(r"member|participant|person", re.I)):
            member_name = ""
            for tag in ["h3", "h4", "strong", "span"]:
                el = member_el.find(tag)
                if el:
                    member_name = el.get_text(strip=True)
                    if member_name and len(member_name) > 2 and len(member_name) < 100:
                        break
                    member_name = ""

            if not member_name:
                continue

            member_key = member_name.lower()
            if member_key in self._seen_members:
                continue
            self._seen_members.add(member_key)

            # Member's business/profession
            profession = ""
            prof_el = member_el.find(class_=re.compile(r"profession|business|category|role", re.I))
            if prof_el:
                profession = prof_el.get_text(strip=True)[:100]

            member_website = ""
            for a in member_el.find_all("a", href=True):
                href = a.get("href", "")
                if href.startswith("http") and "bni.com" not in href.lower():
                    member_website = href
                    break

            member_email = ""
            member_emails = self.extract_emails(str(member_el))
            if member_emails:
                member_email = member_emails[0]

            bio_parts = ["BNI Member"]
            if chapter_name:
                bio_parts.append(chapter_name)
            if profession:
                bio_parts.append(profession)

            contacts.append(ScrapedContact(
                name=member_name,
                email=member_email,
                company="",
                website=member_website or url,
                linkedin="",
                phone="",
                bio=" | ".join(bio_parts),
                source_url=url,
                source_category="networking",
            ))

        return contacts
