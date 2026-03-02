"""
Entrepreneurs' Organization (EO) chapter directory scraper.

EO is a global peer-to-peer network of 17,000+ business owners
in 60+ countries. Chapter pages list location, member count,
and sometimes links to chapter websites.

Strategy: Scrape the EO chapter directory at eonetwork.org
which lists chapters by region/country. Each chapter page may
have basic info about the chapter and links.

Note: EO member data is private (behind login). We can only
get chapter-level info (chapter name, location, website) which
still provides organizational contacts and leadership.

Estimated yield: 200-300 chapters
"""

from __future__ import annotations

import json
import re
from typing import Iterator

from scripts.sourcing.base import BaseScraper, ScrapedContact


# EO regions for navigation
REGIONS = [
    "north-america",
    "latin-america",
    "europe",
    "middle-east-africa",
    "asia-pacific",
    "south-asia",
]

# Direct chapter page paths (known from EO site structure)
CHAPTER_SEARCH_COUNTRIES = [
    "united-states",
    "canada",
    "united-kingdom",
    "australia",
    "india",
    "germany",
    "france",
    "brazil",
    "mexico",
    "south-africa",
    "singapore",
    "japan",
    "china",
    "spain",
    "italy",
    "netherlands",
    "switzerland",
    "united-arab-emirates",
    "israel",
    "nigeria",
    "colombia",
    "argentina",
    "chile",
    "peru",
    "egypt",
    "kenya",
    "new-zealand",
    "philippines",
    "indonesia",
    "thailand",
    "vietnam",
    "malaysia",
    "south-korea",
    "taiwan",
    "hong-kong",
]


class Scraper(BaseScraper):
    SOURCE_NAME = "eonetwork"
    BASE_URL = "https://www.eonetwork.org"
    REQUESTS_PER_MINUTE = 5

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_chapters: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield EO chapter directory URLs."""
        # Main chapters page
        yield f"{self.BASE_URL}/chapters"
        yield f"{self.BASE_URL}/chapters/"

        # Region pages
        for region in REGIONS:
            yield f"{self.BASE_URL}/chapters/{region}"
            yield f"{self.BASE_URL}/chapters?region={region}"

        # Country-specific pages
        for country in CHAPTER_SEARCH_COUNTRIES:
            yield f"{self.BASE_URL}/chapters/{country}"
            yield f"{self.BASE_URL}/chapters?country={country}"

        # Sitemap for additional pages
        yield f"{self.BASE_URL}/sitemap.xml"

        # About pages often list chapter contacts
        yield f"{self.BASE_URL}/about"
        yield f"{self.BASE_URL}/about/chapters"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse EO pages for chapter data."""
        if url.endswith("sitemap.xml"):
            return self._parse_sitemap(html)

        soup = self.parse_html(html)
        contacts = []

        # Try JSON-LD data
        for script in soup.find_all("script", type="application/ld+json"):
            if script.string:
                try:
                    data = json.loads(script.string)
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        if item.get("@type") in ("Organization", "LocalBusiness"):
                            contact = self._parse_jsonld_org(item, url)
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
                chapters = (
                    props.get("chapters", [])
                    or props.get("data", {}).get("chapters", [])
                    or props.get("results", [])
                )
                for chapter in chapters:
                    contact = self._parse_chapter_data(chapter, url)
                    if contact:
                        contacts.append(contact)
            except (json.JSONDecodeError, KeyError):
                pass

        # Parse chapter cards from HTML
        for card in soup.find_all(class_=re.compile(
            r"chapter|region|location|card|listing|result", re.I
        )):
            contact = self._parse_chapter_card(card, url)
            if contact:
                contacts.append(contact)

        # Follow chapter-specific links
        chapter_links = set()
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            text = a.get_text(strip=True)
            # Chapter page links
            if ("/chapter/" in href or "/chapters/" in href) and text and len(text) > 3:
                full_url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                slug = full_url.rstrip("/").split("/")[-1]
                if slug and slug not in self._seen_chapters and slug != "chapters":
                    self._seen_chapters.add(slug)
                    chapter_links.add(full_url)

        for chapter_url in chapter_links:
            chapter_html = self.fetch_page(chapter_url)
            if chapter_html:
                chapter_contacts = self._parse_chapter_page(chapter_url, chapter_html)
                contacts.extend(chapter_contacts)

        # If still no results, try parsing any structured list
        if not contacts:
            for ul in soup.find_all(["ul", "ol"]):
                for li in ul.find_all("li"):
                    link = li.find("a", href=True)
                    if link:
                        text = link.get_text(strip=True)
                        href = link.get("href", "")
                        if text and len(text) > 3 and ("eo" in text.lower() or "chapter" in text.lower()):
                            full_url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                            name_key = text.lower()
                            if name_key not in self._seen_chapters:
                                self._seen_chapters.add(name_key)
                                contacts.append(ScrapedContact(
                                    name=text,
                                    email="",
                                    company="Entrepreneurs Organization",
                                    website=full_url,
                                    linkedin="",
                                    phone="",
                                    bio="EO Chapter | Entrepreneurs Organization",
                                    source_url=url,
                                    source_category="networking",
                                ))

        return contacts

    def _parse_sitemap(self, xml_text: str) -> list[ScrapedContact]:
        """Extract chapter URLs from sitemap."""
        contacts = []
        urls = re.findall(r"<loc>(https?://[^<]+)</loc>", xml_text)

        chapter_urls = [u for u in urls if "/chapter/" in u or "/chapters/" in u]
        self.logger.info("Sitemap: found %d chapter URLs", len(chapter_urls))

        for chapter_url in chapter_urls[:200]:
            slug = chapter_url.rstrip("/").split("/")[-1]
            if slug in self._seen_chapters or slug == "chapters":
                continue
            self._seen_chapters.add(slug)

            html = self.fetch_page(chapter_url)
            if html:
                page_contacts = self._parse_chapter_page(chapter_url, html)
                contacts.extend(page_contacts)

        return contacts

    def _parse_jsonld_org(self, data: dict, source_url: str) -> ScrapedContact | None:
        """Parse JSON-LD organization data."""
        name = (data.get("name") or "").strip()
        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_chapters:
            return None
        self._seen_chapters.add(name_key)

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
                (address.get("addressCountry") or "").strip(),
            ]
            location = ", ".join(p for p in parts if p)

        bio_parts = ["EO Chapter"]
        if location:
            bio_parts.append(location)
        if description:
            bio_parts.append(description[:300])

        display_name = name
        if "eo" not in name.lower() and "entrepreneur" not in name.lower():
            display_name = f"EO {name}"

        return ScrapedContact(
            name=display_name,
            email=email,
            company="Entrepreneurs Organization",
            website=website or source_url,
            linkedin="",
            phone=phone,
            bio=" | ".join(bio_parts),
            source_url=source_url,
            source_category="networking",
        )

    def _parse_chapter_data(self, data: dict, source_url: str) -> ScrapedContact | None:
        """Parse chapter info from JSON data."""
        name = (
            data.get("name")
            or data.get("chapterName")
            or data.get("title")
            or ""
        ).strip()

        if not name:
            return None

        name_key = name.lower()
        if name_key in self._seen_chapters:
            return None
        self._seen_chapters.add(name_key)

        city = (data.get("city") or "").strip()
        country = (data.get("country") or "").strip()
        region = (data.get("region") or "").strip()
        member_count = data.get("memberCount") or data.get("members") or ""

        location_parts = [p for p in [city, country] if p]
        location = ", ".join(location_parts) if location_parts else region

        chapter_url = (data.get("url") or data.get("link") or "").strip()
        if chapter_url and not chapter_url.startswith("http"):
            chapter_url = f"{self.BASE_URL}{chapter_url}"

        bio_parts = ["EO Chapter"]
        if location:
            bio_parts.append(location)
        if member_count:
            bio_parts.append(f"{member_count} members")

        display_name = name
        if "eo" not in name.lower() and "entrepreneur" not in name.lower():
            display_name = f"EO {name}"

        return ScrapedContact(
            name=display_name,
            email="",
            company="Entrepreneurs Organization",
            website=chapter_url or source_url,
            linkedin="",
            phone="",
            bio=" | ".join(bio_parts),
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
                if hasattr(el, "get") and el.get("href"):
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

        location = ""
        loc_el = card.find(class_=re.compile(r"location|city|region|country", re.I))
        if loc_el:
            location = loc_el.get_text(strip=True)[:100]

        bio_parts = ["EO Chapter"]
        if location:
            bio_parts.append(location)

        display_name = name
        if "eo" not in name.lower() and "entrepreneur" not in name.lower():
            display_name = f"EO {name}"

        return ScrapedContact(
            name=display_name,
            email="",
            company="Entrepreneurs Organization",
            website=website or source_url,
            linkedin="",
            phone="",
            bio=" | ".join(bio_parts),
            source_url=source_url,
            source_category="networking",
        )

    def _parse_chapter_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse an individual chapter page."""
        soup = self.parse_html(html)
        contacts = []

        name = ""
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        if not name:
            og = soup.find("meta", property="og:title")
            if og:
                name = (og.get("content") or "").split("|")[0].strip()

        if not name or len(name) < 2:
            return []

        name_key = name.lower()
        if name_key in self._seen_chapters:
            return []
        self._seen_chapters.add(name_key)

        bio = ""
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            bio = (meta_desc.get("content") or "")[:500]

        emails = self.extract_emails(html)
        email = emails[0] if emails else ""

        phone = ""
        phone_match = re.search(
            r"(?:1[-.\s]?)?(?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4}", html
        )
        if phone_match:
            phone = phone_match.group(0)

        website = ""
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if (href.startswith("http")
                    and "eonetwork.org" not in href.lower()
                    and "facebook.com" not in href.lower()
                    and "twitter.com" not in href.lower()
                    and "linkedin.com" not in href.lower()
                    and "instagram.com" not in href.lower()):
                website = href
                break

        linkedin = self.extract_linkedin(html)

        display_name = name
        if "eo" not in name.lower() and "entrepreneur" not in name.lower():
            display_name = f"EO {name}"

        contacts.append(ScrapedContact(
            name=display_name,
            email=email,
            company="Entrepreneurs Organization",
            website=website or url,
            linkedin=linkedin,
            phone=phone,
            bio=f"EO Chapter | {bio}" if bio else "EO Chapter | Entrepreneurs Organization",
            source_url=url,
            source_category="networking",
        ))

        return contacts
