"""
Startup Grind Community Directory Scraper

Scrapes startup and founder data from https://www.startupgrind.com/directory.

Startup Grind is a global startup community powered by Google for
Entrepreneurs. Their directory lists startups, founders, investors,
and mentors from 600+ chapters worldwide.

Strategy:
  Startup Grind's directory is likely powered by Algolia search or a
  similar API backend. We attempt to:

  1. (Primary) Discover and use the underlying search API/Algolia endpoint
     by checking for embedded config in the page HTML
  2. (Fallback) Scrape the server-rendered HTML directory pages with
     region/chapter-based pagination

  Directory pages follow patterns:
    /directory — Main directory
    /directory?page=N — Pagination
    /directory/{chapter} — Chapter-specific listings
    /membership/directory — Alternative path

Estimated yield: 3,000-10,000 startup profiles
"""

from __future__ import annotations

import json
import re
from typing import Iterator, Optional
from datetime import datetime
from urllib.parse import urljoin, urlencode

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Startup Grind major chapter cities for browsing
CHAPTERS = [
    "san-francisco",
    "new-york",
    "los-angeles",
    "london",
    "berlin",
    "amsterdam",
    "singapore",
    "tokyo",
    "sydney",
    "toronto",
    "chicago",
    "seattle",
    "austin",
    "boston",
    "denver",
    "miami",
    "atlanta",
    "dallas",
    "washington-dc",
    "portland",
    "phoenix",
    "san-diego",
    "detroit",
    "minneapolis",
    "salt-lake-city",
    "nashville",
    "charlotte",
    "pittsburgh",
    "columbus",
    "indianapolis",
    "tel-aviv",
    "bangalore",
    "mumbai",
    "dublin",
    "paris",
    "barcelona",
    "stockholm",
    "oslo",
    "helsinki",
    "lisbon",
    "cape-town",
    "nairobi",
    "lagos",
    "sao-paulo",
    "mexico-city",
    "bogota",
    "buenos-aires",
    "manila",
    "jakarta",
    "seoul",
]

# Algolia search config (if the site uses Algolia)
ALGOLIA_APP_ID_PATTERN = re.compile(r'(?:algolia|appId|applicationId)["\s:=]+["\']([A-Z0-9]+)["\']', re.I)
ALGOLIA_KEY_PATTERN = re.compile(r'(?:searchKey|apiKey|search_api_key)["\s:=]+["\']([a-f0-9]+)["\']', re.I)
ALGOLIA_INDEX_PATTERN = re.compile(r'(?:indexName|index_name)["\s:=]+["\']([a-zA-Z0-9_\-]+)["\']', re.I)

MAX_PAGES = 50
PAGE_SIZE = 20


class Scraper(BaseScraper):
    SOURCE_NAME = "startupgrind"
    BASE_URL = "https://www.startupgrind.com"
    REQUESTS_PER_MINUTE = 6

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_slugs: set[str] = set()
        self._seen_names: set[str] = set()
        self._algolia_config: dict | None = None

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield Startup Grind directory page URLs."""
        # Main directory with pagination
        for page in range(1, MAX_PAGES + 1):
            yield f"{self.BASE_URL}/directory?page={page}"

        # Chapter-specific directories
        for chapter in CHAPTERS:
            yield f"{self.BASE_URL}/directory/{chapter}"
            for page in range(2, 11):  # 10 pages per chapter
                yield f"{self.BASE_URL}/directory/{chapter}?page={page}"

        # Alternative paths that may contain directory data
        yield f"{self.BASE_URL}/membership/directory"
        yield f"{self.BASE_URL}/startups"
        yield f"{self.BASE_URL}/companies"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a Startup Grind directory page."""
        contacts = []

        # First page: try to discover Algolia config
        if not self._algolia_config:
            self._discover_algolia(html)

        # If we found Algolia, use it for bulk data
        if self._algolia_config and url.endswith("?page=1"):
            algolia_contacts = self._scrape_via_algolia()
            if algolia_contacts:
                return algolia_contacts

        # HTML parsing fallback
        soup = self.parse_html(html)

        # Try to extract from embedded JSON/script data first
        json_contacts = self._extract_from_scripts(soup, html)
        if json_contacts:
            contacts.extend(json_contacts)

        # Parse directory cards from HTML
        html_contacts = self._parse_directory_cards(soup)
        if html_contacts:
            contacts.extend(html_contacts)

        # Look for profile links and fetch individually
        if not contacts:
            profile_links = self._extract_profile_links(soup)
            for profile_url in profile_links:
                profile_html = self.fetch_page(profile_url)
                if profile_html:
                    contact = self._parse_profile_page(profile_url, profile_html)
                    if contact:
                        contacts.append(contact)

        return contacts

    def _discover_algolia(self, html: str):
        """Try to find Algolia search configuration in the page HTML."""
        app_id_match = ALGOLIA_APP_ID_PATTERN.search(html)
        api_key_match = ALGOLIA_KEY_PATTERN.search(html)
        index_match = ALGOLIA_INDEX_PATTERN.search(html)

        if app_id_match and api_key_match and index_match:
            self._algolia_config = {
                "app_id": app_id_match.group(1),
                "api_key": api_key_match.group(1),
                "index_name": index_match.group(1),
            }
            self.logger.info(
                "Discovered Algolia config: app=%s, index=%s",
                self._algolia_config["app_id"],
                self._algolia_config["index_name"],
            )

    def _scrape_via_algolia(self) -> list[ScrapedContact]:
        """Query Algolia search API directly for bulk data."""
        if not self._algolia_config:
            return []

        contacts = []
        app_id = self._algolia_config["app_id"]
        api_key = self._algolia_config["api_key"]
        index = self._algolia_config["index_name"]

        algolia_url = f"https://{app_id}-dsn.algolia.net/1/indexes/{index}/query"

        for page in range(0, MAX_PAGES):
            payload = {
                "params": urlencode({
                    "query": "",
                    "page": page,
                    "hitsPerPage": PAGE_SIZE,
                }),
            }

            if self.rate_limiter:
                self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

            try:
                resp = self.session.post(
                    algolia_url,
                    json=payload,
                    headers={
                        "X-Algolia-Application-Id": app_id,
                        "X-Algolia-API-Key": api_key,
                        "Content-Type": "application/json",
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
                self.stats["pages_scraped"] += 1
            except Exception as e:
                self.logger.warning("Algolia request failed: %s", e)
                self.stats["errors"] += 1
                break

            hits = data.get("hits", [])
            if not hits:
                break

            for hit in hits:
                contact = self._algolia_hit_to_contact(hit)
                if contact:
                    contacts.append(contact)

            # Check if there are more pages
            nb_pages = data.get("nbPages", 0)
            if page >= nb_pages - 1:
                break

        self.logger.info("Algolia returned %d contacts", len(contacts))
        return contacts

    def _algolia_hit_to_contact(self, hit: dict) -> ScrapedContact | None:
        """Convert an Algolia search hit to a ScrapedContact."""
        name = (hit.get("name") or hit.get("company_name") or hit.get("title") or "").strip()
        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_names:
            return None
        self._seen_names.add(name_key)

        description = (hit.get("description") or hit.get("bio") or hit.get("tagline") or "").strip()
        website = (hit.get("website") or hit.get("url") or hit.get("company_url") or "").strip()
        location = (hit.get("location") or hit.get("city") or hit.get("chapter") or "").strip()
        linkedin = (hit.get("linkedin") or hit.get("linkedin_url") or "").strip()
        email = (hit.get("email") or "").strip()

        # Role/type info
        role = (hit.get("role") or hit.get("type") or "").strip()
        company = (hit.get("company") or hit.get("company_name") or name).strip()

        # Slug for dedup
        slug = (hit.get("objectID") or hit.get("slug") or hit.get("id") or "").strip()
        if slug:
            if slug in self._seen_slugs:
                return None
            self._seen_slugs.add(slug)

        # Build bio
        bio_parts = []
        if description:
            bio_parts.append(description[:500])
        if role:
            bio_parts.append(f"Role: {role}")
        if location:
            bio_parts.append(f"Location: {location}")
        bio = " | ".join(bio_parts) if bio_parts else f"Listed in Startup Grind directory"

        if not website:
            website = f"{self.BASE_URL}/directory"

        return ScrapedContact(
            name=name,
            email=email,
            company=company,
            website=website,
            linkedin=linkedin,
            bio=bio,
            source_category="startups",
            raw_data={
                "algolia_id": slug,
                "location": location,
                "role": role,
            },
        )

    def _extract_from_scripts(self, soup, html: str) -> list[ScrapedContact]:
        """Extract directory data from embedded script tags."""
        contacts = []

        # Check for Next.js __NEXT_DATA__
        next_data_script = soup.find("script", id="__NEXT_DATA__")
        if next_data_script:
            try:
                data = json.loads(next_data_script.string or "")
                page_props = data.get("props", {}).get("pageProps", {})
                # Look for directory/members/companies in page props
                self._find_directory_entries(page_props, contacts)
            except (json.JSONDecodeError, ValueError):
                pass

        # Check for other embedded JSON state patterns
        patterns = [
            r'window\.__INITIAL_STATE__\s*=\s*({.+?});',
            r'window\.__DATA__\s*=\s*({.+?});',
            r'window\.initialState\s*=\s*({.+?});',
            r'<script[^>]*type="application/json"[^>]*>(.*?)</script>',
        ]
        for pattern in patterns:
            for match in re.finditer(pattern, html, re.DOTALL):
                try:
                    data = json.loads(match.group(1))
                    self._find_directory_entries(data, contacts)
                except (json.JSONDecodeError, ValueError):
                    continue

        return contacts

    def _find_directory_entries(self, obj, results: list, depth: int = 0):
        """Recursively find directory entry objects in nested data."""
        if depth > 8:
            return

        if isinstance(obj, dict):
            # Check if this looks like a directory entry
            has_name = bool(obj.get("name") or obj.get("company_name") or obj.get("title"))
            has_signal = bool(
                obj.get("website") or obj.get("url") or obj.get("email")
                or obj.get("linkedin") or obj.get("description") or obj.get("bio")
            )
            if has_name and has_signal:
                contact = self._dict_to_contact(obj)
                if contact:
                    results.append(contact)
            else:
                for value in obj.values():
                    if isinstance(value, (dict, list)):
                        self._find_directory_entries(value, results, depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    self._find_directory_entries(item, results, depth + 1)

    def _dict_to_contact(self, data: dict) -> ScrapedContact | None:
        """Convert a generic dict to ScrapedContact."""
        name = (
            data.get("name") or data.get("company_name")
            or data.get("title") or data.get("full_name") or ""
        ).strip()

        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_names:
            return None
        self._seen_names.add(name_key)

        website = (data.get("website") or data.get("url") or data.get("company_url") or "").strip()
        email = (data.get("email") or "").strip()
        linkedin = (data.get("linkedin") or data.get("linkedin_url") or "").strip()
        company = (data.get("company") or data.get("company_name") or name).strip()
        description = (data.get("description") or data.get("bio") or data.get("tagline") or "").strip()
        location = (data.get("location") or data.get("city") or data.get("chapter") or "").strip()

        bio_parts = []
        if description:
            bio_parts.append(description[:500])
        if location:
            bio_parts.append(f"Location: {location}")
        bio = " | ".join(bio_parts) if bio_parts else f"Listed in Startup Grind directory"

        if not website:
            website = f"{self.BASE_URL}/directory"

        return ScrapedContact(
            name=name,
            email=email,
            company=company,
            website=website,
            linkedin=linkedin,
            bio=bio,
            source_category="startups",
        )

    def _parse_directory_cards(self, soup) -> list[ScrapedContact]:
        """Parse directory listing cards from HTML."""
        contacts = []

        # Try various common card selectors for directory listings
        card_selectors = [
            {"class_": re.compile(r"directory[-_]?card|member[-_]?card|startup[-_]?card", re.I)},
            {"class_": re.compile(r"card|listing|result", re.I)},
            {"attrs": {"data-type": re.compile(r"startup|company|member", re.I)}},
        ]

        cards = []
        for selector in card_selectors:
            found = soup.find_all("div", **selector)
            if found:
                cards = found
                break

        # Also try article tags
        if not cards:
            cards = soup.find_all("article")

        for card in cards:
            contact = self._parse_card(card)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_card(self, card) -> ScrapedContact | None:
        """Parse a single directory card."""
        # Name from heading or link
        name = ""
        for tag in card.find_all(["h2", "h3", "h4"]):
            text = tag.get_text(strip=True)
            if text and len(text) > 1:
                name = text
                break

        if not name:
            link = card.find("a", href=True)
            if link:
                name = link.get_text(strip=True)

        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_names:
            return None
        self._seen_names.add(name_key)

        # Description
        description = ""
        for p_tag in card.find_all("p"):
            text = p_tag.get_text(strip=True)
            if text and len(text) > 10:
                description = text[:500]
                break

        # Website from link
        website = ""
        for a_tag in card.find_all("a", href=True):
            href = a_tag.get("href", "")
            if href.startswith("http") and "startupgrind.com" not in href.lower():
                website = href
                break

        # Profile link as fallback
        if not website:
            profile_link = card.find("a", href=True)
            if profile_link:
                href = profile_link.get("href", "")
                if href.startswith("/"):
                    website = urljoin(self.BASE_URL, href)
                elif href.startswith("http"):
                    website = href

        if not website:
            website = f"{self.BASE_URL}/directory"

        # Location
        location = ""
        loc_el = card.find(class_=re.compile(r"location|chapter|city", re.I))
        if loc_el:
            location = loc_el.get_text(strip=True)

        # Role/title
        role = ""
        role_el = card.find(class_=re.compile(r"role|title|position", re.I))
        if role_el:
            role_text = role_el.get_text(strip=True)
            if role_text != name:
                role = role_text

        # LinkedIn
        linkedin = ""
        for a_tag in card.find_all("a", href=True):
            if "linkedin.com" in a_tag.get("href", "").lower():
                linkedin = a_tag["href"]
                break

        # Build bio
        bio_parts = []
        if description:
            bio_parts.append(description)
        if role:
            bio_parts.append(f"Role: {role}")
        if location:
            bio_parts.append(f"Location: {location}")
        bio = " | ".join(bio_parts) if bio_parts else f"Listed in Startup Grind directory"

        return ScrapedContact(
            name=name,
            company=name,
            website=website,
            linkedin=linkedin,
            bio=bio,
            source_category="startups",
        )

    def _extract_profile_links(self, soup) -> list[str]:
        """Extract profile page links from a directory listing."""
        links = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            # Profile patterns
            if re.match(r"^/(members?|profiles?|people|startups?|companies)/[a-zA-Z0-9_\-]+/?$", href):
                slug = href.strip("/").split("/")[-1]
                if slug and slug not in self._seen_slugs:
                    self._seen_slugs.add(slug)
                    links.append(urljoin(self.BASE_URL, href))
        return links[:20]  # Cap per page

    def _parse_profile_page(self, url: str, html: str) -> ScrapedContact | None:
        """Parse an individual profile page."""
        soup = self.parse_html(html)

        name = ""
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        if not name:
            og_title = soup.find("meta", property="og:title")
            if og_title:
                name = (og_title.get("content", "") or "").split("|")[0].split("-")[0].strip()

        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_names:
            return None
        self._seen_names.add(name_key)

        bio = ""
        og_desc = soup.find("meta", property="og:description")
        if og_desc:
            bio = (og_desc.get("content", "") or "")[:1000]

        if not bio:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc:
                bio = (meta_desc.get("content", "") or "")[:1000]

        # Website
        website = ""
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            text = a_tag.get_text(strip=True).lower()
            if (
                ("website" in text or "visit" in text or "company" in text)
                and href.startswith("http")
                and "startupgrind.com" not in href.lower()
            ):
                website = href
                break

        if not website:
            website = url

        # LinkedIn
        linkedin = self.extract_linkedin(html)

        # Email
        emails = self.extract_emails(html)
        email = emails[0] if emails else ""

        return ScrapedContact(
            name=name,
            email=email,
            company=name,
            website=website,
            linkedin=linkedin,
            bio=bio or f"Listed in Startup Grind directory",
            source_url=url,
            source_category="startups",
        )
