"""
Meetup.com group organizer scraper.

Meetup hosts millions of groups. We target business, tech, and
professional networking groups to find organizers who are often
entrepreneurs, consultants, or community leaders.

Strategy: Use Meetup's GraphQL API (api.meetup.com/gql) to search
for groups by category and location, then extract organizer info.

Meetup API v3 is deprecated but the GraphQL API is publicly accessible
for search queries without authentication.

Fallback: Parse HTML listing pages at meetup.com/find/?keywords=...

Estimated yield: 5,000-20,000 organizers
"""

from __future__ import annotations

import json
import re
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Business/tech/professional categories
SEARCH_TERMS = [
    "business networking",
    "entrepreneur",
    "startup",
    "small business",
    "marketing",
    "digital marketing",
    "real estate investing",
    "business consulting",
    "leadership",
    "executive coaching",
    "sales professionals",
    "technology",
    "web development",
    "data science",
    "artificial intelligence",
    "blockchain",
    "fintech",
    "product management",
    "project management",
    "agile scrum",
    "UX design",
    "cloud computing",
    "cybersecurity",
    "venture capital",
    "angel investing",
    "women in business",
    "black professionals",
    "hispanic business",
    "young professionals",
    "freelancers",
    "coworking",
    "public speaking",
    "professional development",
    "career networking",
    "B2B",
    "SaaS",
    "ecommerce",
    "social media marketing",
    "content creators",
    "podcasters",
]

# Major US cities (lat, lng)
LOCATIONS = [
    (40.7128, -74.0060, "New York"),
    (34.0522, -118.2437, "Los Angeles"),
    (41.8781, -87.6298, "Chicago"),
    (29.7604, -95.3698, "Houston"),
    (33.4484, -112.0740, "Phoenix"),
    (37.7749, -122.4194, "San Francisco"),
    (47.6062, -122.3321, "Seattle"),
    (39.7392, -104.9903, "Denver"),
    (25.7617, -80.1918, "Miami"),
    (33.7490, -84.3880, "Atlanta"),
    (42.3601, -71.0589, "Boston"),
    (38.9072, -77.0369, "Washington DC"),
    (30.2672, -97.7431, "Austin"),
    (32.7767, -96.7970, "Dallas"),
    (45.5152, -122.6784, "Portland"),
    (36.1627, -86.7816, "Nashville"),
    (44.9778, -93.2650, "Minneapolis"),
    (35.2271, -80.8431, "Charlotte"),
    (36.1699, -115.1398, "Las Vegas"),
    (39.9526, -75.1652, "Philadelphia"),
]


class Scraper(BaseScraper):
    SOURCE_NAME = "meetup_organizers"
    BASE_URL = "https://www.meetup.com"
    REQUESTS_PER_MINUTE = 5

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_groups: set[str] = set()
        self._seen_organizers: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield Meetup search URLs."""
        # HTML search pages (fallback, always works)
        for term in SEARCH_TERMS:
            encoded_term = term.replace(" ", "+")
            yield f"{self.BASE_URL}/find/?keywords={encoded_term}&source=EVENTS"

        # Location-based searches
        for lat, lng, city in LOCATIONS:
            for term in SEARCH_TERMS[:10]:  # Top terms per city
                encoded_term = term.replace(" ", "+")
                yield (
                    f"{self.BASE_URL}/find/"
                    f"?keywords={encoded_term}"
                    f"&lat={lat}&lon={lng}"
                    f"&source=GROUPS"
                )

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a Meetup search results page."""
        soup = self.parse_html(html)
        contacts = []

        # Try Next.js data (Meetup uses Next.js)
        next_data = soup.find("script", id="__NEXT_DATA__")
        if next_data and next_data.string:
            try:
                data = json.loads(next_data.string)
                contacts = self._parse_next_data(data, url)
                if contacts:
                    return contacts
            except (json.JSONDecodeError, KeyError):
                pass

        # Try embedded Apollo state (GraphQL cache)
        for script in soup.find_all("script"):
            text = script.string or ""
            if "__APOLLO_STATE__" in text or "apolloState" in text:
                try:
                    match = re.search(r'window\.__APOLLO_STATE__\s*=\s*(\{.*?\});', text, re.S)
                    if match:
                        apollo_data = json.loads(match.group(1))
                        contacts = self._parse_apollo_state(apollo_data, url)
                        if contacts:
                            return contacts
                except (json.JSONDecodeError, KeyError):
                    pass

        # Parse group cards from HTML
        for card in soup.find_all(class_=re.compile(
            r"group|event|result|card|listing", re.I
        )):
            contact = self._parse_group_card(card, url)
            if contact:
                contacts.append(contact)

        # Follow group links to get organizer info
        group_links = set()
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            # Meetup group URLs: /group-name/ or meetup.com/group-name/
            if re.match(r"^/[a-zA-Z0-9\-]+/?$", href):
                # Exclude common non-group paths
                slug = href.strip("/")
                if slug and slug not in (
                    "find", "topics", "cities", "about", "pro",
                    "apps", "help", "blog", "privacy", "terms",
                    "signin", "register", "account",
                ) and not slug.startswith("find"):
                    full_url = f"{self.BASE_URL}/{slug}/"
                    if slug not in self._seen_groups:
                        self._seen_groups.add(slug)
                        group_links.add(full_url)

        for group_url in list(group_links)[:20]:  # Cap to avoid too many requests
            group_html = self.fetch_page(group_url)
            if group_html:
                group_contacts = self._parse_group_page(group_url, group_html)
                contacts.extend(group_contacts)

        return contacts

    def _parse_next_data(self, data: dict, source_url: str) -> list[ScrapedContact]:
        """Extract group/organizer data from Next.js __NEXT_DATA__."""
        contacts = []
        props = data.get("props", {}).get("pageProps", {})

        # Navigate to search results
        results = (
            props.get("results", [])
            or props.get("groups", [])
            or props.get("searchResults", {}).get("edges", [])
        )

        for result in results:
            node = result.get("node", result)
            group_name = (node.get("name") or node.get("title") or "").strip()
            if not group_name:
                continue

            group_slug = (node.get("urlname") or node.get("slug") or "").strip()
            dedup_key = group_slug or group_name.lower()
            if dedup_key in self._seen_groups:
                continue
            self._seen_groups.add(dedup_key)

            organizer = node.get("organizer", {})
            organizer_name = (organizer.get("name") or "").strip()
            member_count = node.get("memberships", {}).get("count") or node.get("memberCount") or ""

            city = (node.get("city") or "").strip()
            state = (node.get("state") or "").strip()
            country = (node.get("country") or "").strip()
            location_parts = [p for p in [city, state, country] if p]
            location = ", ".join(location_parts)

            description = (node.get("description") or "")[:500]

            group_url = f"{self.BASE_URL}/{group_slug}/" if group_slug else source_url

            # Build bio
            bio_parts = ["Meetup Group"]
            if location:
                bio_parts.append(location)
            if member_count:
                bio_parts.append(f"{member_count} members")
            if description:
                # Strip HTML from description
                clean_desc = re.sub(r"<[^>]+>", " ", description).strip()[:200]
                bio_parts.append(clean_desc)

            # Add the group as a contact
            contacts.append(ScrapedContact(
                name=group_name,
                email="",
                company=organizer_name,
                website=group_url,
                linkedin="",
                phone="",
                bio=" | ".join(bio_parts),
                source_url=source_url,
                source_category="networking",
            ))

            # Also add organizer as separate contact if available
            if organizer_name and len(organizer_name) > 2:
                org_key = organizer_name.lower()
                if org_key not in self._seen_organizers:
                    self._seen_organizers.add(org_key)
                    contacts.append(ScrapedContact(
                        name=organizer_name,
                        email="",
                        company="",
                        website=group_url,
                        linkedin="",
                        phone="",
                        bio=f"Meetup Organizer | {group_name} | {location}",
                        source_url=source_url,
                        source_category="networking",
                    ))

        return contacts

    def _parse_apollo_state(self, data: dict, source_url: str) -> list[ScrapedContact]:
        """Extract group data from Apollo GraphQL cache."""
        contacts = []

        for key, value in data.items():
            if not isinstance(value, dict):
                continue
            if value.get("__typename") not in ("Group", "SearchResult"):
                continue

            name = (value.get("name") or "").strip()
            if not name:
                continue

            slug = (value.get("urlname") or "").strip()
            dedup_key = slug or name.lower()
            if dedup_key in self._seen_groups:
                continue
            self._seen_groups.add(dedup_key)

            city = (value.get("city") or "").strip()
            group_url = f"{self.BASE_URL}/{slug}/" if slug else source_url

            bio_parts = ["Meetup Group"]
            if city:
                bio_parts.append(city)

            contacts.append(ScrapedContact(
                name=name,
                email="",
                company="",
                website=group_url,
                linkedin="",
                phone="",
                bio=" | ".join(bio_parts),
                source_url=source_url,
                source_category="networking",
            ))

        return contacts

    def _parse_group_card(self, card, source_url: str) -> ScrapedContact | None:
        """Parse a group card element."""
        name = ""
        website = ""

        for tag in ["h2", "h3", "h4"]:
            el = card.find(tag)
            if el:
                link = el.find("a", href=True)
                if link:
                    name = link.get_text(strip=True)
                    href = link.get("href", "")
                    website = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                else:
                    name = el.get_text(strip=True)
                if name and len(name) > 2 and len(name) < 150:
                    break
                name = ""

        if not name or len(name) < 3:
            return None

        name_key = name.lower()
        if name_key in self._seen_groups:
            return None
        self._seen_groups.add(name_key)

        # Members count
        members = ""
        members_el = card.find(class_=re.compile(r"member|attendee", re.I))
        if members_el:
            members = members_el.get_text(strip=True)

        location = ""
        loc_el = card.find(class_=re.compile(r"location|city|venue", re.I))
        if loc_el:
            location = loc_el.get_text(strip=True)[:100]

        bio_parts = ["Meetup Group"]
        if location:
            bio_parts.append(location)
        if members:
            bio_parts.append(members)

        return ScrapedContact(
            name=name,
            email="",
            company="",
            website=website or source_url,
            linkedin="",
            phone="",
            bio=" | ".join(bio_parts),
            source_url=source_url,
            source_category="networking",
        )

    def _parse_group_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a Meetup group page for organizer info."""
        soup = self.parse_html(html)
        contacts = []

        group_name = ""
        h1 = soup.find("h1")
        if h1:
            group_name = h1.get_text(strip=True)

        if not group_name:
            og = soup.find("meta", property="og:title")
            if og:
                group_name = (og.get("content") or "").split("|")[0].strip()

        # Find organizer section
        for section in soup.find_all(class_=re.compile(r"organizer|host|leader", re.I)):
            org_name = ""
            for tag in ["h3", "h4", "strong", "span", "a"]:
                el = section.find(tag)
                if el:
                    text = el.get_text(strip=True)
                    # Skip labels like "Organized by"
                    if text and len(text) > 2 and len(text) < 100 and "organized" not in text.lower():
                        org_name = text
                        break

            if org_name:
                org_key = org_name.lower()
                if org_key not in self._seen_organizers:
                    self._seen_organizers.add(org_key)
                    contacts.append(ScrapedContact(
                        name=org_name,
                        email="",
                        company="",
                        website=url,
                        linkedin="",
                        phone="",
                        bio=f"Meetup Organizer | {group_name}" if group_name else "Meetup Organizer",
                        source_url=url,
                        source_category="networking",
                    ))

        return contacts
