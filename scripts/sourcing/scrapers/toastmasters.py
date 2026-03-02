"""
Toastmasters International club directory scraper.

Toastmasters has ~16,000 clubs worldwide. The Find a Club tool at
toastmasters.org/find-a-club uses a search API that returns club
data including name, location, meeting details, and charter date.

Strategy: Use the Toastmasters Find API at
  https://www.toastmasters.org/api/sitecore/FindAClub/Search
which accepts POST requests with location/keyword parameters
and returns JSON with club listings.

Estimated yield: 10,000-16,000 clubs
"""

from __future__ import annotations

import json
import re
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Search parameters: lat/lng for major US metros to sweep the country
# Toastmasters search has a radius parameter, using 100mi per point
SEARCH_POINTS = [
    # US major metros (lat, lng, label)
    (40.7128, -74.0060, "New York"),
    (34.0522, -118.2437, "Los Angeles"),
    (41.8781, -87.6298, "Chicago"),
    (29.7604, -95.3698, "Houston"),
    (33.4484, -112.0740, "Phoenix"),
    (39.9526, -75.1652, "Philadelphia"),
    (29.4241, -98.4936, "San Antonio"),
    (32.7157, -117.1611, "San Diego"),
    (32.7767, -96.7970, "Dallas"),
    (30.2672, -97.7431, "Austin"),
    (37.7749, -122.4194, "San Francisco"),
    (47.6062, -122.3321, "Seattle"),
    (39.7392, -104.9903, "Denver"),
    (36.1627, -86.7816, "Nashville"),
    (25.7617, -80.1918, "Miami"),
    (33.7490, -84.3880, "Atlanta"),
    (42.3601, -71.0589, "Boston"),
    (38.9072, -77.0369, "Washington DC"),
    (45.5152, -122.6784, "Portland"),
    (36.1699, -115.1398, "Las Vegas"),
    (44.9778, -93.2650, "Minneapolis"),
    (35.2271, -80.8431, "Charlotte"),
    (39.9612, -82.9988, "Columbus"),
    (39.1031, -84.5120, "Cincinnati"),
    (40.4406, -79.9959, "Pittsburgh"),
    (38.2527, -85.7585, "Louisville"),
    (35.1495, -90.0490, "Memphis"),
    (35.4676, -97.5164, "Oklahoma City"),
    (43.0389, -87.9065, "Milwaukee"),
    (36.7783, -119.4179, "Central California"),
    (27.9506, -82.4572, "Tampa"),
    (28.5383, -81.3792, "Orlando"),
    (37.5407, -77.4360, "Richmond"),
    (39.2904, -76.6122, "Baltimore"),
    (40.7608, -111.8910, "Salt Lake City"),
    (35.7796, -78.6382, "Raleigh"),
    (38.6270, -90.1994, "St Louis"),
    (39.0997, -94.5786, "Kansas City"),
    (41.2565, -95.9345, "Omaha"),
    (21.3069, -157.8583, "Honolulu"),
    # International major cities
    (51.5074, -0.1278, "London"),
    (43.6532, -79.3832, "Toronto"),
    (-33.8688, 151.2093, "Sydney"),
    (19.0760, 72.8777, "Mumbai"),
    (1.3521, 103.8198, "Singapore"),
    (49.2827, -123.1207, "Vancouver"),
    (52.5200, 13.4050, "Berlin"),
    (-23.5505, -46.6333, "Sao Paulo"),
    (35.6762, 139.6503, "Tokyo"),
    (55.7558, 37.6173, "Moscow"),
]

# Keyword searches to complement geographic search
KEYWORDS = [
    "business",
    "corporate",
    "leadership",
    "professional",
    "executive",
    "advanced",
    "community",
    "university",
    "online",
    "virtual",
]


class Scraper(BaseScraper):
    SOURCE_NAME = "toastmasters"
    BASE_URL = "https://www.toastmasters.org"
    REQUESTS_PER_MINUTE = 6

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_clubs: set[str] = set()
        # Update headers for API calls
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://www.toastmasters.org",
            "Referer": "https://www.toastmasters.org/find-a-club",
        })

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield synthetic URLs for API calls."""
        # Geographic search
        for lat, lng, label in SEARCH_POINTS:
            yield f"__api__:lat={lat}&lng={lng}&label={label}"

        # Keyword search
        for keyword in KEYWORDS:
            yield f"__api__:keyword={keyword}"

        # Also try the HTML Find a Club page and sitemap
        yield f"{self.BASE_URL}/find-a-club"
        yield f"{self.BASE_URL}/sitemap.xml"

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Override run() to handle both API and HTML URLs."""
        from datetime import datetime

        pages_done = 0
        contacts_yielded = 0
        start_from = (checkpoint or {}).get("last_url")
        past_checkpoint = start_from is None

        self.logger.info(
            "Starting %s scraper (max_pages=%s, max_contacts=%s)",
            self.SOURCE_NAME,
            max_pages or "unlimited",
            max_contacts or "unlimited",
        )

        for url in self.generate_urls():
            if not past_checkpoint:
                if url == start_from:
                    past_checkpoint = True
                continue

            if url.startswith("__api__:"):
                contacts = self._handle_api_url(url)
            elif url.endswith("sitemap.xml"):
                html = self.fetch_page(url)
                contacts = self._parse_sitemap(html) if html else []
            else:
                html = self.fetch_page(url)
                contacts = self.scrape_page(url, html) if html else []

            for contact in contacts:
                contact.source_platform = self.SOURCE_NAME
                contact.source_url = url
                contact.scraped_at = datetime.now().isoformat()
                contact.email = contact.clean_email()

                if contact.is_valid():
                    self.stats["contacts_valid"] += 1
                    contacts_yielded += 1
                    yield contact

                    if max_contacts and contacts_yielded >= max_contacts:
                        self.logger.info("Reached max_contacts=%d", max_contacts)
                        return

                self.stats["contacts_found"] += 1

            pages_done += 1
            if pages_done % 10 == 0:
                self.logger.info(
                    "Progress: %d pages, %d valid contacts, %d clubs seen",
                    pages_done, self.stats["contacts_valid"], len(self._seen_clubs),
                )

            if max_pages and pages_done >= max_pages:
                self.logger.info("Reached max_pages=%d", max_pages)
                break

        self.logger.info("Scraper complete: %s", self.stats)

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse HTML club finder pages."""
        soup = self.parse_html(html)
        contacts = []

        # Try Next.js data
        next_data = soup.find("script", id="__NEXT_DATA__")
        if next_data and next_data.string:
            try:
                data = json.loads(next_data.string)
                props = data.get("props", {}).get("pageProps", {})
                clubs = props.get("clubs", []) or props.get("results", [])
                for club in clubs:
                    contact = self._parse_club_data(club, url)
                    if contact:
                        contacts.append(contact)
            except (json.JSONDecodeError, KeyError):
                pass

        # Parse club cards from HTML
        for card in soup.find_all(class_=re.compile(r"club|chapter|result|card", re.I)):
            contact = self._parse_club_card(card, url)
            if contact:
                contacts.append(contact)

        return contacts

    def _handle_api_url(self, url: str) -> list[ScrapedContact]:
        """Handle API search URLs."""
        params = {}
        param_str = url[len("__api__:"):]
        for part in param_str.split("&"):
            if "=" in part:
                key, value = part.split("=", 1)
                params[key] = value

        contacts = []

        if "lat" in params and "lng" in params:
            results = self._search_by_location(
                float(params["lat"]),
                float(params["lng"]),
                params.get("label", ""),
            )
        elif "keyword" in params:
            results = self._search_by_keyword(params["keyword"])
        else:
            results = []

        for club in results:
            contact = self._parse_club_data(club, url)
            if contact:
                contacts.append(contact)

        return contacts

    def _search_by_location(self, lat: float, lng: float, label: str) -> list:
        """Search clubs by geographic location."""
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        # Try the Toastmasters API
        api_url = f"{self.BASE_URL}/api/sitecore/FindAClub/Search"

        payload = {
            "latitude": lat,
            "longitude": lng,
            "distance": 100,  # miles
            "distanceUnit": "miles",
            "advanced": False,
            "clubName": "",
            "clubNumber": "",
            "areaNumber": "",
            "districtNumber": "",
            "programType": "",
            "clubStatus": "Open",
        }

        try:
            resp = self.session.post(api_url, json=payload, timeout=30)
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1
            data = resp.json()
            clubs = data if isinstance(data, list) else data.get("clubs", [])
            self.logger.debug("Location search %s: %d clubs", label, len(clubs))
            return clubs
        except Exception as exc:
            # Fallback: try a GET-based search
            self.logger.debug("API POST failed for %s: %s, trying GET", label, exc)
            return self._search_get_fallback(lat, lng)

    def _search_get_fallback(self, lat: float, lng: float) -> list:
        """Fallback GET-based search."""
        search_url = (
            f"{self.BASE_URL}/find-a-club"
            f"?latitude={lat}&longitude={lng}&distance=100"
        )
        html = self.fetch_page(search_url)
        if not html:
            return []

        # Try to extract club data from the HTML response
        clubs = []
        soup = self.parse_html(html)

        # Look for embedded JSON data
        for script in soup.find_all("script"):
            text = script.string or ""
            if "clubName" in text or "ClubName" in text:
                try:
                    # Find JSON arrays in script
                    for match in re.finditer(r'\[(\{[^]]+\})\]', text):
                        arr = json.loads(f"[{match.group(1)}]")
                        clubs.extend(arr)
                except (json.JSONDecodeError, KeyError):
                    pass

        return clubs

    def _search_by_keyword(self, keyword: str) -> list:
        """Search clubs by keyword."""
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        api_url = f"{self.BASE_URL}/api/sitecore/FindAClub/Search"
        payload = {
            "clubName": keyword,
            "advanced": False,
            "clubStatus": "Open",
        }

        try:
            resp = self.session.post(api_url, json=payload, timeout=30)
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1
            data = resp.json()
            return data if isinstance(data, list) else data.get("clubs", [])
        except Exception as exc:
            self.logger.warning("Keyword search failed for '%s': %s", keyword, exc)
            self.stats["errors"] += 1
            return []

    def _parse_club_data(self, data: dict, source_url: str) -> ScrapedContact | None:
        """Parse club data from API response."""
        club_name = (
            data.get("clubName")
            or data.get("name")
            or data.get("ClubName")
            or ""
        ).strip()

        if not club_name:
            return None

        club_number = str(data.get("clubNumber") or data.get("ClubNumber") or "").strip()

        # Deduplicate by club number or name
        dedup_key = club_number or club_name.lower()
        if dedup_key in self._seen_clubs:
            return None
        self._seen_clubs.add(dedup_key)

        # Location
        city = (data.get("city") or data.get("City") or "").strip()
        state = (data.get("state") or data.get("State") or "").strip()
        country = (data.get("country") or data.get("Country") or "").strip()
        location_parts = [p for p in [city, state, country] if p]
        location = ", ".join(location_parts)

        # Meeting info
        meeting_day = (data.get("meetingDay") or data.get("MeetingDay") or "").strip()
        meeting_time = (data.get("meetingTime") or data.get("MeetingTime") or "").strip()
        meeting_location = (data.get("meetingLocation") or data.get("MeetingLocation") or "").strip()

        # Website
        website = (data.get("website") or data.get("Website") or "").strip()
        if website and not website.startswith("http"):
            website = f"https://{website}"

        # Email
        email = (data.get("email") or data.get("Email") or "").strip()

        # Phone
        phone = (data.get("phone") or data.get("Phone") or "").strip()

        # Build bio
        bio_parts = ["Toastmasters Club"]
        if club_number:
            bio_parts.append(f"Club #{club_number}")
        if location:
            bio_parts.append(location)
        if meeting_day:
            meeting_info = meeting_day
            if meeting_time:
                meeting_info += f" {meeting_time}"
            bio_parts.append(f"Meets: {meeting_info}")

        return ScrapedContact(
            name=club_name,
            email=email,
            company="Toastmasters International",
            website=website or source_url,
            linkedin="",
            phone=phone,
            bio=" | ".join(bio_parts),
            source_url=source_url,
            source_category="professional_development",
            raw_data={
                "club_number": club_number,
                "city": city,
                "state": state,
                "country": country,
                "meeting_day": meeting_day,
                "meeting_time": meeting_time,
                "meeting_location": meeting_location,
            },
        )

    def _parse_club_card(self, card, source_url: str) -> ScrapedContact | None:
        """Parse a club card from HTML."""
        name = ""
        for tag in ["h2", "h3", "h4", "strong"]:
            el = card.find(tag)
            if el:
                name = el.get_text(strip=True)
                if name and len(name) > 2 and len(name) < 150:
                    break
                name = ""

        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_clubs:
            return None
        self._seen_clubs.add(name_key)

        location = ""
        loc_el = card.find(class_=re.compile(r"location|address|city", re.I))
        if loc_el:
            location = loc_el.get_text(strip=True)[:100]

        website = ""
        for a in card.find_all("a", href=True):
            href = a.get("href", "")
            if href.startswith("http") and "toastmasters.org" not in href.lower():
                website = href
                break

        bio_parts = ["Toastmasters Club"]
        if location:
            bio_parts.append(location)

        return ScrapedContact(
            name=name,
            email="",
            company="Toastmasters International",
            website=website or source_url,
            linkedin="",
            phone="",
            bio=" | ".join(bio_parts),
            source_url=source_url,
            source_category="professional_development",
        )

    def _parse_sitemap(self, xml_text: str) -> list[ScrapedContact]:
        """Extract club URLs from sitemap."""
        contacts = []
        urls = re.findall(r"<loc>(https?://[^<]+)</loc>", xml_text)

        club_urls = [u for u in urls if "/club/" in u or "/find-a-club/" in u]
        self.logger.info("Sitemap: found %d club URLs", len(club_urls))

        for club_url in club_urls[:300]:
            slug = club_url.rstrip("/").split("/")[-1]
            if slug in self._seen_clubs:
                continue
            self._seen_clubs.add(slug)

            html = self.fetch_page(club_url)
            if html:
                page_contacts = self.scrape_page(club_url, html)
                contacts.extend(page_contacts)

        return contacts
