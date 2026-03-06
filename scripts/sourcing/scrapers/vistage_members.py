"""
Vistage Chair Finder scraper — REST API version.

Vistage is the world's largest CEO coaching and peer advisory
organization with 45,000+ members. Their website lists Chairs
(facilitators/coaches) via a JSON REST API:

    GET /wp-json/vistage/v1/chairfinder/?lat=...&lng=...&radius=100

Strategy:
  - Generate a grid of lat/lng coordinates covering all major US metro
    areas and mid-size cities (100-mile radius per query).
  - The API returns JSON with chair records (name, city, mission, etc.).
  - Dedup by vistage_id since overlapping radius searches return the
    same chairs.

Estimated yield: 800–2,000 unique chairs across the US.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# ---------------------------------------------------------------------------
# Lat/lng grid covering major US metros and mid-size cities.
# 100-mile radius circles from these points blanket the continental US,
# Alaska, and Hawaii.
# ---------------------------------------------------------------------------
US_CITY_COORDS: list[tuple[str, float, float]] = [
    # Northeast
    ("New York, NY", 40.7128, -74.0060),
    ("Boston, MA", 42.3601, -71.0589),
    ("Philadelphia, PA", 39.9526, -75.1652),
    ("Pittsburgh, PA", 40.4406, -79.9959),
    ("Hartford, CT", 41.7658, -72.6734),
    ("Providence, RI", 41.8240, -71.4128),
    ("Albany, NY", 42.6526, -73.7562),
    ("Buffalo, NY", 42.8864, -78.8784),
    ("Syracuse, NY", 43.0481, -76.1474),
    ("Portland, ME", 43.6591, -70.2568),
    ("Burlington, VT", 44.4759, -73.2121),

    # Mid-Atlantic / DC corridor
    ("Washington, DC", 38.9072, -77.0369),
    ("Baltimore, MD", 39.2904, -76.6122),
    ("Richmond, VA", 37.5407, -77.4360),
    ("Virginia Beach, VA", 36.8529, -75.9780),

    # Southeast
    ("Atlanta, GA", 33.7490, -84.3880),
    ("Charlotte, NC", 35.2271, -80.8431),
    ("Raleigh, NC", 35.7796, -78.6382),
    ("Charleston, SC", 32.7765, -79.9311),
    ("Jacksonville, FL", 30.3322, -81.6557),
    ("Miami, FL", 25.7617, -80.1918),
    ("Tampa, FL", 27.9506, -82.4572),
    ("Orlando, FL", 28.5383, -81.3792),
    ("Nashville, TN", 36.1627, -86.7816),
    ("Memphis, TN", 35.1495, -90.0490),
    ("Birmingham, AL", 33.5207, -86.8025),
    ("New Orleans, LA", 29.9511, -90.0715),
    ("Savannah, GA", 32.0809, -81.0912),
    ("Knoxville, TN", 35.9606, -83.9207),

    # Midwest
    ("Chicago, IL", 41.8781, -87.6298),
    ("Detroit, MI", 42.3314, -83.0458),
    ("Minneapolis, MN", 44.9778, -93.2650),
    ("Milwaukee, WI", 43.0389, -87.9065),
    ("Cleveland, OH", 41.4993, -81.6944),
    ("Columbus, OH", 39.9612, -82.9988),
    ("Cincinnati, OH", 39.1031, -84.5120),
    ("Indianapolis, IN", 39.7684, -86.1581),
    ("St. Louis, MO", 38.6270, -90.1994),
    ("Kansas City, MO", 39.0997, -94.5786),
    ("Omaha, NE", 41.2565, -95.9345),
    ("Des Moines, IA", 41.5868, -93.6250),
    ("Madison, WI", 43.0731, -89.4012),
    ("Grand Rapids, MI", 42.9634, -85.6681),

    # South Central
    ("Dallas, TX", 32.7767, -96.7970),
    ("Houston, TX", 29.7604, -95.3698),
    ("San Antonio, TX", 29.4241, -98.4936),
    ("Austin, TX", 30.2672, -97.7431),
    ("Oklahoma City, OK", 35.4676, -97.5164),
    ("Tulsa, OK", 36.1540, -95.9928),
    ("Little Rock, AR", 34.7465, -92.2896),
    ("El Paso, TX", 31.7619, -106.4850),
    ("Lubbock, TX", 33.5779, -101.8552),

    # Mountain West
    ("Denver, CO", 39.7392, -104.9903),
    ("Salt Lake City, UT", 40.7608, -111.8910),
    ("Phoenix, AZ", 33.4484, -112.0740),
    ("Tucson, AZ", 32.2226, -110.9747),
    ("Albuquerque, NM", 35.0844, -106.6504),
    ("Las Vegas, NV", 36.1699, -115.1398),
    ("Boise, ID", 43.6150, -116.2023),
    ("Billings, MT", 45.7833, -108.5007),
    ("Missoula, MT", 46.8721, -114.0000),
    ("Cheyenne, WY", 41.1400, -104.8202),
    ("Colorado Springs, CO", 38.8339, -104.8214),

    # Pacific West
    ("Los Angeles, CA", 34.0522, -118.2437),
    ("San Francisco, CA", 37.7749, -122.4194),
    ("San Diego, CA", 32.7157, -117.1611),
    ("San Jose, CA", 37.3382, -121.8863),
    ("Sacramento, CA", 38.5816, -121.4944),
    ("Seattle, WA", 47.6062, -122.3321),
    ("Portland, OR", 45.5152, -122.6784),
    ("Spokane, WA", 47.6588, -117.4260),
    ("Eugene, OR", 44.0521, -123.0868),
    ("Fresno, CA", 36.7378, -119.7871),
    ("Reno, NV", 39.5296, -119.8138),
    ("Redding, CA", 40.5865, -122.3917),

    # Alaska & Hawaii
    ("Anchorage, AK", 61.2181, -149.9003),
    ("Honolulu, HI", 21.3069, -157.8583),

    # Additional coverage — fill gaps in less dense regions
    ("Fargo, ND", 46.8772, -96.7898),
    ("Sioux Falls, SD", 43.5460, -96.7313),
    ("Wichita, KS", 37.6872, -97.3301),
    ("Baton Rouge, LA", 30.4515, -91.1871),
    ("Jackson, MS", 32.2988, -90.1848),
    ("Charleston, WV", 38.3498, -81.6326),
    ("Lexington, KY", 38.0406, -84.5037),
    ("Louisville, KY", 38.2527, -85.7585),
    ("Wilmington, DE", 39.7391, -75.5398),
    ("Manchester, NH", 42.9956, -71.4548),
    ("Bangor, ME", 44.8016, -68.7712),
    ("Rapid City, SD", 44.0805, -103.2310),
    ("Bismarck, ND", 46.8083, -100.7837),
    ("Great Falls, MT", 47.5053, -111.3008),
]


class Scraper(BaseScraper):
    """Vistage Chair Finder scraper using the REST API."""

    SOURCE_NAME = "vistage_members"
    BASE_URL = "https://www.vistage.com"
    API_URL = "https://www.vistage.com/wp-json/vistage/v1/chairfinder/"
    REQUESTS_PER_MINUTE = 10
    SEARCH_RADIUS = 100  # miles

    TYPICAL_ROLES = ["executive coach", "CEO coach", "peer advisory chair"]
    TYPICAL_NICHES = ["executive coaching", "CEO peer groups", "leadership development"]
    TYPICAL_OFFERINGS = ["peer advisory groups", "executive coaching", "CEO mastermind"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_ids: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield API URLs for each lat/lng coordinate in the grid."""
        for city_name, lat, lng in US_CITY_COORDS:
            yield (
                f"{self.API_URL}?lat={lat}&lng={lng}"
                f"&radius={self.SEARCH_RADIUS}"
            )

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used — we override run() to call fetch_json() instead."""
        return []

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Fetch chair data from the JSON API for each coordinate.

        Overrides the base run() because the API returns JSON, not HTML.
        Uses fetch_json() and deduplicates by vistage_id.
        """
        pages_done = 0
        contacts_yielded = 0
        start_from = (checkpoint or {}).get("last_url")
        past_checkpoint = start_from is None

        self.logger.info(
            "Starting %s scraper (max_pages=%s, checkpoint=%s, %d grid points)",
            self.SOURCE_NAME,
            max_pages or "unlimited",
            start_from or "none",
            len(US_CITY_COORDS),
        )

        for url in self.generate_urls():
            if not past_checkpoint:
                if url == start_from:
                    past_checkpoint = True
                continue

            data = self.fetch_json(url)
            if data is None:
                continue

            # API may return a list directly or a dict with a results key
            chairs = []
            if isinstance(data, list):
                chairs = data
            elif isinstance(data, dict):
                chairs = (
                    data.get("results", [])
                    or data.get("chairs", [])
                    or data.get("data", [])
                )
                # If the dict itself looks like a single chair record
                if not chairs and "name" in data:
                    chairs = [data]

            for chair in chairs:
                contact = self._parse_chair(chair, url)
                if contact is None:
                    continue

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
                    "Progress: %d/%d grid points, %d unique chairs found",
                    pages_done, len(US_CITY_COORDS),
                    self.stats["contacts_valid"],
                )

            if max_pages and pages_done >= max_pages:
                self.logger.info("Reached max_pages=%d", max_pages)
                break

        self.logger.info(
            "Scraper complete: %d grid points queried, %d unique chairs. Stats: %s",
            pages_done, self.stats["contacts_valid"], self.stats,
        )

    def _parse_chair(self, chair: dict, source_url: str) -> Optional[ScrapedContact]:
        """Parse a single chair record from the API response.

        Expected fields from the API:
            name, city, mission, chair_site_url, vistage_id,
            and possibly state, email, phone, linkedin, etc.
        """
        # Dedup by vistage_id (primary) or name (fallback)
        vistage_id = str(chair.get("vistage_id") or "").strip()
        if vistage_id:
            if vistage_id in self._seen_ids:
                return None
            self._seen_ids.add(vistage_id)
        else:
            # Fallback dedup by name
            name_key = (chair.get("name") or "").strip().lower()
            if not name_key or name_key in self._seen_ids:
                return None
            self._seen_ids.add(name_key)

        name = (chair.get("name") or "").strip()
        if not name or len(name) < 2:
            return None

        # Website: prefer chair_site_url, then website, then url
        website = (
            chair.get("chair_site_url")
            or chair.get("website")
            or chair.get("url")
            or ""
        ).strip()
        if website and not website.startswith("http"):
            website = f"https://{website}"

        # Location
        city = (chair.get("city") or "").strip()
        state = (chair.get("state") or "").strip()
        location_parts = [p for p in [city, state] if p]
        location = ", ".join(location_parts)

        # Mission / bio
        mission = (chair.get("mission") or "").strip()
        # Strip HTML tags from mission text
        if mission:
            mission = re.sub(r"<[^>]+>", " ", mission)
            mission = re.sub(r"\s+", " ", mission).strip()

        title = (chair.get("title") or chair.get("headline") or "").strip()

        bio_parts = ["Vistage Chair | Executive Peer Advisory"]
        if title:
            bio_parts.append(title[:150])
        if location:
            bio_parts.append(location)
        if mission:
            bio_parts.append(mission[:500])

        email = (chair.get("email") or "").strip()
        phone = (chair.get("phone") or chair.get("telephone") or "").strip()
        linkedin = (chair.get("linkedin") or chair.get("linkedin_url") or "").strip()

        categories = (chair.get("specialties") or chair.get("expertise") or "").strip()

        return ScrapedContact(
            name=name,
            email=email,
            company="Vistage",
            website=website,
            linkedin=linkedin,
            phone=phone,
            bio=" | ".join(bio_parts),
            location=location,
            categories=categories or "executive_coaching",
            source_url=source_url,
            source_category="executive_coaching",
            raw_data=chair,
        )
