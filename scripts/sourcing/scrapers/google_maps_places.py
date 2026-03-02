"""
Google Maps / Places API Scraper

Fetches business listings from Google Places API for JV-relevant categories.
Requires GOOGLE_PLACES_API_KEY in environment.

If no API key is available, falls back to scraping Google Maps search
result pages (HTML mode), which extracts business names and basic info
from the publicly rendered search results.

Categories targeted:
- Consultants, coaches, marketing agencies, financial advisors
- Business services, training, PR firms, IT services

Strategy:
  - Use Places Text Search API to query each category in major metros
  - Extract: name, phone, website, rating, address, place_id
  - Paginate via next_page_token (up to 60 results per query)
  - Deduplicate by place_id to avoid repeats across overlapping metros

Estimated yield: 100,000-500,000 businesses (API mode, depends on key limits).

Note: Google Places API charges per request. Free tier = $200/month credit.
Text Search costs $0.032 per request. Budget accordingly.
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.parse
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# JV-relevant search terms for Google Places
SEARCH_TERMS = [
    "business consultant",
    "marketing agency",
    "financial advisor",
    "life coach",
    "business coach",
    "management consultant",
    "PR agency",
    "IT services company",
    "web design agency",
    "advertising agency",
    "SEO agency",
    "social media marketing agency",
    "executive coach",
    "accounting firm",
    "insurance agency",
    "real estate agency",
    "event planning company",
    "graphic design studio",
    "video production company",
    "training company",
    "staffing agency",
    "HR consulting firm",
    "legal services",
    "tax preparation services",
]

# Major US metro areas with approximate lat/lng for location bias
METRO_LOCATIONS = [
    ("New York, NY", "40.7128,-74.0060"),
    ("Los Angeles, CA", "34.0522,-118.2437"),
    ("Chicago, IL", "41.8781,-87.6298"),
    ("Houston, TX", "29.7604,-95.3698"),
    ("Phoenix, AZ", "33.4484,-112.0740"),
    ("Philadelphia, PA", "39.9526,-75.1652"),
    ("San Antonio, TX", "29.4241,-98.4936"),
    ("San Diego, CA", "32.7157,-117.1611"),
    ("Dallas, TX", "32.7767,-96.7970"),
    ("San Jose, CA", "37.3382,-121.8863"),
    ("Austin, TX", "30.2672,-97.7431"),
    ("San Francisco, CA", "37.7749,-122.4194"),
    ("Seattle, WA", "47.6062,-122.3321"),
    ("Denver, CO", "39.7392,-104.9903"),
    ("Washington, DC", "38.9072,-77.0369"),
    ("Nashville, TN", "36.1627,-86.7816"),
    ("Boston, MA", "42.3601,-71.0589"),
    ("Atlanta, GA", "33.7490,-84.3880"),
    ("Miami, FL", "25.7617,-80.1918"),
    ("Minneapolis, MN", "44.9778,-93.2650"),
    ("Portland, OR", "45.5152,-122.6784"),
    ("Las Vegas, NV", "36.1699,-115.1398"),
    ("Tampa, FL", "27.9506,-82.4572"),
    ("Charlotte, NC", "35.2271,-80.8431"),
    ("Pittsburgh, PA", "40.4406,-79.9959"),
    ("Orlando, FL", "28.5383,-81.3792"),
    ("Detroit, MI", "42.3314,-83.0458"),
    ("Salt Lake City, UT", "40.7608,-111.8910"),
    ("Raleigh, NC", "35.7796,-78.6382"),
    ("Cleveland, OH", "41.4993,-81.6944"),
    ("St. Louis, MO", "38.6270,-90.1994"),
    ("Kansas City, MO", "39.0997,-94.5786"),
    ("Indianapolis, IN", "39.7684,-86.1581"),
    ("Columbus, OH", "39.9612,-82.9988"),
    ("Cincinnati, OH", "39.1031,-84.5120"),
    ("Sacramento, CA", "38.5816,-121.4944"),
    ("Milwaukee, WI", "43.0389,-87.9065"),
    ("Jacksonville, FL", "30.3322,-81.6557"),
    ("New Orleans, LA", "29.9511,-90.0715"),
    ("Oklahoma City, OK", "35.4676,-97.5164"),
]

# Google Places API
PLACES_TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"


class Scraper(BaseScraper):
    SOURCE_NAME = "google_maps_places"
    BASE_URL = "https://maps.googleapis.com"
    REQUESTS_PER_MINUTE = 30  # API can handle more; stay moderate

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._api_key = os.environ.get("GOOGLE_PLACES_API_KEY", "")
        self._seen_place_ids: set[str] = set()
        self._seen_names: set[str] = set()

        if self._api_key:
            self.logger.info("Google Places API key found — using API mode")
        else:
            self.logger.info(
                "No GOOGLE_PLACES_API_KEY — using HTML fallback. "
                "Set GOOGLE_PLACES_API_KEY in .env for best results."
            )

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Generate Google Maps search URLs for HTML fallback mode."""
        for term in SEARCH_TERMS:
            for metro_name, _latlng in METRO_LOCATIONS:
                query = f"{term} near {metro_name}"
                encoded = urllib.parse.quote(query)
                yield f"https://www.google.com/maps/search/{encoded}/"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Google Maps search results HTML (fallback mode).

        Google Maps is heavily JS-rendered, so the HTML fallback
        extracts whatever structured data is available in the
        initial server-rendered response.
        """
        contacts = []

        # Try to extract any JSON-LD structured data
        soup = self.parse_html(html)
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
            except (json.JSONDecodeError, TypeError):
                continue

            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") in ("LocalBusiness", "Organization", "ProfessionalService"):
                    contact = self._parse_jsonld(item, url)
                    if contact:
                        contacts.append(contact)

        # Try parsing visible text for business-like patterns
        # Google Maps inlines some data in the initial HTML payload
        if not contacts:
            # Look for patterns in the page source that contain business data
            # Google sometimes includes data in JavaScript variables
            for match in re.finditer(
                r'\["([^"]{3,100})","[^"]*","[^"]*",\[null,null,(-?\d+\.\d+),(-?\d+\.\d+)\]',
                html,
            ):
                name = match.group(1).strip()
                if not name or len(name) < 3:
                    continue

                dedup_key = name.lower().replace(" ", "")
                if dedup_key in self._seen_names:
                    continue
                self._seen_names.add(dedup_key)

                contacts.append(ScrapedContact(
                    name=name,
                    email="",
                    company=name,
                    website="",
                    phone="",
                    bio=name,
                    source_category="google_maps",
                    raw_data={"lat": match.group(2), "lng": match.group(3)},
                ))

        return contacts

    def _parse_jsonld(self, item: dict, source_url: str) -> Optional[ScrapedContact]:
        """Parse JSON-LD structured data."""
        name = (item.get("name") or "").strip()
        if not name or len(name) < 2:
            return None

        dedup_key = name.lower().replace(" ", "")
        if dedup_key in self._seen_names:
            return None
        self._seen_names.add(dedup_key)

        phone = (item.get("telephone") or "").strip()
        website = (item.get("url") or "").strip()

        address_obj = item.get("address") or {}
        city = (address_obj.get("addressLocality") or "").strip()
        state = (address_obj.get("addressRegion") or "").strip()

        aggregate = item.get("aggregateRating") or {}
        rating = aggregate.get("ratingValue", "")

        bio_parts = [name]
        if city and state:
            bio_parts.append(f"{city}, {state}")
        if rating:
            bio_parts.append(f"Rating: {rating}/5")
        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=name,
            email="",
            company=name,
            website=website,
            phone=phone,
            bio=bio,
            source_category="google_maps",
            raw_data={
                "city": city,
                "state": state,
                "rating": str(rating),
            },
        )

    # ------------------------------------------------------------------
    # Google Places API mode
    # ------------------------------------------------------------------

    def _api_text_search(
        self,
        query: str,
        location: str = "",
        page_token: str = "",
    ) -> Optional[dict]:
        """Execute a Google Places Text Search API call."""
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        params = {
            "query": query,
            "key": self._api_key,
        }
        if location:
            params["location"] = location
            params["radius"] = "50000"  # 50km radius
        if page_token:
            params["pagetoken"] = page_token

        try:
            resp = self.session.get(PLACES_TEXT_SEARCH_URL, params=params, timeout=30)
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1
            data = resp.json()

            status = data.get("status", "")
            if status not in ("OK", "ZERO_RESULTS"):
                self.logger.warning("Places API status: %s for query: %s", status, query)
                if status == "OVER_QUERY_LIMIT":
                    self.logger.error("API quota exceeded — stopping")
                    return None

            return data
        except Exception as exc:
            self.logger.warning("Places API error for '%s': %s", query, exc)
            self.stats["errors"] += 1
            return None

    def _api_get_details(self, place_id: str) -> Optional[dict]:
        """Fetch additional details for a place (phone, website)."""
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        params = {
            "place_id": place_id,
            "fields": "name,formatted_phone_number,website,url,formatted_address,rating,user_ratings_total,types,business_status",
            "key": self._api_key,
        }

        try:
            resp = self.session.get(PLACES_DETAILS_URL, params=params, timeout=30)
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1
            data = resp.json()
            return data.get("result") if data.get("status") == "OK" else None
        except Exception as exc:
            self.logger.debug("Details fetch failed for %s: %s", place_id, exc)
            self.stats["errors"] += 1
            return None

    def _api_result_to_contact(
        self,
        place: dict,
        search_term: str,
        metro_name: str,
        fetch_details: bool = True,
    ) -> Optional[ScrapedContact]:
        """Convert a Places API result to ScrapedContact."""
        place_id = (place.get("place_id") or "").strip()
        name = (place.get("name") or "").strip()
        if not name or len(name) < 2:
            return None

        # Deduplicate by place_id
        if place_id:
            if place_id in self._seen_place_ids:
                return None
            self._seen_place_ids.add(place_id)
        else:
            dedup_key = name.lower().replace(" ", "")
            if dedup_key in self._seen_names:
                return None
            self._seen_names.add(dedup_key)

        # Skip permanently closed businesses
        business_status = (place.get("business_status") or "").strip()
        if business_status == "CLOSED_PERMANENTLY":
            return None

        address = (place.get("formatted_address") or "").strip()
        rating = place.get("rating", 0) or 0
        user_ratings_total = place.get("user_ratings_total", 0) or 0
        types = place.get("types") or []

        # Basic info from search results
        phone = ""
        website = ""

        # Optionally fetch details for phone + website
        if fetch_details and place_id:
            details = self._api_get_details(place_id)
            if details:
                phone = (details.get("formatted_phone_number") or "").strip()
                website = (details.get("website") or "").strip()
                # Update with more complete data if available
                if details.get("formatted_address"):
                    address = details["formatted_address"]
                if details.get("rating"):
                    rating = details["rating"]
                if details.get("user_ratings_total"):
                    user_ratings_total = details["user_ratings_total"]

        # If no website from details, use Google Maps URL
        if not website:
            website = f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id else ""

        # Parse city/state from address
        city = ""
        state = ""
        if address:
            addr_parts = address.split(",")
            if len(addr_parts) >= 2:
                city = addr_parts[-2].strip() if len(addr_parts) >= 3 else ""
                state_zip = addr_parts[-1].strip()
                state_match = re.match(r"([A-Z]{2})\s*\d{5}(-\d{4})?", state_zip)
                if state_match:
                    state = state_match.group(1)

        # Bio
        bio_parts = [name]
        if city and state:
            bio_parts.append(f"{city}, {state}")
        elif address:
            bio_parts.append(address[:80])
        if rating:
            bio_parts.append(f"Rating: {rating}/5")
        if user_ratings_total:
            bio_parts.append(f"{user_ratings_total} reviews")
        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=name,
            email="",
            company=name,
            website=website,
            phone=phone,
            bio=bio,
            source_category="google_maps",
            raw_data={
                "place_id": place_id,
                "address": address,
                "city": city,
                "state": state,
                "rating": str(rating),
                "user_ratings_total": str(user_ratings_total),
                "business_status": business_status,
                "types": types,
                "search_term": search_term,
                "metro": metro_name,
            },
        )

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Search Google Places for businesses across categories and metros.

        API mode (preferred): Uses Places Text Search + Details APIs.
        HTML mode (fallback): Scrapes Google Maps public search pages.
        """
        self.logger.info(
            "Starting %s scraper — %d terms x %d metros, mode=%s",
            self.SOURCE_NAME, len(SEARCH_TERMS), len(METRO_LOCATIONS),
            "API" if self._api_key else "HTML",
        )

        start_term_idx = (checkpoint or {}).get("term_idx", 0)
        start_metro_idx = (checkpoint or {}).get("metro_idx", 0)
        contacts_yielded = 0
        pages_done = 0

        if self._api_key:
            # API mode
            for term_idx, term in enumerate(SEARCH_TERMS):
                if term_idx < start_term_idx:
                    continue

                for metro_idx, (metro_name, latlng) in enumerate(METRO_LOCATIONS):
                    if term_idx == start_term_idx and metro_idx < start_metro_idx:
                        continue

                    query = f"{term} in {metro_name}"
                    self.logger.info(
                        "Searching: '%s' (term %d/%d, metro %d/%d)",
                        query, term_idx + 1, len(SEARCH_TERMS),
                        metro_idx + 1, len(METRO_LOCATIONS),
                    )

                    page_token = ""
                    pages_this_query = 0

                    while True:
                        result = self._api_text_search(query, location=latlng, page_token=page_token)
                        if not result:
                            break

                        places = result.get("results") or []
                        if not places:
                            break

                        # Only fetch details for first 20 results per query to save API calls
                        for idx, place in enumerate(places):
                            fetch_details = idx < 20 and pages_this_query == 0
                            contact = self._api_result_to_contact(
                                place, term, metro_name, fetch_details=fetch_details,
                            )
                            if contact and contact.is_valid():
                                contact.source_platform = self.SOURCE_NAME
                                contact.source_url = f"https://www.google.com/maps/search/{urllib.parse.quote(query)}"
                                contact.scraped_at = datetime.now().isoformat()
                                contact.email = contact.clean_email()
                                self.stats["contacts_found"] += 1
                                self.stats["contacts_valid"] += 1
                                contacts_yielded += 1
                                yield contact

                                if max_contacts and contacts_yielded >= max_contacts:
                                    self.logger.info("Reached max_contacts=%d", max_contacts)
                                    return

                        pages_done += 1
                        pages_this_query += 1

                        if max_pages and pages_done >= max_pages:
                            self.logger.info("Reached max_pages=%d", max_pages)
                            return

                        # Check for next page
                        page_token = result.get("next_page_token", "")
                        if not page_token:
                            break

                        # Google requires a short delay before using next_page_token
                        time.sleep(2)

                    if pages_done % 20 == 0 and pages_done > 0:
                        self.logger.info(
                            "Progress: %d pages, %d valid contacts",
                            pages_done, self.stats["contacts_valid"],
                        )

        else:
            # HTML fallback mode — use default generate_urls -> fetch -> scrape loop
            self.logger.info("Running in HTML fallback mode (limited data extraction)")

            start_from = (checkpoint or {}).get("last_url")
            past_checkpoint = start_from is None

            for url in self.generate_urls():
                if not past_checkpoint:
                    if url == start_from:
                        past_checkpoint = True
                    continue

                html = self.fetch_page(url)
                if not html:
                    continue

                try:
                    contacts = self.scrape_page(url, html)
                except Exception as exc:
                    self.logger.error("Parse error on %s: %s", url, exc)
                    self.stats["errors"] += 1
                    continue

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
                        "Progress: %d pages, %d valid contacts",
                        pages_done, self.stats["contacts_valid"],
                    )

                if max_pages and pages_done >= max_pages:
                    self.logger.info("Reached max_pages=%d", max_pages)
                    break

        self.logger.info("Scraper complete: %s", self.stats)
