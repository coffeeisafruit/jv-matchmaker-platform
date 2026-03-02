"""
Yelp Business Listings Scraper

Scrapes Yelp search results for JV-relevant business categories.
Uses Yelp's public search pages (HTML scraping) since the Fusion API
requires authentication and has strict rate limits.

Categories targeted:
- Business consultants
- Marketing agencies
- Financial advisors
- Life coaches
- Business coaches
- PR firms
- IT services
- Web design

Strategy:
  - Search Yelp for each category + major US metro area
  - Parse the search results HTML for business listings
  - Extract: business name, phone, website, address, rating, review count
  - Paginate through results (Yelp shows 10 per page, up to ~24 pages)

Estimated yield: 50,000-200,000 businesses across all categories and metros.

Note: Yelp aggressively blocks scrapers. This uses conservative rate
limiting and standard browser headers. If blocked, consider using the
Yelp Fusion API with a valid API key (set YELP_API_KEY in .env).
"""

from __future__ import annotations

import json
import os
import re
import urllib.parse
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# JV-relevant Yelp search categories
CATEGORIES = [
    "business+consultant",
    "marketing+agency",
    "financial+advisor",
    "life+coach",
    "business+coach",
    "pr+firm",
    "it+services",
    "web+design",
    "advertising+agency",
    "management+consultant",
    "executive+coach",
    "accounting+firm",
    "insurance+agency",
    "real+estate+agent",
    "event+planning",
    "graphic+design",
    "seo+services",
    "social+media+marketing",
    "video+production",
    "training+development",
]

# Major US metro areas for geographic coverage
METRO_AREAS = [
    "New York, NY",
    "Los Angeles, CA",
    "Chicago, IL",
    "Houston, TX",
    "Phoenix, AZ",
    "Philadelphia, PA",
    "San Antonio, TX",
    "San Diego, CA",
    "Dallas, TX",
    "San Jose, CA",
    "Austin, TX",
    "Jacksonville, FL",
    "Fort Worth, TX",
    "Columbus, OH",
    "Charlotte, NC",
    "Indianapolis, IN",
    "San Francisco, CA",
    "Seattle, WA",
    "Denver, CO",
    "Washington, DC",
    "Nashville, TN",
    "Oklahoma City, OK",
    "El Paso, TX",
    "Boston, MA",
    "Portland, OR",
    "Las Vegas, NV",
    "Memphis, TN",
    "Louisville, KY",
    "Baltimore, MD",
    "Milwaukee, WI",
    "Albuquerque, NM",
    "Tucson, AZ",
    "Fresno, CA",
    "Sacramento, CA",
    "Mesa, AZ",
    "Kansas City, MO",
    "Atlanta, GA",
    "Omaha, NE",
    "Colorado Springs, CO",
    "Raleigh, NC",
    "Long Beach, CA",
    "Virginia Beach, VA",
    "Miami, FL",
    "Oakland, CA",
    "Minneapolis, MN",
    "Tampa, FL",
    "Tulsa, OK",
    "Arlington, TX",
    "New Orleans, LA",
    "Cleveland, OH",
    "Pittsburgh, PA",
    "Detroit, MI",
    "St. Louis, MO",
    "Salt Lake City, UT",
    "Honolulu, HI",
    "Orlando, FL",
    "Cincinnati, OH",
]

# Max pages per category+metro combo (Yelp caps at ~24 pages = 240 results)
MAX_PAGES_PER_SEARCH = 24

# Yelp Fusion API base URL (used if API key is available)
YELP_API_BASE = "https://api.yelp.com/v3"

# Phone regex
PHONE_RE = re.compile(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")


class Scraper(BaseScraper):
    SOURCE_NAME = "yelp_businesses"
    BASE_URL = "https://www.yelp.com"
    REQUESTS_PER_MINUTE = 6  # Conservative to avoid blocks

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_biz: set[str] = set()
        self._api_key = os.environ.get("YELP_API_KEY", "")
        if self._api_key:
            self.logger.info("Yelp Fusion API key found — using API mode")
        else:
            self.logger.info("No YELP_API_KEY — using HTML scraping mode")

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run() for multi-mode (API vs HTML)."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Yelp search results page HTML."""
        soup = self.parse_html(html)
        contacts = []

        # Yelp embeds structured data as JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string or "")
            except (json.JSONDecodeError, TypeError):
                continue

            # Handle both single item and list of items
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") not in ("LocalBusiness", "Organization", "ProfessionalService"):
                    continue
                contact = self._parse_jsonld_business(item, url)
                if contact:
                    contacts.append(contact)

        # Fallback: parse HTML search result cards if no JSON-LD
        if not contacts:
            contacts = self._parse_html_results(soup, url)

        return contacts

    def _parse_jsonld_business(self, item: dict, source_url: str) -> Optional[ScrapedContact]:
        """Parse a JSON-LD LocalBusiness item."""
        name = (item.get("name") or "").strip()
        if not name or len(name) < 2:
            return None

        dedup_key = name.lower().replace(" ", "")
        if dedup_key in self._seen_biz:
            return None
        self._seen_biz.add(dedup_key)

        phone = (item.get("telephone") or "").strip()
        website = (item.get("url") or "").strip()

        # Address
        address_obj = item.get("address") or {}
        street = (address_obj.get("streetAddress") or "").strip()
        city = (address_obj.get("addressLocality") or "").strip()
        state = (address_obj.get("addressRegion") or "").strip()
        zipcode = (address_obj.get("postalCode") or "").strip()
        full_address = ", ".join(p for p in [street, city, state, zipcode] if p)

        # Rating
        aggregate = item.get("aggregateRating") or {}
        rating = aggregate.get("ratingValue", "")
        review_count = aggregate.get("reviewCount", "")

        # Price range
        price_range = (item.get("priceRange") or "").strip()

        # Bio
        bio_parts = [name]
        if city and state:
            bio_parts.append(f"{city}, {state}")
        if rating:
            bio_parts.append(f"Rating: {rating}/5")
        if review_count:
            bio_parts.append(f"{review_count} reviews")
        if price_range:
            bio_parts.append(f"Price: {price_range}")
        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=name,
            email="",
            company=name,
            website=website,
            phone=phone,
            bio=bio,
            source_category="yelp_business",
            raw_data={
                "address": full_address,
                "city": city,
                "state": state,
                "zipcode": zipcode,
                "rating": str(rating),
                "review_count": str(review_count),
                "price_range": price_range,
            },
        )

    def _parse_html_results(self, soup, source_url: str) -> list[ScrapedContact]:
        """Fallback HTML parser for Yelp search result cards."""
        contacts = []

        # Yelp uses various container patterns; look for common structures
        # Search result containers often have data-testid or specific class patterns
        for container in soup.find_all(["div", "li"], attrs={"data-testid": re.compile(r"serp-ia-card")}):
            contact = self._parse_result_card(container, source_url)
            if contact:
                contacts.append(contact)

        # Alternative: look for h3/h4 links that point to /biz/
        if not contacts:
            for link in soup.find_all("a", href=re.compile(r"/biz/[a-z0-9\-]+")):
                name = link.get_text(strip=True)
                href = link.get("href", "")
                if not name or len(name) < 3 or len(name) > 200:
                    continue

                # Skip navigation/utility links
                if name.lower() in ("more", "see all", "write a review", "map"):
                    continue

                dedup_key = name.lower().replace(" ", "")
                if dedup_key in self._seen_biz:
                    continue
                self._seen_biz.add(dedup_key)

                biz_url = f"https://www.yelp.com{href}" if href.startswith("/") else href

                # Try to find phone/address near this link
                parent = link.find_parent(["div", "li"])
                phone = ""
                address = ""
                if parent:
                    parent_text = parent.get_text(separator=" ", strip=True)
                    phone_match = PHONE_RE.search(parent_text)
                    if phone_match:
                        phone = phone_match.group(0)

                contacts.append(ScrapedContact(
                    name=name,
                    email="",
                    company=name,
                    website=biz_url,
                    phone=phone,
                    bio=name,
                    source_category="yelp_business",
                    raw_data={"yelp_url": biz_url},
                ))

        return contacts

    def _parse_result_card(self, container, source_url: str) -> Optional[ScrapedContact]:
        """Parse a single Yelp search result card element."""
        # Find business name link
        name_link = container.find("a", href=re.compile(r"/biz/"))
        if not name_link:
            return None

        name = name_link.get_text(strip=True)
        if not name or len(name) < 2:
            return None

        dedup_key = name.lower().replace(" ", "")
        if dedup_key in self._seen_biz:
            return None
        self._seen_biz.add(dedup_key)

        href = name_link.get("href", "")
        biz_url = f"https://www.yelp.com{href}" if href.startswith("/") else href

        # Extract phone from card text
        card_text = container.get_text(separator=" ", strip=True)
        phone_match = PHONE_RE.search(card_text)
        phone = phone_match.group(0) if phone_match else ""

        # Look for rating
        rating = ""
        rating_el = container.find(attrs={"aria-label": re.compile(r"\d+\.?\d*\s*star")})
        if rating_el:
            rating_match = re.search(r"(\d+\.?\d*)", rating_el.get("aria-label", ""))
            if rating_match:
                rating = rating_match.group(1)

        bio_parts = [name]
        if rating:
            bio_parts.append(f"Rating: {rating}/5")
        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=name,
            email="",
            company=name,
            website=biz_url,
            phone=phone,
            bio=bio,
            source_category="yelp_business",
            raw_data={"yelp_url": biz_url, "rating": rating},
        )

    # ------------------------------------------------------------------
    # Yelp Fusion API mode
    # ------------------------------------------------------------------

    def _api_search(
        self,
        term: str,
        location: str,
        offset: int = 0,
        limit: int = 50,
    ) -> Optional[dict]:
        """Search businesses via Yelp Fusion API."""
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        url = f"{YELP_API_BASE}/businesses/search"
        params = {
            "term": term.replace("+", " "),
            "location": location,
            "limit": min(limit, 50),
            "offset": offset,
            "sort_by": "best_match",
        }
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Accept": "application/json",
        }

        try:
            resp = self.session.get(url, params=params, headers=headers, timeout=30)
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1
            return resp.json()
        except Exception as exc:
            self.logger.warning("API search failed (%s, %s): %s", term, location, exc)
            self.stats["errors"] += 1
            return None

    def _api_business_to_contact(self, biz: dict, category: str) -> Optional[ScrapedContact]:
        """Convert a Yelp Fusion API business result to ScrapedContact."""
        name = (biz.get("name") or "").strip()
        if not name or len(name) < 2:
            return None

        biz_id = (biz.get("id") or "").strip()
        dedup_key = biz_id if biz_id else name.lower().replace(" ", "")
        if dedup_key in self._seen_biz:
            return None
        self._seen_biz.add(dedup_key)

        phone = (biz.get("display_phone") or biz.get("phone") or "").strip()
        yelp_url = (biz.get("url") or "").strip()
        rating = biz.get("rating", 0) or 0
        review_count = biz.get("review_count", 0) or 0
        is_closed = biz.get("is_closed", False)

        # Location
        location = biz.get("location") or {}
        city = (location.get("city") or "").strip()
        state = (location.get("state") or "").strip()
        zipcode = (location.get("zip_code") or "").strip()
        address1 = (location.get("address1") or "").strip()
        display_address = location.get("display_address") or []
        full_address = ", ".join(display_address) if display_address else ""

        # Categories
        yelp_cats = biz.get("categories") or []
        cat_titles = [c.get("title", "") for c in yelp_cats if c.get("title")]

        # Skip permanently closed
        if is_closed:
            return None

        # Bio
        bio_parts = [name]
        if city and state:
            bio_parts.append(f"{city}, {state}")
        if rating:
            bio_parts.append(f"Rating: {rating}/5")
        if review_count:
            bio_parts.append(f"{review_count} reviews")
        if cat_titles:
            bio_parts.append(f"Categories: {', '.join(cat_titles[:3])}")
        bio = " | ".join(bio_parts)

        # Use yelp URL as website (no direct website from search API)
        website = yelp_url or ""

        return ScrapedContact(
            name=name,
            email="",
            company=name,
            website=website,
            phone=phone,
            bio=bio,
            source_category="yelp_business",
            raw_data={
                "yelp_id": biz_id,
                "yelp_url": yelp_url,
                "address": full_address,
                "city": city,
                "state": state,
                "zipcode": zipcode,
                "rating": str(rating),
                "review_count": str(review_count),
                "categories": cat_titles,
                "category_searched": category,
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
        """Scrape Yelp businesses across categories and metro areas.

        Uses Fusion API if YELP_API_KEY is set, otherwise falls back
        to HTML scraping of public search pages.
        """
        self.logger.info(
            "Starting %s scraper — %d categories x %d metros",
            self.SOURCE_NAME, len(CATEGORIES), len(METRO_AREAS),
        )

        start_cat_idx = (checkpoint or {}).get("cat_idx", 0)
        start_metro_idx = (checkpoint or {}).get("metro_idx", 0)
        contacts_yielded = 0
        pages_done = 0

        for cat_idx, category in enumerate(CATEGORIES):
            if cat_idx < start_cat_idx:
                continue

            for metro_idx, metro in enumerate(METRO_AREAS):
                if cat_idx == start_cat_idx and metro_idx < start_metro_idx:
                    continue

                self.logger.info(
                    "Searching: '%s' in %s (cat %d/%d, metro %d/%d)",
                    category.replace("+", " "), metro,
                    cat_idx + 1, len(CATEGORIES),
                    metro_idx + 1, len(METRO_AREAS),
                )

                if self._api_key:
                    # API mode: paginate through Fusion API results
                    offset = 0
                    while offset < 1000:  # Yelp API caps at 1000 results
                        result = self._api_search(category, metro, offset=offset)
                        if not result:
                            break

                        businesses = result.get("businesses") or []
                        if not businesses:
                            break

                        for biz in businesses:
                            contact = self._api_business_to_contact(biz, category)
                            if contact and contact.is_valid():
                                contact.source_platform = self.SOURCE_NAME
                                contact.source_url = f"https://www.yelp.com/search?find_desc={category}&find_loc={urllib.parse.quote(metro)}"
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
                        offset += len(businesses)

                        total = result.get("total", 0)
                        if offset >= total:
                            break

                        if max_pages and pages_done >= max_pages:
                            self.logger.info("Reached max_pages=%d", max_pages)
                            return

                else:
                    # HTML scraping mode
                    for page_num in range(MAX_PAGES_PER_SEARCH):
                        start = page_num * 10
                        search_url = (
                            f"{self.BASE_URL}/search?"
                            f"find_desc={category}"
                            f"&find_loc={urllib.parse.quote(metro)}"
                            f"&start={start}"
                        )

                        html = self.fetch_page(search_url)
                        if not html:
                            break

                        # Check for Yelp block page
                        if "unusual activity" in html.lower() or "captcha" in html.lower():
                            self.logger.warning("Yelp blocked request — backing off")
                            break

                        try:
                            contacts = self.scrape_page(search_url, html)
                        except Exception as exc:
                            self.logger.error("Parse error on %s: %s", search_url, exc)
                            self.stats["errors"] += 1
                            break

                        if not contacts:
                            break  # No more results

                        for contact in contacts:
                            contact.source_platform = self.SOURCE_NAME
                            contact.source_url = search_url
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
                        if max_pages and pages_done >= max_pages:
                            self.logger.info("Reached max_pages=%d", max_pages)
                            return

                if pages_done % 10 == 0 and pages_done > 0:
                    self.logger.info(
                        "Progress: %d pages, %d valid contacts",
                        pages_done, self.stats["contacts_valid"],
                    )

        self.logger.info("Scraper complete: %s", self.stats)
