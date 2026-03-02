"""
Thumbtack.com professional directory scraper.

Thumbtack is a local services marketplace with public professional
listings. Coaches, consultants, and service providers have public
profiles with business name, pricing, reviews, and descriptions.

Data is extracted from JSON-LD structured data (schema.org ItemList
of LocalBusiness) embedded in each category listing page.

URL patterns:
  - /k/{category}/near-me/          (national, no geo filter)
  - /k/{category}/near-me/?page=N   (pagination)
  - /{state}/{city}/{category}/      (city-specific)

Each page contains ~10 professionals in structured data.
Pagination is available via ?page=N query parameter.

Estimated yield: 3,000-10,000 professionals
"""

from __future__ import annotations

import json
import re
from typing import Iterator

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Service categories relevant to coaching/consulting/JV partnerships
CATEGORIES = [
    "life-coach",
    "business-consulting",
    "career-coach",
    "executive-coach",
    "health-coaches",
    "wellness-coaches",
    "spiritual-life-coaches",
    "christian-life-coaches",
    "couples-life-coaching",
    "financial-advising",
    "business-plan-consulting",
    "tax-preparation",
    "bookkeeping",
    "resume-writing",
    "public-speaking-coaching",
    "personal-trainers",
    "nutritionists",
    "tutoring",
    "spanish-tutoring",
    "math-tutoring",
    "writing-tutoring",
]

# Major US cities for geo-targeted scraping (state/city format)
US_CITIES = [
    ("ny", "new-york"),
    ("ca", "los-angeles"),
    ("il", "chicago"),
    ("tx", "houston"),
    ("az", "phoenix"),
    ("pa", "philadelphia"),
    ("tx", "san-antonio"),
    ("ca", "san-diego"),
    ("tx", "dallas"),
    ("ca", "san-jose"),
    ("tx", "austin"),
    ("fl", "jacksonville"),
    ("ca", "san-francisco"),
    ("oh", "columbus"),
    ("in", "indianapolis"),
    ("nc", "charlotte"),
    ("wa", "seattle"),
    ("co", "denver"),
    ("dc", "washington"),
    ("ma", "boston"),
    ("tn", "nashville"),
    ("or", "portland"),
    ("ok", "oklahoma-city"),
    ("nv", "las-vegas"),
    ("tn", "memphis"),
    ("ky", "louisville"),
    ("md", "baltimore"),
    ("wi", "milwaukee"),
    ("nm", "albuquerque"),
    ("az", "tucson"),
    ("ca", "fresno"),
    ("ca", "sacramento"),
    ("mo", "kansas-city"),
    ("ga", "atlanta"),
    ("fl", "miami"),
    ("fl", "tampa"),
    ("mn", "minneapolis"),
    ("la", "new-orleans"),
    ("oh", "cleveland"),
    ("hi", "honolulu"),
    ("pa", "pittsburgh"),
    ("ca", "oakland"),
    ("mo", "st-louis"),
    ("fl", "orlando"),
    ("mi", "detroit"),
    ("nc", "raleigh"),
    ("ut", "salt-lake-city"),
    ("ct", "hartford"),
    ("va", "richmond"),
    ("al", "birmingham"),
]

# Number of pages to paginate through per category/city combo
MAX_PAGES = 20


class Scraper(BaseScraper):
    SOURCE_NAME = "thumbtack_pros"
    BASE_URL = "https://www.thumbtack.com"
    REQUESTS_PER_MINUTE = 6

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_names: set[str] = set()
        self._seen_urls: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield category listing page URLs with pagination.

        Strategy:
        1. National pages (/k/{category}/near-me/) for each category
        2. City-specific pages (/{state}/{city}/{category}/) for
           top categories in major cities
        """
        # Strategy 1: National "near-me" pages for all categories
        for category in CATEGORIES:
            for page in range(1, MAX_PAGES + 1):
                if page == 1:
                    yield f"{self.BASE_URL}/k/{category}/near-me/"
                else:
                    yield f"{self.BASE_URL}/k/{category}/near-me/?page={page}"

        # Strategy 2: City-specific pages for top coaching/consulting categories
        top_categories = [
            "life-coach",
            "business-consulting",
            "career-coach",
            "executive-coach",
            "health-coaches",
            "financial-advising",
        ]
        for state, city in US_CITIES:
            for category in top_categories:
                for page in range(1, 6):  # Fewer pages per city
                    if page == 1:
                        yield f"{self.BASE_URL}/{state}/{city}/{category}/"
                    else:
                        yield f"{self.BASE_URL}/{state}/{city}/{category}/?page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Thumbtack listing page using JSON-LD structured data."""
        contacts = []

        # Strategy 1: Extract from JSON-LD structured data (most reliable)
        contacts.extend(self._parse_json_ld(html, url))

        # Strategy 2: Fall back to HTML parsing if no structured data
        if not contacts:
            contacts.extend(self._parse_html_cards(html, url))

        return contacts

    def _parse_json_ld(self, html: str, url: str) -> list[ScrapedContact]:
        """Extract professional data from JSON-LD schema.org markup."""
        contacts = []
        soup = self.parse_html(html)

        for script in soup.find_all("script", type="application/ld+json"):
            script_text = script.string or ""
            if not script_text.strip():
                continue

            try:
                data = json.loads(script_text)
            except (json.JSONDecodeError, TypeError):
                continue

            # Look for ItemList containing LocalBusiness entries
            if data.get("@type") != "ItemList":
                continue

            items = data.get("itemListElement", [])
            for item in items:
                biz = item.get("item", {})
                if not biz:
                    continue

                contact = self._parse_business(biz, url)
                if contact:
                    contacts.append(contact)

        return contacts

    def _parse_business(self, biz: dict, source_url: str) -> ScrapedContact | None:
        """Parse a LocalBusiness JSON-LD entry into a ScrapedContact."""
        name = (biz.get("name") or "").strip()
        if not name or len(name) < 2:
            return None

        # Deduplicate
        name_lower = name.lower()
        if name_lower in self._seen_names:
            return None
        self._seen_names.add(name_lower)

        # Build profile URL
        biz_url = (biz.get("url") or "").strip()
        website = ""
        if biz_url:
            if biz_url.startswith("/"):
                website = f"{self.BASE_URL}{biz_url}"
            elif biz_url.startswith("http"):
                website = biz_url
            # Remove fragment identifiers (e.g. #123456789)
            website = website.split("#")[0].rstrip("/")
            if website in self._seen_urls:
                return None
            self._seen_urls.add(website)

        # Pricing
        price_range = (biz.get("priceRange") or "").strip()

        # Rating and reviews
        rating_data = biz.get("aggregateRating", {})
        rating = rating_data.get("ratingValue", "")
        review_count = rating_data.get("reviewCount", 0)

        # Review text (single review snippet)
        review_text = ""
        review_data = biz.get("review", {})
        if review_data:
            review_text = (review_data.get("reviewBody") or "").strip()
            # Clean HTML entities
            review_text = (
                review_text
                .replace("&quot;", '"')
                .replace("&amp;", "&")
                .replace("&apos;", "'")
            )

        # Image URL
        image = (biz.get("image") or "").strip()

        # Build bio
        bio_parts = ["Thumbtack Professional"]

        # Infer category from source URL
        category = self._extract_category(source_url)
        if category:
            bio_parts[0] = f"Thumbtack {category}"

        if price_range:
            bio_parts.append(price_range)
        if rating and review_count:
            bio_parts.append(f"{rating}/5 ({review_count} reviews)")
        if review_text:
            bio_parts.append(review_text[:200])

        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=name,
            email="",  # Not exposed in structured data
            company=name,  # Business name is the company
            website=website,
            linkedin="",
            phone="",
            bio=bio,
            source_category="coaching_consulting",
            raw_data={
                "price_range": price_range,
                "rating": str(rating) if rating else "",
                "review_count": str(review_count) if review_count else "",
                "image_url": image,
                "category": category,
            },
        )

    def _parse_html_cards(self, html: str, url: str) -> list[ScrapedContact]:
        """Fallback: parse professional cards from HTML structure."""
        contacts = []
        soup = self.parse_html(html)

        # Look for professional card elements
        cards = soup.find_all(
            attrs={"data-testid": re.compile(r"pro-card|search-result", re.I)}
        )
        if not cards:
            cards = soup.find_all(class_=re.compile(r"pro-card|result-card|service-card", re.I))

        for card in cards:
            name = ""
            # Try heading elements
            name_el = card.find(["h2", "h3", "h4"])
            if name_el:
                name = name_el.get_text(strip=True)

            if not name:
                # Try data attributes or specific spans
                name_el = card.find(attrs={"data-testid": re.compile(r"name|title", re.I)})
                if name_el:
                    name = name_el.get_text(strip=True)

            if not name or len(name) < 2:
                continue

            name_lower = name.lower()
            if name_lower in self._seen_names:
                continue
            self._seen_names.add(name_lower)

            # Website from profile link
            website = ""
            for a in card.find_all("a", href=True):
                href = a["href"]
                if href.startswith("/") and "thumbtack.com" not in href:
                    website = f"{self.BASE_URL}{href.split('#')[0]}"
                    break

            # Price
            price = ""
            price_el = card.find(class_=re.compile(r"price|cost|rate", re.I))
            if price_el:
                price = price_el.get_text(strip=True)

            # Rating
            rating = ""
            rating_el = card.find(class_=re.compile(r"rating|review|star", re.I))
            if rating_el:
                rating = rating_el.get_text(strip=True)

            category = self._extract_category(url)
            bio_parts = [f"Thumbtack {category}" if category else "Thumbtack Professional"]
            if price:
                bio_parts.append(price)
            if rating:
                bio_parts.append(rating)
            bio = " | ".join(bio_parts)

            contacts.append(ScrapedContact(
                name=name,
                email="",
                company=name,
                website=website,
                bio=bio,
                source_category="coaching_consulting",
            ))

        return contacts

    @staticmethod
    def _extract_category(url: str) -> str:
        """Extract human-readable category from URL."""
        # Match /k/{category}/ or /{state}/{city}/{category}/
        match = re.search(r"/k/([^/]+)/", url)
        if match:
            return match.group(1).replace("-", " ").title()

        # City-specific URL: /{state}/{city}/{category}/
        parts = url.rstrip("/").split("/")
        if len(parts) >= 4:
            category = parts[-1].split("?")[0]
            if category and category not in ("near-me",):
                return category.replace("-", " ").title()

        return ""
