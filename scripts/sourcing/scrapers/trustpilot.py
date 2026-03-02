"""
Trustpilot Business Directory Scraper

Scrapes business listings from Trustpilot category pages.
Uses __NEXT_DATA__ JSON embedded in HTML — no API key needed.

Categories cover 30K+ JV-relevant businesses:
- Marketing agencies (7,400+)
- Sales & marketing companies (10,000+)
- Contractors & consultants (6,100+)
- Internet & software (10,000+)
- Business services, coaching, training, etc.

Each listing includes: company name, domain, location, trust score,
review count, and category tags.
"""

import json
import re
from typing import Iterator, Optional
from datetime import datetime

from scripts.sourcing.base import BaseScraper, ScrapedContact


# JV-relevant Trustpilot categories (slug format)
CATEGORIES = [
    "marketing_agency",
    "sales_marketing",
    "contractors_consultants",
    "internet_software",
    "business_services",
    "advertising_agency",
    "web_design",
    "seo_agency",
    "pr_agency",
    "social_media_agency",
    "content_marketing",
    "email_marketing",
    "lead_generation",
    "recruitment_agency",
    "it_services",
    "web_hosting",
    "software_company",
    "training_company",
    "coaching",
    "consulting_agency",
    "accounting",
    "financial_services",
    "insurance_agency",
    "real_estate_agent",
    "legal_services",
    "printing_and_publishing",
    "graphic_design",
    "video_production",
    "photography",
    "event_management",
]

# Max pages per category (20 results per page)
MAX_PAGES_PER_CATEGORY = 400  # Trustpilot has up to 370 pages per category


class Scraper(BaseScraper):
    SOURCE_NAME = "trustpilot"
    BASE_URL = "https://www.trustpilot.com"
    REQUESTS_PER_MINUTE = 10  # Be polite
    TYPICAL_ROLES = ["Service Provider"]
    TYPICAL_NICHES = ["consulting", "financial_services", "saas_software"]
    TYPICAL_OFFERINGS = ["services", "consulting", "software"]

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_domains: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used — we override run()."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used — we override run()."""
        return []

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Scrape Trustpilot category pages for business data."""
        self.logger.info("Starting %s scraper — %d categories",
                         self.SOURCE_NAME, len(CATEGORIES))

        start_cat_idx = (checkpoint or {}).get("cat_idx", 0)
        contacts_yielded = 0
        pages_done = 0

        for cat_idx, category in enumerate(CATEGORIES):
            if cat_idx < start_cat_idx:
                continue

            self.logger.info("Category: %s (%d/%d)",
                             category, cat_idx + 1, len(CATEGORIES))

            page = 1
            consecutive_empty = 0

            while page <= MAX_PAGES_PER_CATEGORY:
                if self.rate_limiter:
                    self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

                url = f"{self.BASE_URL}/categories/{category}?page={page}"

                try:
                    resp = self.session.get(url, timeout=30)
                    if resp.status_code == 403:
                        self.logger.warning("Cloudflare block on %s", url)
                        break
                    resp.raise_for_status()
                    self.stats["pages_scraped"] += 1
                except Exception as e:
                    self.stats["errors"] += 1
                    self.logger.warning("Error on %s: %s", url, e)
                    break

                # Extract __NEXT_DATA__ JSON
                contacts = self._extract_businesses(resp.text, url, category)

                if not contacts:
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        break
                else:
                    consecutive_empty = 0

                for contact in contacts:
                    contacts_yielded += 1
                    yield contact

                    if max_contacts and contacts_yielded >= max_contacts:
                        self.logger.info("Reached max_contacts=%d", max_contacts)
                        return

                pages_done += 1
                page += 1

                if max_pages and pages_done >= max_pages:
                    self.logger.info("Reached max_pages=%d", max_pages)
                    return

            self.logger.info("Category %s done: %d valid contacts total",
                             category, self.stats["contacts_valid"])

        self.logger.info("Scraper complete: %s", self.stats)

    def _extract_businesses(self, html: str, url: str, category: str) -> list[ScrapedContact]:
        """Extract business data from __NEXT_DATA__ JSON in the page."""
        contacts = []

        # Find __NEXT_DATA__ script tag (may have nonce or other attributes)
        match = re.search(r'<script\s+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
        if not match:
            self.logger.debug("No __NEXT_DATA__ found in %s", url)
            return contacts

        try:
            next_data = json.loads(match.group(1))
        except json.JSONDecodeError:
            self.logger.warning("Failed to parse __NEXT_DATA__ from %s", url)
            return contacts

        # Navigate to business units
        props = next_data.get("props", {})
        page_props = props.get("pageProps", {})

        # Main listing: businessUnits is a dict with "businesses" array
        business_units_data = page_props.get("businessUnits", {})
        if isinstance(business_units_data, dict):
            businesses = business_units_data.get("businesses", [])
        elif isinstance(business_units_data, list):
            businesses = business_units_data
        else:
            businesses = []

        # Also collect sidebar lists for extra contacts
        for key in ("newestBusinessUnits", "recentlyReviewedBusinessUnits"):
            extra = page_props.get(key, [])
            if isinstance(extra, list):
                businesses.extend(extra)

        for bu in businesses:
            contact = self._parse_business(bu, url, category)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_business(self, bu: dict, source_url: str, category: str) -> ScrapedContact | None:
        """Parse a single Trustpilot business unit into ScrapedContact."""
        name = (bu.get("displayName") or bu.get("name") or "").strip()
        if not name or len(name) < 2:
            return None

        # Get domain/identifying name
        domain = (bu.get("identifyingName") or "").strip()
        business_id = (bu.get("businessUnitId") or "").strip()

        # Deduplicate by domain
        dedup_key = domain.lower() if domain else name.upper()
        if dedup_key in self._seen_domains:
            return None
        self._seen_domains.add(dedup_key)

        # Extract contact info (email, phone, website)
        contact_info = bu.get("contact", {}) or {}
        email = (contact_info.get("email") or "").strip()
        phone = (contact_info.get("phone") or "").strip()
        contact_website = (contact_info.get("website") or "").strip()

        # Build website URL — prefer contact website, then domain
        if contact_website:
            website = contact_website
        elif domain and "." in domain:
            website = f"https://{domain}"
        elif domain:
            website = f"https://www.trustpilot.com/review/{domain}"
        else:
            website = f"https://www.trustpilot.com/review/{name.lower().replace(' ', '-')}"

        # Extract additional data
        trust_score = bu.get("trustScore", 0) or 0
        stars = bu.get("stars", 0) or 0
        num_reviews = bu.get("numberOfReviews", 0) or 0

        # Location
        location = bu.get("location", {}) or {}
        city = (location.get("city") or "").strip()
        country = (location.get("country") or "").strip()
        zip_code = (location.get("zipCode") or "").strip()
        address = (location.get("address") or "").strip()

        # Categories
        categories = bu.get("categories", []) or []
        cat_names = [c.get("displayName", "") for c in categories if c.get("displayName")]

        # Build bio
        bio_parts = [name]
        if city and country:
            bio_parts.append(f"{city}, {country}")
        elif country:
            bio_parts.append(country)
        if trust_score:
            bio_parts.append(f"Trust: {trust_score}/5")
        if num_reviews:
            bio_parts.append(f"{num_reviews:,} reviews")
        if cat_names:
            bio_parts.append(f"Categories: {', '.join(cat_names[:3])}")
        bio = " | ".join(bio_parts)

        contact = ScrapedContact(
            name=name,
            email=email,
            company=name,
            website=website,
            linkedin="",
            phone=phone,
            bio=bio,
            source_platform=self.SOURCE_NAME,
            source_url=source_url,
            source_category="business_directory",
            scraped_at=datetime.now().isoformat(),
            raw_data={
                "business_unit_id": business_id,
                "domain": domain,
                "trust_score": trust_score,
                "stars": stars,
                "num_reviews": num_reviews,
                "city": city,
                "country": country,
                "address": address,
                "zip_code": zip_code,
                "category": category,
                "categories": cat_names,
            },
        )

        if not contact.is_valid():
            return None

        self.stats["contacts_found"] += 1
        self.stats["contacts_valid"] += 1
        return contact
