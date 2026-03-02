"""
Alignable small business networking directory scraper.

Alignable is a networking platform for small businesses with
millions of business owners. The public directory at
alignable.com/business-directory lists businesses by category
and location.

Strategy: Paginate through the business directory by category
and location. Alignable has server-rendered listing pages with
business cards containing name, category, and location.

Estimated yield: 5,000-20,000 businesses
"""

from __future__ import annotations

import json
import re
from typing import Iterator

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Business categories on Alignable
CATEGORIES = [
    "accounting",
    "advertising",
    "attorney",
    "auto-repair",
    "banking",
    "business-consulting",
    "business-services",
    "cleaning",
    "coaching",
    "computer-services",
    "construction",
    "dental",
    "education",
    "electrician",
    "event-planning",
    "financial-advisor",
    "fitness",
    "graphic-design",
    "health-wellness",
    "home-improvement",
    "insurance",
    "interior-design",
    "landscaping",
    "marketing",
    "massage-therapy",
    "mortgage",
    "networking",
    "nonprofit",
    "notary",
    "nutrition",
    "photography",
    "plumbing",
    "printing",
    "property-management",
    "real-estate",
    "restaurant",
    "roofing",
    "salon",
    "security",
    "sign-company",
    "social-media",
    "solar",
    "staffing",
    "storage",
    "tax-preparation",
    "technology",
    "therapy",
    "travel",
    "veterinary",
    "web-design",
]

# Major US cities for geographic coverage
CITIES = [
    "new-york-ny",
    "los-angeles-ca",
    "chicago-il",
    "houston-tx",
    "phoenix-az",
    "philadelphia-pa",
    "san-antonio-tx",
    "san-diego-ca",
    "dallas-tx",
    "austin-tx",
    "jacksonville-fl",
    "san-francisco-ca",
    "columbus-oh",
    "charlotte-nc",
    "indianapolis-in",
    "seattle-wa",
    "denver-co",
    "washington-dc",
    "nashville-tn",
    "oklahoma-city-ok",
    "miami-fl",
    "portland-or",
    "las-vegas-nv",
    "atlanta-ga",
    "raleigh-nc",
    "minneapolis-mn",
    "tampa-fl",
    "boston-ma",
    "salt-lake-city-ut",
    "san-jose-ca",
    "pittsburgh-pa",
    "cincinnati-oh",
    "kansas-city-mo",
    "st-louis-mo",
    "orlando-fl",
    "sacramento-ca",
    "detroit-mi",
    "milwaukee-wi",
    "richmond-va",
    "baltimore-md",
]

MAX_PAGES = 10


class Scraper(BaseScraper):
    SOURCE_NAME = "alignable"
    BASE_URL = "https://www.alignable.com"
    REQUESTS_PER_MINUTE = 5

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_businesses: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield business directory listing URLs."""
        # Main directory
        yield f"{self.BASE_URL}/business-directory"

        # By city (more targeted results)
        for city in CITIES:
            for page in range(1, MAX_PAGES + 1):
                if page == 1:
                    yield f"{self.BASE_URL}/business-directory/{city}"
                else:
                    yield f"{self.BASE_URL}/business-directory/{city}?page={page}"

        # By category in top cities
        for category in CATEGORIES:
            for city in CITIES[:15]:  # Top 15 cities per category
                yield f"{self.BASE_URL}/business-directory/{city}/{category}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse an Alignable directory listing page."""
        soup = self.parse_html(html)
        contacts = []

        # Try JSON-LD first
        for script in soup.find_all("script", type="application/ld+json"):
            if script.string:
                try:
                    data = json.loads(script.string)
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        contact = self._parse_jsonld(item, url)
                        if contact:
                            contacts.append(contact)
                except (json.JSONDecodeError, KeyError):
                    pass

        # Parse business cards from HTML
        for card in soup.find_all(class_=re.compile(
            r"business|company|member|profile|card|listing|result", re.I
        )):
            contact = self._parse_business_card(card, url)
            if contact:
                contacts.append(contact)

        # Look for business profile links
        if not contacts:
            profile_links = set()
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                # Alignable profiles: /biz/{business-slug}
                if re.match(r"^/biz/[a-zA-Z0-9\-]+/?$", href):
                    full_url = f"{self.BASE_URL}{href}"
                    slug = href.rstrip("/").split("/")[-1]
                    if slug not in self._seen_businesses:
                        self._seen_businesses.add(slug)
                        profile_links.add(full_url)

            for profile_url in profile_links:
                profile_html = self.fetch_page(profile_url)
                if profile_html:
                    contact = self._parse_profile(profile_url, profile_html)
                    if contact:
                        contacts.append(contact)

        return contacts

    def _parse_jsonld(self, data: dict, source_url: str) -> ScrapedContact | None:
        """Parse JSON-LD structured data for business."""
        data_type = data.get("@type", "")
        if data_type not in ("LocalBusiness", "Organization", "ProfessionalService",
                             "Store", "Restaurant", "MedicalBusiness"):
            return None

        name = (data.get("name") or "").strip()
        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_businesses:
            return None
        self._seen_businesses.add(name_key)

        website = (data.get("url") or "").strip()
        description = (data.get("description") or "")[:500]
        phone = (data.get("telephone") or "").strip()
        email = (data.get("email") or "").strip()

        address = data.get("address", {})
        location = ""
        if isinstance(address, dict):
            parts = [
                (address.get("addressLocality") or "").strip(),
                (address.get("addressRegion") or "").strip(),
            ]
            location = ", ".join(p for p in parts if p)

        bio_parts = ["Small Business"]
        if location:
            bio_parts.append(location)
        if description:
            bio_parts.append(description[:300])
        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=name,
            email=email,
            company=name,
            website=website,
            linkedin="",
            phone=phone,
            bio=bio,
            source_url=source_url,
            source_category="small_business",
        )

    def _parse_business_card(self, card, source_url: str) -> ScrapedContact | None:
        """Parse a business card element from the directory listing."""
        name = ""
        website = ""

        # Find business name
        for tag in ["h2", "h3", "h4", "strong"]:
            el = card.find(tag)
            if el:
                link = el.find("a", href=True)
                if link:
                    name = link.get_text(strip=True)
                    href = link.get("href", "")
                    if href:
                        website = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                else:
                    name = el.get_text(strip=True)
                if name and len(name) > 1 and len(name) < 150:
                    break
                name = ""

        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_businesses:
            return None
        self._seen_businesses.add(name_key)

        # Category / type
        category = ""
        cat_el = card.find(class_=re.compile(r"category|type|industry|specialty", re.I))
        if cat_el:
            category = cat_el.get_text(strip=True)[:100]

        # Location
        location = ""
        loc_el = card.find(class_=re.compile(r"location|address|city", re.I))
        if loc_el:
            location = loc_el.get_text(strip=True)[:100]

        # Bio
        card_text = card.get_text(separator=" | ", strip=True)
        bio_parts = ["Small Business on Alignable"]
        if category:
            bio_parts.append(category)
        if location:
            bio_parts.append(location)
        bio = " | ".join(bio_parts)

        emails = self.extract_emails(str(card))
        email = emails[0] if emails else ""

        return ScrapedContact(
            name=name,
            email=email,
            company=name,
            website=website or source_url,
            linkedin="",
            phone="",
            bio=bio,
            source_url=source_url,
            source_category="small_business",
        )

    def _parse_profile(self, url: str, html: str) -> ScrapedContact | None:
        """Parse a full Alignable business profile page."""
        soup = self.parse_html(html)

        name = ""
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        if not name:
            og = soup.find("meta", property="og:title")
            if og:
                name = (og.get("content") or "").split("|")[0].split("-")[0].strip()

        if not name or len(name) < 2:
            return None

        name_key = name.lower()
        if name_key in self._seen_businesses:
            return None
        self._seen_businesses.add(name_key)

        bio = ""
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            bio = (meta_desc.get("content") or "")[:500]

        website = ""
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            text = a.get_text(strip=True).lower()
            if ("website" in text or "visit" in text) and href.startswith("http"):
                if "alignable.com" not in href.lower():
                    website = href
                    break

        if not website:
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                if (href.startswith("http")
                        and "alignable.com" not in href.lower()
                        and "facebook.com" not in href.lower()
                        and "twitter.com" not in href.lower()
                        and "linkedin.com" not in href.lower()):
                    website = href
                    break

        emails = self.extract_emails(html)
        email = emails[0] if emails else ""
        linkedin = self.extract_linkedin(html)

        phone = ""
        phone_match = re.search(
            r"(?:1[-.\s]?)?(?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4}", html
        )
        if phone_match:
            phone = phone_match.group(0)

        return ScrapedContact(
            name=name,
            email=email,
            company=name,
            website=website or url,
            linkedin=linkedin,
            phone=phone,
            bio=f"Small Business | {bio}" if bio else "Small Business on Alignable",
            source_url=url,
            source_category="small_business",
        )
