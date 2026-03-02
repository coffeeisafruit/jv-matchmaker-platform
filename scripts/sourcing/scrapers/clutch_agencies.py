"""
Clutch.co B2B agency directory scraper.

Clutch is the leading B2B ratings and reviews platform with tens of
thousands of agencies across categories like development, marketing,
design, and consulting.

Strategy: Use sitemap.xml to discover agency profile URLs, then
parse individual profile pages for company info and contact details.

Fallback: Paginate through category listing pages at
  https://clutch.co/agencies/{category}

Estimated yield: 10,000-50,000 agencies
"""

from __future__ import annotations

import re
from typing import Iterator

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Clutch agency categories (URL slugs)
CATEGORIES = [
    "web-developers",
    "app-developers",
    "it-services",
    "digital-marketing",
    "seo-firms",
    "ppc",
    "social-media-marketing",
    "content-marketing",
    "web-designers",
    "graphic-designers",
    "branding",
    "pr-firms",
    "advertising",
    "video-production",
    "agencies/ux",
    "accounting",
    "hr",
    "staffing",
    "consulting",
    "it-strategy-consulting",
    "cybersecurity",
    "cloud-consulting",
    "blockchain",
    "artificial-intelligence",
    "ecommerce-developers",
    "salesforce-consultants",
    "hubspot",
    "erp-consulting",
    "crm-consulting",
    "business-consulting",
    "management-consulting",
    "financial-advisory",
    "legal-services",
    "logistics",
    "translation",
    "ar-vr-development",
    "iot-development",
    "data-analytics",
]

MAX_PAGES_PER_CATEGORY = 50


class Scraper(BaseScraper):
    SOURCE_NAME = "clutch_agencies"
    BASE_URL = "https://clutch.co"
    REQUESTS_PER_MINUTE = 5  # Respectful rate

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_slugs: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield category listing page URLs with pagination."""
        # Start with sitemap for bulk URL discovery
        yield f"{self.BASE_URL}/sitemap.xml"
        yield f"{self.BASE_URL}/sitemap-agencies.xml"

        # Paginate through category listings
        for category in CATEGORIES:
            for page in range(0, MAX_PAGES_PER_CATEGORY):
                if page == 0:
                    yield f"{self.BASE_URL}/{category}"
                else:
                    yield f"{self.BASE_URL}/{category}?page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a Clutch listing page or sitemap."""
        if "sitemap" in url:
            return self._parse_sitemap(url, html)
        return self._parse_listing_page(url, html)

    def _parse_sitemap(self, url: str, xml_text: str) -> list[ScrapedContact]:
        """Extract agency profile URLs from sitemap and fetch them."""
        contacts = []
        urls = re.findall(r"<loc>(https?://[^<]+)</loc>", xml_text)

        # Filter for agency profile URLs: /profile/{slug}
        profile_urls = [
            u for u in urls
            if "/profile/" in u and "#" not in u
        ]

        self.logger.info("Sitemap %s: found %d profile URLs", url, len(profile_urls))

        for profile_url in profile_urls[:2000]:  # Cap per sitemap
            slug = profile_url.rstrip("/").split("/")[-1]
            if slug in self._seen_slugs:
                continue
            self._seen_slugs.add(slug)

            profile_html = self.fetch_page(profile_url)
            if profile_html:
                contact = self._parse_profile(profile_url, profile_html)
                if contact:
                    contacts.append(contact)

        return contacts

    def _parse_listing_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a category listing page for agency cards."""
        soup = self.parse_html(html)
        contacts = []

        # Find profile links in listing cards
        profile_links = set()
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if "/profile/" in href:
                full_url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                # Strip query params and anchors
                full_url = full_url.split("?")[0].split("#")[0]
                slug = full_url.rstrip("/").split("/")[-1]
                if slug and slug not in self._seen_slugs:
                    self._seen_slugs.add(slug)
                    profile_links.add(full_url)

        if not profile_links:
            # Try to parse inline listing data
            return self._parse_inline_listings(soup, url)

        for profile_url in profile_links:
            profile_html = self.fetch_page(profile_url)
            if profile_html:
                contact = self._parse_profile(profile_url, profile_html)
                if contact:
                    contacts.append(contact)

        return contacts

    def _parse_inline_listings(self, soup, source_url: str) -> list[ScrapedContact]:
        """Parse agency info directly from listing cards without fetching profiles."""
        contacts = []

        # Clutch listing cards typically have company name, location, services
        for card in soup.find_all(class_=re.compile(
            r"provider|company|agency|listing|result|card", re.I
        )):
            name = ""
            website = ""
            bio = ""

            # Company name from heading
            for tag in ["h3", "h2", "h4"]:
                heading = card.find(tag)
                if heading:
                    link = heading.find("a", href=True)
                    if link:
                        name = link.get_text(strip=True)
                        href = link.get("href", "")
                        if "/profile/" in href:
                            website = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                    else:
                        name = heading.get_text(strip=True)
                    break

            if not name or len(name) < 2:
                continue

            name_key = name.lower().strip()
            if name_key in self._seen_slugs:
                continue
            self._seen_slugs.add(name_key)

            # Location and tagline
            card_text = card.get_text(separator=" | ", strip=True)
            if len(card_text) > len(name):
                bio = card_text[:500]

            contacts.append(ScrapedContact(
                name=name,
                email="",
                company=name,
                website=website,
                linkedin="",
                phone="",
                bio=f"B2B Agency | {bio}" if bio else f"B2B Agency on Clutch.co",
                source_url=source_url,
                source_category="agency",
            ))

        return contacts

    def _parse_profile(self, url: str, html: str) -> ScrapedContact | None:
        """Parse a Clutch agency profile page."""
        soup = self.parse_html(html)

        # Company name
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

        # Clean name - remove suffixes like "| Clutch.co"
        name = re.sub(r"\s*[|]\s*Clutch.*$", "", name, flags=re.I).strip()

        # Meta description for bio
        bio = ""
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            bio = (meta_desc.get("content") or "")[:500]

        # Website
        website = ""
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            text = a.get_text(strip=True).lower()
            if (
                "visit website" in text
                or "website" in text
                or (a.get("class") and any("website" in c.lower() for c in a.get("class", [])))
            ):
                if href.startswith("http") and "clutch.co" not in href.lower():
                    website = href
                    break

        # Fallback: look for external links
        if not website:
            for a in soup.find_all("a", href=True, rel=lambda x: x and "nofollow" in x):
                href = a.get("href", "")
                if href.startswith("http") and "clutch.co" not in href.lower():
                    website = href
                    break

        # Email and phone
        emails = self.extract_emails(html)
        email = emails[0] if emails else ""

        phone = ""
        phone_match = re.search(
            r"(?:1[-.\s]?)?(?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4}", html
        )
        if phone_match:
            phone = phone_match.group(0)

        linkedin = self.extract_linkedin(html)

        # Location
        location = ""
        loc_el = soup.find(class_=re.compile(r"location|locality|address", re.I))
        if loc_el:
            location = loc_el.get_text(strip=True)[:100]

        # Services / tagline
        tagline = ""
        tagline_el = soup.find(class_=re.compile(r"tagline|summary|description", re.I))
        if tagline_el:
            tagline = tagline_el.get_text(strip=True)[:200]

        # Build bio
        bio_parts = ["B2B Agency"]
        if location:
            bio_parts.append(location)
        if tagline:
            bio_parts.append(tagline)
        elif bio:
            bio_parts.append(bio[:300])
        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=name,
            email=email,
            company=name,
            website=website or url,
            linkedin=linkedin,
            phone=phone,
            bio=bio,
            source_url=url,
            source_category="agency",
        )
