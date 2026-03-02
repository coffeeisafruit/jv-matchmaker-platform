"""
F6S Global Startup Directory Scraper

Scrapes startup/company profiles from https://www.f6s.com/companies.

F6S is the world's largest startup community with 1.6M+ tech startups.
The site uses infinite scroll with JS rendering, but company profile pages
are accessible as standard HTML. We scrape by iterating through category
and location filters, then fetching individual profile pages.

Strategy:
  1. Hit the public company search HTML pages with category/industry filters
  2. Parse listing cards (div.bordered-list-item) for company links
  3. Fetch individual company profile pages for full details
  4. Extract: company name, website, description, location, founder info

Estimated yield: 5,000-15,000 startups (across all categories)
"""

from __future__ import annotations

import re
import json
from typing import Iterator
from urllib.parse import urljoin, quote_plus

from scripts.sourcing.base import BaseScraper, ScrapedContact


# F6S browse categories for startup companies
CATEGORIES = [
    "tech-startups",
    "saas-startups",
    "fintech-startups",
    "healthtech-startups",
    "edtech-startups",
    "ai-startups",
    "ecommerce-startups",
    "marketing-startups",
    "cybersecurity-startups",
    "blockchain-startups",
    "cleantech-startups",
    "biotech-startups",
    "iot-startups",
    "mobile-startups",
    "data-startups",
    "social-media-startups",
    "hr-startups",
    "logistics-startups",
    "real-estate-startups",
    "travel-startups",
    "food-startups",
    "gaming-startups",
    "media-startups",
    "enterprise-startups",
    "legal-startups",
]

# Number of listing pages to crawl per category
MAX_PAGES_PER_CATEGORY = 20


class Scraper(BaseScraper):
    SOURCE_NAME = "f6s_startups"
    BASE_URL = "https://www.f6s.com"
    REQUESTS_PER_MINUTE = 6  # Conservative to avoid blocks

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_slugs: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield F6S company listing page URLs across categories."""
        # Main companies page
        yield f"{self.BASE_URL}/companies"

        # Category-specific listings
        for category in CATEGORIES:
            for page in range(1, MAX_PAGES_PER_CATEGORY + 1):
                yield f"{self.BASE_URL}/companies/{category}?page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse an F6S listing page and extract company profiles."""
        soup = self.parse_html(html)
        contacts = []

        # F6S uses div.bordered-list-item for listing cards
        cards = soup.find_all("div", class_="bordered-list-item")

        # Also try other common F6S card selectors
        if not cards:
            cards = soup.find_all("div", class_="result-item")
        if not cards:
            cards = soup.find_all("div", class_="startup-card")
        if not cards:
            # Fallback: look for any links to /company/ paths
            cards = []
            for a_tag in soup.find_all("a", href=True):
                href = a_tag.get("href", "")
                if "/company/" in href:
                    parent = a_tag.find_parent("div")
                    if parent and parent not in cards:
                        cards.append(parent)

        for card in cards:
            contact = self._parse_card(card)
            if contact:
                contacts.append(contact)

        # If no cards found, try to extract profile links and fetch them
        if not contacts:
            profile_urls = self._extract_profile_links(soup)
            for profile_url in profile_urls:
                profile_html = self.fetch_page(profile_url)
                if profile_html:
                    contact = self._parse_profile_page(profile_url, profile_html)
                    if contact:
                        contacts.append(contact)

        return contacts

    def _extract_profile_links(self, soup) -> list[str]:
        """Extract company profile links from a listing page."""
        links = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            # F6S company profile URLs: /company/{slug} or /{slug}
            if re.match(r"^/company/[a-zA-Z0-9_\-]+/?$", href):
                slug = href.strip("/").split("/")[-1]
                if slug and slug not in self._seen_slugs:
                    self._seen_slugs.add(slug)
                    links.append(urljoin(self.BASE_URL, href))
        return links[:30]  # Cap per page to avoid runaway fetches

    def _parse_card(self, card) -> ScrapedContact | None:
        """Parse a single company card from the listing page."""
        # Try to extract company name from title link
        name = ""
        website = ""
        slug = ""

        title_link = card.find("a", class_="title") or card.find("a")
        if title_link:
            name = title_link.get_text(strip=True)
            href = title_link.get("href", "")
            if href.startswith("/company/") or href.startswith("/"):
                slug = href.strip("/").split("/")[-1]
                if slug in self._seen_slugs:
                    return None
                self._seen_slugs.add(slug)
                website = urljoin(self.BASE_URL, href)

        if not name or len(name) < 2:
            return None

        # Extract subtitle/location
        location = ""
        subtitle = card.find(class_="subtitle")
        if subtitle:
            spans = subtitle.find_all("span")
            for span in spans:
                text = span.get_text(strip=True)
                if text and len(text) > 1:
                    location = text
                    break

        # Extract description
        description = ""
        desc_el = card.find(class_="description") or card.find("p")
        if desc_el:
            description = desc_el.get_text(strip=True)

        # Build bio
        bio_parts = []
        if description:
            bio_parts.append(description)
        if location:
            bio_parts.append(f"Location: {location}")
        bio = " | ".join(bio_parts) if bio_parts else f"Startup listed on F6S"

        return ScrapedContact(
            name=name,
            company=name,
            website=website,
            bio=bio,
            source_category="startups",
        )

    def _parse_profile_page(self, url: str, html: str) -> ScrapedContact | None:
        """Parse a full F6S company profile page."""
        soup = self.parse_html(html)

        # Company name from h1 or og:title
        name = ""
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        if not name:
            og_title = soup.find("meta", property="og:title")
            if og_title:
                name = (og_title.get("content", "") or "").split("|")[0].strip()

        if not name or len(name) < 2:
            return None

        # Description from meta or page content
        bio = ""
        og_desc = soup.find("meta", property="og:description")
        if og_desc:
            bio = (og_desc.get("content", "") or "")[:1000]

        if not bio:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc:
                bio = (meta_desc.get("content", "") or "")[:1000]

        # Website link - look for external links
        website = ""
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            text = a_tag.get_text(strip=True).lower()
            if ("website" in text or "visit" in text) and href.startswith("http"):
                if "f6s.com" not in href.lower():
                    website = href
                    break

        # Fallback: look for rel=nofollow external links
        if not website:
            for a_tag in soup.find_all("a", href=True, rel=True):
                rels = a_tag.get("rel", [])
                if "nofollow" in rels or "noopener" in rels:
                    href = a_tag["href"]
                    if href.startswith("http") and "f6s.com" not in href.lower():
                        website = href
                        break

        if not website:
            website = url  # Use the F6S profile URL as fallback

        # LinkedIn
        linkedin = self.extract_linkedin(html)

        # Email
        emails = self.extract_emails(html)
        email = emails[0] if emails else ""

        # Founder / team info
        founder = ""
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            if re.match(r"^/member/[a-zA-Z0-9_\-]+/?$", href):
                member_name = a_tag.get_text(strip=True)
                if member_name and len(member_name) > 2:
                    founder = member_name
                    break

        # Location
        location = ""
        loc_el = soup.find(class_=re.compile(r"location|city|address", re.I))
        if loc_el:
            location = loc_el.get_text(strip=True)

        if location and bio:
            bio = f"{bio} | Location: {location}"
        elif location:
            bio = f"Location: {location}"

        if founder and bio:
            bio = f"{bio} | Founder: {founder}"

        return ScrapedContact(
            name=name,
            email=email,
            company=name,
            website=website,
            linkedin=linkedin,
            bio=bio or f"Startup listed on F6S",
            source_url=url,
            source_category="startups",
        )
