"""
Gumroad creator directory scraper.

Gumroad's discover page lists products by category. Creator profiles
are at gumroad.com/{username} and contain bio, products, and links.

Focus categories: business, coaching, courses, self-help, marketing.

Estimated yield: 2,000-3,000 creators
"""

from __future__ import annotations

import re
from typing import Iterator
from urllib.parse import urljoin

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Gumroad discover categories relevant to JV
DISCOVER_CATEGORIES = [
    "business",
    "self-improvement",
    "education",
    "health-fitness",
    "online-courses",
    "writing-publishing",
    "productivity",
    "marketing",
    "finance",
    "coaching",
]

# Search queries for additional discovery
SEARCH_QUERIES = [
    "coaching program",
    "business course",
    "marketing course",
    "self help guide",
    "personal development",
    "mindset",
    "leadership",
    "entrepreneurship",
    "consulting template",
    "workshop recording",
]

MAX_PAGES = 20


class Scraper(BaseScraper):
    SOURCE_NAME = "gumroad"
    BASE_URL = "https://gumroad.com"
    REQUESTS_PER_MINUTE = 6

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_creators: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield discover/search page URLs."""
        # Category browsing
        for cat in DISCOVER_CATEGORIES:
            for page in range(1, MAX_PAGES + 1):
                yield f"{self.BASE_URL}/discover?query=&tags[]={cat}&page={page}"

        # Search queries
        for query in SEARCH_QUERIES:
            for page in range(1, MAX_PAGES + 1):
                yield f"{self.BASE_URL}/discover?query={query.replace(' ', '+')}&page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse product listing page for creator info."""
        soup = self.parse_html(html)
        contacts = []

        # Find creator profile links
        creator_links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Creator profiles are at root paths like /username
            # Product pages are at /l/product-slug
            if href.startswith("https://") and "gumroad.com" in href:
                # Extract username from product or profile URLs
                path = href.replace("https://gumroad.com", "").replace("https://www.gumroad.com", "")
                parts = path.strip("/").split("/")
                if parts and len(parts) == 1 and not parts[0].startswith("l") and len(parts[0]) > 1:
                    username = parts[0].split("?")[0]
                    if username not in self._seen_creators and username not in {"discover", "features", "pricing", "about", "blog", "signup", "login"}:
                        self._seen_creators.add(username)
                        creator_links.add(f"{self.BASE_URL}/{username}")

            # Also handle subdomain format (creator.gumroad.com)
            match = re.match(r"https?://([a-z0-9\-]+)\.gumroad\.com", href)
            if match:
                username = match.group(1)
                if username not in self._seen_creators and username not in {"www", "app", "help", "discover"}:
                    self._seen_creators.add(username)
                    creator_links.add(f"https://{username}.gumroad.com")

        if not creator_links:
            return []

        # Fetch creator profiles
        for profile_url in creator_links:
            profile_html = self.fetch_page(profile_url)
            if not profile_html:
                continue
            contact = self._parse_creator_profile(profile_url, profile_html)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_creator_profile(self, url: str, html: str) -> ScrapedContact | None:
        """Extract creator info from their Gumroad profile."""
        soup = self.parse_html(html)

        # Name
        name = ""
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        if not name:
            og = soup.find("meta", property="og:title")
            if og:
                name = og.get("content", "").split("|")[0].split("-")[0].strip()

        if not name or len(name) < 2:
            return None

        # Bio
        bio = ""
        bio_el = soup.find(class_=re.compile(r"bio|about|description", re.I))
        if bio_el:
            bio = bio_el.get_text(strip=True)[:800]

        if not bio:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc:
                bio = meta_desc.get("content", "")[:500]

        # Products (as signal of what they create)
        products = []
        for product_el in soup.find_all(class_=re.compile(r"product|item|card", re.I)):
            product_name = product_el.get_text(strip=True)[:100]
            if product_name and len(product_name) > 3:
                products.append(product_name)

        if products:
            bio = f"{bio} | Products: {', '.join(products[:5])}" if bio else f"Products: {', '.join(products[:5])}"

        # External links
        website = ""
        linkedin = ""
        for a in soup.find_all("a", href=True):
            href = a["href"]
            href_lower = href.lower()
            if "linkedin.com/in/" in href_lower and not linkedin:
                linkedin = href
            elif (
                href.startswith("http")
                and "gumroad.com" not in href_lower
                and "facebook.com" not in href_lower
                and "twitter.com" not in href_lower
                and "instagram.com" not in href_lower
                and not website
            ):
                website = href

        # If no external website, use Gumroad profile
        if not website:
            website = url

        emails = self.extract_emails(html)
        email = emails[0] if emails else ""

        return ScrapedContact(
            name=name,
            email=email,
            website=website,
            linkedin=linkedin,
            bio=bio,
            source_url=url,
            source_category="digital_creators",
        )
