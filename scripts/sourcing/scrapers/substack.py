"""
Substack newsletter directory scraper.

Substack has public explore/discover pages for newsletters by category.
Newsletter pages expose author info, website links, and subscriber hints.

Focus: business, entrepreneurship, personal development, coaching.

Estimated yield: 1,500-2,500 newsletter authors
"""

from __future__ import annotations

import re
import json
from typing import Iterator
from urllib.parse import urljoin

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Substack category/explore pages
CATEGORIES = [
    "business",
    "finance",
    "technology",
    "culture",
    "health",
    "politics",
    "food",
    "sports",
    "faith",
    "education",
]

# Search terms for more targeted discovery
SEARCH_TERMS = [
    "coaching",
    "personal development",
    "entrepreneurship",
    "marketing",
    "leadership",
    "mindset",
    "self improvement",
    "business strategy",
    "consulting",
    "course creator",
    "speaker",
    "productivity",
    "wellness",
    "affiliate marketing",
]

MAX_PAGES = 20


class Scraper(BaseScraper):
    SOURCE_NAME = "substack"
    BASE_URL = "https://substack.com"
    REQUESTS_PER_MINUTE = 6

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_substacks: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield Substack explore page URLs."""
        # Category explore pages
        for cat in CATEGORIES:
            yield f"{self.BASE_URL}/discover/category/{cat}"

        # Search-based discovery
        for term in SEARCH_TERMS:
            yield f"{self.BASE_URL}/search/{term.replace(' ', '%20')}?searching=publication"

        # Top publications pages
        yield f"{self.BASE_URL}/discover"
        for cat in CATEGORIES:
            for page in range(2, MAX_PAGES + 1):
                yield f"{self.BASE_URL}/discover/category/{cat}?page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Substack explore/search page for newsletter links."""
        soup = self.parse_html(html)
        contacts = []

        # Find links to individual Substack publications
        substack_links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Substack publications are at *.substack.com
            match = re.match(r"https?://([a-z0-9\-]+)\.substack\.com", href)
            if match:
                subdomain = match.group(1)
                if subdomain not in self._seen_substacks and subdomain not in {
                    "www", "open", "support", "on", "reader", "api",
                }:
                    self._seen_substacks.add(subdomain)
                    substack_links.add(f"https://{subdomain}.substack.com")

            # Also handle substack.com/@username or substack.com/profile/username
            if "/profile/" in href or "/@" in href:
                slug = href.rstrip("/").split("/")[-1].lstrip("@")
                if slug and slug not in self._seen_substacks:
                    self._seen_substacks.add(slug)
                    substack_links.add(href if href.startswith("http") else urljoin(self.BASE_URL, href))

        if not substack_links:
            return []

        # Fetch each publication's about page
        for pub_url in substack_links:
            about_url = f"{pub_url.rstrip('/')}/about"
            page_html = self.fetch_page(about_url)
            if not page_html:
                # Try the main page
                page_html = self.fetch_page(pub_url)
            if not page_html:
                continue

            contact = self._parse_publication(pub_url, page_html)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_publication(self, url: str, html: str) -> ScrapedContact | None:
        """Extract author info from a Substack publication page."""
        soup = self.parse_html(html)

        # Publication name
        pub_name = ""
        h1 = soup.find("h1")
        if h1:
            pub_name = h1.get_text(strip=True)

        # Author name
        author_name = ""
        # Look for author section
        for el in soup.find_all(class_=re.compile(r"author|byline|creator|writer", re.I)):
            text = el.get_text(strip=True)
            if text and len(text) > 2 and len(text) < 80:
                author_name = text
                break

        # Try og:site_name or og:title
        if not author_name:
            og_author = soup.find("meta", attrs={"name": "author"})
            if og_author:
                author_name = og_author.get("content", "")

        name = author_name if author_name else pub_name
        if not name or len(name) < 2:
            return None

        # Bio/description
        bio = ""
        desc_el = soup.find(class_=re.compile(r"description|about|subtitle", re.I))
        if desc_el:
            bio = desc_el.get_text(strip=True)[:800]

        if not bio:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc:
                bio = meta_desc.get("content", "")[:500]

        if pub_name and pub_name != name:
            bio = f"Newsletter: {pub_name} | {bio}" if bio else f"Newsletter: {pub_name}"

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
                and "substack.com" not in href_lower
                and "facebook.com" not in href_lower
                and "twitter.com" not in href_lower
                and "instagram.com" not in href_lower
                and not website
            ):
                website = href

        if not website:
            website = url

        emails = self.extract_emails(html)
        email = emails[0] if emails else ""

        return ScrapedContact(
            name=name,
            email=email,
            company=pub_name if pub_name != name else "",
            website=website,
            linkedin=linkedin,
            bio=bio,
            source_url=url,
            source_category="newsletter_creators",
        )
