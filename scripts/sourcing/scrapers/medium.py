"""
Medium business/coaching writer scraper.

Medium has public tag pages and top writer lists. Profile pages
expose author bio, website, and social links.

Focus: business, coaching, personal development, entrepreneurship.

Estimated yield: 1,000-2,000 writers
"""

from __future__ import annotations

import re
from typing import Iterator
from urllib.parse import urljoin

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Medium tags relevant to JV candidates
TAGS = [
    "coaching",
    "business",
    "entrepreneurship",
    "personal-development",
    "self-improvement",
    "leadership",
    "marketing",
    "productivity",
    "mindset",
    "health",
    "wellness",
    "motivation",
    "self-help",
    "startup",
    "digital-marketing",
    "personal-growth",
    "online-business",
    "consulting",
    "public-speaking",
    "writing",
    "freelancing",
    "mental-health",
    "finance",
    "relationships",
    "success",
]

MAX_PAGES = 10  # Medium pagination is limited


class Scraper(BaseScraper):
    SOURCE_NAME = "medium"
    BASE_URL = "https://medium.com"
    REQUESTS_PER_MINUTE = 5  # Medium can be aggressive with rate limiting

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_authors: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield Medium tag pages and recommended lists."""
        for tag in TAGS:
            yield f"{self.BASE_URL}/tag/{tag}"
            yield f"{self.BASE_URL}/tag/{tag}/recommended"
            yield f"{self.BASE_URL}/tag/{tag}/latest"

        # Top writers page
        yield f"{self.BASE_URL}/top-writers"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Medium tag/listing page for author links."""
        soup = self.parse_html(html)
        contacts = []

        # Find author profile links
        author_links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Medium author URLs: medium.com/@username or medium.com/u/userid
            if re.match(r"https?://medium\.com/@[a-zA-Z0-9._\-]+$", href):
                username = href.rstrip("/").split("/@")[-1]
                if username and username not in self._seen_authors:
                    self._seen_authors.add(username)
                    author_links.add(href)
            elif "/@" in href and href.startswith("/"):
                username = href.split("/@")[-1].split("/")[0].split("?")[0]
                if username and username not in self._seen_authors:
                    self._seen_authors.add(username)
                    author_links.add(f"{self.BASE_URL}/@{username}")

        if not author_links:
            return []

        # Fetch author profiles
        for profile_url in author_links:
            profile_html = self.fetch_page(profile_url)
            if not profile_html:
                continue
            contact = self._parse_author_profile(profile_url, profile_html)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_author_profile(self, url: str, html: str) -> ScrapedContact | None:
        """Extract author info from their Medium profile."""
        soup = self.parse_html(html)

        # Name
        name = ""
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        if not name:
            h2 = soup.find("h2")
            if h2:
                name = h2.get_text(strip=True)

        if not name:
            og = soup.find("meta", property="og:title")
            if og:
                name = og.get("content", "").split("|")[0].split("–")[0].strip()

        if not name or len(name) < 2:
            return None

        # Skip if name looks like a publication name
        if any(kw in name.lower() for kw in ["publication", "magazine", "journal", "the "]):
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

        # Follower count
        for el in soup.find_all(string=re.compile(r"\d+\s*[Ff]ollower")):
            bio = f"{bio} | {el.strip()}" if bio else el.strip()
            break

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
                and "medium.com" not in href_lower
                and "twitter.com" not in href_lower
                and "facebook.com" not in href_lower
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
            website=website,
            linkedin=linkedin,
            bio=bio,
            source_url=url,
            source_category="content_creators",
        )
