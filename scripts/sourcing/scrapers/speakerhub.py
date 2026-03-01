"""
SpeakerHub directory scraper.

SpeakerHub (speakerhub.com) is a public speaker directory with clean HTML.
Speakers have profile pages with name, bio, topics, website, and social links.

Categories to scrape:
  - Business, Leadership, Marketing, Personal Development,
    Entrepreneurship, Health & Wellness, Education

Estimated yield: 3,000-5,000 speakers
"""

from __future__ import annotations

import re
from typing import Iterator
from urllib.parse import urljoin

from scripts.sourcing.base import BaseScraper, ScrapedContact


# SpeakerHub category slugs and their page counts (approximate)
CATEGORIES = [
    "business",
    "leadership",
    "marketing",
    "personal-development",
    "entrepreneurship",
    "motivation",
    "health-wellness",
    "education",
    "communication",
    "sales",
    "innovation",
    "digital-marketing",
    "social-media",
    "coaching",
    "management",
    "human-resources",
    "finance",
    "technology",
    "mindfulness",
    "productivity",
    "women-in-business",
    "diversity-inclusion",
    "change-management",
    "customer-experience",
    "branding",
]

MAX_PAGES_PER_CATEGORY = 50  # Safety limit


class Scraper(BaseScraper):
    SOURCE_NAME = "speakerhub"
    BASE_URL = "https://speakerhub.com"
    REQUESTS_PER_MINUTE = 8  # Conservative politeness

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_slugs: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield paginated category listing URLs."""
        for category in CATEGORIES:
            for page in range(1, MAX_PAGES_PER_CATEGORY + 1):
                yield f"{self.BASE_URL}/speakers?topic={category}&page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a category listing page for speaker cards, then fetch profiles."""
        soup = self.parse_html(html)
        contacts = []

        # Find speaker cards on listing page
        speaker_links = set()
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "/speaker/" in href or "/speakers/" in href:
                full_url = urljoin(self.BASE_URL, href)
                # Extract slug to deduplicate
                slug = href.rstrip("/").split("/")[-1]
                if slug and slug not in self._seen_slugs:
                    self._seen_slugs.add(slug)
                    speaker_links.add(full_url)

        if not speaker_links:
            # No speakers found = likely past last page for this category
            return []

        # Fetch each speaker's profile page
        for profile_url in speaker_links:
            profile_html = self.fetch_page(profile_url)
            if not profile_html:
                continue

            contact = self._parse_profile(profile_url, profile_html)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_profile(self, url: str, html: str) -> ScrapedContact | None:
        """Extract contact info from a speaker profile page."""
        soup = self.parse_html(html)

        # Name - usually in h1 or a specific class
        name = ""
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        if not name:
            # Try meta og:title
            og_title = soup.find("meta", property="og:title")
            if og_title:
                name = og_title.get("content", "").split("|")[0].strip()

        if not name or len(name) < 2:
            return None

        # Bio / tagline
        bio = ""
        bio_div = (
            soup.find("div", class_=re.compile(r"bio|about|description", re.I))
            or soup.find("p", class_=re.compile(r"bio|tagline|about", re.I))
        )
        if bio_div:
            bio = bio_div.get_text(strip=True)[:1000]

        if not bio:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc:
                bio = meta_desc.get("content", "")[:500]

        # Topics / tags
        topics = []
        for tag_el in soup.find_all(class_=re.compile(r"tag|topic|badge", re.I)):
            text = tag_el.get_text(strip=True)
            if text and len(text) < 50:
                topics.append(text)

        if topics and bio:
            bio = f"{bio} | Topics: {', '.join(topics[:10])}"
        elif topics:
            bio = f"Topics: {', '.join(topics[:10])}"

        # Website
        website = ""
        linkedin = ""
        for a in soup.find_all("a", href=True):
            href = a["href"]
            href_lower = href.lower()
            # External website links
            if (
                "linkedin.com/in/" in href_lower
                and not linkedin
            ):
                linkedin = href
            elif (
                href.startswith("http")
                and "speakerhub.com" not in href_lower
                and "facebook.com" not in href_lower
                and "twitter.com" not in href_lower
                and "instagram.com" not in href_lower
                and "youtube.com" not in href_lower
                and "mailto:" not in href_lower
                and not website
            ):
                website = href

        # Email
        emails = self.extract_emails(html)
        email = emails[0] if emails else ""

        return ScrapedContact(
            name=name,
            email=email,
            website=website,
            linkedin=linkedin,
            bio=bio,
            company="",  # Not always available
            source_url=url,
            source_category="speakers",
        )
