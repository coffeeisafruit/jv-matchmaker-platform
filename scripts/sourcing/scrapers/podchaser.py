"""
Podchaser podcast directory scraper.

Podchaser has a public directory of podcasts with host profiles,
descriptions, and social links. Focus on business/entrepreneurship
categories.

Estimated yield: 2,000-3,000 podcast hosts
"""

from __future__ import annotations

import re
from typing import Iterator
from urllib.parse import urljoin

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Podchaser category/list pages
CATEGORY_PATHS = [
    "/podcasts/best/business",
    "/podcasts/best/entrepreneurship",
    "/podcasts/best/marketing",
    "/podcasts/best/self-improvement",
    "/podcasts/best/health-fitness",
    "/podcasts/best/education",
    "/podcasts/best/careers",
    "/podcasts/best/investing",
    "/podcasts/best/management",
    "/podcasts/best/personal-finance",
    "/podcasts/best/mental-health",
    "/podcasts/best/motivation",
    "/podcasts/best/leadership",
    "/podcasts/best/tech",
    "/podcasts/best/communication",
]

MAX_PAGES_PER_CATEGORY = 30


class Scraper(BaseScraper):
    SOURCE_NAME = "podchaser"
    BASE_URL = "https://www.podchaser.com"
    REQUESTS_PER_MINUTE = 6

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_podcasts: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield category listing URLs with pagination."""
        for path in CATEGORY_PATHS:
            for page in range(1, MAX_PAGES_PER_CATEGORY + 1):
                if page == 1:
                    yield f"{self.BASE_URL}{path}"
                else:
                    yield f"{self.BASE_URL}{path}?page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse podcast listing page, then fetch individual podcast pages."""
        soup = self.parse_html(html)
        contacts = []

        # Find podcast links
        podcast_links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Podchaser podcast URLs look like /podcasts/slug-id
            if re.match(r"/podcasts/[a-z0-9\-]+$", href) or re.match(r"/podcasts/[a-z0-9\-]+-\d+$", href):
                slug = href.rstrip("/").split("/")[-1]
                if slug and slug not in self._seen_podcasts:
                    self._seen_podcasts.add(slug)
                    podcast_links.add(urljoin(self.BASE_URL, href))

        if not podcast_links:
            return []

        # Fetch each podcast page for host info
        for podcast_url in podcast_links:
            page_html = self.fetch_page(podcast_url)
            if not page_html:
                continue
            contact = self._parse_podcast_page(podcast_url, page_html)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_podcast_page(self, url: str, html: str) -> ScrapedContact | None:
        """Extract host info from a podcast page."""
        soup = self.parse_html(html)

        # Podcast title
        title = ""
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)

        # Host/creator name
        host_name = ""
        # Look for creator/host section
        for label in soup.find_all(string=re.compile(r"Host|Creator|Author|Producer", re.I)):
            parent = label.find_parent()
            if parent:
                # Next sibling or next element might have the name
                next_el = parent.find_next_sibling() or parent.find_next()
                if next_el:
                    candidate = next_el.get_text(strip=True)
                    if candidate and len(candidate) < 100 and len(candidate) > 2:
                        host_name = candidate
                        break

        # Fallback: Look for creator links
        if not host_name:
            for a in soup.find_all("a", href=True):
                if "/creators/" in a["href"] or "/people/" in a["href"]:
                    host_name = a.get_text(strip=True)
                    if host_name and len(host_name) > 2:
                        break

        # If no separate host name found, use podcast title
        name = host_name if host_name else title
        if not name or len(name) < 2:
            return None

        # Description/bio
        bio = ""
        desc_el = soup.find(class_=re.compile(r"description|about|summary", re.I))
        if desc_el:
            bio = desc_el.get_text(strip=True)[:800]

        if not bio:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc:
                bio = meta_desc.get("content", "")[:500]

        if title and title != name:
            bio = f"Podcast: {title} | {bio}" if bio else f"Podcast: {title}"

        # Website and social links
        website = ""
        linkedin = ""
        for a in soup.find_all("a", href=True):
            href = a["href"]
            href_lower = href.lower()
            if "linkedin.com/in/" in href_lower and not linkedin:
                linkedin = href
            elif (
                href.startswith("http")
                and "podchaser.com" not in href_lower
                and "apple.com" not in href_lower
                and "spotify.com" not in href_lower
                and "google.com" not in href_lower
                and "facebook.com" not in href_lower
                and "twitter.com" not in href_lower
                and "instagram.com" not in href_lower
                and "youtube.com" not in href_lower
                and "amazon.com" not in href_lower
                and not website
            ):
                website = href

        emails = self.extract_emails(html)
        email = emails[0] if emails else ""

        return ScrapedContact(
            name=name,
            email=email,
            company=title if title != name else "",
            website=website,
            linkedin=linkedin,
            bio=bio,
            source_url=url,
            source_category="podcasters",
        )
