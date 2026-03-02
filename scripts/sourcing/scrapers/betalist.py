"""
BetaList Startup Directory Scraper

Scrapes early-stage startup profiles from https://betalist.com/startups.

BetaList is a curated directory of early-stage startups. It uses a Rails app
with Hotwired (Turbo + Stimulus) for interactivity. Listings are standard
server-rendered HTML with cursor-based pagination (?page=N).

Strategy:
  1. Iterate through listing pages using ?page=N pagination
  2. Parse startup cards from each listing page (name, tagline, link)
  3. Fetch individual startup detail pages for full info
  4. Extract: startup name, tagline, full description, website, maker info

Individual startup URLs: /startups/{slug}
Visit links: /startups/{slug}/visit (redirect to actual website)

Estimated yield: 3,000-8,000 startups
"""

from __future__ import annotations

import re
from typing import Iterator
from urllib.parse import urljoin

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Topic/category pages on BetaList
TOPICS = [
    "",  # Main listing (all startups)
    "saas",
    "ai-tools",
    "developer-tools",
    "productivity",
    "marketing",
    "finance",
    "health",
    "education",
    "e-commerce",
    "social-media",
    "design",
    "analytics",
    "security",
    "automation",
    "no-code",
    "remote-work",
    "crypto",
    "sustainability",
    "hr",
    "real-estate",
    "travel",
    "food",
    "gaming",
    "music",
    "video",
    "writing",
    "api",
    "mobile-apps",
    "browser-extensions",
]

MAX_PAGES_PER_TOPIC = 30


class Scraper(BaseScraper):
    SOURCE_NAME = "betalist"
    BASE_URL = "https://betalist.com"
    REQUESTS_PER_MINUTE = 6

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_slugs: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield BetaList listing page URLs with pagination."""
        for topic in TOPICS:
            for page in range(1, MAX_PAGES_PER_TOPIC + 1):
                if topic:
                    if page == 1:
                        yield f"{self.BASE_URL}/topics/{topic}"
                    else:
                        yield f"{self.BASE_URL}/topics/{topic}?page={page}"
                else:
                    if page == 1:
                        yield f"{self.BASE_URL}/startups"
                    else:
                        yield f"{self.BASE_URL}/startups?page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a BetaList listing page for startup entries."""
        soup = self.parse_html(html)
        contacts = []

        # Find all startup links: /startups/{slug}
        startup_links = set()
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            match = re.match(r"^/startups/([a-zA-Z0-9_\-]+)$", href)
            if match:
                slug = match.group(1)
                # Skip non-startup pages
                if slug in ("new", "visit", "edit", "search"):
                    continue
                if slug not in self._seen_slugs:
                    self._seen_slugs.add(slug)
                    startup_links.add(slug)

        # For each startup found, try to get basic info from the listing
        # and optionally fetch the detail page
        for slug in startup_links:
            # First try to extract from listing page context
            contact = self._extract_from_listing(soup, slug)
            if contact:
                contacts.append(contact)
                continue

            # Fetch individual startup page for full details
            detail_url = f"{self.BASE_URL}/startups/{slug}"
            detail_html = self.fetch_page(detail_url)
            if detail_html:
                contact = self._parse_startup_page(detail_url, detail_html, slug)
                if contact:
                    contacts.append(contact)

        return contacts

    def _extract_from_listing(self, soup, slug: str) -> ScrapedContact | None:
        """Try to extract startup info from the listing page without fetching detail."""
        # Find the link to this startup
        link = soup.find("a", href=f"/startups/{slug}")
        if not link:
            return None

        name = link.get_text(strip=True)
        if not name or len(name) < 2:
            return None

        # Look for adjacent tagline text
        tagline = ""
        parent = link.find_parent()
        if parent:
            # The tagline is often in a sibling or child element
            for sibling in parent.find_next_siblings():
                text = sibling.get_text(strip=True)
                if text and len(text) > 5 and text != name:
                    tagline = text[:300]
                    break
            if not tagline:
                # Check for text nodes directly after the link
                next_text = link.find_next(string=True)
                if next_text:
                    text = next_text.strip()
                    if text and len(text) > 5 and text != name:
                        tagline = text[:300]

        # For listing-only data, we'll use the visit URL as website
        website = f"{self.BASE_URL}/startups/{slug}/visit"

        return ScrapedContact(
            name=name,
            company=name,
            website=website,
            bio=tagline or f"Early-stage startup listed on BetaList",
            source_url=f"{self.BASE_URL}/startups/{slug}",
            source_category="startups",
        )

    def _parse_startup_page(self, url: str, html: str, slug: str) -> ScrapedContact | None:
        """Parse a full BetaList startup detail page."""
        soup = self.parse_html(html)

        # Name from h1
        name = ""
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        if not name:
            og_title = soup.find("meta", property="og:title")
            if og_title:
                name = (og_title.get("content", "") or "").split("|")[0].split("-")[0].strip()

        if not name or len(name) < 2:
            return None

        # Tagline from h2 or meta description
        tagline = ""
        h2 = soup.find("h2")
        if h2:
            tagline = h2.get_text(strip=True)

        # Full description from page content
        description = ""
        og_desc = soup.find("meta", property="og:description")
        if og_desc:
            description = (og_desc.get("content", "") or "")[:1000]

        if not description:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc:
                description = (meta_desc.get("content", "") or "")[:1000]

        # Look for longer description in page body
        for p in soup.find_all("p"):
            text = p.get_text(strip=True)
            if text and len(text) > len(description) and len(text) > 50:
                # Skip navigation/boilerplate text
                if any(skip in text.lower() for skip in [
                    "betalist", "sign up", "log in", "subscribe",
                    "cookie", "privacy", "terms of service",
                ]):
                    continue
                description = text[:1000]
                break

        # Website - look for visit link
        website = ""
        visit_link = soup.find("a", href=re.compile(r"/startups/.+/visit"))
        if visit_link:
            website = f"{self.BASE_URL}{visit_link['href']}"

        # Also check for direct external links
        if not website:
            for a_tag in soup.find_all("a", href=True):
                href = a_tag.get("href", "")
                text = a_tag.get_text(strip=True).lower()
                if (
                    ("visit" in text or "website" in text or "site" in text)
                    and href.startswith("http")
                    and "betalist.com" not in href.lower()
                ):
                    website = href
                    break

        if not website:
            website = f"{self.BASE_URL}/startups/{slug}/visit"

        # Maker/founder info
        maker = ""
        maker_link = soup.find("a", href=re.compile(r"^/@[a-zA-Z0-9_]+$"))
        if maker_link:
            maker = maker_link.get_text(strip=True)
            # Clean up @ prefix
            if maker.startswith("@"):
                maker = maker[1:]

        # Topics/categories
        topics = []
        for a_tag in soup.find_all("a", href=re.compile(r"^/topics/")):
            topic_text = a_tag.get_text(strip=True)
            if topic_text:
                topics.append(topic_text)

        # Email from page
        emails = self.extract_emails(html)
        email = emails[0] if emails else ""

        # LinkedIn
        linkedin = self.extract_linkedin(html)

        # Build bio
        bio_parts = []
        if tagline:
            bio_parts.append(tagline)
        if description and description != tagline:
            bio_parts.append(description[:500])
        if maker:
            bio_parts.append(f"Maker: {maker}")
        if topics:
            bio_parts.append(f"Topics: {', '.join(topics[:5])}")
        bio = " | ".join(bio_parts) if bio_parts else f"Early-stage startup on BetaList"

        return ScrapedContact(
            name=name,
            email=email,
            company=name,
            website=website,
            linkedin=linkedin,
            bio=bio,
            source_url=url,
            source_category="startups",
            raw_data={
                "betalist_slug": slug,
                "maker": maker,
                "topics": topics,
            },
        )
