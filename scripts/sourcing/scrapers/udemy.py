"""
Udemy instructor scraper.

Udemy has a public API-like endpoint for course listings:
  https://www.udemy.com/api-2.0/courses/?category=Business&page=1&page_size=60

Instructor profiles are publicly accessible at:
  https://www.udemy.com/user/{slug}/

Categories: Business, Personal Development, Health & Fitness,
Lifestyle, Marketing, Office Productivity.

Estimated yield: 2,000-4,000 unique instructors
"""

from __future__ import annotations

import re
from typing import Iterator
from urllib.parse import urljoin, quote

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Udemy category slugs relevant to JV candidates
CATEGORIES = [
    "Business",
    "Personal-Development",
    "Health-and-Fitness",
    "Lifestyle",
    "Marketing",
    "Office-Productivity",
]

# Subcategories for more targeted scraping
SUBCATEGORIES = [
    "Business/Entrepreneurship",
    "Business/Communication",
    "Business/Management",
    "Business/Sales",
    "Business/Business-Strategy",
    "Personal-Development/Personal-Transformation",
    "Personal-Development/Leadership",
    "Personal-Development/Career-Development",
    "Personal-Development/Happiness",
    "Personal-Development/Motivation",
    "Marketing/Digital-Marketing",
    "Marketing/Social-Media-Marketing",
    "Marketing/Branding",
    "Marketing/Content-Marketing",
    "Marketing/Affiliate-Marketing",
    "Health-and-Fitness/Mental-Health",
    "Health-and-Fitness/Yoga",
    "Health-and-Fitness/Nutrition",
]

MAX_PAGES_PER_CATEGORY = 40
PAGE_SIZE = 60


class Scraper(BaseScraper):
    SOURCE_NAME = "udemy"
    BASE_URL = "https://www.udemy.com"
    REQUESTS_PER_MINUTE = 8

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_instructors: set[str] = set()
        # Udemy API requires basic auth with their client ID/secret
        # But the web-facing course listing pages work without auth
        self.session.headers.update({
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
        })

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield course listing page URLs."""
        # Try the web-facing course listings
        for subcat in SUBCATEGORIES:
            for page in range(1, MAX_PAGES_PER_CATEGORY + 1):
                # Web URL for course listings
                yield f"{self.BASE_URL}/courses/{subcat}/?p={page}&lang=en"

        # Also try top-level categories
        for cat in CATEGORIES:
            for page in range(1, MAX_PAGES_PER_CATEGORY + 1):
                yield f"{self.BASE_URL}/courses/{cat}/?p={page}&lang=en"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse course listing page for instructor info."""
        soup = self.parse_html(html)
        contacts = []

        # Look for instructor links in course cards
        # Udemy course cards contain instructor info
        instructor_links = set()

        # Find all links to user profiles
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/user/" in href:
                slug = href.rstrip("/").split("/user/")[-1].split("/")[0].split("?")[0]
                if slug and slug not in self._seen_instructors:
                    self._seen_instructors.add(slug)
                    instructor_links.add(slug)

        # Also look for instructor names in JSON-LD structured data
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                import json
                data = json.loads(script.string or "")
                if isinstance(data, list):
                    for item in data:
                        self._extract_from_jsonld(item, instructor_links)
                elif isinstance(data, dict):
                    self._extract_from_jsonld(data, instructor_links)
            except (json.JSONDecodeError, TypeError):
                pass

        # Fetch each instructor profile
        for slug in instructor_links:
            profile_url = f"{self.BASE_URL}/user/{slug}/"
            profile_html = self.fetch_page(profile_url)
            if not profile_html:
                continue
            contact = self._parse_instructor_profile(profile_url, profile_html)
            if contact:
                contacts.append(contact)

        return contacts

    def _extract_from_jsonld(self, data: dict, instructor_links: set) -> None:
        """Extract instructor info from JSON-LD structured data."""
        instructors = data.get("instructor", [])
        if isinstance(instructors, dict):
            instructors = [instructors]
        for inst in instructors:
            if isinstance(inst, dict):
                url = inst.get("url", "")
                if "/user/" in url:
                    slug = url.rstrip("/").split("/user/")[-1].split("/")[0]
                    if slug and slug not in self._seen_instructors:
                        self._seen_instructors.add(slug)
                        instructor_links.add(slug)

    def _parse_instructor_profile(self, url: str, html: str) -> ScrapedContact | None:
        """Extract instructor details from their profile page."""
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

        # Bio/headline
        bio = ""
        # Look for instructor headline
        headline_el = soup.find(attrs={"data-purpose": re.compile(r"instructor-headline")})
        if headline_el:
            bio = headline_el.get_text(strip=True)

        # Extended bio
        bio_el = soup.find(attrs={"data-purpose": re.compile(r"instructor-description|instructor-bio")})
        if bio_el:
            extended = bio_el.get_text(strip=True)[:800]
            bio = f"{bio} | {extended}" if bio else extended

        if not bio:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc:
                bio = meta_desc.get("content", "")[:500]

        # Stats (students, courses, reviews)
        stats_parts = []
        for stat_el in soup.find_all(attrs={"data-purpose": re.compile(r"stat")}):
            text = stat_el.get_text(strip=True)
            if text:
                stats_parts.append(text)
        if stats_parts:
            bio = f"{bio} | Udemy stats: {', '.join(stats_parts[:4])}" if bio else f"Udemy stats: {', '.join(stats_parts[:4])}"

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
                and "udemy.com" not in href_lower
                and "facebook.com" not in href_lower
                and "twitter.com" not in href_lower
                and "youtube.com" not in href_lower
                and not website
            ):
                website = href

        # If no external website, use Udemy profile
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
            source_category="course_creators",
        )
