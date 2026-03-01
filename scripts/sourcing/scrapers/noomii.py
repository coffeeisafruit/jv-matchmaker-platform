"""
Noomii.com coach directory scraper.

Noomii is one of the largest public coaching directories with
standard HTML pagination and clean profile pages.

Categories: business, executive, life, career, health, leadership,
relationship, financial, wellness, performance.

Estimated yield: 1,500-2,500 coaches
"""

from __future__ import annotations

import re
from typing import Iterator
from urllib.parse import urljoin

from scripts.sourcing.base import BaseScraper, ScrapedContact


CATEGORIES = [
    "business-coaches",
    "executive-coaches",
    "life-coaches",
    "career-coaches",
    "health-coaches",
    "leadership-coaches",
    "relationship-coaches",
    "financial-coaches",
    "wellness-coaches",
    "performance-coaches",
    "marketing-coaches",
    "sales-coaches",
    "entrepreneurship-coaches",
    "personal-development-coaches",
    "communication-coaches",
    "confidence-coaches",
    "stress-management-coaches",
    "small-business-coaches",
    "startup-coaches",
    "spiritual-coaches",
    "success-coaches",
    "transition-coaches",
    "womens-coaches",
    "mindset-coaches",
]

MAX_PAGES_PER_CATEGORY = 30


class Scraper(BaseScraper):
    SOURCE_NAME = "noomii"
    BASE_URL = "https://www.noomii.com"
    REQUESTS_PER_MINUTE = 6

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_slugs: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield paginated category listing URLs."""
        for category in CATEGORIES:
            for page in range(1, MAX_PAGES_PER_CATEGORY + 1):
                if page == 1:
                    yield f"{self.BASE_URL}/{category}"
                else:
                    yield f"{self.BASE_URL}/{category}?page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a coach listing page and fetch individual profiles."""
        soup = self.parse_html(html)
        contacts = []

        # Find profile links on the listing page
        profile_links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Noomii profile URLs: /users/{slug}
            # Exclude review links like /users/{slug}#reviews
            if re.search(r"^/users/[a-z0-9\-]+$", href):
                full_url = urljoin(self.BASE_URL, href)
                slug = href.rstrip("/").split("/")[-1]
                if slug and slug not in self._seen_slugs:
                    self._seen_slugs.add(slug)
                    profile_links.add(full_url)

        if not profile_links:
            return []

        # Fetch each profile page
        for profile_url in profile_links:
            profile_html = self.fetch_page(profile_url)
            if not profile_html:
                continue
            contact = self._parse_profile(profile_url, profile_html)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_profile(self, url: str, html: str) -> ScrapedContact | None:
        """Extract contact info from a coach profile page."""
        soup = self.parse_html(html)

        # Name - look in h1 first
        name = ""
        h1 = soup.find("h1")
        if h1:
            # Remove nested tags (like <small>) and get clean text
            for small in h1.find_all("small"):
                small.decompose()
            name = h1.get_text(strip=True)

        # Fallback to schema.org JSON-LD data
        if not name:
            for script in soup.find_all("script", type="application/ld+json"):
                script_text = script.string
                if script_text and '"@type":"Person"' in script_text:
                    # Extract name from JSON-LD
                    match = re.search(r'"name"\s*:\s*"([^"]+)"', script_text)
                    if match:
                        name = match.group(1)
                        break

        # Fallback to og:title
        if not name:
            og = soup.find("meta", property="og:title")
            if og:
                name = og.get("content", "").split("|")[0].split("-")[0].strip()

        if not name or len(name) < 2:
            return None

        # Clean up name - remove coach titles and location suffixes
        name = re.sub(r"\s*[-–|]\s*(Life|Business|Executive|Career|Health|Leadership)\s+Coach.*$", "", name, flags=re.I)
        name = name.strip()

        # Bio - look in schema.org description first
        bio = ""
        for script in soup.find_all("script", type="application/ld+json"):
            script_text = script.string
            if script_text and '"description"' in script_text:
                match = re.search(r'"description"\s*:\s*"([^"]+)"', script_text)
                if match:
                    bio_text = match.group(1)
                    # Unescape common entities
                    bio_text = bio_text.replace(r'\r\n', ' ').replace(r'\n', ' ').replace(r'&amp;', '&')
                    bio = bio_text[:1000]
                    break

        # Fallback to meta description
        if not bio:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc:
                bio = meta_desc.get("content", "")[:500]

        # Website and LinkedIn - look in links
        website = ""
        linkedin = ""

        # Also check schema.org for website
        for script in soup.find_all("script", type="application/ld+json"):
            script_text = script.string
            if script_text and '"sameAs"' in script_text:
                # Extract URLs from sameAs array - look for ["url1", "url2"]
                sameas_match = re.search(r'"sameAs"\s*:\s*\[(.*?)\]', script_text)
                if sameas_match:
                    sameas_urls = re.findall(r'"(https?://[^"]+)"', sameas_match.group(1))
                    for match in sameas_urls:
                        match_lower = match.lower()
                        if "linkedin.com/in/" in match_lower and not linkedin:
                            linkedin = match
                        elif (
                            "noomii.com" not in match_lower
                            and "facebook.com" not in match_lower
                            and "twitter.com" not in match_lower
                            and "instagram.com" not in match_lower
                            and "youtube.com" not in match_lower
                            and "schema.org" not in match_lower
                            and not website
                        ):
                            website = match

        # Also scan regular anchor tags
        for a in soup.find_all("a", href=True):
            href = a["href"]
            href_lower = href.lower()
            if "linkedin.com/in/" in href_lower and not linkedin:
                linkedin = href
            elif (
                href.startswith("http")
                and "noomii.com" not in href_lower
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

        # Phone - look in schema.org or scan for tel: links
        phone = ""
        for script in soup.find_all("script", type="application/ld+json"):
            script_text = script.string
            if script_text and '"telephone"' in script_text:
                match = re.search(r'"telephone"\s*:\s*"([^"]+)"', script_text)
                if match:
                    phone = match.group(1)
                    break

        # Company name (some coaches list their company)
        company = ""
        company_el = soup.find(class_=re.compile(r"company|business|firm", re.I))
        if company_el:
            company = company_el.get_text(strip=True)[:100]

        return ScrapedContact(
            name=name,
            email=email,
            company=company,
            website=website,
            linkedin=linkedin,
            phone=phone,
            bio=bio,
            source_url=url,
            source_category="coaching",
        )
