"""
SPEAKING.com speaker directory scraper.

SPEAKING.com is a speaker bureau with a large directory organized by
topic categories. The site is a WordPress-based platform with standard
HTML pages and WP-PageNavi pagination.

Strategy:
  1. Start from the main speakers page to collect all category URLs
  2. For each category, paginate through listing pages (/page/N/)
  3. On each listing page, find speaker profile links
  4. Fetch individual speaker profile pages for detailed data
  5. Extract name, bio, topics, location from profile pages

The site uses ModSecurity -- requests without a proper User-Agent
will be blocked with a 406 error. The base session headers from
BaseScraper handle this correctly.

URL patterns:
  - Categories: https://speaking.com/category/{topic-slug}/
  - Category pages: https://speaking.com/category/{topic-slug}/page/{n}/
  - Speaker profiles: https://speaking.com/speakers/{speaker-slug}/

Estimated yield: 3,000-5,000 speakers
"""

from __future__ import annotations

import re
from typing import Iterator
from urllib.parse import urljoin

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Topic categories to scrape. Extracted from speaking.com/speakers/ page.
# We use a curated subset of high-value business categories to avoid
# scraping celebrity/entertainment categories with less JV potential.
CATEGORIES = [
    "achievement-peak-performance",
    "advertising-public-relations",
    "artificial-intelligence",
    "branding",
    "business",
    "business-entrepreneurship",
    "business-etiquette",
    "business-management",
    "career",
    "ceo",
    "change-management",
    "coaching",
    "communication",
    "communication-skills",
    "conflict-management",
    "consumer-trends",
    "corporate-culture",
    "corporate-team-building",
    "creativity",
    "cross-cultural-issues",
    "customer-service-speakers",
    "digital-marketing",
    "diversity",
    "e-commerce",
    "economic-outlook",
    "economy-economics",
    "economy-finance",
    "education",
    "emotional-intelligence",
    "employee-engagement",
    "empowerment",
    "energy",
    "entrepreneurship",
    "environment",
    "ethics",
    "finance",
    "futurists",
    "global-affairs",
    "globalization",
    "globalization-international-business",
    "health-healthcare",
    "healthcare",
    "healthcare-management",
    "healthcare-technology",
    "human-resources",
    "innovation",
    "inspirational",
    "international-business",
    "investing",
    "it-security",
    "keynote-business-speaker",
    "leadership-development",
    "leadership-speakers",
    "leadership-training",
    "life-balance",
    "management",
    "management-employees",
    "marketing",
    "media-broadcast-print",
    "motivation-inspiration",
    "motivational-inspirational",
    "motivational-sales",
    "motivational-speakers",
    "negotiation",
    "networking",
    "overcoming-adversity",
    "performance-improvement",
    "personal-development",
    "personal-growth",
    "presentation-skills",
    "productivity",
    "psychology",
    "real-estate",
    "relationships",
    "resilience",
    "retail",
    "sales",
    "social-media",
    "social-media-internet",
    "startup",
    "startup-marketing",
    "strategic-planning",
    "stress-management-speakers",
    "success",
    "sustainability",
    "team-building",
    "technology-speakers",
    "ted-talk-speaker",
    "time-management",
    "trends",
    "wellness",
    "women-speakers",
]

MAX_PAGES_PER_CATEGORY = 20  # Safety limit (most have 2-6 pages)


class Scraper(BaseScraper):
    SOURCE_NAME = "speaking_com"
    BASE_URL = "https://speaking.com"
    REQUESTS_PER_MINUTE = 6  # Conservative -- ModSecurity is active

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_slugs: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield paginated category listing URLs.

        For each category, yields page 1 through MAX_PAGES_PER_CATEGORY.
        Pages beyond the last valid page will return 404, which the base
        class handles gracefully (fetch_page returns None).
        """
        for category in CATEGORIES:
            # Page 1 (no /page/N/ suffix)
            yield f"{self.BASE_URL}/category/{category}/"
            # Pages 2+
            for page in range(2, MAX_PAGES_PER_CATEGORY + 1):
                yield f"{self.BASE_URL}/category/{category}/page/{page}/"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a category listing page and fetch individual speaker profiles.

        Listing pages contain speaker cards with links to full profile
        pages at /speakers/{slug}/. We collect all unique profile URLs,
        then fetch and parse each one.
        """
        soup = self.parse_html(html)
        contacts = []

        # Find speaker profile links on the listing page
        profile_links = set()
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            # Match /speakers/{slug}/ pattern
            match = re.search(r"/speakers/([a-z0-9\-]+)/?$", href, re.I)
            if match:
                slug = match.group(1).lower()
                if slug not in self._seen_slugs:
                    self._seen_slugs.add(slug)
                    full_url = urljoin(self.BASE_URL, href)
                    profile_links.add(full_url)

        if not profile_links:
            # No speakers found - likely past last page
            return []

        # Fetch and parse each speaker profile page
        for profile_url in profile_links:
            profile_html = self.fetch_page(profile_url)
            if not profile_html:
                continue
            contact = self._parse_profile(profile_url, profile_html)
            if contact:
                contacts.append(contact)

        return contacts

    def _parse_profile(self, url: str, html: str) -> ScrapedContact | None:
        """Extract speaker data from an individual profile page.

        Profile pages contain:
          - Speaker name in <h1>
          - Bio in long <p> paragraphs
          - Topic categories as links to /category/{topic}/
          - Location as "Travels from City, State, Country"
          - Fee range text
          - Social sharing links (not personal LinkedIn)
          - Contact through the bureau: speakers@speaking.com
        """
        soup = self.parse_html(html)

        # Name
        name = ""
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        if not name:
            og_title = soup.find("meta", property="og:title")
            if og_title:
                name = (og_title.get("content") or "").split("|")[0].strip()

        if not name or len(name) < 2:
            return None

        # Clean name -- remove trailing descriptors
        name = re.sub(
            r"\s*[-|]\s*(Speaker|Keynote|Author|SPEAKING\.com).*$",
            "",
            name,
            flags=re.I,
        )
        name = name.strip()

        # Bio -- collect all substantial paragraphs
        bio = ""
        paragraphs = soup.find_all("p")
        bio_texts = []
        for p in paragraphs:
            text = p.get_text(strip=True)
            if len(text) > 80 and name.split()[0] in text:
                # Remove common prefix like "Speaker Name Profile"
                text = re.sub(
                    r"^" + re.escape(name) + r"\s*Profile\s*",
                    "",
                    text,
                )
                bio_texts.append(text)
        if bio_texts:
            bio = " ".join(bio_texts[:3])[:1000]

        # Fallback to meta description
        if not bio:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc:
                bio = (meta_desc.get("content") or "")[:500]

        # Fallback to og:description
        if not bio:
            og_desc = soup.find("meta", property="og:description")
            if og_desc:
                bio = (og_desc.get("content") or "")[:500]

        # Topics -- extract from category links
        topics = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            if "/category/" in href:
                topic_text = a_tag.get_text(strip=True)
                if topic_text and len(topic_text) < 60 and topic_text not in topics:
                    topics.append(topic_text)

        # Location -- "Travels from City, State, Country"
        location = ""
        location_match = re.search(
            r"Travels\s+from\s+([^<]+)",
            html,
            re.I,
        )
        if location_match:
            location = re.sub(r"\s+", " ", location_match.group(1)).strip()
            # Remove trailing HTML artifacts
            location = re.sub(r"<.*$", "", location).strip()

        # Fee range
        fee = ""
        fee_match = re.search(
            r"(?:speaking\s+)?fee[^<]*?(?:falls\s+within\s+range:\s*)?(\$[\d,]+(?:\s*(?:to|-)\s*\$[\d,]+)?|(?:Under|Over)\s*\$[\d,]+)",
            html,
            re.I,
        )
        if fee_match:
            fee = fee_match.group(1).strip()

        # Website -- speaking.com is a bureau that routes through its own
        # contact system. External links on profiles are mostly spam/ads,
        # not speaker personal sites. We use the speaker's speaking.com
        # profile URL as the website -- it provides a valid contact path
        # and ensures is_valid() passes for enrichment pipeline pickup.
        website = url

        # LinkedIn -- the share links contain linkedin.com but are sharing
        # links, not personal profiles. Look for actual /in/ profiles.
        linkedin = ""
        linkedin_match = re.search(
            r"https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9_\-]+/?",
            html,
        )
        if linkedin_match:
            linkedin = linkedin_match.group(0)

        # Email -- speaking.com uses a bureau contact, not personal
        email = ""
        emails = self.extract_emails(html)
        for e in emails:
            e_lower = e.lower()
            if (
                "speaking.com" not in e_lower
                and "sentry.io" not in e_lower
                and "wixpress.com" not in e_lower
            ):
                email = e
                break

        # Build enriched bio
        bio_parts = []
        if bio:
            bio_parts.append(bio)
        if location:
            bio_parts.append(f"Location: {location}")
        if topics:
            bio_parts.append(f"Topics: {', '.join(topics[:8])}")
        if fee:
            bio_parts.append(f"Fee: {fee}")

        full_bio = " | ".join(bio_parts) if bio_parts else ""

        return ScrapedContact(
            name=name,
            email=email,
            company="",
            website=website,
            linkedin=linkedin,
            phone="",
            bio=full_bio[:2000],
            source_url=url,
            source_category="speakers",
            raw_data={
                "location": location,
                "topics": topics[:10],
                "fee": fee,
            },
        )
