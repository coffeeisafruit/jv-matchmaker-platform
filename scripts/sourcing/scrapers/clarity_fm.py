"""
Clarity.fm expert consultant directory scraper.

Clarity.fm is an on-demand business advice platform where experts
offer paid phone consultations. The platform has a public JSON API
for browsing and searching expert profiles.

API endpoint: GET https://clarity.fm/search?query={term}&page={N}
  - Returns JSON with members array and pagination metadata
  - Requires X-Requested-With: XMLHttpRequest header
  - Each response contains up to 25 members
  - Maximum 150 results per search query (max field in response)

Member data includes: name, bio, location, hourly_rate, screen_name,
topic_names, clarity_url, short_bio, call_count, review_count,
average_rating, and image_url.

Profile URLs: https://clarity.fm/{screen_name}

Categories (from bootstrap.js):
  - Business (id=141): Career, Branding, Financial, Strategy, etc.
  - Funding (id=80): Crowdfunding, VC, Finance, Bootstrapping
  - Sales & Marketing (id=84): Social Media, SEO, PR, Growth, etc.
  - Product & Design (id=87): UX, Lean Startup, Product Mgmt
  - Technology (id=94): WordPress, Mobile, Software Dev
  - Skills & Management (id=93): Productivity, Entrepreneurship
  - Industries (id=132): SaaS, Real Estate, E-commerce
  - Other (id=150)

Estimated yield: 3,000-8,000 unique experts
"""

from __future__ import annotations

import json
import re
from typing import Iterator, Optional
from urllib.parse import quote

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Search queries to maximize coverage (each limited to 150 results)
# We use a comprehensive list of topic keywords to get different slices
SEARCH_QUERIES = [
    # Broad business topics
    "business",
    "startup",
    "entrepreneur",
    "consulting",
    "coaching",
    "strategy",
    "leadership",
    "management",

    # Sales & Marketing
    "marketing",
    "sales",
    "SEO",
    "social media",
    "content marketing",
    "email marketing",
    "growth hacking",
    "branding",
    "public relations",
    "advertising",
    "copywriting",
    "inbound marketing",
    "lead generation",

    # Funding & Finance
    "fundraising",
    "venture capital",
    "crowdfunding",
    "angel investing",
    "finance",
    "bootstrapping",
    "pricing",

    # Product & Technology
    "product management",
    "product design",
    "user experience",
    "lean startup",
    "software development",
    "mobile",
    "SaaS",
    "WordPress",
    "Ruby on Rails",
    "JavaScript",
    "cloud",
    "CRM",

    # Industry-specific
    "real estate",
    "e-commerce",
    "education",
    "nonprofit",
    "marketplace",
    "restaurant",
    "retail",

    # Skills & Career
    "productivity",
    "public speaking",
    "career advice",
    "human resources",
    "hiring",
    "team building",
    "negotiation",
    "innovation",

    # Specialized topics
    "analytics",
    "customer acquisition",
    "customer service",
    "operations",
    "supply chain",
    "legal",
    "intellectual property",
    "patents",
    "licensing",
    "international business",
    "healthcare",
    "wellness",
    "personal development",
    "book publishing",
    "writing",
    "video production",
    "podcast",
    "bitcoin",
    "blockchain",
    "artificial intelligence",
    "data science",
    "cybersecurity",
]


class Scraper(BaseScraper):
    SOURCE_NAME = "clarity_fm"
    BASE_URL = "https://clarity.fm"
    REQUESTS_PER_MINUTE = 10  # JSON API, relatively lightweight

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_ids: set[str] = set()
        self._seen_screen_names: set[str] = set()
        # Configure headers for AJAX requests
        self.session.headers.update({
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest",
        })

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield search API URLs for each query and page combination.

        Each search query returns max 150 results, paginated at 25/page.
        With ~80 different search terms, we can potentially reach
        ~12,000 results (heavily deduplicated to ~3,000-8,000 unique).
        """
        for query in SEARCH_QUERIES:
            # Each query returns max 150 results = 6 pages of 25
            for page in range(1, 7):
                encoded_query = quote(query)
                yield f"{self.BASE_URL}/search?query={encoded_query}&page={page}"

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Override run() to handle JSON API responses.

        The base class run() uses fetch_page() + scrape_page() which
        expect HTML. We need to fetch JSON and parse it directly.
        """
        from datetime import datetime

        pages_done = 0
        contacts_yielded = 0
        start_from = (checkpoint or {}).get("last_url")
        past_checkpoint = start_from is None

        self.logger.info(
            "Starting %s scraper (max_pages=%s, max_contacts=%s)",
            self.SOURCE_NAME,
            max_pages or "unlimited",
            max_contacts or "unlimited",
        )

        current_query = ""
        query_exhausted = False

        for url in self.generate_urls():
            if not past_checkpoint:
                if url == start_from:
                    past_checkpoint = True
                continue

            # Extract query from URL to track exhaustion
            query_match = re.search(r"query=([^&]+)", url)
            this_query = query_match.group(1) if query_match else ""
            if this_query != current_query:
                current_query = this_query
                query_exhausted = False

            # Skip remaining pages if this query is exhausted
            if query_exhausted:
                continue

            # Fetch JSON data
            data = self._fetch_search(url)
            if data is None:
                continue

            members = data.get("members", [])
            total_matches = data.get("matches", 0)
            max_results = data.get("max", 150)

            if not members:
                query_exhausted = True
                continue

            for member_data in members:
                contact = self._parse_member(member_data)
                if not contact:
                    continue

                contact.source_platform = self.SOURCE_NAME
                contact.source_url = url
                contact.scraped_at = datetime.now().isoformat()
                contact.email = contact.clean_email()

                if contact.is_valid():
                    self.stats["contacts_valid"] += 1
                    contacts_yielded += 1
                    yield contact

                    if max_contacts and contacts_yielded >= max_contacts:
                        self.logger.info("Reached max_contacts=%d", max_contacts)
                        return

                self.stats["contacts_found"] += 1

            pages_done += 1
            if pages_done % 10 == 0:
                self.logger.info(
                    "Progress: %d pages, %d valid contacts, %d total seen",
                    pages_done, self.stats["contacts_valid"], len(self._seen_ids),
                )

            if max_pages and pages_done >= max_pages:
                self.logger.info("Reached max_pages=%d", max_pages)
                break

            # Check if we've gotten all results for this query
            page_match = re.search(r"page=(\d+)", url)
            current_page = int(page_match.group(1)) if page_match else 1
            if current_page * 25 >= min(total_matches, max_results):
                query_exhausted = True

        self.logger.info("Scraper complete: %s", self.stats)

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used - this scraper overrides run() for JSON API access."""
        return []

    def _fetch_search(self, url: str) -> Optional[dict]:
        """Fetch search results from Clarity.fm JSON API."""
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1
            data = resp.json()

            # Validate response structure
            if "members" not in data:
                self.logger.warning("Unexpected response format from %s", url)
                return None

            return data
        except Exception as exc:
            self.logger.warning("Fetch failed for %s: %s", url, exc)
            self.stats["errors"] += 1
            return None

    def _parse_member(self, data: dict) -> ScrapedContact | None:
        """Parse a Clarity.fm member record into a ScrapedContact."""
        member_id = str(data.get("id", "")).strip()
        name = (data.get("name") or "").strip()
        screen_name = (data.get("screen_name") or "").strip()

        if not name or len(name) < 2:
            return None

        # Deduplicate by ID
        if member_id and member_id in self._seen_ids:
            return None
        if member_id:
            self._seen_ids.add(member_id)

        # Also deduplicate by screen_name
        if screen_name and screen_name in self._seen_screen_names:
            return None
        if screen_name:
            self._seen_screen_names.add(screen_name)

        # Extract fields
        bio_text = (data.get("bio") or "").strip()
        short_bio = (data.get("short_bio") or "").strip()
        location = (data.get("location") or "").strip()
        hourly_rate = data.get("hourly_rate", 0)
        call_count = data.get("call_count", 0) or data.get("total_calls", 0)
        avg_rating = data.get("average_rating", 0)
        review_count = data.get("review_count", 0)
        topic_names = (data.get("topic_names") or "").strip()
        clarity_url = (data.get("clarity_url") or "").strip()

        if not clarity_url and screen_name:
            clarity_url = f"https://clarity.fm/{screen_name}"

        # Build comprehensive bio
        bio_parts = ["Clarity.fm Expert"]
        if short_bio:
            bio_parts.append(short_bio)
        if location:
            bio_parts.append(location)
        if hourly_rate:
            bio_parts.append(f"${hourly_rate}/hr")
        if avg_rating and review_count:
            bio_parts.append(f"{avg_rating:.1f}/5 ({review_count} reviews)")
        if call_count:
            bio_parts.append(f"{call_count} calls completed")
        if topic_names:
            # Limit to first 5 topics
            topics = [t.strip() for t in topic_names.split(",")][:5]
            bio_parts.append(f"Topics: {', '.join(topics)}")
        if bio_text and not short_bio:
            # Use first 300 chars of full bio if no short bio
            clean_bio = bio_text.replace("\r\n", " ").replace("\n", " ")
            bio_parts.append(clean_bio[:300])

        bio = " | ".join(bio_parts)

        # Try to extract email from bio text (some experts include it)
        email = ""
        if bio_text:
            emails = self.extract_emails(bio_text)
            email = emails[0] if emails else ""

        # Try to extract LinkedIn from bio text
        linkedin = ""
        if bio_text:
            linkedin = self.extract_linkedin(bio_text)

        # Company name from short_bio (often formatted as "Role at Company")
        company = ""
        if short_bio and " at " in short_bio:
            company = short_bio.split(" at ", 1)[1].strip()
        elif short_bio and " @ " in short_bio:
            company = short_bio.split(" @ ", 1)[1].strip()

        return ScrapedContact(
            name=name,
            email=email,
            company=company,
            website=clarity_url,
            linkedin=linkedin,
            phone="",
            bio=bio,
            source_category="consulting",
            raw_data={
                "clarity_id": member_id,
                "screen_name": screen_name,
                "hourly_rate": str(hourly_rate) if hourly_rate else "",
                "call_count": str(call_count) if call_count else "",
                "avg_rating": str(avg_rating) if avg_rating else "",
                "review_count": str(review_count) if review_count else "",
                "topic_names": topic_names,
                "location": location,
                "is_expert": str(data.get("is_expert", False)),
            },
        )
