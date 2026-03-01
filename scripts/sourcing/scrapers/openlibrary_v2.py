"""
Open Library API scraper — second pass with additional queries.

Extends the first openlibrary scraper with more niche-specific queries
to capture additional unique authors not found in the first pass.

Estimated yield: 2,000-5,000 additional unique authors
"""

from __future__ import annotations

import json
import re
from typing import Iterator
from urllib.parse import urlencode

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Additional queries NOT in the first pass
SEARCH_QUERIES = [
    # Industry-specific business
    "dental practice management", "medical practice marketing",
    "law firm marketing", "accounting firm growth",
    "restaurant business", "salon business",
    "gym business", "fitness studio",
    "veterinary practice", "pharmacy business",
    # Tech/digital
    "artificial intelligence business", "machine learning",
    "blockchain business", "web development",
    "app development business", "cybersecurity",
    "cloud computing", "data science",
    # Advanced business topics
    "mergers acquisitions", "venture capital",
    "private equity", "angel investing",
    "franchise business", "licensing business",
    "intellectual property", "patent strategy",
    "supply chain management", "logistics business",
    "import export business", "manufacturing business",
    # Specialized coaching
    "executive presence", "emotional intelligence leadership",
    "conflict resolution", "team building",
    "organizational development", "change management",
    "diversity inclusion leadership", "cross cultural management",
    "remote team management", "hybrid work",
    # Niche personal development
    "stoicism philosophy", "cognitive behavioral therapy",
    "neurolinguistic programming", "hypnotherapy",
    "breathwork", "cold therapy",
    "biohacking", "longevity",
    "intermittent fasting", "plant based nutrition",
    # Writing / publishing
    "memoir writing", "fiction writing",
    "screenwriting", "journalism",
    "technical writing", "grant writing",
    "ghostwriting", "literary agent",
    # Speaking / events
    "event planning", "conference organizing",
    "trade show marketing", "exhibition design",
    "wedding planning", "party planning",
    # Education
    "homeschooling", "tutoring business",
    "education technology", "online learning",
    "corporate training", "professional development",
    "adult education", "continuing education",
    # Creative business
    "photography business", "videography business",
    "graphic design business", "interior design",
    "architecture business", "landscape design",
    "music business", "art business",
    "crafts business", "handmade business",
    # Niche markets
    "pet business", "children business",
    "senior services", "disability services",
    "green business", "sustainability",
    "organic farming", "agriculture technology",
    "cannabis business", "hemp business",
]

MAX_PAGES_PER_QUERY = 3  # Less depth, more breadth


class Scraper(BaseScraper):
    SOURCE_NAME = "openlibrary_v2"
    BASE_URL = "https://openlibrary.org"
    REQUESTS_PER_MINUTE = 20

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_authors: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield Open Library search API URLs."""
        for query in SEARCH_QUERIES:
            for page in range(1, MAX_PAGES_PER_QUERY + 1):
                params = urlencode({
                    "q": query,
                    "page": page,
                    "limit": 100,
                    "language": "eng",
                })
                yield f"{self.BASE_URL}/search.json?{params}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Open Library search API JSON response."""
        try:
            data = json.loads(html)
        except (json.JSONDecodeError, TypeError):
            return []

        contacts = []
        for doc in data.get("docs", []):
            authors = doc.get("author_name", [])
            if not authors:
                continue

            title = doc.get("title", "")
            publisher = doc.get("publisher", [""])[0] if doc.get("publisher") else ""
            subjects = doc.get("subject", [])[:5]
            first_publish_year = doc.get("first_publish_year", "")
            edition_count = doc.get("edition_count", 0)
            author_keys = doc.get("author_key", [])

            for i, author in enumerate(authors):
                author = author.strip()
                if not author or len(author) < 3 or len(author) > 100:
                    continue

                if any(skip in author.lower() for skip in [
                    "editor", "various", "anonymous", "unknown",
                    "staff", "team", "group", "institute",
                    "university", "press", "publishing",
                    "association", "foundation", "society",
                    "corporation", "company", "inc.",
                ]):
                    continue

                author_key = author.lower().strip()
                if author_key in self._seen_authors:
                    continue
                self._seen_authors.add(author_key)

                bio_parts = []
                if title:
                    bio_parts.append(f"Author: \"{title}\"")
                if first_publish_year:
                    bio_parts.append(f"Published: {first_publish_year}")
                if edition_count and edition_count > 1:
                    bio_parts.append(f"{edition_count} editions")
                if subjects:
                    bio_parts.append(f"Subjects: {', '.join(subjects[:3])}")
                bio = " | ".join(bio_parts) if bio_parts else "Published author"

                website = ""
                if author_keys and i < len(author_keys):
                    website = f"https://openlibrary.org/authors/{author_keys[i]}"

                contacts.append(ScrapedContact(
                    name=author,
                    company=publisher,
                    website=website,
                    bio=bio,
                    source_category="authors",
                ))

        return contacts
