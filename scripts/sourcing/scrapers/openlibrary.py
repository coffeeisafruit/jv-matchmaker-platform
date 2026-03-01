"""
Open Library API scraper for business/self-help authors.

Open Library (openlibrary.org) has a free API with no key required.
Search API returns author names and book info.

This complements google_books by targeting a different index.

Estimated yield: 2,000-4,000 unique authors
"""

from __future__ import annotations

import json
import re
from typing import Iterator
from urllib.parse import urlencode, quote_plus

from scripts.sourcing.base import BaseScraper, ScrapedContact


SEARCH_QUERIES = [
    # Business / entrepreneurship
    "business coaching", "entrepreneurship", "small business",
    "marketing strategy", "digital marketing", "sales",
    "leadership", "management", "consulting",
    "startup", "venture capital", "innovation",
    "branding", "public relations", "advertising",
    "negotiation", "networking", "business growth",
    # Personal development
    "self help", "personal development", "personal growth",
    "motivation", "success", "productivity",
    "habit", "mindset", "goal setting",
    "emotional intelligence", "communication skills",
    "positive thinking", "confidence",
    # Coaching
    "life coaching", "executive coaching", "career coaching",
    "health coaching", "wellness", "fitness coaching",
    "relationship advice", "dating advice",
    "financial advice", "money management",
    # Spiritual / wellness
    "meditation", "yoga", "mindfulness",
    "spiritual growth", "holistic health",
    "alternative medicine", "nutrition",
    # Investing / finance
    "real estate investing", "stock market",
    "personal finance", "wealth building",
    "financial freedom", "passive income",
    # Skills
    "copywriting", "public speaking",
    "social media", "content creation",
    "online courses", "freelancing",
]

MAX_PAGES_PER_QUERY = 5  # 100 results per page × 5 = 500 per query


class Scraper(BaseScraper):
    SOURCE_NAME = "openlibrary"
    BASE_URL = "https://openlibrary.org"
    REQUESTS_PER_MINUTE = 20  # Open Library asks for politeness

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
                    "sort": "rating",
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

                # Skip generic
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

                # Bio
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

                # Website: link to their Open Library author page
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
