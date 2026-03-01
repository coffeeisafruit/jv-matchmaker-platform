"""
Google Books API scraper for business/self-help authors.

The Google Books API is free (no key required for basic search)
and returns author names, book titles, and publisher info.

Strategy: Search for books in JV-relevant categories, extract
unique author names with their book info as bio context.

Estimated yield: 3,000-5,000 unique authors
"""

from __future__ import annotations

import json
import re
from typing import Iterator
from urllib.parse import urlencode, quote_plus

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Google Books search queries - each returns up to 40 results
# with startIndex pagination up to ~1000 results per query
SEARCH_QUERIES = [
    # Business / entrepreneurship
    "business coaching",
    "entrepreneurship",
    "startup business",
    "small business marketing",
    "digital marketing",
    "affiliate marketing",
    "online business",
    "ecommerce",
    "sales strategy",
    "leadership development",
    "executive coaching",
    "management consulting",
    "business strategy",
    "innovation management",
    "brand building",
    "content marketing",
    "social media marketing",
    "email marketing",
    "copywriting",
    "public speaking",
    "negotiation skills",
    # Personal development
    "self help",
    "personal development",
    "mindset",
    "motivation",
    "productivity",
    "time management",
    "habit building",
    "goal setting",
    "success principles",
    "positive psychology",
    "emotional intelligence",
    "confidence building",
    "communication skills",
    # Coaching / consulting
    "life coaching",
    "career coaching",
    "health coaching",
    "wellness coaching",
    "relationship coaching",
    "financial coaching",
    "mindfulness coaching",
    "performance coaching",
    "transformation coaching",
    # Finance / investing
    "personal finance",
    "real estate investing",
    "stock market investing",
    "wealth building",
    "financial freedom",
    "passive income",
    "money mindset",
    # Health / wellness
    "holistic health",
    "nutrition",
    "fitness business",
    "yoga",
    "meditation",
    "mental health",
    "weight loss",
    "functional medicine",
    # Spiritual
    "spiritual growth",
    "spiritual business",
    "manifestation",
    "law of attraction",
    # Niche expertise
    "course creation",
    "online courses",
    "membership sites",
    "webinars",
    "podcasting",
    "blogging",
    "freelancing",
    "consulting business",
    "speaking business",
    "author platform",
]

MAX_RESULTS_PER_QUERY = 40  # API max per request
MAX_PAGES_PER_QUERY = 10  # 10 pages × 40 = 400 per query


class Scraper(BaseScraper):
    SOURCE_NAME = "google_books"
    BASE_URL = "https://www.googleapis.com/books/v1"
    REQUESTS_PER_MINUTE = 30  # Google Books API is generous

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_authors: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield Google Books API search URLs with pagination."""
        for query in SEARCH_QUERIES:
            for page in range(MAX_PAGES_PER_QUERY):
                start_index = page * MAX_RESULTS_PER_QUERY
                params = urlencode({
                    "q": query,
                    "maxResults": MAX_RESULTS_PER_QUERY,
                    "startIndex": start_index,
                    "printType": "books",
                    "langRestrict": "en",
                })
                yield f"{self.BASE_URL}/volumes?{params}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Google Books API JSON response."""
        try:
            data = json.loads(html)
        except (json.JSONDecodeError, TypeError):
            return []

        contacts = []
        for item in data.get("items", []):
            volume_info = item.get("volumeInfo", {})

            authors = volume_info.get("authors", [])
            if not authors:
                continue

            title = volume_info.get("title", "")
            publisher = volume_info.get("publisher", "")
            categories = volume_info.get("categories", [])
            description = volume_info.get("description", "")
            preview_link = volume_info.get("previewLink", "")
            info_link = volume_info.get("infoLink", "")

            for author in authors:
                author = author.strip()
                if not author or len(author) < 3 or len(author) > 100:
                    continue

                # Skip generic/corporate authors
                if any(skip in author.lower() for skip in [
                    "editor", "various", "anonymous", "unknown",
                    "staff", "team", "group", "institute",
                    "university", "press", "publishing",
                    "association", "foundation", "society",
                ]):
                    continue

                author_key = author.lower().strip()
                if author_key in self._seen_authors:
                    continue
                self._seen_authors.add(author_key)

                # Build bio
                bio_parts = []
                if title:
                    bio_parts.append(f"Author: \"{title}\"")
                if publisher:
                    bio_parts.append(f"Publisher: {publisher}")
                if categories:
                    bio_parts.append(f"Category: {', '.join(categories[:3])}")
                if description:
                    # First 200 chars of description
                    bio_parts.append(description[:200].rsplit(" ", 1)[0])
                bio = " | ".join(bio_parts) if bio_parts else "Published author"

                # Use info_link or preview_link as website
                website = info_link or preview_link or ""

                contacts.append(ScrapedContact(
                    name=author,
                    company=publisher,
                    website=website,
                    bio=bio,
                    source_category="authors",
                ))

        return contacts
