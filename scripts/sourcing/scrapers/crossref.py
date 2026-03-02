"""
Crossref API scraper — nonfiction book authors.

Crossref has metadata for 150M+ scholarly works. We search for
books by subject keyword to extract unique nonfiction authors in
JV-relevant niches (self-help, coaching, business, wellness, etc.).

No auth required — just include mailto: parameter for polite pool
(faster rate limits).

Estimated yield: 20,000-50,000+ unique authors
"""

from __future__ import annotations

import json
from typing import Iterator
from urllib.parse import urlencode, quote

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Subject queries for JV-relevant nonfiction
SEARCH_QUERIES = [
    # Self-help / personal development
    "self-help", "personal development", "self improvement",
    "motivation", "success habits", "goal setting",
    "positive psychology", "happiness", "resilience",
    "emotional intelligence", "mindfulness meditation",
    "stress management", "time management", "productivity",
    "confidence building", "assertiveness",
    # Coaching
    "life coaching", "business coaching", "executive coaching",
    "career coaching", "health coaching", "wellness coaching",
    "leadership coaching", "performance coaching",
    "coaching techniques", "coaching psychology",
    # Business / entrepreneurship
    "entrepreneurship", "small business management",
    "startup", "business strategy", "marketing strategy",
    "digital marketing", "social media marketing",
    "content marketing", "copywriting", "sales strategy",
    "negotiation", "influence persuasion",
    "leadership", "management", "team building",
    "organizational development", "change management",
    "innovation", "business growth",
    # Finance / investing
    "personal finance", "financial planning",
    "investing", "wealth building", "real estate investing",
    "stock market", "passive income",
    # Health / wellness
    "nutrition", "fitness", "weight loss",
    "holistic health", "alternative medicine",
    "functional medicine", "integrative health",
    "mental health", "cognitive behavioral therapy",
    "yoga", "meditation", "breathwork",
    "plant based diet", "longevity", "biohacking",
    # Relationships
    "relationship advice", "marriage counseling",
    "dating advice", "communication skills",
    "conflict resolution", "parenting",
    # Spirituality
    "spirituality", "spiritual growth",
    "mindfulness", "consciousness",
    # Speaking / writing
    "public speaking", "presentation skills",
    "storytelling", "creative writing",
    "memoir writing", "nonfiction writing",
    # Education
    "adult education", "online learning",
    "professional development", "corporate training",
]

ROWS_PER_REQUEST = 1000  # Crossref max
MAX_PAGES_PER_QUERY = 5  # 5 pages × 1000 = 5000 results per query


class Scraper(BaseScraper):
    SOURCE_NAME = "crossref"
    BASE_URL = "https://api.crossref.org"
    REQUESTS_PER_MINUTE = 30  # Polite pool allows ~50/sec

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_authors: set[str] = set()
        # Set polite pool User-Agent
        self.session.headers.update({
            "User-Agent": "JVMatchmaker/1.0 (mailto:joe@jvmatches.com)",
        })

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield Crossref API search URLs with cursor-based pagination."""
        for query in SEARCH_QUERIES:
            for page in range(MAX_PAGES_PER_QUERY):
                offset = page * ROWS_PER_REQUEST
                params = urlencode({
                    "query": query,
                    "filter": "type:book,type:book-chapter,type:monograph",
                    "rows": ROWS_PER_REQUEST,
                    "offset": offset,
                    "select": "author,title,publisher,subject,published-print,DOI",
                    "mailto": "joe@jvmatches.com",
                })
                yield f"{self.BASE_URL}/works?{params}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Crossref API JSON response."""
        try:
            data = json.loads(html)
        except (json.JSONDecodeError, TypeError):
            return []

        contacts = []
        message = data.get("message", {})
        items = message.get("items", [])

        for item in items:
            authors = item.get("author", [])
            if not authors:
                continue

            title_list = item.get("title", [])
            title = title_list[0] if title_list else ""
            publisher = item.get("publisher", "")
            subjects = item.get("subject", [])[:3]
            doi = item.get("DOI", "")

            # Published date
            pub_date = ""
            date_parts = item.get("published-print", {}).get("date-parts", [[]])
            if date_parts and date_parts[0]:
                pub_date = str(date_parts[0][0])  # Year

            for author in authors:
                given = (author.get("given") or "").strip()
                family = (author.get("family") or "").strip()

                if not family:
                    continue

                name = f"{given} {family}".strip() if given else family
                if len(name) < 3 or len(name) > 100:
                    continue

                # Skip corporate/institutional names
                if any(skip in name.lower() for skip in [
                    "editor", "various", "anonymous", "unknown",
                    "university", "press", "publishing", "institute",
                    "association", "foundation", "society",
                    "corporation", "company", "inc.",
                    "department", "committee", "council",
                    "ministry", "bureau", "office",
                ]):
                    continue

                author_key = name.lower().strip()
                if author_key in self._seen_authors:
                    continue
                self._seen_authors.add(author_key)

                # Build bio
                bio_parts = []
                if title:
                    bio_parts.append(f'Author: "{title}"')
                if pub_date:
                    bio_parts.append(f"Published: {pub_date}")
                if publisher:
                    bio_parts.append(f"Publisher: {publisher}")
                if subjects:
                    bio_parts.append(f"Subjects: {', '.join(subjects[:3])}")
                bio = " | ".join(bio_parts) if bio_parts else "Published author"

                # Website from DOI
                website = f"https://doi.org/{doi}" if doi else ""

                # ORCID if available
                orcid = author.get("ORCID", "")

                contacts.append(ScrapedContact(
                    name=name,
                    company=publisher,
                    website=website,
                    bio=bio,
                    source_category="author",
                    raw_data={
                        "doi": doi,
                        "orcid": orcid,
                    },
                ))

        return contacts
