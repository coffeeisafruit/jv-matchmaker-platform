"""
PodcastIndex.org API scraper — open podcast directory.

PodcastIndex is the largest open podcast database with 4.4M+ podcasts.
Uses free API keys with HMAC-SHA1 authentication.

Search by term across Business, Self-Improvement, Health & Fitness,
Education categories. Each result includes feed URL, host name, and
description.

Register for free API keys at: https://api.podcastindex.org/

Estimated yield: 20,000-50,000+ unique podcast hosts
"""

from __future__ import annotations

import hashlib
import json
import time
from typing import Iterator
from urllib.parse import urlencode

from scripts.sourcing.base import BaseScraper, ScrapedContact


# JV-relevant search queries
SEARCH_QUERIES = [
    # Coaching
    "business coaching", "life coaching", "executive coaching",
    "health coaching", "wellness coaching", "mindset coaching",
    "leadership coaching", "career coaching", "relationship coaching",
    "fitness coaching", "nutrition coaching", "confidence coaching",
    "ADHD coaching", "dating coaching", "parenting coaching",
    "transformation coaching", "high performance coaching",
    # Personal development
    "personal development", "self improvement", "self help",
    "motivation", "mindset", "habits", "productivity",
    "goal setting", "success mindset", "growth mindset",
    "emotional intelligence", "resilience", "confidence",
    "mental health", "anxiety", "depression recovery",
    # Business / marketing
    "entrepreneurship", "startup", "small business",
    "marketing strategy", "digital marketing", "social media marketing",
    "content marketing", "email marketing", "copywriting",
    "sales strategy", "sales funnel", "affiliate marketing",
    "ecommerce", "Amazon FBA", "dropshipping",
    "real estate investing", "passive income",
    "online business", "side hustle", "solopreneur",
    # Course creators / education
    "online course", "course creator", "online education",
    "teaching online", "digital course", "knowledge business",
    # Speakers / authors
    "motivational speaker", "keynote speaker", "public speaking",
    "TEDx", "author interview", "book launch",
    "self publishing", "writing", "storytelling",
    # Consultants
    "business consultant", "marketing consultant",
    "management consultant", "brand strategy",
    "HR consultant", "financial advisor",
    # Wellness / spiritual
    "meditation", "yoga", "holistic health",
    "functional medicine", "integrative health",
    "energy healing", "spiritual growth",
    "plant based", "biohacking", "longevity",
    "breathwork", "mindfulness",
    # Community / events
    "mastermind", "retreat", "workshop",
    "community building", "membership",
    "summit", "conference",
    # Finance
    "financial coaching", "money mindset",
    "investing for beginners", "stock market",
    "crypto", "wealth building",
    # Niche
    "therapist business", "dentist marketing",
    "lawyer marketing", "doctor entrepreneur",
    "nurse entrepreneur", "teacher entrepreneur",
    "nonprofit leadership", "church leadership",
    "women entrepreneur", "veteran entrepreneur",
]

MAX_RESULTS = 100  # API max per request


class Scraper(BaseScraper):
    SOURCE_NAME = "podcastindex"
    BASE_URL = "https://api.podcastindex.org/api/1.0"
    REQUESTS_PER_MINUTE = 15
    TYPICAL_ROLES = ["Media/Publisher", "Thought Leader"]
    TYPICAL_NICHES = ["podcasting", "content_marketing", "speaking"]
    TYPICAL_OFFERINGS = ["podcast", "interviews", "audience", "media"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_feed_ids: set[str] = set()
        self._api_key = None
        self._api_secret = None

    def _get_auth_headers(self) -> dict:
        """Build PodcastIndex HMAC-SHA1 auth headers."""
        import os
        if not self._api_key:
            self._api_key = os.environ.get("PODCASTINDEX_API_KEY", "")
            self._api_secret = os.environ.get("PODCASTINDEX_API_SECRET", "")

        if not self._api_key or not self._api_secret:
            self.logger.warning("PODCASTINDEX_API_KEY/SECRET not set")
            return {}

        epoch_time = int(time.time())
        data_to_hash = self._api_key + self._api_secret + str(epoch_time)
        sha1_hash = hashlib.sha1(data_to_hash.encode("utf-8")).hexdigest()

        return {
            "X-Auth-Date": str(epoch_time),
            "X-Auth-Key": self._api_key,
            "Authorization": sha1_hash,
            "User-Agent": "JVMatchmaker/1.0",
        }

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield PodcastIndex search API URLs."""
        for query in SEARCH_QUERIES:
            params = urlencode({
                "q": query,
                "max": MAX_RESULTS,
                "clean": 1,
            })
            yield f"{self.BASE_URL}/search/byterm?{params}"

    def fetch_page(self, url: str, timeout: int = 30):
        """Override to add auth headers."""
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)
        try:
            headers = self._get_auth_headers()
            if not headers:
                return None
            resp = self.session.get(url, timeout=timeout, headers=headers)
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1
            return resp.text
        except Exception as exc:
            self.logger.warning("Fetch failed: %s", exc)
            self.stats["errors"] += 1
            return None

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse PodcastIndex search API JSON response."""
        try:
            data = json.loads(html)
        except (json.JSONDecodeError, TypeError):
            return []

        contacts = []
        feeds = data.get("feeds", [])

        for feed in feeds:
            feed_id = str(feed.get("id", ""))
            if not feed_id or feed_id in self._seen_feed_ids:
                continue
            self._seen_feed_ids.add(feed_id)

            # Author/owner is the host
            author = (feed.get("author") or "").strip()
            owner_name = (feed.get("ownerName") or "").strip()
            name = author or owner_name

            if not name or len(name) < 2 or len(name) > 150:
                continue

            # Skip generic/corporate names
            name_lower = name.lower()
            if any(skip in name_lower for skip in [
                "podcast", "network", "media", "radio", "studio",
                "production", "publishing", "llc", "inc.", "ltd",
                "group", "team", "staff", "editor", "various",
                "unknown", "anonymous",
            ]):
                # Only skip if name IS the generic term (not contains)
                if len(name.split()) <= 2:
                    continue

            title = (feed.get("title") or "").strip()
            description = (feed.get("description") or "").strip()
            feed_url = (feed.get("url") or "").strip()
            link = (feed.get("link") or "").strip()
            owner_url = link if link else ""

            # Build bio
            bio_parts = []
            if title:
                bio_parts.append(f"Podcast: \"{title}\"")
            if description:
                # Truncate description
                desc_clean = description[:300].strip()
                bio_parts.append(desc_clean)
            categories = feed.get("categories", {})
            if categories and isinstance(categories, dict):
                cat_names = list(categories.values())[:3]
                if cat_names:
                    bio_parts.append(f"Categories: {', '.join(cat_names)}")
            bio = " | ".join(bio_parts) if bio_parts else "Podcast host"

            # Website
            website = owner_url or ""

            contacts.append(ScrapedContact(
                name=name,
                website=website,
                bio=bio,
                source_category="podcaster",
                raw_data={
                    "feed_id": feed_id,
                    "feed_url": feed_url,
                    "title": title,
                },
            ))

        return contacts
