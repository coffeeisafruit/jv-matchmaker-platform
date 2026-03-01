"""
Apple Podcasts web directory scraper.

Apple Podcasts has public web pages for each podcast show with
host name, description, website link, and episode list.

Uses the iTunes Search API (free, no key required) for discovery,
then scrapes individual podcast pages.

Estimated yield: 2,000-4,000 podcast hosts
"""

from __future__ import annotations

import re
import json
from typing import Iterator
from urllib.parse import urlencode, quote_plus

from scripts.sourcing.base import BaseScraper, ScrapedContact


# iTunes Search API queries for JV-relevant podcasts
# ~80 queries × 200 results = up to 16,000 results (deduped to ~6-8K unique)
SEARCH_QUERIES = [
    # --- Original 35 queries ---
    "business coaching",
    "life coaching",
    "executive coaching",
    "personal development",
    "entrepreneurship",
    "marketing strategy",
    "digital marketing",
    "affiliate marketing",
    "online business",
    "course creator",
    "leadership development",
    "health coaching",
    "wellness coaching",
    "mindset",
    "self improvement",
    "motivation inspiration",
    "sales training",
    "public speaking",
    "financial coaching",
    "productivity",
    "women in business",
    "startup",
    "consulting",
    "real estate investing",
    "spiritual growth",
    "relationship coaching",
    "career development",
    "success mindset",
    "copywriting",
    "social media marketing",
    "content marketing",
    "brand building",
    "mastermind",
    "expert business",
    "thought leadership",
    # --- Expanded: more niche coaching ---
    "fitness coaching",
    "nutrition coaching",
    "weight loss coaching",
    "ADHD coaching",
    "parenting coaching",
    "divorce coaching",
    "grief coaching",
    "dating coaching",
    "confidence coach",
    "anxiety coaching",
    "trauma healing",
    "addiction recovery coaching",
    "sleep coaching",
    # --- Expanded: business niches ---
    "ecommerce business",
    "Amazon FBA",
    "dropshipping",
    "print on demand business",
    "freelancing business",
    "agency owner",
    "SaaS founder",
    "solopreneur",
    "side hustle",
    "passive income",
    "investing for beginners",
    "stock market investing",
    "crypto investing",
    "real estate flipping",
    # --- Expanded: creators/experts ---
    "book writing author",
    "self publishing",
    "blogging business",
    "YouTube creator tips",
    "TikTok business",
    "Instagram marketing",
    "email marketing expert",
    "funnel hacking",
    "webinar marketing",
    "info product",
    "membership site",
    "online summit",
    "virtual events business",
    # --- Expanded: professional services ---
    "therapy practice",
    "therapist business",
    "dentist marketing",
    "doctor entrepreneur",
    "lawyer marketing",
    "accountant business",
    "financial advisor marketing",
    "insurance agent",
    "chiropractor marketing",
    "veterinarian business",
    # --- Expanded: spiritual/wellness ---
    "meditation teacher",
    "yoga teacher business",
    "reiki healer",
    "energy healing",
    "astrology business",
    "tarot business",
    "holistic health",
    "functional medicine",
    "integrative health",
    # --- Expanded: industry-specific ---
    "nonprofit leadership",
    "church leadership",
    "pastor leadership",
    "teacher entrepreneur",
    "nurse entrepreneur",
    "military veteran entrepreneur",
    "women entrepreneur",
    "black entrepreneur",
    "latino entrepreneur",
    "asian entrepreneur",
]

RESULTS_PER_QUERY = 200  # iTunes API max


class Scraper(BaseScraper):
    SOURCE_NAME = "apple_podcasts"
    BASE_URL = "https://itunes.apple.com"
    REQUESTS_PER_MINUTE = 15  # iTunes API is fairly generous

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_ids: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield iTunes Search API URLs across multiple country stores."""
        # Search US, GB, CA, AU stores for maximum coverage
        for country in ["US", "GB", "CA", "AU"]:
            for query in SEARCH_QUERIES:
                params = urlencode({
                    "term": query,
                    "media": "podcast",
                    "limit": RESULTS_PER_QUERY,
                    "country": country,
                })
                yield f"{self.BASE_URL}/search?{params}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse iTunes Search API JSON response."""
        try:
            data = json.loads(html)
        except (json.JSONDecodeError, TypeError):
            return []

        contacts = []
        for result in data.get("results", []):
            track_id = str(result.get("trackId", ""))
            if track_id in self._seen_ids:
                continue
            self._seen_ids.add(track_id)

            # Artist/host name
            artist_name = result.get("artistName", "").strip()
            track_name = result.get("trackName", "").strip()

            name = artist_name
            if not name or name.lower() in {"unknown", "various artists"}:
                name = track_name
            if not name or len(name) < 2:
                continue

            # Website from feed URL domain
            feed_url = result.get("feedUrl", "")
            website = ""
            if feed_url:
                # Extract domain from feed URL as a starting point
                match = re.match(r"https?://([^/]+)", feed_url)
                if match:
                    domain = match.group(1)
                    # Skip common podcast hosting platforms
                    if not any(host in domain for host in [
                        "anchor.fm", "buzzsprout", "libsyn", "podbean",
                        "soundcloud", "spreaker", "transistor.fm",
                        "feedburner", "feeds.feedburner", "rss.",
                        "podcasts.apple", "megaphone.fm",
                    ]):
                        website = f"https://{domain}"

            # Collection view URL (Apple Podcasts page)
            collection_url = result.get("collectionViewUrl", "")

            # Genre info
            genres = result.get("genres", [])
            genre_str = ", ".join(genres[:3]) if genres else ""

            # Build bio
            bio_parts = []
            if track_name and track_name != name:
                bio_parts.append(f"Podcast: {track_name}")
            if genre_str:
                bio_parts.append(f"Genre: {genre_str}")
            track_count = result.get("trackCount", 0)
            if track_count:
                bio_parts.append(f"{track_count} episodes")
            bio = " | ".join(bio_parts) if bio_parts else "Apple Podcasts host"

            contacts.append(ScrapedContact(
                name=name,
                company=track_name if track_name != name else "",
                website=website or collection_url,
                bio=bio,
                source_category="podcasters",
                raw_data={
                    "itunes_id": track_id,
                    "feed_url": feed_url,
                    "genre": genre_str,
                    "track_count": track_count,
                },
            ))

        return contacts
