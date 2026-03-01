"""
YouTube Data API v3 scraper for business/coaching channels.

Free tier: 10,000 units/day
  - search.list: 100 units/call, up to 50 results/call
  - channels.list: ~5 units/call (snippet+statistics+brandingSettings)

Strategy per run:
  ~30 search calls (3,000 units) + ~1,500 channel lookups (7,500 units) = 10,500 units
  Yields ~1,500 channels per day, ~3,000-5,000 unique after multi-day runs.

Requires YOUTUBE_API_KEY environment variable.
"""

from __future__ import annotations

import os
import logging
from typing import Iterator
from urllib.parse import quote_plus

from scripts.sourcing.base import BaseScraper, ScrapedContact

logger = logging.getLogger(__name__)

# Broad set of queries targeting JV-relevant niches
# ~80 queries to maximize daily quota usage
SEARCH_QUERIES = [
    # Coaching
    "business coaching", "life coaching", "executive coaching",
    "health coaching", "wellness coaching", "mindset coaching",
    "leadership coaching", "career coaching", "relationship coaching",
    "transformation coaching", "high ticket coaching",
    "group coaching programs", "coaching business",
    "fitness coaching", "nutrition coaching", "ADHD coaching",
    "dating coaching", "parenting coach", "confidence coach",
    # Course creators / educators
    "course creator", "online course business", "digital course expert",
    "online education entrepreneur", "teachable course creator",
    "kajabi course creator", "thinkific instructor",
    "udemy instructor tips", "skillshare teacher",
    # Speakers
    "motivational speaker", "keynote speaker business",
    "public speaking coach", "TEDx speaker tips",
    "paid speaking gigs", "speaker marketing",
    # Consultants
    "business consultant", "marketing consultant",
    "sales consultant", "strategy consultant",
    "HR consultant", "IT consultant business",
    "management consultant", "brand consultant",
    # Affiliate / JV / launches
    "affiliate marketing expert", "joint venture marketing",
    "affiliate marketing tutorial", "digital product launch",
    "product launch formula", "clickfunnels expert",
    "sales funnel expert", "webinar expert",
    # Personal development
    "personal development", "self improvement",
    "money mindset", "financial coaching",
    "productivity expert", "time management expert",
    "habit building", "goal setting expert",
    # Content creators
    "podcasting for business", "podcast host business",
    "newsletter business", "creator economy",
    "blogging business tips", "YouTube business tips",
    # Community / retreats
    "mastermind group leader", "retreat leader wellness",
    "workshop facilitator", "community builder online",
    "membership site owner", "online community building",
    # Authors
    "self help author", "business book author",
    "author speaker coach", "book launch strategy",
    "self publishing success",
    # Niche experts
    "real estate investing", "ecommerce entrepreneur",
    "Amazon FBA seller", "dropshipping business",
    "crypto investing education", "stock trading education",
    "forex trading education", "day trading business",
    # Wellness / spiritual
    "meditation teacher", "yoga business",
    "holistic health practitioner", "functional medicine",
    "energy healing", "spiritual business",
    # Professional services marketing
    "therapist marketing", "dentist marketing",
    "lawyer marketing", "financial advisor marketing",
    "chiropractor marketing", "doctor entrepreneur",
]

# Minimum subscriber count to consider a channel relevant
MIN_SUBSCRIBERS = 500


class Scraper(BaseScraper):
    SOURCE_NAME = "youtube_api"
    BASE_URL = "https://www.googleapis.com/youtube/v3"
    REQUESTS_PER_MINUTE = 60  # API is fast; quota is the real limit

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.api_key = os.environ.get("YOUTUBE_API_KEY", "")
        if not self.api_key:
            logger.warning(
                "YOUTUBE_API_KEY not set. Get a free key at "
                "https://console.cloud.google.com/apis/credentials"
            )
        self.units_used = 0
        self.daily_limit = 10_000
        self._seen_channel_ids: set[str] = set()

    def _has_budget(self, cost: int) -> bool:
        return self.units_used + cost <= self.daily_limit

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield search API URLs for each query."""
        for query in SEARCH_QUERIES:
            if not self._has_budget(100):
                self.logger.warning("Daily quota exhausted (%d units used)", self.units_used)
                return
            encoded = quote_plus(query)
            yield (
                f"{self.BASE_URL}/search?part=snippet&type=channel"
                f"&q={encoded}&maxResults=50&key={self.api_key}"
            )

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse YouTube search API response and enrich with channel details."""
        import json

        try:
            data = json.loads(html)
        except (json.JSONDecodeError, TypeError):
            return []

        if "error" in data:
            self.logger.error("API error: %s", data["error"].get("message", ""))
            return []

        self.units_used += 100  # search.list cost

        # Collect channel IDs from search results
        channel_ids = []
        for item in data.get("items", []):
            cid = item.get("snippet", {}).get("channelId") or item.get("id", {}).get("channelId")
            if cid and cid not in self._seen_channel_ids:
                channel_ids.append(cid)
                self._seen_channel_ids.add(cid)

        if not channel_ids:
            return []

        # Fetch channel details in batches of 50
        contacts = []
        for i in range(0, len(channel_ids), 50):
            batch_ids = channel_ids[i : i + 50]
            if not self._has_budget(5):
                break
            details = self._fetch_channel_details(batch_ids)
            contacts.extend(details)

        return contacts

    def _fetch_channel_details(self, channel_ids: list[str]) -> list[ScrapedContact]:
        """Fetch channel snippet + statistics + branding for a batch of IDs."""
        import json

        ids_str = ",".join(channel_ids)
        url = (
            f"{self.BASE_URL}/channels?part=snippet,statistics,brandingSettings"
            f"&id={ids_str}&key={self.api_key}"
        )

        resp_text = self.fetch_page(url)
        if not resp_text:
            return []

        self.units_used += 5  # channels.list cost

        try:
            data = json.loads(resp_text)
        except (json.JSONDecodeError, TypeError):
            return []

        contacts = []
        for ch in data.get("items", []):
            snippet = ch.get("snippet", {})
            stats = ch.get("statistics", {})
            branding = ch.get("brandingSettings", {}).get("channel", {})

            # Filter by subscriber count
            sub_count = int(stats.get("subscriberCount", 0))
            if sub_count < MIN_SUBSCRIBERS:
                continue

            name = snippet.get("title", "").strip()
            if not name:
                continue

            description = snippet.get("description", "")
            custom_url = snippet.get("customUrl", "")

            # Build website from custom URL or channel ID
            channel_id = ch.get("id", "")
            website = ""
            if custom_url:
                website = f"https://www.youtube.com/{custom_url}"
            elif channel_id:
                website = f"https://www.youtube.com/channel/{channel_id}"

            # Check branding for external links
            # (YouTube API doesn't expose About page links directly,
            # but description often contains website URLs)
            external_website = ""
            linkedin = ""
            if description:
                urls = self.extract_emails(description)  # won't work, need website extraction
                # Extract URLs from description
                import re
                url_matches = re.findall(
                    r'https?://[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}(?:/[^\s"\'<>]*)?',
                    description,
                )
                for u in url_matches:
                    u_lower = u.lower()
                    if "linkedin.com/in/" in u_lower:
                        linkedin = u
                    elif (
                        "youtube.com" not in u_lower
                        and "youtu.be" not in u_lower
                        and "google.com" not in u_lower
                        and "bit.ly" not in u_lower
                        and not external_website
                    ):
                        external_website = u

            emails = self.extract_emails(description) if description else []

            # Build bio with audience stats
            bio_parts = []
            if description:
                bio_parts.append(description[:500])
            bio_parts.append(
                f"YouTube: {sub_count:,} subscribers, "
                f"{int(stats.get('videoCount', 0)):,} videos, "
                f"{int(stats.get('viewCount', 0)):,} views"
            )
            bio = " | ".join(bio_parts)

            contacts.append(ScrapedContact(
                name=name,
                email=emails[0] if emails else "",
                company=name,  # Channel name as company
                website=external_website or website,
                linkedin=linkedin,
                bio=bio,
                source_category="youtube",
                raw_data={
                    "channel_id": channel_id,
                    "subscriber_count": sub_count,
                    "video_count": int(stats.get("videoCount", 0)),
                    "view_count": int(stats.get("viewCount", 0)),
                    "youtube_url": website,
                },
            ))

        return contacts

    def fetch_page(self, url: str, timeout: int = 30) -> str | None:
        """Override to skip rate limiter for API calls (quota is the limit)."""
        try:
            resp = self.session.get(url, timeout=timeout)
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1
            return resp.text
        except Exception as exc:
            self.logger.warning("Fetch failed: %s", exc)
            self.stats["errors"] += 1
            return None
