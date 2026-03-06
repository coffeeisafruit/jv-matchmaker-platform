"""
eSpeakers marketplace scraper.

eSpeakers (espeakers.com/marketplace) is one of the largest speaker
directories with thousands of professional speakers. The site is a
Next.js SPA with server-side rendering -- speaker data is embedded
in __NEXT_DATA__ JSON on each profile page.

Strategy:
  1. Generate profile URLs by iterating through speaker IDs
  2. Fetch each profile page and extract __NEXT_DATA__ JSON
  3. Parse the rich profile data (name, email, phone, website,
     business, bio, topics, location, awards)

The eSpeakers speaker ID space ranges from ~5000 to ~55000+. Not
all IDs correspond to active profiles -- invalid/empty profiles
are skipped gracefully.

Domain migration note (2026-03):
  Profile URLs at www.espeakers.com/marketplace/profile/{ID} now 301
  redirect to balboa.espeakers.com/screen/redirectprofileslug, which
  redirects again to the slug-based URL. Invalid/non-existent IDs
  return a 500 from the redirect endpoint. We must NOT retry 500s
  (they mean "profile doesn't exist") and must follow redirects.

Estimated yield: 8,000-15,000 speakers
"""

from __future__ import annotations

import json
import re
from typing import Iterator, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from scripts.sourcing.base import BaseScraper, ScrapedContact


# eSpeakers speaker ID range. The space is sparse but covers most
# active speakers between 5000 and 55000.
ID_START = 5000
ID_END = 55000


class Scraper(BaseScraper):
    SOURCE_NAME = "espeakers"
    BASE_URL = "https://www.espeakers.com"
    REQUESTS_PER_MINUTE = 30  # Site tolerates higher rate
    TYPICAL_ROLES = ["Thought Leader", "Educator"]
    TYPICAL_NICHES = ["speaking", "corporate_training", "leadership_coaching"]
    TYPICAL_OFFERINGS = ["speaking", "keynote", "workshops", "training"]

    # Don't retry 500 — eSpeakers returns 500 for non-existent profile IDs
    RETRY_STATUS_FORCELIST = [429, 502, 503, 504]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_ids: set[int] = set()

    def fetch_page(self, url: str, timeout: int = 30) -> Optional[str]:
        """Fetch URL, following redirects. Treat 500 as skip, not error."""
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)
        try:
            resp = self.session.get(url, timeout=timeout, allow_redirects=True)
            if resp.status_code == 500:
                self.logger.debug("Profile not found (500): %s", url)
                return None
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1
            return resp.text
        except requests.RequestException as exc:
            self.logger.warning("Fetch failed for %s: %s", url, exc)
            self.stats["errors"] += 1
            return None

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield eSpeakers marketplace profile URLs by speaker ID.

        Iterates through the full ID range. Pages that 500 or return
        an empty profile are silently skipped in scrape_page().
        """
        for speaker_id in range(ID_START, ID_END + 1):
            yield f"{self.BASE_URL}/marketplace/profile/{speaker_id}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Extract speaker data from __NEXT_DATA__ JSON.

        Each eSpeakers profile page embeds full speaker data in a
        <script id="__NEXT_DATA__"> tag as part of Next.js SSR.
        Fields include name, email, phone, website, businessName,
        city, state, country, bios, topics, programs, awards, etc.
        """
        profile = self._extract_profile(html)
        if not profile:
            return []

        name = (profile.get("name") or "").strip()
        if not name or len(name) < 2:
            return []

        # Deduplicate by speaker ID
        sid = profile.get("sid")
        if sid is not None:
            if sid in self._seen_ids:
                return []
            self._seen_ids.add(sid)

        # Email -- filter out eSpeakers relay addresses
        email = (profile.get("email") or "").strip()
        if email and "espeakers.com" in email.lower():
            email = ""

        # Phone
        phone = (profile.get("phone") or "").strip()
        # Clean phone: remove non-standard chars but keep digits and formatting
        if phone:
            phone = re.sub(r"[^\d\-\+\(\)\s\.]", "", phone).strip()

        # Website
        website = (profile.get("url") or "").strip()
        if website and not website.startswith("http"):
            website = f"https://{website}"

        # Company / business name
        company = (profile.get("businessName") or "").strip()

        # Bio
        bio = self._extract_bio(profile)

        # Location
        city = (profile.get("city") or "").strip()
        state = (profile.get("state") or "").strip()
        country = (profile.get("country") or "").strip()
        location_parts = [p for p in [city, state, country] if p]
        location = ", ".join(location_parts)

        # Topics
        topics = self._extract_topics(profile)
        topic_str = ", ".join(topics[:10]) if topics else ""

        # Awards / certifications
        awards = profile.get("awards") or []
        award_names = []
        for award in awards:
            if isinstance(award, dict):
                award_name = (award.get("name") or "").strip()
                if award_name:
                    award_names.append(award_name)
            elif isinstance(award, str):
                award_names.append(award.strip())

        # Programs / speaking topics
        programs = profile.get("programs") or []
        program_titles = []
        if isinstance(programs, list):
            for prog in programs[:5]:
                if isinstance(prog, dict):
                    title = (prog.get("title") or "").strip()
                    if title:
                        program_titles.append(title)

        # Build enriched bio
        bio_parts = []
        if bio:
            bio_parts.append(bio[:700])
        if location:
            bio_parts.append(f"Location: {location}")
        if topic_str:
            bio_parts.append(f"Topics: {topic_str}")
        if award_names:
            bio_parts.append(f"Awards: {', '.join(award_names[:5])}")
        if program_titles:
            bio_parts.append(f"Programs: {', '.join(program_titles[:3])}")

        full_bio = " | ".join(bio_parts)

        # LinkedIn - scan page HTML for LinkedIn URLs
        linkedin = ""
        linkedin_match = re.search(
            r"https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9_\-]+/?",
            html,
        )
        if linkedin_match:
            linkedin = linkedin_match.group(0)

        # Build profile URL with slug for source_url
        slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
        profile_url = f"{self.BASE_URL}/marketplace/profile/{sid}/{slug}" if sid else url

        contact = ScrapedContact(
            name=name,
            email=email,
            company=company,
            website=website,
            linkedin=linkedin,
            phone=phone,
            bio=full_bio,
            source_url=profile_url,
            source_category="speakers",
            raw_data={
                "sid": sid,
                "city": city,
                "state": state,
                "country": country,
                "topics": topics,
                "awards": award_names[:5],
                "fee": (profile.get("fee") or ""),
                "high_fee": (profile.get("high_fee") or ""),
            },
        )

        return [contact]

    def _extract_profile(self, html: str) -> Optional[dict]:
        """Extract the profile dict from __NEXT_DATA__ JSON in the page."""
        match = re.search(
            r'<script\s+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html,
            re.DOTALL,
        )
        if not match:
            return None

        try:
            data = json.loads(match.group(1))
        except (json.JSONDecodeError, ValueError):
            return None

        profile = (
            data.get("props", {})
            .get("pageProps", {})
            .get("profile")
        )
        if not profile or not isinstance(profile, dict):
            return None

        return profile

    def _extract_bio(self, profile: dict) -> str:
        """Extract best bio text from the bios array.

        eSpeakers stores bios as a list of dicts with lang, oneline,
        and optionally full/text fields. We prefer English bio.
        """
        bios = profile.get("bios") or []
        if not bios:
            short = profile.get("shortprofile") or ""
            if isinstance(short, str):
                return self._clean_html(short)
            return ""

        best_bio = ""
        for bio_entry in bios:
            if isinstance(bio_entry, dict):
                lang = (bio_entry.get("lang") or "").lower()
                text = (
                    bio_entry.get("full")
                    or bio_entry.get("text")
                    or bio_entry.get("oneline")
                    or ""
                )
                if isinstance(text, str) and text:
                    cleaned = self._clean_html(text)
                    if lang == "en" or not best_bio:
                        best_bio = cleaned
                    if lang == "en":
                        break
            elif isinstance(bio_entry, str) and not best_bio:
                best_bio = self._clean_html(bio_entry)

        return best_bio

    def _extract_topics(self, profile: dict) -> list[str]:
        """Extract topic names from the profile."""
        topics_raw = profile.get("topics") or []
        topics = []
        if isinstance(topics_raw, list):
            for item in topics_raw:
                if isinstance(item, list):
                    for t in item:
                        if isinstance(t, str) and t.strip():
                            topics.append(t.strip())
                elif isinstance(item, str) and item.strip():
                    topics.append(item.strip())
                elif isinstance(item, dict):
                    name = (item.get("name") or "").strip()
                    if name:
                        topics.append(name)
        return topics

    @staticmethod
    def _clean_html(text: str) -> str:
        """Strip HTML tags, collapse whitespace, decode entities."""
        clean = re.sub(r"<[^>]+>", " ", text)
        clean = re.sub(r"\s+", " ", clean).strip()
        clean = (
            clean.replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", '"')
            .replace("&#39;", "'")
            .replace("&nbsp;", " ")
        )
        return clean[:1000]
