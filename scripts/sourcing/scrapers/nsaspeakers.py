"""
National Speakers Association (NSA) directory scraper.

NSA's "Find a Speaker" page at nsaspeaker.org directs to an eSpeakers
whitelabel directory at espeakers.com/s/nsas. Each speaker profile is
a Next.js SSR page with rich JSON data embedded in __NEXT_DATA__.

Strategy:
  1. Iterate through eSpeakers marketplace profile IDs
  2. Extract full profile data from __NEXT_DATA__ JSON on each profile page
  3. Filter for NSA members (associations containing "NSA" or "National
     Speakers", or awards containing "CSP"/"CPAE")

The eSpeakers profile ID space is sparse (IDs range from ~5000 to ~55000+).
The NSA whitelabel at espeakers.com/s/nsas exists but has no paginated
API to list members, so we scan marketplace profiles and filter.

Domain migration note (2026-03):
  Profile URLs now 301 redirect through balboa.espeakers.com. Invalid
  IDs return 500 from the redirect endpoint. We must NOT retry 500s
  (they mean "profile doesn't exist") and must follow redirects.

Estimated yield: 2,000-4,000 professional speakers
"""

from __future__ import annotations

import json
import re
from typing import Iterator, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Speaker ID ranges to scan. eSpeakers IDs are roughly 5000-55000+.
# We scan in batches to cover the entire range.
ID_START = 5000
ID_END = 55000
ID_STEP = 1  # Check every ID


class Scraper(BaseScraper):
    SOURCE_NAME = "nsaspeakers"
    BASE_URL = "https://www.espeakers.com"
    REQUESTS_PER_MINUTE = 30  # Site tolerates higher rate
    TYPICAL_ROLES = ["Thought Leader", "Educator"]
    TYPICAL_NICHES = ["speaking", "corporate_training"]
    TYPICAL_OFFERINGS = ["speaking", "keynote", "presentations"]

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
        """Yield eSpeakers marketplace profile URLs by ID.

        Scans the entire ID range. Invalid/empty profiles are skipped
        gracefully in scrape_page().
        """
        for speaker_id in range(ID_START, ID_END + 1, ID_STEP):
            yield f"{self.BASE_URL}/marketplace/profile/{speaker_id}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Extract speaker data from the __NEXT_DATA__ JSON embedded in the page.

        eSpeakers profile pages are Next.js SSR pages. All speaker data
        is pre-rendered as JSON in a <script id="__NEXT_DATA__"> tag.
        """
        profile = self._extract_profile(html)
        if not profile:
            return []

        name = (profile.get("name") or "").strip()
        if not name or len(name) < 2:
            return []

        # Check for NSA association membership
        associations = profile.get("associations") or []
        is_nsa = False
        for assoc in associations:
            if isinstance(assoc, dict):
                assoc_name = (assoc.get("name") or "").lower()
            elif isinstance(assoc, str):
                assoc_name = assoc.lower()
            else:
                continue
            if "national speakers" in assoc_name or "nsa" in assoc_name:
                is_nsa = True
                break

        # Also check awards for CSP/CPAE (NSA credentials)
        awards = profile.get("awards") or []
        for award in awards:
            if isinstance(award, dict):
                award_name = (award.get("name") or "").lower()
            elif isinstance(award, str):
                award_name = award.lower()
            else:
                continue
            if "csp" in award_name or "cpae" in award_name or "certified speaking" in award_name:
                is_nsa = True
                break

        # Accept all speakers regardless of NSA membership since the
        # eSpeakers marketplace has thousands of professional speakers.
        # The NSA filter can be applied later during enrichment.

        # Extract speaker ID for dedup
        sid = profile.get("sid")
        if sid and sid in self._seen_ids:
            return []
        if sid:
            self._seen_ids.add(sid)

        # Build contact
        email = (profile.get("email") or "").strip()
        # Filter out eSpeakers relay emails (speaker+ID@espeakers.com)
        if email and "espeakers.com" in email.lower():
            email = ""

        phone = (profile.get("phone") or "").strip()
        website = (profile.get("url") or "").strip()
        if website and not website.startswith("http"):
            website = f"https://{website}"

        company = (profile.get("businessName") or "").strip()

        # Build bio from bios array
        bio = self._extract_bio(profile)

        # Location info
        city = (profile.get("city") or "").strip()
        state = (profile.get("state") or "").strip()
        country = (profile.get("country") or "").strip()
        location_parts = [p for p in [city, state, country] if p]
        location = ", ".join(location_parts)

        # Topics
        topics_raw = profile.get("topics") or []
        topics = []
        if isinstance(topics_raw, list):
            for t in topics_raw:
                if isinstance(t, list):
                    topics.extend(t)
                elif isinstance(t, str):
                    topics.append(t)
        topic_str = ", ".join(topics[:10]) if topics else ""

        # Build enriched bio
        bio_parts = []
        if bio:
            bio_parts.append(bio[:800])
        if location:
            bio_parts.append(f"Location: {location}")
        if topic_str:
            bio_parts.append(f"Topics: {topic_str}")
        if is_nsa:
            bio_parts.append("NSA Member")

        full_bio = " | ".join(bio_parts)

        # LinkedIn - scan page HTML for LinkedIn profile URLs
        linkedin = ""
        linkedin_match = re.search(
            r'https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9_\-]+/?',
            html,
        )
        if linkedin_match:
            linkedin = linkedin_match.group(0)

        contact = ScrapedContact(
            name=name,
            email=email,
            company=company,
            website=website,
            linkedin=linkedin,
            phone=phone,
            bio=full_bio,
            source_url=url,
            source_category="speakers",
            raw_data={
                "sid": sid,
                "is_nsa": is_nsa,
                "city": city,
                "state": state,
                "country": country,
                "topics": topics,
                "associations": [
                    (a.get("name") if isinstance(a, dict) else str(a))
                    for a in associations[:10]
                ],
            },
        )

        return [contact]

    def _extract_profile(self, html: str) -> Optional[dict]:
        """Extract the profile dict from __NEXT_DATA__ JSON."""
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
        """Extract the best bio text from the profile's bios array."""
        bios = profile.get("bios") or []
        if not bios:
            # Fallback to shortprofile
            short = profile.get("shortprofile") or ""
            if isinstance(short, str):
                return self._clean_html(short)
            return ""

        # Find the English bio (or first available)
        best_bio = ""
        for bio_entry in bios:
            if isinstance(bio_entry, dict):
                lang = (bio_entry.get("lang") or "").lower()
                oneline = bio_entry.get("oneline") or ""
                full = bio_entry.get("full") or bio_entry.get("text") or oneline
                if isinstance(full, str) and full:
                    cleaned = self._clean_html(full)
                    if lang == "en" or not best_bio:
                        best_bio = cleaned
                    if lang == "en":
                        break
            elif isinstance(bio_entry, str) and not best_bio:
                best_bio = self._clean_html(bio_entry)

        return best_bio

    @staticmethod
    def _clean_html(text: str) -> str:
        """Remove HTML tags and clean up text."""
        # Remove HTML tags
        clean = re.sub(r"<[^>]+>", " ", text)
        # Collapse whitespace
        clean = re.sub(r"\s+", " ", clean).strip()
        # Decode common entities
        clean = (
            clean.replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", '"')
            .replace("&#39;", "'")
            .replace("&nbsp;", " ")
        )
        return clean[:1000]
