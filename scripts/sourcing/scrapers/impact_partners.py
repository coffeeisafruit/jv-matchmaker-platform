"""
Impact.com partner / influencer directory scraper.

Impact.com has a public influencer/creator directory at:
  https://impact.com/find-influencers/

The directory is organized by category (apparel, health & beauty, travel,
etc.) with paginated listings. Each listing page shows ~15 creators per
page with their display name, handle, follower count, and short bio.

Individual profile pages (e.g., /find-influencers/apparel/walkinlove/)
contain more detailed bios and engagement metrics, but no external
website URLs or contact info — those require platform login.

The actual partner marketplace for brands (600M+ products) requires
authentication at app.impact.com.

This scraper:
  1. Iterates through all 15 public categories
  2. Paginates through listings (up to 28+ pages per category)
  3. Fetches individual profile pages for richer bio data
  4. Extracts name, handle, follower count, bio, and category

Estimated yield: 2,000-5,000 influencer/creator profiles
"""

from __future__ import annotations

import re
from typing import Iterator, Optional
from urllib.parse import urljoin, urlparse, parse_qs

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Impact.com public influencer directory categories
CATEGORIES = [
    "apparel",
    "art-entertainment",
    "auto-recreational-vehicles",
    "baby",
    "computers-electronics",
    "dating-romance",
    "flowers",
    "gifts",
    "health-beauty",
    "home-garden",
    "marketplace",
    "outdoor-fitness",
    "pets",
    "subscriptions-services",
    "travel",
]

# Maximum pages to scrape per category
# Most categories have 10-30 pages of ~15 creators each
MAX_PAGES_PER_CATEGORY = 50


class Scraper(BaseScraper):
    """Impact.com public influencer directory scraper.

    Scrapes the server-rendered influencer listing pages by category,
    then optionally fetches individual profile pages for richer bios.
    """

    SOURCE_NAME = "impact_partners"
    BASE_URL = "https://impact.com"
    REQUESTS_PER_MINUTE = 8  # Be polite to impact.com

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_handles: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield paginated category listing URLs.

        URL pattern: /find-influencers/{category}/?page={n}
        Page 1 has no ?page parameter.
        """
        for category in CATEGORIES:
            # Page 1 (no param needed)
            yield f"{self.BASE_URL}/find-influencers/{category}/"
            # Pages 2..N
            for page in range(2, MAX_PAGES_PER_CATEGORY + 1):
                yield f"{self.BASE_URL}/find-influencers/{category}/?page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a category listing page for influencer cards.

        The listing pages use a card structure with <a> links pointing
        to individual profile pages. Each card contains:
          - Display name (heading)
          - Follower count
          - @handle
          - Short bio text

        We also fetch individual profile pages for richer bios.
        """
        soup = self.parse_html(html)
        contacts = []

        # Determine current category from URL
        parsed = urlparse(url)
        path_parts = [p for p in parsed.path.strip("/").split("/") if p]
        category = path_parts[-1] if path_parts else "unknown"
        # If last part is a page number param, get the one before
        if category in ("find-influencers",):
            category = "general"
        elif len(path_parts) >= 2 and path_parts[0] == "find-influencers":
            category = path_parts[1]

        # Check if this page has content — empty pages mean we've gone past the end
        # Look for the pagination indicator "X of Y" pattern
        page_text = soup.get_text()
        if "No results found" in page_text or "no influencers" in page_text.lower():
            return []

        # Find influencer profile links: /find-influencers/{category}/{handle}/
        profile_pattern = re.compile(
            r"^/find-influencers/[a-z\-]+/([a-z0-9_\-\.]+)/?$", re.I
        )

        profile_links = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            match = profile_pattern.match(href)
            if match:
                handle = match.group(1)
                # Skip navigation/utility links
                if handle in ("page", "category", "search", ""):
                    continue
                if handle not in self._seen_handles:
                    self._seen_handles.add(handle)
                    profile_links.append({
                        "handle": handle,
                        "href": href,
                        "card_element": a_tag,
                    })

        if not profile_links:
            return []

        for pl in profile_links:
            handle = pl["handle"]
            card = pl["card_element"]

            # Extract data from the listing card itself
            card_text = card.get_text(separator="\n", strip=True)
            lines = [line.strip() for line in card_text.split("\n") if line.strip()]

            display_name = ""
            follower_count = ""
            short_bio = ""
            at_handle = ""

            for i, line in enumerate(lines):
                # Display name is typically the first meaningful line
                if not display_name and not line.startswith("@") and not line.isdigit():
                    # Skip very short lines that might be separators
                    if len(line) > 1 and line != "|":
                        display_name = line

                # Handle with @ prefix
                if line.startswith("@"):
                    at_handle = line

                # Follower count — a line that's purely numeric (possibly with commas)
                if re.match(r"^[\d,]+$", line.replace(",", "")):
                    follower_count = line

                # Bio text — longer text that isn't a name or handle
                if (
                    len(line) > 50
                    and not line.startswith("@")
                    and not re.match(r"^[\d,]+$", line)
                ):
                    short_bio = line

            if not display_name:
                display_name = handle

            # Clean up the display name
            display_name = display_name.strip()
            if display_name == "|":
                display_name = handle

            # Optionally fetch the individual profile page for a richer bio
            profile_url = urljoin(self.BASE_URL, pl["href"])
            richer_bio = self._fetch_profile_bio(profile_url)

            # Build the final bio
            bio_parts = []
            if richer_bio:
                bio_parts.append(richer_bio[:600])
            elif short_bio:
                bio_parts.append(short_bio[:400])

            if follower_count:
                bio_parts.append(f"Followers: {follower_count}")
            if at_handle:
                bio_parts.append(f"Handle: {at_handle}")
            bio_parts.append(f"Category: {category.replace('-', ' ').title()}")
            bio_parts.append("Impact.com creator marketplace")

            bio = " | ".join(bio_parts)

            # The handle might map to a website domain
            # (many handles are domains like "beemoneysavvy.com")
            website = ""
            if "." in handle and not handle.startswith("."):
                # Handle looks like a domain
                website = f"https://{handle}"
            else:
                # Use the Impact.com profile page
                website = profile_url

            contact = ScrapedContact(
                name=display_name,
                company=display_name,
                website=website,
                bio=bio,
                source_url=url,
                source_category="influencer_creator",
                raw_data={
                    "handle": handle,
                    "at_handle": at_handle,
                    "follower_count": follower_count,
                    "category": category,
                    "profile_url": profile_url,
                    "platform": "impact_com",
                },
            )
            contacts.append(contact)

        return contacts

    def _fetch_profile_bio(self, profile_url: str) -> str:
        """Fetch an individual profile page and extract the bio text.

        Returns the bio string, or empty string if fetch fails.
        Individual profile pages contain more detailed bio/description
        text and engagement metrics.
        """
        html = self.fetch_page(profile_url)
        if not html:
            return ""

        soup = self.parse_html(html)

        # Look for bio/description text in the profile page
        # The bio is typically in a paragraph or div after the main heading
        bio_text = ""

        # Try meta description first (usually has a good summary)
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            bio_text = (meta_desc.get("content") or "").strip()

        # Try og:description as fallback
        if not bio_text:
            og_desc = soup.find("meta", property="og:description")
            if og_desc:
                bio_text = (og_desc.get("content") or "").strip()

        # Try to find engagement stats
        page_text = soup.get_text()
        engagement_match = re.search(
            r"([\d.]+%)\s*engagement\s*rate", page_text, re.I
        )
        if engagement_match and bio_text:
            bio_text += f" | Engagement rate: {engagement_match.group(1)}"

        follower_match = re.search(
            r"([\d,]+)\s*social\s*followers", page_text, re.I
        )
        if follower_match and bio_text:
            bio_text += f" | {follower_match.group(1)} social followers"

        return bio_text[:800]
