"""
JVNotifyPro.com JV launch announcement scraper.

JVNotifyPro is one of the longest-running JV marketing communities. Their
blog at jointventures.jvnotifypro.com publishes digest-style posts, each
containing multiple JV launch announcements with rich partnership data:
vendor names, product names, launch dates, commission structures, JV invite
page URLs, contest prizes, and partner relationships.

Data sources (in priority order):
  1. Blog listing pages at jointventures.jvnotifypro.com/page/N/
     - WordPress posts, each containing 1-5 launch announcements
     - Entries separated by ===== delimiters
     - 100+ pages of historical data
  2. v3.jvnotifypro.com homepage sidebar
     - Recent premium and partner announcement links
     - Links to forum threads and announcement redirect URLs

Announcement URLs at v3.jvnotifypro.com/announcements/partner/... redirect
to actual JV request pages (Google Forms, affiliate registration, etc.).
These redirect targets are captured as the website field -- the most
valuable data point for the JV matchmaker platform.

Estimated yield: 500-2,000 unique JV partners with deep launch data
"""

from __future__ import annotations

import re
import time
from typing import Iterator, Optional
from urllib.parse import urljoin

from scripts.sourcing.base import BaseScraper, ScrapedContact


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BLOG_BASE = "https://jointventures.jvnotifypro.com"
V3_BASE = "https://v3.jvnotifypro.com"

# Maximum blog pages to crawl (each page has ~10 posts, each post has 1-5 entries)
MAX_BLOG_PAGES = 500  # Full archive goes back to 2005

# Regex patterns for extracting structured data from entry text
DATE_RE = re.compile(
    r"(?:Pre-Launch\s+Begins?|Launch\s+Day|Launched?|Launch\s+Date|"
    r"Evergreen\s+Affiliate\s+Program\s+Announced|"
    r"Pre-Launch|Going\s+Live)"
    r"[:\s]*([A-Z][a-z]+day,?\s+)?([A-Z][a-z]+ \d{1,2}(?:st|nd|rd|th)?,?\s*\d{4})"
    r"(?:\s*[-–]\s*(?:[A-Z][a-z]+day,?\s+)?([A-Z][a-z]+ \d{1,2}(?:st|nd|rd|th)?,?\s*\d{4}))?",
    re.IGNORECASE,
)

COMMISSION_RE = re.compile(
    r"(\d{1,3}%\s*(?:affiliate\s+)?commission[^.!]*?"
    r"(?:\$[\d,]+[^.!]*?)?)"
    r"|"
    r"(\$[\d,]+(?:\.\d{2})?\s*(?:per|flat|commission|bounty)[^.!]*?)"
    r"|"
    r"((?:up to |earn )?\$[\d,]+(?:\.\d{2})?\s*per\s+(?:sale|lead|referral)[^.!]*?)"
    r"|"
    r"(\d{1,3}%\s*(?:FE|BE|front.?end|back.?end)[^.!]*?)",
    re.IGNORECASE,
)

PRIZE_RE = re.compile(
    r"\$[\d,]+(?:\.\d{2})?\s*(?:in\s+)?(?:JV\s+)?(?:Launch\s+)?(?:Contest\s+)?Prizes?",
    re.IGNORECASE,
)

PRICE_RE = re.compile(
    r"(?:Priced?\s+(?:Between\s+|at\s+|from\s+)?)\$[\d,]+(?:\s*[-–]\s*\$[\d,]+)?",
    re.IGNORECASE,
)

# Known boilerplate phrases to strip from entries
BOILERPLATE_PHRASES = [
    "Fellow JVNP 2.0 Partner",
    "Experienced, serious affiliate marketers",
    "That's All, Folks!",
    "To OUR Success",
    "Mike Merz Sr",
    "JVNotifyPro Joint Ventures",
    "AWeber/JVListPro",
    "Legal",
    "As stated during the registration process",
    "Powered By",
    "JVListPro/AWeber",
]


class Scraper(BaseScraper):
    """Scrape JVNotifyPro blog for JV launch announcements and partner data."""

    SOURCE_NAME = "jvnotifypro"
    BASE_URL = BLOG_BASE
    REQUESTS_PER_MINUTE = 60  # 1 request per second

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_keys: set[str] = set()

    # ------------------------------------------------------------------
    # URL generation
    # ------------------------------------------------------------------

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield blog listing page URLs, then v3 homepage for sidebar data."""
        # Primary: blog listing pages (newest first)
        yield BLOG_BASE + "/"
        for page_num in range(2, MAX_BLOG_PAGES + 1):
            yield f"{BLOG_BASE}/page/{page_num}/"

    # ------------------------------------------------------------------
    # Page parsing
    # ------------------------------------------------------------------

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a blog listing page for JV launch announcement posts."""
        soup = self.parse_html(html)
        contacts = []

        # Find all WordPress post divs
        posts = [
            div for div in soup.find_all("div", class_=True)
            if "type-post" in div.get("class", [])
        ]

        if not posts:
            self.logger.debug("No posts found on %s", url)
            return contacts

        for post in posts:
            post_contacts = self._parse_blog_post(post, url)
            contacts.extend(post_contacts)

        return contacts

    def _parse_blog_post(self, post_div, listing_url: str) -> list[ScrapedContact]:
        """Parse a single WordPress post div containing 1+ launch announcements.

        Each post typically contains multiple announcements separated by
        ===== delimiters. Each announcement has: vendor/product header,
        launch dates, commission details, description, and JV invite link.
        """
        contacts = []

        # Extract post URL from title link
        post_url = listing_url
        title_el = post_div.find(["h1", "h2", "h3"])
        if title_el:
            title_link = title_el.find("a", href=True)
            if title_link:
                post_url = title_link["href"]

        # Extract categories from post div classes
        post_classes = post_div.get("class", [])
        categories = [
            c.replace("category-", "").replace("-", " ").title()
            for c in post_classes
            if c.startswith("category-") and c not in (
                "category-uncategorized",
                "category-weekend-digest",
                "category-weekday-digest",
            )
        ]

        # Get the entry-content div
        entry_content = post_div.find("div", class_="entry-content")
        if not entry_content:
            return contacts

        # Collect all announcement/JV invite links with their redirect URLs
        jv_links = self._extract_jv_links(entry_content)

        # Split content into individual announcements by ===== separator
        # We work with the raw HTML to preserve link associations
        content_text = entry_content.get_text(separator="\n")
        entries = re.split(r"={3,}", content_text)

        for entry_text in entries:
            entry_text = entry_text.strip()
            if not entry_text or len(entry_text) < 50:
                continue

            # Skip boilerplate sections
            if self._is_boilerplate(entry_text):
                continue

            # Try to extract announcement data from this entry
            entry_contacts = self._parse_entry(
                entry_text, jv_links, categories, post_url
            )
            contacts.extend(entry_contacts)

        return contacts

    def _extract_jv_links(self, content_el) -> dict[str, str]:
        """Extract vendor-to-JV-invite-URL mapping from entry content links.

        Returns dict mapping lowercase vendor/product key to the announcement URL.
        The announcement URLs at v3.jvnotifypro.com/announcements/partner/...
        redirect to the actual JV request page.
        """
        jv_links: dict[str, str] = {}

        for a_tag in content_el.find_all("a", href=True):
            href = a_tag["href"]
            text = a_tag.get_text(strip=True)

            # Match announcement partner URLs
            if "/announcements/partner/" in href:
                # Normalize to absolute URL
                if href.startswith("/"):
                    href = V3_BASE + href
                # Use the link text as key (lowered)
                if text:
                    key = re.sub(r"\s+", " ", text.lower().strip())
                    jv_links[key] = href
                # Also extract vendor name from URL path
                # /announcements/partner/vendor_name/Product_Name
                parts = href.split("/announcements/partner/")
                if len(parts) == 2:
                    path_parts = parts[1].strip("/").split("/")
                    if path_parts:
                        vendor_key = path_parts[0].replace("_", " ").lower()
                        jv_links[vendor_key] = href

        return jv_links

    def _parse_entry(
        self,
        text: str,
        jv_links: dict[str, str],
        categories: list[str],
        post_url: str,
    ) -> list[ScrapedContact]:
        """Parse a single announcement entry block into ScrapedContact(s).

        When multiple partners are listed (e.g., 'Russell Brunson + Justin Benton'),
        creates separate contacts for each partner.
        """
        contacts = []
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
        if not lines:
            return contacts

        # --- Extract the header line (vendor - product) ---
        header = self._find_header_line(lines)
        if not header:
            return contacts

        # Parse vendor(s) and product from header
        vendors, product = self._parse_header(header)
        if not vendors:
            return contacts

        # --- Extract launch dates ---
        dates = self._extract_dates(text)

        # --- Extract commission info ---
        commissions = self._extract_commissions(text)

        # --- Extract prize info ---
        prizes = PRIZE_RE.findall(text)

        # --- Extract price info ---
        prices = PRICE_RE.findall(text)

        # --- Find JV invite URL for this entry ---
        jv_url = self._find_jv_url(vendors, product, jv_links)

        # --- Extract description (first substantial paragraph) ---
        description = self._extract_description(lines, header)

        # --- Build bio ---
        category_str = " | ".join(categories) if categories else ""

        # Create a contact for each vendor
        for vendor in vendors:
            vendor_clean = vendor.strip()
            if not vendor_clean or len(vendor_clean) < 2:
                continue

            # Deduplication key: vendor + product
            dedup_key = f"{vendor_clean.lower()}|{(product or '').lower()}"
            if dedup_key in self._seen_keys:
                continue
            self._seen_keys.add(dedup_key)

            # Build structured bio
            bio = self._build_bio(
                dates=dates,
                commissions=commissions,
                prizes=prizes,
                prices=prices,
                partners=[v.strip() for v in vendors if v.strip() != vendor_clean],
                category=category_str,
                description=description,
            )

            contact = ScrapedContact(
                name=vendor_clean,
                company=product or "",
                website=jv_url or post_url,
                email="",
                linkedin="",
                phone="",
                bio=bio,
                source_platform=self.SOURCE_NAME,
                source_url=post_url,
                source_category="jv_launches",
            )
            contacts.append(contact)

        return contacts

    # ------------------------------------------------------------------
    # Header parsing
    # ------------------------------------------------------------------

    def _find_header_line(self, lines: list[str]) -> str:
        """Find the announcement header line (Vendor - Product format)."""
        for line in lines[:8]:
            # Header patterns:
            # "Vendor Name - Product Name Launch Affiliate Program JV Request Form"
            # "Vendor Name + Partner Name - Product Name ..."
            # Must contain a dash separating vendor from product
            if " - " in line or " – " in line:
                # Skip boilerplate
                lower = line.lower()
                if any(bp.lower() in lower for bp in BOILERPLATE_PHRASES):
                    continue
                # Must look like a header (has capitalized words, reasonable length)
                if 10 < len(line) < 300:
                    return line
        return ""

    def _parse_header(self, header: str) -> tuple[list[str], str]:
        """Parse 'Vendor1 + Vendor2 - Product Name ...' into vendors and product.

        Returns (list of vendor names, product name).
        """
        # Normalize dashes
        header = header.replace(" – ", " - ")

        # Split on first ' - ' to separate vendors from product
        parts = header.split(" - ", 1)
        if len(parts) < 2:
            return [], ""

        vendor_part = parts[0].strip()
        product_part = parts[1].strip()

        # Clean up product name: remove trailing JV/affiliate/launch boilerplate
        product_part = re.sub(
            r"\s*(?:Launch\s+)?(?:Affiliate\s+)?(?:Program\s+)?"
            r"(?:JV\s+)?(?:Request\s+)?(?:Page|Form|Registration\s+Page|Invite)?\s*$",
            "",
            product_part,
            flags=re.IGNORECASE,
        ).strip()
        # Remove trailing "launch affiliate program" etc.
        product_part = re.sub(
            r"\s+(?:launch\s+)?(?:affiliate\s+program|jv\s+request|jv\s+invite)"
            r".*$",
            "",
            product_part,
            flags=re.IGNORECASE,
        ).strip()

        # Parse vendor(s): split by '+', '&', ' and '
        # But be careful with company names that contain these (e.g., "Simon & Schuster")
        vendors = re.split(r"\s*\+\s*", vendor_part)

        # Clean vendor names
        cleaned_vendors = []
        for v in vendors:
            v = v.strip()
            # Remove parenthetical company names for cleaner person name
            # e.g., "Eben Pagan (Virtual Coach)" -> keep "Eben Pagan"
            # but preserve company in product field if not already captured
            paren_match = re.match(r"^(.+?)\s*\((.+?)\)\s*$", v)
            if paren_match:
                person_name = paren_match.group(1).strip()
                company_name = paren_match.group(2).strip()
                cleaned_vendors.append(person_name)
                # If product is empty, use the company from parentheses
                if not product_part:
                    product_part = company_name
            else:
                cleaned_vendors.append(v)

        return cleaned_vendors, product_part

    # ------------------------------------------------------------------
    # Data extraction helpers
    # ------------------------------------------------------------------

    def _extract_dates(self, text: str) -> list[str]:
        """Extract launch date strings from entry text."""
        dates = []
        for match in DATE_RE.finditer(text):
            full = match.group(0).strip()
            # Clean up the date string
            full = re.sub(r"\s+", " ", full)
            dates.append(full)
        return dates

    def _extract_commissions(self, text: str) -> list[str]:
        """Extract commission rate strings from entry text."""
        commissions = []
        for match in COMMISSION_RE.finditer(text):
            # Get the first non-empty group
            for group in match.groups():
                if group:
                    clean = re.sub(r"\s+", " ", group.strip())
                    if clean and len(clean) > 3:
                        commissions.append(clean)
                    break
        return commissions

    def _extract_description(self, lines: list[str], header: str) -> str:
        """Extract the first substantial descriptive paragraph."""
        collecting = False
        desc_parts = []

        for line in lines:
            # Start collecting after we pass the header
            if not collecting:
                if line == header or header in line:
                    collecting = True
                continue

            # Skip short lines (dates, labels)
            if len(line) < 30:
                # But if we already have description, stop
                if desc_parts:
                    break
                continue

            # Skip lines that are just commission/date data
            lower = line.lower()
            if any(kw in lower for kw in [
                "pre-launch", "launch day:", "commission",
                "jv request", "jv invite", "affiliate program",
                "registration page", "click here",
            ]):
                if desc_parts:
                    break
                continue

            # Skip boilerplate
            if self._is_boilerplate(line):
                break

            desc_parts.append(line)
            # Usually one good paragraph is enough
            if len(" ".join(desc_parts)) > 100:
                break

        description = " ".join(desc_parts).strip()
        # Truncate if too long
        if len(description) > 500:
            description = description[:497] + "..."
        return description

    def _find_jv_url(
        self,
        vendors: list[str],
        product: str,
        jv_links: dict[str, str],
    ) -> str:
        """Find the JV invite URL for a given vendor+product combination."""
        # Try matching vendor names against collected JV links
        for vendor in vendors:
            vendor_lower = vendor.strip().lower()
            for key, url in jv_links.items():
                if vendor_lower in key or key in vendor_lower:
                    return url

        # Try matching product name
        if product:
            product_lower = product.lower()
            for key, url in jv_links.items():
                if product_lower in key or key in product_lower:
                    return url

        # Try matching individual vendor name words
        for vendor in vendors:
            name_parts = vendor.strip().lower().split()
            if len(name_parts) >= 2:
                last_name = name_parts[-1]
                for key, url in jv_links.items():
                    if last_name in key:
                        return url

        return ""

    def _build_bio(
        self,
        dates: list[str],
        commissions: list[str],
        prizes: list[str],
        prices: list[str],
        partners: list[str],
        category: str,
        description: str,
    ) -> str:
        """Build a structured bio string from extracted data."""
        parts = []

        if dates:
            parts.append("Launch: " + "; ".join(dates[:3]))

        if commissions:
            parts.append("Commission: " + " + ".join(commissions[:3]))

        if prizes:
            parts.append("Prizes: " + ", ".join(prizes[:2]))

        if prices:
            parts.append("Price: " + ", ".join(prices[:2]))

        if partners:
            parts.append("Partners: " + ", ".join(partners[:5]))

        if category:
            parts.append("Category: " + category)

        bio = " | ".join(parts)

        # Append description if we have room
        if description:
            if bio:
                bio += " | " + description
            else:
                bio = description

        # Truncate to fit ScrapedContact.bio limit (2000 chars in to_ingestion_dict)
        if len(bio) > 1900:
            bio = bio[:1897] + "..."

        return bio if bio else "JV launch partner listed on JVNotifyPro"

    def _is_boilerplate(self, text: str) -> bool:
        """Check if text is boilerplate content to skip."""
        lower = text.lower().strip()

        # Check against known phrases
        for phrase in BOILERPLATE_PHRASES:
            if phrase.lower() in lower:
                return True

        # Skip very short or ad-like content
        if lower.startswith("*****"):
            return True
        if lower.startswith("new!") and len(lower) < 30:
            return True
        if "check out upcoming" in lower:
            return True
        if "jvnewswatch" in lower:
            return True
        if "rss" == lower or lower.startswith("legal"):
            return True

        return False


# ---------------------------------------------------------------------------
# Standalone test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    scraper = Scraper()
    count = 0
    for contact in scraper.run(max_contacts=5):
        count += 1
        print(f"\n--- Contact {count} ---")
        print(f"  Name:    {contact.name}")
        print(f"  Company: {contact.company}")
        print(f"  Website: {contact.website}")
        print(f"  Bio:     {contact.bio[:200]}...")
        print(f"  Source:  {contact.source_url}")
    print(f"\nTotal: {count} contacts")
    print(f"Stats: {scraper.stats}")
