"""
Zapier Solution Partner Directory scraper.

Scrapes https://zapier.com/partnerdirectory for Zapier solution
partners (agencies, consultants, automation experts).

The directory is a Nuxt.js SSR application powered by PartnerPage.io.
The initial HTML response includes server-rendered partner cards with
full description text, tier badges, and review counts. The page uses
infinite scroll for additional partners, loaded via PartnerPage API.

This scraper uses two strategies:
1. Parse the SSR HTML for partner cards visible in the initial render
2. Attempt the PartnerPage.io API for additional partners

Individual partner profile pages live at:
  https://zapier.com/partnerdirectory/{slug}

Estimated yield: 380+ solution partners
"""

from __future__ import annotations

import json
import re
from typing import Iterator, Optional
from datetime import datetime

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Zapier's PartnerPage directory UUID (from NUXT data)
DIRECTORY_UUID = "66b48503-a95e-48ef-b481-08a869c8d437"

# PartnerPage API patterns to try for paginated data
PARTNERPAGE_API_PATTERNS = [
    "https://app.partnerpage.io/api/v1/directories/{dir_id}/partners",
    "https://api.partnerpage.io/directories/{dir_id}/partners",
    "https://app.partnerpage.io/api/directories/{dir_id}/partners",
    "https://zapier.partnerpage.io/api/partners",
    "https://zapier.partnerpage.io/api/v1/partners",
]

# Tier values
TIERS = {"Platinum", "Gold", "Silver", "Bronze"}

# Maximum partner profile pages to fetch individually
MAX_PROFILE_FETCHES = 500


class Scraper(BaseScraper):
    SOURCE_NAME = "zapier_partners"
    BASE_URL = "https://zapier.com/partnerdirectory"
    REQUESTS_PER_MINUTE = 6

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_names: set[str] = set()
        self._partner_slugs: list[str] = []

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run() for multi-strategy approach."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse partner data from a page HTML response."""
        return self._parse_ssr_html(html, url)

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Multi-strategy scraping: SSR HTML + API + profile pages."""
        self.logger.info("Starting %s scraper", self.SOURCE_NAME)
        contacts_yielded = 0

        # Strategy 1: Parse SSR HTML from the main directory page
        self.logger.info("Strategy 1: Fetching SSR HTML from %s", self.BASE_URL)
        html = self.fetch_page(self.BASE_URL)
        if html:
            contacts = self._parse_ssr_html(html, self.BASE_URL)
            self.logger.info("SSR HTML yielded %d partners", len(contacts))

            for contact in contacts:
                contact.source_platform = self.SOURCE_NAME
                contact.scraped_at = datetime.now().isoformat()
                contact.email = contact.clean_email()

                if contact.is_valid():
                    self.stats["contacts_valid"] += 1
                    contacts_yielded += 1
                    yield contact

                    if max_contacts and contacts_yielded >= max_contacts:
                        self.logger.info("Reached max_contacts=%d", max_contacts)
                        return

                self.stats["contacts_found"] += 1

        # Strategy 2: Try PartnerPage API for additional partners
        self.logger.info("Strategy 2: Attempting PartnerPage API...")
        api_endpoint = self._discover_partnerpage_api()
        if api_endpoint:
            self.logger.info("PartnerPage API found: %s", api_endpoint)
            for contact in self._fetch_via_api(api_endpoint, max_contacts):
                contact.source_platform = self.SOURCE_NAME
                contact.scraped_at = datetime.now().isoformat()
                contact.email = contact.clean_email()

                if contact.is_valid():
                    self.stats["contacts_valid"] += 1
                    contacts_yielded += 1
                    yield contact

                    if max_contacts and contacts_yielded >= max_contacts:
                        return

                self.stats["contacts_found"] += 1
        else:
            self.logger.info("PartnerPage API not accessible, trying profile pages...")

        # Strategy 3: Fetch individual partner profile pages
        # Use slugs discovered from the web search or known partners
        if self._partner_slugs:
            self.logger.info(
                "Strategy 3: Fetching %d known profile pages...",
                len(self._partner_slugs),
            )
            pages_fetched = 0
            for slug in self._partner_slugs:
                if pages_fetched >= MAX_PROFILE_FETCHES:
                    break
                if max_contacts and contacts_yielded >= max_contacts:
                    break

                profile_url = f"{self.BASE_URL}/{slug}"
                profile_html = self.fetch_page(profile_url)
                if not profile_html:
                    continue

                pages_fetched += 1
                profile_contacts = self._parse_profile_page(profile_html, profile_url)

                for contact in profile_contacts:
                    contact.source_platform = self.SOURCE_NAME
                    contact.scraped_at = datetime.now().isoformat()
                    contact.email = contact.clean_email()

                    if contact.is_valid():
                        self.stats["contacts_valid"] += 1
                        contacts_yielded += 1
                        yield contact

                    self.stats["contacts_found"] += 1

        self.logger.info(
            "Scraper complete: %d contacts yielded. Stats: %s",
            contacts_yielded, self.stats,
        )

    def _parse_ssr_html(self, html: str, url: str) -> list[ScrapedContact]:
        """Parse partner cards from the server-rendered HTML.

        The SSR HTML contains partner cards with:
        - Logo images where alt text = partner name
        - Tier badge images (Platinum, Gold, Silver, Bronze)
        - Description text
        - Rating number (e.g., "5.0")
        - Review count in parens (e.g., "(462)")
        """
        soup = self.parse_html(html)
        contacts = []

        # Extract partner names from logo images
        partner_names = []
        for img in soup.find_all("img", alt=True):
            alt = (img.get("alt") or "").strip()
            # Filter out non-partner images
            if (
                alt
                and len(alt) > 2
                and len(alt) < 120
                and alt.lower() not in ("platinum", "gold", "silver", "bronze")
                and not alt.endswith(" logo")
                and "graphic" not in alt.lower()
                and "zapier" not in alt.lower()
                and "our team" != alt.lower()
                and "partnerpage" not in alt.lower()
            ):
                if alt not in partner_names:
                    partner_names.append(alt)

        if not partner_names:
            return contacts

        # Now extract the full text content to match names with descriptions
        # The HTML structure alternates: tier badge, partner name, description, rating, review count
        body = soup.find("body")
        if not body:
            return contacts

        # Strip scripts and styles for text extraction
        for tag in body.find_all(["script", "style", "noscript"]):
            tag.decompose()

        full_text = body.get_text(separator="\n", strip=True)
        lines = [l.strip() for l in full_text.split("\n") if l.strip()]

        # Parse partner cards from the text
        # Pattern: [Tier] "Accepting new clients" [Name] [Description] [Rating] ([ReviewCount])
        i = 0
        current_tier = ""
        while i < len(lines):
            line = lines[i]

            # Detect tier
            if line in TIERS:
                current_tier = line
                i += 1
                continue

            # Skip "Accepting new clients" marker
            if line == "Accepting new clients":
                i += 1
                continue

            # Check if this line is a partner name
            if line in partner_names and line not in self._seen_names:
                partner_name = line
                self._seen_names.add(partner_name)

                # Next line(s) should be the description
                description = ""
                rating = ""
                reviews = ""
                i += 1

                if i < len(lines):
                    # The next line is usually the description (long text)
                    desc_line = lines[i]
                    # Skip if it's a tier or rating
                    if desc_line not in TIERS and not re.match(r"^\d\.\d$", desc_line):
                        description = desc_line
                        i += 1

                # Look for rating pattern (N.N)
                while i < len(lines):
                    if re.match(r"^\d\.\d$", lines[i]):
                        rating = lines[i]
                        i += 1
                        # Next should be review count in parens
                        if i < len(lines) and re.match(r"^\(\d+\)$", lines[i]):
                            reviews = lines[i].strip("()")
                            i += 1
                        break
                    elif lines[i] in TIERS:
                        break
                    else:
                        # Might be continuation of description
                        i += 1

                # Extract website URLs from description
                website = ""
                linkedin = ""
                websites_found = re.findall(
                    r"https?://(?:www\.)?([a-zA-Z0-9\-]+\.(?:com|io|co|net|org|digital|tech|agency)(?:/\S*)?)",
                    description,
                )
                for w in websites_found:
                    full_url = f"https://{w}"
                    w_lower = w.lower()
                    if "linkedin.com" in w_lower:
                        linkedin = full_url
                    elif (
                        "zapier.com" not in w_lower
                        and "calendly.com" not in w_lower
                        and "partnerpage" not in w_lower
                        and "fiverr" not in w_lower
                        and "upwork" not in w_lower
                        and not website
                    ):
                        website = full_url

                # Generate slug from name
                slug = re.sub(r"[^a-z0-9]+", "-", partner_name.lower()).strip("-")
                if slug and slug not in [s for s in self._partner_slugs]:
                    self._partner_slugs.append(slug)

                # Build bio
                bio_parts = []
                if description:
                    # Truncate to reasonable size
                    bio_parts.append(description[:500])
                if current_tier:
                    bio_parts.append(f"Zapier {current_tier} Solution Partner")
                if rating and reviews:
                    bio_parts.append(f"Rating: {rating}/5 ({reviews} reviews)")

                bio = " | ".join(bio_parts) if bio_parts else f"{partner_name} - Zapier Solution Partner"

                contact = ScrapedContact(
                    name=partner_name,
                    company=partner_name,
                    website=website,
                    linkedin=linkedin,
                    bio=bio[:2000],
                    source_url=f"{self.BASE_URL}/{slug}" if slug else url,
                    source_category="zapier_solution_partner",
                    raw_data={
                        "tier": current_tier,
                        "rating": rating,
                        "reviews": reviews,
                        "slug": slug,
                    },
                )
                contacts.append(contact)
                continue

            i += 1

        return contacts

    def _discover_partnerpage_api(self) -> Optional[str]:
        """Try PartnerPage API endpoint patterns."""
        for pattern in PARTNERPAGE_API_PATTERNS:
            endpoint = pattern.format(dir_id=DIRECTORY_UUID)
            try:
                if self.rate_limiter:
                    self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

                resp = self.session.get(
                    endpoint,
                    params={"limit": 5, "offset": 0},
                    timeout=15,
                    headers={
                        "Accept": "application/json",
                        "Referer": "https://zapier.com/partnerdirectory",
                    },
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, (dict, list)):
                        if isinstance(data, list) and len(data) > 0:
                            return endpoint
                        elif isinstance(data, dict) and (
                            "results" in data
                            or "partners" in data
                            or "data" in data
                        ):
                            return endpoint
            except Exception as exc:
                self.logger.debug("API probe failed: %s: %s", endpoint, exc)
                continue

        return None

    def _fetch_via_api(self, endpoint: str, max_contacts: int = 0) -> Iterator[ScrapedContact]:
        """Fetch partners via PartnerPage API."""
        offset = 0
        limit = 20
        total_fetched = 0

        while True:
            try:
                if self.rate_limiter:
                    self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

                resp = self.session.get(
                    endpoint,
                    params={"limit": limit, "offset": offset},
                    timeout=30,
                    headers={
                        "Accept": "application/json",
                        "Referer": "https://zapier.com/partnerdirectory",
                    },
                )
                if resp.status_code != 200:
                    self.logger.warning("API returned %d at offset %d", resp.status_code, offset)
                    break

                data = resp.json()
                self.stats["pages_scraped"] += 1

                # Handle various response formats
                partners = (
                    data.get("results", [])
                    or data.get("partners", [])
                    or data.get("data", [])
                    or (data if isinstance(data, list) else [])
                )

                if not partners:
                    break

                for partner in partners:
                    contact = self._api_partner_to_contact(partner)
                    if contact:
                        yield contact
                        total_fetched += 1

                offset += limit

                total = data.get("count", data.get("total", 0))
                if total and offset >= total:
                    break

                if max_contacts and total_fetched >= max_contacts:
                    break

            except Exception as exc:
                self.logger.error("API fetch error at offset %d: %s", offset, exc)
                self.stats["errors"] += 1
                break

    def _api_partner_to_contact(self, partner: dict) -> Optional[ScrapedContact]:
        """Convert a PartnerPage API partner object to ScrapedContact."""
        name = (
            partner.get("name")
            or partner.get("company_name")
            or partner.get("title")
            or ""
        ).strip()

        if not name or len(name) < 2:
            return None

        if name in self._seen_names:
            return None
        self._seen_names.add(name)

        description = (partner.get("description") or partner.get("bio") or "").strip()
        website = (partner.get("website") or partner.get("url") or "").strip()
        email = (partner.get("email") or partner.get("contact_email") or "").strip()

        # Location
        location = partner.get("location", {})
        if isinstance(location, dict):
            city = (location.get("city") or "").strip()
            country = (location.get("country") or "").strip()
        elif isinstance(location, str):
            city = location
            country = ""
        else:
            city = country = ""

        tier = (partner.get("tier", {}).get("name", "") if isinstance(partner.get("tier"), dict)
                else str(partner.get("tier", ""))).strip()

        rating = str(partner.get("rating", partner.get("average_rating", ""))).strip()
        reviews = str(partner.get("number_of_reviews", partner.get("review_count", ""))).strip()

        slug = (partner.get("custom_url") or partner.get("slug") or "").strip()
        if not slug:
            slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")

        # Build bio
        bio_parts = []
        if description:
            bio_parts.append(description[:500])
        if tier:
            bio_parts.append(f"Zapier {tier} Solution Partner")
        loc_str = ", ".join(filter(None, [city, country]))
        if loc_str:
            bio_parts.append(f"Location: {loc_str}")
        if rating and reviews and rating != "0":
            bio_parts.append(f"Rating: {rating}/5 ({reviews} reviews)")

        bio = " | ".join(bio_parts) if bio_parts else f"{name} - Zapier Solution Partner"

        return ScrapedContact(
            name=name,
            email=email,
            company=name,
            website=website,
            bio=bio[:2000],
            source_url=f"{self.BASE_URL}/{slug}" if slug else self.BASE_URL,
            source_category="zapier_solution_partner",
            raw_data={
                "tier": tier,
                "rating": rating,
                "reviews": reviews,
                "slug": slug,
                "city": city,
                "country": country,
            },
        )

    def _parse_profile_page(self, html: str, url: str) -> list[ScrapedContact]:
        """Parse an individual partner profile page for contact data."""
        soup = self.parse_html(html)

        # Extract partner name from the page
        name = ""
        # Try og:title
        og_title = soup.find("meta", property="og:title")
        if og_title:
            title_text = (og_title.get("content") or "").strip()
            # Remove " - Zapier Solutions Partner Directory" suffix
            name = re.sub(r"\s*[-|]\s*Zapier.*$", "", title_text).strip()

        if not name:
            h1 = soup.find("h1")
            if h1:
                name = h1.get_text(strip=True)

        if not name or len(name) < 2 or name in self._seen_names:
            return []

        self._seen_names.add(name)

        # Description
        description = ""
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            description = (meta_desc.get("content") or "").strip()[:1000]

        # Website - look for external links
        website = ""
        linkedin = ""
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            href_lower = href.lower()
            if any(domain in href_lower for domain in [
                "zapier.com", "partnerpage.io", "facebook.com",
                "twitter.com", "instagram.com", "youtube.com",
                "calendly.com",
            ]):
                continue

            if "linkedin.com" in href_lower:
                if not linkedin:
                    linkedin = href
            elif href.startswith("http") and not website:
                website = href

        # Email
        email = ""
        emails = self.extract_emails(html)
        for e in emails:
            if "zapier" not in e.lower() and "partnerpage" not in e.lower():
                email = e
                break

        bio = description if description else f"{name} - Zapier Solution Partner"

        return [ScrapedContact(
            name=name,
            email=email,
            company=name,
            website=website,
            linkedin=linkedin,
            bio=bio[:2000],
            source_url=url,
            source_category="zapier_solution_partner",
        )]
