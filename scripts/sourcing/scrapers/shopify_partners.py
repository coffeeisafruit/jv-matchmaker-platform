"""
Shopify Partner Directory scraper.

Scrapes https://www.shopify.com/partners/directory for Shopify
expert partners (agencies, consultants, freelancers).

The directory renders server-side HTML with partner cards on listing
pages. Pagination via ``?page=N`` query param, 16 partners per page.

Individual profiles at ``/partners/directory/partner/{slug}`` contain
rich data: company name, description, website, email, phone, location,
services, partner tier, reviews, and pricing.

Estimated yield: 4,600+ partners (directory reports 4,654 total)
"""

from __future__ import annotations

import re
from typing import Iterator, Optional
from urllib.parse import urljoin

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Service category paths for broader coverage
SERVICE_CATEGORIES = [
    "services",           # All services (default)
    "services/store-design",
    "services/theme-customization",
    "services/custom-apps",
    "services/store-migration",
    "services/troubleshooting",
    "services/seo",
    "services/content-creation",
    "services/paid-marketing",
    "services/email-marketing",
    "services/social-media-marketing",
]

# Max pages per category listing (16 partners per page, ~291 pages for "services")
MAX_PAGES_PER_CATEGORY = 300

# Partner tier names for reference
PARTNER_TIERS = ["Select", "Plus", "Premier", "Platinum"]


class Scraper(BaseScraper):
    SOURCE_NAME = "shopify_partners"
    BASE_URL = "https://www.shopify.com/partners/directory"
    REQUESTS_PER_MINUTE = 6

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._seen_slugs: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield paginated listing URLs.

        Starts with the main /services listing which covers all
        partners. Additional category-specific URLs are skipped
        to avoid duplicates — the main listing is comprehensive.
        """
        for page in range(1, MAX_PAGES_PER_CATEGORY + 1):
            yield f"{self.BASE_URL}/services?page={page}"

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse partner listing page and fetch individual profiles."""
        soup = self.parse_html(html)
        contacts = []

        # Find partner card links
        # Pattern: /partners/directory/partner/{slug}
        profile_slugs = {}
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            match = re.search(r"/partners/directory/partner/([\w\-]+)", href)
            if match:
                slug = match.group(1)
                if slug not in self._seen_slugs:
                    full_url = urljoin("https://www.shopify.com", href)
                    # Extract card-level data (name, location, rating)
                    card_data = self._extract_card_data(a, soup)
                    profile_slugs[slug] = {
                        "url": full_url,
                        "card": card_data,
                    }

        if not profile_slugs:
            # Check if this is an empty page (pagination end)
            # Look for "no results" indicators
            page_text = soup.get_text(strip=True).lower()
            if "no partners found" in page_text or "no results" in page_text:
                self.logger.info("No more partners at %s", url)
            return []

        # Fetch each partner profile page for rich data
        for slug, info in profile_slugs.items():
            if slug in self._seen_slugs:
                continue
            self._seen_slugs.add(slug)

            profile_url = info["url"]
            card_data = info["card"]

            # Fetch the individual profile page
            profile_html = self.fetch_page(profile_url)
            if profile_html:
                contact = self._parse_profile(profile_url, profile_html, card_data)
            else:
                # Fall back to card-level data only
                contact = self._card_to_contact(card_data, profile_url)

            if contact:
                contacts.append(contact)

        return contacts

    def _extract_card_data(self, link_elem, soup) -> dict:
        """Extract basic partner data from a listing card element."""
        data = {
            "name": "",
            "location": "",
            "rating": "",
            "reviews": "",
            "starting_price": "",
            "tier": "",
        }

        # The link text is usually the partner name
        name_text = link_elem.get_text(strip=True)
        if name_text and len(name_text) < 150:
            data["name"] = name_text

        # Try to find the card container
        card = link_elem.find_parent(
            ["div", "article", "li", "section"],
        )
        if not card:
            return data

        card_text = card.get_text(separator="\n", strip=True)
        lines = [l.strip() for l in card_text.split("\n") if l.strip()]

        # Extract location (pattern: "City, Country")
        for line in lines:
            if "," in line and len(line) < 60:
                parts = line.split(",")
                if len(parts) == 2:
                    city = parts[0].strip()
                    country = parts[1].strip()
                    if city and country and city[0].isupper():
                        data["location"] = f"{city}, {country}"
                        break

        # Extract rating (pattern: "N.N" near "reviews")
        rating_match = re.search(r"(\d\.\d)\s*(?:out of 5|stars?)?", card_text)
        if rating_match:
            data["rating"] = rating_match.group(1)

        # Extract review count
        review_match = re.search(r"(\d[\d,]*)\s*reviews?", card_text, re.I)
        if review_match:
            data["reviews"] = review_match.group(1)

        # Extract price
        price_match = re.search(r"(?:Starting\s+(?:from|at)\s+)?\$[\d,]+", card_text, re.I)
        if price_match:
            data["starting_price"] = price_match.group(0)

        # Extract tier
        for tier in PARTNER_TIERS:
            if tier.lower() in card_text.lower():
                data["tier"] = tier
                break

        return data

    def _parse_profile(self, url: str, html: str, card_data: dict) -> Optional[ScrapedContact]:
        """Parse a full partner profile page for rich contact data."""
        soup = self.parse_html(html)
        page_text = html

        # Company name — h1 or og:title
        name = ""
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        if not name:
            og_title = soup.find("meta", property="og:title")
            if og_title:
                name = (og_title.get("content") or "").split("|")[0].split("-")[0].strip()

        if not name:
            name = card_data.get("name", "")

        if not name or len(name) < 2:
            return None

        # Description / bio
        bio = ""
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            bio = (meta_desc.get("content") or "").strip()[:1000]

        if not bio:
            og_desc = soup.find("meta", property="og:description")
            if og_desc:
                bio = (og_desc.get("content") or "").strip()[:1000]

        # Look for longer description in page body
        # Shopify profiles often have a detailed "About" section
        for heading in soup.find_all(["h2", "h3"]):
            if "about" in heading.get_text(strip=True).lower():
                sibling = heading.find_next_sibling(["p", "div"])
                if sibling:
                    about_text = sibling.get_text(strip=True)
                    if len(about_text) > len(bio):
                        bio = about_text[:1000]
                break

        # Website — look for external website link
        website = ""
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            link_text = a.get_text(strip=True).lower()
            href_lower = href.lower()

            # Skip Shopify's own links and social media
            if any(domain in href_lower for domain in [
                "shopify.com", "facebook.com", "twitter.com",
                "instagram.com", "youtube.com", "tiktok.com",
                "pinterest.com", "x.com",
            ]):
                continue

            # Prioritize links explicitly labeled as "website" or "visit site"
            if any(kw in link_text for kw in ["website", "visit", "site", "web"]):
                if href.startswith("http"):
                    website = href
                    break

            # Also capture LinkedIn
            if "linkedin.com" in href_lower:
                continue  # Handle separately below

            # Any external http link could be the website
            if href.startswith("http") and not website:
                website = href

        # LinkedIn
        linkedin = ""
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if "linkedin.com" in href.lower():
                linkedin = href
                break

        # Email — look for mailto: links and email patterns
        email = ""
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if href.startswith("mailto:"):
                email = href.replace("mailto:", "").split("?")[0].strip()
                break

        if not email:
            emails = self.extract_emails(page_text)
            # Filter out Shopify-related emails
            for e in emails:
                if "shopify" not in e.lower() and "example" not in e.lower():
                    email = e
                    break

        # Phone — look for tel: links
        phone = ""
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if href.startswith("tel:"):
                phone = href.replace("tel:", "").strip()
                break

        if not phone:
            phone_match = re.search(
                r"(?:1[-.\s]?)?(?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4}",
                page_text,
            )
            if phone_match:
                phone = phone_match.group(0)

        # Location
        location = card_data.get("location", "")
        if not location:
            # Look for location in meta or structured data
            for elem in soup.find_all(string=re.compile(r"[A-Z][a-z]+,\s+[A-Z][a-z]+")):
                text = elem.strip()
                if "," in text and len(text) < 60:
                    location = text
                    break

        # Services
        services = []
        for heading in soup.find_all(["h2", "h3"]):
            if "service" in heading.get_text(strip=True).lower():
                container = heading.find_next_sibling(["ul", "div"])
                if container:
                    for item in container.find_all(["li", "span", "a"]):
                        svc = item.get_text(strip=True)
                        if svc and len(svc) < 80:
                            services.append(svc)
                break

        # Tier
        tier = card_data.get("tier", "")
        if not tier:
            for t in PARTNER_TIERS:
                if t.lower() in page_text.lower():
                    tier = t
                    break

        # Rating and reviews
        rating = card_data.get("rating", "")
        reviews = card_data.get("reviews", "")
        starting_price = card_data.get("starting_price", "")

        # Build enriched bio
        bio_parts = []
        if bio:
            bio_parts.append(bio)
        if tier:
            bio_parts.append(f"Shopify {tier} Partner")
        if location:
            bio_parts.append(f"Location: {location}")
        if rating and reviews:
            bio_parts.append(f"Rating: {rating}/5 ({reviews} reviews)")
        if starting_price:
            bio_parts.append(f"Starting at {starting_price}")
        if services:
            bio_parts.append(f"Services: {', '.join(services[:5])}")

        final_bio = " | ".join(bio_parts) if bio_parts else f"{name} - Shopify Partner"

        contact = ScrapedContact(
            name=name,
            email=email,
            company=name,
            website=website,
            linkedin=linkedin,
            phone=phone,
            bio=final_bio[:2000],
            source_url=url,
            source_category="shopify_partner",
            raw_data={
                "tier": tier,
                "rating": rating,
                "reviews": reviews,
                "starting_price": starting_price,
                "location": location,
                "services": services,
                "slug": url.rstrip("/").split("/")[-1],
            },
        )

        return contact

    def _card_to_contact(self, card_data: dict, url: str) -> Optional[ScrapedContact]:
        """Create a contact from card-level data only (when profile fetch fails)."""
        name = card_data.get("name", "")
        if not name or len(name) < 2:
            return None

        location = card_data.get("location", "")
        tier = card_data.get("tier", "")
        rating = card_data.get("rating", "")
        reviews = card_data.get("reviews", "")
        starting_price = card_data.get("starting_price", "")

        bio_parts = [name]
        if tier:
            bio_parts.append(f"Shopify {tier} Partner")
        if location:
            bio_parts.append(location)
        if rating and reviews:
            bio_parts.append(f"{rating}/5 ({reviews} reviews)")
        if starting_price:
            bio_parts.append(starting_price)

        return ScrapedContact(
            name=name,
            company=name,
            website=url,  # Use the Shopify profile as website
            bio=" | ".join(bio_parts),
            source_url=url,
            source_category="shopify_partner",
            raw_data=card_data,
        )
