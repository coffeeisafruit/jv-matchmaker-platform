"""
Greylock Partners Portfolio Scraper

Fetches portfolio companies from Greylock's WordPress-based website.
Company data is rendered as HTML cards with logo, name, description,
metadata (domain, stage, status, HQ), and social links.

Data includes:
- Company name, website, description/tagline
- Funding stage, status (Active/Acquired/Public)
- Headquarters location
- LinkedIn, Twitter URLs

Source: https://greylock.com/portfolio/
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


class Scraper(BaseScraper):
    SOURCE_NAME = "greylock_portfolio"
    BASE_URL = "https://greylock.com/portfolio/"
    REQUESTS_PER_MINUTE = 10

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_names: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run() for single-page scraping."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse Greylock portfolio page for company cards.

        Greylock's WordPress page uses a unique structure:
        - Company name is in the img alt text (e.g., "0x Labs Logo")
        - The <h2> tag contains the tagline/description, NOT the name
        - Metadata (Domain, First Partnered) appears in div text
        - External links for website, LinkedIn, Twitter
        """
        soup = self.parse_html(html)
        contacts = []

        # Strategy 1: Extract from img alt text paired with h2 taglines
        # Each company has a logo <img> with alt="CompanyName Logo"
        # followed by an <h2> with the tagline
        all_imgs = soup.find_all("img", alt=re.compile(r".+\s+Logo", re.I))

        for img in all_imgs:
            alt = (img.get("alt") or "").strip()
            if not alt:
                continue

            # Extract company name from alt text (remove "Logo" and variants)
            # Alt patterns: "CompanyName Logo", "CompanyName Logo Grey",
            # "CompanyName Logo F", "CompanyName Logo-F-1"
            name = re.sub(
                r"\s*[-_]?\s*(?:Logo|logo|Icon|icon)(?:\s+(?:Reverse|Grey|White|Dark|Light|F|F-\d+|Full|Small|Mark))?\s*[-_]?\s*\d*$",
                "", alt,
            ).strip()
            name = re.sub(r"\s+", " ", name).strip()

            if not name or len(name) < 2:
                continue

            # Skip non-company images (Greylock's own branding, UI elements)
            if name.lower() in ("greylock", "greylock partners", "portfolio",
                                "filter", "search", "menu", "close", "arrow",
                                "hero", "header", "footer", "bg", "background"):
                continue

            name_lower = name.lower()
            if name_lower in self._seen_names:
                continue
            self._seen_names.add(name_lower)

            # Get the parent container to find associated metadata
            parent = img.parent
            # Walk up to find a reasonable container
            for _ in range(5):
                if parent is None or parent.name in ("body", "html", "[document]"):
                    break
                # Check if this container has enough content to be a card
                text_len = len(parent.get_text(strip=True))
                if text_len > 50:
                    break
                parent = parent.parent

            description = ""
            website = ""
            linkedin = ""
            twitter = ""
            stage = ""
            domain = ""

            if parent:
                # h2 contains the tagline
                h2 = parent.find("h2")
                if h2:
                    description = h2.get_text(strip=True)

                # Extract external website link
                for a_tag in parent.find_all("a", href=True):
                    href = (a_tag.get("href") or "").strip()
                    if not href.startswith("http"):
                        continue
                    if "greylock.com" in href:
                        continue
                    if "linkedin.com" in href:
                        linkedin = href
                    elif "twitter.com" in href or "x.com" in href:
                        twitter = href
                    elif not website:
                        website = href

                # Extract metadata from text
                card_text = parent.get_text(" ", strip=True)

                stage_match = re.search(r"First Partnered:\s*([^|.\n]+)", card_text, re.I)
                if stage_match:
                    stage = stage_match.group(1).strip()

                domain_match = re.search(r"Domain:\s*([^|.\n]+)", card_text, re.I)
                if domain_match:
                    domain = domain_match.group(1).strip()

            # Build bio
            bio_parts = []
            if description:
                bio_parts.append(description)
            bio_parts.append("Greylock Partners portfolio company.")
            if domain:
                bio_parts.append(f"Domain: {domain}.")
            if stage:
                bio_parts.append(f"First partnered: {stage}.")
            bio = " ".join(bio_parts)

            contact = ScrapedContact(
                name=name,
                email="",
                company=name,
                website=website or self.BASE_URL,
                linkedin=linkedin,
                phone="",
                bio=bio,
                source_platform=self.SOURCE_NAME,
                source_url=self.BASE_URL,
                source_category="vc_portfolio",
                raw_data={
                    "domain": domain,
                    "stage": stage,
                    "twitter": twitter,
                    "tagline": description,
                    "vc_firm": "Greylock Partners",
                },
            )
            contacts.append(contact)

        return contacts

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Fetch Greylock portfolio companies from the main page.

        The portfolio page renders all companies on a single page
        with client-side filtering.
        """
        self.logger.info("Starting %s scraper", self.SOURCE_NAME)

        contacts_yielded = 0

        # Fetch main portfolio page
        html = self.fetch_page(self.BASE_URL)
        if not html:
            self.logger.error("Failed to fetch portfolio page")
            return

        try:
            contacts = self.scrape_page(self.BASE_URL, html)
        except Exception as exc:
            self.logger.error("Parse error: %s", exc)
            self.stats["errors"] += 1
            return

        self.logger.info("Found %d companies on portfolio page", len(contacts))

        for contact in contacts:
            contact.source_platform = self.SOURCE_NAME
            contact.source_url = self.BASE_URL
            contact.scraped_at = datetime.now().isoformat()
            contact.email = contact.clean_email()

            self.stats["contacts_found"] += 1

            if contact.is_valid():
                self.stats["contacts_valid"] += 1
                contacts_yielded += 1
                yield contact

                if max_contacts and contacts_yielded >= max_contacts:
                    self.logger.info("Reached max_contacts=%d", max_contacts)
                    return

        self.logger.info("Scraper complete: %s", self.stats)
