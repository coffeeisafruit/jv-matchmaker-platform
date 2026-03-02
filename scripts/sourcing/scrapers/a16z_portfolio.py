"""
Andreessen Horowitz (a16z) Portfolio Scraper

Fetches portfolio company data from a16z's portfolio page.
Source: https://a16z.com/portfolio/

The a16z portfolio page is a WordPress site with a Vue.js frontend.
Company data is embedded as HTML-encoded JSON in a data-json attribute
on a div.portfolio-app element. This gives us all 800+ portfolio
companies in a single page load without needing pagination.

Data includes:
- Company name, website, description
- Founders list
- Investment stage and categories
- Social links (LinkedIn, Twitter)
- Exit/IPO status

Uses the default BaseScraper.run() flow via generate_urls/scrape_page,
since all data is available in a single HTML page.
"""

import html as html_module
import json
from typing import Iterator

from scripts.sourcing.base import BaseScraper, ScrapedContact


class Scraper(BaseScraper):
    SOURCE_NAME = "a16z_portfolio"
    BASE_URL = "https://a16z.com/portfolio/"
    REQUESTS_PER_MINUTE = 5  # Only need one request, be polite

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield the single portfolio page URL.

        All company data is embedded in this one page.
        """
        yield self.BASE_URL

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse the a16z portfolio page HTML for embedded company JSON.

        The page contains a <div class="portfolio-app" data-json="...">
        element where the data-json attribute holds HTML-encoded JSON
        with all portfolio companies.
        """
        soup = self.parse_html(html)

        # Find the Vue app mount point with embedded JSON
        app_div = soup.find("div", class_="portfolio-app")
        if not app_div:
            # Fallback: find any element with data-json containing companies
            app_div = soup.find(attrs={"data-json": True})

        if not app_div or not app_div.get("data-json"):
            self.logger.error(
                "Could not find portfolio-app div with data-json attribute"
            )
            return []

        # Decode HTML entities and parse JSON
        raw_json = html_module.unescape(app_div["data-json"])
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            self.logger.error("Failed to parse portfolio JSON: %s", exc)
            return []

        contacts = []

        # Process regular companies
        companies = data.get("companies", [])
        self.logger.info("Found %d companies in portfolio data", len(companies))
        for company in companies:
            contact = self._parse_company(company)
            if contact:
                contacts.append(contact)

        # Process featured companies (have a nested structure)
        featured = data.get("featuredCompanies", [])
        self.logger.info("Found %d featured companies", len(featured))
        for entry in featured:
            company = entry.get("company", {})
            if company:
                contact = self._parse_company(company)
                if contact:
                    contacts.append(contact)

        return contacts

    def _parse_company(self, company: dict) -> ScrapedContact | None:
        """Parse a single company dict into a ScrapedContact."""
        name = (company.get("post_title") or "").strip()
        if not name:
            name = (company.get("a16z_display_name") or "").strip()
        if not name:
            return None

        display_name = (
            company.get("a16z_display_name")
            or company.get("a16z_company_name")
            or name
        )

        website = (company.get("company_url") or "").strip()
        if website and not website.startswith("http"):
            website = "https://" + website

        description = (company.get("website_description") or "").strip()
        founders = (company.get("founders_list") or "").strip()

        # Extract social links
        linkedin = ""
        twitter = ""
        socials = company.get("socials") or []
        for social in socials:
            social_url = (social.get("url") or "").strip()
            icon_info = social.get("constants.select.social_icons", {})
            icon_value = (icon_info.get("value") or "").lower() if icon_info else ""
            label = (icon_info.get("label") or "").lower() if icon_info else ""

            if "linkedin" in icon_value or "linkedin" in label:
                linkedin = social_url
            elif "twitter" in icon_value or label in ("x", "twitter"):
                twitter = social_url

        # Determine stage and status
        stages = company.get("stage") or []
        initial_stage = (company.get("website_initial_stage") or "").strip()
        current_status = (
            company.get("website_current_status") or ""
        ).strip()
        categories = (company.get("website_categories") or "").strip()
        supercategory = (
            company.get("website_supercategory") or ""
        ).strip()

        # Determine if it's an exit
        is_exit = any(
            s in ("m&a", "ipo", "spac")
            for s in stages
        )
        acquirer = (company.get("acquirer") or "").strip()
        ticker = (company.get("ticker_symbol") or "").strip()

        # Build bio
        bio_parts = []
        if description:
            bio_parts.append(description)
        if founders:
            bio_parts.append(f"Founders: {founders}")
        if supercategory:
            bio_parts.append(f"Category: {supercategory}")
        if categories and categories != supercategory:
            bio_parts.append(f"Sectors: {categories}")
        if initial_stage:
            bio_parts.append(f"Stage: {initial_stage}")
        if is_exit:
            exit_detail = "Exit"
            if ticker:
                exit_detail = f"IPO ({ticker})"
            elif acquirer:
                exit_detail = f"Acquired by {acquirer}"
            bio_parts.append(f"Status: {exit_detail}")
        elif current_status:
            bio_parts.append(f"Status: {current_status}")
        bio_parts.append("Portfolio: a16z (Andreessen Horowitz)")
        bio = " | ".join(bio_parts)

        # Logo URL
        logo = company.get("logo") or {}
        logo_url = ""
        if isinstance(logo, dict):
            logo_url = (logo.get("url") or "").strip()

        contact = ScrapedContact(
            name=display_name,
            email="",
            company=display_name,
            website=website,
            linkedin=linkedin,
            phone="",
            bio=bio,
            source_category="vc_portfolio",
            raw_data={
                "wp_id": str(company.get("ID", "")),
                "stages": stages,
                "initial_stage": initial_stage,
                "current_status": current_status,
                "founders": founders,
                "categories": categories,
                "supercategory": supercategory,
                "is_exit": is_exit,
                "acquirer": acquirer,
                "ticker": ticker,
                "twitter": twitter,
                "logo_url": logo_url,
                "filter_by": (company.get("filter_by") or ""),
            },
        )
        return contact
