"""
500 Global Portfolio Scraper

Fetches portfolio company data from 500 Global's companies page.
Source: https://500.co/companies

The 500.co site is a Next.js app using Builder.io as its CMS. The
CompaniesTable component loads data from a JSON API at /api/startups.
This returns all 2,200+ portfolio companies in a single response
without pagination.

Data includes:
- Company name, website, description (oneLiner)
- LinkedIn URL
- Founders/team positions
- Business model, stage, industries
- Country and region of operation
- Investment batch information

Overrides run() because data is fetched via a single JSON API endpoint,
not by crawling HTML pages.
"""

import json
from typing import Iterator, Optional
from datetime import datetime

from scripts.sourcing.base import BaseScraper, ScrapedContact


API_URL = "https://500.co/api/startups"


class Scraper(BaseScraper):
    SOURCE_NAME = "fivehundred_global"
    BASE_URL = "https://500.co/companies"
    REQUESTS_PER_MINUTE = 5  # Single API call, be polite

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.session.headers.update({
            "Accept": "application/json",
        })

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run() for JSON API fetch."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse the /api/startups JSON response into ScrapedContacts."""
        try:
            data = json.loads(html)
        except json.JSONDecodeError:
            self.logger.warning("Failed to parse JSON from %s", url)
            return []

        companies = data.get("res", [])
        if not companies:
            self.logger.warning("No companies in API response")
            return []

        contacts = []
        seen_ids = set()

        for entry in companies:
            org = entry.get("organization") or {}
            org_id = org.get("id")

            # Skip duplicates
            if org_id and org_id in seen_ids:
                continue
            if org_id:
                seen_ids.add(org_id)

            # Get the business name (preferred) or organization name
            name = (org.get("businessName") or "").strip()
            if not name:
                name = (org.get("name") or "").strip()
            if not name:
                continue

            website = (org.get("companyUrl") or "").strip()
            if website and not website.startswith("http"):
                website = "https://" + website

            linkedin = (org.get("companyLinkedIn") or "").strip()
            if linkedin and not linkedin.startswith("http"):
                linkedin = "https://" + linkedin

            one_liner = (entry.get("oneLiner") or "").strip()

            # Extract founders/team from positions
            positions = org.get("positions") or []
            founders = []
            for pos in positions:
                person = pos.get("person") or {}
                first = (person.get("firstName") or "").strip()
                last = (person.get("lastName") or "").strip()
                if first and last:
                    # Some records have first name repeated in last name
                    if last.lower().startswith(first.lower()):
                        founders.append(last)
                    else:
                        founders.append(f"{first} {last}")
                elif first:
                    founders.append(first)
                elif last:
                    founders.append(last)
            founders_str = ", ".join(founders) if founders else ""

            # Business model, stage, industries
            biz_model = ""
            model_data = entry.get("businessModel") or {}
            if model_data:
                biz_model = (model_data.get("name") or "").strip()

            stage = ""
            stage_data = entry.get("stage") or {}
            if stage_data:
                stage = (stage_data.get("name") or "").strip()

            industries = []
            for ind in (entry.get("industries") or []):
                ind_name = (ind.get("name") or "").strip()
                if ind_name:
                    industries.append(ind_name)

            # Location
            country_data = org.get("countryOfOperation") or {}
            country = (country_data.get("name") or "").strip()
            region_data = org.get("regionOfOperation") or {}
            region = (region_data.get("name") or "").strip()

            # Batch/investment info
            batches = entry.get("batches") or []
            batch_names = []
            for batch in batches:
                batch_name = (batch.get("name") or "").strip()
                if batch_name:
                    batch_names.append(batch_name)

            investments = entry.get("investments") or []
            tenant_data = entry.get("tenant") or []
            if isinstance(tenant_data, list):
                tenant = ", ".join(
                    t.get("slug", "") for t in tenant_data if isinstance(t, dict)
                )
            elif isinstance(tenant_data, str):
                tenant = tenant_data.strip()
            else:
                tenant = str(tenant_data)

            # Alternative names
            alt_name = (org.get("alternativeName") or "").strip()

            # Image URL
            image_url = (org.get("imageUrl") or "").strip()

            # Build bio
            bio_parts = []
            if one_liner:
                bio_parts.append(one_liner)
            if founders_str:
                bio_parts.append(f"Founders: {founders_str}")
            if stage:
                bio_parts.append(f"Stage: {stage}")
            if biz_model:
                bio_parts.append(f"Model: {biz_model}")
            if industries:
                bio_parts.append(f"Industries: {', '.join(industries)}")
            if country:
                bio_parts.append(f"Country: {country}")
            if batch_names:
                bio_parts.append(f"Batch: {', '.join(batch_names)}")
            bio_parts.append("Portfolio: 500 Global")
            bio = " | ".join(bio_parts)

            contact = ScrapedContact(
                name=name,
                email="",
                company=name,
                website=website,
                linkedin=linkedin,
                phone="",
                bio=bio,
                source_category="accelerator_portfolio",
                raw_data={
                    "org_id": str(org_id or ""),
                    "startup_id": str(entry.get("id", "")),
                    "alt_name": alt_name,
                    "stage": stage,
                    "business_model": biz_model,
                    "industries": industries,
                    "country": country,
                    "region": region,
                    "tenant": tenant,
                    "batches": batch_names,
                    "founders": founders_str,
                    "image_url": image_url,
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
        """Fetch all 500 Global portfolio companies via /api/startups.

        The API returns all companies in a single JSON response.
        Yields ScrapedContact objects compatible with the runner.
        """
        self.logger.info("Starting %s scraper", self.SOURCE_NAME)

        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        try:
            resp = self.session.get(API_URL, timeout=60)
            resp.raise_for_status()
            html = resp.text
            self.stats["pages_scraped"] += 1
        except Exception as exc:
            self.stats["errors"] += 1
            self.logger.error("API fetch failed: %s", exc)
            return

        self.logger.info(
            "Fetched %d bytes from %s", len(html), API_URL
        )

        try:
            contacts = self.scrape_page(API_URL, html)
        except Exception as exc:
            self.stats["errors"] += 1
            self.logger.error("Parse error: %s", exc)
            return

        self.logger.info("Parsed %d companies", len(contacts))

        contacts_yielded = 0
        for contact in contacts:
            contact.source_platform = self.SOURCE_NAME
            contact.source_url = self.BASE_URL
            contact.scraped_at = datetime.now().isoformat()
            contact.email = contact.clean_email()

            if contact.is_valid():
                self.stats["contacts_valid"] += 1
                contacts_yielded += 1
                yield contact

                if max_contacts and contacts_yielded >= max_contacts:
                    self.logger.info(
                        "Reached max_contacts=%d", max_contacts
                    )
                    return

            self.stats["contacts_found"] += 1

        self.logger.info("Scraper complete: %s", self.stats)
