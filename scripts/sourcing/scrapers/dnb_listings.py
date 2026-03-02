"""
Dun & Bradstreet (D&B) Business Directory Scraper

Scrapes the D&B public business directory at
https://www.dnb.com/business-directory.html for company profiles.

Strategy:
  - Start from the business directory landing page
  - Browse industry categories (SIC/NAICS-based) and state directories
  - Parse company listing pages for basic info
  - Extract: company name, industry, location, website, DUNS number
  - Follow links to individual company pages for additional details

D&B URL patterns:
  - Directory root: /business-directory.html
  - Industry pages: /business-directory/{industry-slug}.html
  - State pages: /business-directory/{state-slug}.html
  - Company pages: /business-directory/company-profiles.{slug}.html

Also scrapes D&B's publicly accessible company search API if available.

Estimated yield: 30,000-100,000 companies.

Note: D&B has rate limiting and may block scrapers. The public directory
is intentionally accessible for discovery purposes, but aggressive
scraping will be blocked. Use conservative rate limiting.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# JV-relevant industry categories on D&B
INDUSTRY_CATEGORIES = [
    ("professional-services", "Professional Services"),
    ("management-consulting", "Management Consulting"),
    ("marketing-advertising", "Marketing & Advertising"),
    ("financial-services", "Financial Services"),
    ("information-technology", "Information Technology"),
    ("business-services", "Business Services"),
    ("accounting", "Accounting"),
    ("insurance", "Insurance"),
    ("real-estate", "Real Estate"),
    ("legal-services", "Legal Services"),
    ("human-resources", "Human Resources"),
    ("staffing", "Staffing & Recruiting"),
    ("engineering-services", "Engineering Services"),
    ("construction", "Construction"),
    ("manufacturing", "Manufacturing"),
    ("healthcare", "Healthcare"),
    ("education", "Education"),
    ("telecommunications", "Telecommunications"),
    ("transportation", "Transportation"),
    ("wholesale-trade", "Wholesale Trade"),
    ("retail-trade", "Retail Trade"),
    ("food-services", "Food Services"),
    ("media-entertainment", "Media & Entertainment"),
    ("software", "Software"),
    ("research-development", "Research & Development"),
    ("environmental-services", "Environmental Services"),
    ("energy", "Energy"),
    ("aerospace-defense", "Aerospace & Defense"),
    ("automotive", "Automotive"),
    ("pharmaceutical", "Pharmaceutical"),
]

# US state slugs for geographic browsing
US_STATES = [
    ("alabama", "AL"), ("alaska", "AK"), ("arizona", "AZ"), ("arkansas", "AR"),
    ("california", "CA"), ("colorado", "CO"), ("connecticut", "CT"), ("delaware", "DE"),
    ("florida", "FL"), ("georgia", "GA"), ("hawaii", "HI"), ("idaho", "ID"),
    ("illinois", "IL"), ("indiana", "IN"), ("iowa", "IA"), ("kansas", "KS"),
    ("kentucky", "KY"), ("louisiana", "LA"), ("maine", "ME"), ("maryland", "MD"),
    ("massachusetts", "MA"), ("michigan", "MI"), ("minnesota", "MN"), ("mississippi", "MS"),
    ("missouri", "MO"), ("montana", "MT"), ("nebraska", "NE"), ("nevada", "NV"),
    ("new-hampshire", "NH"), ("new-jersey", "NJ"), ("new-mexico", "NM"), ("new-york", "NY"),
    ("north-carolina", "NC"), ("north-dakota", "ND"), ("ohio", "OH"), ("oklahoma", "OK"),
    ("oregon", "OR"), ("pennsylvania", "PA"), ("rhode-island", "RI"), ("south-carolina", "SC"),
    ("south-dakota", "SD"), ("tennessee", "TN"), ("texas", "TX"), ("utah", "UT"),
    ("vermont", "VT"), ("virginia", "VA"), ("washington", "WA"), ("west-virginia", "WV"),
    ("wisconsin", "WI"), ("wyoming", "WY"),
]

# Max pages per category/state
MAX_PAGES_PER_SECTION = 100


class Scraper(BaseScraper):
    SOURCE_NAME = "dnb_listings"
    BASE_URL = "https://www.dnb.com"
    REQUESTS_PER_MINUTE = 5  # Conservative

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_slugs: set[str] = set()
        self._seen_names: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run()."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used -- we override run()."""
        return []

    def _parse_directory_page(self, html: str, source_url: str) -> list[tuple[str, dict]]:
        """Parse a D&B directory page for company links and basic data.

        Returns list of (company_url, metadata_dict) tuples.
        """
        soup = self.parse_html(html)
        companies = []

        # Strategy 1: Look for company profile links
        # Pattern: /business-directory/company-profiles.{slug}.html
        for link in soup.find_all("a", href=re.compile(r"/business-directory/company-profiles\.")):
            href = link.get("href", "")
            name = link.get_text(strip=True)

            if not name or len(name) < 2 or len(name) > 200:
                continue

            # Skip navigational text
            if name.lower() in ("view profile", "see more", "learn more"):
                # Look for name in parent
                parent = link.find_parent(["div", "li", "tr"])
                if parent:
                    heading = parent.find(["h2", "h3", "h4", "strong"])
                    if heading:
                        name = heading.get_text(strip=True)
                    else:
                        continue

            slug_match = re.search(r"company-profiles\.([^.]+)\.html", href)
            slug = slug_match.group(1) if slug_match else ""

            if slug and slug in self._seen_slugs:
                continue
            if slug:
                self._seen_slugs.add(slug)

            full_url = f"{self.BASE_URL}{href}" if href.startswith("/") else href

            # Try to get additional data from the same container
            metadata = {"name": name}
            parent = link.find_parent(["div", "li", "tr", "article"])
            if parent:
                parent_text = parent.get_text(separator=" | ", strip=True)

                # Look for industry
                industry_match = re.search(r"(?:Industry|Sector):\s*([^|]+)", parent_text)
                if industry_match:
                    metadata["industry"] = industry_match.group(1).strip()

                # Look for location
                location_match = re.search(r"(?:Location|City|HQ):\s*([^|]+)", parent_text)
                if location_match:
                    metadata["location"] = location_match.group(1).strip()

                # Look for employees
                emp_match = re.search(r"(\d[\d,]+)\s*(?:employees|staff)", parent_text, re.IGNORECASE)
                if emp_match:
                    metadata["employees"] = emp_match.group(1).strip()

                # Look for revenue
                rev_match = re.search(r"\$[\d,.]+\s*(?:M|B|million|billion)", parent_text, re.IGNORECASE)
                if rev_match:
                    metadata["revenue"] = rev_match.group(0).strip()

            companies.append((full_url, metadata))

        # Strategy 2: Look for any business listing cards/items
        if not companies:
            for card in soup.find_all(["div", "li", "article"], class_=re.compile(r"company|business|listing|result", re.IGNORECASE)):
                link = card.find("a", href=True)
                if not link:
                    continue

                href = link.get("href", "")
                if "dnb.com" not in href and not href.startswith("/"):
                    continue

                name_el = card.find(["h2", "h3", "h4", "strong"])
                name = name_el.get_text(strip=True) if name_el else link.get_text(strip=True)

                if not name or len(name) < 2:
                    continue

                name_key = re.sub(r"[^\w]", "", name.lower())
                if name_key in self._seen_names:
                    continue
                self._seen_names.add(name_key)

                full_url = f"{self.BASE_URL}{href}" if href.startswith("/") else href
                companies.append((full_url, {"name": name}))

        # Strategy 3: Look for structured data in page
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                ld = json.loads(script.string or "")
            except (json.JSONDecodeError, TypeError):
                continue

            items = ld if isinstance(ld, list) else [ld]
            for item in items:
                if item.get("@type") == "ItemList":
                    for element in item.get("itemListElement", []):
                        if isinstance(element, dict):
                            item_data = element.get("item") or element
                            name = (item_data.get("name") or "").strip()
                            url = (item_data.get("url") or "").strip()
                            if name and url:
                                name_key = re.sub(r"[^\w]", "", name.lower())
                                if name_key not in self._seen_names:
                                    self._seen_names.add(name_key)
                                    companies.append((url, {"name": name}))

        return companies

    def _parse_company_page(self, html: str, source_url: str) -> Optional[ScrapedContact]:
        """Parse an individual D&B company profile page."""
        soup = self.parse_html(html)

        name = ""
        website = ""
        industry = ""
        description = ""
        location = ""
        phone = ""
        employees = ""
        revenue = ""
        duns = ""
        founded = ""

        # JSON-LD structured data
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                ld = json.loads(script.string or "")
            except (json.JSONDecodeError, TypeError):
                continue

            items = ld if isinstance(ld, list) else [ld]
            for item in items:
                if item.get("@type") in ("Organization", "Corporation", "LocalBusiness"):
                    name = (item.get("name") or "").strip()
                    website = (item.get("url") or "").strip()
                    description = (item.get("description") or "").strip()
                    phone = (item.get("telephone") or "").strip()

                    addr = item.get("address") or {}
                    if isinstance(addr, dict):
                        locality = (addr.get("addressLocality") or "").strip()
                        region = (addr.get("addressRegion") or "").strip()
                        country = (addr.get("addressCountry") or "").strip()
                        parts = [p for p in [locality, region, country] if p]
                        location = ", ".join(parts)

                    employees_obj = item.get("numberOfEmployees") or {}
                    if isinstance(employees_obj, dict):
                        employees = str(employees_obj.get("value") or "")

                    duns = (item.get("duns") or item.get("dunsNumber") or "").strip()

        # Open Graph fallback
        if not name:
            og_title = soup.find("meta", property="og:title")
            if og_title:
                name = (og_title.get("content") or "").strip()
                name = re.sub(r"\s*[-|]\s*D&?N?B?.*$", "", name).strip()
                name = re.sub(r"\s*[-|]\s*Dun\s*&\s*Bradstreet.*$", "", name).strip()

        if not description:
            og_desc = soup.find("meta", property="og:description")
            if og_desc:
                description = (og_desc.get("content") or "").strip()

        # Meta description fallback
        if not description:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc:
                description = (meta_desc.get("content") or "").strip()

        # Parse profile data elements
        for section in soup.find_all(["div", "section", "dl"], class_=re.compile(r"company|profile|detail", re.IGNORECASE)):
            for row in section.find_all(["div", "tr", "li"]):
                row_text = row.get_text(separator=" ", strip=True)

                if not industry and re.search(r"(?:industry|sector)", row_text, re.IGNORECASE):
                    value = re.sub(r"^.*(?:industry|sector)\s*:?\s*", "", row_text, flags=re.IGNORECASE).strip()
                    if value and len(value) < 100:
                        industry = value

                if not location and re.search(r"(?:location|address|headquarters|hq)", row_text, re.IGNORECASE):
                    value = re.sub(r"^.*(?:location|address|headquarters|hq)\s*:?\s*", "", row_text, flags=re.IGNORECASE).strip()
                    if value and len(value) < 200:
                        location = value

                if not employees and re.search(r"(?:employee|staff|size)", row_text, re.IGNORECASE):
                    emp_match = re.search(r"(\d[\d,]+)", row_text)
                    if emp_match:
                        employees = emp_match.group(1)

                if not revenue and re.search(r"(?:revenue|sales)", row_text, re.IGNORECASE):
                    rev_match = re.search(r"\$[\d,.]+\s*(?:M|B|K|million|billion|thousand)?", row_text, re.IGNORECASE)
                    if rev_match:
                        revenue = rev_match.group(0)

                if not founded and re.search(r"(?:founded|established|incorporated)", row_text, re.IGNORECASE):
                    year_match = re.search(r"((?:19|20)\d{2})", row_text)
                    if year_match:
                        founded = year_match.group(1)

                if not duns and re.search(r"(?:D-?U-?N-?S|DUNS)", row_text, re.IGNORECASE):
                    duns_match = re.search(r"(\d{2}-\d{3}-\d{4}|\d{9})", row_text)
                    if duns_match:
                        duns = duns_match.group(1)

                if not website:
                    website_link = row.find("a", href=re.compile(r"^https?://(?!.*dnb\.com)"))
                    if website_link:
                        website = (website_link.get("href") or "").strip()

        # Phone from page text
        if not phone:
            phone_pattern = re.compile(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}")
            page_text = soup.get_text()
            phone_match = phone_pattern.search(page_text)
            if phone_match:
                phone = phone_match.group(0)

        if not name or len(name) < 2:
            return None

        # Deduplicate
        name_key = re.sub(r"[^\w]", "", name.lower())
        if name_key in self._seen_names:
            return None
        self._seen_names.add(name_key)

        # If website is a D&B URL, use the D&B profile as website
        if website and "dnb.com" in website:
            dnb_url = website
            website = source_url  # Use the source URL instead
        else:
            dnb_url = source_url

        if not website:
            website = dnb_url

        # Bio
        bio_parts = [name]
        if industry:
            bio_parts.append(f"Industry: {industry}")
        if location:
            bio_parts.append(location)
        if employees:
            bio_parts.append(f"Employees: {employees}")
        if revenue:
            bio_parts.append(f"Revenue: {revenue}")
        if founded:
            bio_parts.append(f"Founded: {founded}")
        if duns:
            bio_parts.append(f"DUNS: {duns}")
        if description:
            remaining = 2000 - len(" | ".join(bio_parts)) - 3
            if remaining > 50:
                bio_parts.append(description[:remaining])
        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=name,
            email="",
            company=name,
            website=website,
            linkedin="",
            phone=phone,
            bio=bio,
            source_category="dnb_directory",
            raw_data={
                "dnb_url": dnb_url,
                "industry": industry,
                "location": location,
                "employees": employees,
                "revenue": revenue,
                "founded": founded,
                "duns": duns,
            },
        )

    def _discover_category_urls(self, category_slug: str) -> list[str]:
        """Discover paginated URLs within a D&B directory category."""
        urls = []
        base = f"{self.BASE_URL}/business-directory/{category_slug}.html"

        html = self.fetch_page(base)
        if not html:
            return [base]  # Return base URL to attempt anyway

        soup = self.parse_html(html)

        # Look for pagination links
        for link in soup.find_all("a", href=re.compile(r"/business-directory/")):
            href = link.get("href", "")
            full_url = f"{self.BASE_URL}{href}" if href.startswith("/") else href
            if full_url not in urls:
                urls.append(full_url)

        # Also look for sub-category links
        for link in soup.find_all("a", href=re.compile(rf"/business-directory/{re.escape(category_slug)}")):
            href = link.get("href", "")
            full_url = f"{self.BASE_URL}{href}" if href.startswith("/") else href
            if full_url not in urls:
                urls.append(full_url)

        if not urls:
            urls.append(base)

        return urls

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Scrape D&B business directory.

        Two-phase approach:
        1. Browse industry category pages for company listings
        2. Browse state directory pages for company listings
        """
        self.logger.info(
            "Starting %s scraper — %d industries, %d states",
            self.SOURCE_NAME, len(INDUSTRY_CATEGORIES), len(US_STATES),
        )

        contacts_yielded = 0
        pages_done = 0
        start_phase = (checkpoint or {}).get("phase", 0)
        start_idx = (checkpoint or {}).get("idx", 0)

        # Phase 1: Industry category browsing
        if start_phase <= 0:
            self.logger.info("Phase 1: Industry category browsing")
            for ind_idx, (slug, industry_name) in enumerate(INDUSTRY_CATEGORIES):
                if start_phase == 0 and ind_idx < start_idx:
                    continue

                self.logger.info(
                    "Industry: %s (%d/%d)",
                    industry_name, ind_idx + 1, len(INDUSTRY_CATEGORIES),
                )

                # Get category page and any sub-pages
                category_url = f"{self.BASE_URL}/business-directory/{slug}.html"
                page_num = 1
                consecutive_empty = 0

                while page_num <= MAX_PAGES_PER_SECTION:
                    if page_num == 1:
                        current_url = category_url
                    else:
                        current_url = f"{self.BASE_URL}/business-directory/{slug}/{page_num}.html"

                    html = self.fetch_page(current_url)
                    if not html:
                        # Try alternate URL pattern
                        alt_url = f"{self.BASE_URL}/business-directory/{slug}?page={page_num}"
                        html = self.fetch_page(alt_url)
                        if not html:
                            break

                    company_links = self._parse_directory_page(html, current_url)

                    if not company_links:
                        consecutive_empty += 1
                        if consecutive_empty >= 2:
                            break
                        page_num += 1
                        continue
                    else:
                        consecutive_empty = 0

                    for company_url, metadata in company_links:
                        company_name = metadata.get("name", "")
                        if not company_name:
                            continue

                        # For browse results, create contact from available metadata
                        name_key = re.sub(r"[^\w]", "", company_name.lower())
                        if name_key in self._seen_names:
                            continue
                        self._seen_names.add(name_key)

                        bio_parts = [company_name]
                        if metadata.get("industry"):
                            bio_parts.append(f"Industry: {metadata['industry']}")
                        else:
                            bio_parts.append(f"Industry: {industry_name}")
                        if metadata.get("location"):
                            bio_parts.append(metadata["location"])
                        if metadata.get("employees"):
                            bio_parts.append(f"Employees: {metadata['employees']}")
                        if metadata.get("revenue"):
                            bio_parts.append(f"Revenue: {metadata['revenue']}")
                        bio = " | ".join(bio_parts)

                        contact = ScrapedContact(
                            name=company_name,
                            email="",
                            company=company_name,
                            website=company_url,
                            linkedin="",
                            phone="",
                            bio=bio,
                            source_platform=self.SOURCE_NAME,
                            source_url=current_url,
                            source_category="dnb_directory",
                            scraped_at=datetime.now().isoformat(),
                            raw_data={
                                "dnb_url": company_url,
                                "industry": metadata.get("industry", industry_name),
                                "location": metadata.get("location", ""),
                                "employees": metadata.get("employees", ""),
                                "revenue": metadata.get("revenue", ""),
                            },
                        )

                        if contact.is_valid():
                            contact.email = contact.clean_email()
                            self.stats["contacts_found"] += 1
                            self.stats["contacts_valid"] += 1
                            contacts_yielded += 1
                            yield contact

                            if max_contacts and contacts_yielded >= max_contacts:
                                self.logger.info("Reached max_contacts=%d", max_contacts)
                                return

                    pages_done += 1
                    page_num += 1

                    if max_pages and pages_done >= max_pages:
                        self.logger.info("Reached max_pages=%d", max_pages)
                        return

                if pages_done % 10 == 0 and pages_done > 0:
                    self.logger.info(
                        "Progress: %d pages, %d valid contacts",
                        pages_done, self.stats["contacts_valid"],
                    )

        # Phase 2: State directory browsing
        if start_phase <= 1:
            self.logger.info("Phase 2: State directory browsing")
            for state_idx, (state_slug, state_code) in enumerate(US_STATES):
                if start_phase == 1 and state_idx < start_idx:
                    continue

                self.logger.info(
                    "State: %s (%d/%d)",
                    state_code, state_idx + 1, len(US_STATES),
                )

                page_num = 1
                consecutive_empty = 0

                while page_num <= MAX_PAGES_PER_SECTION:
                    if page_num == 1:
                        current_url = f"{self.BASE_URL}/business-directory/{state_slug}.html"
                    else:
                        current_url = f"{self.BASE_URL}/business-directory/{state_slug}/{page_num}.html"

                    html = self.fetch_page(current_url)
                    if not html:
                        alt_url = f"{self.BASE_URL}/business-directory/{state_slug}?page={page_num}"
                        html = self.fetch_page(alt_url)
                        if not html:
                            break

                    company_links = self._parse_directory_page(html, current_url)

                    if not company_links:
                        consecutive_empty += 1
                        if consecutive_empty >= 2:
                            break
                        page_num += 1
                        continue
                    else:
                        consecutive_empty = 0

                    for company_url, metadata in company_links:
                        company_name = metadata.get("name", "")
                        if not company_name:
                            continue

                        name_key = re.sub(r"[^\w]", "", company_name.lower())
                        if name_key in self._seen_names:
                            continue
                        self._seen_names.add(name_key)

                        bio_parts = [company_name]
                        if metadata.get("industry"):
                            bio_parts.append(f"Industry: {metadata['industry']}")
                        bio_parts.append(f"State: {state_code}")
                        if metadata.get("location"):
                            bio_parts.append(metadata["location"])
                        bio = " | ".join(bio_parts)

                        contact = ScrapedContact(
                            name=company_name,
                            email="",
                            company=company_name,
                            website=company_url,
                            linkedin="",
                            phone="",
                            bio=bio,
                            source_platform=self.SOURCE_NAME,
                            source_url=current_url,
                            source_category="dnb_directory",
                            scraped_at=datetime.now().isoformat(),
                            raw_data={
                                "dnb_url": company_url,
                                "industry": metadata.get("industry", ""),
                                "location": metadata.get("location", ""),
                                "state": state_code,
                            },
                        )

                        if contact.is_valid():
                            contact.email = contact.clean_email()
                            self.stats["contacts_found"] += 1
                            self.stats["contacts_valid"] += 1
                            contacts_yielded += 1
                            yield contact

                            if max_contacts and contacts_yielded >= max_contacts:
                                self.logger.info("Reached max_contacts=%d", max_contacts)
                                return

                    pages_done += 1
                    page_num += 1

                    if max_pages and pages_done >= max_pages:
                        self.logger.info("Reached max_pages=%d", max_pages)
                        return

                if pages_done % 10 == 0 and pages_done > 0:
                    self.logger.info(
                        "Progress: %d pages, %d valid contacts",
                        pages_done, self.stats["contacts_valid"],
                    )

        self.logger.info("Scraper complete: %s", self.stats)
