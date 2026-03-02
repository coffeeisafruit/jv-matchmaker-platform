"""
Glassdoor Company Directory Scraper

Scrapes Glassdoor company listings from their publicly accessible
company pages and explore/browse pages.

Strategy:
  - Use Glassdoor's sitemap to discover company page URLs in bulk
  - Browse company listings by industry and location
  - Parse company profile pages for structured data (JSON-LD, meta tags)
  - Each company page has: name, website, industry, description,
    headquarters, company size, founded year, revenue range

Glassdoor URL patterns:
  - Sitemap: https://www.glassdoor.com/sitemap.xml
  - Company pages: /Overview/Working-at-{company}-EI_IE{id}.htm
  - Industry browse: /Explore/browse-companies.htm?overall_rating_low=3&industry={id}
  - Location browse: /Explore/browse-companies.htm?locId={id}&locType=C

Estimated yield: 50,000-200,000 companies.

Note: Glassdoor uses Cloudflare protection and may require cookies.
The scraper uses conservative rate limiting and attempts to work
with the initial HTML response. If blocked, consider using the
Glassdoor API partner program.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Glassdoor industry IDs for browsing
INDUSTRIES = [
    ("200063", "Accounting"),
    ("200064", "Advertising & Marketing"),
    ("200065", "Aerospace & Defense"),
    ("200066", "Agriculture"),
    ("200067", "Arts, Entertainment & Recreation"),
    ("200068", "Automotive"),
    ("200069", "Biotechnology"),
    ("200070", "Business Services"),
    ("200071", "Charitable Organizations"),
    ("200072", "Computer Hardware"),
    ("200073", "Construction"),
    ("200074", "Consulting"),
    ("200075", "Consumer Products"),
    ("200076", "Education"),
    ("200077", "Energy, Mining & Utilities"),
    ("200078", "Financial Services"),
    ("200079", "Food & Beverage"),
    ("200080", "Government"),
    ("200081", "Health Care"),
    ("200082", "Human Resources"),
    ("200083", "Information Technology"),
    ("200084", "Insurance"),
    ("200085", "Internet"),
    ("200086", "Legal"),
    ("200087", "Manufacturing"),
    ("200088", "Media"),
    ("200089", "Pharmaceutical"),
    ("200090", "Real Estate"),
    ("200091", "Restaurants & Food Service"),
    ("200092", "Retail"),
    ("200093", "Staffing & Outsourcing"),
    ("200094", "Telecommunications"),
    ("200095", "Transportation & Logistics"),
    ("200096", "Travel & Tourism"),
]

# US metro location IDs for geographic browsing
LOCATIONS = [
    ("1132348", "New York, NY"),
    ("1147401", "Los Angeles, CA"),
    ("1128808", "Chicago, IL"),
    ("1140171", "Houston, TX"),
    ("1133904", "Phoenix, AZ"),
    ("1152672", "San Antonio, TX"),
    ("1147311", "San Diego, CA"),
    ("1139761", "Dallas, TX"),
    ("1147436", "San Jose, CA"),
    ("1154532", "Austin, TX"),
    ("1147431", "San Francisco, CA"),
    ("1150505", "Seattle, WA"),
    ("1139977", "Denver, CO"),
    ("1138213", "Washington, DC"),
    ("1144541", "Nashville, TN"),
    ("1154532", "Boston, MA"),
    ("1155583", "Atlanta, GA"),
    ("1154170", "Miami, FL"),
    ("1142551", "Minneapolis, MN"),
    ("1151614", "Portland, OR"),
]

# Max pages per industry/location browse
MAX_BROWSE_PAGES = 50


class Scraper(BaseScraper):
    SOURCE_NAME = "glassdoor_companies"
    BASE_URL = "https://www.glassdoor.com"
    REQUESTS_PER_MINUTE = 5  # Conservative — Cloudflare protected

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_ids: set[str] = set()
        self._seen_names: set[str] = set()
        # Glassdoor requires specific headers
        self.session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.5",
            "Cache-Control": "no-cache",
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1",
        })

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run()."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used -- we override run()."""
        return []

    def _parse_company_page(self, html: str, source_url: str) -> Optional[ScrapedContact]:
        """Parse a Glassdoor company overview page."""
        soup = self.parse_html(html)

        name = ""
        website = ""
        industry = ""
        description = ""
        headquarters = ""
        company_size = ""
        founded = ""
        revenue = ""
        rating = ""
        review_count = ""

        # Strategy 1: JSON-LD structured data
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
                    addr = item.get("address") or {}
                    if isinstance(addr, dict):
                        locality = (addr.get("addressLocality") or "").strip()
                        region = (addr.get("addressRegion") or "").strip()
                        country = (addr.get("addressCountry") or "").strip()
                        parts = [p for p in [locality, region, country] if p]
                        headquarters = ", ".join(parts)
                    aggregate = item.get("aggregateRating") or {}
                    if aggregate:
                        rating = str(aggregate.get("ratingValue", ""))
                        review_count = str(aggregate.get("reviewCount", ""))

        # Strategy 2: Open Graph tags
        if not name:
            og_title = soup.find("meta", property="og:title")
            if og_title:
                title_text = (og_title.get("content") or "").strip()
                # Clean up: "Working at Company Name | Glassdoor"
                name = re.sub(r"^Working at\s+", "", title_text)
                name = re.sub(r"\s*[|\-]\s*Glassdoor.*$", "", name).strip()

        if not description:
            og_desc = soup.find("meta", property="og:description")
            if og_desc:
                description = (og_desc.get("content") or "").strip()

        # Strategy 3: Parse specific Glassdoor data elements
        # Company info module typically has structured data
        info_section = soup.find("div", {"data-test": "employerInfo"}) or soup.find("div", class_=re.compile(r"infoEntity|companyInfo"))
        if info_section:
            for row in info_section.find_all(["div", "li"]):
                label_el = row.find(["label", "span", "dt"], class_=re.compile(r"label|key"))
                value_el = row.find(["span", "div", "dd"], class_=re.compile(r"value|content"))
                if not label_el or not value_el:
                    continue

                label = label_el.get_text(strip=True).lower()
                value = value_el.get_text(strip=True)

                if "website" in label:
                    website = website or value
                elif "headquarter" in label:
                    headquarters = headquarters or value
                elif "size" in label:
                    company_size = company_size or value
                elif "founded" in label:
                    founded = founded or value
                elif "industry" in label:
                    industry = industry or value
                elif "revenue" in label:
                    revenue = revenue or value

        # Strategy 4: Look for data in inline JavaScript / Apollo state
        for script in soup.find_all("script"):
            text = script.string or ""
            if "apolloState" in text or "__NEXT_DATA__" in text or "window.__ENV" in text:
                # Try to extract company data from JS
                name_match = re.search(r'"name"\s*:\s*"([^"]{2,100})"', text)
                if name_match and not name:
                    name = name_match.group(1).strip()

                website_match = re.search(r'"website"\s*:\s*"(https?://[^"]+)"', text)
                if website_match and not website:
                    website = website_match.group(1).strip()

                industry_match = re.search(r'"industry(?:Name)?"\s*:\s*"([^"]+)"', text)
                if industry_match and not industry:
                    industry = industry_match.group(1).strip()

                size_match = re.search(r'"size(?:Category)?"\s*:\s*"([^"]+)"', text)
                if size_match and not company_size:
                    company_size = size_match.group(1).strip()

                hq_match = re.search(r'"headquarters"\s*:\s*"([^"]+)"', text)
                if hq_match and not headquarters:
                    headquarters = hq_match.group(1).strip()

        if not name or len(name) < 2:
            return None

        # Deduplicate
        name_key = re.sub(r"[^\w]", "", name.lower())
        if name_key in self._seen_names:
            return None
        self._seen_names.add(name_key)

        # If website points to Glassdoor, clear it (not useful)
        if website and "glassdoor.com" in website:
            glassdoor_url = website
            website = ""
        else:
            glassdoor_url = source_url

        # If no external website, use the Glassdoor URL
        if not website:
            website = glassdoor_url

        # Bio
        bio_parts = [name]
        if industry:
            bio_parts.append(f"Industry: {industry}")
        if headquarters:
            bio_parts.append(headquarters)
        if rating:
            bio_parts.append(f"Rating: {rating}/5")
        if review_count:
            bio_parts.append(f"{review_count} reviews")
        if company_size:
            bio_parts.append(f"Size: {company_size}")
        if founded:
            bio_parts.append(f"Founded: {founded}")
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
            phone="",
            bio=bio,
            source_category="glassdoor_company",
            raw_data={
                "glassdoor_url": glassdoor_url,
                "industry": industry,
                "headquarters": headquarters,
                "company_size": company_size,
                "founded": founded,
                "revenue": revenue,
                "rating": rating,
                "review_count": review_count,
            },
        )

    def _parse_browse_page(self, html: str, source_url: str) -> list[tuple[str, str]]:
        """Parse a Glassdoor browse/explore page for company URLs and names.

        Returns list of (company_url, company_name) tuples.
        """
        soup = self.parse_html(html)
        companies = []

        # Look for company links in browse results
        # Glassdoor company overview URLs follow pattern: /Overview/Working-at-*-EI_IE*.htm
        for link in soup.find_all("a", href=re.compile(r"/Overview/.*EI_IE\d+")):
            href = link.get("href", "")
            name = link.get_text(strip=True)

            if not name or len(name) < 2 or len(name) > 200:
                continue

            # Clean name
            name = re.sub(r"^Working at\s+", "", name).strip()

            # Extract company ID for dedup
            id_match = re.search(r"EI_IE(\d+)", href)
            company_id = id_match.group(1) if id_match else ""

            if company_id and company_id in self._seen_ids:
                continue
            if company_id:
                self._seen_ids.add(company_id)

            full_url = f"{self.BASE_URL}{href}" if href.startswith("/") else href
            companies.append((full_url, name))

        # Also check for employer cards / list items
        if not companies:
            for card in soup.find_all(["div", "li"], attrs={"data-test": re.compile(r"employer")}):
                link = card.find("a", href=True)
                if not link:
                    continue
                href = link.get("href", "")
                name = link.get_text(strip=True)
                if not name or "glassdoor" in name.lower():
                    continue

                id_match = re.search(r"EI_IE(\d+)", href)
                company_id = id_match.group(1) if id_match else ""
                if company_id and company_id in self._seen_ids:
                    continue
                if company_id:
                    self._seen_ids.add(company_id)

                full_url = f"{self.BASE_URL}{href}" if href.startswith("/") else href
                companies.append((full_url, name))

        return companies

    def _fetch_sitemap_urls(self) -> list[str]:
        """Discover company page URLs from Glassdoor sitemaps."""
        urls = []

        # Main sitemap index
        sitemap_index_url = f"{self.BASE_URL}/sitemap.xml"
        self.logger.info("Fetching sitemap index: %s", sitemap_index_url)

        html = self.fetch_page(sitemap_index_url)
        if not html:
            return urls

        # Find company-related sitemaps
        company_sitemaps = []
        for match in re.finditer(r"<loc>(https?://[^<]*(?:company|employer|overview)[^<]*)</loc>", html, re.IGNORECASE):
            company_sitemaps.append(match.group(1))

        # If no specific company sitemaps found, look for general sitemaps
        if not company_sitemaps:
            for match in re.finditer(r"<loc>(https?://[^<]+\.xml[^<]*)</loc>", html):
                company_sitemaps.append(match.group(1))

        self.logger.info("Found %d potential sitemaps to scan", len(company_sitemaps))

        for sitemap_url in company_sitemaps[:20]:  # Limit to 20 sitemaps
            sitemap_html = self.fetch_page(sitemap_url)
            if not sitemap_html:
                continue

            for match in re.finditer(r"<loc>(https?://www\.glassdoor\.com/Overview/[^<]+)</loc>", sitemap_html):
                url = match.group(1)
                id_match = re.search(r"EI_IE(\d+)", url)
                if id_match:
                    company_id = id_match.group(1)
                    if company_id not in self._seen_ids:
                        self._seen_ids.add(company_id)
                        urls.append(url)

            if len(urls) > 100000:
                break  # Safety limit

        self.logger.info("Found %d company URLs from sitemaps", len(urls))
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
        """Scrape Glassdoor company directory.

        Three-phase approach:
        1. Sitemap discovery for bulk company URLs
        2. Industry browse pages
        3. Location browse pages
        """
        self.logger.info(
            "Starting %s scraper — %d industries, %d locations",
            self.SOURCE_NAME, len(INDUSTRIES), len(LOCATIONS),
        )

        contacts_yielded = 0
        pages_done = 0
        start_phase = (checkpoint or {}).get("phase", 0)
        start_idx = (checkpoint or {}).get("idx", 0)

        # Phase 1: Sitemap-based discovery
        if start_phase <= 0:
            self.logger.info("Phase 1: Sitemap discovery")
            sitemap_urls = self._fetch_sitemap_urls()

            for idx, company_url in enumerate(sitemap_urls):
                if start_phase == 0 and idx < start_idx:
                    continue

                html = self.fetch_page(company_url)
                if not html:
                    continue

                # Check for Cloudflare block
                if "cf-browser-verification" in html.lower() or "just a moment" in html.lower():
                    self.logger.warning("Cloudflare block detected — stopping sitemap phase")
                    break

                contact = self._parse_company_page(html, company_url)
                if contact and contact.is_valid():
                    contact.source_platform = self.SOURCE_NAME
                    contact.source_url = company_url
                    contact.scraped_at = datetime.now().isoformat()
                    contact.email = contact.clean_email()
                    self.stats["contacts_found"] += 1
                    self.stats["contacts_valid"] += 1
                    contacts_yielded += 1
                    yield contact

                    if max_contacts and contacts_yielded >= max_contacts:
                        self.logger.info("Reached max_contacts=%d", max_contacts)
                        return

                pages_done += 1
                if max_pages and pages_done >= max_pages:
                    self.logger.info("Reached max_pages=%d", max_pages)
                    return

                if pages_done % 50 == 0:
                    self.logger.info(
                        "Phase 1: %d/%d URLs, %d valid contacts",
                        idx + 1, len(sitemap_urls), self.stats["contacts_valid"],
                    )

        # Phase 2: Industry browsing
        if start_phase <= 1:
            self.logger.info("Phase 2: Industry browsing")
            for ind_idx, (industry_id, industry_name) in enumerate(INDUSTRIES):
                if start_phase == 1 and ind_idx < start_idx:
                    continue

                self.logger.info(
                    "Industry: %s (%d/%d)",
                    industry_name, ind_idx + 1, len(INDUSTRIES),
                )

                consecutive_empty = 0
                for page_num in range(1, MAX_BROWSE_PAGES + 1):
                    browse_url = (
                        f"{self.BASE_URL}/Explore/browse-companies.htm"
                        f"?overall_rating_low=3&industry={industry_id}&page={page_num}"
                    )

                    html = self.fetch_page(browse_url)
                    if not html:
                        break

                    if "cf-browser-verification" in html.lower():
                        self.logger.warning("Cloudflare block on industry browse")
                        break

                    company_links = self._parse_browse_page(html, browse_url)

                    if not company_links:
                        consecutive_empty += 1
                        if consecutive_empty >= 2:
                            break
                        continue
                    else:
                        consecutive_empty = 0

                    for company_url, company_name in company_links:
                        # Quick contact from browse data (no detail page fetch)
                        name_key = re.sub(r"[^\w]", "", company_name.lower())
                        if name_key in self._seen_names:
                            continue
                        self._seen_names.add(name_key)

                        contact = ScrapedContact(
                            name=company_name,
                            email="",
                            company=company_name,
                            website=company_url,
                            linkedin="",
                            phone="",
                            bio=f"{company_name} | Industry: {industry_name} | Glassdoor rated 3+",
                            source_platform=self.SOURCE_NAME,
                            source_url=browse_url,
                            source_category="glassdoor_company",
                            scraped_at=datetime.now().isoformat(),
                            raw_data={
                                "glassdoor_url": company_url,
                                "industry": industry_name,
                                "industry_id": industry_id,
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
                    if max_pages and pages_done >= max_pages:
                        self.logger.info("Reached max_pages=%d", max_pages)
                        return

        # Phase 3: Location browsing
        if start_phase <= 2:
            self.logger.info("Phase 3: Location browsing")
            for loc_idx, (loc_id, loc_name) in enumerate(LOCATIONS):
                if start_phase == 2 and loc_idx < start_idx:
                    continue

                self.logger.info(
                    "Location: %s (%d/%d)",
                    loc_name, loc_idx + 1, len(LOCATIONS),
                )

                consecutive_empty = 0
                for page_num in range(1, MAX_BROWSE_PAGES + 1):
                    browse_url = (
                        f"{self.BASE_URL}/Explore/browse-companies.htm"
                        f"?overall_rating_low=3&locId={loc_id}&locType=C&page={page_num}"
                    )

                    html = self.fetch_page(browse_url)
                    if not html:
                        break

                    if "cf-browser-verification" in html.lower():
                        self.logger.warning("Cloudflare block on location browse")
                        break

                    company_links = self._parse_browse_page(html, browse_url)

                    if not company_links:
                        consecutive_empty += 1
                        if consecutive_empty >= 2:
                            break
                        continue
                    else:
                        consecutive_empty = 0

                    for company_url, company_name in company_links:
                        name_key = re.sub(r"[^\w]", "", company_name.lower())
                        if name_key in self._seen_names:
                            continue
                        self._seen_names.add(name_key)

                        contact = ScrapedContact(
                            name=company_name,
                            email="",
                            company=company_name,
                            website=company_url,
                            linkedin="",
                            phone="",
                            bio=f"{company_name} | {loc_name} | Glassdoor rated 3+",
                            source_platform=self.SOURCE_NAME,
                            source_url=browse_url,
                            source_category="glassdoor_company",
                            scraped_at=datetime.now().isoformat(),
                            raw_data={
                                "glassdoor_url": company_url,
                                "location": loc_name,
                                "location_id": loc_id,
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
                    if max_pages and pages_done >= max_pages:
                        self.logger.info("Reached max_pages=%d", max_pages)
                        return

                if pages_done % 10 == 0 and pages_done > 0:
                    self.logger.info(
                        "Progress: %d pages, %d valid contacts",
                        pages_done, self.stats["contacts_valid"],
                    )

        self.logger.info("Scraper complete: %s", self.stats)
