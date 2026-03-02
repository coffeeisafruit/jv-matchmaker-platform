"""
LinkedIn Company Pages Scraper (via Google Search)

Discovers LinkedIn company pages through Google search queries using
site:linkedin.com/company/ operator. This approach accesses only
publicly-indexed LinkedIn company pages — no login required.

Strategy:
  - Build Google search queries: site:linkedin.com/company/ "{industry}"
  - Parse Google search result snippets for company metadata
  - For each discovered LinkedIn company URL, fetch the public page
    and extract structured JSON-LD data (available without login)
  - Paginate through Google results (10 per page, up to 30 pages per query)

Industries targeted:
  - Marketing agencies, consulting firms, tech companies
  - Financial services, coaching, training
  - Business services, staffing, PR firms

Estimated yield: 50,000-150,000 company profiles.

Note: Google may rate-limit or show CAPTCHAs if requests are too fast.
Conservative rate limiting is essential. LinkedIn pages may also require
JS rendering for full data — we extract what's available from the
initial HTML response (JSON-LD, meta tags, og: tags).
"""

from __future__ import annotations

import json
import re
import time
import urllib.parse
from datetime import datetime
from typing import Iterator, Optional

from scripts.sourcing.base import BaseScraper, ScrapedContact


# Industry keywords for Google site: searches
INDUSTRY_KEYWORDS = [
    "marketing agency",
    "digital marketing",
    "business consultant",
    "management consulting",
    "financial advisor",
    "financial services",
    "life coach",
    "business coach",
    "executive coaching",
    "web design agency",
    "software development company",
    "IT consulting",
    "public relations firm",
    "advertising agency",
    "SEO company",
    "content marketing agency",
    "social media agency",
    "graphic design studio",
    "video production company",
    "event planning company",
    "staffing agency",
    "recruitment firm",
    "HR consulting",
    "accounting firm",
    "insurance agency",
    "real estate company",
    "training company",
    "leadership development",
    "sales training",
    "business intelligence",
    "data analytics company",
    "cybersecurity firm",
    "cloud computing services",
    "ecommerce agency",
    "branding agency",
    "market research company",
    "lead generation company",
    "CRM consulting",
    "supply chain consulting",
    "healthcare consulting",
    "education technology",
    "SaaS company",
    "startup accelerator",
    "venture capital firm",
    "private equity firm",
    "wealth management",
    "investment advisory",
    "tax consulting",
    "legal services firm",
    "intellectual property law",
]

# Google search result pagination
GOOGLE_RESULTS_PER_PAGE = 10
MAX_GOOGLE_PAGES = 30  # Up to 300 results per keyword


class Scraper(BaseScraper):
    SOURCE_NAME = "linkedin_companies"
    BASE_URL = "https://www.google.com"
    REQUESTS_PER_MINUTE = 4  # Very conservative for Google scraping

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_slugs: set[str] = set()
        self._seen_names: set[str] = set()
        # Use a more browser-like session for Google
        self.session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.5",
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1",
        })

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run()."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used -- we override run()."""
        return []

    def _parse_google_results(self, html: str) -> list[dict]:
        """Parse Google search results HTML to extract LinkedIn company URLs and snippets."""
        soup = self.parse_html(html)
        results = []

        # Google search results are typically in <div class="g"> blocks
        for result_div in soup.find_all("div", class_="g"):
            link = result_div.find("a", href=re.compile(r"linkedin\.com/company/"))
            if not link:
                continue

            href = link.get("href", "")

            # Extract the actual LinkedIn URL (Google may wrap it)
            linkedin_url = href
            if "/url?q=" in href:
                match = re.search(r"/url\?q=(https?://[^&]+)", href)
                if match:
                    linkedin_url = urllib.parse.unquote(match.group(1))

            # Must be a company page
            if "/company/" not in linkedin_url:
                continue

            # Extract company slug from URL
            slug_match = re.search(r"linkedin\.com/company/([a-zA-Z0-9\-_]+)", linkedin_url)
            if not slug_match:
                continue
            slug = slug_match.group(1).lower().rstrip("/")

            # Skip if already seen
            if slug in self._seen_slugs:
                continue

            # Extract title and snippet
            title_el = result_div.find("h3")
            title = title_el.get_text(strip=True) if title_el else ""

            # Clean up title — remove " | LinkedIn" suffix
            title = re.sub(r"\s*[|\-]\s*LinkedIn.*$", "", title).strip()

            # Extract snippet/description
            snippet_el = result_div.find("div", class_=re.compile(r"VwiC3b|IsZvec|s3v9rd"))
            if not snippet_el:
                # Try broader snippet search
                for div in result_div.find_all("div"):
                    text = div.get_text(strip=True)
                    if len(text) > 50 and "linkedin.com" not in text.lower():
                        snippet_el = div
                        break

            snippet = snippet_el.get_text(strip=True) if snippet_el else ""

            results.append({
                "url": linkedin_url,
                "slug": slug,
                "title": title,
                "snippet": snippet,
            })

        return results

    def _fetch_linkedin_page(self, url: str) -> Optional[dict]:
        """Fetch a LinkedIn company page and extract available structured data.

        LinkedIn company pages include JSON-LD, Open Graph tags, and
        meta description even without JavaScript rendering.
        """
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        try:
            resp = self.session.get(url, timeout=30, allow_redirects=True)
            if resp.status_code == 999:
                # LinkedIn's anti-scraping status code
                self.logger.debug("LinkedIn returned 999 for %s", url)
                return None
            resp.raise_for_status()
            self.stats["pages_scraped"] += 1
        except Exception as exc:
            self.logger.debug("Failed to fetch LinkedIn page %s: %s", url, exc)
            self.stats["errors"] += 1
            return None

        html = resp.text
        soup = self.parse_html(html)

        data = {
            "name": "",
            "description": "",
            "website": "",
            "industry": "",
            "location": "",
            "logo": "",
            "employees": "",
        }

        # Extract from JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                ld = json.loads(script.string or "")
            except (json.JSONDecodeError, TypeError):
                continue

            items = ld if isinstance(ld, list) else [ld]
            for item in items:
                if item.get("@type") in ("Organization", "Corporation", "LocalBusiness"):
                    data["name"] = (item.get("name") or "").strip()
                    data["description"] = (item.get("description") or "").strip()
                    data["website"] = (item.get("url") or "").strip()
                    addr = item.get("address") or {}
                    if isinstance(addr, dict):
                        locality = (addr.get("addressLocality") or "").strip()
                        region = (addr.get("addressRegion") or "").strip()
                        country = (addr.get("addressCountry") or "").strip()
                        parts = [p for p in [locality, region, country] if p]
                        data["location"] = ", ".join(parts)
                    employees = item.get("numberOfEmployees") or {}
                    if isinstance(employees, dict):
                        data["employees"] = str(employees.get("value") or "")
                    break

        # Fallback: Open Graph tags
        if not data["name"]:
            og_title = soup.find("meta", property="og:title")
            if og_title:
                data["name"] = (og_title.get("content") or "").strip()
                data["name"] = re.sub(r"\s*[|\-]\s*LinkedIn.*$", "", data["name"]).strip()

        if not data["description"]:
            og_desc = soup.find("meta", property="og:description")
            if og_desc:
                data["description"] = (og_desc.get("content") or "").strip()

        # Fallback: meta description
        if not data["description"]:
            meta_desc = soup.find("meta", attrs={"name": "description"})
            if meta_desc:
                data["description"] = (meta_desc.get("content") or "").strip()

        # Try to extract industry from description or page content
        if not data["industry"]:
            # LinkedIn descriptions often follow pattern: "Company | Industry | Location"
            desc = data["description"]
            if " | " in desc:
                parts = desc.split(" | ")
                if len(parts) >= 2:
                    data["industry"] = parts[1].strip()

        return data

    def _build_contact(
        self,
        slug: str,
        google_data: dict,
        linkedin_data: Optional[dict],
        keyword: str,
    ) -> Optional[ScrapedContact]:
        """Build a ScrapedContact from combined Google + LinkedIn data."""
        # Prefer LinkedIn data over Google snippet data
        if linkedin_data and linkedin_data.get("name"):
            name = linkedin_data["name"]
        else:
            name = google_data.get("title", "").strip()

        if not name or len(name) < 2:
            return None

        # Normalize name for dedup
        name_key = re.sub(r"[^\w]", "", name.lower())
        if name_key in self._seen_names:
            return None
        self._seen_names.add(name_key)
        self._seen_slugs.add(slug)

        description = ""
        website = ""
        industry = ""
        location = ""

        if linkedin_data:
            description = linkedin_data.get("description", "")
            website = linkedin_data.get("website", "")
            industry = linkedin_data.get("industry", "")
            location = linkedin_data.get("location", "")

        # If no dedicated website, use LinkedIn URL
        linkedin_url = f"https://www.linkedin.com/company/{slug}/"
        if not website or "linkedin.com" in website:
            website = linkedin_url

        # Use Google snippet as fallback description
        if not description:
            description = google_data.get("snippet", "")

        # Bio
        bio_parts = [name]
        if industry:
            bio_parts.append(f"Industry: {industry}")
        if location:
            bio_parts.append(location)
        if description:
            # Truncate description for bio
            desc_short = description[:200]
            if len(description) > 200:
                desc_short += "..."
            bio_parts.append(desc_short)
        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=name,
            email="",
            company=name,
            website=website,
            linkedin=linkedin_url,
            phone="",
            bio=bio,
            source_category="linkedin_companies",
            raw_data={
                "linkedin_slug": slug,
                "linkedin_url": linkedin_url,
                "industry": industry,
                "location": location,
                "description": description[:500],
                "search_keyword": keyword,
                "employees": (linkedin_data or {}).get("employees", ""),
            },
        )

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Discover LinkedIn company pages via Google search.

        For each industry keyword:
        1. Search Google with site:linkedin.com/company/ filter
        2. Parse search results for LinkedIn company URLs
        3. Optionally fetch each LinkedIn page for structured data
        4. Yield ScrapedContact for each unique company found
        """
        self.logger.info(
            "Starting %s scraper — %d industry keywords",
            self.SOURCE_NAME, len(INDUSTRY_KEYWORDS),
        )

        start_kw_idx = (checkpoint or {}).get("kw_idx", 0)
        contacts_yielded = 0
        pages_done = 0

        for kw_idx, keyword in enumerate(INDUSTRY_KEYWORDS):
            if kw_idx < start_kw_idx:
                continue

            self.logger.info(
                "Keyword: '%s' (%d/%d)",
                keyword, kw_idx + 1, len(INDUSTRY_KEYWORDS),
            )

            consecutive_empty = 0

            for page_num in range(MAX_GOOGLE_PAGES):
                start = page_num * GOOGLE_RESULTS_PER_PAGE
                query = f'site:linkedin.com/company/ "{keyword}"'
                encoded_query = urllib.parse.quote(query)
                google_url = f"https://www.google.com/search?q={encoded_query}&start={start}&num={GOOGLE_RESULTS_PER_PAGE}"

                if self.rate_limiter:
                    self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

                html = self.fetch_page(google_url)
                if not html:
                    break

                # Check for CAPTCHA / block
                if "captcha" in html.lower() or "unusual traffic" in html.lower():
                    self.logger.warning("Google CAPTCHA detected — pausing keyword '%s'", keyword)
                    time.sleep(30)  # Back off
                    break

                google_results = self._parse_google_results(html)

                if not google_results:
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        break
                    continue
                else:
                    consecutive_empty = 0

                for gresult in google_results:
                    slug = gresult["slug"]
                    if slug in self._seen_slugs:
                        continue

                    # Optionally fetch LinkedIn page for extra data
                    # (only for first 5 results per page to save requests)
                    linkedin_data = None
                    if google_results.index(gresult) < 5:
                        linkedin_url = gresult["url"]
                        linkedin_data = self._fetch_linkedin_page(linkedin_url)

                    contact = self._build_contact(slug, gresult, linkedin_data, keyword)
                    if contact and contact.is_valid():
                        contact.source_platform = self.SOURCE_NAME
                        contact.source_url = google_url
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

                if pages_done % 10 == 0:
                    self.logger.info(
                        "Progress: %d pages, %d valid contacts",
                        pages_done, self.stats["contacts_valid"],
                    )

        self.logger.info("Scraper complete: %s", self.stats)
