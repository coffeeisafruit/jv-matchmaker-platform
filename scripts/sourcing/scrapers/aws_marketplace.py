"""
AWS Marketplace scraper.

AWS Marketplace (aws.amazon.com/marketplace) hosts 10,000+ software
products from ISVs (Independent Software Vendors) that run on AWS.

The marketplace has a search API used by the frontend:
  https://aws.amazon.com/marketplace/api/awsmpsearch/

The API returns structured JSON with:
- Product title and vendor/company name
- Description, categories
- Pricing model, delivery method
- Rating and review count
- Company website and logo

Estimated yield: 5,000-10,000 ISV/SaaS companies
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Iterator, Optional
from urllib.parse import urlencode, quote_plus

from scripts.sourcing.base import BaseScraper, ScrapedContact


# AWS product categories for search
CATEGORIES = [
    "Infrastructure Software",
    "Business Applications",
    "Machine Learning",
    "IoT",
    "Data Products",
    "DevOps",
    "Security",
    "Networking",
    "Storage",
    "Analytics",
    "Databases",
    "Migration",
    "Financial Services",
    "Healthcare",
    "Media",
    "Education",
    "Government",
    "Retail",
    "Manufacturing",
]

# Additional search terms
SEARCH_TERMS = [
    "SaaS", "monitoring", "backup", "container",
    "serverless", "kubernetes", "CI/CD", "testing",
    "logging", "API gateway", "firewall", "VPN",
    "data lake", "ETL", "streaming", "messaging",
    "CRM", "ERP", "billing", "compliance",
    "identity", "encryption", "WAF", "CDN",
    "disaster recovery", "load balancer",
    "chatbot", "NLP", "computer vision",
    "blockchain", "edge computing",
    "observability", "APM", "SIEM",
    "antivirus", "DLP", "vulnerability scanner",
]


class Scraper(BaseScraper):
    """AWS Marketplace scraper.

    Browses the marketplace search pages and uses the search API
    to extract ISV/vendor company information.
    """

    SOURCE_NAME = "aws_marketplace"
    BASE_URL = "https://aws.amazon.com/marketplace"
    SEARCH_API = "https://aws.amazon.com/marketplace/api/awsmpsearch/search"
    REQUESTS_PER_MINUTE = 8

    def __init__(self, rate_limiter=None):
        super().__init__(rate_limiter=rate_limiter)
        self._seen_ids: set[str] = set()
        self._seen_vendors: set[str] = set()

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Not used -- we override run() for API + HTML combination."""
        return iter([])

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Not used -- parsing is done inline in run()."""
        return []

    def _search_api(
        self,
        query: str = "",
        category: str = "",
        page: int = 0,
        page_size: int = 50,
    ) -> Optional[dict]:
        """Try the AWS Marketplace search API."""
        if self.rate_limiter:
            self.rate_limiter.wait(self.SOURCE_NAME, self.REQUESTS_PER_MINUTE)

        # Try the JSON search API
        params = {
            "page": page,
            "pageSize": page_size,
            "searchTerms": query,
        }
        if category:
            params["category"] = category

        try:
            resp = self.session.get(
                self.SEARCH_API,
                params=params,
                timeout=30,
                headers={
                    "Accept": "application/json",
                    "Referer": f"{self.BASE_URL}/search",
                },
            )
            if resp.status_code == 200:
                self.stats["pages_scraped"] += 1
                return resp.json()
        except Exception:
            pass

        return None

    def _search_html(self, query: str = "", page: int = 1) -> Optional[str]:
        """Fetch AWS Marketplace search results as HTML."""
        params = {}
        if query:
            params["searchTerms"] = query
        if page > 1:
            params["page"] = page

        url = f"{self.BASE_URL}/search/results"
        if params:
            url += f"?{urlencode(params)}"

        return self.fetch_page(url)

    def _parse_search_results_html(self, html: str, url: str) -> list[ScrapedContact]:
        """Parse AWS Marketplace search results HTML."""
        contacts = []

        # Try embedded JSON first
        for pattern in [
            r'window\.__PRELOADED_STATE__\s*=\s*({.*?});\s*</script>',
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            r'"searchResults"\s*:\s*(\[{.*?}\])',
        ]:
            match = re.search(pattern, html, re.DOTALL)
            if match:
                try:
                    data = json.loads(match.group(1))
                    if isinstance(data, dict):
                        results = (
                            data.get("searchResults")
                            or data.get("results")
                            or data.get("props", {}).get("pageProps", {}).get("results", [])
                        )
                    elif isinstance(data, list):
                        results = data
                    else:
                        continue

                    for result in results:
                        if isinstance(result, dict):
                            contact = self._result_to_contact(result, url)
                            if contact:
                                contacts.append(contact)
                    if contacts:
                        return contacts
                except json.JSONDecodeError:
                    continue

        # Fallback: HTML parsing
        soup = self.parse_html(html)

        cards = (
            soup.select("[class*='search-result']")
            or soup.select("[class*='ProductCard']")
            or soup.select("[class*='product-card']")
            or soup.select("[data-testid*='search-result']")
            or soup.select("article")
        )

        for card in cards:
            name_el = (
                card.select_one("h2")
                or card.select_one("h3")
                or card.select_one("[class*='title']")
                or card.select_one("[class*='name']")
            )
            if not name_el:
                continue

            name = name_el.get_text(strip=True)
            if not name or len(name) < 2:
                continue
            if name in self._seen_ids:
                continue
            self._seen_ids.add(name)

            # Vendor
            vendor_el = (
                card.select_one("[class*='vendor']")
                or card.select_one("[class*='seller']")
                or card.select_one("[class*='company']")
                or card.select_one("[class*='publisher']")
            )
            company = vendor_el.get_text(strip=True) if vendor_el else name

            # Link
            link_el = card.select_one("a[href]")
            listing_url = ""
            if link_el:
                href = link_el.get("href", "")
                if href.startswith("/"):
                    listing_url = f"https://aws.amazon.com{href}"
                elif href.startswith("http"):
                    listing_url = href

            # Description
            desc_el = card.select_one("[class*='description']") or card.select_one("p")
            description = desc_el.get_text(strip=True) if desc_el else ""

            # Pricing
            price_el = card.select_one("[class*='pricing']") or card.select_one("[class*='price']")
            pricing = price_el.get_text(strip=True) if price_el else ""

            bio_parts = []
            if description:
                bio_parts.append(description[:500])
            if pricing:
                bio_parts.append(f"Pricing: {pricing}")
            if not bio_parts:
                bio_parts.append("AWS Marketplace product")
            bio = " | ".join(bio_parts)

            contacts.append(ScrapedContact(
                name=company,
                company=company,
                website=listing_url or url,
                bio=bio,
                source_url=listing_url or url,
                source_category="cloud_marketplace",
                raw_data={
                    "product_name": name,
                    "platform": "aws_marketplace",
                },
            ))

        return contacts

    def _result_to_contact(self, result: dict, url: str) -> Optional[ScrapedContact]:
        """Convert a search result dict to ScrapedContact."""
        name = (
            result.get("title")
            or result.get("name")
            or result.get("productTitle")
            or ""
        ).strip()
        if not name or len(name) < 2:
            return None

        product_id = (
            result.get("id")
            or result.get("productId")
            or result.get("listingId")
            or name.lower()
        ).strip()
        if product_id in self._seen_ids:
            return None
        self._seen_ids.add(product_id)

        # Vendor
        vendor = (
            result.get("vendor")
            or result.get("vendorName")
            or result.get("sellerName")
            or result.get("company")
            or {}
        )
        if isinstance(vendor, str):
            company = vendor
            vendor_url = ""
        elif isinstance(vendor, dict):
            company = (vendor.get("name") or vendor.get("displayName") or "").strip()
            vendor_url = (vendor.get("url") or vendor.get("website") or "").strip()
        else:
            company = name
            vendor_url = ""

        company = company or name

        description = (
            result.get("description")
            or result.get("shortDescription")
            or result.get("summary")
            or ""
        ).strip()

        # Categories
        categories = result.get("categories") or result.get("productCategories") or []
        if isinstance(categories, list):
            cat_names = []
            for c in categories:
                if isinstance(c, str):
                    cat_names.append(c)
                elif isinstance(c, dict):
                    cat_names.append((c.get("name") or c.get("displayName") or "").strip())
            categories = [c for c in cat_names if c]

        # Pricing
        pricing = (result.get("pricingModel") or result.get("pricing") or "").strip()

        # Rating
        rating = result.get("rating") or result.get("averageRating") or 0
        review_count = result.get("reviewCount") or result.get("numReviews") or 0

        # Listing URL
        slug = (result.get("slug") or result.get("urlSlug") or "").strip()
        listing_url = ""
        if slug:
            listing_url = f"{self.BASE_URL}/pp/{slug}"
        elif product_id and product_id != name.lower():
            listing_url = f"{self.BASE_URL}/pp/{product_id}"

        website = vendor_url or listing_url or url

        bio_parts = []
        if description:
            bio_parts.append(description[:500])
        if categories:
            bio_parts.append(f"Categories: {', '.join(categories[:5])}")
        if pricing:
            bio_parts.append(f"Pricing: {pricing}")
        if rating:
            bio_parts.append(f"Rating: {rating}/5")
        if review_count:
            bio_parts.append(f"{review_count} reviews")
        if not bio_parts:
            bio_parts.append("AWS Marketplace product")
        bio = " | ".join(bio_parts)

        return ScrapedContact(
            name=company,
            company=company,
            website=website,
            bio=bio,
            source_url=listing_url or url,
            source_category="cloud_marketplace",
            raw_data={
                "product_name": name,
                "product_id": product_id,
                "categories": categories,
                "pricing": pricing,
                "rating": rating,
                "review_count": review_count,
                "platform": "aws_marketplace",
            },
        )

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Scrape AWS Marketplace via API and HTML pages.

        Tries the search API first, falls back to HTML page scraping.
        """
        self.logger.info("Starting %s scraper", self.SOURCE_NAME)

        contacts_yielded = 0
        pages_done = 0

        # Resume from checkpoint
        start_from = (checkpoint or {}).get("last_term", "")
        past_checkpoint = not start_from

        # Phase 1: Search by category names
        all_search_terms = list(CATEGORIES) + list(SEARCH_TERMS)

        for term in all_search_terms:
            if not past_checkpoint:
                if term == start_from:
                    past_checkpoint = True
                else:
                    continue

            for page in range(0, 5):
                # Try API first
                api_data = self._search_api(query=term, page=page)

                if api_data and isinstance(api_data, dict):
                    results = (
                        api_data.get("results")
                        or api_data.get("searchResults")
                        or api_data.get("items")
                        or []
                    )

                    if not results:
                        break

                    for result in results:
                        if not isinstance(result, dict):
                            continue
                        contact = self._result_to_contact(result, f"{self.BASE_URL}/search?searchTerms={quote_plus(term)}")
                        if not contact:
                            continue

                        contact.source_platform = self.SOURCE_NAME
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
                else:
                    # Fallback to HTML
                    html = self._search_html(query=term, page=page + 1)
                    if not html:
                        break

                    page_contacts = self._parse_search_results_html(html, f"{self.BASE_URL}/search?searchTerms={quote_plus(term)}")

                    if not page_contacts:
                        break

                    for contact in page_contacts:
                        contact.source_platform = self.SOURCE_NAME
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

                pages_done += 1

                if pages_done % 10 == 0:
                    self.logger.info(
                        "Progress: %d pages, %d valid contacts (search: '%s')",
                        pages_done, self.stats["contacts_valid"], term,
                    )

                if max_pages and pages_done >= max_pages:
                    self.logger.info("Reached max_pages=%d", max_pages)
                    self.logger.info("Scraper complete: %s", self.stats)
                    return

        self.logger.info("Scraper complete: %s", self.stats)
