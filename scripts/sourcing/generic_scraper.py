"""
Universal contact scraper for any directory URL.

Uses crawl4ai for JS-rendered pages, falls back to requests+BeautifulSoup
for simple HTML. Extracts contacts (name, email, website, phone, bio,
LinkedIn, company) using regex patterns and HTML structure analysis.

Designed to handle the 2,800+ directory URLs in the JV partnership lists.
"""

from __future__ import annotations

import asyncio
import csv
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from scripts.sourcing.base import (
    EMAIL_RE,
    JUNK_EMAIL_DOMAINS,
    LINKEDIN_RE,
    ScrapedContact,
)

logger = logging.getLogger("sourcing.generic")

# ---------------------------------------------------------------------------
# Patterns for finding member directory / profile listing pages
# ---------------------------------------------------------------------------

DIRECTORY_LINK_PATTERNS = [
    re.compile(r"(members?|directory|people|team|staff|our-team|about-us|who-we-are)", re.I),
    re.compile(r"(find.a.member|find.a.pro|search.members?|member.search)", re.I),
    re.compile(r"(portfolio|companies|startups|founders?|alumni)", re.I),
    re.compile(r"(speakers?|authors?|experts?|consultants?|coaches?|advisors?)", re.I),
    re.compile(r"(partners?|agencies?|providers?|vendors?|certified)", re.I),
    re.compile(r"(profile|roster|listing|browse|explore|discover)", re.I),
]

# Patterns that indicate a page lists people/companies
LISTING_PAGE_INDICATORS = [
    re.compile(r"class=[\"'][^\"']*(?:card|profile|member|listing|result|person|team)[^\"']*[\"']", re.I),
    re.compile(r"class=[\"'][^\"']*(?:grid|directory|roster|gallery)[^\"']*[\"']", re.I),
]

# Pagination patterns
PAGINATION_PATTERNS = [
    re.compile(r"[?&]page=(\d+)", re.I),
    re.compile(r"/page/(\d+)", re.I),
    re.compile(r"[?&]p=(\d+)", re.I),
    re.compile(r"[?&]offset=(\d+)", re.I),
    re.compile(r"[?&]start=(\d+)", re.I),
]

NEXT_PAGE_LINK_PATTERNS = [
    re.compile(r"(next|Next|NEXT|›|»|→|next.page|load.more)", re.I),
    re.compile(r'rel=["\']next["\']', re.I),
    re.compile(r'class=["\'][^"\']*next[^"\']*["\']', re.I),
    re.compile(r'aria-label=["\'].*next.*["\']', re.I),
]

# Phone regex
PHONE_RE = re.compile(
    r"(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}"
)

# Name patterns (common in profile cards)
NAME_PATTERNS = [
    re.compile(r"<h[1-4][^>]*>([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})</h[1-4]>"),
    re.compile(r'class=["\'][^"\']*name[^"\']*["\'][^>]*>([^<]+)<'),
]


# ---------------------------------------------------------------------------
# Contact extraction from HTML blocks
# ---------------------------------------------------------------------------

def extract_contacts_from_html(html: str, base_url: str, source_name: str = "") -> list[ScrapedContact]:
    """Extract contacts from an HTML page using structural analysis."""
    soup = BeautifulSoup(html, "html.parser")
    contacts = []
    seen_names = set()

    # Strategy 1: Look for profile/member cards (divs with structured data)
    card_selectors = [
        {"class_": re.compile(r"card|profile|member|person|team-member|speaker|author|expert|partner|listing|result", re.I)},
    ]

    cards = []
    for sel in card_selectors:
        cards.extend(soup.find_all(["div", "article", "li", "section", "tr"], **sel))

    # Also look for cards within grid/list containers
    containers = soup.find_all(["div", "ul", "section"], class_=re.compile(
        r"grid|directory|roster|team|members|results|listings|cards|people|partners", re.I
    ))
    for container in containers:
        # Get direct children that look like cards
        children = container.find_all(["div", "article", "li", "section"], recursive=False)
        if len(children) >= 3:  # Likely a listing if 3+ similar children
            cards.extend(children)

    base_domain = urlparse(base_url).netloc.lower().replace("www.", "")

    if cards:
        for card in cards:
            contact = _extract_from_card(card, base_url)
            if contact and contact.name not in seen_names:
                # Quality gate: require at least one real contact signal
                # An external website (different domain), email, phone, or LinkedIn
                has_real_signal = False
                if contact.email:
                    has_real_signal = True
                elif contact.phone:
                    has_real_signal = True
                elif contact.linkedin:
                    has_real_signal = True
                elif contact.website:
                    ws_domain = urlparse(contact.website).netloc.lower().replace("www.", "")
                    if ws_domain and ws_domain != base_domain:
                        has_real_signal = True

                if not has_real_signal:
                    continue

                contact.source_platform = source_name
                seen_names.add(contact.name)
                contacts.append(contact)

    # Strategy 2: Look for structured data (JSON-LD, microdata)
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            import json
            data = json.loads(script.string or "")
            ld_contacts = _extract_from_jsonld(data, base_url, source_name)
            for c in ld_contacts:
                if c.name not in seen_names:
                    seen_names.add(c.name)
                    contacts.append(c)
        except (json.JSONDecodeError, TypeError):
            pass

    # Strategy 3: Table-based directories
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        if len(rows) >= 3:
            table_contacts = _extract_from_table(table, base_url, source_name)
            for c in table_contacts:
                if c.name not in seen_names:
                    seen_names.add(c.name)
                    contacts.append(c)

    # Strategy 4: If no structured cards found, scan entire page for
    # name + email/website combos
    if not contacts:
        contacts = _extract_from_freetext(html, base_url, source_name)

    return contacts


def _looks_like_name_or_company(text: str) -> bool:
    """Check if text looks like a person name or company name vs a section heading."""
    text = text.strip()
    if len(text) < 2 or len(text) > 80:
        return False
    # Reject common section headings / marketing phrases
    reject_patterns = [
        r"^(about|contact|home|services|products|features|pricing|blog|news|faq|help)",
        r"^(get started|learn more|sign up|join|subscribe|download|view|explore|discover)",
        r"^(our|the|a|an|this|that|these|those|your|my|we|they)\s",
        r"^(build|create|grow|become|considering|platform|overview)",
        r"(platform|overview|solution|framework|powered by|copyright)",
        r"^\d+",  # Starts with number
        r"^(why|how|what|when|where)\s",
    ]
    for pat in reject_patterns:
        if re.search(pat, text, re.I):
            return False
    # Should contain at least one letter
    if not re.search(r"[a-zA-Z]", text):
        return False
    return True


def _extract_from_card(card, base_url: str) -> Optional[ScrapedContact]:
    """Extract a contact from a single profile/member card element."""
    card_html = str(card)
    card_text = card.get_text(separator=" ", strip=True)

    # Name: look in headings first, then name-classed elements
    name = ""
    for tag in card.find_all(["h1", "h2", "h3", "h4", "h5", "a", "span", "p", "strong"]):
        tag_class = " ".join(tag.get("class", []))
        tag_text = tag.get_text(strip=True)

        if not _looks_like_name_or_company(tag_text):
            continue

        # Check if it has a name-related class
        if re.search(r"name|title|author|speaker|person", tag_class, re.I):
            name = tag_text
            break

        # Check if text looks like a person name (2-4 capitalized words)
        if tag.name in ("h2", "h3", "h4", "strong") and re.match(
            r"^[A-Z][a-z]+(?:\s+[A-Z]\.?)(?:\s+[A-Z][a-z]+){0,2}$", tag_text
        ):
            name = tag_text
            break

    # Also check for company/organization names if no person name
    company = ""
    for tag in card.find_all(["span", "p", "div", "small"]):
        tag_class = " ".join(tag.get("class", []))
        tag_text = tag.get_text(strip=True)
        if re.search(r"company|org|firm|business|employer", tag_class, re.I) and tag_text:
            company = tag_text[:150]
            break

    if not name:
        # Try headings without class check — but require name-like text
        for h in card.find_all(["h2", "h3", "h4"]):
            txt = h.get_text(strip=True)
            if _looks_like_name_or_company(txt):
                name = txt
                break

    if not name:
        return None

    # Email
    emails = EMAIL_RE.findall(card_html)
    email = ""
    for e in emails:
        domain = e.split("@")[-1]
        if domain not in JUNK_EMAIL_DOMAINS:
            email = e.lower()
            break

    # Website
    website = ""
    linkedin = ""
    for a in card.find_all("a", href=True):
        href = a["href"]
        if not href.startswith("http"):
            href = urljoin(base_url, href)

        href_lower = href.lower()
        if "linkedin.com/in/" in href_lower and not linkedin:
            linkedin = href
        elif (
            href.startswith("http")
            and urlparse(base_url).netloc not in href_lower
            and "facebook.com" not in href_lower
            and "twitter.com" not in href_lower
            and "instagram.com" not in href_lower
            and "youtube.com" not in href_lower
            and not website
        ):
            website = href

    # Phone
    phones = PHONE_RE.findall(card_text)
    phone = phones[0] if phones else ""

    # Bio
    bio = ""
    for p in card.find_all(["p", "div"]):
        p_class = " ".join(p.get("class", []))
        p_text = p.get_text(strip=True)
        if (
            re.search(r"bio|desc|about|summary|excerpt|overview", p_class, re.I)
            and len(p_text) > 20
        ):
            bio = p_text[:1000]
            break
    if not bio:
        # Get longest paragraph as bio
        paras = [p.get_text(strip=True) for p in card.find_all("p") if len(p.get_text(strip=True)) > 30]
        if paras:
            bio = max(paras, key=len)[:1000]

    # JV-relevant fields: location, categories, revenue signals, pricing, ratings
    location = ""
    for tag in card.find_all(["span", "p", "div", "small", "address"]):
        tag_class = " ".join(tag.get("class", []))
        tag_text = tag.get_text(strip=True)
        if re.search(r"location|address|city|region|country|geo|place|locale", tag_class, re.I) and tag_text:
            location = tag_text[:150]
            break

    categories = ""
    for tag in card.find_all(["span", "p", "div", "small", "a"]):
        tag_class = " ".join(tag.get("class", []))
        tag_text = tag.get_text(strip=True)
        if re.search(r"categor|industry|sector|service|specialt|expertise|skill|tag|badge", tag_class, re.I) and tag_text:
            categories = tag_text[:200]
            break

    # Revenue/size signals: employee count, deal size, revenue
    revenue_indicator = ""
    revenue_re = re.compile(
        r"(\$[\d,.]+[MBKmk]?|\d+\+?\s*employees?|\d+\s*staff|revenue|annual|funding|raised|ARR|MRR)",
        re.I,
    )
    card_fulltext = card.get_text(separator=" ", strip=True)
    rev_match = revenue_re.search(card_fulltext)
    if rev_match:
        # Grab surrounding context (up to 100 chars around the match)
        start = max(0, rev_match.start() - 30)
        end = min(len(card_fulltext), rev_match.end() + 70)
        revenue_indicator = card_fulltext[start:end].strip()

    # Rating
    rating = ""
    for tag in card.find_all(["span", "div", "p"]):
        tag_class = " ".join(tag.get("class", []))
        tag_text = tag.get_text(strip=True)
        if re.search(r"rating|stars?|score|review", tag_class, re.I) and tag_text:
            # Extract numeric rating
            m = re.search(r"(\d+(?:\.\d+)?)\s*(?:/\s*\d+|stars?)?", tag_text)
            if m:
                rating = m.group(1)
                break

    # Pricing
    pricing = ""
    for tag in card.find_all(["span", "div", "p"]):
        tag_class = " ".join(tag.get("class", []))
        tag_text = tag.get_text(strip=True)
        if re.search(r"price|cost|fee|rate|starting.at|from\s*\$", tag_class + " " + tag_text, re.I):
            price_match = re.search(r"\$[\d,.]+(?:\s*/\s*\w+)?", tag_text)
            if price_match:
                pricing = price_match.group()
                break

    # Partner tier
    tier = ""
    for tag in card.find_all(["span", "div", "p", "img"]):
        tag_class = " ".join(tag.get("class", []))
        tag_text = tag.get_text(strip=True) if tag.name != "img" else (tag.get("alt", "") or tag.get("title", ""))
        if re.search(r"tier|level|badge|partner|certif|status|rank", tag_class, re.I) and tag_text:
            tier = tag_text[:80]
            break

    # Product focus
    product_focus = ""
    for tag in card.find_all(["span", "p", "div"]):
        tag_class = " ".join(tag.get("class", []))
        tag_text = tag.get_text(strip=True)
        if re.search(r"product|service|offering|solution|focus|specialty", tag_class, re.I) and tag_text:
            product_focus = tag_text[:200]
            break

    return ScrapedContact(
        name=name,
        email=email,
        company=company,
        website=website,
        linkedin=linkedin,
        phone=phone,
        bio=bio,
        location=location,
        categories=categories,
        revenue_indicator=revenue_indicator,
        rating=rating,
        pricing=pricing,
        tier=tier,
        product_focus=product_focus,
    )


def _extract_from_jsonld(data, base_url: str, source_name: str) -> list[ScrapedContact]:
    """Extract contacts from JSON-LD structured data."""
    contacts = []

    if isinstance(data, list):
        for item in data:
            contacts.extend(_extract_from_jsonld(item, base_url, source_name))
        return contacts

    if not isinstance(data, dict):
        return contacts

    dtype = data.get("@type", "")
    if dtype in ("Person", "Organization", "LocalBusiness", "ProfessionalService"):
        name = data.get("name", "")
        if name and len(name) >= 2:
            email = data.get("email", "")
            website = data.get("url", "") or data.get("sameAs", "")
            if isinstance(website, list):
                website = website[0] if website else ""
            phone = data.get("telephone", "")
            bio = data.get("description", "")[:1000] if data.get("description") else ""

            linkedin = ""
            same_as = data.get("sameAs", [])
            if isinstance(same_as, str):
                same_as = [same_as]
            for url in same_as:
                if "linkedin.com/in/" in url.lower():
                    linkedin = url
                    break

            contacts.append(ScrapedContact(
                name=name,
                email=email or "",
                website=website if isinstance(website, str) else "",
                phone=phone or "",
                bio=bio,
                linkedin=linkedin,
                source_platform=source_name,
            ))

    # Recurse into nested objects
    for key, val in data.items():
        if isinstance(val, (dict, list)):
            contacts.extend(_extract_from_jsonld(val, base_url, source_name))

    return contacts


def _extract_from_table(table, base_url: str, source_name: str) -> list[ScrapedContact]:
    """Extract contacts from HTML table."""
    contacts = []
    rows = table.find_all("tr")
    if len(rows) < 2:
        return contacts

    # Try to identify header row
    headers = []
    header_row = rows[0]
    for th in header_row.find_all(["th", "td"]):
        headers.append(th.get_text(strip=True).lower())

    # Map columns to fields
    col_map = {}
    for i, h in enumerate(headers):
        if re.search(r"name|company|organization|firm", h):
            col_map.setdefault("name", i)
        elif re.search(r"email|e-mail", h):
            col_map["email"] = i
        elif re.search(r"website|url|web", h):
            col_map["website"] = i
        elif re.search(r"phone|tel|mobile", h):
            col_map["phone"] = i
        elif re.search(r"linkedin", h):
            col_map["linkedin"] = i
        elif re.search(r"bio|description|about", h):
            col_map["bio"] = i
        elif re.search(r"company|org|firm", h):
            col_map["company"] = i

    if "name" not in col_map:
        return contacts

    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        if len(cells) <= col_map["name"]:
            continue

        name = cells[col_map["name"]].get_text(strip=True)
        if not name or len(name) < 2:
            continue

        contact = ScrapedContact(
            name=name,
            email=cells[col_map["email"]].get_text(strip=True) if "email" in col_map and len(cells) > col_map["email"] else "",
            website=cells[col_map["website"]].get_text(strip=True) if "website" in col_map and len(cells) > col_map["website"] else "",
            phone=cells[col_map["phone"]].get_text(strip=True) if "phone" in col_map and len(cells) > col_map["phone"] else "",
            linkedin=cells[col_map["linkedin"]].get_text(strip=True) if "linkedin" in col_map and len(cells) > col_map["linkedin"] else "",
            bio=cells[col_map["bio"]].get_text(strip=True)[:1000] if "bio" in col_map and len(cells) > col_map["bio"] else "",
            company=cells[col_map["company"]].get_text(strip=True) if "company" in col_map and len(cells) > col_map["company"] else "",
            source_platform=source_name,
        )
        contacts.append(contact)

    return contacts


def _extract_from_freetext(html: str, base_url: str, source_name: str) -> list[ScrapedContact]:
    """Last resort: extract contacts from unstructured page text."""
    soup = BeautifulSoup(html, "html.parser")
    contacts = []

    # Find all emails on the page
    emails = set(EMAIL_RE.findall(html))
    emails = {e for e in emails if e.split("@")[-1] not in JUNK_EMAIL_DOMAINS}

    # For each email, try to find a nearby name
    for email in list(emails)[:100]:  # Cap at 100
        # Find the element containing this email
        el = soup.find(string=re.compile(re.escape(email)))
        if not el:
            continue

        # Look for name in parent/sibling elements
        parent = el.parent
        name = ""
        for _ in range(5):  # Walk up 5 levels
            if parent is None:
                break
            # Check headings in this context
            for h in parent.find_all(["h1", "h2", "h3", "h4", "strong", "b"]):
                txt = h.get_text(strip=True)
                if 3 <= len(txt) <= 60 and re.match(r"^[A-Z]", txt):
                    name = txt
                    break
            if name:
                break
            parent = parent.parent

        if not name:
            # Use email prefix as name hint
            name = email.split("@")[0].replace(".", " ").replace("_", " ").title()

        contacts.append(ScrapedContact(
            name=name,
            email=email,
            source_platform=source_name,
        ))

    return contacts


# ---------------------------------------------------------------------------
# Pagination discovery
# ---------------------------------------------------------------------------

def find_next_page_url(html: str, current_url: str) -> Optional[str]:
    """Find the next page URL from pagination elements."""
    soup = BeautifulSoup(html, "html.parser")

    # Look for "next" links
    for a in soup.find_all("a", href=True):
        a_text = a.get_text(strip=True)
        a_html = str(a)

        for pattern in NEXT_PAGE_LINK_PATTERNS:
            if pattern.search(a_text) or pattern.search(a_html):
                href = a["href"]
                if not href.startswith("http"):
                    href = urljoin(current_url, href)
                if href != current_url:
                    return href

    # Look for page number links (find current + 1)
    parsed = urlparse(current_url)
    current_page = 1
    for pattern in PAGINATION_PATTERNS:
        m = pattern.search(current_url)
        if m:
            current_page = int(m.group(1))
            break

    # Find link to current_page + 1
    next_page = current_page + 1
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.startswith("http"):
            href = urljoin(current_url, href)

        for pattern in PAGINATION_PATTERNS:
            m = pattern.search(href)
            if m and int(m.group(1)) == next_page:
                return href

    return None


def find_directory_links(html: str, base_url: str) -> list[str]:
    """Find links on a page that likely lead to member directories."""
    soup = BeautifulSoup(html, "html.parser")
    directory_links = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        a_text = a.get_text(strip=True)

        if not href.startswith("http"):
            href = urljoin(base_url, href)

        # Stay on same domain
        if urlparse(href).netloc != urlparse(base_url).netloc:
            continue

        # Check if link text or URL matches directory patterns
        combined = f"{href} {a_text}"
        for pattern in DIRECTORY_LINK_PATTERNS:
            if pattern.search(combined):
                directory_links.append(href)
                break

    return list(set(directory_links))


# ---------------------------------------------------------------------------
# Main scraper class
# ---------------------------------------------------------------------------

class GenericDirectoryScraper:
    """Universal scraper that handles any directory URL."""

    def __init__(self, max_pages_per_site: int = 50, use_crawl4ai: bool = True):
        self.max_pages = max_pages_per_site
        self.use_crawl4ai = use_crawl4ai
        self.session = self._build_session()
        self.stats = {
            "sites_attempted": 0,
            "sites_with_contacts": 0,
            "total_contacts": 0,
            "errors": 0,
        }

    def _build_session(self) -> requests.Session:
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()
        retry = Retry(total=2, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        return session

    def _fetch_html(self, url: str, timeout: int = 10) -> Optional[str]:
        """Fetch page with requests (no JS rendering)."""
        try:
            resp = self.session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            logger.debug("requests fetch failed for %s: %s", url, e)
            return None

    async def _fetch_with_crawl4ai(self, url: str) -> Optional[str]:
        """Fetch page with crawl4ai (JS rendering with wait for content)."""
        try:
            from crawl4ai import AsyncWebCrawler, CrawlerRunConfig

            config = CrawlerRunConfig(
                wait_until="networkidle",
                page_timeout=30000,
                verbose=False,
            )
            async with AsyncWebCrawler(verbose=False) as crawler:
                result = await crawler.arun(url=url, config=config)
                if result and result.html and len(result.html) > 500:
                    return result.html
                return None
        except Exception as e:
            logger.debug("crawl4ai fetch failed for %s: %s", url, e)
            return None

    def scrape_site(self, url: str, name: str, category: str = "") -> list[ScrapedContact]:
        """Scrape a single directory site for contacts.

        1. Fetch the landing page
        2. Look for directory/member listing links
        3. Scrape listing pages with pagination
        4. Return all found contacts
        """
        self.stats["sites_attempted"] += 1
        logger.info("Scraping site: %s (%s)", name, url)
        all_contacts = []
        seen_urls = set()

        # Step 1: Fetch landing page
        html = self._fetch_html(url)
        if not html:
            # Try with crawl4ai for JS-rendered sites
            if self.use_crawl4ai:
                html = asyncio.run(self._fetch_with_crawl4ai(url))
            if not html:
                logger.warning("Could not fetch %s", url)
                self.stats["errors"] += 1
                return []

        seen_urls.add(url)

        # Step 2: Try to extract contacts from landing page
        contacts = extract_contacts_from_html(html, url, name)
        if contacts:
            all_contacts.extend(contacts)
            logger.info("Found %d contacts on landing page of %s", len(contacts), name)

        # Step 3: Find directory/listing subpages
        dir_links = find_directory_links(html, url)
        logger.info("Found %d potential directory links on %s", len(dir_links), name)

        # Step 4: Scrape directory pages with pagination
        pages_scraped = 1
        urls_to_scrape = dir_links[:10]  # Limit initial directory links

        while urls_to_scrape and pages_scraped < self.max_pages:
            page_url = urls_to_scrape.pop(0)
            if page_url in seen_urls:
                continue
            seen_urls.add(page_url)

            time.sleep(0.5)  # Rate limiting (reduced for parallel runs)

            page_html = self._fetch_html(page_url)
            if not page_html:
                if self.use_crawl4ai:
                    page_html = asyncio.run(self._fetch_with_crawl4ai(page_url))
                if not page_html:
                    continue

            pages_scraped += 1
            page_contacts = extract_contacts_from_html(page_html, page_url, name)
            all_contacts.extend(page_contacts)

            if page_contacts:
                logger.info("Found %d contacts on %s (page %d)", len(page_contacts), page_url, pages_scraped)

                # Look for next page
                next_url = find_next_page_url(page_html, page_url)
                if next_url and next_url not in seen_urls:
                    urls_to_scrape.insert(0, next_url)  # Priority: follow pagination

        # Deduplicate by name
        seen_names = set()
        unique_contacts = []
        for c in all_contacts:
            key = c.name.lower().strip()
            if key not in seen_names:
                seen_names.add(key)
                c.source_category = category
                c.scraped_at = datetime.now().isoformat()
                unique_contacts.append(c)

        if unique_contacts:
            self.stats["sites_with_contacts"] += 1
            self.stats["total_contacts"] += len(unique_contacts)

        logger.info(
            "Site %s complete: %d unique contacts from %d pages",
            name, len(unique_contacts), pages_scraped,
        )
        return unique_contacts


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

def export_contacts_to_csv(contacts: list[ScrapedContact], filepath: str):
    """Export contacts to CSV file."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    fieldnames = [
        "name", "email", "company", "website", "linkedin",
        "phone", "bio",
        "pricing", "rating", "review_count", "tier",
        "categories", "location", "product_focus", "revenue_indicator",
        "source_platform", "source_url", "source_category", "scraped_at",
    ]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for c in contacts:
            writer.writerow({
                "name": c.name,
                "email": c.email,
                "company": c.company,
                "website": c.website,
                "linkedin": c.linkedin,
                "phone": c.phone,
                "bio": c.bio[:500] if c.bio else "",
                "pricing": c.pricing,
                "rating": c.rating,
                "review_count": c.review_count,
                "tier": c.tier,
                "categories": c.categories,
                "location": c.location,
                "product_focus": c.product_focus,
                "revenue_indicator": c.revenue_indicator,
                "source_platform": c.source_platform,
                "source_url": c.source_url,
                "source_category": c.source_category,
                "scraped_at": c.scraped_at,
            })

    logger.info("Exported %d contacts to %s", len(contacts), filepath)


# ---------------------------------------------------------------------------
# CLI for testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

    if len(sys.argv) < 2:
        print("Usage: python -m scripts.sourcing.generic_scraper <url> [name] [output.csv]")
        sys.exit(1)

    url = sys.argv[1]
    name = sys.argv[2] if len(sys.argv) > 2 else urlparse(url).netloc
    output = sys.argv[3] if len(sys.argv) > 3 else f"Filling Database/partners/{name}.csv"

    scraper = GenericDirectoryScraper(max_pages_per_site=30)
    contacts = scraper.scrape_site(url, name)

    if contacts:
        export_contacts_to_csv(contacts, output)
        print(f"\nExported {len(contacts)} contacts to {output}")
    else:
        print(f"\nNo contacts found on {url}")

    print(f"Stats: {scraper.stats}")
