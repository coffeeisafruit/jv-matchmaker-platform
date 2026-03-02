"""
Generic professional association member directory scraper.

Targets trade associations and professional societies that have public
"Find a Member" / "Member Directory" pages. Reads association URLs from
the master CSV at Filling Database/directories/jv_directories_master.csv
(2,525 associations with has_public_directory=yes).

Strategy per association URL:
  1. Fetch the landing page.
  2. Discover "member directory" links via keyword matching in anchor hrefs/text.
  3. Follow discovered links and scrape member listings.
  4. Handle pagination (next-page links, page=N query params).
  5. Extract contacts from member cards.

Common directory patterns:
  /find-a-member, /member-directory, /directory, /members,
  /find-a-professional, /search, /our-members, /roster

Uses requests + BeautifulSoup (no JS rendering).
Estimated yield: highly variable per association (0-5,000 members each).
"""

from __future__ import annotations

import re
from typing import Iterator, Optional
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

from scripts.sourcing.base import BaseScraper, ScrapedContact


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Keywords to identify directory links on an association's homepage.
# Ordered roughly by specificity so we can prefer the best matches.
DIRECTORY_LINK_KEYWORDS = [
    "find-a-member",
    "find a member",
    "member-directory",
    "member directory",
    "find-a-professional",
    "find a professional",
    "member-search",
    "member search",
    "our-members",
    "our members",
    "directory",
    "roster",
    "find-a-",
    "find a ",
    "member-listing",
    "member listing",
    "who-we-are",
    "who we are",
    "people",
    "professionals",
    "our-team",
    "our team",
    "staff",
    "search",
    "members",
]

# href substrings that strongly indicate a member directory path
DIRECTORY_HREF_PATTERNS = [
    "find-a-member",
    "findamember",
    "member-directory",
    "memberdirectory",
    "find-a-professional",
    "findaprofessional",
    "member-search",
    "membersearch",
    "our-members",
    "ourmembers",
    "/directory",
    "/roster",
    "/members",
    "/people",
    "/search",
    "/find",
    "member-listing",
    "memberlisting",
]

# Pagination link keywords
PAGINATION_KEYWORDS = ["next", "next page", "next >>", ">>", ">"]

# Max pages to follow per directory to avoid infinite loops
MAX_PAGES_PER_DIRECTORY = 50

# Max directory links to follow per association (some sites have many /search links)
MAX_DIRECTORY_LINKS_PER_ASSOCIATION = 3

# Phone number regex (from base module, but also match international formats)
PHONE_RE_FALLBACK = re.compile(
    r"(?:\+1[-.\s]?)?(?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4}"
)


# ---------------------------------------------------------------------------
# Scraper class
# ---------------------------------------------------------------------------

class Scraper(BaseScraper):
    """Generic scraper for professional association member directories."""

    SOURCE_NAME = "association_members"
    BASE_URL = ""  # varies per association
    REQUESTS_PER_MINUTE = 6  # polite default; 1 req per 10 seconds

    def __init__(self, association_urls: Optional[list[dict]] = None, **kwargs):
        """Initialize with an optional list of association URL dicts.

        Args:
            association_urls: List of dicts with keys:
                - url (str): Association homepage URL
                - name (str): Association name
                - category (str): Category/subcategory
            **kwargs: Passed to BaseScraper (includes rate_limiter=None).
        """
        super().__init__(**kwargs)
        self._association_urls: list[dict] = association_urls or []
        self._seen_members: set[str] = set()  # (association_domain, member_name) dedup
        self._current_association: dict = {}

    def set_association_urls(self, urls: list[dict]) -> None:
        """Set association URLs after construction (for runner script use)."""
        self._association_urls = urls

    # ------------------------------------------------------------------
    # URL generation — yields association homepages
    # ------------------------------------------------------------------

    def generate_urls(self, **kwargs) -> Iterator[str]:
        """Yield association homepage URLs.

        The base class run() method calls this, fetches each URL, then
        calls scrape_page(). We override run() instead to handle the
        two-phase discovery (homepage -> directory -> member pages).
        """
        for entry in self._association_urls:
            yield entry.get("url", "")

    # ------------------------------------------------------------------
    # Override run() for two-phase scraping
    # ------------------------------------------------------------------

    def run(
        self,
        max_pages: int = 0,
        max_contacts: int = 0,
        checkpoint: Optional[dict] = None,
    ) -> Iterator[ScrapedContact]:
        """Main scraping loop.

        For each association:
          1. Fetch homepage
          2. Discover directory links
          3. Scrape directory pages (with pagination)
          4. Yield ScrapedContact objects

        Supports checkpoint-based resumption via association URL.
        """
        from datetime import datetime

        contacts_yielded = 0
        pages_scraped = 0
        start_from = (checkpoint or {}).get("last_url")
        past_checkpoint = start_from is None

        self.logger.info(
            "Starting association_members scraper (%d associations, checkpoint=%s)",
            len(self._association_urls),
            start_from or "none",
        )

        for entry in self._association_urls:
            assoc_url = entry.get("url", "").strip()
            if not assoc_url:
                continue

            # Checkpoint resumption: skip until we reach last processed URL
            if not past_checkpoint:
                if assoc_url == start_from:
                    past_checkpoint = True
                continue

            self._current_association = entry
            assoc_name = entry.get("name", urlparse(assoc_url).netloc)
            self.logger.info("Processing association: %s (%s)", assoc_name, assoc_url)

            # Phase 1: Fetch homepage and discover directory links
            html = self.fetch_page(assoc_url)
            pages_scraped += 1
            if not html:
                self.logger.warning("Could not fetch homepage for %s", assoc_name)
                continue

            directory_urls = self._discover_directory_links(assoc_url, html)
            if not directory_urls:
                self.logger.info("No directory links found for %s", assoc_name)
                continue

            self.logger.info(
                "Found %d directory link(s) for %s: %s",
                len(directory_urls), assoc_name,
                [u[:80] for u in directory_urls],
            )

            # Phase 2: Scrape each directory link (with pagination)
            for dir_url in directory_urls[:MAX_DIRECTORY_LINKS_PER_ASSOCIATION]:
                pages_in_dir = 0

                current_url = dir_url
                while current_url and pages_in_dir < MAX_PAGES_PER_DIRECTORY:
                    dir_html = self.fetch_page(current_url)
                    pages_scraped += 1
                    if not dir_html:
                        break

                    try:
                        contacts = self._scrape_member_listing(
                            current_url, dir_html, assoc_name, entry
                        )
                    except Exception as exc:
                        self.logger.error(
                            "Parse error on %s: %s", current_url, exc
                        )
                        self.stats["errors"] += 1
                        break

                    for contact in contacts:
                        contact.source_platform = self.SOURCE_NAME
                        contact.source_url = current_url
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

                    pages_in_dir += 1

                    if max_pages and pages_scraped >= max_pages:
                        self.logger.info("Reached max_pages=%d", max_pages)
                        return

                    # Find next page
                    next_url = self._find_next_page(current_url, dir_html)
                    if next_url and next_url != current_url:
                        current_url = next_url
                    else:
                        break

                if max_pages and pages_scraped >= max_pages:
                    self.logger.info("Reached max_pages=%d", max_pages)
                    return

            if pages_scraped % 10 == 0:
                self.logger.info(
                    "Progress: %d pages, %d valid contacts from %d associations",
                    pages_scraped,
                    self.stats["contacts_valid"],
                    len(self._association_urls),
                )

        self.logger.info("Scraper complete: %s", self.stats)

    # ------------------------------------------------------------------
    # scrape_page — required by ABC but we use run() override instead
    # ------------------------------------------------------------------

    def scrape_page(self, url: str, html: str) -> list[ScrapedContact]:
        """Parse a single page (required by BaseScraper ABC).

        In practice, the custom run() method handles page parsing via
        _scrape_member_listing(). This method is a fallback that delegates
        to the internal parser.
        """
        return self._scrape_member_listing(
            url, html,
            self._current_association.get("name", ""),
            self._current_association,
        )

    # ------------------------------------------------------------------
    # Directory link discovery
    # ------------------------------------------------------------------

    def _discover_directory_links(self, base_url: str, html: str) -> list[str]:
        """Find member directory links on an association homepage.

        Scores each link by keyword relevance and returns the best matches.

        Returns:
            List of absolute URLs for probable member directory pages.
        """
        soup = self.parse_html(html)
        base_domain = urlparse(base_url).netloc.lower()
        candidates: list[tuple[int, str]] = []  # (score, url)
        seen_urls: set[str] = set()

        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue
            if href.startswith("mailto:") or href.startswith("tel:"):
                continue

            # Resolve relative URLs
            full_url = urljoin(base_url, href)
            parsed = urlparse(full_url)

            # Only follow links on the same domain (or common subdomains)
            link_domain = parsed.netloc.lower()
            if not self._is_same_site(base_domain, link_domain):
                continue

            # Normalize URL for dedup
            normalized = urlunparse((
                parsed.scheme, parsed.netloc, parsed.path.rstrip("/"),
                "", "", "",
            ))
            if normalized in seen_urls:
                continue

            # Score the link
            link_text = a_tag.get_text(strip=True).lower()
            href_lower = href.lower()
            path_lower = parsed.path.lower()

            score = self._score_directory_link(link_text, href_lower, path_lower)
            if score > 0:
                seen_urls.add(normalized)
                candidates.append((score, full_url))

        # Sort by score descending, return top matches
        candidates.sort(key=lambda x: -x[0])
        return [url for _, url in candidates[:MAX_DIRECTORY_LINKS_PER_ASSOCIATION]]

    def _score_directory_link(
        self, link_text: str, href: str, path: str
    ) -> int:
        """Score a link for how likely it is to be a member directory.

        Higher score = more likely. Returns 0 if no match.
        """
        score = 0

        # Check href/path patterns (most reliable)
        for i, pattern in enumerate(DIRECTORY_HREF_PATTERNS):
            if pattern in path:
                # Earlier patterns are more specific -> higher score
                score += max(20 - i, 5)
                break

        # Check link text
        for i, keyword in enumerate(DIRECTORY_LINK_KEYWORDS):
            if keyword in link_text:
                score += max(15 - i, 3)
                break

        # Bonus for combined signals
        if "member" in link_text and "member" in path:
            score += 10
        if "directory" in link_text and "directory" in path:
            score += 10
        if "find" in link_text and ("find" in path or "search" in path):
            score += 8

        # Penalize generic links that are likely navigation
        if path in ("/", "/about", "/contact", "/login", "/register", "/join"):
            score = 0
        if "login" in path or "register" in path or "join" in path:
            score = 0
        if "privacy" in path or "terms" in path or "cookie" in path:
            score = 0

        return score

    # ------------------------------------------------------------------
    # Member listing parsing
    # ------------------------------------------------------------------

    def _scrape_member_listing(
        self,
        url: str,
        html: str,
        assoc_name: str,
        assoc_entry: dict,
    ) -> list[ScrapedContact]:
        """Extract member contacts from a directory listing page.

        Uses multiple strategies to handle different page layouts:
          1. Structured cards (divs/articles with member data)
          2. Table rows with member information
          3. List items with member links
          4. vCard / hCard microformat data
        """
        soup = self.parse_html(html)
        contacts: list[ScrapedContact] = []
        assoc_domain = urlparse(url).netloc.lower()
        category = assoc_entry.get("category", "")
        subcategory = assoc_entry.get("subcategory", "")

        # Strategy 1: Look for structured member cards
        # Common patterns: div.member-card, div.member-item, article.member, etc.
        card_selectors = [
            {"class_": re.compile(r"member[-_]?(card|item|listing|entry|row|result|block|profile|info)", re.I)},
            {"class_": re.compile(r"(card|item|listing|entry|row|result|block|profile)[-_]?member", re.I)},
            {"class_": re.compile(r"directory[-_]?(card|item|listing|entry|row|result)", re.I)},
            {"class_": re.compile(r"(search|find)[-_]?result", re.I)},
            {"class_": re.compile(r"vcard", re.I)},
        ]

        member_elements = []
        for selector in card_selectors:
            found = soup.find_all(["div", "article", "li", "section", "tr"], **selector)
            if found:
                member_elements = found
                break

        if member_elements:
            for elem in member_elements:
                contact = self._parse_member_card(elem, url, assoc_name, assoc_domain, category, subcategory)
                if contact:
                    contacts.append(contact)

        # Strategy 2: Look for table-based directories
        if not contacts:
            for table in soup.find_all("table"):
                table_contacts = self._parse_member_table(table, url, assoc_name, assoc_domain, category, subcategory)
                contacts.extend(table_contacts)

        # Strategy 3: Look for list items containing member info
        if not contacts:
            # Find lists that appear to contain member data
            for ul in soup.find_all(["ul", "ol"]):
                ul_class = " ".join(ul.get("class", []))
                # Skip navigation lists
                if any(skip in ul_class.lower() for skip in ["nav", "menu", "breadcrumb", "footer"]):
                    continue
                list_contacts = self._parse_member_list(ul, url, assoc_name, assoc_domain, category, subcategory)
                if list_contacts:
                    contacts.extend(list_contacts)
                    break  # take the first list that yields contacts

        # Strategy 4: Generic link scanning for member profile pages
        if not contacts:
            contacts = self._scan_for_profile_links(soup, url, assoc_name, assoc_domain, category, subcategory)

        return contacts

    def _parse_member_card(
        self,
        elem,
        page_url: str,
        assoc_name: str,
        assoc_domain: str,
        category: str,
        subcategory: str,
    ) -> Optional[ScrapedContact]:
        """Extract a contact from a member card element."""
        text = elem.get_text(separator=" ", strip=True)
        if len(text) < 5:
            return None

        # --- Name ---
        name = ""
        # Try heading tags first
        for tag in ["h2", "h3", "h4", "h5", "strong", "b"]:
            heading = elem.find(tag)
            if heading:
                candidate = heading.get_text(strip=True)
                if 2 < len(candidate) < 100 and not self._looks_like_label(candidate):
                    name = candidate
                    break

        # Try class-based name detection
        if not name:
            name_el = elem.find(
                class_=re.compile(r"(member[-_]?)?name|title|person|fn", re.I)
            )
            if name_el:
                candidate = name_el.get_text(strip=True)
                if 2 < len(candidate) < 100:
                    name = candidate

        # Try first link text as name
        if not name:
            first_link = elem.find("a", href=True)
            if first_link:
                candidate = first_link.get_text(strip=True)
                if 2 < len(candidate) < 100 and not self._looks_like_label(candidate):
                    name = candidate

        if not name:
            return None

        # Dedup by (domain, name)
        dedup_key = (assoc_domain, name.lower().strip())
        if dedup_key in self._seen_members:
            return None
        self._seen_members.add(dedup_key)

        # --- Company ---
        company = ""
        company_el = elem.find(
            class_=re.compile(r"company|org(anization)?|firm|business|employer", re.I)
        )
        if company_el:
            company = company_el.get_text(strip=True)[:150]

        # --- Email ---
        email = ""
        # Check mailto links first
        mailto = elem.find("a", href=re.compile(r"^mailto:", re.I))
        if mailto:
            email = mailto["href"].replace("mailto:", "").split("?")[0].strip()
        else:
            emails = self.extract_emails(str(elem))
            if emails:
                email = emails[0]

        # --- Website ---
        website = ""
        _skip_domains = {
            "linkedin.com", "facebook.com", "twitter.com", "instagram.com",
            "youtube.com", "x.com", "tiktok.com", "pinterest.com",
        }
        for a_tag in elem.find_all("a", href=True):
            href = a_tag["href"]
            if href.startswith("mailto:") or href.startswith("tel:"):
                continue
            if href.startswith("http"):
                link_domain = urlparse(href).netloc.lower()
                # Skip social media links — those are not the member's website
                base_link_domain = link_domain.replace("www.", "")
                if base_link_domain in _skip_domains:
                    continue
                # External link (not the association's own site) is likely the member's website
                if not self._is_same_site(assoc_domain, link_domain):
                    website = href
                    break

        # --- LinkedIn ---
        linkedin = self.extract_linkedin(str(elem))

        # --- Phone ---
        phone = ""
        # Check tel: links
        tel_link = elem.find("a", href=re.compile(r"^tel:", re.I))
        if tel_link:
            phone = tel_link["href"].replace("tel:", "").strip()
        else:
            phone_match = PHONE_RE_FALLBACK.search(text)
            if phone_match:
                phone = phone_match.group(0)

        # --- Bio ---
        bio = ""
        bio_el = elem.find(
            class_=re.compile(r"bio|description|summary|about|detail|specialty|expertise", re.I)
        )
        if bio_el:
            bio = bio_el.get_text(strip=True)[:500]
        elif len(text) > len(name) + 20:
            # Use remaining text as a brief bio
            bio_text = text.replace(name, "", 1).strip()
            if company:
                bio_text = bio_text.replace(company, "", 1).strip()
            if len(bio_text) > 10:
                bio = bio_text[:500]

        # Build source category
        source_cat = f"association:{category}" if category else "association"
        if subcategory:
            source_cat += f":{subcategory}"

        return ScrapedContact(
            name=name,
            email=email or "",
            company=company or "",
            website=website or "",
            linkedin=linkedin or "",
            phone=phone or "",
            bio=bio or "",
            source_category=source_cat,
            raw_data={
                "association_name": assoc_name,
                "association_url": page_url,
                "category": category,
                "subcategory": subcategory,
            },
        )

    def _parse_member_table(
        self,
        table,
        page_url: str,
        assoc_name: str,
        assoc_domain: str,
        category: str,
        subcategory: str,
    ) -> list[ScrapedContact]:
        """Extract contacts from an HTML table."""
        contacts = []
        rows = table.find_all("tr")
        if len(rows) < 2:
            return contacts

        # Try to identify column headers
        header_row = rows[0]
        headers = [
            th.get_text(strip=True).lower()
            for th in header_row.find_all(["th", "td"])
        ]

        # Map header positions
        col_map = {}
        for i, h in enumerate(headers):
            if any(k in h for k in ["name", "contact", "person", "member"]):
                col_map["name"] = i
            elif any(k in h for k in ["company", "organization", "firm", "business"]):
                col_map["company"] = i
            elif any(k in h for k in ["email", "e-mail"]):
                col_map["email"] = i
            elif any(k in h for k in ["phone", "telephone", "tel"]):
                col_map["phone"] = i
            elif any(k in h for k in ["website", "web", "url", "site"]):
                col_map["website"] = i
            elif any(k in h for k in ["city", "location", "state", "address"]):
                col_map.setdefault("location", i)

        # If no name column found, skip this table
        if "name" not in col_map:
            return contacts

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) <= col_map.get("name", 0):
                continue

            name_cell = cells[col_map["name"]]
            name = name_cell.get_text(strip=True)
            if not name or len(name) < 2 or len(name) > 150:
                continue

            # Dedup
            dedup_key = (assoc_domain, name.lower().strip())
            if dedup_key in self._seen_members:
                continue
            self._seen_members.add(dedup_key)

            def _get_cell(key: str) -> str:
                idx = col_map.get(key)
                if idx is not None and idx < len(cells):
                    return cells[idx].get_text(strip=True)
                return ""

            company = _get_cell("company")
            phone = _get_cell("phone")

            # Email from column or from scanning the row
            email = _get_cell("email")
            if not email:
                row_emails = self.extract_emails(str(row))
                email = row_emails[0] if row_emails else ""

            # Website from column or from links
            website = _get_cell("website")
            if not website:
                for a_tag in row.find_all("a", href=True):
                    href = a_tag["href"]
                    if href.startswith("http") and not self._is_same_site(
                        assoc_domain, urlparse(href).netloc.lower()
                    ):
                        website = href
                        break

            linkedin = self.extract_linkedin(str(row))
            location = _get_cell("location")

            source_cat = f"association:{category}" if category else "association"

            contacts.append(ScrapedContact(
                name=name,
                email=email or "",
                company=company or "",
                website=website or "",
                linkedin=linkedin or "",
                phone=phone or "",
                bio=location or "",
                source_category=source_cat,
                raw_data={
                    "association_name": assoc_name,
                    "association_url": page_url,
                    "category": category,
                    "subcategory": subcategory,
                },
            ))

        return contacts

    def _parse_member_list(
        self,
        ul_element,
        page_url: str,
        assoc_name: str,
        assoc_domain: str,
        category: str,
        subcategory: str,
    ) -> list[ScrapedContact]:
        """Extract contacts from a UL/OL list."""
        contacts = []
        items = ul_element.find_all("li", recursive=False)
        if len(items) < 3:
            return contacts

        for li in items:
            text = li.get_text(separator=" ", strip=True)
            if len(text) < 5 or len(text) > 500:
                continue

            # Name: first link or first strong/b text
            name = ""
            link = li.find("a", href=True)
            if link:
                candidate = link.get_text(strip=True)
                if 2 < len(candidate) < 100:
                    name = candidate
            if not name:
                strong = li.find(["strong", "b"])
                if strong:
                    candidate = strong.get_text(strip=True)
                    if 2 < len(candidate) < 100:
                        name = candidate
            if not name:
                continue

            # Dedup
            dedup_key = (assoc_domain, name.lower().strip())
            if dedup_key in self._seen_members:
                continue
            self._seen_members.add(dedup_key)

            emails = self.extract_emails(str(li))
            linkedin = self.extract_linkedin(str(li))
            phone_match = PHONE_RE_FALLBACK.search(text)

            website = ""
            for a_tag in li.find_all("a", href=True):
                href = a_tag["href"]
                if href.startswith("http") and not self._is_same_site(
                    assoc_domain, urlparse(href).netloc.lower()
                ):
                    website = href
                    break

            source_cat = f"association:{category}" if category else "association"

            contacts.append(ScrapedContact(
                name=name,
                email=emails[0] if emails else "",
                company="",
                website=website or "",
                linkedin=linkedin or "",
                phone=phone_match.group(0) if phone_match else "",
                bio="",
                source_category=source_cat,
                raw_data={
                    "association_name": assoc_name,
                    "association_url": page_url,
                },
            ))

        return contacts

    def _scan_for_profile_links(
        self,
        soup,
        page_url: str,
        assoc_name: str,
        assoc_domain: str,
        category: str,
        subcategory: str,
    ) -> list[ScrapedContact]:
        """Last-resort: scan for links that look like member profile pages.

        Looks for patterns like /members/john-doe, /profile/12345, etc.
        """
        contacts = []
        profile_patterns = re.compile(
            r"/(member|profile|person|people|user|professional|directory)s?/"
            r"[a-zA-Z0-9_\-]+/?$",
            re.I,
        )

        seen_profile_urls: set[str] = set()

        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            full_url = urljoin(page_url, href)
            parsed = urlparse(full_url)

            if not self._is_same_site(assoc_domain, parsed.netloc.lower()):
                continue

            if not profile_patterns.search(parsed.path):
                continue

            if full_url in seen_profile_urls:
                continue
            seen_profile_urls.add(full_url)

            link_text = a_tag.get_text(strip=True)
            if not link_text or len(link_text) < 2 or len(link_text) > 100:
                continue
            if self._looks_like_label(link_text):
                continue

            # Dedup
            dedup_key = (assoc_domain, link_text.lower().strip())
            if dedup_key in self._seen_members:
                continue
            self._seen_members.add(dedup_key)

            source_cat = f"association:{category}" if category else "association"

            contacts.append(ScrapedContact(
                name=link_text,
                email="",
                company="",
                website=full_url,
                linkedin="",
                phone="",
                bio="",
                source_category=source_cat,
                raw_data={
                    "association_name": assoc_name,
                    "association_url": page_url,
                    "profile_url": full_url,
                },
            ))

        return contacts

    # ------------------------------------------------------------------
    # Pagination
    # ------------------------------------------------------------------

    def _find_next_page(self, current_url: str, html: str) -> Optional[str]:
        """Find the "next page" link on a directory listing page.

        Checks for:
          1. rel="next" link
          2. Link text matching "Next", ">>", etc.
          3. Page number increment in query params
        """
        soup = self.parse_html(html)

        # Strategy 1: <link rel="next"> or <a rel="next">
        rel_next = soup.find(["a", "link"], rel="next", href=True)
        if rel_next:
            return urljoin(current_url, rel_next["href"])

        # Strategy 2: Anchor with "next" text or aria-label
        for a_tag in soup.find_all("a", href=True):
            # Check aria-label
            aria = (a_tag.get("aria-label") or "").lower()
            if "next" in aria:
                return urljoin(current_url, a_tag["href"])

            # Check link text
            text = a_tag.get_text(strip=True).lower()
            if text in ("next", "next page", "next >>", ">>", ">", "next >"):
                href = a_tag["href"]
                if href and href != "#":
                    return urljoin(current_url, href)

            # Check class/id for pagination
            classes = " ".join(a_tag.get("class", [])).lower()
            if "next" in classes and a_tag["href"] != "#":
                return urljoin(current_url, a_tag["href"])

        # Strategy 3: Increment page= query param
        parsed = urlparse(current_url)
        params = parse_qs(parsed.query)
        for key in ("page", "p", "pg", "pageNum", "pagenum", "offset", "start"):
            if key in params:
                try:
                    current_val = int(params[key][0])
                    params[key] = [str(current_val + 1)]
                    new_query = urlencode(params, doseq=True)
                    next_url = urlunparse((
                        parsed.scheme, parsed.netloc, parsed.path,
                        parsed.params, new_query, "",
                    ))
                    # Verify the page link exists somewhere in the page
                    if f"page={current_val + 1}" in html or f"p={current_val + 1}" in html:
                        return next_url
                except (ValueError, IndexError):
                    pass

        return None

    # ------------------------------------------------------------------
    # Utility methods
    # ------------------------------------------------------------------

    @staticmethod
    def _is_same_site(domain_a: str, domain_b: str) -> bool:
        """Check if two domains belong to the same site.

        Handles www. prefix and common subdomain differences.
        """
        def normalize(d: str) -> str:
            d = d.lower().strip()
            if d.startswith("www."):
                d = d[4:]
            return d

        return normalize(domain_a) == normalize(domain_b)

    @staticmethod
    def _looks_like_label(text: str) -> bool:
        """Check if text looks like a UI label rather than a person/company name."""
        labels = {
            "view", "more", "details", "read more", "see more", "learn more",
            "click here", "visit", "view profile", "go", "search", "home",
            "about", "contact", "login", "register", "sign in", "sign up",
            "back", "previous", "next", "first", "last", "page",
            "member directory", "find a member", "our members", "members",
            "directory", "all", "filter", "sort", "reset", "apply",
        }
        return text.lower().strip() in labels
