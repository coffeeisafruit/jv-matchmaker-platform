"""
Reusable contact information scraper.

Uses Playwright (via OWL venv) to browse websites intelligently,
discover contact pages, and extract emails, phone numbers, and booking links.

Usage:
    from matching.enrichment.contact_scraper import ContactScraper

    scraper = ContactScraper()
    result = scraper.scrape_contact_info("https://example.com", "John Doe", "Acme Corp")
    # result = {
    #     'email': 'john@example.com',           # best personal email
    #     'secondary_emails': ['info@example.com'],
    #     'phone': '555-123-4567',
    #     'booking_link': 'https://calendly.com/john',
    #     'linkedin': 'https://linkedin.com/in/johndoe',
    # }
"""

import os
import re
import logging
import subprocess
import tempfile
from typing import Dict, List, Optional
from urllib.parse import urlparse, urljoin

logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OWL_PYTHON = os.path.join(PROJECT_ROOT, 'owl_framework', '.venv', 'bin', 'python')

# ── Regex patterns ────────────────────────────────────────────────────

EMAIL_RE = re.compile(r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b')

PHONE_RE = re.compile(
    r'(?:(?:\+\d{1,3}[\s\-.]?)?\(?\d{2,4}\)?[\s\-.]?\d{3,4}[\s\-.]?\d{3,4})'
)

BOOKING_LINK_PATTERNS = [
    re.compile(r'https?://(?:www\.)?calendly\.com/[\w-]+(?:/[\w-]+)?', re.IGNORECASE),
    re.compile(r'https?://(?:www\.)?acuityscheduling\.com/[\w-]+(?:/[\w-]+)?', re.IGNORECASE),
    re.compile(r'https?://(?:www\.)?savvycal\.com/[\w-]+(?:/[\w-]+)?', re.IGNORECASE),
    re.compile(r'https?://(?:www\.)?tidycal\.com/[\w-]+(?:/[\w-]+)?', re.IGNORECASE),
    re.compile(r'https?://(?:www\.)?hubspot\.com/meetings/[\w-]+(?:/[\w-]+)?', re.IGNORECASE),
    re.compile(r'https?://(?:www\.)?zcal\.co/[\w-]+(?:/[\w-]+)?', re.IGNORECASE),
    re.compile(r'https?://(?:www\.)?oncehub\.com/[\w-]+(?:/[\w-]+)?', re.IGNORECASE),
    re.compile(r'https?://(?:www\.)?youcanbook\.me/[\w-]+', re.IGNORECASE),
]

LINKEDIN_RE = re.compile(
    r'https?://(?:www\.)?linkedin\.com/in/([\w-]+)', re.IGNORECASE
)

# ── Contact page discovery ────────────────────────────────────────────

# Keywords that indicate a contact/about page (scored by relevance)
CONTACT_KEYWORDS_HIGH = [
    'contact', 'get-in-touch', 'reach-out', 'connect', 'email-us', 'email',
    'talk-to-us', 'lets-talk', 'inquir', 'message',
]
CONTACT_KEYWORDS_MED = [
    'about', 'team', 'our-story', 'meet', 'who-we-are', 'founder',
    'work-with', 'hire', 'book', 'schedule', 'consult', 'bio',
]
# Link text patterns (what the user sees, not the URL)
CONTACT_TEXT_HIGH = [
    'contact', 'get in touch', 'reach out', 'email us', 'talk to us',
    'send message', 'let\'s talk', 'inquire',
]
CONTACT_TEXT_MED = [
    'about', 'team', 'our story', 'meet', 'who we are', 'founder',
    'work with', 'book a call', 'schedule', 'connect',
]

# URLs to always skip
SKIP_URL_PATTERNS = [
    r'/blog/', r'/blog$', r'/tag/', r'/category/', r'/wp-content/',
    r'/privacy', r'/terms', r'/cart', r'/login', r'/signup',
    r'/wp-admin', r'/feed', r'\.xml$', r'\.pdf$', r'\.jpg$', r'\.png$',
    r'\.gif$', r'\.css$', r'\.js$', r'\.zip$', r'\.mp[34]$',
    r'facebook\.com', r'instagram\.com', r'twitter\.com', r'tiktok\.com',
    r'youtube\.com', r'pinterest\.com',
]

# Domains that are 3rd-party profile pages (not the person's own site)
THIRD_PARTY_DOMAINS = [
    'speakerhub.com', 'alignable.com', 'medium.com', 'udemy.com',
    'zoominfo.com', 'theorg.com', 'crunchbase.com', 'about.me',
    'linktr.ee', 'linktree.com', 'allamericanspeakers.com',
]

# ── Junk email prefixes (truly discard) ───────────────────────────────
JUNK_PREFIXES = [
    'noreply', 'no-reply', 'no_reply', 'donotreply', 'do-not-reply',
    'spam', 'abuse', 'postmaster', 'mailer-daemon', 'daemon',
    'bounce', 'unsubscribe', 'opt-out',
]

# Generic prefixes (NOT junk — save as secondary)
GENERIC_PREFIXES = [
    'info', 'contact', 'support', 'hello', 'hi', 'help',
    'admin', 'office', 'team', 'sales', 'enquir', 'general',
    'reception', 'mail', 'service', 'billing', 'privacy', 'legal',
]


class ContactScraper:
    """Scrapes websites for contact information using Playwright."""

    def __init__(self, browse_timeout: int = 45):
        self.browse_timeout = browse_timeout

    def scrape_contact_info(
        self, website: str, name: str, company: str = ''
    ) -> Dict:
        """
        Main entry point. Browses website intelligently and extracts contact info.

        Returns dict with:
            email: str | None           — best personal email
            secondary_emails: list[str] — all other valid emails
            phone: str | None           — first phone found
            booking_link: str | None    — calendar booking URL
            linkedin: str | None        — LinkedIn profile URL
        """
        result = {
            'email': None,
            'secondary_emails': [],
            'phone': None,
            'booking_link': None,
            'linkedin': None,
        }

        if not website or not website.startswith('http'):
            return result

        parsed = urlparse(website)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        website_domain = parsed.netloc.lower().replace('www.', '')

        # Skip third-party profile pages
        if any(tp in website_domain for tp in THIRD_PARTY_DOMAINS):
            logger.debug(f"Skipping third-party domain: {website_domain}")
            return result

        # Step 1: Browse homepage and get both text + HTML (for link discovery)
        homepage_html, homepage_text = self._browse_page_full(website)
        if not homepage_text:
            return result

        all_text = [homepage_text]

        # Step 2: Discover contact-relevant pages from homepage links
        contact_urls = self._discover_contact_pages(homepage_html or '', base_url)

        # Step 3: Browse top contact pages
        for url in contact_urls[:3]:
            _, page_text = self._browse_page_full(url)
            if page_text and 'ERROR' not in page_text[:20]:
                all_text.append(page_text)

        # Step 4: Extract contact info from all collected text
        combined_text = '\n\n'.join(all_text)

        # Emails
        all_emails = self._extract_emails(combined_text)
        classified = self._classify_emails(all_emails, name, website_domain)
        result['email'] = classified['primary']
        result['secondary_emails'] = classified['secondary']

        # Phone
        phones = self._extract_phones(combined_text)
        if phones:
            result['phone'] = phones[0]

        # Booking link
        booking = self._extract_booking_links(combined_text)
        if booking:
            result['booking_link'] = booking

        # LinkedIn (from links on the page)
        linkedin = self._extract_linkedin(combined_text)
        if linkedin:
            result['linkedin'] = linkedin

        return result

    # Playwright browse script — URL passed via sys.argv to avoid injection
    _BROWSE_SCRIPT = """
import asyncio, sys
from playwright.async_api import async_playwright

async def browse():
    url = sys.argv[1]
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            await page.goto(url, timeout=30000, wait_until="domcontentloaded")
            await asyncio.sleep(2)
            html = await page.content()
            text = await page.evaluate("() => document.body ? document.body.innerText : ''")
            print("===HTML_START===")
            print(html[:50000])
            print("===HTML_END===")
            print("===TEXT_START===")
            print(text[:20000])
            print("===TEXT_END===")
        except Exception as e:
            print(f"ERROR: {e}", file=sys.stderr)
        finally:
            await browser.close()

asyncio.run(browse())
"""

    def _browse_page_full(self, url: str) -> tuple:
        """
        Browse URL with Playwright, returning (html, text).

        Uses process groups to ensure Chromium children are killed on timeout.
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(self._BROWSE_SCRIPT)
            script_path = f.name

        try:
            # Use Popen with process group so we can kill Chromium children on timeout
            proc = subprocess.Popen(
                [OWL_PYTHON, script_path, url],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,  # creates new process group
            )

            try:
                stdout, stderr = proc.communicate(timeout=self.browse_timeout)
            except subprocess.TimeoutExpired:
                # Kill entire process group (Python + Chromium children)
                import signal
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                proc.wait(timeout=5)
                logger.warning(f"Timeout after {self.browse_timeout}s browsing {url}")
                return '', ''

            if proc.returncode != 0:
                if stderr and stderr.strip():
                    logger.warning(f"Playwright error for {url}: {stderr.strip()[:200]}")
                return '', ''

            html = ''
            text = ''
            if '===HTML_START===' in stdout and '===HTML_END===' in stdout:
                html = stdout.split('===HTML_START===')[1].split('===HTML_END===')[0].strip()
            if '===TEXT_START===' in stdout and '===TEXT_END===' in stdout:
                text = stdout.split('===TEXT_START===')[1].split('===TEXT_END===')[0].strip()

            return html, text

        except Exception as e:
            logger.warning(f"Browse error for {url}: {e}")
            return '', ''
        finally:
            os.unlink(script_path)

    def _discover_contact_pages(self, html: str, base_url: str) -> List[str]:
        """
        Parse homepage HTML for links, score by contact-relevance, return top URLs.
        """
        if not html:
            # Fallback to hardcoded paths if no HTML available
            return [
                f"{base_url}/contact",
                f"{base_url}/about",
                f"{base_url}/team",
            ]

        # Extract all <a> tags with href and optional link text
        link_pattern = re.compile(
            r'<a\s[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
            re.IGNORECASE | re.DOTALL
        )

        scored_urls = []
        seen = set()

        for match in link_pattern.finditer(html):
            href = match.group(1).strip()
            link_text = re.sub(r'<[^>]+>', '', match.group(2)).strip().lower()

            # Normalize URL
            if href.startswith('mailto:') or href.startswith('tel:'):
                continue
            if href.startswith('#') or href.startswith('javascript:'):
                continue

            full_url = urljoin(base_url, href)
            parsed_url = urlparse(full_url)

            # Must be same domain
            if parsed_url.netloc.lower().replace('www.', '') != \
               urlparse(base_url).netloc.lower().replace('www.', ''):
                continue

            # Skip unwanted patterns
            if any(re.search(pat, full_url, re.IGNORECASE) for pat in SKIP_URL_PATTERNS):
                continue

            if full_url in seen:
                continue
            seen.add(full_url)

            # Score the URL
            path_lower = parsed_url.path.lower()
            score = 0

            for kw in CONTACT_KEYWORDS_HIGH:
                if kw in path_lower:
                    score += 10
            for kw in CONTACT_KEYWORDS_MED:
                if kw in path_lower:
                    score += 5

            for txt in CONTACT_TEXT_HIGH:
                if txt in link_text:
                    score += 10
            for txt in CONTACT_TEXT_MED:
                if txt in link_text:
                    score += 5

            if score > 0:
                scored_urls.append((score, full_url))

        # Sort by score descending, return top URLs
        scored_urls.sort(key=lambda x: -x[0])
        urls = [url for _, url in scored_urls[:4]]

        # If nothing scored, fall back to common paths
        if not urls:
            urls = [
                f"{base_url}/contact",
                f"{base_url}/about",
            ]

        return urls

    def _extract_emails(self, text: str) -> List[str]:
        """Extract and deduplicate all email addresses from text."""
        raw = EMAIL_RE.findall(text)
        # Deduplicate case-insensitively
        seen = set()
        unique = []
        for email in raw:
            lower = email.lower()
            if lower not in seen:
                seen.add(lower)
                unique.append(email)
        return unique

    def _classify_emails(
        self, emails: List[str], name: str, website_domain: str
    ) -> Dict:
        """
        Classify emails into primary (best personal) and secondary (all others).

        Primary selection priority:
          1. Domain matches website + contains person's name
          2. Domain matches website
          3. Contains first or last name
          4. Any non-generic email
        Secondary: all non-junk emails not selected as primary.
        Discard: truly junk emails (noreply, mailer-daemon, etc.)
        """
        if not emails:
            return {'primary': None, 'secondary': []}

        name_parts = [p.lower() for p in name.split() if len(p) > 1]
        first_name = name_parts[0] if name_parts else ''
        last_name = name_parts[-1] if len(name_parts) >= 2 else ''

        # Filter out true junk
        valid_emails = []
        for email in emails:
            local_part = email.split('@')[0].lower()
            if any(local_part.startswith(j) for j in JUNK_PREFIXES):
                continue
            # Skip emails from known junk domains
            domain = email.split('@')[1].lower() if '@' in email else ''
            if domain in ('example.com', 'test.com', 'placeholder.com'):
                continue
            valid_emails.append(email)

        if not valid_emails:
            return {'primary': None, 'secondary': []}

        # Classify each email
        def is_generic(email: str) -> bool:
            local = email.split('@')[0].lower()
            return any(local.startswith(g) for g in GENERIC_PREFIXES)

        def domain_matches(email: str) -> bool:
            domain = email.split('@')[1].lower() if '@' in email else ''
            return domain == website_domain or domain == f'www.{website_domain}'

        def name_matches(email: str) -> bool:
            lower = email.lower()
            if first_name and first_name in lower:
                return True
            if last_name and last_name in lower:
                return True
            return False

        # Score each email for primary selection
        scored = []
        for email in valid_emails:
            score = 0
            dm = domain_matches(email)
            nm = name_matches(email)
            generic = is_generic(email)

            if dm and nm:
                score = 100  # Best: on their domain AND has their name
            elif dm and not generic:
                score = 80   # On their domain, personal-looking
            elif nm:
                score = 60   # Has their name (different domain)
            elif dm and generic:
                score = 40   # On their domain but generic (info@theirsite.com)
            elif not generic:
                score = 20   # Personal-looking, different domain
            else:
                score = 0    # Generic, different domain

            scored.append((score, email))

        scored.sort(key=lambda x: -x[0])

        # Primary = highest scored non-generic, or highest scored overall
        primary = None
        for score, email in scored:
            if score >= 20:  # At least personal-looking
                primary = email
                break

        # Secondary = everything else (except primary)
        secondary = [
            email for _, email in scored
            if email != primary
        ]

        return {'primary': primary, 'secondary': secondary}

    def _extract_phones(self, text: str) -> List[str]:
        """Extract phone numbers from text, filtering false positives."""
        raw = PHONE_RE.findall(text)
        valid = []
        for phone in raw:
            # Clean up
            digits = re.sub(r'\D', '', phone)
            # Must have 7-15 digits
            if 7 <= len(digits) <= 15:
                # Skip if it looks like a year (1900-2099)
                if len(digits) == 4 and 1900 <= int(digits) <= 2099:
                    continue
                # Skip common false positives
                if digits in ('0000000', '1111111', '1234567'):
                    continue
                cleaned = phone.strip()
                if cleaned not in valid:
                    valid.append(cleaned)
        return valid

    def _extract_booking_links(self, text: str) -> Optional[str]:
        """Extract the first booking/calendar link from text."""
        for pattern in BOOKING_LINK_PATTERNS:
            match = pattern.search(text)
            if match:
                return match.group(0)
        return None

    def _extract_linkedin(self, text: str) -> Optional[str]:
        """Extract LinkedIn profile URL from text."""
        match = LINKEDIN_RE.search(text)
        if match:
            return f"https://www.linkedin.com/in/{match.group(1)}"
        return None
