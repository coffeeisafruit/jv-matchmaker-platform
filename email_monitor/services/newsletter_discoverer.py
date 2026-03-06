"""
Discovers newsletter signup forms on profile websites.

Primary: requests + BeautifulSoup (no extra deps, works everywhere).
Fallback: Crawl4AI for JS-rendered pages (optional, skipped if not installed).

Failed discoveries are logged with structured error codes so they can be
retried later with a different approach (e.g. Crawl4AI, headless browser).

Error codes:
  http_403     — site blocked bot (retry with Crawl4AI stealth headers)
  http_404     — URL not found (retry with root domain)
  http_other   — other HTTP error
  timeout      — request timed out (retry with Crawl4AI + longer timeout)
  js_required  — page returned empty/minimal HTML (JS-rendered, needs Crawl4AI)
  no_form      — page loaded but no newsletter form found (manual subscribe)
  captcha      — CAPTCHA wall detected
  error        — unexpected exception
"""

import logging
import re
import requests
from typing import Optional
from dataclasses import dataclass
from urllib.parse import urlparse, urljoin

logger = logging.getLogger(__name__)

REQUEST_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

ESP_PATTERNS = {
    'ConvertKit': [
        r'data-sv-form',
        r'app\.convertkit\.com',
        r'api\.convertkit\.com',
        r'ck\.page',
    ],
    'Mailchimp': [
        r'mc-embedded-subscribe-form',
        r'list-manage\.com',
        r'mailchimp\.com',
    ],
    'ActiveCampaign': [
        r'_form_[a-z0-9]+',
        r'activehosted\.com',
        r'activecampaign\.com',
    ],
    'Beehiiv': [
        r'beehiiv\.com',
        r'embeds\.beehiiv\.com',
    ],
    'Substack': [
        r'substack\.com/embed',
        r'substackapi\.com',
    ],
    'AWeber': [
        r'aweber\.com',
        r'awlist[0-9]+',
    ],
    'GetResponse': [
        r'getresponse\.com',
        r'gr-form',
    ],
    'Kajabi': [
        r'kajabi\.com',
        r'kajabi-form',
    ],
    'Drip': [
        r'getdrip\.com',
        r'drip-ef-',
    ],
    'MailerLite': [
        r'mailerlite\.com',
        r'ml-form-embed',
    ],
    'Kit': [  # ConvertKit rebranded
        r'kit\.com/forms',
        r'myflodesk\.com',
    ],
}

NEWSLETTER_URL_PATTERNS = re.compile(
    r'/(subscribe|newsletter|optin|opt-in|join|signup|sign-up|mailing-list|email-list|free)',
    re.IGNORECASE,
)

CAPTCHA_PATTERNS = re.compile(
    r'(captcha|recaptcha|hcaptcha|turnstile|cf-challenge)',
    re.IGNORECASE,
)

# Pages with very little text are likely JS-rendered
JS_RENDER_THRESHOLD = 500  # bytes of visible text


@dataclass
class DiscoveryResult:
    profile_id: str
    website: str
    signup_url: str = ''
    form_action: str = ''
    esp_detected: str = ''
    subscribe_method: str = 'form_post'
    success: bool = False
    error: str = ''  # structured error code for retry routing


def _detect_esp(html: str) -> str:
    for esp, patterns in ESP_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, html, re.IGNORECASE):
                return esp
    return ''


def _find_signup_form(html: str, base_url: str) -> tuple[str, str]:
    """
    Find newsletter signup form in HTML.
    Returns (signup_url, form_action) or ('', '') if not found.
    """
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')

        for form in soup.find_all('form'):
            email_input = (
                form.find('input', {'type': 'email'}) or
                form.find('input', {'name': re.compile(r'email', re.I)})
            )
            if not email_input:
                continue

            action = form.get('action', '')
            form_action = urljoin(base_url, action) if action else base_url

            form_text = form.get_text(' ', strip=True).lower()
            form_html = str(form).lower()
            is_newsletter = any(kw in form_text or kw in form_html for kw in [
                'subscribe', 'newsletter', 'email list', 'join', 'updates',
                'weekly', 'free', 'tips', 'list', 'get access', 'sign up',
            ])
            if is_newsletter:
                return base_url, form_action

        # No form — look for newsletter page link to follow
        for a in soup.find_all('a', href=True):
            if NEWSLETTER_URL_PATTERNS.search(a['href']):
                return urljoin(base_url, a['href']), urljoin(base_url, a['href'])

    except Exception as exc:
        logger.debug('Form detection error: %s', exc)

    return '', ''


def _fetch_page(url: str) -> tuple[Optional[str], str]:
    """
    Fetch page HTML.

    Stage 1: requests (fast, no deps)
    Stage 2: Crawl4AI (JS-rendered pages, optional)

    Returns (html_or_None, error_code).
    """
    # Stage 1: requests
    try:
        resp = requests.get(url, timeout=10, headers=REQUEST_HEADERS, allow_redirects=True)
        if resp.status_code == 403:
            logger.debug('HTTP 403 for %s — blocked', url)
            return None, 'http_403'
        if resp.status_code == 404:
            return None, 'http_404'
        if resp.status_code >= 400:
            return None, 'http_other'

        html = resp.text
        if not html:
            return None, 'js_required'

        # Check if page is JS-rendered (very little visible text)
        try:
            from bs4 import BeautifulSoup
            visible = BeautifulSoup(html, 'html.parser').get_text()
            if len(visible.strip()) < JS_RENDER_THRESHOLD:
                raise ValueError('js_required')
        except ValueError as e:
            if str(e) == 'js_required':
                # Try Crawl4AI before giving up
                pass
            else:
                pass
        else:
            if CAPTCHA_PATTERNS.search(html):
                return None, 'captcha'
            return html, ''

    except requests.Timeout:
        logger.debug('Timeout fetching %s', url)
        # Fall through to Crawl4AI
    except requests.RequestException as exc:
        logger.debug('Request error for %s: %s', url, exc)
        return None, 'error'

    # Stage 2: Crawl4AI (optional)
    try:
        import asyncio
        from crawl4ai import AsyncWebCrawler

        async def _crawl():
            async with AsyncWebCrawler(headless=True, verbose=False) as crawler:
                result = await crawler.arun(url=url, timeout=20)
                return result.html if result.success else None

        html = asyncio.run(_crawl())
        if html:
            return html, ''
        return None, 'js_required'
    except ImportError:
        return None, 'timeout'  # timed out on requests, Crawl4AI not available
    except Exception as exc:
        logger.debug('Crawl4AI failed for %s: %s', url, exc)
        return None, 'error'


def discover_newsletter(profile_id: str, website: str) -> DiscoveryResult:
    """
    Discover newsletter signup form on a website.

    Tries the homepage first, then common newsletter paths.
    Sets result.error to a structured code on failure for retry routing.
    """
    from urllib.parse import urlparse

    result = DiscoveryResult(profile_id=str(profile_id), website=website)

    parsed = urlparse(website)
    if not parsed.scheme:
        website = 'https://' + website
        parsed = urlparse(website)

    # Build base domain URL for common paths
    base = f'{parsed.scheme}://{parsed.netloc}'

    urls_to_try = [website]
    # Only add common paths if the profile URL isn't already a subpage
    if parsed.path in ('', '/', None):
        for path in ['/newsletter', '/subscribe', '/join', '/email-list']:
            urls_to_try.append(base + path)
    else:
        # Also try root domain — profile URL might be a bio/about page
        if base != website:
            urls_to_try.append(base)
        for path in ['/newsletter', '/subscribe']:
            urls_to_try.append(base + path)

    last_error = 'no_form'

    for url in urls_to_try:
        html, error_code = _fetch_page(url)

        if error_code == 'http_403':
            last_error = 'http_403'
            continue  # try next URL
        if error_code in ('http_404', 'http_other'):
            last_error = error_code
            continue
        if error_code in ('js_required', 'timeout', 'captcha', 'error'):
            last_error = error_code
            continue
        if not html:
            continue

        esp = _detect_esp(html)
        signup_url, form_action = _find_signup_form(html, url)

        if signup_url:
            result.signup_url = signup_url
            result.form_action = form_action
            result.esp_detected = esp
            result.success = True
            return result

        # ESP detected in HTML but no parseable form (iframe embed, JS widget)
        if esp:
            result.signup_url = url
            result.form_action = url
            result.esp_detected = esp
            result.subscribe_method = 'esp_api' if esp in ('ConvertKit', 'Mailchimp') else 'headless'
            result.success = True
            return result

    result.error = last_error
    return result
