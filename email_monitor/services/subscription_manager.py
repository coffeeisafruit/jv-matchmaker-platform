"""
Newsletter subscription manager with three-method fallback and auto double opt-in confirmation.

Method 1: ESP API (preferred — ConvertKit, Mailchimp)
Method 2: HTTP form POST
Method 3: Playwright headless (via owl_framework, last resort)

After subscribing, polls Gmail for confirmation email (up to 5 min) and
auto-clicks the confirmation link to activate the subscription.
"""

import logging
import re
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlencode, urlparse, parse_qs

logger = logging.getLogger(__name__)

RATE_LIMIT_PER_HOUR = 20  # Max subscriptions per hour
CAPTCHA_PATTERNS = re.compile(
    r'(captcha|recaptcha|hcaptcha|turnstile|challenge)',
    re.IGNORECASE,
)


@dataclass
class SubscriptionResult:
    status: str  # active | pending_confirm | failed
    reason: str = ''
    esp: str = ''


def _subscribe_convertkit(form_url: str, email: str, name: str = '') -> SubscriptionResult:
    """Subscribe via ConvertKit form API."""
    import requests

    # Extract form ID from URL
    form_id_match = re.search(r'/forms?/([0-9]+)', form_url)
    if not form_id_match:
        return SubscriptionResult(status='failed', reason='Could not extract ConvertKit form ID')

    form_id = form_id_match.group(1)
    api_url = f'https://api.convertkit.com/v3/forms/{form_id}/subscribe'

    try:
        resp = requests.post(api_url, json={
            'email': email,
            'first_name': name.split()[0] if name else '',
        }, timeout=10)
        if resp.status_code in (200, 201):
            return SubscriptionResult(status='pending_confirm', esp='ConvertKit')
        return SubscriptionResult(
            status='failed', reason=f'ConvertKit API {resp.status_code}: {resp.text[:200]}'
        )
    except Exception as exc:
        return SubscriptionResult(status='failed', reason=str(exc))


def _subscribe_mailchimp(form_action: str, email: str) -> SubscriptionResult:
    """Subscribe via Mailchimp form POST."""
    import requests

    try:
        # Convert /subscribe/post to /subscribe/post-json for API
        api_url = form_action.replace('/post?', '/post-json?').rstrip('?') + '&EMAIL=' + email
        resp = requests.get(api_url, timeout=10)
        data = resp.json() if resp.headers.get('content-type', '').startswith('application/json') else {}
        if data.get('result') == 'success':
            return SubscriptionResult(status='pending_confirm', esp='Mailchimp')
        return SubscriptionResult(status='failed', reason=data.get('msg', 'Mailchimp failed'))
    except Exception as exc:
        return SubscriptionResult(status='failed', reason=str(exc))


def _build_browser_headers(url: str) -> dict:
    """Build realistic browser headers for form submissions."""
    parsed = urlparse(url)
    origin = f'{parsed.scheme}://{parsed.netloc}'
    return {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': origin,
        'Referer': url,
        'Content-Type': 'application/x-www-form-urlencoded',
    }


def _subscribe_form_post(form_action: str, email: str, name: str = '') -> SubscriptionResult:
    """Subscribe via direct HTTP form POST (Method 2).

    Tries POST first, then GET fallback for 405 errors.
    Sends first_name for forms that require it.
    Only flags CAPTCHA on failed responses (avoids false positives).
    """
    import requests

    if not form_action:
        return SubscriptionResult(status='failed', reason='No form action URL')

    headers = _build_browser_headers(form_action)
    first_name = name.split()[0] if name else ''
    form_data = {
        'email': email, 'EMAIL': email, 'email_address': email,
        'name': name, 'first_name': first_name, 'FNAME': first_name,
    }

    try:
        resp = requests.post(
            form_action, data=form_data, headers=headers,
            timeout=15, allow_redirects=True,
        )

        # GET fallback: some forms (especially ESPs) only accept GET
        if resp.status_code == 405:
            headers.pop('Content-Type', None)
            resp = requests.get(
                form_action, params={'email': email, 'EMAIL': email},
                headers=headers, timeout=15, allow_redirects=True,
            )

        body = resp.text.lower()

        # Only flag CAPTCHA if the response indicates failure
        # (many successful pages mention 'recaptcha' in scripts/footer)
        if resp.status_code >= 400 and CAPTCHA_PATTERNS.search(body):
            return SubscriptionResult(status='failed', reason='CAPTCHA detected')

        if resp.status_code < 400:
            # Check for hard CAPTCHA gate (entire page is a challenge, not just a mention)
            if re.search(r'(please verify|complete the challenge|prove you.re human)', body):
                return SubscriptionResult(status='failed', reason='CAPTCHA challenge page')
            return SubscriptionResult(status='pending_confirm')

        return SubscriptionResult(
            status='failed', reason=f'Form POST returned {resp.status_code}'
        )
    except Exception as exc:
        return SubscriptionResult(status='failed', reason=str(exc)[:200])


def _subscribe_headless(signup_url: str, email: str) -> SubscriptionResult:
    """Subscribe via Playwright headless browser (Method 3 — last resort)."""
    try:
        # owl_framework provides Playwright automation
        from owl_framework.browser import BrowserSession
        with BrowserSession(headless=True) as session:
            session.navigate(signup_url)
            session.fill('[type="email"], [name="email"], [name="EMAIL"]', email)
            session.click('[type="submit"], button:contains("Subscribe"), button:contains("Join")')
            time.sleep(2)
            body = session.content().lower()
            if CAPTCHA_PATTERNS.search(body):
                return SubscriptionResult(status='failed', reason='CAPTCHA detected')
            return SubscriptionResult(status='pending_confirm', esp='headless')
    except ImportError:
        return SubscriptionResult(status='failed', reason='owl_framework not available')
    except Exception as exc:
        return SubscriptionResult(status='failed', reason=str(exc))


def _wait_for_confirmation_email(
    monitor_address: str, timeout_seconds: int = 300, poll_interval: int = 30
) -> Optional[dict]:
    """Delegate to gmail_poller.wait_for_confirmation_email (no-SDK version)."""
    from email_monitor.services.gmail_poller import wait_for_confirmation_email
    return wait_for_confirmation_email(monitor_address, timeout_seconds, poll_interval)


def _extract_and_click_confirm_link(email_body: dict) -> bool:
    """Extract confirmation link from email body and GET it."""
    from email_monitor.services.gmail_poller import (
        CONFIRMATION_LINKS, _click_confirmation_link
    )
    for body in (email_body.get('body_html', ''), email_body.get('body_text', '')):
        match = CONFIRMATION_LINKS.search(body)
        if match:
            return _click_confirmation_link(match.group(0))
    return False


def subscribe_and_confirm(
    profile_id: str,
    monitor_address: str,
    signup_url: str,
    form_action: str,
    esp_detected: str,
    profile_name: str = '',
    confirm_timeout: int = 60,  # seconds to wait for confirmation email (default: 60s, Railway cron uses 300)
) -> SubscriptionResult:
    """
    Subscribe to a newsletter and auto-confirm the double opt-in.

    Tries ESP API first, then form POST, then headless browser.
    After subscribing, polls Gmail up to 5 min for confirmation email.
    """
    # If no form_action stored, try a quick single-page re-discovery (no subpage crawl)
    if not form_action and signup_url and not signup_url.startswith('http_'):
        try:
            from email_monitor.services.newsletter_discoverer import _fetch_page, _find_signup_form
            html, _ = _fetch_page(signup_url)
            if html:
                _, rediscovered_action = _find_signup_form(html, signup_url)
                if rediscovered_action:
                    form_action = rediscovered_action
        except Exception:
            pass  # Don't let re-discovery failures block subscription

    # Method 1: ESP API (ConvertKit/Kit and Mailchimp have direct API support)
    if esp_detected in ('ConvertKit', 'Kit'):
        result = _subscribe_convertkit(form_action or signup_url, monitor_address, profile_name)
    elif esp_detected == 'Mailchimp':
        result = _subscribe_mailchimp(form_action or signup_url, monitor_address)
    else:
        result = SubscriptionResult(status='failed', reason='No ESP API — trying form POST')

    # Method 2: Form POST to discovered form action
    if result.status == 'failed' and form_action:
        result = _subscribe_form_post(form_action, monitor_address, profile_name)

    # Method 2b: POST to signup_url directly (works for many ESPs that accept POST on page URL)
    if result.status == 'failed' and signup_url and signup_url != form_action:
        result = _subscribe_form_post(signup_url, monitor_address, profile_name)

    if result.status == 'failed':
        return result

    # Skip confirmation polling if confirm_timeout=0 (bulk mode — cron handles async)
    if confirm_timeout <= 0:
        return SubscriptionResult(status='pending_confirm', reason='Bulk mode — cron will confirm', esp=result.esp)

    # Attempt immediate confirmation
    from django.conf import settings
    if not settings.GMAIL_MONITOR_REFRESH_TOKEN:
        return SubscriptionResult(status='pending_confirm', reason='Gmail not configured', esp=result.esp)

    confirmation_email = _wait_for_confirmation_email(monitor_address, timeout_seconds=confirm_timeout)
    if confirmation_email and _extract_and_click_confirm_link(confirmation_email):
        return SubscriptionResult(status='active', esp=result.esp)

    return SubscriptionResult(status='pending_confirm', reason='Confirmation email not received', esp=result.esp)
