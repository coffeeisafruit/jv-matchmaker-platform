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


def _subscribe_form_post(form_action: str, email: str) -> SubscriptionResult:
    """Subscribe via direct HTTP form POST (Method 2)."""
    import requests

    if not form_action:
        return SubscriptionResult(status='failed', reason='No form action URL')

    try:
        resp = requests.post(
            form_action,
            data={'email': email, 'EMAIL': email, 'email_address': email},
            headers={
                'User-Agent': 'Mozilla/5.0',
                'Referer': form_action,
            },
            timeout=15,
            allow_redirects=True,
        )
        body = resp.text.lower()
        if CAPTCHA_PATTERNS.search(body):
            return SubscriptionResult(status='failed', reason='CAPTCHA detected')
        if resp.status_code < 400:
            return SubscriptionResult(status='pending_confirm')
        return SubscriptionResult(
            status='failed', reason=f'Form POST returned {resp.status_code}'
        )
    except Exception as exc:
        return SubscriptionResult(status='failed', reason=str(exc))


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
    # If no form_action was stored, re-discover it from the signup URL
    if not form_action and signup_url and not signup_url.startswith('http_'):
        from email_monitor.services.newsletter_discoverer import _fetch_page, _find_signup_form
        html, _ = _fetch_page(signup_url)
        if html:
            _, rediscovered_action = _find_signup_form(html, signup_url)
            if rediscovered_action:
                form_action = rediscovered_action

    # Method 1: ESP API
    if esp_detected == 'ConvertKit':
        result = _subscribe_convertkit(form_action or signup_url, monitor_address, profile_name)
    elif esp_detected == 'Mailchimp':
        result = _subscribe_mailchimp(form_action or signup_url, monitor_address)
    else:
        result = SubscriptionResult(status='failed', reason='ESP not matched — trying form POST')

    # Method 2: Form POST (only when we have a real form action, not just a page URL)
    if result.status == 'failed' and form_action and form_action != signup_url:
        result = _subscribe_form_post(form_action, monitor_address)

    # Method 2b: If no form_action but we have a signup_url, try posting to it directly
    if result.status == 'failed' and signup_url and not esp_detected:
        result = _subscribe_form_post(signup_url, monitor_address)

    # Method 3: Headless (last resort — only if owl_framework available)
    if result.status == 'failed' and signup_url:
        result = _subscribe_headless(signup_url, monitor_address)

    if result.status == 'failed':
        return result

    # Attempt immediate confirmation (5 min window)
    from django.conf import settings
    if not settings.GMAIL_MONITOR_REFRESH_TOKEN:
        # Gmail not configured — return pending_confirm, cron will handle
        return SubscriptionResult(status='pending_confirm', reason='Gmail not configured', esp=result.esp)

    confirmation_email = _wait_for_confirmation_email(monitor_address, timeout_seconds=confirm_timeout)
    if confirmation_email and _extract_and_click_confirm_link(confirmation_email):
        return SubscriptionResult(status='active', esp=result.esp)

    return SubscriptionResult(status='pending_confirm', reason='Confirmation email not received within 5 min', esp=result.esp)
