"""
Gmail API polling for the newsletter monitor inbox.

Uses raw HTTP requests (no google-api-python-client dependency) — consistent
with how the outreach app refreshes tokens in email_service.py.

Fetches unread messages, links them to MonitoredSubscription via +addressing
on the To: header, creates InboundEmail records, and auto-confirms double
opt-in confirmation emails.
"""

import base64
import email as email_lib
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from django.conf import settings

logger = logging.getLogger(__name__)

GMAIL_API = 'https://gmail.googleapis.com/gmail/v1'
TOKEN_URI = 'https://oauth2.googleapis.com/token'

CONFIRMATION_SENDERS = re.compile(
    r'(confirmations@mail\.beehiiv\.com|donotreply@convertkit\.com|'
    r'mc\.sendgrid\.net|noreply@mailchimp\.com|noreply@activecampaign\.com|'
    r'noreply@aweber\.com|confirm@getresponse\.com)',
    re.IGNORECASE,
)
CONFIRMATION_SUBJECTS = re.compile(
    r'\b(confirm|verify|activate|please confirm|double opt.?in)\b',
    re.IGNORECASE,
)
CONFIRMATION_LINKS = re.compile(
    r'https?://[^\s"\'<>]+(?:/confirm|/activate|/verify|[?&](?:confirm|token|activate))[^\s"\'<>]*',
    re.IGNORECASE,
)

# Module-level token cache (refreshed lazily per process)
_access_token: str = ''
_token_expires_at: float = 0.0


def _get_access_token() -> str:
    """
    Return a valid access token, refreshing if expired.
    Matches the same refresh pattern as outreach/email_service.py._refresh_google_token().
    """
    global _access_token, _token_expires_at

    if _access_token and time.time() < _token_expires_at - 60:
        return _access_token

    resp = requests.post(TOKEN_URI, data={
        'client_id': settings.GMAIL_MONITOR_CLIENT_ID,
        'client_secret': settings.GMAIL_MONITOR_CLIENT_SECRET,
        'refresh_token': settings.GMAIL_MONITOR_REFRESH_TOKEN,
        'grant_type': 'refresh_token',
    }, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    _access_token = data['access_token']
    _token_expires_at = time.time() + data.get('expires_in', 3600)
    return _access_token


def _gmail_get(path: str, params: dict = None) -> dict:
    """Authenticated GET to Gmail API."""
    token = _get_access_token()
    resp = requests.get(
        f'{GMAIL_API}{path}',
        headers={'Authorization': f'Bearer {token}'},
        params=params or {},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def _gmail_post(path: str, body: dict) -> dict:
    """Authenticated POST to Gmail API."""
    token = _get_access_token()
    resp = requests.post(
        f'{GMAIL_API}{path}',
        headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
        json=body,
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def _mark_read(msg_id: str) -> None:
    """Remove UNREAD label from a message."""
    try:
        _gmail_post(f'/users/me/messages/{msg_id}/modify',
                    {'removeLabelIds': ['UNREAD']})
    except Exception as exc:
        logger.debug('Failed to mark message %s as read: %s', msg_id, exc)


def _extract_header(headers: list[dict], name: str) -> str:
    for h in headers:
        if h.get('name', '').lower() == name.lower():
            return h.get('value', '')
    return ''


def _extract_uuid_prefix(to_address: str) -> Optional[str]:
    match = re.search(r'\+([a-f0-9]{8})@', to_address, re.IGNORECASE)
    return match.group(1).lower() if match else None


def _decode_part(part: dict) -> str:
    data = part.get('body', {}).get('data', '')
    if not data:
        return ''
    return base64.urlsafe_b64decode(data + '==').decode('utf-8', errors='replace')


def _extract_body(msg: dict) -> tuple[str, str]:
    text, html = '', ''

    def walk(part: dict) -> None:
        nonlocal text, html
        mime = part.get('mimeType', '')
        if mime == 'text/plain':
            text += _decode_part(part)
        elif mime == 'text/html':
            html += _decode_part(part)
        for sub in part.get('parts', []):
            walk(sub)

    walk(msg.get('payload', {}))
    return text, html


def _is_confirmation_email(from_addr: str, subject: str) -> bool:
    return bool(CONFIRMATION_SENDERS.search(from_addr) or CONFIRMATION_SUBJECTS.search(subject))


def _extract_confirmation_link(body_text: str, body_html: str) -> Optional[str]:
    for body in (body_html, body_text):
        match = CONFIRMATION_LINKS.search(body)
        if match:
            return match.group(0)
    return None


def _click_confirmation_link(url: str) -> bool:
    try:
        resp = requests.get(url, timeout=15, allow_redirects=True)
        return resp.status_code < 400
    except Exception as exc:
        logger.warning('Confirmation click failed: %s', exc)
        return False


def poll_inbox(max_results: int = 100) -> dict:
    """
    Poll the monitor Gmail inbox for unread messages.

    Returns summary: {processed, confirmed, skipped, errors}
    """
    from email_monitor.models import MonitoredSubscription, InboundEmail
    from email_monitor.services.link_extractor import extract_links

    if not all([settings.GMAIL_MONITOR_REFRESH_TOKEN,
                settings.GMAIL_MONITOR_CLIENT_ID,
                settings.GMAIL_MONITOR_CLIENT_SECRET]):
        logger.error('Gmail Monitor credentials not configured.')
        return {'processed': 0, 'confirmed': 0, 'skipped': 0, 'errors': 1}

    stats = {'processed': 0, 'confirmed': 0, 'skipped': 0, 'errors': 0}

    try:
        result = _gmail_get('/users/me/messages', {'q': 'is:unread', 'maxResults': max_results})
    except Exception as exc:
        logger.error('Gmail list failed: %s', exc)
        stats['errors'] += 1
        return stats

    messages = result.get('messages', [])
    logger.info('Found %d unread messages', len(messages))

    for msg_ref in messages:
        msg_id = msg_ref['id']
        try:
            msg = _gmail_get(f'/users/me/messages/{msg_id}', {'format': 'full'})
            headers = msg.get('payload', {}).get('headers', [])
            to_addr = _extract_header(headers, 'To')
            from_addr = _extract_header(headers, 'From')
            subject = _extract_header(headers, 'Subject')
            date_str = _extract_header(headers, 'Date')

            # Parse received_at
            try:
                received_at = email_lib.utils.parsedate_to_datetime(date_str)
                if received_at.tzinfo is None:
                    received_at = received_at.replace(tzinfo=timezone.utc)
            except Exception:
                received_at = datetime.now(timezone.utc)

            uuid_prefix = _extract_uuid_prefix(to_addr)
            subscription = None
            if uuid_prefix:
                subscription = MonitoredSubscription.objects.filter(
                    monitor_address__icontains=f'+{uuid_prefix}@'
                ).first()

            body_text, body_html = _extract_body(msg)

            # Handle confirmation emails (auto-click to activate)
            if _is_confirmation_email(from_addr, subject):
                confirm_link = _extract_confirmation_link(body_text, body_html)
                if confirm_link and _click_confirmation_link(confirm_link):
                    if subscription:
                        MonitoredSubscription.objects.filter(pk=subscription.pk).update(
                            status='active'
                        )
                    stats['confirmed'] += 1
                    logger.info('Confirmed subscription via %s', confirm_link[:80])
                _mark_read(msg_id)
                continue

            # Regular newsletter email — needs a matched subscription
            if subscription is None:
                logger.debug('No subscription for To: %s — skipping', to_addr)
                stats['skipped'] += 1
                _mark_read(msg_id)
                continue

            # Dedup
            if InboundEmail.objects.filter(gmail_message_id=msg_id).exists():
                stats['skipped'] += 1
                _mark_read(msg_id)
                continue

            links = extract_links(body_html or body_text)

            # Parse "Name <email>" format
            from_name = ''
            name_match = re.match(r'^"?(.+?)"?\s*<', from_addr)
            if name_match:
                from_name = name_match.group(1).strip('"\'')
            raw_from = re.search(r'<(.+?)>', from_addr)
            from_email = raw_from.group(1) if raw_from else from_addr

            InboundEmail.objects.create(
                subscription=subscription,
                gmail_message_id=msg_id,
                from_address=from_email,
                from_name=from_name,
                subject=subject,
                received_at=received_at,
                body_text=body_text[:200_000],
                body_html=body_html[:200_000],
                links_extracted=links,
            )
            MonitoredSubscription.objects.filter(pk=subscription.pk).update(
                last_email_received_at=received_at,
                total_emails_received=subscription.total_emails_received + 1,
                status='active',
            )
            _mark_read(msg_id)
            stats['processed'] += 1

        except Exception as exc:
            logger.exception('Error processing message %s: %s', msg_id, exc)
            stats['errors'] += 1

    # Auto-fail subscriptions stuck in pending_confirm for >48h
    from django.utils import timezone as tz
    from datetime import timedelta
    stale = MonitoredSubscription.objects.filter(
        status='pending_confirm',
        subscribed_at__lt=tz.now() - timedelta(hours=48),
    ).update(status='failed')
    if stale:
        logger.info('Auto-failed %d stale pending_confirm subscriptions', stale)

    return stats


def wait_for_confirmation_email(
    monitor_address: str, timeout_seconds: int = 300, poll_interval: int = 30
) -> Optional[dict]:
    """
    Poll Gmail for a confirmation email addressed to monitor_address.
    Used by subscription_manager immediately after subscribing.

    Returns {'body_text', 'body_html', 'msg_id'} or None.
    """
    elapsed = 0
    while elapsed < timeout_seconds:
        try:
            result = _gmail_get('/users/me/messages', {
                'q': f'is:unread to:{monitor_address} subject:(confirm OR verify OR activate)',
                'maxResults': 5,
            })
            messages = result.get('messages', [])
            if messages:
                msg = _gmail_get(f'/users/me/messages/{messages[0]["id"]}', {'format': 'full'})
                body_text, body_html = _extract_body(msg)
                return {'body_text': body_text, 'body_html': body_html, 'msg_id': messages[0]['id']}
        except Exception as exc:
            logger.debug('Confirmation poll error: %s', exc)

        time.sleep(poll_interval)
        elapsed += poll_interval

    return None
