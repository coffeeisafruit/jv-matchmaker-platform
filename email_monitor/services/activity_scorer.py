"""
Email activity scoring — converts raw InboundEmail records into
mailing_activity_score (0-1) and promotion_willingness_score (0-1).

Scoring thresholds:
  mailing_activity_score:
    5+/week  → 1.0 | 3-4/week → 0.85 | 1-2/week → 0.65
    biweekly → 0.4 | occasional → 0.2 | dormant → 0.0

  promotion_willingness_score (% of emails that promote a partner):
    25%+  → 1.0 | 15-24% → 0.8 | 5-14% → 0.5
    1-4%  → 0.3 | 0%      → 0.1 | no data → None
"""

import logging
from datetime import date, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


def compute_mailing_activity_score(emails_per_week: float) -> float:
    """Convert average emails/week to a 0-1 activity score."""
    if emails_per_week >= 5:
        return 1.0
    elif emails_per_week >= 3:
        return 0.85
    elif emails_per_week >= 1:
        return 0.65
    elif emails_per_week >= 0.5:
        return 0.4
    elif emails_per_week > 0:
        return 0.2
    return 0.0


def compute_promotion_willingness_score(
    total_emails: int, promotional_emails: int
) -> Optional[float]:
    """
    Convert partner promotion ratio to a 0-1 score.

    Returns None if no emails at all (null-aware — won't suppress scoring).
    """
    if total_emails == 0:
        return None
    ratio = promotional_emails / total_emails
    if ratio >= 0.25:
        return 1.0
    elif ratio >= 0.15:
        return 0.8
    elif ratio >= 0.05:
        return 0.5
    elif ratio >= 0.01:
        return 0.3
    return 0.1  # mails but never promotes — still signals an active list


def build_promotion_network(profile_id: str, month: date) -> dict:
    """
    Build the promotion_network JSON for a profile from the past 90 days of AI analyses.

    Returns a dict matching the SupabaseProfile.promotion_network schema.
    """
    from email_monitor.models import InboundEmail, MonitoredSubscription
    from django.db.models import Q

    ninety_days_ago = month - timedelta(days=90)

    subscriptions = MonitoredSubscription.objects.filter(profile_id=profile_id)
    emails = InboundEmail.objects.filter(
        subscription__in=subscriptions,
        received_at__date__gte=ninety_days_ago,
        analyzed_at__isnull=False,
        analysis__isnull=False,
    )

    # Aggregate promoted partners across all emails
    partner_counts: dict[str, dict] = {}
    own_offers: list[dict] = []
    promotion_style_counts: dict[str, int] = {}

    for email_record in emails:
        analysis = email_record.analysis or {}

        # Count promoted partners
        for partner in analysis.get('promoted_partners', []):
            key = (partner.get('name', '') or '').lower().strip()
            if not key:
                continue
            if key not in partner_counts:
                partner_counts[key] = {
                    'name': partner.get('name', ''),
                    'domain': _extract_domain(partner.get('website_or_url', '')),
                    'product_type': partner.get('product_type', ''),
                    'niche': partner.get('niche', ''),
                    'count': 0,
                    'first_seen': email_record.received_at.date().isoformat(),
                    'last_seen': email_record.received_at.date().isoformat(),
                }
            partner_counts[key]['count'] += 1
            partner_counts[key]['last_seen'] = email_record.received_at.date().isoformat()

        # Own offers (deduplicated by name)
        for offer in analysis.get('own_products_mentioned', []):
            offer_name = (offer.get('name', '') or '').lower()
            if offer_name and not any(o['name'].lower() == offer_name for o in own_offers):
                own_offers.append({
                    'name': offer.get('name', ''),
                    'type': offer.get('type', ''),
                    'price_signal': offer.get('price_signal', ''),
                })

        # Promotion style counts
        style = analysis.get('promotion_style', '')
        if style and style != 'none':
            promotion_style_counts[style] = promotion_style_counts.get(style, 0) + 1

    promoted_partners = sorted(partner_counts.values(), key=lambda x: -x['count'])
    peak_style = max(promotion_style_counts, key=promotion_style_counts.get) if promotion_style_counts else ''

    # Compute avg emails/week for cadence
    total_emails = emails.count()
    weeks = max(1, 90 / 7)
    avg_per_week = total_emails / weeks

    return {
        'promoted_partners': promoted_partners,
        'own_offers': own_offers,
        'promotion_cadence': {
            'avg_per_week': round(avg_per_week, 2),
            'style': peak_style,
        },
        'total_partner_promotions': sum(p['count'] for p in promoted_partners),
        'unique_partners': len(promoted_partners),
    }


def _extract_domain(url: str) -> str:
    """Extract bare domain from a URL."""
    if not url:
        return ''
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url if '://' in url else 'https://' + url)
        return (parsed.hostname or '').removeprefix('www.')
    except Exception:
        return ''


def compute_jv_readiness_delta(
    profile_id: str,
    was_active_prior_month: bool,
    total_emails_30d: int,
    partner_promos_30d: int,
    unique_partners_90d: int,
    has_own_products: bool,
) -> int:
    """
    Compute jv_readiness_score delta based on email monitor signals.

    Returns an integer delta (positive or negative) to add to jv_readiness_score.
    Capped so total score stays in 0-100 range (enforced by SQL LEAST/GREATEST).
    """
    delta = 0

    # Active mailer: ≥4 emails in last 30 days
    if total_emails_30d >= 4:
        delta += 5

    # Partner promotion frequency
    if partner_promos_30d >= 8:
        delta += 12
    elif partner_promos_30d >= 1:
        delta += 8

    # Diversity of promoted partners
    if unique_partners_90d >= 3:
        delta += 5

    # Has own products/offers
    if has_own_products:
        delta += 3

    # Dormant penalty — was active (had prior emails), now 0 for 60 days
    if was_active_prior_month and total_emails_30d == 0:
        delta -= 5

    return delta
