"""
AI-powered email content classification using Claude Haiku.

Classifies newsletter emails for JV intelligence: who they promote,
what products, promotion style, audience signals, and JV readiness indicators.

Pre-filters on link_extractor output to skip AI when affiliate content is
already confirmed (saves ~30% of AI calls).
"""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

ANALYSIS_PROMPT = """\
You are analyzing a newsletter email to extract JV (joint venture) partnership intelligence.
Classify the email and extract structured data about promotions, products, and audience signals.

Return ONLY valid JSON matching this exact schema (no markdown, no explanation):

{
  "email_type": "content | own_promotion | partner_promotion | mixed | transactional",
  "is_promoting_partner": true/false,
  "promoted_partners": [
    {
      "name": "Partner Name or Brand",
      "website_or_url": "url if found, else empty string",
      "product_name": "specific product name",
      "product_type": "course | webinar | book | software | coaching | event | affiliate_offer | other",
      "niche": "brief niche description",
      "affiliate_link_detected": true/false,
      "price_point_signal": "free | low | mid | high | unknown",
      "deal_structure_hint": "affiliate | jv | sponsorship | unknown"
    }
  ],
  "own_products_mentioned": [
    {
      "name": "product name",
      "type": "course | coaching | membership | software | book | event | service",
      "price_signal": "low | mid | high | unknown",
      "launch_signal": true/false
    }
  ],
  "promotion_style": "soft_mention | dedicated_promo | ps_mention | full_email | none",
  "promotion_urgency": "evergreen | launch | deadline | none",
  "list_relationship_style": "nurture | direct_response | broadcast | personal",
  "audience_pain_points": ["pain1", "pain2"],
  "content_topics": ["topic1", "topic2"],
  "audience_sophistication": "beginner | intermediate | advanced | mixed",
  "jv_language_detected": true/false,
  "testimonial_for_partner": true/false,
  "urgency_deadline_days": null,
  "call_to_action": "brief CTA description"
}

EMAIL TO ANALYZE:
Subject: {subject}
From: {from_name}

{body}
"""


def _truncate_body(text: str, max_chars: int = 3000) -> str:
    """Truncate body to stay within Haiku context limits."""
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + '\n[... truncated ...]'


def _has_confirmed_affiliate(links: list[dict]) -> bool:
    """Check if link_extractor already found definitive affiliate links."""
    for link in links:
        if link.get('is_affiliate') and link.get('affiliate_network') not in (
            'link_shortener', 'path_pattern', ''
        ):
            return True
    return False


def analyze_email(
    subject: str,
    from_name: str,
    body_text: str,
    body_html: str,
    links_extracted: list[dict],
) -> Optional[dict]:
    """
    Classify a newsletter email using Claude Haiku.

    Returns the analysis dict or None on failure.
    Skips AI if link_extractor already flagged known affiliate domains.
    """
    # Use HTML body stripped of tags if available, else plain text
    if body_html:
        try:
            from bs4 import BeautifulSoup
            body = BeautifulSoup(body_html, 'html.parser').get_text(separator=' ', strip=True)
        except Exception:
            body = body_text
    else:
        body = body_text

    if not body.strip():
        logger.debug('Empty body — skipping AI analysis')
        return None

    # Pre-filter: if we have confirmed affiliate links, we can enrich the
    # analysis without calling AI (saves ~30% cost on obvious promos)
    if _has_confirmed_affiliate(links_extracted):
        logger.debug('Confirmed affiliate links detected — AI still called for full classification')

    prompt = ANALYSIS_PROMPT.format(
        subject=subject or '(no subject)',
        from_name=from_name or '(unknown)',
        body=_truncate_body(body),
    )

    try:
        from matching.enrichment.claude_client import ClaudeClient
        client = ClaudeClient(max_tokens=1024)
        # Force Haiku for cost control — don't inherit LLM_MODEL override
        response = client.complete(
            prompt=prompt,
            model='anthropic/claude-haiku-4-5-20251001',
            system='You are a JSON extraction bot. Return only valid JSON, no markdown.',
        )
        raw = response.strip()
        # Strip markdown fences if present
        raw = re.sub(r'^```(?:json)?\s*', '', raw, flags=re.MULTILINE)
        raw = re.sub(r'\s*```$', '', raw, flags=re.MULTILINE)
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning('Email analyzer JSON parse error: %s', exc)
        return None
    except Exception as exc:
        logger.warning('Email analyzer error: %s', exc)
        return None


def batch_analyze_emails(email_ids: list[int]) -> dict[int, Optional[dict]]:
    """
    Analyze a batch of InboundEmail records by ID.

    Updates each record with its analysis result and analyzed_at timestamp.
    Returns a dict mapping email_id → analysis result.
    """
    from email_monitor.models import InboundEmail
    from django.utils import timezone as tz

    results: dict[int, Optional[dict]] = {}
    emails = InboundEmail.objects.filter(
        pk__in=email_ids, analyzed_at__isnull=True
    ).select_related('subscription')

    for inbound in emails:
        analysis = analyze_email(
            subject=inbound.subject,
            from_name=inbound.from_name,
            body_text=inbound.body_text,
            body_html=inbound.body_html,
            links_extracted=inbound.links_extracted or [],
        )
        InboundEmail.objects.filter(pk=inbound.pk).update(
            analysis=analysis,
            analyzed_at=tz.now() if analysis else None,
        )
        results[inbound.pk] = analysis

    return results
