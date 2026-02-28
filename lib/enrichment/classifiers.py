"""
Profile Classification Service

Zero-shot classification for niche categories, offer types, and content style.
Replaces the keyword-regex niche mapping in scripts/export_community_graph.py
with semantic classification that handles edge cases and multi-label assignment.

Usage:
    from lib.enrichment.hf_client import HFClient
    from lib.enrichment.classifiers import ProfileClassificationService

    service = ProfileClassificationService(HFClient())

    # Classify niche
    niches = service.classify_niche(profile_dict)
    # [{"category": "Leadership & Management", "confidence": 0.92}, ...]

    # Classify offer types
    offers = service.classify_offer_types(profile_dict)
    # [{"type": "online_course", "confidence": 0.88}, {"type": "coaching_1on1", "confidence": 0.72}]
"""

import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger('enrichment.hf.classifiers')


# Expanded from the 11-category keyword regex in export_community_graph.py (lines 37-75)
# to 25 categories that cover the full JV MatchMaker member base
NICHE_LABELS = [
    "Business Coaching & Consulting",
    "Health & Wellness",
    "Mindset & Personal Development",
    "Relationships & Dating",
    "Spirituality & Energy Work",
    "Fitness & Nutrition",
    "Financial Education & Wealth Building",
    "Marketing & Sales",
    "Leadership & Management",
    "Parenting & Family",
    "Career Development & Transitions",
    "Creative Arts & Expression",
    "Real Estate & Property Investing",
    "Technology & SaaS",
    "Education & Online Learning",
    "Productivity & Performance",
    "Women's Empowerment",
    "Mental Health & Therapy",
    "Alternative & Holistic Health",
    "Grief & Trauma Recovery",
    "Communication & Public Speaking",
    "Entrepreneurship & Startups",
    "Corporate Training & HR",
    "Life Coaching (General)",
    "Service Provider / Agency",
]

# Structured taxonomy for JV partnership format matching
# These map to specific JV collaboration formats
OFFER_TYPE_LABELS = [
    "online_course",
    "coaching_1on1",
    "group_coaching",
    "mastermind",
    "done_for_you_service",
    "software_saas",
    "book_author",
    "speaking",
    "consulting",
    "certification_program",
    "membership_community",
    "affiliate_network",
    "agency",
    "digital_product",
    "live_events",
    "podcast",
    "retreat_workshop",
]

# Human-readable labels for zero-shot (model performs better with natural language)
OFFER_TYPE_NATURAL_LABELS = {
    "online_course": "online course or digital training program",
    "coaching_1on1": "one-on-one coaching or mentoring",
    "group_coaching": "group coaching program",
    "mastermind": "mastermind group or peer advisory",
    "done_for_you_service": "done-for-you service or implementation",
    "software_saas": "software product or SaaS platform",
    "book_author": "published book author",
    "speaking": "professional speaking or keynote presentations",
    "consulting": "consulting or advisory services",
    "certification_program": "certification or accreditation program",
    "membership_community": "membership site or paid community",
    "affiliate_network": "affiliate program or referral network",
    "agency": "agency or managed services",
    "digital_product": "digital product like templates, toolkits, or downloads",
    "live_events": "live events, conferences, or summits",
    "podcast": "podcast or audio show",
    "retreat_workshop": "retreat or in-person workshop",
}

CONTENT_STYLE_LABELS = [
    "educational and teaching-focused",
    "inspirational and motivational",
    "tactical and step-by-step practical",
    "storytelling and narrative-driven",
    "data-driven and evidence-based",
    "provocative and contrarian",
    "nurturing and empathetic",
]

# Minimum confidence to include in results
CONFIDENCE_THRESHOLD = 0.3
MAX_LABELS_PER_FIELD = 3


class ProfileClassificationService:
    """
    Zero-shot classification for profile enrichment.

    Uses HF Inference API with DeBERTa-v3 or BART models for
    classification without training data. Results are stored in
    niche_categories, offer_types, and content_style JSON fields.
    """

    def __init__(self, hf_client, model: str = None):
        """
        Args:
            hf_client: HFClient instance
            model: Override classification model
        """
        self.hf = hf_client
        self.model = model

    def classify_niche(self, profile: dict) -> list[dict]:
        """
        Classify profile into niche categories.

        Concatenates niche + what_you_do + who_you_serve for richer context,
        then runs zero-shot classification against 25 categories.

        Args:
            profile: Dict with text fields (niche, what_you_do, who_you_serve)

        Returns:
            List of {"category": str, "confidence": float} dicts,
            sorted by confidence, filtered by threshold, max 3 results.
        """
        parts = []
        for field in ['niche', 'what_you_do', 'who_you_serve', 'offering']:
            val = profile.get(field, '')
            if val and isinstance(val, str) and len(val.strip()) > 3:
                parts.append(val.strip())

        if not parts:
            return []

        text = ' | '.join(parts)

        scores = self.hf.classify_zero_shot(
            text=text,
            labels=NICHE_LABELS,
            multi_label=True,
            model=self.model,
        )

        if not scores:
            return []

        results = [
            {"category": label, "confidence": round(score, 4)}
            for label, score in scores.items()
            if score >= CONFIDENCE_THRESHOLD
        ]
        results.sort(key=lambda x: x['confidence'], reverse=True)
        return results[:MAX_LABELS_PER_FIELD]

    def classify_offer_types(self, profile: dict) -> list[dict]:
        """
        Classify offering into structured taxonomy.

        Uses natural-language label descriptions for better zero-shot accuracy.

        Args:
            profile: Dict with text fields (offering, signature_programs, what_you_do)

        Returns:
            List of {"type": str, "confidence": float} dicts.
        """
        parts = []
        for field in ['offering', 'signature_programs', 'what_you_do']:
            val = profile.get(field, '')
            if val and isinstance(val, str) and len(val.strip()) > 3:
                parts.append(val.strip())

        if not parts:
            return []

        text = ' | '.join(parts)

        # Use natural language labels for better accuracy
        natural_labels = list(OFFER_TYPE_NATURAL_LABELS.values())
        scores = self.hf.classify_zero_shot(
            text=text,
            labels=natural_labels,
            multi_label=True,
            model=self.model,
        )

        if not scores:
            return []

        # Map back to code labels
        natural_to_code = {v: k for k, v in OFFER_TYPE_NATURAL_LABELS.items()}
        results = []
        for label, score in scores.items():
            if score >= CONFIDENCE_THRESHOLD:
                code = natural_to_code.get(label, label)
                results.append({"type": code, "confidence": round(score, 4)})

        results.sort(key=lambda x: x['confidence'], reverse=True)
        return results[:MAX_LABELS_PER_FIELD + 2]  # Allow up to 5 offer types

    def classify_content_style(self, profile: dict) -> dict:
        """
        Classify content/communication style (Phase C enrichment).

        Args:
            profile: Dict with text fields

        Returns:
            {"primary": str, "secondary": str | None, "confidence": float}
        """
        parts = []
        for field in ['what_you_do', 'bio', 'social_proof']:
            val = profile.get(field, '')
            if val and isinstance(val, str) and len(val.strip()) > 10:
                parts.append(val.strip())

        if not parts:
            return {}

        text = ' | '.join(parts)

        scores = self.hf.classify_zero_shot(
            text=text,
            labels=CONTENT_STYLE_LABELS,
            multi_label=False,  # Pick dominant style
            model=self.model,
        )

        if not scores:
            return {}

        sorted_labels = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        result = {
            "primary": sorted_labels[0][0],
            "confidence": round(sorted_labels[0][1], 4),
        }
        if len(sorted_labels) > 1 and sorted_labels[1][1] >= 0.2:
            result["secondary"] = sorted_labels[1][0]

        return result

    def classify_profile_batch(
        self,
        profiles: list[dict],
        include_content_style: bool = False,
    ) -> list[dict]:
        """
        Run all classifications on a batch of profiles.

        Args:
            profiles: List of profile dicts
            include_content_style: Also run content style classification (slower)

        Returns:
            List of dicts with niche_categories, offer_types, and optionally content_style.
        """
        results = []
        total = len(profiles)

        for i, profile in enumerate(profiles):
            result = {
                'niche_categories': self.classify_niche(profile),
                'offer_types': self.classify_offer_types(profile),
                'classification_updated_at': datetime.now(timezone.utc).isoformat(),
            }

            if include_content_style:
                result['content_style'] = self.classify_content_style(profile)

            results.append(result)

            if (i + 1) % 25 == 0 or i == total - 1:
                logger.info(f"Classification progress: {i + 1}/{total} profiles")

        return results

    def store_classifications(
        self,
        supabase_client,
        profile_id: str,
        classifications: dict,
    ) -> bool:
        """
        Write classification results to Supabase profiles table.

        Follows the same provenance pattern as consolidate_to_supabase_batch().

        Args:
            supabase_client: Initialized Supabase client
            profile_id: UUID of the profile
            classifications: Output from classify_niche/classify_offer_types

        Returns:
            True on success.
        """
        update_data = {}
        for key in ['niche_categories', 'offer_types', 'content_style',
                     'classification_updated_at']:
            if key in classifications:
                update_data[key] = classifications[key]

        if not update_data:
            return False

        try:
            supabase_client.table('profiles').update(update_data).eq(
                'id', profile_id
            ).execute()
            return True
        except Exception as e:
            logger.error(f"Failed to store classifications for {profile_id}: {e}")
            return False
