"""
Match Enrichment and Verification Service

This service:
1. Enriches raw match data with full profile fields (seeking, offering, who_they_serve)
2. Generates compelling mutual benefit reasoning using actual profile data
3. Verifies match quality before PDF generation using MULTIPLE specialized agents
4. Sanitizes all text for PDF rendering (encoding, formatting, capitalization)
"""

import json
import re
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

from matching.enrichment.ai_verification import ClaudeVerificationService
from matching.enrichment.text_sanitizer import TextSanitizer  # extracted module

logger = logging.getLogger(__name__)


class VerificationStatus(Enum):
    PASSED = "passed"
    NEEDS_ENRICHMENT = "needs_enrichment"
    REJECTED = "rejected"


@dataclass
class VerificationResult:
    status: VerificationStatus
    score: float  # 0-100
    issues: List[str]
    suggestions: List[str]


@dataclass
class EnrichedMatch:
    """A match with full profile data and compelling reasoning."""
    name: str
    company: str
    email: str
    linkedin: str
    website: str
    niche: str
    list_size: int
    social_reach: int
    score: float

    # Rich profile data (the gold we were missing!)
    who_they_serve: str
    what_they_do: str
    seeking: str
    offering: str
    notes: str  # Calendar links, best contact method, etc.

    # Generated compelling reasoning
    why_fit: str
    mutual_benefit: str
    outreach_message: str

    # Verification
    verification_score: float
    verification_passed: bool

    # Data quality tracking - what sources did we use?
    data_quality: str = 'unknown'  # 'rich', 'partial', 'sparse'
    has_explicit_seeking: bool = False
    has_explicit_offering: bool = False
    explanation_source: str = 'template_fallback'  # 'llm_verified', 'llm_partial', 'template_fallback'


class MatchEnrichmentService:
    """
    Enriches matches with full profile data and generates compelling mutual benefit reasoning.
    """

    def __init__(self, client_profile: Dict):
        """
        Initialize with client profile (e.g., Janet Bray Attwood's data).

        Args:
            client_profile: Dict with keys: name, company, what_you_do, who_you_serve, seeking, offering
        """
        self.client = client_profile
        self.client_name = client_profile.get('name', 'Client')
        self.client_first_name = self.client_name.split()[0]
        self.ai_service = ClaudeVerificationService()

    def enrich_match(self, match_data: Dict, partner_profile: Optional[Dict] = None) -> EnrichedMatch:
        """
        Enrich a single match with full profile data and generate compelling reasoning.

        Args:
            match_data: Basic match data (name, score, niche, etc.)
            partner_profile: Optional full profile data (if available from Supabase)

        Returns:
            EnrichedMatch with compelling mutual benefit reasoning
        """
        # Extract partner data (prefer full profile if available)
        partner = partner_profile or match_data

        name = match_data.get('name', '')
        company = match_data.get('company', '')

        # Get the rich data - with FALLBACKS for sparse profiles
        who_they_serve = partner.get('who_you_serve') or partner.get('who_they_serve') or ''
        what_they_do = partner.get('what_you_do') or partner.get('what_they_do') or ''
        seeking = partner.get('seeking') or ''
        offering = partner.get('offering') or ''

        # FALLBACK: Use business_focus/niche if primary fields are empty
        business_focus = partner.get('business_focus') or partner.get('niche') or match_data.get('niche', '')
        if not what_they_do and business_focus:
            what_they_do = business_focus
        if not offering and business_focus:
            # Infer offering from their business focus
            offering = f"expertise in {business_focus}"

        # Track data quality for content generation
        has_explicit_seeking = bool(partner.get('seeking'))
        has_explicit_offering = bool(partner.get('offering'))

        # Calculate data quality score
        quality_score = sum([
            bool(who_they_serve),
            has_explicit_seeking,
            has_explicit_offering,
            bool(partner.get('what_you_do')),
        ])
        if quality_score >= 3:
            data_quality = 'rich'
        elif quality_score >= 1:
            data_quality = 'partial'
        else:
            data_quality = 'sparse'

        # DON'T infer seeking - it's misleading to claim they want something they didn't say
        # Instead, we'll use different content templates for sparse profiles

        # Try LLM explanation first, fall back to templates
        explanation_source = 'template_fallback'
        llm_explanation = None

        if self.ai_service.is_available():
            llm_explanation, explanation_source = self.generate_llm_explanation(partner)

        if llm_explanation and explanation_source in ('llm_verified', 'llm_partial'):
            why_fit = self._format_llm_why_fit(llm_explanation, name)
            mutual_benefit = self._format_llm_mutual_benefit(llm_explanation, name)
        else:
            why_fit = self._generate_why_fit(name, company, who_they_serve, what_they_do, seeking, offering, match_data)
            mutual_benefit = self._generate_mutual_benefit(name, company, who_they_serve, seeking, offering, match_data)

        # Outreach stays template-based (works well, LLM variability adds risk)
        outreach_message = self._generate_outreach(name, company, who_they_serve, seeking, offering, match_data)

        # Create enriched match
        enriched = EnrichedMatch(
            name=name,
            company=company,
            email=match_data.get('email', ''),
            linkedin=match_data.get('linkedin', ''),
            website=partner.get('website') or match_data.get('website', ''),
            niche=match_data.get('niche', ''),
            list_size=match_data.get('list_size', 0),
            social_reach=partner.get('social_reach', 0) or 0,
            score=match_data.get('score', 0),
            who_they_serve=who_they_serve,
            what_they_do=what_they_do,
            seeking=seeking,
            offering=offering,
            notes=partner.get('notes', '') or '',
            why_fit=why_fit,
            mutual_benefit=mutual_benefit,
            outreach_message=outreach_message,
            verification_score=0,
            verification_passed=False,
            data_quality=data_quality,
            has_explicit_seeking=has_explicit_seeking,
            has_explicit_offering=has_explicit_offering,
            explanation_source=explanation_source,
        )

        return enriched

    # =========================================================================
    # LLM MATCH EXPLANATIONS (B2) — Generate + Verify + Fallback
    # =========================================================================

    @staticmethod
    def _format_jv_history(jv_history) -> str:
        """Format jv_history list into readable text.

        Args:
            jv_history: List of dicts with keys: partner_name, format, source_quote.

        Returns:
            Formatted string, e.g. "Podcast guest with John Smith, Bundle with Acme Corp"
            or "No disclosed partnerships" if empty/None.
        """
        if not jv_history or not isinstance(jv_history, list):
            return "No disclosed partnerships"

        parts = []
        for entry in jv_history:
            if not isinstance(entry, dict):
                continue
            fmt = entry.get('format', 'Partnership')
            partner = entry.get('partner_name', 'Unknown')
            parts.append(f"{fmt} with {partner}")

        return ", ".join(parts) if parts else "No disclosed partnerships"

    @staticmethod
    def _format_content_platforms(content_platforms) -> str:
        """Format content_platforms dict into readable text.

        Args:
            content_platforms: Dict with keys like podcast_name, youtube_channel,
                instagram_handle, facebook_group, tiktok_handle, newsletter_name.

        Returns:
            Formatted string listing non-empty platforms, e.g.
            "Podcast: The Marketing Show, YouTube: @handle"
            or "Not specified" if empty/None.
        """
        if not content_platforms or not isinstance(content_platforms, dict):
            return "Not specified"

        label_map = {
            'podcast_name': 'Podcast',
            'youtube_channel': 'YouTube',
            'instagram_handle': 'Instagram',
            'facebook_group': 'Facebook Group',
            'tiktok_handle': 'TikTok',
            'newsletter_name': 'Newsletter',
        }

        parts = []
        for key, label in label_map.items():
            value = content_platforms.get(key)
            if value:
                parts.append(f"{label}: {value}")

        return ", ".join(parts) if parts else "Not specified"

    def _build_enriched_context(self, profile: dict) -> str:
        """Build enriched context from all available data beyond structured fields."""
        parts = []
        if profile.get('bio'):
            parts.append(f"Credentials/social proof: {profile['bio']}")
        if profile.get('signature_programs'):
            parts.append(f"Signature programs: {profile['signature_programs']}")
        if profile.get('current_projects'):
            parts.append(f"Current projects: {profile['current_projects']}")
        if profile.get('tags'):
            tags = profile['tags'] if isinstance(profile['tags'], list) else []
            if tags:
                parts.append(f"Keywords/tags: {', '.join(tags)}")

        # New enrichment fields (safe access for older profiles)
        revenue_tier = profile.get('revenue_tier') if isinstance(profile, dict) else getattr(profile, 'revenue_tier', None)
        if revenue_tier:
            parts.append(f"Revenue tier: {revenue_tier}")

        jv_history = profile.get('jv_history') if isinstance(profile, dict) else getattr(profile, 'jv_history', None)
        formatted_jv = self._format_jv_history(jv_history)
        if formatted_jv != "No disclosed partnerships":
            parts.append(f"Past JV partnerships: {formatted_jv}")

        content_platforms = profile.get('content_platforms') if isinstance(profile, dict) else getattr(profile, 'content_platforms', None)
        formatted_platforms = self._format_content_platforms(content_platforms)
        if formatted_platforms != "Not specified":
            parts.append(f"Content platforms: {formatted_platforms}")

        audience_engagement_score = profile.get('audience_engagement_score') if isinstance(profile, dict) else getattr(profile, 'audience_engagement_score', None)
        if audience_engagement_score is not None:
            parts.append(f"Audience engagement score: {audience_engagement_score}")

        if not parts:
            return "--- Additional Context ---\nNo enriched data available. Analysis based on profile fields only."
        return "--- Additional Context (from enrichment) ---\n" + "\n".join(parts)

    def _generate_llm_explanation(self, partner_profile: dict) -> Optional[dict]:
        """
        Generate match explanation via LLM (Call 1: Generation).

        Returns structured JSON dict or None on any failure.
        """
        if not self.ai_service.is_available():
            return None

        client_context = self._build_enriched_context(self.client)
        partner_context = self._build_enriched_context(partner_profile)

        prompt = (
            "You are analyzing a potential JV (Joint Venture) partnership between two professionals. "
            "Your goal is to help both parties immediately see why this connection is worth pursuing "
            "— both the clear synergies and the less obvious opportunities they might not see on their own.\n\n"
            f"=== PARTNER A (the client) ===\n"
            f"Name: {self.client.get('name', '')}\n"
            f"Serves: {self.client.get('who_you_serve', '')}\n"
            f"What they do: {self.client.get('what_you_do', '')}\n"
            f"Seeking: {self.client.get('seeking', '')}\n"
            f"Offering: {self.client.get('offering', '')}\n"
            f"Audience size: {self.client.get('list_size', '')}\n"
            f"Revenue tier: {self.client.get('revenue_tier') or 'Not disclosed'}\n"
            f"Past JV partnerships: {self._format_jv_history(self.client.get('jv_history'))}\n"
            f"Content platforms: {self._format_content_platforms(self.client.get('content_platforms'))}\n"
            f"Audience engagement: {self.client.get('audience_engagement_score') or 'Unknown'}\n"
            f"{client_context}\n\n"
            f"=== PARTNER B (the match) ===\n"
            f"Name: {partner_profile.get('name', '')}\n"
            f"Serves: {partner_profile.get('who_you_serve', '')}\n"
            f"What they do: {partner_profile.get('what_you_do', '')}\n"
            f"Seeking: {partner_profile.get('seeking', '')}\n"
            f"Offering: {partner_profile.get('offering', '')}\n"
            f"Audience size: {partner_profile.get('list_size', '')}\n"
            f"Revenue tier: {partner_profile.get('revenue_tier') or 'Not disclosed'}\n"
            f"Past JV partnerships: {self._format_jv_history(partner_profile.get('jv_history'))}\n"
            f"Content platforms: {self._format_content_platforms(partner_profile.get('content_platforms'))}\n"
            f"Audience engagement: {partner_profile.get('audience_engagement_score') or 'Unknown'}\n"
            f"{partner_context}\n\n"
            "=== INSTRUCTIONS ===\n\n"
            "Analyze the mutual value of this partnership. Include BOTH:\n"
            "- Clear, obvious connections (audience overlap, complementary offerings, direct need/offer matches)\n"
            "- Non-obvious insights (unexpected synergies, strategic positioning opportunities, timing advantages, audience psychology connections)\n\n"
            "When analyzing partnership fit, also consider:\n"
            "- Revenue tier alignment (similar pricing = similar customer base)\n"
            "- Past JV history (experienced JV partners are lower-risk)\n"
            "- Content platform overlap (shared platforms = easier cross-promotion)\n"
            "- Specific partnership formats they've done before (podcast swaps, bundles, affiliates)\n\n"
            "For each claim, reference ONLY data explicitly present in the profiles above. "
            "If a field is empty or missing, do not invent information for it. "
            'If you must make a reasonable inference, explicitly label it as "[inferred from: field_name]".\n\n'
            "Respond in this exact JSON structure:\n\n"
            "{\n"
            '  "what_partner_b_brings_to_a": {\n'
            '    "summary": "2-3 sentences explaining the specific value Partner B offers Partner A. Be concrete — name the audiences, offerings, and mechanisms.",\n'
            '    "key_points": ["point 1", "point 2"]\n'
            "  },\n"
            '  "what_partner_a_brings_to_b": {\n'
            '    "summary": "2-3 sentences explaining the specific value Partner A offers Partner B. Be concrete.",\n'
            '    "key_points": ["point 1", "point 2"]\n'
            "  },\n"
            '  "connection_insights": [\n'
            "    {\n"
            '      "type": "obvious",\n'
            '      "insight": "The clear, direct reason these two should connect"\n'
            "    },\n"
            "    {\n"
            '      "type": "non_obvious",\n'
            '      "insight": "A deeper or unexpected reason this partnership has high potential"\n'
            "    }\n"
            "  ],\n"
            '  "reciprocity_assessment": {\n'
            '    "balance": "balanced | slightly_asymmetric | significantly_asymmetric",\n'
            '    "stronger_side": "partner_a | partner_b | neither",\n'
            '    "explanation": "1 sentence explaining the balance or imbalance",\n'
            '    "gap": "If asymmetric: what\'s missing or unclear about the weaker side\'s contribution. null if balanced."\n'
            "  },\n"
            '  "citations": {\n'
            '    "each claim text": "source_field_name (e.g., \'partner_b.seeking\', \'partner_a.enriched_bio\')"\n'
            "  },\n"
            '  "confidence": {\n'
            '    "data_richness": "high | medium | low — based on how much profile data was available",\n'
            '    "explanation_confidence": "high | medium | low — how confident you are in the analysis"\n'
            "  }\n"
            "}\n\n"
            "=== EXAMPLES OF GOOD CONNECTION INSIGHTS ===\n\n"
            "Example 1 (Health coaching + Software):\n"
            '- Obvious: "Sarah\'s health coaching audience of 12,000 subscribers is the exact buyer demographic '
            "for Marcus's meal planning software — they're already investing in nutrition guidance and would see "
            'the software as a natural next step."\n'
            '- Non-obvious: "Marcus\'s SaaS platform could solve the #1 support burden for Sarah\'s team — clients '
            "constantly asking for structured meal plans. An integration lets Sarah offer more value to existing "
            'clients while generating recurring affiliate revenue from a problem she\'s currently solving manually."\n\n'
            "Example 2 (Business consultant + Course creator):\n"
            '- Obvious: "David\'s consulting clients are mid-stage entrepreneurs who need exactly the kind of '
            "systems training Rachel's 'Scale Your Operations' course provides. Her course solves the "
            'implementation gap his consulting identifies."\n'
            "- Non-obvious: \"Rachel's course completion data shows her students' biggest struggle is financial "
            "modeling — which is David's core expertise. A co-created bonus module on financial projections for "
            "Rachel's course positions David as the go-to consultant for her graduates when they're ready for "
            '1:1 help."\n\n'
            "Example 3 (Podcast host + Event producer):\n"
            "- Obvious: \"Maria's podcast reaches 5,000 weekly listeners in the personal development space — "
            'the same audience James needs for his live workshop events."\n'
            "- Non-obvious: \"James's event attendees are highly engaged but have no ongoing community after "
            "events end. Maria's podcast could become the continuity vehicle — attendees subscribe to stay "
            'connected, giving Maria a warm audience segment with proven willingness to invest in experiences."\n\n'
            "Note: These examples show the quality bar. Your insights should be THIS specific — referencing "
            "actual data points from the profiles, not generic statements."
        )

        try:
            response = self.ai_service._call_claude(prompt)
            if not response:
                return None

            # Strip markdown code fences if present
            text = response.strip()
            if text.startswith("```json"):
                text = text[7:]
            elif text.startswith("```"):
                text = text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

            data = json.loads(text)

            # Validate required top-level keys
            required = ['what_partner_b_brings_to_a', 'what_partner_a_brings_to_b']
            if not all(k in data for k in required):
                logger.warning(f"LLM response missing required keys: {list(data.keys())}")
                return None

            return data

        except json.JSONDecodeError as e:
            logger.error(f"LLM explanation JSON parse failed: {e}")
            return None
        except Exception as e:
            logger.error(f"LLM explanation generation failed: {e}")
            return None

    def _verify_explanation(self, explanation: dict, partner_profile: dict) -> dict:
        """
        Verify LLM explanation grounding via fact-check (Call 2: Verification).

        Returns dict with 'claims', 'grounded_percentage', 'recommendation'.
        """
        fallback = {'claims': [], 'grounded_percentage': 0.0, 'recommendation': 'fall_back_to_template'}

        if not self.ai_service.is_available():
            return fallback

        client_context = self._build_enriched_context(self.client)
        partner_context = self._build_enriched_context(partner_profile)

        profile_block = (
            f"=== PARTNER A (the client) ===\n"
            f"Name: {self.client.get('name', '')}\n"
            f"Serves: {self.client.get('who_you_serve', '')}\n"
            f"What they do: {self.client.get('what_you_do', '')}\n"
            f"Seeking: {self.client.get('seeking', '')}\n"
            f"Offering: {self.client.get('offering', '')}\n"
            f"Audience size: {self.client.get('list_size', '')}\n"
            f"Revenue tier: {self.client.get('revenue_tier') or 'Not disclosed'}\n"
            f"Past JV partnerships: {self._format_jv_history(self.client.get('jv_history'))}\n"
            f"Content platforms: {self._format_content_platforms(self.client.get('content_platforms'))}\n"
            f"Audience engagement: {self.client.get('audience_engagement_score') or 'Unknown'}\n"
            f"{client_context}\n\n"
            f"=== PARTNER B (the match) ===\n"
            f"Name: {partner_profile.get('name', '')}\n"
            f"Serves: {partner_profile.get('who_you_serve', '')}\n"
            f"What they do: {partner_profile.get('what_you_do', '')}\n"
            f"Seeking: {partner_profile.get('seeking', '')}\n"
            f"Offering: {partner_profile.get('offering', '')}\n"
            f"Audience size: {partner_profile.get('list_size', '')}\n"
            f"Revenue tier: {partner_profile.get('revenue_tier') or 'Not disclosed'}\n"
            f"Past JV partnerships: {self._format_jv_history(partner_profile.get('jv_history'))}\n"
            f"Content platforms: {self._format_content_platforms(partner_profile.get('content_platforms'))}\n"
            f"Audience engagement: {partner_profile.get('audience_engagement_score') or 'Unknown'}\n"
            f"{partner_context}"
        )

        prompt = (
            "You are a fact-checker verifying a JV match explanation against source profile data.\n\n"
            f"ORIGINAL PROFILES:\n{profile_block}\n\n"
            f"GENERATED EXPLANATION:\n{json.dumps(explanation, indent=2)}\n\n"
            "For each factual claim in the explanation:\n"
            "1. Identify the specific profile field that supports it\n"
            '2. Rate it: "grounded" (directly stated in profile), "inferred" (reasonable inference '
            'from available data), or "ungrounded" (not supported by any profile data)\n\n'
            "Respond in JSON:\n"
            "{\n"
            '  "claims": [\n'
            '    {"claim": "...", "status": "grounded|inferred|ungrounded", "source_field": "...", "note": "..."}\n'
            "  ],\n"
            '  "grounded_percentage": 0.0-1.0,\n'
            '  "recommendation": "use_as_is | remove_ungrounded | fall_back_to_template"\n'
            "}"
        )

        try:
            response = self.ai_service._call_claude(prompt)
            if not response:
                return fallback

            text = response.strip()
            if text.startswith("```json"):
                text = text[7:]
            elif text.startswith("```"):
                text = text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

            data = json.loads(text)

            if 'grounded_percentage' not in data:
                return fallback

            return data

        except (json.JSONDecodeError, Exception) as e:
            logger.error(f"LLM verification failed: {e}")
            return fallback

    def generate_llm_explanation(self, partner_profile: dict) -> Tuple[Optional[dict], str]:
        """
        Public orchestrator: generate + verify LLM explanation.

        Returns (explanation_dict or None, explanation_source).
        """
        # Call 1: Generate
        explanation = self._generate_llm_explanation(partner_profile)
        if not explanation:
            logger.info("LLM generation returned None — using template fallback")
            return None, 'template_fallback'

        # Call 2: Verify
        verification = self._verify_explanation(explanation, partner_profile)
        grounded_pct = verification.get('grounded_percentage', 0.0)

        # Log cost estimate (2 calls, rough token counts)
        logger.info(
            f"LLM explanation: {grounded_pct:.0%} grounded, "
            f"{len(verification.get('claims', []))} claims checked, "
            f"model={self.ai_service.model}, "
            f"est_cost=$0.01-0.03 (2 calls)"
        )

        # Classify based on grounding
        if grounded_pct >= 0.8:
            return explanation, 'llm_verified'
        elif grounded_pct >= 0.5:
            return explanation, 'llm_partial'
        else:
            logger.warning(
                f"LLM explanation too speculative ({grounded_pct:.0%} grounded) — template fallback"
            )
            return None, 'template_fallback'

    def _format_llm_why_fit(self, explanation: dict, partner_name: str) -> str:
        """Format LLM explanation into the WHY FIT section for PDF."""
        first_name = partner_name.split()[0] if partner_name else 'Partner'
        parts = []

        # What partner brings to client
        b_brings = explanation.get('what_partner_b_brings_to_a', {})
        summary = b_brings.get('summary', '') if isinstance(b_brings, dict) else str(b_brings)
        if summary:
            parts.append(TextSanitizer.sanitize(summary))

        # Connection insights
        insights = explanation.get('connection_insights', [])
        for insight_obj in insights:
            if isinstance(insight_obj, dict):
                insight_text = insight_obj.get('insight', '')
                insight_type = insight_obj.get('type', '')
                if insight_text:
                    label = "Key insight" if insight_type == 'non_obvious' else "Connection"
                    parts.append(f"{label}: {TextSanitizer.sanitize(insight_text)}")

        result = '\n\n'.join(parts) if parts else f"{first_name} is a strong potential JV partner."
        return TextSanitizer.truncate_safe(result, 520)

    def _format_llm_mutual_benefit(self, explanation: dict, partner_name: str) -> str:
        """Format LLM explanation into the MUTUAL BENEFIT section for PDF."""
        first_name = partner_name.split()[0] if partner_name else 'Partner'

        # What partner gets from client
        a_brings = explanation.get('what_partner_a_brings_to_b', {})
        partner_gets = a_brings.get('key_points', []) if isinstance(a_brings, dict) else []

        # What client gets from partner
        b_brings = explanation.get('what_partner_b_brings_to_a', {})
        client_gets = b_brings.get('key_points', []) if isinstance(b_brings, dict) else []

        # Sanitize
        partner_gets = [TextSanitizer.sanitize(p) for p in partner_gets[:3] if p]
        client_gets = [TextSanitizer.sanitize(p) for p in client_gets[:3] if p]

        sections = []
        if client_gets:
            bullets = TextSanitizer.format_bullet_list(client_gets)
            sections.append(f"WHAT {self.client_first_name.upper()} GETS:\n{bullets}")
        if partner_gets:
            bullets = TextSanitizer.format_bullet_list(partner_gets)
            sections.append(f"WHAT {first_name.upper()} GETS:\n{bullets}")

        # Reciprocity note
        recip = explanation.get('reciprocity_assessment', {})
        if isinstance(recip, dict) and recip.get('explanation'):
            sections.append(f"Balance: {TextSanitizer.sanitize(recip['explanation'])}")

        result = '\n\n'.join(sections) if sections else "Mutual cross-promotion and audience sharing opportunity."
        return TextSanitizer.truncate_safe(result, 450)

    # =========================================================================
    # TEMPLATE EXPLANATIONS (original)
    # =========================================================================

    def _generate_why_fit(self, name: str, company: str, who_they_serve: str,
                          what_they_do: str, seeking: str, offering: str,
                          match_data: Dict) -> str:
        """Generate compelling 'why this is a good fit' reasoning using actual profile data."""

        first_name = name.split()[0] if name else 'Partner'
        list_size = match_data.get('list_size', 0)
        niche = match_data.get('niche', '')

        # Sanitize inputs first - prevents encoding issues
        who_they_serve = TextSanitizer.sanitize(who_they_serve)
        seeking = TextSanitizer.sanitize(seeking)
        offering = TextSanitizer.sanitize(offering)

        parts = []

        # 1. Audience alignment (using who_they_serve)
        if who_they_serve:
            client_audience = self.client.get('who_you_serve', '')
            client_methodology = self.client.get('methodology', self.client.get('what_you_do', 'their expertise'))
            # Find overlap in audiences
            overlap = self._find_audience_overlap(who_they_serve, client_audience)

            # Use safe truncation - never cut words
            audience_snippet = TextSanitizer.truncate_safe(who_they_serve, 100)

            if overlap:
                parts.append(f"AUDIENCE ALIGNMENT: {first_name} serves {audience_snippet} - these are exactly the people who need {self.client_first_name}'s {TextSanitizer.truncate_safe(client_methodology, 60)} before they can fully benefit from {first_name}'s expertise.")
            else:
                short_audience = TextSanitizer.truncate_safe(who_they_serve, 80)
                parts.append(f"AUDIENCE SYNERGY: {first_name}'s audience ({short_audience}) are growth-minded individuals actively investing in themselves - prime candidates for {self.client_first_name}'s offerings.")

        # 2. What they're actively seeking (the gold!)
        if seeking:
            # Parse what they want
            seeking_lower = seeking.lower()
            seeking_snippet = TextSanitizer.truncate_safe(seeking, 100)

            # Match their seeking to what client offers
            client_offering_desc = self.client.get('offering', 'their network and engaged audience')
            client_programs = self.client.get('signature_programs', 'their programs')
            if any(kw in seeking_lower for kw in ['cross-promotion', 'promotion', 'email', 'list']):
                parts.append(f"THEY WANT THIS: {first_name} is actively seeking '{seeking_snippet}' - {self.client_first_name} offers exactly this with {TextSanitizer.truncate_safe(client_offering_desc, 80)}.")
            elif any(kw in seeking_lower for kw in ['speaking', 'interview', 'podcast', 'guest']):
                parts.append(f"THEY WANT THIS: {first_name} wants '{seeking_snippet}' - {self.client_first_name} can provide speaking platforms and interview opportunities through {TextSanitizer.truncate_safe(client_programs, 60)}.")
            elif any(kw in seeking_lower for kw in ['affiliate', 'partner', 'jv', 'joint venture']):
                parts.append(f"THEY WANT THIS: {first_name} is seeking '{seeking_snippet}' - perfectly aligned with {self.client_first_name}'s JV partnership goals.")
            elif seeking:
                long_seeking = TextSanitizer.truncate_safe(seeking, 150)
                parts.append(f"ACTIVE INTEREST: {first_name} has stated they're seeking: {long_seeking}")

        # 3. Scale match (always include partner's name for personalization)
        if list_size >= 50000:
            parts.append(f"SCALE: {first_name}'s reach of {list_size:,} subscribers provides significant cross-promotion value for {self.client_first_name}.")
        elif list_size >= 10000:
            parts.append(f"ENGAGED AUDIENCE: {first_name} has {list_size:,} subscribers in a complementary niche.")

        # 4. What they offer (potential for client) - use actual data only
        if offering:
            offering_snippet = TextSanitizer.truncate_safe(offering, 150)
            parts.append(f"THEY OFFER: {first_name} brings {offering_snippet}")

        # 5. For sparse profiles, use business focus factually (not as inference)
        if not parts and what_they_do:
            # State what we KNOW, not what we assume they want
            focus_snippet = TextSanitizer.truncate_safe(what_they_do, 120)
            parts.append(f"EXPERTISE: {first_name} specializes in {focus_snippet} - a complementary area for {self.client_first_name}'s audience.")

        # Ensure we always have personalized content with the partner's name
        if not parts:
            # Minimal fallback - focuses on scale if available
            if list_size >= 10000:
                parts.append(f"REACH: {first_name} has built an audience of {list_size:,} in the {niche or 'personal development'} space.")
            else:
                parts.append(f"COMPLEMENTARY: {first_name} operates in {niche or 'personal development'} - explore potential synergies.")

        # SMART LENGTH MANAGEMENT: Keep up to 3 sections, max 520 chars
        # Tighter PDF spacing allows for more content now
        MAX_LENGTH = 520
        MAX_SECTIONS = 3  # Allow up to 3 sections for more info

        result_parts = []
        current_length = 0

        for part in parts[:MAX_SECTIONS]:  # Limit to top 3
            section_length = len(part) + 4  # +4 for '\n\n' separator
            if current_length + section_length <= MAX_LENGTH:
                result_parts.append(part)
                current_length += section_length
            # Don't break - try to fit what we can

        result = '\n\n'.join(result_parts)
        return TextSanitizer.sanitize(result)

    def _generate_mutual_benefit(self, name: str, company: str, who_they_serve: str,
                                  seeking: str, offering: str, match_data: Dict) -> str:
        """Generate clear mutual benefit statement with proper formatting."""

        first_name = name.split()[0] if name else 'Partner'
        list_size = match_data.get('list_size', 0)

        # What partner gets
        client_audience_desc = self.client.get('audience_description',
                                               self.client.get('offering', 'their engaged audience'))
        client_programs = self.client.get('signature_programs', 'their programs')
        client_credentials = self.client.get('credentials',
                                              self.client.get('bio', ''))

        partner_gets = []
        if seeking:
            # Parse their seeking to describe what they'll get
            seeking_lower = seeking.lower()
            if 'cross-promotion' in seeking_lower or 'email' in seeking_lower:
                partner_gets.append(f"Exposure to {self.client_first_name}'s {TextSanitizer.truncate_safe(client_audience_desc, 80)}")
            if 'speaking' in seeking_lower or 'interview' in seeking_lower:
                partner_gets.append(f"Speaking opportunities through {TextSanitizer.truncate_safe(client_programs, 60)}")
            if 'affiliate' in seeking_lower:
                partner_gets.append("Affiliate partnership with proven high-conversion programs")
            if 'podcast' in seeking_lower or 'guest' in seeking_lower:
                cred_snippet = f" with {TextSanitizer.truncate_safe(client_credentials, 40)}" if client_credentials else ''
                partner_gets.append(f"Guest appearance opportunities{cred_snippet}")

        if not partner_gets:
            partner_gets.append(f"Access to {self.client_first_name}'s engaged audience")

        # What client gets
        client_gets = []
        if list_size >= 10000:
            client_gets.append(f"Promotion to {list_size:,} subscribers")
        if offering:
            offering_lower = offering.lower()
            if 'podcast' in offering_lower:
                client_gets.append("Podcast guest opportunity")
            elif 'email' in offering_lower or 'list' in offering_lower:
                client_gets.append("Email promotion to their list")
            elif 'speaking' in offering_lower:
                client_gets.append("Speaking platform access")

        if not client_gets:
            client_gets.append(f"Access to {first_name}'s complementary audience")

        # LIMIT to 2 bullets each to fit PDF space (max ~300 chars for STRATEGY)
        partner_bullets = TextSanitizer.format_bullet_list(partner_gets[:2])
        client_bullets = TextSanitizer.format_bullet_list(client_gets[:2])

        benefit = f"""WHAT {first_name.upper()} GETS:
{partner_bullets}

WHAT {self.client_first_name.upper()} GETS:
{client_bullets}"""

        return TextSanitizer.sanitize(benefit)

    def _generate_outreach(self, name: str, company: str, who_they_serve: str,
                           seeking: str, offering: str, match_data: Dict) -> str:
        """
        Generate warm, relationship-focused outreach.

        Per Chelsea's feedback: Be WARM, not transactional. Focus on connection
        and mutual values, not bullet points and numbers.
        """

        first_name = name.split()[0] if name else 'there'

        # Handle Dr. prefix
        if first_name.lower() == 'dr.':
            parts = name.split()
            first_name = parts[1] if len(parts) > 1 else first_name

        # Sanitize inputs
        seeking = TextSanitizer.sanitize(seeking)
        who_they_serve = TextSanitizer.sanitize(who_they_serve)
        offering = TextSanitizer.sanitize(offering)
        company = TextSanitizer.sanitize(company)

        # Build warm, personalized opening based on their profile
        if seeking:
            # Reference what resonated without quoting numbers
            seeking_preview = TextSanitizer.truncate_safe(seeking, 80, '')
            if 'partner' in seeking.lower() or 'collaborat' in seeking.lower():
                warm_hook = f"I came across your JV Directory profile and really resonated with how you approach partnerships"
                warm_detail = "- especially the emphasis on collaboration and long-term alignment"
            elif 'speaking' in seeking.lower() or 'event' in seeking.lower():
                warm_hook = f"I noticed we share a similar passion for transformational events and speaking"
                warm_detail = ""
            else:
                warm_hook = f"Your profile caught my attention"
                warm_detail = "- I love the work you're doing"
        elif who_they_serve:
            audience_preview = TextSanitizer.truncate_safe(who_they_serve, 60, '')
            warm_hook = f"I love that you serve {audience_preview}"
            warm_detail = "- it's so aligned with the community I've built"
        elif company:
            warm_hook = f"I've admired what you're building with {company}"
            warm_detail = ""
        else:
            warm_hook = "Your work in the transformation space caught my attention"
            warm_detail = ""

        # Build the connection bridge - focus on values, not offers
        client_company = self.client.get('company', '')
        client_role = self.client.get('role', 'founder')
        client_what = self.client.get('what_you_do', 'building meaningful partnerships')
        company_intro = f", {client_role} of {client_company}" if client_company else ''
        connection_text = f"I'm {self.client_name}{company_intro}. Over the years, we've built a community around {TextSanitizer.truncate_safe(client_what, 80)}, and I'm always interested in connecting with people who value meaningful, win-win partnerships as much as we do."

        # Soft, non-transactional invitation
        invitation = """There may be some natural ways we could support one another that I'd love to chat about.

If you're open to it, I'd love to hop on a short call to get to know each other and see if there's a mutual fit."""

        message = f"""Subject: Connection from {self.client_first_name}

Hi {first_name},

{warm_hook}{warm_detail}.

{connection_text}

{invitation}

You can grab a time that works for you here: [calendar link]

Looking forward to connecting,
{self.client_first_name}"""

        return TextSanitizer.sanitize(message)

    def _find_audience_overlap(self, their_audience: str, our_audience: str) -> bool:
        """Check if there's meaningful audience overlap."""
        their_lower = their_audience.lower()
        our_lower = our_audience.lower()

        overlap_keywords = [
            'coach', 'entrepreneur', 'leader', 'speaker', 'author',
            'trainer', 'consultant', 'professional', 'business owner',
            'seeker', 'transform', 'growth', 'development'
        ]

        their_matches = sum(1 for kw in overlap_keywords if kw in their_lower)
        our_matches = sum(1 for kw in overlap_keywords if kw in our_lower)

        return their_matches >= 2 or our_matches >= 2


# =============================================================================
# MULTI-AGENT VERIFICATION SYSTEM
# Each agent is specialized to catch specific issues
# =============================================================================

@dataclass
class VerificationIssue:
    """A single verification issue found by an agent."""
    agent: str
    severity: str  # 'critical', 'warning', 'info'
    issue: str
    suggestion: str
    location: str  # which field: 'why_fit', 'mutual_benefit', 'outreach', etc.


class BaseVerificationAgent:
    """Base class for all verification agents."""

    name: str = "base"

    def verify(self, match: 'EnrichedMatch') -> List[VerificationIssue]:
        """Return list of issues found. Empty list = passed."""
        raise NotImplementedError


class EncodingVerificationAgent(BaseVerificationAgent):
    """
    AGENT 1: Encoding Verification
    Catches any remaining problematic characters that could render as boxes.
    """

    name = "encoding"

    # Characters that commonly cause rendering issues
    PROBLEMATIC_PATTERNS = [
        (r'[\u2014\u2013\u2212\u2010\u2011]', 'em/en-dash'),  # Various dashes
        (r'[\u2018\u2019\u201c\u201d]', 'smart quotes'),  # Curly quotes
        (r'[\u25a0\u25aa\u25cf\u25cb]', 'geometric shapes'),  # Squares/circles
        (r'[\u00ad\u200b\u200c\u200d\ufeff]', 'invisible chars'),  # Zero-width
        (r'[^\x00-\x7F]', 'non-ASCII'),  # Anything outside basic ASCII
    ]

    def verify(self, match: EnrichedMatch) -> List[VerificationIssue]:
        issues = []

        # Check all text fields
        fields_to_check = [
            ('why_fit', match.why_fit),
            ('mutual_benefit', match.mutual_benefit),
            ('outreach_message', match.outreach_message),
            ('seeking', match.seeking),
            ('offering', match.offering),
            ('who_they_serve', match.who_they_serve),
        ]

        for field_name, text in fields_to_check:
            if not text:
                continue

            for pattern, desc in self.PROBLEMATIC_PATTERNS:
                matches = re.findall(pattern, text)
                if matches:
                    # Only flag non-ASCII if there are problematic chars
                    if desc == 'non-ASCII':
                        # Filter to only truly problematic chars
                        bad_chars = [c for c in matches if ord(c) > 127 and c not in '\n\t']
                        if bad_chars:
                            issues.append(VerificationIssue(
                                agent=self.name,
                                severity='critical',
                                issue=f"Found {len(bad_chars)} problematic characters ({desc}): {bad_chars[:5]}",
                                suggestion="Run through TextSanitizer.sanitize()",
                                location=field_name
                            ))
                    else:
                        issues.append(VerificationIssue(
                            agent=self.name,
                            severity='critical',
                            issue=f"Found {desc}: '{matches[:3]}'",
                            suggestion="Replace with ASCII equivalent",
                            location=field_name
                        ))

        return issues


class FormattingVerificationAgent(BaseVerificationAgent):
    """
    AGENT 2: Formatting Verification
    Ensures proper structure, spacing, bullets, and readability.
    """

    name = "formatting"

    def verify(self, match: EnrichedMatch) -> List[VerificationIssue]:
        issues = []

        # Check WHY FIT has proper section breaks
        if match.why_fit:
            sections = match.why_fit.split('\n\n')
            if len(sections) == 1 and len(match.why_fit) > 200:
                issues.append(VerificationIssue(
                    agent=self.name,
                    severity='warning',
                    issue="WHY FIT is one large block without section breaks",
                    suggestion="Add paragraph breaks between AUDIENCE, THEY WANT, SCALE sections",
                    location='why_fit'
                ))

        # Check MUTUAL BENEFIT has both GETS sections
        if match.mutual_benefit:
            mb_upper = match.mutual_benefit.upper()

            if 'WHAT' not in mb_upper:
                issues.append(VerificationIssue(
                    agent=self.name,
                    severity='critical',
                    issue="MUTUAL BENEFIT missing WHAT X GETS structure",
                    suggestion="Include 'WHAT [NAME] GETS:' and 'WHAT [CLIENT] GETS:' sections",
                    location='mutual_benefit'
                ))

            # Check BOTH parties' benefits are included (not just one)
            gets_count = mb_upper.count('GETS:')
            if gets_count < 2:
                issues.append(VerificationIssue(
                    agent=self.name,
                    severity='critical',
                    issue=f"MUTUAL BENEFIT only shows {gets_count} party's benefits (need both)",
                    suggestion="Ensure both 'WHAT [PARTNER] GETS:' and 'WHAT [CLIENT] GETS:' are present",
                    location='mutual_benefit'
                ))

            # Check for actual bullet content
            if match.mutual_benefit.count('*') < 2:
                issues.append(VerificationIssue(
                    agent=self.name,
                    severity='warning',
                    issue="MUTUAL BENEFIT has fewer than 2 bullet points",
                    suggestion="Add specific benefits for each party",
                    location='mutual_benefit'
                ))

            # Check for content length that will be truncated in PDF
            if len(match.mutual_benefit) > 450:
                issues.append(VerificationIssue(
                    agent=self.name,
                    severity='warning',
                    issue=f"MUTUAL BENEFIT is {len(match.mutual_benefit)} chars (will be truncated at 450)",
                    suggestion="Condense bullet points to fit within PDF space",
                    location='mutual_benefit'
                ))

        # Check WHY FIT length
        if match.why_fit and len(match.why_fit) > 600:
            issues.append(VerificationIssue(
                agent=self.name,
                severity='warning',
                issue=f"WHY FIT is {len(match.why_fit)} chars (will be truncated at 600)",
                suggestion="Condense sections to fit within PDF space",
                location='why_fit'
            ))

        # Check outreach has proper structure
        if match.outreach_message:
            if 'Subject:' not in match.outreach_message:
                issues.append(VerificationIssue(
                    agent=self.name,
                    severity='warning',
                    issue="Outreach missing Subject line",
                    suggestion="Add 'Subject: [personalized subject]'",
                    location='outreach_message'
                ))

        return issues


class ContentVerificationAgent(BaseVerificationAgent):
    """
    AGENT 3: Content Verification
    Checks for empty sections, generic text, and missing specificity.
    """

    name = "content"

    GENERIC_PHRASES = [
        'your work resonates',
        'aligned business',
        'great synergy',
        'perfect fit',
        'complementary services',
        'win-win',
    ]

    def verify(self, match: EnrichedMatch) -> List[VerificationIssue]:
        issues = []

        first_name = match.name.split()[0] if match.name else 'Partner'

        # Check for empty sections
        if not match.why_fit or len(match.why_fit.strip()) < 50:
            issues.append(VerificationIssue(
                agent=self.name,
                severity='critical',
                issue="WHY FIT is empty or too short",
                suggestion="Generate compelling reasoning using profile data",
                location='why_fit'
            ))

        if not match.mutual_benefit or len(match.mutual_benefit.strip()) < 50:
            issues.append(VerificationIssue(
                agent=self.name,
                severity='critical',
                issue="MUTUAL BENEFIT is empty or too short",
                suggestion="Add specific benefits for both parties",
                location='mutual_benefit'
            ))

        # Check for "WHAT X GETS:" followed by nothing
        if match.mutual_benefit:
            # Look for pattern like "GETS:\n*" with nothing or "GETS:\n\n"
            empty_section_pattern = r'GETS:\s*\n\s*(\n|$|\*\s*$)'
            if re.search(empty_section_pattern, match.mutual_benefit):
                issues.append(VerificationIssue(
                    agent=self.name,
                    severity='critical',
                    issue=f"Found empty 'WHAT X GETS' section with no content",
                    suggestion="Ensure each GETS section has actual bullet points",
                    location='mutual_benefit'
                ))

        # Check name is used
        if match.why_fit and first_name.lower() not in match.why_fit.lower():
            issues.append(VerificationIssue(
                agent=self.name,
                severity='warning',
                issue=f"WHY FIT doesn't mention partner's name ({first_name})",
                suggestion="Include their name for personalization",
                location='why_fit'
            ))

        # Check for too many generic phrases
        all_text = (match.why_fit + match.mutual_benefit).lower()
        generic_count = sum(1 for phrase in self.GENERIC_PHRASES if phrase in all_text)
        if generic_count >= 2:
            issues.append(VerificationIssue(
                agent=self.name,
                severity='warning',
                issue=f"Content uses {generic_count} generic phrases",
                suggestion="Replace with specific references to their profile data",
                location='why_fit'
            ))

        # Check if seeking data is being used
        if match.seeking and len(match.seeking) > 20:
            if match.seeking[:30].lower() not in match.why_fit.lower():
                issues.append(VerificationIssue(
                    agent=self.name,
                    severity='warning',
                    issue="Not directly quoting what they're seeking",
                    suggestion=f"Reference their seeking: '{match.seeking[:50]}...'",
                    location='why_fit'
                ))

        return issues


class CapitalizationVerificationAgent(BaseVerificationAgent):
    """
    AGENT 4: Capitalization Verification
    Ensures proper capitalization on bullet points and section headers.
    """

    name = "capitalization"

    def verify(self, match: EnrichedMatch) -> List[VerificationIssue]:
        issues = []

        # Check all text fields with bullets
        fields_to_check = [
            ('mutual_benefit', match.mutual_benefit),
            ('outreach_message', match.outreach_message),
        ]

        for field_name, text in fields_to_check:
            if not text:
                continue

            # Find ACTUAL bullet points (at start of line) and check capitalization
            # Pattern: start of line or after newline, bullet marker, space, lowercase letter
            bullet_patterns = [
                (r'(?:^|\n)\s*\* ([a-z])', '*'),   # * at line start
                (r'(?:^|\n)\s*• ([a-z])', '•'),   # • at line start
                (r'(?:^|\n)\s*- ([a-z])', '-'),   # - at line start (actual list item)
            ]

            for pattern, bullet in bullet_patterns:
                matches = re.findall(pattern, text)
                if matches:
                    issues.append(VerificationIssue(
                        agent=self.name,
                        severity='warning',
                        issue=f"Found {len(matches)} bullet(s) starting with lowercase: '{bullet} {matches[0]}...'",
                        suggestion="Capitalize first letter after bullet point",
                        location=field_name
                    ))

        # Check section headers are uppercase
        if match.why_fit:
            header_pattern = r'^([A-Z][A-Z ]+):'
            headers = re.findall(header_pattern, match.why_fit, re.MULTILINE)
            if not headers and 'AUDIENCE' in match.why_fit.upper():
                # Check if headers are mixed case
                if 'Audience' in match.why_fit or 'They Want' in match.why_fit:
                    issues.append(VerificationIssue(
                        agent=self.name,
                        severity='info',
                        issue="Section headers are not consistently uppercase",
                        suggestion="Use 'AUDIENCE ALIGNMENT:' not 'Audience Alignment:'",
                        location='why_fit'
                    ))

        return issues


class TruncationVerificationAgent(BaseVerificationAgent):
    """
    AGENT 5: Truncation Verification
    Catches cut-off words and improper truncation.
    """

    name = "truncation"

    # Patterns that indicate bad truncation
    TRUNCATION_PATTERNS = [
        (r'\w+[—–-]$', 'word ending in dash'),  # "opportu—"
        (r'\w+\.\.\.$', 'triple dot truncation'),  # Could be OK, check context
        (r'\w{1,3}\.\.\.', 'very short word before ellipsis'),  # "op..."
        (r'[a-z]{1,2}$', 'ends with 1-2 lowercase letters (possible cut)'),  # "o" or "op"
    ]

    def verify(self, match: EnrichedMatch) -> List[VerificationIssue]:
        issues = []

        fields_to_check = [
            ('why_fit', match.why_fit),
            ('mutual_benefit', match.mutual_benefit),
            ('outreach_message', match.outreach_message),
        ]

        for field_name, text in fields_to_check:
            if not text:
                continue

            # Check for words cut off by dashes
            dash_truncated = re.findall(r'(\w+)[—–-](?:\s|$)', text)
            for word in dash_truncated:
                if len(word) > 2:  # Ignore intentional hyphens like "self-"
                    issues.append(VerificationIssue(
                        agent=self.name,
                        severity='critical',
                        issue=f"Word appears truncated: '{word}—'",
                        suggestion="Use TextSanitizer.truncate_safe() for word-safe truncation",
                        location=field_name
                    ))

            # Check for lines that end abruptly (possible truncation)
            lines = text.split('\n')
            for line in lines:
                line = line.strip()
                if not line:
                    continue

                # Check if line ends with partial word
                if re.search(r'\b\w{1,2}$', line) and not line.endswith(('a', 'I', 'to', 'in', 'on', 'of', 'or', 'an', 'at', 'by', 'is', 'be', 'we', 'up', 'so', 'no', 'go', 'me', 'my', 'do', 'if', 'as', 'it', 'us', 'am', '5K', '10')):
                    # Could be a truncation issue
                    last_word = line.split()[-1] if line.split() else ''
                    if len(last_word) <= 2 and last_word.isalpha():
                        issues.append(VerificationIssue(
                            agent=self.name,
                            severity='info',
                            issue=f"Line may be truncated: '...{line[-30:]}'",
                            suggestion="Verify text is complete",
                            location=field_name
                        ))

        return issues


class DataQualityVerificationAgent(BaseVerificationAgent):
    """
    AGENT 6: Data Quality Verification
    Catches useless boilerplate text, misplaced data, and field content issues.
    """

    name = "data_quality"

    # Patterns that indicate boilerplate/useless text (not actual contact info)
    BOILERPLATE_PATTERNS = [
        r'you can contact me',
        r'feel free to',
        r'in a variety of ways',
        r'including the following',
        r'reach out to me',
        r'don\'t hesitate to',
        r'any of the following',
        r'various ways',
    ]

    # Patterns that indicate actual contact information
    ACTUAL_CONTACT_PATTERNS = [
        r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',  # Email
        r'\d{3}[-.]?\d{3}[-.]?\d{4}',  # Phone (xxx-xxx-xxxx)
        r'\+\d{1,3}[-\s]?\d+',  # International phone
        r'calendly\.com',  # Calendly link
        r'cal\.com',  # Cal.com link
        r'Email:',  # Explicit email label
        r'Phone:',  # Explicit phone label
        r'Text:',  # Explicit text label
    ]

    def verify(self, match: EnrichedMatch) -> List[VerificationIssue]:
        issues = []

        # Check if website is actually a LinkedIn URL (confusing UX)
        if match.website:
            website_lower = match.website.lower()
            if 'linkedin.com' in website_lower:
                issues.append(VerificationIssue(
                    agent=self.name,
                    severity='warning',
                    issue=f"Website field contains LinkedIn URL: {match.website[:50]}",
                    suggestion="Move LinkedIn URL to linkedin field; use actual website or leave empty",
                    location='website'
                ))

        # Check notes for boilerplate in "Best contact" section
        if match.notes:
            notes_lower = match.notes.lower()

            # Look for "best contact" section with boilerplate instead of actual info
            has_best_contact = 'best contact' in notes_lower or 'best way to contact' in notes_lower

            if has_best_contact:
                # Check if it's just boilerplate intro text
                has_boilerplate = any(re.search(pattern, notes_lower) for pattern in self.BOILERPLATE_PATTERNS)
                has_actual_contact = any(re.search(pattern, match.notes, re.IGNORECASE) for pattern in self.ACTUAL_CONTACT_PATTERNS)

                if has_boilerplate and not has_actual_contact:
                    issues.append(VerificationIssue(
                        agent=self.name,
                        severity='critical',
                        issue="'Best contact' section contains intro text instead of actual contact info",
                        suggestion="Extract actual contact methods (Email:, Phone:) not intro sentences",
                        location='notes'
                    ))

        # Check for empty required fields
        if not match.name or len(match.name.strip()) < 2:
            issues.append(VerificationIssue(
                agent=self.name,
                severity='critical',
                issue="Name field is empty or too short",
                suggestion="Ensure valid name is provided",
                location='name'
            ))

        # Check for nonsense email values
        if match.email:
            email_lower = match.email.lower()
            if email_lower in ['update', 'n/a', 'none', 'na', '-', '.']:
                issues.append(VerificationIssue(
                    agent=self.name,
                    severity='warning',
                    issue=f"Email field contains placeholder value: '{match.email}'",
                    suggestion="Leave email empty rather than using placeholder",
                    location='email'
                ))

        # Check for URL in wrong fields
        if match.email and ('http' in match.email or '.com/' in match.email):
            issues.append(VerificationIssue(
                agent=self.name,
                severity='warning',
                issue="Email field contains URL instead of email address",
                suggestion="Move URL to website field",
                location='email'
            ))

        return issues


class MatchVerificationAgent:
    """
    MASTER VERIFICATION AGENT
    Coordinates multiple specialized agents to ensure PERFECT quality.
    """

    # Minimum score to pass (each agent contributes to total)
    MIN_SCORE_TO_PASS = 85  # Stricter threshold
    MAX_RETRY_ATTEMPTS = 3

    def __init__(self):
        # Initialize all specialized agents
        self.agents = [
            EncodingVerificationAgent(),
            FormattingVerificationAgent(),
            ContentVerificationAgent(),
            CapitalizationVerificationAgent(),
            TruncationVerificationAgent(),
            DataQualityVerificationAgent(),  # NEW: Catches boilerplate, misplaced data
        ]

        # Points deducted per issue severity
        self.severity_penalties = {
            'critical': 15,
            'warning': 5,
            'info': 1,
        }

    def verify(self, enriched_match: EnrichedMatch) -> VerificationResult:
        """
        Run ALL verification agents and aggregate results.
        """
        all_issues = []

        # Run each specialized agent
        for agent in self.agents:
            try:
                agent_issues = agent.verify(enriched_match)
                all_issues.extend(agent_issues)
                logger.debug(f"{agent.name}: Found {len(agent_issues)} issues")
            except Exception as e:
                logger.error(f"Agent {agent.name} failed: {e}")

        # Calculate score (start at 100, deduct for issues)
        score = 100.0
        for issue in all_issues:
            penalty = self.severity_penalties.get(issue.severity, 5)
            score -= penalty

        score = max(0, score)  # Don't go negative

        # Determine status
        critical_issues = [i for i in all_issues if i.severity == 'critical']

        if critical_issues:
            status = VerificationStatus.REJECTED if score < 50 else VerificationStatus.NEEDS_ENRICHMENT
        elif score >= self.MIN_SCORE_TO_PASS:
            status = VerificationStatus.PASSED
        elif score >= 60:
            status = VerificationStatus.NEEDS_ENRICHMENT
        else:
            status = VerificationStatus.REJECTED

        # Format issues and suggestions for output
        issue_strings = [f"[{i.agent}] {i.issue}" for i in all_issues]
        suggestion_strings = [f"[{i.agent}] {i.suggestion}" for i in all_issues if i.severity in ('critical', 'warning')]

        return VerificationResult(
            status=status,
            score=score,
            issues=issue_strings,
            suggestions=suggestion_strings
        )

    def verify_and_fix(self, enriched_match: EnrichedMatch) -> Tuple[EnrichedMatch, VerificationResult]:
        """
        Verify and attempt to fix issues automatically.
        Returns the fixed match and verification result.
        """
        # First, apply automatic fixes
        fixed_match = self._apply_auto_fixes(enriched_match)

        # Then verify
        result = self.verify(fixed_match)

        return fixed_match, result

    def _apply_auto_fixes(self, match: EnrichedMatch) -> EnrichedMatch:
        """Apply automatic fixes for common issues."""
        # Create a copy with fixed fields
        return EnrichedMatch(
            name=match.name,
            company=match.company,
            email=match.email,
            linkedin=match.linkedin,
            website=match.website,
            niche=match.niche,
            list_size=match.list_size,
            social_reach=match.social_reach,
            score=match.score,
            who_they_serve=TextSanitizer.sanitize(match.who_they_serve),
            what_they_do=TextSanitizer.sanitize(match.what_they_do),
            seeking=TextSanitizer.sanitize(match.seeking),
            offering=TextSanitizer.sanitize(match.offering),
            notes=match.notes,
            why_fit=TextSanitizer.sanitize(match.why_fit),
            mutual_benefit=TextSanitizer.sanitize(match.mutual_benefit),
            outreach_message=TextSanitizer.sanitize(match.outreach_message),
            verification_score=match.verification_score,
            verification_passed=match.verification_passed,
        )


def enrich_and_verify_matches(
    matches: List[Dict],
    client_profile: Dict,
    supabase_profiles: Optional[Dict[str, Dict]] = None
) -> List[EnrichedMatch]:
    """
    Main entry point: Enrich matches with full data, verify quality, and auto-fix issues.

    Args:
        matches: List of basic match dicts
        client_profile: Client's profile data
        supabase_profiles: Optional dict mapping names to full Supabase profiles

    Returns:
        List of EnrichedMatch objects that passed verification (with auto-fixes applied)
    """
    enrichment_service = MatchEnrichmentService(client_profile)
    verification_agent = MatchVerificationAgent()

    verified_matches = []

    for match_data in matches:
        name = match_data.get('name', '')

        # Get full profile if available
        full_profile = None
        if supabase_profiles:
            full_profile = supabase_profiles.get(name.lower())

        # Enrich
        enriched = enrichment_service.enrich_match(match_data, full_profile)

        # Verify AND auto-fix issues
        fixed_match, result = verification_agent.verify_and_fix(enriched)

        fixed_match.verification_score = result.score
        fixed_match.verification_passed = result.status == VerificationStatus.PASSED

        if result.status == VerificationStatus.PASSED:
            verified_matches.append(fixed_match)
            logger.info(f"✓ {name}: Verified (score: {result.score})")
        elif result.status == VerificationStatus.NEEDS_ENRICHMENT:
            # Still include but flag for review
            logger.warning(f"⚠ {name}: Needs review (score: {result.score})")
            for issue in result.issues[:3]:  # Show top 3 issues
                logger.warning(f"   - {issue}")
            verified_matches.append(fixed_match)
        else:
            logger.warning(f"✗ {name}: Rejected (score: {result.score})")
            for issue in result.issues:
                logger.warning(f"   - {issue}")

    return verified_matches
