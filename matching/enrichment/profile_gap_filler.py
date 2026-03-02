"""
AI-Powered Profile Gap Filler

Uses Claude (via ClaudeClient) to generate missing profile sections
from SupabaseProfile data. All prompts constrain Claude to ONLY use
data from the provided fields (anti-hallucination).
"""

import json
import logging
import os
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class ProfileGapFiller:
    """
    Uses Claude (via ClaudeClient) to generate missing profile sections
    from SupabaseProfile data. All prompts constrain Claude to ONLY use
    data from the provided fields (anti-hallucination).
    """

    def __init__(self):
        from .claude_client import ClaudeClient

        openrouter_key = os.environ.get('OPENROUTER_API_KEY', '')
        anthropic_key = os.environ.get('ANTHROPIC_API_KEY', '')
        self.client = ClaudeClient(
            max_tokens=2048,
            openrouter_key=openrouter_key,
            anthropic_key=anthropic_key,
        )

    def is_available(self) -> bool:
        """Check if the client has a configured API key."""
        return self.client.api_key is not None

    def fill_gaps(
        self,
        profile_data: dict,
        current_profile: dict,
        missing_fields: list,
    ) -> dict:
        """
        Generate content for missing fields.

        Args:
            profile_data: Raw SupabaseProfile data as dict (name, company,
                what_you_do, who_you_serve, etc.)
            current_profile: Current MemberReport.client_profile dict.
            missing_fields: List of field names that need filling.

        Returns:
            Dict of field_name -> generated_value for successfully filled fields.
        """
        if not self.is_available():
            logger.warning("ProfileGapFiller: No API key available, skipping AI generation")
            return {}

        filled: Dict = {}

        # Map field names to generator methods
        generators = {
            'about_story_paragraphs': self.generate_about_story,
            'credentials': self.generate_credentials,
            'faqs': self.generate_faqs,
            'key_message_headline': self.generate_key_message,
            'key_message_points': self.generate_key_message,
            'partner_deliverables': self.generate_partner_deliverables,
            'why_converts': self.generate_why_converts,
            'perfect_for': self.generate_perfect_for,
            'outreach_templates': self.generate_outreach_emails,
        }

        # Deduplicate -- key_message_headline and key_message_points share a generator
        called: set = set()
        for field_name in missing_fields:
            gen = generators.get(field_name)
            if gen and gen not in called:
                called.add(gen)
                try:
                    result = gen(profile_data)
                    if result:
                        if isinstance(result, dict):
                            filled.update(result)
                        else:
                            filled[field_name] = result
                except Exception as e:
                    logger.error(f"ProfileGapFiller: Failed to generate {field_name}: {e}")

        return filled

    # ------------------------------------------------------------------
    # Generator methods
    # ------------------------------------------------------------------

    def generate_about_story(self, profile_data: dict) -> Optional[List[str]]:
        """Generate 2-3 about/story paragraphs from profile data.

        Uses: bio, what_you_do, who_you_serve, company, signature_programs, niche.

        Returns:
            List of paragraph strings, or None on failure.
        """
        prompt = f"""You are generating content for a JV (joint venture) partner profile page.

CRITICAL: Use ONLY the data provided below. Do NOT invent, assume, or hallucinate any facts.
If the data is insufficient, return null.

PROFILE DATA:
- Name: {profile_data.get('name', '')}
- Company: {profile_data.get('company', '')}
- Bio: {profile_data.get('bio', '')}
- What they do: {profile_data.get('what_you_do', '')}
- Who they serve: {profile_data.get('who_you_serve', '')}
- Signature programs: {profile_data.get('signature_programs', '')}
- Niche: {profile_data.get('niche', '')}

TASK: Write 2-3 paragraphs about this person based ONLY on the data above. Do not invent any facts.
Each paragraph should be 2-4 sentences. Write in third person. Focus on what they do, who they
help, and what makes them notable.

Return ONLY valid JSON, no markdown, no explanation:
{{"paragraphs": ["paragraph 1 text", "paragraph 2 text", "paragraph 3 text"]}}
"""
        response = self._call_claude(prompt)
        data = self._parse_json_response(response)
        if data and isinstance(data.get('paragraphs'), list):
            return data['paragraphs']
        return None

    def generate_credentials(self, profile_data: dict) -> Optional[List[str]]:
        """Generate 3-5 credentials or notable achievements from profile data.

        Uses: signature_programs, bio, company, what_you_do.

        Returns:
            List of credential strings, or None on failure.
        """
        prompt = f"""You are generating content for a JV (joint venture) partner profile page.

CRITICAL: Use ONLY the data provided below. Do NOT invent, assume, or hallucinate any facts.
If the data is insufficient, return null.

PROFILE DATA:
- Name: {profile_data.get('name', '')}
- Company: {profile_data.get('company', '')}
- Bio: {profile_data.get('bio', '')}
- What they do: {profile_data.get('what_you_do', '')}
- Signature programs: {profile_data.get('signature_programs', '')}

TASK: List 3-5 credentials or notable achievements based ONLY on the data above. Each credential
should be a concise statement (one sentence or phrase). Only include things that are clearly
supported by the provided data.

Return ONLY valid JSON, no markdown, no explanation:
{{"credentials": ["credential 1", "credential 2", "credential 3"]}}
"""
        response = self._call_claude(prompt)
        data = self._parse_json_response(response)
        if data and isinstance(data.get('credentials'), list):
            return data['credentials']
        return None

    def generate_faqs(self, profile_data: dict) -> Optional[List[Dict[str, str]]]:
        """Generate 4-5 FAQs a potential JV partner would ask.

        Uses: what_you_do, who_you_serve, seeking, offering, niche.

        Returns:
            List of {{"q": "...", "a": "..."}} dicts, or None on failure.
        """
        prompt = f"""You are generating content for a JV (joint venture) partner profile page.

CRITICAL: Use ONLY the data provided below. Do NOT invent, assume, or hallucinate any facts.
If the data is insufficient for a field, return null for that field.

PROFILE DATA:
- Name: {profile_data.get('name', '')}
- Company: {profile_data.get('company', '')}
- What they do: {profile_data.get('what_you_do', '')}
- Who they serve: {profile_data.get('who_you_serve', '')}
- Seeking: {profile_data.get('seeking', '')}
- Offering: {profile_data.get('offering', '')}
- Niche: {profile_data.get('niche', '')}

TASK: Generate 4-5 FAQs that a potential JV partner would ask about partnering with this person.
Questions should address partnership logistics, audience overlap, mutual benefits, and what to
expect from a collaboration. Answers should be based ONLY on the provided data.

Return ONLY valid JSON, no markdown, no explanation:
{{"faqs": [{{"q": "question text", "a": "answer text"}}, ...]}}
"""
        response = self._call_claude(prompt)
        data = self._parse_json_response(response)
        if data and isinstance(data.get('faqs'), list):
            # Validate each FAQ has q and a keys
            valid_faqs = [
                faq for faq in data['faqs']
                if isinstance(faq, dict) and 'q' in faq and 'a' in faq
            ]
            return valid_faqs if valid_faqs else None
        return None

    def generate_key_message(self, profile_data: dict) -> Optional[Dict]:
        """Generate a key positioning headline and 3 supporting points.

        Uses: offering, what_you_do, who_you_serve, list_size, social_reach.

        Returns:
            Dict with 'key_message_headline' and 'key_message_points' keys,
            or None on failure.
        """
        prompt = f"""You are generating content for a JV (joint venture) partner profile page.

CRITICAL: Use ONLY the data provided below. Do NOT invent, assume, or hallucinate any facts.
If the data is insufficient for a field, return null for that field.

PROFILE DATA:
- Name: {profile_data.get('name', '')}
- Company: {profile_data.get('company', '')}
- What they do: {profile_data.get('what_you_do', '')}
- Who they serve: {profile_data.get('who_you_serve', '')}
- Offering: {profile_data.get('offering', '')}
- List size: {profile_data.get('list_size', '')}
- Social reach: {profile_data.get('social_reach', '')}

TASK: Create a key positioning headline (one compelling sentence that captures their unique value
for JV partners) and 3 supporting bullet points that expand on why a partner should work with
this person. Base everything ONLY on the provided data.

Return ONLY valid JSON, no markdown, no explanation:
{{"key_message_headline": "headline text", "key_message_points": ["point 1", "point 2", "point 3"]}}
"""
        response = self._call_claude(prompt)
        data = self._parse_json_response(response)
        if data and 'key_message_headline' in data and 'key_message_points' in data:
            return {
                'key_message_headline': data['key_message_headline'],
                'key_message_points': data['key_message_points'],
            }
        return None

    def generate_outreach_emails(self, profile_data: dict) -> Optional[Dict]:
        """Generate two outreach email templates (Initial + Follow-Up).

        Uses: name, company, what_you_do, offering, who_you_serve,
              booking_link, list_size, signature_programs.

        Returns:
            Dict with 'outreach_templates' key containing initial and followup
            email dicts, or None on failure.
        """
        booking_link = profile_data.get('booking_link', '')
        booking_instruction = (
            f"Include their actual booking link: {booking_link}"
            if booking_link
            else "No booking link available -- use a generic CTA like 'reply to this email'."
        )

        prompt = f"""You are generating content for a JV (joint venture) partner profile page.

CRITICAL: Use ONLY the data provided below. Do NOT invent, assume, or hallucinate any facts.
If the data is insufficient for a field, return null for that field.

PROFILE DATA:
- Name: {profile_data.get('name', '')}
- Company: {profile_data.get('company', '')}
- What they do: {profile_data.get('what_you_do', '')}
- Who they serve: {profile_data.get('who_you_serve', '')}
- Offering: {profile_data.get('offering', '')}
- Booking link: {booking_link}
- List size: {profile_data.get('list_size', '')}
- Signature programs: {profile_data.get('signature_programs', '')}

TASK: Generate two outreach emails that a JV partner could send to this person. The emails must
be SPECIFIC to this client's actual business and offerings.

INITIAL EMAIL (120-250 words):
- Reference their actual offering and what they do
- {booking_instruction}
- Include social proof or a reason the partnership makes sense
- Use {{{{partner_first_name}}}} as the only personalization token for the recipient
- Professional but warm tone

FOLLOW-UP EMAIL (50-100 words):
- NOT just "following up" -- restate the value proposition differently
- Reference a specific aspect of their business
- Use {{{{partner_first_name}}}} as the only personalization token for the recipient

Return ONLY valid JSON, no markdown, no explanation:
{{
    "outreach_templates": {{
        "initial": {{
            "title": "Initial Outreach",
            "text": "email body text"
        }},
        "followup": {{
            "title": "Follow-Up",
            "text": "email body text"
        }}
    }}
}}
"""
        response = self._call_claude(prompt)
        data = self._parse_json_response(response)
        if data and isinstance(data.get('outreach_templates'), dict):
            templates = data['outreach_templates']
            # Validate both templates exist with required keys
            if (
                isinstance(templates.get('initial'), dict)
                and 'text' in templates['initial']
                and isinstance(templates.get('followup'), dict)
                and 'text' in templates['followup']
            ):
                return {'outreach_templates': templates}
        return None

    def generate_perfect_for(self, profile_data: dict) -> Optional[List[str]]:
        """Generate 3-5 ideal partner archetype descriptions.

        Uses: who_you_serve, niche, audience_type, seeking.

        Returns:
            List of archetype description strings, or None on failure.
        """
        prompt = f"""You are generating content for a JV (joint venture) partner profile page.

CRITICAL: Use ONLY the data provided below. Do NOT invent, assume, or hallucinate any facts.
If the data is insufficient, return null.

PROFILE DATA:
- Name: {profile_data.get('name', '')}
- Company: {profile_data.get('company', '')}
- Who they serve: {profile_data.get('who_you_serve', '')}
- Niche: {profile_data.get('niche', '')}
- Audience type: {profile_data.get('audience_type', '')}
- Seeking: {profile_data.get('seeking', '')}

TASK: List 3-5 ideal JV partner archetypes for this person. Each entry should describe the type
of partner who would be the best fit (e.g., "Coaches who serve high-achieving women and want to
cross-promote programs"). Base these ONLY on the provided data about who they serve and what
they're seeking.

Return ONLY valid JSON, no markdown, no explanation:
{{"perfect_for": ["archetype 1 description", "archetype 2 description", ...]}}
"""
        response = self._call_claude(prompt)
        data = self._parse_json_response(response)
        if data and isinstance(data.get('perfect_for'), list):
            return data['perfect_for']
        return None

    def generate_why_converts(self, profile_data: dict) -> Optional[List[str]]:
        """Generate 2-4 reasons why this person's offering converts well.

        Uses: offering, what_you_do, list_size, revenue_tier, signature_programs.

        Returns:
            List of conversion reason strings, or None on failure.
        """
        prompt = f"""You are generating content for a JV (joint venture) partner profile page.

CRITICAL: Use ONLY the data provided below. Do NOT invent, assume, or hallucinate any facts.
If the data is insufficient, return null.

PROFILE DATA:
- Name: {profile_data.get('name', '')}
- Company: {profile_data.get('company', '')}
- What they do: {profile_data.get('what_you_do', '')}
- Offering: {profile_data.get('offering', '')}
- List size: {profile_data.get('list_size', '')}
- Revenue tier: {profile_data.get('revenue_tier', '')}
- Signature programs: {profile_data.get('signature_programs', '')}

TASK: List 2-4 reasons why this person's offering converts well for JV partners. Each reason
should explain a specific aspect that makes their offering attractive for partner promotions
(e.g., proven product, engaged audience, strong funnel). Base these ONLY on the provided data.

Return ONLY valid JSON, no markdown, no explanation:
{{"why_converts": ["reason 1", "reason 2", "reason 3"]}}
"""
        response = self._call_claude(prompt)
        data = self._parse_json_response(response)
        if data and isinstance(data.get('why_converts'), list):
            return data['why_converts']
        return None

    def generate_partner_deliverables(self, profile_data: dict) -> Optional[List[str]]:
        """Generate 3-5 things partners receive when collaborating.

        Uses: offering, what_you_do, content_platforms.

        Returns:
            List of deliverable description strings, or None on failure.
        """
        prompt = f"""You are generating content for a JV (joint venture) partner profile page.

CRITICAL: Use ONLY the data provided below. Do NOT invent, assume, or hallucinate any facts.
If the data is insufficient, return null.

PROFILE DATA:
- Name: {profile_data.get('name', '')}
- Company: {profile_data.get('company', '')}
- What they do: {profile_data.get('what_you_do', '')}
- Offering: {profile_data.get('offering', '')}
- Content platforms: {profile_data.get('content_platforms', '')}

TASK: List 3-5 specific things that JV partners receive when collaborating with this person.
These should be concrete deliverables or benefits (e.g., "Promotion to their email list of X
subscribers", "Guest spot on their podcast", "Co-branded landing page"). Base these ONLY on the
provided data.

Return ONLY valid JSON, no markdown, no explanation:
{{"partner_deliverables": ["deliverable 1", "deliverable 2", "deliverable 3"]}}
"""
        response = self._call_claude(prompt)
        data = self._parse_json_response(response)
        if data and isinstance(data.get('partner_deliverables'), list):
            return data['partner_deliverables']
        return None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _call_claude(self, prompt: str) -> Optional[str]:
        """Call Claude via the shared ClaudeClient."""
        if not self.client:
            return None
        return self.client.call(prompt)

    def _parse_json_response(self, response: str) -> Optional[Dict]:
        """Extract JSON from Claude's response, handling markdown code blocks.

        Delegates to ClaudeClient.parse_json for consistent parsing across
        the enrichment pipeline.
        """
        from .claude_client import ClaudeClient
        return ClaudeClient.parse_json(response)
