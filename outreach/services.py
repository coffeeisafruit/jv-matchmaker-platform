"""
PVP Generator and Clay Webhook Services for GTM Engine Platform.

Implements Jordan Crawford's Pain-to-Solution pattern for generating
Permissionless Value Propositions (PVPs) and handles Clay enrichment webhooks.
"""

import hashlib
import hmac
import json
import logging
from dataclasses import dataclass
from typing import Any

from django.conf import settings

logger = logging.getLogger(__name__)


@dataclass
class PVPResult:
    """Result of PVP generation."""
    pain_point_addressed: str
    value_offered: str
    call_to_action: str
    full_message: str
    quality_score: float
    quality_breakdown: dict
    personalization_data: dict


class PVPGeneratorService:
    """
    Service for generating Permissionless Value Propositions (PVPs).

    Implements Jordan Crawford's Pain-to-Solution pattern which focuses on:
    1. Identifying a specific pain point the prospect has
    2. Offering genuine value upfront without asking for anything
    3. Demonstrating understanding of their situation
    4. Creating a natural opening for further conversation

    Quality is scored using a 7-criterion rubric based on AutoClaygent methodology.
    """

    # Pattern types and their prompts
    PATTERN_PROMPTS = {
        'pain_solution': """You are an expert B2B outreach specialist implementing Jordan Crawford's Pain-to-Solution pattern.

Your task is to create a Permissionless Value Proposition (PVP) - a personalized outreach message that provides genuine value upfront without asking for anything in return.

## The Pain-to-Solution Framework

1. **Identify the Pain**: Start with a specific, observable pain point the prospect is likely experiencing
2. **Show Understanding**: Demonstrate you understand their world and challenges
3. **Offer Value First**: Provide something useful (insight, resource, observation) without asking for anything
4. **Create Natural Opening**: End with an invitation to continue the conversation, not a hard ask

## Context About the Prospect

**Profile Information:**
- Name: {profile_name}
- Company: {profile_company}
- Industry: {profile_industry}
- Audience Size: {profile_audience_size}
- Audience Description: {profile_audience_description}
- Content Style: {profile_content_style}
{enrichment_section}

**Match Analysis:**
- Intent Score: {intent_score} (signals they may need what you offer)
- Synergy Score: {synergy_score} (audience/content alignment)
- Momentum Score: {momentum_score} (recent activity level)
- Overall Match Score: {final_score}
{score_breakdown_section}

## About Your Offering (ICP Context)

**Target Industry:** {icp_industry}
**Pain Points You Solve:**
{pain_points}

**Goals You Help Achieve:**
{goals}

## Your Task

Create a PVP email that:
1. Opens with a specific observation about their business/content (not generic flattery)
2. Connects to a pain point they likely face
3. Offers a genuine insight, resource, or observation that helps them
4. Closes with a soft invitation to connect (not a hard pitch)

The message should be:
- 100-150 words maximum
- Written in a conversational, human tone
- Specific to THIS person (no templates that could apply to anyone)
- Valuable even if they never respond

## Output Format

Provide your response in the following JSON format:
{{
    "pain_point_addressed": "The specific pain point you're addressing",
    "value_offered": "The specific value/insight you're providing",
    "call_to_action": "Your soft closing invitation",
    "full_message": "The complete email message",
    "personalization_elements": ["list", "of", "specific", "personalization", "points", "used"]
}}""",

        'insight_share': """You are an expert B2B outreach specialist creating an Insight Share PVP.

The Insight Share pattern focuses on sharing a valuable, relevant insight that demonstrates expertise and creates curiosity.

## Context About the Prospect

**Profile Information:**
- Name: {profile_name}
- Company: {profile_company}
- Industry: {profile_industry}
- Audience Size: {profile_audience_size}
- Audience Description: {profile_audience_description}
{enrichment_section}

**Match Analysis:**
- Intent Score: {intent_score}
- Synergy Score: {synergy_score}
- Momentum Score: {momentum_score}
- Overall Match Score: {final_score}
{score_breakdown_section}

## About Your Offering (ICP Context)

**Target Industry:** {icp_industry}
**Pain Points You Solve:**
{pain_points}

**Goals You Help Achieve:**
{goals}

## Your Task

Create an Insight Share PVP that:
1. Leads with a compelling industry insight or trend observation
2. Makes it specific to their business context
3. Offers additional value (report, analysis, or deeper insight)
4. Creates natural curiosity without hard selling

Output in JSON format:
{{
    "pain_point_addressed": "The challenge/opportunity your insight addresses",
    "value_offered": "The specific insight you're sharing",
    "call_to_action": "Your invitation to learn more",
    "full_message": "The complete email message",
    "personalization_elements": ["list", "of", "specific", "personalization", "points"]
}}""",

        'mutual_benefit': """You are an expert B2B outreach specialist creating a Mutual Benefit PVP.

The Mutual Benefit pattern focuses on proposing a collaboration or exchange that clearly benefits both parties.

## Context About the Prospect

**Profile Information:**
- Name: {profile_name}
- Company: {profile_company}
- Industry: {profile_industry}
- Audience Size: {profile_audience_size}
- Audience Description: {profile_audience_description}
{enrichment_section}

**Match Analysis:**
- Intent Score: {intent_score}
- Synergy Score: {synergy_score}
- Momentum Score: {momentum_score}
- Overall Match Score: {final_score}
{score_breakdown_section}

## About Your Offering (ICP Context)

**Target Industry:** {icp_industry}
**Pain Points You Solve:**
{pain_points}

**Goals You Help Achieve:**
{goals}

## Your Task

Create a Mutual Benefit PVP that:
1. Identifies a specific collaboration opportunity
2. Clearly articulates what they get AND what you get
3. Makes the benefit to them obvious and valuable
4. Proposes a low-commitment first step

Output in JSON format:
{{
    "pain_point_addressed": "The opportunity/need you're addressing",
    "value_offered": "The mutual benefit you're proposing",
    "call_to_action": "Your proposed first step",
    "full_message": "The complete email message",
    "personalization_elements": ["list", "of", "specific", "personalization", "points"]
}}""",

        'social_proof': """You are an expert B2B outreach specialist creating a Social Proof PVP.

The Social Proof pattern leverages relevant success stories and connections to build credibility.

## Context About the Prospect

**Profile Information:**
- Name: {profile_name}
- Company: {profile_company}
- Industry: {profile_industry}
- Audience Size: {profile_audience_size}
- Audience Description: {profile_audience_description}
{enrichment_section}

**Match Analysis:**
- Intent Score: {intent_score}
- Synergy Score: {synergy_score}
- Momentum Score: {momentum_score}
- Overall Match Score: {final_score}
{score_breakdown_section}

## About Your Offering (ICP Context)

**Target Industry:** {icp_industry}
**Pain Points You Solve:**
{pain_points}

**Goals You Help Achieve:**
{goals}

## Your Task

Create a Social Proof PVP that:
1. References a relevant mutual connection, similar company, or shared experience
2. Shares a brief, relevant success story
3. Connects the proof to their specific situation
4. Offers to share more details without being pushy

Output in JSON format:
{{
    "pain_point_addressed": "The challenge your social proof addresses",
    "value_offered": "The relevant proof/story you're sharing",
    "call_to_action": "Your invitation to learn more",
    "full_message": "The complete email message",
    "personalization_elements": ["list", "of", "specific", "personalization", "points"]
}}""",

        'curiosity_hook': """You are an expert B2B outreach specialist creating a Curiosity Hook PVP.

The Curiosity Hook pattern creates intrigue about something specific and valuable to the prospect.

## Context About the Prospect

**Profile Information:**
- Name: {profile_name}
- Company: {profile_company}
- Industry: {profile_industry}
- Audience Size: {profile_audience_size}
- Audience Description: {profile_audience_description}
{enrichment_section}

**Match Analysis:**
- Intent Score: {intent_score}
- Synergy Score: {synergy_score}
- Momentum Score: {momentum_score}
- Overall Match Score: {final_score}
{score_breakdown_section}

## About Your Offering (ICP Context)

**Target Industry:** {icp_industry}
**Pain Points You Solve:**
{pain_points}

**Goals You Help Achieve:**
{goals}

## Your Task

Create a Curiosity Hook PVP that:
1. Opens with something specific you noticed about their business
2. Hints at a valuable insight or opportunity without fully revealing it
3. Creates genuine curiosity (not clickbait)
4. Makes it easy for them to satisfy their curiosity

Output in JSON format:
{{
    "pain_point_addressed": "The opportunity/area you're creating curiosity about",
    "value_offered": "The teased value/insight",
    "call_to_action": "How they can learn more",
    "full_message": "The complete email message",
    "personalization_elements": ["list", "of", "specific", "personalization", "points"]
}}"""
    }

    # Quality scoring rubric based on AutoClaygent 7-criterion methodology
    QUALITY_CRITERIA = {
        'personalization': {
            'weight': 20,
            'description': 'Uses specific details about the prospect that could not apply to anyone else'
        },
        'relevance': {
            'weight': 15,
            'description': 'Addresses a pain point or opportunity relevant to their specific situation'
        },
        'value_first': {
            'weight': 20,
            'description': 'Provides genuine value upfront without asking for anything'
        },
        'clarity': {
            'weight': 10,
            'description': 'Message is clear, concise, and easy to understand'
        },
        'tone': {
            'weight': 10,
            'description': 'Professional yet conversational, human-sounding'
        },
        'call_to_action': {
            'weight': 15,
            'description': 'Soft, non-pushy invitation that creates natural next step'
        },
        'credibility': {
            'weight': 10,
            'description': 'Establishes credibility without being boastful'
        }
    }

    def __init__(self, api_key: str = None):
        """Initialize the PVP Generator Service."""
        self.api_key = api_key or settings.ANTHROPIC_API_KEY
        self.model = settings.AI_CONFIG.get('default_model', 'claude-sonnet-4-20250514')
        self.max_tokens = settings.AI_CONFIG.get('max_tokens', 4096)
        self.temperature = settings.AI_CONFIG.get('temperature', 0.7)

    def generate_pvp(self, match, pattern_type: str = 'pain_solution', icp=None) -> PVPResult:
        """
        Generate a PVP for a given match.

        Args:
            match: Match object containing profile and score information
            pattern_type: Type of PVP pattern to use
            icp: Optional ICP object for additional context

        Returns:
            PVPResult with generated content and quality score
        """
        # Build the prompt with match context
        prompt = self._build_prompt(match, pattern_type, icp)

        # Call Claude API
        response = self._call_claude(prompt)

        # Parse the response
        pvp_data = self._parse_response(response)

        # Calculate quality score
        quality_score, quality_breakdown = self._calculate_quality_score(
            pvp_data, match, pattern_type
        )

        return PVPResult(
            pain_point_addressed=pvp_data.get('pain_point_addressed', ''),
            value_offered=pvp_data.get('value_offered', ''),
            call_to_action=pvp_data.get('call_to_action', ''),
            full_message=pvp_data.get('full_message', ''),
            quality_score=quality_score,
            quality_breakdown=quality_breakdown,
            personalization_data={
                'elements': pvp_data.get('personalization_elements', []),
                'pattern_type': pattern_type
            }
        )

    def generate_pvp_for_supabase(
        self,
        profile,  # SupabaseProfile
        match_data,  # SupabaseMatch or None
        pattern_type: str = 'pain_solution',
        icp=None
    ) -> PVPResult:
        """
        Generate a PVP for a Supabase profile.

        Args:
            profile: SupabaseProfile object
            match_data: Optional SupabaseMatch object with scoring data
            pattern_type: Type of PVP pattern to use
            icp: Optional ICP object for additional context

        Returns:
            PVPResult with generated content and quality score
        """
        # Build the prompt with Supabase profile context
        prompt = self._build_supabase_prompt(profile, match_data, pattern_type, icp)

        # Call Claude API
        response = self._call_claude(prompt)

        # Parse the response
        pvp_data = self._parse_response(response)

        # Calculate quality score for Supabase profile
        quality_score, quality_breakdown = self._calculate_supabase_quality_score(
            pvp_data, profile, match_data, pattern_type
        )

        return PVPResult(
            pain_point_addressed=pvp_data.get('pain_point_addressed', ''),
            value_offered=pvp_data.get('value_offered', ''),
            call_to_action=pvp_data.get('call_to_action', ''),
            full_message=pvp_data.get('full_message', ''),
            quality_score=quality_score,
            quality_breakdown=quality_breakdown,
            personalization_data={
                'elements': pvp_data.get('personalization_elements', []),
                'pattern_type': pattern_type,
                'supabase_profile_id': str(profile.id)
            }
        )

    def _build_supabase_prompt(self, profile, match_data, pattern_type: str, icp=None) -> str:
        """Build the prompt with Supabase profile and match context."""
        # Build enrichment section from Supabase profile fields
        enrichment_section = ""
        enrichment_fields = [
            ('what_you_do', 'What They Do'),
            ('who_you_serve', 'Who They Serve'),
            ('seeking', 'What They\'re Seeking'),
            ('offering', 'What They\'re Offering'),
            ('current_projects', 'Current Projects'),
            ('bio', 'Bio'),
        ]
        for field, label in enrichment_fields:
            value = getattr(profile, field, None)
            if value:
                enrichment_section += f"\n- {label}: {value}"

        if enrichment_section:
            enrichment_section = "\n**Additional Context:**" + enrichment_section

        # Build score breakdown section from match data
        score_breakdown_section = ""
        if match_data:
            score_breakdown_section = "\n**Match Analysis:**\n"
            if match_data.harmonic_mean:
                score_breakdown_section += f"- Overall Match Score: {float(match_data.harmonic_mean) * 100:.0f}%\n"
            if match_data.score_ab:
                score_breakdown_section += f"- Your→Them Score: {float(match_data.score_ab) * 100:.0f}%\n"
            if match_data.score_ba:
                score_breakdown_section += f"- Them→You Score: {float(match_data.score_ba) * 100:.0f}%\n"
            if match_data.rich_analysis:
                score_breakdown_section += f"- Analysis: {match_data.rich_analysis[:500]}...\n"

        # Build pain points and goals from ICP
        pain_points = "- General business pain points"
        goals = "- General business goals"
        icp_industry = profile.niche or "General"

        if icp:
            if icp.pain_points:
                pain_points = "\n".join(f"- {p}" for p in icp.pain_points)
            if icp.goals:
                goals = "\n".join(f"- {g}" for g in icp.goals)
            icp_industry = icp.industry

        # Calculate audience size
        audience_size = "Unknown"
        if profile.list_size or profile.social_reach:
            list_size = profile.list_size or 0
            social = profile.social_reach or 0
            total = list_size + social
            if total > 100000:
                audience_size = f"{total:,} (Large)"
            elif total > 10000:
                audience_size = f"{total:,} (Medium)"
            elif total > 0:
                audience_size = f"{total:,} (Small)"

        # Get the pattern prompt
        pattern_prompt = self.PATTERN_PROMPTS.get(
            pattern_type,
            self.PATTERN_PROMPTS['pain_solution']
        )

        # Format the prompt
        return pattern_prompt.format(
            profile_name=profile.name or "Unknown",
            profile_company=profile.company or "Unknown",
            profile_industry=profile.niche or "Unknown",
            profile_audience_size=audience_size,
            profile_audience_description=profile.who_you_serve or "Not specified",
            profile_content_style=profile.what_you_do or "Not specified",
            enrichment_section=enrichment_section,
            intent_score=f"{float(match_data.score_ba or 0) * 100:.0f}" if match_data else "N/A",
            synergy_score=f"{float(match_data.harmonic_mean or 0) * 100:.0f}" if match_data else "N/A",
            momentum_score="N/A",  # Not available in SupabaseMatch
            final_score=f"{float(match_data.harmonic_mean or 0) * 100:.0f}" if match_data else "N/A",
            score_breakdown_section=score_breakdown_section,
            icp_industry=icp_industry,
            pain_points=pain_points,
            goals=goals
        )

    def _calculate_supabase_quality_score(
        self,
        pvp_data: dict,
        profile,
        match_data,
        pattern_type: str
    ) -> tuple[float, dict]:
        """Calculate quality score for Supabase-based PVP."""
        breakdown = {}
        total_score = 0

        full_message = pvp_data.get('full_message', '')
        personalization_elements = pvp_data.get('personalization_elements', [])

        # 1. Personalization (20 points)
        personalization_score = min(len(personalization_elements) * 4, 20)
        if profile.name and profile.name.split()[0] in full_message:
            personalization_score = min(personalization_score + 2, 20)
        if profile.company and profile.company in full_message:
            personalization_score = min(personalization_score + 3, 20)
        breakdown['personalization'] = {
            'score': personalization_score,
            'max': 20,
            'notes': f'{len(personalization_elements)} personalization elements detected'
        }
        total_score += personalization_score

        # 2. Relevance (15 points)
        relevance_score = 10
        if pvp_data.get('pain_point_addressed') and len(pvp_data['pain_point_addressed']) > 20:
            relevance_score += 5
        breakdown['relevance'] = {
            'score': relevance_score,
            'max': 15,
            'notes': 'Pain point addressed in message'
        }
        total_score += relevance_score

        # 3. Value First (20 points)
        value_score = 10
        value_offered = pvp_data.get('value_offered', '')
        if len(value_offered) > 30:
            value_score += 5
        if 'insight' in value_offered.lower() or 'help' in full_message.lower():
            value_score += 5
        breakdown['value_first'] = {
            'score': value_score,
            'max': 20,
            'notes': 'Value proposition present in message'
        }
        total_score += value_score

        # 4. Clarity (10 points)
        word_count = len(full_message.split())
        clarity_score = 10 if 50 <= word_count <= 200 else 7
        breakdown['clarity'] = {
            'score': clarity_score,
            'max': 10,
            'notes': f'{word_count} words'
        }
        total_score += clarity_score

        # 5. Tone (10 points)
        tone_score = 8
        pushy_words = ['buy', 'purchase', 'discount', 'limited time', 'act now']
        if any(word in full_message.lower() for word in pushy_words):
            tone_score -= 3
        if '?' in full_message:
            tone_score += 2
        tone_score = max(0, min(tone_score, 10))
        breakdown['tone'] = {
            'score': tone_score,
            'max': 10,
            'notes': 'Conversational tone assessment'
        }
        total_score += tone_score

        # 6. Call to Action (15 points)
        cta = pvp_data.get('call_to_action', '')
        cta_score = 10 if cta else 5
        soft_cta_words = ['thoughts', 'interested', 'curious', 'worth', 'open to']
        if any(word in cta.lower() for word in soft_cta_words):
            cta_score += 5
        breakdown['call_to_action'] = {
            'score': cta_score,
            'max': 15,
            'notes': 'Soft CTA present' if cta_score >= 12 else 'CTA could be softer'
        }
        total_score += cta_score

        # 7. Credibility (10 points)
        credibility_score = 7
        if match_data and match_data.harmonic_mean and float(match_data.harmonic_mean) > 0.7:
            credibility_score += 3
        breakdown['credibility'] = {
            'score': credibility_score,
            'max': 10,
            'notes': 'Credibility established through relevance'
        }
        total_score += credibility_score

        return total_score, breakdown

    def _build_prompt(self, match, pattern_type: str, icp=None) -> str:
        """Build the prompt with match and ICP context."""
        profile = match.profile

        # Build enrichment section if available
        enrichment_section = ""
        if profile.enrichment_data:
            enrichment_section = "\n**Enrichment Data (from Clay):**\n"
            for key, value in profile.enrichment_data.items():
                if value:
                    enrichment_section += f"- {key}: {value}\n"

        # Build score breakdown section
        score_breakdown_section = ""
        if match.score_breakdown:
            score_breakdown_section = "\n**Score Breakdown:**\n"
            for key, value in match.score_breakdown.items():
                score_breakdown_section += f"- {key}: {value}\n"

        # Build pain points and goals from ICP
        pain_points = "- General business pain points"
        goals = "- General business goals"
        icp_industry = profile.industry or "General"

        if icp:
            if icp.pain_points:
                pain_points = "\n".join(f"- {p}" for p in icp.pain_points)
            if icp.goals:
                goals = "\n".join(f"- {g}" for g in icp.goals)
            icp_industry = icp.industry

        # Get the pattern prompt
        pattern_prompt = self.PATTERN_PROMPTS.get(
            pattern_type,
            self.PATTERN_PROMPTS['pain_solution']
        )

        # Format the prompt
        return pattern_prompt.format(
            profile_name=profile.name,
            profile_company=profile.company or "Unknown",
            profile_industry=profile.industry or "Unknown",
            profile_audience_size=profile.get_audience_size_display() if profile.audience_size else "Unknown",
            profile_audience_description=profile.audience_description or "Not specified",
            profile_content_style=profile.content_style or "Not specified",
            enrichment_section=enrichment_section,
            intent_score=f"{match.intent_score:.2f}",
            synergy_score=f"{match.synergy_score:.2f}",
            momentum_score=f"{match.momentum_score:.2f}",
            final_score=f"{match.final_score:.2f}",
            score_breakdown_section=score_breakdown_section,
            icp_industry=icp_industry,
            pain_points=pain_points,
            goals=goals
        )

    def _call_claude(self, prompt: str) -> str:
        """Call the Claude API with the prompt."""
        try:
            import anthropic

            client = anthropic.Anthropic(api_key=self.api_key)

            message = client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                temperature=self.temperature,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )

            return message.content[0].text

        except ImportError:
            logger.error("anthropic package not installed")
            raise ImportError("Please install the anthropic package: pip install anthropic")
        except Exception as e:
            logger.error(f"Error calling Claude API: {e}")
            raise

    def _parse_response(self, response: str) -> dict:
        """Parse the JSON response from Claude."""
        try:
            # Try to extract JSON from the response
            # Claude sometimes wraps JSON in markdown code blocks
            if "```json" in response:
                start = response.find("```json") + 7
                end = response.find("```", start)
                response = response[start:end].strip()
            elif "```" in response:
                start = response.find("```") + 3
                end = response.find("```", start)
                response = response[start:end].strip()

            return json.loads(response)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Claude response as JSON: {e}")
            # Return a default structure with the raw response
            return {
                'pain_point_addressed': 'Unable to parse',
                'value_offered': 'Unable to parse',
                'call_to_action': 'Unable to parse',
                'full_message': response,
                'personalization_elements': []
            }

    def _calculate_quality_score(
        self,
        pvp_data: dict,
        match,
        pattern_type: str
    ) -> tuple[float, dict]:
        """
        Calculate quality score based on 7-criterion rubric.

        Returns:
            Tuple of (total_score, breakdown_dict)
        """
        breakdown = {}
        total_score = 0

        full_message = pvp_data.get('full_message', '')
        personalization_elements = pvp_data.get('personalization_elements', [])

        # 1. Personalization (20 points)
        personalization_score = min(len(personalization_elements) * 4, 20)
        if match.profile.name.split()[0] in full_message:
            personalization_score = min(personalization_score + 2, 20)
        if match.profile.company and match.profile.company in full_message:
            personalization_score = min(personalization_score + 3, 20)
        breakdown['personalization'] = {
            'score': personalization_score,
            'max': 20,
            'notes': f'{len(personalization_elements)} personalization elements detected'
        }
        total_score += personalization_score

        # 2. Relevance (15 points)
        relevance_score = 10  # Base score
        if pvp_data.get('pain_point_addressed') and len(pvp_data['pain_point_addressed']) > 20:
            relevance_score += 5
        breakdown['relevance'] = {
            'score': relevance_score,
            'max': 15,
            'notes': 'Pain point addressed in message'
        }
        total_score += relevance_score

        # 3. Value First (20 points)
        value_score = 10  # Base score
        value_offered = pvp_data.get('value_offered', '')
        if len(value_offered) > 30:
            value_score += 5
        if 'insight' in value_offered.lower() or 'help' in full_message.lower():
            value_score += 5
        breakdown['value_first'] = {
            'score': value_score,
            'max': 20,
            'notes': 'Value proposition present in message'
        }
        total_score += value_score

        # 4. Clarity (10 points)
        word_count = len(full_message.split())
        clarity_score = 10 if 50 <= word_count <= 200 else 7
        breakdown['clarity'] = {
            'score': clarity_score,
            'max': 10,
            'notes': f'{word_count} words - {"optimal" if 50 <= word_count <= 200 else "suboptimal"} length'
        }
        total_score += clarity_score

        # 5. Tone (10 points)
        tone_score = 8  # Default good tone
        pushy_words = ['buy', 'purchase', 'discount', 'limited time', 'act now']
        if any(word in full_message.lower() for word in pushy_words):
            tone_score -= 3
        if '?' in full_message:  # Questions indicate conversational tone
            tone_score += 2
        tone_score = max(0, min(tone_score, 10))
        breakdown['tone'] = {
            'score': tone_score,
            'max': 10,
            'notes': 'Conversational tone assessment'
        }
        total_score += tone_score

        # 6. Call to Action (15 points)
        cta = pvp_data.get('call_to_action', '')
        cta_score = 10 if cta else 5
        soft_cta_words = ['thoughts', 'interested', 'curious', 'worth', 'open to']
        if any(word in cta.lower() for word in soft_cta_words):
            cta_score += 5
        breakdown['call_to_action'] = {
            'score': cta_score,
            'max': 15,
            'notes': 'Soft CTA present' if cta_score >= 12 else 'CTA could be softer'
        }
        total_score += cta_score

        # 7. Credibility (10 points)
        credibility_score = 7  # Base credibility
        if match.final_score > 0.7:
            credibility_score += 3  # High match score indicates good fit
        breakdown['credibility'] = {
            'score': credibility_score,
            'max': 10,
            'notes': 'Credibility established through relevance'
        }
        total_score += credibility_score

        return total_score, breakdown

    def get_quality_threshold(self) -> float:
        """Get the minimum quality threshold from settings."""
        return settings.GTM_CONFIG.get('quality_threshold', 8.0) * 10  # Convert to 100-point scale


class ClayWebhookService:
    """
    Service for handling Clay enrichment webhooks.

    Validates webhook signatures and processes enrichment data
    to update Profile records.
    """

    def __init__(self):
        """Initialize the Clay Webhook Service."""
        self.webhook_secret = settings.CLAY_WEBHOOK_SECRET

    def validate_signature(self, payload: bytes, signature: str) -> bool:
        """
        Validate the webhook signature from Clay.

        Args:
            payload: Raw request body bytes
            signature: Signature from X-Clay-Signature header

        Returns:
            True if signature is valid, False otherwise
        """
        if not self.webhook_secret:
            logger.warning("CLAY_WEBHOOK_SECRET not configured - skipping validation")
            return True  # Allow in development if not configured

        if not signature:
            logger.warning("No signature provided in webhook request")
            return False

        # Calculate expected signature
        expected_signature = hmac.new(
            self.webhook_secret.encode('utf-8'),
            payload,
            hashlib.sha256
        ).hexdigest()

        # Compare signatures using constant-time comparison
        return hmac.compare_digest(signature, expected_signature)

    def parse_enrichment_data(self, data: dict) -> dict:
        """
        Parse and normalize enrichment data from Clay webhook.

        Args:
            data: Raw webhook payload

        Returns:
            Normalized enrichment data dictionary
        """
        enrichment = {}

        # Map common Clay fields to our schema
        field_mapping = {
            'linkedin_url': ['linkedin_url', 'linkedinUrl', 'linkedin'],
            'company_name': ['company_name', 'companyName', 'company'],
            'job_title': ['job_title', 'jobTitle', 'title', 'position'],
            'location': ['location', 'city', 'geography'],
            'company_size': ['company_size', 'companySize', 'employees'],
            'industry': ['industry', 'sector'],
            'website': ['website', 'website_url', 'companyWebsite'],
            'twitter_handle': ['twitter', 'twitter_handle', 'twitterHandle'],
            'recent_posts': ['recent_posts', 'recentPosts', 'posts'],
            'bio': ['bio', 'summary', 'about'],
            'skills': ['skills', 'expertise'],
            'connections': ['connections', 'connectionCount'],
            'company_description': ['company_description', 'companyDescription'],
            'funding_stage': ['funding_stage', 'fundingStage', 'funding'],
            'tech_stack': ['tech_stack', 'techStack', 'technologies'],
        }

        for our_field, clay_fields in field_mapping.items():
            for clay_field in clay_fields:
                if clay_field in data and data[clay_field]:
                    enrichment[our_field] = data[clay_field]
                    break

        # Include any additional fields from Clay
        known_fields = set()
        for fields in field_mapping.values():
            known_fields.update(fields)

        for key, value in data.items():
            if key not in known_fields and value:
                enrichment[f'clay_{key}'] = value

        return enrichment

    def update_profile(self, profile_id: int, enrichment_data: dict) -> bool:
        """
        Update a Profile with enrichment data.

        Args:
            profile_id: ID of the profile to update
            enrichment_data: Parsed enrichment data

        Returns:
            True if update successful, False otherwise
        """
        from matching.models import Profile

        try:
            profile = Profile.objects.get(id=profile_id)

            # Merge with existing enrichment data
            existing_data = profile.enrichment_data or {}
            existing_data.update(enrichment_data)
            profile.enrichment_data = existing_data

            # Update source if not already set
            if profile.source == Profile.Source.MANUAL:
                profile.source = Profile.Source.CLAY

            # Update specific fields if they're better than what we have
            if not profile.linkedin_url and enrichment_data.get('linkedin_url'):
                profile.linkedin_url = enrichment_data['linkedin_url']

            if not profile.website_url and enrichment_data.get('website'):
                profile.website_url = enrichment_data['website']

            if not profile.industry and enrichment_data.get('industry'):
                profile.industry = enrichment_data['industry']

            profile.save()

            logger.info(f"Updated profile {profile_id} with Clay enrichment data")
            return True

        except Profile.DoesNotExist:
            logger.error(f"Profile {profile_id} not found for Clay webhook")
            return False
        except Exception as e:
            logger.error(f"Error updating profile {profile_id}: {e}")
            return False

    def process_webhook(self, payload: dict) -> dict:
        """
        Process a complete Clay webhook payload.

        Args:
            payload: Complete webhook payload

        Returns:
            Dictionary with processing results
        """
        results = {
            'processed': 0,
            'failed': 0,
            'errors': []
        }

        # Handle both single record and batch formats
        records = payload.get('records', [payload]) if 'records' in payload else [payload]

        for record in records:
            try:
                # Extract profile identifier
                profile_id = record.get('profile_id') or record.get('id')
                email = record.get('email')

                if not profile_id and email:
                    # Try to find profile by email
                    from matching.models import Profile
                    try:
                        profile = Profile.objects.get(email=email)
                        profile_id = profile.id
                    except Profile.DoesNotExist:
                        results['errors'].append(f"No profile found for email: {email}")
                        results['failed'] += 1
                        continue

                if not profile_id:
                    results['errors'].append("No profile identifier in record")
                    results['failed'] += 1
                    continue

                # Parse and update
                enrichment_data = self.parse_enrichment_data(record)

                if self.update_profile(profile_id, enrichment_data):
                    results['processed'] += 1
                else:
                    results['failed'] += 1

            except Exception as e:
                results['errors'].append(str(e))
                results['failed'] += 1

        return results
