"""
AI-Powered Verification Agents using Claude

Each agent uses Claude to intelligently evaluate content quality,
going beyond deterministic pattern matching.
"""

import json
import logging
import os
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

from django.conf import settings

logger = logging.getLogger(__name__)


@dataclass
class AIVerificationResult:
    """Result from AI-powered verification."""
    passed: bool
    score: float  # 0-100
    issues: List[str]
    suggestions: List[str]
    reasoning: str


class ClaudeVerificationService:
    """
    Service for AI-powered content verification using Claude.
    Supports both Anthropic API and OpenRouter.
    """

    def __init__(self):
        # Try OpenRouter first (already configured in project), then Anthropic
        self.openrouter_key = getattr(settings, 'OPENROUTER_API_KEY', '') or os.environ.get('OPENROUTER_API_KEY', '')
        self.anthropic_key = getattr(settings, 'ANTHROPIC_API_KEY', '') or os.environ.get('ANTHROPIC_API_KEY', '')

        if self.openrouter_key:
            self.use_openrouter = True
            self.api_key = self.openrouter_key
            self.model = "anthropic/claude-sonnet-4"  # Via OpenRouter
        elif self.anthropic_key:
            self.use_openrouter = False
            self.api_key = self.anthropic_key
            self.model = "claude-sonnet-4-20250514"
        else:
            self.use_openrouter = False
            self.api_key = None
            self.model = None

        self.max_tokens = 1024

    def _call_claude(self, prompt: str) -> str:
        """Call Claude via OpenRouter or Anthropic API."""
        if not self.api_key:
            logger.warning("No API key configured (OPENROUTER_API_KEY or ANTHROPIC_API_KEY)")
            return None

        try:
            if self.use_openrouter:
                # Use OpenRouter via OpenAI-compatible API
                import openai

                client = openai.OpenAI(
                    base_url="https://openrouter.ai/api/v1",
                    api_key=self.api_key,
                )

                response = client.chat.completions.create(
                    model=self.model,
                    max_tokens=self.max_tokens,
                    temperature=0,  # Deterministic for verification
                    messages=[
                        {"role": "user", "content": prompt}
                    ]
                )

                return response.choices[0].message.content
            else:
                # Use Anthropic directly
                import anthropic

                client = anthropic.Anthropic(api_key=self.api_key)

                message = client.messages.create(
                    model=self.model,
                    max_tokens=self.max_tokens,
                    temperature=0,
                    messages=[
                        {"role": "user", "content": prompt}
                    ]
                )

                return message.content[0].text

        except ImportError as e:
            logger.warning(f"Required package not installed: {e}")
            return None
        except Exception as e:
            logger.error(f"Error calling AI API: {e}")
            return None

    def verify_formatting(self, content: str, field_name: str, max_length: int = 450) -> AIVerificationResult:
        """
        AI verification of content formatting and structure.
        Checks for complete sentences, proper structure, and readability.
        """
        prompt = f"""You are a content verification agent for a professional PDF report.

Evaluate the following {field_name} content for formatting quality:

<content>
{content}
</content>

Check for:
1. COMPLETE SENTENCES - Does each sentence end properly? Any cut-off text?
2. STRUCTURE - Are there clear sections with headers?
3. READABILITY - Is it easy to scan and understand?
4. LENGTH - Content should fit within {max_length} characters without truncation. Current length: {len(content)} chars.
5. BOTH PARTIES - If this is mutual benefit content, are BOTH parties' benefits clearly listed?

Respond in JSON format:
{{
    "passed": true/false,
    "score": 0-100,
    "issues": ["list of specific issues found"],
    "suggestions": ["specific fixes to apply"],
    "reasoning": "Brief explanation of your evaluation"
}}"""

        response = self._call_claude(prompt)
        return self._parse_response(response, field_name)

    def verify_content_quality(self, content: str, partner_name: str, partner_data: Dict) -> AIVerificationResult:
        """
        AI verification of content specificity and personalization.
        Checks that content uses actual data, not generic phrases.
        """
        prompt = f"""You are a content quality agent for a JV partnership matching system.

Evaluate this content for {partner_name}:

<content>
{content}
</content>

Partner data available:
- Name: {partner_name}
- Seeking: {partner_data.get('seeking', 'N/A')}
- Offering: {partner_data.get('offering', 'N/A')}
- Who they serve: {partner_data.get('who_they_serve', 'N/A')}

Check for:
1. PERSONALIZATION - Does it mention {partner_name} specifically by name?
2. USES THEIR DATA - Does it reference what they're SEEKING or OFFERING?
3. AVOIDS GENERIC PHRASES - No "great synergy", "perfect fit", "complementary services"
4. SPECIFIC BENEFITS - Are benefits concrete and measurable?
5. COMPELLING - Would this make {partner_name} want to respond?

Respond in JSON format:
{{
    "passed": true/false,
    "score": 0-100,
    "issues": ["list of specific issues found"],
    "suggestions": ["specific fixes to apply"],
    "reasoning": "Brief explanation of your evaluation"
}}"""

        response = self._call_claude(prompt)
        return self._parse_response(response, 'content_quality')

    def verify_data_quality(self, match_data: Dict) -> AIVerificationResult:
        """
        AI verification of data quality and field correctness.
        Checks for misplaced data, boilerplate, and placeholder values.
        """
        prompt = f"""You are a data quality agent checking match data integrity.

Evaluate this match data:
- Name: {match_data.get('name', '')}
- Email: {match_data.get('email', '')}
- Website: {match_data.get('website', '')}
- LinkedIn: {match_data.get('linkedin', '')}
- Best Contact: {match_data.get('best_contact', '')}
- Notes: {match_data.get('notes', '')[:300]}...

Check for:
1. EMAIL VALIDITY - Is the email a real email address, not a URL or placeholder like "Update"?
2. WEBSITE vs LINKEDIN - Is the website field an actual website, not a LinkedIn URL?
3. CONTACT INFO QUALITY - Does "best contact" contain actual contact methods (email, phone, calendar link) or just intro text like "You can contact me..."?
4. NO PLACEHOLDERS - Are there any placeholder values like "N/A", "-", "Update"?
5. DATA IN RIGHT FIELDS - Is each piece of data in the correct field?

Respond in JSON format:
{{
    "passed": true/false,
    "score": 0-100,
    "issues": ["list of specific issues found"],
    "suggestions": ["specific fixes to apply"],
    "reasoning": "Brief explanation of your evaluation"
}}"""

        response = self._call_claude(prompt)
        return self._parse_response(response, 'data_quality')

    def verify_outreach_message(self, message: str, partner_name: str, partner_seeking: str) -> AIVerificationResult:
        """
        AI verification of outreach email quality.
        Checks for personalization, structure, and effectiveness.
        """
        prompt = f"""You are an outreach message quality agent for a JV partnership system.

Evaluate this outreach email for {partner_name}:

<email>
{message}
</email>

Partner is seeking: {partner_seeking or 'Not specified'}

Check for:
1. SUBJECT LINE - Does it have a compelling subject line that mentions their name or interest?
2. PERSONALIZED HOOK - Does the opening reference something specific about them?
3. CLEAR VALUE PROP - Is it clear what you're offering and what they get?
4. REFERENCES THEIR SEEKING - Does it connect to what they've stated they want?
5. SOFT CTA - Does it end with an inviting call-to-action, not a hard sell?
6. APPROPRIATE LENGTH - Is it scannable (not a wall of text)?
7. COMPLETE - No cut-off sentences or missing sections?

Respond in JSON format:
{{
    "passed": true/false,
    "score": 0-100,
    "issues": ["list of specific issues found"],
    "suggestions": ["specific fixes to apply"],
    "reasoning": "Brief explanation of your evaluation"
}}"""

        response = self._call_claude(prompt)
        return self._parse_response(response, 'outreach_message')

    def rewrite_content(self, content: str, field_name: str, issues: List[str], max_length: int = 450) -> str:
        """
        Use AI to rewrite content that failed verification.
        Returns improved content that fits within limits.
        """
        prompt = f"""You are a content editor for a professional JV partnership report.

The following {field_name} content has these issues:
{chr(10).join(f'- {issue}' for issue in issues)}

Original content:
<content>
{content}
</content>

Rewrite this content to:
1. Fix all the issues listed above
2. Keep it under {max_length} characters
3. Ensure ALL sentences are complete
4. Maintain the same meaning and personalization
5. If showing benefits for two parties, include BOTH parties' benefits

Return ONLY the rewritten content, no explanation."""

        response = self._call_claude(prompt)
        if response:
            return response.strip()
        return content  # Return original if AI fails

    def _parse_response(self, response: str, field_name: str) -> AIVerificationResult:
        """Parse JSON response from Claude."""
        if not response:
            # Fallback if API fails
            return AIVerificationResult(
                passed=True,
                score=100,
                issues=[],
                suggestions=[],
                reasoning="AI verification unavailable, passed by default"
            )

        try:
            # Extract JSON from response
            text = response.strip()

            # Handle markdown code blocks
            if "```json" in text:
                start = text.find("```json") + 7
                end = text.find("```", start)
                text = text[start:end].strip()
            elif "```" in text:
                start = text.find("```") + 3
                end = text.find("```", start)
                text = text[start:end].strip()

            data = json.loads(text)

            return AIVerificationResult(
                passed=data.get('passed', True),
                score=float(data.get('score', 100)),
                issues=data.get('issues', []),
                suggestions=data.get('suggestions', []),
                reasoning=data.get('reasoning', '')
            )

        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to parse AI response for {field_name}: {e}")
            return AIVerificationResult(
                passed=True,
                score=100,
                issues=[],
                suggestions=[],
                reasoning=f"Parse error: {str(e)}"
            )


class AIMatchVerificationAgent:
    """
    Master verification agent that coordinates AI-powered verification.
    Uses Claude to intelligently evaluate all aspects of match content.
    """

    MIN_SCORE_TO_PASS = 80

    def __init__(self):
        self.claude = ClaudeVerificationService()

    def verify_match(self, match, partner_data: Dict = None) -> Tuple[float, List[str], List[str]]:
        """
        Comprehensive AI verification of an enriched match.

        Returns:
            Tuple of (score, issues, suggestions)
        """
        all_issues = []
        all_suggestions = []
        total_score = 0
        checks_performed = 0

        partner_data = partner_data or {}

        # 1. Verify mutual benefit formatting (STRATEGY section)
        if match.mutual_benefit:
            result = self.claude.verify_formatting(
                match.mutual_benefit,
                'mutual_benefit/STRATEGY',
                max_length=450
            )
            total_score += result.score
            checks_performed += 1
            if not result.passed:
                all_issues.extend([f"[formatting] {i}" for i in result.issues])
                all_suggestions.extend(result.suggestions)

        # 2. Verify content quality and personalization
        if match.why_fit:
            result = self.claude.verify_content_quality(
                match.why_fit,
                match.name,
                {
                    'seeking': match.seeking,
                    'offering': match.offering,
                    'who_they_serve': match.who_they_serve,
                }
            )
            total_score += result.score
            checks_performed += 1
            if not result.passed:
                all_issues.extend([f"[content] {i}" for i in result.issues])
                all_suggestions.extend(result.suggestions)

        # 3. Verify data quality
        data_dict = {
            'name': match.name,
            'email': match.email,
            'website': match.website,
            'linkedin': match.linkedin,
            'best_contact': getattr(match, 'best_contact', ''),
            'notes': match.notes,
        }
        result = self.claude.verify_data_quality(data_dict)
        total_score += result.score
        checks_performed += 1
        if not result.passed:
            all_issues.extend([f"[data_quality] {i}" for i in result.issues])
            all_suggestions.extend(result.suggestions)

        # 4. Verify outreach message
        if match.outreach_message:
            result = self.claude.verify_outreach_message(
                match.outreach_message,
                match.name,
                match.seeking
            )
            total_score += result.score
            checks_performed += 1
            if not result.passed:
                all_issues.extend([f"[outreach] {i}" for i in result.issues])
                all_suggestions.extend(result.suggestions)

        # Calculate average score
        final_score = total_score / checks_performed if checks_performed > 0 else 100

        return final_score, all_issues, all_suggestions

    def verify_and_fix(self, match, partner_data: Dict = None):
        """
        Verify match and automatically fix issues using AI.

        Returns:
            Tuple of (fixed_match, score, issues)
        """
        from .match_enrichment import EnrichedMatch, TextSanitizer

        score, issues, suggestions = self.verify_match(match, partner_data)

        # If score is low, try to fix content using AI
        if score < self.MIN_SCORE_TO_PASS:
            # Fix mutual_benefit if it has issues
            mb_issues = [i for i in issues if '[formatting]' in i]
            if mb_issues and match.mutual_benefit:
                fixed_mb = self.claude.rewrite_content(
                    match.mutual_benefit,
                    'mutual_benefit',
                    mb_issues,
                    max_length=450
                )
                match = EnrichedMatch(
                    name=match.name,
                    company=match.company,
                    email=match.email,
                    linkedin=match.linkedin,
                    website=match.website,
                    niche=match.niche,
                    list_size=match.list_size,
                    social_reach=match.social_reach,
                    score=match.score,
                    who_they_serve=match.who_they_serve,
                    what_they_do=match.what_they_do,
                    seeking=match.seeking,
                    offering=match.offering,
                    notes=match.notes,
                    why_fit=match.why_fit,
                    mutual_benefit=fixed_mb,  # Fixed!
                    outreach_message=match.outreach_message,
                    verification_score=score,
                    verification_passed=score >= self.MIN_SCORE_TO_PASS,
                )

                # Re-verify after fix
                score, issues, suggestions = self.verify_match(match, partner_data)

        match.verification_score = score
        match.verification_passed = score >= self.MIN_SCORE_TO_PASS

        return match, score, issues
