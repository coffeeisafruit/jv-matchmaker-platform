"""
GEX 4-Email Sequence Generator Service.

Implements the GEX methodology for creating personalized 4-email outreach sequences
with various pattern types (Lookalike, Trigger-Based, Creative Ideas, Poke the Bear, Super Short).
"""

import logging
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

from django.conf import settings

from .models import OutreachSequence, OutreachEmail

logger = logging.getLogger(__name__)


@dataclass
class EmailContent:
    """Content for a single email in the sequence."""
    email_number: int
    is_threaded: bool
    subject_line: str
    body: str
    personalization_notes: str


class SequenceGeneratorService:
    """Generate 4-email outreach sequences using AI."""

    # Sequence templates by type
    SEQUENCE_TEMPLATES = {
        'lookalike': 'Saw you\'re in {industry} like our partner {case_study}...',
        'trigger': '{trigger_line}. {ai_personalization}. If we could help {outcome}...',
        'creative_ideas': 'Saw how you {mission}. Had 3 ideas: {idea1}, {idea2}, {idea3}...',
        'poke_bear': 'How do you know {current_solution} is {optimal_outcome}?',
        'super_short': 'Are you like other {title} who keep telling me they\'re {problem}?',
    }

    # Email structure guidelines
    EMAIL_GUIDELINES = {
        1: {
            'type': 'net_new',
            'is_threaded': False,
            'purpose': 'Why you, why now + compelling offer',
            'structure': 'Hook → Value Prop → Social Proof → Clear CTA',
            'max_words': 150
        },
        2: {
            'type': 'threaded',
            'is_threaded': True,
            'purpose': 'More context, case study, deeper value',
            'structure': 'Reminder → Case Study → Specific Results → Soft CTA',
            'max_words': 200
        },
        3: {
            'type': 'net_new',
            'is_threaded': False,
            'purpose': 'Different value prop, lower friction CTA',
            'structure': 'New Angle → Alternative Value → Lower Ask → Easy Next Step',
            'max_words': 120
        },
        4: {
            'type': 'threaded',
            'is_threaded': True,
            'purpose': 'Hail mary / "right person?" check',
            'structure': 'Acknowledge Silence → Right Person Check → Final Value → Graceful Exit',
            'max_words': 100
        }
    }

    def __init__(self, sequence: OutreachSequence):
        """
        Initialize the sequence generator.

        Args:
            sequence: OutreachSequence model instance
        """
        self.sequence = sequence
        self.target = sequence.target_profile
        self.match = sequence.match
        self.user = sequence.user

        # API configuration
        self.api_key = settings.OPENROUTER_API_KEY or settings.ANTHROPIC_API_KEY
        self.model = settings.AI_CONFIG.get('default_model', 'google/gemma-2-9b-it:free')
        self.max_tokens = settings.AI_CONFIG.get('max_tokens', 4096)
        self.temperature = settings.AI_CONFIG.get('temperature', 0.7)
        self.use_openrouter = bool(settings.OPENROUTER_API_KEY)

    def generate_emails(self) -> List[OutreachEmail]:
        """
        Generate all 4 emails for the sequence.

        Returns:
            List of OutreachEmail instances (not yet saved to database)
        """
        emails = []

        # Extract trigger data first if sequence is trigger-based
        if self.sequence.sequence_type == OutreachSequence.SequenceType.TRIGGER:
            trigger_data = self._get_trigger_data()
            self.sequence.trigger_data = trigger_data
            self.sequence.save(update_fields=['trigger_data'])

        # Generate each email
        for email_number in range(1, 5):
            email_content = self._generate_email(email_number)

            # Create OutreachEmail instance
            email = OutreachEmail(
                sequence=self.sequence,
                email_number=email_number,
                is_threaded=email_content.is_threaded,
                subject_line=email_content.subject_line,
                body=email_content.body
            )

            emails.append(email)

            # Log personalization notes
            logger.info(
                f"Generated Email {email_number} for sequence {self.sequence.id}: "
                f"{email_content.personalization_notes}"
            )

        return emails

    def _generate_email(self, email_number: int) -> EmailContent:
        """
        Generate a single email using AI.

        Args:
            email_number: Position in sequence (1-4)

        Returns:
            EmailContent with subject, body, and metadata
        """
        guidelines = self.EMAIL_GUIDELINES[email_number]

        # Build the AI prompt
        prompt = self._build_email_prompt(email_number, guidelines)

        # Call AI API
        response = self._call_ai(prompt)

        # Parse response
        email_content = self._parse_email_response(response, email_number, guidelines)

        return email_content

    def _build_email_prompt(self, email_number: int, guidelines: Dict[str, Any]) -> str:
        """
        Build the AI prompt for generating a specific email.

        Args:
            email_number: Position in sequence (1-4)
            guidelines: Email guidelines from EMAIL_GUIDELINES

        Returns:
            Formatted prompt string
        """
        # Base context about target
        target_context = self._build_target_context()

        # Sequence-specific template
        sequence_template = self.SEQUENCE_TEMPLATES.get(
            self.sequence.sequence_type,
            self.SEQUENCE_TEMPLATES['lookalike']
        )

        # Previous emails context (for threaded emails)
        previous_emails = ""
        if email_number > 1:
            previous_emails = self._build_previous_emails_context(email_number)

        prompt = f"""You are an expert email copywriter specializing in the GEX methodology for B2B outreach.

## Your Task
Generate Email #{email_number} in a 4-email sequence for a potential JV partnership opportunity.

## Email Guidelines
- Type: {guidelines['type'].upper()} ({guidelines['purpose']})
- Structure: {guidelines['structure']}
- Is Threaded: {'Yes' if guidelines['is_threaded'] else 'No'}
- Maximum Length: {guidelines['max_words']} words

## Sequence Type
{self.sequence.get_sequence_type_display()} - {sequence_template}

## Target Profile
{target_context}

{previous_emails}

## Instructions
1. Write a compelling email following the exact structure outlined above
2. Personalize heavily based on the target's profile data
3. Keep it under {guidelines['max_words']} words
4. Use conversational, human tone (avoid corporate speak)
5. For threaded emails: reference the previous email naturally
6. Make the value proposition crystal clear
7. End with an appropriate CTA based on email position

## Output Format
Provide your response in JSON format:
{{
    "subject_line": "The email subject line (or empty string if threaded)",
    "body": "The complete email body",
    "personalization_elements": ["element1", "element2", "element3"],
    "reasoning": "Brief explanation of your approach"
}}

IMPORTANT: For threaded emails (Email #2 and #4), the subject_line should be an empty string "" since they reply to the previous thread.
"""

        return prompt

    def _build_target_context(self) -> str:
        """Build context about the target profile."""
        if not self.target:
            return "Target profile not available"

        context = f"""
**Name:** {self.target.name}
**Company:** {self.target.company or 'Unknown'}
**Industry/Niche:** {self.target.niche or 'Unknown'}
**What They Do:** {self.target.what_you_do or 'Not specified'}
**Who They Serve:** {self.target.who_you_serve or 'Not specified'}
**What They're Seeking:** {self.target.seeking or 'Not specified'}
**What They're Offering:** {self.target.offering or 'Not specified'}
**List Size:** {self.target.list_size or 0:,}
**Social Reach:** {self.target.social_reach or 0:,}
**Bio:** {self.target.bio or 'Not available'}
"""

        # Add match data if available
        if self.match:
            context += f"""
**Match Score (Harmonic Mean):** {float(self.match.harmonic_mean or 0) * 100:.0f}%
**Your→Them Score:** {float(self.match.score_ab or 0) * 100:.0f}%
**Them→You Score:** {float(self.match.score_ba or 0) * 100:.0f}%
**Match Analysis:** {self.match.rich_analysis or 'Not available'}
"""

        # Add trigger data if available
        if self.sequence.trigger_data:
            context += "\n**Trigger Signals:**\n"
            for key, value in self.sequence.trigger_data.items():
                context += f"- {key}: {value}\n"

        return context

    def _build_previous_emails_context(self, current_email_number: int) -> str:
        """Build context of previous emails in the sequence."""
        previous_emails = self.sequence.emails.filter(
            email_number__lt=current_email_number
        ).order_by('email_number')

        if not previous_emails.exists():
            return ""

        context = "\n## Previous Emails in This Sequence\n"
        for email in previous_emails:
            context += f"\n**Email #{email.email_number}:**\n"
            if email.subject_line:
                context += f"Subject: {email.subject_line}\n"
            context += f"Body:\n{email.body}\n"
            context += f"---\n"

        return context

    def _get_trigger_data(self) -> Dict[str, Any]:
        """
        Extract trigger signals from profile for trigger-based sequences.

        Returns:
            Dictionary of trigger signals and their values
        """
        triggers = {}

        if not self.target:
            return triggers

        # Recent activity triggers
        if self.target.last_active_at:
            days_since_active = (datetime.now().date() - self.target.last_active_at.date()).days
            if days_since_active <= 7:
                triggers['recent_activity'] = f"Active {days_since_active} days ago"

        # Profile update triggers
        if self.target.profile_updated_at:
            days_since_update = (datetime.now().date() - self.target.profile_updated_at.date()).days
            if days_since_update <= 30:
                triggers['profile_update'] = f"Updated profile {days_since_update} days ago"

        # Growth triggers (if we have historical data)
        if self.target.list_size and self.target.list_size > 10000:
            triggers['audience_size'] = f"Large audience ({self.target.list_size:,})"

        # Content triggers
        if self.target.current_projects:
            triggers['current_projects'] = self.target.current_projects

        # Intent triggers from seeking/offering
        if self.target.seeking:
            triggers['seeking'] = self.target.seeking

        if self.target.offering:
            triggers['offering'] = self.target.offering

        return triggers

    def _call_ai(self, prompt: str) -> str:
        """Call the AI API with the prompt."""
        try:
            if self.use_openrouter:
                import openai

                client = openai.OpenAI(
                    base_url="https://openrouter.ai/api/v1",
                    api_key=self.api_key,
                )

                response = client.chat.completions.create(
                    model=self.model,
                    max_tokens=self.max_tokens,
                    temperature=self.temperature,
                    messages=[
                        {"role": "user", "content": prompt}
                    ]
                )

                return response.choices[0].message.content
            else:
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

        except ImportError as e:
            logger.error(f"Required package not installed: {e}")
            raise ImportError("Please install required packages: pip install openai anthropic")
        except Exception as e:
            logger.error(f"Error calling AI API: {e}")
            raise

    def _parse_email_response(
        self,
        response: str,
        email_number: int,
        guidelines: Dict[str, Any]
    ) -> EmailContent:
        """Parse the AI response into EmailContent."""
        import json

        try:
            # Try to extract JSON from the response
            if "```json" in response:
                start = response.find("```json") + 7
                end = response.find("```", start)
                response = response[start:end].strip()
            elif "```" in response:
                start = response.find("```") + 3
                end = response.find("```", start)
                response = response[start:end].strip()

            data = json.loads(response)

            subject_line = data.get('subject_line', '')
            body = data.get('body', '')
            personalization_elements = data.get('personalization_elements', [])
            reasoning = data.get('reasoning', '')

            # For threaded emails, ensure no subject line
            if guidelines['is_threaded']:
                subject_line = ''

            # Generate fallback subject for non-threaded emails if missing
            if not guidelines['is_threaded'] and not subject_line:
                subject_line = self._generate_fallback_subject(email_number)

            return EmailContent(
                email_number=email_number,
                is_threaded=guidelines['is_threaded'],
                subject_line=subject_line,
                body=body,
                personalization_notes=f"Elements: {', '.join(personalization_elements)}. {reasoning}"
            )

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse AI response as JSON: {e}")

            # Return a fallback email
            return EmailContent(
                email_number=email_number,
                is_threaded=guidelines['is_threaded'],
                subject_line=self._generate_fallback_subject(email_number) if not guidelines['is_threaded'] else '',
                body=response,  # Use raw response as body
                personalization_notes="Failed to parse AI response, using raw output"
            )

    def _generate_fallback_subject(self, email_number: int) -> str:
        """Generate a fallback subject line."""
        company = self.target.company if self.target else "your company"
        name = self.target.name.split()[0] if self.target and self.target.name else "there"

        fallback_subjects = {
            1: f"Quick idea for {company}",
            3: f"{name} - different approach",
        }

        return fallback_subjects.get(email_number, f"Following up - {company}")

    def save_emails(self, emails: List[OutreachEmail]) -> None:
        """
        Save generated emails to the database.

        Args:
            emails: List of OutreachEmail instances to save
        """
        OutreachEmail.objects.bulk_create(emails)
        logger.info(f"Saved {len(emails)} emails for sequence {self.sequence.id}")

    def schedule_emails(
        self,
        start_date: Optional[datetime] = None,
        days_between: int = 3
    ) -> None:
        """
        Schedule emails for sending.

        Args:
            start_date: When to start the sequence (default: tomorrow)
            days_between: Days between each email (default: 3)
        """
        if start_date is None:
            start_date = datetime.now() + timedelta(days=1)

        emails = self.sequence.emails.all().order_by('email_number')

        for i, email in enumerate(emails):
            scheduled_for = start_date + timedelta(days=i * days_between)
            email.scheduled_for = scheduled_for
            email.save(update_fields=['scheduled_for'])

        logger.info(f"Scheduled {emails.count()} emails starting {start_date}")


def create_sequence_from_match(
    user,
    match,
    sequence_type: str = OutreachSequence.SequenceType.LOOKALIKE,
    generate_now: bool = True
) -> OutreachSequence:
    """
    Convenience function to create a sequence from a match.

    Args:
        user: User creating the sequence
        match: SupabaseMatch instance
        sequence_type: Type of sequence to create
        generate_now: Whether to generate emails immediately

    Returns:
        OutreachSequence instance with emails (if generate_now=True)
    """
    target_profile = match.get_suggested_profile()

    sequence = OutreachSequence.objects.create(
        user=user,
        match=match,
        target_profile=target_profile,
        name=f"{target_profile.name} - {sequence_type}",
        sequence_type=sequence_type,
        status=OutreachSequence.Status.DRAFT
    )

    if generate_now:
        generator = SequenceGeneratorService(sequence)
        emails = generator.generate_emails()
        generator.save_emails(emails)

        sequence.status = OutreachSequence.Status.READY
        sequence.save(update_fields=['status'])

    return sequence
