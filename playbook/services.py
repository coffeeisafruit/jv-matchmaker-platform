"""
PlaybookGeneratorService - Generate customized playbook content using AI.

This service takes a GeneratedPlaybook and its associated TransformationAnalysis
to create fully customized, AI-generated content for each play in the playbook.
"""

import os
from datetime import date, timedelta
from typing import Dict, List, Optional
import json

from django.conf import settings
from django.utils import timezone

from .models import GeneratedPlaybook, GeneratedPlay, LaunchPlay


class PlaybookGeneratorService:
    """Generate customized playbook content using AI."""

    def __init__(self, playbook: GeneratedPlaybook):
        """
        Initialize the service with a GeneratedPlaybook instance.

        Args:
            playbook: The GeneratedPlaybook to generate content for
        """
        self.playbook = playbook
        self.transformation = playbook.transformation
        self.api_key = self._get_api_key()

    def _get_api_key(self) -> Optional[str]:
        """Get the first available AI API key from settings."""
        # Priority order: OpenRouter (multi-model), Gemini (fast), Anthropic, OpenAI
        api_keys = [
            ('OPENROUTER_API_KEY', os.environ.get('OPENROUTER_API_KEY')),
            ('GEMINI_API_KEY', os.environ.get('GEMINI_API_KEY')),
            ('ANTHROPIC_API_KEY', os.environ.get('ANTHROPIC_API_KEY')),
            ('OPENAI_API_KEY', os.environ.get('OPENAI_API_KEY')),
        ]

        for key_name, key_value in api_keys:
            if key_value:
                return key_value

        return None

    def _build_context(self) -> Dict:
        """Build context dictionary from playbook and transformation data."""
        context = {
            'playbook_name': self.playbook.name,
            'playbook_size': self.playbook.get_size_display(),
        }

        if self.transformation:
            context.update({
                'before_state': self.transformation.before_state,
                'after_state': self.transformation.after_state,
                'transformation_summary': self.transformation.transformation_summary,
                'key_obstacles': self.transformation.key_obstacles,
                'value_drivers': self.transformation.value_drivers,
            })

            # Include ICP data if available
            if self.transformation.icp:
                icp = self.transformation.icp
                context.update({
                    'target_audience': icp.name,
                    'industry': icp.industry,
                    'pain_points': icp.pain_points,
                    'goals': icp.goals,
                })

        return context

    def _generate_with_ai(self, prompt: str) -> Dict:
        """
        Call AI API to generate customized content.

        Args:
            prompt: The prompt to send to the AI

        Returns:
            Dict with keys: custom_content, custom_hook, custom_cta
        """
        if not self.api_key:
            # Return placeholder if no API key available
            return {
                'custom_content': '[AI content would be generated here with API key]',
                'custom_hook': '[AI-generated hook]',
                'custom_cta': '[AI-generated CTA]',
            }

        # TODO: Implement actual AI API calls
        # For now, return structured placeholder
        return {
            'custom_content': f'[Generated content based on: {prompt[:100]}...]',
            'custom_hook': '[Compelling hook based on transformation]',
            'custom_cta': '[Call to action aligned with phase]',
        }

    def _build_play_prompt(self, launch_play: LaunchPlay, context: Dict) -> str:
        """
        Build the AI prompt for a specific play.

        Args:
            launch_play: The LaunchPlay template to customize
            context: Business/transformation context

        Returns:
            Formatted prompt string
        """
        prompt = f"""You are an expert launch content strategist. Generate customized content for this play:

PLAY DETAILS:
- Play #{launch_play.play_number}: {launch_play.name}
- Phase: {launch_play.get_phase_display()}
- Purpose: {launch_play.purpose}
- Psychology: {launch_play.psychology}

BUSINESS CONTEXT:
- Playbook: {context.get('playbook_name', 'N/A')}
"""

        if 'target_audience' in context:
            prompt += f"- Target Audience: {context['target_audience']}\n"

        if 'industry' in context:
            prompt += f"- Industry: {context['industry']}\n"

        if 'transformation_summary' in context:
            prompt += f"\nTRANSFORMATION:\n{context['transformation_summary']}\n"

        if 'before_state' in context:
            prompt += f"\nBEFORE STATE:\n{context['before_state']}\n"

        if 'after_state' in context:
            prompt += f"\nAFTER STATE:\n{context['after_state']}\n"

        if 'pain_points' in context and context['pain_points']:
            prompt += f"\nPAIN POINTS:\n" + "\n".join(f"- {p}" for p in context['pain_points']) + "\n"

        if 'value_drivers' in context and context['value_drivers']:
            prompt += f"\nVALUE DRIVERS:\n" + "\n".join(f"- {v}" for v in context['value_drivers']) + "\n"

        prompt += f"""
PLAY TEMPLATE:
- Content Concept: {launch_play.content_concept}
- AHA Moment: {launch_play.aha_moment}
- Vibe/Authority: {launch_play.vibe_authority}
- Soft CTA: {launch_play.soft_cta}
- Hook Inspirations: {', '.join(launch_play.hook_inspirations[:3]) if launch_play.hook_inspirations else 'None'}

TASK:
Generate customized content for this play that:
1. Aligns with the business context and transformation
2. Addresses the target audience's pain points
3. Follows the psychology principle: {launch_play.psychology}
4. Achieves the purpose: {launch_play.purpose}

OUTPUT FORMAT (JSON):
{{
  "custom_content": "Full customized content for this play (2-4 paragraphs)",
  "custom_hook": "Compelling subject line or opening hook (max 100 chars)",
  "custom_cta": "Clear call to action for this phase (1-2 sentences)"
}}

Generate content that feels authentic, specific to this business, and actionable.
Avoid generic language. Use the transformation context to make it personal.
"""

        return prompt

    def generate_play_content(self, generated_play: GeneratedPlay) -> Dict:
        """
        Generate AI-customized content for a single play.

        Args:
            generated_play: The GeneratedPlay instance to populate

        Returns:
            Dict with custom_content, custom_hook, custom_cta
        """
        context = self._build_context()
        prompt = self._build_play_prompt(generated_play.launch_play, context)

        # Generate content using AI
        result = self._generate_with_ai(prompt)

        # Update the generated_play instance
        generated_play.custom_content = result['custom_content']
        generated_play.custom_hook = result['custom_hook']
        generated_play.custom_cta = result['custom_cta']
        generated_play.save()

        return result

    def generate_full_playbook(self) -> List[GeneratedPlay]:
        """
        Generate content for all plays in the playbook.

        Returns:
            List of GeneratedPlay instances with customized content
        """
        # Determine which plays to include based on playbook size
        size_filter = {
            'small': {'included_in_small': True},
            'medium': {'included_in_medium': True},
            'large': {'included_in_large': True},  # All plays
        }

        filter_kwargs = size_filter.get(self.playbook.size, {'included_in_large': True})
        launch_plays = LaunchPlay.objects.filter(**filter_kwargs).order_by('play_number')

        generated_plays = []

        for launch_play in launch_plays:
            # Create GeneratedPlay instance
            generated_play = GeneratedPlay.objects.create(
                playbook=self.playbook,
                launch_play=launch_play,
            )

            # Generate customized content
            self.generate_play_content(generated_play)
            generated_plays.append(generated_play)

        return generated_plays

    def calculate_schedule(self, start_date: date) -> Dict[int, date]:
        """
        Calculate scheduled dates for each play based on phase and playbook size.

        Phase durations (for 'large' size):
        - Pre-launch: 40-75 days (spread 23 plays)
        - Launch Announcement: Days 1-2 (2 plays)
        - Launch Nurture: Days 2-10 (15 plays)
        - Launch Urgency: Days 10-13 (7 plays)
        - Post-Launch Buyers: Days 14-20 (5 plays)
        - Post-Launch Non-Buyers: Days 14-20 (3 plays)

        Args:
            start_date: The launch announcement date (not pre-launch start)

        Returns:
            Dict mapping play_number to scheduled date
        """
        schedule = {}

        # Get all plays for this playbook size
        size_filter = {
            'small': {'included_in_small': True},
            'medium': {'included_in_medium': True},
            'large': {'included_in_large': True},
        }
        filter_kwargs = size_filter.get(self.playbook.size, {'included_in_large': True})
        plays = LaunchPlay.objects.filter(**filter_kwargs).order_by('play_number')

        # Group plays by phase
        phases = {
            'pre_launch': [],
            'launch_announcement': [],
            'launch_nurture': [],
            'launch_urgency': [],
            'post_buyers': [],
            'post_non_buyers': [],
        }

        for play in plays:
            phases[play.phase].append(play)

        # Calculate dates for each phase
        current_date = start_date

        # Pre-launch: Work backwards from launch date
        pre_launch_plays = phases['pre_launch']
        if pre_launch_plays:
            # Spread evenly across 40-75 days before launch
            pre_launch_duration = 60  # Default to 60 days for medium/large
            if self.playbook.size == 'small':
                pre_launch_duration = 21  # ~3 weeks

            days_between = pre_launch_duration // len(pre_launch_plays)
            pre_launch_start = start_date - timedelta(days=pre_launch_duration)

            for i, play in enumerate(pre_launch_plays):
                schedule[play.play_number] = pre_launch_start + timedelta(days=i * days_between)

        # Launch Announcement: Days 1-2
        for i, play in enumerate(phases['launch_announcement']):
            schedule[play.play_number] = start_date + timedelta(days=i)
            current_date = start_date + timedelta(days=i + 1)

        # Launch Nurture: Days 2-10 (spread evenly)
        nurture_plays = phases['launch_nurture']
        if nurture_plays:
            nurture_duration = 8  # Days 2-10
            days_between = max(1, nurture_duration // len(nurture_plays))

            for i, play in enumerate(nurture_plays):
                schedule[play.play_number] = current_date + timedelta(days=i * days_between)

            current_date = current_date + timedelta(days=nurture_duration)

        # Launch Urgency: Days 10-13 (final push)
        urgency_plays = phases['launch_urgency']
        if urgency_plays:
            urgency_duration = 3
            days_between = max(1, urgency_duration // len(urgency_plays))

            for i, play in enumerate(urgency_plays):
                schedule[play.play_number] = current_date + timedelta(days=i * days_between)

            current_date = current_date + timedelta(days=urgency_duration)

        # Post-Launch Buyers: Days 14-20
        buyers_plays = phases['post_buyers']
        if buyers_plays:
            post_duration = 7
            days_between = max(1, post_duration // len(buyers_plays))

            for i, play in enumerate(buyers_plays):
                schedule[play.play_number] = current_date + timedelta(days=i * days_between)

        # Post-Launch Non-Buyers: Days 14-20 (parallel with buyers)
        non_buyers_plays = phases['post_non_buyers']
        if non_buyers_plays:
            post_duration = 7
            days_between = max(1, post_duration // len(non_buyers_plays))

            for i, play in enumerate(non_buyers_plays):
                schedule[play.play_number] = current_date + timedelta(days=i * days_between)

        return schedule

    def apply_schedule(self, start_date: date) -> None:
        """
        Calculate and apply schedule to all GeneratedPlay instances in this playbook.

        Args:
            start_date: The launch announcement date
        """
        schedule = self.calculate_schedule(start_date)

        for generated_play in self.playbook.plays.all():
            play_number = generated_play.launch_play.play_number
            if play_number in schedule:
                generated_play.scheduled_date = schedule[play_number]
                generated_play.save()
