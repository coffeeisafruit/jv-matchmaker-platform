"""
Services for the Positioning app.

This module contains the TransformationService class that handles
AI-powered transformation analysis using the Claude API.
"""

import json
import os
from typing import Optional
from django.conf import settings

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False


class TransformationService:
    """
    Service class for generating AI-powered transformation analysis.

    Takes an ICP and before/after states, then uses Claude API to generate
    a comprehensive transformation analysis including obstacles and value drivers.
    """

    # Sample prompt template for transformation analysis
    ANALYSIS_PROMPT = """You are an expert business strategist specializing in customer transformation journeys.

Analyze the following customer transformation scenario and provide strategic insights.

{icp_context}

## Before State (Current Situation)
{before_state}

## After State (Desired Outcome)
{after_state}

Please provide a comprehensive analysis in the following JSON format:
{{
    "transformation_summary": "A 2-3 sentence summary of the transformation journey",
    "key_obstacles": [
        {{
            "obstacle": "Name of the obstacle",
            "description": "Detailed description of the obstacle",
            "severity": "high|medium|low",
            "mitigation": "How to address this obstacle"
        }}
    ],
    "value_drivers": [
        {{
            "driver": "Name of the value driver",
            "description": "Why this creates value for the customer",
            "impact": "high|medium|low",
            "messaging_angle": "How to communicate this value in marketing"
        }}
    ],
    "recommended_positioning": "A recommended positioning statement based on this transformation",
    "emotional_journey": {{
        "before_feelings": ["list", "of", "feelings"],
        "after_feelings": ["list", "of", "feelings"]
    }}
}}

Provide at least 3 key obstacles and 3 value drivers. Be specific and actionable.
Respond ONLY with the JSON object, no additional text."""

    ICP_CONTEXT_TEMPLATE = """## Ideal Customer Profile
- Name: {name}
- Industry: {industry}
- Company Size: {company_size}
- Pain Points: {pain_points}
- Goals: {goals}
- Budget Range: {budget_range}
"""

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the TransformationService.

        Args:
            api_key: Optional API key. If not provided, will use settings.ANTHROPIC_API_KEY
        """
        self.api_key = api_key or getattr(settings, 'ANTHROPIC_API_KEY', '') or os.environ.get('ANTHROPIC_API_KEY', '')
        self.model = getattr(settings, 'AI_CONFIG', {}).get('default_model', 'claude-sonnet-4-20250514')
        self.max_tokens = getattr(settings, 'AI_CONFIG', {}).get('max_tokens', 4096)

    def _get_client(self):
        """Get the Anthropic client."""
        if not HAS_ANTHROPIC:
            raise ImportError(
                "The 'anthropic' package is not installed. "
                "Please install it with: pip install anthropic"
            )

        if not self.api_key:
            raise ValueError(
                "No Anthropic API key configured. "
                "Set ANTHROPIC_API_KEY in environment or settings."
            )

        return anthropic.Anthropic(api_key=self.api_key)

    def _build_icp_context(self, icp) -> str:
        """Build the ICP context string for the prompt."""
        if not icp:
            return "## Ideal Customer Profile\nNo specific ICP provided."

        pain_points = ', '.join(icp.pain_points) if icp.pain_points else 'Not specified'
        goals = ', '.join(icp.goals) if icp.goals else 'Not specified'

        return self.ICP_CONTEXT_TEMPLATE.format(
            name=icp.name,
            industry=icp.industry,
            company_size=icp.get_company_size_display(),
            pain_points=pain_points,
            goals=goals,
            budget_range=icp.budget_range or 'Not specified',
        )

    def _build_prompt(self, icp, before_state: str, after_state: str) -> str:
        """Build the full prompt for the AI."""
        icp_context = self._build_icp_context(icp)

        return self.ANALYSIS_PROMPT.format(
            icp_context=icp_context,
            before_state=before_state,
            after_state=after_state,
        )

    def _parse_response(self, response_text: str) -> dict:
        """Parse the AI response into a dictionary."""
        try:
            # Try to extract JSON from the response
            # Handle potential markdown code blocks
            text = response_text.strip()
            if text.startswith('```json'):
                text = text[7:]
            if text.startswith('```'):
                text = text[3:]
            if text.endswith('```'):
                text = text[:-3]

            return json.loads(text.strip())
        except json.JSONDecodeError as e:
            # Return a default structure if parsing fails
            return {
                'transformation_summary': response_text[:500],
                'key_obstacles': [
                    {
                        'obstacle': 'Analysis parsing error',
                        'description': f'Could not parse AI response: {str(e)}',
                        'severity': 'medium',
                        'mitigation': 'Please try regenerating the analysis'
                    }
                ],
                'value_drivers': [
                    {
                        'driver': 'Raw analysis available',
                        'description': 'The AI provided analysis but in an unexpected format',
                        'impact': 'medium',
                        'messaging_angle': 'Review the raw response for insights'
                    }
                ],
                'raw_response': response_text,
            }

    def analyze(self, icp, before_state: str, after_state: str) -> dict:
        """
        Perform transformation analysis using Claude API.

        Args:
            icp: The ICP model instance (can be None)
            before_state: Description of the customer's current state
            after_state: Description of the customer's desired end state

        Returns:
            dict: Analysis results including transformation_summary,
                  key_obstacles, and value_drivers
        """
        prompt = self._build_prompt(icp, before_state, after_state)

        try:
            client = self._get_client()

            message = client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            )

            # Extract text from response
            response_text = message.content[0].text
            return self._parse_response(response_text)

        except ImportError as e:
            return self._get_mock_analysis(before_state, after_state, str(e))
        except ValueError as e:
            return self._get_mock_analysis(before_state, after_state, str(e))
        except Exception as e:
            # For any other errors, return mock data with error info
            return self._get_mock_analysis(before_state, after_state, str(e))

    def _get_mock_analysis(self, before_state: str, after_state: str, error_msg: str = '') -> dict:
        """
        Generate mock analysis data for testing or when API is unavailable.

        This allows the system to function without an API key configured.
        """
        return {
            'transformation_summary': (
                f"This transformation takes customers from their current challenges "
                f"({before_state[:50]}...) to their desired outcomes ({after_state[:50]}...). "
                f"The journey involves overcoming key obstacles and leveraging specific value drivers."
            ),
            'key_obstacles': [
                {
                    'obstacle': 'Resistance to Change',
                    'description': 'Customers may be hesitant to adopt new processes or tools',
                    'severity': 'high',
                    'mitigation': 'Provide clear ROI metrics and case studies'
                },
                {
                    'obstacle': 'Resource Constraints',
                    'description': 'Limited time, budget, or personnel to implement changes',
                    'severity': 'medium',
                    'mitigation': 'Offer phased implementation and clear quick wins'
                },
                {
                    'obstacle': 'Knowledge Gaps',
                    'description': 'Customers may lack expertise to fully leverage the solution',
                    'severity': 'medium',
                    'mitigation': 'Provide comprehensive onboarding and training resources'
                }
            ],
            'value_drivers': [
                {
                    'driver': 'Time Savings',
                    'description': 'Automation and efficiency improvements free up valuable time',
                    'impact': 'high',
                    'messaging_angle': 'Get back X hours per week to focus on strategic work'
                },
                {
                    'driver': 'Cost Reduction',
                    'description': 'Eliminate redundant processes and reduce operational costs',
                    'impact': 'high',
                    'messaging_angle': 'Reduce operational costs by up to X%'
                },
                {
                    'driver': 'Competitive Advantage',
                    'description': 'Stay ahead of competitors with modern capabilities',
                    'impact': 'medium',
                    'messaging_angle': 'Join industry leaders who have already transformed'
                }
            ],
            'recommended_positioning': (
                'We help [target customers] overcome [key obstacle] '
                'to achieve [primary value driver], resulting in [measurable outcome].'
            ),
            'emotional_journey': {
                'before_feelings': ['frustrated', 'overwhelmed', 'uncertain'],
                'after_feelings': ['confident', 'empowered', 'in control']
            },
            '_mock_data': True,
            '_api_error': error_msg or 'Using mock data (no API key configured)',
        }


class PositioningService:
    """
    Service class for generating positioning recommendations.

    Uses transformation analysis and ICP data to generate
    compelling positioning statements and messaging.
    """

    def generate_positioning_statement(self, icp, transformation_analysis) -> dict:
        """
        Generate a positioning statement based on ICP and transformation analysis.

        Args:
            icp: The ICP model instance
            transformation_analysis: TransformationAnalysis model instance

        Returns:
            dict: Positioning recommendations
        """
        # Extract key data
        pain_points = icp.pain_points if icp else []
        goals = icp.goals if icp else []
        obstacles = transformation_analysis.key_obstacles if transformation_analysis else []
        value_drivers = transformation_analysis.value_drivers if transformation_analysis else []

        # Build positioning components
        positioning = {
            'target_audience': self._build_target_audience(icp),
            'pain_statement': self._build_pain_statement(pain_points, obstacles),
            'value_proposition': self._build_value_proposition(goals, value_drivers),
            'differentiation': self._build_differentiation(value_drivers),
            'full_statement': '',
        }

        # Combine into full positioning statement
        positioning['full_statement'] = (
            f"For {positioning['target_audience']} who {positioning['pain_statement']}, "
            f"our solution {positioning['value_proposition']} "
            f"unlike alternatives that {positioning['differentiation']}."
        )

        return positioning

    def _build_target_audience(self, icp) -> str:
        """Build target audience description."""
        if not icp:
            return "businesses looking for transformation"

        size_display = dict(icp.COMPANY_SIZE_CHOICES).get(icp.company_size, icp.company_size)
        return f"{size_display} companies in {icp.industry}"

    def _build_pain_statement(self, pain_points: list, obstacles: list) -> str:
        """Build pain statement from pain points and obstacles."""
        all_pains = pain_points[:2] if pain_points else []
        if obstacles and len(all_pains) < 2:
            all_pains.extend([o.get('obstacle', '') for o in obstacles[:2-len(all_pains)]])

        if not all_pains:
            return "face significant challenges"

        return f"struggle with {' and '.join(all_pains[:2]).lower()}"

    def _build_value_proposition(self, goals: list, value_drivers: list) -> str:
        """Build value proposition from goals and value drivers."""
        benefits = []
        if goals:
            benefits.extend(goals[:2])
        if value_drivers and len(benefits) < 2:
            benefits.extend([v.get('driver', '') for v in value_drivers[:2-len(benefits)]])

        if not benefits:
            return "delivers transformative results"

        return f"enables {' and '.join(benefits[:2]).lower()}"

    def _build_differentiation(self, value_drivers: list) -> str:
        """Build differentiation statement."""
        if not value_drivers:
            return "fail to address the full transformation journey"

        high_impact = [v for v in value_drivers if v.get('impact') == 'high']
        if high_impact:
            return f"overlook {high_impact[0].get('driver', 'key value drivers').lower()}"

        return "provide generic, one-size-fits-all solutions"
