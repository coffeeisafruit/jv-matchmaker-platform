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

try:
    import google.generativeai as genai
    HAS_GEMINI = True
except ImportError:
    HAS_GEMINI = False

try:
    from openai import OpenAI
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False


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


class ICPSuggestionService:
    """
    Service class for generating AI-powered ICP pain point and goal suggestions.

    Uses Gemini API (preferred) or Claude API to generate contextually relevant
    suggestions based on the industry/niche and company size (B2B) or demographics (B2C).
    """

    # B2B Prompts
    B2B_PAIN_POINTS_PROMPT = """You are an expert business consultant. Generate 5 specific, actionable pain points that businesses in the {industry} industry typically face.

Company Size Context: {company_size}

Focus on challenges that are:
1. Specific to this industry (not generic business problems)
2. Relevant to the company size
3. Actionable and addressable
4. Written in plain language (not jargon-heavy)

Respond ONLY with a JSON array of 5 pain point strings, like:
["Pain point 1", "Pain point 2", "Pain point 3", "Pain point 4", "Pain point 5"]

Do not include any other text, just the JSON array."""

    B2B_GOALS_PROMPT = """You are an expert business consultant. Generate 5 specific, achievable goals that businesses in the {industry} industry typically pursue.

Company Size Context: {company_size}

Focus on goals that are:
1. Specific to this industry (not generic business goals)
2. Relevant to the company size
3. Measurable and achievable
4. Written in plain language

Respond ONLY with a JSON array of 5 goal strings, like:
["Goal 1", "Goal 2", "Goal 3", "Goal 4", "Goal 5"]

Do not include any other text, just the JSON array."""

    # B2C Prompts
    B2C_PAIN_POINTS_PROMPT = """You are an expert consumer marketing consultant. Generate 5 specific, relatable pain points that consumers in the {niche} market typically face.

Target Demographic: {demographics}

Focus on challenges that are:
1. Specific to this niche/market (not generic life problems)
2. Relevant to the target demographic
3. Emotionally resonant and relatable
4. Written in conversational language

Respond ONLY with a JSON array of 5 pain point strings, like:
["Pain point 1", "Pain point 2", "Pain point 3", "Pain point 4", "Pain point 5"]

Do not include any other text, just the JSON array."""

    B2C_GOALS_PROMPT = """You are an expert consumer marketing consultant. Generate 5 specific, aspirational goals that consumers in the {niche} market typically pursue.

Target Demographic: {demographics}

Focus on goals that are:
1. Specific to this niche/market (not generic life goals)
2. Relevant to the target demographic
3. Aspirational but achievable
4. Written in conversational language

Respond ONLY with a JSON array of 5 goal strings, like:
["Goal 1", "Goal 2", "Goal 3", "Goal 4", "Goal 5"]

Do not include any other text, just the JSON array."""

    def __init__(self, api_key: Optional[str] = None):
        """Initialize the service with optional API key."""
        # Get API keys - prefer OpenRouter, then Gemini, then Anthropic
        self.openrouter_api_key = getattr(settings, 'OPENROUTER_API_KEY', '') or os.environ.get('OPENROUTER_API_KEY', '')
        self.gemini_api_key = api_key or getattr(settings, 'GEMINI_API_KEY', '') or os.environ.get('GEMINI_API_KEY', '')
        self.anthropic_api_key = getattr(settings, 'ANTHROPIC_API_KEY', '') or os.environ.get('ANTHROPIC_API_KEY', '')

        # Model settings
        self.openrouter_model = getattr(settings, 'AI_CONFIG', {}).get('openrouter_model', 'meta-llama/llama-3.2-3b-instruct:free')
        self.gemini_model = getattr(settings, 'AI_CONFIG', {}).get('gemini_model', 'gemini-2.5-flash')
        self.anthropic_model = getattr(settings, 'AI_CONFIG', {}).get('default_model', 'claude-sonnet-4-20250514')

        # Determine which provider to use (priority: OpenRouter > Gemini > Anthropic)
        self.use_openrouter = HAS_OPENAI and bool(self.openrouter_api_key)
        self.use_gemini = HAS_GEMINI and bool(self.gemini_api_key) and not self.use_openrouter
        self.use_anthropic = HAS_ANTHROPIC and bool(self.anthropic_api_key) and not self.use_openrouter and not self.use_gemini

    def _get_openrouter_client(self):
        """Get the OpenRouter client (OpenAI-compatible)."""
        if not HAS_OPENAI:
            raise ImportError("The 'openai' package is not installed.")
        if not self.openrouter_api_key:
            raise ValueError("No OpenRouter API key configured.")
        return OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=self.openrouter_api_key,
        )

    def _get_gemini_model(self):
        """Get the Gemini model."""
        if not HAS_GEMINI:
            raise ImportError("The 'google-generativeai' package is not installed.")
        if not self.gemini_api_key:
            raise ValueError("No Gemini API key configured.")
        genai.configure(api_key=self.gemini_api_key)
        return genai.GenerativeModel(self.gemini_model)

    def _get_anthropic_client(self):
        """Get the Anthropic client."""
        if not HAS_ANTHROPIC:
            raise ImportError("The 'anthropic' package is not installed.")
        if not self.anthropic_api_key:
            raise ValueError("No Anthropic API key configured.")
        return anthropic.Anthropic(api_key=self.anthropic_api_key)

    def _parse_json_array(self, response_text: str) -> list:
        """Parse a JSON array from the response."""
        try:
            text = response_text.strip()
            if text.startswith('```json'):
                text = text[7:]
            if text.startswith('```'):
                text = text[3:]
            if text.endswith('```'):
                text = text[:-3]
            return json.loads(text.strip())
        except json.JSONDecodeError:
            return []

    def _call_ai(self, prompt: str) -> str:
        """Call the AI provider (OpenRouter > Gemini > Anthropic)."""
        if self.use_openrouter:
            client = self._get_openrouter_client()
            response = client.chat.completions.create(
                model=self.openrouter_model,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.choices[0].message.content
        elif self.use_gemini:
            model = self._get_gemini_model()
            response = model.generate_content(prompt)
            return response.text
        elif self.use_anthropic:
            client = self._get_anthropic_client()
            message = client.messages.create(
                model=self.anthropic_model,
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}]
            )
            return message.content[0].text
        else:
            raise ValueError("No AI provider configured. Set OPENROUTER_API_KEY, GEMINI_API_KEY, or ANTHROPIC_API_KEY.")

    def generate_pain_points(self, industry: str, company_size: str, customer_type: str = 'b2b', age_range: str = '') -> list:
        """Generate AI-powered pain point suggestions."""
        if not industry:
            return []

        if customer_type == 'b2c':
            # B2C: Use consumer-focused prompt
            age_labels = {
                '18-24': '18-24 year olds (Gen Z)',
                '25-34': '25-34 year olds (Millennials)',
                '35-44': '35-44 year olds (Younger Gen X)',
                '45-54': '45-54 year olds (Older Gen X)',
                '55-64': '55-64 year olds (Baby Boomers)',
                '65+': '65+ year olds (Seniors)',
            }
            demographics = age_labels.get(age_range, 'Adults of various ages')

            prompt = self.B2C_PAIN_POINTS_PROMPT.format(
                niche=industry,
                demographics=demographics
            )
        else:
            # B2B: Use business-focused prompt
            size_labels = {
                'solo': 'Solo/Freelancer',
                'small': 'Small Business (1-10 employees)',
                'medium': 'Medium Business (11-50 employees)',
                'large': 'Large Business (51-200 employees)',
                'enterprise': 'Enterprise (200+ employees)',
            }
            size_context = size_labels.get(company_size, 'Small to medium business')

            prompt = self.B2B_PAIN_POINTS_PROMPT.format(
                industry=industry,
                company_size=size_context
            )

        try:
            response_text = self._call_ai(prompt)
            return self._parse_json_array(response_text)
        except Exception:
            return []

    def generate_goals(self, industry: str, company_size: str, customer_type: str = 'b2b', age_range: str = '') -> list:
        """Generate AI-powered goal suggestions."""
        if not industry:
            return []

        if customer_type == 'b2c':
            # B2C: Use consumer-focused prompt
            age_labels = {
                '18-24': '18-24 year olds (Gen Z)',
                '25-34': '25-34 year olds (Millennials)',
                '35-44': '35-44 year olds (Younger Gen X)',
                '45-54': '45-54 year olds (Older Gen X)',
                '55-64': '55-64 year olds (Baby Boomers)',
                '65+': '65+ year olds (Seniors)',
            }
            demographics = age_labels.get(age_range, 'Adults of various ages')

            prompt = self.B2C_GOALS_PROMPT.format(
                niche=industry,
                demographics=demographics
            )
        else:
            # B2B: Use business-focused prompt
            size_labels = {
                'solo': 'Solo/Freelancer',
                'small': 'Small Business (1-10 employees)',
                'medium': 'Medium Business (11-50 employees)',
                'large': 'Large Business (51-200 employees)',
                'enterprise': 'Enterprise (200+ employees)',
            }
            size_context = size_labels.get(company_size, 'Small to medium business')

            prompt = self.B2B_GOALS_PROMPT.format(
                industry=industry,
                company_size=size_context
            )

        try:
            response_text = self._call_ai(prompt)
            return self._parse_json_array(response_text)
        except Exception:
            return []


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
