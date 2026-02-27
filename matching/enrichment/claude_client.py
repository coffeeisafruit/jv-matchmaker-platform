"""
Shared Claude API client.

Consolidates duplicated API key detection, Claude API calls (OpenRouter
preferred, Anthropic fallback), and JSON response parsing from:

- ProfileResearchService (ai_research.py)
- ClaudeVerificationService (ai_verification.py)
"""

import json
import logging
import os
from typing import Dict, Optional

from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

logger = logging.getLogger(__name__)


def _is_retryable(exc: BaseException) -> bool:
    """Return True for transient API errors worth retrying."""
    retryable_names = {
        'RateLimitError', 'APIConnectionError', 'APITimeoutError',
        'InternalServerError', 'APIStatusError',
        'ConnectionError', 'Timeout',
    }
    for cls in type(exc).__mro__:
        if cls.__name__ in retryable_names:
            return True
    if hasattr(exc, 'status_code') and exc.status_code in (429, 500, 502, 503, 504):
        return True
    return False


class ClaudeClient:
    """Shared Claude API client. OpenRouter preferred, Anthropic fallback."""

    def __init__(
        self,
        max_tokens: int = 2048,
        openrouter_key: str = None,
        anthropic_key: str = None,
    ):
        # Auto-detect keys from Django settings / environment if not provided
        if openrouter_key is None and anthropic_key is None:
            try:
                from django.conf import settings
                openrouter_key = (
                    getattr(settings, 'OPENROUTER_API_KEY', '') or
                    os.environ.get('OPENROUTER_API_KEY', '')
                )
                anthropic_key = (
                    getattr(settings, 'ANTHROPIC_API_KEY', '') or
                    os.environ.get('ANTHROPIC_API_KEY', '')
                )
            except Exception:
                openrouter_key = os.environ.get('OPENROUTER_API_KEY', '')
                anthropic_key = os.environ.get('ANTHROPIC_API_KEY', '')

        if openrouter_key:
            self.use_openrouter = True
            self.api_key = openrouter_key
            self.model = "anthropic/claude-sonnet-4"
        elif anthropic_key:
            self.use_openrouter = False
            self.api_key = anthropic_key
            self.model = "claude-sonnet-4-20250514"
        else:
            self.use_openrouter = False
            self.api_key = None
            self.model = None

        self.max_tokens = max_tokens

    def is_available(self) -> bool:
        """Check if the client has a configured API key."""
        return self.api_key is not None

    def call(self, prompt: str) -> Optional[str]:
        """Call Claude via OpenRouter or Anthropic API. Returns raw response text."""
        if not self.api_key:
            return None

        try:
            return self._call_api(prompt)
        except ImportError as e:
            logger.warning(f"Required package not installed: {e}")
            return None
        except Exception as e:
            logger.error(f"AI API call failed after retries: {e}")
            return None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception(_is_retryable),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _call_api(self, prompt: str) -> str:
        """Execute API call with tenacity retry on transient errors."""
        if self.use_openrouter:
            import openai

            client = openai.OpenAI(
                base_url="https://openrouter.ai/api/v1",
                api_key=self.api_key,
            )

            response = client.chat.completions.create(
                model=self.model,
                max_tokens=self.max_tokens,
                temperature=0,
                messages=[{"role": "user", "content": prompt}]
            )

            return response.choices[0].message.content
        else:
            import anthropic

            client = anthropic.Anthropic(api_key=self.api_key)

            message = client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                temperature=0,
                messages=[{"role": "user", "content": prompt}]
            )

            return message.content[0].text

    @staticmethod
    def parse_json(response: str) -> Optional[Dict]:
        """Parse a JSON response from Claude, handling markdown code blocks."""
        if not response:
            return None

        try:
            text = response.strip()
            if "```json" in text:
                start = text.find("```json") + 7
                end = text.find("```", start)
                text = text[start:end].strip()
            elif "```" in text:
                start = text.find("```") + 3
                end = text.find("```", start)
                text = text[start:end].strip()

            return json.loads(text)
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Failed to parse JSON response: {e}")
            return None
