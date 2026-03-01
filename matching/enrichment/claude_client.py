"""
Shared Claude API client and Pydantic AI agents.

Provides:
  - ClaudeClient: raw API calls with tenacity retry (backward compat)
  - get_pydantic_model(): returns a Pydantic AI model for agent usage
  - Pre-configured agents: research_agent, extended_signals_agent,
    formatting_verifier, content_verifier, data_quality_verifier
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

from matching.enrichment.cost_guard import (
    get_cost_guard,
    get_circuit_breaker,
    BudgetExceededError,
    CircuitOpenError,
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
            get_circuit_breaker().check("openrouter")
            get_cost_guard().check_budget("claude_ai", estimated_cost=0.015)
        except (BudgetExceededError, CircuitOpenError) as e:
            logger.error(f"API call blocked: {e}")
            raise

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
                    temperature=0,
                    messages=[{"role": "user", "content": prompt}]
                )

                result = response.choices[0].message.content
            else:
                import anthropic

                client = anthropic.Anthropic(api_key=self.api_key)

                message = client.messages.create(
                    model=self.model,
                    max_tokens=self.max_tokens,
                    temperature=0,
                    messages=[{"role": "user", "content": prompt}]
                )

                result = message.content[0].text

            get_circuit_breaker().record_success("openrouter")
            return result
        except Exception as e:
            if _is_retryable(e):
                get_circuit_breaker().record_failure("openrouter")
            raise

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


# ────────────────────────────────────────────────────────────────
# Pydantic AI model resolver + pre-configured agents
# ────────────────────────────────────────────────────────────────

def get_pydantic_model():
    """Return a Pydantic AI model configured with available API keys.

    Prefers OpenRouter (via OpenAI provider) over direct Anthropic.
    Returns None if no API key is available.
    """
    openrouter_key = anthropic_key = ""
    try:
        from django.conf import settings
        openrouter_key = getattr(settings, 'OPENROUTER_API_KEY', '') or os.environ.get('OPENROUTER_API_KEY', '')
        anthropic_key = getattr(settings, 'ANTHROPIC_API_KEY', '') or os.environ.get('ANTHROPIC_API_KEY', '')
    except Exception:
        openrouter_key = os.environ.get('OPENROUTER_API_KEY', '')
        anthropic_key = os.environ.get('ANTHROPIC_API_KEY', '')

    if openrouter_key:
        from pydantic_ai.providers.openai import OpenAIProvider
        from pydantic_ai.models.openai import OpenAIModel
        provider = OpenAIProvider(
            base_url="https://openrouter.ai/api/v1",
            api_key=openrouter_key,
        )
        return OpenAIModel("anthropic/claude-sonnet-4", provider=provider)
    elif anthropic_key:
        from pydantic_ai.models.anthropic import AnthropicModel
        return AnthropicModel("claude-sonnet-4-20250514", api_key=anthropic_key)
    else:
        return None


def get_model_for_tier(tier: int = 3) -> str:
    """Return the appropriate Claude model name based on profile tier.

    Tier routing:
        0-1: Haiku (fast, cheap — auto-fill and entry-level profiles)
        2-3: Sonnet (default — standard enrichment)
        4-5: Opus (premium — high-value profiles needing deeper reasoning)

    Returns the model string suitable for the current API provider
    (OpenRouter format or direct Anthropic format).
    """
    openrouter_key = ""
    try:
        from django.conf import settings
        openrouter_key = getattr(settings, 'OPENROUTER_API_KEY', '') or os.environ.get('OPENROUTER_API_KEY', '')
    except Exception:
        openrouter_key = os.environ.get('OPENROUTER_API_KEY', '')

    use_openrouter = bool(openrouter_key)

    if tier <= 1:
        return "anthropic/claude-haiku-3-5" if use_openrouter else "claude-3-5-haiku-20241022"
    elif tier <= 3:
        return "anthropic/claude-sonnet-4" if use_openrouter else "claude-sonnet-4-20250514"
    else:
        return "anthropic/claude-opus-4" if use_openrouter else "claude-opus-4-20250514"


def get_pydantic_model_for_tier(tier: int = 3):
    """Return a Pydantic AI model configured for the given profile tier.

    Uses get_model_for_tier() to select the model, then wraps it
    in the appropriate Pydantic AI provider.
    """
    model_name = get_model_for_tier(tier)

    openrouter_key = anthropic_key = ""
    try:
        from django.conf import settings
        openrouter_key = getattr(settings, 'OPENROUTER_API_KEY', '') or os.environ.get('OPENROUTER_API_KEY', '')
        anthropic_key = getattr(settings, 'ANTHROPIC_API_KEY', '') or os.environ.get('ANTHROPIC_API_KEY', '')
    except Exception:
        openrouter_key = os.environ.get('OPENROUTER_API_KEY', '')
        anthropic_key = os.environ.get('ANTHROPIC_API_KEY', '')

    if openrouter_key:
        from pydantic_ai.providers.openai import OpenAIProvider
        from pydantic_ai.models.openai import OpenAIModel
        provider = OpenAIProvider(
            base_url="https://openrouter.ai/api/v1",
            api_key=openrouter_key,
        )
        return OpenAIModel(model_name, provider=provider)
    elif anthropic_key:
        from pydantic_ai.models.anthropic import AnthropicModel
        return AnthropicModel(model_name, api_key=anthropic_key)
    else:
        return None


# --- Lazy-loaded Pydantic AI agents ---
#
# Agent instantiation is deferred so that ``from .claude_client import ClaudeClient``
# works even when pydantic-ai is not installed (e.g. in test environments that only
# need the raw HTTP client).

_research_agent = None
_extended_signals_agent = None
_formatting_verifier = None
_content_verifier = None
_data_quality_verifier = None


def _get_agent_class():
    """Import and return pydantic_ai.Agent (raises ImportError if missing)."""
    from pydantic_ai import Agent
    return Agent


def _get_schemas():
    """Import output schemas for agents."""
    from matching.enrichment.schemas import (
        CoreProfileExtraction,
        ExtendedSignalsExtraction,
        AIVerificationResult,
    )
    return CoreProfileExtraction, ExtendedSignalsExtraction, AIVerificationResult


def _make_research_agent():
    Agent = _get_agent_class()
    CoreProfileExtraction, _, _ = _get_schemas()
    return Agent(
        output_type=CoreProfileExtraction,
        instructions=(
            "You are a business research assistant extracting FACTUAL profile data. "
            "Only extract information that is EXPLICITLY stated on the website. "
            "DO NOT make assumptions or infer anything. If information is not clearly "
            "stated, leave that field empty. Business accuracy matters — do NOT "
            "fabricate or assume. Set confidence to 'high' only if you found clear, "
            "explicit statements. Include source_quotes with 1-2 direct quotes."
        ),
    )


def _make_extended_signals_agent():
    Agent = _get_agent_class()
    _, ExtendedSignalsExtraction, _ = _get_schemas()
    return Agent(
        output_type=ExtendedSignalsExtraction,
        instructions=(
            "You are a business intelligence analyst extracting PARTNERSHIP and "
            "REVENUE signals. Only extract information that is EXPLICITLY stated "
            "or clearly demonstrated. Do NOT fabricate partnerships, prices, or "
            "platform names. Revenue tier should be based on evidence, not "
            "assumptions. For jv_history, only include partnerships you can cite "
            "from the content."
        ),
    )


def _make_formatting_verifier():
    Agent = _get_agent_class()
    _, _, AIVerificationResult = _get_schemas()
    return Agent(
        output_type=AIVerificationResult,
        instructions=(
            "You are a content formatting quality checker. Evaluate text for: "
            "complete sentences, clear structure, readability, appropriate length "
            "(max 450 chars), and whether it describes benefits for both parties. "
            "Score 0-100 based on formatting quality."
        ),
    )


def _make_content_verifier():
    Agent = _get_agent_class()
    _, _, AIVerificationResult = _get_schemas()
    return Agent(
        output_type=AIVerificationResult,
        instructions=(
            "You are a content quality evaluator. Check text for: personalization, "
            "use of actual data (not generic phrases), specific benefits mentioned, "
            "and whether the content is compelling and accurate. Score 0-100."
        ),
    )


def _make_data_quality_verifier():
    Agent = _get_agent_class()
    _, _, AIVerificationResult = _get_schemas()
    return Agent(
        output_type=AIVerificationResult,
        instructions=(
            "You are a data quality checker for business profiles. Verify: email "
            "format validity, website vs LinkedIn distinction, contact info quality, "
            "no placeholder values, and that data is in the correct fields. "
            "Score 0-100 based on data integrity."
        ),
    )


def __getattr__(name: str):
    """Module-level __getattr__ for lazy agent instantiation.

    Allows ``from .claude_client import research_agent`` to work without
    importing pydantic_ai at module load time.
    """
    global _research_agent, _extended_signals_agent
    global _formatting_verifier, _content_verifier, _data_quality_verifier

    if name == "research_agent":
        if _research_agent is None:
            _research_agent = _make_research_agent()
        return _research_agent
    if name == "extended_signals_agent":
        if _extended_signals_agent is None:
            _extended_signals_agent = _make_extended_signals_agent()
        return _extended_signals_agent
    if name == "formatting_verifier":
        if _formatting_verifier is None:
            _formatting_verifier = _make_formatting_verifier()
        return _formatting_verifier
    if name == "content_verifier":
        if _content_verifier is None:
            _content_verifier = _make_content_verifier()
        return _content_verifier
    if name == "data_quality_verifier":
        if _data_quality_verifier is None:
            _data_quality_verifier = _make_data_quality_verifier()
        return _data_quality_verifier
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
