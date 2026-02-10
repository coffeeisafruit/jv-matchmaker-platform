"""
Configuration settings for OWL-based profile enrichment.

Costs (approximate):
- Claude Haiku: ~$0.00025 per 1K input tokens, $0.00125 per 1K output tokens
- Tavily: ~$0.004 per search (with 1000 free searches/month)

Target: <$0.05 per profile
"""

import os
from dataclasses import dataclass
from typing import Optional

from django.conf import settings as django_settings


@dataclass
class EnrichmentConfig:
    """Configuration for the enrichment pipeline."""

    # LLM Settings - Uses Claude Agent SDK (Claude Code Max subscription)
    # No API key needed - SDK uses your Claude Code CLI authentication
    llm_provider: str = "claude_agent_sdk"  # Uses Claude Code Max subscription
    llm_model: str = "default"  # SDK handles model selection
    llm_max_tokens: int = 2048
    llm_temperature: float = 0.0  # Deterministic for consistency

    # Search Settings
    search_provider: str = "tavily"  # or "serper" or "duckduckgo"
    max_searches_per_profile: int = 4  # Control costs
    search_depth: str = "basic"  # "basic" or "advanced"

    # Batch Processing
    batch_size: int = 10
    delay_between_profiles: float = 1.5  # seconds
    max_retries: int = 2
    retry_delay: float = 5.0  # seconds

    # Cost Tracking
    target_cost_per_profile: float = 0.05  # USD
    max_total_budget: float = 300.0  # USD

    # Output
    output_dir: str = "output"
    save_interval: int = 10  # Save progress every N profiles

    @classmethod
    def from_env(cls) -> "EnrichmentConfig":
        """Create config from environment variables."""
        return cls(
            llm_provider=os.environ.get("ENRICHMENT_LLM_PROVIDER", "openrouter"),
            search_provider=os.environ.get("ENRICHMENT_SEARCH_PROVIDER", "tavily"),
            max_searches_per_profile=int(os.environ.get("ENRICHMENT_MAX_SEARCHES", "4")),
            batch_size=int(os.environ.get("ENRICHMENT_BATCH_SIZE", "10")),
        )


def get_api_key(key_name: str) -> Optional[str]:
    """Get API key from Django settings or environment."""
    # Try Django settings first
    value = getattr(django_settings, key_name, None)
    if value:
        return value

    # Fall back to environment
    return os.environ.get(key_name)


def get_openrouter_key() -> Optional[str]:
    return get_api_key("OPENROUTER_API_KEY")


def get_anthropic_key() -> Optional[str]:
    return get_api_key("ANTHROPIC_API_KEY")


def get_tavily_key() -> Optional[str]:
    return get_api_key("TAVILY_API_KEY")


def get_serper_key() -> Optional[str]:
    return get_api_key("SERPER_API_KEY")


# Cost estimation helpers
COST_ESTIMATES = {
    "claude-3-haiku": {
        "input_per_1k": 0.00025,
        "output_per_1k": 0.00125,
    },
    "claude-3-sonnet": {
        "input_per_1k": 0.003,
        "output_per_1k": 0.015,
    },
    "tavily_basic": 0.004,  # per search
    "tavily_advanced": 0.008,  # per search
    "serper": 0.001,  # per search (with paid plan)
}


def estimate_profile_cost(
    input_tokens: int = 1500,
    output_tokens: int = 800,
    num_searches: int = 4,
    model: str = "claude-3-haiku",
    search_provider: str = "tavily_basic"
) -> float:
    """Estimate cost for enriching one profile."""
    llm_cost = COST_ESTIMATES[model]
    search_cost = COST_ESTIMATES.get(search_provider, 0.004)

    total = (
        (input_tokens / 1000) * llm_cost["input_per_1k"] +
        (output_tokens / 1000) * llm_cost["output_per_1k"] +
        num_searches * search_cost
    )
    return round(total, 5)
