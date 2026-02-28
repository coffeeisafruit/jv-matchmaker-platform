"""
Admin analysis agent — generates AI suggestions for the monthly admin report.

Replaces the raw Claude API call in admin_notification.py with a
Pydantic AI agent that returns structured suggestions.
"""

from __future__ import annotations

import logging
from typing import Optional

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class AdminSuggestion(BaseModel):
    """A single actionable suggestion for the admin."""
    category: str = Field(description="match_gap, niche_adjustment, client_outreach, cost_optimization, or system_health")
    priority: str = Field(description="high, medium, or low")
    suggestion: str = Field(description="The actionable suggestion text")
    client_name: str = Field(default="", description="Which client this applies to, if specific")
    data_point: str = Field(default="", description="Supporting data point")


class AdminAnalysisOutput(BaseModel):
    """Structured output from the admin analysis agent."""
    suggestions: list[AdminSuggestion] = Field(default_factory=list)
    summary: str = Field(default="", description="2-3 sentence executive summary")
    alerts: list[str] = Field(default_factory=list, description="Urgent items needing attention")


def get_admin_analysis_agent():
    """Create and return the admin analysis Pydantic AI agent."""
    from pydantic_ai import Agent

    return Agent(
        output_type=AdminAnalysisOutput,
        instructions=(
            "You are an AI operations analyst for a JV (Joint Venture) matchmaking platform. "
            "Analyze the monthly processing data and generate actionable suggestions. "
            "Focus on: clients with match gaps (need more 70+ matches), niche adjustments "
            "that could improve scores, clients who haven't confirmed profiles, cost "
            "optimization opportunities, and system health issues. "
            "Be specific — reference actual client names, scores, and numbers. "
            "Prioritize suggestions by impact."
        ),
    )


def analyze_monthly_data(data: dict) -> AdminAnalysisOutput:
    """Run the admin analysis agent on monthly processing data.

    Falls back to an empty result if the agent fails.
    """
    try:
        from matching.enrichment.claude_client import get_pydantic_model

        model = get_pydantic_model()
        if not model:
            logger.warning("No AI model available for admin analysis")
            return AdminAnalysisOutput(summary="AI analysis unavailable — no API key configured.")

        agent = get_admin_analysis_agent()

        prompt = _build_analysis_prompt(data)
        result = agent.run_sync(prompt, model=model)
        return result.output

    except Exception as exc:
        logger.warning("Admin analysis agent failed: %s", exc)
        return AdminAnalysisOutput(
            summary=f"AI analysis failed: {exc}",
            alerts=["Admin analysis agent encountered an error — review data manually."],
        )


def _build_analysis_prompt(data: dict) -> str:
    """Build the analysis prompt from monthly processing data."""
    parts = ["Analyze the following monthly processing data for the JV matchmaker platform:\n"]

    if data.get("processing"):
        parts.append("## Processing Results")
        parts.append(str(data["processing"]))

    if data.get("verifications"):
        parts.append("\n## Client Verification Status")
        parts.append(str(data["verifications"]))

    if data.get("acquisitions"):
        parts.append("\n## Acquisition Results")
        parts.append(str(data["acquisitions"]))

    if data.get("costs"):
        parts.append("\n## Cost Summary")
        parts.append(str(data["costs"]))

    parts.append("\nGenerate actionable suggestions with specific client names and numbers.")
    return "\n".join(parts)
