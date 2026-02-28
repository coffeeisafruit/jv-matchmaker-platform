"""
Report intro agent — generates personalized monthly report introductions.

Creates a brief, compelling intro for each client's monthly report
that highlights what changed, new matches found, and key improvements.
"""

from __future__ import annotations

import logging
from typing import Optional

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class ReportIntro(BaseModel):
    """Structured output for a personalized report introduction."""
    greeting: str = Field(description="Personalized greeting line")
    highlights: list[str] = Field(default_factory=list, description="2-4 bullet point highlights")
    body: str = Field(description="2-3 sentence summary paragraph")
    call_to_action: str = Field(default="", description="Suggested next step for the client")


def get_report_intro_agent():
    """Create and return the report intro Pydantic AI agent."""
    from pydantic_ai import Agent

    return Agent(
        output_type=ReportIntro,
        instructions=(
            "You are writing a brief, warm introduction for a client's monthly "
            "JV partner match report. The client is a business owner or thought leader "
            "looking for strategic partnerships. Keep the tone professional but friendly. "
            "Highlight specific improvements: new matches added, score increases, "
            "notable partners found. Be concise — this intro appears at the top of "
            "their report page. Do NOT use generic filler. Reference actual data."
        ),
    )


def generate_intro(
    client_name: str,
    changes: dict,
    top_match_name: str = "",
    top_match_score: float = 0.0,
) -> ReportIntro:
    """Generate a personalized report intro for a client.

    Falls back to a template-based intro if the AI agent fails.
    """
    try:
        from matching.enrichment.claude_client import get_pydantic_model

        model = get_pydantic_model()
        if not model:
            return _template_fallback(client_name, changes)

        agent = get_report_intro_agent()

        prompt = (
            f"Generate a report introduction for {client_name}.\n\n"
            f"Monthly changes:\n"
            f"- New matches added: {changes.get('new_matches', 0)}\n"
            f"- Matches improved: {changes.get('improved_matches', 0)}\n"
            f"- Top match: {top_match_name} (score: {top_match_score:.0f})\n"
            f"- Total matches above 70: {changes.get('total_above_70', 0)}\n"
            f"- Profile was {'updated' if changes.get('profile_updated') else 'unchanged'} this month\n"
        )

        result = agent.run_sync(prompt, model=model)
        return result.output

    except Exception as exc:
        logger.warning("Report intro agent failed for %s: %s", client_name, exc)
        return _template_fallback(client_name, changes)


def _template_fallback(client_name: str, changes: dict) -> ReportIntro:
    """Template-based fallback when AI is unavailable."""
    first_name = client_name.split()[0] if client_name else "there"
    new_matches = changes.get("new_matches", 0)
    total = changes.get("total_above_70", 0)

    highlights = []
    if new_matches:
        highlights.append(f"{new_matches} new high-quality match{'es' if new_matches != 1 else ''} discovered")
    if total:
        highlights.append(f"{total} total matches scoring 70+")
    if changes.get("profile_updated"):
        highlights.append("Your profile updates have been applied to matching")

    return ReportIntro(
        greeting=f"Hi {first_name},",
        highlights=highlights or ["Your monthly matches have been refreshed"],
        body=(
            f"Your updated match report is ready with {total} partners "
            f"scoring 70 or above on our compatibility index."
        ),
        call_to_action="Review your top matches and reach out to start a conversation.",
    )
