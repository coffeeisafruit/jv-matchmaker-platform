"""
Discovery agent -- intelligent tool selection for JV prospect research.

Given partial information about a prospect (name, website, niche, etc.),
the agent reasons about which research tools to use and in what order,
then returns consolidated discovery results.
"""

from __future__ import annotations

import logging
from typing import Optional

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Output schemas
# ---------------------------------------------------------------------------

class DiscoveryPlan(BaseModel):
    """The agent's plan for which tools to use."""
    tools_selected: list[str] = Field(
        description="Tools chosen: exa_search, exa_similar, apollo, serper, duckduckgo",
    )
    reasoning: str = Field(description="Why these tools were selected")


class ProspectProfile(BaseModel):
    """Consolidated discovery result for a single prospect."""
    name: str = Field(default="")
    company: str = Field(default="")
    website: str = Field(default="")
    linkedin: str = Field(default="")
    email: str = Field(default="")
    niche: str = Field(default="")
    what_they_do: str = Field(default="")
    who_they_serve: str = Field(default="")
    seeking: str = Field(default="")
    offering: str = Field(default="")
    confidence: str = Field(default="low", description="low/medium/high")
    source_tools: list[str] = Field(
        default_factory=list, description="Which tools provided data",
    )


class DiscoveryResult(BaseModel):
    """Output of the discovery agent."""
    plan: DiscoveryPlan
    prospects: list[ProspectProfile] = Field(default_factory=list)
    total_cost: float = Field(default=0.0, description="Total API cost in dollars")
    tools_used: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Agent factory
# ---------------------------------------------------------------------------

_DISCOVERY_INSTRUCTIONS = (
    "You are a JV prospect discovery agent. Given partial information about "
    "a niche, seed names, or seed URLs, decide which research tools to use "
    "and return a structured DiscoveryResult.\n\n"
    "## Tool selection rules\n"
    "- Has website → use exa_search (Exa extraction) first\n"
    "- Name only → use exa_search (Exa discovery search), then apollo\n"
    "- Has niche + seed URLs → use exa_similar (Exa find-similar)\n"
    "- Need email → use apollo (Apollo search_people)\n"
    "- Fallback → use duckduckgo (free) before paid tools\n\n"
    "## Cost awareness\n"
    "- DuckDuckGo: FREE ($0.00/query)\n"
    "- Serper: $0.001/query\n"
    "- Exa: ~$0.02/call\n"
    "- Apollo: ~$0.05/call\n\n"
    "Always try the cheapest tool first. Minimize cost.\n\n"
    "## Output\n"
    "Return a DiscoveryResult with:\n"
    "- plan: your DiscoveryPlan with tools_selected and reasoning\n"
    "- prospects: consolidated ProspectProfile list from the research\n"
    "- total_cost: estimated total API cost\n"
    "- tools_used: which tools were actually called\n"
    "- errors: any errors encountered\n\n"
    "Be specific in your reasoning about WHY you chose each tool."
)


def get_discovery_agent():
    """Return a Pydantic AI Agent configured for prospect discovery."""
    from pydantic_ai import Agent

    return Agent(
        output_type=DiscoveryResult,
        instructions=_DISCOVERY_INSTRUCTIONS,
    )


# ---------------------------------------------------------------------------
# Tool orchestration helpers
# ---------------------------------------------------------------------------

def _run_duckduckgo(query: str, max_results: int) -> tuple[list[dict], list[str]]:
    """Run DuckDuckGo search. Returns (results, errors)."""
    try:
        from matching.enrichment.search_tools import DuckDuckGoSearchService

        svc = DuckDuckGoSearchService()
        if not svc.available:
            return [], ["DuckDuckGo: package not installed"]
        hits = svc.search(query, max_results=max_results)
        return [
            {"title": h.title, "url": h.url, "snippet": h.snippet, "source": "duckduckgo"}
            for h in hits
        ], []
    except Exception as exc:
        return [], [f"DuckDuckGo error: {exc}"]


def _run_serper(query: str, max_results: int) -> tuple[list[dict], float, list[str]]:
    """Run Serper search. Returns (results, cost, errors)."""
    try:
        from matching.enrichment.search_tools import SerperSearchService

        svc = SerperSearchService()
        if not svc.available:
            return [], 0.0, ["Serper: API key not configured"]
        hits = svc.search(query, max_results=max_results)
        cost = sum(h.cost for h in hits)
        return [
            {"title": h.title, "url": h.url, "snippet": h.snippet, "source": "serper"}
            for h in hits
        ], cost, []
    except Exception as exc:
        return [], 0.0, [f"Serper error: {exc}"]


def _run_exa_search(query: str, max_results: int) -> tuple[list[dict], float, list[str]]:
    """Run Exa discovery search. Returns (results, cost, errors)."""
    try:
        from matching.enrichment.exa_research import ExaResearchService

        svc = ExaResearchService()
        if not svc.available:
            return [], 0.0, ["Exa: API key not configured"]
        result = svc.client.search(
            query=query,
            type="auto",
            num_results=min(max_results, 10),
            contents=False,
        )
        cost = result.cost_dollars.total if result.cost_dollars else 0.0
        hits = []
        for r in result.results:
            hits.append({
                "title": r.title or "",
                "url": r.url or "",
                "snippet": "",
                "source": "exa_search",
            })
        return hits, cost, []
    except Exception as exc:
        return [], 0.0, [f"Exa search error: {exc}"]


def _run_exa_similar(seed_urls: list[str], max_results: int) -> tuple[list[dict], float, list[str]]:
    """Run Exa find-similar from seed URLs. Returns (results, cost, errors)."""
    try:
        from matching.enrichment.exa_research import ExaResearchService

        svc = ExaResearchService()
        if not svc.available:
            return [], 0.0, ["Exa: API key not configured"]
        result = svc.client.find_similar(
            url=seed_urls[0],
            num_results=min(max_results, 10),
            contents=False,
        )
        cost = result.cost_dollars.total if result.cost_dollars else 0.0
        hits = []
        for r in result.results:
            hits.append({
                "title": r.title or "",
                "url": r.url or "",
                "snippet": "",
                "source": "exa_similar",
            })
        return hits, cost, []
    except Exception as exc:
        return [], 0.0, [f"Exa similar error: {exc}"]


def _run_apollo(niche: str, max_results: int) -> tuple[list[dict], float, list[str]]:
    """Run Apollo people search. Returns (results, cost, errors)."""
    try:
        from matching.enrichment.apollo_enrichment import ApolloEnrichmentService

        svc = ApolloEnrichmentService()
        if not svc.api_key:
            return [], 0.0, ["Apollo: API key not configured"]
        prospects = svc.search_people(title=niche, max_results=max_results)
        cost = len(prospects) * 0.05 if prospects else 0.0
        return [
            {
                "name": p.get("name", ""),
                "email": p.get("email", ""),
                "linkedin": p.get("linkedin", ""),
                "company": p.get("company", ""),
                "website": p.get("website", ""),
                "niche": p.get("industry", ""),
                "source": "apollo",
            }
            for p in prospects
        ], cost, []
    except Exception as exc:
        return [], 0.0, [f"Apollo error: {exc}"]


def _hits_to_prospects(hits: list[dict]) -> list[ProspectProfile]:
    """Convert raw search hits into ProspectProfile objects."""
    prospects = []
    seen_urls: set[str] = set()

    for hit in hits:
        url = (hit.get("url") or "").rstrip("/").lower()
        if url and url in seen_urls:
            continue
        if url:
            seen_urls.add(url)

        source = hit.get("source", "unknown")
        prospects.append(ProspectProfile(
            name=hit.get("name") or hit.get("title", ""),
            company=hit.get("company", ""),
            website=hit.get("url", ""),
            linkedin=hit.get("linkedin", ""),
            email=hit.get("email", ""),
            niche=hit.get("niche", ""),
            what_they_do=hit.get("snippet", ""),
            confidence="low",
            source_tools=[source],
        ))

    return prospects


# ---------------------------------------------------------------------------
# Main convenience function
# ---------------------------------------------------------------------------

def discover_prospects(
    niche: str,
    seed_names: list[str] | None = None,
    seed_urls: list[str] | None = None,
    max_results: int = 20,
) -> DiscoveryResult:
    """Run the discovery agent to find JV prospects.

    This is the main entry point. It constructs a prompt with available
    context, runs the agent to get a discovery plan, then orchestrates
    the actual tool calls based on that plan.

    Args:
        niche: Business niche to search.
        seed_names: Names of known prospects to research.
        seed_urls: URLs of known-good partners (for find-similar).
        max_results: Target number of prospects.

    Returns:
        DiscoveryResult with prospects and metadata.
    """
    seed_names = seed_names or []
    seed_urls = seed_urls or []

    # --- Build prompt for the agent ---
    prompt_parts = [
        f"Find JV prospects in the niche: {niche}",
        f"Target: {max_results} prospects",
    ]
    if seed_names:
        prompt_parts.append(f"Seed names to research: {', '.join(seed_names)}")
    if seed_urls:
        prompt_parts.append(f"Seed URLs of known-good partners: {', '.join(seed_urls)}")

    prompt_parts.append(
        "Decide which tools to use and return a DiscoveryResult "
        "with your plan, estimated cost, and an empty prospects list "
        "(the orchestrator will fill in actual results)."
    )
    prompt = "\n".join(prompt_parts)

    # --- Get the agent's plan via AI ---
    try:
        from pydantic_ai import Agent  # noqa: F811 — import guard
    except ImportError:
        return DiscoveryResult(
            plan=DiscoveryPlan(
                tools_selected=[],
                reasoning="pydantic_ai is not installed",
            ),
            errors=["pydantic_ai package is not installed — cannot run discovery agent"],
        )

    try:
        from matching.enrichment.claude_client import get_pydantic_model_for_tier

        model = get_pydantic_model_for_tier(tier=2)
        if not model:
            return DiscoveryResult(
                plan=DiscoveryPlan(
                    tools_selected=[],
                    reasoning="No AI model available (missing API keys)",
                ),
                errors=["No AI model available — configure OPENROUTER_API_KEY or ANTHROPIC_API_KEY"],
            )

        agent = get_discovery_agent()
        result = agent.run_sync(prompt, model=model)
        plan = result.output.plan

    except Exception as exc:
        logger.warning("Discovery agent planning failed: %s", exc)
        # Fallback: build a reasonable default plan
        default_tools = []
        if seed_urls:
            default_tools.append("exa_similar")
        default_tools.append("duckduckgo")
        if seed_names:
            default_tools.append("exa_search")
        default_tools.append("apollo")

        plan = DiscoveryPlan(
            tools_selected=default_tools,
            reasoning=f"Fallback plan (agent error: {exc}). Using default tool order.",
        )

    # --- Execute tools from the plan ---
    all_hits: list[dict] = []
    total_cost = 0.0
    tools_used: list[str] = []
    errors: list[str] = []

    search_query = f"{niche} coach consultant speaker entrepreneur"
    per_tool = max(max_results // max(len(plan.tools_selected), 1), 5)

    for tool_name in plan.tools_selected:
        if len(all_hits) >= max_results:
            break

        if tool_name == "duckduckgo":
            hits, tool_errors = _run_duckduckgo(search_query, per_tool)
            all_hits.extend(hits)
            errors.extend(tool_errors)
            if hits:
                tools_used.append("duckduckgo")

        elif tool_name == "serper":
            hits, cost, tool_errors = _run_serper(search_query, per_tool)
            all_hits.extend(hits)
            total_cost += cost
            errors.extend(tool_errors)
            if hits:
                tools_used.append("serper")

        elif tool_name == "exa_search":
            query = search_query
            if seed_names:
                query = " ".join(seed_names) + " " + niche
            hits, cost, tool_errors = _run_exa_search(query, per_tool)
            all_hits.extend(hits)
            total_cost += cost
            errors.extend(tool_errors)
            if hits:
                tools_used.append("exa_search")

        elif tool_name == "exa_similar":
            if seed_urls:
                hits, cost, tool_errors = _run_exa_similar(seed_urls, per_tool)
                all_hits.extend(hits)
                total_cost += cost
                errors.extend(tool_errors)
                if hits:
                    tools_used.append("exa_similar")
            else:
                errors.append("exa_similar requested but no seed_urls provided")

        elif tool_name == "apollo":
            hits, cost, tool_errors = _run_apollo(niche, per_tool)
            all_hits.extend(hits)
            total_cost += cost
            errors.extend(tool_errors)
            if hits:
                tools_used.append("apollo")

        else:
            errors.append(f"Unknown tool: {tool_name}")

    # --- Consolidate results ---
    prospects = _hits_to_prospects(all_hits)[:max_results]

    return DiscoveryResult(
        plan=plan,
        prospects=prospects,
        total_cost=round(total_cost, 4),
        tools_used=tools_used,
        errors=errors,
    )
