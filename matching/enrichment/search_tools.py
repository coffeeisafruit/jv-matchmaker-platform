"""
Search tool wrappers for the acquisition discovery pipeline.

Each service follows a common interface: search(query, max_results) -> list[dict].
Results are normalized to a common format:
    {"title": str, "url": str, "snippet": str, "source": str}

Cost tracking is built into each call via the cost_tracking module.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class SearchResult:
    """Normalized search result from any tool."""
    title: str
    url: str
    snippet: str
    source: str  # "serper", "tavily", "duckduckgo"
    cost: float = 0.0
    raw: dict | None = None


# ---------------------------------------------------------------------------
# Serper.dev (Google SERP results)
# ---------------------------------------------------------------------------

class SerperSearchService:
    """Google search results via Serper.dev API ($1/1K queries)."""

    API_URL = "https://google.serper.dev/search"
    COST_PER_QUERY = 0.001  # ~$1/1K queries

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.environ.get("SERPER_API_KEY", "")
        self.available = bool(self.api_key)

    def search(self, query: str, max_results: int = 10) -> list[SearchResult]:
        """Search Google via Serper.dev."""
        if not self.available:
            logger.warning("Serper API key not configured")
            return []

        import httpx

        try:
            resp = httpx.post(
                self.API_URL,
                json={"q": query, "num": min(max_results, 100)},
                headers={
                    "X-API-KEY": self.api_key,
                    "Content-Type": "application/json",
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("Serper search failed: %s", exc)
            return []

        results = []
        for item in data.get("organic", [])[:max_results]:
            results.append(SearchResult(
                title=item.get("title", ""),
                url=item.get("link", ""),
                snippet=item.get("snippet", ""),
                source="serper",
                cost=self.COST_PER_QUERY / max(len(data.get("organic", [])), 1),
                raw=item,
            ))

        logger.info("Serper: %d results for '%s'", len(results), query[:60])
        return results


# ---------------------------------------------------------------------------
# Tavily (AI-extracted content with citations)
# ---------------------------------------------------------------------------

class TavilySearchService:
    """AI-powered search via Tavily API (~$0.004-0.008/query)."""

    API_URL = "https://api.tavily.com/search"
    COST_PER_QUERY = 0.005

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.environ.get("TAVILY_API_KEY", "")
        self.available = bool(self.api_key)

    def search(
        self,
        query: str,
        max_results: int = 10,
        search_depth: str = "basic",
    ) -> list[SearchResult]:
        """Search via Tavily with AI-extracted content."""
        if not self.available:
            logger.warning("Tavily API key not configured")
            return []

        import httpx

        try:
            resp = httpx.post(
                self.API_URL,
                json={
                    "api_key": self.api_key,
                    "query": query,
                    "max_results": min(max_results, 20),
                    "search_depth": search_depth,
                    "include_answer": False,
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("Tavily search failed: %s", exc)
            return []

        results = []
        for item in data.get("results", [])[:max_results]:
            results.append(SearchResult(
                title=item.get("title", ""),
                url=item.get("url", ""),
                snippet=item.get("content", "")[:500],
                source="tavily",
                cost=self.COST_PER_QUERY / max(len(data.get("results", [])), 1),
                raw=item,
            ))

        logger.info("Tavily: %d results for '%s'", len(results), query[:60])
        return results


# ---------------------------------------------------------------------------
# DuckDuckGo (free fallback)
# ---------------------------------------------------------------------------

class DuckDuckGoSearchService:
    """Free web search via DuckDuckGo (rate-limited, ~20-50 req/min)."""

    COST_PER_QUERY = 0.0  # Free

    def __init__(self):
        self.available = True
        try:
            from duckduckgo_search import DDGS
            self._ddgs_cls = DDGS
        except ImportError:
            logger.warning("duckduckgo-search package not installed")
            self.available = False
            self._ddgs_cls = None

    def search(self, query: str, max_results: int = 10) -> list[SearchResult]:
        """Search via DuckDuckGo (free, no API key needed)."""
        if not self.available:
            return []

        try:
            with self._ddgs_cls() as ddgs:
                raw_results = list(ddgs.text(query, max_results=min(max_results, 50)))
        except Exception as exc:
            logger.warning("DuckDuckGo search failed: %s", exc)
            return []

        results = []
        for item in raw_results[:max_results]:
            results.append(SearchResult(
                title=item.get("title", ""),
                url=item.get("href", item.get("link", "")),
                snippet=item.get("body", item.get("snippet", "")),
                source="duckduckgo",
                cost=0.0,
                raw=item,
            ))

        logger.info("DuckDuckGo: %d results for '%s'", len(results), query[:60])
        return results


# ---------------------------------------------------------------------------
# Unified search interface
# ---------------------------------------------------------------------------

def search_multi(
    query: str,
    tools: list[str] | None = None,
    max_results_per_tool: int = 10,
) -> list[SearchResult]:
    """Search across multiple tools, returning deduplicated results.

    Args:
        query: Search query string.
        tools: List of tool names to use. Defaults to all available.
            Options: "serper", "tavily", "duckduckgo"
        max_results_per_tool: Max results per tool.

    Returns:
        Combined, deduplicated list of SearchResult objects.
    """
    if tools is None:
        tools = ["serper", "exa", "duckduckgo"]

    services = {
        "serper": SerperSearchService,
        "tavily": TavilySearchService,
        "duckduckgo": DuckDuckGoSearchService,
    }

    all_results: list[SearchResult] = []
    seen_urls: set[str] = set()

    for tool_name in tools:
        svc_cls = services.get(tool_name)
        if not svc_cls:
            logger.warning("Unknown search tool: %s", tool_name)
            continue

        svc = svc_cls()
        if not svc.available:
            continue

        results = svc.search(query, max_results=max_results_per_tool)
        for r in results:
            normalized_url = r.url.rstrip("/").lower()
            if normalized_url not in seen_urls:
                seen_urls.add(normalized_url)
                all_results.append(r)

    logger.info(
        "Multi-search: %d unique results across %s for '%s'",
        len(all_results), tools, query[:60],
    )
    return all_results
