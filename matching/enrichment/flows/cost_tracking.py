"""
Cost tracking for search tools and AI API usage.

Logs per-query costs to monthly JSONL files, generates monthly summaries,
and supports cost-per-useful-result analysis for tool allocation
optimization.

Cost log directory: ``scripts/enrichment_batches/cost_logs/``
File naming:        ``costs_2026-02.jsonl`` (one file per month)
Each line is a JSON object matching :class:`CostEntry` fields.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from prefect import task, get_run_logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

COST_LOG_DIR = Path(__file__).resolve().parents[3] / "scripts" / "enrichment_batches" / "cost_logs"

TRACKED_TOOLS: set[str] = {
    "exa_websets",
    "exa_search",
    "exa_find_similar",
    "serper",
    "tavily",
    "apollo",
    "duckduckgo",
    "claude_ai",
}

# Approximate per-query cost defaults (USD) for tools without metered billing.
# Used when the caller does not supply a cost value.
DEFAULT_COST_PER_QUERY: dict[str, float] = {
    "exa_websets": 0.10,
    "exa_search": 0.005,
    "exa_find_similar": 0.005,
    "serper": 0.002,
    "tavily": 0.01,
    "apollo": 0.03,
    "duckduckgo": 0.0,
    "claude_ai": 0.015,
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class CostEntry:
    """A single cost log entry."""

    tool: str
    query: str
    cost: float
    results_returned: int = 0
    results_useful: int = 0  # scored 60+ by ISMC pre-filter
    timestamp: str = ""
    context: str = ""  # e.g. "acquisition_for_client_xyz"
    profile_id: str = ""

    def __post_init__(self) -> None:
        if not self.timestamp:
            self.timestamp = datetime.now(timezone.utc).isoformat()


@dataclass
class MonthlyCostReport:
    """Aggregated monthly cost report."""

    month: str  # "2026-02"
    total_cost: float = 0.0
    per_tool: dict[str, dict[str, Any]] = field(default_factory=dict)
    per_client: dict[str, float] = field(default_factory=dict)
    cost_per_useful_result: dict[str, float] = field(default_factory=dict)
    recommendations: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# File helpers
# ---------------------------------------------------------------------------

def _ensure_log_dir() -> Path:
    """Create cost log directory if it doesn't exist and return the path."""
    COST_LOG_DIR.mkdir(parents=True, exist_ok=True)
    return COST_LOG_DIR


def _log_file_for_month(month: str) -> Path:
    """Return the JSONL file path for *month* (``YYYY-MM``)."""
    return _ensure_log_dir() / f"costs_{month}.jsonl"


def _current_month() -> str:
    """Return the current UTC month as ``YYYY-MM``."""
    return datetime.now(timezone.utc).strftime("%Y-%m")


def _month_from_timestamp(ts: str) -> str:
    """Extract ``YYYY-MM`` from an ISO-8601 timestamp string."""
    return ts[:7] if len(ts) >= 7 else _current_month()


def _read_entries_for_month(month: str) -> list[dict[str, Any]]:
    """Read all cost entries from the JSONL file for *month*."""
    path = _log_file_for_month(month)
    if not path.exists():
        return []
    entries: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return entries


def _read_entries_for_days(days: int) -> list[dict[str, Any]]:
    """Read cost entries from the last *days* calendar days.

    Scans at most two monthly files (current and previous month) and
    filters by timestamp.
    """
    now = datetime.now(timezone.utc)
    cutoff = now.timestamp() - (days * 86400)

    # Collect candidate month strings
    months: set[str] = set()
    months.add(now.strftime("%Y-%m"))
    # Also include previous month in case the window spans a boundary
    prev = now.replace(day=1)
    for _ in range(2):
        months.add(prev.strftime("%Y-%m"))
        # Step back one month
        if prev.month == 1:
            prev = prev.replace(year=prev.year - 1, month=12)
        else:
            prev = prev.replace(month=prev.month - 1)

    entries: list[dict[str, Any]] = []
    for m in sorted(months):
        for entry in _read_entries_for_month(m):
            ts_str = entry.get("timestamp", "")
            try:
                ts = datetime.fromisoformat(ts_str).timestamp()
            except (ValueError, TypeError):
                continue
            if ts >= cutoff:
                entries.append(entry)

    return entries


# ---------------------------------------------------------------------------
# Prefect tasks
# ---------------------------------------------------------------------------

@task(name="log-search-cost")
def log_search_cost(entry: CostEntry) -> None:
    """Log a single search tool cost entry.

    Appends to a JSONL cost log file at ``scripts/enrichment_batches/cost_logs/``
    named by the entry's month.
    """
    logger = get_run_logger()

    month = _month_from_timestamp(entry.timestamp)
    path = _log_file_for_month(month)

    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(asdict(entry), default=str) + "\n")

    logger.info(
        "Logged cost entry: tool=%s cost=$%.4f results=%d/%d useful",
        entry.tool,
        entry.cost,
        entry.results_useful,
        entry.results_returned,
    )


@task(name="log-search-costs-batch")
def log_search_costs_batch(entries: list[CostEntry]) -> int:
    """Log multiple cost entries.  Returns count logged."""
    logger = get_run_logger()

    # Group entries by month so we open each file only once
    by_month: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        month = _month_from_timestamp(entry.timestamp)
        by_month.setdefault(month, []).append(asdict(entry))

    count = 0
    for month, dicts in by_month.items():
        path = _log_file_for_month(month)
        with path.open("a", encoding="utf-8") as fh:
            for d in dicts:
                fh.write(json.dumps(d, default=str) + "\n")
                count += 1

    logger.info("Batch-logged %d cost entries across %d months", count, len(by_month))
    return count


@task(name="generate-monthly-cost-report")
def generate_monthly_cost_report(
    month: str = "",
) -> MonthlyCostReport:
    """Generate aggregated monthly cost report.

    Reads from cost log files, aggregates by tool and client, computes
    cost-per-useful-result metrics, and generates optimization
    recommendations.

    Parameters
    ----------
    month:
        Month string in ``YYYY-MM`` format.  Defaults to the current
        UTC month.
    """
    logger = get_run_logger()

    if not month:
        month = _current_month()

    entries = _read_entries_for_month(month)
    logger.info("Generating cost report for %s: %d entries", month, len(entries))

    report = MonthlyCostReport(month=month)

    for entry in entries:
        tool = entry.get("tool", "unknown")
        cost = float(entry.get("cost", 0.0))
        returned = int(entry.get("results_returned", 0))
        useful = int(entry.get("results_useful", 0))
        profile_id = entry.get("profile_id", "")

        report.total_cost += cost

        # Per-tool aggregation
        if tool not in report.per_tool:
            report.per_tool[tool] = {
                "cost": 0.0,
                "queries": 0,
                "results_returned": 0,
                "results_useful": 0,
            }
        report.per_tool[tool]["cost"] += cost
        report.per_tool[tool]["queries"] += 1
        report.per_tool[tool]["results_returned"] += returned
        report.per_tool[tool]["results_useful"] += useful

        # Per-client aggregation (profile_id serves as client key)
        if profile_id:
            report.per_client[profile_id] = (
                report.per_client.get(profile_id, 0.0) + cost
            )

    # Cost per useful result
    for tool, stats in report.per_tool.items():
        if stats["results_useful"] > 0:
            report.cost_per_useful_result[tool] = round(
                stats["cost"] / stats["results_useful"], 4
            )
        else:
            report.cost_per_useful_result[tool] = float("inf")

    # Generate recommendations
    report.recommendations = _generate_recommendations(report)

    report.total_cost = round(report.total_cost, 4)
    for stats in report.per_tool.values():
        stats["cost"] = round(stats["cost"], 4)

    logger.info(
        "Monthly report for %s: $%.2f total, %d tools, %d profiles",
        month,
        report.total_cost,
        len(report.per_tool),
        len(report.per_client),
    )
    return report


@task(name="get-cost-summary")
def get_cost_summary(
    days: int = 30,
    tool: str = "",
) -> dict[str, Any]:
    """Quick cost summary for a recent period.

    Parameters
    ----------
    days:
        Number of calendar days to look back.
    tool:
        If non-empty, restrict to entries for this specific tool.

    Returns
    -------
    dict with: total_cost, query_count, avg_cost_per_query,
    tool_breakdown, top_5_expensive_queries
    """
    logger = get_run_logger()
    entries = _read_entries_for_days(days)

    if tool:
        entries = [e for e in entries if e.get("tool") == tool]

    total_cost = 0.0
    tool_breakdown: dict[str, dict[str, Any]] = {}
    all_queries: list[dict[str, Any]] = []

    for entry in entries:
        t = entry.get("tool", "unknown")
        cost = float(entry.get("cost", 0.0))
        total_cost += cost

        if t not in tool_breakdown:
            tool_breakdown[t] = {"cost": 0.0, "queries": 0}
        tool_breakdown[t]["cost"] += cost
        tool_breakdown[t]["queries"] += 1

        all_queries.append({
            "tool": t,
            "query": entry.get("query", ""),
            "cost": cost,
            "timestamp": entry.get("timestamp", ""),
        })

    query_count = len(entries)
    avg_cost = round(total_cost / query_count, 4) if query_count else 0.0

    # Round tool breakdown costs
    for stats in tool_breakdown.values():
        stats["cost"] = round(stats["cost"], 4)

    # Top 5 most expensive queries
    all_queries.sort(key=lambda q: q["cost"], reverse=True)
    top_5 = all_queries[:5]

    summary: dict[str, Any] = {
        "days": days,
        "total_cost": round(total_cost, 4),
        "query_count": query_count,
        "avg_cost_per_query": avg_cost,
        "tool_breakdown": tool_breakdown,
        "top_5_expensive_queries": top_5,
    }

    logger.info(
        "Cost summary (%d days): $%.2f across %d queries",
        days, total_cost, query_count,
    )
    return summary


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _generate_recommendations(report: MonthlyCostReport) -> list[str]:
    """Generate cost optimization recommendations from a monthly report."""
    recs: list[str] = []

    if not report.per_tool:
        return recs

    # Compare cost per useful result across tools
    finite_cpur = {
        t: c
        for t, c in report.cost_per_useful_result.items()
        if c != float("inf") and c > 0
    }

    if len(finite_cpur) >= 2:
        best_tool = min(finite_cpur, key=finite_cpur.get)  # type: ignore[arg-type]
        worst_tool = max(finite_cpur, key=finite_cpur.get)  # type: ignore[arg-type]
        ratio = finite_cpur[worst_tool] / finite_cpur[best_tool]
        if ratio >= 2.0:
            recs.append(
                f"{best_tool} found {ratio:.1f}x more useful results per dollar "
                f"than {worst_tool}. Consider shifting broad queries to {best_tool}."
            )

    # Flag tools with zero useful results
    for tool, stats in report.per_tool.items():
        if stats["queries"] >= 5 and stats["results_useful"] == 0:
            cost = stats["cost"]
            recs.append(
                f"{tool} had {stats['queries']} queries ($"
                f"{cost:.2f}) with zero useful results. "
                f"Review query strategy or consider dropping."
            )

    # High-cost alert
    if report.total_cost > 150.0:
        recs.append(
            f"Monthly cost ${report.total_cost:.2f} exceeds $150 threshold. "
            f"Evaluate Vast.ai integration per VAST_AI_INTEGRATION.md."
        )

    # Low-yield tools (< 10% useful results)
    for tool, stats in report.per_tool.items():
        returned = stats["results_returned"]
        useful = stats["results_useful"]
        if returned >= 20 and useful / returned < 0.10:
            yield_pct = (useful / returned) * 100
            recs.append(
                f"{tool} yield is {yield_pct:.0f}% ({useful}/{returned} useful). "
                f"Consider tightening pre-filter or switching tools for this use case."
            )

    return recs
