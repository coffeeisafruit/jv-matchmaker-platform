"""
Real-time budget enforcement and circuit breaker for API calls.

Reads from existing JSONL cost logs (matching/enrichment/flows/cost_tracking.py)
and blocks calls when daily or monthly budgets are exceeded.
"""
import json
import logging
import os
import time
import threading
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

COST_LOG_DIR = Path(__file__).resolve().parents[3] / "scripts" / "enrichment_batches" / "cost_logs"


class BudgetExceededError(Exception):
    """Raised when an API call would exceed the configured budget."""
    pass


class CircuitOpenError(Exception):
    """Raised when the circuit breaker is open due to consecutive failures."""
    pass


class CostGuard:
    """Real-time budget enforcement using existing JSONL cost logs."""

    def __init__(self):
        self.monthly_budget = float(os.environ.get("API_MONTHLY_BUDGET", "200.0"))
        self.daily_budget = float(os.environ.get("API_DAILY_BUDGET", "25.0"))

    def _read_month_costs(self) -> float:
        """Read current month's total cost from JSONL log."""
        now = datetime.now(timezone.utc)
        filename = f"costs_{now.strftime('%Y-%m')}.jsonl"
        filepath = COST_LOG_DIR / filename

        if not filepath.exists():
            return 0.0

        total = 0.0
        try:
            with open(filepath, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        total += float(entry.get("cost_usd", 0.0))
                    except (json.JSONDecodeError, ValueError):
                        continue
        except Exception as e:
            logger.warning(f"Failed to read cost log {filepath}: {e}")

        return total

    def _read_day_costs(self) -> float:
        """Read today's total cost from JSONL log."""
        now = datetime.now(timezone.utc)
        today_str = now.strftime('%Y-%m-%d')
        filename = f"costs_{now.strftime('%Y-%m')}.jsonl"
        filepath = COST_LOG_DIR / filename

        if not filepath.exists():
            return 0.0

        total = 0.0
        try:
            with open(filepath, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        ts = entry.get("timestamp", "")
                        if ts.startswith(today_str):
                            total += float(entry.get("cost_usd", 0.0))
                    except (json.JSONDecodeError, ValueError):
                        continue
        except Exception as e:
            logger.warning(f"Failed to read cost log: {e}")

        return total

    def check_budget(self, tool: str, estimated_cost: float = 0.0) -> None:
        """Raise BudgetExceededError if spend would exceed limits."""
        daily = self._read_day_costs()
        if daily + estimated_cost > self.daily_budget:
            raise BudgetExceededError(
                f"Daily budget exceeded: ${daily:.2f} spent + ${estimated_cost:.2f} estimated "
                f"> ${self.daily_budget:.2f} limit (tool: {tool})"
            )

        monthly = self._read_month_costs()
        if monthly + estimated_cost > self.monthly_budget:
            raise BudgetExceededError(
                f"Monthly budget exceeded: ${monthly:.2f} spent + ${estimated_cost:.2f} estimated "
                f"> ${self.monthly_budget:.2f} limit (tool: {tool})"
            )

    def get_summary(self) -> dict:
        """Get current spend summary."""
        daily = self._read_day_costs()
        monthly = self._read_month_costs()
        return {
            "daily_spend": round(daily, 2),
            "daily_budget": self.daily_budget,
            "daily_remaining": round(max(0, self.daily_budget - daily), 2),
            "monthly_spend": round(monthly, 2),
            "monthly_budget": self.monthly_budget,
            "monthly_remaining": round(max(0, self.monthly_budget - monthly), 2),
        }


class CircuitBreaker:
    """Prevents cost-wasting retries when an API is consistently failing."""

    def __init__(self, failure_threshold: int = 5, cooldown_seconds: int = 600):
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self._lock = threading.Lock()
        self._failures: dict[str, int] = {}
        self._opened_at: dict[str, float] = {}

    def record_failure(self, tool: str) -> None:
        """Record a failure for the given tool."""
        with self._lock:
            self._failures[tool] = self._failures.get(tool, 0) + 1
            if self._failures[tool] >= self.failure_threshold:
                self._opened_at[tool] = time.time()
                logger.warning(
                    f"Circuit breaker OPEN for {tool} "
                    f"({self._failures[tool]} consecutive failures, "
                    f"cooldown {self.cooldown_seconds}s)"
                )

    def record_success(self, tool: str) -> None:
        """Reset failure count on success."""
        with self._lock:
            self._failures.pop(tool, None)
            self._opened_at.pop(tool, None)

    def check(self, tool: str) -> None:
        """Raise CircuitOpenError if circuit is open for this tool."""
        with self._lock:
            opened_at = self._opened_at.get(tool)
            if opened_at is None:
                return

            elapsed = time.time() - opened_at
            if elapsed < self.cooldown_seconds:
                raise CircuitOpenError(
                    f"Circuit breaker open for {tool} "
                    f"({int(self.cooldown_seconds - elapsed)}s remaining)"
                )
            else:
                # Cooldown expired — half-open state, allow one attempt
                self._opened_at.pop(tool, None)
                self._failures[tool] = self.failure_threshold - 1  # One more failure re-opens


# Module-level singletons
_cost_guard = None
_circuit_breaker = None


def get_cost_guard() -> CostGuard:
    global _cost_guard
    if _cost_guard is None:
        _cost_guard = CostGuard()
    return _cost_guard


def get_circuit_breaker() -> CircuitBreaker:
    global _circuit_breaker
    if _circuit_breaker is None:
        _circuit_breaker = CircuitBreaker()
    return _circuit_breaker
