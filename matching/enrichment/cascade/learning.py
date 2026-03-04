"""
Cascade learning metrics — enrichment-specific learning that feeds
into the existing LearningLog/ProfileQualityLog infrastructure.

JSONL append-only log of per-run metrics.
apply_learning() analyzes data and returns concrete actions.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

LEARNING_DIR = (
    Path(__file__).resolve().parents[3]
    / "scripts"
    / "enrichment_batches"
    / "cascade_learning"
)


# ---------- Action types ----------

@dataclass
class LearningAction:
    """Base class for learning-driven actions."""

    action_type: str = ""
    reason: str = ""
    timestamp: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()


@dataclass
class AdjustThreshold(LearningAction):
    """Auto-adjust a cascade threshold."""

    action_type: str = "adjust_threshold"
    field_name: str = ""
    old_value: float = 0.0
    new_value: float = 0.0


@dataclass
class PauseModel(LearningAction):
    """Pause a model due to quality regression."""

    action_type: str = "pause_model"
    model: str = ""


@dataclass
class SkipProfiles(LearningAction):
    """Skip profiles that consistently fail."""

    action_type: str = "skip_profiles"
    profile_ids: list = field(default_factory=list)


@dataclass
class Alert(LearningAction):
    """Administrative alert."""

    action_type: str = "alert"
    severity: str = "warning"
    message: str = ""


# ---------- Run metrics ----------

@dataclass
class RunMetrics:
    """Metrics for a single cascade run."""

    run_id: str = ""
    timestamp: str = ""
    model_used: str = ""

    # Layer 1
    l1_profiles_scraped: int = 0
    l1_hit_rate: float = 0.0  # % of profiles where data was found

    # Layer 2
    l2_profiles_rescored: int = 0
    l2_qualified_count: int = 0
    l2_avg_score_delta: float = 0.0

    # Layer 3
    l3_profiles_enriched: int = 0
    l3_json_parse_rate: float = 0.0
    l3_field_fill_rate: float = 0.0
    l3_cost: float = 0.0

    # Layer 4
    l4_conflicts_found: int = 0
    l4_conflicts_resolved: int = 0
    l4_verdicts: dict = field(default_factory=dict)

    # Layer 5-6
    l5_new_matches: int = 0
    l6_clients_with_gaps: int = 0
    l6_acquisitions_triggered: int = 0

    # Overall
    total_cost: float = 0.0
    total_runtime_seconds: float = 0.0


# ---------- Main learning log ----------

class CascadeLearningLog:
    """JSONL log of cascade run metrics + learning actions."""

    def __init__(self):
        LEARNING_DIR.mkdir(parents=True, exist_ok=True)
        self.log_file = LEARNING_DIR / "cascade_learning_log.jsonl"
        self.config_file = LEARNING_DIR / "cascade_config.json"

    def record_run_metrics(self, metrics: RunMetrics) -> None:
        """Append run metrics to the learning log."""
        metrics.timestamp = datetime.now().isoformat()

        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                entry = {"type": "run_metrics", **asdict(metrics)}
                f.write(json.dumps(entry, default=str) + "\n")
        except Exception as e:
            logger.error("Failed to write learning log: %s", e)

    def record_action(self, action: LearningAction) -> None:
        """Append a learning action to the log."""
        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                entry = {"type": "action", **asdict(action)}
                f.write(json.dumps(entry, default=str) + "\n")
        except Exception as e:
            logger.error("Failed to write learning action: %s", e)

    def get_recent_metrics(self, last_n: int = 10) -> list[RunMetrics]:
        """Read the last N run metrics."""
        if not self.log_file.exists():
            return []

        entries = []
        with open(self.log_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    if data.get("type") == "run_metrics":
                        entries.append(data)
                except json.JSONDecodeError:
                    continue

        # Return last N
        recent = entries[-last_n:]
        results = []
        for d in recent:
            d.pop("type", None)
            try:
                results.append(RunMetrics(**{
                    k: v for k, v in d.items()
                    if k in RunMetrics.__dataclass_fields__
                }))
            except TypeError:
                continue

        return results

    def get_cascade_config(self) -> dict:
        """Read current cascade configuration (thresholds, skip lists, etc.)."""
        if not self.config_file.exists():
            return {
                "score_threshold": 50.0,
                "match_threshold": 64,
                "buffer_target": 30,
                "skip_profile_ids": [],
                "paused_models": [],
            }

        try:
            with open(self.config_file, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return {
                "score_threshold": 50.0,
                "match_threshold": 64,
                "buffer_target": 30,
                "skip_profile_ids": [],
                "paused_models": [],
            }

    def save_cascade_config(self, config: dict) -> None:
        """Save updated cascade configuration."""
        try:
            with open(self.config_file, "w") as f:
                json.dump(config, f, indent=2, default=str)
        except Exception as e:
            logger.error("Failed to save cascade config: %s", e)

    def apply_learning(self) -> list[LearningAction]:
        """Analyze learning log and return concrete actions.

        Called after each cascade run by record_learning_task().
        Checks thresholds and model health to auto-adjust config.
        """
        actions: list[LearningAction] = []
        recent = self.get_recent_metrics(last_n=5)
        config = self.get_cascade_config()

        if not recent:
            return actions

        latest = recent[-1]

        # --- Check JSON parse rate ---
        if latest.l3_json_parse_rate > 0 and latest.l3_json_parse_rate < 90:
            action = PauseModel(
                model=latest.model_used,
                reason=f"JSON parse rate {latest.l3_json_parse_rate:.1f}% < 90% threshold",
            )
            actions.append(action)
            config.setdefault("paused_models", [])
            if latest.model_used not in config["paused_models"]:
                config["paused_models"].append(latest.model_used)

        # --- Check field fill rate ---
        if latest.l3_field_fill_rate > 0 and latest.l3_field_fill_rate < 60:
            action = Alert(
                severity="warning",
                message=f"Field fill rate {latest.l3_field_fill_rate:.1f}% < 60% — review prompt quality",
            )
            actions.append(action)

        # --- Check score threshold ROI (auto-tuning) ---
        if len(recent) >= 3:
            avg_qualified = sum(m.l2_qualified_count for m in recent) / len(recent)
            avg_enriched = sum(m.l3_profiles_enriched for m in recent) / len(recent)

            if avg_qualified > 0:
                success_rate = avg_enriched / avg_qualified * 100

                current_threshold = config.get("score_threshold", 50.0)

                # If high success rate in 40-50 band, lower threshold
                if success_rate > 80 and current_threshold > 40:
                    old = current_threshold
                    new = max(40, current_threshold - 5)
                    action = AdjustThreshold(
                        field_name="score_threshold",
                        old_value=old,
                        new_value=new,
                        reason=f"Success rate {success_rate:.0f}% > 80% — lowering threshold",
                    )
                    actions.append(action)
                    config["score_threshold"] = new

                # If low success rate, raise threshold
                elif success_rate < 50 and current_threshold < 60:
                    old = current_threshold
                    new = min(60, current_threshold + 5)
                    action = AdjustThreshold(
                        field_name="score_threshold",
                        old_value=old,
                        new_value=new,
                        reason=f"Success rate {success_rate:.0f}% < 50% — raising threshold",
                    )
                    actions.append(action)
                    config["score_threshold"] = new

        # --- Cost per useful profile alert ---
        if latest.l3_profiles_enriched > 0:
            cost_per = latest.l3_cost / latest.l3_profiles_enriched
            if cost_per > 0.05:
                action = Alert(
                    severity="warning",
                    message=f"Cost per useful profile ${cost_per:.3f} > $0.05 — review tier ROI",
                )
                actions.append(action)

        # Save updated config
        if actions:
            self.save_cascade_config(config)
            for action in actions:
                self.record_action(action)

        return actions

    def get_threshold_recommendation(self) -> dict:
        """Analyze enrichment success by score band for threshold tuning."""
        recent = self.get_recent_metrics(last_n=10)
        if not recent:
            return {"recommendation": "insufficient_data"}

        avg_parse = sum(m.l3_json_parse_rate for m in recent if m.l3_json_parse_rate > 0)
        count_with_parse = sum(1 for m in recent if m.l3_json_parse_rate > 0)
        avg_parse = avg_parse / count_with_parse if count_with_parse else 0

        avg_fill = sum(m.l3_field_fill_rate for m in recent if m.l3_field_fill_rate > 0)
        count_with_fill = sum(1 for m in recent if m.l3_field_fill_rate > 0)
        avg_fill = avg_fill / count_with_fill if count_with_fill else 0

        config = self.get_cascade_config()

        return {
            "current_threshold": config.get("score_threshold", 50.0),
            "avg_json_parse_rate": round(avg_parse, 1),
            "avg_field_fill_rate": round(avg_fill, 1),
            "runs_analyzed": len(recent),
            "paused_models": config.get("paused_models", []),
        }

    def get_model_health(self) -> dict:
        """Check JSON parse rate and field fill rate trends."""
        recent = self.get_recent_metrics(last_n=5)
        if not recent:
            return {"status": "no_data"}

        latest = recent[-1]
        return {
            "model": latest.model_used,
            "json_parse_rate": latest.l3_json_parse_rate,
            "field_fill_rate": latest.l3_field_fill_rate,
            "is_healthy": (
                latest.l3_json_parse_rate >= 90
                and latest.l3_field_fill_rate >= 60
            ),
        }
