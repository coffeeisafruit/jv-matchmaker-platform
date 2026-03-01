"""
Verification dashboard — aggregates pipeline health across all steps.

Generates a JSON summary at the end of every pipeline run.
"""
import json
import logging
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

DASHBOARD_DIR = Path(__file__).resolve().parents[2] / "scripts" / "enrichment_batches"


@dataclass
class StepMetrics:
    """Metrics for a single pipeline step."""
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    warnings: int = 0
    details: list = field(default_factory=list)


@dataclass
class PipelineReport:
    """Complete pipeline run report."""
    batch_id: str = ""
    started_at: str = ""
    completed_at: str = ""
    total_profiles: int = 0
    step_1_ingest: StepMetrics = field(default_factory=StepMetrics)
    step_2_selection: StepMetrics = field(default_factory=StepMetrics)
    step_3_email: StepMetrics = field(default_factory=StepMetrics)
    step_4_ai_research: StepMetrics = field(default_factory=StepMetrics)
    step_5_apollo: StepMetrics = field(default_factory=StepMetrics)
    step_6_verification: StepMetrics = field(default_factory=StepMetrics)
    step_7_retry: StepMetrics = field(default_factory=StepMetrics)
    step_8_consolidation: StepMetrics = field(default_factory=StepMetrics)
    step_9_scoring: StepMetrics = field(default_factory=StepMetrics)
    step_10_export: StepMetrics = field(default_factory=StepMetrics)
    embeddings_regenerated: int = 0
    matches_recalculated: int = 0
    cost_usd: float = 0.0


class VerificationDashboard:
    """Aggregates and saves pipeline verification metrics."""

    def __init__(self, batch_id: str = ""):
        self.report = PipelineReport(
            batch_id=batch_id or datetime.now().strftime("%Y%m%d_%H%M%S"),
            started_at=datetime.now().isoformat(),
        )

    def record(self, step: str, status: str, detail: str = ""):
        """Record a verification event.

        Args:
            step: Step name like 'step_1_ingest', 'step_6_verification'
            status: 'passed', 'failed', 'skipped', or 'warning'
            detail: Optional detail string
        """
        metrics = getattr(self.report, step, None)
        if metrics is None:
            logger.warning(f"Unknown step: {step}")
            return

        if status == 'passed':
            metrics.passed += 1
        elif status == 'failed':
            metrics.failed += 1
        elif status == 'skipped':
            metrics.skipped += 1
        elif status == 'warning':
            metrics.warnings += 1

        if detail:
            metrics.details.append(detail)

    def set_totals(self, **kwargs):
        """Set top-level metrics."""
        for key, value in kwargs.items():
            if hasattr(self.report, key):
                setattr(self.report, key, value)

    def save(self) -> str:
        """Save report to JSON file, then check failure thresholds and alert."""
        self.report.completed_at = datetime.now().isoformat()

        DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
        filepath = DASHBOARD_DIR / f"verification_{self.report.batch_id}.json"

        try:
            with open(filepath, 'w') as f:
                json.dump(asdict(self.report), f, indent=2, default=str)
            logger.info(f"Verification dashboard saved: {filepath}")
        except Exception as e:
            logger.warning(f"Failed to save dashboard: {e}")

        # Check failure thresholds and fire alerts
        self._check_thresholds()

        return str(filepath)

    def _check_thresholds(self):
        """Alert when failure rates exceed acceptable levels."""
        from config.alerting import send_alert

        s = self.summary()
        total = s['total_profiles']
        if total == 0:
            return

        failed = s['total_failed']
        failure_rate = failed / total

        # Critical: >50% failure rate means something systemic is broken
        if failure_rate > 0.5:
            send_alert(
                "critical",
                f"Pipeline batch {self.report.batch_id}: {failure_rate:.0%} failure rate",
                f"{failed}/{total} profiles failed. Check logs and retry queue.",
            )
        elif failure_rate > 0.2:
            send_alert(
                "warning",
                f"Pipeline batch {self.report.batch_id}: {failure_rate:.0%} failure rate",
                f"{failed}/{total} profiles failed.",
            )

        # Per-step alerts for critical steps
        step_alerts = [
            ('step_8_consolidation', 'DB consolidation', 0.1),  # >10% DB writes failing is bad
            ('step_4_ai_research', 'AI research', 0.3),  # >30% AI failures (API down?)
            ('step_6_verification', 'Verification gate', 0.4),  # >40% quarantined
        ]
        for step_name, label, threshold in step_alerts:
            metrics = getattr(self.report, step_name, None)
            if metrics and (metrics.passed + metrics.failed) > 0:
                step_fail_rate = metrics.failed / (metrics.passed + metrics.failed)
                if step_fail_rate > threshold:
                    send_alert(
                        "warning",
                        f"{label}: {step_fail_rate:.0%} failure rate in batch {self.report.batch_id}",
                        f"{metrics.failed} failed, {metrics.passed} passed.",
                    )

    def summary(self) -> dict:
        """Return a compact summary dict for logging."""
        r = self.report
        total_passed = sum(
            getattr(r, f'step_{i}_{name}').passed
            for i, name in enumerate([
                'ingest', 'selection', 'email', 'ai_research', 'apollo',
                'verification', 'retry', 'consolidation', 'scoring', 'export',
            ], 1)
            if hasattr(r, f'step_{i}_{name}')
        )
        total_failed = sum(
            getattr(r, f'step_{i}_{name}').failed
            for i, name in enumerate([
                'ingest', 'selection', 'email', 'ai_research', 'apollo',
                'verification', 'retry', 'consolidation', 'scoring', 'export',
            ], 1)
            if hasattr(r, f'step_{i}_{name}')
        )
        return {
            'batch_id': r.batch_id,
            'total_profiles': r.total_profiles,
            'total_passed': total_passed,
            'total_failed': total_failed,
            'embeddings_regenerated': r.embeddings_regenerated,
            'matches_recalculated': r.matches_recalculated,
        }
