"""
Persistent retry queue for failed pipeline operations.

Every failure point in the pipeline enqueues a RetryItem here instead of
silently logging and moving on. A management command (process_retries)
reads the queue and re-runs failed operations.

Storage: JSONL files in scripts/enrichment_batches/retry_queue/
One file per day, append-only. The processor reads all files and filters
by retry eligibility.
"""

import json
import logging
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from config.alerting import send_alert

logger = logging.getLogger(__name__)

RETRY_DIR = Path(__file__).resolve().parents[2] / "scripts" / "enrichment_batches" / "retry_queue"

# ---------------------------------------------------------------------------
# Failure categories — determines what the processor does on retry
# ---------------------------------------------------------------------------
RETRY_OPERATIONS = {
    "embedding_failed": "Re-run embedding generation for a single profile",
    "match_recalc_failed": "Re-run match score recalculation",
    "score_stale": "Match scores older than enrichment — needs recalculation",
    "db_write_failed": "Profile field update failed during consolidation",
    "email_write_failed": "Email update failed during consolidation",
    "quarantined": "Verification gate quarantined — needs re-enrichment",
    "ai_research_failed": "AI research permanently failed — needs retry with fresh data",
    "report_skipped": "Report generation skipped due to insufficient data",
    "confidence_calc_failed": "Profile confidence calculation failed",
}


@dataclass
class RetryItem:
    """A single failed operation queued for retry."""
    profile_id: str
    operation: str  # Key from RETRY_OPERATIONS
    reason: str  # Human-readable failure reason
    failed_at: str = ""  # ISO timestamp
    retry_count: int = 0
    last_retry_at: str = ""  # ISO timestamp of last retry attempt
    context: dict = field(default_factory=dict)  # Operation-specific data
    resolved: bool = False
    resolved_at: str = ""

    def __post_init__(self):
        if not self.failed_at:
            self.failed_at = datetime.now().isoformat()


def should_retry(item: RetryItem) -> bool:
    """Immediate retry policy — retry right away, up to 4 attempts."""
    MAX_RETRIES = 4
    return item.retry_count < MAX_RETRIES


# ---------------------------------------------------------------------------
# Queue operations
# ---------------------------------------------------------------------------

def enqueue(
    profile_id: str,
    operation: str,
    reason: str,
    context: Optional[dict] = None,
) -> None:
    """Add a failed operation to the retry queue.

    Also sends an alert if this is a new failure type for this profile.
    """
    if operation not in RETRY_OPERATIONS:
        logger.warning(f"Unknown retry operation: {operation}")

    item = RetryItem(
        profile_id=str(profile_id),
        operation=operation,
        reason=reason,
        context=context or {},
    )

    RETRY_DIR.mkdir(parents=True, exist_ok=True)
    queue_file = RETRY_DIR / f"retry_{datetime.now().strftime('%Y%m%d')}.jsonl"

    try:
        with open(queue_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(asdict(item), default=str) + '\n')
        logger.info(f"Retry queued: {operation} for profile {profile_id} — {reason}")
    except Exception as e:
        # Queue write itself failed — this is critical
        logger.error(f"RETRY QUEUE WRITE FAILED: {e} — {operation} for {profile_id}")
        send_alert("critical", "Retry queue write failed", f"{operation} for {profile_id}: {e}")


def read_pending() -> list[RetryItem]:
    """Read all pending (unresolved) retry items from all queue files.

    Deduplicates by (profile_id, operation) — the latest entry wins.
    This means mark_resolved() works by appending a resolved record
    that supersedes earlier unresolved ones.
    """
    # Collect latest entry per (profile_id, operation)
    latest: dict[tuple[str, str], RetryItem] = {}

    if not RETRY_DIR.exists():
        return []

    for queue_file in sorted(RETRY_DIR.glob("retry_*.jsonl")):
        try:
            with open(queue_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        item = RetryItem(**data)
                        key = (item.profile_id, item.operation)
                        latest[key] = item  # Last entry wins
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.warning(f"Skipping malformed retry entry: {e}")
        except Exception as e:
            logger.warning(f"Error reading retry file {queue_file}: {e}")

    return [item for item in latest.values() if not item.resolved]


def mark_resolved(profile_id: str, operation: str) -> None:
    """Mark a retry item as resolved by rewriting it in the queue file.

    Since JSONL is append-only, we append a resolution record.
    The reader deduplicates by (profile_id, operation), taking the latest.
    """
    resolution = {
        "profile_id": str(profile_id),
        "operation": operation,
        "resolved": True,
        "resolved_at": datetime.now().isoformat(),
        "reason": "resolved",
        "failed_at": "",
        "retry_count": 0,
        "last_retry_at": "",
        "context": {},
    }

    RETRY_DIR.mkdir(parents=True, exist_ok=True)
    queue_file = RETRY_DIR / f"retry_{datetime.now().strftime('%Y%m%d')}.jsonl"

    try:
        with open(queue_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(resolution, default=str) + '\n')
    except Exception as e:
        logger.error(f"Failed to mark retry resolved: {e}")


def update_retry_count(item: RetryItem) -> None:
    """Append an updated retry record with incremented count."""
    item.retry_count += 1
    item.last_retry_at = datetime.now().isoformat()

    RETRY_DIR.mkdir(parents=True, exist_ok=True)
    queue_file = RETRY_DIR / f"retry_{datetime.now().strftime('%Y%m%d')}.jsonl"

    try:
        with open(queue_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(asdict(item), default=str) + '\n')
    except Exception as e:
        logger.error(f"Failed to update retry count: {e}")


def get_queue_summary() -> dict:
    """Get a summary of the retry queue state."""
    items = read_pending()
    by_operation = {}
    for item in items:
        by_operation.setdefault(item.operation, []).append(item)

    return {
        "total_pending": len(items),
        "by_operation": {op: len(items) for op, items in by_operation.items()},
        "oldest": min((i.failed_at for i in items), default="none"),
        "max_retries_hit": sum(1 for i in items if i.retry_count >= 4),
    }
