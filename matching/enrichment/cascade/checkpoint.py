"""
JSONL checkpoint tracking for cascade layers.

Same append-only pattern as retry_queue.py — one file per layer per run.
Crash at any point → resume without reprocessing.

Files stored in: scripts/enrichment_batches/cascade_checkpoints/
"""

import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

CHECKPOINT_DIR = (
    Path(__file__).resolve().parents[3]
    / "scripts"
    / "enrichment_batches"
    / "cascade_checkpoints"
)


@dataclass
class CheckpointEntry:
    """A single processed-profile record."""

    profile_id: str
    layer: int
    status: str  # "success", "skipped", "error"
    fields_filled: list[str]
    timestamp: str = ""
    error: str = ""

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()


class CascadeCheckpoint:
    """Append-only JSONL checkpoint for a cascade layer run.

    Usage::

        cp = CascadeCheckpoint(layer=1, run_id="2026-03-03")
        already_done = cp.get_processed_ids()
        # ... process profiles not in already_done ...
        cp.mark_processed("uuid-123", "success", ["email", "phone"])
    """

    def __init__(self, layer: int, run_id: Optional[str] = None):
        self.layer = layer
        self.run_id = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
        self.filepath = CHECKPOINT_DIR / f"cascade_L{layer}_{self.run_id}.jsonl"

    def mark_processed(
        self,
        profile_id: str,
        status: str,
        fields_filled: Optional[list[str]] = None,
        error: str = "",
    ) -> None:
        """Append a checkpoint entry for a processed profile."""
        entry = CheckpointEntry(
            profile_id=str(profile_id),
            layer=self.layer,
            status=status,
            fields_filled=fields_filled or [],
            error=error,
        )
        try:
            with open(self.filepath, "a", encoding="utf-8") as f:
                f.write(json.dumps(asdict(entry), default=str) + "\n")
        except Exception as e:
            logger.error("Checkpoint write failed: %s", e)

    def get_processed_ids(self) -> set[str]:
        """Read all processed profile IDs from this checkpoint file.

        Also scans any same-layer checkpoint files in the directory
        (for resume across runs).
        """
        processed: set[str] = set()
        if not CHECKPOINT_DIR.exists():
            return processed

        pattern = f"cascade_L{self.layer}_*.jsonl"
        for cp_file in CHECKPOINT_DIR.glob(pattern):
            try:
                with open(cp_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            data = json.loads(line)
                            if data.get("status") in ("success", "skipped"):
                                processed.add(data["profile_id"])
                        except (json.JSONDecodeError, KeyError):
                            continue
            except Exception as e:
                logger.warning("Error reading checkpoint file %s: %s", cp_file, e)

        return processed

    def get_stats(self) -> dict:
        """Return summary stats from this checkpoint file."""
        success = error = skipped = 0
        all_fields: list[str] = []

        if not self.filepath.exists():
            return {"success": 0, "error": 0, "skipped": 0, "fields_filled": {}}

        with open(self.filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    status = data.get("status", "")
                    if status == "success":
                        success += 1
                        all_fields.extend(data.get("fields_filled", []))
                    elif status == "error":
                        error += 1
                    elif status == "skipped":
                        skipped += 1
                except json.JSONDecodeError:
                    continue

        field_counts: dict[str, int] = {}
        for f in all_fields:
            field_counts[f] = field_counts.get(f, 0) + 1

        return {
            "success": success,
            "error": error,
            "skipped": skipped,
            "fields_filled": field_counts,
        }
