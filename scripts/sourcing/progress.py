"""
Progress tracking and resumability for scrapers.

Persists checkpoint state as JSON files under config/state/.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


STATE_DIR = Path(__file__).parent / "config" / "state"


class ProgressTracker:
    """Tracks scraping progress per source for resumability."""

    def __init__(self):
        STATE_DIR.mkdir(parents=True, exist_ok=True)

    def _path(self, source: str) -> Path:
        return STATE_DIR / f"{source}.json"

    def load(self, source: str) -> dict:
        """Load checkpoint state for a source."""
        path = self._path(source)
        if path.exists():
            return json.loads(path.read_text())
        return {}

    def save(self, source: str, state: dict) -> None:
        """Save checkpoint state for a source."""
        state["updated_at"] = datetime.now().isoformat()
        self._path(source).write_text(json.dumps(state, indent=2))

    def update_checkpoint(
        self,
        source: str,
        last_url: str,
        contacts_total: int,
        contacts_new: int = 0,
    ) -> None:
        """Update checkpoint after a batch."""
        state = self.load(source)
        state["last_url"] = last_url
        state["contacts_total"] = contacts_total
        state["contacts_new"] = state.get("contacts_new", 0) + contacts_new
        state["runs"] = state.get("runs", 0) + 1
        self.save(source, state)

    def get_summary(self) -> dict[str, dict]:
        """Get progress summary for all sources."""
        summary = {}
        for path in sorted(STATE_DIR.glob("*.json")):
            source = path.stem
            data = json.loads(path.read_text())
            summary[source] = {
                "contacts_total": data.get("contacts_total", 0),
                "contacts_new": data.get("contacts_new", 0),
                "runs": data.get("runs", 0),
                "last_run": data.get("updated_at", "never"),
            }
        return summary

    def reset(self, source: str) -> None:
        """Clear checkpoint for a source."""
        path = self._path(source)
        if path.exists():
            path.unlink()
