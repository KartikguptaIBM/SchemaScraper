"""Watermark and run-state manager. Persists to JSON files in state/."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path


class StateManager:
    def __init__(self, state_dir: Path):
        self._dir = state_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        self._watermark_file = self._dir / "watermarks.json"
        self._runs_file = self._dir / "runs.jsonl"
        self._quarantine_hashes_file = self._dir / "quarantined_hashes.json"

    # ── watermarks ──────────────────────────────────────────────────────────
    def get_watermark(self, source: str) -> str | None:
        if not self._watermark_file.exists():
            return None
        data = json.loads(self._watermark_file.read_text())
        return data.get(source)

    def set_watermark(self, source: str, value: str) -> None:
        data: dict = {}
        if self._watermark_file.exists():
            data = json.loads(self._watermark_file.read_text())
        data[source] = value
        self._watermark_file.write_text(json.dumps(data, indent=2))

    # ── quarantine hashes ────────────────────────────────────────────────────
    def get_quarantined_hashes(self, source: str) -> set[str]:
        """Return the set of _row_hash values already quarantined for source."""
        if not self._quarantine_hashes_file.exists():
            return set()
        data = json.loads(self._quarantine_hashes_file.read_text())
        return set(data.get(source, []))

    def add_quarantined_hashes(self, source: str, hashes: set[str]) -> None:
        """Persist new quarantined hashes for source (merged with existing)."""
        data: dict = {}
        if self._quarantine_hashes_file.exists():
            data = json.loads(self._quarantine_hashes_file.read_text())
        existing = set(data.get(source, []))
        data[source] = sorted(existing | hashes)
        self._quarantine_hashes_file.write_text(json.dumps(data, indent=2))

    # ── run history ─────────────────────────────────────────────────────────
    def record_run(self, metadata: dict) -> None:
        with self._runs_file.open("a") as f:
            f.write(json.dumps(metadata) + "\n")
