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
        self._pending_file   = self._dir / "watermarks_pending.json"
        self._runs_file      = self._dir / "run_history.jsonl"

    # ── watermarks ──────────────────────────────────────────────────────────
    def get_watermark(self, source: str) -> str | None:
        if not self._watermark_file.exists():
            return None
        data = json.loads(self._watermark_file.read_text())
        return data.get(source)

    def get_all_watermarks(self) -> dict:
        if not self._watermark_file.exists():
            return {}
        return json.loads(self._watermark_file.read_text())

    def stage_watermark(self, source: str, value: str) -> None:
        """Write a candidate watermark to the pending file — not yet committed."""
        data: dict = {}
        if self._pending_file.exists():
            data = json.loads(self._pending_file.read_text())
        data[source] = value
        self._pending_file.write_text(json.dumps(data, indent=2))

    def commit_watermark(self) -> None:
        """Merge pending watermarks into the live file. Call only on full success."""
        if not self._pending_file.exists():
            return
        pending = json.loads(self._pending_file.read_text())
        live: dict = {}
        if self._watermark_file.exists():
            live = json.loads(self._watermark_file.read_text())
        live.update(pending)
        self._watermark_file.write_text(json.dumps(live, indent=2))
        self._pending_file.unlink()

    def discard_pending_watermark(self) -> None:
        """Discard staged watermarks on pipeline failure — live watermark unchanged."""
        if self._pending_file.exists():
            self._pending_file.unlink()

    # ── run history ─────────────────────────────────────────────────────────
    def record_run(self, metadata: dict) -> None:
        with self._runs_file.open("a") as f:
            f.write(json.dumps(metadata) + "\n")

    def get_recent_runs(self, n: int = 5) -> list[dict]:
        if not self._runs_file.exists():
            return []
        lines = self._runs_file.read_text().splitlines()
        return [json.loads(line) for line in lines[-n:]]
