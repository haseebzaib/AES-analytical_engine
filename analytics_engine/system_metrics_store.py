from __future__ import annotations

import json
from pathlib import Path


class SystemMetricsStore:
    def __init__(self, gateway_root: Path) -> None:
        self._state_dir = gateway_root / "system_related" / "observability" / "state"
        self._current_file = self._state_dir / "metrics_current.json"
        self._history_file = self._state_dir / "metrics_history.json"

    def _read_json(self, path: Path) -> dict[str, object]:
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}

    def get_current(self) -> dict[str, object]:
        return self._read_json(self._current_file)

    def get_history(self) -> dict[str, object]:
        payload = self._read_json(self._history_file)
        if "samples" not in payload:
            return {"samples": []}
        return payload
