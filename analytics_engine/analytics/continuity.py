"""
In-memory continuity tracker.
Updated by the sensor-analytics background worker every 30 s.
Tracks per-device gap state and anomaly counts for the Insights page.
"""
import threading
import time
from typing import Optional

_GAP_THRESHOLD_S = 120


class ContinuityState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._devices: dict[str, dict] = {}
        self._last_updated: Optional[float] = None

    def update(self, devices: list[dict]) -> None:
        now_ms = int(time.time() * 1000)
        new: dict[str, dict] = {}

        for d in devices:
            key = f"{d.get('source', '')}:{d.get('device_id', '')}"
            last_ms = d.get("timestamp_ms") or 0
            age_s = (now_ms - last_ms) / 1000 if last_ms else _GAP_THRESHOLD_S + 1
            status = d.get("status", "unknown")
            gap_open = age_s > _GAP_THRESHOLD_S or status == "error"

            new[key] = {
                "source":      d.get("source"),
                "device_id":   d.get("device_id"),
                "device_name": d.get("name"),
                "last_seen_ms": last_ms,
                "age_seconds": round(age_s, 1),
                "gap_open":    gap_open,
                "status":      status,
            }

        with self._lock:
            self._devices = new
            self._last_updated = time.time()

    def anomaly_count(self) -> int:
        with self._lock:
            return sum(1 for v in self._devices.values() if v["gap_open"])

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "devices":      dict(self._devices),
                "last_updated": self._last_updated,
            }
