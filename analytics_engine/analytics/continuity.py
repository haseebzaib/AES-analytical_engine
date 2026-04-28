"""
In-memory continuity tracker.
Updated by the sensor-analytics background worker every 30 s.
Tracks per-device gap state and anomaly counts for the Insights page.
"""
import logging
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)

_GAP_THRESHOLD_S = 120


class ContinuityState:
    def __init__(self) -> None:
        self._lock         = threading.Lock()
        self._devices: dict[str, dict] = {}
        self._last_updated: Optional[float] = None

    def update(self, devices: list[dict]) -> None:
        now_ms = int(time.time() * 1000)
        new: dict[str, dict] = {}

        for d in devices:
            key     = f"{d.get('source', '')}:{d.get('device_id', '')}"
            last_ms = d.get("timestamp_ms") or 0
            age_s   = (now_ms - last_ms) / 1000 if last_ms else _GAP_THRESHOLD_S + 1
            status  = d.get("status", "unknown")
            error   = d.get("error")

            # Detect all-zero metric values (connected but not measuring)
            metrics      = d.get("metrics") or {}
            numeric_vals = [m.get("value") for m in metrics.values()
                            if isinstance(m.get("value"), (int, float))]
            no_data      = bool(numeric_vals) and all(v == 0 for v in numeric_vals)

            gap_open = age_s > _GAP_THRESHOLD_S or status == "error"
            anomaly  = gap_open or no_data or bool(error)

            new[key] = {
                "source":       d.get("source"),
                "device_id":    d.get("device_id"),
                "device_name":  d.get("name"),
                "last_seen_ms": last_ms,
                "age_seconds":  round(age_s, 1),
                "gap_open":     gap_open,
                "no_data":      no_data,
                "has_error":    bool(error),
                "status":       status,
                "anomaly":      anomaly,
            }

            if gap_open:
                logger.warning("Continuity: GAP on %s — age=%.1fs status=%s", key, age_s, status)
            if no_data:
                logger.warning("Continuity: NO MEASUREMENTS on %s — all metrics are zero (device not measuring?)", key)
            if error:
                logger.warning("Continuity: DEVICE ERROR on %s — %s", key, error.get("message", ""))

        gaps   = sum(1 for v in new.values() if v["gap_open"])
        logger.info(
            "Continuity update: %d device(s), %d gap(s) detected",
            len(new), gaps,
        )

        with self._lock:
            self._devices      = new
            self._last_updated = time.time()

    def anomaly_count(self) -> int:
        with self._lock:
            return sum(1 for v in self._devices.values() if v.get("anomaly"))

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "devices":      dict(self._devices),
                "last_updated": self._last_updated,
            }
