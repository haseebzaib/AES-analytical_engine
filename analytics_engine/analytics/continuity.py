"""
In-memory continuity tracker — updated by the sensor-analytics background worker.

Health model follows the AES/PES data contract (analytics_processing_engine_documentation.md):

  Layer 1 — device.status   : "ok" | "warning" | "error"
  Layer 2 — device.error    : null | {type, severity, message, details}
  Layer 3 — metric.quality  : "good" | "stale" | "error"  (per-metric, authoritative)

Anomaly definition (any one is sufficient):
  - device status is not "ok"
  - device error field is not null
  - any metric quality is "stale" or "error"
  - any metric timestamp_ms is older than METRIC_AGE_WARN_S (belt-and-suspenders)

Logging philosophy:
  - Log only when a device's health state CHANGES (ok→degraded or degraded→ok).
  - Routine healthy ticks are DEBUG only — no INFO spam every 5 s.
"""
import logging
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)

# Belt-and-suspenders metric freshness threshold.
# PES should already mark quality "stale" before this triggers,
# but this catches cases where PES stops writing entirely.
METRIC_AGE_WARN_S = 30


def _evaluate_device(d: dict, now_ms: int) -> dict:
    """
    Evaluate one device state dict against the three-layer health model.
    Returns a health snapshot dict.
    """
    status  = d.get("status", "unknown")
    error   = d.get("error")   # null → None, dict → active error
    metrics = d.get("metrics") or {}

    stale_metrics: list[str] = []
    error_metrics: list[str] = []
    aged_metrics:  list[str] = []

    for mkey, m in metrics.items():
        q = m.get("quality", "good")
        if q == "stale":
            stale_metrics.append(mkey)
        elif q == "error":
            error_metrics.append(mkey)

        # Belt-and-suspenders: metric's own timestamp
        mts = m.get("timestamp_ms")
        if mts and (now_ms - mts) / 1000 > METRIC_AGE_WARN_S:
            aged_metrics.append(mkey)

    anomaly = (
        status != "ok"
        or error is not None
        or bool(stale_metrics)
        or bool(error_metrics)
        or bool(aged_metrics)
    )

    return {
        "source":        d.get("source"),
        "device_id":     d.get("device_id"),
        "device_name":   d.get("name"),
        "last_seen_ms":  d.get("timestamp_ms") or 0,
        "status":        status,
        "has_error":     error is not None,
        "error_type":    error.get("type")    if error else None,
        "error_message": error.get("message") if error else None,
        "stale_metrics": stale_metrics,
        "error_metrics": error_metrics,
        "aged_metrics":  aged_metrics,
        "anomaly":       anomaly,
    }


def _health_level(snap: dict) -> str:
    """Map a snapshot to a single comparable health level string."""
    if snap["status"] == "error" or snap["error_metrics"]:
        return "error"
    if snap["status"] == "warning" or snap["stale_metrics"] or snap["has_error"] or snap["aged_metrics"]:
        return "warning"
    return "ok"


class ContinuityState:
    """Thread-safe in-memory device health tracker."""

    def __init__(self) -> None:
        self._lock    = threading.Lock()
        self._devices: dict[str, dict] = {}     # current health snapshot per device key
        self._prev_levels: dict[str, str] = {}  # previous health level per key (for change detection)
        self._last_updated: Optional[float] = None

    def update(self, devices: list[dict]) -> None:
        now_ms = int(time.time() * 1000)
        new: dict[str, dict] = {}

        for d in devices:
            key  = f"{d.get('source', '')}:{d.get('device_id', '')}"
            name = d.get("name", key)
            snap = _evaluate_device(d, now_ms)
            new[key] = snap

            current_level = _health_level(snap)
            prev_level    = self._prev_levels.get(key, "ok")

            if current_level != prev_level:
                # State changed — log at the appropriate level
                if current_level == "ok":
                    logger.info("Device %s  recovered → OK", name)
                elif current_level == "warning":
                    self._log_degraded(name, snap, "WARNING")
                else:
                    self._log_degraded(name, snap, "ERROR")
            else:
                # No change — DEBUG only (written to file, not console)
                logger.debug(
                    "Device %s  health=%s  (unchanged)",
                    name, current_level,
                )

        # Log summary only when something is actually wrong
        anomalies = sum(1 for v in new.values() if v["anomaly"])
        if anomalies:
            logger.warning(
                "Continuity: %d/%d device(s) have anomalies",
                anomalies, len(new),
            )
        else:
            logger.debug("Continuity: %d device(s), all healthy", len(new))

        with self._lock:
            self._prev_levels  = {k: _health_level(v) for k, v in new.items()}
            self._devices      = new
            self._last_updated = time.time()

    @staticmethod
    def _log_degraded(name: str, snap: dict, level: str) -> None:
        log = logger.warning if level == "WARNING" else logger.error
        log("Device %s  → %s  status=%s", name, level, snap["status"])
        if snap["has_error"]:
            log(
                "  └─ error  type=%-25s  msg=%s",
                snap["error_type"], snap["error_message"],
            )
        if snap["stale_metrics"]:
            log("  └─ stale metrics : %s", ", ".join(snap["stale_metrics"]))
        if snap["error_metrics"]:
            log("  └─ error metrics : %s", ", ".join(snap["error_metrics"]))
        if snap["aged_metrics"]:
            log(
                "  └─ aged  metrics : %s  (no refresh >%ds)",
                ", ".join(snap["aged_metrics"]), METRIC_AGE_WARN_S,
            )

    # ── Public API ────────────────────────────────────────────────────────

    def anomaly_count(self) -> int:
        with self._lock:
            return sum(1 for v in self._devices.values() if v["anomaly"])

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "devices":      dict(self._devices),
                "last_updated": self._last_updated,
            }
