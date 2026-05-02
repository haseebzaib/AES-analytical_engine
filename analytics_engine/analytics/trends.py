"""
Tier 3 — Trend Detection Engine.

Computes linear regression slope from the Redis rolling sample buffer
(up to 60 samples at ~5 s intervals = ~5 min of data) for every active metric.

Outputs per-metric:
  direction  — "rising" | "falling" | "stable"
  slope      — units per minute (positive = rising, negative = falling)

The slope is stored in analytical.db:trend_snapshots so the dashboard and
alert rules engine can consume it without re-computing every request.

Sensitivity (% change per minute to classify as rising/falling):
  low    → 10 %    (only large, obvious trends)
  medium →  3 %    (default — balanced)
  high   →  1 %    (catches gentle drifts)

The sensitivity used for DB classification is medium.  The UI lets the operator
pick a different sensitivity client-side without changing stored data, because
the raw slope is always available and the client re-classifies on the fly.

Time-to-threshold:
  Not computed server-side (we don't have the current live value here).
  The API endpoint enriches trend snapshots with TTT using the live Redis value
  and the enabled alert rules for that metric.
"""
from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from analytics_engine.sensor_store import SensorStore
    from analytics_engine.analytical_store import AnalyticalStore

logger = logging.getLogger(__name__)

# Slope threshold for medium sensitivity: if |slope| > N% of current value/min → not stable.
_SENSITIVITY_PCT: dict[str, float] = {
    "low":    10.0,
    "medium":  3.0,
    "high":    1.0,
}
_DEFAULT_SENSITIVITY = "medium"

# Minimum samples required for a meaningful trend estimate.
_MIN_SAMPLES = 8

# Samples per minute at a 5-second poll interval.
_SAMPLES_PER_MINUTE = 12.0


def _classify(slope_per_min: float, current_value: float, sensitivity: str = _DEFAULT_SENSITIVITY) -> str:
    """Return 'rising', 'falling', or 'stable' based on slope and sensitivity."""
    threshold_pct = _SENSITIVITY_PCT.get(sensitivity, _SENSITIVITY_PCT[_DEFAULT_SENSITIVITY])
    if current_value == 0:
        # Avoid divide-by-zero: use absolute threshold of 0.001 units/min
        return "rising" if slope_per_min > 0.001 else "falling" if slope_per_min < -0.001 else "stable"
    pct_per_min = abs(slope_per_min / current_value) * 100
    if pct_per_min < threshold_pct:
        return "stable"
    return "rising" if slope_per_min > 0 else "falling"


def _linear_slope(values: list[float]) -> float:
    """
    Ordinary least-squares slope of values against a uniform time index.
    Returns slope in units-per-sample.  Multiply by _SAMPLES_PER_MINUTE for /min.
    """
    n = len(values)
    if n < 2:
        return 0.0
    sum_x  = n * (n - 1) / 2          # 0+1+…+(n-1)
    sum_x2 = n * (n - 1) * (2*n - 1) / 6
    sum_y  = sum(values)
    sum_xy = sum(i * v for i, v in enumerate(values))
    denom  = n * sum_x2 - sum_x ** 2
    if abs(denom) < 1e-12:
        return 0.0
    return (n * sum_xy - sum_x * sum_y) / denom


class TrendsEngine:
    """Computes metric-level trend snapshots from the Redis rolling buffer."""

    def __init__(
        self,
        sensor_store:     "SensorStore",
        analytical_store: "AnalyticalStore",
    ) -> None:
        self._sensor_store     = sensor_store
        self._analytical_store = analytical_store

    # ── Public entry point ────────────────────────────────────────────────────

    def tick(self, devices: list[dict]) -> None:
        """Called by the sensor-analytics worker every 5 s."""
        for device in devices:
            src = device.get("source", "")
            did = device.get("device_id", "")
            if not (src and did):
                continue
            try:
                self._process_device(src, did, device.get("metrics") or {})
            except Exception as exc:
                logger.debug("trends: error for %s/%s: %s", src, did, exc)

    # ── Internals ─────────────────────────────────────────────────────────────

    def _process_device(
        self,
        source:    str,
        device_id: str,
        live_metrics: dict,
    ) -> None:
        samples_by_metric = self._sensor_store.device_samples_per_metric(
            source, device_id, limit=60
        )

        now_ms = int(time.time() * 1000)

        for metric_name, values in samples_by_metric.items():
            if len(values) < _MIN_SAMPLES:
                continue

            slope_per_sample = _linear_slope(values)
            slope_per_min    = slope_per_sample * _SAMPLES_PER_MINUTE

            current_value = values[-1]
            direction     = _classify(slope_per_min, current_value)

            self._analytical_store.save_trend_snapshot({
                "source":       source,
                "device_id":    device_id,
                "metric_name":  metric_name,
                "direction":    direction,
                "slope":        round(slope_per_min, 6),
                "computed_at":  now_ms,
            })

            logger.debug(
                "trends: %s/%s/%s  dir=%-8s  slope=%+.4g/min",
                source, device_id, metric_name, direction, slope_per_min,
            )


# ── Helper used by the API endpoint ──────────────────────────────────────────

def enrich_with_ttt(
    trends:      list[dict],
    live_values: dict[str, float],   # metric_name → current value
    alert_rules: list[dict],
) -> list[dict]:
    """
    Add time-to-threshold (TTT) estimates to trend dicts.

    For each trend, if there is an enabled alert rule whose threshold the
    current slope is moving toward, compute how many minutes until the
    value would cross the threshold at the current rate.

    Mutates and returns the input list.
    """
    rules_by_metric: dict[str, list[dict]] = {}
    for r in alert_rules:
        rules_by_metric.setdefault(r["metric_name"], []).append(r)

    for t in trends:
        mname  = t["metric_name"]
        slope  = t.get("slope", 0.0)        # units/minute
        curr   = live_values.get(mname)
        t["ttt_minutes"] = None
        t["ttt_rule"]    = None

        if slope == 0 or curr is None:
            continue

        for rule in rules_by_metric.get(mname, []):
            if not rule.get("enabled"):
                continue
            thresh = float(rule["threshold"])
            cond   = rule["condition"]

            # Only forecast when moving toward the threshold
            moving_toward = (
                (cond in ("gt", "gte") and slope > 0 and curr < thresh) or
                (cond in ("lt", "lte") and slope < 0 and curr > thresh)
            )
            if not moving_toward:
                continue

            delta = thresh - curr           # positive if threshold is above current
            ttt   = delta / slope           # minutes (slope is units/min)
            if ttt > 0:
                t["ttt_minutes"] = round(ttt, 1)
                t["ttt_rule"]    = {
                    "rule_id":   rule["id"],
                    "threshold": thresh,
                    "severity":  rule.get("severity", "warning"),
                    "condition": cond,
                }
                break  # report the nearest threshold only

    return trends
