"""
Tier 2 — Rolling Window Statistics Engine.

Materialises pre-computed 5-min / 1-hr / 24-hr aggregates into analytical.db
on every sensor-analytics tick so the dashboard can read them instantly
without running heavy queries on every page load.

Computation source: pes.db:sensor_samples (always current, always local).
Fallback to analytical.db:metric_archive when pes.db has been rotated.

Window recompute cadence (to avoid hammering SQLite on every 5-s tick):
  5min  → every 5 ticks  (~25 s)
  1hr   → every 12 ticks (~60 s)
  24hr  → every 60 ticks (~5 min)
"""
from __future__ import annotations

import logging
import math
import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from analytics_engine.sensor_store import SensorStore
    from analytics_engine.analytical_store import AnalyticalStore

logger = logging.getLogger(__name__)

_WINDOWS: dict[str, int] = {
    "5min":  5  * 60,
    "1hr":   60 * 60,
    "24hr":  24 * 60 * 60,
}

_COMPUTE_EVERY: dict[str, int] = {
    "5min":  5,
    "1hr":   12,
    "24hr":  60,
}


class StatsEngine:
    """Materialises rolling window statistics into analytical.db."""

    def __init__(
        self,
        sensor_store:     "SensorStore",
        analytical_store: "AnalyticalStore",
    ) -> None:
        self._sensor_store     = sensor_store
        self._analytical_store = analytical_store
        self._tick_count       = 0

    def tick(self, devices: list[dict]) -> None:
        """Called by the sensor-analytics worker every 5 s."""
        self._tick_count += 1
        if not devices:
            return

        conn = self._sensor_store._db()
        if conn is None:
            logger.debug("stats: pes.db unavailable — skipping tick")
            return

        try:
            for device in devices:
                src     = device.get("source", "")
                did     = device.get("device_id", "")
                metrics = list((device.get("metrics") or {}).keys())
                if not (src and did and metrics):
                    continue
                self._process_device(conn, src, did, metrics)
        except Exception as exc:
            logger.warning("stats: unexpected error during tick: %s", exc)
        finally:
            conn.close()

    def _process_device(
        self,
        conn,
        source:   str,
        device_id: str,
        metrics:  list[str],
    ) -> None:
        now_ms = int(time.time() * 1000)

        for window_name, window_secs in _WINDOWS.items():
            if self._tick_count % _COMPUTE_EVERY[window_name] != 0:
                continue

            since_ms = now_ms - window_secs * 1000

            for metric in metrics:
                try:
                    self._compute_and_store(conn, source, device_id, metric, window_name, since_ms)
                except Exception as exc:
                    logger.debug(
                        "stats: %s/%s/%s/%s failed: %s",
                        source, device_id, metric, window_name, exc,
                    )

    def _compute_and_store(
        self,
        conn,
        source:    str,
        device_id: str,
        metric:    str,
        window:    str,
        since_ms:  int,
    ) -> None:
        row = conn.execute(
            """
            SELECT
                COUNT(*)      AS n,
                SUM(CASE WHEN quality = 'good' THEN 1 ELSE 0 END) AS good_n,
                AVG(value)    AS avg_val,
                MIN(value)    AS min_val,
                MAX(value)    AS max_val,
                AVG(value * value) AS avg_sq
            FROM sensor_samples
            WHERE source    = ?
              AND device_id = ?
              AND metric    = ?
              AND timestamp_ms > ?
            """,
            (source, device_id, metric, since_ms),
        ).fetchone()

        if not row or not row["n"]:
            return

        n       = row["n"]
        good_n  = row["good_n"] or 0
        avg_val = row["avg_val"]
        min_val = row["min_val"]
        max_val = row["max_val"]
        avg_sq  = row["avg_sq"] or 0

        # Population stddev  =  sqrt( E[x²] – E[x]² )
        # Clamped to 0 to avoid floating-point negative sqrt input.
        variance = avg_sq - (avg_val ** 2) if avg_val is not None else 0
        stddev   = math.sqrt(max(0.0, variance)) if avg_val is not None else None

        def _r(v):
            return round(v, 5) if v is not None else None

        self._analytical_store.save_metric_stats({
            "source":       source,
            "device_id":    device_id,
            "metric_name":  metric,
            "window":       window,
            "avg":          _r(avg_val),
            "min":          _r(min_val),
            "max":          _r(max_val),
            "stddev":       _r(stddev),
            "sample_count": n,
            "good_count":   good_n,
        })

        logger.debug(
            "stats: %s/%s/%s [%s]  n=%d  avg=%.4g  stddev=%.4g",
            source, device_id, metric, window, n, avg_val or 0, stddev or 0,
        )
