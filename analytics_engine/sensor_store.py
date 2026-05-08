"""
Reads live sensor state from Redis and durable history from PES SQLite.
AES never writes to pes.db — SQLite is opened read-only.
"""
import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_DEVICE_INDEX_KEY = "pes:devices:index"
_DEFAULT_PES_DB   = Path("/opt/gateway/software_storage/PES/pes.db")


class SensorStore:
    def __init__(self, redis_client, db_path: Path | str | None = None) -> None:
        self._redis   = redis_client
        self._db_path = Path(db_path) if db_path else _DEFAULT_PES_DB
        self._redis_empty_count = 0   # consecutive live_devices() calls returning empty

    # ── SQLite ────────────────────────────────────────────────────────────

    def _db(self) -> Optional[sqlite3.Connection]:
        if not self._db_path.exists():
            logger.debug("PES database not found at %s — SQLite reads will return empty", self._db_path)
            return None
        try:
            conn = sqlite3.connect(
                f"file:{self._db_path}?mode=ro",
                uri=True,
                timeout=3.0,
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            return conn
        except Exception as exc:
            logger.warning("PES database open failed: %s", exc)
            return None

    # ── Live state (Redis) ────────────────────────────────────────────────

    def live_devices(self) -> list[dict]:
        """Read pes:devices:index then each listed device state key."""
        raw = self._redis.get_full(_DEVICE_INDEX_KEY)
        if not raw:
            self._redis_empty_count += 1
            # Escalating warnings: first at 3 consecutive misses (~15s), then every 12 (~60s)
            if self._redis_empty_count == 3:
                logger.warning(
                    "Redis: pes:devices:index empty for %d consecutive reads — "
                    "is PES running? Is Redis reachable?",
                    self._redis_empty_count,
                )
            elif self._redis_empty_count > 3 and self._redis_empty_count % 12 == 0:
                logger.warning(
                    "Redis: still no device data after %d reads (~%ds) — "
                    "PES may be down or not yet connected to hardware",
                    self._redis_empty_count, self._redis_empty_count * 5,
                )
            else:
                logger.debug("Redis: pes:devices:index returned empty — no active devices")
            return []

        if self._redis_empty_count >= 3:
            logger.info(
                "Redis: device data resumed after %d empty reads (~%ds)",
                self._redis_empty_count, self._redis_empty_count * 5,
            )
        self._redis_empty_count = 0

        try:
            index = json.loads(raw)
        except Exception as exc:
            logger.warning("Redis: failed to parse pes:devices:index: %s", exc)
            return []

        entries = index.get("devices", [])
        logger.debug("Redis: device index has %d device(s)", len(entries))

        result = []
        for entry in entries:
            key       = entry.get("key", "")
            state_raw = self._redis.get_full(key)
            if not state_raw:
                logger.warning("Redis: state key %s returned empty", key)
                continue
            try:
                state   = json.loads(state_raw)
                self._log_device_state(state, key)
                result.append(state)
            except Exception as exc:
                logger.warning("Redis: failed to parse state for %s: %s", key, exc)

        logger.info("live_devices: %d device(s) active", len(result))
        return result

    def _log_device_state(self, state: dict, key: str) -> None:
        """
        Log one device state snapshot using the three-layer health model:
          Layer 1 — device.status      (ok / warning / error)
          Layer 2 — device.error       (null or active error block from PES)
          Layer 3 — per-metric.quality (good / stale / error — authoritative)

        Zeros are NOT treated as anomalies (doc rule: zeros are valid sensor values).
        """
        metrics = state.get("metrics") or {}
        status  = state.get("status", "?")
        error   = state.get("error")        # None when healthy, dict when PES reports a fault
        name    = state.get("name", key)
        now_ms  = int(time.time() * 1000)
        age_ms  = now_ms - (state.get("timestamp_ms") or 0)

        # Layer 3 — classify metrics by quality
        stale_metrics = {k: m for k, m in metrics.items() if m.get("quality") == "stale"}
        error_metrics = {k: m for k, m in metrics.items() if m.get("quality") == "error"}

        # Belt-and-suspenders: metrics whose own timestamp_ms is very old
        # (covers cases where PES stops writing entirely without changing quality)
        aged_metrics = {
            k: m for k, m in metrics.items()
            if m.get("timestamp_ms") and (now_ms - m["timestamp_ms"]) / 1000 > 30
        }

        # Compact metric summary: first 4 metrics with value + quality flag
        parts = []
        for k, m in list(metrics.items())[:4]:
            q   = m.get("quality", "?")
            val = m.get("value")
            v   = f"{val:.5g}" if isinstance(val, (int, float)) else str(val)
            flag = "" if q == "good" else f"[{q.upper()}]"
            parts.append(f"{k}={v}{flag}")
        summary = "  ".join(parts)
        if len(metrics) > 4:
            summary += f"  (+{len(metrics) - 4} more)"

        logger.info(
            "Device %-28s  status=%-8s  age=%4dms  %s",
            name, status, age_ms, summary,
        )

        # Layer 2 — device error block set by PES
        if error:
            details = error.get("details") or {}
            logger.warning(
                "  └─ PES ERROR  type=%-20s  severity=%-8s  msg=%s",
                error.get("type", "?"),
                error.get("severity", "?"),
                error.get("message", ""),
            )
            if details.get("consecutive_failures"):
                logger.warning(
                    "  └─ consecutive_failures=%s  last_error=%s",
                    details.get("consecutive_failures"),
                    details.get("last_error", "?"),
                )

        # Layer 3 — stale metrics (PES kept last known value but it is old)
        for mkey, m in stale_metrics.items():
            logger.warning(
                "  └─ STALE  %-22s  last_val=%-12s  metric_age=%dms",
                mkey,
                m.get("value"),
                now_ms - (m.get("timestamp_ms") or now_ms),
            )

        # Layer 3 — error metrics (this metric failed in the latest poll)
        for mkey, m in error_metrics.items():
            logger.warning(
                "  └─ METRIC ERROR  %-22s  last_val=%s",
                mkey,
                m.get("value"),
            )

        # Belt-and-suspenders aged metrics
        for mkey, m in aged_metrics.items():
            if mkey not in stale_metrics and mkey not in error_metrics:
                logger.warning(
                    "  └─ AGED (no PES refresh)  %-22s  metric_age=%.0fs",
                    mkey,
                    (now_ms - m["timestamp_ms"]) / 1000,
                )

        # Full per-metric detail at DEBUG (log file only — not console)
        for mkey, m in metrics.items():
            val     = m.get("value")
            unit    = (m.get("unit") or "").strip()
            quality = m.get("quality", "?")
            mage    = f"  mage={(now_ms - m['timestamp_ms'])//1000}s" if m.get("timestamp_ms") else ""
            val_str = f"{val:.5g}" if isinstance(val, (int, float)) else str(val)
            logger.debug("    %-24s = %-12s %-8s [%s]%s", mkey, val_str, unit, quality, mage)

    def device_samples(self, source: str, device_id: str, limit: int = 100) -> list[dict]:
        """Read the rolling Redis sample buffer for one device."""
        key      = f"pes:device:{source}:{device_id}:samples"
        raw_list = self._redis.lrange(key, 0, limit - 1)
        samples  = []
        for raw in raw_list:
            try:
                samples.append(json.loads(raw))
            except Exception:
                pass
        logger.debug("Redis: %s → %d sample(s) in rolling buffer", key, len(samples))
        return samples

    def device_samples_per_metric(
        self,
        source: str,
        device_id: str,
        limit: int = 60,
    ) -> dict[str, list[float]]:
        """Extract per-metric float arrays from the Redis rolling buffer (oldest → newest)."""
        raw = self.device_samples(source, device_id, limit)
        result: dict[str, list] = {}
        for sample in reversed(raw):
            for mKey, m in (sample.get("metrics") or {}).items():
                if mKey not in result:
                    result[mKey] = []
                v = m.get("value")
                if v is not None and m.get("quality") == "good":
                    try:
                        result[mKey].append(float(v))
                    except (TypeError, ValueError):
                        pass
        return result

    # ── History (SQLite) ──────────────────────────────────────────────────

    def metric_history(
        self,
        source: str,
        device_id: str,
        metric: str,
        window_hours: int = 24,
        buckets: int = 120,
    ) -> dict:
        """Time-bucketed avg/min/max from sensor_samples."""
        empty: dict = {"timestamps": [], "avg": [], "min": [], "max": [], "count": []}
        conn = self._db()
        if conn is None:
            return empty

        now_ms    = int(time.time() * 1000)
        since_ms  = now_ms - window_hours * 3_600_000
        bucket_ms = max(1, (window_hours * 3_600_000) // buckets)

        logger.debug(
            "SQLite: metric_history %s/%s/%s  window=%dh  bucket=%dms",
            source, device_id, metric, window_hours, bucket_ms,
        )

        try:
            cur = conn.execute(
                """
                SELECT
                    (timestamp_ms / :bms) * :bms AS bucket,
                    AVG(value)  AS avg_val,
                    MIN(value)  AS min_val,
                    MAX(value)  AS max_val,
                    COUNT(*)    AS cnt
                FROM sensor_samples
                WHERE source    = :src
                  AND device_id = :did
                  AND metric    = :metric
                  AND timestamp_ms > :since
                  AND quality   = 'good'
                GROUP BY bucket
                ORDER BY bucket
                """,
                {"bms": bucket_ms, "src": source, "did": device_id,
                 "metric": metric, "since": since_ms},
            )
            rows = cur.fetchall()
            logger.info(
                "SQLite: metric_history %s/%s/%s  window=%dh → %d bucket(s)",
                source, device_id, metric, window_hours, len(rows),
            )
        except Exception as exc:
            logger.warning("SQLite: metric_history query failed: %s", exc)
            rows = []
        finally:
            conn.close()

        def _r(v):
            return round(v, 4) if v is not None else None

        return {
            "timestamps": [r["bucket"]    for r in rows],
            "avg":        [_r(r["avg_val"]) for r in rows],
            "min":        [_r(r["min_val"]) for r in rows],
            "max":        [_r(r["max_val"]) for r in rows],
            "count":      [r["cnt"]        for r in rows],
        }

    def recent_events(
        self,
        limit: int = 100,
        source: Optional[str] = None,
        device_id: Optional[str] = None,
        since_ms: Optional[int] = None,
    ) -> list[dict]:
        """Latest rows from sensor_events, newest first."""
        conn = self._db()
        if conn is None:
            return []

        conditions: list[str] = []
        params: dict = {"limit": limit}
        if source:
            conditions.append("source = :source")
            params["source"] = source
        if device_id:
            conditions.append("device_id = :device_id")
            params["device_id"] = device_id
        if since_ms:
            conditions.append("timestamp_ms > :since_ms")
            params["since_ms"] = since_ms

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        try:
            cur = conn.execute(
                f"""
                SELECT id, timestamp_ms, source, device_id, device_name, device_type,
                       severity, event_type, message, details_json
                FROM sensor_events
                {where}
                ORDER BY timestamp_ms DESC
                LIMIT :limit
                """,
                params,
            )
            rows = [dict(r) for r in cur.fetchall()]
            logger.info("SQLite: recent_events → %d row(s)  (filter: source=%s device_id=%s)", len(rows), source, device_id)
        except Exception as exc:
            logger.warning("SQLite: recent_events query failed: %s", exc)
            rows = []
        finally:
            conn.close()

        return rows

    # ── Summary ───────────────────────────────────────────────────────────

    def summary_stats(self, devices: Optional[list[dict]] = None) -> dict:
        if devices is None:
            devices = self.live_devices()

        now_ms        = int(time.time() * 1000)
        total_metrics = 0
        good_metrics  = 0
        anomaly_count = 0

        for device in devices:
            status = device.get("status", "ok")
            error  = device.get("error")
            has_anomaly = status != "ok" or error is not None
            for m in (device.get("metrics") or {}).values():
                total_metrics += 1
                q = m.get("quality", "good")
                if q == "good":
                    good_metrics += 1
                else:
                    has_anomaly = True   # stale or error metric counts as anomaly
            if has_anomaly:
                anomaly_count += 1

        quality_pct = round(100 * good_metrics / total_metrics) if total_metrics else 100

        last_event_ms: Optional[int] = None
        conn = self._db()
        if conn:
            try:
                cur = conn.execute("SELECT MAX(timestamp_ms) AS mx FROM sensor_events")
                row = cur.fetchone()
                if row and row["mx"]:
                    last_event_ms = row["mx"]
            except Exception:
                pass
            finally:
                conn.close()

        stats = {
            "active_devices": len(devices),
            "quality_pct":    quality_pct,
            "anomaly_count":  anomaly_count,
            "last_event_ms":  last_event_ms,
        }
        logger.debug("summary_stats: %s", stats)
        return stats
