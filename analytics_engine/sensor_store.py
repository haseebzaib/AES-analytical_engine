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
_DEFAULT_PES_DB = Path("/opt/gateway/software_storage/PES/pes.db")


class SensorStore:
    def __init__(self, redis_client, db_path: Path | str | None = None) -> None:
        self._redis = redis_client
        self._db_path = Path(db_path) if db_path else _DEFAULT_PES_DB

    # ── SQLite ────────────────────────────────────────────────────────────

    def _db(self) -> Optional[sqlite3.Connection]:
        if not self._db_path.exists():
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
            logger.debug("PES db open failed: %s", exc)
            return None

    # ── Live state (Redis) ────────────────────────────────────────────────

    def live_devices(self) -> list[dict]:
        """Read pes:devices:index then each listed device state key."""
        raw = self._redis.get_full(_DEVICE_INDEX_KEY)
        if not raw:
            return []
        try:
            index = json.loads(raw)
        except Exception:
            return []

        result = []
        for entry in index.get("devices", []):
            state_raw = self._redis.get_full(entry.get("key", ""))
            if not state_raw:
                continue
            try:
                result.append(json.loads(state_raw))
            except Exception:
                continue
        return result

    def device_samples(self, source: str, device_id: str, limit: int = 100) -> list[dict]:
        """Read the rolling Redis sample buffer for one device."""
        key = f"pes:device:{source}:{device_id}:samples"
        raw_list = self._redis.lrange(key, 0, limit - 1)
        samples = []
        for raw in raw_list:
            try:
                samples.append(json.loads(raw))
            except Exception:
                pass
        return samples

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

        now_ms = int(time.time() * 1000)
        since_ms = now_ms - window_hours * 3_600_000
        bucket_ms = max(1, (window_hours * 3_600_000) // buckets)

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
                {
                    "bms": bucket_ms,
                    "src": source,
                    "did": device_id,
                    "metric": metric,
                    "since": since_ms,
                },
            )
            rows = cur.fetchall()
        except Exception as exc:
            logger.debug("metric_history query failed: %s", exc)
            rows = []
        finally:
            conn.close()

        def _r(v):
            return round(v, 4) if v is not None else None

        return {
            "timestamps": [r["bucket"] for r in rows],
            "avg":        [_r(r["avg_val"]) for r in rows],
            "min":        [_r(r["min_val"]) for r in rows],
            "max":        [_r(r["max_val"]) for r in rows],
            "count":      [r["cnt"] for r in rows],
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
        except Exception as exc:
            logger.debug("recent_events query failed: %s", exc)
            rows = []
        finally:
            conn.close()

        return rows

    # ── Summary ───────────────────────────────────────────────────────────

    def summary_stats(self, devices: Optional[list[dict]] = None) -> dict:
        """Compute KPI summary from live device state."""
        if devices is None:
            devices = self.live_devices()

        now_ms = int(time.time() * 1000)
        total_metrics = 0
        good_metrics = 0
        anomaly_count = 0

        for device in devices:
            for m in (device.get("metrics") or {}).values():
                total_metrics += 1
                if m.get("quality") == "good":
                    good_metrics += 1
            status = device.get("status", "ok")
            age_s = (now_ms - (device.get("timestamp_ms") or 0)) / 1000
            if status in ("error", "warning") or age_s > 120:
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

        return {
            "active_devices": len(devices),
            "quality_pct":    quality_pct,
            "anomaly_count":  anomaly_count,
            "last_event_ms":  last_event_ms,
        }
