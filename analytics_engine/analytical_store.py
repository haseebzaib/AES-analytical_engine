"""
AES-owned analytical database.

Rules:
  - AES creates and exclusively owns this file. PES never touches it.
  - Every public method is safe to call even if the DB file was deleted — it
    auto-recreates the file and all tables on the next access.
  - All exceptions are caught and logged; callers never see a crash.
  - File size is capped at 5 GB. When breached, the oldest metric_archive rows
    are pruned in chunks until the file is back under 85 % of the cap.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time
from pathlib import Path

logger = logging.getLogger(__name__)

_CAP_BYTES    = 5 * 1024 * 1024 * 1024        # 5 GB hard cap
_TARGET_BYTES = int(_CAP_BYTES * 0.85)         # prune back to 85 %
_PRUNE_CHUNK  = 50_000                         # rows deleted per iteration


class AnalyticalStore:
    """Thread-safe SQLite store for AES-derived analytics data."""

    def __init__(self, db_path: Path | str) -> None:
        self._db_path = Path(db_path)
        self._lock    = threading.Lock()
        # Parent dir may not exist yet on a fresh device
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        # Verify tables exist (creates file if missing)
        self._ensure_tables()

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _open(self) -> sqlite3.Connection | None:
        """
        Open the database, creating the file if it does not exist.
        Always runs CREATE TABLE IF NOT EXISTS so the schema is correct even
        after the file was deleted mid-run.
        Returns None only if SQLite itself is broken.
        """
        try:
            conn = sqlite3.connect(
                str(self._db_path),
                timeout=5.0,
                check_same_thread=False,
            )
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous  = NORMAL")
            conn.execute("PRAGMA foreign_keys = ON")
            conn.row_factory = sqlite3.Row
            # Fast schema check — no-op when tables already exist
            self._create_tables(conn)
            return conn
        except Exception as exc:
            logger.error("analytical.db: open failed: %s", exc)
            return None

    def _create_tables(self, conn: sqlite3.Connection) -> None:
        """Idempotent DDL — safe to call every open()."""
        conn.executescript("""
            -- ── Long-term sample archive ──────────────────────────────────
            -- Rows harvested from pes.db:sensor_samples before PES rotation.
            CREATE TABLE IF NOT EXISTS metric_archive (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                source       TEXT    NOT NULL,
                device_id    TEXT    NOT NULL,
                metric_name  TEXT    NOT NULL,
                value        REAL    NOT NULL,
                unit         TEXT    NOT NULL DEFAULT '',
                quality      TEXT    NOT NULL DEFAULT 'good',
                timestamp_ms INTEGER NOT NULL,
                harvested_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_arc_ts
                ON metric_archive(source, device_id, metric_name, timestamp_ms);

            -- ── Harvest progress ──────────────────────────────────────────
            -- Tracks the last pes.db rowid copied per (source, device_id).
            CREATE TABLE IF NOT EXISTS harvest_cursor (
                source      TEXT    NOT NULL,
                device_id   TEXT    NOT NULL,
                last_row_id INTEGER NOT NULL DEFAULT 0,
                updated_at  INTEGER NOT NULL,
                PRIMARY KEY (source, device_id)
            );

            -- ── Alert rules (Tier 1 — user-configured) ───────────────────
            CREATE TABLE IF NOT EXISTS alert_rules (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                source      TEXT    NOT NULL,
                device_id   TEXT    NOT NULL,
                metric_name TEXT    NOT NULL,
                condition   TEXT    NOT NULL,   -- gt / lt / gte / lte / eq
                threshold   REAL    NOT NULL,
                severity    TEXT    NOT NULL DEFAULT 'warning',
                enabled     INTEGER NOT NULL DEFAULT 1,
                created_at  INTEGER NOT NULL
            );

            -- ── Alert events (Tier 1 — fired / resolved) ─────────────────
            CREATE TABLE IF NOT EXISTS alert_events (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_id        INTEGER,          -- NULL for device-level events
                source         TEXT    NOT NULL,
                device_id      TEXT    NOT NULL,
                metric_name    TEXT,
                event_type     TEXT    NOT NULL, -- fired / resolved / stale / error / offline
                severity       TEXT    NOT NULL DEFAULT 'warning',
                message        TEXT    NOT NULL DEFAULT '',
                value_at_event REAL,
                timestamp_ms   INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_aev_ts
                ON alert_events(timestamp_ms);
            CREATE INDEX IF NOT EXISTS idx_aev_device
                ON alert_events(source, device_id, timestamp_ms);

            -- ── Materialised rolling stats (Tier 2) ───────────────────────
            CREATE TABLE IF NOT EXISTS metric_stats (
                source        TEXT    NOT NULL,
                device_id     TEXT    NOT NULL,
                metric_name   TEXT    NOT NULL,
                window        TEXT    NOT NULL, -- 5min / 1hr / 24hr
                avg           REAL,
                min           REAL,
                max           REAL,
                stddev        REAL,
                sample_count  INTEGER NOT NULL DEFAULT 0,
                good_count    INTEGER NOT NULL DEFAULT 0,
                computed_at   INTEGER NOT NULL,
                PRIMARY KEY (source, device_id, metric_name, window)
            );

            -- ── Trend snapshots (Tier 3) ──────────────────────────────────
            CREATE TABLE IF NOT EXISTS trend_snapshots (
                source       TEXT    NOT NULL,
                device_id    TEXT    NOT NULL,
                metric_name  TEXT    NOT NULL,
                direction    TEXT    NOT NULL, -- rising / falling / stable
                slope        REAL,             -- units per minute
                computed_at  INTEGER NOT NULL,
                PRIMARY KEY (source, device_id, metric_name)
            );
        """)
        conn.commit()
        # Schema migration: add n_samples column if it doesn't exist yet
        try:
            conn.execute("ALTER TABLE trend_snapshots ADD COLUMN n_samples INTEGER")
            conn.commit()
        except Exception:
            pass  # column already exists

    def _ensure_tables(self) -> None:
        """Called once at startup to guarantee the schema is in place."""
        conn = self._open()
        if conn:
            conn.close()

    # ── Size cap ─────────────────────────────────────────────────────────────

    def check_and_prune(self) -> None:
        """
        Prune oldest metric_archive rows when the DB file exceeds 5 GB.
        Designed to be called once per archival tick — not on every write.
        """
        try:
            size = self._db_path.stat().st_size
        except OSError:
            return
        if size <= _CAP_BYTES:
            return

        logger.warning(
            "analytical.db: size %.2f GB exceeds 5 GB cap — pruning oldest rows",
            size / 1024 ** 3,
        )
        conn = self._open()
        if conn is None:
            return
        try:
            while True:
                conn.execute(
                    "DELETE FROM metric_archive WHERE id IN "
                    "(SELECT id FROM metric_archive ORDER BY id ASC LIMIT ?)",
                    (_PRUNE_CHUNK,),
                )
                conn.commit()
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                try:
                    new_size = self._db_path.stat().st_size
                except OSError:
                    break
                if new_size <= _TARGET_BYTES:
                    logger.info(
                        "analytical.db: pruned to %.2f GB", new_size / 1024 ** 3
                    )
                    break
        except Exception as exc:
            logger.error("analytical.db: prune failed: %s", exc)
        finally:
            conn.close()

    # ── Archival: cursor tracking ─────────────────────────────────────────────

    def get_harvest_cursor(self, source: str, device_id: str) -> int:
        """Return the last pes.db rowid harvested for this device (0 = never)."""
        conn = self._open()
        if conn is None:
            return 0
        try:
            row = conn.execute(
                "SELECT last_row_id FROM harvest_cursor "
                "WHERE source = ? AND device_id = ?",
                (source, device_id),
            ).fetchone()
            return int(row["last_row_id"]) if row else 0
        except Exception as exc:
            logger.warning("analytical.db: get_harvest_cursor failed: %s", exc)
            return 0
        finally:
            conn.close()

    def update_harvest_cursor(
        self, source: str, device_id: str, last_row_id: int
    ) -> None:
        """Upsert the harvest cursor for one device."""
        with self._lock:
            conn = self._open()
            if conn is None:
                return
            try:
                conn.execute(
                    """
                    INSERT INTO harvest_cursor (source, device_id, last_row_id, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(source, device_id) DO UPDATE SET
                        last_row_id = excluded.last_row_id,
                        updated_at  = excluded.updated_at
                    """,
                    (source, device_id, last_row_id, int(time.time() * 1000)),
                )
                conn.commit()
            except Exception as exc:
                logger.warning("analytical.db: update_harvest_cursor failed: %s", exc)
            finally:
                conn.close()

    # ── Archival: batch insert ────────────────────────────────────────────────

    def append_archive_batch(self, rows: list[dict]) -> int:
        """
        Insert harvested rows into metric_archive.

        Expected keys per row:
          source, device_id, metric_name, value, unit, quality, timestamp_ms

        Returns count of rows inserted (0 on error or empty input).
        """
        if not rows:
            return 0
        now      = int(time.time() * 1000)
        inserted = 0
        with self._lock:
            conn = self._open()
            if conn is None:
                return 0
            try:
                conn.executemany(
                    """
                    INSERT INTO metric_archive
                        (source, device_id, metric_name, value, unit,
                         quality, timestamp_ms, harvested_at)
                    VALUES
                        (:source, :device_id, :metric_name, :value, :unit,
                         :quality, :timestamp_ms, :harvested_at)
                    """,
                    [{**r, "harvested_at": now} for r in rows],
                )
                conn.commit()
                inserted = len(rows)
            except Exception as exc:
                logger.error("analytical.db: append_archive_batch failed: %s", exc)
            finally:
                conn.close()
        return inserted

    # ── History queries ───────────────────────────────────────────────────────

    def metric_history_bucketed(
        self,
        source: str,
        device_id: str,
        metric_name: str,
        window_hours: int = 24,
        buckets: int = 200,
    ) -> dict:
        """
        Time-bucketed avg / min / max from metric_archive.
        Returns the same dict shape as SensorStore.metric_history() so callers
        can switch between the two sources transparently.
        """
        empty: dict = {
            "timestamps": [], "avg": [], "min": [], "max": [], "count": []
        }
        conn = self._open()
        if conn is None:
            return empty

        now_ms   = int(time.time() * 1000)
        since_ms = now_ms - window_hours * 3_600_000
        bms      = max(1, (window_hours * 3_600_000) // buckets)

        try:
            cur = conn.execute(
                """
                SELECT
                    (timestamp_ms / :bms) * :bms AS bucket,
                    AVG(value)  AS avg_val,
                    MIN(value)  AS min_val,
                    MAX(value)  AS max_val,
                    COUNT(*)    AS cnt
                FROM metric_archive
                WHERE source      = :src
                  AND device_id   = :did
                  AND metric_name = :metric
                  AND timestamp_ms > :since
                  AND quality     = 'good'
                GROUP BY bucket
                ORDER BY bucket
                """,
                {
                    "bms":    bms,
                    "src":    source,
                    "did":    device_id,
                    "metric": metric_name,
                    "since":  since_ms,
                },
            )
            rows = cur.fetchall()
        except Exception as exc:
            logger.warning("analytical.db: metric_history_bucketed failed: %s", exc)
            rows = []
        finally:
            conn.close()

        def _r(v):
            return round(v, 4) if v is not None else None

        return {
            "timestamps": [r["bucket"]       for r in rows],
            "avg":        [_r(r["avg_val"])   for r in rows],
            "min":        [_r(r["min_val"])   for r in rows],
            "max":        [_r(r["max_val"])   for r in rows],
            "count":      [r["cnt"]           for r in rows],
        }

    def archive_row_count(self) -> int:
        """Total rows in metric_archive. Used for diagnostics."""
        conn = self._open()
        if conn is None:
            return 0
        try:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM metric_archive"
            ).fetchone()
            return int(row["n"]) if row else 0
        except Exception:
            return 0
        finally:
            conn.close()

    # ── Alert rules (Tier 1) ──────────────────────────────────────────────────

    def get_alert_rules(
        self,
        source: str | None = None,
        device_id: str | None = None,
        enabled_only: bool = False,
    ) -> list[dict]:
        """Return all configured alert rules, optionally filtered."""
        conn = self._open()
        if conn is None:
            return []
        conditions: list[str] = []
        params: list = []
        if source:
            conditions.append("source = ?"); params.append(source)
        if device_id:
            conditions.append("device_id = ?"); params.append(device_id)
        if enabled_only:
            conditions.append("enabled = 1")
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        try:
            rows = conn.execute(
                f"SELECT * FROM alert_rules {where} ORDER BY id ASC", params
            ).fetchall()
            return [dict(r) for r in rows]
        except Exception as exc:
            logger.warning("analytical.db: get_alert_rules failed: %s", exc)
            return []
        finally:
            conn.close()

    def create_alert_rule(self, rule: dict) -> int:
        """
        Insert a new alert rule. Expected keys:
          source, device_id, metric_name, condition, threshold, severity
        Returns the new rule id, or -1 on failure.
        """
        with self._lock:
            conn = self._open()
            if conn is None:
                return -1
            try:
                cur = conn.execute(
                    """
                    INSERT INTO alert_rules
                        (source, device_id, metric_name, condition, threshold, severity, enabled, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, 1, ?)
                    """,
                    (
                        rule["source"], rule["device_id"], rule["metric_name"],
                        rule["condition"], float(rule["threshold"]),
                        rule.get("severity", "warning"),
                        int(time.time() * 1000),
                    ),
                )
                conn.commit()
                return cur.lastrowid or -1
            except Exception as exc:
                logger.error("analytical.db: create_alert_rule failed: %s", exc)
                return -1
            finally:
                conn.close()

    def delete_alert_rule(self, rule_id: int) -> bool:
        """Delete a rule by id. Returns True on success."""
        with self._lock:
            conn = self._open()
            if conn is None:
                return False
            try:
                conn.execute("DELETE FROM alert_rules WHERE id = ?", (rule_id,))
                conn.commit()
                return True
            except Exception as exc:
                logger.warning("analytical.db: delete_alert_rule failed: %s", exc)
                return False
            finally:
                conn.close()

    def set_rule_enabled(self, rule_id: int, enabled: bool) -> bool:
        """Enable or disable a rule. Returns True on success."""
        with self._lock:
            conn = self._open()
            if conn is None:
                return False
            try:
                conn.execute(
                    "UPDATE alert_rules SET enabled = ? WHERE id = ?",
                    (1 if enabled else 0, rule_id),
                )
                conn.commit()
                return True
            except Exception as exc:
                logger.warning("analytical.db: set_rule_enabled failed: %s", exc)
                return False
            finally:
                conn.close()

    # ── Alert events (Tier 1) ─────────────────────────────────────────────────

    def add_alert_event(self, event: dict) -> None:
        """
        Write one alert event. Expected keys:
          rule_id (optional), source, device_id, metric_name (optional),
          event_type, severity, message, value_at_event (optional), timestamp_ms
        """
        with self._lock:
            conn = self._open()
            if conn is None:
                return
            try:
                conn.execute(
                    """
                    INSERT INTO alert_events
                        (rule_id, source, device_id, metric_name,
                         event_type, severity, message, value_at_event, timestamp_ms)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.get("rule_id"),
                        event["source"],
                        event["device_id"],
                        event.get("metric_name"),
                        event["event_type"],
                        event.get("severity", "warning"),
                        event.get("message", ""),
                        event.get("value_at_event"),
                        event["timestamp_ms"],
                    ),
                )
                conn.commit()
            except Exception as exc:
                logger.error("analytical.db: add_alert_event failed: %s", exc)
            finally:
                conn.close()

    def get_alert_events(
        self,
        source: str | None = None,
        device_id: str | None = None,
        since_ms: int | None = None,
        limit: int = 200,
    ) -> list[dict]:
        """Return alert events, newest first, optionally filtered."""
        conn = self._open()
        if conn is None:
            return []
        conditions: list[str] = []
        params: list = []
        if source:
            conditions.append("source = ?"); params.append(source)
        if device_id:
            conditions.append("device_id = ?"); params.append(device_id)
        if since_ms:
            conditions.append("timestamp_ms > ?"); params.append(since_ms)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        params.append(limit)
        try:
            rows = conn.execute(
                f"""
                SELECT id, rule_id, source, device_id, metric_name,
                       event_type, severity, message, value_at_event, timestamp_ms
                FROM alert_events {where}
                ORDER BY timestamp_ms DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
            return [dict(r) for r in rows]
        except Exception as exc:
            logger.warning("analytical.db: get_alert_events failed: %s", exc)
            return []
        finally:
            conn.close()

    # ── Rolling window stats (Tier 2) ────────────────────────────────────────

    def save_metric_stats(self, stats: dict) -> None:
        """
        Upsert pre-computed rolling stats for one metric + window.
        Expected keys: source, device_id, metric_name, window, avg, min, max,
                       stddev, sample_count, good_count.
        """
        with self._lock:
            conn = self._open()
            if conn is None:
                return
            try:
                conn.execute(
                    """
                    INSERT INTO metric_stats
                        (source, device_id, metric_name, window,
                         avg, min, max, stddev, sample_count, good_count, computed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source, device_id, metric_name, window) DO UPDATE SET
                        avg          = excluded.avg,
                        min          = excluded.min,
                        max          = excluded.max,
                        stddev       = excluded.stddev,
                        sample_count = excluded.sample_count,
                        good_count   = excluded.good_count,
                        computed_at  = excluded.computed_at
                    """,
                    (
                        stats["source"], stats["device_id"], stats["metric_name"],
                        stats["window"],
                        stats.get("avg"), stats.get("min"), stats.get("max"),
                        stats.get("stddev"),
                        stats.get("sample_count", 0), stats.get("good_count", 0),
                        int(time.time() * 1000),
                    ),
                )
                conn.commit()
            except Exception as exc:
                logger.error("analytical.db: save_metric_stats failed: %s", exc)
            finally:
                conn.close()

    def get_metric_stats(
        self,
        source: str,
        device_id: str,
        window: str | None = None,
    ) -> list[dict]:
        """Return materialized stats for a device, optionally filtered by window."""
        conn = self._open()
        if conn is None:
            return []
        where  = "WHERE source = ? AND device_id = ?"
        params: list = [source, device_id]
        if window:
            where += " AND window = ?"
            params.append(window)
        try:
            rows = conn.execute(
                f"SELECT * FROM metric_stats {where} ORDER BY metric_name, window",
                params,
            ).fetchall()
            return [dict(r) for r in rows]
        except Exception as exc:
            logger.warning("analytical.db: get_metric_stats failed: %s", exc)
            return []
        finally:
            conn.close()

    # ── Trend snapshots (Tier 3) ──────────────────────────────────────────────

    def save_trend_snapshot(self, snap: dict) -> None:
        """
        Upsert the latest trend snapshot for one metric.
        Expected keys: source, device_id, metric_name, direction, slope, computed_at.
        """
        with self._lock:
            conn = self._open()
            if conn is None:
                return
            try:
                conn.execute(
                    """
                    INSERT INTO trend_snapshots
                        (source, device_id, metric_name, direction, slope, computed_at, n_samples)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source, device_id, metric_name) DO UPDATE SET
                        direction   = excluded.direction,
                        slope       = excluded.slope,
                        computed_at = excluded.computed_at,
                        n_samples   = excluded.n_samples
                    """,
                    (
                        snap["source"], snap["device_id"], snap["metric_name"],
                        snap["direction"], snap.get("slope"), snap["computed_at"],
                        snap.get("n_samples"),
                    ),
                )
                conn.commit()
            except Exception as exc:
                logger.error("analytical.db: save_trend_snapshot failed: %s", exc)
            finally:
                conn.close()

    def get_trend_snapshots(self, source: str, device_id: str) -> list[dict]:
        """Return the latest trend snapshot for every metric of a device."""
        conn = self._open()
        if conn is None:
            return []
        try:
            rows = conn.execute(
                "SELECT * FROM trend_snapshots WHERE source = ? AND device_id = ? ORDER BY metric_name",
                (source, device_id),
            ).fetchall()
            return [dict(r) for r in rows]
        except Exception as exc:
            logger.warning("analytical.db: get_trend_snapshots failed: %s", exc)
            return []
        finally:
            conn.close()
