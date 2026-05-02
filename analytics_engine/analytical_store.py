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
