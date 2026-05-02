"""
Archival job: periodically copies new rows from pes.db:sensor_samples into
analytical.db:metric_archive before PES's 5 GB rolling delete erases them.

Design rules:
  - Read-only on pes.db. Never modifies PES data.
  - Uses a per-device row cursor so it always picks up from where it left off,
    even after AES restarts or the device reboots.
  - Safe to call when pes.db does not exist (skips gracefully).
  - Safe to call when analytical.db was deleted (store auto-recreates it).
  - All exceptions are caught; tick() never raises.
"""
from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from analytics_engine.analytical_store import AnalyticalStore

logger = logging.getLogger(__name__)

_BATCH_SIZE = 5_000   # max rows harvested per device per tick


class ArchivalJob:
    """
    Call tick() from a BackgroundWorker every 5 minutes.
    Reads pes.db once per tick and writes any new rows to analytical.db.
    """

    def __init__(
        self,
        pes_db_path: Path | str,
        analytical_store: "AnalyticalStore",
    ) -> None:
        self._pes_db_path = Path(pes_db_path)
        self._store       = analytical_store

    # ── Public entry point ────────────────────────────────────────────────────

    def tick(self) -> None:
        """Run one harvest pass. Called by the background worker thread."""
        t0 = time.monotonic()

        if not self._pes_db_path.exists():
            logger.debug("archival: pes.db not found — skipping tick")
            return

        conn = self._open_pes()
        if conn is None:
            return

        try:
            total = self._harvest_all(conn)
        finally:
            conn.close()

        # Size check once per tick — not per insert
        self._store.check_and_prune()

        if total:
            elapsed = time.monotonic() - t0
            logger.info(
                "archival: harvested %d row(s) from pes.db in %.2fs", total, elapsed
            )
        else:
            logger.debug("archival: no new rows this tick")

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _open_pes(self) -> sqlite3.Connection | None:
        """Open pes.db read-only. Returns None on failure."""
        try:
            conn = sqlite3.connect(
                f"file:{self._pes_db_path}?mode=ro",
                uri=True,
                timeout=3.0,
                check_same_thread=False,
            )
            return conn
        except Exception as exc:
            logger.warning("archival: cannot open pes.db: %s", exc)
            return None

    def _harvest_all(self, conn: sqlite3.Connection) -> int:
        """Harvest every device found in pes.db. Returns total rows inserted."""
        try:
            devices = conn.execute(
                "SELECT DISTINCT source, device_id FROM sensor_samples"
            ).fetchall()
        except Exception as exc:
            logger.warning("archival: failed to list devices: %s", exc)
            return 0

        total = 0
        for row in devices:
            total += self._harvest_device(conn, source=row[0], device_id=row[1])
        return total

    def _harvest_device(
        self,
        conn: sqlite3.Connection,
        source: str,
        device_id: str,
    ) -> int:
        """
        Copy new rows for one device from pes.db into analytical.db.
        Uses the stored cursor to only fetch rows we have not seen yet.
        """
        cursor_id = self._store.get_harvest_cursor(source, device_id)

        try:
            rows_raw = conn.execute(
                """
                SELECT
                    rowid,
                    source,
                    device_id,
                    metric      AS metric_name,
                    value,
                    quality,
                    timestamp_ms
                FROM sensor_samples
                WHERE source    = ?
                  AND device_id = ?
                  AND rowid     > ?
                ORDER BY rowid ASC
                LIMIT ?
                """,
                (source, device_id, cursor_id, _BATCH_SIZE),
            ).fetchall()
        except Exception as exc:
            logger.warning(
                "archival: query failed for %s/%s: %s", source, device_id, exc
            )
            return 0

        if not rows_raw:
            return 0

        # Build dicts expected by AnalyticalStore.append_archive_batch()
        # pes.db:sensor_samples has no unit column — store empty string
        batch = [
            {
                "source":       r[1],
                "device_id":    r[2],
                "metric_name":  r[3],
                "value":        r[4],
                "unit":         "",
                "quality":      r[5] if r[5] in ("good", "stale", "error") else "good",
                "timestamp_ms": r[6],
            }
            for r in rows_raw
        ]

        inserted = self._store.append_archive_batch(batch)
        if inserted:
            last_rowid = rows_raw[-1][0]
            self._store.update_harvest_cursor(source, device_id, last_rowid)
            logger.debug(
                "archival: %s/%s — %d row(s) → cursor=%d",
                source, device_id, inserted, last_rowid,
            )

        return inserted
