"""
Forwarding buffer — SQLite-backed per-profile outbox for MQTT and HTTPS delivery.

Ring-buffer design: messages are NEVER dropped due to failed delivery attempts.
They stay in the buffer until either:
  a) Successfully delivered (mark_sent → deleted).
  b) Buffer is full → oldest pending entries are evicted to make room for new ones.

This means if delivery fails due to a configuration error (e.g. HTTP 400), fixing
the configuration will allow all buffered messages to be delivered — nothing is
permanently lost just because the server rejected them a few times.

Retry cooldown (exponential backoff) prevents hammering the server:
  attempt 1 → wait 30s, attempt 2 → 60s, attempt 3 → 120s, attempt 4+ → 300s.

Buffer entry lifecycle:
  pending  →  deleted   (mark_sent — successfully delivered)
  pending  →  pending   (mark_failed — stays, backoff applied to last_attempt_ms)
  pending  →  evicted   (enqueue when buffer full — oldest 10% removed for new entry)
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time
from pathlib import Path

logger = logging.getLogger(__name__)

# Storage sizing:
#   Each pending row ≈ 600 bytes (500-byte payload + metadata + SQLite overhead).
#   500 k rows × 600 B ≈ 300 MB per profile; with 6 profiles max → ~1.8 GB total.
#   At a 1 msg/s publish rate that is ~5.7 days of backlog per profile.
#   Increase if the gateway has ample disk space and needs longer offline tolerance.
_MAX_PER_PROFILE = 500_000   # ring-buffer hard cap — oldest 10% evicted when full
_DRAIN_BATCH     = 10        # max messages returned per drain_batch() call

# Retry backoff in ms: attempt 1→30s, 2→60s, 3→120s, 4+→300s
# last_attempt_ms stores "do not retry before this epoch ms" (not "last tried at")
_RETRY_BACKOFF_MS = [30_000, 60_000, 120_000, 300_000]


def _now_ms() -> int:
    return int(time.time() * 1000)


class ForwardingBufferStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self._lock    = threading.Lock()
        # In-memory session stats (per AES session; pending counts come from SQLite)
        self._session_replayed: dict[str, int] = {}
        self._session_evicted:  dict[str, int] = {}   # ring-buffer evictions (buffer full)
        # Rolling buffer-level history for sparkline (profile_id → list[int])
        self._level_history: dict[str, list[int]] = {}
        self._ensure_schema()
        self._reset_stale_cooldowns()

    # ── Schema + startup ─────────────────────────────────────────────────────

    def _reset_stale_cooldowns(self) -> None:
        """
        On startup, clear retry cooldowns for all pending messages so they are
        retried promptly after a restart.  The backoff will re-apply if delivery
        continues to fail.
        """
        try:
            with self._conn() as conn:
                updated = conn.execute(
                    "UPDATE pending_messages SET last_attempt_ms = 0 WHERE status = 'pending'"
                ).rowcount
            if updated:
                logger.info("ForwardingBufferStore: reset cooldowns on %d pending messages (restart)", updated)
        except Exception as exc:
            logger.warning("ForwardingBufferStore: could not reset cooldowns on startup: %s", exc)

    def _ensure_schema(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self._conn() as conn:
                conn.executescript("""
                    CREATE TABLE IF NOT EXISTS pending_messages (
                        id             INTEGER PRIMARY KEY AUTOINCREMENT,
                        profile_id     TEXT    NOT NULL,
                        protocol       TEXT    NOT NULL,
                        path           TEXT    NOT NULL,
                        payload_json   TEXT    NOT NULL,
                        qos            INTEGER DEFAULT 1,
                        retain         INTEGER DEFAULT 0,
                        created_at_ms  INTEGER NOT NULL,
                        attempt_count  INTEGER DEFAULT 0,
                        last_attempt_ms INTEGER DEFAULT 0,
                        status         TEXT    DEFAULT 'pending'
                    );
                    CREATE INDEX IF NOT EXISTS idx_pending_profile
                        ON pending_messages (profile_id, status, id);

                    CREATE TABLE IF NOT EXISTS forwarding_events (
                        id             INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp_ms   INTEGER NOT NULL,
                        profile_id     TEXT    NOT NULL,
                        profile_name   TEXT    NOT NULL DEFAULT '',
                        protocol       TEXT    NOT NULL DEFAULT '',
                        destination    TEXT    NOT NULL DEFAULT '',
                        event_type     TEXT    NOT NULL,
                        severity       TEXT    NOT NULL DEFAULT 'info',
                        status         TEXT    NOT NULL DEFAULT '',
                        reason         TEXT    NOT NULL DEFAULT '',
                        started_at_ms  INTEGER,
                        ended_at_ms    INTEGER,
                        duration_ms    INTEGER,
                        http_status    INTEGER,
                        pending_count  INTEGER,
                        message        TEXT    NOT NULL DEFAULT ''
                    );
                    CREATE INDEX IF NOT EXISTS idx_forwarding_events_ts
                        ON forwarding_events (timestamp_ms DESC);
                    CREATE INDEX IF NOT EXISTS idx_forwarding_events_profile
                        ON forwarding_events (profile_id, timestamp_ms DESC);

                    CREATE TABLE IF NOT EXISTS forwarding_open_outages (
                        profile_id     TEXT PRIMARY KEY,
                        profile_name   TEXT    NOT NULL DEFAULT '',
                        protocol       TEXT    NOT NULL DEFAULT '',
                        destination    TEXT    NOT NULL DEFAULT '',
                        started_at_ms  INTEGER NOT NULL,
                        reason         TEXT    NOT NULL DEFAULT '',
                        severity       TEXT    NOT NULL DEFAULT 'error',
                        status         TEXT    NOT NULL DEFAULT 'down'
                    );
                """)
            logger.info("ForwardingBufferStore ready at %s", self._db_path)
        except Exception as exc:
            logger.error("ForwardingBufferStore schema error: %s", exc)

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path), timeout=5.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    # ── Public API ────────────────────────────────────────────────────────────

    def enqueue(
        self,
        profile_id:   str,
        protocol:     str,
        path:         str,
        payload_json: str,
        qos:          int  = 1,
        retain:       bool = False,
    ) -> bool:
        """
        Add one message to the outbox.  Returns False if the profile is full.
        Evicts the oldest pending entries when at capacity.
        """
        with self._lock:
            try:
                with self._conn() as conn:
                    # Count current pending for this profile
                    count = conn.execute(
                        "SELECT COUNT(*) FROM pending_messages "
                        "WHERE profile_id=? AND status='pending'",
                        (profile_id,),
                    ).fetchone()[0]

                    if count >= _MAX_PER_PROFILE:
                        # Ring-buffer: evict oldest 10% to make room for new entries
                        evict_n = max(1, _MAX_PER_PROFILE // 10)
                        conn.execute(
                            "DELETE FROM pending_messages WHERE id IN ("
                            "  SELECT id FROM pending_messages "
                            "  WHERE profile_id=? AND status='pending' "
                            "  ORDER BY id ASC LIMIT ?"
                            ")",
                            (profile_id, evict_n),
                        )
                        self._session_evicted[profile_id] = \
                            self._session_evicted.get(profile_id, 0) + evict_n
                        self._insert_event_conn(conn, {
                            "profile_id": profile_id,
                            "protocol": protocol,
                            "event_type": "buffer_eviction",
                            "severity": "error",
                            "status": "evicted",
                            "reason": "Forwarding buffer full; oldest pending messages evicted.",
                            "pending_count": count,
                            "message": f"Evicted {evict_n} oldest pending message(s).",
                        })
                        logger.warning(
                            "Buffer full for profile '%s' — evicted %d oldest entries (ring-buffer)",
                            profile_id, evict_n,
                        )

                    conn.execute(
                        "INSERT INTO pending_messages "
                        "(profile_id, protocol, path, payload_json, qos, retain, created_at_ms) "
                        "VALUES (?,?,?,?,?,?,?)",
                        (profile_id, protocol, path, payload_json,
                         qos, int(retain), int(time.time() * 1000)),
                    )
                    return True
            except Exception as exc:
                logger.error("Buffer enqueue error for profile '%s': %s", profile_id, exc)
                return False

    def drain_batch(self, profile_id: str) -> list[dict]:
        """
        Return up to _DRAIN_BATCH pending messages that are past their retry cooldown.
        Does NOT mark them sent — caller must call mark_sent() or mark_failed().
        """
        now_ms = int(time.time() * 1000)
        with self._lock:
            try:
                with self._conn() as conn:
                    # last_attempt_ms holds "do not retry before this time"
                    rows = conn.execute(
                        "SELECT id, profile_id, protocol, path, payload_json, qos, retain, created_at_ms "
                        "FROM pending_messages "
                        "WHERE profile_id=? AND status='pending' AND last_attempt_ms <= ? "
                        "ORDER BY id ASC LIMIT ?",
                        (profile_id, now_ms, _DRAIN_BATCH),
                    ).fetchall()
                    return [dict(r) for r in rows]
            except Exception as exc:
                logger.error("Buffer drain_batch error: %s", exc)
                return []

    def mark_sent(self, msg_id: int, profile_id: str) -> None:
        """Mark a message as successfully delivered — remove it from the outbox."""
        with self._lock:
            try:
                with self._conn() as conn:
                    conn.execute(
                        "DELETE FROM pending_messages WHERE id=?", (msg_id,)
                    )
                self._session_replayed[profile_id] = \
                    self._session_replayed.get(profile_id, 0) + 1
            except Exception as exc:
                logger.error("Buffer mark_sent error (id=%d): %s", msg_id, exc)

    def mark_failed(self, msg_id: int, profile_id: str) -> bool:
        """
        Record a failed delivery attempt — apply exponential backoff cooldown.
        Messages are NEVER dropped due to retries.  They stay pending until either
        successfully delivered or evicted by the ring-buffer when full.
        Always returns False (message stays in queue).
        """
        now_ms = int(time.time() * 1000)
        with self._lock:
            try:
                with self._conn() as conn:
                    # Read current attempt count to compute backoff
                    row = conn.execute(
                        "SELECT attempt_count FROM pending_messages WHERE id=?", (msg_id,)
                    ).fetchone()
                    if not row:
                        return False
                    attempts = (row["attempt_count"] or 0) + 1
                    backoff_idx = min(attempts - 1, len(_RETRY_BACKOFF_MS) - 1)
                    # Store "do not retry before" in last_attempt_ms
                    not_before_ms = now_ms + _RETRY_BACKOFF_MS[backoff_idx]

                    conn.execute(
                        "UPDATE pending_messages "
                        "SET attempt_count = ?, last_attempt_ms = ? "
                        "WHERE id = ?",
                        (attempts, not_before_ms, msg_id),
                    )
                    logger.debug(
                        "Buffer: id=%d profile='%s' attempt %d — retry in %ds",
                        msg_id, profile_id, attempts, _RETRY_BACKOFF_MS[backoff_idx] // 1000,
                    )
            except Exception as exc:
                logger.error("Buffer mark_failed error (id=%d): %s", msg_id, exc)
        return False  # Message stays pending — never dropped by retry logic

    def pending_count(self, profile_id: str) -> int:
        """Return the number of currently pending messages for a profile."""
        try:
            with self._conn() as conn:
                row = conn.execute(
                    "SELECT COUNT(*) FROM pending_messages WHERE profile_id=? AND status='pending'",
                    (profile_id,),
                ).fetchone()
                return row[0] if row else 0
        except Exception:
            return 0

    def snapshot_level(self, profile_id: str) -> None:
        """
        Record current pending count in the rolling history (call every tick).
        Keeps the last 60 samples (~5 min at 5s interval).
        """
        count = self.pending_count(profile_id)
        hist  = self._level_history.setdefault(profile_id, [])
        hist.append(count)
        if len(hist) > 60:
            hist.pop(0)

    def get_level_history(self, profile_id: str) -> list[int]:
        return list(self._level_history.get(profile_id, []))

    def oldest_pending_ms(self, profile_id: str) -> int | None:
        """Return the created_at_ms of the oldest pending entry, or None."""
        try:
            with self._conn() as conn:
                row = conn.execute(
                    "SELECT MIN(created_at_ms) FROM pending_messages "
                    "WHERE profile_id=? AND status='pending'",
                    (profile_id,),
                ).fetchone()
                return row[0] if row and row[0] else None
        except Exception:
            return None

    def get_stats(self, profile_id: str) -> dict:
        """Return aggregated stats for one profile."""
        pending  = self.pending_count(profile_id)
        replayed = self._session_replayed.get(profile_id, 0)
        evicted  = self._session_evicted.get(profile_id, 0)   # only ring-buffer evictions
        total    = replayed + evicted
        rate     = round(100 * replayed / total, 1) if total else None  # None = no history yet
        oldest   = self.oldest_pending_ms(profile_id)
        now_ms   = int(time.time() * 1000)

        # Count how many are cooling down (in retry backoff, not yet eligible)
        cooling = 0
        try:
            with self._conn() as conn:
                cooling = conn.execute(
                    "SELECT COUNT(*) FROM pending_messages "
                    "WHERE profile_id=? AND status='pending' AND last_attempt_ms > ?",
                    (profile_id, now_ms),
                ).fetchone()[0] or 0
        except Exception:
            pass

        return {
            "profile_id":           profile_id,
            "pending":              pending,
            "cooling_down":         cooling,           # in backoff, waiting to retry
            "ready_to_retry":       max(0, pending - cooling),
            "replayed":             replayed,          # successfully sent from buffer
            "dropped":              evicted,           # evicted because buffer was full
            "success_rate":         rate,
            "oldest_pending_ms":    oldest,
            "oldest_pending_age_s": round((now_ms - oldest) / 1000) if oldest else None,
            "level_history":        self.get_level_history(profile_id),
        }

    def get_all_stats(self, profile_ids: list[str]) -> dict[str, dict]:
        return {pid: self.get_stats(pid) for pid in profile_ids}

    # ── Audit events / outages ────────────────────────────────────────────────

    def _insert_event_conn(self, conn: sqlite3.Connection, event: dict) -> int:
        now_ms = int(event.get("timestamp_ms") or _now_ms())
        cur = conn.execute(
            """
            INSERT INTO forwarding_events
                (timestamp_ms, profile_id, profile_name, protocol, destination,
                 event_type, severity, status, reason, started_at_ms, ended_at_ms,
                 duration_ms, http_status, pending_count, message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now_ms,
                str(event.get("profile_id", "")),
                str(event.get("profile_name", "")),
                str(event.get("protocol", "")),
                str(event.get("destination", "")),
                str(event.get("event_type", "")),
                str(event.get("severity", "info")),
                str(event.get("status", "")),
                str(event.get("reason", "")),
                event.get("started_at_ms"),
                event.get("ended_at_ms"),
                event.get("duration_ms"),
                event.get("http_status"),
                event.get("pending_count"),
                str(event.get("message", "")),
            ),
        )
        return int(cur.lastrowid or 0)

    def record_event(self, event: dict) -> int:
        """Append one durable forwarding audit event. Returns event id, or 0."""
        with self._lock:
            try:
                with self._conn() as conn:
                    return self._insert_event_conn(conn, event)
            except Exception as exc:
                logger.error("Forwarding audit event insert failed: %s", exc)
                return 0

    def begin_outage(
        self,
        profile_id: str,
        profile_name: str,
        protocol: str,
        destination: str,
        reason: str,
        *,
        severity: str = "error",
        status: str = "down",
        http_status: int | None = None,
        pending_count: int | None = None,
    ) -> None:
        """
        Start one open outage if not already open.
        Repeated failures update reason but do not spam event history.
        """
        if not profile_id:
            return
        now = _now_ms()
        with self._lock:
            try:
                with self._conn() as conn:
                    row = conn.execute(
                        "SELECT started_at_ms, reason FROM forwarding_open_outages WHERE profile_id=?",
                        (profile_id,),
                    ).fetchone()
                    if row:
                        if reason and reason != row["reason"]:
                            conn.execute(
                                """
                                UPDATE forwarding_open_outages
                                SET profile_name=?, protocol=?, destination=?, reason=?, severity=?, status=?
                                WHERE profile_id=?
                                """,
                                (profile_name, protocol, destination, reason, severity, status, profile_id),
                            )
                        return

                    conn.execute(
                        """
                        INSERT INTO forwarding_open_outages
                            (profile_id, profile_name, protocol, destination,
                             started_at_ms, reason, severity, status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (profile_id, profile_name, protocol, destination, now, reason, severity, status),
                    )
                    self._insert_event_conn(conn, {
                        "timestamp_ms": now,
                        "profile_id": profile_id,
                        "profile_name": profile_name,
                        "protocol": protocol,
                        "destination": destination,
                        "event_type": "outage_started",
                        "severity": severity,
                        "status": status,
                        "reason": reason,
                        "started_at_ms": now,
                        "http_status": http_status,
                        "pending_count": pending_count,
                        "message": "Forwarding delivery outage started.",
                    })
            except Exception as exc:
                logger.error("Forwarding outage begin failed for profile '%s': %s", profile_id, exc)

    def resolve_outage(
        self,
        profile_id: str,
        profile_name: str,
        protocol: str,
        destination: str,
        *,
        pending_count: int | None = None,
    ) -> None:
        """Close any open outage and write recovery event with duration."""
        if not profile_id:
            return
        now = _now_ms()
        with self._lock:
            try:
                with self._conn() as conn:
                    row = conn.execute(
                        "SELECT * FROM forwarding_open_outages WHERE profile_id=?",
                        (profile_id,),
                    ).fetchone()
                    if not row:
                        return
                    started = int(row["started_at_ms"])
                    duration = max(0, now - started)
                    reason = row["reason"] or ""
                    conn.execute(
                        "DELETE FROM forwarding_open_outages WHERE profile_id=?",
                        (profile_id,),
                    )
                    self._insert_event_conn(conn, {
                        "timestamp_ms": now,
                        "profile_id": profile_id,
                        "profile_name": profile_name or row["profile_name"],
                        "protocol": protocol or row["protocol"],
                        "destination": destination or row["destination"],
                        "event_type": "outage_recovered",
                        "severity": "info",
                        "status": "recovered",
                        "reason": reason,
                        "started_at_ms": started,
                        "ended_at_ms": now,
                        "duration_ms": duration,
                        "pending_count": pending_count,
                        "message": "Forwarding delivery recovered.",
                    })
            except Exception as exc:
                logger.error("Forwarding outage resolve failed for profile '%s': %s", profile_id, exc)

    def get_open_outage(self, profile_id: str) -> dict | None:
        try:
            with self._conn() as conn:
                row = conn.execute(
                    "SELECT * FROM forwarding_open_outages WHERE profile_id=?",
                    (profile_id,),
                ).fetchone()
                return dict(row) if row else None
        except Exception:
            return None

    def get_events(
        self,
        *,
        profile_id: str | None = None,
        severity: str | None = None,
        since_ms: int | None = None,
        limit: int = 200,
    ) -> list[dict]:
        """Return durable forwarding audit events, newest first."""
        conditions: list[str] = []
        params: list = []
        if profile_id:
            conditions.append("profile_id = ?")
            params.append(profile_id)
        if severity:
            sev_order = {"info": 0, "warning": 1, "error": 2, "critical": 3}
            min_sev = sev_order.get(severity, 0)
            allowed = [k for k, v in sev_order.items() if v >= min_sev]
            conditions.append(f"severity IN ({','.join('?' for _ in allowed)})")
            params.extend(allowed)
        if since_ms:
            conditions.append("timestamp_ms >= ?")
            params.append(since_ms)
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        params.append(max(1, min(1000, int(limit))))
        try:
            with self._conn() as conn:
                rows = conn.execute(
                    f"""
                    SELECT *
                    FROM forwarding_events
                    {where}
                    ORDER BY timestamp_ms DESC, id DESC
                    LIMIT ?
                    """,
                    params,
                ).fetchall()
                return [dict(r) for r in rows]
        except Exception as exc:
            logger.error("Forwarding audit event query failed: %s", exc)
            return []

    def clear_delivered(self) -> None:
        """Housekeeping: no-op (pending messages stay until delivered or evicted by ring-buffer)."""
        pass

    def get_storage_info(self) -> dict:
        """Return buffer storage info: total rows, DB file size, capacity."""
        try:
            db_bytes = self._db_path.stat().st_size if self._db_path.exists() else 0
        except OSError:
            db_bytes = 0
        try:
            with self._conn() as conn:
                total_pending = conn.execute(
                    "SELECT COUNT(*) FROM pending_messages WHERE status='pending'"
                ).fetchone()[0] or 0
        except Exception:
            total_pending = 0
        return {
            "db_size_bytes":    db_bytes,
            "db_size_mb":       round(db_bytes / 1_048_576, 2),
            "total_pending":    total_pending,
            "max_per_profile":  _MAX_PER_PROFILE,
            "estimated_bytes_per_msg": 600,
            "estimated_capacity_mb":  round(_MAX_PER_PROFILE * 600 / 1_048_576, 0),
        }

    def log_buffer_state(self, profile_id: str) -> None:
        """Log a summary of the current buffer state for a profile."""
        stats = self.get_stats(profile_id)
        if stats["pending"] > 0:
            logger.info(
                "Buffer '%s': %d pending (%d ready, %d cooling) · %d recovered · %d evicted",
                profile_id, stats["pending"], stats["ready_to_retry"],
                stats["cooling_down"], stats["replayed"], stats["dropped"],
            )
