from __future__ import annotations

import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _as_dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _clean_text(value: Any, default: str = "") -> str:
    text = str(value or "").strip()
    return text or default


class NetworkEventStore:
    """Durable AES-owned connectivity audit store."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self._lock = threading.Lock()
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self._conn() as conn:
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS network_events (
                        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp_ms            INTEGER NOT NULL,
                        event_type              TEXT    NOT NULL,
                        severity                TEXT    NOT NULL DEFAULT 'info',
                        previous_uplink         TEXT    NOT NULL DEFAULT '',
                        active_uplink           TEXT    NOT NULL DEFAULT '',
                        interface               TEXT    NOT NULL DEFAULT '',
                        status                  TEXT    NOT NULL DEFAULT '',
                        reason                  TEXT    NOT NULL DEFAULT '',
                        started_at_ms           INTEGER,
                        ended_at_ms             INTEGER,
                        duration_ms             INTEGER,
                        internet_ok             INTEGER,
                        message                 TEXT    NOT NULL DEFAULT ''
                    );
                    CREATE INDEX IF NOT EXISTS idx_network_events_ts
                        ON network_events (timestamp_ms DESC);
                    CREATE INDEX IF NOT EXISTS idx_network_events_type
                        ON network_events (event_type, timestamp_ms DESC);

                    CREATE TABLE IF NOT EXISTS network_open_outages (
                        id                      TEXT PRIMARY KEY,
                        started_at_ms           INTEGER NOT NULL,
                        active_uplink           TEXT    NOT NULL DEFAULT '',
                        reason                  TEXT    NOT NULL DEFAULT '',
                        status                  TEXT    NOT NULL DEFAULT 'down'
                    );

                    CREATE TABLE IF NOT EXISTS network_status (
                        id                      TEXT PRIMARY KEY,
                        active_uplink           TEXT    NOT NULL DEFAULT 'none',
                        active_uplink_since_ms  INTEGER NOT NULL,
                        has_uplink              INTEGER NOT NULL DEFAULT 0,
                        internet_ok             INTEGER NOT NULL DEFAULT 0,
                        recovery_count          INTEGER NOT NULL DEFAULT 0,
                        tailscale_recovery_count INTEGER NOT NULL DEFAULT 0,
                        updated_at_ms           INTEGER NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS network_interface_status (
                        interface               TEXT PRIMARY KEY,
                        status                  TEXT    NOT NULL DEFAULT 'unknown',
                        reason                  TEXT    NOT NULL DEFAULT '',
                        status_since_ms         INTEGER NOT NULL,
                        updated_at_ms           INTEGER NOT NULL
                    );
                    """
                )
                conn.execute(
                    """
                    UPDATE network_events
                    SET severity='info'
                    WHERE event_type IN ('recovery_action', 'tailscale_recovery')
                      AND severity='warning'
                    """
                )
            logger.info("NetworkEventStore ready at %s", self._db_path)
        except Exception as exc:
            logger.error("NetworkEventStore schema error: %s", exc)

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path), timeout=5.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def record_state(self, state: dict) -> None:
        """Sample live image state and append transition events with AES timestamps."""
        sample = self._extract_sample(state)
        now = _now_ms()
        with self._lock:
            try:
                with self._conn() as conn:
                    prev = conn.execute(
                        "SELECT * FROM network_status WHERE id='current'"
                    ).fetchone()
                    active_since = now
                    if prev:
                        active_since = int(prev["active_uplink_since_ms"] or now)
                        prev_uplink = str(prev["active_uplink"] or "none")
                        prev_has = bool(prev["has_uplink"])
                        prev_recovery = int(prev["recovery_count"] or 0)
                        prev_ts_recovery = int(prev["tailscale_recovery_count"] or 0)

                        if prev_uplink != sample["active_uplink"]:
                            active_since = now
                            self._insert_event_conn(conn, {
                                "timestamp_ms": now,
                                "event_type": "uplink_switch",
                                "severity": "warning" if sample["active_uplink"] == "none" else "info",
                                "previous_uplink": prev_uplink,
                                "active_uplink": sample["active_uplink"],
                                "interface": sample["active_uplink"],
                                "status": "switched",
                                "reason": sample["reason"] or f"Active uplink changed from {prev_uplink} to {sample['active_uplink']}.",
                                "internet_ok": int(sample["internet_ok"]),
                                "message": f"Active uplink switched from {prev_uplink} to {sample['active_uplink']}.",
                            })

                        if prev_has and not sample["has_uplink"]:
                            self._begin_outage_conn(conn, now, sample)
                        elif not prev_has and sample["has_uplink"]:
                            self._resolve_outage_conn(conn, now, sample)

                        if sample["recovery_count"] > prev_recovery:
                            self._insert_event_conn(conn, {
                                "timestamp_ms": now,
                                "event_type": "recovery_action",
                                "severity": "info",
                                "active_uplink": sample["active_uplink"],
                                "interface": sample["active_uplink"],
                                "status": "recovery",
                                "reason": sample["recovery_reason"],
                                "internet_ok": int(sample["internet_ok"]),
                                "message": "Network recovery action recorded by gateway image.",
                            })

                        if sample["tailscale_recovery_count"] > prev_ts_recovery:
                            self._insert_event_conn(conn, {
                                "timestamp_ms": now,
                                "event_type": "tailscale_recovery",
                                "severity": "info",
                                "active_uplink": sample["active_uplink"],
                                "interface": sample["active_uplink"],
                                "status": "recovery",
                                "reason": sample["tailscale_reason"],
                                "internet_ok": int(sample["internet_ok"]),
                                "message": "Tailscale recovery action recorded by gateway image.",
                            })
                    else:
                        if not sample["has_uplink"]:
                            self._begin_outage_conn(conn, now, sample)

                    for iface_sample in self._extract_interface_samples(state, sample["active_uplink"]):
                        self._record_interface_sample_conn(conn, now, iface_sample)

                    conn.execute(
                        """
                        INSERT INTO network_status
                            (id, active_uplink, active_uplink_since_ms, has_uplink,
                             internet_ok, recovery_count, tailscale_recovery_count, updated_at_ms)
                        VALUES ('current', ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(id) DO UPDATE SET
                            active_uplink=excluded.active_uplink,
                            active_uplink_since_ms=excluded.active_uplink_since_ms,
                            has_uplink=excluded.has_uplink,
                            internet_ok=excluded.internet_ok,
                            recovery_count=excluded.recovery_count,
                            tailscale_recovery_count=excluded.tailscale_recovery_count,
                            updated_at_ms=excluded.updated_at_ms
                        """,
                        (
                            sample["active_uplink"],
                            active_since,
                            int(sample["has_uplink"]),
                            int(sample["internet_ok"]),
                            sample["recovery_count"],
                            sample["tailscale_recovery_count"],
                            now,
                        ),
                    )
            except Exception as exc:
                logger.error("Network audit state sample failed: %s", exc)

    def _extract_sample(self, state: dict) -> dict:
        uplink_stats = _as_dict(state.get("uplink_stats"))
        network = _as_dict(uplink_stats.get("network"))
        recovery = _as_dict(state.get("recovery"))
        tailscale = _as_dict(state.get("tailscale_recovery"))

        active = _clean_text(state.get("active_uplink"), "none")
        if active in {"", "null", "None"}:
            active = "none"

        has_uplink_raw = network.get("has_uplink")
        has_uplink = bool(has_uplink_raw) if isinstance(has_uplink_raw, bool) else active != "none"

        internet_ok = self._internet_ok(state, network)
        monitor_status = _clean_text(state.get("monitor_status"))
        if active == "none" and monitor_status in {"", "idle", "unknown"}:
            monitor_status = "No active uplink"
        reason = (
            _clean_text(network.get("reason"))
            or _clean_text(recovery.get("last_reason"))
            or self._interface_reason(state, active)
            or monitor_status
        )
        return {
            "active_uplink": active,
            "has_uplink": has_uplink,
            "internet_ok": internet_ok,
            "reason": reason,
            "recovery_count": self._safe_int(recovery.get("count")),
            "recovery_reason": _clean_text(recovery.get("last_reason"), reason),
            "tailscale_recovery_count": self._safe_int(tailscale.get("count")),
            "tailscale_reason": _clean_text(tailscale.get("last_reason"), reason),
        }

    def _internet_ok(self, state: dict, network: dict) -> bool:
        if isinstance(network.get("internet_ok"), bool):
            return bool(network.get("internet_ok"))
        for key in ("eth0", "eth1", "wifi_client", "cellular"):
            section = _as_dict(state.get(key))
            if bool(section.get("internet_ok")) or bool(section.get("connected")):
                return True
        return False

    def _interface_reason(self, state: dict, active: str) -> str:
        if active == "wifi_client":
            diag = _as_dict(_as_dict(state.get("wifi_client")).get("diagnostics"))
            return _clean_text(diag.get("reason"))
        if active in {"eth0", "eth1"}:
            eth = _as_dict(state.get(active))
            if not eth.get("link_up"):
                return f"{active} link down"
            if not eth.get("internet_ok"):
                return f"{active} has no internet"
        if active == "cellular":
            cel = _as_dict(state.get("cellular"))
            if not cel.get("enabled"):
                return "cellular disabled"
            if not cel.get("connected"):
                return "cellular disconnected"
        return ""

    def _extract_interface_samples(self, state: dict, active_uplink: str) -> list[dict]:
        uplink_stats = _as_dict(state.get("uplink_stats"))
        stat_ifaces = _as_dict(uplink_stats.get("interfaces"))
        eth0 = _as_dict(state.get("eth0"))
        eth1 = _as_dict(state.get("eth1"))
        wifi = _as_dict(state.get("wifi_client"))
        cellular = _as_dict(state.get("cellular"))

        samples = [
            self._ethernet_sample("eth0", eth0, _as_dict(stat_ifaces.get("eth0")), active_uplink),
            self._ethernet_sample("eth1", eth1, _as_dict(stat_ifaces.get("eth1")), active_uplink),
            self._wifi_sample(wifi, _as_dict(stat_ifaces.get("wifi_client")), active_uplink),
            self._cellular_sample(cellular, _as_dict(stat_ifaces.get("cellular")), active_uplink),
        ]
        return [sample for sample in samples if sample]

    def _ethernet_sample(self, key: str, eth: dict, stats: dict, active_uplink: str) -> dict:
        link_up = bool(eth.get("link_up"))
        address = _clean_text(eth.get("address"))
        internet_known = "internet_ok" in eth
        internet_ok = bool(eth.get("internet_ok"))
        if link_up and address and (internet_ok or not internet_known):
            status = "ok"
            reason = address
        elif active_uplink == key:
            status = "issue"
            reason = f"{key} active uplink has no internet" if link_up else f"{key} active uplink link down"
        else:
            status = "standby" if link_up else "disabled"
            reason = address or ("Link present, waiting for IP" if link_up else "No link")
        return {
            "interface": key,
            "status": status,
            "reason": reason,
            "track_on_first": active_uplink == key or bool(stats.get("down_events")),
        }

    def _wifi_sample(self, wifi: dict, stats: dict, active_uplink: str) -> dict:
        diag = _as_dict(wifi.get("diagnostics"))
        enabled = bool(wifi.get("enabled")) or bool(wifi.get("configured_ssid"))
        present = wifi.get("present", True) is not False
        connected = bool(wifi.get("connected_ssid"))
        reason_raw = _clean_text(diag.get("reason"))
        reason = self._wifi_reason_label(reason_raw)

        if connected or bool(wifi.get("internet_ok")):
            status = "ok"
            reason = _clean_text(wifi.get("connected_ssid"), "Wi-Fi connected")
        elif not present:
            status = "issue" if enabled or active_uplink == "wifi_client" else "disabled"
            reason = "Wi-Fi interface not detected"
        elif enabled or active_uplink == "wifi_client":
            status = "issue"
            target = _clean_text(wifi.get("configured_ssid"))
            reason = f"{reason or 'Wi-Fi not connected'}" + (f" ({target})" if target else "")
        else:
            status = "standby"
            reason = "Wi-Fi available"

        return {
            "interface": "wifi_client",
            "status": status,
            "reason": reason,
            "track_on_first": enabled or active_uplink == "wifi_client" or bool(stats.get("down_events")),
        }

    def _cellular_sample(self, cellular: dict, stats: dict, active_uplink: str) -> dict:
        enabled = bool(cellular.get("enabled"))
        connected = bool(cellular.get("connected"))
        present = bool(cellular.get("present"))
        if connected:
            status = "ok"
            reason = _clean_text(cellular.get("operator"), "Cellular connected")
        elif enabled:
            status = "issue"
            if not present:
                reason = "Cellular modem not detected"
            elif cellular.get("sim_status") == "missing":
                reason = "SIM card missing"
            elif cellular.get("sim_status") == "locked":
                reason = "SIM PIN required"
            else:
                reason = "Cellular not connected"
        else:
            status = "disabled"
            reason = "Cellular disabled"
        return {
            "interface": "cellular",
            "status": status,
            "reason": reason,
            "track_on_first": enabled or active_uplink == "cellular" or bool(stats.get("down_events")),
        }

    def _wifi_reason_label(self, reason: str) -> str:
        return {
            "auth_failed": "Wi-Fi authentication failed",
            "bad_password": "Wi-Fi authentication failed",
            "wrong_password": "Wi-Fi authentication failed",
            "4way_handshake_failed": "Wi-Fi authentication failed",
            "ssid_not_found": "Target Wi-Fi network not found",
            "scanning": "Scanning for Wi-Fi network",
            "disconnected": "Wi-Fi disconnected",
            "authenticating": "Wi-Fi authenticating",
            "associating": "Wi-Fi associating",
            "waiting_for_ip": "Wi-Fi waiting for DHCP",
            "connected_no_internet": "Wi-Fi connected, no internet",
            "supplicant_inactive": "Wi-Fi supplicant not running",
            "interface_missing": "Wi-Fi interface not detected",
            "interface_disabled": "Wi-Fi interface disabled",
            "ssid_missing": "No Wi-Fi SSID configured",
            "disabled": "Wi-Fi disabled",
        }.get(reason, reason.replace("_", " ") if reason else "")

    def _record_interface_sample_conn(self, conn: sqlite3.Connection, now: int, sample: dict) -> None:
        iface = str(sample.get("interface") or "")
        if not iface:
            return
        status = str(sample.get("status") or "unknown")
        reason = str(sample.get("reason") or "")
        prev = conn.execute(
            "SELECT * FROM network_interface_status WHERE interface=?",
            (iface,),
        ).fetchone()
        since = now
        if prev:
            since = int(prev["status_since_ms"] or now)
            prev_status = str(prev["status"] or "unknown")
            prev_reason = str(prev["reason"] or "")
            if prev_status != status:
                if status == "issue":
                    since = now
                    self._insert_event_conn(conn, {
                        "timestamp_ms": now,
                        "event_type": "interface_issue_started",
                        "severity": "warning",
                        "interface": iface,
                        "status": status,
                        "reason": reason,
                        "started_at_ms": now,
                        "message": f"{iface} issue started.",
                    })
                elif prev_status == "issue":
                    duration = max(0, now - since)
                    self._insert_event_conn(conn, {
                        "timestamp_ms": now,
                        "event_type": "interface_recovered",
                        "severity": "info",
                        "interface": iface,
                        "status": status,
                        "reason": prev_reason,
                        "started_at_ms": since,
                        "ended_at_ms": now,
                        "duration_ms": duration,
                        "message": f"{iface} recovered.",
                    })
                    since = now
                else:
                    since = now
            elif status == "issue" and reason and reason != prev_reason:
                self._insert_event_conn(conn, {
                    "timestamp_ms": now,
                    "event_type": "interface_issue_changed",
                    "severity": "warning",
                    "interface": iface,
                    "status": status,
                    "reason": reason,
                    "started_at_ms": since,
                    "message": f"{iface} issue reason changed.",
                })
        elif status == "issue" and sample.get("track_on_first"):
            self._insert_event_conn(conn, {
                "timestamp_ms": now,
                "event_type": "interface_issue_started",
                "severity": "warning",
                "interface": iface,
                "status": status,
                "reason": reason,
                "started_at_ms": now,
                "message": f"{iface} issue started.",
            })

        conn.execute(
            """
            INSERT INTO network_interface_status
                (interface, status, reason, status_since_ms, updated_at_ms)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(interface) DO UPDATE SET
                status=excluded.status,
                reason=excluded.reason,
                status_since_ms=excluded.status_since_ms,
                updated_at_ms=excluded.updated_at_ms
            """,
            (iface, status, reason, since, now),
        )

    def _safe_int(self, value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def _insert_event_conn(self, conn: sqlite3.Connection, event: dict) -> int:
        cur = conn.execute(
            """
            INSERT INTO network_events
                (timestamp_ms, event_type, severity, previous_uplink, active_uplink,
                 interface, status, reason, started_at_ms, ended_at_ms, duration_ms,
                 internet_ok, message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(event.get("timestamp_ms") or _now_ms()),
                str(event.get("event_type", "")),
                str(event.get("severity", "info")),
                str(event.get("previous_uplink", "")),
                str(event.get("active_uplink", "")),
                str(event.get("interface", "")),
                str(event.get("status", "")),
                str(event.get("reason", "")),
                event.get("started_at_ms"),
                event.get("ended_at_ms"),
                event.get("duration_ms"),
                event.get("internet_ok"),
                str(event.get("message", "")),
            ),
        )
        return int(cur.lastrowid or 0)

    def _begin_outage_conn(self, conn: sqlite3.Connection, now: int, sample: dict) -> None:
        row = conn.execute("SELECT started_at_ms FROM network_open_outages WHERE id='network'").fetchone()
        if row:
            conn.execute(
                """
                UPDATE network_open_outages
                SET active_uplink=?, reason=?, status='down'
                WHERE id='network'
                """,
                (sample["active_uplink"], sample["reason"]),
            )
            return
        conn.execute(
            """
            INSERT INTO network_open_outages
                (id, started_at_ms, active_uplink, reason, status)
            VALUES ('network', ?, ?, ?, 'down')
            """,
            (now, sample["active_uplink"], sample["reason"]),
        )
        self._insert_event_conn(conn, {
            "timestamp_ms": now,
            "event_type": "outage_started",
            "severity": "error",
            "active_uplink": sample["active_uplink"],
            "interface": sample["active_uplink"],
            "status": "down",
            "reason": sample["reason"],
            "started_at_ms": now,
            "internet_ok": int(sample["internet_ok"]),
            "message": "Network outage started.",
        })

    def _resolve_outage_conn(self, conn: sqlite3.Connection, now: int, sample: dict) -> None:
        row = conn.execute("SELECT * FROM network_open_outages WHERE id='network'").fetchone()
        if not row:
            return
        started = int(row["started_at_ms"])
        duration = max(0, now - started)
        conn.execute("DELETE FROM network_open_outages WHERE id='network'")
        self._insert_event_conn(conn, {
            "timestamp_ms": now,
            "event_type": "outage_recovered",
            "severity": "info",
            "active_uplink": sample["active_uplink"],
            "interface": sample["active_uplink"],
            "status": "recovered",
            "reason": row["reason"] or sample["reason"],
            "started_at_ms": started,
            "ended_at_ms": now,
            "duration_ms": duration,
            "internet_ok": int(sample["internet_ok"]),
            "message": "Network outage recovered.",
        })

    def get_events(
        self,
        *,
        severity: str | None = None,
        since_ms: int | None = None,
        limit: int = 200,
    ) -> list[dict]:
        conditions: list[str] = []
        params: list = []
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
                    FROM network_events
                    {where}
                    ORDER BY timestamp_ms DESC, id DESC
                    LIMIT ?
                    """,
                    params,
                ).fetchall()
                return [dict(r) for r in rows]
        except Exception as exc:
            logger.error("Network audit event query failed: %s", exc)
            return []

    def summary(self) -> dict:
        now = _now_ms()
        try:
            with self._conn() as conn:
                status = conn.execute("SELECT * FROM network_status WHERE id='current'").fetchone()
                open_outage = conn.execute("SELECT * FROM network_open_outages WHERE id='network'").fetchone()
                last_outage = conn.execute(
                    """
                    SELECT * FROM network_events
                    WHERE event_type='outage_recovered'
                    ORDER BY timestamp_ms DESC, id DESC
                    LIMIT 1
                    """
                ).fetchone()
                last_switch = conn.execute(
                    """
                    SELECT * FROM network_events
                    WHERE event_type='uplink_switch'
                    ORDER BY timestamp_ms DESC, id DESC
                    LIMIT 1
                    """
                ).fetchone()
                last_recovery = conn.execute(
                    """
                    SELECT * FROM network_events
                    WHERE event_type IN ('recovery_action', 'tailscale_recovery')
                    ORDER BY timestamp_ms DESC, id DESC
                    LIMIT 1
                    """
                ).fetchone()
                counts = {
                    "uplink_switches": self._count(conn, "uplink_switch"),
                    "outage_starts": self._count(conn, "outage_started"),
                    "outage_recovered": self._count(conn, "outage_recovered"),
                    "recovery_actions": self._count(conn, "recovery_action") + self._count(conn, "tailscale_recovery"),
                    "interface_issues": self._count(conn, "interface_issue_started"),
                    "interface_recovered": self._count(conn, "interface_recovered"),
                }
                total_downtime = conn.execute(
                    "SELECT COALESCE(SUM(duration_ms),0) FROM network_events WHERE event_type='outage_recovered'"
                ).fetchone()[0] or 0

            payload: dict[str, Any] = {
                "counts": counts,
                "total_downtime_ms": int(total_downtime),
                "last_outage": dict(last_outage) if last_outage else None,
                "last_switch": dict(last_switch) if last_switch else None,
                "last_recovery": dict(last_recovery) if last_recovery else None,
                "open_outage": dict(open_outage) if open_outage else None,
            }
            if status:
                row = dict(status)
                since = int(row.get("active_uplink_since_ms") or now)
                row["active_duration_ms"] = max(0, now - since)
                payload["status"] = row
                payload["active_duration_ms"] = row["active_duration_ms"]
                payload["active_uplink_since_ms"] = since
                payload["uplink_switch_count"] = counts["uplink_switches"]
            if payload["open_outage"]:
                started = int(payload["open_outage"].get("started_at_ms") or now)
                payload["open_outage"]["duration_ms"] = max(0, now - started)
            return payload
        except Exception as exc:
            logger.error("Network audit summary failed: %s", exc)
            return {}

    def _count(self, conn: sqlite3.Connection, event_type: str) -> int:
        return int(conn.execute(
            "SELECT COUNT(*) FROM network_events WHERE event_type=?",
            (event_type,),
        ).fetchone()[0] or 0)


class NetworkAuditJob:
    def __init__(self, network_settings_store: Any, event_store: NetworkEventStore) -> None:
        self._network_settings_store = network_settings_store
        self._event_store = event_store

    def tick(self) -> None:
        state = self._network_settings_store.get_state()
        if isinstance(state, dict):
            self._event_store.record_state(state)
