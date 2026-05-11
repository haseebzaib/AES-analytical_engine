"""
HTTPS forwarding worker — POSTs sensor data, analytics, and events
to HTTPS/HTTP endpoints according to the active forwarding profiles.

Registered with AnalyticsRuntime:
    runtime.register_worker("https-forwarder", interval_seconds=1.0, tick_fn=forwarder.tick)

Payload strategy:
    Sensor data and analytics are BATCHED — all in-scope devices in a single
    POST per interval.  This keeps subprocess overhead to O(profiles) not
    O(profiles × devices).  Events are per-device POSTs fired immediately on
    status transitions regardless of the publish interval.

Config hot-reload:
    On every tick the config hash is compared.  If a profile is added, removed,
    or changed the corresponding client object is recreated — no AES restart
    required.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import TYPE_CHECKING

from .https_client import HttpsProfileClient

if TYPE_CHECKING:
    from analytics_engine.sensor_store import SensorStore
    from analytics_engine.analytical_store import AnalyticalStore
    from analytics_engine.interfaces.forwarding_config_store import ForwardingConfigStore
    from analytics_engine.forwarding_buffer_store import ForwardingBufferStore

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_ms() -> int:
    return int(time.time() * 1000)


def _profile_hash(profile: dict) -> str:
    h = profile.get("https", {})
    key = {
        "id":      profile.get("id"),
        "enabled": profile.get("enabled"),
        "scope":   profile.get("scope"),
        "https": {k: h.get(k) for k in (
            "host", "port", "tls",
            "tls_ca_path", "tls_cert_path", "tls_key_path",
            "sensor_path", "analytics_path", "events_path",
            "auth_type", "auth_value",
            "interval_seconds", "timeout_seconds",
        )},
    }
    return hashlib.md5(
        json.dumps(key, sort_keys=True).encode(), usedforsecurity=False,
    ).hexdigest()


def _in_scope(device: dict, scope: str) -> bool:
    if scope == "all":
        return True
    key = f"{device.get('source')}:{device.get('device_id')}"
    return key in {s.strip() for s in scope.split(",")}


# ── Forwarder ─────────────────────────────────────────────────────────────────

class HttpsForwarder:
    """
    Manages one HttpsProfileClient per enabled HTTPS profile and POSTs
    batched sensor / analytics payloads on the configured interval.

    Thread safety:
        tick() is called from a single BackgroundWorker thread.
        HttpsProfileClient.post() uses subprocess — each call is independent.
    """

    def __init__(
        self,
        sensor_store:            "SensorStore",
        analytical_store:        "AnalyticalStore | None",
        forwarding_config_store: "ForwardingConfigStore",
        gateway_id:              str,
        buffer_store:            "ForwardingBufferStore | None" = None,
    ) -> None:
        self._sensor_store            = sensor_store
        self._analytical_store        = analytical_store
        self._forwarding_config_store = forwarding_config_store
        self._gateway_id              = gateway_id
        self._buffer_store            = buffer_store

        # profile_id → HttpsProfileClient
        self._clients: dict[str, HttpsProfileClient] = {}
        # profile_id → last hash (detect config changes)
        self._profile_hashes: dict[str, str] = {}
        # profile_id → monotonic timestamp of last publish
        self._last_publish: dict[str, float] = {}
        # "source:device_id" → last known status (for event detection)
        self._prev_device_status: dict[str, str] = {}

        self._log = logging.getLogger("comms.https_forwarder")
        self._log.info("HttpsForwarder initialised  gateway_id=%s", gateway_id)

    # ── Entry point ───────────────────────────────────────────────────────────

    def tick(self) -> None:
        """Called every 1 s by BackgroundWorker.  Never raises."""
        try:
            self._sync_profiles()
        except Exception as exc:
            self._log.error("Profile sync error: %s", exc, exc_info=True)
        try:
            self._publish_due()
        except Exception as exc:
            self._log.error("Publish error: %s", exc, exc_info=True)

    def get_status(self) -> list[dict]:
        """Return status snapshot for all active HTTPS clients (used by /api/forwarding/status)."""
        statuses = []
        for pid, client in self._clients.items():
            s = client.get_status()
            if self._buffer_store:
                s["buffer"] = self._buffer_store.get_stats(pid)
            statuses.append(s)
        return statuses

    def _drain_buffer(self, client: HttpsProfileClient, profile: dict) -> None:
        """Drain buffered POST payloads before sending new data."""
        if not self._buffer_store:
            return
        pid = profile["id"]
        batch = self._buffer_store.drain_batch(pid)
        if not batch:
            return
        drained = 0
        for msg in batch:
            try:
                import json as _json
                payload = _json.loads(msg["payload_json"])
                ok = client.post(msg["path"], payload)
                if ok:
                    self._buffer_store.mark_sent(msg["id"], pid)
                    drained += 1
                else:
                    self._buffer_store.mark_failed(msg["id"], pid)
            except Exception as exc:
                self._log.debug("Buffer drain error for profile '%s': %s", profile.get("name"), exc)
        if drained:
            self._log.info(
                "Buffer: drained %d POST(s) for profile '%s'  remaining=%d",
                drained, profile.get("name"), self._buffer_store.pending_count(pid),
            )

    def stop(self) -> None:
        """Stop all active HTTPS clients (terminates openssl tunnel processes)."""
        count = len(self._clients)
        self._log.info("Stopping %d active HTTPS client(s) …", count)
        for pid, client in list(self._clients.items()):
            try:
                client.stop()
            except Exception as exc:
                self._log.warning("Error stopping client %s: %s", pid[:8], exc)
        self._clients.clear()
        self._profile_hashes.clear()
        if count:
            self._log.info("All HTTPS clients stopped")

    # ── Config hot-reload ─────────────────────────────────────────────────────

    def _sync_profiles(self) -> None:
        try:
            config = self._forwarding_config_store.get_config()
        except Exception as exc:
            self._log.error("Cannot read forwarding config: %s — keeping existing clients", exc)
            return

        wanted: dict[str, dict] = {
            p["id"]: p
            for p in config.get("profiles", [])
            if p.get("protocol") == "https" and p.get("enabled")
        }

        for pid in list(self._clients.keys()):
            if pid not in wanted:
                name = self._clients[pid]._name
                self._log.info("Profile '%s' disabled/removed — stopping HTTPS client", name)
                self._clients.pop(pid).stop()
                self._profile_hashes.pop(pid, None)
                self._last_publish.pop(pid, None)

        for pid, profile in wanted.items():
            current_hash = _profile_hash(profile)
            if pid in self._clients:
                if self._profile_hashes.get(pid) == current_hash:
                    continue
                self._log.info(
                    "Profile '%s' config changed — restarting HTTPS client",
                    profile.get("name"),
                )
                self._clients.pop(pid).stop()
                self._last_publish.pop(pid, None)

            h = profile.get("https", {})
            self._log.info(
                "Starting HTTPS client  profile='%s'  endpoint=%s://%s:%d%s",
                profile.get("name"),
                "https" if h.get("tls", True) else "http",
                h.get("host", "?"),
                h.get("port", 443),
                "  mTLS=ON" if h.get("tls_cert_path") else "",
            )
            client = HttpsProfileClient(profile, self._gateway_id)
            client.start()
            self._clients[pid] = client
            self._profile_hashes[pid] = current_hash

    # ── Publish scheduling ────────────────────────────────────────────────────

    def _publish_due(self) -> None:
        if not self._clients:
            return

        try:
            config = self._forwarding_config_store.get_config()
        except Exception:
            return

        profile_map = {p["id"]: p for p in config.get("profiles", [])}
        now = time.monotonic()

        try:
            devices = self._sensor_store.live_devices()
        except Exception as exc:
            self._log.error("Cannot fetch live devices from Redis: %s", exc)
            return

        for pid, client in list(self._clients.items()):
            profile = profile_map.get(pid)
            if not profile:
                continue

            interval = profile["https"].get("interval_seconds", 30)

            # Events fire immediately regardless of publish interval
            self._check_status_events(client, profile, devices)

            if now - self._last_publish.get(pid, 0) < interval:
                continue

            self._last_publish[pid] = now

            # Drain backlog before sending new data
            self._drain_buffer(client, profile)

            # Snapshot buffer level for sparkline
            if self._buffer_store:
                self._buffer_store.snapshot_level(pid)

            self._post_sensor_batch(client, profile, devices)
            self._post_analytics_batch(client, profile, devices)

    # ── Sensor batch ──────────────────────────────────────────────────────────

    def _post_sensor_batch(
        self,
        client:  HttpsProfileClient,
        profile: dict,
        devices: list[dict],
    ) -> None:
        scope         = profile.get("scope", "all")
        sensor_path   = profile["https"].get("sensor_path", "")
        if not sensor_path:
            return

        device_payloads = []
        for device in devices:
            if not _in_scope(device, scope):
                continue
            src = device.get("source", "")
            did = device.get("device_id", "")
            if not src or not did:
                continue

            readings: dict = {}
            for mname, m in (device.get("metrics") or {}).items():
                if m.get("value") is None:
                    continue
                readings[mname] = {
                    "value":   m["value"],
                    "unit":    m.get("unit", ""),
                    "quality": m.get("quality", "unknown"),
                }

            device_payloads.append({
                "device_id": did,
                "source":    src,
                "status":    device.get("status", "unknown"),
                "readings":  readings,
            })

        if not device_payloads:
            return

        payload = {
            "gateway_id": self._gateway_id,
            "ts":         _now_ms(),
            "devices":    device_payloads,
        }
        self._safe_post(client, sensor_path, payload, "sensor", profile.get("id", ""))

    # ── Analytics batch ───────────────────────────────────────────────────────

    def _post_analytics_batch(
        self,
        client:  HttpsProfileClient,
        profile: dict,
        devices: list[dict],
    ) -> None:
        if self._analytical_store is None:
            return

        analytics_path = profile["https"].get("analytics_path", "")
        if not analytics_path:
            return

        scope = profile.get("scope", "all")
        device_analytics = []

        for device in devices:
            if not _in_scope(device, scope):
                continue
            src = device.get("source", "")
            did = device.get("device_id", "")
            if not src or not did:
                continue

            entry: dict = {"device_id": did, "source": src}

            try:
                rows = self._analytical_store.get_metric_stats(src, did)
                if rows:
                    stats: dict = {}
                    for r in rows:
                        metric = r["metric_name"]
                        win    = r["window"]
                        stats.setdefault(metric, {})[win] = {
                            "avg":    r.get("avg"),
                            "min":    r.get("min"),
                            "max":    r.get("max"),
                            "stddev": r.get("stddev"),
                        }
                    entry["stats"] = stats
            except Exception as exc:
                self._log.debug("Stats read failed for %s/%s: %s", src, did, exc)

            try:
                snapshots = self._analytical_store.get_trend_snapshots(src, did)
                if snapshots:
                    trends: dict = {}
                    for s in snapshots:
                        trend_entry: dict = {
                            "direction":     s.get("direction"),
                            "slope_per_min": s.get("slope"),
                        }
                        if s.get("ttt_minutes") is not None:
                            trend_entry["ttt_minutes"] = s["ttt_minutes"]
                        trends[s["metric_name"]] = trend_entry
                    entry["trends"] = trends
            except Exception as exc:
                self._log.debug("Trends read failed for %s/%s: %s", src, did, exc)

            try:
                rules = self._analytical_store.get_alert_rules(
                    source=src, device_id=did, enabled_only=True,
                )
                if rules:
                    entry["active_alerts"] = [
                        {
                            "rule_id":   r["id"],
                            "metric":    r["metric_name"],
                            "condition": r["condition"],
                            "threshold": r["threshold"],
                            "severity":  r.get("severity", "warning"),
                        }
                        for r in rules
                    ]
            except Exception as exc:
                self._log.debug("Alert rules read failed for %s/%s: %s", src, did, exc)

            if len(entry) > 2:   # has more than just device_id + source
                device_analytics.append(entry)

        if not device_analytics:
            return

        payload = {
            "gateway_id": self._gateway_id,
            "ts":         _now_ms(),
            "devices":    device_analytics,
        }
        self._safe_post(client, analytics_path, payload, "analytics", profile.get("id", ""))

    # ── Events (status changes) ───────────────────────────────────────────────

    def _check_status_events(
        self,
        client:  HttpsProfileClient,
        profile: dict,
        devices: list[dict],
    ) -> None:
        events_path = profile["https"].get("events_path", "")
        scope       = profile.get("scope", "all")

        for device in devices:
            src = device.get("source", "")
            did = device.get("device_id", "")
            if not src or not did:
                continue
            if not _in_scope(device, scope):
                continue

            key        = f"{src}:{did}"
            new_status = device.get("status", "unknown")
            old_status = self._prev_device_status.get(key)

            if old_status is None:
                self._prev_device_status[key] = new_status
                continue

            if new_status == old_status:
                continue

            self._prev_device_status[key] = new_status
            self._log.info(
                "Status change  %s/%s  %s → %s  profile='%s'",
                src, did, old_status, new_status, profile.get("name"),
            )

            if not events_path:
                continue

            err_msg = ""
            if device.get("error") and isinstance(device["error"], dict):
                err_msg = device["error"].get("message", "")

            payload = {
                "gateway_id": self._gateway_id,
                "ts":         _now_ms(),
                "device_id":  did,
                "source":     src,
                "type":       "status_change",
                "payload": {
                    "previous": old_status,
                    "current":  new_status,
                    "message":  err_msg,
                },
            }
            self._safe_post(client, events_path, payload, "event", profile.get("id", ""))

    # ── Safe post wrapper ─────────────────────────────────────────────────────

    def _safe_post(
        self,
        client:     HttpsProfileClient,
        path:       str,
        payload:    dict,
        label:      str,
        profile_id: str = "",
    ) -> None:
        try:
            body_str = json.dumps(payload, ensure_ascii=False, indent=2)
            ok = client.post(path, payload)
            if ok:
                self._log.debug("POST %s [%s]  bytes=%d\n%s", path, label, len(body_str), body_str)
            elif self._buffer_store and profile_id:
                self._buffer_store.enqueue(profile_id, "https", path, body_str)
                self._log.debug("Buffered POST %s [%s] for profile '%s'", path, label, profile_id)
        except (TypeError, ValueError) as exc:
            self._log.error("JSON serialisation error for %s [%s]: %s", path, label, exc)
        except Exception as exc:
            self._log.error("Unexpected error posting to %s [%s]: %s", path, label, exc)
