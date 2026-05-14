"""
MQTT forwarding worker — publishes sensor data, analytics, and events
to MQTT brokers according to the active forwarding profiles.

Registered with AnalyticsRuntime:
    runtime.register_worker("mqtt-forwarder", interval_seconds=1.0, tick_fn=forwarder.tick)

The 1-second tick does NOT publish every second. It checks whether each
profile's publish interval has elapsed, then publishes in bulk if so.

Config hot-reload:
    On every tick the config hash is compared. If a profile is added, removed,
    or changed (including TLS config), the corresponding client is stopped and
    restarted with the new settings — no AES restart required.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import TYPE_CHECKING

from .mqtt_client import MqttProfileClient

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
    """MD5 of all fields that affect how a client must be configured."""
    key = {
        "id":      profile.get("id"),
        "enabled": profile.get("enabled"),
        "scope":   profile.get("scope"),
        "mqtt":    {k: profile.get("mqtt", {}).get(k) for k in (
            "host", "port", "tls",
            "tls_ca_path", "tls_cert_path", "tls_key_path",
            "client_id", "username", "password",
            "qos", "retain", "interval_seconds",
        )},
    }
    return hashlib.md5(
        json.dumps(key, sort_keys=True).encode(), usedforsecurity=False
    ).hexdigest()


def _in_scope(device: dict, scope: str) -> bool:
    """Return True if this device should be forwarded under the given scope."""
    if scope == "all":
        return True
    key = f"{device.get('source')}:{device.get('device_id')}"
    return key in {s.strip() for s in scope.split(",")}


# ── Forwarder ─────────────────────────────────────────────────────────────────

class MqttForwarder:
    """
    Manages a pool of MqttProfileClients (one per enabled MQTT profile)
    and publishes data to each broker on the configured interval.

    Thread safety:
        tick() is called from a single BackgroundWorker thread, so internal
        state does not need locking. MqttProfileClient.publish() is thread-safe.
    """

    def __init__(
        self,
        sensor_store:             "SensorStore",
        analytical_store:         "AnalyticalStore | None",
        forwarding_config_store:  "ForwardingConfigStore",
        gateway_id:               str,
        buffer_store:             "ForwardingBufferStore | None" = None,
    ) -> None:
        self._sensor_store            = sensor_store
        self._analytical_store        = analytical_store
        self._forwarding_config_store = forwarding_config_store
        self._gateway_id              = gateway_id
        self._buffer_store            = buffer_store

        # profile_id → MqttProfileClient
        self._clients: dict[str, MqttProfileClient] = {}
        # profile_id → last hash (detect changes)
        self._profile_hashes: dict[str, str] = {}
        # profile_id → monotonic timestamp of last publish
        self._last_publish: dict[str, float] = {}
        # "source:device_id" → last known status (for event detection)
        self._prev_device_status: dict[str, str] = {}

        self._log = logging.getLogger("comms.mqtt_forwarder")
        self._log.info("MqttForwarder initialised  gateway_id=%s  buffer=%s",
                       gateway_id, "enabled" if buffer_store else "disabled")

    # ── Entry point ───────────────────────────────────────────────────────────

    def tick(self) -> None:
        """Called every 1 s by the BackgroundWorker. Never raises."""
        try:
            self._sync_profiles()
        except Exception as exc:
            self._log.error("Profile sync error: %s", exc, exc_info=True)
        try:
            self._publish_due()
        except Exception as exc:
            self._log.error("Publish error: %s", exc, exc_info=True)

    def get_status(self) -> list[dict]:
        """Return status snapshot for all active MQTT clients (used by /api/forwarding/status)."""
        statuses = []
        for pid, client in self._clients.items():
            s = client.get_status()
            if self._buffer_store:
                s["buffer"] = self._buffer_store.get_stats(pid)
                s["open_outage"] = self._buffer_store.get_open_outage(pid)
            statuses.append(s)
        return statuses

    def _audit_outage(
        self,
        profile: dict,
        client: MqttProfileClient,
        reason: str,
        *,
        status: str = "down",
    ) -> None:
        if not self._buffer_store:
            return
        pid = profile.get("id", "")
        st = client.get_status()
        self._buffer_store.begin_outage(
            pid,
            profile.get("name", st.get("profile_name", "")),
            "mqtt",
            st.get("broker", ""),
            reason,
            severity="warning" if status == "connecting" else "error",
            status=status,
            pending_count=self._buffer_store.pending_count(pid),
        )

    def _audit_recovered(self, profile: dict, client: MqttProfileClient) -> None:
        if not self._buffer_store:
            return
        pid = profile.get("id", "")
        st = client.get_status()
        self._buffer_store.resolve_outage(
            pid,
            profile.get("name", st.get("profile_name", "")),
            "mqtt",
            st.get("broker", ""),
            pending_count=self._buffer_store.pending_count(pid),
        )

    def _drain_buffer(self, client: MqttProfileClient, profile: dict) -> None:
        """Drain buffered messages for a profile before publishing new data."""
        if not self._buffer_store or not client.is_connected:
            return
        pid = profile["id"]
        batch = self._buffer_store.drain_batch(pid)
        if not batch:
            return
        qos    = profile["mqtt"].get("qos", 1)
        retain = profile["mqtt"].get("retain", False)
        drained = 0
        for msg in batch:
            try:
                ok = client.publish(msg["path"], msg["payload_json"], qos=qos, retain=retain)
                if ok:
                    self._buffer_store.mark_sent(msg["id"], pid)
                    self._audit_recovered(profile, client)
                    drained += 1
                else:
                    self._buffer_store.mark_failed(msg["id"], pid)
            except Exception as exc:
                self._log.debug("Buffer drain error for profile '%s': %s", profile.get("name"), exc)
        if drained:
            self._log.info(
                "Buffer: drained %d message(s) for profile '%s'  remaining=%d",
                drained, profile.get("name"), self._buffer_store.pending_count(pid),
            )

    def stop(self) -> None:
        """Disconnect all active clients cleanly. Called on AES shutdown."""
        count = len(self._clients)
        self._log.info("Stopping %d active MQTT client(s) …", count)
        for pid, client in list(self._clients.items()):
            try:
                client.stop()
            except Exception as exc:
                self._log.warning("Error stopping client %s: %s", pid[:8], exc)
        self._clients.clear()
        self._profile_hashes.clear()
        if count:
            self._log.info("All MQTT clients stopped")

    # ── Config hot-reload ─────────────────────────────────────────────────────

    def _sync_profiles(self) -> None:
        """
        Compare current config against running clients.
        Start new clients, stop removed/disabled ones, restart changed ones.
        All changes are logged so remote admins can see what happened.
        """
        try:
            config = self._forwarding_config_store.get_config()
        except Exception as exc:
            self._log.error("Cannot read forwarding config: %s — keeping existing clients", exc)
            return

        # Only MQTT profiles that are enabled
        wanted: dict[str, dict] = {
            p["id"]: p
            for p in config.get("profiles", [])
            if p.get("protocol") == "mqtt" and p.get("enabled")
        }

        # ── Stop clients no longer wanted ─────────────────────────────────────
        for pid in list(self._clients.keys()):
            if pid not in wanted:
                name = self._clients[pid]._name
                self._log.info("Profile '%s' disabled/removed — disconnecting", name)
                self._clients.pop(pid).stop()
                self._profile_hashes.pop(pid, None)
                self._last_publish.pop(pid, None)

        # ── Start or restart clients for wanted profiles ───────────────────────
        for pid, profile in wanted.items():
            current_hash = _profile_hash(profile)

            if pid in self._clients:
                if self._profile_hashes.get(pid) == current_hash:
                    continue  # nothing changed for this profile
                # Something changed — restart with new settings
                self._log.info(
                    "Profile '%s' config changed — restarting client",
                    profile.get("name"),
                )
                self._clients.pop(pid).stop()
                self._last_publish.pop(pid, None)

            # Start a fresh client
            self._log.info(
                "Starting MQTT client  profile='%s'  broker=%s:%d%s",
                profile.get("name"),
                profile["mqtt"].get("host", "?"),
                profile["mqtt"].get("port", 1883),
                "  TLS=ON" if profile["mqtt"].get("tls") else "",
            )
            client = MqttProfileClient(profile, self._gateway_id)
            client.start()
            self._clients[pid] = client
            self._profile_hashes[pid] = current_hash

    # ── Publish scheduling ────────────────────────────────────────────────────

    def _publish_due(self) -> None:
        """For each client whose interval has elapsed, publish all device data."""
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

            if client.is_connected:
                self._audit_recovered(profile, client)
            else:
                st = client.get_status()
                self._audit_outage(
                    profile,
                    client,
                    st.get("last_error") or "MQTT client not connected.",
                    status=st.get("state", "down"),
                )

            interval = profile["mqtt"].get("interval_seconds", 5)
            if now - self._last_publish.get(pid, 0) < interval:
                # Check events even when data interval hasn't elapsed
                self._check_status_events(client, profile, devices)
                continue

            self._last_publish[pid] = now

            if not client.is_connected:
                self._log.debug(
                    "Profile '%s' interval elapsed but not connected yet — skipping",
                    profile.get("name"),
                )
                st = client.get_status()
                reason = st.get("last_error") or "MQTT client not connected."
                self._audit_outage(
                    profile,
                    client,
                    reason,
                    status=st.get("state", "down"),
                )
                continue

            # Drain backlog before sending new data (backlog has priority)
            self._drain_buffer(client, profile)

            # Snapshot buffer level for sparkline history
            if self._buffer_store:
                self._buffer_store.snapshot_level(pid)

            self._publish_all(client, profile, devices)

    # ── Publish all data for all in-scope devices ─────────────────────────────

    def _publish_all(self, client: MqttProfileClient, profile: dict, devices: list[dict]) -> None:
        scope  = profile.get("scope", "all")
        qos    = profile["mqtt"].get("qos", 1)
        retain = profile["mqtt"].get("retain", False)
        pid    = profile.get("id", "")

        published = 0
        for device in devices:
            if not _in_scope(device, scope):
                continue
            src = device.get("source", "")
            did = device.get("device_id", "")
            if not src or not did:
                continue

            self._pub_sensor_data(client, device, src, did, qos, retain, pid)
            self._pub_analytics(client, src, did, qos, pid)
            self._check_status_events(client, profile, [device])
            published += 1

        if published:
            self._log.info(
                "Published to %d device(s)  profile='%s'",
                published, profile.get("name"),
            )

    # ── sensor_data ───────────────────────────────────────────────────────────

    def _pub_sensor_data(
        self, client: MqttProfileClient,
        device: dict, src: str, did: str, qos: int, retain: bool,
        profile_id: str = "",
    ) -> None:
        readings: dict = {}
        for mname, m in (device.get("metrics") or {}).items():
            if m.get("value") is None:
                continue
            readings[mname] = {
                "value":   m["value"],
                "unit":    m.get("unit", ""),
                "quality": m.get("quality", "unknown"),
            }

        payload = {
            "gateway_id": self._gateway_id,
            "ts":         _now_ms(),
            "device_id":  did,
            "source":     src,
            "status":     device.get("status", "unknown"),
            "readings":   readings,
        }
        topic = f"{self._gateway_id}/{src}/{did}/sensor_data"
        self._safe_publish(client, topic, payload, qos, retain, profile_id)

    # ── analytics ─────────────────────────────────────────────────────────────

    def _pub_analytics(
        self, client: MqttProfileClient,
        src: str, did: str, qos: int,
        profile_id: str = "",
    ) -> None:
        if self._analytical_store is None:
            return

        payload: dict = {
            "gateway_id": self._gateway_id,
            "ts":         _now_ms(),
            "device_id":  did,
            "source":     src,
        }

        # Rolling stats (5min / 1hr / 24hr)
        try:
            rows = self._analytical_store.get_metric_stats(src, did)
            if rows:
                stats: dict[str, dict] = {}
                for r in rows:
                    metric = r["metric_name"]
                    win    = r["window"]
                    stats.setdefault(metric, {})[win] = {
                        "avg":    r.get("avg"),
                        "min":    r.get("min"),
                        "max":    r.get("max"),
                        "stddev": r.get("stddev"),
                    }
                payload["stats"] = stats
        except Exception as exc:
            self._log.debug("Stats read failed for %s/%s: %s", src, did, exc)

        # Trend snapshots
        try:
            snapshots = self._analytical_store.get_trend_snapshots(src, did)
            if snapshots:
                trends: dict[str, dict] = {}
                for s in snapshots:
                    entry: dict = {
                        "direction":     s.get("direction"),
                        "slope_per_min": s.get("slope"),
                    }
                    if s.get("ttt_minutes") is not None:
                        entry["ttt_minutes"] = s["ttt_minutes"]
                    trends[s["metric_name"]] = entry
                payload["trends"] = trends
        except Exception as exc:
            self._log.debug("Trends read failed for %s/%s: %s", src, did, exc)

        # Alert rule states
        try:
            rules = self._analytical_store.get_alert_rules(
                source=src, device_id=did, enabled_only=True
            )
            if rules:
                payload["active_alerts"] = [
                    {
                        "rule_id":      r["id"],
                        "metric":       r["metric_name"],
                        "condition":    r["condition"],
                        "threshold":    r["threshold"],
                        "severity":     r.get("severity", "warning"),
                        "state":        "ok",
                        "triggered_at": None,
                    }
                    for r in rules
                ]
        except Exception as exc:
            self._log.debug("Alert rules read failed for %s/%s: %s", src, did, exc)

        # Skip publish if analytics is empty (just header fields)
        if len(payload) <= 4:
            return

        topic = f"{self._gateway_id}/{src}/{did}/analytics"
        self._safe_publish(client, topic, payload, qos, retain=False, profile_id=profile_id)

    # ── events (status changes) ───────────────────────────────────────────────

    def _check_status_events(
        self,
        client: MqttProfileClient,
        profile: dict,
        devices: list[dict],
    ) -> None:
        """
        Compare each device's current status to the last known status.
        Publish a status_change event the moment a transition is detected.
        Events fire immediately regardless of publish interval.
        """
        qos = profile["mqtt"].get("qos", 1)

        for device in devices:
            src = device.get("source", "")
            did = device.get("device_id", "")
            if not src or not did:
                continue

            key        = f"{src}:{did}"
            new_status = device.get("status", "unknown")
            old_status = self._prev_device_status.get(key)

            if old_status is None:
                # First observation — record without firing an event
                self._prev_device_status[key] = new_status
                continue

            if new_status == old_status:
                continue

            # Status changed
            self._prev_device_status[key] = new_status
            self._log.info(
                "Status change  %s/%s  %s → %s  profile='%s'",
                src, did, old_status, new_status, profile.get("name"),
            )

            if not client.is_connected:
                self._log.warning(
                    "Status change event for %s/%s could not be published — not connected",
                    src, did,
                )
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
            topic = f"{self._gateway_id}/{src}/{did}/events"
            self._safe_publish(client, topic, payload, qos, retain=False)

    # ── Safe publish wrapper ──────────────────────────────────────────────────

    def _safe_publish(
        self,
        client: MqttProfileClient,
        topic: str,
        payload: dict,
        qos: int,
        retain: bool,
        profile_id: str = "",
    ) -> None:
        try:
            body = json.dumps(payload, ensure_ascii=False, indent=2)
            ok   = client.publish(topic, body, qos=qos, retain=retain)
            if ok:
                if self._buffer_store and profile_id:
                    self._buffer_store.resolve_outage(
                        profile_id,
                        client.get_status().get("profile_name", ""),
                        "mqtt",
                        client.get_status().get("broker", ""),
                        pending_count=self._buffer_store.pending_count(profile_id),
                    )
                self._log.info(
                    "PUBLISH  topic=%s  qos=%d  retain=%s  bytes=%d",
                    topic, qos, retain, len(body),
                )
                self._log.debug("PAYLOAD  %s\n%s", topic, body)
            elif self._buffer_store and profile_id:
                st = client.get_status()
                self._buffer_store.begin_outage(
                    profile_id,
                    st.get("profile_name", ""),
                    "mqtt",
                    st.get("broker", ""),
                    st.get("last_error") or "MQTT publish failed; message buffered locally.",
                    severity="error",
                    status=st.get("state", "down"),
                    pending_count=self._buffer_store.pending_count(profile_id),
                )
                # Not connected or publish queue full — buffer for later
                self._buffer_store.enqueue(
                    profile_id, "mqtt", topic, body, qos=qos, retain=retain,
                )
                self._log.debug("Buffered topic=%s for profile '%s'", topic, profile_id)
        except (TypeError, ValueError) as exc:
            self._log.error("JSON serialisation error for topic %s: %s", topic, exc)
        except Exception as exc:
            self._log.error("Unexpected error publishing to %s: %s", topic, exc)
