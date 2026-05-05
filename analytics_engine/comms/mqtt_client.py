"""
MQTT connection manager for one forwarding profile.

Wraps paho-mqtt 2.x with:
  - Automatic reconnection (exponential backoff, handled by paho)
  - TLS / mTLS via cert file paths stored by ForwardingConfigStore
  - Thread-safe publish (paho loop runs on its own background thread)
  - Structured logging — every significant state change is logged
"""
from __future__ import annotations

import logging
import ssl
import threading
from pathlib import Path

import paho.mqtt.client as mqtt

# Reconnect backoff range (paho handles the exponential curve internally)
_RECONNECT_MIN = 2   # seconds before first retry
_RECONNECT_MAX = 60  # cap


class MqttProfileClient:
    """
    One persistent MQTT connection for one forwarding profile.

    Lifecycle:
        client = MqttProfileClient(profile, gateway_id)
        client.start()          # non-blocking; loop thread starts
        client.publish(...)     # safe from any thread at any time
        client.stop()           # clean disconnect + thread teardown
    """

    def __init__(self, profile: dict, gateway_id: str) -> None:
        self._profile    = profile
        self._gateway_id = gateway_id
        self._cfg        = profile.get("mqtt", {})
        self._name       = profile.get("name", "unnamed")
        self._pid        = profile.get("id", "?")[:8]

        self._connected  = False
        self._client: mqtt.Client | None = None

        self._log = logging.getLogger(f"comms.mqtt[{self._name}]")

    # ── Public interface ──────────────────────────────────────────────────────

    def start(self) -> None:
        """Build the paho client, configure TLS if needed, and connect async."""
        host = self._cfg.get("host", "").strip()
        port = int(self._cfg.get("port", 1883))

        if not host:
            self._log.error("No broker host configured for profile '%s' — skipping", self._name)
            return

        try:
            self._client = self._build_client()
        except Exception as exc:
            self._log.error("Failed to build MQTT client for '%s': %s", self._name, exc)
            self._client = None
            return

        try:
            self._log.info(
                "Connecting to broker %s:%d%s …",
                host, port,
                " (TLS)" if self._cfg.get("tls") else "",
            )
            self._client.connect_async(host, port, keepalive=60)
            self._client.loop_start()
        except Exception as exc:
            self._log.error("connect_async failed for '%s': %s — will not retry until config reload", self._name, exc)
            self._client = None

    def stop(self) -> None:
        """Disconnect and shut down the paho loop thread."""
        if self._client is None:
            return
        try:
            self._client.disconnect()
            self._client.loop_stop()
            self._log.info("Disconnected from broker (profile '%s')", self._name)
        except Exception as exc:
            self._log.warning("Error during disconnect for '%s': %s", self._name, exc)
        finally:
            self._connected = False
            self._client    = None

    def publish(self, topic: str, payload: str, qos: int = 1, retain: bool = False) -> bool:
        """
        Publish a message. Returns True if the message was handed to paho, False otherwise.
        Safe to call from any thread; paho queues internally when QoS > 0.
        """
        if not self._connected or self._client is None:
            self._log.debug("Not connected — drop publish to %s", topic)
            return False
        try:
            info = self._client.publish(topic, payload, qos=qos, retain=retain)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                self._log.warning(
                    "publish() error rc=%d on topic '%s' (profile '%s')",
                    info.rc, topic, self._name,
                )
                return False
            self._log.debug("→ %s  %d bytes  QoS=%d", topic, len(payload), qos)
            return True
        except Exception as exc:
            self._log.error("Unexpected publish error on '%s': %s", topic, exc)
            return False

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ── Client construction ───────────────────────────────────────────────────

    def _build_client(self) -> mqtt.Client:
        cfg       = self._cfg
        client_id = (cfg.get("client_id") or "").strip() or \
                    f"metacrust_{self._gateway_id[-12:]}_{self._pid}"

        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            clean_session=True,
        )
        client.reconnect_delay_set(min_delay=_RECONNECT_MIN, max_delay=_RECONNECT_MAX)

        # Broker authentication
        username = cfg.get("username", "").strip()
        password = cfg.get("password", "")
        if username:
            client.username_pw_set(username, password or None)
            self._log.debug("Broker auth: username='%s'", username)

        # TLS / mTLS
        if cfg.get("tls"):
            self._configure_tls(client, cfg)

        # Callbacks
        client.on_connect    = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_publish    = self._on_publish

        return client

    def _configure_tls(self, client: mqtt.Client, cfg: dict) -> None:
        """
        Configure TLS on the paho client.
        Three possible modes:
          1. TLS with system CAs:      tls=True, no cert files
          2. TLS with custom CA:       tls=True, tls_ca_path set
          3. mTLS (mutual TLS):        tls=True, all three cert paths set
        """
        ca_path   = cfg.get("tls_ca_path",   "") or None
        cert_path = cfg.get("tls_cert_path", "") or None
        key_path  = cfg.get("tls_key_path",  "") or None

        # Validate all provided paths exist before touching the client
        for label, path in (("CA cert", ca_path), ("client cert", cert_path), ("client key", key_path)):
            if path and not Path(path).is_file():
                raise FileNotFoundError(
                    f"TLS {label} file not found: {path!r}  "
                    f"(profile '{self._name}') — fix in Data Forwarding or re-upload the file"
                )

        # Describe the security mode in the log
        if cert_path and key_path:
            mode = "mTLS (mutual TLS)"
        else:
            mode = "TLS"
        if ca_path:
            mode += " with custom CA"
        else:
            mode += " with system CAs"
        self._log.info("Configuring %s for profile '%s'", mode, self._name)

        # paho tls_set with None = use system trust store
        client.tls_set(
            ca_certs=ca_path,
            certfile=cert_path,
            keyfile=key_path,
            tls_version=ssl.PROTOCOL_TLS_CLIENT,
        )
        # Server certificate verification is ON by default (tls_insecure_set(False))
        # — never disable this in production

    # ── paho callbacks ────────────────────────────────────────────────────────

    def _on_connect(self, client, userdata, connect_flags, reason_code, properties) -> None:
        try:
            failed = reason_code.is_failure
        except AttributeError:
            failed = bool(reason_code)

        if failed:
            self._connected = False
            self._log.error(
                "Broker refused connection for profile '%s': %s  (will retry in %d–%d s)",
                self._name, reason_code, _RECONNECT_MIN, _RECONNECT_MAX,
            )
        else:
            self._connected = True
            self._log.info(
                "Connected to %s:%d  profile='%s'  QoS=%d  retain=%s",
                self._cfg.get("host", "?"),
                self._cfg.get("port", 1883),
                self._name,
                self._cfg.get("qos", 1),
                self._cfg.get("retain", False),
            )

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties) -> None:
        self._connected = False
        try:
            failed = reason_code.is_failure
        except AttributeError:
            failed = bool(reason_code)

        if failed:
            self._log.warning(
                "Disconnected unexpectedly from %s:%d  profile='%s'  reason=%s  "
                "— paho will reconnect automatically (backoff %d–%d s)",
                self._cfg.get("host", "?"),
                self._cfg.get("port", 1883),
                self._name,
                reason_code,
                _RECONNECT_MIN,
                _RECONNECT_MAX,
            )
        else:
            self._log.info("Cleanly disconnected from broker (profile '%s')", self._name)

    def _on_publish(self, client, userdata, mid, reason_code, properties) -> None:
        try:
            if reason_code.is_failure:
                self._log.warning(
                    "Publish delivery failed  profile='%s'  mid=%d  reason=%s",
                    self._name, mid, reason_code,
                )
        except AttributeError:
            pass  # QoS 0 — no reason_code
