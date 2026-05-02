"""
Data Forwarding configuration store.

Schema version 1.  Config file: {storage_root}/AES/forwarding_config.json

Each profile defines one upstream destination:
  - MQTT  : publish sensor readings to an MQTT broker
  - HTTPS : POST sensor readings to an HTTP endpoint

PES reads this file when AES sets the Redis key "forwarding_config" = "1".

Profile shape:
  id              — unique string (short UUID-like)
  name            — human label
  enabled         — whether PES should actively forward
  protocol        — "mqtt" | "https"
  mqtt            — MQTT-specific settings (present when protocol="mqtt")
  https           — HTTPS-specific settings (present when protocol="https")
  scope           — "all" | comma-separated "source:device_id" keys
  format          — "json" | "json_batch" | "csv"
"""
import json
import logging
import os
import secrets
import tempfile
import threading
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_MAX_PROFILES = 10


# ── Validation helpers ────────────────────────────────────────────────────────

def _str(v: Any, maxlen: int = 256, default: str = "") -> str:
    return str(v).strip()[:maxlen] if v else default


def _bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() in ("true", "1", "yes")
    return bool(v) if v is not None else default


def _int_clamp(v: Any, lo: int, hi: int, default: int) -> int:
    try:
        return max(lo, min(hi, int(v)))
    except (TypeError, ValueError):
        return default


def _merge_mqtt(raw: Any) -> dict:
    r = raw if isinstance(raw, dict) else {}
    return {
        "host":           _str(r.get("host"), 253),
        "port":           _int_clamp(r.get("port", 1883), 1, 65535, 1883),
        "tls":            _bool(r.get("tls"), False),
        "client_id":      _str(r.get("client_id"), 128),
        "username":       _str(r.get("username"), 128),
        "password":       _str(r.get("password"), 256),
        "topic_template": _str(r.get("topic_template", "metacrust/{device_id}/{metric}"), 512,
                              "metacrust/{device_id}/{metric}"),
        "qos":            _int_clamp(r.get("qos", 1), 0, 2, 1),
        "retain":         _bool(r.get("retain"), False),
    }


def _merge_https(raw: Any) -> dict:
    r = raw if isinstance(raw, dict) else {}
    auth_type = str(r.get("auth_type", "none"))
    if auth_type not in ("none", "bearer", "api_key", "basic"):
        auth_type = "none"
    return {
        "url":              _str(r.get("url"), 512),
        "auth_type":        auth_type,
        "auth_value":       _str(r.get("auth_value"), 512),
        "interval_seconds": _int_clamp(r.get("interval_seconds", 30), 1, 3600, 30),
        "timeout_seconds":  _int_clamp(r.get("timeout_seconds", 10), 1, 120, 10),
    }


def _merge_profile(raw: Any) -> dict | None:
    if not isinstance(raw, dict):
        return None
    protocol = str(raw.get("protocol", "mqtt")).lower()
    if protocol not in ("mqtt", "https"):
        protocol = "mqtt"
    fmt = str(raw.get("format", "json"))
    if fmt not in ("json", "json_batch", "csv"):
        fmt = "json"

    # Generate a stable id if missing
    profile_id = _str(raw.get("id"), 32)
    if not profile_id:
        profile_id = secrets.token_hex(8)

    profile: dict = {
        "id":       profile_id,
        "name":     _str(raw.get("name", "Unnamed Profile"), 64, "Unnamed Profile"),
        "enabled":  _bool(raw.get("enabled"), False),
        "protocol": protocol,
        "scope":    _str(raw.get("scope", "all"), 512, "all"),
        "format":   fmt,
    }

    if protocol == "mqtt":
        profile["mqtt"]  = _merge_mqtt(raw.get("mqtt"))
    else:
        profile["https"] = _merge_https(raw.get("https"))

    return profile


def _validate_and_merge(raw: Any) -> dict:
    r = raw if isinstance(raw, dict) else {}
    profiles = []
    for p in (r.get("profiles") or [])[:_MAX_PROFILES]:
        merged = _merge_profile(p)
        if merged:
            profiles.append(merged)
    return {"version": 1, "profiles": profiles}


# ── Store ─────────────────────────────────────────────────────────────────────

class ForwardingConfigStore:
    def __init__(self, storage_root: Path) -> None:
        self._path   = storage_root / "AES" / "forwarding_config.json"
        self._lock   = threading.Lock()
        self._config: dict = {}

    def ensure_initialized(self) -> None:
        with self._lock:
            self._config = self._load_locked()

    def get_config(self) -> dict:
        with self._lock:
            if not self._config:
                self._config = self._load_locked()
            return dict(self._config)

    def save_config(self, payload: Any) -> tuple[bool, dict]:
        try:
            merged = _validate_and_merge(payload)
        except Exception as exc:
            logger.error("Forwarding config validation error: %s", exc)
            return False, {"message": "Invalid configuration payload."}
        with self._lock:
            try:
                self._write_locked(merged)
                self._config = merged
                return True, {"message": "Forwarding configuration saved."}
            except Exception as exc:
                logger.error("Forwarding config write error: %s", exc)
                return False, {"message": "Failed to write configuration file."}

    def _load_locked(self) -> dict:
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            return _validate_and_merge(raw)
        except FileNotFoundError:
            defaults = _validate_and_merge({})
            try:
                self._write_locked(defaults)
            except Exception:
                pass
            return defaults
        except Exception as exc:
            logger.warning("Forwarding config unreadable (%s), using defaults", exc)
            return _validate_and_merge({})

    def _write_locked(self, config: dict) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self._path.parent, prefix=".forwarding_", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(config, f, indent=2)
            os.replace(tmp, self._path)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
