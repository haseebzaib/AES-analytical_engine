"""
Data Forwarding configuration store.

Schema version 2.  Config file: {storage_root}/AES/forwarding_config.json
TLS cert files:    {storage_root}/AES/.tls/{profile_id}_{ca|cert|key}.pem  (chmod 600)

Each profile defines one upstream destination:
  - MQTT / MQTTS : publish sensor readings to an MQTT broker
  - HTTPS / mTLS : POST sensor readings to an HTTP endpoint

The JSON config stores file *paths* for certificates — PEM content is never
stored inline. PES reads the paths and passes them directly to its TLS library.

Profile shape (on-disk JSON):
  id                — unique string (short UUID-like)
  name              — human label
  enabled           — whether PES should actively forward
  protocol          — "mqtt" | "https"
  mqtt / https      — protocol-specific settings (cert paths, not PEM content)
  scope             — "all" | comma-separated "source:device_id" keys

UI API:
  GET  /api/forwarding/config  → returns _loaded flags, never PEM content
  POST /api/forwarding/config  → accepts PEM content (or null=keep, ""=clear)
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


# ── Primitive validators ──────────────────────────────────────────────────────

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


# ── Store ─────────────────────────────────────────────────────────────────────

class ForwardingConfigStore:
    def __init__(self, storage_root: Path) -> None:
        self._path    = storage_root / "AES" / "forwarding_config.json"
        self._tls_dir = storage_root / "AES" / ".tls"
        self._lock    = threading.Lock()
        self._config: dict = {}

    # ── Cert file helpers ─────────────────────────────────────────────────────

    def _cert_path(self, profile_id: str, name: str) -> Path:
        return self._tls_dir / f"{profile_id}_{name}.pem"

    def _write_cert(self, profile_id: str, name: str, pem: str) -> str:
        """Write PEM to hidden .tls directory, return absolute path string."""
        self._tls_dir.mkdir(parents=True, exist_ok=True)
        try:
            self._tls_dir.chmod(0o700)
        except OSError:
            pass
        p = self._cert_path(profile_id, name)
        p.write_text(pem, encoding="utf-8")
        try:
            p.chmod(0o600)
        except OSError:
            pass
        return str(p)

    def _delete_cert(self, profile_id: str, name: str) -> None:
        try:
            self._cert_path(profile_id, name).unlink(missing_ok=True)
        except OSError:
            pass

    def _delete_all_certs(self, profile_id: str) -> None:
        for name in ("ca", "cert", "key"):
            self._delete_cert(profile_id, name)

    def _handle_cert(
        self,
        profile_id: str,
        name: str,
        pem_field: Any,
        existing_path: str,
    ) -> str:
        """
        Decide what to do with a certificate field from the UI payload.

        pem_field semantics:
          None / absent  → keep existing file, return existing_path unchanged
          ""             → user cleared the cert; delete file, return ""
          "<pem>"        → new content; write to file, return new path
        """
        if pem_field is None:
            return existing_path  # keep

        pem = str(pem_field).strip()
        if not pem:
            # Explicit clear
            if existing_path:
                try:
                    Path(existing_path).unlink(missing_ok=True)
                except OSError:
                    pass
            return ""

        # New PEM content
        return self._write_cert(profile_id, name, pem)

    # ── Profile builder (UI payload → internal path-based format) ────────────

    def _process_profile(self, raw: Any, existing: dict) -> dict | None:
        if not isinstance(raw, dict):
            return None

        protocol = str(raw.get("protocol", "mqtt")).lower()
        if protocol not in ("mqtt", "https"):
            protocol = "mqtt"

        profile_id = _str(raw.get("id"), 32)
        if not profile_id:
            profile_id = secrets.token_hex(8)

        profile: dict = {
            "id":       profile_id,
            "name":     _str(raw.get("name", "Unnamed Profile"), 64, "Unnamed Profile"),
            "enabled":  _bool(raw.get("enabled"), False),
            "protocol": protocol,
            "scope":    _str(raw.get("scope", "all"), 512, "all"),
        }

        if protocol == "mqtt":
            r = raw.get("mqtt") or {}
            ex = existing.get("mqtt", {})
            profile["mqtt"] = {
                "host":             _str(r.get("host"), 253),
                "port":             _int_clamp(r.get("port", 1883), 1, 65535, 1883),
                "tls":              _bool(r.get("tls"), False),
                "tls_ca_path":      self._handle_cert(profile_id, "ca",   r.get("tls_ca"),   ex.get("tls_ca_path",   "")),
                "tls_cert_path":    self._handle_cert(profile_id, "cert", r.get("tls_cert"), ex.get("tls_cert_path", "")),
                "tls_key_path":     self._handle_cert(profile_id, "key",  r.get("tls_key"),  ex.get("tls_key_path",  "")),
                "client_id":        _str(r.get("client_id"), 128),
                "username":         _str(r.get("username"), 128),
                "password":         _str(r.get("password"), 256),
                "qos":              _int_clamp(r.get("qos", 1), 0, 2, 1),
                "retain":           _bool(r.get("retain"), False),
                "interval_seconds": _int_clamp(r.get("interval_seconds", 5), 1, 3600, 5),
            }
        else:
            r = raw.get("https") or {}
            ex = existing.get("https", {})
            auth_type = str(r.get("auth_type", "none"))
            if auth_type not in ("none", "bearer", "api_key", "basic"):
                auth_type = "none"
            profile["https"] = {
                "host":             _str(r.get("host"), 253),
                "port":             _int_clamp(r.get("port", 443), 1, 65535, 443),
                "sensor_path":      _str(r.get("sensor_path", "/ingest"), 512, "/ingest"),
                "analytics_path":   _str(r.get("analytics_path", ""), 512),
                "events_path":      _str(r.get("events_path", ""), 512),
                "auth_type":        auth_type,
                "auth_value":       _str(r.get("auth_value"), 512),
                "tls_ca_path":      self._handle_cert(profile_id, "ca",   r.get("tls_ca"),   ex.get("tls_ca_path",   "")),
                "tls_cert_path":    self._handle_cert(profile_id, "cert", r.get("tls_cert"), ex.get("tls_cert_path", "")),
                "tls_key_path":     self._handle_cert(profile_id, "key",  r.get("tls_key"),  ex.get("tls_key_path",  "")),
                "interval_seconds": _int_clamp(r.get("interval_seconds", 30), 1, 3600, 30),
                "timeout_seconds":  _int_clamp(r.get("timeout_seconds", 10), 1, 120, 10),
            }

        return profile

    # ── Raw load validator (disk → internal) ──────────────────────────────────

    def _load_raw(self, data: Any) -> dict:
        """Validate and normalise data that was read from disk (already path-based)."""
        r = data if isinstance(data, dict) else {}
        profiles = []
        for p in (r.get("profiles") or [])[:_MAX_PROFILES]:
            if not isinstance(p, dict):
                continue
            protocol = str(p.get("protocol", "mqtt")).lower()
            if protocol not in ("mqtt", "https"):
                protocol = "mqtt"
            profile_id = _str(p.get("id"), 32) or secrets.token_hex(8)
            profile: dict = {
                "id":       profile_id,
                "name":     _str(p.get("name", "Unnamed Profile"), 64, "Unnamed Profile"),
                "enabled":  _bool(p.get("enabled"), False),
                "protocol": protocol,
                "scope":    _str(p.get("scope", "all"), 512, "all"),
            }
            if protocol == "mqtt":
                m = p.get("mqtt") or {}
                profile["mqtt"] = {
                    "host":             _str(m.get("host"), 253),
                    "port":             _int_clamp(m.get("port", 1883), 1, 65535, 1883),
                    "tls":              _bool(m.get("tls"), False),
                    "tls_ca_path":      _str(m.get("tls_ca_path",   ""), 512),
                    "tls_cert_path":    _str(m.get("tls_cert_path", ""), 512),
                    "tls_key_path":     _str(m.get("tls_key_path",  ""), 512),
                    "client_id":        _str(m.get("client_id"), 128),
                    "username":         _str(m.get("username"), 128),
                    "password":         _str(m.get("password"), 256),
                    "qos":              _int_clamp(m.get("qos", 1), 0, 2, 1),
                    "retain":           _bool(m.get("retain"), False),
                    "interval_seconds": _int_clamp(m.get("interval_seconds", 5), 1, 3600, 5),
                }
            else:
                h = p.get("https") or {}
                auth_type = str(h.get("auth_type", "none"))
                if auth_type not in ("none", "bearer", "api_key", "basic"):
                    auth_type = "none"
                profile["https"] = {
                    "host":             _str(h.get("host"), 253),
                    "port":             _int_clamp(h.get("port", 443), 1, 65535, 443),
                    "sensor_path":      _str(h.get("sensor_path", "/ingest"), 512, "/ingest"),
                    "analytics_path":   _str(h.get("analytics_path", ""), 512),
                    "events_path":      _str(h.get("events_path", ""), 512),
                    "auth_type":        auth_type,
                    "auth_value":       _str(h.get("auth_value"), 512),
                    "tls_ca_path":      _str(h.get("tls_ca_path",   ""), 512),
                    "tls_cert_path":    _str(h.get("tls_cert_path", ""), 512),
                    "tls_key_path":     _str(h.get("tls_key_path",  ""), 512),
                    "interval_seconds": _int_clamp(h.get("interval_seconds", 30), 1, 3600, 30),
                    "timeout_seconds":  _int_clamp(h.get("timeout_seconds", 10), 1, 120, 10),
                }
            profiles.append(profile)
        return {"version": 2, "profiles": profiles}

    # ── UI transform — paths → loaded flags ───────────────────────────────────

    def _for_ui(self, config: dict) -> dict:
        """
        Return a copy of the config safe for the browser:
        - cert file paths are replaced by boolean *_loaded flags
        - PEM content never leaves the device
        """
        profiles = []
        for p in config.get("profiles", []):
            pc = dict(p)
            if "mqtt" in pc:
                m = dict(pc["mqtt"])
                for f in ("ca", "cert", "key"):
                    path = m.pop(f"tls_{f}_path", "")
                    m[f"tls_{f}_loaded"] = bool(path and Path(path).exists())
                pc["mqtt"] = m
            if "https" in pc:
                h = dict(pc["https"])
                for f in ("ca", "cert", "key"):
                    path = h.pop(f"tls_{f}_path", "")
                    h[f"tls_{f}_loaded"] = bool(path and Path(path).exists())
                pc["https"] = h
            profiles.append(pc)
        return {"version": config.get("version", 2), "profiles": profiles}

    # ── Public interface ──────────────────────────────────────────────────────

    def ensure_initialized(self) -> None:
        with self._lock:
            self._config = self._load_locked()

    def get_config(self) -> dict:
        """Internal config (with cert paths). Used by PES."""
        with self._lock:
            if not self._config:
                self._config = self._load_locked()
            return dict(self._config)

    def get_config_for_ui(self) -> dict:
        """Config with cert paths → loaded flags. Used by the web API."""
        return self._for_ui(self.get_config())

    def save_config(self, payload: Any) -> tuple[bool, dict]:
        """
        Accept a UI payload (PEM content | null=keep | ""=clear for cert fields),
        write cert files as needed, persist the path-based config to disk.
        """
        if not isinstance(payload, dict):
            return False, {"message": "Invalid payload."}

        with self._lock:
            if not self._config:
                self._config = self._load_locked()

            existing_by_id: dict[str, dict] = {
                p["id"]: p for p in (self._config.get("profiles") or [])
            }

            new_profiles = []
            try:
                for raw_p in (payload.get("profiles") or [])[:_MAX_PROFILES]:
                    existing = existing_by_id.get(_str(raw_p.get("id"), 32), {})
                    merged = self._process_profile(raw_p, existing)
                    if merged:
                        new_profiles.append(merged)
            except Exception as exc:
                logger.error("Forwarding config validation error: %s", exc)
                return False, {"message": "Invalid configuration payload."}

            # Clean up cert files for deleted profiles
            new_ids = {p["id"] for p in new_profiles}
            for pid in existing_by_id:
                if pid not in new_ids:
                    self._delete_all_certs(pid)

            merged_config = {"version": 2, "profiles": new_profiles}
            try:
                self._write_locked(merged_config)
                self._config = merged_config
                return True, {"message": "Forwarding configuration saved."}
            except Exception as exc:
                logger.error("Forwarding config write error: %s", exc)
                return False, {"message": "Failed to write configuration file."}

    # ── Internals ─────────────────────────────────────────────────────────────

    def _load_locked(self) -> dict:
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            return self._load_raw(raw)
        except FileNotFoundError:
            defaults = self._load_raw({})
            try:
                self._write_locked(defaults)
            except Exception:
                pass
            return defaults
        except Exception as exc:
            logger.warning("Forwarding config unreadable (%s), using defaults", exc)
            return self._load_raw({})

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
