"""
Modbus TCP configuration store.

Schema version 1. Config file: {storage_root}/AES/modbus_tcp_config.json

Supports up to MAX_CONNECTIONS devices per config. Each connection targets
a Modbus TCP device over Ethernet (eth0 or eth1) and carries its own
register map.

PES reloads this file after AES sets Redis key "modbus_tcp_config" = "1".
"""
import json
import logging
import os
import tempfile
import threading
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

MAX_CONNECTIONS = 10

# ── Allowed values ────────────────────────────────────────────────────────────
_INTERFACES = frozenset(["eth0", "eth1"])
_POLL_INTERVALS_MS = frozenset([500, 1000, 2000, 5000, 10000])
_REGISTER_TYPES = frozenset([
    "coil", "discrete_input", "input_register", "holding_register",
])
_DATA_TYPES = frozenset([
    "bool", "uint16", "int16", "uint32", "int32", "float32",
])
_WORD_ORDERS = frozenset(["big", "little"])


# ── Merge / validate helpers ──────────────────────────────────────────────────

def _clamp_int(v: Any, lo: int, hi: int, default: int) -> int:
    try:
        return max(lo, min(hi, int(v)))
    except (TypeError, ValueError):
        return default


def _merge_register(raw: Any) -> dict | None:
    if not isinstance(raw, dict):
        return None
    reg_type = str(raw.get("register_type", ""))
    if reg_type not in _REGISTER_TYPES:
        return None
    data_type = str(raw.get("data_type", "uint16"))
    if data_type not in _DATA_TYPES:
        data_type = "uint16"
    word_order = str(raw.get("word_order", "big"))
    if word_order not in _WORD_ORDERS:
        word_order = "big"
    try:
        scale = float(raw.get("scale", 1.0))
    except (TypeError, ValueError):
        scale = 1.0
    return {
        "name": str(raw.get("name", ""))[:64],
        "register_type": reg_type,
        "address": _clamp_int(raw.get("address", 0), 0, 65535, 0),
        "data_type": data_type,
        "word_order": word_order,
        "scale": scale,
        "unit": str(raw.get("unit", ""))[:16],
    }


def _merge_connection(raw: Any) -> dict | None:
    if not isinstance(raw, dict):
        return None
    iface = str(raw.get("interface", "eth0"))
    if iface not in _INTERFACES:
        iface = "eth0"
    interval = int(raw.get("poll_interval_ms", 1000))
    if interval not in _POLL_INTERVALS_MS:
        interval = 1000
    raw_regs = raw.get("registers", [])
    registers = []
    if isinstance(raw_regs, list):
        for r in raw_regs:
            merged = _merge_register(r)
            if merged:
                registers.append(merged)
    conn_id = str(raw.get("id", "")).strip()
    if not conn_id:
        return None
    return {
        "id": conn_id[:32],
        "name": str(raw.get("name", "Unnamed Device"))[:64],
        "enabled": bool(raw.get("enabled", False)),
        "interface": iface,
        "ip": str(raw.get("ip", ""))[:64],
        "port": _clamp_int(raw.get("port", 502), 1, 65535, 502),
        "unit_id": _clamp_int(raw.get("unit_id", 1), 1, 247, 1),
        "poll_interval_ms": interval,
        "registers": registers,
    }


def _validate_and_merge(raw: Any) -> dict:
    r = raw if isinstance(raw, dict) else {}
    raw_conns = r.get("connections", [])
    connections = []
    if isinstance(raw_conns, list):
        for c in raw_conns[:MAX_CONNECTIONS]:
            merged = _merge_connection(c)
            if merged:
                connections.append(merged)
    return {
        "version": 1,
        "max_connections": MAX_CONNECTIONS,
        "connections": connections,
    }


# ── Store ─────────────────────────────────────────────────────────────────────

class ModbusTcpConfigStore:
    def __init__(self, storage_root: Path) -> None:
        self._path = storage_root / "AES" / "modbus_tcp_config.json"
        self._lock = threading.Lock()
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
            logger.error("Modbus TCP config validation error: %s", exc)
            return False, {"message": "Invalid configuration payload."}
        with self._lock:
            try:
                self._write_locked(merged)
                self._config = merged
                return True, {"message": "Modbus TCP configuration saved."}
            except Exception as exc:
                logger.error("Modbus TCP config write error: %s", exc)
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
            logger.warning("Modbus TCP config unreadable (%s), using defaults", exc)
            return _validate_and_merge({})

    def _write_locked(self, config: dict) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self._path.parent, prefix=".modbus_tcp_", suffix=".tmp")
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
