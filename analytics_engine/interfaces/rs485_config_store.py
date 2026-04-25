"""
RS485 / Modbus RTU interface configuration store.

Schema version 1. Config file: {storage_root}/AES/rs485_config.json

Ports map to PES serial channels:
  port_2 → Ch2 (RS485, ttyAMA4)
  port_3 → Ch3 (RS485, ttyAMA0)

PES reloads this file after AES sets Redis key "rs485_config" = "1".
"""
import json
import logging
import os
import tempfile
import threading
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ── Allowed values ────────────────────────────────────────────────────────────
_BAUD_RATES = frozenset([
    50, 75, 110, 134, 150, 200, 300, 600,
    1200, 1800, 2400, 4800, 9600,
    19200, 38400, 57600, 115200, 230400,
])
_PARITY = frozenset(["none", "even", "odd"])
_STOP_BITS = frozenset([1, 2])
_DATA_BITS = frozenset([5, 6, 7, 8])
_POLL_INTERVALS_MS = frozenset([500, 1000, 2000, 5000, 10000])
_REGISTER_TYPES = frozenset([
    "coil", "discrete_input", "input_register", "holding_register",
])
_DATA_TYPES = frozenset([
    "bool", "uint16", "int16", "uint32", "int32", "float32",
])
_WORD_ORDERS = frozenset(["big", "little"])

# ── Defaults ──────────────────────────────────────────────────────────────────
_DEFAULT_SERIAL: dict[str, Any] = {
    "baud_rate": 9600,
    "data_bits": 8,
    "parity": "none",
    "stop_bits": 1,
}

_DEFAULT_MODBUS_RTU: dict[str, Any] = {
    "slave_address": 1,
    "poll_interval_ms": 1000,
    "registers": [],
}

_DEFAULT_PORT: dict[str, Any] = {
    "enabled": False,
    "serial": dict(_DEFAULT_SERIAL),
    "modbus_rtu": dict(_DEFAULT_MODBUS_RTU),
}


# ── Merge / validate helpers ──────────────────────────────────────────────────

def _clamp_int(v: Any, lo: int, hi: int, default: int) -> int:
    try:
        return max(lo, min(hi, int(v)))
    except (TypeError, ValueError):
        return default


def _merge_serial(raw: Any) -> dict:
    s = raw if isinstance(raw, dict) else {}
    baud = int(s.get("baud_rate", 9600))
    if baud not in _BAUD_RATES:
        baud = 9600
    parity = str(s.get("parity", "none"))
    if parity not in _PARITY:
        parity = "none"
    stop_bits = int(s.get("stop_bits", 1))
    if stop_bits not in _STOP_BITS:
        stop_bits = 1
    data_bits = int(s.get("data_bits", 8))
    if data_bits not in _DATA_BITS:
        data_bits = 8
    return {"baud_rate": baud, "data_bits": data_bits, "parity": parity, "stop_bits": stop_bits}


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


def _merge_modbus_rtu(raw: Any) -> dict:
    m = raw if isinstance(raw, dict) else {}
    interval = int(m.get("poll_interval_ms", 1000))
    if interval not in _POLL_INTERVALS_MS:
        interval = 1000
    raw_regs = m.get("registers", [])
    registers = []
    if isinstance(raw_regs, list):
        for r in raw_regs:
            merged = _merge_register(r)
            if merged:
                registers.append(merged)
    return {
        "slave_address": _clamp_int(m.get("slave_address", 1), 1, 247, 1),
        "poll_interval_ms": interval,
        "registers": registers,
    }


def _merge_port(raw: Any) -> dict:
    p = raw if isinstance(raw, dict) else {}
    return {
        "enabled": bool(p.get("enabled", False)),
        "serial": _merge_serial(p.get("serial")),
        "modbus_rtu": _merge_modbus_rtu(p.get("modbus_rtu")),
    }


def _validate_and_merge(raw: Any) -> dict:
    r = raw if isinstance(raw, dict) else {}
    rs485 = r.get("rs485", {}) if isinstance(r.get("rs485"), dict) else {}
    return {
        "version": 1,
        "rs485": {
            "port_2": _merge_port(rs485.get("port_2")),
            "port_3": _merge_port(rs485.get("port_3")),
        },
    }


# ── Store ─────────────────────────────────────────────────────────────────────

class Rs485ConfigStore:
    def __init__(self, storage_root: Path) -> None:
        self._path = storage_root / "AES" / "rs485_config.json"
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
            logger.error("RS485 config validation error: %s", exc)
            return False, {"message": "Invalid configuration payload."}
        with self._lock:
            try:
                self._write_locked(merged)
                self._config = merged
                return True, {"message": "RS485 configuration saved."}
            except Exception as exc:
                logger.error("RS485 config write error: %s", exc)
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
            logger.warning("RS485 config unreadable (%s), using defaults", exc)
            return _validate_and_merge({})

    def _write_locked(self, config: dict) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self._path.parent, prefix=".rs485_", suffix=".tmp")
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
