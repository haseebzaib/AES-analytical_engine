"""
RS232 interface configuration store.

Schema version 1. Config file: {storage_root}/AES/rs232_config.json

Ports map to PES serial channels:
  port_0 → Ch0 (RS232)
  port_1 → Ch1 (RS232)

PES reads this file after AES sets Redis key "rs232_config" = "1".
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
_ALARM_STATES = frozenset([
    "off", "audible", "visible", "audible_visible",
    "relay", "audible_relay", "visible_relay", "audible_visible_relay",
])
_ANALOG_STATES = frozenset(["off", "voltage", "current"])
_ANALOG_CHANNELS = frozenset(["pm1", "pm25", "pm4", "pm10", "total"])
_SENSORS = frozenset(["dustrak"])

# ── Defaults ──────────────────────────────────────────────────────────────────
_DEFAULT_SERIAL: dict[str, Any] = {
    "baud_rate": 9600,
    "data_bits": 8,
    "parity": "none",
    "stop_bits": 1,
}

_DEFAULT_POLLING: dict[str, bool] = {
    "read_identity_on_init": True,
    "poll_status": True,
    "auto_start_measurement": False,
    "poll_measurements": True,
    "poll_measurement_stats": False,
    "poll_fault_messages": False,
    "poll_alarm_messages": False,
    "poll_log_info": False,
}

_DEFAULT_DRIVER: dict[str, Any] = {
    "update_ram_after_write": True,
}

_DEFAULT_ALARM_CH: dict[str, Any] = {
    "alarm1_state": "off",
    "alarm1_mg_per_m3": 0.0,
    "stel_alarm1_enabled": False,
    "alarm2_state": "off",
    "alarm2_mg_per_m3": 0.0,
}

_ALARM_CHANNELS = ("pm1", "pm25", "pm4", "pm10", "total")

_DEFAULT_ALARMS: dict[str, Any] = {ch: dict(_DEFAULT_ALARM_CH) for ch in _ALARM_CHANNELS}

_DEFAULT_ANALOG_OUTPUT: dict[str, Any] = {
    "state": "off",
    "channel": None,
    "min_mg_per_m3": 0.0,
    "max_mg_per_m3": 1.0,
}

_DEFAULT_DUSTRAK: dict[str, Any] = {
    "polling": dict(_DEFAULT_POLLING),
    "driver": dict(_DEFAULT_DRIVER),
    "alarms": {ch: dict(_DEFAULT_ALARM_CH) for ch in _ALARM_CHANNELS},
    "analog_output": dict(_DEFAULT_ANALOG_OUTPUT),
}

_DEFAULT_PORT: dict[str, Any] = {
    "enabled": False,
    "serial": dict(_DEFAULT_SERIAL),
    "sensor": "dustrak",
    "dustrak": {
        "polling": dict(_DEFAULT_POLLING),
        "driver": dict(_DEFAULT_DRIVER),
        "alarms": {ch: dict(_DEFAULT_ALARM_CH) for ch in _ALARM_CHANNELS},
        "analog_output": dict(_DEFAULT_ANALOG_OUTPUT),
    },
}


# ── Merge / validate helpers ──────────────────────────────────────────────────

def _clamp_float(v: Any, lo: float = 0.0, hi: float = 1e6) -> float:
    try:
        f = float(v)
        return max(lo, min(hi, f))
    except (TypeError, ValueError):
        return lo


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


def _merge_polling(raw: Any) -> dict:
    p = raw if isinstance(raw, dict) else {}
    return {k: bool(p.get(k, _DEFAULT_POLLING[k])) for k in _DEFAULT_POLLING}


def _merge_driver(raw: Any) -> dict:
    d = raw if isinstance(raw, dict) else {}
    return {"update_ram_after_write": bool(d.get("update_ram_after_write", True))}


def _merge_alarm_ch(raw: Any) -> dict:
    a = raw if isinstance(raw, dict) else {}
    a1s = str(a.get("alarm1_state", "off"))
    a2s = str(a.get("alarm2_state", "off"))
    return {
        "alarm1_state": a1s if a1s in _ALARM_STATES else "off",
        "alarm1_mg_per_m3": _clamp_float(a.get("alarm1_mg_per_m3", 0.0)),
        "stel_alarm1_enabled": bool(a.get("stel_alarm1_enabled", False)),
        "alarm2_state": a2s if a2s in _ALARM_STATES else "off",
        "alarm2_mg_per_m3": _clamp_float(a.get("alarm2_mg_per_m3", 0.0)),
    }


def _merge_alarms(raw: Any) -> dict:
    a = raw if isinstance(raw, dict) else {}
    return {ch: _merge_alarm_ch(a.get(ch)) for ch in _ALARM_CHANNELS}


def _merge_analog_output(raw: Any) -> dict:
    ao = raw if isinstance(raw, dict) else {}
    state = str(ao.get("state", "off"))
    if state not in _ANALOG_STATES:
        state = "off"
    ch = ao.get("channel")
    if ch is not None:
        ch = str(ch)
        if ch not in _ANALOG_CHANNELS:
            ch = None
    if state == "off":
        ch = None
    return {
        "state": state,
        "channel": ch,
        "min_mg_per_m3": _clamp_float(ao.get("min_mg_per_m3", 0.0)),
        "max_mg_per_m3": _clamp_float(ao.get("max_mg_per_m3", 1.0)),
    }


def _merge_dustrak(raw: Any) -> dict:
    d = raw if isinstance(raw, dict) else {}
    return {
        "polling": _merge_polling(d.get("polling")),
        "driver": _merge_driver(d.get("driver")),
        "alarms": _merge_alarms(d.get("alarms")),
        "analog_output": _merge_analog_output(d.get("analog_output")),
    }


def _merge_port(raw: Any) -> dict:
    p = raw if isinstance(raw, dict) else {}
    sensor = str(p.get("sensor", "dustrak"))
    if sensor not in _SENSORS:
        sensor = "dustrak"
    return {
        "enabled": bool(p.get("enabled", False)),
        "serial": _merge_serial(p.get("serial")),
        "sensor": sensor,
        "dustrak": _merge_dustrak(p.get("dustrak")),
    }


def _validate_and_merge(raw: Any) -> dict:
    r = raw if isinstance(raw, dict) else {}
    rs232 = r.get("rs232", {}) if isinstance(r.get("rs232"), dict) else {}
    return {
        "version": 1,
        "rs232": {
            "port_0": _merge_port(rs232.get("port_0")),
            "port_1": _merge_port(rs232.get("port_1")),
        },
    }


# ── Store ─────────────────────────────────────────────────────────────────────

class Rs232ConfigStore:
    def __init__(self, storage_root: Path) -> None:
        self._path = storage_root / "AES" / "rs232_config.json"
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
            logger.error("RS232 config validation error: %s", exc)
            return False, {"message": "Invalid configuration payload."}
        with self._lock:
            try:
                self._write_locked(merged)
                self._config = merged
                return True, {"message": "RS232 configuration saved."}
            except Exception as exc:
                logger.error("RS232 config write error: %s", exc)
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
            logger.warning("RS232 config unreadable (%s), using defaults", exc)
            return _validate_and_merge({})

    def _write_locked(self, config: dict) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self._path.parent, prefix=".rs232_", suffix=".tmp")
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
