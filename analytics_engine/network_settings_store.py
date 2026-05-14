from __future__ import annotations

import ipaddress
import json
import threading
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


def _utc_timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _timestamp_token() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _default_network_document() -> dict[str, object]:
    return {
        "version": 2,
        "network": {
            "defaults_behavior": {
                "create_defaults_if_missing": True,
                "restore_defaults_if_invalid": True,
                "backup_invalid_file": True,
            },
            "wifi_client": {
                "enabled": False,
                "interface": "wlan0",
                "auto_connect": True,
                "ssid": "",
                "hidden_ssid": False,
                "security": "wpa2-psk",
                "passphrase": "",
                "country_code": "PK",
                "band": "auto",
                "dhcp": True,
                "static_address": "",
                "static_gateway": "",
                "static_dns": [],
                "route_metric": 300,
            },
            "wifi_ap": {
                "enabled": False,
                "interface": "wlan0",
                "ssid": "Gateway-Setup",
                "security": "wpa2-psk",
                "passphrase": "",
                "country_code": "PK",
                "band": "2.4ghz",
                "channel": "auto",
                "channel_width": "20",
                "subnet_cidr": "192.168.50.1/24",
                "dhcp_server_enabled": True,
                "dhcp_range_start": "192.168.50.100",
                "dhcp_range_end": "192.168.50.180",
                "nat_enabled": True,
                "client_isolation": False,
                "shared_uplink_mode": "auto",
            },
            "cellular": {
                "enabled":         False,
                "active_modem_id": "sim7600",
                "apn":             "",
                "username":        "",
                "password":        "",
                "pin":             "",
                "roaming_allowed": False,
                "modems": [
                    {
                        "id":              "sim7600",
                        "enabled":         True,
                        "backend":         "qmi",
                        "interface_type":  "qmi",
                        "control_device":  "/dev/cdc-wdm0",
                        "data_interface":  "wwan0",
                        "route_metric":    500,
                        "ip_type":         "4",
                    }
                ],
            },
            "uplink": {
                "uplink_priority": ["eth0", "eth1", "wifi_client", "cellular"],
                "failback_enabled": True,
                "stable_seconds_before_switch": 0,
                "require_connectivity_check": True,
                "fail_count_threshold": 1,
                "recover_count_threshold": 1,
                "connectivity_targets": ["1.1.1.1", "8.8.8.8"],
            },
        },
    }


def _default_network_state() -> dict[str, object]:
    return {
        "active_uplink": "none",
        "monitor_status": "idle",
        "recovery": {
            "count": 0,
            "last_reason": "",
            "last_timestamp": "",
        },
        "eth0": {
            "link_up": False,
            "interface_up": False,
            "address": "",
            "internet_ok": False,
        },
        "eth1": {
            "link_up": False,
            "interface_up": False,
            "address": "",
            "internet_ok": False,
        },
        "wifi_client": {
            "interface": "wlan0",
            "enabled": False,
            "present": False,
            "link_up": False,
            "interface_up": False,
            "address": "",
            "connected_ssid": "",
            "internet_ok": False,
        },
        "wifi_ap": {
            "interface": "wlan0",
            "enabled": False,
            "address": "",
            "clients": 0,
        },
        "last_apply_status": "not_applied",
        "last_apply_timestamp": _utc_timestamp(),
    }


def _default_apply_result(*, status: str, used_defaults: bool, errors: list[dict[str, object]] | None = None) -> dict[str, object]:
    return {
        "ok": status in {"ok", "fallback_to_defaults", "not_applied"},
        "status": status,
        "timestamp": _utc_timestamp(),
        "config_generation": 1,
        "used_defaults": used_defaults,
        "active_uplink": "none",
        "errors": errors or [],
        "warnings": [],
    }


@dataclass(frozen=True, slots=True)
class NetworkStorageLayout:
    gateway_root: Path
    storage_root: Path
    aes_dir: Path
    network_root: Path
    generated_network_dir: Path
    settings_file: Path
    last_good_file: Path
    state_file: Path
    apply_result_file: Path

    @classmethod
    def from_roots(cls, gateway_root: Path, storage_root: Path) -> "NetworkStorageLayout":
        aes_dir = storage_root / "AES"
        network_root = gateway_root / "network"
        generated_network_dir = network_root / "generated"
        return cls(
            gateway_root=gateway_root,
            storage_root=storage_root,
            aes_dir=aes_dir,
            network_root=network_root,
            generated_network_dir=generated_network_dir,
            settings_file=aes_dir / "network_settings.json",
            last_good_file=aes_dir / "network_settings.last_good.json",
            state_file=network_root / "state.json",
            apply_result_file=network_root / "apply-result.json",
        )

    def ensure_directories(self) -> None:
        for directory in (
            self.gateway_root,
            self.storage_root,
            self.aes_dir,
            self.network_root,
            self.generated_network_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)


class NetworkSettingsStore:
    def __init__(self, gateway_root: Path, storage_root: Path) -> None:
        self.layout = NetworkStorageLayout.from_roots(gateway_root=gateway_root, storage_root=storage_root)
        self.layout.ensure_directories()
        self._lock = threading.Lock()
        self._data = self._load_or_initialize()

    def _write_json(self, path: Path, document: dict[str, object]) -> None:
        temp_path = path.with_suffix(path.suffix + ".tmp")
        temp_path.write_text(json.dumps(document, indent=2), encoding="utf-8")
        temp_path.replace(path)

    def _read_json(self, path: Path) -> dict[str, object]:
        return json.loads(path.read_text(encoding="utf-8"))

    def _backup_invalid_settings(self, raw_text: str) -> None:
        backup_path = self.layout.aes_dir / f"network_settings.invalid.{_timestamp_token()}.json"
        backup_path.write_text(raw_text, encoding="utf-8")

    def _initialize_defaults(self, *, status: str, errors: list[dict[str, object]] | None = None) -> dict[str, object]:
        document = _default_network_document()
        self._write_json(self.layout.settings_file, document)
        if not self.layout.last_good_file.exists():
            self._write_json(self.layout.last_good_file, document)
        if not self.layout.state_file.exists():
            self._write_json(self.layout.state_file, _default_network_state())
        self._write_json(
            self.layout.apply_result_file,
            _default_apply_result(status=status, used_defaults=True, errors=errors),
        )
        return document

    def _validate_document(self, document: object) -> list[dict[str, object]]:
        errors: list[dict[str, object]] = []
        if not isinstance(document, dict):
            return [{"scope": "network", "code": "invalid_root", "message": "Network settings root must be a JSON object."}]

        version = document.get("version")
        if version != 2:
            errors.append(
                {
                    "scope": "network",
                    "code": "invalid_version",
                    "message": "Network settings version must be 2.",
                }
            )

        network = document.get("network")
        if not isinstance(network, dict):
            errors.append(
                {
                    "scope": "network",
                    "code": "missing_network",
                    "message": "Network settings document is missing the network section.",
                }
            )
            return errors

        required_sections = ("defaults_behavior", "wifi_client", "wifi_ap", "cellular", "uplink")
        for section in required_sections:
            if not isinstance(network.get(section), dict):
                errors.append(
                    {
                        "scope": section,
                        "code": "missing_section",
                        "message": f"Network settings are missing the {section} section.",
                    }
                )

        if errors:
            return errors

        wifi_client = network["wifi_client"]
        wifi_ap = network["wifi_ap"]
        cellular = network["cellular"]
        uplink = network["uplink"]

        if not str(wifi_client.get("interface", "")).strip():
            errors.append({"scope": "wifi_client", "code": "missing_interface", "message": "Wi-Fi client interface is required."})
        if len(str(wifi_client.get("country_code", "")).strip()) != 2:
            errors.append({"scope": "wifi_client", "code": "invalid_country", "message": "Wi-Fi client country code must be 2 letters."})
        if not self._is_positive_int(wifi_client.get("route_metric")):
            errors.append({"scope": "wifi_client", "code": "invalid_metric", "message": "Wi-Fi client route metric must be a positive integer."})
        if not self._is_string_list(wifi_client.get("static_dns")):
            errors.append({"scope": "wifi_client", "code": "invalid_dns", "message": "Wi-Fi client DNS must be an array of strings."})
        if bool(wifi_client.get("enabled", False)):
            if not str(wifi_client.get("ssid", "")).strip():
                errors.append({"scope": "wifi_client", "code": "missing_ssid", "message": "Wi-Fi client SSID is required when enabled."})
            if str(wifi_client.get("security", "open")) != "open" and len(str(wifi_client.get("passphrase", ""))) < 8:
                errors.append({"scope": "wifi_client", "code": "missing_passphrase", "message": "Wi-Fi client passphrase must be at least 8 characters for secured networks."})
        if not bool(wifi_client.get("dhcp", True)):
            if not str(wifi_client.get("static_address", "")).strip():
                errors.append({"scope": "wifi_client", "code": "missing_static_address", "message": "Wi-Fi client static address is required when DHCP is disabled."})
            if not str(wifi_client.get("static_gateway", "")).strip():
                errors.append({"scope": "wifi_client", "code": "missing_static_gateway", "message": "Wi-Fi client static gateway is required when DHCP is disabled."})

        if not str(wifi_ap.get("interface", "")).strip():
            errors.append({"scope": "wifi_ap", "code": "missing_interface", "message": "Wi-Fi AP interface is required."})
        if len(str(wifi_ap.get("country_code", "")).strip()) != 2:
            errors.append({"scope": "wifi_ap", "code": "invalid_country", "message": "Wi-Fi AP country code must be 2 letters."})
        if bool(wifi_ap.get("enabled", False)):
            if not str(wifi_ap.get("ssid", "")).strip():
                errors.append({"scope": "wifi_ap", "code": "missing_ssid", "message": "Wi-Fi AP SSID is required when enabled."})
            if str(wifi_ap.get("security", "open")) != "open" and len(str(wifi_ap.get("passphrase", ""))) < 8:
                errors.append({"scope": "wifi_ap", "code": "missing_passphrase", "message": "Wi-Fi AP passphrase must be at least 8 characters for secured networks."})
            subnet_cidr = str(wifi_ap.get("subnet_cidr", "")).strip()
            if not subnet_cidr:
                errors.append({"scope": "wifi_ap", "code": "missing_subnet", "message": "Wi-Fi AP subnet CIDR is required when enabled."})
            else:
                try:
                    ipaddress.ip_interface(subnet_cidr)
                except ValueError:
                    errors.append({"scope": "wifi_ap", "code": "invalid_subnet", "message": "Wi-Fi AP subnet CIDR is invalid."})
            if bool(wifi_ap.get("dhcp_server_enabled", False)):
                if not str(wifi_ap.get("dhcp_range_start", "")).strip():
                    errors.append({"scope": "wifi_ap", "code": "missing_dhcp_start", "message": "Wi-Fi AP DHCP start address is required when DHCP server is enabled."})
                if not str(wifi_ap.get("dhcp_range_end", "")).strip():
                    errors.append({"scope": "wifi_ap", "code": "missing_dhcp_end", "message": "Wi-Fi AP DHCP end address is required when DHCP server is enabled."})
            shared_uplink_mode = str(wifi_ap.get("shared_uplink_mode", "auto")).strip()
            if shared_uplink_mode not in {"auto", "ethernet", "eth0"}:
                errors.append({"scope": "wifi_ap", "code": "unsupported_shared_uplink", "message": "Current gateway image supports Wi-Fi AP sharing only through Ethernet or Auto."})

        if bool(wifi_client.get("enabled", False)) and bool(wifi_ap.get("enabled", False)):
            errors.append({"scope": "wifi", "code": "client_ap_conflict", "message": "Current gateway image supports either Wi-Fi client or Wi-Fi AP on wlan0, not both at the same time."})

        # Cellular — validate user-visible fields only; platform defaults are injected by the UI
        if not isinstance(cellular.get("enabled"), bool):
            errors.append({"scope": "cellular", "code": "invalid_enabled", "message": "Cellular enabled must be a boolean."})
        if not isinstance(cellular.get("roaming_allowed"), bool):
            errors.append({"scope": "cellular", "code": "invalid_roaming", "message": "Roaming allowed must be a boolean."})
        for field in ("apn", "username", "password", "pin"):
            if not isinstance(cellular.get(field, ""), str):
                errors.append({"scope": "cellular", "code": f"invalid_{field}", "message": f"Cellular {field} must be a string."})
        if bool(cellular.get("enabled")):
            if not str(cellular.get("apn", "")).strip():
                errors.append({"scope": "cellular", "code": "missing_apn", "message": "APN is required when cellular is enabled."})
        pin = str(cellular.get("pin", "")).strip()
        if pin and (not pin.isdigit() or not (4 <= len(pin) <= 8)):
            errors.append({"scope": "cellular", "code": "invalid_pin", "message": "SIM PIN must be 4 to 8 digits."})
        # Ensure SIM7600 platform default is present when cellular is enabled
        modems = cellular.get("modems")
        if bool(cellular.get("enabled")) and (not isinstance(modems, list) or len(modems) == 0):
            errors.append({"scope": "cellular", "code": "missing_modem", "message": "No modem profile found. Save again to reinject defaults."})
        # Preserve uplink_priority cellular entry
        uplink_priority = uplink.get("uplink_priority", [])
        if isinstance(uplink_priority, list) and "cellular" not in uplink_priority:
            pass  # UI may omit cellular — that's allowed

        uplink_priority = uplink.get("uplink_priority")
        if not isinstance(uplink_priority, list) or not all(isinstance(item, str) for item in uplink_priority):
            errors.append({"scope": "uplink", "code": "invalid_priority_list", "message": "Uplink priority must be an array of strings."})
        else:
            unknown = [item for item in uplink_priority if item not in {"eth0", "eth1", "wifi_client", "cellular"}]
            if unknown:
                errors.append({"scope": "uplink", "code": "unknown_uplink", "message": f"Unknown uplinks in priority list: {', '.join(unknown)}."})
        if not self._is_non_negative_int(uplink.get("stable_seconds_before_switch")):
            errors.append({"scope": "uplink", "code": "invalid_stable_seconds", "message": "Stable seconds before switch must be zero or a positive integer."})
        if not self._is_positive_int(uplink.get("fail_count_threshold")):
            errors.append({"scope": "uplink", "code": "invalid_fail_threshold", "message": "Fail count threshold must be a positive integer."})
        if not self._is_positive_int(uplink.get("recover_count_threshold")):
            errors.append({"scope": "uplink", "code": "invalid_recover_threshold", "message": "Recover count threshold must be a positive integer."})
        if not self._is_string_list(uplink.get("connectivity_targets")):
            errors.append({"scope": "uplink", "code": "invalid_connectivity_targets", "message": "Connectivity targets must be an array of strings."})

        return errors

    def _load_or_initialize(self) -> dict[str, object]:
        if not self.layout.settings_file.exists():
            return self._initialize_defaults(status="not_applied")

        raw_text = ""
        try:
            raw_text = self.layout.settings_file.read_text(encoding="utf-8")
            document = json.loads(raw_text)
        except (OSError, json.JSONDecodeError) as exc:
            if raw_text:
                self._backup_invalid_settings(raw_text)
            return self._initialize_defaults(
                status="fallback_to_defaults",
                errors=[
                    {
                        "scope": "network",
                        "code": "invalid_json",
                        "message": "Saved network settings were invalid and defaults were restored.",
                        "detail": str(exc),
                    }
                ],
            )

        validation_errors = self._validate_document(document)
        if validation_errors:
            self._backup_invalid_settings(raw_text)
            return self._initialize_defaults(status="fallback_to_defaults", errors=validation_errors)

        if not self.layout.last_good_file.exists():
            self._write_json(self.layout.last_good_file, document)
        if not self.layout.state_file.exists():
            self._write_json(self.layout.state_file, _default_network_state())
        if not self.layout.apply_result_file.exists():
            self._write_json(
                self.layout.apply_result_file,
                _default_apply_result(status="ok", used_defaults=False),
            )
        return document

    def ensure_initialized(self) -> None:
        with self._lock:
            self.layout.ensure_directories()
            self._data = self._load_or_initialize()

    def get_settings(self) -> dict[str, object]:
        with self._lock:
            return deepcopy(self._data)

    @staticmethod
    def _is_string_list(value: object) -> bool:
        return isinstance(value, list) and all(isinstance(item, str) for item in value)

    @staticmethod
    def _is_positive_int(value: object) -> bool:
        return isinstance(value, int) and value > 0

    @staticmethod
    def _is_non_negative_int(value: object) -> bool:
        return isinstance(value, int) and value >= 0

    def get_state(self) -> dict[str, object]:
        with self._lock:
            return self._read_json(self.layout.state_file)

    def get_apply_result(self) -> dict[str, object]:
        with self._lock:
            return self._read_json(self.layout.apply_result_file)

    def save_settings(self, document: dict[str, object]) -> tuple[bool, dict[str, object]]:
        validation_errors = self._validate_document(document)
        if validation_errors:
            return False, {
                "saved": False,
                "apply_requested": False,
                "apply_status": "validation_error",
                "errors": validation_errors,
                "result_path": str(self.layout.apply_result_file),
            }

        with self._lock:
            self._write_json(self.layout.settings_file, document)
            self._data = deepcopy(document)

        return True, {
            "saved": True,
            "apply_requested": False,
            "apply_status": "saved",
            "errors": [],
            "result_path": str(self.layout.apply_result_file),
        }
