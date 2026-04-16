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
        "version": 1,
        "network": {
            "defaults_behavior": {
                "create_defaults_if_missing": True,
                "restore_defaults_if_invalid": True,
                "backup_invalid_file": True,
            },
            "ethernet": {
                "enabled": True,
                "interface": "eth0",
                "role": "uplink",
                "dhcp": True,
                "static_address": "",
                "static_gateway": "",
                "static_dns": [],
                "route_metric": 100,
                "mtu": 1500,
                "uplink_allowed": True,
                "downstream_allowed": False,
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
                "route_metric": 200,
                "uplink_allowed": True,
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
                "active_modem_id": "",
                "modems": [],
            },
            "policy": {
                "uplink_priority": ["eth0", "wifi_client", "cellular"],
                "failback_enabled": True,
                "stable_seconds_before_switch": 5,
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
        "ethernet": {
            "interface": "eth0",
            "enabled": True,
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
        "cellular": {
            "active_modem_id": "",
            "connected": False,
            "interface": "",
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
    system_related_root: Path
    network_root: Path
    state_dir: Path
    generated_network_dir: Path
    settings_file: Path
    last_good_file: Path
    state_file: Path
    apply_result_file: Path

    @classmethod
    def from_roots(cls, gateway_root: Path, storage_root: Path) -> "NetworkStorageLayout":
        aes_dir = storage_root / "AES"
        system_related_root = gateway_root / "system_related"
        network_root = system_related_root / "network"
        state_dir = network_root / "state"
        generated_network_dir = network_root / "generated"
        return cls(
            gateway_root=gateway_root,
            storage_root=storage_root,
            aes_dir=aes_dir,
            system_related_root=system_related_root,
            network_root=network_root,
            state_dir=state_dir,
            generated_network_dir=generated_network_dir,
            settings_file=aes_dir / "network_settings.json",
            last_good_file=aes_dir / "network_settings.last_good.json",
            state_file=state_dir / "network_state.json",
            apply_result_file=state_dir / "network_apply_result.json",
        )

    def ensure_directories(self) -> None:
        for directory in (
            self.gateway_root,
            self.storage_root,
            self.aes_dir,
            self.system_related_root,
            self.network_root,
            self.state_dir,
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
        if version != 1:
            errors.append(
                {
                    "scope": "network",
                    "code": "invalid_version",
                    "message": "Network settings version must be 1.",
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

        required_sections = ("defaults_behavior", "ethernet", "wifi_client", "wifi_ap", "cellular", "policy")
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

        ethernet = network["ethernet"]
        wifi_client = network["wifi_client"]
        wifi_ap = network["wifi_ap"]
        cellular = network["cellular"]
        policy = network["policy"]

        if not str(ethernet.get("interface", "")).strip():
            errors.append({"scope": "ethernet", "code": "missing_interface", "message": "Ethernet interface is required."})
        if not self._is_positive_int(ethernet.get("route_metric")):
            errors.append({"scope": "ethernet", "code": "invalid_metric", "message": "Ethernet route metric must be a positive integer."})
        if not self._is_positive_int(ethernet.get("mtu")):
            errors.append({"scope": "ethernet", "code": "invalid_mtu", "message": "Ethernet MTU must be a positive integer."})
        if not bool(ethernet.get("dhcp", True)):
            if not str(ethernet.get("static_address", "")).strip():
                errors.append({"scope": "ethernet", "code": "missing_static_address", "message": "Ethernet static address is required when DHCP is disabled."})
            if not str(ethernet.get("static_gateway", "")).strip():
                errors.append({"scope": "ethernet", "code": "missing_static_gateway", "message": "Ethernet static gateway is required when DHCP is disabled."})
        if not self._is_string_list(ethernet.get("static_dns")):
            errors.append({"scope": "ethernet", "code": "invalid_dns", "message": "Ethernet DNS must be an array of strings."})

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

        modems = cellular.get("modems")
        active_modem_id = str(cellular.get("active_modem_id", "")).strip()
        modem_ids: set[str] = set()
        if not isinstance(modems, list):
            errors.append({"scope": "cellular", "code": "invalid_modems", "message": "Cellular modems must be an array."})
        else:
            for modem in modems:
                if not isinstance(modem, dict):
                    errors.append({"scope": "cellular", "code": "invalid_modem_entry", "message": "Each modem entry must be an object."})
                    continue
                modem_id = str(modem.get("id", "")).strip()
                if not modem_id:
                    errors.append({"scope": "cellular", "code": "missing_modem_id", "message": "Each modem profile needs a unique id."})
                elif modem_id in modem_ids:
                    errors.append({"scope": "cellular", "code": "duplicate_modem_id", "message": f"Duplicate modem id '{modem_id}' found."})
                modem_ids.add(modem_id)
                if bool(modem.get("enabled", False)) and not str(modem.get("backend", "")).strip():
                    errors.append({"scope": "cellular", "code": "missing_backend", "message": f"Enabled modem '{modem_id or 'unnamed'}' requires a backend."})
                if str(modem.get("interface_type", "")).strip() == "ppp":
                    if not str(modem.get("control_device", "")).strip():
                        errors.append({"scope": "cellular", "code": "missing_control_device", "message": f"PPP modem '{modem_id or 'unnamed'}' requires a control device."})
                    if not str(modem.get("dial_number", "")).strip():
                        errors.append({"scope": "cellular", "code": "missing_dial_number", "message": f"PPP modem '{modem_id or 'unnamed'}' requires a dial number."})
            if active_modem_id and active_modem_id not in modem_ids:
                errors.append({"scope": "cellular", "code": "invalid_active_modem", "message": "Active modem id does not match any modem profile."})

        uplink_priority = policy.get("uplink_priority")
        if not isinstance(uplink_priority, list) or not all(isinstance(item, str) for item in uplink_priority):
            errors.append({"scope": "policy", "code": "invalid_priority_list", "message": "Uplink priority must be an array of strings."})
        else:
            unknown = [item for item in uplink_priority if item not in {"eth0", "wifi_client", "cellular"}]
            if unknown:
                errors.append({"scope": "policy", "code": "unknown_uplink", "message": f"Unknown uplinks in priority list: {', '.join(unknown)}."})
        if not self._is_non_negative_int(policy.get("stable_seconds_before_switch")):
            errors.append({"scope": "policy", "code": "invalid_stable_seconds", "message": "Stable seconds before switch must be zero or a positive integer."})
        if not self._is_positive_int(policy.get("fail_count_threshold")):
            errors.append({"scope": "policy", "code": "invalid_fail_threshold", "message": "Fail count threshold must be a positive integer."})
        if not self._is_positive_int(policy.get("recover_count_threshold")):
            errors.append({"scope": "policy", "code": "invalid_recover_threshold", "message": "Recover count threshold must be a positive integer."})
        if not self._is_string_list(policy.get("connectivity_targets")):
            errors.append({"scope": "policy", "code": "invalid_connectivity_targets", "message": "Connectivity targets must be an array of strings."})

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
