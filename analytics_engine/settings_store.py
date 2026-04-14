from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import threading
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path


DEFAULT_USERNAME = "gateway"
DEFAULT_PASSWORD = "gateway"
PBKDF2_ITERATIONS = 120_000


def _hash_password(password: str, salt_hex: str) -> str:
    return hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        bytes.fromhex(salt_hex),
        PBKDF2_ITERATIONS,
    ).hex()


def _make_default_document() -> dict[str, object]:
    salt_hex = secrets.token_hex(16)
    return {
        "credentials": {
            "username": DEFAULT_USERNAME,
            "salt": salt_hex,
            "password_hash": _hash_password(DEFAULT_PASSWORD, salt_hex),
        },
        "wifi": {
            "interface": "wlan0",
            "enabled": False,
            "mode": "client",
            "auto_start": True,
            "country_code": "PK",
            "band": "auto",
            "channel": "auto",
            "channel_width": "20",
            "hidden_ssid": False,
            "ssid": "",
            "security": "wpa2-psk",
            "password": "",
            "client_dhcp": True,
            "client_address": "",
            "client_gateway": "",
            "client_dns": "",
            "route_metric": "200",
            "access_point_address": "192.168.50.1/24",
            "access_point_dhcp_server": True,
            "access_point_dhcp_range_start": "192.168.50.100",
            "access_point_dhcp_range_end": "192.168.50.180",
            "share_uplink": True,
            "uplink_interface": "eth0",
        },
    }


@dataclass(frozen=True, slots=True)
class StorageLayout:
    root_dir: Path
    aes_dir: Path
    pes_dir: Path
    shared_dir: Path
    system_dir: Path
    settings_dir: Path
    state_dir: Path
    cache_dir: Path
    exports_dir: Path
    uploads_dir: Path
    reports_dir: Path
    shared_certificates_dir: Path
    shared_backups_dir: Path
    shared_imports_dir: Path
    system_logs_dir: Path
    system_runtime_dir: Path
    settings_file: Path

    @classmethod
    def from_root(cls, root_dir: Path) -> "StorageLayout":
        aes_dir = root_dir / "analytics_engine_aes"
        pes_dir = root_dir / "processing_engine_pes"
        shared_dir = root_dir / "shared"
        system_dir = root_dir / "system"
        settings_dir = aes_dir / "settings"
        state_dir = aes_dir / "state"
        cache_dir = aes_dir / "cache"
        exports_dir = aes_dir / "exports"
        uploads_dir = aes_dir / "uploads"
        reports_dir = aes_dir / "reports"
        shared_certificates_dir = shared_dir / "certificates"
        shared_backups_dir = shared_dir / "backups"
        shared_imports_dir = shared_dir / "imports"
        system_logs_dir = system_dir / "logs"
        system_runtime_dir = system_dir / "runtime"
        return cls(
            root_dir=root_dir,
            aes_dir=aes_dir,
            pes_dir=pes_dir,
            shared_dir=shared_dir,
            system_dir=system_dir,
            settings_dir=settings_dir,
            state_dir=state_dir,
            cache_dir=cache_dir,
            exports_dir=exports_dir,
            uploads_dir=uploads_dir,
            reports_dir=reports_dir,
            shared_certificates_dir=shared_certificates_dir,
            shared_backups_dir=shared_backups_dir,
            shared_imports_dir=shared_imports_dir,
            system_logs_dir=system_logs_dir,
            system_runtime_dir=system_runtime_dir,
            settings_file=settings_dir / "system_settings.json",
        )

    def ensure_directories(self) -> None:
        directories = (
            self.root_dir,
            self.aes_dir,
            self.pes_dir,
            self.shared_dir,
            self.system_dir,
            self.settings_dir,
            self.state_dir,
            self.cache_dir,
            self.exports_dir,
            self.uploads_dir,
            self.reports_dir,
            self.shared_certificates_dir,
            self.shared_backups_dir,
            self.shared_imports_dir,
            self.system_logs_dir,
            self.system_runtime_dir,
        )
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)


class SettingsStore:
    def __init__(self, storage_root: Path) -> None:
        self.layout = StorageLayout.from_root(storage_root)
        self.layout.ensure_directories()
        self._path = self.layout.settings_file
        self._lock = threading.Lock()
        self._data = self._load_or_initialize()

    def _load_or_initialize(self) -> dict[str, object]:
        if not self._path.exists():
            document = _make_default_document()
            self._write(document)
            return document

        try:
            document = json.loads(self._path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            document = _make_default_document()
            self._write(document)
        return document

    def _write(self, document: dict[str, object]) -> None:
        temp_path = self._path.with_suffix(".tmp")
        temp_path.write_text(json.dumps(document, indent=2), encoding="utf-8")
        temp_path.replace(self._path)

    def verify_credentials(self, username: str, password: str) -> bool:
        with self._lock:
            credentials = self._data["credentials"]
            stored_username = str(credentials["username"])
            stored_hash = str(credentials["password_hash"])
            salt_hex = str(credentials["salt"])

        if username != stored_username:
            return False

        candidate_hash = _hash_password(password, salt_hex)
        return hmac.compare_digest(candidate_hash, stored_hash)

    def get_username(self) -> str:
        with self._lock:
            return str(self._data["credentials"]["username"])

    def update_credentials(
        self,
        current_password: str,
        new_username: str,
        new_password: str,
    ) -> tuple[bool, str]:
        new_username = new_username.strip()

        if not new_username:
            return False, "Username cannot be empty."
        if len(new_password) < 4:
            return False, "New password must be at least 4 characters."

        with self._lock:
            credentials = self._data["credentials"]
            current_hash = _hash_password(current_password, str(credentials["salt"]))
            if not hmac.compare_digest(current_hash, str(credentials["password_hash"])):
                return False, "Current password is incorrect."

            salt_hex = secrets.token_hex(16)
            credentials["username"] = new_username
            credentials["salt"] = salt_hex
            credentials["password_hash"] = _hash_password(new_password, salt_hex)
            self._write(self._data)

        return True, "Gateway credentials updated."

    def get_wifi_profile(self) -> dict[str, object]:
        with self._lock:
            return deepcopy(self._data["wifi"])

    def update_wifi_profile(self, profile: dict[str, object]) -> tuple[bool, str]:
        normalized = {
            "interface": str(profile.get("interface", "wlan0")).strip() or "wlan0",
            "enabled": bool(profile.get("enabled", False)),
            "mode": str(profile.get("mode", "client")).strip() or "client",
            "auto_start": bool(profile.get("auto_start", True)),
            "country_code": str(profile.get("country_code", "PK")).strip().upper() or "PK",
            "band": str(profile.get("band", "auto")).strip() or "auto",
            "channel": str(profile.get("channel", "auto")).strip() or "auto",
            "channel_width": str(profile.get("channel_width", "20")).strip() or "20",
            "hidden_ssid": bool(profile.get("hidden_ssid", False)),
            "ssid": str(profile.get("ssid", "")).strip(),
            "security": str(profile.get("security", "wpa2-psk")).strip() or "wpa2-psk",
            "password": str(profile.get("password", "")),
            "client_dhcp": bool(profile.get("client_dhcp", True)),
            "client_address": str(profile.get("client_address", "")).strip(),
            "client_gateway": str(profile.get("client_gateway", "")).strip(),
            "client_dns": str(profile.get("client_dns", "")).strip(),
            "route_metric": str(profile.get("route_metric", "200")).strip() or "200",
            "access_point_address": str(profile.get("access_point_address", "")).strip(),
            "access_point_dhcp_server": bool(profile.get("access_point_dhcp_server", True)),
            "access_point_dhcp_range_start": str(profile.get("access_point_dhcp_range_start", "")).strip(),
            "access_point_dhcp_range_end": str(profile.get("access_point_dhcp_range_end", "")).strip(),
            "share_uplink": bool(profile.get("share_uplink", True)),
            "uplink_interface": str(profile.get("uplink_interface", "eth0")).strip() or "eth0",
        }

        if normalized["interface"] != "wlan0":
            return False, "Only wlan0 is supported for Wi-Fi configuration right now."
        if normalized["mode"] not in {"client", "access-point"}:
            return False, "Invalid Wi-Fi mode."
        if normalized["band"] not in {"auto", "2.4ghz", "5ghz"}:
            return False, "Invalid Wi-Fi band."
        if normalized["channel_width"] not in {"20", "40", "80"}:
            return False, "Invalid channel width."
        if normalized["security"] not in {"open", "wpa2-psk", "wpa2-wpa3", "wpa3-sae"}:
            return False, "Invalid Wi-Fi security mode."
        if len(str(normalized["country_code"])) != 2:
            return False, "Country code must be a 2-letter value."
        if normalized["uplink_interface"] not in {"eth0", "ppp0", "auto"}:
            return False, "Invalid uplink interface."

        if normalized["enabled"] and not normalized["ssid"]:
            return False, "SSID is required when Wi-Fi is enabled."

        if normalized["security"] != "open" and normalized["enabled"] and len(normalized["password"]) < 8:
            return False, "Passphrase must be at least 8 characters for secured Wi-Fi."

        if normalized["mode"] == "client":
            if not normalized["client_dhcp"] and not normalized["client_address"]:
                return False, "Client static address is required when DHCP is disabled."
        else:
            if not normalized["access_point_address"]:
                return False, "Access point address is required."
            if normalized["access_point_dhcp_server"]:
                if not normalized["access_point_dhcp_range_start"] or not normalized["access_point_dhcp_range_end"]:
                    return False, "Access point DHCP range is required when DHCP server is enabled."

        with self._lock:
            self._data["wifi"] = normalized
            self._write(self._data)

        return True, "Wi-Fi profile saved."
