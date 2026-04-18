from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import threading
from dataclasses import dataclass
from pathlib import Path


DEFAULT_USERNAME = "gateway"
DEFAULT_PASSWORD = "gateway"
PBKDF2_ITERATIONS = 120_000
# 192.168.18.126

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
        }
    }


@dataclass(frozen=True, slots=True)
class StorageLayout:
    root_dir: Path
    aes_dir: Path
    settings_file: Path

    @classmethod
    def from_root(cls, root_dir: Path) -> "StorageLayout":
        aes_dir = root_dir / "AES"
        return cls(
            root_dir=root_dir,
            aes_dir=aes_dir,
            settings_file=aes_dir / "system_settings.json",
        )

    def ensure_directories(self) -> None:
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.aes_dir.mkdir(parents=True, exist_ok=True)


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

        credentials = document.get("credentials")
        if not isinstance(credentials, dict):
            document = _make_default_document()
            self._write(document)
            return document

        if not all(key in credentials for key in ("username", "salt", "password_hash")):
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
