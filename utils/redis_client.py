"""
Lightweight Redis client for AES — no external dependencies.
Uses raw RESP2 protocol directly over TCP.
Any module in AES can import and use this.
"""
import logging
import socket
from typing import Optional

logger = logging.getLogger(__name__)

_COMMON_BAUD_RATES = (
    50, 75, 110, 134, 150, 200, 300, 600,
    1200, 1800, 2400, 4800, 9600,
    19200, 38400, 57600, 115200, 230400,
)


class RedisClient:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        timeout: float = 1.0,
    ) -> None:
        self._host = host
        self._port = port
        self._timeout = timeout

    # ── RESP2 helpers ────────────────────────────────────────────────────
    def _build_command(self, *args: str) -> bytes:
        parts: list[bytes] = [f"*{len(args)}\r\n".encode()]
        for arg in args:
            b = arg.encode("utf-8")
            parts.append(f"${len(b)}\r\n".encode())
            parts.append(b)
            parts.append(b"\r\n")
        return b"".join(parts)

    def _send(self, *args: str) -> Optional[bytes]:
        try:
            with socket.create_connection((self._host, self._port), timeout=self._timeout) as sock:
                sock.sendall(self._build_command(*args))
                return sock.recv(512)
        except Exception as exc:
            logger.debug("Redis %s failed: %s", args[0] if args else "cmd", exc)
            return None

    # ── Public API ───────────────────────────────────────────────────────
    def ping(self) -> bool:
        reply = self._send("PING")
        return reply is not None and reply.startswith(b"+PONG")

    def set(self, key: str, value: str) -> bool:
        reply = self._send("SET", key, value)
        return reply is not None and reply.startswith(b"+OK")

    def get(self, key: str) -> Optional[str]:
        reply = self._send("GET", key)
        if reply is None or reply.startswith(b"$-1"):
            return None
        try:
            lines = reply.split(b"\r\n")
            if lines[0].startswith(b"$") and len(lines) > 1:
                return lines[1].decode("utf-8")
        except Exception:
            pass
        return None

    def delete(self, key: str) -> bool:
        reply = self._send("DEL", key)
        return reply is not None and not reply.startswith(b"-")

    def notify_changed(self, key: str) -> None:
        """Signal to PES that config key has been updated. PES polls and reacts."""
        self.set(key, "1")
