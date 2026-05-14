"""
HTTPS profile client — persistent TLS tunnel via openssl s_client.

Architecture:
    One `openssl s_client -connect host:port` subprocess is spawned per profile
    and kept alive across requests.  HTTP/1.1 requests are written to the
    process stdin; the HTTP response is read back from stdout using a minimal
    hand-written HTTP/1.1 parser (select-based, non-blocking with timeout).

    On broken pipe or server close, the tunnel is marked dead.  The next
    call to post() detects this and restarts the subprocess automatically.

    For plain HTTP (tls=False) a persistent http.client.HTTPConnection is
    used instead (keep-alive, same idea — reconnects on failure).

TPM2.0 upgrade path:
    All TPM2 hooks are marked TODO-TPM2.  The only change required is adding
    two flags to _openssl_cmd():
        -engine tpm2tss          (load TPM2 engine)
        -key <tpm2_handle>       (hardware key reference, not a file path)
    Everything else — the tunnel lifecycle, the parser, the forwarder — stays
    identical.
"""
from __future__ import annotations

import base64
import http.client
import json
import logging
import os
import select
import subprocess
import threading
import time


class HttpsProfileClient:
    def __init__(self, profile: dict, gateway_id: str) -> None:
        cfg              = profile.get("https") or {}
        self._name       = profile.get("name", "?")
        self._profile_id = profile.get("id", "")
        self._host       = cfg.get("host", "")
        self._port       = int(cfg.get("port", 443))
        self._tls        = bool(cfg.get("tls", True))
        self._ca         = cfg.get("tls_ca_path", "")
        self._cert       = cfg.get("tls_cert_path", "")
        self._key        = cfg.get("tls_key_path", "")
        self._timeout    = int(cfg.get("timeout_seconds", 10))
        self._auth_type  = cfg.get("auth_type", "none")
        self._auth_value = cfg.get("auth_value", "")

        # TLS path: persistent openssl subprocess
        self._proc: subprocess.Popen | None = None
        self._force_restart = False   # set when server sends Connection: close
        self._lock = threading.Lock() # guards _proc / _force_restart

        # Plain HTTP path: persistent http.client connection
        self._http_conn: http.client.HTTPConnection | None = None

        # Status tracking (read by get_status())
        self._last_error:     str        = ""
        self._post_count:     int        = 0
        self._last_post_at:   float | None = None   # monotonic
        self._last_post_at_ms:int | None = None
        self._last_status_code: int | None = None
        self._tunnel_restarts: int       = 0
        self._down_at:        float | None = None   # monotonic; set on first failure, cleared on success
        self._down_at_ms:     int | None = None
        self._last_error_at_ms:int | None = None

        self._log = logging.getLogger("comms.https_client")

    def get_status(self) -> dict:
        """Return a snapshot of connection/post status for the UI."""
        now    = time.monotonic()
        # TLS: alive = subprocess is running
        # Plain HTTP: alive = no consecutive delivery failures (down_at is None)
        alive  = (self._proc is not None and self._proc.poll() is None) if self._tls else (self._down_at is None)
        scheme = "https" if self._tls else "http"
        return {
            "profile_id":       self._profile_id,
            "profile_name":     self._name,
            "endpoint":         f"{scheme}://{self._host}:{self._port}",
            "tls":              self._tls,
            "tunnel_alive":     alive,
            "tunnel_restarts":  self._tunnel_restarts,
            "last_error":       self._last_error,
            "last_error_at_ms": self._last_error_at_ms,
            "last_status_code": self._last_status_code,
            "last_post_ago":    round(now - self._last_post_at) if self._last_post_at else None,
            "last_post_at_ms":  self._last_post_at_ms,
            "post_count":       self._post_count,
            "down_since_ago":   round(now - self._down_at) if self._down_at else None,
            "down_since_ms":    self._down_at_ms,
        }

    # ── Public lifecycle ──────────────────────────────────────────────────────

    def start(self) -> None:
        """Spawn the TLS tunnel (or open the HTTP connection)."""
        if self._tls:
            self._spawn_tunnel()
        else:
            self._http_conn = http.client.HTTPConnection(
                self._host, self._port, timeout=self._timeout,
            )
            self._log.info("[%s] Plain-HTTP connection object created", self._name)

    def stop(self) -> None:
        """Tear down the tunnel / connection cleanly."""
        with self._lock:
            self._kill_tunnel()
        if self._http_conn:
            try:
                self._http_conn.close()
            except Exception:
                pass
            self._http_conn = None
        self._log.info("[%s] stopped", self._name)

    @property
    def is_alive(self) -> bool:
        if self._tls:
            with self._lock:
                return self._proc is not None and self._proc.poll() is None
        return True  # plain HTTP always attempts reconnect in post()

    # ── Public post ───────────────────────────────────────────────────────────

    def post(self, path: str, payload: dict) -> bool:
        """POST payload as JSON to path.  Returns True on 2xx."""
        if not self._host or not path:
            return False
        try:
            ok = self._post_tls(path, payload) if self._tls else self._post_plain(path, payload)
        except Exception as exc:
            self._log.error("[%s] POST %s unexpected: %s", self._name, path, exc)
            self._last_error = str(exc)
            self._last_error_at_ms = int(time.time() * 1000)
            ok = False
        if ok:
            self._down_at = None
            self._down_at_ms = None
        elif self._down_at is None:
            self._down_at = time.monotonic()
            self._down_at_ms = int(time.time() * 1000)
        return ok

    # ── TLS path ──────────────────────────────────────────────────────────────

    def _post_tls(self, path: str, payload: dict) -> bool:
        body    = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = self._build_request(path, body)

        with self._lock:
            # Restart tunnel if needed
            if self._force_restart or self._proc is None or self._proc.poll() is not None:
                self._log.info("[%s] Restarting TLS tunnel", self._name)
                self._kill_tunnel()
                self._spawn_tunnel()
                self._force_restart    = False
                self._tunnel_restarts += 1

            proc = self._proc

        if proc is None:
            msg = "Failed to start TLS tunnel"
            self._log.error("[%s] %s", self._name, msg)
            self._last_error = msg
            self._last_error_at_ms = int(time.time() * 1000)
            return False

        try:
            proc.stdin.write(request)
            proc.stdin.flush()
        except OSError as exc:
            self._log.warning("[%s] Write to tunnel failed: %s — will restart", self._name, exc)
            self._last_error = f"Tunnel write error: {exc}"
            self._last_error_at_ms = int(time.time() * 1000)
            with self._lock:
                self._kill_tunnel()
            return False

        try:
            code, headers = self._read_http_response(proc)
        except (TimeoutError, EOFError, ValueError, OSError) as exc:
            self._log.warning("[%s] Response read error: %s — will restart tunnel", self._name, exc)
            self._last_error = f"Response error: {exc}"
            self._last_error_at_ms = int(time.time() * 1000)
            with self._lock:
                self._kill_tunnel()
            return False

        # Server requested connection close — restart before next request
        if headers.get("connection", "").lower() == "close":
            with self._lock:
                self._force_restart = True

        ok = 200 <= code < 300
        self._last_status_code = code
        self._last_post_at     = time.monotonic()
        self._last_post_at_ms  = int(time.time() * 1000)
        if ok:
            self._post_count  += 1
            self._last_error   = ""
            self._log.info(
                "[%s] POST %s  status=%d  bytes=%d", self._name, path, code, len(body),
            )
        else:
            self._last_error = f"HTTP {code}"
            self._last_error_at_ms = int(time.time() * 1000)
            self._log.warning(
                "[%s] POST %s  status=%d  bytes=%d", self._name, path, code, len(body),
            )
        return ok

    def _spawn_tunnel(self) -> None:
        cmd = self._openssl_cmd()
        self._log.info(
            "[%s] Spawning TLS tunnel  %s:%d%s",
            self._name, self._host, self._port,
            "  mTLS=ON" if self._cert else "",
        )
        self._proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def _kill_tunnel(self) -> None:
        """Must be called with self._lock held."""
        if self._proc is not None:
            try:
                self._proc.stdin.close()
            except Exception:
                pass
            try:
                self._proc.terminate()
            except Exception:
                pass
            self._proc = None

    def _openssl_cmd(self) -> list[str]:
        cmd = [
            "openssl", "s_client",
            "-connect", f"{self._host}:{self._port}",
            "-quiet",
        ]
        if self._ca:
            cmd += ["-CAfile", self._ca]
        cmd.append("-verify_return_error")

        if self._cert and self._key:
            cmd += ["-cert", self._cert, "-key", self._key]
            # TODO-TPM2: replace the two lines above with:
            #   cmd += ["-engine", "tpm2tss"]
            #   cmd += ["-cert", self._cert]
            #   cmd += ["-key", self._key]   # <- TPM2 object handle, e.g. "0x81000001"

        return cmd

    # ── HTTP/1.1 response parser ──────────────────────────────────────────────

    def _read_http_response(
        self, proc: subprocess.Popen,
    ) -> tuple[int, dict]:
        """
        Read one complete HTTP/1.1 response from the tunnel stdout.
        Uses select() so we never block longer than self._timeout.
        Returns (status_code, headers_dict).
        """
        fd       = proc.stdout.fileno()
        deadline = time.monotonic() + self._timeout

        # ── byte-level read with timeout ──────────────────────────────────────

        def _readbyte() -> bytes:
            while True:
                rem = deadline - time.monotonic()
                if rem <= 0:
                    raise TimeoutError(f"[{self._name}] response timed out")
                ready, _, _ = select.select([fd], [], [], min(rem, 0.5))
                if ready:
                    b = os.read(fd, 1)
                    if not b:
                        raise EOFError(f"[{self._name}] tunnel closed by remote")
                    return b

        def _readline() -> str:
            buf = b""
            while True:
                buf += _readbyte()
                if buf.endswith(b"\n"):
                    return buf.decode("utf-8", errors="replace").rstrip("\r\n")

        # ── Status line ───────────────────────────────────────────────────────

        status_line = _readline()
        if not status_line.startswith("HTTP/"):
            raise ValueError(f"unexpected first line: {status_line!r}")
        parts = status_line.split(None, 2)
        if len(parts) < 2:
            raise ValueError(f"malformed status line: {status_line!r}")
        code = int(parts[1])

        # ── Headers ───────────────────────────────────────────────────────────

        headers: dict[str, str] = {}
        while True:
            line = _readline()
            if not line:
                break
            if ": " in line:
                name, _, val = line.partition(": ")
                headers[name.lower()] = val.strip()

        # ── Body consumption (keep the tunnel byte-stream in sync) ────────────

        te = headers.get("transfer-encoding", "").lower()
        cl = int(headers.get("content-length", "0"))

        def _readn(n: int) -> None:
            remaining = n
            while remaining > 0:
                rem = deadline - time.monotonic()
                if rem <= 0:
                    raise TimeoutError(f"[{self._name}] body read timed out")
                ready, _, _ = select.select([fd], [], [], min(rem, 0.5))
                if not ready:
                    raise TimeoutError(f"[{self._name}] body read stalled")
                chunk = os.read(fd, min(remaining, 4096))
                if not chunk:
                    raise EOFError(f"[{self._name}] tunnel closed mid-body")
                remaining -= len(chunk)

        if te == "chunked":
            while True:
                chunk_size_str = _readline()
                # chunk size line may have extension parameters after semicolon
                chunk_size = int(chunk_size_str.split(";")[0].strip(), 16)
                if chunk_size == 0:
                    _readline()   # trailing CRLF after last chunk
                    break
                _readn(chunk_size)
                _readline()       # CRLF after chunk data
        elif cl > 0:
            _readn(cl)

        return code, headers

    # ── Plain HTTP path ───────────────────────────────────────────────────────

    def _post_plain(self, path: str, payload: dict) -> bool:
        body    = json.dumps(payload, ensure_ascii=False)
        headers = {"Content-Type": "application/json", "Connection": "keep-alive"}
        for h in self._extra_headers():
            name, _, value = h.partition(": ")
            if name:
                headers[name] = value

        # Reconnect if the persistent connection dropped
        for attempt in range(2):
            try:
                if self._http_conn is None:
                    self._http_conn = http.client.HTTPConnection(
                        self._host, self._port, timeout=self._timeout,
                    )
                self._http_conn.request("POST", path, body.encode("utf-8"), headers)
                resp = self._http_conn.getresponse()
                resp.read()
                ok = 200 <= resp.status < 300
                self._last_status_code = resp.status
                self._last_post_at     = time.monotonic()
                self._last_post_at_ms  = int(time.time() * 1000)
                if ok:
                    self._post_count += 1
                    self._last_error  = ""
                    self._log.info(
                        "[%s] POST %s  status=%d  bytes=%d",
                        self._name, path, resp.status, len(body),
                    )
                else:
                    self._last_error = f"HTTP {resp.status}"
                    self._last_error_at_ms = int(time.time() * 1000)
                    self._log.warning(
                        "[%s] POST %s  status=%d", self._name, path, resp.status,
                    )
                return ok
            except Exception as exc:
                if attempt == 0:
                    self._log.debug(
                        "[%s] HTTP connection error (%s) — reconnecting", self._name, exc,
                    )
                    if self._http_conn:
                        try:
                            self._http_conn.close()
                        except Exception:
                            pass
                    self._http_conn = None
                else:
                    self._log.error("[%s] HTTP POST %s failed: %s", self._name, path, exc)
        return False

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _build_request(self, path: str, body: bytes) -> bytes:
        lines = [
            f"POST {path} HTTP/1.1",
            f"Host: {self._host}:{self._port}",
            "Content-Type: application/json",
            f"Content-Length: {len(body)}",
            "Connection: keep-alive",
        ]
        for h in self._extra_headers():
            lines.append(h)
        lines.append("")   # blank line → end of headers
        lines.append("")
        return "\r\n".join(lines).encode("utf-8") + body

    def _extra_headers(self) -> list[str]:
        if self._auth_type == "bearer":
            return [f"Authorization: Bearer {self._auth_value}"]
        if self._auth_type == "basic":
            encoded = base64.b64encode(self._auth_value.encode("utf-8")).decode()
            return [f"Authorization: Basic {encoded}"]
        if self._auth_type == "api_key":
            return [f"X-API-Key: {self._auth_value}"]
        return []
