import csv
import hashlib
import io
import json
import logging
from pathlib import Path
import subprocess
import time as _time

from fastapi import APIRouter, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from analytics_engine.settings_store import DEFAULT_USERNAME, ROOT_USERNAME

logger = logging.getLogger(__name__)

router    = APIRouter()
_here     = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(_here / "templates"))

# ── Cache-busting hashes computed once at server start ────────────────────────
def _file_hash(path: Path) -> str:
    try:
        return hashlib.md5(path.read_bytes()).hexdigest()[:10]
    except Exception:
        return "dev"

templates.env.globals["js_hash"]  = _file_hash(_here / "static" / "js"  / "app.js")
templates.env.globals["css_hash"] = _file_hash(_here / "static" / "css" / "app.css")
DEFAULT_PASSWORD = "gateway"


def _settings_store(request: Request):
    return request.app.state.settings_store


def _network_settings_store(request: Request):
    return request.app.state.network_settings_store

def _system_metrics_store(request: Request):
    return request.app.state.system_metrics_store

def _sensor_store(request: Request):
    return request.app.state.sensor_store

def _continuity_state(request: Request):
    return getattr(request.app.state, "continuity_state", None)


def _overview_status_payload(network_state: dict[str, object]) -> dict[str, object]:
    active_uplink = str(network_state.get("active_uplink", "none"))
    eth0 = network_state.get("eth0", {}) if isinstance(network_state.get("eth0"), dict) else {}
    eth1 = network_state.get("eth1", {}) if isinstance(network_state.get("eth1"), dict) else {}
    wifi_client = network_state.get("wifi_client", {}) if isinstance(network_state.get("wifi_client"), dict) else {}
    wifi_ap = network_state.get("wifi_ap", {}) if isinstance(network_state.get("wifi_ap"), dict) else {}

    eth0_connected = bool(eth0.get("link_up")) and bool(eth0.get("address"))
    eth1_connected = bool(eth1.get("link_up")) and bool(eth1.get("address"))
    ethernet_connected = eth0_connected or eth1_connected
    wifi_connected = bool(wifi_client.get("connected_ssid"))
    wifi_ap_enabled = bool(wifi_ap.get("enabled"))
    wifi_present = bool(wifi_client.get("present", True))

    if active_uplink in ("eth0", "eth1"):
        primary_link = "Ethernet"
    elif active_uplink == "wifi_client":
        primary_link = "Wi-Fi"
    else:
        primary_link = "Offline"

    if eth0_connected:
        ethernet_state = "Connected"
        ethernet_tone = "active"
        ethernet_detail = eth0.get("address") or "eth0 address assigned"
    elif eth1_connected:
        ethernet_state = "Connected"
        ethernet_tone = "active"
        ethernet_detail = eth1.get("address") or "eth1 address assigned"
    else:
        ethernet_state = "Disconnected"
        ethernet_tone = "inactive"
        ethernet_detail = "No cable link on eth0 or eth1"

    if wifi_connected:
        wifi_state = "Connected"
        wifi_tone = "active"
        wifi_detail = wifi_client.get("connected_ssid") or "Wireless uplink active"
    elif wifi_ap_enabled:
        wifi_state = "Access Point"
        wifi_tone = "standby"
        wifi_detail = f"{wifi_ap.get('clients', 0)} client(s) on hotspot"
    elif wifi_present:
        wifi_state = "Standby"
        wifi_tone = "standby"
        wifi_detail = "Radio available for setup"
    else:
        wifi_state = "Unavailable"
        wifi_tone = "inactive"
        wifi_detail = "Wireless interface not detected"

    gateway_health = "Online" if ethernet_connected or wifi_connected or wifi_ap_enabled else "Standby"

    return {
        "status_chips": [
            {"label": "Gateway", "value": gateway_health},
            {"label": "Primary Link", "value": primary_link},
            {"label": "Wireless", "value": wifi_state},
        ],
        "connectivity_items": [
            {
                "label": "Ethernet",
                "state": ethernet_state,
                "detail": ethernet_detail,
                "tone": ethernet_tone,
            },
            {
                "label": "Wi-Fi",
                "state": wifi_state,
                "detail": wifi_detail,
                "tone": wifi_tone,
            },
        ],
        "visual": {
            "gateway_online": ethernet_connected or wifi_connected or wifi_ap_enabled,
            "ethernet_active": ethernet_connected or active_uplink in ("eth0", "eth1"),
            "wifi_active": wifi_connected or wifi_ap_enabled or active_uplink == "wifi_client",
        },
    }


def _primary_sections(active_label: str) -> list[dict[str, object]]:
    items = [
        ("Overview", "Over", "/dashboard"),
        ("Monitor", "Mon", "/monitor"),
        ("Insights", "Info", "/insights"),
        ("Interfaces", "I/O", "/interfaces"),
        ("Network Probe", "Probe", "#"),
        ("Data Forwarding", "Fwd", "/forwarding"),
        ("Connectivity", "Conn", "/connectivity"),
        ("Security", "Sec", "#"),
        ("System", "Sys", "/system"),
    ]
    return [
        {
            "label": label,
            "compact": compact,
            "href": href,
            "active": label == active_label,
            "disabled": href == "#",
        }
        for label, compact, href in items
    ]


def _client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _session_user(request: Request) -> str:
    return request.session.get("username", "unknown")


def _is_authenticated(request: Request) -> bool:
    if not request.session.get("authenticated"):
        return False
    # Root session is not bound to the server nonce — survives restarts indefinitely
    if request.session.get("username") == ROOT_USERNAME:
        return True
    return request.session.get("session_nonce") == getattr(request.app.state, "session_nonce", None)


def _run_network_apply_service() -> tuple[bool, dict[str, object]]:
    networkctl = Path("/opt/gateway/scripts/gateway-networkctl")
    if not networkctl.exists():
        logger.error("gateway-networkctl not found at %s", networkctl)
        return False, {
            "apply_requested": False,
            "apply_status": "apply_error",
            "errors": [
                {
                    "scope": "apply_service",
                    "code": "networkctl_missing",
                    "message": "The gateway-networkctl wrapper is missing from the image.",
                }
            ],
        }

    try:
        completed = subprocess.run(
            ["sudo", "-n", str(networkctl), "apply"],
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except FileNotFoundError as exc:
        logger.error("Could not execute gateway-networkctl: %s", exc)
        return False, {
            "apply_requested": False,
            "apply_status": "apply_error",
            "errors": [
                {
                    "scope": "apply_service",
                    "code": "command_missing",
                    "message": "Could not execute the gateway network apply command.",
                    "detail": str(exc),
                }
            ],
        }
    except subprocess.TimeoutExpired:
        logger.error("gateway-networkctl apply timed out")
        return False, {
            "apply_requested": True,
            "apply_status": "apply_error",
            "errors": [
                {
                    "scope": "apply_service",
                    "code": "apply_timeout",
                    "message": "The gateway network apply command did not finish in time.",
                }
            ],
        }

    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        logger.error("gateway-networkctl apply failed (rc=%d): %s", completed.returncode, detail)
        return False, {
            "apply_requested": True,
            "apply_status": "apply_error",
            "errors": [
                {
                    "scope": "apply_service",
                    "code": "apply_command_failed",
                    "message": "The gateway network apply command failed.",
                    "detail": detail,
                }
            ],
        }

    return True, {
        "apply_requested": True,
        "apply_status": "apply_requested",
        "errors": [],
    }


def _run_networkctl_command(*args: str, timeout: int = 60) -> tuple[bool, dict[str, object]]:
    networkctl = Path("/opt/gateway/scripts/gateway-networkctl")
    if not networkctl.exists():
        return False, {
            "errors": [
                {
                    "scope": "networkctl",
                    "code": "networkctl_missing",
                    "message": "The gateway-networkctl wrapper is missing from the image.",
                }
            ]
        }

    try:
        completed = subprocess.run(
            ["sudo", "-n", str(networkctl), *args],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        return False, {
            "errors": [
                {
                    "scope": "networkctl",
                    "code": "command_missing",
                    "message": "Could not execute gateway-networkctl.",
                    "detail": str(exc),
                }
            ]
        }
    except subprocess.TimeoutExpired:
        return False, {
            "errors": [
                {
                    "scope": "networkctl",
                    "code": "timeout",
                    "message": "gateway-networkctl did not finish in time.",
                }
            ]
        }

    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        return False, {
            "errors": [
                {
                    "scope": "networkctl",
                    "code": "command_failed",
                    "message": "gateway-networkctl returned a failure status.",
                    "detail": detail,
                }
            ]
        }

    output = (completed.stdout or "").strip()
    if not output:
        return True, {}

    try:
        return True, json.loads(output)
    except json.JSONDecodeError:
        return True, {"output": output}


@router.get("/", response_class=HTMLResponse)
async def login_page(request: Request) -> HTMLResponse:
    if _is_authenticated(request):
        return RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)

    return templates.TemplateResponse(
        request,
        "login.html",
        {
            "product_name": "MetaCrust Edge Gateway",
            "page_title": "Secure Access",
        },
    )


@router.head("/", response_class=HTMLResponse)
async def login_page_head(request: Request) -> HTMLResponse:
    return await login_page(request)


@router.post("/api/login")
async def login_action(request: Request) -> JSONResponse:
    payload = await request.json()
    username = str(payload.get("username", "")).strip()
    password = str(payload.get("password", ""))

    ip = _client_ip(request)
    if _settings_store(request).verify_credentials(username, password):
        request.session["authenticated"] = True
        request.session["username"] = username
        request.session["session_nonce"] = getattr(request.app.state, "session_nonce", None)
        logger.info("AUTH  login_success  user=%s  ip=%s", username, ip)
        return JSONResponse({"ok": True, "redirect": "/dashboard"})

    logger.warning("AUTH  login_failed  user=%s  ip=%s", username, ip)
    return JSONResponse(
        {"ok": False, "message": "Invalid gateway credentials."},
        status_code=status.HTTP_401_UNAUTHORIZED,
    )


@router.post("/logout")
async def logout_action(request: Request) -> RedirectResponse:
    logger.info("AUTH  logout  user=%s  ip=%s", _session_user(request), _client_ip(request))
    request.session.clear()
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request) -> HTMLResponse:
    if not _is_authenticated(request):
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)

    network_state = _network_settings_store(request).get_state()
    overview_payload = _overview_status_payload(network_state)
    system_metrics = _system_metrics_store(request).get_current()

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "product_name": "MetaCrust Edge Gateway",
            "page_title": "Control Plane",
            "primary_sections": _primary_sections("Overview"),
            "status_chips": overview_payload["status_chips"],
            "connectivity_items": overview_payload["connectivity_items"],
            "overview_visual": overview_payload["visual"],
            "system_metrics": system_metrics,
            "domain_cards": [
                {
                    "title": "Insights",
                    "description": "Continuity, anomalies, incidents, trends, and evidence across sensor and network data.",
                },
                {
                    "title": "Interfaces",
                    "description": "RS232, RS485, Modbus RTU, GPS, IMU, DI/DO, and attached field devices.",
                },
                {
                    "title": "Network Probe",
                    "description": "Ping, SNMP, discovery, interface statistics, and later flow visibility.",
                },
                {
                    "title": "Destinations",
                    "description": "MQTTS, HTTPS, buffering, retries, and upstream delivery profiles.",
                },
                {
                    "title": "Connectivity",
                    "description": "Ethernet, Wi-Fi, uplink setup, and local network behavior.",
                },
                {
                    "title": "Security",
                    "description": "Gateway access, certificates, keys, firewall policy, and trust controls.",
                },
            ],
        },
    )


@router.get("/monitor", response_class=HTMLResponse)
async def monitor_page(request: Request) -> HTMLResponse:
    if not _is_authenticated(request):
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)

    return templates.TemplateResponse(
        request,
        "monitor.html",
        {
            "product_name": "MetaCrust Edge Gateway",
            "page_title": "Monitor",
            "primary_sections": _primary_sections("Monitor"),
        },
    )


@router.get("/connectivity", response_class=HTMLResponse)
async def connectivity_page(request: Request) -> HTMLResponse:
    if not _is_authenticated(request):
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)

    network_settings = _network_settings_store(request).get_settings()
    network_state = _network_settings_store(request).get_state()
    apply_result = _network_settings_store(request).get_apply_result()
    return templates.TemplateResponse(
        request,
        "connectivity.html",
        {
            "product_name": "MetaCrust Edge Gateway",
            "page_title": "Connectivity",
            "primary_sections": _primary_sections("Connectivity"),
            "connectivity_tabs": [
                {"id": "ethernet", "label": "Ethernet", "active": True, "disabled": False},
                {"id": "wifi", "label": "Wi-Fi", "active": False, "disabled": False},
                {"id": "cellular", "label": "Cellular", "active": False, "disabled": False},
                {"id": "policy", "label": "Uplink Policy", "active": False, "disabled": False},
            ],
            "network_settings": network_settings,
            "network_state": network_state,
            "apply_result": apply_result,
        },
    )


@router.get("/system", response_class=HTMLResponse)
async def system_page(request: Request) -> HTMLResponse:
    if not _is_authenticated(request):
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)

    return templates.TemplateResponse(
        request,
        "system.html",
        {
            "product_name": "MetaCrust Edge Gateway",
            "page_title": "System",
            "primary_sections": _primary_sections("System"),
            "system_tabs": [
                {"id": "access", "label": "Access", "active": True, "disabled": False},
                {"id": "identity", "label": "Identity", "active": False, "disabled": True},
                {"id": "services", "label": "Services", "active": False, "disabled": True},
                {"id": "updates", "label": "Updates", "active": False, "disabled": True},
            ],
            "current_username": request.session.get("username", DEFAULT_USERNAME),
        },
    )


@router.post("/api/system/access")
async def update_access(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    payload = await request.json()
    new_username = str(payload.get("new_username", "")).strip()
    current_password = str(payload.get("current_password", ""))
    new_password = str(payload.get("new_password", ""))
    confirm_password = str(payload.get("confirm_password", ""))

    if new_password != confirm_password:
        return JSONResponse({"ok": False, "message": "New passwords do not match."}, status_code=status.HTTP_400_BAD_REQUEST)

    old_user = _session_user(request)
    success, message = _settings_store(request).update_credentials(
        current_password=current_password,
        new_username=new_username,
        new_password=new_password,
    )
    if not success:
        logger.warning("CONFIG  credentials_change_failed  user=%s  reason=%s", old_user, message)
        return JSONResponse({"ok": False, "message": message}, status_code=status.HTTP_400_BAD_REQUEST)

    logger.info("CONFIG  credentials_changed  user=%s→%s", old_user, new_username)
    request.session["username"] = new_username
    return JSONResponse({"ok": True, "message": message})


@router.get("/api/network/settings")
async def get_network_settings(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    return JSONResponse(_network_settings_store(request).get_settings())


@router.post("/api/network/settings")
async def save_network_settings(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    payload = await request.json()
    success, response = _network_settings_store(request).save_settings(payload)
    status_code = status.HTTP_200_OK if success else status.HTTP_400_BAD_REQUEST
    response["ok"] = success
    uplink_cfg = payload.get("uplink") or {}
    priority   = uplink_cfg.get("uplink_priority", [])
    if success:
        logger.info("CONFIG  network_saved  user=%s  priority=%s", _session_user(request), priority)
    else:
        logger.warning("CONFIG  network_save_failed  user=%s  reason=%s", _session_user(request), response.get("message", "unknown"))
    return JSONResponse(response, status_code=status_code)


@router.post("/api/network/apply")
async def apply_network_settings(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    user = _session_user(request)
    success, response = _run_network_apply_service()
    response["ok"] = success
    response["result_path"] = str(_network_settings_store(request).layout.apply_result_file)
    if success:
        apply_result = _network_settings_store(request).get_apply_result()
        response.update(
            {
                "apply_status": apply_result.get("status", response.get("apply_status")),
                "errors": apply_result.get("errors", []),
                "warnings": apply_result.get("warnings", []),
                "active_uplink": apply_result.get("active_uplink", "none"),
                "used_defaults": apply_result.get("used_defaults", False),
                "timestamp": apply_result.get("timestamp"),
            }
        )
        response["ok"] = bool(apply_result.get("ok", True))
        success = response["ok"]
    if success:
        logger.info("CONFIG  network_applied  user=%s  uplink=%s", user, response.get("active_uplink", "?"))
    else:
        logger.error("CONFIG  network_apply_failed  user=%s  errors=%s", user, response.get("errors", []))
    status_code = status.HTTP_200_OK if success else status.HTTP_500_INTERNAL_SERVER_ERROR
    return JSONResponse(response, status_code=status_code)


@router.post("/api/network/save-and-apply")
async def save_and_apply_network_settings(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    payload = await request.json()
    saved, save_response = _network_settings_store(request).save_settings(payload)
    if not saved:
        save_response["ok"] = False
        return JSONResponse(save_response, status_code=status.HTTP_400_BAD_REQUEST)

    applied, apply_response = _run_network_apply_service()
    response = {**save_response, **apply_response}
    response["ok"] = applied
    response["saved"] = True
    response["result_path"] = str(_network_settings_store(request).layout.apply_result_file)
    if applied:
        apply_result = _network_settings_store(request).get_apply_result()
        response.update(
            {
                "apply_status": apply_result.get("status", response.get("apply_status")),
                "errors": apply_result.get("errors", []),
                "warnings": apply_result.get("warnings", []),
                "active_uplink": apply_result.get("active_uplink", "none"),
                "used_defaults": apply_result.get("used_defaults", False),
                "timestamp": apply_result.get("timestamp"),
            }
        )
        response["ok"] = bool(apply_result.get("ok", True))
        applied = response["ok"]
        if not applied:
            logger.error("Network apply reported failure: status=%s errors=%s", apply_result.get("status"), apply_result.get("errors"))
    status_code = status.HTTP_200_OK if applied else status.HTTP_500_INTERNAL_SERVER_ERROR
    return JSONResponse(response, status_code=status_code)


@router.get("/api/network/state")
async def get_network_state(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    return JSONResponse(_network_settings_store(request).get_state())


@router.get("/api/network/apply-result")
async def get_network_apply_result(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    return JSONResponse(_network_settings_store(request).get_apply_result())


@router.get("/api/system/metrics")
async def get_system_metrics(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    payload = _system_metrics_store(request).get_current()
    payload["ok"] = True
    return JSONResponse(payload)


@router.get("/api/system/metrics/history")
async def get_system_metrics_history(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    payload = _system_metrics_store(request).get_history()
    payload["ok"] = True
    return JSONResponse(payload)


@router.get("/interfaces", response_class=HTMLResponse)
async def interfaces_page(request: Request) -> HTMLResponse:
    if not _is_authenticated(request):
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)

    rs232_config = request.app.state.rs232_config_store.get_config()
    rs485_config = request.app.state.rs485_config_store.get_config()
    modbus_tcp_config = request.app.state.modbus_tcp_config_store.get_config()
    return templates.TemplateResponse(
        request,
        "interfaces.html",
        {
            "product_name": "MetaCrust Edge Gateway",
            "page_title": "Interfaces",
            "primary_sections": _primary_sections("Interfaces"),
            "rs232_config": rs232_config,
            "rs485_config": rs485_config,
            "modbus_tcp_config": modbus_tcp_config,
        },
    )


@router.get("/api/interfaces/rs232/config")
async def get_rs232_config(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    config = request.app.state.rs232_config_store.get_config()
    config["ok"] = True
    return JSONResponse(config)


@router.post("/api/interfaces/rs232/config")
async def save_rs232_config(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    payload = await request.json()
    success, response = request.app.state.rs232_config_store.save_config(payload)
    if success:
        request.app.state.redis_notifier.notify_changed("rs232_config")
        ports = payload.get("rs232") or {}
        summary = "  ".join(
            f"{pid}={'ON' if p.get('enabled') else 'OFF'}({p.get('sensor','?')})"
            for pid, p in ports.items()
        ) or "no_ports"
        logger.info("CONFIG  rs232_saved  user=%s  %s", _session_user(request), summary)
    else:
        logger.warning("CONFIG  rs232_save_failed  user=%s  reason=%s", _session_user(request), response.get("message", "unknown"))
    response["ok"] = success
    status_code = status.HTTP_200_OK if success else status.HTTP_400_BAD_REQUEST
    return JSONResponse(response, status_code=status_code)


@router.get("/api/interfaces/rs485/config")
async def get_rs485_config(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    config = request.app.state.rs485_config_store.get_config()
    config["ok"] = True
    return JSONResponse(config)


@router.post("/api/interfaces/rs485/config")
async def save_rs485_config(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    payload = await request.json()
    success, response = request.app.state.rs485_config_store.save_config(payload)
    if success:
        request.app.state.redis_notifier.notify_changed("rs485_config")
        ports = payload.get("rs485") or {}
        summary = "  ".join(
            f"{pid}={'ON' if p.get('enabled') else 'OFF'}({len((p.get('modbus_rtu') or {}).get('registers', []))} regs)"
            for pid, p in ports.items()
        ) or "no_ports"
        logger.info("CONFIG  rs485_saved  user=%s  %s", _session_user(request), summary)
    else:
        logger.warning("CONFIG  rs485_save_failed  user=%s  reason=%s", _session_user(request), response.get("message", "unknown"))
    response["ok"] = success
    status_code = status.HTTP_200_OK if success else status.HTTP_400_BAD_REQUEST
    return JSONResponse(response, status_code=status_code)


@router.get("/api/interfaces/modbus-tcp/config")
async def get_modbus_tcp_config(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    config = request.app.state.modbus_tcp_config_store.get_config()
    config["ok"] = True
    return JSONResponse(config)


@router.post("/api/interfaces/modbus-tcp/config")
async def save_modbus_tcp_config(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    payload = await request.json()
    success, response = request.app.state.modbus_tcp_config_store.save_config(payload)
    if success:
        request.app.state.redis_notifier.notify_changed("modbus_tcp_config")
        conns   = payload.get("connections") or []
        enabled = sum(1 for c in conns if c.get("enabled"))
        names   = [c.get("name") or c.get("id", "?") for c in conns if c.get("enabled")]
        logger.info(
            "CONFIG  modbus_tcp_saved  user=%s  total=%d  enabled=%d  active=[%s]",
            _session_user(request), len(conns), enabled, ", ".join(names),
        )
    else:
        logger.warning("CONFIG  modbus_tcp_save_failed  user=%s  reason=%s", _session_user(request), response.get("message", "unknown"))
    response["ok"] = success
    status_code = status.HTTP_200_OK if success else status.HTTP_400_BAD_REQUEST
    return JSONResponse(response, status_code=status_code)


_RS232_PORT_MAP = {
    "port_0": {"endpoint": "/dev/ttyAMA2", "channel": "Ch0"},
    "port_1": {"endpoint": "/dev/ttyAMA3", "channel": "Ch1"},
}
_RS485_PORT_MAP = {
    "port_2": {"endpoint": "/dev/ttyAMA4", "channel": "Ch2"},
    "port_3": {"endpoint": "/dev/ttyAMA0", "channel": "Ch3"},
}
_DUSTRAK_METRICS = [
    {"name": "pm1",   "unit": "mg/m³"},
    {"name": "pm25",  "unit": "mg/m³"},
    {"name": "pm4",   "unit": "mg/m³"},
    {"name": "pm10",  "unit": "mg/m³"},
    {"name": "total", "unit": "mg/m³"},
]


# ══════════════════════════════════════════════════════════════════════════════
#  Data Forwarding page
# ══════════════════════════════════════════════════════════════════════════════

def _forwarding_store(request: Request):
    return request.app.state.forwarding_config_store


@router.get("/forwarding", response_class=HTMLResponse)
async def forwarding_page(request: Request) -> HTMLResponse:
    if not _is_authenticated(request):
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
    return templates.TemplateResponse(
        request,
        "forwarding.html",
        {
            "product_name":     "MetaCrust Edge Gateway",
            "page_title":       "Data Forwarding",
            "primary_sections": _primary_sections("Data Forwarding"),
        },
    )


@router.get("/api/forwarding/config")
async def get_forwarding_config(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)
    config = _forwarding_store(request).get_config()
    config["ok"] = True
    return JSONResponse(config)


@router.post("/api/forwarding/config")
async def save_forwarding_config(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)
    payload = await request.json()
    success, response = _forwarding_store(request).save_config(payload)
    if success:
        request.app.state.redis_notifier.notify_changed("forwarding_config")
        profiles = payload.get("profiles") or []
        enabled  = sum(1 for p in profiles if p.get("enabled"))
        logger.info(
            "CONFIG  forwarding_saved  user=%s  profiles=%d  enabled=%d",
            _session_user(request), len(profiles), enabled,
        )
    else:
        logger.warning("CONFIG  forwarding_save_failed  user=%s  reason=%s",
                       _session_user(request), response.get("message"))
    response["ok"] = success
    return JSONResponse(response, status_code=status.HTTP_200_OK if success else status.HTTP_400_BAD_REQUEST)


@router.get("/insights", response_class=HTMLResponse)
async def insights_page(request: Request) -> HTMLResponse:
    if not _is_authenticated(request):
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)

    return templates.TemplateResponse(
        request,
        "insights.html",
        {
            "product_name":    "MetaCrust Edge Gateway",
            "page_title":      "Insights",
            "primary_sections": _primary_sections("Insights"),
        },
    )


@router.get("/api/insights/configured")
async def insights_configured(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    rs232  = request.app.state.rs232_config_store.get_config()
    rs485  = request.app.state.rs485_config_store.get_config()
    modbus = request.app.state.modbus_tcp_config_store.get_config()

    devices = []

    # RS232
    for port_id, meta in _RS232_PORT_MAP.items():
        port = (rs232.get("rs232") or {}).get(port_id, {})
        if not port.get("enabled"):
            continue
        sensor_type = port.get("sensor", "dustrak")
        devices.append({
            "source":           "rs232",
            "device_id":        port_id,
            "name":             f"{sensor_type.capitalize()} {meta['channel']}",
            "device_type":      sensor_type,
            "transport":        {"type": "serial", "endpoint": meta["endpoint"], "channel": meta["channel"]},
            "expected_metrics": list(_DUSTRAK_METRICS),
        })

    # RS485 / Modbus RTU
    for port_id, meta in _RS485_PORT_MAP.items():
        port = (rs485.get("rs485") or {}).get(port_id, {})
        if not port.get("enabled"):
            continue
        regs    = (port.get("modbus_rtu") or {}).get("registers", [])
        metrics = [{"name": r["name"], "unit": r.get("unit", "")} for r in regs if r.get("name")]
        if not metrics:
            continue
        devices.append({
            "source":           "rs485",
            "device_id":        port_id,
            "name":             port.get("name") or port_id,
            "device_type":      "modbus_rtu",
            "transport":        {"type": "modbus_rtu", "endpoint": meta["endpoint"], "channel": meta["channel"]},
            "expected_metrics": metrics,
        })

    # Modbus TCP
    for conn in (modbus.get("connections") or []):
        if not conn.get("enabled"):
            continue
        regs    = conn.get("registers", [])
        metrics = [{"name": r["name"].strip(), "unit": r.get("unit", "").strip()} for r in regs if r.get("name", "").strip()]
        if not metrics:
            continue
        devices.append({
            "source":           "modbus_tcp",
            "device_id":        conn["id"],
            "name":             conn.get("name") or conn["id"],
            "device_type":      "modbus_tcp",
            "transport":        {
                "type":      "modbus_tcp",
                "endpoint":  conn.get("ip", ""),
                "port":      conn.get("port", 502),
                "interface": conn.get("interface", "eth0"),
            },
            "expected_metrics": metrics,
        })

    # Merge in any device PES is reporting via Redis that isn't already covered
    configured_keys = {(d["source"], d["device_id"]) for d in devices}
    for live in _sensor_store(request).live_devices():
        key = (live.get("source"), live.get("device_id"))
        if key in configured_keys:
            continue
        metrics = live.get("metrics") or {}
        devices.append({
            "source":           live.get("source", ""),
            "device_id":        live.get("device_id", ""),
            "name":             live.get("name") or live.get("device_id", ""),
            "device_type":      live.get("device_type", ""),
            "transport":        live.get("transport") or {},
            "expected_metrics": [
                {"name": k, "unit": (m.get("unit") or "").strip()}
                for k, m in metrics.items()
            ],
        })

    return JSONResponse({"ok": True, "devices": devices})


@router.get("/api/insights/live")
async def insights_live(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    store   = _sensor_store(request)
    devices = store.live_devices()
    for device in devices:
        device["_samples"] = store.device_samples_per_metric(
            device["source"], device["device_id"], limit=60
        )
    return JSONResponse({"ok": True, "devices": devices})


_WINDOW_MAP: dict[str, tuple[int, int]] = {
    "1h":  (1,   60),    # window_hours, buckets
    "6h":  (6,   180),
    "24h": (24,  240),
    "7d":  (168, 280),
}


def _analytical_store(request: Request):
    return getattr(request.app.state, "analytical_store", None)


@router.get("/api/insights/history")
async def insights_history(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    source    = request.query_params.get("source", "").strip()
    device_id = request.query_params.get("device_id", "").strip()
    window    = request.query_params.get("window", "1h").strip()

    # Accept either ?metrics=pm25,pm10 (new) or ?metric=pm25 (legacy single)
    metrics_raw = request.query_params.get("metrics") or request.query_params.get("metric") or ""
    metric_names = [m.strip() for m in metrics_raw.split(",") if m.strip()]

    if not (source and device_id and metric_names):
        return JSONResponse(
            {"ok": False, "message": "source, device_id, and metrics are required."},
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    window_hours, buckets = _WINDOW_MAP.get(window, _WINDOW_MAP["1h"])
    store  = _sensor_store(request)
    result = {}

    for metric in metric_names[:8]:   # cap at 8 metrics per request
        result[metric] = store.metric_history(
            source, device_id, metric,
            window_hours=window_hours,
            buckets=buckets,
        )

    return JSONResponse({"ok": True, "window": window, "window_hours": window_hours, "metrics": result})


_EVENTS_WINDOW_MAP: dict[str, int] = {
    "1h": 1, "6h": 6, "24h": 24, "7d": 168,
}


@router.get("/api/insights/events")
async def insights_events(request: Request) -> JSONResponse:
    """
    Merged event timeline: pes.db:sensor_events  +  analytical.db:alert_events.
    Query params: window (1h/6h/24h/7d), source, device_id, severity, limit.
    """
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    try:
        limit = max(1, min(500, int(request.query_params.get("limit", "200"))))
    except ValueError:
        limit = 200

    window_key = request.query_params.get("window", "24h")
    window_h   = _EVENTS_WINDOW_MAP.get(window_key, 24)
    since_ms   = int((_time.time() - window_h * 3600) * 1000)

    source    = request.query_params.get("source")    or None
    device_id = request.query_params.get("device_id") or None
    severity  = (request.query_params.get("severity") or "").lower() or None

    # ── PES sensor events ────────────────────────────────────────────────────
    pes_events = _sensor_store(request).recent_events(
        limit=limit, source=source, device_id=device_id, since_ms=since_ms,
    )
    for e in pes_events:
        e["origin"] = "pes"

    # ── AES alert events ─────────────────────────────────────────────────────
    astore = _analytical_store(request)
    alert_events: list[dict] = []
    if astore:
        raw = astore.get_alert_events(
            source=source, device_id=device_id, since_ms=since_ms, limit=limit,
        )
        for e in raw:
            e["origin"]      = "alert"
            e["event_type"]  = f"alert:{e['event_type']}"   # fired / resolved
            e["device_name"] = e.get("device_id", "")       # best effort
        alert_events = raw

    # ── Merge + sort ─────────────────────────────────────────────────────────
    merged = pes_events + alert_events
    merged.sort(key=lambda e: e.get("timestamp_ms", 0), reverse=True)

    # Severity filter (applied after merge)
    if severity:
        _sev_order = {"info": 0, "warning": 1, "error": 2, "critical": 2}
        min_sev    = _sev_order.get(severity, 0)
        merged     = [e for e in merged if _sev_order.get((e.get("severity") or "info").lower(), 0) >= min_sev]

    merged = merged[:limit]

    # ── Deduplicate consecutive identical events ───────────────────────────────
    # Events are newest-first. Collapse runs of same (device_id + event_type)
    # within a 10-minute window to avoid spam rows (e.g. stale_data every 5 s).
    deduped: list[dict] = []
    for ev in merged:
        prev = deduped[-1] if deduped else None
        same = (
            prev is not None
            and prev.get("device_id")  == ev.get("device_id")
            and prev.get("source")     == ev.get("source")
            and prev.get("event_type") == ev.get("event_type")
            and (prev.get("timestamp_ms", 0) - ev.get("timestamp_ms", 0)) < 600_000  # 10 min
        )
        if same:
            prev["_count"] = prev.get("_count", 1) + 1
            prev["_first_ts"] = ev.get("timestamp_ms")   # oldest timestamp in the run
        else:
            deduped.append(dict(ev))

    return JSONResponse({"ok": True, "events": deduped, "window": window_key})


# ── Alert rules CRUD ──────────────────────────────────────────────────────────

@router.get("/api/insights/alert-rules")
async def get_alert_rules(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)
    store = _analytical_store(request)
    if store is None:
        return JSONResponse({"ok": True, "rules": []})
    rules = store.get_alert_rules()
    return JSONResponse({"ok": True, "rules": rules})


@router.post("/api/insights/alert-rules")
async def create_alert_rule(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)
    store = _analytical_store(request)
    if store is None:
        return JSONResponse({"ok": False, "message": "Analytical store unavailable."}, status_code=status.HTTP_503_SERVICE_UNAVAILABLE)

    payload = await request.json()
    required = ("source", "device_id", "metric_name", "condition", "threshold")
    missing  = [k for k in required if not payload.get(k) and payload.get(k) != 0]
    if missing:
        return JSONResponse({"ok": False, "message": f"Missing: {', '.join(missing)}"}, status_code=status.HTTP_400_BAD_REQUEST)
    if payload["condition"] not in ("gt", "lt", "gte", "lte", "eq"):
        return JSONResponse({"ok": False, "message": "condition must be gt/lt/gte/lte/eq"}, status_code=status.HTTP_400_BAD_REQUEST)
    try:
        payload["threshold"] = float(payload["threshold"])
    except (ValueError, TypeError):
        return JSONResponse({"ok": False, "message": "threshold must be a number"}, status_code=status.HTTP_400_BAD_REQUEST)

    rule_id = store.create_alert_rule(payload)
    if rule_id < 0:
        return JSONResponse({"ok": False, "message": "Failed to save rule."}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    # Tell the rules engine to reload immediately
    re = getattr(request.app.state, "rules_engine", None)
    if re:
        re.reload()

    logger.info(
        "CONFIG  alert_rule_created  user=%s  device=%s/%s  metric=%s  cond=%s  threshold=%s  severity=%s",
        _session_user(request), payload["source"], payload["device_id"],
        payload["metric_name"], payload["condition"], payload["threshold"],
        payload.get("severity", "warning"),
    )
    return JSONResponse({"ok": True, "rule_id": rule_id})


@router.delete("/api/insights/alert-rules/{rule_id}")
async def delete_alert_rule(request: Request, rule_id: int) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)
    store = _analytical_store(request)
    if store is None:
        return JSONResponse({"ok": False, "message": "Analytical store unavailable."}, status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
    ok = store.delete_alert_rule(rule_id)
    if ok:
        re = getattr(request.app.state, "rules_engine", None)
        if re:
            re.reload()
        logger.info("CONFIG  alert_rule_deleted  user=%s  rule_id=%d", _session_user(request), rule_id)
    return JSONResponse({"ok": ok})


@router.put("/api/insights/alert-rules/{rule_id}")
async def toggle_alert_rule(request: Request, rule_id: int) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)
    store = _analytical_store(request)
    if store is None:
        return JSONResponse({"ok": False, "message": "Analytical store unavailable."}, status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
    payload = await request.json()
    enabled = bool(payload.get("enabled", True))
    ok      = store.set_rule_enabled(rule_id, enabled)
    if ok:
        re = getattr(request.app.state, "rules_engine", None)
        if re:
            re.reload()
        logger.info(
            "CONFIG  alert_rule_toggled  user=%s  rule_id=%d  enabled=%s",
            _session_user(request), rule_id, enabled,
        )
    return JSONResponse({"ok": ok})


@router.get("/api/insights/summary")
async def insights_summary(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    store   = _sensor_store(request)
    devices = store.live_devices()
    stats   = store.summary_stats(devices)

    continuity = _continuity_state(request)
    if continuity is not None:
        stats["anomaly_count"] = continuity.anomaly_count()

    stats["ok"] = True
    return JSONResponse(stats)


# ── Tier 2: Rolling stats ─────────────────────────────────────────────────────

@router.get("/api/insights/stats")
async def insights_stats(request: Request) -> JSONResponse:
    """
    Return pre-computed rolling window stats for one device.
    Query params: source, device_id
    Response: { ok, stats: { "5min": {metric: {avg,min,max,stddev,sample_count,good_count,health_pct}}, ... } }
    """
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    source    = request.query_params.get("source", "").strip()
    device_id = request.query_params.get("device_id", "").strip()

    if not (source and device_id):
        return JSONResponse({"ok": False, "message": "source and device_id required."}, status_code=status.HTTP_400_BAD_REQUEST)

    astore = _analytical_store(request)
    if astore is None:
        return JSONResponse({"ok": True, "stats": {}})

    rows = astore.get_metric_stats(source, device_id)

    # Pivot: { window → { metric → stats_dict } }
    result: dict[str, dict] = {}
    for r in rows:
        win    = r["window"]
        metric = r["metric_name"]
        n      = r.get("sample_count") or 0
        good   = r.get("good_count")   or 0
        health = round(good / n * 100, 1) if n > 0 else None
        result.setdefault(win, {})[metric] = {
            "avg":          r.get("avg"),
            "min":          r.get("min"),
            "max":          r.get("max"),
            "stddev":       r.get("stddev"),
            "sample_count": n,
            "good_count":   good,
            "health_pct":   health,
            "computed_at":  r.get("computed_at"),
        }

    return JSONResponse({"ok": True, "stats": result})


# ── Tier 3: Trend detection ───────────────────────────────────────────────────

@router.get("/api/insights/trends")
async def insights_trends(request: Request) -> JSONResponse:
    """
    Return trend snapshots enriched with time-to-threshold estimates.
    Query params: source, device_id
    """
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    source    = request.query_params.get("source", "").strip()
    device_id = request.query_params.get("device_id", "").strip()

    if not (source and device_id):
        return JSONResponse({"ok": False, "message": "source and device_id required."}, status_code=status.HTTP_400_BAD_REQUEST)

    astore = _analytical_store(request)
    if astore is None:
        return JSONResponse({"ok": True, "trends": []})

    trends = astore.get_trend_snapshots(source, device_id)

    # Enrich with time-to-threshold using live value + alert rules
    live_device = next(
        (d for d in _sensor_store(request).live_devices()
         if d.get("source") == source and d.get("device_id") == device_id),
        None,
    )
    live_values: dict[str, float] = {}
    if live_device:
        for mname, m in (live_device.get("metrics") or {}).items():
            v = m.get("value")
            if v is not None and isinstance(v, (int, float)):
                live_values[mname] = float(v)

    alert_rules = astore.get_alert_rules(source=source, device_id=device_id, enabled_only=True)

    from analytics_engine.analytics.trends import enrich_with_ttt
    trends = enrich_with_ttt(trends, live_values, alert_rules)

    return JSONResponse({"ok": True, "trends": trends})


@router.post("/api/network/wifi/scan")
async def scan_wifi_networks(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    success, response = _run_networkctl_command("scan-wifi", "wlan0", timeout=30)
    response["ok"] = success and bool(response.get("ok", True))
    status_code = status.HTTP_200_OK if response["ok"] else status.HTTP_500_INTERNAL_SERVER_ERROR
    return JSONResponse(response, status_code=status_code)


# ══════════════════════════════════════════════════════════════════════════════
#  CSV Export
# ══════════════════════════════════════════════════════════════════════════════

_CSV_WINDOW_MAP: dict[str, int] = {"1h": 1, "6h": 6, "24h": 24, "7d": 168}


@router.get("/api/insights/export/csv")
async def export_csv(request: Request):
    """
    Download sensor data as a CSV file.

    Query params:
      source     — e.g. rs232, modbus_tcp
      device_id  — e.g. rs232_port_0, emulator_tcp
      metrics    — comma-separated metric names, or "all"
      window     — 1h | 6h | 24h | 7d  (default 24h)
      name       — optional human-readable device name (used in filename)
    """
    if not _is_authenticated(request):
        return JSONResponse(
            {"ok": False, "message": "Authentication required."},
            status_code=status.HTTP_401_UNAUTHORIZED,
        )

    source    = request.query_params.get("source", "").strip()
    device_id = request.query_params.get("device_id", "").strip()
    metrics_q = request.query_params.get("metrics", "all").strip()
    window    = request.query_params.get("window", "24h").strip()
    dev_name  = request.query_params.get("name", device_id).strip()

    if not (source and device_id):
        return JSONResponse(
            {"ok": False, "message": "source and device_id are required."},
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    window_h = _CSV_WINDOW_MAP.get(window, 24)

    # Determine which metrics to export
    if metrics_q == "all" or not metrics_q:
        # Discover from live device or configured list
        live = next(
            (d for d in _sensor_store(request).live_devices()
             if d.get("source") == source and d.get("device_id") == device_id),
            None,
        )
        metric_names = list((live or {}).get("metrics", {}).keys()) if live else []
    else:
        metric_names = [m.strip() for m in metrics_q.split(",") if m.strip()]

    if not metric_names:
        return JSONResponse(
            {"ok": False, "message": "No metrics found for this device."},
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    store    = _sensor_store(request)
    now_ms   = int(_time.time() * 1000)
    since_ms = now_ms - window_h * 3_600_000

    # Fetch raw sample rows from pes.db for each metric, then merge on timestamp
    # We query all metrics in one pass by reading sensor_samples directly.
    rows_by_ts: dict[int, dict] = {}

    conn = store._db()
    if conn is not None:
        try:
            placeholders = ",".join("?" * len(metric_names))
            cur = conn.execute(
                f"""
                SELECT timestamp_ms, metric, value, quality
                FROM sensor_samples
                WHERE source      = ?
                  AND device_id   = ?
                  AND metric      IN ({placeholders})
                  AND timestamp_ms > ?
                ORDER BY timestamp_ms ASC
                """,
                [source, device_id, *metric_names, since_ms],
            )
            for r in cur.fetchall():
                ts = r["timestamp_ms"]
                if ts not in rows_by_ts:
                    rows_by_ts[ts] = {}
                rows_by_ts[ts][r["metric"]] = r["value"]
        except Exception as exc:
            logger.warning("CSV export: query failed: %s", exc)
        finally:
            conn.close()

    # Build CSV in memory
    buf = io.StringIO()
    writer = csv.writer(buf)

    # Header
    writer.writerow(["timestamp_ms", "datetime_utc"] + metric_names)

    # Data rows
    import datetime as _dt
    for ts_ms in sorted(rows_by_ts):
        dt_str = _dt.datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
        row_data = rows_by_ts[ts_ms]
        writer.writerow([ts_ms, dt_str] + [row_data.get(m, "") for m in metric_names])

    csv_bytes = buf.getvalue().encode("utf-8")
    safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in dev_name)
    date_str  = _dt.datetime.utcnow().strftime("%Y-%m-%d")
    filename  = f"{safe_name}_{window}_{date_str}.csv"

    logger.info(
        "CSV export  user=%s  device=%s/%s  window=%s  metrics=%d  rows=%d",
        _session_user(request), source, device_id, window, len(metric_names), len(rows_by_ts),
    )

    from fastapi.responses import Response
    return Response(
        content=csv_bytes,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
