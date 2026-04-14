from pathlib import Path

from fastapi import APIRouter, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from analytics_engine.settings_store import DEFAULT_USERNAME

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))
DEFAULT_PASSWORD = "gateway"


def _settings_store(request: Request):
    return request.app.state.settings_store


def _primary_sections(active_label: str) -> list[dict[str, object]]:
    items = [
        ("Overview", "Over", "/dashboard"),
        ("Insights", "Info", "#"),
        ("Field Interfaces", "Field", "#"),
        ("Network Intelligence", "Net", "#"),
        ("Destinations", "Dest", "#"),
        ("Connectivity", "Conn", "#"),
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


def _is_authenticated(request: Request) -> bool:
    return bool(
        request.session.get("authenticated")
        and request.session.get("session_nonce") == getattr(request.app.state, "session_nonce", None)
    )


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

    if _settings_store(request).verify_credentials(username, password):
        request.session["authenticated"] = True
        request.session["username"] = username
        request.session["session_nonce"] = getattr(request.app.state, "session_nonce", None)
        return JSONResponse({"ok": True, "redirect": "/dashboard"})

    return JSONResponse(
        {"ok": False, "message": "Invalid gateway credentials."},
        status_code=status.HTTP_401_UNAUTHORIZED,
    )


@router.post("/logout")
async def logout_action(request: Request) -> RedirectResponse:
    request.session.clear()
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request) -> HTMLResponse:
    if not _is_authenticated(request):
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "product_name": "MetaCrust Edge Gateway",
            "page_title": "Control Plane",
            "primary_sections": _primary_sections("Overview"),
            "status_chips": [
                {"label": "Acquisition", "value": "Healthy"},
                {"label": "Delivery", "value": "Normal"},
                {"label": "Remote Access", "value": "Enabled"},
            ],
            "connectivity_items": [
                {
                    "label": "Ethernet",
                    "state": "Connected",
                    "detail": "Primary uplink is active",
                    "tone": "active",
                },
                {
                    "label": "Wi-Fi",
                    "state": "Standby",
                    "detail": "Available for onboard setup",
                    "tone": "standby",
                },
                {
                    "label": "Remote Access",
                    "state": "Enabled",
                    "detail": "Secure remote path available",
                    "tone": "active",
                },
            ],
            "domain_cards": [
                {
                    "title": "Insights",
                    "description": "Continuity, anomalies, incidents, trends, and evidence across sensor and network data.",
                },
                {
                    "title": "Field Interfaces",
                    "description": "RS232, RS485, Modbus RTU, GPS, IMU, DI/DO, and attached field devices.",
                },
                {
                    "title": "Network Intelligence",
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


@router.get("/system", response_class=HTMLResponse)
async def system_page(request: Request) -> HTMLResponse:
    if not _is_authenticated(request):
        return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)

    wifi_profile = _settings_store(request).get_wifi_profile()
    return templates.TemplateResponse(
        request,
        "system.html",
        {
            "product_name": "MetaCrust Edge Gateway",
            "page_title": "System",
            "primary_sections": _primary_sections("System"),
            "system_tabs": [
                {"id": "access", "label": "Access", "active": True, "disabled": False},
                {"id": "wifi", "label": "Wi-Fi", "active": False, "disabled": False},
                {"id": "identity", "label": "Identity", "active": False, "disabled": True},
                {"id": "services", "label": "Services", "active": False, "disabled": True},
                {"id": "updates", "label": "Updates", "active": False, "disabled": True},
            ],
            "current_username": request.session.get("username", DEFAULT_USERNAME),
            "wifi_profile": wifi_profile,
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

    success, message = _settings_store(request).update_credentials(
        current_password=current_password,
        new_username=new_username,
        new_password=new_password,
    )
    if not success:
        return JSONResponse({"ok": False, "message": message}, status_code=status.HTTP_400_BAD_REQUEST)

    request.session["username"] = new_username
    return JSONResponse({"ok": True, "message": message})


@router.post("/api/system/wifi")
async def update_wifi(request: Request) -> JSONResponse:
    if not _is_authenticated(request):
        return JSONResponse({"ok": False, "message": "Authentication required."}, status_code=status.HTTP_401_UNAUTHORIZED)

    payload = await request.json()
    success, message = _settings_store(request).update_wifi_profile(payload)
    if not success:
        return JSONResponse({"ok": False, "message": message}, status_code=status.HTTP_400_BAD_REQUEST)

    return JSONResponse({"ok": True, "message": message})
