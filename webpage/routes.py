from pathlib import Path

from fastapi import APIRouter, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates


router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))
DEFAULT_USERNAME = "gateway"
DEFAULT_PASSWORD = "gateway"


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

    if username == DEFAULT_USERNAME and password == DEFAULT_PASSWORD:
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
            "primary_sections": [
                {"label": "Overview", "compact": "Over"},
                {"label": "Insights", "compact": "Info"},
                {"label": "Field Interfaces", "compact": "Field"},
                {"label": "Network Intelligence", "compact": "Net"},
                {"label": "Destinations", "compact": "Dest"},
                {"label": "Connectivity", "compact": "Conn"},
                {"label": "Security", "compact": "Sec"},
                {"label": "System", "compact": "Sys"},
            ],
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
