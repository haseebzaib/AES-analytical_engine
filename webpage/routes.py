from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates


router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))


@router.get("/", response_class=HTMLResponse)
async def login_page(request: Request) -> HTMLResponse:
    runtime = getattr(request.app.state, "runtime", None)
    runtime_snapshot = runtime.snapshot() if runtime else {}

    return templates.TemplateResponse(
        request,
        "login.html",
        {
            "product_name": "MetaCrust Edge Gateway",
            "page_title": "Secure Access",
            "hero_metrics": [
                {"label": "Runtime state", "value": str(runtime_snapshot.get("runtime_state", "idle")).title()},
                {"label": "Workers online", "value": str(runtime_snapshot.get("worker_count", 0))},
                {"label": "Gateway modes", "value": "2"},
            ],
        },
    )
