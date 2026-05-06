import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from webpage.routes import router as webpage_router


WEBPAGE_DIR = Path(os.environ.get("AES_WEBPAGE_DIR", Path(__file__).resolve().parent))


def configure_webpage(app: FastAPI) -> None:
    app.mount("/static", StaticFiles(directory=WEBPAGE_DIR / "static"), name="static")
    app.include_router(webpage_router)
