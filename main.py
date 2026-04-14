from contextlib import asynccontextmanager
import os
import secrets
from pathlib import Path

from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware
import uvicorn

from analytics_engine.runtime import AnalyticsRuntime
from analytics_engine.settings_store import SettingsStore
from webpage.app import configure_webpage


runtime = AnalyticsRuntime()
storage_root = Path(os.environ.get("METACRUST_STORAGE_ROOT", "/opt/gateway/software_storage"))
settings_store = SettingsStore(storage_root)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.session_nonce = secrets.token_urlsafe(16)
    runtime.start()
    app.state.runtime = runtime
    app.state.settings_store = settings_store
    try:
        yield
    finally:
        runtime.stop()


app = FastAPI(title="MetaCrust Edge Gateway", lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key="metacrust-edge-gateway-dev-session-key")
configure_webpage(app)


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
