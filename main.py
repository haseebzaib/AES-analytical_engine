from contextlib import asynccontextmanager
import logging
import os
import secrets
from pathlib import Path

from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware
import uvicorn


class _PollingEndpointFilter(logging.Filter):
    _SKIP = frozenset(["/api/network/state", "/api/network/apply-result", "/api/system/metrics"])  # substring match covers /metrics/history too

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return not any(ep in msg for ep in self._SKIP)


logging.getLogger("uvicorn.access").addFilter(_PollingEndpointFilter())

from analytics_engine.network_settings_store import NetworkSettingsStore
from analytics_engine.interfaces.rs232_config_store import Rs232ConfigStore
from analytics_engine.interfaces.rs485_config_store import Rs485ConfigStore
from analytics_engine.interfaces.modbus_tcp_config_store import ModbusTcpConfigStore
from utils.redis_client import RedisClient as RedisNotifier
from analytics_engine.runtime import AnalyticsRuntime
from analytics_engine.settings_store import SettingsStore
from analytics_engine.system_metrics_store import SystemMetricsStore
from webpage.app import configure_webpage


runtime = AnalyticsRuntime()
gateway_root = Path(os.environ.get("METACRUST_GATEWAY_ROOT", "/opt/gateway"))
storage_root = Path(os.environ.get("METACRUST_STORAGE_ROOT", str(gateway_root / "software_storage")))
settings_store = SettingsStore(storage_root)
network_settings_store = NetworkSettingsStore(gateway_root=gateway_root, storage_root=storage_root)
system_metrics_store = SystemMetricsStore(gateway_root=gateway_root)
rs232_config_store = Rs232ConfigStore(storage_root=storage_root)
rs485_config_store = Rs485ConfigStore(storage_root=storage_root)
modbus_tcp_config_store = ModbusTcpConfigStore(storage_root=storage_root)
redis_notifier = RedisNotifier()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.session_nonce = secrets.token_urlsafe(16)
    network_settings_store.ensure_initialized()
    rs232_config_store.ensure_initialized()
    rs485_config_store.ensure_initialized()
    modbus_tcp_config_store.ensure_initialized()
    runtime.start()
    app.state.runtime = runtime
    app.state.gateway_root = gateway_root
    app.state.settings_store = settings_store
    app.state.network_settings_store = network_settings_store
    app.state.system_metrics_store = system_metrics_store
    app.state.rs232_config_store = rs232_config_store
    app.state.rs485_config_store = rs485_config_store
    app.state.modbus_tcp_config_store = modbus_tcp_config_store
    app.state.redis_notifier = redis_notifier
    try:
        yield
    finally:
        runtime.stop()


app = FastAPI(title="MetaCrust Edge Gateway", lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key="metacrust-edge-gateway-dev-session-key")
configure_webpage(app)


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
