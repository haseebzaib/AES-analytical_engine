from contextlib import asynccontextmanager
import logging
import logging.handlers
import os
import secrets
from pathlib import Path

from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware
import uvicorn


# ── Logging setup ─────────────────────────────────────────────────────────────
def _setup_logging(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "aes.log"

    fmt = logging.Formatter(
        fmt="%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console — INFO and above
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)

    # Rotating file — DEBUG and above (10 MB × 5 files)
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)

    root = logging.getLogger()
    root.handlers.clear()          # remove any pre-existing handlers (prevents duplication)
    root.setLevel(logging.DEBUG)
    root.addHandler(console)
    root.addHandler(file_handler)

    # Silence noisy third-party loggers
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("fastapi").setLevel(logging.WARNING)
    logging.getLogger("multipart").setLevel(logging.WARNING)


class _PollingEndpointFilter(logging.Filter):
    """Keep high-frequency polling endpoints out of the access log."""
    _SKIP = frozenset([
        "/api/network/state",
        "/api/network/apply-result",
        "/api/system/metrics",
        "/api/insights/live",
        "/api/insights/summary",
    ])

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return not any(ep in msg for ep in self._SKIP)


# ── Paths ─────────────────────────────────────────────────────────────────────
gateway_root        = Path(os.environ.get("METACRUST_GATEWAY_ROOT", "/opt/gateway"))
storage_root        = Path(os.environ.get("METACRUST_STORAGE_ROOT", str(gateway_root / "software_storage")))
pes_db_path         = Path(os.environ.get("PES_DB_PATH",         str(storage_root / "PES" / "pes.db")))
analytical_db_path  = Path(os.environ.get("AES_ANALYTICAL_DB",   str(storage_root / "AES" / "analytical.db")))
log_dir             = Path(os.environ.get("AES_LOG_DIR",          str(gateway_root  / "logs")))

_setup_logging(log_dir)
logging.getLogger("uvicorn.access").addFilter(_PollingEndpointFilter())

logger = logging.getLogger(__name__)

# ── Imports after logging is configured ───────────────────────────────────────
from analytics_engine.network_settings_store import NetworkSettingsStore
from analytics_engine.interfaces.rs232_config_store import Rs232ConfigStore
from analytics_engine.interfaces.rs485_config_store import Rs485ConfigStore
from analytics_engine.interfaces.modbus_tcp_config_store import ModbusTcpConfigStore
from analytics_engine.interfaces.forwarding_config_store import ForwardingConfigStore
from analytics_engine.sensor_store import SensorStore
from analytics_engine.analytical_store import AnalyticalStore
from analytics_engine.archival_job import ArchivalJob
from analytics_engine.analytics.continuity import ContinuityState
from analytics_engine.analytics.rules import RulesEngine
from analytics_engine.analytics.stats import StatsEngine
from analytics_engine.analytics.trends import TrendsEngine
from utils.redis_client import RedisClient as RedisNotifier
from analytics_engine.runtime import AnalyticsRuntime
from analytics_engine.settings_store import SettingsStore
from analytics_engine.system_metrics_store import SystemMetricsStore
from webpage.app import configure_webpage

# ── Store instances ───────────────────────────────────────────────────────────
settings_store          = SettingsStore(storage_root)
network_settings_store  = NetworkSettingsStore(gateway_root=gateway_root, storage_root=storage_root)
system_metrics_store    = SystemMetricsStore(gateway_root=gateway_root)
rs232_config_store      = Rs232ConfigStore(storage_root=storage_root)
rs485_config_store      = Rs485ConfigStore(storage_root=storage_root)
modbus_tcp_config_store   = ModbusTcpConfigStore(storage_root=storage_root)
forwarding_config_store   = ForwardingConfigStore(storage_root=storage_root)
redis_notifier          = RedisNotifier()
sensor_store            = SensorStore(redis_notifier, db_path=pes_db_path)
analytical_store        = AnalyticalStore(analytical_db_path)
continuity_state        = ContinuityState()
rules_engine            = RulesEngine(analytical_store)
stats_engine            = StatsEngine(sensor_store, analytical_store)
trends_engine           = TrendsEngine(sensor_store, analytical_store)
runtime                 = AnalyticsRuntime(
    sensor_store     = sensor_store,
    continuity_state = continuity_state,
    rules_engine     = rules_engine,
    stats_engine     = stats_engine,
    trends_engine    = trends_engine,
)

# Register the archival worker (every 5 minutes) before runtime.start()
_archival_job = ArchivalJob(pes_db_path=pes_db_path, analytical_store=analytical_store)
runtime.register_worker("archival", interval_seconds=300.0, tick_fn=_archival_job.tick)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("━━━ MetaCrust AES starting ━━━")
    logger.info("  gateway_root     : %s", gateway_root)
    logger.info("  storage_root     : %s", storage_root)
    logger.info("  pes_db_path      : %s  (exists=%s)", pes_db_path, pes_db_path.exists())
    logger.info("  analytical_db    : %s  (exists=%s)", analytical_db_path, analytical_db_path.exists())
    logger.info("  log_dir          : %s", log_dir)

    app.state.session_nonce = secrets.token_urlsafe(16)

    network_settings_store.ensure_initialized()
    rs232_config_store.ensure_initialized()
    rs485_config_store.ensure_initialized()
    modbus_tcp_config_store.ensure_initialized()
    forwarding_config_store.ensure_initialized()

    redis_ok = redis_notifier.ping()
    logger.info("  redis        : %s", "OK" if redis_ok else "UNREACHABLE — sensor data will be empty")

    runtime.start()
    logger.info("  workers      : started (%d)", len(runtime._workers))
    logger.info("━━━ AES ready ━━━")

    app.state.runtime                 = runtime
    app.state.gateway_root            = gateway_root
    app.state.settings_store          = settings_store
    app.state.network_settings_store  = network_settings_store
    app.state.system_metrics_store    = system_metrics_store
    app.state.rs232_config_store      = rs232_config_store
    app.state.rs485_config_store      = rs485_config_store
    app.state.modbus_tcp_config_store   = modbus_tcp_config_store
    app.state.forwarding_config_store   = forwarding_config_store
    app.state.redis_notifier          = redis_notifier
    app.state.sensor_store            = sensor_store
    app.state.analytical_store        = analytical_store
    app.state.continuity_state        = continuity_state
    app.state.rules_engine            = rules_engine
    app.state.stats_engine            = stats_engine
    app.state.trends_engine           = trends_engine
    try:
        yield
    finally:
        runtime.stop()
        logger.info("━━━ AES stopped ━━━")


app = FastAPI(title="MetaCrust Edge Gateway", lifespan=lifespan)
app.add_middleware(SessionMiddleware, secret_key="metacrust-edge-gateway-dev-session-key")
configure_webpage(app)


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, log_config=None)
