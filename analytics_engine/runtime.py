from __future__ import annotations

import logging
import time as _time
from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Event, Thread
from time import sleep
from typing import TYPE_CHECKING

from utils.led import toggle_led

if TYPE_CHECKING:
    from analytics_engine.sensor_store import SensorStore
    from analytics_engine.analytics.continuity import ContinuityState
    from analytics_engine.analytics.rules import RulesEngine
    from analytics_engine.analytics.stats import StatsEngine
    from analytics_engine.analytics.trends import TrendsEngine

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class BackgroundWorker:
    name: str
    interval_seconds: float
    tick_fn: Callable[[], None] | None = None
    status: str = "idle"
    _thread: Thread | None = field(default=None, init=False, repr=False)

    def start(self, stop_event: Event) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = Thread(
            target=self._run,
            args=(stop_event,),
            name=self.name,
            daemon=True,
        )
        self._thread.start()

    def _run(self, stop_event: Event) -> None:
        self.status = "running"
        _slow_threshold = max(self.interval_seconds * 0.9, 2.0)
        _consecutive_errors = 0

        while not stop_event.is_set():
            if self.tick_fn:
                t0 = _time.monotonic()
                try:
                    self.tick_fn()
                    _consecutive_errors = 0
                    elapsed = _time.monotonic() - t0
                    if elapsed > _slow_threshold:
                        logger.warning(
                            "Worker '%s' tick took %.2fs (interval=%.1fs) — consider tuning",
                            self.name, elapsed, self.interval_seconds,
                        )
                except Exception as exc:
                    _consecutive_errors += 1
                    # Escalate: first failure = WARNING, repeated = ERROR every 10
                    if _consecutive_errors == 1:
                        logger.warning(
                            "Worker '%s' tick error (1st): %s", self.name, exc, exc_info=True,
                        )
                    elif _consecutive_errors % 10 == 0:
                        logger.error(
                            "Worker '%s' tick error (%d consecutive): %s",
                            self.name, _consecutive_errors, exc,
                        )
            sleep(self.interval_seconds)
        self.status = "stopped"
        logger.info("Worker '%s' stopped", self.name)


class AnalyticsRuntime:
    """Owns long-running engine services outside the webpage layer."""

    def __init__(
        self,
        sensor_store:     "SensorStore | None"     = None,
        continuity_state: "ContinuityState | None" = None,
        rules_engine:     "RulesEngine | None"     = None,
        stats_engine:     "StatsEngine | None"     = None,
        trends_engine:    "TrendsEngine | None"    = None,
    ) -> None:
        self._stop_event       = Event()
        self._sensor_store     = sensor_store
        self._continuity_state = continuity_state
        self._rules_engine     = rules_engine
        self._stats_engine     = stats_engine
        self._trends_engine    = trends_engine

        self._workers = [
            BackgroundWorker(name="analytics-loop",  interval_seconds=1.0,  tick_fn=toggle_led),
            BackgroundWorker(name="config-sync",     interval_seconds=5.0),
            BackgroundWorker(name="health-rollup",   interval_seconds=10.0),
            BackgroundWorker(
                name="sensor-analytics",
                interval_seconds=5.0,
                tick_fn=self._sensor_analytics_tick,
            ),
        ]
        self._started   = False
        self._tick_n    = 0
        self._no_device_ticks = 0   # consecutive ticks with zero live devices

    def _sensor_analytics_tick(self) -> None:
        if self._sensor_store is None or self._continuity_state is None:
            return

        self._tick_n += 1
        devices = self._sensor_store.live_devices()

        if not devices:
            self._no_device_ticks += 1
            # Warn once at 12 ticks (~60 s), then every 60 ticks (~5 min)
            if self._no_device_ticks == 12 or self._no_device_ticks % 60 == 0:
                logger.warning(
                    "sensor-analytics: no live devices from Redis for %d consecutive ticks "
                    "(~%ds) — PES running? Redis reachable?",
                    self._no_device_ticks,
                    self._no_device_ticks * 5,
                )
        else:
            if self._no_device_ticks >= 12:
                logger.info(
                    "sensor-analytics: devices resumed after %d empty ticks (~%ds)",
                    self._no_device_ticks, self._no_device_ticks * 5,
                )
            self._no_device_ticks = 0

        # Periodic heartbeat every 5 min (60 ticks) — confirms engine is alive in field logs
        if self._tick_n % 60 == 0:
            anomaly_count = self._continuity_state.anomaly_count() if self._continuity_state else 0
            logger.info(
                "sensor-analytics heartbeat  tick=%d  devices=%d  anomalies=%d",
                self._tick_n, len(devices), anomaly_count,
            )

        self._continuity_state.update(devices)
        if self._rules_engine is not None:
            self._rules_engine.tick(devices)
        if self._stats_engine is not None:
            self._stats_engine.tick(devices)
        if self._trends_engine is not None:
            self._trends_engine.tick(devices)

    def register_worker(
        self,
        name: str,
        interval_seconds: float,
        tick_fn: "Callable[[], None]",
    ) -> None:
        """
        Register an additional background worker.
        Must be called BEFORE start(). Safe to call multiple times.
        """
        if self._started:
            logger.warning(
                "register_worker(%s): runtime already started — worker will not run",
                name,
            )
            return
        self._workers.append(
            BackgroundWorker(name=name, interval_seconds=interval_seconds, tick_fn=tick_fn)
        )

    def start(self) -> None:
        if self._started:
            return
        self._stop_event.clear()
        for worker in self._workers:
            worker.start(self._stop_event)
        self._started = True

    def stop(self) -> None:
        if not self._started:
            return
        self._stop_event.set()
        self._started = False

    def snapshot(self) -> dict[str, object]:
        return {
            "runtime_state": "running" if self._started else "idle",
            "worker_count":  len(self._workers),
            "workers": [
                {"name": w.name, "status": w.status}
                for w in self._workers
            ],
        }
