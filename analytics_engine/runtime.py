from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Event, Thread
from time import sleep
from typing import TYPE_CHECKING

from utils.led import toggle_led

if TYPE_CHECKING:
    from analytics_engine.sensor_store import SensorStore
    from analytics_engine.analytics.continuity import ContinuityState

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
        while not stop_event.is_set():
            if self.tick_fn:
                try:
                    self.tick_fn()
                except Exception as exc:
                    logger.debug("Worker %s tick error: %s", self.name, exc)
            sleep(self.interval_seconds)
        self.status = "stopped"


class AnalyticsRuntime:
    """Owns long-running engine services outside the webpage layer."""

    def __init__(
        self,
        sensor_store: "SensorStore | None" = None,
        continuity_state: "ContinuityState | None" = None,
    ) -> None:
        self._stop_event = Event()
        self._sensor_store = sensor_store
        self._continuity_state = continuity_state

        self._workers = [
            BackgroundWorker(name="analytics-loop",  interval_seconds=1.0,  tick_fn=toggle_led),
            BackgroundWorker(name="config-sync",     interval_seconds=5.0),
            BackgroundWorker(name="health-rollup",   interval_seconds=10.0),
            BackgroundWorker(
                name="sensor-analytics",
                interval_seconds=30.0,
                tick_fn=self._sensor_analytics_tick,
            ),
        ]
        self._started = False

    def _sensor_analytics_tick(self) -> None:
        if self._sensor_store is None or self._continuity_state is None:
            return
        devices = self._sensor_store.live_devices()
        self._continuity_state.update(devices)

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
