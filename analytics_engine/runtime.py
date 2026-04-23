from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Event, Thread
from time import sleep

from utils.led import toggle_led


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
                self.tick_fn()
            sleep(self.interval_seconds)
        self.status = "stopped"


class AnalyticsRuntime:
    """Owns long-running engine services outside the webpage layer."""

    def __init__(self) -> None:
        self._stop_event = Event()
        self._workers = [
            BackgroundWorker(name="analytics-loop", interval_seconds=1.0, tick_fn=toggle_led),
            BackgroundWorker(name="config-sync", interval_seconds=5.0),
            BackgroundWorker(name="health-rollup", interval_seconds=10.0),
        ]
        self._started = False

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
            "worker_count": len(self._workers),
            "workers": [
                {"name": worker.name, "status": worker.status}
                for worker in self._workers
            ],
        }
