"""Single-threaded in-process worker for queued API jobs."""

from __future__ import annotations

from queue import Queue
from threading import Event, Thread
from typing import Callable


class JobWorker:
    """Runs queued jobs sequentially in a background thread."""

    def __init__(self) -> None:
        self._queue: Queue[Callable[[], None] | None] = Queue()
        self._stop_event = Event()
        self._thread = Thread(target=self._run, name="finance-cli-api-worker", daemon=True)

    def start(self) -> None:
        if not self._thread.is_alive():
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._queue.put(None)
        self._thread.join(timeout=5)

    def submit(self, task: Callable[[], None]) -> None:
        self._queue.put(task)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            task = self._queue.get()
            if task is None:
                break
            task()
