from __future__ import annotations

import asyncio
import threading
from typing import Any


class BlockingAsyncRunner:
    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._started = threading.Event()

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        self._started.set()
        try:
            loop.run_forever()
        finally:
            loop.close()

    def _ensure_started(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._started.clear()
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
            self._started.wait()

    def call(self, coro: Any) -> Any:
        self._ensure_started()
        assert self._loop is not None
        if self._thread is not None and threading.current_thread() is self._thread:
            raise RuntimeError(
                "BlockingAsyncRunner.call() invoked from its own event-loop thread; "
                "this would deadlock. Use the async API directly instead."
            )
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()

    def close(self) -> None:
        with self._lock:
            if self._loop is None:
                return
            if self._loop.is_running():
                self._loop.call_soon_threadsafe(self._loop.stop)
            thread = self._thread
            self._thread = None
            self._loop = None
        if thread is not None:
            thread.join(timeout=5)
