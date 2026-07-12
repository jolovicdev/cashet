from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass
from typing import Any


@dataclass
class SQLiteLockState:
    thread_lock: threading.Lock
    file_lock: Any


SQLITE_LOCKS: dict[str, SQLiteLockState] = {}
SQLITE_LOCKS_GUARD = threading.Lock()


def sqlite_lock_state(lock_path: str) -> SQLiteLockState:
    with SQLITE_LOCKS_GUARD:
        state = SQLITE_LOCKS.get(lock_path)
        if state is None:
            from filelock import FileLock

            state = SQLiteLockState(
                thread_lock=threading.Lock(),
                file_lock=FileLock(lock_path, timeout=30, thread_local=False),
            )
            SQLITE_LOCKS[lock_path] = state
        return state


class SQLiteFingerprintLock:
    def __init__(self, lock_path: str) -> None:
        self._state = sqlite_lock_state(lock_path)

    async def __aenter__(self) -> None:
        await asyncio.to_thread(self._state.thread_lock.acquire)
        try:
            await asyncio.to_thread(self._state.file_lock.acquire)
        except Exception:
            self._state.thread_lock.release()
            raise

    async def __aexit__(self, *args: Any) -> None:
        try:
            await asyncio.to_thread(self._state.file_lock.release)
        finally:
            self._state.thread_lock.release()
