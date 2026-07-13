from __future__ import annotations

import asyncio
import hashlib
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from cashet._locks import SQLiteFingerprintLock
from cashet._runner import BlockingAsyncRunner
from cashet._sqlite_core import SQLiteStoreCore
from cashet.models import Commit, ObjectRef, TaskStatus


class AsyncSQLiteStore:
    def __init__(self, root: Path) -> None:
        self._core = SQLiteStoreCore(root)
        self._write_lock = asyncio.Lock()

    def _fingerprint_lock(self, fingerprint: str) -> SQLiteFingerprintLock:
        # Striping bounds the process-global lock registry at 256 entries per
        # store instead of one per fingerprint forever. Distinct fingerprints
        # sharing a stripe only serialize their claim sections.
        stripe = hashlib.sha256(fingerprint.encode()).hexdigest()[:2]
        return SQLiteFingerprintLock(str(self._core.root / f".lock-{stripe}"))

    @property
    def root(self) -> Path:
        return self._core.root

    @property
    def objects_dir(self) -> Path:
        return self._core.objects_dir

    @property
    def db_path(self) -> Path:
        return self._core.db_path

    async def put_blob(self, data: bytes) -> ObjectRef:
        async with self._write_lock:
            return await asyncio.to_thread(self._core.put_blob, data)

    async def get_blob(self, ref: ObjectRef) -> bytes:
        return await asyncio.to_thread(self._core.get_blob, ref)

    async def put_commit(self, commit: Commit) -> None:
        async with self._write_lock:
            await asyncio.to_thread(self._core.put_commit, commit)

    async def get_commit(self, hash: str) -> Commit | None:
        return await asyncio.to_thread(self._core.get_commit, hash)

    async def find_by_fingerprint(self, fingerprint: str) -> Commit | None:
        return await asyncio.to_thread(self._core.find_by_fingerprint, fingerprint)

    async def find_running_by_fingerprint(self, fingerprint: str) -> Commit | None:
        return await asyncio.to_thread(self._core.find_running_by_fingerprint, fingerprint)

    async def list_commits(
        self,
        func_name: str | None = None,
        limit: int = 50,
        status: TaskStatus | None = None,
        tags: dict[str, str | None] | None = None,
    ) -> list[Commit]:
        return await asyncio.to_thread(
            self._core.list_commits,
            func_name=func_name,
            limit=limit,
            status=status,
            tags=tags,
        )

    async def get_history(self, hash: str) -> list[Commit]:
        return await asyncio.to_thread(self._core.get_history, hash)

    async def stats(self) -> dict[str, int]:
        return await asyncio.to_thread(self._core.stats)

    async def evict(self, older_than: datetime, max_size_bytes: int | None = None) -> int:
        async with self._write_lock:
            return await asyncio.to_thread(self._core.evict, older_than, max_size_bytes)

    async def delete_commit(self, hash: str) -> bool:
        async with self._write_lock:
            return await asyncio.to_thread(self._core.delete_commit, hash)

    async def delete_by_tags(self, tags: dict[str, str | None]) -> int:
        async with self._write_lock:
            return await asyncio.to_thread(self._core.delete_by_tags, tags)

    async def close(self) -> None:
        await asyncio.to_thread(self._core.close)


class SQLiteStore:
    def __init__(self, root: Path) -> None:
        self._async_store = AsyncSQLiteStore(root)
        self._runner = BlockingAsyncRunner()

    @classmethod
    def from_async(
        cls, async_store: AsyncSQLiteStore, *, runner: BlockingAsyncRunner | None = None
    ) -> SQLiteStore:
        instance = cls.__new__(cls)
        instance._async_store = async_store
        instance._runner = runner or BlockingAsyncRunner()
        return instance

    @property
    def root(self) -> Path:
        return self._async_store.root

    @property
    def objects_dir(self) -> Path:
        return self._async_store.objects_dir

    @property
    def db_path(self) -> Path:
        return self._async_store.db_path

    def _connect(self, *, immediate: bool = False) -> sqlite3.Connection:
        core: Any = self._async_store._core  # pyright: ignore[reportPrivateUsage]
        return core._connect(immediate=immediate)

    def blob_exists(self, hash: str) -> bool:
        core: Any = self._async_store._core  # pyright: ignore[reportPrivateUsage]
        return core.blob_exists(hash)

    def put_blob(self, data: bytes) -> ObjectRef:
        return self._runner.call(self._async_store.put_blob(data))

    def get_blob(self, ref: ObjectRef) -> bytes:
        return self._runner.call(self._async_store.get_blob(ref))

    def put_commit(self, commit: Commit) -> None:
        self._runner.call(self._async_store.put_commit(commit))

    def get_commit(self, hash: str) -> Commit | None:
        return self._runner.call(self._async_store.get_commit(hash))

    def find_by_fingerprint(self, fingerprint: str) -> Commit | None:
        return self._runner.call(self._async_store.find_by_fingerprint(fingerprint))

    def find_running_by_fingerprint(self, fingerprint: str) -> Commit | None:
        return self._runner.call(self._async_store.find_running_by_fingerprint(fingerprint))

    def list_commits(
        self,
        func_name: str | None = None,
        limit: int = 50,
        status: TaskStatus | None = None,
        tags: dict[str, str | None] | None = None,
    ) -> list[Commit]:
        return self._runner.call(
            self._async_store.list_commits(
                func_name=func_name, limit=limit, status=status, tags=tags
            )
        )

    def get_history(self, hash: str) -> list[Commit]:
        return self._runner.call(self._async_store.get_history(hash))

    def stats(self) -> dict[str, int]:
        return self._runner.call(self._async_store.stats())

    def evict(self, older_than: datetime, max_size_bytes: int | None = None) -> int:
        return self._runner.call(self._async_store.evict(older_than, max_size_bytes))

    def delete_commit(self, hash: str) -> bool:
        return self._runner.call(self._async_store.delete_commit(hash))

    def delete_by_tags(self, tags: dict[str, str | None]) -> int:
        return self._runner.call(self._async_store.delete_by_tags(tags))

    def close(self) -> None:
        self._runner.call(self._async_store.close())
        self._runner.close()
