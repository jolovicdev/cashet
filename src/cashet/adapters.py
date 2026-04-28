from __future__ import annotations

import asyncio
from datetime import datetime

from cashet.models import Commit, ObjectRef, TaskStatus
from cashet.protocols import Store


class SyncStoreAdapter:
    def __init__(self, store: Store) -> None:
        self._store = store

    async def put_blob(self, data: bytes) -> ObjectRef:
        return await asyncio.to_thread(self._store.put_blob, data)

    async def get_blob(self, ref: ObjectRef) -> bytes:
        return await asyncio.to_thread(self._store.get_blob, ref)

    async def put_commit(self, commit: Commit) -> None:
        await asyncio.to_thread(self._store.put_commit, commit)

    async def get_commit(self, hash: str) -> Commit | None:
        return await asyncio.to_thread(self._store.get_commit, hash)

    async def find_by_fingerprint(self, fingerprint: str) -> Commit | None:
        return await asyncio.to_thread(self._store.find_by_fingerprint, fingerprint)

    async def find_running_by_fingerprint(self, fingerprint: str) -> Commit | None:
        return await asyncio.to_thread(self._store.find_running_by_fingerprint, fingerprint)

    async def list_commits(
        self,
        func_name: str | None = None,
        limit: int = 50,
        status: TaskStatus | None = None,
        tags: dict[str, str | None] | None = None,
    ) -> list[Commit]:
        return await asyncio.to_thread(
            self._store.list_commits,
            func_name=func_name,
            limit=limit,
            status=status,
            tags=tags,
        )

    async def get_history(self, hash: str) -> list[Commit]:
        return await asyncio.to_thread(self._store.get_history, hash)

    async def stats(self) -> dict[str, int]:
        return await asyncio.to_thread(self._store.stats)

    async def evict(self, older_than: datetime, max_size_bytes: int | None = None) -> int:
        return await asyncio.to_thread(self._store.evict, older_than, max_size_bytes)

    async def delete_commit(self, hash: str) -> bool:
        return await asyncio.to_thread(self._store.delete_commit, hash)

    async def close(self) -> None:
        await asyncio.to_thread(self._store.close)
