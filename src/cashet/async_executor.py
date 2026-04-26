from __future__ import annotations

import asyncio
import contextlib
import traceback
import weakref
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Any

from cashet.dag import (
    async_find_existing_commit,
    async_find_parent_hash,
    async_resolve_input_refs,
    build_commit,
)
from cashet.hashing import Serializer
from cashet.models import Commit, ObjectRef, TaskDef, TaskStatus
from cashet.protocols import AsyncStore

_DEFAULT_RUNNING_TTL = timedelta(seconds=300)
_lock_cache: weakref.WeakKeyDictionary[Any, asyncio.Lock] = weakref.WeakKeyDictionary()


@contextlib.asynccontextmanager
async def _async_store_lock(
    store: AsyncStore, fingerprint: str | None = None
) -> AsyncGenerator[None, None]:
    if fingerprint is not None:
        fp_lock = getattr(store, "_fingerprint_lock", None)
        if fp_lock is not None:
            async with fp_lock(fingerprint):
                yield
        else:
            cached = _lock_cache.get(store)
            if cached is None:
                cached = asyncio.Lock()
                _lock_cache[store] = cached
            async with cached:
                yield
    else:
        cached = _lock_cache.get(store)
        if cached is None:
            cached = asyncio.Lock()
            _lock_cache[store] = cached
        async with cached:
            yield


def _is_stale_claim(commit: Commit, ttl: timedelta) -> bool:
    return datetime.now(UTC) - commit.claimed_at > ttl


class AsyncLocalExecutor:
    def __init__(
        self, running_ttl: timedelta | None = None, timeout: timedelta | None = None
    ) -> None:
        self._running_ttl = running_ttl or _DEFAULT_RUNNING_TTL
        self._timeout = timeout

    async def submit(
        self,
        func: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        task_def: TaskDef,
        store: AsyncStore,
        serializer: Serializer,
    ) -> tuple[Commit, bool]:
        fp = task_def.fingerprint
        if not task_def.force:
            async with _async_store_lock(store, fp):
                existing = await async_find_existing_commit(store, task_def)
                if existing is not None:
                    existing.status = TaskStatus.CACHED
                    await store.put_commit(existing)
                    return existing, True

        while True:
            async with _async_store_lock(store, fp):
                if not task_def.force:
                    existing = await async_find_existing_commit(store, task_def)
                    if existing is not None:
                        existing.status = TaskStatus.CACHED
                        await store.put_commit(existing)
                        return existing, True

                claim = await store.find_running_by_fingerprint(task_def.fingerprint)
                if claim is not None:
                    if _is_stale_claim(claim, self._running_ttl):
                        claim.claimed_at = datetime.now(UTC)
                        claim.task_def.retries = task_def.retries
                        await store.put_commit(claim)
                        break
                else:
                    input_refs = await async_resolve_input_refs(args, kwargs)
                    parent_hash = await async_find_parent_hash(store, task_def)
                    claim = build_commit(task_def, input_refs, parent_hash=parent_hash)
                    claim.status = TaskStatus.RUNNING
                    await store.put_commit(claim)
                    break
            await asyncio.sleep(0.1)

        commit = await self._execute(func, args, kwargs, claim, store, serializer)

        async with _async_store_lock(store, fp):
            if not task_def.force:
                latest = await async_find_existing_commit(store, task_def)
                if latest is not None:
                    return latest, False
            await store.put_commit(commit)
            return commit, False

    async def _execute(
        self,
        func: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        commit: Commit,
        store: AsyncStore,
        serializer: Serializer,
    ) -> Commit:
        stop_event = asyncio.Event()

        async def _heartbeat() -> None:
            interval = self._running_ttl.total_seconds() / 2
            while True:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=interval)
                    break
                except TimeoutError:
                    pass
                if commit.status != TaskStatus.RUNNING:
                    break
                async with _async_store_lock(store, commit.task_def.fingerprint):
                    if commit.status != TaskStatus.RUNNING:
                        break
                    commit.claimed_at = datetime.now(UTC)
                    await store.put_commit(commit)

        heartbeat_task = asyncio.create_task(_heartbeat())

        try:
            retries = max(0, commit.task_def.retries)
            effective_timeout = commit.task_def.timeout or self._timeout
            for attempt in range(retries + 1):
                try:
                    resolved_args = self._resolve_args(args)
                    resolved_kwargs = self._resolve_kwargs(kwargs)
                    if effective_timeout is not None:
                        result = await asyncio.wait_for(
                            asyncio.to_thread(func, *resolved_args, **resolved_kwargs),
                            timeout=effective_timeout.total_seconds(),
                        )
                    else:
                        result = await asyncio.to_thread(func, *resolved_args, **resolved_kwargs)
                    output_ref = await self._store_result(result, store, serializer)
                    commit.output_ref = output_ref
                    commit.status = TaskStatus.COMPLETED
                    break
                except TimeoutError:
                    if attempt < retries:
                        await asyncio.sleep(0.5 * attempt)
                        continue
                    commit.status = TaskStatus.FAILED
                    commit.error = f"TimeoutError: task exceeded {effective_timeout}"
                    break
                except Exception:
                    if attempt < retries:
                        await asyncio.sleep(0.5 * attempt)
                        continue
                    commit.status = TaskStatus.FAILED
                    commit.error = traceback.format_exc()
                    break
        finally:
            stop_event.set()
            await asyncio.wait_for(heartbeat_task, timeout=self._running_ttl.total_seconds() * 2)

        return commit

    def _resolve_args(self, args: tuple[Any, ...]) -> tuple[Any, ...]:
        resolved: list[Any] = []
        for arg in args:
            if hasattr(arg, "__cashet_ref__"):
                resolved.append(arg.load())
            else:
                resolved.append(arg)
        return tuple(resolved)

    def _resolve_kwargs(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        resolved: dict[str, Any] = {}
        for key, val in kwargs.items():
            if hasattr(val, "__cashet_ref__"):
                resolved[key] = val.load()
            else:
                resolved[key] = val
        return resolved

    async def _store_result(
        self, result: Any, store: AsyncStore, serializer: Serializer
    ) -> ObjectRef:
        data = serializer.dumps(result)
        return await store.put_blob(data)
