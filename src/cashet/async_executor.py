from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import time
import traceback
import weakref
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Any

from cashet.dag import (
    build_commit,
    find_existing_commit,
    find_parent_hash,
    resolve_input_refs,
)
from cashet.hashing import Serializer
from cashet.models import Commit, ObjectRef, TaskDef, TaskStatus
from cashet.protocols import AsyncStore

_DEFAULT_RUNNING_TTL = timedelta(seconds=300)
_lock_cache: weakref.WeakKeyDictionary[Any, asyncio.Lock] = weakref.WeakKeyDictionary()

logger = logging.getLogger("cashet")


@contextlib.asynccontextmanager
async def _async_store_lock(
    store: AsyncStore, fingerprint: str | None = None
) -> AsyncGenerator[None, None]:
    if fingerprint is not None:
        fp_lock = getattr(store, "_fingerprint_lock", None)
        if fp_lock is not None:
            async with fp_lock(fingerprint):
                yield
            return
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
        func_name = task_def.func_name
        if not task_def.force:
            async with _async_store_lock(store, fp):
                existing = await find_existing_commit(store, task_def)
                if existing is not None:
                    existing.status = TaskStatus.CACHED
                    await store.put_commit(existing)
                    logger.info(
                        "task cached fingerprint=%s func=%s commit=%s",
                        fp,
                        func_name,
                        existing.hash[:12],
                    )
                    return existing, True

        while True:
            async with _async_store_lock(store, fp):
                if not task_def.force:
                    existing = await find_existing_commit(store, task_def)
                    if existing is not None:
                        existing.status = TaskStatus.CACHED
                        await store.put_commit(existing)
                        return existing, True

                claim = await store.find_running_by_fingerprint(task_def.fingerprint)
                if claim is not None:
                    if _is_stale_claim(claim, self._running_ttl):
                        logger.warning(
                            "stale claim reclaimed fingerprint=%s func=%s claim=%s",
                            fp,
                            func_name,
                            claim.hash[:12],
                        )
                        # Reclaimed claims must run with current task options, not stale ones.
                        claim.claimed_at = datetime.now(UTC)
                        claim.task_def.cache = task_def.cache
                        claim.task_def.retries = task_def.retries
                        claim.task_def.force = task_def.force
                        claim.task_def.timeout = task_def.timeout
                        claim.task_def.ttl = task_def.ttl
                        claim.task_def.tags = task_def.tags
                        claim.tags = task_def.tags
                        claim.expires_at = (
                            datetime.now(UTC) + task_def.ttl if task_def.ttl else None
                        )
                        await store.put_commit(claim)
                        break
                else:
                    input_refs = resolve_input_refs(args, kwargs)
                    parent_hash = await find_parent_hash(store, task_def)
                    claim = build_commit(task_def, input_refs, parent_hash=parent_hash)
                    claim.status = TaskStatus.RUNNING
                    await store.put_commit(claim)
                    logger.debug(
                        "claim acquired fingerprint=%s func=%s commit=%s",
                        fp,
                        func_name,
                        claim.hash[:12],
                    )
                    break
            await asyncio.sleep(0.1)

        exec_start = time.perf_counter()
        commit = await self._execute(func, args, kwargs, claim, store, serializer)
        exec_duration_ms = int((time.perf_counter() - exec_start) * 1000)

        async with _async_store_lock(store, fp):
            if not task_def.force:
                latest = await find_existing_commit(store, task_def)
                if latest is not None:
                    logger.info(
                        "task finished (race winner) fp=%s func=%s commit=%s duration_ms=%d",
                        fp,
                        func_name,
                        latest.hash[:12],
                        exec_duration_ms,
                    )
                    return latest, False
            await store.put_commit(commit)
            status = "completed" if commit.status == TaskStatus.COMPLETED else "failed"
            logger.info(
                "task finished fingerprint=%s func=%s commit=%s status=%s duration_ms=%d",
                fp,
                func_name,
                commit.hash[:12],
                status,
                exec_duration_ms,
            )
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

        start_time = time.perf_counter()
        try:
            retries = max(0, commit.task_def.retries)
            effective_timeout = commit.task_def.timeout or self._timeout
            for attempt in range(retries + 1):
                logger.info(
                    "task started fingerprint=%s func=%s commit=%s attempt=%d max_attempts=%d",
                    commit.task_def.fingerprint,
                    commit.task_def.func_name,
                    commit.hash[:12],
                    attempt + 1,
                    retries + 1,
                )
                try:
                    resolved_args = await self._resolve_args(args)
                    resolved_kwargs = await self._resolve_kwargs(kwargs)
                    if effective_timeout is not None:
                        result = await asyncio.wait_for(
                            asyncio.to_thread(func, *resolved_args, **resolved_kwargs),
                            timeout=effective_timeout.total_seconds(),
                        )
                    else:
                        result = await asyncio.to_thread(
                            func, *resolved_args, **resolved_kwargs
                        )
                    output_ref = await self._store_result(result, store, serializer)
                    commit.output_ref = output_ref
                    commit.status = TaskStatus.COMPLETED
                    break
                except TimeoutError:
                    if attempt < retries:
                        logger.warning(
                            "task retrying fingerprint=%s func=%s commit=%s attempt=%d error=%s",
                            commit.task_def.fingerprint,
                            commit.task_def.func_name,
                            commit.hash[:12],
                            attempt + 1,
                            "TimeoutError",
                        )
                        await asyncio.sleep(0.5 * attempt)
                        continue
                    commit.status = TaskStatus.FAILED
                    commit.error = f"TimeoutError: task exceeded {effective_timeout}"
                    break
                except Exception as e:
                    if attempt < retries:
                        logger.warning(
                            "task retrying fingerprint=%s func=%s commit=%s attempt=%d error=%s",
                            commit.task_def.fingerprint,
                            commit.task_def.func_name,
                            commit.hash[:12],
                            attempt + 1,
                            str(e),
                        )
                        await asyncio.sleep(0.5 * attempt)
                        continue
                    commit.status = TaskStatus.FAILED
                    commit.error = traceback.format_exc()
                    break
        finally:
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            if commit.status == TaskStatus.FAILED:
                logger.error(
                    "task failed fingerprint=%s func=%s commit=%s duration_ms=%d error=%s",
                    commit.task_def.fingerprint,
                    commit.task_def.func_name,
                    commit.hash[:12],
                    duration_ms,
                    commit.error,
                )
            stop_event.set()
            await asyncio.wait_for(heartbeat_task, timeout=self._running_ttl.total_seconds() * 2)

        return commit

    async def _resolve_value(self, value: Any) -> Any:
        async_load = getattr(value, "__cashet_async_load__", None)
        if async_load is not None:
            return await async_load()
        if hasattr(value, "__cashet_ref__") and hasattr(value, "load"):
            if inspect.iscoroutinefunction(value.load):
                return await value.load()
            return await asyncio.to_thread(value.load)
        return value

    async def _resolve_args(self, args: tuple[Any, ...]) -> tuple[Any, ...]:
        resolved = [await self._resolve_value(a) for a in args]
        return tuple(resolved)

    async def _resolve_kwargs(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        resolved: dict[str, Any] = {}
        for k, v in kwargs.items():
            resolved[k] = await self._resolve_value(v)
        return resolved

    async def _store_result(
        self, result: Any, store: AsyncStore, serializer: Serializer
    ) -> ObjectRef:
        data = serializer.dumps(result)
        return await store.put_blob(data)
