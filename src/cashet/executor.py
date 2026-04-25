from __future__ import annotations

import concurrent.futures
import contextlib
import threading
import time
import traceback
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from filelock import FileLock

from cashet.dag import (
    build_commit,
    find_existing_commit,
    find_parent_hash,
    resolve_input_refs,
)
from cashet.hashing import Serializer
from cashet.models import Commit, ObjectRef, TaskDef, TaskStatus
from cashet.protocols import Store

_STORE_LOCKS: dict[str, threading.Lock] = {}
_STORE_LOCKS_GUARD = threading.Lock()
_FALLBACK_LOCK = threading.Lock()
_DEFAULT_RUNNING_TTL = timedelta(seconds=300)


def _get_thread_lock(store: Store) -> threading.Lock:
    root = getattr(store, "root", None)
    if root is not None:
        key = str(Path(root).resolve())
        with _STORE_LOCKS_GUARD:
            if key not in _STORE_LOCKS:
                _STORE_LOCKS[key] = threading.Lock()
            return _STORE_LOCKS[key]
    store_lock = getattr(store, "_lock", None)
    if store_lock is not None:
        return store_lock
    return _FALLBACK_LOCK


@contextlib.contextmanager
def _store_lock(store: Store) -> Generator[None, None, None]:
    tlock = _get_thread_lock(store)
    root = getattr(store, "root", None)
    if root is not None:
        lock_path = Path(root) / ".lock"
        with FileLock(str(lock_path)), tlock:
            yield
    else:
        with tlock:
            yield

def _is_stale_claim(commit: Commit, ttl: timedelta) -> bool:
    return datetime.now(UTC) - commit.claimed_at > ttl


class LocalExecutor:
    def __init__(
        self, running_ttl: timedelta | None = None, timeout: timedelta | None = None
    ) -> None:
        self._running_ttl = running_ttl or _DEFAULT_RUNNING_TTL
        self._timeout = timeout

    def submit(
        self,
        func: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        task_def: TaskDef,
        store: Store,
        serializer: Serializer,
    ) -> tuple[Commit, bool]:
        if not task_def.force:
            with _store_lock(store):
                existing = find_existing_commit(store, task_def)
                if existing is not None:
                    existing.status = TaskStatus.CACHED
                    store.put_commit(existing)
                    return existing, True

        while True:
            with _store_lock(store):
                if not task_def.force:
                    existing = find_existing_commit(store, task_def)
                    if existing is not None:
                        existing.status = TaskStatus.CACHED
                        store.put_commit(existing)
                        return existing, True

                claim = store.find_running_by_fingerprint(task_def.fingerprint)
                if claim is not None:
                    if _is_stale_claim(claim, self._running_ttl):
                        # NOTE: refreshing claimed_at before execution creates a small race —
                        # if this process crashes before _execute runs, the claim looks fresh
                        # and will block others for another TTL cycle.
                        claim.claimed_at = datetime.now(UTC)
                        claim.task_def.retries = task_def.retries
                        store.put_commit(claim)
                        break
                else:
                    input_refs = resolve_input_refs(args, kwargs)
                    parent_hash = find_parent_hash(store, task_def)
                    claim = build_commit(task_def, input_refs, parent_hash=parent_hash)
                    claim.status = TaskStatus.RUNNING
                    store.put_commit(claim)
                    break
            time.sleep(0.1)

        commit = self._execute(func, args, kwargs, claim, store, serializer)

        with _store_lock(store):
            if not task_def.force:
                latest = find_existing_commit(store, task_def)
                if latest is not None:
                    return latest, False
            store.put_commit(commit)
            return commit, False

    def _execute(
        self,
        func: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        commit: Commit,
        store: Store,
        serializer: Serializer,
    ) -> Commit:
        stop_event = threading.Event()

        def _heartbeat() -> None:
            interval = self._running_ttl.total_seconds() / 2
            while not stop_event.wait(interval):
                if commit.status != TaskStatus.RUNNING:
                    break
                with _store_lock(store):
                    if commit.status != TaskStatus.RUNNING:
                        break
                    commit.claimed_at = datetime.now(UTC)
                    store.put_commit(commit)

        heartbeat_thread = threading.Thread(target=_heartbeat, daemon=True)
        heartbeat_thread.start()

        try:
            retries = max(0, commit.task_def.retries)
            effective_timeout = commit.task_def.timeout or self._timeout
            for attempt in range(retries + 1):
                try:
                    resolved_args = self._resolve_args(args)
                    resolved_kwargs = self._resolve_kwargs(kwargs)
                    if effective_timeout is not None:
                        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
                        try:
                            future = pool.submit(func, *resolved_args, **resolved_kwargs)
                            result = future.result(timeout=effective_timeout.total_seconds())
                        finally:
                            pool.shutdown(wait=False)
                    else:
                        result = func(*resolved_args, **resolved_kwargs)
                    output_ref = self._store_result(result, store, serializer)
                    commit.output_ref = output_ref
                    commit.status = TaskStatus.COMPLETED
                    break
                except concurrent.futures.TimeoutError:
                    if attempt < retries:
                        time.sleep(0.5 * attempt)
                        continue
                    commit.status = TaskStatus.FAILED
                    commit.error = f"TimeoutError: task exceeded {effective_timeout}"
                    break
                except Exception:
                    if attempt < retries:
                        time.sleep(0.5 * attempt)
                        continue
                    commit.status = TaskStatus.FAILED
                    commit.error = traceback.format_exc()
                    break
        finally:
            stop_event.set()
            heartbeat_thread.join(timeout=self._running_ttl.total_seconds() * 2)
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

    def _store_result(self, result: Any, store: Store, serializer: Serializer) -> ObjectRef:
        data = serializer.dumps(result)
        return store.put_blob(data)
