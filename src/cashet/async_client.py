from __future__ import annotations

import asyncio
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast, overload

from cashet._batch import (
    BatchKey,
    build_deps,
    normalize_tasks,
    reverse_deps,
    topological_sort,
    unpack_dict_tasks,
    unpack_list_tasks,
)
from cashet.async_executor import AsyncLocalExecutor
from cashet.dag import AsyncResultRef
from cashet.hashing import PickleSerializer, Serializer, build_task_def
from cashet.models import Commit, TaskError, TaskStatus
from cashet.protocols import AsyncStore
from cashet.store import AsyncSQLiteStore

_DEFAULT_STORE_DIR = ".cashet"
_CASHET_DIR_ENV = "CASHET_DIR"
_DEFAULT_GC_TTL_DAYS = 30


class AsyncClient:
    def __init__(
        self,
        store_dir: str | Path | None = None,
        store: AsyncStore | None = None,
        executor: Any | None = None,
        serializer: Serializer | None = None,
    ) -> None:
        if store is not None:
            self.store: AsyncStore = store
            self.store_dir = (
                Path(store_dir)
                if store_dir is not None
                else Path(os.environ.get(_CASHET_DIR_ENV) or Path.cwd() / _DEFAULT_STORE_DIR)
            )
        else:
            if store_dir is None:
                store_dir = os.environ.get(_CASHET_DIR_ENV) or None
            if store_dir is None:
                store_dir = Path.cwd() / _DEFAULT_STORE_DIR
            self.store_dir = Path(store_dir)
            self.store = AsyncSQLiteStore(self.store_dir)
        self._executor = executor or AsyncLocalExecutor()
        if serializer is None:
            import warnings

            warnings.warn(
                "Using PickleSerializer by default — arbitrary code execution risk on "
                "untrusted cached results. Pass serializer=SafePickleSerializer() to opt in.",
                stacklevel=2,
            )
        self.serializer: Serializer = serializer or PickleSerializer()

    async def _resolve_async_refs(
        self, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        resolved_args: list[Any] = []
        for arg in args:
            if isinstance(arg, AsyncResultRef):
                resolved_args.append(await arg.load())
            else:
                resolved_args.append(arg)
        resolved_kwargs: dict[str, Any] = {}
        for key, val in kwargs.items():
            if isinstance(val, AsyncResultRef):
                resolved_kwargs[key] = await val.load()
            else:
                resolved_kwargs[key] = val
        return tuple(resolved_args), resolved_kwargs

    async def submit(
        self,
        func: Callable[..., Any],
        *args: Any,
        _cache: bool | None = None,
        _tags: dict[str, str] | None = None,
        _retries: int | None = None,
        _force: bool | None = None,
        _timeout: int | float | None = None,
        **kwargs: Any,
    ) -> AsyncResultRef:
        raw_func = getattr(func, "_cashet_wrapped_func", func)
        cache = _cache if _cache is not None else getattr(raw_func, "_cashet_cache", True)
        tags = _tags if _tags is not None else getattr(raw_func, "_cashet_tags", {})
        retries = _retries if _retries is not None else getattr(raw_func, "_cashet_retries", 0)
        force = _force if _force is not None else getattr(raw_func, "_cashet_force", False)
        timeout_seconds = (
            _timeout if _timeout is not None else getattr(raw_func, "_cashet_timeout", None)
        )
        timeout = timedelta(seconds=timeout_seconds) if timeout_seconds is not None else None
        task_def = build_task_def(
            raw_func,
            args,
            kwargs,
            cache=cache,
            tags=tags,
            retries=retries,
            force=force,
            timeout=timeout,
        )
        resolved_args, resolved_kwargs = await self._resolve_async_refs(args, kwargs)
        commit, _was_cached = await self._executor.submit(
            raw_func, resolved_args, resolved_kwargs, task_def, self.store, self.serializer
        )
        if commit.output_ref is None:
            raise TaskError(f"Task {commit.task_def.func_name} failed: {commit.error}")
        ref = AsyncResultRef(
            commit.output_ref, self.store, self.serializer, commit_hash=commit.hash
        )
        return ref

    @overload
    async def submit_many(
        self,
        tasks: list[
            Callable[..., Any]
            | tuple[Callable[..., Any], tuple[Any, ...]]
            | tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any]]
        ],
        *,
        _cache: bool | None = None,
        _tags: dict[str, str] | None = None,
        _retries: int | None = None,
        _force: bool | None = None,
        _timeout: int | float | None = None,
        max_workers: int | None = None,
    ) -> list[AsyncResultRef]: ...

    @overload
    async def submit_many(
        self,
        tasks: dict[
            str,
            Callable[..., Any]
            | tuple[Callable[..., Any], tuple[Any, ...]]
            | tuple[Callable[..., Any], tuple[Any, ...], dict[str, Any]],
        ],
        *,
        _cache: bool | None = None,
        _tags: dict[str, str] | None = None,
        _retries: int | None = None,
        _force: bool | None = None,
        _timeout: int | float | None = None,
        max_workers: int | None = None,
    ) -> dict[str, AsyncResultRef]: ...

    async def submit_many(
        self,
        tasks: list[Any] | dict[str, Any],
        *,
        _cache: bool | None = None,
        _tags: dict[str, str] | None = None,
        _retries: int | None = None,
        _force: bool | None = None,
        _timeout: int | float | None = None,
        max_workers: int | None = None,
    ) -> list[AsyncResultRef] | dict[str, AsyncResultRef]:
        is_dict = isinstance(tasks, dict)
        if is_dict:
            keys, raw_tasks = unpack_dict_tasks(tasks)
        else:
            keys, raw_tasks = unpack_list_tasks(tasks)

        key_set = set(keys)
        normalized = normalize_tasks(raw_tasks, _cache, _tags, _retries, _force, _timeout)
        deps, task_refs = build_deps(keys, normalized, key_set)
        order = topological_sort(deps)
        workers = max_workers if max_workers is not None else 1
        results = await _execute_batch_async(
            order,
            keys,
            normalized,
            task_refs,
            deps,
            self._executor,
            self.store,
            self.serializer,
            workers,
        )

        if is_dict:
            return cast(dict[str, AsyncResultRef], {k: results[k] for k in keys})
        return [results[k] for k in keys]

    async def log(
        self,
        func_name: str | None = None,
        limit: int = 50,
        status: TaskStatus | str | None = None,
        tags: dict[str, str | None] | None = None,
    ) -> list[Commit]:
        resolved_status: TaskStatus | None = None
        if isinstance(status, str):
            try:
                resolved_status = TaskStatus(status)
            except ValueError as e:
                valid = ", ".join(s.value for s in TaskStatus)
                raise ValueError(f"Invalid status '{status}'. Valid values: {valid}") from e
        else:
            resolved_status = status
        return await self.store.list_commits(
            func_name=func_name, limit=limit, status=resolved_status, tags=tags
        )

    async def show(self, hash: str) -> Commit | None:
        return await self.store.get_commit(hash)

    async def history(self, hash: str) -> list[Commit]:
        return await self.store.get_history(hash)

    async def get(self, hash: str) -> Any:
        commit = await self.store.get_commit(hash)
        if commit is None:
            raise KeyError(f"No commit found with hash {hash}")
        if commit.output_ref is None:
            raise ValueError(f"Commit {hash[:12]} has no output")
        ref = AsyncResultRef(
            commit.output_ref, self.store, self.serializer, commit_hash=commit.hash
        )
        return await ref.load()

    async def stats(self) -> dict[str, int]:
        return await self.store.stats()

    async def rm(self, hash: str) -> bool:
        return await self.store.delete_commit(hash)

    async def gc(
        self, older_than: timedelta | None = None, max_size_bytes: int | None = None
    ) -> int:
        ttl = older_than if older_than is not None else timedelta(days=_DEFAULT_GC_TTL_DAYS)
        cutoff = datetime.now(UTC) - ttl
        return await self.store.evict(cutoff, max_size_bytes=max_size_bytes)

    async def clear(self) -> int:
        return await self.gc(timedelta(days=0))

    async def close(self) -> None:
        await self.store.close()

    async def serve(self, host: str = "127.0.0.1", port: int = 8000) -> None:
        import uvicorn

        from cashet.server import create_async_app

        app = create_async_app(self)
        config = uvicorn.Config(app, host=host, port=port, log_level="info")
        server = uvicorn.Server(config)
        await server.serve()


async def _execute_batch_async(
    order: list[BatchKey],
    keys: list[BatchKey],
    normalized: list[Any],
    task_refs: dict[BatchKey, list[tuple[str, Any, BatchKey]]],
    deps: dict[BatchKey, set[BatchKey]],
    executor: Any,
    store: AsyncStore,
    serializer: Serializer,
    max_workers: int | None,
) -> dict[BatchKey, AsyncResultRef]:
    pos = {k: i for i, k in enumerate(keys)}
    results: dict[BatchKey, AsyncResultRef] = {}
    rev = reverse_deps(deps)
    in_degree = {k: len(deps[k]) for k in keys}

    async def _run_single(key: BatchKey) -> AsyncResultRef:
        func, args, kwargs, cache, tags, retries, force, timeout = normalized[pos[key]]
        hash_args = list(args)
        hash_kwargs = dict(kwargs)
        exec_args = list(args)
        exec_kwargs = dict(kwargs)
        for kind, arg_key, target_key in task_refs.get(key, []):
            target_ref = results.get(target_key)
            if target_ref is None:
                raise RuntimeError(f"TaskRef({target_key!r}) unresolved")
            loaded = await target_ref.load()
            if kind == "arg":
                hash_args[arg_key] = target_ref
                exec_args[arg_key] = loaded
            else:
                hash_kwargs[arg_key] = target_ref
                exec_kwargs[arg_key] = loaded
        task_def = build_task_def(
            func,
            tuple(hash_args),
            hash_kwargs,
            cache=cache,
            tags=tags,
            retries=retries,
            force=force,
            timeout=timeout,
        )
        commit, _was_cached = await executor.submit(
            func, tuple(exec_args), exec_kwargs, task_def, store, serializer
        )
        if commit.output_ref is None:
            raise TaskError(f"Task {commit.task_def.func_name} failed: {commit.error}")
        ref = AsyncResultRef(commit.output_ref, store, serializer, commit_hash=commit.hash)
        return ref

    if max_workers is None or max_workers == 1:
        for key in order:
            results[key] = await _run_single(key)
        return results

    tasks: dict[asyncio.Task[Any], BatchKey] = {}
    for key in keys:
        if in_degree[key] == 0:
            task = asyncio.create_task(_run_single(key))
            tasks[task] = key

    while tasks:
        done, _ = await asyncio.wait(tasks.keys(), return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            key = tasks.pop(task)
            results[key] = task.result()
            for dependent in rev[key]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    new_task = asyncio.create_task(_run_single(dependent))
                    tasks[new_task] = dependent

    return results
