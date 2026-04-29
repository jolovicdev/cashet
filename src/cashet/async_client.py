from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Any, TypeVar, cast, overload

from cashet._batch import (
    build_deps,
    execute_batch,
    normalize_tasks,
    topological_sort,
    unpack_dict_tasks,
    unpack_list_tasks,
)
from cashet._client_base import (
    diff_commits,
    resolve_status,
    resolve_store_dir,
    resolve_task_config,
    set_task_metadata,
)
from cashet._export import export_store, import_store
from cashet.async_executor import AsyncLocalExecutor
from cashet.dag import AsyncResultRef
from cashet.hashing import PickleSerializer, Serializer, build_task_def, warn_default_pickle
from cashet.models import Commit, TaskError, TaskStatus
from cashet.protocols import AsyncExecutor, AsyncStore
from cashet.store import AsyncSQLiteStore

T = TypeVar("T")

_DEFAULT_GC_TTL_DAYS = 30


class AsyncClient:
    def __init__(
        self,
        store_dir: str | Path | None = None,
        store: AsyncStore | None = None,
        executor: AsyncExecutor | None = None,
        serializer: Serializer | None = None,
        max_workers: int = 1,
    ) -> None:
        self.store_dir = resolve_store_dir(store_dir, store)
        self.store: AsyncStore = store or AsyncSQLiteStore(self.store_dir)
        self._executor = executor or AsyncLocalExecutor()
        if serializer is None:
            warn_default_pickle()
        self.serializer: Serializer = serializer or PickleSerializer()
        self._registered_tasks: dict[str, Callable[..., Any]] = {}
        self._max_workers = max_workers

    def task(
        self,
        func: Callable[..., Any] | None = None,
        *,
        cache: bool = True,
        name: str | None = None,
        tags: dict[str, str] | None = None,
        retries: int = 0,
        force: bool = False,
        timeout: int | float | None = None,
    ) -> Callable[..., Any]:
        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            task_name = name or fn.__qualname__
            set_task_metadata(fn, task_name, cache, tags, retries, force, timeout)
            self._registered_tasks[task_name] = fn

            @wraps(fn)
            async def wrapper(*args: Any, **kwargs: Any) -> AsyncResultRef[Any]:
                return await self.submit(fn, *args, **kwargs)

            wrapper._cashet_wrapped_func = fn  # pyright: ignore[reportAttributeAccessIssue]
            set_task_metadata(wrapper, task_name, cache, tags, retries, force, timeout)
            return wrapper

        if func is not None:
            return decorator(func)
        return decorator

    async def submit(
        self,
        func: Callable[..., T],
        *args: Any,
        _cache: bool | None = None,
        _tags: dict[str, str] | None = None,
        _retries: int | None = None,
        _force: bool | None = None,
        _timeout: int | float | None = None,
        **kwargs: Any,
    ) -> AsyncResultRef[T]:
        raw_func, cache, tags, retries, force, timeout = resolve_task_config(
            func, _cache, _tags, _retries, _force, _timeout
        )
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
        commit, _was_cached = await self._executor.submit(
            raw_func, args, kwargs, task_def, self.store, self.serializer
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
    ) -> list[AsyncResultRef[Any]]: ...

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
    ) -> dict[str, AsyncResultRef[Any]]: ...

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
    ) -> list[AsyncResultRef[Any]] | dict[str, AsyncResultRef[Any]]:
        is_dict = isinstance(tasks, dict)
        if is_dict:
            keys, raw_tasks = unpack_dict_tasks(tasks)
        else:
            keys, raw_tasks = unpack_list_tasks(tasks)

        key_set = set(keys)
        normalized = normalize_tasks(raw_tasks, _cache, _tags, _retries, _force, _timeout)
        deps, task_refs = build_deps(keys, normalized, key_set)
        order = topological_sort(deps)
        workers = max_workers if max_workers is not None else self._max_workers
        results = await execute_batch(
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
            return cast(dict[str, AsyncResultRef[Any]], {k: results[k] for k in keys})
        return [results[k] for k in keys]

    async def log(
        self,
        func_name: str | None = None,
        limit: int = 50,
        status: TaskStatus | str | None = None,
        tags: dict[str, str | None] | None = None,
    ) -> list[Commit]:
        resolved_status = resolve_status(status)
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

    async def diff(self, hash_a: str, hash_b: str) -> dict[str, Any]:
        commit_a = await self.store.get_commit(hash_a)
        commit_b = await self.store.get_commit(hash_b)
        if commit_a is None or commit_b is None:
            raise KeyError("One or both commits not found")
        return await diff_commits(commit_a, commit_b, self.store, self.serializer)

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

    async def export(self, path: str | Path) -> None:
        await export_store(self.store, Path(path))

    async def import_archive(self, path: str | Path) -> int:
        return await import_store(self.store, Path(path))

    async def close(self) -> None:
        await self.store.close()

    async def __aenter__(self) -> AsyncClient:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    async def map(
        self,
        func: Callable[..., T],
        items: Iterable[Any],
        *args: Any,
        _cache: bool | None = None,
        _tags: dict[str, str] | None = None,
        _retries: int | None = None,
        _force: bool | None = None,
        _timeout: int | float | None = None,
        max_workers: int | None = None,
        **kwargs: Any,
    ) -> list[AsyncResultRef[T]]:
        task_list: list[Any] = [(func, (item, *args), kwargs) for item in items]
        return await self.submit_many(
            task_list,
            _cache=_cache,
            _tags=_tags,
            _retries=_retries,
            _force=_force,
            _timeout=_timeout,
            max_workers=max_workers,
        )

    async def serve(
        self,
        host: str = "127.0.0.1",
        port: int = 8000,
        require_token: str | None = None,
        *,
        tasks: Mapping[str, Callable[..., Any]] | None = None,
        allow_remote_code: bool = False,
    ) -> None:
        import uvicorn

        from cashet.server import create_async_app

        app = create_async_app(
            self,
            require_token=require_token,
            tasks=tasks,
            allow_remote_code=allow_remote_code,
        )
        config = uvicorn.Config(app, host=host, port=port, log_level="info")
        server = uvicorn.Server(config)
        await server.serve()
