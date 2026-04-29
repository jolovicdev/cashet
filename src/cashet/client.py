from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable, Iterable, Mapping
from datetime import timedelta
from functools import wraps
from pathlib import Path
from typing import Any, TypeVar, cast, overload

from cashet._batch import (
    build_deps,
    normalize_tasks,
    topological_sort,
    unpack_dict_tasks,
    unpack_list_tasks,
)
from cashet._client_base import (
    resolve_store_dir,
    set_task_metadata,
)
from cashet._runner import BlockingAsyncRunner
from cashet.adapters import SyncStoreAdapter
from cashet.async_client import AsyncClient
from cashet.async_executor import AsyncLocalExecutor
from cashet.dag import ResultRef
from cashet.executor import LocalExecutor
from cashet.hashing import Serializer
from cashet.models import Commit, TaskStatus
from cashet.protocols import AsyncExecutor, AsyncStore, Executor, Store
from cashet.store import AsyncSQLiteStore, SQLiteStore

T = TypeVar("T")

_DEFAULT_GC_TTL_DAYS = 30


class Client:
    def __init__(
        self,
        store_dir: str | Path | None = None,
        store: Store | None = None,
        executor: Executor | None = None,
        serializer: Serializer | None = None,
        max_workers: int = 1,
    ) -> None:
        self.store_dir = resolve_store_dir(store_dir, store)

        if store is not None:
            self.store = store
            if isinstance(store, SQLiteStore) or hasattr(store, "_async_store"):
                async_store = store._async_store  # pyright: ignore[reportPrivateUsage, reportAttributeAccessIssue]
                self._runner = getattr(store, "_runner", None) or BlockingAsyncRunner()
            else:
                async_store = SyncStoreAdapter(store)
                self._runner = BlockingAsyncRunner()
        else:
            self._runner = BlockingAsyncRunner()
            async_store = AsyncSQLiteStore(self.store_dir)
            self.store = SQLiteStore.from_async(async_store, runner=self._runner)

        async_executor: AsyncExecutor
        if executor is not None:
            self._executor = executor
            if hasattr(executor, "_async_executor"):
                async_executor = executor._async_executor  # pyright: ignore[reportPrivateUsage, reportAttributeAccessIssue]
                executor_runner = getattr(executor, "_runner", None)
                if executor_runner is not None:
                    self._runner = executor_runner
            elif inspect.iscoroutinefunction(getattr(executor, "submit", None)):
                async_executor = executor  # type: ignore[assignment]
            else:
                async_executor = _SyncExecutorAdapter(executor, self._runner, self.store)
        else:
            async_executor = AsyncLocalExecutor()
            self._executor = LocalExecutor.from_async(async_executor, runner=self._runner)

        self._async_client = AsyncClient(
            store_dir=store_dir,
            store=async_store,
            executor=async_executor,
            serializer=serializer,
            max_workers=max_workers,
        )
        self.serializer = self._async_client.serializer
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
            def wrapper(*args: Any, **kwargs: Any) -> ResultRef[Any]:
                return self.submit(fn, *args, **kwargs)

            wrapper._cashet_wrapped_func = fn  # pyright: ignore[reportAttributeAccessIssue]
            set_task_metadata(wrapper, task_name, cache, tags, retries, force, timeout)
            return wrapper

        if func is not None:
            return decorator(func)
        return decorator

    def submit(
        self,
        func: Callable[..., T],
        *args: Any,
        _cache: bool | None = None,
        _tags: dict[str, str] | None = None,
        _retries: int | None = None,
        _force: bool | None = None,
        _timeout: int | float | None = None,
        **kwargs: Any,
    ) -> ResultRef[T]:
        async_ref = self._runner.call(
            self._async_client.submit(
                func,
                *args,
                _cache=_cache,
                _tags=_tags,
                _retries=_retries,
                _force=_force,
                _timeout=_timeout,
                **kwargs,
            )
        )
        return ResultRef(async_ref, self._runner)

    @overload
    def submit_many(
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
    ) -> list[ResultRef[Any]]: ...

    @overload
    def submit_many(
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
    ) -> dict[str, ResultRef[Any]]: ...

    def submit_many(
        self,
        tasks: list[Any] | dict[str, Any],
        *,
        _cache: bool | None = None,
        _tags: dict[str, str] | None = None,
        _retries: int | None = None,
        _force: bool | None = None,
        _timeout: int | float | None = None,
        max_workers: int | None = None,
    ) -> list[ResultRef[Any]] | dict[str, ResultRef[Any]]:
        is_dict = isinstance(tasks, dict)
        if is_dict:
            keys, raw_tasks = unpack_dict_tasks(tasks)
        else:
            keys, raw_tasks = unpack_list_tasks(tasks)

        key_set = set(keys)
        normalized = normalize_tasks(raw_tasks, _cache, _tags, _retries, _force, _timeout)
        deps, _task_refs = build_deps(keys, normalized, key_set)
        _order = topological_sort(deps)
        workers = max_workers if max_workers is not None else self._max_workers

        async_results = self._runner.call(
            self._async_client.submit_many(
                tasks,
                _cache=_cache,
                _tags=_tags,
                _retries=_retries,
                _force=_force,
                _timeout=_timeout,
                max_workers=workers,
            )
        )

        if is_dict:
            return cast(
                dict[str, ResultRef[Any]],
                {k: ResultRef(async_results[k], self._runner) for k in keys},
            )
        return [ResultRef(async_results[k], self._runner) for k in keys]

    def log(
        self,
        func_name: str | None = None,
        limit: int = 50,
        status: TaskStatus | str | None = None,
        tags: dict[str, str | None] | None = None,
    ) -> list[Commit]:
        return self._runner.call(self._async_client.log(func_name, limit, status, tags))

    def show(self, hash: str) -> Commit | None:
        return self._runner.call(self._async_client.show(hash))

    def history(self, hash: str) -> list[Commit]:
        return self._runner.call(self._async_client.history(hash))

    def get(self, hash: str) -> Any:
        return self._runner.call(self._async_client.get(hash))

    def diff(self, hash_a: str, hash_b: str) -> dict[str, Any]:
        return self._runner.call(self._async_client.diff(hash_a, hash_b))

    def stats(self) -> dict[str, int]:
        return self._runner.call(self._async_client.stats())

    def rm(self, hash: str) -> bool:
        return self._runner.call(self._async_client.rm(hash))

    def gc(self, older_than: timedelta | None = None, max_size_bytes: int | None = None) -> int:
        return self._runner.call(self._async_client.gc(older_than, max_size_bytes))

    def clear(self) -> int:
        return self._runner.call(self._async_client.clear())

    def map(
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
    ) -> list[ResultRef[T]]:
        async_refs = self._runner.call(
            self._async_client.map(
                func,
                items,
                *args,
                _cache=_cache,
                _tags=_tags,
                _retries=_retries,
                _force=_force,
                _timeout=_timeout,
                max_workers=max_workers,
                **kwargs,
            )
        )
        return [ResultRef(ref, self._runner) for ref in async_refs]

    def serve(
        self,
        host: str = "127.0.0.1",
        port: int = 8000,
        require_token: str | None = None,
        *,
        tasks: Mapping[str, Callable[..., Any]] | None = None,
        allow_remote_code: bool = False,
    ) -> None:
        import uvicorn

        from cashet.server import create_app

        app = create_app(
            self,
            require_token=require_token,
            tasks=tasks,
            allow_remote_code=allow_remote_code,
        )
        uvicorn.run(app, host=host, port=port)

    def export(self, path: str | Path) -> None:
        self._runner.call(self._async_client.export(path))

    def import_archive(self, path: str | Path) -> int:
        return self._runner.call(self._async_client.import_archive(path))

    def close(self) -> None:
        self._runner.call(self._async_client.close())
        self._runner.close()

    def __enter__(self) -> Client:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


class _SyncExecutorAdapter:
    def __init__(
        self, executor: Executor, runner: BlockingAsyncRunner, sync_store: Store | None = None
    ) -> None:
        self._executor = executor
        self._runner = runner
        self._sync_store = sync_store

    async def submit(
        self,
        func: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        task_def: Any,
        store: AsyncStore,
        serializer: Any,
    ) -> tuple[Commit, bool]:
        resolved_store = self._sync_store if self._sync_store is not None else cast(Store, store)
        return await asyncio.to_thread(
            self._executor.submit, func, args, kwargs, task_def, resolved_store, serializer
        )
