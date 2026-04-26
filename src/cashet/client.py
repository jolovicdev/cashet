from __future__ import annotations

import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Any, cast, overload

from cashet._batch import (
    build_deps,
    execute_batch,
    normalize_tasks,
    topological_sort,
    unpack_dict_tasks,
    unpack_list_tasks,
)
from cashet.dag import ResultRef
from cashet.executor import LocalExecutor
from cashet.hashing import PickleSerializer, Serializer, build_task_def
from cashet.models import Commit, TaskError, TaskStatus
from cashet.protocols import Executor, Store
from cashet.store import SQLiteStore

_DEFAULT_STORE_DIR = ".cashet"
_CASHET_DIR_ENV = "CASHET_DIR"
_DIFF_SIZE_LIMIT = 10 * 1024 * 1024
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
        if store is not None:
            self.store: Store = store
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
            self.store = SQLiteStore(self.store_dir)
        self._executor: Executor = executor or LocalExecutor()
        if serializer is None:
            import warnings

            warnings.warn(
                "Using PickleSerializer by default — arbitrary code execution risk on "
                "untrusted cached results. Pass serializer=SafePickleSerializer() to opt in.",
                stacklevel=2,
            )
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
            fn._cashet_cache = cache  # pyright: ignore[reportFunctionMemberAccess]
            fn._cashet_name = task_name  # pyright: ignore[reportFunctionMemberAccess]
            fn._cashet_tags = tags or {}  # pyright: ignore[reportFunctionMemberAccess]
            fn._cashet_retries = retries  # pyright: ignore[reportFunctionMemberAccess]
            fn._cashet_force = force  # pyright: ignore[reportFunctionMemberAccess]
            fn._cashet_timeout = timeout  # pyright: ignore[reportFunctionMemberAccess]
            self._registered_tasks[task_name] = fn

            @wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> ResultRef:
                return self.submit(fn, *args, **kwargs)

            wrapper._cashet_wrapped_func = fn  # pyright: ignore[reportAttributeAccessIssue]
            wrapper._cashet_cache = cache  # pyright: ignore[reportAttributeAccessIssue]
            wrapper._cashet_name = task_name  # pyright: ignore[reportAttributeAccessIssue]
            wrapper._cashet_tags = tags or {}  # pyright: ignore[reportAttributeAccessIssue]
            wrapper._cashet_retries = retries  # pyright: ignore[reportAttributeAccessIssue]
            wrapper._cashet_force = force  # pyright: ignore[reportAttributeAccessIssue]
            wrapper._cashet_timeout = timeout  # pyright: ignore[reportAttributeAccessIssue]
            return wrapper

        if func is not None:
            return decorator(func)
        return decorator

    def submit(
        self,
        func: Callable[..., Any],
        *args: Any,
        _cache: bool | None = None,
        _tags: dict[str, str] | None = None,
        _retries: int | None = None,
        _force: bool | None = None,
        _timeout: int | float | None = None,
        **kwargs: Any,
    ) -> ResultRef:
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
        commit, _was_cached = self._executor.submit(
            raw_func, args, kwargs, task_def, self.store, self.serializer
        )
        if commit.output_ref is None:
            raise TaskError(f"Task {commit.task_def.func_name} failed: {commit.error}")
        ref = ResultRef(commit.output_ref, self.store, self.serializer, commit_hash=commit.hash)
        return ref

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
    ) -> list[ResultRef]: ...

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
    ) -> dict[str, ResultRef]: ...

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
    ) -> list[ResultRef] | dict[str, ResultRef]:
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
        results = execute_batch(
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
            return cast(dict[str, ResultRef], {k: results[k] for k in keys})
        return [results[k] for k in keys]

    def log(
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
        return self.store.list_commits(
            func_name=func_name, limit=limit, status=resolved_status, tags=tags
        )

    def show(self, hash: str) -> Commit | None:
        return self.store.get_commit(hash)

    def history(self, hash: str) -> list[Commit]:
        return self.store.get_history(hash)

    def get(self, hash: str) -> Any:
        commit = self.store.get_commit(hash)
        if commit is None:
            raise KeyError(f"No commit found with hash {hash}")
        if commit.output_ref is None:
            raise ValueError(f"Commit {hash[:12]} has no output")
        ref = ResultRef(commit.output_ref, self.store, self.serializer, commit_hash=commit.hash)
        return ref.load()

    def diff(self, hash_a: str, hash_b: str) -> dict[str, Any]:
        commit_a = self.store.get_commit(hash_a)
        commit_b = self.store.get_commit(hash_b)
        if commit_a is None or commit_b is None:
            raise KeyError("One or both commits not found")
        return _diff_commits(commit_a, commit_b, self.store, self.serializer)

    def stats(self) -> dict[str, int]:
        return self.store.stats()

    def rm(self, hash: str) -> bool:
        return self.store.delete_commit(hash)

    def gc(self, older_than: timedelta | None = None, max_size_bytes: int | None = None) -> int:
        ttl = older_than if older_than is not None else timedelta(days=_DEFAULT_GC_TTL_DAYS)
        cutoff = datetime.now(UTC) - ttl
        return self.store.evict(cutoff, max_size_bytes=max_size_bytes)

    def clear(self) -> int:
        return self.gc(timedelta(days=0))

    def serve(self, host: str = "127.0.0.1", port: int = 8000) -> None:
        import uvicorn

        from cashet.server import create_app

        app = create_app(self)
        uvicorn.run(app, host=host, port=port)

    def close(self) -> None:
        self.store.close()

    def __enter__(self) -> Client:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


def _load_result(commit: Commit, store: Store, serializer: Serializer) -> Any:
    if commit.output_ref is None:
        return None
    data = store.get_blob(commit.output_ref)
    return serializer.loads(data)


def _diff_commits(a: Commit, b: Commit, store: Store, serializer: Serializer) -> dict[str, Any]:
    diff: dict[str, Any] = {
        "a": {
            "hash": a.hash[:12],
            "func": a.task_def.func_name,
            "time": a.created_at.isoformat(),
        },
        "b": {
            "hash": b.hash[:12],
            "func": b.task_def.func_name,
            "time": b.created_at.isoformat(),
        },
        "func_changed": a.task_def.func_hash != b.task_def.func_hash,
        "args_changed": a.task_def.args_hash != b.task_def.args_hash,
    }
    if a.task_def.func_source != b.task_def.func_source:
        diff["source_diff"] = {
            "a_source": a.task_def.func_source,
            "b_source": b.task_def.func_source,
        }
    if a.task_def.args_snapshot != b.task_def.args_snapshot:
        diff["args_diff"] = {
            "a_args": a.task_def.args_snapshot.decode("utf-8", errors="replace"),
            "b_args": b.task_def.args_snapshot.decode("utf-8", errors="replace"),
        }
    try:
        size_a = a.output_ref.size if a.output_ref else 0
        size_b = b.output_ref.size if b.output_ref else 0
        if size_a > _DIFF_SIZE_LIMIT or size_b > _DIFF_SIZE_LIMIT:
            diff["output_changed"] = "skipped_large_output"
            diff["a_output_size"] = size_a
            diff["b_output_size"] = size_b
        else:
            val_a = _load_result(a, store, serializer)
            val_b = _load_result(b, store, serializer)
            if val_a != val_b:
                diff["output_changed"] = True
                diff["a_output_repr"] = repr(val_a)
                diff["b_output_repr"] = repr(val_b)
            else:
                diff["output_changed"] = False
    except Exception:
        diff["output_changed"] = "unable_to_compare"
    return diff
