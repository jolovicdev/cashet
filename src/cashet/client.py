from __future__ import annotations

import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Any, cast, overload

from cashet.dag import ResultRef, TaskRef
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
        self.serializer: Serializer = serializer or PickleSerializer()
        self._registered_tasks: dict[str, Callable[..., Any]] = {}

    def task(
        self,
        func: Callable[..., Any] | None = None,
        *,
        cache: bool = True,
        name: str | None = None,
        tags: dict[str, str] | None = None,
        retries: int = 0,
    ) -> Callable[..., Any]:
        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            task_name = name or fn.__qualname__
            fn._cashet_cache = cache  # pyright: ignore[reportFunctionMemberAccess]
            fn._cashet_name = task_name  # pyright: ignore[reportFunctionMemberAccess]
            fn._cashet_tags = tags or {}  # pyright: ignore[reportFunctionMemberAccess]
            fn._cashet_retries = retries  # pyright: ignore[reportFunctionMemberAccess]
            self._registered_tasks[task_name] = fn

            @wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> ResultRef:
                return self.submit(fn, *args, **kwargs)

            wrapper._cashet_wrapped_func = fn  # pyright: ignore[reportAttributeAccessIssue]
            wrapper._cashet_cache = cache  # pyright: ignore[reportAttributeAccessIssue]
            wrapper._cashet_name = task_name  # pyright: ignore[reportAttributeAccessIssue]
            wrapper._cashet_tags = tags or {}  # pyright: ignore[reportAttributeAccessIssue]
            wrapper._cashet_retries = retries  # pyright: ignore[reportAttributeAccessIssue]
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
        **kwargs: Any,
    ) -> ResultRef:
        raw_func = getattr(func, "_cashet_wrapped_func", func)
        cache = _cache if _cache is not None else getattr(raw_func, "_cashet_cache", True)
        tags = _tags if _tags is not None else getattr(raw_func, "_cashet_tags", {})
        retries = _retries if _retries is not None else getattr(raw_func, "_cashet_retries", 0)
        task_def = build_task_def(raw_func, args, kwargs, cache=cache, tags=tags, retries=retries)
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
    ) -> dict[str, ResultRef]: ...

    def submit_many(
        self,
        tasks: list[Any] | dict[str, Any],
        *,
        _cache: bool | None = None,
        _tags: dict[str, str] | None = None,
        _retries: int | None = None,
    ) -> list[ResultRef] | dict[str, ResultRef]:
        is_dict = isinstance(tasks, dict)
        if is_dict:
            keys, raw_tasks = _unpack_dict_tasks(tasks)
        else:
            keys, raw_tasks = _unpack_list_tasks(tasks)

        key_set = set(keys)
        normalized = _normalize_tasks(raw_tasks, _cache, _tags, _retries)
        deps, task_refs = _build_deps(keys, normalized, key_set)
        order = _topological_sort(deps)
        results = _execute_batch(
            order, keys, normalized, task_refs, self._executor, self.store, self.serializer
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
        ref = ResultRef(
            commit.output_ref, self.store, self.serializer, commit_hash=commit.hash
        )
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

    def gc(self, older_than: timedelta | None = None) -> int:
        ttl = older_than if older_than is not None else timedelta(days=_DEFAULT_GC_TTL_DAYS)
        cutoff = datetime.now(UTC) - ttl
        return self.store.evict(cutoff)

    def clear(self) -> int:
        return self.gc(timedelta(days=0))

    def close(self) -> None:
        self.store.close()

    def __enter__(self) -> Client:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


_BatchKey = int | str
_NormalizedTask = tuple[Any, tuple[Any, ...], dict[str, Any], bool, dict[str, str], int]


def _unpack_dict_tasks(
    tasks: dict[str, Any],
) -> tuple[list[_BatchKey], list[Any]]:
    items = list(tasks.items())
    return [k for k, _ in items], [t for _, t in items]


def _unpack_list_tasks(tasks: list[Any]) -> tuple[list[_BatchKey], list[Any]]:
    return list(range(len(tasks))), list(tasks)


def _normalize_tasks(
    raw_tasks: list[Any],
    default_cache: bool | None,
    default_tags: dict[str, str] | None,
    default_retries: int | None = None,
) -> list[_NormalizedTask]:
    normalized: list[_NormalizedTask] = []
    for i, task in enumerate(raw_tasks):
        if callable(task):
            func, args, kwargs = task, (), {}
        elif not isinstance(task, tuple):
            raise TypeError(
                f"Task {i}: expected callable or tuple, got {type(task).__name__}"
            )
        elif len(task) == 2:
            func, args = task  # type: ignore[misc]
            kwargs = {}
        elif len(task) == 3:
            func, args, kwargs = task  # type: ignore[misc]
        else:
            raise TypeError(
                f"Task {i}: expected tuple of length 2 or 3, got length {len(task)}"
            )
        if not callable(func):
            raise TypeError(f"Task {i}: expected callable, got {type(func).__name__}")
        if not isinstance(args, tuple):
            raise TypeError(f"Task {i}: expected args as tuple, got {type(args).__name__}")
        if not isinstance(kwargs, dict):
            raise TypeError(f"Task {i}: expected kwargs as dict, got {type(kwargs).__name__}")
        raw_func = getattr(func, "_cashet_wrapped_func", func)
        cache = (
            default_cache
            if default_cache is not None
            else getattr(
                raw_func,
                "_cashet_cache",
                True,
            )
        )
        tags = (
            default_tags
            if default_tags is not None
            else getattr(
                raw_func,
                "_cashet_tags",
                {},
            )
        )
        retries = (
            default_retries
            if default_retries is not None
            else getattr(
                raw_func,
                "_cashet_retries",
                0,
            )
        )
        normalized.append((raw_func, args, kwargs, cache, tags, retries))
    return normalized


def _build_deps(
    keys: list[_BatchKey],
    normalized: list[_NormalizedTask],
    key_set: set[_BatchKey],
) -> tuple[dict[_BatchKey, set[_BatchKey]], dict[_BatchKey, list[tuple[str, Any, _BatchKey]]]]:
    deps: dict[_BatchKey, set[_BatchKey]] = {k: set() for k in keys}
    task_refs: dict[_BatchKey, list[tuple[str, Any, _BatchKey]]] = {}
    for key, (_func, args, kwargs, _cache, _tags, _retries) in zip(keys, normalized, strict=True):
        for j, arg in enumerate(args):
            if isinstance(arg, TaskRef):
                deps[key].add(arg.key)
                task_refs.setdefault(key, []).append(("arg", j, arg.key))
        for kw_key, val in kwargs.items():
            if isinstance(val, TaskRef):
                deps[key].add(val.key)
                task_refs.setdefault(key, []).append(("kwarg", kw_key, val.key))
        for d in deps[key]:
            if d not in key_set:
                raise ValueError(f"TaskRef({d!r}) not found")
            if d == key:
                raise ValueError(f"Task {key!r} cannot depend on itself")
    return deps, task_refs


def _topological_sort(deps: dict[_BatchKey, set[_BatchKey]]) -> list[_BatchKey]:
    visited: set[_BatchKey] = set()
    temp: set[_BatchKey] = set()
    order: list[_BatchKey] = []

    def visit(n: _BatchKey) -> None:
        if n in temp:
            raise ValueError("Circular dependency detected in batch tasks")
        if n in visited:
            return
        temp.add(n)
        for d in deps[n]:
            visit(d)
        temp.remove(n)
        visited.add(n)
        order.append(n)

    for n in deps:
        visit(n)
    return order


def _execute_batch(
    order: list[_BatchKey],
    keys: list[_BatchKey],
    normalized: list[_NormalizedTask],
    task_refs: dict[_BatchKey, list[tuple[str, Any, _BatchKey]]],
    executor: Executor,
    store: Store,
    serializer: Serializer,
) -> dict[_BatchKey, ResultRef]:
    pos = {k: i for i, k in enumerate(keys)}
    results: dict[_BatchKey, ResultRef] = {}
    for key in order:
        func, args, kwargs, cache, tags, retries = normalized[pos[key]]
        resolved_args = list(args)
        resolved_kwargs = dict(kwargs)
        for kind, arg_key, target_key in task_refs.get(key, []):
            target_ref = results.get(target_key)
            if target_ref is None:
                raise RuntimeError(f"TaskRef({target_key!r}) unresolved")
            if kind == "arg":
                resolved_args[arg_key] = target_ref
            else:
                resolved_kwargs[arg_key] = target_ref
        task_def = build_task_def(
            func, tuple(resolved_args), resolved_kwargs, cache=cache, tags=tags, retries=retries
        )
        commit, _was_cached = executor.submit(
            func, tuple(resolved_args), resolved_kwargs, task_def, store, serializer
        )
        if commit.output_ref is None:
            raise TaskError(f"Task {commit.task_def.func_name} failed: {commit.error}")
        ref = ResultRef(commit.output_ref, store, serializer, commit_hash=commit.hash)
        results[key] = ref
    return results


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
