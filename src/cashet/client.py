from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Any

from cashet.dag import ResultRef
from cashet.executor import LocalExecutor
from cashet.hashing import PickleSerializer, Serializer, build_task_def
from cashet.models import Commit, TaskStatus
from cashet.protocols import Executor, Store
from cashet.store import SQLiteStore

_DEFAULT_STORE_DIR = ".cashet"
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
                Path(store_dir) if store_dir is not None else Path.cwd() / _DEFAULT_STORE_DIR
            )
        else:
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
    ) -> Callable[..., Any]:
        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            task_name = name or fn.__qualname__
            fn._cashet_cache = cache  # pyright: ignore[reportFunctionMemberAccess]
            fn._cashet_name = task_name  # pyright: ignore[reportFunctionMemberAccess]
            fn._cashet_tags = tags or {}  # pyright: ignore[reportFunctionMemberAccess]
            self._registered_tasks[task_name] = fn

            @wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> ResultRef:
                return self.submit(fn, *args, **kwargs)

            wrapper._cashet_wrapped_func = fn  # pyright: ignore[reportAttributeAccessIssue]
            wrapper._cashet_cache = cache  # pyright: ignore[reportAttributeAccessIssue]
            wrapper._cashet_name = task_name  # pyright: ignore[reportAttributeAccessIssue]
            wrapper._cashet_tags = tags or {}  # pyright: ignore[reportAttributeAccessIssue]
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
        **kwargs: Any,
    ) -> ResultRef:
        raw_func = getattr(func, "_cashet_wrapped_func", func)
        cache = _cache if _cache is not None else getattr(raw_func, "_cashet_cache", True)
        tags = _tags if _tags is not None else getattr(raw_func, "_cashet_tags", {})
        task_def = build_task_def(raw_func, args, kwargs, cache=cache, tags=tags)
        commit, was_cached = self._executor.submit(
            raw_func, args, kwargs, task_def, self.store, self.serializer
        )
        if commit.output_ref is None:
            raise RuntimeError(f"Task {commit.task_def.func_name} failed: {commit.error}")
        ref = ResultRef(commit.output_ref, self.store, self.serializer)
        if was_cached:
            object.__setattr__(ref, "_value", _load_result(commit, self.store, self.serializer))
            object.__setattr__(ref, "_loaded", True)
        return ref

    def log(
        self,
        func_name: str | None = None,
        limit: int = 50,
        status: TaskStatus | None = None,
        tags: dict[str, str | None] | None = None,
    ) -> list[Commit]:
        return self.store.list_commits(func_name=func_name, limit=limit, status=status, tags=tags)

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
        ref = ResultRef(commit.output_ref, self.store, self.serializer)
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
