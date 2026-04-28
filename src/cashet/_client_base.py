from __future__ import annotations

import os
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import Any

from cashet.hashing import Serializer
from cashet.models import Commit, TaskStatus
from cashet.protocols import AsyncStore

_DEFAULT_STORE_DIR = ".cashet"
_CASHET_DIR_ENV = "CASHET_DIR"
_DIFF_SIZE_LIMIT = 10 * 1024 * 1024


def resolve_store_dir(
    store_dir: str | Path | None,
    store: object | None,
) -> Path:
    if store is not None:
        return (
            Path(store_dir)
            if store_dir is not None
            else Path(os.environ.get(_CASHET_DIR_ENV) or Path.cwd() / _DEFAULT_STORE_DIR)
        )
    if store_dir is None:
        store_dir = os.environ.get(_CASHET_DIR_ENV) or None
    if store_dir is None:
        store_dir = Path.cwd() / _DEFAULT_STORE_DIR
    return Path(store_dir)


def resolve_task_config(
    func: Callable[..., Any],
    _cache: bool | None,
    _tags: dict[str, str] | None,
    _retries: int | None,
    _force: bool | None,
    _timeout: int | float | None,
) -> tuple[Any, bool, dict[str, str], int, bool, timedelta | None]:
    raw_func = getattr(func, "_cashet_wrapped_func", func)
    cache = _cache if _cache is not None else getattr(raw_func, "_cashet_cache", True)
    tags = _tags if _tags is not None else getattr(raw_func, "_cashet_tags", {})
    retries = _retries if _retries is not None else getattr(raw_func, "_cashet_retries", 0)
    force = _force if _force is not None else getattr(raw_func, "_cashet_force", False)
    timeout_seconds = (
        _timeout if _timeout is not None else getattr(raw_func, "_cashet_timeout", None)
    )
    timeout = timedelta(seconds=timeout_seconds) if timeout_seconds is not None else None
    return raw_func, cache, tags, retries, force, timeout


def set_task_metadata(
    func: Callable[..., Any],
    task_name: str,
    cache: bool,
    tags: dict[str, str] | None,
    retries: int,
    force: bool,
    timeout: int | float | None,
) -> None:
    target: Any = func
    target._cashet_cache = cache
    target._cashet_name = task_name
    target._cashet_tags = tags or {}
    target._cashet_retries = retries
    target._cashet_force = force
    target._cashet_timeout = timeout


def resolve_status(status: TaskStatus | str | None) -> TaskStatus | None:
    if isinstance(status, str):
        try:
            return TaskStatus(status)
        except ValueError as e:
            valid = ", ".join(s.value for s in TaskStatus)
            raise ValueError(f"Invalid status '{status}'. Valid values: {valid}") from e
    return status


async def load_result(commit: Commit, store: AsyncStore, serializer: Serializer) -> Any:
    if commit.output_ref is None:
        return None
    data = await store.get_blob(commit.output_ref)
    return serializer.loads(data)


def _diff_base(a: Commit, b: Commit) -> dict[str, Any]:
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
    return diff


def _output_sizes(a: Commit, b: Commit) -> tuple[int, int]:
    return a.output_ref.size if a.output_ref else 0, b.output_ref.size if b.output_ref else 0


def _record_large_output(diff: dict[str, Any], size_a: int, size_b: int) -> None:
    diff["output_changed"] = "skipped_large_output"
    diff["a_output_size"] = size_a
    diff["b_output_size"] = size_b


def _record_output_comparison(diff: dict[str, Any], val_a: Any, val_b: Any) -> None:
    if val_a != val_b:
        diff["output_changed"] = True
        diff["a_output_repr"] = repr(val_a)
        diff["b_output_repr"] = repr(val_b)
    else:
        diff["output_changed"] = False


async def diff_commits(
    a: Commit, b: Commit, store: AsyncStore, serializer: Serializer
) -> dict[str, Any]:
    diff = _diff_base(a, b)
    try:
        size_a, size_b = _output_sizes(a, b)
        if size_a > _DIFF_SIZE_LIMIT or size_b > _DIFF_SIZE_LIMIT:
            _record_large_output(diff, size_a, size_b)
        else:
            val_a = await load_result(a, store, serializer)
            val_b = await load_result(b, store, serializer)
            _record_output_comparison(diff, val_a, val_b)
    except Exception:
        diff["output_changed"] = "unable_to_compare"
    return diff
