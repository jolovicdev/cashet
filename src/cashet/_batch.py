from __future__ import annotations

from datetime import timedelta
from typing import Any

from cashet.dag import ResultRef, TaskRef
from cashet.hashing import Serializer, build_task_def
from cashet.models import TaskError
from cashet.protocols import Executor, Store

BatchKey = int | str
NormalizedTask = tuple[
    Any, tuple[Any, ...], dict[str, Any], bool, dict[str, str], int, bool, timedelta | None
]


def unpack_dict_tasks(tasks: dict[str, Any]) -> tuple[list[BatchKey], list[Any]]:
    items = list(tasks.items())
    return [k for k, _ in items], [t for _, t in items]


def unpack_list_tasks(tasks: list[Any]) -> tuple[list[BatchKey], list[Any]]:
    return list(range(len(tasks))), list(tasks)


def normalize_tasks(
    raw_tasks: list[Any],
    default_cache: bool | None,
    default_tags: dict[str, str] | None,
    default_retries: int | None = None,
    default_force: bool | None = None,
    default_timeout: int | float | None = None,
) -> list[NormalizedTask]:
    normalized: list[NormalizedTask] = []
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
            else getattr(raw_func, "_cashet_cache", True)
        )
        tags = (
            default_tags
            if default_tags is not None
            else getattr(raw_func, "_cashet_tags", {})
        )
        retries = (
            default_retries
            if default_retries is not None
            else getattr(raw_func, "_cashet_retries", 0)
        )
        force = (
            default_force
            if default_force is not None
            else getattr(raw_func, "_cashet_force", False)
        )
        timeout_seconds = (
            default_timeout
            if default_timeout is not None
            else getattr(raw_func, "_cashet_timeout", None)
        )
        timeout = timedelta(seconds=timeout_seconds) if timeout_seconds is not None else None
        normalized.append((raw_func, args, kwargs, cache, tags, retries, force, timeout))
    return normalized


def build_deps(
    keys: list[BatchKey],
    normalized: list[NormalizedTask],
    key_set: set[BatchKey],
) -> tuple[dict[BatchKey, set[BatchKey]], dict[BatchKey, list[tuple[str, Any, BatchKey]]]]:
    deps: dict[BatchKey, set[BatchKey]] = {k: set() for k in keys}
    task_refs: dict[BatchKey, list[tuple[str, Any, BatchKey]]] = {}
    for key, item in zip(keys, normalized, strict=True):
        _func, args, kwargs, _cache, _tags, _retries, _force, _timeout = item
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


def reverse_deps(deps: dict[BatchKey, set[BatchKey]]) -> dict[BatchKey, set[BatchKey]]:
    rev: dict[BatchKey, set[BatchKey]] = {k: set() for k in deps}
    for key, dep_set in deps.items():
        for d in dep_set:
            rev[d].add(key)
    return rev


def topological_sort(deps: dict[BatchKey, set[BatchKey]]) -> list[BatchKey]:
    visited: set[BatchKey] = set()
    temp: set[BatchKey] = set()
    order: list[BatchKey] = []

    def visit(n: BatchKey) -> None:
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


def execute_batch(
    order: list[BatchKey],
    keys: list[BatchKey],
    normalized: list[NormalizedTask],
    task_refs: dict[BatchKey, list[tuple[str, Any, BatchKey]]],
    deps: dict[BatchKey, set[BatchKey]],
    executor: Executor,
    store: Store,
    serializer: Serializer,
    max_workers: int | None,
) -> dict[BatchKey, ResultRef]:
    from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

    pos = {k: i for i, k in enumerate(keys)}
    results: dict[BatchKey, ResultRef] = {}
    rev = reverse_deps(deps)
    in_degree = {k: len(deps[k]) for k in keys}

    def _run_single(key: BatchKey) -> ResultRef:
        func, args, kwargs, cache, tags, retries, force, timeout = normalized[pos[key]]
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
            func,
            tuple(resolved_args),
            resolved_kwargs,
            cache=cache,
            tags=tags,
            retries=retries,
            force=force,
            timeout=timeout,
        )
        commit, _was_cached = executor.submit(
            func, tuple(resolved_args), resolved_kwargs, task_def, store, serializer
        )
        if commit.output_ref is None:
            raise TaskError(f"Task {commit.task_def.func_name} failed: {commit.error}")
        ref = ResultRef(commit.output_ref, store, serializer, commit_hash=commit.hash)
        return ref

    if max_workers is None or max_workers == 1:
        for key in order:
            results[key] = _run_single(key)
        return results

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures: dict[Any, BatchKey] = {}
        for key in keys:
            if in_degree[key] == 0:
                futures[pool.submit(_run_single, key)] = key

        while futures:
            done, _ = wait(futures, return_when=FIRST_COMPLETED)
            for future in done:
                key = futures.pop(future)
                results[key] = future.result()
                for dependent in rev[key]:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        futures[pool.submit(_run_single, dependent)] = dependent

    return results
