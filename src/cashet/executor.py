from __future__ import annotations

import traceback
from typing import Any

from cashet.dag import (
    build_commit,
    find_existing_commit,
    find_parent_hash,
    resolve_input_refs,
)
from cashet.hashing import Serializer
from cashet.models import Commit, ObjectRef, TaskDef, TaskStatus
from cashet.protocols import Store


class LocalExecutor:
    def submit(
        self,
        func: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        task_def: TaskDef,
        store: Store,
        serializer: Serializer,
    ) -> tuple[Commit, bool]:
        input_refs = resolve_input_refs(args, kwargs)
        existing = find_existing_commit(store, task_def)
        if existing is not None:
            existing.status = TaskStatus.CACHED
            store.put_commit(existing)
            return existing, True

        parent_hash = find_parent_hash(store, task_def)
        commit = build_commit(task_def, input_refs, parent_hash=parent_hash)
        commit = self._execute(func, args, kwargs, commit, store, serializer)
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
        store.put_commit(commit)
        try:
            commit.status = TaskStatus.RUNNING
            store.put_commit(commit)
            resolved_args = self._resolve_args(args)
            resolved_kwargs = self._resolve_kwargs(kwargs)
            result = func(*resolved_args, **resolved_kwargs)
            output_ref = self._store_result(result, store, serializer)
            commit.output_ref = output_ref
            commit.status = TaskStatus.COMPLETED
        except Exception:
            commit.status = TaskStatus.FAILED
            commit.error = traceback.format_exc()
        store.put_commit(commit)
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
