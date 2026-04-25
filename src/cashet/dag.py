from __future__ import annotations

import hashlib
import time
from typing import Any

from cashet.hashing import Serializer
from cashet.models import Commit, ObjectRef, TaskDef, TaskStatus
from cashet.protocols import Store


class TaskRef:
    def __init__(self, key: int | str) -> None:
        self.key = key


def resolve_input_refs(args: tuple[Any, ...], kwargs: dict[str, Any]) -> list[ObjectRef]:
    refs: list[ObjectRef] = []
    for arg in args:
        if hasattr(arg, "__cashet_ref__"):
            refs.append(arg.__cashet_ref__())
    for val in kwargs.values():
        if hasattr(val, "__cashet_ref__"):
            refs.append(val.__cashet_ref__())
    return refs


class ResultRef:
    __slots__ = ("_commit_hash", "_loaded", "_ref", "_serializer", "_store", "_value")

    def __init__(
        self,
        ref: ObjectRef,
        store: Store,
        serializer: Serializer,
        commit_hash: str = "",
    ) -> None:
        self._ref = ref
        self._store = store
        self._serializer = serializer
        self._commit_hash = commit_hash
        self._value: Any = None
        self._loaded = False

    def __cashet_ref__(self) -> ObjectRef:
        return self._ref

    @property
    def hash(self) -> str:
        return self._ref.hash

    @property
    def commit_hash(self) -> str:
        return self._commit_hash

    @property
    def short_hash(self) -> str:
        return self._ref.short()

    @property
    def size(self) -> int:
        return self._ref.size

    def load(self) -> Any:
        if not self._loaded:
            data = self._store.get_blob(self._ref)
            self._value = self._serializer.loads(data)
            self._loaded = True
        return self._value

    def __repr__(self) -> str:
        ch = self._commit_hash[:12] if self._commit_hash else "?"
        return f"ResultRef(commit={ch}, blob={self.short_hash}, size={self.size})"


def compute_commit_hash(
    task_def: TaskDef,
    input_refs: list[ObjectRef],
    *,
    salt: str | None = None,
) -> str:
    h = hashlib.sha256()
    h.update(task_def.func_hash.encode("utf-8"))
    h.update(task_def.args_hash.encode("utf-8"))
    for ref in sorted(input_refs, key=lambda r: r.hash):
        h.update(ref.hash.encode("utf-8"))
    if salt is not None:
        h.update(salt.encode("utf-8"))
    return h.hexdigest()


def find_existing_commit(store: Store, task_def: TaskDef) -> Commit | None:
    if not task_def.cache:
        return None
    fingerprint = f"{task_def.func_hash}:{task_def.args_hash}"
    return store.find_by_fingerprint(fingerprint)


def find_parent_hash(store: Store, task_def: TaskDef) -> str | None:
    fingerprint = f"{task_def.func_hash}:{task_def.args_hash}"
    existing = store.find_by_fingerprint(fingerprint)
    if existing is not None:
        return existing.hash
    return None


def build_commit(
    task_def: TaskDef,
    input_refs: list[ObjectRef],
    parent_hash: str | None = None,
) -> Commit:
    salt = None
    if not task_def.cache or task_def.force:
        salt = f"{time.time_ns()}"
    commit_hash = compute_commit_hash(task_def, input_refs, salt=salt)
    return Commit(
        hash=commit_hash,
        task_def=task_def,
        input_refs=input_refs,
        parent_hash=parent_hash,
        status=TaskStatus.PENDING,
        tags=task_def.tags,
    )
