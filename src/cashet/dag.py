from __future__ import annotations

import asyncio
import hashlib
import time
from typing import Any, Generic, TypeVar

from cashet.hashing import Serializer
from cashet.models import Commit, ObjectRef, TaskDef, TaskStatus
from cashet.protocols import AsyncStore

T = TypeVar("T")


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


class AsyncResultRef(Generic[T]):
    __slots__ = (
        "_commit_hash", "_load_lock", "_loaded", "_ref", "_serializer", "_store", "_value"
    )

    def __init__(
        self,
        ref: ObjectRef,
        store: AsyncStore,
        serializer: Serializer,
        commit_hash: str = "",
    ) -> None:
        self._ref = ref
        self._store = store
        self._serializer = serializer
        self._commit_hash = commit_hash
        self._value: Any = None
        self._loaded = False
        self._load_lock: Any = None

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

    @property
    def ref(self) -> ObjectRef:
        return self._ref

    async def load(self) -> T:
        if self._loaded:
            return self._value
        if self._load_lock is None:
            self._load_lock = asyncio.Lock()
        async with self._load_lock:
            if self._loaded:
                return self._value
            data = await self._store.get_blob(self._ref)
            self._value = self._serializer.loads(data)
            self._loaded = True
            return self._value

    def __repr__(self) -> str:
        ch = self._commit_hash[:12] if self._commit_hash else "?"
        return f"AsyncResultRef(commit={ch}, blob={self.short_hash}, size={self.size})"


class ResultRef(Generic[T]):
    __slots__ = ("_async_ref", "_commit_hash", "_ref", "_runner")

    def __init__(self, async_ref: AsyncResultRef[T], runner: Any) -> None:
        self._async_ref = async_ref
        self._runner = runner
        self._ref = async_ref.ref
        self._commit_hash = async_ref.commit_hash

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

    async def __cashet_async_load__(self) -> T:
        return await self._async_ref.load()

    def load(self) -> T:
        return self._runner.call(self._async_ref.load())

    def __repr__(self) -> str:
        ch = self._commit_hash[:12] if self._commit_hash else "?"
        return f"ResultRef(commit={ch}, blob={self.short_hash}, size={self.size})"


async def find_existing_commit(store: AsyncStore, task_def: TaskDef) -> Commit | None:
    if not task_def.cache:
        return None
    return await store.find_by_fingerprint(task_def.fingerprint)


async def find_parent_hash(store: AsyncStore, task_def: TaskDef) -> str | None:
    existing = await store.find_by_fingerprint(task_def.fingerprint)
    if existing is not None:
        return existing.hash
    return None


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
