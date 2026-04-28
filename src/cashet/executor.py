from __future__ import annotations

from datetime import timedelta
from typing import Any

from cashet._runner import BlockingAsyncRunner
from cashet.adapters import SyncStoreAdapter
from cashet.async_executor import AsyncLocalExecutor
from cashet.hashing import Serializer
from cashet.models import Commit, TaskDef
from cashet.protocols import Store


class LocalExecutor:
    def __init__(
        self, running_ttl: timedelta | None = None, timeout: timedelta | None = None
    ) -> None:
        self._async_executor = AsyncLocalExecutor(running_ttl, timeout)
        self._runner = BlockingAsyncRunner()

    @classmethod
    def from_async(
        cls, async_executor: AsyncLocalExecutor, *, runner: BlockingAsyncRunner | None = None
    ) -> LocalExecutor:
        instance = cls.__new__(cls)
        instance._async_executor = async_executor
        instance._runner = runner or BlockingAsyncRunner()
        return instance

    def submit(
        self,
        func: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        task_def: TaskDef,
        store: Store,
        serializer: Serializer,
    ) -> tuple[Commit, bool]:
        async_store: Any = getattr(store, "_async_store", None)
        if async_store is None:
            async_store = SyncStoreAdapter(store)
        return self._runner.call(
            self._async_executor.submit(func, args, kwargs, task_def, async_store, serializer)
        )
