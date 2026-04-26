from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest_asyncio

from cashet.models import Commit, TaskDef, TaskStatus
from cashet.store import AsyncSQLiteStore


@pytest_asyncio.fixture
async def async_sqlite_store(tmp_path: Path) -> AsyncSQLiteStore:
    return AsyncSQLiteStore(tmp_path / ".cashet")


class TestAsyncSQLiteStoreProtocol:
    async def test_put_and_get_blob(self, async_sqlite_store: AsyncSQLiteStore) -> None:
        data = b"hello async sqlite"
        ref = await async_sqlite_store.put_blob(data)
        assert ref.size == len(data)
        fetched = await async_sqlite_store.get_blob(ref)
        assert fetched == data

    async def test_put_and_get_commit(self, async_sqlite_store: AsyncSQLiteStore) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="test_func",
            func_source="def test_func(): pass",
            args_hash="b" * 64,
            args_snapshot=b"args",
        )
        commit = Commit(hash="c" * 64, task_def=task_def, status=TaskStatus.COMPLETED)
        await async_sqlite_store.put_commit(commit)
        fetched = await async_sqlite_store.get_commit("c" * 64)
        assert fetched is not None
        assert fetched.hash == commit.hash

    async def test_find_by_fingerprint(self, async_sqlite_store: AsyncSQLiteStore) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        commit = Commit(hash="c" * 64, task_def=task_def, status=TaskStatus.COMPLETED)
        await async_sqlite_store.put_commit(commit)
        found = await async_sqlite_store.find_by_fingerprint(task_def.fingerprint)
        assert found is not None
        assert found.hash == commit.hash

    async def test_list_commits(self, async_sqlite_store: AsyncSQLiteStore) -> None:
        for i in range(3):
            task_def = TaskDef(
                func_hash=f"{i:064d}",
                func_name=f"func_{i}",
                func_source=f"def func_{i}(): pass",
                args_hash="b" * 64,
                args_snapshot=b"",
            )
            commit = Commit(hash=f"{i:064d}", task_def=task_def, status=TaskStatus.COMPLETED)
            await async_sqlite_store.put_commit(commit)
        commits = await async_sqlite_store.list_commits(limit=10)
        assert len(commits) == 3

    async def test_delete_commit(self, async_sqlite_store: AsyncSQLiteStore) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        commit = Commit(hash="c" * 64, task_def=task_def, status=TaskStatus.COMPLETED)
        await async_sqlite_store.put_commit(commit)
        assert await async_sqlite_store.delete_commit("c" * 64) is True
        assert await async_sqlite_store.get_commit("c" * 64) is None

    async def test_stats(self, async_sqlite_store: AsyncSQLiteStore) -> None:
        await async_sqlite_store.put_blob(b"data")
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        await async_sqlite_store.put_commit(
            Commit(hash="c" * 64, task_def=task_def, status=TaskStatus.COMPLETED)
        )
        s = await async_sqlite_store.stats()
        assert s["total_commits"] == 1
        assert s["stored_objects"] == 1

    async def test_evict_old_commits(self, async_sqlite_store: AsyncSQLiteStore) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        commit = Commit(hash="c" * 64, task_def=task_def, status=TaskStatus.COMPLETED)
        await async_sqlite_store.put_commit(commit)
        deleted = await async_sqlite_store.evict(datetime.now(UTC) + timedelta(seconds=1))
        assert deleted == 1
        assert await async_sqlite_store.get_commit("c" * 64) is None

    async def test_close(self, async_sqlite_store: AsyncSQLiteStore) -> None:
        await async_sqlite_store.close()
