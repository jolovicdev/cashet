from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio

from cashet.models import Commit, TaskDef, TaskStatus
from cashet.redis_store import AsyncRedisStore

pytestmark = pytest.mark.redis


@pytest_asyncio.fixture
async def async_redis_store() -> AsyncRedisStore:
    store = AsyncRedisStore()
    await store._redis.flushdb()
    return store


class TestAsyncRedisStoreProtocol:
    async def test_put_and_get_blob(self, async_redis_store: AsyncRedisStore) -> None:
        data = b"hello async redis"
        ref = await async_redis_store.put_blob(data)
        assert ref.size == len(data)
        fetched = await async_redis_store.get_blob(ref)
        assert fetched == data

    async def test_put_and_get_commit(self, async_redis_store: AsyncRedisStore) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="test_func",
            func_source="def test_func(): pass",
            args_hash="b" * 64,
            args_snapshot=b"args",
        )
        commit = Commit(hash="c" * 64, task_def=task_def, status=TaskStatus.COMPLETED)
        await async_redis_store.put_commit(commit)
        fetched = await async_redis_store.get_commit("c" * 64)
        assert fetched is not None
        assert fetched.hash == commit.hash

    async def test_find_by_fingerprint(self, async_redis_store: AsyncRedisStore) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        commit = Commit(hash="c" * 64, task_def=task_def, status=TaskStatus.COMPLETED)
        await async_redis_store.put_commit(commit)
        found = await async_redis_store.find_by_fingerprint(task_def.fingerprint)
        assert found is not None
        assert found.hash == commit.hash

    async def test_list_commits(self, async_redis_store: AsyncRedisStore) -> None:
        for i in range(3):
            task_def = TaskDef(
                func_hash=f"{i:064d}",
                func_name=f"func_{i}",
                func_source=f"def func_{i}(): pass",
                args_hash="b" * 64,
                args_snapshot=b"",
            )
            commit = Commit(hash=f"{i:064d}", task_def=task_def, status=TaskStatus.COMPLETED)
            await async_redis_store.put_commit(commit)
        commits = await async_redis_store.list_commits(limit=10)
        assert len(commits) == 3

    async def test_delete_commit(self, async_redis_store: AsyncRedisStore) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        commit = Commit(hash="c" * 64, task_def=task_def, status=TaskStatus.COMPLETED)
        await async_redis_store.put_commit(commit)
        assert await async_redis_store.delete_commit("c" * 64) is True
        assert await async_redis_store.get_commit("c" * 64) is None

    async def test_stats(self, async_redis_store: AsyncRedisStore) -> None:
        await async_redis_store.put_blob(b"data")
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        await async_redis_store.put_commit(
            Commit(hash="c" * 64, task_def=task_def, status=TaskStatus.COMPLETED)
        )
        s = await async_redis_store.stats()
        assert s["total_commits"] == 1
        assert s["stored_objects"] == 1

    async def test_evict_old_commits(self, async_redis_store: AsyncRedisStore) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        commit = Commit(hash="c" * 64, task_def=task_def, status=TaskStatus.COMPLETED)
        await async_redis_store.put_commit(commit)
        deleted = await async_redis_store.evict(datetime.now(UTC) + timedelta(seconds=1))
        assert deleted == 1
        assert await async_redis_store.get_commit("c" * 64) is None

    async def test_close(self, async_redis_store: AsyncRedisStore) -> None:
        await async_redis_store.close()
