from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

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

    async def test_list_commits_filter_tags_searches_past_limit_window(
        self, async_redis_store: AsyncRedisStore
    ) -> None:
        for i in range(5):
            tags = {"keep": "yes"} if i == 0 else {}
            task_def = TaskDef(
                func_hash=f"{i:064d}",
                func_name="f",
                func_source="def f(): pass",
                args_hash="b" * 64,
                args_snapshot=b"",
                tags=tags,
            )
            commit = Commit(
                hash=f"{i:064d}",
                task_def=task_def,
                status=TaskStatus.COMPLETED,
                created_at=datetime.now(UTC) + timedelta(seconds=i),
                tags=tags,
            )
            await async_redis_store.put_commit(commit)
        commits = await async_redis_store.list_commits(limit=1, tags={"keep": "yes"})
        assert len(commits) == 1
        assert commits[0].hash == f"{0:064d}"

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

    async def test_delete_commit_with_short_prefix_deletes_full_key(
        self, async_redis_store: AsyncRedisStore
    ) -> None:
        ref = await async_redis_store.put_blob(b"prefix delete")
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        commit = Commit(
            hash="c1" + "0" * 62,
            task_def=task_def,
            output_ref=ref,
            status=TaskStatus.COMPLETED,
        )
        await async_redis_store.put_commit(commit)
        assert await async_redis_store.delete_commit("c1") is True
        assert await async_redis_store.get_commit(commit.hash) is None
        with pytest.raises(ValueError, match="not found"):
            await async_redis_store.get_blob(ref)

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
        assert s["disk_bytes"] == 4

    async def test_stats_use_maintained_blob_counters(
        self,
        async_redis_store: AsyncRedisStore,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        first = await async_redis_store.put_blob(b"data")
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        commit = Commit(
            hash="c" * 64,
            task_def=task_def,
            output_ref=first,
            status=TaskStatus.COMPLETED,
        )
        await async_redis_store.put_commit(commit)
        assert (await async_redis_store.stats())["disk_bytes"] == 4

        def fail_scan_iter(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("blob stats should use maintained Redis counters")

        monkeypatch.setattr(async_redis_store._redis, "scan_iter", fail_scan_iter)
        await async_redis_store.put_blob(b"more")
        s = await async_redis_store.stats()
        assert s["stored_objects"] == 2
        assert s["disk_bytes"] == 8

        assert await async_redis_store.delete_commit(commit.hash) is True
        s = await async_redis_store.stats()
        assert s["stored_objects"] == 1
        assert s["disk_bytes"] == 4

    async def test_evict_max_size_uses_cashet_blob_bytes(
        self, async_redis_store: AsyncRedisStore
    ) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        old_ref = await async_redis_store.put_blob(b"x" * 10)
        new_ref = await async_redis_store.put_blob(b"y" * 10)
        old = Commit(
            hash="a1" + "0" * 62,
            task_def=task_def,
            output_ref=old_ref,
            status=TaskStatus.COMPLETED,
            created_at=datetime.now(UTC) - timedelta(seconds=1),
        )
        new = Commit(
            hash="a2" + "0" * 62,
            task_def=task_def,
            output_ref=new_ref,
            status=TaskStatus.COMPLETED,
        )
        await async_redis_store.put_commit(old)
        await async_redis_store.put_commit(new)
        deleted = await async_redis_store.evict(
            datetime.now(UTC) - timedelta(days=1), max_size_bytes=10
        )
        assert deleted == 1
        assert await async_redis_store.get_commit(old.hash) is None
        assert await async_redis_store.get_commit(new.hash) is not None
        assert (await async_redis_store.stats())["disk_bytes"] == 10

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

    async def test_evict_preserves_recently_accessed_old_commit(
        self, async_redis_store: AsyncRedisStore
    ) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        commit = Commit(
            hash="c" * 64,
            task_def=task_def,
            status=TaskStatus.COMPLETED,
            created_at=datetime.now(UTC) - timedelta(days=10),
        )
        await async_redis_store.put_commit(commit)
        assert await async_redis_store.find_by_fingerprint(task_def.fingerprint) is not None
        deleted = await async_redis_store.evict(datetime.now(UTC) - timedelta(days=1))
        assert deleted == 0
        assert await async_redis_store.get_commit(commit.hash) is not None

    async def test_close(self, async_redis_store: AsyncRedisStore) -> None:
        await async_redis_store.close()
