from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from cashet import Client
from cashet.models import Commit, TaskDef, TaskStatus
from cashet.redis_store import RedisStore

pytestmark = pytest.mark.redis


@pytest.fixture
def redis_store() -> RedisStore:
    store = RedisStore()
    store._redis.flushdb()
    return store


class TestRedisStoreProtocol:
    def test_put_and_get_blob(self, redis_store: RedisStore) -> None:
        data = b"hello redis"
        ref = redis_store.put_blob(data)
        assert ref.hash == hashlib.sha256(data).hexdigest()
        assert redis_store.get_blob(ref) == data

    def test_blob_dedup(self, redis_store: RedisStore) -> None:
        data = b"dedup me"
        ref1 = redis_store.put_blob(data)
        ref2 = redis_store.put_blob(data)
        assert ref1.hash == ref2.hash

    def test_put_and_get_commit(self, redis_store: RedisStore) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="test_func",
            func_source="def test_func(): pass",
            args_hash="b" * 64,
            args_snapshot=b"args",
        )
        commit = Commit(hash="c" * 64, task_def=task_def, status=TaskStatus.COMPLETED)
        redis_store.put_commit(commit)
        fetched = redis_store.get_commit("c" * 64)
        assert fetched is not None
        assert fetched.hash == commit.hash
        assert fetched.task_def.func_name == "test_func"

    def test_find_by_fingerprint(self, redis_store: RedisStore) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        commit = Commit(hash="c" * 64, task_def=task_def, status=TaskStatus.COMPLETED)
        redis_store.put_commit(commit)
        found = redis_store.find_by_fingerprint(task_def.fingerprint)
        assert found is not None
        assert found.hash == commit.hash

    def test_find_running_by_fingerprint(self, redis_store: RedisStore) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        commit = Commit(hash="c" * 64, task_def=task_def, status=TaskStatus.RUNNING)
        redis_store.put_commit(commit)
        found = redis_store.find_running_by_fingerprint(task_def.fingerprint)
        assert found is not None
        assert found.hash == commit.hash

    def test_list_commits(self, redis_store: RedisStore) -> None:
        for i in range(3):
            task_def = TaskDef(
                func_hash=f"{i:064d}",
                func_name=f"func_{i}",
                func_source=f"def func_{i}(): pass",
                args_hash="b" * 64,
                args_snapshot=b"",
            )
            commit = Commit(hash=f"{i:064d}", task_def=task_def, status=TaskStatus.COMPLETED)
            redis_store.put_commit(commit)
        commits = redis_store.list_commits(limit=10)
        assert len(commits) == 3

    def test_list_commits_filter_func(self, redis_store: RedisStore) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="target",
            func_source="def target(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        commit = Commit(hash="c" * 64, task_def=task_def, status=TaskStatus.COMPLETED)
        redis_store.put_commit(commit)
        commits = redis_store.list_commits(func_name="target")
        assert len(commits) == 1
        assert commits[0].task_def.func_name == "target"

    def test_list_commits_filter_status(self, redis_store: RedisStore) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        redis_store.put_commit(
            Commit(hash="c1" + "0" * 62, task_def=task_def, status=TaskStatus.COMPLETED)
        )
        redis_store.put_commit(
            Commit(hash="c2" + "0" * 62, task_def=task_def, status=TaskStatus.FAILED)
        )
        commits = redis_store.list_commits(status=TaskStatus.COMPLETED)
        assert len(commits) == 1
        assert commits[0].status == TaskStatus.COMPLETED

    def test_list_commits_filter_tags(self, redis_store: RedisStore) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
            tags={"env": "test"},
        )
        commit = Commit(
            hash="c" * 64, task_def=task_def, tags={"env": "test"}, status=TaskStatus.COMPLETED
        )
        redis_store.put_commit(commit)
        commits = redis_store.list_commits(tags={"env": "test"})
        assert len(commits) == 1

    def test_get_history(self, redis_store: RedisStore) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        c1 = Commit(hash="c1" + "0" * 62, task_def=task_def, status=TaskStatus.COMPLETED)
        c2 = Commit(hash="c2" + "0" * 62, task_def=task_def, status=TaskStatus.COMPLETED)
        redis_store.put_commit(c1)
        redis_store.put_commit(c2)
        history = redis_store.get_history("c1" + "0" * 62)
        assert len(history) == 2

    def test_stats(self, redis_store: RedisStore) -> None:
        redis_store.put_blob(b"data")
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        redis_store.put_commit(
            Commit(hash="c" * 64, task_def=task_def, status=TaskStatus.COMPLETED)
        )
        s = redis_store.stats()
        assert s["total_commits"] == 1
        assert s["stored_objects"] == 1

    def test_evict_old_commits(self, redis_store: RedisStore) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        commit = Commit(hash="c" * 64, task_def=task_def, status=TaskStatus.COMPLETED)
        redis_store.put_commit(commit)
        deleted = redis_store.evict(datetime.now(UTC) + timedelta(seconds=1))
        assert deleted == 1
        assert redis_store.get_commit("c" * 64) is None

    def test_delete_commit(self, redis_store: RedisStore) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        commit = Commit(hash="c" * 64, task_def=task_def, status=TaskStatus.COMPLETED)
        redis_store.put_commit(commit)
        assert redis_store.delete_commit("c" * 64) is True
        assert redis_store.get_commit("c" * 64) is None
        assert redis_store.delete_commit("c" * 64) is False

    def test_delete_commit_removes_orphan_blobs(self, redis_store: RedisStore) -> None:
        data = b"orphan blob"
        ref = redis_store.put_blob(data)
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        commit = Commit(
            hash="c" * 64, task_def=task_def, output_ref=ref, status=TaskStatus.COMPLETED
        )
        redis_store.put_commit(commit)
        redis_store.delete_commit("c" * 64)
        with pytest.raises(ValueError, match="not found"):
            redis_store.get_blob(ref)

    def test_delete_commit_preserves_shared_blobs(self, redis_store: RedisStore) -> None:
        data = b"shared blob"
        ref = redis_store.put_blob(data)
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        c1 = Commit(
            hash="c1" + "0" * 62, task_def=task_def, output_ref=ref, status=TaskStatus.COMPLETED
        )
        c2 = Commit(
            hash="c2" + "0" * 62, task_def=task_def, output_ref=ref, status=TaskStatus.COMPLETED
        )
        redis_store.put_commit(c1)
        redis_store.put_commit(c2)
        redis_store.delete_commit("c1" + "0" * 62)
        assert redis_store.get_blob(ref) == data

    def test_ambiguous_prefix_raises(self, redis_store: RedisStore) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        redis_store.put_commit(
            Commit(hash="ab" + "0" * 62, task_def=task_def, status=TaskStatus.COMPLETED)
        )
        redis_store.put_commit(
            Commit(hash="ab" + "1" * 62, task_def=task_def, status=TaskStatus.COMPLETED)
        )
        with pytest.raises(ValueError, match="Ambiguous prefix"):
            redis_store.get_commit("ab")

    def test_get_commit_short_prefix(self, redis_store: RedisStore) -> None:
        task_def = TaskDef(
            func_hash="a" * 64,
            func_name="f",
            func_source="def f(): pass",
            args_hash="b" * 64,
            args_snapshot=b"",
        )
        commit = Commit(hash="c1" + "0" * 62, task_def=task_def, status=TaskStatus.COMPLETED)
        redis_store.put_commit(commit)
        fetched = redis_store.get_commit("c1")
        assert fetched is not None
        assert fetched.hash == commit.hash

    def test_close(self, redis_store: RedisStore) -> None:
        redis_store.close()


class TestRedisStoreWithClient:
    def test_submit_and_cache(self, redis_store: RedisStore) -> None:
        client = Client(store=redis_store)

        def add(x: int, y: int) -> int:
            return x + y

        ref1 = client.submit(add, 3, 4)
        assert ref1.load() == 7

        ref2 = client.submit(add, 3, 4)
        assert ref2.load() == 7
        assert ref1.hash == ref2.hash

    def test_log_and_stats(self, redis_store: RedisStore) -> None:
        client = Client(store=redis_store)

        def greet(name: str) -> str:
            return f"hello {name}"

        client.submit(greet, "world")
        log = client.log()
        assert len(log) == 1
        assert log[0].task_def.func_name == greet.__qualname__
        s = client.stats()
        assert s["total_commits"] == 1

    def test_distributed_lock_with_redis(self, redis_store: RedisStore) -> None:
        import threading

        client = Client(store=redis_store)
        call_count = 0

        def expensive(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * x

        refs: list[Any] = []
        lock = threading.Lock()

        def run(x: int) -> None:
            ref = client.submit(expensive, x)
            with lock:
                refs.append(ref)

        threads = [threading.Thread(target=run, args=(5,)) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert call_count == 1
        assert len({r.hash for r in refs}) == 1
