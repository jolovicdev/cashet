from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar

import pytest

from cashet import Client, TaskError
from cashet.executor import LocalExecutor
from cashet.hashing import PickleSerializer, build_task_def
from cashet.models import Commit, ObjectRef, StorageTier, TaskStatus
from cashet.store import SQLiteStore


class TestProcessSafety:
    def test_local_executor_accepts_plain_sync_store_protocol(
        self, store_dir: Path
    ) -> None:
        wrapped = SQLiteStore(store_dir)

        class StoreProxy:
            def put_blob(self, data: bytes) -> ObjectRef:
                return wrapped.put_blob(data)

            def get_blob(self, ref: ObjectRef) -> bytes:
                return wrapped.get_blob(ref)

            def put_commit(self, commit: Commit) -> None:
                wrapped.put_commit(commit)

            def get_commit(self, hash: str) -> Commit | None:
                return wrapped.get_commit(hash)

            def find_by_fingerprint(self, fingerprint: str) -> Commit | None:
                return wrapped.find_by_fingerprint(fingerprint)

            def find_running_by_fingerprint(self, fingerprint: str) -> Commit | None:
                return wrapped.find_running_by_fingerprint(fingerprint)

            def list_commits(
                self,
                func_name: str | None = None,
                limit: int = 50,
                status: TaskStatus | None = None,
                tags: dict[str, str | None] | None = None,
            ) -> list[Commit]:
                return wrapped.list_commits(func_name, limit, status, tags)

            def get_history(self, hash: str) -> list[Commit]:
                return wrapped.get_history(hash)

            def stats(self) -> dict[str, int]:
                return wrapped.stats()

            def evict(
                self, older_than: datetime, max_size_bytes: int | None = None
            ) -> int:
                return wrapped.evict(older_than, max_size_bytes)

            def delete_commit(self, hash: str) -> bool:
                return wrapped.delete_commit(hash)

            def close(self) -> None:
                wrapped.close()

        def add(x: int, y: int) -> int:
            return x + y

        serializer = PickleSerializer()
        task_def = build_task_def(add, (2, 3), {})
        commit, cached = LocalExecutor().submit(
            add, (2, 3), {}, task_def, StoreProxy(), serializer
        )

        assert cached is False
        assert commit.output_ref is not None
        assert serializer.loads(wrapped.get_blob(commit.output_ref)) == 5

    def test_cross_process_dedup(self, store_dir: Path) -> None:
        import multiprocessing

        # Pre-initialize store to avoid initialization races
        _ = Client(store_dir=store_dir)

        def worker(store_dir_str: str, x: int) -> None:
            c = Client(store_dir=store_dir_str)

            def expensive(v: int) -> int:
                return v * v

            c.submit(expensive, x)

        processes = [
            multiprocessing.Process(target=worker, args=(str(store_dir), 7))
            for _ in range(4)
        ]
        for p in processes:
            p.start()
        for p in processes:
            p.join()

        client = Client(store_dir=store_dir)
        log = client.log()
        assert len(log) == 1

    def test_store_lock_released_during_execution(self, store_dir: Path) -> None:
        import threading
        import time

        client = Client(store_dir=store_dir)

        def slow() -> int:
            time.sleep(0.5)
            return 1

        def fast() -> int:
            return 2

        slow_done = threading.Event()
        fast_done = threading.Event()

        def run_slow() -> None:
            client.submit(slow)
            slow_done.set()

        def run_fast() -> None:
            time.sleep(0.1)
            client.submit(fast)
            fast_done.set()

        t1 = threading.Thread(target=run_slow)
        t2 = threading.Thread(target=run_fast)
        t1.start()
        t2.start()

        assert fast_done.wait(timeout=1.0), "Fast task was blocked by slow task"
        assert not slow_done.is_set(), "Slow task should not be done yet"

        t1.join(timeout=2.0)
        t2.join(timeout=2.0)

    def test_running_claim_blocks_then_resolves(self, store_dir: Path) -> None:
        import threading
        import time

        client = Client(store_dir=store_dir)
        exec_count = 0

        def slow() -> int:
            nonlocal exec_count
            exec_count += 1
            time.sleep(0.3)
            return 42

        results: list[int] = []

        def submitter() -> None:
            ref = client.submit(slow)
            results.append(ref.load())

        t1 = threading.Thread(target=submitter)
        t2 = threading.Thread(target=submitter)
        t1.start()
        time.sleep(0.05)
        t2.start()
        t1.join()
        t2.join()

        assert results == [42, 42]
        assert exec_count == 1

    def test_stale_running_claim_reclaimed(self, store_dir: Path) -> None:
        import cashet.dag as dag
        import cashet.hashing as hashing
        from cashet.models import TaskStatus

        client = Client(store_dir=store_dir)

        def simple() -> int:
            return 42

        task_def = hashing.build_task_def(simple, (), {})
        input_refs = dag.resolve_input_refs((), {})
        commit = dag.build_commit(task_def, input_refs)
        commit.status = TaskStatus.RUNNING
        commit.created_at = datetime.now(UTC) - timedelta(seconds=400)
        commit.claimed_at = datetime.now(UTC) - timedelta(seconds=400)
        client.store.put_commit(commit)

        ref = client.submit(simple)
        assert ref.load() == 42

        log = client.log()
        assert len(log) == 1
        assert log[0].status.value == "completed"

    def test_running_claim_heartbeat_prevents_reclaim(self, store_dir: Path) -> None:
        import threading
        import time

        client = Client(
            store_dir=store_dir,
            executor=LocalExecutor(running_ttl=timedelta(milliseconds=100)),
        )
        exec_count = 0

        def slow() -> int:
            nonlocal exec_count
            exec_count += 1
            time.sleep(0.3)
            return 42

        results: list[int] = []

        def submitter() -> None:
            ref = client.submit(slow)
            results.append(ref.load())

        t1 = threading.Thread(target=submitter)
        t2 = threading.Thread(target=submitter)
        t1.start()
        time.sleep(0.05)
        t2.start()
        t1.join()
        t2.join()

        assert results == [42, 42]
        assert exec_count == 1

    def test_heartbeat_preserves_created_at(self, store_dir: Path) -> None:
        import time

        client = Client(
            store_dir=store_dir,
            executor=LocalExecutor(running_ttl=timedelta(milliseconds=100)),
        )

        def slow() -> int:
            time.sleep(0.3)
            return 42

        ref = client.submit(slow)
        assert ref.load() == 42

        commit = client.log(limit=1)[0]
        age = datetime.now(UTC) - commit.created_at
        assert age >= timedelta(milliseconds=250)

    def test_reclaimed_stale_claim_uses_current_retries(self, store_dir: Path) -> None:
        import cashet.dag as dag
        import cashet.hashing as hashing
        from cashet.models import TaskStatus

        client = Client(store_dir=store_dir)
        attempts = 0

        def flaky() -> int:
            nonlocal attempts
            attempts += 1
            if attempts < 2:
                raise RuntimeError("boom")
            return 7

        task_def = hashing.build_task_def(flaky, (), {}, retries=0)
        input_refs = dag.resolve_input_refs((), {})
        commit = dag.build_commit(task_def, input_refs)
        commit.status = TaskStatus.RUNNING
        commit.created_at = datetime.now(UTC) - timedelta(seconds=400)
        commit.claimed_at = datetime.now(UTC) - timedelta(seconds=400)
        client.store.put_commit(commit)

        ref = client.submit(flaky, _retries=3)
        assert ref.load() == 7
        assert attempts == 2

    def test_reclaimed_stale_claim_uses_current_timeout(self, store_dir: Path) -> None:
        import time

        import cashet.dag as dag
        import cashet.hashing as hashing
        from cashet.models import TaskStatus

        client = Client(store_dir=store_dir)

        def slow() -> int:
            time.sleep(0.25)
            return 7

        task_def = hashing.build_task_def(slow, (), {})
        input_refs = dag.resolve_input_refs((), {})
        commit = dag.build_commit(task_def, input_refs)
        commit.status = TaskStatus.RUNNING
        commit.created_at = datetime.now(UTC) - timedelta(seconds=400)
        commit.claimed_at = datetime.now(UTC) - timedelta(seconds=400)
        client.store.put_commit(commit)

        with pytest.raises(TaskError, match="TimeoutError"):
            client.submit(slow, _timeout=0.01)

    def test_running_claim_lookup_is_not_limited_to_1000_rows(self, store_dir: Path) -> None:
        import cashet.dag as dag
        import cashet.hashing as hashing
        from cashet.models import TaskStatus

        client = Client(
            store_dir=store_dir,
            executor=LocalExecutor(running_ttl=timedelta(seconds=10)),
        )

        def target(x: int) -> int:
            return x + 1

        target_def = hashing.build_task_def(target, (1,), {})
        target_commit = dag.build_commit(target_def, [])
        target_commit.status = TaskStatus.RUNNING
        target_commit.created_at = datetime.now(UTC) - timedelta(seconds=2)
        target_commit.claimed_at = datetime.now(UTC)
        client.store.put_commit(target_commit)

        for i in range(1001):
            def other(v: int = i) -> int:
                return v

            task_def = hashing.build_task_def(other, (i,), {}, cache=False)
            commit = dag.build_commit(task_def, [])
            commit.status = TaskStatus.RUNNING
            commit.created_at = datetime.now(UTC) + timedelta(milliseconds=i + 1)
            commit.claimed_at = datetime.now(UTC) + timedelta(milliseconds=i + 1)
            client.store.put_commit(commit)

        claim = client.store.find_running_by_fingerprint(target_def.fingerprint)
        assert claim is not None
        assert claim.hash == target_commit.hash


class TestStoreOperations:
    def test_log(self, client: Client) -> None:
        def f(x: int) -> int:
            return x

        client.submit(f, 1)
        client.submit(f, 2)
        client.submit(f, 3)
        log = client.log()
        assert len(log) == 3

    def test_retries_roundtrip(self, client: Client) -> None:
        def stable() -> int:
            return 42

        ref = client.submit(stable, _retries=3)
        assert ref.load() == 42
        commit = client.log(limit=1)[0]
        assert commit.task_def.retries == 3

    def test_missing_delete_does_not_poison_next_write(self, client: Client) -> None:
        def stable() -> int:
            return 42

        assert client.rm("0" * 64) is False
        ref = client.submit(stable)
        assert ref.load() == 42

    def test_log_filter_by_func(self, client: Client) -> None:
        def foo(x: int) -> int:
            return x

        def bar(x: int) -> int:
            return x * 2

        client.submit(foo, 1)
        client.submit(bar, 1)
        foo_qualname = foo.__qualname__
        log = client.log(func_name=foo_qualname)
        assert len(log) == 1
        assert log[0].task_def.func_name == foo_qualname

    def test_show(self, client: Client) -> None:
        def greet(name: str) -> str:
            return f"hello, {name}"

        client.submit(greet, "world")
        log = client.log()
        commit = client.show(log[0].hash)
        assert commit is not None
        assert "greet" in commit.task_def.func_name
        assert commit.status.value in ("completed", "cached")

    def test_history(self, client: Client) -> None:
        counter = 0

        def compute(x: int) -> int:
            nonlocal counter
            counter += 1
            return x + counter

        client.submit(compute, 5, _cache=False)
        client.submit(compute, 5, _cache=False)
        log = client.log()
        history = client.history(log[0].hash)
        assert len(history) == 2

    def test_diff(self, client: Client) -> None:
        def add(x: int, y: int) -> int:
            return x + y

        client.submit(add, 1, 2, _cache=False)
        client.submit(add, 3, 4, _cache=False)
        log = client.log()
        d = client.diff(log[0].hash, log[1].hash)
        assert "a" in d
        assert "b" in d
        assert d["args_changed"] is True

    def test_get(self, client: Client) -> None:
        def multiply(a: int, b: int) -> int:
            return a * b

        client.submit(multiply, 6, 7)
        log = client.log()
        result = client.get(log[0].hash)
        assert result == 42

    def test_stats(self, client: Client) -> None:
        def f() -> int:
            return 42

        client.submit(f)
        s = client.stats()
        assert s["total_commits"] >= 1
        assert s["completed_commits"] >= 1

    def test_stats_disk_bytes(self, client: Client) -> None:
        def f() -> bytes:
            return b"x" * 1000

        client.submit(f)
        s = client.stats()
        assert "disk_bytes" in s
        assert s["disk_bytes"] > 0

    def test_thread_safety(self, store_dir: Path) -> None:
        import threading

        client = Client(store_dir=store_dir)
        results: list[int] = []
        lock = threading.Lock()

        def worker(x: int) -> int:
            return x * x

        def run(x: int) -> None:
            ref = client.submit(worker, x)
            with lock:
                results.append(ref.load())

        threads = [threading.Thread(target=run, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert sorted(results) == [i * i for i in range(10)]

    def test_thread_safety_dedup(self, store_dir: Path) -> None:
        import threading

        client = Client(store_dir=store_dir)
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

    def test_thread_safety_cross_client_dedup(self, store_dir: Path) -> None:
        import threading

        # Pre-initialize store to avoid initialization races
        _ = Client(store_dir=store_dir)

        exec_count = 0
        lock = threading.Lock()

        def expensive(x: int) -> int:
            nonlocal exec_count
            with lock:
                exec_count += 1
            return x * x

        def run(x: int) -> None:
            c = Client(store_dir=store_dir)
            c.submit(expensive, x)

        threads = [threading.Thread(target=run, args=(7,)) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert exec_count == 1

    def test_cashet_dir_env(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        env_dir = tmp_path / "env_cashet"
        monkeypatch.setenv("CASHET_DIR", str(env_dir))
        client = Client()
        assert client.store_dir == env_dir


class TestBlobIntegrity:
    def test_valid_blob_reads_back(self, store_dir: Path) -> None:
        store = SQLiteStore(store_dir)
        data = b"hello world"
        ref = store.put_blob(data)
        assert store.get_blob(ref) == data

    def test_compressed_blob_reads_back(self, store_dir: Path) -> None:
        store = SQLiteStore(store_dir)
        data = b"x" * 1000
        ref = store.put_blob(data)
        assert store.get_blob(ref) == data

    def test_corrupted_blob_raises_valueerror(self, store_dir: Path) -> None:
        store = SQLiteStore(store_dir)
        data = b"important data" * 100
        ref = store.put_blob(data)
        obj_path = store.objects_dir / ref.hash[:2] / ref.hash[2:]
        obj_path.write_bytes(b"corrupted garbage data")
        with pytest.raises(ValueError, match="integrity check failed"):
            store.get_blob(ref)

    def test_corrupted_compressed_blob_raises_valueerror(self, store_dir: Path) -> None:
        store = SQLiteStore(store_dir)
        data = b"y" * 2000
        ref = store.put_blob(data)
        obj_path = store.objects_dir / ref.hash[:2] / ref.hash[2:]
        import zlib

        obj_path.write_bytes(zlib.compress(b"wrong data that is long enough"))
        with pytest.raises(ValueError, match="integrity check failed"):
            store.get_blob(ref)

    def test_wrong_hash_ref_raises_valueerror(self, store_dir: Path) -> None:
        store = SQLiteStore(store_dir)
        data = b"good data"
        store.put_blob(data)
        fake_ref = ObjectRef(hash="ab" + "c" * 62, size=9, tier=StorageTier.BLOB)
        fake_path = store.objects_dir / fake_ref.hash[:2] / fake_ref.hash[2:]
        fake_path.parent.mkdir(parents=True, exist_ok=True)
        fake_path.write_bytes(b"fake data")
        with pytest.raises(ValueError, match="integrity check failed"):
            store.get_blob(fake_ref)


class TestInlineStorage:
    def test_small_blob_uses_inline_tier(self, store_dir: Path) -> None:
        store = SQLiteStore(store_dir)
        data = b"tiny"
        ref = store.put_blob(data)
        assert ref.tier == StorageTier.INLINE
        assert not (store.objects_dir / ref.hash[:2] / ref.hash[2:]).exists()

    def test_large_blob_uses_blob_tier(self, store_dir: Path) -> None:
        store = SQLiteStore(store_dir)
        data = b"x" * 2000
        ref = store.put_blob(data)
        assert ref.tier == StorageTier.BLOB
        assert (store.objects_dir / ref.hash[:2] / ref.hash[2:]).exists()

    def test_inline_round_trip(self, store_dir: Path) -> None:
        store = SQLiteStore(store_dir)
        data = b"inline data"
        ref = store.put_blob(data)
        assert store.get_blob(ref) == data

    def test_inline_dedup(self, store_dir: Path) -> None:
        store = SQLiteStore(store_dir)
        data = b"dedup"
        ref1 = store.put_blob(data)
        ref2 = store.put_blob(data)
        assert ref1.hash == ref2.hash
        conn = sqlite3.connect(str(store.db_path))
        count = conn.execute(
            "SELECT COUNT(*) FROM inline_objects WHERE hash = ?", (ref1.hash,)
        ).fetchone()[0]
        assert count == 1

    def test_inline_delete_removes_orphans(self, store_dir: Path) -> None:
        store = SQLiteStore(store_dir)
        data = b"orphan"
        ref = store.put_blob(data)
        # create a dummy commit referencing the inline blob so delete_commit finds it
        from cashet.models import Commit, TaskDef, TaskStatus
        task_def = TaskDef(
            func_hash="a", func_name="f", func_source="", args_hash="b", args_snapshot=b""
        )
        commit = Commit(
            hash="c" * 64,
            task_def=task_def,
            output_ref=ref,
            status=TaskStatus.COMPLETED,
        )
        store.put_commit(commit)
        store.delete_commit(commit.hash)
        assert store.blob_exists(ref.hash) is False

    def test_inline_stats(self, store_dir: Path) -> None:
        store = SQLiteStore(store_dir)
        store.put_blob(b"small")
        large_data = bytes(range(256)) * 10  # incompressible-ish
        store.put_blob(large_data)
        stats = store.stats()
        assert stats["inline_objects"] == 1
        assert stats["inline_bytes"] == 5
        assert stats["blob_objects"] == 1
        assert 0 < stats["blob_bytes"] <= len(large_data)


class TestGarbageCollection:
    def test_evict_old_commits(self, client: Client) -> None:
        def make_val(x: int) -> int:
            return x

        client.submit(make_val, 1, _cache=False)
        client.submit(make_val, 2, _cache=False)

        assert client.stats()["total_commits"] == 2

        cutoff = datetime.now(UTC) + timedelta(seconds=1)
        deleted = client.store.evict(cutoff)
        assert deleted == 2
        assert client.stats()["total_commits"] == 0

    def test_evict_preserves_recent(self, client: Client) -> None:
        def make_val(x: int) -> int:
            return x

        client.submit(make_val, 1, _cache=False)
        assert client.stats()["total_commits"] == 1

        deleted = client.gc(timedelta(days=365))
        assert deleted == 0
        assert client.stats()["total_commits"] == 1

    def test_gc_default_30_days(self, client: Client) -> None:
        def make_val(x: int) -> int:
            return x

        client.submit(make_val, 1, _cache=False)
        deleted = client.gc()
        assert deleted == 0

    def test_evict_removes_orphan_blobs(self, client: Client) -> None:
        def make_bytes() -> bytes:
            return b"unique_payload_" * 100

        client.submit(make_bytes, _cache=False)
        assert client.stats()["stored_objects"] >= 1

        client.store.evict(datetime.now(UTC) + timedelta(seconds=1))
        assert client.stats()["stored_objects"] == 0

    def test_gc_timedelta_zero_evicts_all(self, client: Client) -> None:
        def make_val(x: int) -> int:
            return x

        client.submit(make_val, 1, _cache=False)
        client.submit(make_val, 2, _cache=False)
        assert client.stats()["total_commits"] == 2

        deleted = client.gc(timedelta(days=0))
        assert deleted == 2
        assert client.stats()["total_commits"] == 0

    def test_clear_evicts_all(self, client: Client) -> None:
        def make_val(x: int) -> int:
            return x

        client.submit(make_val, 1, _cache=False)
        client.submit(make_val, 2, _cache=False)
        assert client.stats()["total_commits"] == 2

        deleted = client.clear()
        assert deleted == 2
        assert client.stats()["total_commits"] == 0

    def test_gc_evict_reports_failed_commits(self, client: Client) -> None:
        def boom() -> None:
            raise ValueError("deliberate")

        with pytest.raises(RuntimeError):
            client.submit(boom)

        assert client.stats()["total_commits"] == 1
        deleted = client.store.evict(datetime.now(UTC) + timedelta(seconds=1))
        assert deleted == 1
        assert client.stats()["total_commits"] == 0

    def test_gc_exact_orphan_detection(self, client: Client) -> None:
        import time

        def produce() -> bytes:
            return b"keep_me"

        def consume(data: bytes) -> bytes:
            return data + b"_consumed"

        ref1 = client.submit(produce)
        time.sleep(1.1)
        client.submit(consume, ref1)
        assert client.stats()["total_commits"] == 2
        assert client.stats()["stored_objects"] >= 1

        cutoff = datetime.now(UTC) - timedelta(seconds=0.5)
        client.store.evict(cutoff)
        assert client.stats()["total_commits"] == 1
        assert client.stats()["stored_objects"] >= 1

    def test_client_close(self, store_dir: Path) -> None:
        client = Client(store_dir=store_dir)
        client.close()

    def test_client_context_manager(self, store_dir: Path) -> None:
        with Client(store_dir=store_dir) as client:
            def f() -> int:
                return 1

            ref = client.submit(f)
            assert ref.load() == 1

    def test_empty_hash_returns_none(self, client: Client) -> None:
        assert client.show("") is None
        assert client.history("") == []

    def test_custom_ref_like_object_resolves(self, client: Client) -> None:
        class CustomRef:
            def __init__(
                self,
                ref: Any,
                store: SQLiteStore,
                serializer: PickleSerializer,
            ) -> None:
                self._ref = ref
                self._store = store
                self._serializer = serializer

            def __cashet_ref__(self) -> ObjectRef:
                return self._ref

            def load(self) -> Any:
                data = self._store.get_blob(self._ref)
                return self._serializer.loads(data)

        def producer() -> int:
            return 42

        def consumer(x: int) -> int:
            return x * 2

        pref = client.submit(producer)
        custom = CustomRef(pref._ref, client.store, client.serializer)
        result = client.submit(consumer, custom)
        assert result.load() == 84

    def test_ambiguous_short_hash_raises(self, client: Client) -> None:
        for i in range(20):

            def make_val(idx: int = i) -> int:
                return idx

            client.submit(make_val, _cache=False)

        hashes = [c.hash for c in client.log(limit=50)]
        assert len(hashes) == 20
        from collections import Counter

        first_digits = Counter(h[0] for h in hashes)
        ambiguous_digit = next(d for d, count in first_digits.items() if count > 1)

        with pytest.raises(ValueError, match="Ambiguous prefix"):
            client.store.get_commit(ambiguous_digit)
        with pytest.raises(ValueError, match="Ambiguous prefix"):
            client.store.get_history(ambiguous_digit)

    def test_invalid_hash_prefix_does_not_wildcard_match(self, client: Client) -> None:
        def val() -> int:
            return 1

        ref = client.submit(val)
        assert client.show("_") is None
        assert client.history("_") == []
        assert client.show(ref.commit_hash) is not None


class TestTags:
    def test_submit_with_tags(self, client: Client) -> None:
        def add(x: int, y: int) -> int:
            return x + y

        ref = client.submit(add, 1, 2, _tags={"env": "test"})
        assert ref.load() == 3
        commit = client.log(limit=1)[0]
        assert commit.tags == {"env": "test"}

    def test_task_decorator_with_tags(self, client: Client) -> None:
        @client.task(tags={"type": "demo"})
        def greet(name: str) -> str:
            return f"hi {name}"

        ref = greet("world")
        assert ref.load() == "hi world"
        commit = client.log(limit=1)[0]
        assert commit.tags == {"type": "demo"}

    def test_log_filters_by_tags(self, client: Client) -> None:
        def a() -> int:
            return 1

        def b() -> int:
            return 2

        client.submit(a, _tags={"run": "x"})
        client.submit(b, _tags={"run": "y"})

        all_commits = client.log()
        assert len(all_commits) == 2

        filtered = client.log(tags={"run": "x"})
        assert len(filtered) == 1
        assert "a" in filtered[0].task_def.func_name

    def test_list_commits_with_multiple_tags(self, client: Client) -> None:
        def a() -> int:
            return 1

        def b() -> int:
            return 2

        client.submit(a, _tags={"run": "x", "version": "1"})
        client.submit(b, _tags={"run": "x", "version": "2"})

        filtered = client.log(tags={"run": "x", "version": "1"})
        assert len(filtered) == 1
        assert "a" in filtered[0].task_def.func_name

    def test_log_filters_by_bare_key_tag(self, client: Client) -> None:
        def a() -> int:
            return 1

        def b() -> int:
            return 2

        client.submit(a, _tags={"env": "prod"})
        client.submit(b, _tags={})

        filtered = client.log(tags={"env": None})
        assert len(filtered) == 1
        assert "a" in filtered[0].task_def.func_name

    def test_log_filters_by_empty_string_tag_value(self, client: Client) -> None:
        def a() -> int:
            return 1

        def b() -> int:
            return 2

        client.submit(a, _tags={"env": ""})
        client.submit(b, _tags={"env": "prod"})

        filtered = client.log(tags={"env": ""})
        assert len(filtered) == 1
        assert "a" in filtered[0].task_def.func_name

    def test_log_accepts_string_status(self, client: Client) -> None:
        def boom() -> None:
            raise ValueError("fail")

        with pytest.raises(RuntimeError):
            client.submit(boom)

        filtered = client.log(status="failed")
        assert len(filtered) == 1

    def test_log_rejects_invalid_string_status(self, client: Client) -> None:
        with pytest.raises(ValueError, match="Invalid status"):
            client.log(status="nope")


class TestDeleteCommit:
    def test_rm_deletes_commit(self, client: Client) -> None:
        def val() -> int:
            return 42

        client.submit(val, _cache=False)
        commit_hash = client.log(limit=1)[0].hash
        assert client.stats()["total_commits"] == 1

        assert client.rm(commit_hash) is True
        assert client.stats()["total_commits"] == 0
        assert client.show(commit_hash) is None

    def test_rm_removes_orphan_blobs(self, client: Client) -> None:
        def payload() -> bytes:
            return b"orphan_me" * 100

        client.submit(payload, _cache=False)
        commit_hash = client.log(limit=1)[0].hash
        before = client.stats()["stored_objects"]
        assert before >= 1

        client.rm(commit_hash)
        assert client.stats()["stored_objects"] == 0

    def test_rm_preserves_shared_blobs(self, client: Client) -> None:
        def payload_a() -> bytes:
            return b"shared_payload" * 100

        def payload_b() -> bytes:
            return b"shared_payload" * 100

        client.submit(payload_a, _cache=False)
        client.submit(payload_b, _cache=False)
        commits = client.log(limit=2)
        assert client.stats()["stored_objects"] == 1

        client.rm(commits[0].hash)
        assert client.stats()["stored_objects"] == 1
        assert client.show(commits[1].hash) is not None

        client.rm(commits[1].hash)
        assert client.stats()["stored_objects"] == 0

    def test_rm_with_short_hash(self, client: Client) -> None:
        def val() -> int:
            return 1

        client.submit(val, _cache=False)
        commit_hash = client.log(limit=1)[0].hash
        short = commit_hash[:12]
        assert client.rm(short) is True
        assert client.show(commit_hash) is None

    def test_rm_ambiguous_short_hash_raises(self, client: Client) -> None:
        for i in range(20):

            def make_val(idx: int = i) -> int:
                return idx

            client.submit(make_val, _cache=False)

        hashes = [c.hash for c in client.log(limit=50)]
        from collections import Counter

        first_digits = Counter(h[0] for h in hashes)
        ambiguous = next(d for d, count in first_digits.items() if count > 1)

        with pytest.raises(ValueError, match="Ambiguous prefix"):
            client.rm(ambiguous)

        # Verify the rolled-back transaction does not poison the next write.
        def after() -> int:
            return 99

        ref = client.submit(after)
        assert ref.load() == 99

    def test_rm_missing_returns_false(self, client: Client) -> None:
        assert client.rm("deadbeef00000000000000000000000000000000") is False

    def test_rm_invalid_hash_prefix_does_not_delete(self, client: Client) -> None:
        def val() -> int:
            return 1

        ref = client.submit(val)
        assert client.rm("_") is False
        assert client.show(ref.commit_hash) is not None

    def test_rm_clears_parent_hash_of_child(self, client: Client) -> None:
        def val() -> int:
            return 1

        client.submit(val, _cache=False)
        client.submit(val, _cache=False)
        commits = client.log(limit=2)
        assert len(commits) == 2
        older = commits[1]
        newer = commits[0]
        assert newer.parent_hash == older.hash

        client.rm(older.hash)
        newer_after = client.show(newer.hash)
        assert newer_after is not None
        assert newer_after.parent_hash is None

    def test_evict_clears_parent_hash_of_child(self, client: Client) -> None:
        def val() -> int:
            return 1

        client.submit(val, _cache=False)
        client.submit(val, _cache=False)
        commits = client.log(limit=2)
        older = commits[1]
        newer = commits[0]
        assert newer.parent_hash == older.hash

        conn = client.store._connect()
        old_time = (datetime.now(UTC) - timedelta(days=1)).isoformat()
        conn.execute(
            "UPDATE commits SET last_accessed_at = ? WHERE hash = ?",
            (old_time, older.hash),
        )

        client.store.evict(datetime.now(UTC) - timedelta(hours=12))
        newer_after = client.show(newer.hash)
        assert newer_after is not None
        assert newer_after.parent_hash is None


class TestSizeAwareGC:
    def test_gc_max_size_evicts_oldest(self, client: Client) -> None:
        def make_bytes(n: int) -> bytes:
            return b"x" * n

        ref1 = client.submit(make_bytes, 5000, _cache=False)
        ref2 = client.submit(make_bytes, 5000, _cache=False)
        assert client.stats()["disk_bytes"] > 0

        client.gc(max_size_bytes=1)
        assert client.show(ref1.hash) is None
        assert client.show(ref2.hash) is None

    def test_gc_max_size_respects_limit(self, client: Client) -> None:
        def make_bytes(n: int) -> bytes:
            return b"x" * n

        client.submit(make_bytes, 100, _cache=False)
        client.submit(make_bytes, 100, _cache=False)
        client.submit(make_bytes, 100, _cache=False)
        initial_stats = client.stats()
        initial_size = initial_stats["disk_bytes"]
        assert initial_size > 0

        # Evict until under a very small limit
        client.gc(max_size_bytes=1)
        final_stats = client.stats()
        assert final_stats["disk_bytes"] <= 1
        assert final_stats["total_commits"] == 0

    def test_gc_max_size_does_not_walk_stats_per_commit(
        self, client: Client, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def make_bytes(n: int) -> bytes:
            return b"x" * n

        client.submit(make_bytes, 100, _cache=False)
        client.submit(make_bytes, 100, _cache=False)
        client.submit(make_bytes, 100, _cache=False)
        calls = 0
        original = client.store.stats

        def counted_stats() -> dict[str, int]:
            nonlocal calls
            calls += 1
            return original()

        monkeypatch.setattr(client.store, "stats", counted_stats)
        client.gc(max_size_bytes=1)
        assert calls == 0

    def test_gc_combines_time_and_size(self, client: Client) -> None:
        def make_bytes(n: int) -> bytes:
            return b"x" * n

        client.submit(make_bytes, 100, _cache=False)
        stats = client.stats()
        assert stats["total_commits"] == 1

        # Time-based eviction keeps it (recent), size-based should evict it
        client.gc(older_than=timedelta(days=1), max_size_bytes=1)
        assert client.stats()["total_commits"] == 0


class TestTTL:
    def test_ttl_roundtrip(self, client: Client) -> None:
        def val() -> int:
            return 1

        ref = client.submit(val, _ttl=3600)
        assert ref.load() == 1
        commit = client.show(ref.commit_hash)
        assert commit is not None
        assert commit.expires_at is not None
        assert commit.expires_at > datetime.now(UTC)
        assert commit.task_def.ttl == timedelta(seconds=3600)

    def test_ttl_expiration_causes_reexecution(self, client: Client) -> None:
        call_count = 0

        def val() -> int:
            nonlocal call_count
            call_count += 1
            return call_count

        ref1 = client.submit(val, _ttl=0.05)
        assert ref1.load() == 1

        # Should still be cached immediately
        ref2 = client.submit(val, _ttl=0.05)
        assert ref2.load() == 1
        assert ref1.commit_hash == ref2.commit_hash

        # Wait for TTL to expire
        import time

        time.sleep(0.1)

        ref3 = client.submit(val, _ttl=0.05)
        assert ref3.load() == 2

    def test_ttl_on_task_decorator(self, client: Client) -> None:
        @client.task(ttl=3600)
        def val() -> int:
            return 1

        ref = val()
        assert ref.load() == 1
        commit = client.show(ref.commit_hash)
        assert commit is not None
        assert commit.task_def.ttl == timedelta(seconds=3600)
        assert commit.expires_at is not None


class TestTagInvalidation:
    def test_invalidate_deletes_matching_tags(self, client: Client) -> None:
        def a(x: int) -> int:
            return x

        def b(x: int) -> int:
            return x + 1

        ref_a = client.submit(a, 1, _tags={"model": "v1"})
        ref_b = client.submit(b, 1, _tags={"model": "v1"})
        ref_c = client.submit(a, 2, _tags={"model": "v2"})

        assert ref_a.load() == 1
        assert ref_b.load() == 2
        assert ref_c.load() == 2

        deleted = client.invalidate({"model": "v1"})
        assert deleted == 2

        assert client.show(ref_a.commit_hash) is None
        assert client.show(ref_b.commit_hash) is None
        assert client.show(ref_c.commit_hash) is not None

    def test_invalidate_bare_key_deletes_any_value(self, client: Client) -> None:
        def a(x: int) -> int:
            return x

        def b(x: int) -> int:
            return x + 1

        ref_a = client.submit(a, 1, _tags={"model": "v1"})
        ref_b = client.submit(b, 1, _tags={"model": "v2"})
        ref_c = client.submit(a, 2, _tags={"other": "x"})

        deleted = client.invalidate({"model": None})
        assert deleted == 2

        assert client.show(ref_a.commit_hash) is None
        assert client.show(ref_b.commit_hash) is None
        assert client.show(ref_c.commit_hash) is not None

    def test_invalidate_no_match_returns_zero(self, client: Client) -> None:
        def a() -> int:
            return 1

        ref = client.submit(a, _tags={"model": "v1"})
        deleted = client.invalidate({"model": "v2"})
        assert deleted == 0
        assert client.show(ref.commit_hash) is not None


class TestMigrations:
    _BASE_COLUMNS: ClassVar[list[str]] = [
        "hash", "fingerprint", "func_name", "func_hash", "args_hash",
        "args_snapshot", "func_source", "dep_versions", "cache", "input_refs",
        "output_hash", "output_size", "output_tier", "parent_hash",
        "status", "error", "tags", "created_at",
    ]
    _MIGRATED_COLUMNS: ClassVar[list[str]] = [
        "last_accessed_at", "retries", "force", "timeout_seconds",
        "expires_at", "ttl_seconds", "claimed_at",
    ]

    def _build_base_db(self, db_path: str) -> None:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS commits (
                hash TEXT PRIMARY KEY,
                fingerprint TEXT NOT NULL,
                func_name TEXT NOT NULL,
                func_hash TEXT NOT NULL,
                args_hash TEXT NOT NULL,
                args_snapshot BLOB,
                func_source TEXT,
                dep_versions TEXT,
                cache INTEGER NOT NULL DEFAULT 1,
                input_refs TEXT,
                output_hash TEXT,
                output_size INTEGER,
                output_tier TEXT,
                parent_hash TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                error TEXT,
                tags TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (parent_hash) REFERENCES commits(hash)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_fingerprint ON commits(fingerprint)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_func_name ON commits(func_name)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_created_at ON commits(created_at)"
        )
        conn.execute("""
            CREATE TABLE IF NOT EXISTS inline_objects (
                hash TEXT PRIMARY KEY,
                data BLOB NOT NULL
            )
        """)
        conn.close()

    def test_migration_from_base_schema(self, tmp_path: Path) -> None:
        from cashet.store import _SQLiteStoreCore

        root = tmp_path / ".cashet"
        root.mkdir()
        db_path = root / "meta.db"
        (root / "objects").mkdir()

        self._build_base_db(str(db_path))

        core = _SQLiteStoreCore(root)
        try:
            col_names = [
                r[1]
                for r in core._connect().execute(
                    "PRAGMA table_info(commits)"
                ).fetchall()
            ]
            for col in self._BASE_COLUMNS + self._MIGRATED_COLUMNS:
                assert col in col_names, f"Column {col} missing after migration"

            assert "last_accessed_at" in col_names
            assert "idx_last_accessed_at" in [
                r[1] for r in core._connect().execute(
                    "SELECT * FROM sqlite_master WHERE type='index'"
                ).fetchall()
            ]
        finally:
            core.close()

    def test_migration_idempotent(self, tmp_path: Path) -> None:
        from cashet.store import _SQLiteStoreCore

        root = tmp_path / ".cashet"
        root.mkdir()
        db_path = root / "meta.db"
        (root / "objects").mkdir()

        self._build_base_db(str(db_path))

        core1 = _SQLiteStoreCore(root)
        core1.close()

        core2 = _SQLiteStoreCore(root)
        try:
            col_names = [
                r[1]
                for r in core2._connect().execute(
                    "PRAGMA table_info(commits)"
                ).fetchall()
            ]
            for col in self._BASE_COLUMNS + self._MIGRATED_COLUMNS:
                assert col in col_names, f"Column {col} missing after second migration"
        finally:
            core2.close()

    def test_operations_after_migration(self, tmp_path: Path) -> None:
        from cashet.store import _SQLiteStoreCore

        root = tmp_path / ".cashet"
        root.mkdir()
        db_path = root / "meta.db"
        (root / "objects").mkdir()

        self._build_base_db(str(db_path))

        core = _SQLiteStoreCore(root)
        try:
            from cashet.dag import build_commit, resolve_input_refs
            from cashet.hashing import build_task_def
            from cashet.models import TaskStatus

            def add(x: int, y: int) -> int:
                return x + y

            task_def = build_task_def(
                add, (2, 3), {}, tags={"env": "test"}, ttl=timedelta(hours=1)
            )
            input_refs = resolve_input_refs((2,), {})
            commit = build_commit(task_def, input_refs)
            commit.status = TaskStatus.COMPLETED

            blob = core.put_blob(b"result")
            commit.output_ref = blob
            core.put_commit(commit)

            fetched = core.get_commit(commit.hash)
            assert fetched is not None
            assert fetched.task_def.tags == {"env": "test"}
            assert fetched.task_def.ttl == timedelta(hours=1)
            assert fetched.output_ref is not None
            assert core.get_blob(fetched.output_ref) == b"result"
        finally:
            core.close()

    def test_partial_migration_missing_retries_only(self, tmp_path: Path) -> None:
        from cashet.store import _SQLiteStoreCore

        root = tmp_path / ".cashet"
        root.mkdir()
        db_path = root / "meta.db"
        (root / "objects").mkdir()

        self._build_base_db(str(db_path))

        pre_conn = sqlite3.connect(str(db_path))
        pre_conn.execute(
            "ALTER TABLE commits ADD COLUMN last_accessed_at TEXT DEFAULT NULL"
        )
        pre_conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_last_accessed_at ON commits(last_accessed_at)"
        )
        pre_conn.close()

        core = _SQLiteStoreCore(root)
        try:
            col_names = [
                r[1]
                for r in core._connect().execute(
                    "PRAGMA table_info(commits)"
                ).fetchall()
            ]
            for col in self._BASE_COLUMNS + self._MIGRATED_COLUMNS:
                assert col in col_names, f"Column {col} missing: partial migration failed"
        finally:
            core.close()
