from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from cashet import Client
from cashet.hashing import PickleSerializer
from cashet.models import ObjectRef, StorageTier
from cashet.store import SQLiteStore


class TestStoreOperations:
    def test_log(self, client: Client) -> None:
        def f(x: int) -> int:
            return x

        client.submit(f, 1)
        client.submit(f, 2)
        client.submit(f, 3)
        log = client.log()
        assert len(log) == 3

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
        data = b"important data"
        ref = store.put_blob(data)
        obj_path = store.objects_dir / ref.hash[:2] / ref.hash[2:]
        obj_path.write_bytes(b"corrupted garbage data")
        with pytest.raises(ValueError, match="integrity check failed"):
            store.get_blob(ref)

    def test_corrupted_compressed_blob_raises_valueerror(self, store_dir: Path) -> None:
        store = SQLiteStore(store_dir)
        data = b"y" * 1000
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

    def test_rm_missing_returns_false(self, client: Client) -> None:
        assert client.rm("deadbeef00000000000000000000000000000000") is False

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
