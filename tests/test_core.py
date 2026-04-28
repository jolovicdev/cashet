from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from cashet import Client, ResultRef, TaskError, TaskRef, TaskStatus
from cashet.dag import build_commit, resolve_input_refs
from cashet.models import Commit, TaskDef
from cashet.models import TaskStatus as ModelTaskStatus


class TestBasicSubmit:
    def test_submit_simple_function(self, client: Client) -> None:
        def add(x: int, y: int) -> int:
            return x + y

        ref = client.submit(add, 3, 4)
        assert isinstance(ref, ResultRef)
        assert ref.load() == 7

    def test_submit_with_kwargs(self, client: Client) -> None:
        def greet(name: str, greeting: str = "hello") -> str:
            return f"{greeting}, {name}"

        ref = client.submit(greet, "world", greeting="hi")
        assert ref.load() == "hi, world"

    def test_submit_returns_none(self, client: Client) -> None:
        def noop() -> None:
            pass

        ref = client.submit(noop)
        assert ref.load() is None

    def test_submit_complex_return(self, client: Client) -> None:
        def make_dict() -> dict[str, Any]:
            return {"a": [1, 2, 3], "b": {"nested": True}}

        ref = client.submit(make_dict)
        result = ref.load()
        assert result == {"a": [1, 2, 3], "b": {"nested": True}}

    def test_sync_executor_receives_sync_store_with_default_store(
        self, store_dir: Path
    ) -> None:
        class RecordingExecutor:
            def __init__(self) -> None:
                self.seen_sync_store = False

            def submit(
                self,
                func: Any,
                args: tuple[Any, ...],
                kwargs: dict[str, Any],
                task_def: TaskDef,
                store: Any,
                serializer: Any,
            ) -> tuple[Commit, bool]:
                result = func(*args, **kwargs)
                output_ref = store.put_blob(serializer.dumps(result))
                self.seen_sync_store = not hasattr(output_ref, "__await__")
                commit = build_commit(task_def, resolve_input_refs(args, kwargs))
                commit.output_ref = output_ref
                commit.status = ModelTaskStatus.COMPLETED
                store.put_commit(commit)
                return commit, False

        executor = RecordingExecutor()
        client = Client(store_dir=store_dir, executor=executor)

        def add(x: int, y: int) -> int:
            return x + y

        ref = client.submit(add, 2, 5)
        assert ref.load() == 7
        assert executor.seen_sync_store is True


class TestDeduplication:
    def test_same_call_returns_cached(self, client: Client) -> None:
        call_count = 0

        def expensive(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * x

        ref1 = client.submit(expensive, 5)
        assert call_count == 1
        assert ref1.load() == 25

        ref2 = client.submit(expensive, 5)
        assert call_count == 1
        assert ref2.load() == 25

        assert ref1.hash == ref2.hash

    def test_different_args_different_results(self, client: Client) -> None:
        def double(x: int) -> int:
            return x * 2

        ref1 = client.submit(double, 5)
        ref2 = client.submit(double, 10)
        assert ref1.hash != ref2.hash
        assert ref1.load() == 10
        assert ref2.load() == 20

    def test_dedup_persists_across_client_instances(self, store_dir: Path) -> None:
        def compute(x: int) -> int:
            return x**3

        c1 = Client(store_dir=store_dir)
        ref1 = c1.submit(compute, 7)
        assert ref1.load() == 343

        c2 = Client(store_dir=store_dir)
        ref2 = c2.submit(compute, 7)
        assert ref2.load() == 343
        assert ref1.hash == ref2.hash


class TestTaskDecorator:
    def test_task_decorator_registers(self, client: Client) -> None:
        @client.task
        def my_task(x: int) -> int:
            return x + 1

        assert any("my_task" in name for name in client._registered_tasks)
        ref = client.submit(my_task, 5)
        assert ref.load() == 6

    def test_task_with_cache_false(self, client: Client) -> None:
        counter = 0

        @client.task(cache=False)
        def non_cached() -> int:
            nonlocal counter
            counter += 1
            return counter

        ref1 = client.submit(non_cached)
        ref2 = client.submit(non_cached)
        assert ref1.load() == 1
        assert ref2.load() == 2

    def test_task_with_custom_name(self, client: Client) -> None:
        @client.task(name="custom_name")
        def unnamed() -> str:
            return "hello"

        assert "custom_name" in client._registered_tasks
        ref = unnamed()
        commit = client.show(ref.commit_hash)
        assert commit is not None
        assert commit.task_def.func_name == "custom_name"
        assert len(client.log(func_name="custom_name")) == 1

    def test_task_decorator_callable_returns_result_ref(self, client: Client) -> None:
        @client.task
        def add(x: int, y: int) -> int:
            return x + y

        ref = add(2, 3)
        assert isinstance(ref, ResultRef)
        assert ref.load() == 5

    def test_task_decorator_callable_with_kwargs(self, client: Client) -> None:
        @client.task
        def greet(name: str, greeting: str = "hello") -> str:
            return f"{greeting}, {name}"

        ref = greet("world", greeting="hi")
        assert isinstance(ref, ResultRef)
        assert ref.load() == "hi, world"

    def test_task_decorator_callable_respects_cache_false(self, client: Client) -> None:
        counter = 0

        @client.task(cache=False)
        def uncached() -> int:
            nonlocal counter
            counter += 1
            return counter

        r1 = uncached()
        r2 = uncached()
        assert r1.load() == 1
        assert r2.load() == 2

    def test_submit_still_works_with_decorated_task(self, client: Client) -> None:
        @client.task
        def mul(x: int, y: int) -> int:
            return x * y

        ref = client.submit(mul, 3, 4)
        assert isinstance(ref, ResultRef)
        assert ref.load() == 12

    def test_task_decorator_preserves_metadata(self, client: Client) -> None:
        @client.task(name="my_task")
        def do_thing(x: int) -> int:
            """A docstring."""
            return x * 2

        assert do_thing.__name__ == "do_thing"
        assert do_thing.__doc__ == "A docstring."
        assert hasattr(do_thing, "_cashet_wrapped_func")


class TestDAGResolution:
    def test_result_ref_as_input(self, client: Client) -> None:
        def gen_data() -> list[int]:
            return [1, 2, 3]

        def double(data: list[int]) -> list[int]:
            return [x * 2 for x in data]

        data_ref = client.submit(gen_data)
        assert isinstance(data_ref, ResultRef)

        result_ref = client.submit(double, data_ref)
        assert result_ref.load() == [2, 4, 6]

    def test_chained_pipeline(self, client: Client) -> None:
        def step1() -> int:
            return 10

        def step2(x: int) -> int:
            return x * 3

        def step3(x: int) -> str:
            return f"result: {x}"

        r1 = client.submit(step1)
        r2 = client.submit(step2, r1)
        r3 = client.submit(step3, r2)
        assert r3.load() == "result: 30"


class TestLargeOutputs:
    def test_large_blob_compression(self, client: Client) -> None:
        def make_large() -> bytes:
            return b"x" * (1024 * 100)

        ref = client.submit(make_large)
        result = ref.load()
        assert len(result) == 1024 * 100
        assert result == b"x" * (1024 * 100)

    def test_large_dict(self, client: Client) -> None:
        def big_map() -> dict[int, str]:
            return {i: f"value_{i}" for i in range(10000)}

        ref = client.submit(big_map)
        result = ref.load()
        assert len(result) == 10000
        assert result[9999] == "value_9999"


class TestFailureHandling:
    def test_failing_function(self, client: Client) -> None:
        def boom() -> None:
            raise ValueError("kaboom")

        with pytest.raises(RuntimeError, match="kaboom"):
            client.submit(boom)

        log = client.log()
        assert any(c.status == TaskStatus.FAILED for c in log)

    def test_show_failed_commit(self, client: Client) -> None:
        def crash() -> None:
            raise RuntimeError("deliberate crash")

        with pytest.raises(RuntimeError):
            client.submit(crash)

        log = client.log(status=TaskStatus.FAILED)
        assert len(log) == 1
        assert log[0].error is not None
        assert "deliberate crash" in log[0].error


class TestContentAddressableDedup:
    def test_same_output_same_blob(self, client: Client) -> None:
        def constant() -> int:
            return 42

        ref1 = client.submit(constant)
        ref2 = client.submit(constant, _cache=False)
        assert ref1.hash == ref2.hash

    def test_blob_not_duplicated_on_disk(self, store_dir: Path) -> None:
        def make_bytes() -> bytes:
            return b"unique content" * 1000

        c1 = Client(store_dir=store_dir)
        c1.submit(make_bytes)
        c1.submit(make_bytes, _cache=False)

        obj_dir = store_dir / "objects"
        blob_files: list[Path] = []
        for prefix_dir in obj_dir.iterdir():
            if prefix_dir.is_dir():
                blob_files.extend(prefix_dir.iterdir())
        unique_hashes: set[str] = set()
        for f in blob_files:
            unique_hashes.add(f.name)
        assert len(unique_hashes) <= 3


class TestNonDeterministicOptOut:
    def test_cache_false_always_reruns(self, client: Client) -> None:
        counter = 0

        def rand_val() -> int:
            nonlocal counter
            counter += 1
            return counter

        r1 = client.submit(rand_val, _cache=False)
        r2 = client.submit(rand_val, _cache=False)
        assert r1.load() == 1
        assert r2.load() == 2

    def test_cache_decorator_flag(self, client: Client) -> None:
        counter = 0

        @client.task(cache=False)
        def uncached() -> int:
            nonlocal counter
            counter += 1
            return counter

        r1 = client.submit(uncached)
        r2 = client.submit(uncached)
        assert r1.load() != r2.load()


class TestForceRerun:
    def test_force_reruns_cached_task(self, client: Client) -> None:
        call_count = 0

        def compute(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        r1 = client.submit(compute, 5)
        assert r1.load() == 10
        assert call_count == 1

        r2 = client.submit(compute, 5, _force=True)
        assert r2.load() == 10
        assert call_count == 2

    def test_force_decorator(self, client: Client) -> None:
        call_count = 0

        @client.task(force=True)
        def compute() -> int:
            nonlocal call_count
            call_count += 1
            return call_count

        r1 = client.submit(compute)
        r2 = client.submit(compute)
        assert r1.load() == 1
        assert r2.load() == 2

    def test_force_preserves_parent_hash(self, client: Client) -> None:
        def compute(x: int) -> int:
            return x + 1

        r1 = client.submit(compute, 5)
        r2 = client.submit(compute, 5, _force=True)
        assert r2.commit_hash != r1.commit_hash
        c2 = client.show(r2.commit_hash)
        assert c2 is not None
        assert c2.parent_hash == r1.commit_hash


class TestTimeout:
    def test_task_times_out(self, client: Client) -> None:
        import time

        def slow() -> str:
            time.sleep(5)
            return "done"

        with pytest.raises(TaskError, match="TimeoutError"):
            client.submit(slow, _timeout=0.1)

    def test_task_within_timeout_succeeds(self, client: Client) -> None:
        import time

        def fast() -> str:
            time.sleep(0.01)
            return "done"

        ref = client.submit(fast, _timeout=1)
        assert ref.load() == "done"

    def test_timeout_with_retry(self, client: Client) -> None:
        import time

        attempts = 0

        def flaky() -> str:
            nonlocal attempts
            attempts += 1
            if attempts < 2:
                time.sleep(5)
            return "ok"

        ref = client.submit(flaky, _timeout=0.1, _retries=1)
        assert ref.load() == "ok"
        assert attempts == 2

    def test_timeout_decorator(self, client: Client) -> None:
        import time

        @client.task(timeout=0.1)
        def slow() -> str:
            time.sleep(5)
            return "done"

        with pytest.raises(TaskError, match="TimeoutError"):
            slow()


class TestParentHashLineage:
    def test_second_commit_has_parent(self, client: Client) -> None:
        def compute(x: int) -> int:
            return x * x

        r1 = client.submit(compute, 5)
        r2 = client.submit(compute, 5)
        assert r1.hash == r2.hash

    def test_cache_false_creates_lineage(self, client: Client) -> None:
        counter = 0

        def compute(x: int) -> int:
            nonlocal counter
            counter += 1
            return x + counter

        client.submit(compute, 5, _cache=False)
        client.submit(compute, 5, _cache=False)
        log = client.log()
        assert len(log) == 2

        second = log[0]
        first = log[1]
        assert second.parent_hash == first.hash

    def test_history_follows_lineage(self, client: Client) -> None:
        counter = 0

        def compute(x: int) -> int:
            nonlocal counter
            counter += 1
            return x + counter

        client.submit(compute, 5, _cache=False)
        client.submit(compute, 5, _cache=False)
        client.submit(compute, 5, _cache=False)

        log = client.log()
        history = client.history(log[0].hash)
        assert len(history) == 3

        assert history[2].parent_hash == history[1].hash
        assert history[1].parent_hash == history[0].hash


class TestSubmitMany:
    def test_submit_many_basic(self, client: Client) -> None:
        def add(x: int, y: int) -> int:
            return x + y

        def mul(x: int, y: int) -> int:
            return x * y

        refs = client.submit_many([
            (add, (1, 2)),
            (mul, (3, 4)),
        ])
        assert len(refs) == 2
        assert refs[0].load() == 3
        assert refs[1].load() == 12

    def test_submit_many_with_taskref(self, client: Client) -> None:
        def step1() -> int:
            return 10

        def step2(x: int) -> int:
            return x * 3

        def step3(x: int) -> str:
            return f"result: {x}"

        refs = client.submit_many([
            step1,
            (step2, (TaskRef(0),)),
            (step3, (TaskRef(1),)),
        ])
        assert len(refs) == 3
        assert refs[0].load() == 10
        assert refs[1].load() == 30
        assert refs[2].load() == "result: 30"

    def test_submit_many_fan_out(self, client: Client) -> None:
        def gen() -> int:
            return 5

        def double(x: int) -> int:
            return x * 2

        def triple(x: int) -> int:
            return x * 3

        refs = client.submit_many([
            gen,
            (double, (TaskRef(0),)),
            (triple, (TaskRef(0),)),
        ])
        assert len(refs) == 3
        assert refs[0].load() == 5
        assert refs[1].load() == 10
        assert refs[2].load() == 15

    def test_submit_many_circular_raises(self, client: Client) -> None:
        def noop(x: int) -> int:
            return x

        with pytest.raises(ValueError, match="Circular dependency"):
            client.submit_many([
                (noop, (TaskRef(1),)),
                (noop, (TaskRef(0),)),
            ])

    def test_submit_many_self_ref_raises(self, client: Client) -> None:
        def noop(x: int) -> int:
            return x

        with pytest.raises(ValueError, match="cannot depend on itself"):
            client.submit_many([
                (noop, (TaskRef(0),)),
            ])

    def test_submit_many_out_of_range_raises(self, client: Client) -> None:
        def noop(x: int) -> int:
            return x

        with pytest.raises(ValueError, match="not found"):
            client.submit_many([
                (noop, (TaskRef(5),)),
            ])

    def test_commit_repr(self, client: Client) -> None:
        def add(x: int, y: int) -> int:
            return x + y

        client.submit(add, 1, 2)
        commit = client.log(limit=1)[0]
        rep = repr(commit)
        assert "Commit(" in rep
        assert "add" in rep
        assert "completed" in rep or "cached" in rep

    def test_submit_many_dict(self, client: Client) -> None:
        def add(x: int, y: int) -> int:
            return x + y

        def mul(x: int, y: int) -> int:
            return x * y

        refs = client.submit_many({
            "first": (add, (1, 2)),
            "second": (mul, (TaskRef("first"), 4)),
        })
        assert isinstance(refs, dict)
        assert refs["first"].load() == 3
        assert refs["second"].load() == 12

    def test_submit_many_dict_circular_raises(self, client: Client) -> None:
        def noop(x: int) -> int:
            return x

        with pytest.raises(ValueError, match="Circular dependency"):
            client.submit_many({
                "a": (noop, (TaskRef("b"),)),
                "b": (noop, (TaskRef("a"),)),
            })

    def test_submit_many_dict_missing_ref_raises(self, client: Client) -> None:
        def noop(x: int) -> int:
            return x

        with pytest.raises(ValueError, match="not found"):
            client.submit_many({
                "a": (noop, (TaskRef("missing"),)),
            })

    def test_submit_many_rejects_non_callable(self, client: Client) -> None:
        with pytest.raises(TypeError, match="expected callable"):
            client.submit_many(["not a function"])

    def test_submit_many_rejects_non_tuple_args(self, client: Client) -> None:
        def f(x: int) -> int:
            return x

        with pytest.raises(TypeError, match="expected args as tuple"):
            client.submit_many([(f, "not_a_tuple")])

    def test_submit_many_parallel_fan_out(self, client: Client) -> None:
        import threading
        import time

        barrier = threading.Barrier(2)

        def gen() -> int:
            return 5

        def double(x: int) -> int:
            time.sleep(0.05)
            barrier.wait(timeout=2)
            return x * 2

        def triple(x: int) -> int:
            time.sleep(0.05)
            barrier.wait(timeout=2)
            return x * 3

        refs = client.submit_many([
            gen,
            (double, (TaskRef(0),)),
            (triple, (TaskRef(0),)),
        ], max_workers=2)
        assert refs[0].load() == 5
        assert refs[1].load() == 10
        assert refs[2].load() == 15

    def test_submit_many_parallel_order_respected(self, client: Client) -> None:
        import time

        order: list[int] = []

        def step1() -> int:
            time.sleep(0.05)
            order.append(1)
            return 10

        def step2(x: int) -> int:
            order.append(2)
            return x * 2

        refs = client.submit_many([
            step1,
            (step2, (TaskRef(0),)),
        ], max_workers=2)
        assert refs[1].load() == 20
        assert order == [1, 2]


class TestResultRefCommitHash:
    def test_commit_hash_works_with_show(self, client: Client) -> None:
        def add(x: int, y: int) -> int:
            return x + y

        ref = client.submit(add, 1, 2)
        commit = client.show(ref.commit_hash)
        assert commit is not None
        assert "add" in commit.task_def.func_name

    def test_commit_hash_works_with_get(self, client: Client) -> None:
        def val() -> int:
            return 42

        ref = client.submit(val)
        result = client.get(ref.commit_hash)
        assert result == 42

    def test_commit_hash_works_with_history(self, client: Client) -> None:
        def val(x: int) -> int:
            return x

        ref1 = client.submit(val, 5, _cache=False)
        client.submit(val, 5, _cache=False)
        history = client.history(ref1.commit_hash)
        assert len(history) == 2

    def test_commit_hash_works_with_rm(self, client: Client) -> None:
        def val() -> int:
            return 1

        ref = client.submit(val, _cache=False)
        assert client.rm(ref.commit_hash) is True
        assert client.show(ref.commit_hash) is None

    def test_hash_is_blob_not_commit(self, client: Client) -> None:
        def val() -> int:
            return 42

        ref = client.submit(val)
        assert ref.hash != ref.commit_hash
        commit = client.show(ref.commit_hash)
        assert commit is not None
        assert ref.hash == commit.output_ref.hash


class TestRetries:
    def test_retry_succeeds_eventually(self, client: Client) -> None:
        attempts = 0

        def flaky() -> int:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise ValueError("not yet")
            return 42

        ref = client.submit(flaky, _retries=3)
        assert ref.load() == 42
        assert attempts == 3

    def test_retry_exhausted_raises(self, client: Client) -> None:
        attempts = 0

        def always_fails() -> int:
            nonlocal attempts
            attempts += 1
            raise ValueError("always fails")

        with pytest.raises(RuntimeError, match="always fails"):
            client.submit(always_fails, _retries=2)
        assert attempts == 3

    def test_task_decorator_retries(self, client: Client) -> None:
        attempts = 0

        @client.task(retries=2)
        def flaky_task() -> int:
            nonlocal attempts
            attempts += 1
            if attempts < 2:
                raise RuntimeError("retry me")
            return 7

        ref = client.submit(flaky_task)
        assert ref.load() == 7
        assert attempts == 2

    def test_zero_retries_no_retry(self, client: Client) -> None:
        attempts = 0

        def once() -> int:
            nonlocal attempts
            attempts += 1
            raise ValueError("once")

        with pytest.raises(RuntimeError, match="once"):
            client.submit(once, _retries=0)
        assert attempts == 1

    def test_negative_retries_treated_as_zero(self, client: Client) -> None:
        attempts = 0

        def once() -> int:
            nonlocal attempts
            attempts += 1
            raise ValueError("once")

        with pytest.raises(RuntimeError, match="once"):
            client.submit(once, _retries=-1)
        assert attempts == 1


class TestDiffSizeLimit:
    def test_small_outputs_compared_normally(self, client: Client) -> None:
        def make_val(x: int) -> int:
            return x

        client.submit(make_val, 1, _cache=False)
        client.submit(make_val, 2, _cache=False)
        log = client.log()
        d = client.diff(log[0].hash, log[1].hash)
        assert d["output_changed"] is True
        assert "a_output_repr" in d
        assert "b_output_repr" in d

    def test_large_outputs_skip_comparison(self, client: Client) -> None:
        import cashet._client_base as base_mod

        original_limit = base_mod._DIFF_SIZE_LIMIT
        base_mod._DIFF_SIZE_LIMIT = 100
        try:

            def make_big(suffix: str) -> bytes:
                return (suffix * 200).encode()

            client.submit(make_big, "a", _cache=False)
            client.submit(make_big, "b", _cache=False)
            log = client.log()
            d = client.diff(log[0].hash, log[1].hash)
            assert d["output_changed"] == "skipped_large_output"
            assert "a_output_size" in d
            assert "b_output_size" in d
        finally:
            base_mod._DIFF_SIZE_LIMIT = original_limit

    def test_one_large_one_small_skips(self, client: Client) -> None:
        import cashet._client_base as base_mod

        original_limit = base_mod._DIFF_SIZE_LIMIT
        base_mod._DIFF_SIZE_LIMIT = 100
        try:

            def make_small() -> bytes:
                return b"hi"

            def make_big() -> bytes:
                return b"x" * 200

            client.submit(make_small, _cache=False)
            client.submit(make_big, _cache=False)
            log = client.log()
            d = client.diff(log[0].hash, log[1].hash)
            assert d["output_changed"] == "skipped_large_output"
        finally:
            base_mod._DIFF_SIZE_LIMIT = original_limit
