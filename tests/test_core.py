from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from cashet import Client, ResultRef, TaskStatus


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
        import cashet.client as client_mod

        original_limit = client_mod._DIFF_SIZE_LIMIT
        client_mod._DIFF_SIZE_LIMIT = 100
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
            client_mod._DIFF_SIZE_LIMIT = original_limit

    def test_one_large_one_small_skips(self, client: Client) -> None:
        import cashet.client as client_mod

        original_limit = client_mod._DIFF_SIZE_LIMIT
        client_mod._DIFF_SIZE_LIMIT = 100
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
            client_mod._DIFF_SIZE_LIMIT = original_limit


