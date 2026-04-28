from __future__ import annotations

from pathlib import Path

import pytest
import pytest_asyncio

from cashet.async_client import AsyncClient
from cashet.dag import AsyncResultRef, TaskRef


@pytest_asyncio.fixture
async def async_client(tmp_path: Path) -> AsyncClient:
    return AsyncClient(store_dir=tmp_path / ".cashet")


class TestAsyncClientSubmit:
    async def test_submit_simple_function(self, async_client: AsyncClient) -> None:
        def add(x: int, y: int) -> int:
            return x + y

        ref = await async_client.submit(add, 3, 4)
        result = await ref.load()
        assert result == 7

    async def test_submit_cached(self, async_client: AsyncClient) -> None:
        call_count = 0

        def expensive(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * x

        ref1 = await async_client.submit(expensive, 5)
        assert await ref1.load() == 25
        assert call_count == 1

        ref2 = await async_client.submit(expensive, 5)
        assert await ref2.load() == 25
        assert call_count == 1
        assert ref1.hash == ref2.hash

    async def test_submit_with_kwargs(self, async_client: AsyncClient) -> None:
        def greet(name: str, greeting: str = "hello") -> str:
            return f"{greeting}, {name}"

        ref = await async_client.submit(greet, "world", greeting="hi")
        assert await ref.load() == "hi, world"

    async def test_async_result_ref_chaining(self, async_client: AsyncClient) -> None:
        def step1() -> int:
            return 10

        def step2(x: int) -> int:
            return x * 3

        r1 = await async_client.submit(step1)
        r2 = await async_client.submit(step2, r1)
        assert await r2.load() == 30
        commit = await async_client.show(r2.commit_hash)
        assert commit is not None
        assert [ref.hash for ref in commit.input_refs] == [r1.hash]


class TestAsyncClientSubmitMany:
    async def test_submit_many_basic(self, async_client: AsyncClient) -> None:
        def add(x: int, y: int) -> int:
            return x + y

        def mul(x: int, y: int) -> int:
            return x * y

        refs = await async_client.submit_many([
            (add, (1, 2)),
            (mul, (3, 4)),
        ])
        assert len(refs) == 2
        assert await refs[0].load() == 3
        assert await refs[1].load() == 12

    async def test_submit_many_with_taskref(self, async_client: AsyncClient) -> None:
        def step1() -> int:
            return 10

        def step2(x: int) -> int:
            return x * 3

        def step3(x: int) -> str:
            return f"result: {x}"

        refs = await async_client.submit_many([
            step1,
            (step2, (TaskRef(0),)),
            (step3, (TaskRef(1),)),
        ])
        assert len(refs) == 3
        assert await refs[0].load() == 10
        assert await refs[1].load() == 30
        assert await refs[2].load() == "result: 30"
        commit = await async_client.show(refs[2].commit_hash)
        assert commit is not None
        assert [ref.hash for ref in commit.input_refs] == [refs[1].hash]

    async def test_submit_many_fan_out(self, async_client: AsyncClient) -> None:
        def gen() -> int:
            return 5

        def double(x: int) -> int:
            return x * 2

        def triple(x: int) -> int:
            return x * 3

        refs = await async_client.submit_many([
            gen,
            (double, (TaskRef(0),)),
            (triple, (TaskRef(0),)),
        ])
        assert len(refs) == 3
        assert await refs[0].load() == 5
        assert await refs[1].load() == 10
        assert await refs[2].load() == 15

    async def test_submit_many_dict(self, async_client: AsyncClient) -> None:
        def add(x: int, y: int) -> int:
            return x + y

        def mul(x: int, y: int) -> int:
            return x * y

        refs = await async_client.submit_many({
            "first": (add, (1, 2)),
            "second": (mul, (TaskRef("first"), 4)),
        })
        assert isinstance(refs, dict)
        assert await refs["first"].load() == 3
        assert await refs["second"].load() == 12

    async def test_submit_many_parallel(self, async_client: AsyncClient) -> None:
        import time

        order: list[int] = []

        def step1() -> int:
            time.sleep(0.05)
            order.append(1)
            return 10

        def step2(x: int) -> int:
            order.append(2)
            return x * 2

        refs = await async_client.submit_many([
            step1,
            (step2, (TaskRef(0),)),
        ], max_workers=2)
        assert await refs[1].load() == 20
        assert order == [1, 2]

    async def test_submit_many_parallel_respects_max_workers(
        self, async_client: AsyncClient
    ) -> None:
        import threading
        import time

        active = 0
        max_active = 0
        lock = threading.Lock()

        def work(x: int) -> int:
            nonlocal active, max_active
            with lock:
                active += 1
                max_active = max(max_active, active)
            time.sleep(0.05)
            with lock:
                active -= 1
            return x

        refs = await async_client.submit_many(
            [(work, (i,)) for i in range(5)], max_workers=2
        )
        assert [await ref.load() for ref in refs] == list(range(5))
        assert max_active <= 2

    async def test_submit_many_rejects_zero_max_workers(
        self, async_client: AsyncClient
    ) -> None:
        def val() -> int:
            return 1

        with pytest.raises(ValueError, match="max_workers"):
            await async_client.submit_many([val], max_workers=0)


class TestAsyncClientOperations:
    async def test_log(self, async_client: AsyncClient) -> None:
        def f(x: int) -> int:
            return x

        await async_client.submit(f, 1)
        await async_client.submit(f, 2)
        log = await async_client.log()
        assert len(log) == 2

    async def test_show(self, async_client: AsyncClient) -> None:
        def greet(name: str) -> str:
            return f"hello, {name}"

        ref = await async_client.submit(greet, "world")
        commit = await async_client.show(ref.commit_hash)
        assert commit is not None
        assert "greet" in commit.task_def.func_name

    async def test_get(self, async_client: AsyncClient) -> None:
        def val() -> int:
            return 42

        ref = await async_client.submit(val)
        result = await async_client.get(ref.commit_hash)
        assert result == 42

    async def test_stats(self, async_client: AsyncClient) -> None:
        def f() -> int:
            return 1

        await async_client.submit(f)
        s = await async_client.stats()
        assert s["total_commits"] >= 1

    async def test_delete_and_gc(self, async_client: AsyncClient) -> None:
        def make_val(x: int) -> int:
            return x

        ref = await async_client.submit(make_val, 1)
        assert await async_client.rm(ref.commit_hash) is True
        assert await async_client.show(ref.commit_hash) is None

    async def test_clear(self, async_client: AsyncClient) -> None:
        def make_val(x: int) -> int:
            return x

        await async_client.submit(make_val, 1)
        deleted = await async_client.clear()
        assert deleted >= 1
        assert (await async_client.stats())["total_commits"] == 0


class TestAsyncTaskDecorator:
    async def test_task_decorator_registers(self, async_client: AsyncClient) -> None:
        @async_client.task
        def my_task(x: int) -> int:
            return x + 1

        assert any("my_task" in name for name in async_client._registered_tasks)
        ref = await async_client.submit(my_task, 5)
        assert await ref.load() == 6

    async def test_task_with_custom_name(self, async_client: AsyncClient) -> None:
        @async_client.task(name="custom_name")
        def unnamed() -> str:
            return "hello"

        ref = await unnamed()
        commit = await async_client.show(ref.commit_hash)
        assert commit is not None
        assert commit.task_def.func_name == "custom_name"
        assert len(await async_client.log(func_name="custom_name")) == 1

    async def test_task_with_cache_false(self, async_client: AsyncClient) -> None:
        counter = 0

        @async_client.task(cache=False)
        def non_cached() -> int:
            nonlocal counter
            counter += 1
            return counter

        ref1 = await async_client.submit(non_cached)
        ref2 = await async_client.submit(non_cached)
        assert await ref1.load() == 1
        assert await ref2.load() == 2

    async def test_task_decorator_callable_returns_async_result_ref(
        self, async_client: AsyncClient
    ) -> None:
        @async_client.task
        def add(x: int, y: int) -> int:
            return x + y

        ref = await add(2, 3)
        assert isinstance(ref, AsyncResultRef)
        assert await ref.load() == 5

    async def test_task_decorator_callable_with_kwargs(
        self, async_client: AsyncClient
    ) -> None:
        @async_client.task
        def greet(name: str, greeting: str = "hello") -> str:
            return f"{greeting}, {name}"

        ref = await greet("world", greeting="hi")
        assert isinstance(ref, AsyncResultRef)
        assert await ref.load() == "hi, world"

    async def test_task_decorator_preserves_metadata(
        self, async_client: AsyncClient
    ) -> None:
        @async_client.task(name="my_task")
        def do_thing(x: int) -> int:
            """A docstring."""
            return x * 2

        assert do_thing.__name__ == "do_thing"
        assert do_thing.__doc__ == "A docstring."
        assert hasattr(do_thing, "_cashet_wrapped_func")


class TestAsyncContextManager:
    async def test_async_with(self) -> None:
        from pathlib import Path

        store_dir = Path("/tmp/cashet_test_async_ctx")
        store_dir.mkdir(parents=True, exist_ok=True)
        async with AsyncClient(store_dir=store_dir) as client:
            def val() -> int:
                return 42

            ref = await client.submit(val)
            assert await ref.load() == 42


class TestAsyncDiff:
    async def test_diff_small_outputs(self, async_client: AsyncClient) -> None:
        def make_val(x: int) -> int:
            return x

        await async_client.submit(make_val, 1, _cache=False)
        await async_client.submit(make_val, 2, _cache=False)
        log = await async_client.log()
        d = await async_client.diff(log[0].hash, log[1].hash)
        assert d["output_changed"] is True
        assert "a_output_repr" in d
        assert "b_output_repr" in d

    async def test_diff_missing_commit_raises(self, async_client: AsyncClient) -> None:
        with pytest.raises(KeyError, match="not found"):
            await async_client.diff("deadbeef", "cafebabe")
