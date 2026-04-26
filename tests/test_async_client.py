from __future__ import annotations

from pathlib import Path

import pytest_asyncio

from cashet.async_client import AsyncClient
from cashet.dag import TaskRef


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
