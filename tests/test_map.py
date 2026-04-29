from __future__ import annotations

from pathlib import Path

import pytest_asyncio

from cashet import Client
from cashet.async_client import AsyncClient


@pytest_asyncio.fixture
async def async_client(tmp_path: Path) -> AsyncClient:
    client = AsyncClient(store_dir=tmp_path / ".cashet")
    yield client
    await client.close()


def double(x: int) -> int:
    return x * 2


def add(x: int, y: int) -> int:
    return x + y


def greet(name: str, greeting: str = "hello") -> str:
    return f"{greeting} {name}"


class TestSyncMap:
    def test_map_basic(self, client: Client) -> None:
        refs = client.map(double, [1, 2, 3])
        assert len(refs) == 3
        results = [r.load() for r in refs]
        assert results == [2, 4, 6]

    def test_map_with_extra_args(self, client: Client) -> None:
        refs = client.map(add, [1, 2, 3], 10)
        assert [r.load() for r in refs] == [11, 12, 13]

    def test_map_with_kwargs(self, client: Client) -> None:
        refs = client.map(greet, ["alice", "bob"], greeting="hi")
        assert [r.load() for r in refs] == ["hi alice", "hi bob"]

    def test_map_caching(self, client: Client) -> None:
        call_count = 0

        def counter(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        refs1 = client.map(counter, [1, 2])
        assert [r.load() for r in refs1] == [2, 4]
        assert call_count == 2

        refs2 = client.map(counter, [1, 2])
        assert [r.load() for r in refs2] == [2, 4]
        assert call_count == 2

    def test_map_empty(self, client: Client) -> None:
        refs = client.map(double, [])
        assert refs == []


class TestAsyncMap:
    async def test_map_basic(self, async_client: AsyncClient) -> None:
        refs = await async_client.map(double, [1, 2, 3])
        assert len(refs) == 3
        results = [await r.load() for r in refs]
        assert results == [2, 4, 6]

    async def test_map_with_extra_args(self, async_client: AsyncClient) -> None:
        refs = await async_client.map(add, [1, 2, 3], 10)
        assert [await r.load() for r in refs] == [11, 12, 13]

    async def test_map_caching(self, async_client: AsyncClient) -> None:
        call_count = 0

        def counter(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        refs1 = await async_client.map(counter, [1, 2])
        assert [await r.load() for r in refs1] == [2, 4]
        assert call_count == 2

        refs2 = await async_client.map(counter, [1, 2])
        assert [await r.load() for r in refs2] == [2, 4]
        assert call_count == 2

    async def test_map_empty(self, async_client: AsyncClient) -> None:
        refs = await async_client.map(double, [])
        assert refs == []
