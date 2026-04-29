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


def add(x: int, y: int) -> int:
    return x + y


def greet(name: str) -> str:
    return f"hello {name}"


class TestSyncExport:
    def test_export_import_roundtrip(self, client: Client, tmp_path: Path) -> None:
        ref1 = client.submit(add, 1, 2)
        ref2 = client.submit(greet, "world")
        assert ref1.load() == 3
        assert ref2.load() == "hello world"

        archive = tmp_path / "export.tar.gz"
        client.export(archive)
        assert archive.exists()

        client2 = Client(store_dir=tmp_path / ".cashet2")
        count = client2.import_archive(archive)
        assert count == 2

        imported1 = client2.show(ref1.commit_hash)
        assert imported1 is not None
        assert imported1.task_def.func_name == "add"

        imported2 = client2.show(ref2.commit_hash)
        assert imported2 is not None
        assert imported2.task_def.func_name == "greet"

        assert client2.get(ref1.commit_hash) == 3
        assert client2.get(ref2.commit_hash) == "hello world"

    def test_import_skips_existing(self, client: Client, tmp_path: Path) -> None:
        ref = client.submit(add, 5, 6)
        assert ref.load() == 11

        archive = tmp_path / "export.tar.gz"
        client.export(archive)

        count = client.import_archive(archive)
        assert count == 0

    def test_export_empty_store(self, client: Client, tmp_path: Path) -> None:
        archive = tmp_path / "empty.tar.gz"
        client.export(archive)
        assert archive.exists()

        client2 = Client(store_dir=tmp_path / ".cashet2")
        count = client2.import_archive(archive)
        assert count == 0


class TestAsyncExport:
    async def test_export_import_roundtrip(
        self, async_client: AsyncClient, tmp_path: Path
    ) -> None:
        ref1 = await async_client.submit(add, 1, 2)
        ref2 = await async_client.submit(greet, "world")
        assert await ref1.load() == 3
        assert await ref2.load() == "hello world"

        archive = tmp_path / "export.tar.gz"
        await async_client.export(archive)
        assert archive.exists()

        client2 = AsyncClient(store_dir=tmp_path / ".cashet2")
        count = await client2.import_archive(archive)
        assert count == 2

        imported1 = await client2.show(ref1.commit_hash)
        assert imported1 is not None
        assert imported1.task_def.func_name == "add"

        imported2 = await client2.show(ref2.commit_hash)
        assert imported2 is not None
        assert imported2.task_def.func_name == "greet"

        assert await client2.get(ref1.commit_hash) == 3
        assert await client2.get(ref2.commit_hash) == "hello world"
        await client2.close()

    async def test_import_skips_existing(self, async_client: AsyncClient, tmp_path: Path) -> None:
        ref = await async_client.submit(add, 5, 6)
        assert await ref.load() == 11

        archive = tmp_path / "export.tar.gz"
        await async_client.export(archive)

        count = await async_client.import_archive(archive)
        assert count == 0
