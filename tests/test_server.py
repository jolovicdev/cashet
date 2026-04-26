from __future__ import annotations

import base64
from typing import Any

import httpx
import pytest
from starlette.testclient import TestClient

from cashet.async_client import AsyncClient
from cashet.client import Client
from cashet.redis_store import AsyncRedisStore
from cashet.server import create_app, create_async_app


@pytest.fixture
def server_client(tmp_path: Any) -> TestClient:
    client = Client(store_dir=tmp_path / ".cashet")
    app = create_app(client)
    return TestClient(app)


class TestServerSubmit:
    def test_submit_via_source(self, server_client: TestClient) -> None:
        payload = {
            "func_source": "def add(x, y):\n    return x + y",
            "func_name": "add",
            "args_b64": base64.b64encode(Client().serializer.dumps((3, 4))).decode(),
            "kwargs_b64": base64.b64encode(Client().serializer.dumps({})).decode(),
        }
        response = server_client.post("/submit", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert "commit_hash" in data
        assert "result_b64" in data
        result = Client().serializer.loads(base64.b64decode(data["result_b64"]))
        assert result == 7

    def test_submit_via_dill(self, server_client: TestClient) -> None:
        import dill

        def mul(x: int, y: int) -> int:
            return x * y

        payload = {
            "func_b64": base64.b64encode(dill.dumps(mul)).decode(),
            "args_b64": base64.b64encode(Client().serializer.dumps((3, 4))).decode(),
            "kwargs_b64": base64.b64encode(Client().serializer.dumps({})).decode(),
        }
        response = server_client.post("/submit", json=payload)
        assert response.status_code == 200
        data = response.json()
        result = Client().serializer.loads(base64.b64decode(data["result_b64"]))
        assert result == 12

    def test_submit_missing_func_raises_400(self, server_client: TestClient) -> None:
        payload = {
            "args_b64": base64.b64encode(Client().serializer.dumps((1,))).decode(),
        }
        response = server_client.post("/submit", json=payload)
        assert response.status_code == 400


@pytest.mark.redis
class TestAsyncServerSubmit:
    async def test_async_submit_via_source(self) -> None:
        client = AsyncClient(store=AsyncRedisStore())
        await client.store._redis.flushdb()
        app = create_async_app(client)
        payload = {
            "func_source": "def add(x, y):\n    return x + y",
            "func_name": "add",
            "args_b64": base64.b64encode(Client().serializer.dumps((3, 4))).decode(),
            "kwargs_b64": base64.b64encode(Client().serializer.dumps({})).decode(),
        }
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as ac:
            response = await ac.post("/submit", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert "commit_hash" in data
        result = Client().serializer.loads(base64.b64decode(data["result_b64"]))
        assert result == 7
        await client.close()

    async def test_async_result(self) -> None:
        client = AsyncClient(store=AsyncRedisStore())
        await client.store._redis.flushdb()
        app = create_async_app(client)
        payload = {
            "func_source": "def val():\n    return 42",
            "func_name": "val",
            "args_b64": base64.b64encode(Client().serializer.dumps(())).decode(),
            "kwargs_b64": base64.b64encode(Client().serializer.dumps({})).decode(),
        }
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as ac:
            submit_resp = await ac.post("/submit", json=payload)
            commit_hash = submit_resp.json()["commit_hash"]
            result_resp = await ac.get(f"/result/{commit_hash}")
        assert result_resp.status_code == 200
        data = result_resp.json()
        result = Client().serializer.loads(base64.b64decode(data["result_b64"]))
        assert result == 42
        await client.close()

    async def test_async_commit(self) -> None:
        client = AsyncClient(store=AsyncRedisStore())
        await client.store._redis.flushdb()
        app = create_async_app(client)
        payload = {
            "func_source": "def greet():\n    return 'hi'",
            "func_name": "greet",
            "args_b64": base64.b64encode(Client().serializer.dumps(())).decode(),
            "kwargs_b64": base64.b64encode(Client().serializer.dumps({})).decode(),
        }
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as ac:
            submit_resp = await ac.post("/submit", json=payload)
            commit_hash = submit_resp.json()["commit_hash"]
            response = await ac.get(f"/commit/{commit_hash}")
        assert response.status_code == 200
        data = response.json()
        assert data["function"] == "greet"
        await client.close()

    async def test_async_log(self) -> None:
        client = AsyncClient(store=AsyncRedisStore())
        await client.store._redis.flushdb()
        app = create_async_app(client)
        payload = {
            "func_source": "def f():\n    return 1",
            "func_name": "f",
            "args_b64": base64.b64encode(Client().serializer.dumps(())).decode(),
            "kwargs_b64": base64.b64encode(Client().serializer.dumps({})).decode(),
        }
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as ac:
            await ac.post("/submit", json=payload)
            response = await ac.get("/log")
        assert response.status_code == 200
        assert len(response.json()) == 1
        await client.close()

    async def test_async_stats(self) -> None:
        client = AsyncClient(store=AsyncRedisStore())
        await client.store._redis.flushdb()
        app = create_async_app(client)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as ac:
            response = await ac.get("/stats")
        assert response.status_code == 200
        assert "total_commits" in response.json()
        await client.close()

    async def test_async_gc(self) -> None:
        client = AsyncClient(store=AsyncRedisStore())
        await client.store._redis.flushdb()
        app = create_async_app(client)
        payload = {
            "func_source": "def f():\n    return 1",
            "func_name": "f",
            "args_b64": base64.b64encode(Client().serializer.dumps(())).decode(),
            "kwargs_b64": base64.b64encode(Client().serializer.dumps({})).decode(),
        }
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as ac:
            await ac.post("/submit", json=payload)
            response = await ac.post("/gc", json={"older_than_days": 0})
        assert response.status_code == 200
        assert response.json()["deleted"] == 1
        await client.close()
