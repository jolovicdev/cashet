from __future__ import annotations

import base64
from pathlib import Path

import httpx
import pytest
from starlette.testclient import TestClient

from cashet.async_client import AsyncClient
from cashet.client import Client
from cashet.hashing import PickleSerializer
from cashet.server import create_app, create_async_app


def _add(x: int, y: int) -> int:
    return x + y


def _mul(x: int, y: int) -> int:
    return x * y


def _value() -> int:
    return 42


def _greet() -> str:
    return "hi"


def _one() -> int:
    return 1


def _boom() -> int:
    raise RuntimeError("secret internal path")


@pytest.fixture
def server_client(tmp_path: Path) -> TestClient:
    client = Client(store_dir=tmp_path / ".cashet")
    app = create_app(client, tasks={"add": _add})
    return TestClient(app)


class TestServerSubmit:
    def test_submit_registered_task_with_json_args(self, server_client: TestClient) -> None:
        payload = {"task": "add", "args": [3, 4], "kwargs": {}}
        response = server_client.post("/submit", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert "commit_hash" in data
        assert "result_b64" in data
        result = PickleSerializer().loads(base64.b64decode(data["result_b64"]))
        assert result == 7

    def test_remote_source_disabled_by_default(
        self, server_client: TestClient, tmp_path: Path
    ) -> None:
        marker = tmp_path / "remote-source-executed"
        payload = {
            "func_source": (
                "from pathlib import Path\n"
                f"Path({str(marker)!r}).write_text('owned')\n"
                "def f():\n"
                "    return 1"
            ),
            "func_name": "f",
            "args": [],
        }
        response = server_client.post("/submit", json=payload)
        assert response.status_code == 403
        assert response.json()["error"] == "remote code execution is disabled"
        assert not marker.exists()

    def test_serialized_args_disabled_by_default(self, server_client: TestClient) -> None:
        payload = {
            "task": "add",
            "args_b64": base64.b64encode(PickleSerializer().dumps((3, 4))).decode(),
            "kwargs_b64": base64.b64encode(PickleSerializer().dumps({})).decode(),
        }
        response = server_client.post("/submit", json=payload)
        assert response.status_code == 403
        assert response.json()["error"] == "serialized args are disabled"

    def test_submit_via_source_requires_explicit_unsafe_remote_code(self, tmp_path: Path) -> None:
        client = Client(store_dir=tmp_path / ".cashet")
        app = create_app(client, require_token="secret123", allow_remote_code=True)
        tc = TestClient(app)
        payload = {
            "func_source": "def add(x, y):\n    return x + y",
            "func_name": "add",
            "args_b64": base64.b64encode(PickleSerializer().dumps((3, 4))).decode(),
            "kwargs_b64": base64.b64encode(PickleSerializer().dumps({})).decode(),
        }
        response = tc.post(
            "/submit",
            json=payload,
            headers={"Authorization": "Bearer secret123"},
        )
        assert response.status_code == 200
        data = response.json()
        result = PickleSerializer().loads(base64.b64decode(data["result_b64"]))
        assert result == 7

    def test_submit_via_dill_requires_explicit_unsafe_remote_code(self, tmp_path: Path) -> None:
        import dill

        client = Client(store_dir=tmp_path / ".cashet")
        app = create_app(client, require_token="secret123", allow_remote_code=True)
        tc = TestClient(app)
        payload = {
            "func_b64": base64.b64encode(dill.dumps(_mul)).decode(),
            "args_b64": base64.b64encode(PickleSerializer().dumps((3, 4))).decode(),
            "kwargs_b64": base64.b64encode(PickleSerializer().dumps({})).decode(),
        }
        response = tc.post(
            "/submit",
            json=payload,
            headers={"Authorization": "Bearer secret123"},
        )
        assert response.status_code == 200
        data = response.json()
        result = PickleSerializer().loads(base64.b64decode(data["result_b64"]))
        assert result == 12

    def test_unsafe_remote_code_requires_auth_token(self, tmp_path: Path) -> None:
        client = Client(store_dir=tmp_path / ".cashet")
        with pytest.raises(ValueError, match="allow_remote_code=True requires"):
            create_app(client, allow_remote_code=True)

    def test_unsafe_remote_code_requires_non_empty_auth_token(self, tmp_path: Path) -> None:
        client = Client(store_dir=tmp_path / ".cashet")
        with pytest.raises(ValueError, match="allow_remote_code=True requires"):
            create_app(client, require_token="", allow_remote_code=True)

    def test_submit_missing_task_raises_400(self, server_client: TestClient) -> None:
        response = server_client.post("/submit", json={"args": [1]})
        assert response.status_code == 400

    def test_submit_internal_error_is_generic(self, tmp_path: Path) -> None:
        client = Client(store_dir=tmp_path / ".cashet")
        app = create_app(client, tasks={"boom": _boom})
        tc = TestClient(app)
        response = tc.post("/submit", json={"task": "boom"})
        assert response.status_code == 500
        assert response.json()["error"] == "Internal server error"
        assert "secret internal path" not in response.text


class TestAsyncServerSubmit:
    async def test_async_submit_registered_task_with_sqlite(self, tmp_path: Path) -> None:
        client = AsyncClient(store_dir=tmp_path / ".cashet")
        app = create_async_app(client, tasks={"add": _add})
        payload = {"task": "add", "args": [3, 4], "kwargs": {}}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as ac:
            response = await ac.post("/submit", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert "commit_hash" in data
        result = PickleSerializer().loads(base64.b64decode(data["result_b64"]))
        assert result == 7
        await client.close()

    async def test_async_result_with_sqlite(self, tmp_path: Path) -> None:
        client = AsyncClient(store_dir=tmp_path / ".cashet")
        app = create_async_app(client, tasks={"val": _value})
        payload = {"task": "val"}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as ac:
            submit_resp = await ac.post("/submit", json=payload)
            commit_hash = submit_resp.json()["commit_hash"]
            result_resp = await ac.get(f"/result/{commit_hash}")
        assert result_resp.status_code == 200
        data = result_resp.json()
        result = PickleSerializer().loads(base64.b64decode(data["result_b64"]))
        assert result == 42
        await client.close()

    async def test_async_commit_with_sqlite(self, tmp_path: Path) -> None:
        client = AsyncClient(store_dir=tmp_path / ".cashet")
        app = create_async_app(client, tasks={"greet": _greet})
        payload = {"task": "greet"}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as ac:
            submit_resp = await ac.post("/submit", json=payload)
            commit_hash = submit_resp.json()["commit_hash"]
            response = await ac.get(f"/commit/{commit_hash}")
        assert response.status_code == 200
        data = response.json()
        assert data["function"] == "_greet"
        await client.close()

    async def test_async_log_with_sqlite(self, tmp_path: Path) -> None:
        client = AsyncClient(store_dir=tmp_path / ".cashet")
        app = create_async_app(client, tasks={"one": _one})
        payload = {"task": "one"}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as ac:
            await ac.post("/submit", json=payload)
            response = await ac.get("/log")
        assert response.status_code == 200
        assert len(response.json()) == 1
        await client.close()

    async def test_async_stats_with_sqlite(self, tmp_path: Path) -> None:
        client = AsyncClient(store_dir=tmp_path / ".cashet")
        app = create_async_app(client)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as ac:
            response = await ac.get("/stats")
        assert response.status_code == 200
        assert "total_commits" in response.json()
        await client.close()

    async def test_async_gc_with_sqlite(self, tmp_path: Path) -> None:
        client = AsyncClient(store_dir=tmp_path / ".cashet")
        app = create_async_app(client, tasks={"one": _one})
        payload = {"task": "one"}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as ac:
            await ac.post("/submit", json=payload)
            response = await ac.post("/gc", json={"older_than_days": 0})
        assert response.status_code == 200
        assert response.json()["deleted"] == 1
        await client.close()


class TestServerAuth:
    def test_no_token_returns_401(self) -> None:
        client = Client()
        app = create_app(client, require_token="secret123", tasks={"one": _one})
        tc = TestClient(app)
        response = tc.post("/submit", json={"task": "one"})
        assert response.status_code == 401
        assert response.json()["error"] == "unauthorized"

    def test_wrong_token_returns_401(self) -> None:
        client = Client()
        app = create_app(client, require_token="secret123", tasks={"one": _one})
        tc = TestClient(app)
        response = tc.post(
            "/submit",
            json={"task": "one"},
            headers={"Authorization": "Bearer wrong"},
        )
        assert response.status_code == 401

    def test_valid_token_succeeds(self) -> None:
        client = Client()
        app = create_app(client, require_token="secret123", tasks={"add": _add})
        tc = TestClient(app)
        response = tc.post(
            "/submit",
            json={"task": "add", "args": [3, 4], "kwargs": {}},
            headers={"Authorization": "Bearer secret123"},
        )
        assert response.status_code == 200
        assert "commit_hash" in response.json()

    def test_no_auth_when_token_not_set(self) -> None:
        client = Client()
        app = create_app(client, tasks={"add": _add})
        tc = TestClient(app)
        response = tc.post("/submit", json={"task": "add", "args": [3, 4]})
        assert response.status_code == 200
