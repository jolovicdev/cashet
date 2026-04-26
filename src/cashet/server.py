from __future__ import annotations

import asyncio
import base64
import json
import types
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from cashet.async_client import AsyncClient
from cashet.client import Client


class _CustomJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        return json.dumps(content, default=str, separators=(",", ":")).encode("utf-8")


def _reconstruct_func(data: dict[str, Any]) -> Any:
    """Reconstruct a function from serialized data.

    SECURITY WARNING: This deserializes arbitrary code via dill and exec.
    Only expose the server to trusted networks.
    """
    func_b64 = data.get("func_b64")
    func_source = data.get("func_source")
    func_name = data.get("func_name", "")

    if func_b64:
        import dill  # type: ignore[reportMissingTypeStubs]

        return dill.loads(base64.b64decode(func_b64))  # type: ignore[reportUnknownMemberType]
    if func_source:
        namespace: dict[str, Any] = {}
        exec(compile(func_source, "<remote>", "exec"), namespace)
        func = namespace.get(func_name)
        if func is None:
            for obj in namespace.values():
                if isinstance(obj, types.FunctionType) and getattr(
                    obj, "__qualname__", ""
                ) == func_name:
                    func = obj
                    break
        return func
    return None


async def _async_submit(request: Request) -> JSONResponse:
    client: AsyncClient = request.app.state.client
    data = await request.json()
    func = _reconstruct_func(data)

    if not isinstance(func, types.FunctionType):
        return _CustomJSONResponse(
            {"error": "func_b64 or func_source required"}, status_code=400
        )

    serializer = client.serializer
    args = ()
    kwargs: dict[str, Any] = {}
    if "args_b64" in data:
        args = serializer.loads(base64.b64decode(data["args_b64"]))
    if "kwargs_b64" in data:
        kwargs = serializer.loads(base64.b64decode(data["kwargs_b64"]))

    cache = data.get("cache", True)
    tags = data.get("tags", {})
    retries = data.get("retries", 0)
    force = data.get("force", False)
    timeout = data.get("timeout")

    try:
        ref = await client.submit(
            func,
            *args,
            _cache=cache,
            _tags=tags,
            _retries=retries,
            _force=force,
            _timeout=timeout,
            **kwargs,
        )
        result = await ref.load()
        result_b64 = base64.b64encode(serializer.dumps(result)).decode()
        return _CustomJSONResponse(
            {
                "commit_hash": ref.commit_hash,
                "blob_hash": ref.hash,
                "result_b64": result_b64,
            }
        )
    except Exception as e:
        return _CustomJSONResponse({"error": str(e)}, status_code=500)


async def _async_result(request: Request) -> JSONResponse:
    client: AsyncClient = request.app.state.client
    commit_hash = request.path_params["commit_hash"]
    try:
        result = await client.get(commit_hash)
        serializer = client.serializer
        result_b64 = base64.b64encode(serializer.dumps(result)).decode()
        return _CustomJSONResponse({"result_b64": result_b64})
    except Exception as e:
        return _CustomJSONResponse({"error": str(e)}, status_code=404)


async def _async_commit(request: Request) -> JSONResponse:
    client: AsyncClient = request.app.state.client
    commit_hash = request.path_params["commit_hash"]
    c = await client.show(commit_hash)
    if c is None:
        return _CustomJSONResponse({"error": "not found"}, status_code=404)
    return _CustomJSONResponse(c.summary())


async def _async_log(request: Request) -> JSONResponse:
    client: AsyncClient = request.app.state.client
    func_name = request.query_params.get("func")
    limit = int(request.query_params.get("limit", "50"))
    status = request.query_params.get("status")
    commits = await client.log(func_name=func_name, limit=limit, status=status)
    return _CustomJSONResponse([c.summary() for c in commits])


async def _async_stats(request: Request) -> JSONResponse:
    client: AsyncClient = request.app.state.client
    return _CustomJSONResponse(await client.stats())


async def _async_gc(request: Request) -> JSONResponse:
    from datetime import timedelta

    client: AsyncClient = request.app.state.client
    data = await request.json()
    older_than_days = data.get("older_than_days", 30)
    max_size = data.get("max_size")
    deleted = await client.gc(timedelta(days=older_than_days), max_size_bytes=max_size)
    return _CustomJSONResponse({"deleted": deleted})


def create_async_app(client: AsyncClient) -> Starlette:
    routes = [
        Route("/submit", _async_submit, methods=["POST"]),
        Route("/result/{commit_hash}", _async_result, methods=["GET"]),
        Route("/commit/{commit_hash}", _async_commit, methods=["GET"]),
        Route("/log", _async_log, methods=["GET"]),
        Route("/stats", _async_stats, methods=["GET"]),
        Route("/gc", _async_gc, methods=["POST"]),
    ]
    app = Starlette(routes=routes)
    app.state.client = client
    return app


# Sync handlers run sync Client operations in threads so they don't block the event loop


async def _submit(request: Request) -> JSONResponse:
    client: Client = request.app.state.client
    data = await request.json()
    func = _reconstruct_func(data)

    if not isinstance(func, types.FunctionType):
        return _CustomJSONResponse(
            {"error": "func_b64 or func_source required"}, status_code=400
        )

    serializer = client.serializer
    args = ()
    kwargs: dict[str, Any] = {}
    if "args_b64" in data:
        args = serializer.loads(base64.b64decode(data["args_b64"]))
    if "kwargs_b64" in data:
        kwargs = serializer.loads(base64.b64decode(data["kwargs_b64"]))

    cache = data.get("cache", True)
    tags = data.get("tags", {})
    retries = data.get("retries", 0)
    force = data.get("force", False)
    timeout = data.get("timeout")

    def _run() -> JSONResponse:
        try:
            ref = client.submit(
                func,
                *args,
                _cache=cache,
                _tags=tags,
                _retries=retries,
                _force=force,
                _timeout=timeout,
                **kwargs,
            )
            result = ref.load()
            result_b64 = base64.b64encode(serializer.dumps(result)).decode()
            return _CustomJSONResponse(
                {
                    "commit_hash": ref.commit_hash,
                    "blob_hash": ref.hash,
                    "result_b64": result_b64,
                }
            )
        except Exception as e:
            return _CustomJSONResponse({"error": str(e)}, status_code=500)

    return await asyncio.to_thread(_run)


async def _result(request: Request) -> JSONResponse:
    client: Client = request.app.state.client
    commit_hash = request.path_params["commit_hash"]

    def _run() -> JSONResponse:
        try:
            result = client.get(commit_hash)
            serializer = client.serializer
            result_b64 = base64.b64encode(serializer.dumps(result)).decode()
            return _CustomJSONResponse({"result_b64": result_b64})
        except Exception as e:
            return _CustomJSONResponse({"error": str(e)}, status_code=404)

    return await asyncio.to_thread(_run)


async def _commit(request: Request) -> JSONResponse:
    client: Client = request.app.state.client
    commit_hash = request.path_params["commit_hash"]

    def _run() -> JSONResponse:
        c = client.show(commit_hash)
        if c is None:
            return _CustomJSONResponse({"error": "not found"}, status_code=404)
        return _CustomJSONResponse(c.summary())

    return await asyncio.to_thread(_run)


async def _log(request: Request) -> JSONResponse:
    client: Client = request.app.state.client
    func_name = request.query_params.get("func")
    limit = int(request.query_params.get("limit", "50"))
    status = request.query_params.get("status")

    def _run() -> JSONResponse:
        commits = client.log(func_name=func_name, limit=limit, status=status)
        return _CustomJSONResponse([c.summary() for c in commits])

    return await asyncio.to_thread(_run)


async def _stats(request: Request) -> JSONResponse:
    client: Client = request.app.state.client
    return await asyncio.to_thread(
        lambda: _CustomJSONResponse(client.stats())
    )


async def _gc(request: Request) -> JSONResponse:
    from datetime import timedelta

    client: Client = request.app.state.client
    data = await request.json()
    older_than_days = data.get("older_than_days", 30)
    max_size = data.get("max_size")

    def _run() -> JSONResponse:
        deleted = client.gc(timedelta(days=older_than_days), max_size_bytes=max_size)
        return _CustomJSONResponse({"deleted": deleted})

    return await asyncio.to_thread(_run)


def create_app(client: Client) -> Starlette:
    routes = [
        Route("/submit", _submit, methods=["POST"]),
        Route("/result/{commit_hash}", _result, methods=["GET"]),
        Route("/commit/{commit_hash}", _commit, methods=["GET"]),
        Route("/log", _log, methods=["GET"]),
        Route("/stats", _stats, methods=["GET"]),
        Route("/gc", _gc, methods=["POST"]),
    ]
    app = Starlette(routes=routes)
    app.state.client = client
    return app
