from __future__ import annotations

import asyncio
import base64
import hmac
import json
import logging
import time
import types
from collections.abc import Callable, Mapping
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from cashet.async_client import AsyncClient
from cashet.client import Client

logger = logging.getLogger("cashet")

TaskRegistry = Mapping[str, Callable[..., Any]]


class _CustomJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        return json.dumps(content, default=str, separators=(",", ":")).encode("utf-8")


def _server_tasks(client: Any, tasks: TaskRegistry | None) -> dict[str, Callable[..., Any]]:
    registered = getattr(client, "_registered_tasks", {})
    resolved: dict[str, Callable[..., Any]] = dict(registered)
    if tasks is not None:
        resolved.update(tasks)
    return resolved


def _validate_remote_code_options(
    allow_remote_code: bool, require_token: str | None
) -> None:
    if allow_remote_code and not require_token:
        raise ValueError("allow_remote_code=True requires a non-empty require_token")


def _reconstruct_func(data: dict[str, Any]) -> Callable[..., Any] | None:
    func_b64 = data.get("func_b64")
    func_source = data.get("func_source")
    func_name = data.get("func_name", "")

    if func_b64:
        import dill  # type: ignore[reportMissingTypeStubs]

        loaded = dill.loads(base64.b64decode(func_b64))  # type: ignore[reportUnknownMemberType]
        return loaded if isinstance(loaded, types.FunctionType) else None
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
        return func if isinstance(func, types.FunctionType) else None
    return None


def _resolve_func(
    data: dict[str, Any],
    tasks: TaskRegistry,
    allow_remote_code: bool,
) -> tuple[Callable[..., Any] | None, JSONResponse | None]:
    task_name = data.get("task") or data.get("task_name")
    if task_name is not None:
        if not isinstance(task_name, str):
            return None, _CustomJSONResponse({"error": "task must be a string"}, status_code=400)
        func = tasks.get(task_name)
        if func is None:
            return None, _CustomJSONResponse(
                {"error": f"unknown task {task_name}"}, status_code=404
            )
        return func, None

    if data.get("func_b64") or data.get("func_source"):
        if not allow_remote_code:
            return None, _CustomJSONResponse(
                {"error": "remote code execution is disabled"}, status_code=403
            )
        func = _reconstruct_func(data)
        if func is None:
            return None, _CustomJSONResponse(
                {"error": "func_b64 or func_source must define a function"},
                status_code=400,
            )
        return func, None

    return None, _CustomJSONResponse(
        {"error": "task required"}, status_code=400
    )


def _decode_call(
    data: dict[str, Any], serializer: Any, allow_serialized_payloads: bool
) -> tuple[tuple[Any, ...], dict[str, Any], JSONResponse | None]:
    if "args_b64" in data or "kwargs_b64" in data:
        if not allow_serialized_payloads:
            return (), {}, _CustomJSONResponse(
                {"error": "serialized args are disabled"}, status_code=403
            )
        args: Any = ()
        kwargs: Any = {}
        if "args_b64" in data:
            args = serializer.loads(base64.b64decode(data["args_b64"]))
        if "kwargs_b64" in data:
            kwargs = serializer.loads(base64.b64decode(data["kwargs_b64"]))
        if not isinstance(args, tuple | list):
            return (), {}, _CustomJSONResponse(
                {"error": "args_b64 must decode to a tuple or list"}, status_code=400
            )
        if not isinstance(kwargs, dict):
            return (), {}, _CustomJSONResponse(
                {"error": "kwargs_b64 must decode to a dict"}, status_code=400
            )
        return tuple(args), kwargs, None

    args = data.get("args", [])
    kwargs = data.get("kwargs", {})
    if not isinstance(args, list):
        return (), {}, _CustomJSONResponse({"error": "args must be a list"}, status_code=400)
    if not isinstance(kwargs, dict):
        return (), {}, _CustomJSONResponse({"error": "kwargs must be an object"}, status_code=400)
    return tuple(args), kwargs, None


def _require_token(handler: Any, token: str | None) -> Any:
    if token is None:
        return handler

    async def wrapper(request: Request) -> JSONResponse:
        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer ") or not hmac.compare_digest(auth[7:], token):
            logger.warning(
                "request unauthorized method=%s path=%s",
                request.method,
                request.url.path,
            )
            return _CustomJSONResponse({"error": "unauthorized"}, status_code=401)
        return await handler(request)

    return wrapper


async def _async_submit(request: Request) -> JSONResponse:
    client: AsyncClient = request.app.state.client
    data = await request.json()
    func, error = _resolve_func(
        data,
        request.app.state.tasks,
        request.app.state.allow_remote_code,
    )
    if error is not None:
        return error
    if func is None:
        return _CustomJSONResponse({"error": "task required"}, status_code=400)

    serializer = client.serializer
    args, kwargs, error = _decode_call(
        data, serializer, request.app.state.allow_remote_code
    )
    if error is not None:
        return error

    cache = data.get("cache", True)
    tags = data.get("tags", {})
    retries = data.get("retries", 0)
    force = data.get("force", False)
    timeout = data.get("timeout")

    start = time.perf_counter()
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
        duration = int((time.perf_counter() - start) * 1000)
        logger.info(
            "request method=%s path=%s status=%d duration_ms=%d",
            request.method,
            request.url.path,
            200,
            duration,
        )
        return _CustomJSONResponse(
            {
                "commit_hash": ref.commit_hash,
                "blob_hash": ref.hash,
                "result_b64": result_b64,
            }
        )
    except Exception:
        duration = int((time.perf_counter() - start) * 1000)
        logger.exception(
            "request failed method=%s path=%s status=500 duration_ms=%d",
            request.method,
            request.url.path,
            duration,
        )
        return _CustomJSONResponse({"error": "Internal server error"}, status_code=500)


async def _async_result(request: Request) -> JSONResponse:
    client: AsyncClient = request.app.state.client
    commit_hash = request.path_params["commit_hash"]
    start = time.perf_counter()
    try:
        result = await client.get(commit_hash)
        serializer = client.serializer
        result_b64 = base64.b64encode(serializer.dumps(result)).decode()
        duration = int((time.perf_counter() - start) * 1000)
        logger.info(
            "request method=%s path=%s status=%d duration_ms=%d",
            request.method, request.url.path, 200, duration,
        )
        return _CustomJSONResponse({"result_b64": result_b64})
    except (KeyError, ValueError):
        duration = int((time.perf_counter() - start) * 1000)
        logger.info(
            "request method=%s path=%s status=%d duration_ms=%d",
            request.method, request.url.path, 404, duration,
        )
        return _CustomJSONResponse({"error": "not found"}, status_code=404)
    except Exception:
        duration = int((time.perf_counter() - start) * 1000)
        logger.exception(
            "request failed method=%s path=%s status=500 duration_ms=%d",
            request.method, request.url.path, duration,
        )
        return _CustomJSONResponse({"error": "Internal server error"}, status_code=500)


async def _async_commit(request: Request) -> JSONResponse:
    client: AsyncClient = request.app.state.client
    commit_hash = request.path_params["commit_hash"]
    start = time.perf_counter()
    c = await client.show(commit_hash)
    status_code = 200 if c is not None else 404
    duration = int((time.perf_counter() - start) * 1000)
    logger.info(
        "request method=%s path=%s status=%d duration_ms=%d",
        request.method, request.url.path, status_code, duration,
    )
    if c is None:
        return _CustomJSONResponse({"error": "not found"}, status_code=404)
    return _CustomJSONResponse(c.summary())


async def _async_log(request: Request) -> JSONResponse:
    client: AsyncClient = request.app.state.client
    func_name = request.query_params.get("func")
    limit = int(request.query_params.get("limit", "50"))
    status = request.query_params.get("status")
    start = time.perf_counter()
    commits = await client.log(func_name=func_name, limit=limit, status=status)
    duration = int((time.perf_counter() - start) * 1000)
    logger.info(
        "request method=%s path=%s status=%d duration_ms=%d",
        request.method, request.url.path, 200, duration,
    )
    return _CustomJSONResponse([c.summary() for c in commits])


async def _async_stats(request: Request) -> JSONResponse:
    client: AsyncClient = request.app.state.client
    start = time.perf_counter()
    result = await client.stats()
    duration = int((time.perf_counter() - start) * 1000)
    logger.info(
        "request method=%s path=%s status=%d duration_ms=%d",
        request.method, request.url.path, 200, duration,
    )
    return _CustomJSONResponse(result)


async def _async_gc(request: Request) -> JSONResponse:
    from datetime import timedelta

    client: AsyncClient = request.app.state.client
    data = await request.json()
    older_than_days = data.get("older_than_days", 30)
    max_size = data.get("max_size")
    start = time.perf_counter()
    deleted = await client.gc(timedelta(days=older_than_days), max_size_bytes=max_size)
    duration = int((time.perf_counter() - start) * 1000)
    logger.info(
        "request method=%s path=%s status=%d duration_ms=%d",
        request.method, request.url.path, 200, duration,
    )
    return _CustomJSONResponse({"deleted": deleted})


def create_async_app(
    client: AsyncClient,
    require_token: str | None = None,
    *,
    tasks: TaskRegistry | None = None,
    allow_remote_code: bool = False,
) -> Starlette:
    _validate_remote_code_options(allow_remote_code, require_token)
    routes = [
        Route("/submit", _require_token(_async_submit, require_token), methods=["POST"]),
        Route(
            "/result/{commit_hash}",
            _require_token(_async_result, require_token),
            methods=["GET"],
        ),
        Route(
            "/commit/{commit_hash}",
            _require_token(_async_commit, require_token),
            methods=["GET"],
        ),
        Route("/log", _require_token(_async_log, require_token), methods=["GET"]),
        Route("/stats", _require_token(_async_stats, require_token), methods=["GET"]),
        Route("/gc", _require_token(_async_gc, require_token), methods=["POST"]),
    ]
    app = Starlette(routes=routes)
    app.state.client = client
    app.state.tasks = _server_tasks(client, tasks)
    app.state.allow_remote_code = allow_remote_code
    return app


# Sync handlers run sync Client operations in threads so they don't block the event loop


async def _submit(request: Request) -> JSONResponse:
    client: Client = request.app.state.client
    data = await request.json()
    func, error = _resolve_func(
        data,
        request.app.state.tasks,
        request.app.state.allow_remote_code,
    )
    if error is not None:
        return error
    if func is None:
        return _CustomJSONResponse({"error": "task required"}, status_code=400)

    serializer = client.serializer
    args, kwargs, error = _decode_call(
        data, serializer, request.app.state.allow_remote_code
    )
    if error is not None:
        return error

    cache = data.get("cache", True)
    tags = data.get("tags", {})
    retries = data.get("retries", 0)
    force = data.get("force", False)
    timeout = data.get("timeout")

    start = time.perf_counter()

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
        except Exception:
            logger.exception(
                "request failed method=POST path=/submit status=500",
            )
            return _CustomJSONResponse({"error": "Internal server error"}, status_code=500)

    response = await asyncio.to_thread(_run)
    duration = int((time.perf_counter() - start) * 1000)
    logger.info(
        "request method=%s path=%s status=%d duration_ms=%d",
        request.method, request.url.path, response.status_code, duration,
    )
    return response


async def _result(request: Request) -> JSONResponse:
    client: Client = request.app.state.client
    commit_hash = request.path_params["commit_hash"]
    start = time.perf_counter()

    def _run() -> JSONResponse:
        try:
            result = client.get(commit_hash)
            serializer = client.serializer
            result_b64 = base64.b64encode(serializer.dumps(result)).decode()
            return _CustomJSONResponse({"result_b64": result_b64})
        except (KeyError, ValueError):
            return _CustomJSONResponse({"error": "not found"}, status_code=404)
        except Exception:
            logger.exception(
                "request failed method=%s path=%s status=500",
                request.method, request.url.path,
            )
            return _CustomJSONResponse({"error": "Internal server error"}, status_code=500)

    response = await asyncio.to_thread(_run)
    duration = int((time.perf_counter() - start) * 1000)
    logger.info(
        "request method=%s path=%s status=%d duration_ms=%d",
        request.method, request.url.path, response.status_code, duration,
    )
    return response


async def _commit(request: Request) -> JSONResponse:
    client: Client = request.app.state.client
    commit_hash = request.path_params["commit_hash"]
    start = time.perf_counter()

    def _run() -> JSONResponse:
        c = client.show(commit_hash)
        if c is None:
            return _CustomJSONResponse({"error": "not found"}, status_code=404)
        return _CustomJSONResponse(c.summary())

    response = await asyncio.to_thread(_run)
    duration = int((time.perf_counter() - start) * 1000)
    logger.info(
        "request method=%s path=%s status=%d duration_ms=%d",
        request.method, request.url.path, response.status_code, duration,
    )
    return response


async def _log(request: Request) -> JSONResponse:
    client: Client = request.app.state.client
    func_name = request.query_params.get("func")
    limit = int(request.query_params.get("limit", "50"))
    status = request.query_params.get("status")
    start = time.perf_counter()

    def _run() -> JSONResponse:
        commits = client.log(func_name=func_name, limit=limit, status=status)
        return _CustomJSONResponse([c.summary() for c in commits])

    response = await asyncio.to_thread(_run)
    duration = int((time.perf_counter() - start) * 1000)
    logger.info(
        "request method=%s path=%s status=%d duration_ms=%d",
        request.method, request.url.path, response.status_code, duration,
    )
    return response


async def _stats(request: Request) -> JSONResponse:
    client: Client = request.app.state.client
    start = time.perf_counter()
    response = await asyncio.to_thread(
        lambda: _CustomJSONResponse(client.stats())
    )
    duration = int((time.perf_counter() - start) * 1000)
    logger.info(
        "request method=%s path=%s status=%d duration_ms=%d",
        request.method, request.url.path, response.status_code, duration,
    )
    return response


async def _gc(request: Request) -> JSONResponse:
    from datetime import timedelta

    client: Client = request.app.state.client
    data = await request.json()
    older_than_days = data.get("older_than_days", 30)
    max_size = data.get("max_size")
    start = time.perf_counter()

    def _run() -> JSONResponse:
        deleted = client.gc(timedelta(days=older_than_days), max_size_bytes=max_size)
        return _CustomJSONResponse({"deleted": deleted})

    response = await asyncio.to_thread(_run)
    duration = int((time.perf_counter() - start) * 1000)
    logger.info(
        "request method=%s path=%s status=%d duration_ms=%d",
        request.method, request.url.path, response.status_code, duration,
    )
    return response


def create_app(
    client: Client,
    require_token: str | None = None,
    *,
    tasks: TaskRegistry | None = None,
    allow_remote_code: bool = False,
) -> Starlette:
    _validate_remote_code_options(allow_remote_code, require_token)
    routes = [
        Route("/submit", _require_token(_submit, require_token), methods=["POST"]),
        Route(
            "/result/{commit_hash}",
            _require_token(_result, require_token),
            methods=["GET"],
        ),
        Route(
            "/commit/{commit_hash}",
            _require_token(_commit, require_token),
            methods=["GET"],
        ),
        Route("/log", _require_token(_log, require_token), methods=["GET"]),
        Route("/stats", _require_token(_stats, require_token), methods=["GET"]),
        Route("/gc", _require_token(_gc, require_token), methods=["POST"]),
    ]
    app = Starlette(routes=routes)
    app.state.client = client
    app.state.tasks = _server_tasks(client, tasks)
    app.state.allow_remote_code = allow_remote_code
    return app
