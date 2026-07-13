from __future__ import annotations

import asyncio
import base64
import functools
import hmac
import json
import logging
import time
import types
from collections.abc import Callable, Mapping
from datetime import timedelta
from typing import Any

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from cashet.async_client import AsyncClient
from cashet.client import Client
from cashet.models import Commit, TaskError

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


_DEFAULT_MAX_CONTENT_LENGTH = 500 * 1024 * 1024


def _too_large_response(max_size: int) -> _CustomJSONResponse:
    return _CustomJSONResponse(
        {"error": f"request body exceeds {max_size} bytes"}, status_code=413
    )


def _task_failed_response(exc: TaskError) -> _CustomJSONResponse:
    # A failing task is the caller's error, not a server bug: surface the final
    # traceback line ("ExceptionType: message") but never server file paths.
    lines = str(exc).strip().splitlines()
    detail = lines[-1] if lines else "task failed"
    return _CustomJSONResponse(
        {"error": "task failed", "detail": detail}, status_code=422
    )


class _BadRequestError(Exception):
    pass


def _query_int(request: Request, name: str, default: int) -> int:
    raw = request.query_params.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise _BadRequestError(f"{name} must be an integer") from exc


async def _json_body(request: Request) -> dict[str, Any]:
    body = await request.body()
    if not body:
        return {}
    data = json.loads(body)
    if not isinstance(data, dict):
        raise _BadRequestError("request body must be a JSON object")
    return data


def _gc_params(data: dict[str, Any]) -> tuple[float, int | None]:
    older_than_days = data.get("older_than_days", 30)
    if isinstance(older_than_days, bool) or not isinstance(older_than_days, int | float):
        raise _BadRequestError("older_than_days must be a number")
    max_size = data.get("max_size")
    if max_size is not None and (isinstance(max_size, bool) or not isinstance(max_size, int)):
        raise _BadRequestError("max_size must be an integer or null")
    return older_than_days, max_size


def _safe_handler(handler: Any) -> Any:
    @functools.wraps(handler)
    async def wrapper(request: Request) -> JSONResponse:
        try:
            return await handler(request)
        except _BadRequestError as exc:
            return _CustomJSONResponse({"error": str(exc)}, status_code=400)
        except json.JSONDecodeError:
            return _CustomJSONResponse({"error": "invalid JSON body"}, status_code=400)
        except Exception:
            logger.exception(
                "request failed method=%s path=%s status=500",
                request.method,
                request.url.path,
            )
            return _CustomJSONResponse({"error": "Internal server error"}, status_code=500)

    return wrapper


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


def _token_authorized(auth: str, token: str) -> bool:
    return auth.startswith("Bearer ") and hmac.compare_digest(auth[7:], token)


def _require_token(handler: Any, token: str | None) -> Any:
    if token is None:
        return handler

    async def wrapper(request: Request) -> JSONResponse:
        auth = request.headers.get("authorization", "")
        if not _token_authorized(auth, token):
            logger.warning(
                "request unauthorized method=%s path=%s",
                request.method,
                request.url.path,
            )
            return _CustomJSONResponse({"error": "unauthorized"}, status_code=401)
        return await handler(request)

    return wrapper


def _submit_options(data: dict[str, Any]) -> dict[str, Any]:
    # Keys match the underscore-prefixed keyword parameters of Client.submit
    # and AsyncClient.submit, so the ops adapters can splat them straight in.
    return {
        "_cache": data.get("cache", True),
        "_tags": data.get("tags", {}),
        "_retries": data.get("retries", 0),
        "_force": data.get("force", False),
        "_timeout": data.get("timeout"),
        "_ttl": data.get("ttl"),
    }


class _AsyncOps:
    def __init__(self, client: AsyncClient) -> None:
        self.client = client
        self.serializer: Any = client.serializer

    async def submit_and_load(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        options: dict[str, Any],
    ) -> tuple[str, str, bytes]:
        ref = await self.client.submit(func, *args, **options, **kwargs)
        result = await ref.load()
        return ref.commit_hash, ref.hash, self.serializer.dumps(result)

    async def get_result(self, commit_hash: str) -> bytes:
        return self.serializer.dumps(await self.client.get(commit_hash))

    async def show(self, commit_hash: str) -> Commit | None:
        return await self.client.show(commit_hash)

    async def log(
        self, func_name: str | None, limit: int, status: str | None
    ) -> list[Commit]:
        return await self.client.log(func_name=func_name, limit=limit, status=status)

    async def stats(self) -> dict[str, int]:
        return await self.client.stats()

    async def gc(self, older_than: timedelta, max_size: int | None) -> int:
        return await self.client.gc(older_than, max_size_bytes=max_size)


class _SyncOps:
    # Sync Client calls run in threads so they never block the event loop.
    def __init__(self, client: Client) -> None:
        self.client = client
        self.serializer: Any = client.serializer

    async def submit_and_load(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        options: dict[str, Any],
    ) -> tuple[str, str, bytes]:
        def run() -> tuple[str, str, bytes]:
            ref = self.client.submit(func, *args, **options, **kwargs)
            result = ref.load()
            return ref.commit_hash, ref.hash, self.serializer.dumps(result)

        return await asyncio.to_thread(run)

    async def get_result(self, commit_hash: str) -> bytes:
        return await asyncio.to_thread(
            lambda: self.serializer.dumps(self.client.get(commit_hash))
        )

    async def show(self, commit_hash: str) -> Commit | None:
        return await asyncio.to_thread(self.client.show, commit_hash)

    async def log(
        self, func_name: str | None, limit: int, status: str | None
    ) -> list[Commit]:
        return await asyncio.to_thread(
            lambda: self.client.log(func_name=func_name, limit=limit, status=status)
        )

    async def stats(self) -> dict[str, int]:
        return await asyncio.to_thread(self.client.stats)

    async def gc(self, older_than: timedelta, max_size: int | None) -> int:
        return await asyncio.to_thread(
            lambda: self.client.gc(older_than, max_size_bytes=max_size)
        )


_ServerOps = _AsyncOps | _SyncOps


def _log_request(request: Request, status_code: int, start: float) -> None:
    logger.info(
        "request method=%s path=%s status=%d duration_ms=%d",
        request.method,
        request.url.path,
        status_code,
        int((time.perf_counter() - start) * 1000),
    )


def _log_request_failed(request: Request, start: float) -> None:
    logger.exception(
        "request failed method=%s path=%s status=500 duration_ms=%d",
        request.method,
        request.url.path,
        int((time.perf_counter() - start) * 1000),
    )


async def _submit(request: Request) -> JSONResponse:
    ops: _ServerOps = request.app.state.ops
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

    args, kwargs, error = _decode_call(
        data, ops.serializer, request.app.state.allow_remote_code
    )
    if error is not None:
        return error

    options = _submit_options(data)
    start = time.perf_counter()
    try:
        commit_hash, blob_hash, payload = await ops.submit_and_load(
            func, args, kwargs, options
        )
    except TaskError as exc:
        _log_request(request, 422, start)
        return _task_failed_response(exc)
    except Exception:
        _log_request_failed(request, start)
        return _CustomJSONResponse({"error": "Internal server error"}, status_code=500)
    _log_request(request, 200, start)
    return _CustomJSONResponse(
        {
            "commit_hash": commit_hash,
            "blob_hash": blob_hash,
            "result_b64": base64.b64encode(payload).decode(),
        }
    )


async def _result(request: Request) -> JSONResponse:
    ops: _ServerOps = request.app.state.ops
    commit_hash = request.path_params["commit_hash"]
    start = time.perf_counter()
    try:
        payload = await ops.get_result(commit_hash)
    except (KeyError, ValueError):
        _log_request(request, 404, start)
        return _CustomJSONResponse({"error": "not found"}, status_code=404)
    except Exception:
        _log_request_failed(request, start)
        return _CustomJSONResponse({"error": "Internal server error"}, status_code=500)
    _log_request(request, 200, start)
    return _CustomJSONResponse({"result_b64": base64.b64encode(payload).decode()})


async def _commit(request: Request) -> JSONResponse:
    ops: _ServerOps = request.app.state.ops
    commit_hash = request.path_params["commit_hash"]
    start = time.perf_counter()
    c = await ops.show(commit_hash)
    _log_request(request, 200 if c is not None else 404, start)
    if c is None:
        return _CustomJSONResponse({"error": "not found"}, status_code=404)
    return _CustomJSONResponse(c.summary())


async def _log(request: Request) -> JSONResponse:
    ops: _ServerOps = request.app.state.ops
    func_name = request.query_params.get("func")
    limit = _query_int(request, "limit", 50)
    status = request.query_params.get("status")
    start = time.perf_counter()
    commits = await ops.log(func_name, limit, status)
    _log_request(request, 200, start)
    return _CustomJSONResponse([c.summary() for c in commits])


async def _stats(request: Request) -> JSONResponse:
    ops: _ServerOps = request.app.state.ops
    start = time.perf_counter()
    result = await ops.stats()
    _log_request(request, 200, start)
    return _CustomJSONResponse(result)


async def _gc(request: Request) -> JSONResponse:
    ops: _ServerOps = request.app.state.ops
    older_than_days, max_size = _gc_params(await _json_body(request))
    start = time.perf_counter()
    deleted = await ops.gc(timedelta(days=older_than_days), max_size)
    _log_request(request, 200, start)
    return _CustomJSONResponse({"deleted": deleted})


class _SizeLimitMiddleware:
    # Pure-ASGI so the body cap is enforced on bytes actually received, not on a
    # client-supplied Content-Length that is absent for chunked requests.
    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        starlette_app = scope.get("app")
        state = starlette_app.state if starlette_app is not None else None
        max_size = getattr(state, "max_content_length", _DEFAULT_MAX_CONTENT_LENGTH)

        # Reject unauthenticated requests before buffering any body, so an
        # unauthenticated client cannot force buffering up to max_content_length.
        require_token = getattr(state, "require_token", None)
        if require_token is not None:
            auth = ""
            for name, value in scope["headers"]:
                if name == b"authorization":
                    auth = value.decode("latin-1")
                    break
            if not _token_authorized(auth, require_token):
                await _CustomJSONResponse(
                    {"error": "unauthorized"}, status_code=401
                )(scope, receive, send)
                return

        for name, value in scope["headers"]:
            if name == b"content-length":
                try:
                    declared = int(value)
                except ValueError:
                    await _CustomJSONResponse(
                        {"error": "invalid content-length"}, status_code=400
                    )(scope, receive, send)
                    return
                if declared > max_size:
                    await _too_large_response(max_size)(scope, receive, send)
                    return
                break

        body = bytearray()
        while True:
            message = await receive()
            if message["type"] != "http.request":
                break
            body.extend(message.get("body", b""))
            if len(body) > max_size:
                await _too_large_response(max_size)(scope, receive, send)
                return
            if not message.get("more_body", False):
                break

        buffered = bytes(body)
        replayed = False

        async def replay_receive() -> Any:
            nonlocal replayed
            if not replayed:
                replayed = True
                return {"type": "http.request", "body": buffered, "more_body": False}
            return await receive()

        await self.app(scope, replay_receive, send)


def _build_app(
    ops: _ServerOps,
    require_token: str | None,
    tasks: TaskRegistry | None,
    allow_remote_code: bool,
    max_content_length: int,
) -> Starlette:
    _validate_remote_code_options(allow_remote_code, require_token)
    routes = [
        Route(
            "/submit",
            _require_token(_safe_handler(_submit), require_token),
            methods=["POST"],
        ),
        Route(
            "/result/{commit_hash}",
            _require_token(_safe_handler(_result), require_token),
            methods=["GET"],
        ),
        Route(
            "/commit/{commit_hash}",
            _require_token(_safe_handler(_commit), require_token),
            methods=["GET"],
        ),
        Route("/log", _require_token(_safe_handler(_log), require_token), methods=["GET"]),
        Route(
            "/stats",
            _require_token(_safe_handler(_stats), require_token),
            methods=["GET"],
        ),
        Route("/gc", _require_token(_safe_handler(_gc), require_token), methods=["POST"]),
    ]
    app = Starlette(
        routes=routes,
        middleware=[Middleware(_SizeLimitMiddleware)],
    )
    app.state.client = ops.client
    app.state.ops = ops
    app.state.tasks = _server_tasks(ops.client, tasks)
    app.state.allow_remote_code = allow_remote_code
    app.state.max_content_length = max_content_length
    app.state.require_token = require_token
    return app


def create_app(
    client: Client,
    require_token: str | None = None,
    *,
    tasks: TaskRegistry | None = None,
    allow_remote_code: bool = False,
    max_content_length: int = _DEFAULT_MAX_CONTENT_LENGTH,
) -> Starlette:
    return _build_app(
        _SyncOps(client), require_token, tasks, allow_remote_code, max_content_length
    )


def create_async_app(
    client: AsyncClient,
    require_token: str | None = None,
    *,
    tasks: TaskRegistry | None = None,
    allow_remote_code: bool = False,
    max_content_length: int = _DEFAULT_MAX_CONTENT_LENGTH,
) -> Starlette:
    return _build_app(
        _AsyncOps(client), require_token, tasks, allow_remote_code, max_content_length
    )
