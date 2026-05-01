# AGENTS.md

## Project overview

cashet is a content-addressable compute cache with git semantics. Python functions + args are hashed into cache keys, results stored as immutable blobs, identical calls deduplicated. Local by default (SQLite), with optional Redis and HTTP server for distributed use.

## Commands

- Install deps: `uv sync`
- Install with Redis: `uv sync --extra redis`
- Install with HTTP server: `uv sync --extra server`
- Install with all extras: `uv sync --all-extras`
- Run tests: `uv run pytest tests/ -v`
- Redis tests auto-detect: skipped if Redis isn't reachable at `localhost:6379`. Set `CASHET_REDIS=1` to skip auto-detection and force-enable.
- Start Redis for full suite: `docker run -d --name cashet-redis -p 6379:6379 redis:7-alpine`
- Lint: `uv run ruff check src/ tests/`
- Type check: `uv run pyright src/`
- Format fix: `uv run ruff format src/ tests/`
- Run CLI: `uv run cashet --help`

Always run `ruff check`, `pyright`, and `pytest` before committing. All three must pass clean.

## Tech stack

- Python >=3.11, src layout (`src/cashet/`)
- Build: hatchling
- Package manager: uv (no pip directly)
- Linting: ruff (target py311)
- Type checking: pyright strict mode (target 3.11)
- Testing: pytest
- CLI: click + rich
- Optional backends: Redis (via `redis` extra)
- Optional HTTP server: Starlette + uvicorn (via `server` extra)

## Architecture

Protocol-based dependency injection via three pluggable protocols in `src/cashet/protocols.py`:

- **Store** — metadata + blob storage. Default: `SQLiteStore` in `store.py`. Also: `RedisStore` and `AsyncRedisStore` in `redis_store.py`
- **AsyncStore** — async protocol for IO-bound backends (Redis, S3, HTTP). Defined in `protocols.py`
- **Executor** — runs functions. Default: `LocalExecutor` in `executor.py` (sync). Async variant: `AsyncLocalExecutor` in `async_executor.py`
- **Serializer** — serialize/deserialize results. Default: `PickleSerializer` in `hashing.py`

Data flow (sync): `Client.submit()` → `build_task_def()` hashes function source + args → `LocalExecutor.submit()` checks cache → runs if needed → `build_commit()` creates commit with parent lineage → blobs stored via `Store.put_blob()` with zlib compression (256B threshold).

Async data flow: `AsyncClient.submit()` → `build_task_def()` → `AsyncLocalExecutor.submit()` checks cache via async store → runs function in thread via `asyncio.to_thread()` → stores result via async `AsyncStore.put_blob()`. Heartbeat and locking are asyncio-native.

HTTP server: `client.serve(host, port, require_token=None)` exposes endpoints over HTTP. When `require_token` is set, all requests must include `Authorization: Bearer <token>`. Async server (`AsyncClient.serve()`) uses `create_async_app()` with native async handlers. Sync server (`Client.serve()`) uses `create_app()` which wraps sync Client calls in `asyncio.to_thread`.

## Key design decisions

- Function identity = source code only. Closure mutable state is NOT hashed. Users must pass values as explicit args for cache invalidation.
- Blobs are content-addressable (SHA-256). Identical outputs share one blob on disk.
- `cache=False` opt-out for non-deterministic functions. These get timestamp-salted hashes so they don't overwrite previous commits.
- All source files use `from __future__ import annotations` for union type syntax compatibility with >=3.10.
- `ResultRef` objects enable DAG chaining — pass a ref as an arg to `submit()` and it auto-resolves.
- `AsyncResultRef` is the async counterpart for `AsyncClient`.
- IO-bound layers (Store, HTTP) are async. CPU-bound execution stays on threads via `asyncio.to_thread()`.
- Redis blob data keys: `cashet:blob:data:{hash}`. Ref counters: `cashet:blob:ref:{hash}`. Per-fingerprint locks: `cashet:lock:{fingerprint}`.
- Redis `delete_commit` uses a Lua script (`_DECR_DELETE_SCRIPT`) for atomic DECR + conditional DELETE of blob keys when ref count reaches zero, avoiding a TOCTOU race.
- Sync `Client` and async `AsyncClient` share common helpers extracted to `_client_base.py` (store dir resolution, task config, diff).

## Code style

- No comments or docstrings. Code is self-documenting through naming.
- `from __future__ import annotations` in every source file.
- Ruff rules: E, F, I, N, W, UP, B, SIM, RUF. Line length 99.
- Pyright strict mode with reportUnknown* set to "none".

## Boundaries

- Never commit secrets or .env files
- Never modify `uv.lock` manually — use `uv` commands
- PyPI-ready package with proper metadata and classifiers
- Import name is `cashet`, CLI entry point is `cashet`
- Update CHANGELOG.md before tagging a release.
