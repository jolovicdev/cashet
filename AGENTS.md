# AGENTS.md

## Project overview

cashet is a content-addressable compute cache with git semantics. Python functions + args are hashed into cache keys, results stored as immutable blobs, identical calls deduplicated. Local-only but protocol-based for future distributed backends.

## Commands

- Install deps: `uv sync`
- Run tests: `uv run pytest tests/ -v`
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

## Architecture

Protocol-based dependency injection via three pluggable protocols in `src/cashet/protocols.py`:

- **Store** — metadata + blob storage. Default: `SQLiteStore` in `store.py`
- **Executor** — runs functions. Default: `LocalExecutor` in `executor.py`
- **Serializer** — serialize/deserialize results. Default: `PickleSerializer` in `hashing.py`

Data flow: `Client.submit()` → `build_task_def()` hashes function source + args → `LocalExecutor.submit()` checks cache → runs if needed → `build_commit()` creates commit with parent lineage → blobs stored via `Store.put_blob()` with zlib compression (256B threshold).

## Key design decisions

- Function identity = source code only. Closure mutable state is NOT hashed. Users must pass values as explicit args for cache invalidation.
- Blobs are content-addressable (SHA-256). Identical outputs share one blob on disk.
- `cache=False` opt-out for non-deterministic functions. These get timestamp-salted hashes so they don't overwrite previous commits.
- All source files use `from __future__ import annotations` for union type syntax compatibility with >=3.10.
- `ResultRef` objects enable DAG chaining — pass a ref as an arg to `submit()` and it auto-resolves.

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
