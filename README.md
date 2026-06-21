<h1 align="center">cashet</h1>

<p align="center">
  <strong>Content-addressable compute cache for Python: persistent function memoization with Redis, async, DAG pipelines, and an HTTP server.</strong><br>
  Hash a function plus its arguments into a cache key, store the result as an immutable blob, and return it instantly on every later call.<br>
  Think git for your function results.
</p>

<p align="center">
  <em>Keywords: Python caching, memoization, disk cache, Redis cache, async asyncio, DAG pipeline, content-addressable, reproducible computation, joblib / lru_cache alternative.</em>
</p>

<p align="center">
  <a href="#install">Install</a> ·
  <a href="#quick-start">Quick Start</a> ·
  <a href="#why-cashet">Why cashet</a> ·
  <a href="#use-cases">Use Cases</a> ·
  <a href="#cli">CLI</a> ·
  <a href="#python-api">Python API</a> ·
  <a href="#how-it-works">How It Works</a>
</p>

<p align="center">
  <a href="https://pypi.org/project/cashet/"><img src="https://img.shields.io/pypi/v/cashet?color=blue" alt="PyPI"></a>
  <a href="https://github.com/jolovicdev/cashet/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-MIT-green" alt="License"></a>
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.11%2B-blue" alt="Python"></a>
</p>

---

## What is cashet

cashet is a caching library for expensive Python functions. You submit a function and its arguments, cashet runs it once, stores the result as a content-addressed blob, and serves that result instantly on every later call with the same code and inputs. Caches persist across process restarts, can be shared across machines through Redis, and can be inspected, diffed, and chained from the CLI or the Python API.

The cache key is a SHA-256 hash of the function's AST-normalized source plus its arguments. Comments, docstrings, and formatting do not invalidate the cache; only a real change to the code or the inputs does. Identical results are deduplicated to a single blob on disk, and every result is a git-like object you can inspect and chain into pipelines.

## Install

Install as a global CLI tool:

```bash
uv tool install cashet
# or
pipx install cashet
```

Add to a project as a library and project-local CLI:

```bash
uv add cashet
# or
pip install cashet
```

Optional backends:

```bash
uv add "cashet[redis]"     # shared cache via Redis
uv add "cashet[server]"    # HTTP server
uv add "cashet[redis,server]"
```

Develop and contribute:

```bash
git clone https://github.com/jolovicdev/cashet.git
cd cashet
uv sync --all-extras
uv run pytest
```

## Quick Start

```python
from cashet import Client

client = Client()  # creates .cashet/ in the current directory

def expensive_transform(data, scale=1.0):
    # imagine this takes 10 minutes
    return [x * scale for x in data]

# First call runs the function
ref = client.submit(expensive_transform, [1, 2, 3], scale=2.0)
print(ref.load())  # [2.0, 4.0, 6.0]

# Same args again: instant, returns the cached result with no re-computation
ref2 = client.submit(expensive_transform, [1, 2, 3], scale=2.0)
print(ref2.load())  # [2.0, 4.0, 6.0]
```

`Client` works as a context manager so the store connection closes cleanly:

```python
with Client() as client:
    ref = client.submit(expensive_transform, [1, 2, 3], scale=2.0)
    print(ref.load())
```

### Pipelines

Pass a result reference straight into the next task. Each step's output feeds the next, and cashet records the lineage:

```python
client = Client()

raw = client.submit(load_dataset, "data/train.csv")
normalized = client.submit(normalize, raw)            # receives raw's output
model = client.submit(train_model, normalized, lr=0.001)

print(model.load())
```

Re-run the script and everything returns instantly from cache. Change one argument and only that step and the steps downstream of it re-run.

### Shared cache with Redis

Multiple processes or machines share one cache:

```python
from cashet import Client
from cashet.redis_store import RedisStore

client = Client(store=RedisStore("redis://localhost:6379/0"))
ref = client.submit(expensive_transform, [1, 2, 3], scale=2.0)
```

### Async

`AsyncClient` is a drop-in for asyncio workflows and accepts both sync and async callables:

```python
import asyncio
from cashet.async_client import AsyncClient

async def main():
    client = AsyncClient()
    ref = await client.submit(lambda x: x * 2, 21)
    print(await ref.load())  # 42
    await client.close()

asyncio.run(main())
```

### HTTP server

Expose cache metadata and registered tasks over HTTP:

```python
from cashet import Client

client = Client()

def double(x):
    return x * 2

client.serve(port=8000, tasks={"double": double})
```

```python
import requests

requests.post("http://localhost:8000/submit", json={"task": "double", "args": [5]})
```

By default the server only runs tasks registered in the server process. Submitting Python source, dill payloads, or serializer-encoded args is disabled unless you explicitly opt in (see [HTTP server endpoints](#http-server-endpoints)).

## Why cashet

You already have `functools.lru_cache` and `joblib.Memory`. cashet adds persistence, sharing, provenance, and pipelines on top of memoization:

| | lru_cache | joblib.Memory | cashet |
|---|---|---|---|
| AST-normalized hashing | No | No | Yes |
| DAG / pipeline chaining | No | No | Yes |
| Content-addressable storage | No | No | Yes |
| CLI to inspect history | No | No | Yes |
| Diff two runs | No | No | Yes |
| Garbage collection / eviction | No | No | Yes |
| Pluggable serialization | No | No | Yes |
| Pluggable store / executor | No | No | Yes |
| Redis backend (shared cache) | No | No | Yes |
| Async client (asyncio) | No | No | Yes |
| HTTP server | No | No | Yes |
| Persists across restarts | No | Yes | Yes |

The core idea: hash the function's AST-normalized source plus its arguments into a unique cache key. Same function and same args give the same result, stored immutably on disk, that you can inspect, diff, and chain.

## Use Cases

**ML experiment tracking.** Run many hyperparameter sweeps that share one expensive preprocessing step. `preprocess` runs once and every training job reuses its cached output. Use `TaskRef(index)` to wire one task's output into another within a batch:

```python
from cashet import Client, TaskRef

client = Client()

results = client.submit_many([
    (preprocess, ("s3://bucket/images", 224)),
    (train, (TaskRef(0), 0.01, 0.2)),
    (train, (TaskRef(0), 0.001, 0.2)),
    (train, (TaskRef(0), 0.0001, 0.2)),
])
```

**Data pipeline debugging.** Your ETL job fails at step 5. Fix the function and re-run the script. Unchanged upstream steps return from cache; only the changed step and everything after it re-executes, because changing a function's source changes its hash and invalidates downstream entries.

**Reproducible notebooks.** Share a result hash and a colleague can inspect exactly how it was produced from their terminal with `cashet show <hash>` and retrieve it with `cashet get <hash> -o out.bin`.

**Incremental computation.** Process a large dataset in chunks with `client.map`. Already-processed chunks return instantly; add a new chunk and only that one runs:

```python
refs = client.map(process_chunk, range(100), source_file="data.parquet")
results = [r.load() for r in refs]
```

## CLI

```bash
cashet log                         # commit history
cashet log --func preprocess       # filter by function
cashet log --tag env=prod          # filter by tag
cashet show <hash>                 # full commit details (source, args, error)
cashet get <hash>                  # retrieve a result (pretty-prints str/dict/list)
cashet get <hash> -o output.bin    # write a result to a file
cashet diff <hash_a> <hash_b>      # compare two commits
cashet history <hash>              # lineage of one function+args over time
cashet rm <hash>                   # delete a commit
cashet invalidate -t env=prod      # delete commits by tag
cashet gc --older-than 30          # evict entries older than 30 days
cashet gc --max-size 1GB           # evict oldest entries until under a size limit
cashet clear                       # remove everything
cashet export backup.tar.gz        # export commits and blobs to an archive
cashet import backup.tar.gz        # import from an archive
cashet stats                       # storage statistics
cashet serve --host 127.0.0.1 --port 8000
```

`cashet show`, `cashet get`, and `cashet rm` exit with a non-zero status when the commit is not found, so they compose in scripts.

## Python API

### Client and AsyncClient

```python
from cashet import Client

client = Client(
    store_dir=".cashet",   # blob + metadata directory; falls back to $CASHET_DIR
    store=None,            # or any Store (SQLiteStore, RedisStore, ...)
    executor=None,         # or any Executor
    serializer=None,       # defaults to PickleSerializer
    max_workers=1,         # parallelism for submit_many (1 = sequential)
)
```

`AsyncClient` (from `cashet.async_client`) mirrors `Client`: `submit`, `submit_many`, `map`, `log`, `show`, `get`, `diff`, `history`, `stats`, `gc`, `rm`, `clear`, `invalidate`, and `serve` are all `async def`. It returns `AsyncResultRef` with an `async load()`.

### submit and ResultRef

```python
ref = client.submit(my_func, arg1, arg2, key="value")
ref.hash          # content hash of the result blob
ref.commit_hash   # commit hash (use for show / history / rm / get)
ref.size          # size in bytes
ref.load()        # deserialize and return the result
```

If the same function and arguments were submitted before, `submit` returns the cached result without re-executing. `ResultRef` is generic, so `submit` infers the return type from the function's annotation. Pass a `ResultRef` as an argument (including nested inside lists, tuples, sets, frozensets, and dicts) to chain tasks; it resolves to its output before execution.

### Per-task options

Each option is available per call (prefixed with `_`) and on the `@client.task` decorator:

```python
ref = client.submit(fetch_api, url, _retries=3, _timeout=30, _ttl=3600, _tags={"env": "prod"})

@client.task(cache=False, retries=3, timeout=30, ttl=3600, tags={"team": "ml"})
def fetch_api(url):
    ...

ref = my_task(url)  # decorated tasks are directly callable and return a ResultRef
```

- `cache=False` runs the function every time but still records lineage (non-deterministic work).
- `force=True` skips the cache and re-executes once.
- `retries=N` retries failed attempts, then raises `TaskError` with the original traceback.
- `timeout=seconds` bounds an attempt. Local timeouts are soft: cashet stops waiting and records a failure, but Python cannot kill running thread code, so keep task functions idempotent.
- `ttl=seconds` expires a commit. Expired commits are skipped on lookup and removed at garbage collection.
- `tags={...}` attach metadata for filtering. Tags are not part of the cache key.

### submit_many and map

```python
from cashet import TaskRef

refs = client.submit_many([
    step1_func,
    (step2_func, (TaskRef(0),)),
    (step3_func, (TaskRef(1), "extra")),
], max_workers=4)

refs = client.map(process_chunk, range(100), source_file="data.parquet")
```

`submit_many` orders tasks topologically and runs independent ones in parallel up to `max_workers`. With the default `SQLiteStore`, parallel execution serializes on the SQLite write lock, so `max_workers > 1` mainly helps compute-heavy tasks; use `RedisStore` for true fan-out across processes.

### Inspecting, diffing, and eviction

```python
commits = client.log(func_name="preprocess", limit=10, status="failed", tags={"experiment": "v1"})

commit = client.show(hash)
commit.task_def.func_source     # the source code
commit.task_def.args_snapshot   # the serialized args
commit.parent_hash              # previous commit for the same func + args

result = client.get(hash)
diff = client.diff(hash_a, hash_b)   # {'func_changed': ..., 'args_changed': ..., 'output_changed': ...}
history = client.history(hash)       # all runs of the same func + args

from datetime import timedelta
client.gc(older_than=timedelta(days=7))
client.gc(max_size_bytes=1024 ** 3)  # evict until under 1 GB
client.invalidate(tags={"experiment": "v1"})
client.clear()

stats = client.stats()
# {'total_commits', 'completed_commits', 'stored_objects', 'disk_bytes',
#  'blob_objects', 'blob_bytes', 'inline_objects', 'inline_bytes'}
```

### Export and import

Export the whole cache to a portable archive and import it elsewhere. Blobs are content-addressed, so deduplication is preserved across stores. Use this for migrations (SQLite to Redis), CI cache warm-up, or backups.

```python
client.export("backup.tar.gz")

client2 = Client(store_dir=".cashet2")
result = client2.import_archive("backup.tar.gz")
print(f"imported {result.imported}, skipped {result.skipped}")
```

`import_archive` verifies every blob's content hash and returns an `ImportResult(imported, skipped)`. Commits whose blobs are missing from the archive are skipped and reported rather than silently dropped; existing commits are skipped without re-import.

### HTTP server endpoints

```python
from cashet import Client

client = Client()
client.serve(host="127.0.0.1", port=8000, require_token="secret123")
```

| Method | Path | Description |
|---|---|---|
| POST | `/submit` | Run a registered task |
| GET | `/result/{hash}` | Fetch the deserialized result |
| GET | `/commit/{hash}` | Commit metadata |
| GET | `/log` | List commits (`?func=`, `?limit=`, `?status=`) |
| GET | `/stats` | Storage statistics |
| POST | `/gc` | Run garbage collection |

When `require_token` is set, every request needs an `Authorization: Bearer <token>` header. Request bodies are size-limited (default 500 MB). Use `AsyncClient.serve()` for the native async server.

> **Security.** `/submit` does not execute client-supplied Python by default. The legacy `func_source`, `func_b64`, `args_b64`, and `kwargs_b64` payloads require both `allow_remote_code=True` and a non-empty token (`cashet serve --require-token secret123 --allow-remote-code`). That mode deserializes and runs arbitrary Python, so expose it only to trusted clients.

### Serialization

```python
from cashet import Client, PickleSerializer, SafePickleSerializer, JsonSerializer

Client(serializer=PickleSerializer())                       # default, arbitrary Python objects
Client(serializer=SafePickleSerializer())                   # restrict unpickling to an allowlist
Client(serializer=SafePickleSerializer(extra_classes=[MyClass]))
Client(serializer=JsonSerializer())                         # JSON-safe data
```

Implement the `Serializer` protocol (`dumps`/`loads`) for MessagePack or any custom format.

### Pluggable backends

Everything is protocol-based, so you can swap the store, executor, or serializer without changing task code.

```python
from cashet import Client
from cashet.store import SQLiteStore
from cashet.executor import LocalExecutor

client = Client(store=SQLiteStore(".cashet"), executor=LocalExecutor())
```

| Protocol | Default | Built-in alternatives | Implement for |
|---|---|---|---|
| `Store` | `SQLiteStore` | `RedisStore` | RocksDB, S3, Postgres |
| `AsyncStore` | `AsyncSQLiteStore` | `AsyncRedisStore` | async variants |
| `Executor` | `LocalExecutor` | | Celery, Kafka, RQ |
| `AsyncExecutor` | `AsyncLocalExecutor` | | Celery, Kafka, RQ |
| `Serializer` | `PickleSerializer` | `JsonSerializer`, `SafePickleSerializer` | MessagePack, custom |

## How It Works

```
client.submit(func, arg1, arg2)
         │
         ▼
  ┌──────────────────┐
  │  Hash function   │  SHA256(AST-normalized source + dep versions + referenced helpers)
  │  Hash arguments  │  SHA256(canonical repr of args/kwargs)
  └────────┬─────────┘
           │
           ▼
  ┌──────────────────┐
  │  Fingerprint     │  func_hash:args_hash
  │  cache lookup    │  ← Store protocol (SQLiteStore, RedisStore, ...)
  └────────┬─────────┘
           │
     ┌─────┴─────┐
   CACHED       MISS
     │            │
     ▼            ▼
  Return ref   Execute (Executor protocol), store result as a blob,
               record a commit with parent lineage, return ref
```

Storage layout under `.cashet/`:

```
.cashet/
├── objects/          # content-addressable blobs, like git objects
│   └── a3/b4c5d6...   # compressed result blob
└── meta.db           # SQLite: commits, fingerprints, provenance, inline objects
```

Small results (under 1 KB) are stored inline in `meta.db` to avoid inode overhead; larger results are compressed blobs in `objects/`.

### Key design decisions

- **Function identity is source code, not runtime state.** The hash covers AST-normalized source, default and keyword-default values, immutable referenced globals, and referenced user-defined helper functions. Change a helper and the caller's cache invalidates. Builtins and stdlib are skipped.
- **Closure variables are not hashed** and emit a `ClosureWarning`. To invalidate on a mutable value, pass it as an explicit argument.
- **Arguments are hashed by value, including objects.** Custom objects are hashed by their `__dict__` and `__slots__` state plus their class module and qualname, so same-named classes from different modules do not collide.
- **Source is canonicalized with `ast.unparse`,** so comments, docstrings, and whitespace do not invalidate the cache, and hashes stay stable across Python versions.
- **Blobs are deduplicated by content hash.** Identical results share one blob on disk.
- **Nested refs resolve through containers.** `ResultRef` values inside lists, tuples, sets, frozensets, and dicts are loaded before execution and recorded as commit inputs.
- **Non-cached tasks get a timestamp-salted commit hash,** so they always re-execute while still recording lineage.

### Concurrency

cashet is safe across threads, processes, and machines that share one store. Concurrent submissions of the same uncached task are deduplicated: the function runs exactly once and all callers get the same result. Cross-process claims use file locks (SQLite) or per-fingerprint Redis locks, with a heartbeat lease so a crashed worker's claim is reclaimed (default 5 minutes, configurable via `LocalExecutor(running_ttl=...)`).

### Notebooks

cashet resolves function source through `inspect.getsource`, then `dill.source.getsource` for interactive sessions, then a bytecode fallback that survives a kernel restart. Edit a notebook cell, rerun it, and the cache invalidates on the new code.

## Configuration

- **`CASHET_DIR`** overrides the default `.cashet` store directory, equivalent to `store_dir=`.
- **`CASHET_LOG`** set to `DEBUG`, `INFO`, `WARNING`, or `ERROR` prints logs to stderr with a `[cashet]` tag, including fingerprint, function name, commit hash, and duration.

## Version Compatibility

Upgrading across a hash-format change does not corrupt anything; old entries simply miss and recompute on first access. To start clean, run `cashet clear` or point at a fresh store directory.

- **0.4.4 to 0.4.5:** Hashing fixes (slotted-object state, referenced global containers, `ast.unparse` canonicalization) change function and argument cache keys, so results cached by earlier versions recompute on first access. The Redis tag index key scheme changed and is not migrated; rewrite affected commits to rebuild tag indexes. `import_archive` now returns `ImportResult(imported, skipped)` instead of a bare count.
- **0.3.x to 0.4.0:** Added per-entry TTL and tag-based invalidation. SQLite auto-migrates; old caches stay readable.
- **0.3.0 to 0.3.1:** Redis blob keys renamed to `cashet:blob:data:{hash}` and stats backfilled once. Clear Redis caches before upgrading if you rely on long-lived reuse.
- **0.2.x to 0.3.x:** Hash format unified; caches from 0.2.x do not hit on 0.3.x.

## Project Status

Beta. The core (hashing, DAG resolution, fingerprint dedup) is stable and works for single-machine, multiprocess, and multi-machine (Redis) workflows.

Built in: `SQLiteStore` and `AsyncSQLiteStore`, `RedisStore` and `AsyncRedisStore`, `LocalExecutor` and `AsyncLocalExecutor`, `PickleSerializer`, `JsonSerializer`, and `SafePickleSerializer`, the HTTP server, and the CLI. Not yet built: RocksDB and S3 stores, Celery and Kafka executors. Pull requests welcome.

## License

MIT
