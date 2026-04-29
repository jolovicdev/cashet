<h1 align="center">cashet</h1>

<p align="center">
  <strong>A Python memoization cache with Redis, async support, and an HTTP server.</strong><br>
  Hash functions + args into cache keys. Results stored as immutable blobs. Chain outputs with DAG resolution.<br>
  Run a function once — get the same result instantly every time after.
</p>

<p align="center">
  <a href="#install">Install</a> ·
  <a href="#quickstart">Quick Start</a> ·
  <a href="#why">Why</a> ·
  <a href="#use-cases">Use Cases</a> ·
  <a href="#cli">CLI</a> ·
  <a href="#api">API</a> ·
  <a href="#how-it-works">How It Works</a>
</p>

<p align="center">
  <a href="https://pypi.org/project/cashet/"><img src="https://img.shields.io/pypi/v/cashet?color=blue" alt="PyPI"></a>
  <a href="https://github.com/jolovicdev/cashet/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-MIT-green" alt="License"></a>
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.11%2B-blue" alt="Python"></a>
</p>

---

## Install

**Global CLI tool** (recommended):

```bash
uv tool install cashet
# or
pipx install cashet
```

Then use the CLI anywhere:

```bash
cashet --help
```

**In a project** (library + CLI):

```bash
uv add cashet
# or
pip install cashet
```

This installs `cashet` as both an importable Python library (`from cashet import Client`) and a project-local CLI (`uv run cashet`).

**With Redis backend:**

```bash
uv add "cashet[redis]"
# or
pip install "cashet[redis]"
```

**With HTTP server:**

```bash
uv add "cashet[server]"
# or
pip install "cashet[server]"
```

**All extras:**

```bash
uv add "cashet[redis,server]"
```

**Develop / contribute:**

```bash
git clone https://github.com/jolovicdev/cashet.git
cd cashet
uv sync --all-extras
uv run pytest
```

## Version Compatibility

**0.2.x → 0.3.x: Hash format changed.** `hash_args` was unified with `serialize_args` to use a single canonical representation via `_stable_repr_to`. Same function + same arguments produce different cache fingerprints across the major version line. Caches created with 0.2.x **will not hit** on 0.3.x.

**0.3.0 → 0.3.1:** Redis blob data keys renamed from `cashet:blob:{hash}` to `cashet:blob:data:{hash}` to fix a stats double-counting bug. Redis blob object/byte totals are maintained in `cashet:stats:blob` after a one-time backfill for existing caches. Function hashing also now includes defaults / keyword defaults in more cases, and custom object argument hashing includes the defining module. Existing 0.3.0 caches may miss after upgrading. Clear old caches before upgrading if you use Redis or rely on long-lived cache reuse across versions.

**Upgrading from an incompatible version:** either clear the cache (`cashet clear`) and rebuild, or point the new version at a fresh store directory.

## Quick Start

```python
from cashet import Client

client = Client()  # creates .cashet/ in current directory

def expensive_transform(data, scale=1.0):
    # imagine this takes 10 minutes
    return [x * scale for x in data]

# First call: runs the function
ref = client.submit(expensive_transform, [1, 2, 3], scale=2.0)
print(ref.load())  # [2.0, 4.0, 6.0]

# Second call with same args: instant — returns cached result
ref2 = client.submit(expensive_transform, [1, 2, 3], scale=2.0)
print(ref2.load())  # [2.0, 4.0, 6.0] — no re-computation
```

You can also use `Client` as a context manager to ensure the store connection is closed cleanly:

```python
with Client() as client:
    ref = client.submit(expensive_transform, [1, 2, 3], scale=2.0)
    print(ref.load())
```

Chain tasks into a pipeline where each step's output feeds into the next:

```python
from cashet import Client

client = Client()

def load_dataset(path):
    return list(range(100))

def normalize(data):
    max_val = max(data)
    return [x / max_val for x in data]

def train_model(data, lr=0.01):
    return {"loss": 0.05, "lr": lr, "samples": len(data)}

# Step 1: load
raw = client.submit(load_dataset, "data/train.csv")

# Step 2: normalize (receives raw output as input)
normalized = client.submit(normalize, raw)

# Step 3: train (receives normalized output)
model = client.submit(train_model, normalized, lr=0.001)

print(model.load())  # {'loss': 0.05, 'lr': 0.001, 'samples': 100}
```

Re-run the script — everything returns instantly from cache. Change one argument and only that step (and downstream) re-runs.

**Shared cache with Redis** — multiple processes or machines share one cache:

```python
from cashet import Client
from cashet.redis_store import RedisStore

client = Client(store=RedisStore("redis://localhost:6379"))

# Any process on any machine using the same Redis gets cached results
ref = client.submit(expensive_transform, [1, 2, 3], scale=2.0)
```

**Async API** — drop-in `AsyncClient` for asyncio workflows:

```python
import asyncio
from cashet.async_client import AsyncClient

async def main():
    client = AsyncClient()

    def compute(x: int) -> int:
        return x * 2

    ref = await client.submit(compute, 21)
    result = await ref.load()
    print(result)  # 42

asyncio.run(main())
```

**HTTP server** — expose cache metadata and registered tasks over HTTP:

```bash
python -c "from cashet import Client; Client().serve(port=8000)"
```

```python
# With auth token
from cashet import Client
client = Client()
client.serve(port=8000, require_token="my-secret-token")
```

Register server-side tasks in the server process:

```python
from cashet import Client

client = Client()

def double(x):
    return x * 2

client.serve(port=8000, tasks={"double": double})
```

Submit JSON args from another process:

```python
import requests

r = requests.post("http://localhost:8000/submit", json={
    "task": "double",
    "args": [5],
})
```

Submitting Python source, dill payloads, or serializer-encoded args is disabled by default.
For trusted internal clients only, enable the legacy remote-code path with both
`allow_remote_code=True` and a non-empty `require_token=...`.

## Why

You already have caches (`functools.lru_cache`, `joblib.Memory`). Here's what's different:

| | lru_cache | joblib.Memory | **cashet** |
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

The core idea: **hash the function's AST-normalized source + arguments = unique cache key**. Comments, docstrings, and formatting changes don't invalidate the cache — only semantic changes do. Same function + same args = same result, stored immutably on disk. The result is a git-like blob you can inspect, diff, and chain.

## Use Cases

### 1. ML Experiment Tracking Without the Bloat

You run 200 hyperparameter sweeps overnight. Half crash. You fix a bug and re-run. Without cashet, you re-process the dataset 200 times. With cashet:

```python
from cashet import Client, TaskError, TaskRef

client = Client()

def preprocess(dataset_path, image_size):
    # 45 minutes of image resizing
    ...

def train(data, learning_rate, dropout):
    ...

# Batch submit with topological ordering
# TaskRef(0) refers to the first task's output
results = client.submit_many([
    (preprocess, ("s3://my-bucket/images", 224)),
    (train, (TaskRef(0), 0.01, 0.2)),
    (train, (TaskRef(0), 0.01, 0.5)),
    (train, (TaskRef(0), 0.001, 0.2)),
    (train, (TaskRef(0), 0.001, 0.5)),
    (train, (TaskRef(0), 0.0001, 0.2)),
    (train, (TaskRef(0), 0.0001, 0.5)),
])
```

`preprocess` runs **once** — all 6 training jobs reuse its cached output. Re-run the script tomorrow and even the training results come from cache (same function + same args = instant).

### 2. Data Pipeline Debugging

Your ETL pipeline fails at step 5. You fix a typo. Now you need to re-run steps 5-7 but steps 1-4 are unchanged and expensive:

```python
from cashet import Client

client = Client()

raw = client.submit(load_s3, "s3://logs/2024-05-01/")
clean = client.submit(remove_pii, raw)
enriched = client.submit(join_crm, clean, "select * from users")
report = client.submit(generate_report, enriched)
```

Fix the `join_crm` function and re-run the script. Steps 1-2 return instantly from cache. Only step 3 onward re-executes. This works because cashet tracks which function produced which output — changing a function's source code changes its hash, invalidating downstream cache entries.

### 3. Reproducible Notebook Results

`cashet` is designed to work in Jupyter notebooks and IPython sessions. Share a result with a colleague and they can verify exactly how it was produced:

```python
# your notebook
ref = client.submit(generate_forecast, date="2024-01-01", model="v3")
print(f"Result hash: {ref.hash}")
```

```bash
# their terminal — inspect provenance
cashet show <hash>

# Output:
# Hash:     a3b4c5d6...
# Function: generate_forecast
# Source:   def generate_forecast(date, model): ...
# Args:     (('2024-01-01',), {'model': 'v3'})
# Created:  2024-05-01T10:32:17

# Retrieve the actual result
cashet get <hash> -o forecast.csv
```

### 4. Incremental Computation

Process a large dataset in chunks. Already-processed chunks return instantly:

```python
from cashet import Client

client = Client()

def process_chunk(chunk_id, source_file):
    # expensive per-chunk processing
    ...

results = []
for chunk_id in range(100):
    ref = client.submit(process_chunk, chunk_id, "huge_file.parquet")
    results.append(ref)
```

First run processes all 100 chunks. Second run (even after restarting Python) returns all 100 results instantly. Add a new chunk? Only that one runs.

## CLI

```bash
# Show commit history
cashet log

# Filter by function name
cashet log --func "preprocess"

# Filter by tag
cashet log --tag env=prod --tag experiment=run-1

# Show full commit details (source code, args, error)
cashet show <hash>

# Retrieve a result (pretty-prints strings/dicts/lists)
cashet get <hash>

# Write a result to file
cashet get <hash> -o output.bin

# Compare two commits
cashet diff <hash_a> <hash_b>

# Show lineage of a result (same function+args over time)
cashet history <hash>

# Delete a specific commit
cashet rm <hash>

# Evict old cache entries and orphaned blobs
cashet gc --older-than 30

# Evict oldest entries until under a size limit
cashet gc --max-size 1GB

# Clear everything (alias for gc --older-than 0)
cashet clear

# Export all commits and blobs to a tar.gz archive
cashet export backup.tar.gz

# Import commits and blobs from a tar.gz archive
cashet import backup.tar.gz

# Storage statistics (includes disk size)
cashet stats

# Start the HTTP server
cashet serve --host 127.0.0.1 --port 8000

# Legacy unsafe remote-code mode requires auth
cashet serve --require-token secret123 --allow-remote-code
```

## API

### `Client`

```python
from cashet import Client

client = Client(
    store_dir=".cashet",       # where to store blobs + metadata (SQLiteStore)
                               # falls back to $CASHET_DIR env var if set
    store=None,                # or inject any Store implementation (SQLiteStore, RedisStore)
    executor=None,             # or inject any Executor implementation
    serializer=None,           # defaults to PickleSerializer
    max_workers=1,             # max parallelism for submit_many (default: 1, sequential)
)
```

### `AsyncClient`

```python
import asyncio
from cashet.async_client import AsyncClient

async def main():
    client = AsyncClient(
        store_dir=".cashet",   # defaults to AsyncSQLiteStore
        store=None,            # or AsyncRedisStore, or any AsyncStore
        executor=None,         # defaults to AsyncLocalExecutor
        serializer=None,       # defaults to PickleSerializer
        max_workers=1,         # max parallelism for submit_many (default: 1, sequential)
    )

    def square(x: int) -> int:
        return x * x

    ref = await client.submit(square, 5)
    result = await ref.load()  # 25
    await client.close()

asyncio.run(main())
```

`AsyncClient` mirrors `Client` — `submit()`, `submit_many()`, `log()`, `show()`, `get()`, `stats()`, `gc()`, `rm()`, `clear()`, `serve()` are all `async def`. `submit()` returns `AsyncResultRef` with `async load()`. Chain tasks by passing `AsyncResultRef` as an argument.

### HTTP Server

```python
# Start the server
from cashet import Client
client = Client()
client.serve(host="127.0.0.1", port=8000)

# With Bearer token authentication
client.serve(host="0.0.0.0", port=8000, require_token="secret123")
```

Execute tasks over HTTP by registering callables in the server process and sending
JSON arguments from another process:

```python
from cashet import Client

client = Client()

def add(x: int, y: int) -> int:
    return x + y

client.serve(port=8000, tasks={"add": add})
```

```python
import requests

r = requests.post("http://localhost:8000/submit", json={"task": "add", "args": [3, 4]})
```

Endpoints:

| Method | Path | Description |
|---|---|---|
| POST | `/submit` | Submit a registered task for execution |
| GET | `/result/{hash}` | Fetch deserialized result |
| GET | `/commit/{hash}` | Commit metadata |
| GET | `/log` | List commits (query: `?func=`, `?limit=`, `?status=`) |
| GET | `/stats` | Storage statistics |
| POST | `/gc` | Run garbage collection |

Use `AsyncClient.serve()` for the async variant with `create_async_app()`.

> **Security:** `/submit` does not execute client-supplied Python by default. The legacy
> `func_source`, `func_b64`, `args_b64`, and `kwargs_b64` payloads require
> `allow_remote_code=True` and a non-empty Bearer token. In the CLI, this is
> `cashet serve --require-token secret123 --allow-remote-code`. That mode deserializes
> and executes arbitrary Python, so expose it only to trusted clients.

### Redis Backend

```python
from cashet import Client
from cashet.redis_store import RedisStore

# Sync
client = Client(store=RedisStore("redis://localhost:6379/0"))

# Async
from cashet.async_client import AsyncClient
from cashet.redis_store import AsyncRedisStore

client = AsyncClient(store=AsyncRedisStore("redis://localhost:6379/0"))
```

Redis-backed stores support all operations — cache dedup, fingerprint lookup, history, eviction, and size-based GC. Cross-process claim dedup uses per-fingerprint Redis locks (`cashet:lock:{fingerprint}`) to prevent redundant execution across machines. Last-access eviction is indexed in Redis (`cashet:index:last_accessed`), blob stats are maintained in `cashet:stats:blob`, and blob ref counts (`cashet:blob:ref:{hash}`) enable orphan cleanup without scanning all commits.

### Pluggable Backends

Everything is protocol-based. Swap the store, executor, or serializer without touching your task code:

```python
from pathlib import Path

from cashet import Client, Store, Executor, Serializer
from cashet.store import SQLiteStore
from cashet.executor import LocalExecutor

# These are equivalent (the defaults):
client = Client(store_dir=".cashet")

# Explicit injection:
client = Client(
    store=SQLiteStore(Path(".cashet")),
    executor=LocalExecutor(),
)
```

**Store protocol** — implement this to use RocksDB, Redis, S3, or anything else:

```python
from cashet.protocols import Store

class RedisStore:
    def put_blob(self, data: bytes) -> ObjectRef: ...
    def get_blob(self, ref: ObjectRef) -> bytes: ...
    def put_commit(self, commit: Commit) -> None: ...
    def get_commit(self, hash: str) -> Commit | None: ...
    def find_by_fingerprint(self, fingerprint: str) -> Commit | None: ...
    def find_running_by_fingerprint(self, fingerprint: str) -> Commit | None: ...
    def list_commits(self, ...) -> list[Commit]: ...
    def get_history(self, hash: str) -> list[Commit]: ...
    def stats(self) -> dict[str, int]: ...
    def evict(self, older_than: datetime, max_size_bytes: int | None = None) -> int: ...
    def delete_commit(self, hash: str) -> bool: ...
    def close(self) -> None: ...

client = Client(store=RedisStore("redis://localhost"))
# Everything else works identically
```

**Executor protocol** — implement this for distributed execution (Celery, Kafka, RQ):

```python
from cashet.protocols import Executor

class CeleryExecutor:
    def submit(self, func, args, kwargs, task_def, store, serializer):
        # Push to Celery, poll for result
        ...

client = Client(
    store=RedisStore("redis://localhost"),
    executor=CeleryExecutor(),
)
```

**Serializer protocol** — already covered below.

### `client.submit(func, *args, **kwargs) -> ResultRef`

Submit a function for execution. Returns a `ResultRef` — a lazy handle to the result.

```python
ref = client.submit(my_func, arg1, arg2, key="value")
ref.hash         # content hash of the result blob
ref.commit_hash  # commit hash (use this for show/history/rm/get)
ref.size         # size in bytes
ref.load()       # deserialize and return the result
```

If the same function + same arguments have been submitted before, returns the cached result **without re-executing**.

### `client.clear()`

Remove all cache entries and orphaned blobs. Equivalent to `client.gc(timedelta(days=0))`.

```python
client.clear()
```

### `client.submit_many(tasks) -> list[ResultRef]`

Submit a batch of tasks with automatic topological ordering. Use `TaskRef(index)` to wire outputs between tasks in the batch.

```python
from cashet import TaskRef

refs = client.submit_many([
    step1_func,
    (step2_func, (TaskRef(0),)),
    (step3_func, (TaskRef(1), "extra_arg")),
], max_workers=4)  # run independent tasks in parallel
```

This enables parallel fan-out and ensures each task only runs after its dependencies.

> **Note:** With the default `SQLiteStore`, parallel execution serializes on the SQLite write lock — `max_workers > 1` only benefits compute-heavy tasks where execution time dominates. For true parallel fan-out, use `RedisStore`.

### `client.map(func, items, *args, **kwargs) -> list[ResultRef]`

Map a function over an iterable with per-item caching. Each item becomes the first positional argument; additional `*args` and `**kwargs` are appended.

```python
refs = client.map(process_chunk, range(100), source_file="data.parquet")
# refs[0].load() == process_chunk(0, source_file="data.parquet")

results = [r.load() for r in refs]
```

Already-computed items return instantly from cache. Add a new chunk later and only that item re-runs.

**Opt out of caching:**

```python
# Per-call
ref = client.submit(non_deterministic_func, _cache=False)

# Per-function via decorator
@client.task(cache=False)
def random_score():
    return random.random()
```

**Force re-execution (skip cache, always run):**

```python
# Per-call
ref = client.submit(my_func, arg, _force=True)

# Per-function via decorator
@client.task(force=True)
def always_rerun():
    ...
```

**Tag commits:**

```python
# Per-call
ref = client.submit(train, data, lr=0.01, _tags={"experiment": "v1"})

# Per-function via decorator
@client.task(tags={"team": "ml"})
def preprocess(raw):
    ...
```

Tags are not part of the cache key — they are metadata for organization and filtering.

**Retry flaky operations:**

```python
# Per-call
ref = client.submit(fetch_api, url, _retries=3)

# Per-function via decorator
@client.task(retries=3)
def fetch_api(url):
    ...
```

Retries wait briefly between attempts. When retries are exhausted, `client.submit` raises `TaskError` with the original traceback included in the message.

**Task timeouts:**

```python
# Per-call (seconds)
ref = client.submit(slow_func, _timeout=30)

# Per-function via decorator
@client.task(timeout=30)
def slow_func():
    ...
```

Timeouts can be combined with retries — a timed-out attempt counts as a failure and will be retried. Local execution uses Python threads, so timeouts are soft: cashet stops waiting and records the attempt as failed, but Python cannot safely kill already-running thread code. Use idempotent task functions; hard cancellation belongs in a process/distributed executor such as a future Celery executor.

### `@client.task`

Register a function with cashet metadata and make it directly callable:

```python
@client.task
def my_func(x):
    return x * 2

ref = my_func(5)  # Returns ResultRef, same as client.submit(my_func, 5)
ref.load()        # 10

@client.task(cache=False, name="custom_task_name", tags={"env": "prod"})
def other_func(x):
    return x + 1
```

`client.submit(my_func, 5)` still works identically.

### `client.log()`, `client.show()`, `client.get()`, `client.diff()`, `client.history()`, `client.rm()`, `client.gc()`

```python
# List commits
commits = client.log(func_name="preprocess", limit=10)

# Filter by status
commits = client.log(status="failed")

# Filter by tags
commits = client.log(tags={"experiment": "v1"})

# Get commit details
commit = client.show(hash)
commit.task_def.func_source  # the source code
commit.task_def.args_snapshot  # the serialized args
commit.parent_hash  # previous commit for same func+args
commit.created_at

# Load a result by commit hash
result = client.get(hash)

# Diff two commits
diff = client.diff(hash_a, hash_b)
# {'func_changed': True, 'args_changed': False, 'output_changed': True, ...}

# Get lineage (all runs of same func+args)
history = client.history(hash)

# Evict old entries (default: 30 days)
evicted = client.gc()
# Evict entries older than 7 days
from datetime import timedelta
evicted = client.gc(older_than=timedelta(days=7))
# Evict oldest entries until under size limit
evicted = client.gc(max_size_bytes=1024 * 1024 * 1024)  # 1GB

# Storage stats
stats = client.stats()
# {
#     'total_commits': 42,
#     'completed_commits': 40,
#     'stored_objects': 38,      # blob_objects + inline_objects
#     'disk_bytes': 10485760,    # blob_bytes + inline_bytes
#     'blob_objects': 35,
#     'blob_bytes': 9437184,
#     'inline_objects': 3,
#     'inline_bytes': 1048576,
# }
```

### `client.export(path)` / `client.import_archive(path)`

Export the entire cache to a portable `.tar.gz` archive and import it elsewhere. Blobs are content-addressable, so deduplication is preserved across stores.

```python
# Export
client.export("backup.tar.gz")

# Import into a fresh store
client2 = Client(store_dir=".cashet2")
count = client2.import_archive("backup.tar.gz")
print(f"Imported {count} commits")
```

Use this for migrations (SQLite → Redis), CI cache warm-up, or backups. Existing commits are skipped during import.

### Jupyter & Notebook Support

`cashet` works seamlessly in Jupyter notebooks, IPython, and the Python REPL. It uses a tiered source-resolution strategy:

1. **`inspect.getsource()`** — for normal `.py` files
2. **`dill.source.getsource()`** — for interactive sessions with live history
3. **`dis.Bytecode` fallback** — for any live function, even after a kernel restart

This means you can define functions in a notebook cell, rerun the cell with changes, and `cashet` will correctly invalidate the cache based on the new code.

```python
# In a notebook cell
client = Client()

def preprocess(data):
    return [x * 2 for x in data]

ref = client.submit(preprocess, [1, 2, 3])
```

Change the cell body and rerun — the cache invalidates automatically.

### Thread Safety

`cashet` is safe to use from multiple threads and processes sharing the same store directory. Concurrent submissions of the same uncached task are deduplicated: the function executes **exactly once** and all callers receive the same cached result. This works across `multiprocessing.Process`, `ProcessPoolExecutor`, and multiple independent Python interpreters.

> **Note:** Cross-process dedup uses a 5-minute timeout by default. If a process dies while running a task, its claim is automatically reclaimed after that timeout so other workers are not blocked forever. You can adjust this via `LocalExecutor(running_ttl=...)`:
>
> ```python
> from datetime import timedelta
> from cashet.executor import LocalExecutor
>
> client = Client(executor=LocalExecutor(running_ttl=timedelta(minutes=10)))
> ```

```python
import threading

def worker():
    c = Client()  # separate Client instance, same store
    c.submit(expensive_func, arg)

threads = [threading.Thread(target=worker) for _ in range(10)]
for t in threads:
    t.start()
for t in threads:
    t.join()
# expensive_func ran only once
```

### `ResultRef`

A lazy reference to a stored result. Pass it as an argument to chain tasks:

```python
step1 = client.submit(func_a, input_data)
step2 = client.submit(func_b, step1)  # step1 auto-resolves to its output
```

`ResultRef` is generic — `submit()` infers the return type from the function annotation:

```python
ref: ResultRef[int] = client.submit(double, 5)
result = ref.load()  # typed as int
```

### Custom Serialization

```python
from cashet import Client, PickleSerializer, SafePickleSerializer, JsonSerializer

# Default: pickle (handles arbitrary Python objects)
client = Client(serializer=PickleSerializer())

# Safe pickle: restricts deserialization to an allowlist of known types
client = Client(serializer=SafePickleSerializer())

# Allow custom classes through the allowlist
client = Client(serializer=SafePickleSerializer(extra_classes=[MyClass]))

# For JSON-safe data (dicts, lists, primitives)
client = Client(serializer=JsonSerializer())

# Or implement the Serializer protocol
from cashet.hashing import Serializer

class MySerializer:
    def dumps(self, obj) -> bytes:
        ...
    def loads(self, data: bytes):
        ...
```

## How It Works

```
client.submit(func, arg1, arg2)
         │
         ▼
  ┌─────────────────┐
  │  Hash function   │  SHA256(AST-normalized source + dep versions + referenced user helpers)
  │  Hash arguments  │  SHA256(canonical repr of args/kwargs)
  └────────┬────────┘
           │
           ▼
  ┌─────────────────┐
  │  Fingerprint     │  func_hash:args_hash
  │  cache lookup    │  ← Store protocol (SQLiteStore, RedisStore, ...)
  └────────┬────────┘
           │
     ┌─────┴─────┐
     │            │
  CACHED       MISS
     │            │
     ▼            ▼
  Return ref   ← Executor protocol (LocalExecutor, CeleryExecutor, ...)
               Execute function
               Store result as blob → Store protocol
               Record commit with parent lineage
               Return ref
```

**Architecture (protocol-based):**

| Protocol | Default | Built-in alternatives | Implement for |
|---|---|---|---|
| `Store` | `SQLiteStore` | `RedisStore` | RocksDB, S3, Postgres |
| `AsyncStore` | `AsyncSQLiteStore` | `AsyncRedisStore` | async variants of above |
| `Executor` | `LocalExecutor` | — | Celery, Kafka, RQ |
| `AsyncExecutor` | `AsyncLocalExecutor` | — | Celery, Kafka, RQ |
| `Serializer` | `PickleSerializer` | `JsonSerializer`, `SafePickleSerializer` | MessagePack, custom |

**Storage layout** (in `.cashet/`):

```
.cashet/
├── objects/          # content-addressable blobs (like git objects)
│   ├── a3/
│   │   └── b4c5d6... # compressed result blob
│   └── e7/
│       └── f8g9h0...
└── meta.db           # SQLite: commits, fingerprints, provenance, inline_objects
```

**Small objects** (<1KB) are stored inline in `meta.db` instead of the filesystem. This reduces inode overhead for caches with many tiny results. Larger objects are stored as compressed blobs in `objects/` as usual.

**Key design decisions:**

- **Closure variables are not hashed** and emit a `ClosureWarning` if present. Function identity is source code, defaults, keyword defaults, and referenced helper functions; not arbitrary runtime state. If you need cache invalidation based on a value, pass it as an explicit argument.
- **Referenced user-defined helper functions are hashed recursively.** If your cached function calls or references a helper from your own project (via `co_names` / `globals`), that helper's source is included in the cache key. Change the helper and the caller's cache invalidates. Builtin and stdlib functions are skipped. This behavior is automatic and invisible — no decorators or imports needed.
- **Blobs are deduplicated by content hash.** Identical results share one blob on disk.
- **Source is hashed as an AST.** Comments, docstrings, and whitespace changes don't invalidate the cache.
- **Custom object arguments include their class module and qualname** in the argument hash so same-named classes from different modules do not collide.
- **Non-cached tasks get unique commit hashes** (timestamp salt) so they always re-execute but still record lineage.
- **Parent tracking:** Each commit records the hash of the previous commit for the same function+args, forming a history chain you can traverse.

## Configuration

- **`CASHET_DIR`** — override the default `.cashet` store directory. Equivalent to passing `store_dir=`.
- **`CASHET_LOG`** — set to `DEBUG`, `INFO`, `WARNING`, or `ERROR` to print log output to stderr with a `[cashet]` tag. Messages include task fingerprint, function name, commit hash, and duration inline — no structured logging backend needed.

## Project Status

**Beta.** The core (hashing, DAG resolution, fingerprint dedup) is stable. Works reliably for single-machine, multiprocess, and multi-machine (Redis) workflows.

Built-in: `SQLiteStore` + `AsyncSQLiteStore`, `RedisStore` + `AsyncRedisStore`, `LocalExecutor` + `AsyncLocalExecutor`, `PickleSerializer` + `JsonSerializer` + `SafePickleSerializer`, HTTP server (`client.serve()`), CLI.

Not yet built: RocksDB, S3 stores; Celery/Kafka executors. PRs welcome.

## License

MIT
