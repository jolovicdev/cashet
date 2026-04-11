<h1 align="center">cashet</h1>

<p align="center">
  <strong>Content-addressable compute cache with git semantics</strong><br>
  Run a function once. Get the same result instantly every time after that.
</p>

<p align="center">
  <a href="#install">Install</a> · <a href="#quickstart">Quick Start</a> · <a href="#why">Why</a> · <a href="#use-cases">Use Cases</a> · <a href="#cli">CLI</a> · <a href="#api">API</a> · <a href="#how-it-works">How It Works</a>
</p>

---

## Install

**Use it in any project** (installs the `cashet` CLI globally):

```bash
uv tool install git+https://github.com/jolovicdev/cashet.git
```

```bash
cashet --help
```

**Develop / contribute:**

```bash
git clone https://github.com/jolovicdev/cashet.git
cd cashet
uv sync
uv run pytest
```

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

## Why

You already have caches (`functools.lru_cache`, `joblib.Memory`). Here's what's different:

| | lru_cache | joblib.Memory | **cashet** |
|---|---|---|---|
| Persists across restarts | No | Yes | Yes |
| Content-addressable storage | No | No | Yes (like git blobs) |
| AST-normalized hashing | No | No | Yes (comments/formatting don't break cache) |
| DAG resolution (chain outputs) | No | No | Yes |
| CLI to inspect history | No | No | Yes |
| Diff two runs | No | No | Yes |
| Garbage collection / eviction | No | No | Yes |
| Pluggable serialization | No | No | Yes |
| Explicit cache opt-out | No | Partial | Yes |
| Pluggable store / executor | No | No | Yes |

The core idea: **hash the function's AST-normalized source + arguments = unique cache key**. Comments, docstrings, and formatting changes don't invalidate the cache — only semantic changes do. Same function + same args = same result, stored immutably on disk. The result is a git-like blob you can inspect, diff, and chain.

## Use Cases

### 1. ML Experiment Tracking Without the Bloat

You run 200 hyperparameter sweeps overnight. Half crash. You fix a bug and re-run. Without cashet, you re-process the dataset 200 times. With cashet:

```python
from cashet import Client

client = Client()

def preprocess(dataset_path, image_size):
    # 45 minutes of image resizing
    ...

def train(data, learning_rate, dropout):
    ...

data = client.submit(preprocess, "s3://my-bucket/images", 224)

for lr in [0.01, 0.001, 0.0001]:
    for dropout in [0.2, 0.5]:
        client.submit(train, data, lr, dropout)
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

Share a result with a colleague and they can verify exactly how it was produced:

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

# Retrieve a result to file
cashet get <hash> -o output.bin

# Compare two commits
cashet diff <hash_a> <hash_b>

# Show lineage of a result (same function+args over time)
cashet history <hash>

# Delete a specific commit
cashet rm <hash>

# Evict old cache entries and orphaned blobs
cashet gc --older-than 30

# Storage statistics
cashet stats
```

## API

### `Client`

```python
from cashet import Client

client = Client(
    store_dir=".cashet",         # where to store blobs + metadata (SQLiteStore)
    store=None,                # or inject any Store implementation
    executor=None,             # or inject any Executor implementation
    serializer=None,           # defaults to PickleSerializer
)
```

### Pluggable Backends

Everything is protocol-based. Swap the store, executor, or serializer without touching your task code:

```python
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
    def list_commits(self, ...) -> list[Commit]: ...
    def get_history(self, hash: str) -> list[Commit]: ...
    def stats(self) -> dict[str, int]: ...
    def evict(self, older_than: datetime) -> int: ...
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
ref.hash        # content hash of the result
ref.size        # size in bytes
ref.load()      # deserialize and return the result
```

If the same function + same arguments have been submitted before, returns the cached result **without re-executing**.

**Opt out of caching:**

```python
# Per-call
ref = client.submit(non_deterministic_func, _cache=False)

# Per-function via decorator
@client.task(cache=False)
def random_score():
    return random.random()
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

# Storage stats
stats = client.stats()
# {'total_commits': 42, 'completed_commits': 40, 'stored_objects': 38}
```

### `ResultRef`

A lazy reference to a stored result. Pass it as an argument to chain tasks:

```python
step1 = client.submit(func_a, input_data)
step2 = client.submit(func_b, step1)  # step1 auto-resolves to its output
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

| Protocol | Default | Implement for |
|---|---|---|
| `Store` | `SQLiteStore` | RocksDB, Redis, S3, Postgres |
| `Executor` | `LocalExecutor` | Celery, Kafka, RQ, subprocess |
| `Serializer` | `PickleSerializer` | JSON, MessagePack, custom formats |

**Storage layout** (in `.cashet/`):

```
.cashet/
├── objects/          # content-addressable blobs (like git objects)
│   ├── a3/
│   │   └── b4c5d6... # compressed result blob
│   └── e7/
│       └── f8g9h0...
└── meta.db           # SQLite: commits, fingerprints, provenance
```

**Key design decisions:**

- **Closure variables are not hashed** and emit a `ClosureWarning` if present. Function identity is source code, not runtime state. If you need cache invalidation based on a value, pass it as an explicit argument.
- **Referenced user-defined helper functions are hashed recursively.** Change an imported helper in your own code and the caller's cache invalidates correctly. Builtin and third-party library functions are skipped.
- **Blobs are deduplicated by content hash.** Identical results share one blob on disk.
- **Source is hashed as an AST.** Comments, docstrings, and whitespace changes don't invalidate the cache.
- **Non-cached tasks get unique commit hashes** (timestamp salt) so they always re-execute but still record lineage.
- **Parent tracking:** Each commit records the hash of the previous commit for the same function+args, forming a history chain you can traverse.

## Project Status

**Experimental.** The core (hashing, DAG resolution, fingerprint dedup) is stable. The defaults work reliably for single-machine workflows. The protocol layer (`Store`, `Executor`, `Serializer`) is ready for alternative backends — implementing a Redis store or Celery executor is a single-file job.

Built-in: `SQLiteStore` + `LocalExecutor` + `PickleSerializer`.
Not yet built: Redis, RocksDB, S3 stores; Celery/Kafka executors. PRs welcome.

## License

MIT
