# Changelog

## 0.5.0 - 12.7.2026.

Experimental correctness and performance release. Cache hits are read-only
and roughly 10x faster; several data-integrity and cross-store consistency
bugs are fixed. Read the Notes before upgrading shared stores.

### Fixed
- Blobs are written atomically (temp file, then rename). A crash mid-write
  could leave a truncated file at the content-addressed path that the
  deduplication check then trusted forever, permanently poisoning that hash.
  `get_blob` also deletes a corrupt blob file so the next `put_blob` can store
  the content again.
- Redis fingerprint lookups return the newest completed commit, matching
  SQLite. The per-fingerprint index was ordered by expiry with arbitrary tie
  breaking, so after a force re-run later calls could keep serving the older
  result. Legacy index entries are rescored in place on first lookup.
- Garbage collection removes TTL-expired commits on both stores instead of
  keeping them until they age past the access-time cutoff. Running and pending
  commits are exempt from expiry eviction: their TTL clock starts at claim
  time, so a task outliving its TTL must not be deleted mid-execution.
- A store error during a heartbeat renewal no longer kills the lease loop
  (which let another worker reclaim and double-run a live task) and can no
  longer surface as an exception that destroys an already-computed result.
  Failed renewals retry on a quarter of the normal interval, so a single
  transient error cannot leave the claim stale before the next attempt.
- Set and frozenset arguments are ordered by their stable serialized form
  when hashing. Ordering by raw repr embedded memory addresses, so the same
  logical set could hash differently in another process.
- Per-fingerprint lock state no longer grows without bound: SQLite fingerprint
  locks are striped across 256 paths, and Redis locks are created per
  acquisition instead of being cached per fingerprint.

### Performance
Measured with `benchmarks/bench_hot_path.py` on one machine (medians,
0.4.5 vs 0.5.0):
- sync cache hit: 4.0 ms to 0.35 ms
- async cache hit: 3.9 ms to 0.21 ms
- hashing (`build_task_def`): 256 us to 10 us
- cache miss (run + store): 8.2 ms to 2.7 ms

- Cache hits are read-only: no fingerprint lock, no commit rewrite, and no
  redundant parent query on the claim path.
- `last_accessed_at` is bumped at most once per hour per commit. Eviction
  cutoffs are measured in days, so LRU eviction behavior is unchanged.
- SQLite connections use `synchronous=NORMAL` under WAL: a power loss can
  drop the newest commits, which a compute cache recomputes; the database
  itself cannot corrupt.
- Function source resolution and AST canonicalization are memoized per
  function object. Globals, defaults, and closures are still hashed live, so
  mutated module state keeps invalidating as before.

### Changed
- A cache hit no longer rewrites the stored commit's status to `cached`;
  stored commits keep `completed` and `cashet log` shows them as such.
- The HTTP server returns 422 with the final exception line (no traceback,
  no server file paths) when a submitted task raises. 500 is reserved for
  actual server errors.
- CLI commands that operate on an existing store exit non-zero when the store
  directory does not exist instead of silently creating an empty one;
  `import` and `serve` still create it. A group-level `--store-dir` option
  overrides `$CASHET_DIR` and the `./.cashet` default.
- Redis tests target `redis://localhost:6379/15` by default and honor
  `CASHET_TEST_REDIS_URL`. The suite flushes the target database, which
  previously defaulted to a shared database 0.
- Internal modules reorganized; public import paths are unchanged.
  Serializers moved from `cashet.hashing` to `cashet.serializers` (the old
  imports still work via re-exports). `store.py` keeps the public SQLite
  classes and delegates to `_sqlite_core`, `_sqlite_schema`, and `_locks`;
  `redis_store.py` keeps the Redis classes and delegates its key scheme and
  codec to `_redis_codec`. The HTTP server's duplicated sync and async
  handlers were unified into one set behind an ops adapter.

### Notes
- Argument hashes change for sets containing custom objects or single-element
  tuples; affected entries recompute on first access.
- The Redis per-fingerprint index is rescored lazily. Entries written by
  older versions with a TTL can still order incorrectly until rewritten, and
  commits written before 0.5.0 are absent from the new expiry index, so they
  only age out by access time.
- Blob writes create short-lived `*.tmp` files inside `objects/`. A failed
  write removes its temp file, storage stats never count them, and garbage
  collection sweeps any left behind by a crash once they are an hour old.

## 0.4.5 - 21.6.2026.

### Fixed
- Hash objects by their `__slots__`/`__dict__` state instead of the memory
  address. Slotted value types (including `@dataclass(slots=True)`) cached by
  identity before, so identical calls missed the cache every run and lossy
  custom reprs could collide. Truly opaque objects now warn instead of silently
  address-hashing.
- Record `ResultRef` values nested inside custom-object attributes in commit
  lineage, matching what the hasher already accounts for, so garbage collection
  cannot evict a blob a commit still depends on.
- Hash referenced module-global `dict`/`list`/`set` values consistently with
  scalars, so config-style globals invalidate the cache when they change.
- Canonicalize function source with `ast.unparse` instead of `ast.dump`, which
  keeps the same comment/whitespace/docstring insensitivity but stays stable
  across Python versions whose `ast.dump` field set differs.
- Treat the `last_accessed_at` bump on a cache hit as best-effort so a locked
  database no longer turns a successful lookup into an error.
- Skip a post-eviction `VACUUM` that is blocked by a concurrent writer instead
  of failing an eviction whose deletes already committed.
- Prevent Redis tag index key collisions: presence and value indexes now use
  distinct prefixes and the value key length-prefixes the tag key, so a `:` in
  a key or value can no longer cross-match unrelated commits.
- Make the Redis blob delete script idempotent against a missing reference
  counter so it cannot drop a blob another commit still references.
- Enforce the HTTP server request size limit on the bytes actually received,
  closing a bypass for chunked requests that omit `Content-Length`. On
  token-protected servers, unauthenticated requests are now rejected before any
  body is buffered.
- Validate handler input (returning 400) and wrap every server handler in a
  generic 500 barrier so malformed input and internal errors no longer leak
  stack traces; `/gc` also accepts an empty body and falls back to defaults.
- Report commits skipped during a lossy import. A truncated archive no longer
  looks like a clean import.
- Exit non-zero from `cashet show` and `cashet get` when a commit is missing,
  matching `cashet rm`.
- Cancel in-flight sibling tasks when a parallel `submit_many` task fails,
  instead of leaving them running after the error is surfaced.

### Performance
- Redis `find_running_by_fingerprint` uses a per-fingerprint running-claim index
  for O(1) lookup instead of scanning a fingerprint's full commit history on
  every submit.

### Changed
- `import_archive` (sync and async) now returns an `ImportResult(imported,
  skipped)` named tuple instead of a bare imported count.
- Sync `submit_many` no longer recomputes the dependency graph that the async
  client already builds.

### Notes
- The hashing fixes change function and argument cache keys, so results cached
  by earlier versions recompute on first access. Blobs are content-addressed and
  remain until garbage collected.
- The Redis tag index key scheme changed. Tag indexes written by older versions
  are not migrated; rewrite affected commits to rebuild them.

## 0.4.4 — 11.5.2026.

### Fixed
- Await `async def` task callables submitted through `AsyncClient` instead of
  trying to cache the coroutine object.
- Include immutable referenced global values, including globals referenced only
  inside nested code objects, in function hashes so global constants invalidate
  cached results when changed.
- Resolve nested `ResultRef` / `AsyncResultRef` values inside containers and
  record deduplicated input refs in commit metadata.
- Preserve tuple subclasses while resolving refs and keep dict/frozenset
  resolution from creating unhashable container members.
- Preserve awaitable objects returned by task functions instead of awaiting
  returned values a second time.
- Include stable immutable built-in globals such as `range`, `slice`, and
  `datetime` values in function hashes.
- Raise a clear `cashet[redis]` install error when `RedisStore` or
  `AsyncRedisStore` is imported from a base install without the Redis extra.

## 0.4.3 — 1.5.2026.

### Fixed
- Add `freezegun` to dev dependencies so TTL/GC tests don't fail with
  `ModuleNotFoundError` on fresh installs or CI.
- Restore `expires_at <= now` race guard in Redis `find_by_fingerprint` after
  the `ZREVRANGEBYSCORE` pushdown — a commit can expire between the server-side
  filter and the `get_commit` call.

## 0.4.2 — 1.5.2026.

### Performance
- `delete_by_tags` in SQLite batches all matching rows into a single DELETE with one
  orphan-detection pass instead of row-by-row `_delete_commit_body` calls.
- `delete_by_tags` in Redis uses tag-set indexes (`cashet:tag:{key}`,
  `cashet:tag:{key}:{value}`) with SINTER instead of a full `zrevrange(all)` scan.
- `find_by_fingerprint` in Redis pushes TTL filtering server-side via
  `ZREVRANGEBYSCORE` using `expires_at` timestamp as the sorted-set score.

### Added
- `cashet invalidate -t key=value` / `-t key` CLI command.

### Fixed
- Deterministic TTL and GC tests using `freezegun` instead of `time.sleep`.

## 0.4.1 — 1.5.2026.

### Fixed
- Hash prefix lookups now normalize to lowercase. Previously, uppercase hex characters
  (A-F) passed as hash prefixes would fail to match because SHA-256 digests are always
  lowercase.
- `idx_last_accessed_at` index creation moved after the `last_accessed_at` column
  migration. Previously, opening a database created before the column existed crashed
  with "no such column".
- `find_by_fingerprint` pushes TTL expiration filtering into the SQL query instead of
  iterating all matching rows in Python. Expired entries no longer cause O(n) scanning.

### Added
- Migration tests covering base schema, idempotent re-open, post-migration operations,
  and partial migration states.
- Test verifying that an ambiguous hash prefix in `delete_commit` properly rolls back
  the transaction without poisoning subsequent writes.

## 0.4.0 — 30.4.2026.

### Added
- Per-entry TTL (`ttl` parameter). Results expire after the specified duration and are
  automatically re-executed on the next call.
- Tag-based invalidation (`client.invalidate({"key": "value"})`). Delete all commits
  matching one or more tag key-value pairs. Bare-key queries (`{"key": None}`) match
  any value.

### Fixed
- Hash prefix validation guards against SQL `LIKE` wildcards (`_`, `%`) and Redis glob
  patterns (`*`, `?`) passed as hash arguments.
- Stale `RUNNING` claim reclamation now propagates current task options (cache, retries,
  force, timeout, TTL, tags) instead of reusing the stale claim's original config.
- `delete_commit` now issues `ROLLBACK` on early-return paths (missing hash, ambiguous
  prefix) instead of leaving a dangling write transaction that poisoned the next writer.
- Schema forward-migrations added for `force`, `timeout_seconds`, `ttl_seconds`,
  `expires_at`, and `claimed_at` columns.
- Archive import verifies blob content hash to prevent corrupted or tampered archives
  from poisoning the store.

## 0.3.2 — 21.4.2026.

### Added
- Archive manifest for export/import integrity verification.

### Changed
- Generic `ResultRef[T]` for typed result references.
- `client.map()` for parallel execution over iterables.

## 0.3.1 — 17.4.2026.

### Fixed
- Redis `put_commit` ref-count race closed with `WATCH`/`MULTI`/`EXEC` transaction.

### Changed
- Core refactor: protocol-based dependency injection with pluggable `Store`, `AsyncStore`,
  `Executor`, and `Serializer` protocols.
- Store improvements and server feature additions.
- Async protocol formalized.

## 0.3.0 — 14.4.2026.

### Added
- Async client (`AsyncClient`) with native `asyncio` support.
- Redis backend (`RedisStore`, `AsyncRedisStore`) for distributed cache sharing.
- HTTP server (`client.serve()`) exposing cache operations over REST. Optional bearer
  token authentication.

## 0.2.0 — 7.4.2026.

### Added
- Force rerun (`force=True`) to bypass cache and re-execute.
- Task timeouts with configurable per-task and executor-level defaults.
- Parallel batch execution (`submit_many` with `max_workers`).
- Inline storage tier for small blobs (<1KB) stored directly in SQLite.
- Size-based garbage collection (`gc(max_size_bytes=N)`).

## 0.1.3 — 3.4.2026.

### Added
- Cross-process claim deduplication via file locks.
- Heartbeat leases to prevent stale `RUNNING` claims from blocking re-execution.

### Fixed
- Thread safety and correctness fixes for concurrent access patterns.

## 0.1.2 — 1.4.2026.

### Added
- Batch DAG execution with dependency resolution (`submit_many`).
- Jupyter notebook support for interactive caching.
- Thread safety across multiple clients sharing one store.
- Cache observability via `client.log()`, `client.show()`, `client.history()`.

### Changed
- Progressive function hashing includes closure functions and module dependencies.
- CLI polish and richer output formatting.

## 0.1.1 — 29.3.2026.

### Added
- Initial public release with content-addressable compute caching.
- SQLite backend with blob deduplication and zlib compression.
- Function source + args hashing for cache key derivation.
- `ResultRef` pass-through for DAG chaining.
