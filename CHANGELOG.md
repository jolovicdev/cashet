# Changelog

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
