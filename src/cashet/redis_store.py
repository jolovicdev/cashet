from __future__ import annotations

import base64
import hashlib
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from redis.exceptions import WatchError

from cashet._runner import BlockingAsyncRunner
from cashet.models import Commit, ObjectRef, StorageTier, TaskDef, TaskStatus

logger = logging.getLogger("cashet")

_DECR_DELETE_SCRIPT = """
    local ref_key = KEYS[1]
    local blob_key = KEYS[2]
    local stats_key = KEYS[3]
    local count = redis.call('DECR', ref_key)
    if count <= 0 then
        local existed = redis.call('EXISTS', blob_key)
        local bytes = 0
        if existed == 1 then
            bytes = redis.call('STRLEN', blob_key)
        end
        redis.call('DEL', blob_key, ref_key)
        if existed == 1 and redis.call('HGET', stats_key, 'ready') == '1' then
            redis.call('HINCRBY', stats_key, 'objects', -1)
            redis.call('HINCRBY', stats_key, 'bytes', -bytes)
        end
        return bytes
    end
    return 0
"""


def _commit_key(hash: str) -> str:
    return f"cashet:commit:{hash}"


def _blob_key(hash: str) -> str:
    return f"cashet:blob:data:{hash}"


def _fp_key(fingerprint: str) -> str:
    return f"cashet:index:fingerprint:{fingerprint}"


def _func_key(func_name: str) -> str:
    return f"cashet:index:func:{func_name}"


def _status_key(status: str) -> str:
    return f"cashet:index:status:{status}"


def _access_key() -> str:
    return "cashet:index:last_accessed"


def _blob_stats_key() -> str:
    return "cashet:stats:blob"


def _blob_stats_lock_key() -> str:
    return "cashet:stats:blob:lock"


def _stats_ready(raw: Any) -> bool:
    if isinstance(raw, bytes):
        return raw == b"1"
    return raw == "1" or raw == 1


def _encode_commit(commit: Commit) -> bytes:
    d: dict[str, Any] = {
        "hash": commit.hash,
        "fingerprint": commit.fingerprint,
        "func_name": commit.task_def.func_name,
        "func_hash": commit.task_def.func_hash,
        "args_hash": commit.task_def.args_hash,
        "args_snapshot_b64": base64.b64encode(commit.task_def.args_snapshot).decode(),
        "func_source": commit.task_def.func_source,
        "dep_versions": commit.task_def.dep_versions,
        "cache": commit.task_def.cache,
        "retries": commit.task_def.retries,
        "force": commit.task_def.force,
        "timeout_seconds": (
            commit.task_def.timeout.total_seconds() if commit.task_def.timeout else None
        ),
        "input_refs": [
            {"hash": r.hash, "size": r.size, "tier": r.tier.value} for r in commit.input_refs
        ],
        "output_hash": commit.output_ref.hash if commit.output_ref else None,
        "output_size": commit.output_ref.size if commit.output_ref else None,
        "output_tier": commit.output_ref.tier.value if commit.output_ref else None,
        "parent_hash": commit.parent_hash,
        "status": commit.status.value,
        "error": commit.error,
        "tags": commit.tags,
        "created_at": commit.created_at.isoformat(),
        "claimed_at": commit.claimed_at.isoformat(),
        "last_accessed_at": datetime.now(UTC).isoformat(),
    }
    return json.dumps(d, separators=(",", ":")).encode()


def _decode_commit(data: bytes) -> Commit:
    d = json.loads(data)
    task_def = TaskDef(
        func_hash=d["func_hash"],
        func_name=d["func_name"],
        func_source=d.get("func_source", ""),
        args_hash=d["args_hash"],
        args_snapshot=base64.b64decode(d.get("args_snapshot_b64", "")),
        dep_versions=d.get("dep_versions", {}),
        cache=d.get("cache", True),
        tags=d.get("tags", {}),
        retries=d.get("retries", 0),
        force=d.get("force", False),
        timeout=(
            timedelta(seconds=d["timeout_seconds"])
            if d.get("timeout_seconds") is not None
            else None
        ),
    )
    input_refs = [
        ObjectRef(
            hash=r["hash"],
            size=r.get("size", 0),
            tier=StorageTier(r.get("tier", "blob")),
        )
        for r in d.get("input_refs", [])
    ]
    output_ref = None
    if d.get("output_hash"):
        output_ref = ObjectRef(
            hash=d["output_hash"],
            size=d.get("output_size", 0),
            tier=StorageTier(d.get("output_tier", "blob")),
        )
    created_at = (
        datetime.fromisoformat(d["created_at"]) if "created_at" in d else datetime.now(UTC)
    )
    claimed_at = (
        datetime.fromisoformat(d["claimed_at"]) if "claimed_at" in d else datetime.now(UTC)
    )
    return Commit(
        hash=d["hash"],
        task_def=task_def,
        input_refs=input_refs,
        output_ref=output_ref,
        parent_hash=d.get("parent_hash"),
        status=TaskStatus(d["status"]),
        created_at=created_at,
        claimed_at=claimed_at,
        error=d.get("error"),
        tags=d.get("tags", {}),
    )


def _decode_hash(raw: Any) -> str:
    return raw.decode() if isinstance(raw, bytes) else raw


def _commit_access_timestamp(data: bytes) -> float:
    d = json.loads(data)
    value = d.get("last_accessed_at") or d.get("created_at")
    if value is None:
        return datetime.now(UTC).timestamp()
    return datetime.fromisoformat(value).timestamp()


def _blob_ref_key(blob_hash: str) -> str:
    return f"cashet:blob:ref:{blob_hash}"


def _blob_hashes(commit: Commit) -> set[str]:
    hashes: set[str] = set()
    if commit.output_ref:
        hashes.add(commit.output_ref.hash)
    for ref in commit.input_refs:
        hashes.add(ref.hash)
    return hashes


def _matches_tags(commit: Commit, tags: dict[str, str | None]) -> bool:
    for key, val in tags.items():
        if val is None:
            if key not in commit.tags:
                return False
        else:
            if commit.tags.get(key) != val:
                return False
    return True


def _stats_dict(total: int, completed: int, blob_count: int, blob_bytes: int) -> dict[str, int]:
    return {
        "total_commits": total,
        "completed_commits": completed,
        "stored_objects": blob_count,
        "disk_bytes": blob_bytes,
        "blob_objects": blob_count,
        "blob_bytes": blob_bytes,
        "inline_objects": 0,
        "inline_bytes": 0,
    }


def _index_commit_commands(pipe: Any, commit: Commit) -> None:
    pipe.get(_commit_key(commit.hash))
    pipe.set(_commit_key(commit.hash), _encode_commit(commit))
    ts = commit.created_at.timestamp()
    pipe.zadd("cashet:index:all", {commit.hash: ts})
    pipe.zadd(_fp_key(commit.fingerprint), {commit.hash: ts})
    pipe.zadd(_func_key(commit.task_def.func_name), {commit.hash: ts})
    now_ts = datetime.now(UTC).timestamp()
    pipe.zadd(_access_key(), {commit.hash: now_ts})
    for status in TaskStatus:
        pipe.srem(_status_key(status.value), commit.hash)
    pipe.sadd(_status_key(commit.status.value), commit.hash)


def _remove_commit_index_commands(pipe: Any, commit: Commit, resolved_hash: str) -> None:
    pipe.delete(_commit_key(resolved_hash))
    pipe.zrem("cashet:index:all", resolved_hash)
    pipe.zrem(_fp_key(commit.fingerprint), resolved_hash)
    pipe.zrem(_func_key(commit.task_def.func_name), resolved_hash)
    pipe.zrem(_access_key(), resolved_hash)
    for status in TaskStatus:
        pipe.srem(_status_key(status.value), resolved_hash)


def _commit_hash_from_key(raw: Any) -> str:
    key_str = raw.decode() if isinstance(raw, bytes) else str(raw)
    return key_str.split(":")[-1]


class AsyncRedisStore:
    def __init__(
        self, redis_url: str = "redis://localhost:6379/0", lock_timeout: int = 30
    ) -> None:
        import redis.asyncio as aioredis

        self._redis: Any = aioredis.from_url(redis_url)
        self._lock_timeout = lock_timeout
        self._async_locks: dict[str, Any] = {}

    def _fingerprint_lock(self, fingerprint: str) -> Any:
        lock = self._async_locks.get(fingerprint)
        if lock is None:
            lock = self._redis.lock(
                f"cashet:lock:{fingerprint}",
                timeout=self._lock_timeout,
                blocking_timeout=10,
            )
            self._async_locks[fingerprint] = lock
        return lock

    async def put_blob(self, data: bytes) -> ObjectRef:
        content_hash = hashlib.sha256(data).hexdigest()
        key = _blob_key(content_hash)
        if await self._blob_stats_ready():
            stored = await self._redis.set(key, data, nx=True)
            if stored:
                await self._incr_blob_stats(1, len(data))
        else:
            async with self._blob_stats_lock():
                ready = await self._blob_stats_ready()
                stored = await self._redis.set(key, data, nx=True)
                if stored and ready:
                    await self._incr_blob_stats(1, len(data))
        if stored:
            logger.info(
                "blob stored hash=%s size=%d tier=blob",
                content_hash[:12],
                len(data),
            )
        else:
            logger.debug(
                "blob deduplicated hash=%s size=%d",
                content_hash[:12],
                len(data),
            )
        return ObjectRef(hash=content_hash, size=len(data), tier=StorageTier.BLOB)

    async def get_blob(self, ref: ObjectRef) -> bytes:
        data = await self._redis.get(_blob_key(ref.hash))
        if data is None:
            logger.warning("blob not found hash=%s", ref.hash[:12])
            raise ValueError(f"Blob {ref.hash} not found")
        if isinstance(data, str):
            data = data.encode()
        if hashlib.sha256(data).hexdigest() != ref.hash:
            logger.error("blob integrity check failed hash=%s", ref.hash[:12])
            raise ValueError(f"Blob {ref.hash} integrity check failed")
        return data

    async def put_commit(self, commit: Commit) -> None:
        ck = _commit_key(commit.hash)
        new_hashes = _blob_hashes(commit)
        while True:
            async with self._redis.pipeline(transaction=True) as pipe:
                await pipe.watch(ck)
                existing_raw = await pipe.get(ck)
                pipe.multi()
                _index_commit_commands(pipe, commit)
                if existing_raw is None:
                    for h in new_hashes:
                        pipe.incr(_blob_ref_key(h))
                else:
                    old_hashes = _blob_hashes(_decode_commit(existing_raw))
                    for h in new_hashes - old_hashes:
                        pipe.incr(_blob_ref_key(h))
                try:
                    await pipe.execute()
                    return
                except WatchError:
                    continue

    async def get_commit(self, hash: str) -> Commit | None:
        if not hash:
            return None
        if len(hash) < 64:
            matches: list[str] = []
            async for key in self._redis.scan_iter(match=_commit_key(hash) + "*", count=100):
                matches.append(key)
            if not matches:
                return None
            if len(matches) > 1:
                hashes: list[str] = []
                for m in matches:
                    hashes.append(_commit_hash_from_key(m))
                matches_str = ", ".join(h[:12] for h in hashes)
                raise ValueError(
                    f"Ambiguous prefix {hash[:12]} matches {len(matches)} commits: {matches_str}"
                )
            data = await self._redis.get(matches[0])
        else:
            data = await self._redis.get(_commit_key(hash))
        if data is None:
            return None
        return _decode_commit(data)

    async def find_by_fingerprint(self, fingerprint: str) -> Commit | None:
        hashes = await self._redis.zrevrange(_fp_key(fingerprint), 0, -1)
        for h in hashes:
            h_str = h.decode() if isinstance(h, bytes) else h
            commit = await self.get_commit(h_str)
            if commit is not None and commit.status in (TaskStatus.COMPLETED, TaskStatus.CACHED):
                await self._touch_commit(h_str)
                return commit
        return None

    async def find_running_by_fingerprint(self, fingerprint: str) -> Commit | None:
        hashes = await self._redis.zrevrange(_fp_key(fingerprint), 0, -1)
        for h in hashes:
            h_str = h.decode() if isinstance(h, bytes) else h
            commit = await self.get_commit(h_str)
            if commit is not None and commit.status == TaskStatus.RUNNING:
                return commit
        return None

    async def list_commits(
        self,
        func_name: str | None = None,
        limit: int = 50,
        status: TaskStatus | None = None,
        tags: dict[str, str | None] | None = None,
    ) -> list[Commit]:
        if func_name:
            hashes = await self._redis.zrevrange(_func_key(func_name), 0, -1)
        else:
            hashes = await self._redis.zrevrange("cashet:index:all", 0, -1)
        commits: list[Commit] = []
        for h in hashes:
            h_str = h.decode() if isinstance(h, bytes) else h
            commit = await self.get_commit(h_str)
            if commit is None:
                continue
            if status is not None and commit.status != status:
                continue
            if tags is not None and not _matches_tags(commit, tags):
                continue
            commits.append(commit)
            if len(commits) >= limit:
                break
        return commits

    async def get_history(self, hash: str) -> list[Commit]:
        if not hash:
            return []
        commit = await self.get_commit(hash)
        if commit is None:
            return []
        fingerprint = commit.fingerprint
        hashes = await self._redis.zrange(_fp_key(fingerprint), 0, -1)
        commits: list[Commit] = []
        for h in hashes:
            h_str = h.decode() if isinstance(h, bytes) else h
            c = await self.get_commit(h_str)
            if c is not None and c.status in (TaskStatus.COMPLETED, TaskStatus.CACHED):
                commits.append(c)
        return commits

    async def stats(self) -> dict[str, int]:
        total = await self._redis.zcard("cashet:index:all")
        completed = 0
        for status in ("completed", "cached"):
            completed += await self._redis.scard(_status_key(status))
        blob_count, blob_bytes = await self._blob_storage_totals()
        return _stats_dict(total, completed, blob_count, blob_bytes)

    def _blob_stats_lock(self) -> Any:
        return self._redis.lock(
            _blob_stats_lock_key(),
            timeout=self._lock_timeout,
            blocking_timeout=10,
        )

    async def _blob_stats_ready(self) -> bool:
        return _stats_ready(await self._redis.hget(_blob_stats_key(), "ready"))

    async def _incr_blob_stats(self, objects_delta: int, bytes_delta: int) -> None:
        pipe = self._redis.pipeline()
        pipe.hincrby(_blob_stats_key(), "objects", objects_delta)
        pipe.hincrby(_blob_stats_key(), "bytes", bytes_delta)
        await pipe.execute()

    async def _scan_blob_storage_totals(self) -> tuple[int, int]:
        blob_count = 0
        blob_bytes = 0
        async for key in self._redis.scan_iter(match="cashet:blob:data:*"):
            blob_count += 1
            blob_bytes += await self._redis.strlen(key)
        return blob_count, blob_bytes

    async def _ensure_blob_stats(self) -> None:
        if await self._blob_stats_ready():
            return
        async with self._blob_stats_lock():
            if await self._blob_stats_ready():
                return
            blob_count, blob_bytes = await self._scan_blob_storage_totals()
            await self._redis.hset(
                _blob_stats_key(),
                mapping={"objects": blob_count, "bytes": blob_bytes, "ready": 1},
            )

    async def _blob_storage_totals(self) -> tuple[int, int]:
        await self._ensure_blob_stats()
        blob_count, blob_bytes = await self._redis.hmget(
            _blob_stats_key(), "objects", "bytes"
        )
        return int(blob_count or 0), int(blob_bytes or 0)

    async def _blob_size(self, blob_hash: str) -> int:
        return await self._redis.strlen(_blob_key(blob_hash))

    async def _bytes_freed_by_delete(self, commit: Commit) -> int:
        freed = 0
        for h in _blob_hashes(commit):
            ref_count = int(await self._redis.get(_blob_ref_key(h)) or 0)
            if ref_count <= 1:
                freed += await self._blob_size(h)
        return freed

    async def _backfill_access_index(self) -> None:
        all_hashes = await self._redis.zrange("cashet:index:all", 0, -1)
        for h in all_hashes:
            h_str = _decode_hash(h)
            score = await self._redis.zscore(_access_key(), h_str)
            if score is None:
                data = await self._redis.get(_commit_key(h_str))
                if data is None:
                    continue
                await self._redis.zadd(_access_key(), {h_str: _commit_access_timestamp(data)})

    async def evict(self, older_than: datetime, max_size_bytes: int | None = None) -> int:
        deleted = 0
        total = await self._redis.zcard("cashet:index:all")
        indexed = await self._redis.zcard(_access_key())
        if total > 0 and indexed < total:
            await self._backfill_access_index()
        cutoff_ts = older_than.timestamp()
        old_hashes_raw = await self._redis.zrangebyscore(_access_key(), "-inf", cutoff_ts)
        old_hashes = [_decode_hash(h) for h in old_hashes_raw]
        for h_str in old_hashes:
            commit = await self.get_commit(h_str)
            if commit is None:
                await self._redis.zrem(_access_key(), h_str)
                continue
            if await self.delete_commit(h_str):
                deleted += 1
        if max_size_bytes is not None:
            current_bytes = (await self._blob_storage_totals())[1]
            while current_bytes > max_size_bytes:
                candidates = await self._redis.zrange(_access_key(), 0, 0)
                if not candidates:
                    break
                oldest_hash = _decode_hash(candidates[0])
                commit = await self.get_commit(oldest_hash)
                if commit is None:
                    await self._redis.zrem(_access_key(), oldest_hash)
                    continue
                freed = await self._bytes_freed_by_delete(commit)
                if await self.delete_commit(oldest_hash):
                    current_bytes -= freed
                    deleted += 1
                else:
                    break
        if deleted:
            logger.info(
                "eviction complete deleted=%d reason=%s",
                deleted,
                "size_limit" if max_size_bytes is not None else "ttl",
            )
        else:
            logger.debug("eviction found no candidates")
        return deleted

    async def delete_commit(self, hash: str) -> bool:
        if not hash:
            return False
        if await self._blob_stats_ready():
            return await self._delete_commit(hash)
        async with self._blob_stats_lock():
            return await self._delete_commit(hash)

    async def _delete_commit(self, hash: str) -> bool:
        commit = await self.get_commit(hash)
        if commit is None:
            return False
        resolved_hash = commit.hash
        pipe = self._redis.pipeline()
        _remove_commit_index_commands(pipe, commit, resolved_hash)
        await pipe.execute()
        for h in _blob_hashes(commit):
            deleted = await self._redis.eval(
                _DECR_DELETE_SCRIPT,
                3,
                _blob_ref_key(h),
                _blob_key(h),
                _blob_stats_key(),
            )
            if deleted:
                logger.debug("orphan blob cleaned hash=%s", h[:12])
        logger.debug("commit deleted hash=%s", resolved_hash[:12])
        return True

    async def _touch_commit(self, hash: str) -> None:
        now = datetime.now(UTC).timestamp()
        await self._redis.zadd(_access_key(), {hash: now})

    async def close(self) -> None:
        logger.debug("closing async redis store")
        await self._redis.aclose()


class RedisStore:
    def __init__(
        self, redis_url: str = "redis://localhost:6379/0", lock_timeout: int = 30
    ) -> None:
        self._async_store = AsyncRedisStore(redis_url, lock_timeout)
        self._runner = BlockingAsyncRunner()

    @classmethod
    def from_async(
        cls, async_store: AsyncRedisStore, *, runner: BlockingAsyncRunner | None = None
    ) -> RedisStore:
        instance = cls.__new__(cls)
        instance._async_store = async_store
        instance._runner = runner or BlockingAsyncRunner()
        return instance

    def put_blob(self, data: bytes) -> ObjectRef:
        return self._runner.call(self._async_store.put_blob(data))

    def get_blob(self, ref: ObjectRef) -> bytes:
        return self._runner.call(self._async_store.get_blob(ref))

    def put_commit(self, commit: Commit) -> None:
        self._runner.call(self._async_store.put_commit(commit))

    def get_commit(self, hash: str) -> Commit | None:
        return self._runner.call(self._async_store.get_commit(hash))

    def find_by_fingerprint(self, fingerprint: str) -> Commit | None:
        return self._runner.call(self._async_store.find_by_fingerprint(fingerprint))

    def find_running_by_fingerprint(self, fingerprint: str) -> Commit | None:
        return self._runner.call(self._async_store.find_running_by_fingerprint(fingerprint))

    def list_commits(
        self,
        func_name: str | None = None,
        limit: int = 50,
        status: TaskStatus | None = None,
        tags: dict[str, str | None] | None = None,
    ) -> list[Commit]:
        return self._runner.call(
            self._async_store.list_commits(
                func_name=func_name, limit=limit, status=status, tags=tags
            )
        )

    def get_history(self, hash: str) -> list[Commit]:
        return self._runner.call(self._async_store.get_history(hash))

    def stats(self) -> dict[str, int]:
        return self._runner.call(self._async_store.stats())

    def evict(self, older_than: datetime, max_size_bytes: int | None = None) -> int:
        return self._runner.call(self._async_store.evict(older_than, max_size_bytes))

    def delete_commit(self, hash: str) -> bool:
        return self._runner.call(self._async_store.delete_commit(hash))

    def close(self) -> None:
        self._runner.call(self._async_store.close())
        self._runner.close()

    def _flushdb(self) -> None:
        redis_client = self._async_store._redis  # pyright: ignore[reportPrivateUsage]
        self._runner.call(redis_client.flushdb())
