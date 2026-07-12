from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime
from typing import Any

from redis.exceptions import WatchError

from cashet._ids import normalize_hash_prefix
from cashet._redis_codec import (
    DECR_DELETE_SCRIPT,
    access_key,
    blob_hashes,
    blob_key,
    blob_ref_key,
    blob_stats_key,
    blob_stats_lock_key,
    commit_access_timestamp,
    commit_hash_from_key,
    commit_key,
    decode_commit,
    decode_hash,
    expires_key,
    fp_key,
    func_key,
    index_commit_commands,
    matches_tags,
    remove_commit_index_commands,
    running_key,
    stats_dict,
    stats_ready,
    status_key,
    tag_key,
    tag_value_key,
)
from cashet._runner import BlockingAsyncRunner
from cashet.models import Commit, ObjectRef, StorageTier, TaskStatus

logger = logging.getLogger("cashet")


class AsyncRedisStore:
    def __init__(
        self, redis_url: str = "redis://localhost:6379/0", lock_timeout: int = 30
    ) -> None:
        import redis.asyncio as aioredis

        self._redis: Any = aioredis.from_url(redis_url)
        self._lock_timeout = lock_timeout

    def _fingerprint_lock(self, fingerprint: str) -> Any:
        # A fresh Lock per acquisition: redis-py Lock objects carry per-holder
        # token state, so caching one per fingerprint leaked memory and risked
        # token confusion when two coroutines reused the same instance.
        return self._redis.lock(
            f"cashet:lock:{fingerprint}",
            timeout=self._lock_timeout,
            blocking_timeout=10,
        )

    async def put_blob(self, data: bytes) -> ObjectRef:
        content_hash = hashlib.sha256(data).hexdigest()
        key = blob_key(content_hash)
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
        data = await self._redis.get(blob_key(ref.hash))
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
        ck = commit_key(commit.hash)
        new_hashes = blob_hashes(commit)
        while True:
            async with self._redis.pipeline(transaction=True) as pipe:
                await pipe.watch(ck)
                existing_raw = await pipe.get(ck)
                pipe.multi()
                index_commit_commands(pipe, commit)
                if existing_raw is None:
                    for h in new_hashes:
                        pipe.incr(blob_ref_key(h))
                else:
                    old_hashes = blob_hashes(decode_commit(existing_raw))
                    for h in new_hashes - old_hashes:
                        pipe.incr(blob_ref_key(h))
                try:
                    await pipe.execute()
                    return
                except WatchError:
                    continue

    async def get_commit(self, hash: str) -> Commit | None:
        normalized = normalize_hash_prefix(hash)
        if normalized is None:
            return None
        hash = normalized
        if len(hash) < 64:
            matches: list[str] = []
            async for key in self._redis.scan_iter(match=commit_key(hash) + "*", count=100):
                matches.append(key)
            if not matches:
                return None
            if len(matches) > 1:
                hashes: list[str] = []
                for m in matches:
                    hashes.append(commit_hash_from_key(m))
                matches_str = ", ".join(h[:12] for h in hashes)
                raise ValueError(
                    f"Ambiguous prefix {hash[:12]} matches {len(matches)} commits: {matches_str}"
                )
            data = await self._redis.get(matches[0])
        else:
            data = await self._redis.get(commit_key(hash))
        if data is None:
            return None
        return decode_commit(data)

    async def find_by_fingerprint(self, fingerprint: str) -> Commit | None:
        # Scored by created_at, so descending order is recency order and the
        # newest completed commit wins, matching SQLiteStore.
        entries = await self._redis.zrevrange(fp_key(fingerprint), 0, -1, withscores=True)
        if any(score == float("inf") for _, score in entries):
            await self._rescore_legacy_fingerprint_index(fingerprint, entries)
            entries = await self._redis.zrevrange(
                fp_key(fingerprint), 0, -1, withscores=True
            )
        now = datetime.now(UTC)
        for h, _score in entries:
            h_str = decode_hash(h)
            commit = await self.get_commit(h_str)
            if commit is None:
                continue
            if commit.status not in (TaskStatus.COMPLETED, TaskStatus.CACHED):
                continue
            if commit.expires_at is not None and commit.expires_at <= now:
                continue
            await self._touch_commit(h_str)
            return commit
        return None

    async def _rescore_legacy_fingerprint_index(
        self, fingerprint: str, entries: list[tuple[Any, float]]
    ) -> None:
        # Indexes written before 0.5.0 scored this zset by expiry (infinity for
        # commits without a TTL), which would shadow every newer commit; rescore
        # by created_at so recency ordering holds for pre-upgrade entries.
        rescored: dict[str, float] = {}
        for h, _score in entries:
            h_str = decode_hash(h)
            data = await self._redis.get(commit_key(h_str))
            if data is None:
                continue
            rescored[h_str] = decode_commit(data).created_at.timestamp()
        if rescored:
            await self._redis.zadd(fp_key(fingerprint), rescored)

    async def find_running_by_fingerprint(self, fingerprint: str) -> Commit | None:
        hashes = await self._redis.smembers(running_key(fingerprint))
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
            hashes = await self._redis.zrevrange(func_key(func_name), 0, -1)
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
            if tags is not None and not matches_tags(commit, tags):
                continue
            commits.append(commit)
            if len(commits) >= limit:
                break
        return commits

    async def get_history(self, hash: str) -> list[Commit]:
        normalized = normalize_hash_prefix(hash)
        if normalized is None:
            return []
        hash = normalized
        commit = await self.get_commit(hash)
        if commit is None:
            return []
        fingerprint = commit.fingerprint
        hashes = await self._redis.zrange(fp_key(fingerprint), 0, -1)
        now = datetime.now(UTC)
        commits: list[Commit] = []
        for h in hashes:
            h_str = h.decode() if isinstance(h, bytes) else h
            c = await self.get_commit(h_str)
            if c is not None and c.status in (TaskStatus.COMPLETED, TaskStatus.CACHED):
                if c.expires_at is not None and c.expires_at <= now:
                    continue
                commits.append(c)
        return commits

    async def stats(self) -> dict[str, int]:
        total = await self._redis.zcard("cashet:index:all")
        completed = 0
        for status in ("completed", "cached"):
            completed += await self._redis.scard(status_key(status))
        blob_count, blob_bytes = await self._blob_storage_totals()
        return stats_dict(total, completed, blob_count, blob_bytes)

    def _blob_stats_lock(self) -> Any:
        return self._redis.lock(
            blob_stats_lock_key(),
            timeout=self._lock_timeout,
            blocking_timeout=10,
        )

    async def _blob_stats_ready(self) -> bool:
        return stats_ready(await self._redis.hget(blob_stats_key(), "ready"))

    async def _incr_blob_stats(self, objects_delta: int, bytes_delta: int) -> None:
        pipe = self._redis.pipeline()
        pipe.hincrby(blob_stats_key(), "objects", objects_delta)
        pipe.hincrby(blob_stats_key(), "bytes", bytes_delta)
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
                blob_stats_key(),
                mapping={"objects": blob_count, "bytes": blob_bytes, "ready": 1},
            )

    async def _blob_storage_totals(self) -> tuple[int, int]:
        await self._ensure_blob_stats()
        blob_count, blob_bytes = await self._redis.hmget(
            blob_stats_key(), "objects", "bytes"
        )
        return int(blob_count or 0), int(blob_bytes or 0)

    async def _blob_size(self, blob_hash: str) -> int:
        return await self._redis.strlen(blob_key(blob_hash))

    async def _bytes_freed_by_delete(self, commit: Commit) -> int:
        freed = 0
        for h in blob_hashes(commit):
            ref_count = int(await self._redis.get(blob_ref_key(h)) or 0)
            if ref_count <= 1:
                freed += await self._blob_size(h)
        return freed

    async def _backfill_access_index(self) -> None:
        all_hashes = await self._redis.zrange("cashet:index:all", 0, -1)
        for h in all_hashes:
            h_str = decode_hash(h)
            score = await self._redis.zscore(access_key(), h_str)
            if score is None:
                data = await self._redis.get(commit_key(h_str))
                if data is None:
                    continue
                await self._redis.zadd(access_key(), {h_str: commit_access_timestamp(data)})

    async def evict(self, older_than: datetime, max_size_bytes: int | None = None) -> int:
        deleted = 0
        total = await self._redis.zcard("cashet:index:all")
        indexed = await self._redis.zcard(access_key())
        if total > 0 and indexed < total:
            await self._backfill_access_index()
        cutoff_ts = older_than.timestamp()
        old_hashes_raw = await self._redis.zrangebyscore(access_key(), "-inf", cutoff_ts)
        old_hashes = [decode_hash(h) for h in old_hashes_raw]
        for h_str in old_hashes:
            commit = await self.get_commit(h_str)
            if commit is None:
                await self._redis.zrem(access_key(), h_str)
                continue
            if await self.delete_commit(h_str):
                deleted += 1
        now_ts = datetime.now(UTC).timestamp()
        expired_raw = await self._redis.zrangebyscore(expires_key(), "-inf", now_ts)
        for h in expired_raw:
            h_str = decode_hash(h)
            if await self.delete_commit(h_str):
                deleted += 1
            else:
                await self._redis.zrem(expires_key(), h_str)
        if max_size_bytes is not None:
            current_bytes = (await self._blob_storage_totals())[1]
            while current_bytes > max_size_bytes:
                candidates = await self._redis.zrange(access_key(), 0, 0)
                if not candidates:
                    break
                oldest_hash = decode_hash(candidates[0])
                commit = await self.get_commit(oldest_hash)
                if commit is None:
                    await self._redis.zrem(access_key(), oldest_hash)
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
        normalized = normalize_hash_prefix(hash)
        if normalized is None:
            return False
        hash = normalized
        if await self._blob_stats_ready():
            return await self._delete_commit(hash)
        async with self._blob_stats_lock():
            return await self._delete_commit(hash)

    async def delete_by_tags(self, tags: dict[str, str | None]) -> int:
        set_keys: list[str] = []
        for key, val in tags.items():
            set_keys.append(tag_key(key) if val is None else tag_value_key(key, val))
        if len(set_keys) == 1:
            hashes = await self._redis.smembers(set_keys[0])
        else:
            hashes = await self._redis.sinter(set_keys)
        deleted = 0
        for h in hashes:
            h_str = h.decode() if isinstance(h, bytes) else h
            if await self._delete_commit_obj_by_hash(h_str):
                deleted += 1
        return deleted

    async def _delete_commit(self, hash: str) -> bool:
        commit = await self.get_commit(hash)
        if commit is None:
            return False
        return await self._delete_commit_obj(commit)

    async def _delete_commit_obj_by_hash(self, hash: str) -> bool:
        commit = await self.get_commit(hash)
        if commit is None:
            return False
        return await self._delete_commit_obj(commit)

    async def _delete_commit_obj(self, commit: Commit) -> bool:
        resolved_hash = commit.hash
        pipe = self._redis.pipeline()
        remove_commit_index_commands(pipe, commit, resolved_hash)
        await pipe.execute()
        for h in blob_hashes(commit):
            deleted = await self._redis.eval(
                DECR_DELETE_SCRIPT,
                3,
                blob_ref_key(h),
                blob_key(h),
                blob_stats_key(),
            )
            if deleted:
                logger.debug("orphan blob cleaned hash=%s", h[:12])
        logger.debug("commit deleted hash=%s", resolved_hash[:12])
        return True

    async def _touch_commit(self, hash: str) -> None:
        now = datetime.now(UTC).timestamp()
        await self._redis.zadd(access_key(), {hash: now})

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

    def delete_by_tags(self, tags: dict[str, str | None]) -> int:
        return self._runner.call(self._async_store.delete_by_tags(tags))

    def close(self) -> None:
        self._runner.call(self._async_store.close())
        self._runner.close()

    def _flushdb(self) -> None:
        redis_client = self._async_store._redis  # pyright: ignore[reportPrivateUsage]
        self._runner.call(redis_client.flushdb())
