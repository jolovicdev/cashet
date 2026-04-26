from __future__ import annotations

import base64
import hashlib
import json
import threading
from datetime import UTC, datetime, timedelta
from typing import Any

import redis

from cashet.models import Commit, ObjectRef, StorageTier, TaskDef, TaskStatus


def _commit_key(hash: str) -> str:
    return f"cashet:commit:{hash}"


def _blob_key(hash: str) -> str:
    return f"cashet:blob:{hash}"


def _fp_key(fingerprint: str) -> str:
    return f"cashet:index:fingerprint:{fingerprint}"


def _func_key(func_name: str) -> str:
    return f"cashet:index:func:{func_name}"


def _status_key(status: str) -> str:
    return f"cashet:index:status:{status}"


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


class RedisStore:
    def __init__(
        self, redis_url: str = "redis://localhost:6379/0", lock_timeout: int = 30
    ) -> None:
        self._redis: Any = redis.from_url(redis_url)
        self._lock_timeout = lock_timeout
        self._locks: dict[str, Any] = {}
        self._local_lock = threading.Lock()

    def _fingerprint_lock(self, fingerprint: str) -> Any:
        lock = self._locks.get(fingerprint)
        if lock is None:
            lock = self._redis.lock(
                f"cashet:lock:{fingerprint}",
                timeout=self._lock_timeout,
                blocking_timeout=10,
            )
            self._locks[fingerprint] = lock
        return lock

    def put_blob(self, data: bytes) -> ObjectRef:
        content_hash = hashlib.sha256(data).hexdigest()
        key = _blob_key(content_hash)
        if not self._redis.exists(key):
            self._redis.set(key, data)
        return ObjectRef(hash=content_hash, size=len(data), tier=StorageTier.BLOB)

    def get_blob(self, ref: ObjectRef) -> bytes:
        data = self._redis.get(_blob_key(ref.hash))
        if data is None:
            raise ValueError(f"Blob {ref.hash} not found")
        if isinstance(data, str):
            data = data.encode()
        if hashlib.sha256(data).hexdigest() != ref.hash:
            raise ValueError(f"Blob {ref.hash} integrity check failed")
        return data

    def put_commit(self, commit: Commit) -> None:
        with self._local_lock:
            pipe = self._redis.pipeline()
            existing = self._redis.get(_commit_key(commit.hash))
            pipe.set(_commit_key(commit.hash), _encode_commit(commit))
            ts = commit.created_at.timestamp()
            pipe.zadd("cashet:index:all", {commit.hash: ts})
            pipe.zadd(_fp_key(commit.fingerprint), {commit.hash: ts})
            pipe.zadd(_func_key(commit.task_def.func_name), {commit.hash: ts})
            for status in TaskStatus:
                pipe.srem(_status_key(status.value), commit.hash)
            pipe.sadd(_status_key(commit.status.value), commit.hash)
            new_hashes = _blob_hashes(commit)
            if existing is None:
                for h in new_hashes:
                    pipe.incr(_blob_ref_key(h))
            else:
                old_hashes = _blob_hashes(_decode_commit(existing))
                for h in new_hashes - old_hashes:
                    pipe.incr(_blob_ref_key(h))
            pipe.execute()

    def get_commit(self, hash: str) -> Commit | None:
        if not hash:
            return None
        if len(hash) < 64:
            matches = list(self._redis.scan_iter(match=_commit_key(hash) + "*", count=100))
            if not matches:
                return None
            if len(matches) > 1:
                hashes: list[str] = []
                for m in matches:
                    key_str = m.decode() if isinstance(m, bytes) else str(m)
                    hashes.append(key_str.split(":")[-1])
                matches_str = ", ".join(h[:12] for h in hashes)
                raise ValueError(
                    f"Ambiguous prefix {hash[:12]} matches {len(matches)} commits: {matches_str}"
                )
            data = self._redis.get(matches[0])
        else:
            data = self._redis.get(_commit_key(hash))
        if data is None:
            return None
        return _decode_commit(data)

    def find_by_fingerprint(self, fingerprint: str) -> Commit | None:
        hashes = self._redis.zrevrange(_fp_key(fingerprint), 0, -1)
        for h in hashes:
            h_str = h.decode() if isinstance(h, bytes) else h
            commit = self.get_commit(h_str)
            if commit is not None and commit.status in (TaskStatus.COMPLETED, TaskStatus.CACHED):
                self._touch_commit(h_str)
                return commit
        return None

    def find_running_by_fingerprint(self, fingerprint: str) -> Commit | None:
        hashes = self._redis.zrevrange(_fp_key(fingerprint), 0, -1)
        for h in hashes:
            h_str = h.decode() if isinstance(h, bytes) else h
            commit = self.get_commit(h_str)
            if commit is not None and commit.status == TaskStatus.RUNNING:
                return commit
        return None

    def list_commits(
        self,
        func_name: str | None = None,
        limit: int = 50,
        status: TaskStatus | None = None,
        tags: dict[str, str | None] | None = None,
    ) -> list[Commit]:
        if func_name:
            hashes = self._redis.zrevrange(_func_key(func_name), 0, limit * 2 - 1)
        else:
            hashes = self._redis.zrevrange("cashet:index:all", 0, limit * 2 - 1)
        commits: list[Commit] = []
        for h in hashes:
            h_str = h.decode() if isinstance(h, bytes) else h
            commit = self.get_commit(h_str)
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

    def get_history(self, hash: str) -> list[Commit]:
        if not hash:
            return []
        commit = self.get_commit(hash)
        if commit is None:
            return []
        fingerprint = commit.fingerprint
        hashes = self._redis.zrange(_fp_key(fingerprint), 0, -1)
        commits: list[Commit] = []
        for h in hashes:
            h_str = h.decode() if isinstance(h, bytes) else h
            c = self.get_commit(h_str)
            if c is not None and c.status in (TaskStatus.COMPLETED, TaskStatus.CACHED):
                commits.append(c)
        return commits

    def stats(self) -> dict[str, int]:
        total = self._redis.zcard("cashet:index:all")
        completed = 0
        for status in ("completed", "cached"):
            completed += self._redis.scard(_status_key(status))
        blob_count = 0
        for _ in self._redis.scan_iter(match="cashet:blob:*"):
            blob_count += 1
        return {
            "total_commits": total,
            "completed_commits": completed,
            "stored_objects": blob_count,
            "disk_bytes": 0,
            "blob_objects": blob_count,
            "blob_bytes": 0,
            "inline_objects": 0,
            "inline_bytes": 0,
        }

    def evict(self, older_than: datetime, max_size_bytes: int | None = None) -> int:
        cutoff_ts = older_than.timestamp()
        old_hashes = self._redis.zrangebyscore("cashet:index:all", 0, cutoff_ts)
        deleted = 0
        for h in old_hashes:
            h_str = h.decode() if isinstance(h, bytes) else h
            if self.delete_commit(h_str):
                deleted += 1
        if max_size_bytes is not None:
            while True:
                info = self._redis.info("memory")
                used = info.get("used_memory", 0)
                if used <= max_size_bytes:
                    break
                oldest = self._redis.zrange("cashet:index:all", 0, 0)
                if not oldest:
                    break
                h = oldest[0]
                h_str = h.decode() if isinstance(h, bytes) else h
                if self.delete_commit(h_str):
                    deleted += 1
        return deleted

    def delete_commit(self, hash: str) -> bool:
        if not hash:
            return False
        commit = self.get_commit(hash)
        if commit is None:
            return False
        pipe = self._redis.pipeline()
        pipe.delete(_commit_key(hash))
        pipe.zrem("cashet:index:all", hash)
        pipe.zrem(_fp_key(commit.fingerprint), hash)
        pipe.zrem(_func_key(commit.task_def.func_name), hash)
        for status in TaskStatus:
            pipe.srem(_status_key(status.value), hash)
        for h in _blob_hashes(commit):
            pipe.decr(_blob_ref_key(h))
        pipe.execute()
        for h in _blob_hashes(commit):
            count = self._redis.get(_blob_ref_key(h))
            count = 0 if count is None else int(count)
            if count <= 0:
                self._redis.delete(_blob_key(h), _blob_ref_key(h))
        return True

    def _touch_commit(self, hash: str) -> None:
        data = self._redis.get(_commit_key(hash))
        if data is None:
            return
        d = json.loads(data)
        d["last_accessed_at"] = datetime.now(UTC).isoformat()
        self._redis.set(_commit_key(hash), json.dumps(d, separators=(",", ":")).encode())

    def close(self) -> None:
        self._redis.close()


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
        exists = await self._redis.exists(key)
        if not exists:
            await self._redis.set(key, data)
        return ObjectRef(hash=content_hash, size=len(data), tier=StorageTier.BLOB)

    async def get_blob(self, ref: ObjectRef) -> bytes:
        data = await self._redis.get(_blob_key(ref.hash))
        if data is None:
            raise ValueError(f"Blob {ref.hash} not found")
        if isinstance(data, str):
            data = data.encode()
        if hashlib.sha256(data).hexdigest() != ref.hash:
            raise ValueError(f"Blob {ref.hash} integrity check failed")
        return data

    async def put_commit(self, commit: Commit) -> None:
        existing = await self._redis.get(_commit_key(commit.hash))
        pipe = self._redis.pipeline()
        pipe.set(_commit_key(commit.hash), _encode_commit(commit))
        ts = commit.created_at.timestamp()
        pipe.zadd("cashet:index:all", {commit.hash: ts})
        pipe.zadd(_fp_key(commit.fingerprint), {commit.hash: ts})
        pipe.zadd(_func_key(commit.task_def.func_name), {commit.hash: ts})
        for status in TaskStatus:
            pipe.srem(_status_key(status.value), commit.hash)
        pipe.sadd(_status_key(commit.status.value), commit.hash)
        new_hashes = _blob_hashes(commit)
        if existing is None:
            for h in new_hashes:
                pipe.incr(_blob_ref_key(h))
        else:
            old_hashes = _blob_hashes(_decode_commit(existing))
            for h in new_hashes - old_hashes:
                pipe.incr(_blob_ref_key(h))
        await pipe.execute()

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
                    key_str = m.decode() if isinstance(m, bytes) else str(m)
                    hashes.append(key_str.split(":")[-1])
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
            hashes = await self._redis.zrevrange(_func_key(func_name), 0, limit * 2 - 1)
        else:
            hashes = await self._redis.zrevrange("cashet:index:all", 0, limit * 2 - 1)
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
        blob_count = 0
        async for _ in self._redis.scan_iter(match="cashet:blob:*"):
            blob_count += 1
        return {
            "total_commits": total,
            "completed_commits": completed,
            "stored_objects": blob_count,
            "disk_bytes": 0,
            "blob_objects": blob_count,
            "blob_bytes": 0,
            "inline_objects": 0,
            "inline_bytes": 0,
        }

    async def evict(self, older_than: datetime, max_size_bytes: int | None = None) -> int:
        cutoff_ts = older_than.timestamp()
        old_hashes = await self._redis.zrangebyscore("cashet:index:all", 0, cutoff_ts)
        deleted = 0
        for h in old_hashes:
            h_str = h.decode() if isinstance(h, bytes) else h
            if await self.delete_commit(h_str):
                deleted += 1
        if max_size_bytes is not None:
            while True:
                info = await self._redis.info("memory")
                used = info.get("used_memory", 0)
                if used <= max_size_bytes:
                    break
                oldest = await self._redis.zrange("cashet:index:all", 0, 0)
                if not oldest:
                    break
                h = oldest[0]
                h_str = h.decode() if isinstance(h, bytes) else h
                if await self.delete_commit(h_str):
                    deleted += 1
        return deleted

    async def delete_commit(self, hash: str) -> bool:
        if not hash:
            return False
        commit = await self.get_commit(hash)
        if commit is None:
            return False
        pipe = self._redis.pipeline()
        pipe.delete(_commit_key(hash))
        pipe.zrem("cashet:index:all", hash)
        pipe.zrem(_fp_key(commit.fingerprint), hash)
        pipe.zrem(_func_key(commit.task_def.func_name), hash)
        for status in TaskStatus:
            pipe.srem(_status_key(status.value), hash)
        for h in _blob_hashes(commit):
            pipe.decr(_blob_ref_key(h))
        await pipe.execute()
        for h in _blob_hashes(commit):
            count = await self._redis.get(_blob_ref_key(h))
            count = 0 if count is None else int(count)
            if count <= 0:
                await self._redis.delete(_blob_key(h), _blob_ref_key(h))
        return True

    async def _touch_commit(self, hash: str) -> None:
        data = await self._redis.get(_commit_key(hash))
        if data is None:
            return
        d = json.loads(data)
        d["last_accessed_at"] = datetime.now(UTC).isoformat()
        encoded = json.dumps(d, separators=(",", ":")).encode()
        await self._redis.set(_commit_key(hash), encoded)

    async def close(self) -> None:
        await self._redis.aclose()
