from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from cashet.models import Commit, ObjectRef, StorageTier, TaskDef, TaskStatus

DECR_DELETE_SCRIPT = """
    local ref_key = KEYS[1]
    local blob_key = KEYS[2]
    local stats_key = KEYS[3]
    if redis.call('EXISTS', ref_key) == 0 then
        return 0
    end
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


def commit_key(hash: str) -> str:
    return f"cashet:commit:{hash}"


def blob_key(hash: str) -> str:
    return f"cashet:blob:data:{hash}"


def fp_key(fingerprint: str) -> str:
    return f"cashet:index:fingerprint:{fingerprint}"


def running_key(fingerprint: str) -> str:
    return f"cashet:index:running:{fingerprint}"


def func_key(func_name: str) -> str:
    return f"cashet:index:func:{func_name}"


def status_key(status: str) -> str:
    return f"cashet:index:status:{status}"


def tag_key(key: str) -> str:
    return f"cashet:tagk:{key}"


def tag_value_key(key: str, value: str) -> str:
    # Length-prefix the key so a ':' inside a tag key or value can never make
    # two distinct (key, value) pairs collide onto the same set.
    return f"cashet:tagv:{len(key)}:{key}:{value}"


def access_key() -> str:
    return "cashet:index:last_accessed"


def expires_key() -> str:
    return "cashet:index:expires"


def blob_stats_key() -> str:
    return "cashet:stats:blob"


def blob_stats_lock_key() -> str:
    return "cashet:stats:blob:lock"


def stats_ready(raw: Any) -> bool:
    if isinstance(raw, bytes):
        return raw == b"1"
    return raw == "1" or raw == 1


def encode_commit(commit: Commit) -> bytes:
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
        "ttl_seconds": (
            commit.task_def.ttl.total_seconds() if commit.task_def.ttl else None
        ),
        "expires_at": commit.expires_at.isoformat() if commit.expires_at else None,
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


def decode_commit(data: bytes) -> Commit:
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
        ttl=(
            timedelta(seconds=d["ttl_seconds"])
            if d.get("ttl_seconds") is not None
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
    expires_at = (
        datetime.fromisoformat(d["expires_at"]) if d.get("expires_at") is not None else None
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
        expires_at=expires_at,
    )


def decode_hash(raw: Any) -> str:
    return raw.decode() if isinstance(raw, bytes) else raw


def commit_access_timestamp(data: bytes) -> float:
    d = json.loads(data)
    value = d.get("last_accessed_at") or d.get("created_at")
    if value is None:
        return datetime.now(UTC).timestamp()
    return datetime.fromisoformat(value).timestamp()


def blob_ref_key(blob_hash: str) -> str:
    return f"cashet:blob:ref:{blob_hash}"


def blob_hashes(commit: Commit) -> set[str]:
    hashes: set[str] = set()
    if commit.output_ref:
        hashes.add(commit.output_ref.hash)
    for ref in commit.input_refs:
        hashes.add(ref.hash)
    return hashes


def matches_tags(commit: Commit, tags: dict[str, str | None]) -> bool:
    for key, val in tags.items():
        if val is None:
            if key not in commit.tags:
                return False
        else:
            if commit.tags.get(key) != val:
                return False
    return True


def stats_dict(total: int, completed: int, blob_count: int, blob_bytes: int) -> dict[str, int]:
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


def index_commit_commands(pipe: Any, commit: Commit) -> None:
    pipe.get(commit_key(commit.hash))
    pipe.set(commit_key(commit.hash), encode_commit(commit))
    ts = commit.created_at.timestamp()
    pipe.zadd("cashet:index:all", {commit.hash: ts})
    pipe.zadd(fp_key(commit.fingerprint), {commit.hash: ts})
    # Index expiry only once the commit is terminal: expires_at is stamped at
    # claim time, so a task outliving its TTL is expired while RUNNING, and
    # expiry eviction must not delete a live claim out from under its worker.
    if commit.expires_at is not None and commit.status not in (
        TaskStatus.RUNNING,
        TaskStatus.PENDING,
    ):
        pipe.zadd(expires_key(), {commit.hash: commit.expires_at.timestamp()})
    else:
        pipe.zrem(expires_key(), commit.hash)
    pipe.zadd(func_key(commit.task_def.func_name), {commit.hash: ts})
    now_ts = datetime.now(UTC).timestamp()
    pipe.zadd(access_key(), {commit.hash: now_ts})
    for status in TaskStatus:
        pipe.srem(status_key(status.value), commit.hash)
    pipe.sadd(status_key(commit.status.value), commit.hash)
    if commit.status == TaskStatus.RUNNING:
        pipe.sadd(running_key(commit.fingerprint), commit.hash)
    else:
        pipe.srem(running_key(commit.fingerprint), commit.hash)
    for key, val in commit.tags.items():
        pipe.sadd(tag_key(key), commit.hash)
        pipe.sadd(tag_value_key(key, val), commit.hash)


def remove_commit_index_commands(pipe: Any, commit: Commit, resolved_hash: str) -> None:
    pipe.delete(commit_key(resolved_hash))
    pipe.zrem("cashet:index:all", resolved_hash)
    pipe.zrem(fp_key(commit.fingerprint), resolved_hash)
    pipe.zrem(expires_key(), resolved_hash)
    pipe.srem(running_key(commit.fingerprint), resolved_hash)
    pipe.zrem(func_key(commit.task_def.func_name), resolved_hash)
    pipe.zrem(access_key(), resolved_hash)
    for status in TaskStatus:
        pipe.srem(status_key(status.value), resolved_hash)
    for key, val in commit.tags.items():
        pipe.srem(tag_key(key), resolved_hash)
        pipe.srem(tag_value_key(key, val), resolved_hash)


def commit_hash_from_key(raw: Any) -> str:
    return decode_hash(raw).split(":")[-1]
