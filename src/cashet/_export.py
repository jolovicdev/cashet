from __future__ import annotations

import base64
import json
import logging
import tarfile
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any

from cashet.models import Commit, ObjectRef, StorageTier, TaskDef, TaskStatus
from cashet.protocols import AsyncStore

logger = logging.getLogger("cashet")

_ARCHIVE_ROOT = "cashet-export"
_LIST_ALL_LIMIT = 999_999_999
_COMMIT_FILE = f"{_ARCHIVE_ROOT}/commits.jsonl"
_BLOB_DIR = f"{_ARCHIVE_ROOT}/blobs"


def _object_ref_to_dict(ref: ObjectRef) -> dict[str, Any]:
    return {
        "hash": ref.hash,
        "size": ref.size,
        "tier": ref.tier.value,
    }


def _dict_to_object_ref(data: dict[str, Any]) -> ObjectRef:
    return ObjectRef(
        hash=data["hash"],
        size=data["size"],
        tier=StorageTier(data["tier"]),
    )


def _task_def_to_dict(task_def: TaskDef) -> dict[str, Any]:
    result: dict[str, Any] = {
        "func_hash": task_def.func_hash,
        "func_name": task_def.func_name,
        "func_source": task_def.func_source,
        "args_hash": task_def.args_hash,
        "args_snapshot": base64.b64encode(task_def.args_snapshot).decode("ascii"),
        "dep_versions": task_def.dep_versions,
        "cache": task_def.cache,
        "tags": task_def.tags,
        "retries": task_def.retries,
        "force": task_def.force,
    }
    if task_def.timeout is not None:
        result["timeout"] = task_def.timeout.total_seconds()
    return result


def _dict_to_task_def(data: dict[str, Any]) -> TaskDef:
    timeout: timedelta | None = None
    if "timeout" in data and data["timeout"] is not None:
        timeout = timedelta(seconds=data["timeout"])
    return TaskDef(
        func_hash=data["func_hash"],
        func_name=data["func_name"],
        func_source=data["func_source"],
        args_hash=data["args_hash"],
        args_snapshot=base64.b64decode(data["args_snapshot"]),
        dep_versions=data.get("dep_versions", {}),
        cache=data.get("cache", True),
        tags=data.get("tags", {}),
        retries=data.get("retries", 0),
        force=data.get("force", False),
        timeout=timeout,
    )


def _commit_to_dict(commit: Commit) -> dict[str, Any]:
    result: dict[str, Any] = {
        "hash": commit.hash,
        "task_def": _task_def_to_dict(commit.task_def),
        "input_refs": [_object_ref_to_dict(r) for r in commit.input_refs],
        "parent_hash": commit.parent_hash,
        "status": commit.status.value,
        "created_at": commit.created_at.isoformat(),
        "claimed_at": commit.claimed_at.isoformat(),
        "error": commit.error,
        "tags": commit.tags,
    }
    if commit.output_ref is not None:
        result["output_ref"] = _object_ref_to_dict(commit.output_ref)
    return result


def _dict_to_commit(data: dict[str, Any]) -> Commit:
    output_ref: ObjectRef | None = None
    if "output_ref" in data and data["output_ref"] is not None:
        output_ref = _dict_to_object_ref(data["output_ref"])
    return Commit(
        hash=data["hash"],
        task_def=_dict_to_task_def(data["task_def"]),
        input_refs=[_dict_to_object_ref(r) for r in data.get("input_refs", [])],
        output_ref=output_ref,
        parent_hash=data.get("parent_hash"),
        status=TaskStatus(data["status"]),
        created_at=datetime.fromisoformat(data["created_at"]),
        claimed_at=datetime.fromisoformat(data["claimed_at"]),
        error=data.get("error"),
        tags=data.get("tags", {}),
    )


def _blob_path(hash: str) -> str:
    return f"{_BLOB_DIR}/{hash[:2]}/{hash[2:]}"


async def export_store(store: AsyncStore, tar_path: Path) -> None:
    commits = await store.list_commits(limit=_LIST_ALL_LIMIT)
    blob_refs: dict[str, ObjectRef] = {}
    for commit in commits:
        if commit.output_ref is not None:
            blob_refs[commit.output_ref.hash] = commit.output_ref
        for ref in commit.input_refs:
            blob_refs[ref.hash] = ref
    with tarfile.open(tar_path, "w:gz") as tar:
        for ref in blob_refs.values():
            data = await store.get_blob(ref)
            info = tarfile.TarInfo(name=_blob_path(ref.hash))
            info.size = len(data)
            tar.addfile(info, BytesIO(data))

        lines = [json.dumps(_commit_to_dict(c)).encode("utf-8") + b"\n" for c in commits]
        commit_data = b"".join(lines)
        info = tarfile.TarInfo(name=_COMMIT_FILE)
        info.size = len(commit_data)
        tar.addfile(info, BytesIO(commit_data))


def _is_safe_tar_member(name: str) -> bool:
    if name.startswith("/") or name.startswith("\\"):
        return False
    return ".." not in name.split("/")


async def import_store(store: AsyncStore, tar_path: Path) -> int:
    imported = 0
    with tarfile.open(tar_path, "r:gz") as tar:
        members: dict[str, tarfile.TarInfo] = {}
        for m in tar.getmembers():
            if not _is_safe_tar_member(m.name):
                logger.warning("skipping unsafe tar member: %s", m.name)
                continue
            members[m.name] = m
        blob_refs: dict[str, ObjectRef] = {}
        missing_blobs: set[str] = set()

        commit_member = members.get(_COMMIT_FILE)
        if commit_member is not None and commit_member.isfile():
            f = tar.extractfile(commit_member)
            if f is not None:
                with f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        data = json.loads(line)
                        commit = _dict_to_commit(data)
                        existing = await store.get_commit(commit.hash)
                        if existing is not None:
                            continue
                        refs_to_import = list(commit.input_refs)
                        if commit.output_ref is not None:
                            refs_to_import.append(commit.output_ref)
                        has_missing = False
                        for ref in refs_to_import:
                            h = ref.hash
                            if h in missing_blobs:
                                has_missing = True
                                continue
                            if h not in blob_refs:
                                blob_member = members.get(_blob_path(h))
                                if blob_member is not None:
                                    tf = tar.extractfile(blob_member)
                                    if tf is not None:
                                        with tf:
                                            blob_refs[h] = await store.put_blob(tf.read())
                                else:
                                    logger.warning("missing blob in archive hash=%s", h[:12])
                                    missing_blobs.add(h)
                                    has_missing = True
                        if has_missing:
                            continue
                        commit.input_refs = [
                            blob_refs.get(r.hash, r) for r in commit.input_refs
                        ]
                        if commit.output_ref is not None:
                            out_h = commit.output_ref.hash
                            if out_h in blob_refs:
                                commit.output_ref = blob_refs[out_h]
                        await store.put_commit(commit)
                        imported += 1
    return imported
