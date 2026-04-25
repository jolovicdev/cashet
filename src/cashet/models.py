from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any


class StorageTier(enum.Enum):
    INLINE = "inline"
    BLOB = "blob"
    EXTERNAL = "external"


class TaskError(RuntimeError):
    pass


class TaskStatus(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CACHED = "cached"


@dataclass(frozen=True)
class ObjectRef:
    hash: str
    size: int = 0
    tier: StorageTier = StorageTier.BLOB

    def short(self) -> str:
        return self.hash[:12]


@dataclass
class TaskDef:
    func_hash: str
    func_name: str
    func_source: str
    args_hash: str
    args_snapshot: bytes
    dep_versions: dict[str, str] = field(default_factory=dict[str, str])
    cache: bool = True
    tags: dict[str, str] = field(default_factory=dict[str, str])
    retries: int = 0
    force: bool = False
    timeout: timedelta | None = None

    @property
    def fingerprint(self) -> str:
        return f"{self.func_hash}:{self.args_hash}"


@dataclass
class Commit:
    hash: str
    task_def: TaskDef
    input_refs: list[ObjectRef] = field(default_factory=list[ObjectRef])
    output_ref: ObjectRef | None = None
    parent_hash: str | None = None
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    claimed_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    error: str | None = None
    tags: dict[str, str] = field(default_factory=dict[str, str])

    @property
    def fingerprint(self) -> str:
        return f"{self.task_def.func_hash}:{self.task_def.args_hash}"

    def short_hash(self) -> str:
        return self.hash[:12]

    def __repr__(self) -> str:
        return (
            f"Commit({self.short_hash()}, {self.task_def.func_name}, "
            f"status={self.status.value})"
        )

    def summary(self) -> dict[str, Any]:
        return {
            "hash": self.hash,
            "short_hash": self.short_hash(),
            "function": self.task_def.func_name,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "output": self.output_ref.hash if self.output_ref else None,
            "parent": self.parent_hash[:12] if self.parent_hash else None,
            "tags": self.tags,
        }
