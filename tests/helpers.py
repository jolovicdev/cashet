from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

from cashet.models import Commit, TaskDef, TaskStatus

DEFAULT_TEST_REDIS_URL = "redis://localhost:6379/15"


def redis_test_url() -> str:
    return os.environ.get("CASHET_TEST_REDIS_URL", DEFAULT_TEST_REDIS_URL)


def make_task_def(args_hash: str = "b" * 64) -> TaskDef:
    return TaskDef(
        func_hash="a" * 64,
        func_name="f",
        func_source="def f(): pass",
        args_hash=args_hash,
        args_snapshot=b"",
    )


def make_commit(
    hash: str,
    task_def: TaskDef,
    *,
    hours_ago: float = 0,
    expires_at: datetime | None = None,
    status: TaskStatus = TaskStatus.COMPLETED,
) -> Commit:
    return Commit(
        hash=hash,
        task_def=task_def,
        status=status,
        created_at=datetime.now(UTC) - timedelta(hours=hours_ago),
        expires_at=expires_at,
    )


class PickleableCustom:
    def __init__(self, val: int) -> None:
        self.val = val

    def __eq__(self, other: object) -> bool:
        return isinstance(other, PickleableCustom) and self.val == other.val
