from cashet.async_client import AsyncClient
from cashet.client import Client
from cashet.dag import AsyncResultRef, ResultRef, TaskRef
from cashet.executor import LocalExecutor
from cashet.hashing import (
    ClosureWarning,
    JsonSerializer,
    PickleSerializer,
    SafePickleSerializer,
    Serializer,
)
from cashet.models import Commit, ObjectRef, TaskDef, TaskError, TaskStatus
from cashet.protocols import AsyncStore, Executor, Store
from cashet.store import AsyncSQLiteStore, SQLiteStore

try:
    from cashet.redis_store import AsyncRedisStore, RedisStore
except ImportError:
    AsyncRedisStore = None  # type: ignore[misc,assignment]
    RedisStore = None  # type: ignore[misc,assignment]

__all__ = [
    "AsyncClient",
    "AsyncRedisStore",
    "AsyncResultRef",
    "AsyncSQLiteStore",
    "AsyncStore",
    "Client",
    "ClosureWarning",
    "Commit",
    "Executor",
    "JsonSerializer",
    "LocalExecutor",
    "ObjectRef",
    "PickleSerializer",
    "RedisStore",
    "ResultRef",
    "SQLiteStore",
    "SafePickleSerializer",
    "Serializer",
    "Store",
    "TaskDef",
    "TaskError",
    "TaskRef",
    "TaskStatus",
]
