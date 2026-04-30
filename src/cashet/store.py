from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import sqlite3
import threading
import zlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from cashet._runner import BlockingAsyncRunner
from cashet.models import Commit, ObjectRef, StorageTier, TaskDef, TaskStatus

_BLOB_COMPRESS_THRESHOLD = 256
_INLINE_THRESHOLD = 1024

logger = logging.getLogger("cashet")


# Guard against accidental wildcard matches when user passes invalid characters.
def _is_hash_prefix(hash: str) -> bool:
    return 0 < len(hash) <= 64 and all(c in "0123456789abcdefABCDEF" for c in hash)


@dataclass
class _SQLiteLockState:
    thread_lock: threading.Lock
    file_lock: Any


_SQLITE_LOCKS: dict[str, _SQLiteLockState] = {}
_SQLITE_LOCKS_GUARD = threading.Lock()


def _sqlite_lock_state(lock_path: str) -> _SQLiteLockState:
    with _SQLITE_LOCKS_GUARD:
        state = _SQLITE_LOCKS.get(lock_path)
        if state is None:
            from filelock import FileLock

            state = _SQLiteLockState(
                thread_lock=threading.Lock(),
                file_lock=FileLock(lock_path, timeout=30, thread_local=False),
            )
            _SQLITE_LOCKS[lock_path] = state
        return state


class _SQLiteStoreCore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.objects_dir = root / "objects"
        self.db_path = root / "meta.db"
        self.objects_dir.mkdir(parents=True, exist_ok=True)
        self._tls = threading.local()
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self, *, immediate: bool = False) -> sqlite3.Connection:
        conn: sqlite3.Connection | None = getattr(self._tls, "conn", None)
        try:
            if conn is not None:
                conn.execute("SELECT 1")
            else:
                conn = None
        except sqlite3.Error:
            conn = None
        if conn is None:
            conn = sqlite3.connect(str(self.db_path), isolation_level=None)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            self._tls.conn = conn
        if immediate:
            conn.execute("BEGIN IMMEDIATE")
        return conn

    def close(self) -> None:
        logger.debug("closing sqlite store path=%s", str(self.db_path))
        conn: sqlite3.Connection | None = getattr(self._tls, "conn", None)
        if conn is not None:
            conn.close()
            self._tls.conn = None

    def _init_db(self) -> None:
        logger.debug("initializing sqlite store path=%s", str(self.db_path))
        conn = self._connect()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS commits (
                hash TEXT PRIMARY KEY,
                fingerprint TEXT NOT NULL,
                func_name TEXT NOT NULL,
                func_hash TEXT NOT NULL,
                args_hash TEXT NOT NULL,
                args_snapshot BLOB,
                func_source TEXT,
                dep_versions TEXT,
                cache INTEGER NOT NULL DEFAULT 1,
                retries INTEGER NOT NULL DEFAULT 0,
                force INTEGER NOT NULL DEFAULT 0,
                timeout_seconds REAL,
                ttl_seconds REAL,
                input_refs TEXT,
                output_hash TEXT,
                output_size INTEGER,
                output_tier TEXT,
                parent_hash TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                error TEXT,
                tags TEXT,
                created_at TEXT NOT NULL,
                claimed_at TEXT NOT NULL,
                last_accessed_at TEXT,
                expires_at TEXT,
                FOREIGN KEY (parent_hash) REFERENCES commits(hash)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fingerprint ON commits(fingerprint)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_func_name ON commits(func_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON commits(created_at)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_last_accessed_at ON commits(last_accessed_at)"
        )
        self._migrate_last_accessed_at(conn)
        self._migrate_retries(conn)
        self._migrate_task_options(conn)
        self._migrate_expires_at(conn)
        self._migrate_ttl(conn)
        self._migrate_claimed_at(conn)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS inline_objects (
                hash TEXT PRIMARY KEY,
                data BLOB NOT NULL
            )
        """)

    def _migrate_last_accessed_at(self, conn: sqlite3.Connection) -> None:
        col_names = [r[1] for r in conn.execute("PRAGMA table_info(commits)").fetchall()]
        if "last_accessed_at" not in col_names:
            conn.execute(
                "ALTER TABLE commits ADD COLUMN last_accessed_at TEXT DEFAULT NULL"
            )
            conn.execute(
                "UPDATE commits SET last_accessed_at = created_at WHERE last_accessed_at IS NULL"
            )

    def _migrate_retries(self, conn: sqlite3.Connection) -> None:
        col_names = [r[1] for r in conn.execute("PRAGMA table_info(commits)").fetchall()]
        if "retries" not in col_names:
            conn.execute(
                "ALTER TABLE commits ADD COLUMN retries INTEGER NOT NULL DEFAULT 0"
            )

    def _migrate_task_options(self, conn: sqlite3.Connection) -> None:
        col_names = [r[1] for r in conn.execute("PRAGMA table_info(commits)").fetchall()]
        if "force" not in col_names:
            conn.execute(
                "ALTER TABLE commits ADD COLUMN force INTEGER NOT NULL DEFAULT 0"
            )
        if "timeout_seconds" not in col_names:
            conn.execute("ALTER TABLE commits ADD COLUMN timeout_seconds REAL")

    def _migrate_expires_at(self, conn: sqlite3.Connection) -> None:
        col_names = [r[1] for r in conn.execute("PRAGMA table_info(commits)").fetchall()]
        if "expires_at" not in col_names:
            conn.execute("ALTER TABLE commits ADD COLUMN expires_at TEXT")

    def _migrate_ttl(self, conn: sqlite3.Connection) -> None:
        col_names = [r[1] for r in conn.execute("PRAGMA table_info(commits)").fetchall()]
        if "ttl_seconds" not in col_names:
            conn.execute("ALTER TABLE commits ADD COLUMN ttl_seconds REAL")

    def _migrate_claimed_at(self, conn: sqlite3.Connection) -> None:
        col_names = [r[1] for r in conn.execute("PRAGMA table_info(commits)").fetchall()]
        if "claimed_at" not in col_names:
            conn.execute(
                "ALTER TABLE commits ADD COLUMN claimed_at TEXT NOT NULL DEFAULT ''"
            )
            conn.execute(
                "UPDATE commits SET claimed_at = created_at WHERE claimed_at = ''"
            )

    def put_blob(self, data: bytes) -> ObjectRef:
        content_hash = hashlib.sha256(data).hexdigest()
        if len(data) < _INLINE_THRESHOLD:
            conn = self._connect()
            conn.execute(
                "INSERT OR IGNORE INTO inline_objects (hash, data) VALUES (?, ?)",
                (content_hash, data),
            )
            logger.info(
                "blob stored hash=%s size=%d tier=inline",
                content_hash[:12],
                len(data),
            )
            return ObjectRef(hash=content_hash, size=len(data), tier=StorageTier.INLINE)
        prefix = content_hash[:2]
        suffix = content_hash[2:]
        obj_path = self.objects_dir / prefix / suffix
        if obj_path.exists():
            logger.debug(
                "blob deduplicated hash=%s size=%d",
                content_hash[:12],
                len(data),
            )
            return ObjectRef(hash=content_hash, size=len(data), tier=StorageTier.BLOB)
        obj_path.parent.mkdir(parents=True, exist_ok=True)
        stored = data
        if len(data) >= _BLOB_COMPRESS_THRESHOLD:
            compressed = zlib.compress(data, level=6)
            if len(compressed) < len(data):
                stored = compressed
        obj_path.write_bytes(stored)
        logger.info(
            "blob stored hash=%s size=%d tier=blob compressed=%s",
            content_hash[:12],
            len(data),
            "true" if stored is not data else "false",
        )
        return ObjectRef(hash=content_hash, size=len(data), tier=StorageTier.BLOB)

    def get_blob(self, ref: ObjectRef) -> bytes:
        if ref.tier == StorageTier.INLINE:
            conn = self._connect()
            row = conn.execute(
                "SELECT data FROM inline_objects WHERE hash = ?", (ref.hash,)
            ).fetchone()
            if row is None:
                logger.warning("inline blob not found hash=%s", ref.hash[:12])
                raise ValueError(f"Inline blob {ref.hash} not found")
            data = row["data"]
            if hashlib.sha256(data).hexdigest() != ref.hash:
                logger.error(
                    "inline blob integrity check failed hash=%s",
                    ref.hash[:12],
                )
                raise ValueError(f"Inline blob {ref.hash} integrity check failed")
            return data
        prefix = ref.hash[:2]
        suffix = ref.hash[2:]
        obj_path = self.objects_dir / prefix / suffix
        raw = obj_path.read_bytes()
        try:
            decompressed = zlib.decompress(raw)
            if hashlib.sha256(decompressed).hexdigest() == ref.hash:
                return decompressed
        except zlib.error:
            pass
        if hashlib.sha256(raw).hexdigest() != ref.hash:
            logger.error(
                "blob integrity check failed hash=%s",
                ref.hash[:12],
            )
            raise ValueError(f"Blob {ref.hash} integrity check failed")
        return raw

    def blob_exists(self, hash: str) -> bool:
        if (self.objects_dir / hash[:2] / hash[2:]).exists():
            return True
        conn = self._connect()
        row = conn.execute(
            "SELECT 1 FROM inline_objects WHERE hash = ?", (hash,)
        ).fetchone()
        return row is not None

    def put_commit(self, commit: Commit) -> None:
        conn = self._connect(immediate=True)
        now = datetime.now(UTC).isoformat()
        try:
            self._put_commit_row(conn, commit, now)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def _put_commit_row(
        self, conn: sqlite3.Connection, commit: Commit, accessed_at: str
    ) -> None:
        output_hash = commit.output_ref.hash if commit.output_ref else None
        output_size = commit.output_ref.size if commit.output_ref else None
        output_tier = commit.output_ref.tier.value if commit.output_ref else None
        timeout_seconds = (
            commit.task_def.timeout.total_seconds() if commit.task_def.timeout else None
        )
        conn.execute(
            """INSERT OR REPLACE INTO commits
               (hash, fingerprint, func_name, func_hash, args_hash, args_snapshot,
                func_source, dep_versions, cache, retries, force, timeout_seconds,
                ttl_seconds, input_refs, output_hash, output_size, output_tier, parent_hash,
                status, error, tags, created_at,
                claimed_at, last_accessed_at, expires_at)
               VALUES (
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
               )""",
            (
                commit.hash,
                commit.fingerprint,
                commit.task_def.func_name,
                commit.task_def.func_hash,
                commit.task_def.args_hash,
                commit.task_def.args_snapshot,
                commit.task_def.func_source,
                json.dumps(commit.task_def.dep_versions),
                int(commit.task_def.cache),
                commit.task_def.retries,
                int(commit.task_def.force),
                timeout_seconds,
                commit.task_def.ttl.total_seconds() if commit.task_def.ttl else None,
                json.dumps([r.hash for r in commit.input_refs]),
                output_hash,
                output_size,
                output_tier,
                commit.parent_hash,
                commit.status.value,
                commit.error,
                json.dumps(commit.tags),
                commit.created_at.isoformat(),
                commit.claimed_at.isoformat(),
                accessed_at,
                commit.expires_at.isoformat() if commit.expires_at else None,
            ),
        )

    def find_by_fingerprint(self, fingerprint: str) -> Commit | None:
        conn = self._connect()
        cursor = conn.execute(
            """SELECT * FROM commits
               WHERE fingerprint = ? AND status IN ('completed', 'cached')
               ORDER BY created_at DESC""",
            (fingerprint,),
        )
        now = datetime.now(UTC)
        for row in cursor:
            commit = self._row_to_commit(row)
            if commit.expires_at is not None and commit.expires_at <= now:
                continue
            conn.execute(
                "UPDATE commits SET last_accessed_at = ? WHERE hash = ?",
                (now.isoformat(), row["hash"]),
            )
            return commit
        return None

    def find_running_by_fingerprint(self, fingerprint: str) -> Commit | None:
        conn = self._connect()
        row = conn.execute(
            """SELECT * FROM commits
               WHERE fingerprint = ? AND status = 'running'
               ORDER BY claimed_at DESC LIMIT 1""",
            (fingerprint,),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_commit(row)

    def get_commit(self, hash: str) -> Commit | None:
        if not _is_hash_prefix(hash):
            return None
        conn = self._connect()
        if len(hash) < 64:
            rows = conn.execute(
                "SELECT * FROM commits WHERE hash LIKE ?", (hash + "%",)
            ).fetchall()
            if not rows:
                return None
            if len(rows) > 1:
                matches = ", ".join(r["hash"][:12] for r in rows)
                raise ValueError(
                    f"Ambiguous prefix {hash[:12]} matches {len(rows)} commits: {matches}"
                )
            return self._row_to_commit(rows[0])
        row = conn.execute("SELECT * FROM commits WHERE hash = ?", (hash,)).fetchone()
        if row is None:
            return None
        return self._row_to_commit(row)

    def list_commits(
        self,
        func_name: str | None = None,
        limit: int = 50,
        status: TaskStatus | None = None,
        tags: dict[str, str | None] | None = None,
    ) -> list[Commit]:
        conn = self._connect()
        query = "SELECT * FROM commits WHERE 1=1"
        params: list[Any] = []
        if func_name:
            query += " AND func_name = ?"
            params.append(func_name)
        if status:
            query += " AND status = ?"
            params.append(status.value)
        if tags:
            for key, val in tags.items():
                if val is None:
                    query += " AND json_extract(tags, ?) IS NOT NULL"
                    params.append(f"$.{key}")
                else:
                    query += " AND json_extract(tags, ?) = ?"
                    params.append(f"$.{key}")
                    params.append(val)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(query, params).fetchall()
        return [self._row_to_commit(r) for r in rows]

    def get_history(self, hash: str) -> list[Commit]:
        if not _is_hash_prefix(hash):
            return []
        conn = self._connect()
        if len(hash) < 64:
            rows = conn.execute(
                "SELECT * FROM commits WHERE hash LIKE ?", (hash + "%",)
            ).fetchall()
            if not rows:
                return []
            if len(rows) > 1:
                matches = ", ".join(r["hash"][:12] for r in rows)
                raise ValueError(
                    f"Ambiguous prefix {hash[:12]} matches {len(rows)} commits: {matches}"
                )
            commit = self._row_to_commit(rows[0])
        else:
            row = conn.execute("SELECT * FROM commits WHERE hash = ?", (hash,)).fetchone()
            if row is None:
                return []
            commit = self._row_to_commit(row)
        fingerprint = commit.fingerprint
        rows = conn.execute(
            """SELECT * FROM commits
               WHERE fingerprint = ? AND status IN ('completed', 'cached')
               ORDER BY created_at ASC""",
            (fingerprint,),
        ).fetchall()
        now = datetime.now(UTC)
        result: list[Commit] = []
        for r in rows:
            c = self._row_to_commit(r)
            if c.expires_at is not None and c.expires_at <= now:
                continue
            result.append(c)
        return result

    def stats(self) -> dict[str, int]:
        conn = self._connect()
        total = conn.execute("SELECT COUNT(*) FROM commits").fetchone()[0]
        completed = conn.execute(
            "SELECT COUNT(*) FROM commits WHERE status IN ('completed', 'cached')"
        ).fetchone()[0]
        obj_count, total_bytes = self._blob_storage_totals()
        inline_count, inline_bytes = self._inline_storage_totals(conn)
        return {
            "total_commits": total,
            "completed_commits": completed,
            "stored_objects": obj_count + inline_count,
            "disk_bytes": total_bytes + inline_bytes,
            "blob_objects": obj_count,
            "blob_bytes": total_bytes,
            "inline_objects": inline_count,
            "inline_bytes": inline_bytes,
        }

    def _blob_storage_totals(self) -> tuple[int, int]:
        obj_count = 0
        total_bytes = 0
        for p in self.objects_dir.iterdir():
            if p.is_dir():
                for f in p.iterdir():
                    if f.is_file():
                        obj_count += 1
                        total_bytes += f.stat().st_size
        return obj_count, total_bytes

    def _inline_storage_totals(self, conn: sqlite3.Connection) -> tuple[int, int]:
        inline_row = conn.execute(
            "SELECT COUNT(*) AS cnt, COALESCE(SUM(LENGTH(data)), 0) AS bytes FROM inline_objects"
        ).fetchone()
        inline_count = inline_row["cnt"] if inline_row else 0
        inline_bytes = inline_row["bytes"] if inline_row else 0
        return inline_count, inline_bytes

    def _storage_bytes(self, conn: sqlite3.Connection) -> int:
        return self._blob_storage_totals()[1] + self._inline_storage_totals(conn)[1]

    def evict(
        self, older_than: datetime, max_size_bytes: int | None = None
    ) -> int:
        conn = self._connect(immediate=True)
        orphans: list[str] = []
        try:
            cutoff = older_than.isoformat()
            candidates: set[str] = set()
            evicted_hashes = [
                row[0]
                for row in conn.execute(
                    "SELECT hash FROM commits WHERE last_accessed_at < ?", (cutoff,)
                )
            ]
            for row in conn.execute(
                "SELECT output_hash FROM commits "
                "WHERE last_accessed_at < ? AND output_hash IS NOT NULL",
                (cutoff,),
            ):
                candidates.add(row[0])
            for row in conn.execute(
                "SELECT input_refs FROM commits "
                "WHERE last_accessed_at < ? AND input_refs IS NOT NULL",
                (cutoff,),
            ):
                for h in json.loads(row[0]):
                    candidates.add(h)
            if evicted_hashes:
                placeholders = ", ".join("?" for _ in evicted_hashes)
                conn.execute(
                    f"UPDATE commits SET parent_hash = NULL WHERE parent_hash IN ({placeholders})",
                    evicted_hashes,
                )
            cursor = conn.execute(
                "DELETE FROM commits WHERE last_accessed_at < ?", (cutoff,)
            )
            deleted = cursor.rowcount
            if candidates:
                orphans = self._find_orphan_objects(conn, candidates)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        if orphans:
            logger.info(
                "orphan objects cleaned count=%d",
                len(orphans),
            )
            self._delete_orphan_objects(conn, orphans)

        if max_size_bytes is not None:
            current_bytes = self._storage_bytes(conn)
            if current_bytes > max_size_bytes:
                deleted += self._evict_to_size(current_bytes, max_size_bytes)

        if deleted:
            logger.info(
                "eviction complete deleted=%d reason=%s",
                deleted,
                "size_limit" if max_size_bytes is not None else "ttl",
            )
            c = self._connect()
            mode = c.execute("PRAGMA auto_vacuum").fetchone()[0]
            if mode == 2:
                c.execute("PRAGMA incremental_vacuum")
            else:
                c.execute("VACUUM")
        else:
            logger.debug("eviction found no candidates")

        return deleted

    def _evict_to_size(self, current_bytes: int, max_size_bytes: int) -> int:
        pending_orphans: list[str] = []
        deleted = 0
        conn = self._connect(immediate=True)
        try:
            ref_counts = self._object_ref_counts(conn)
            rows = conn.execute(
                "SELECT * FROM commits ORDER BY last_accessed_at ASC"
            ).fetchall()
            for target in rows:
                if current_bytes <= max_size_bytes:
                    break
                refs = self._row_object_refs(target)
                conn.execute(
                    "UPDATE commits SET parent_hash = NULL WHERE parent_hash = ?",
                    (target["hash"],),
                )
                conn.execute("DELETE FROM commits WHERE hash = ?", (target["hash"],))
                deleted += 1
                for obj_hash in refs:
                    count = ref_counts.get(obj_hash, 0)
                    if count <= 0:
                        continue
                    if count == 1:
                        ref_counts.pop(obj_hash, None)
                        if obj_hash not in pending_orphans:
                            current_bytes -= self._object_storage_size(conn, obj_hash)
                            pending_orphans.append(obj_hash)
                    else:
                        ref_counts[obj_hash] = count - 1
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        if pending_orphans:
            logger.info(
                "orphan objects cleaned count=%d",
                len(pending_orphans),
            )
            self._delete_orphan_objects(conn, pending_orphans)
        return deleted

    def _delete_commit_body(self, conn: sqlite3.Connection, hash: str) -> tuple[bool, list[str]]:
        if len(hash) < 64:
            rows = conn.execute(
                "SELECT * FROM commits WHERE hash LIKE ?", (hash + "%",)
            ).fetchall()
            if not rows:
                return False, []
            if len(rows) > 1:
                matches = ", ".join(r["hash"][:12] for r in rows)
                raise ValueError(
                    f"Ambiguous prefix {hash[:12]} matches {len(rows)} commits: {matches}"
                )
            target = rows[0]
        else:
            target = conn.execute(
                "SELECT * FROM commits WHERE hash = ?", (hash,)
            ).fetchone()
            if target is None:
                return False, []

        candidates = set(self._row_object_refs(target))
        conn.execute(
            "UPDATE commits SET parent_hash = NULL WHERE parent_hash = ?",
            (target["hash"],),
        )
        conn.execute("DELETE FROM commits WHERE hash = ?", (target["hash"],))

        orphans: list[str] = []
        if candidates:
            orphans = self._find_orphan_objects(conn, candidates)
        return True, orphans

    def delete_commit(self, hash: str) -> bool:
        if not _is_hash_prefix(hash):
            return False
        conn = self._connect(immediate=True)
        try:
            success, orphans = self._delete_commit_body(conn, hash)
            if not success:
                # Avoid leaving a dangling transaction that poisons the next write.
                conn.execute("ROLLBACK")
                return False
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        if orphans:
            logger.info(
                "orphan objects cleaned count=%d",
                len(orphans),
            )
            self._delete_orphan_objects(conn, orphans)
        logger.debug("commit deleted hash=%s", hash[:12])
        return True

    def delete_by_tags(self, tags: dict[str, str | None]) -> int:
        conn = self._connect(immediate=True)
        query = "SELECT hash FROM commits WHERE 1=1"
        params: list[Any] = []
        for key, val in tags.items():
            if val is None:
                query += " AND json_extract(tags, ?) IS NOT NULL"
                params.append(f"$.{key}")
            else:
                query += " AND json_extract(tags, ?) = ?"
                params.append(f"$.{key}")
                params.append(val)
        rows = conn.execute(query, params).fetchall()
        deleted = 0
        all_orphans: list[str] = []
        try:
            for row in rows:
                success, orphans = self._delete_commit_body(conn, row[0])
                if success:
                    deleted += 1
                    all_orphans.extend(orphans)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        if all_orphans:
            logger.info(
                "orphan objects cleaned count=%d",
                len(all_orphans),
            )
            self._delete_orphan_objects(conn, all_orphans)
        return deleted

    def _row_object_refs(self, row: sqlite3.Row) -> list[str]:
        refs: list[str] = []
        if row["output_hash"]:
            refs.append(row["output_hash"])
        if row["input_refs"]:
            refs.extend(set(json.loads(row["input_refs"])))
        return refs

    def _object_ref_counts(self, conn: sqlite3.Connection) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in conn.execute("SELECT output_hash FROM commits WHERE output_hash IS NOT NULL"):
            counts[row[0]] = counts.get(row[0], 0) + 1
        for row in conn.execute("SELECT input_refs FROM commits WHERE input_refs IS NOT NULL"):
            for h in set(json.loads(row[0])):
                counts[h] = counts.get(h, 0) + 1
        return counts

    def _object_storage_size(self, conn: sqlite3.Connection, obj_hash: str) -> int:
        size = 0
        blob_path = self.objects_dir / obj_hash[:2] / obj_hash[2:]
        if blob_path.exists():
            size += blob_path.stat().st_size
        row = conn.execute(
            "SELECT LENGTH(data) AS size FROM inline_objects WHERE hash = ?", (obj_hash,)
        ).fetchone()
        if row is not None:
            size += row["size"] or 0
        return size

    def _find_orphan_objects(self, conn: sqlite3.Connection, candidates: set[str]) -> list[str]:
        still_output: set[str] = set()
        for row in conn.execute("SELECT output_hash FROM commits WHERE output_hash IS NOT NULL"):
            still_output.add(row[0])
        still_input: set[str] = set()
        for row in conn.execute("SELECT input_refs FROM commits WHERE input_refs IS NOT NULL"):
            still_input.update(json.loads(row[0]))
        return [h for h in candidates if h not in still_output and h not in still_input]

    def _delete_orphan_objects(self, conn: sqlite3.Connection | None, orphans: list[str]) -> int:
        if conn is None:
            conn = self._connect()
        freed = 0
        for obj_hash in orphans:
            blob_path = self.objects_dir / obj_hash[:2] / obj_hash[2:]
            if blob_path.exists():
                freed += blob_path.stat().st_size
                blob_path.unlink()
            row = conn.execute(
                "SELECT LENGTH(data) AS size FROM inline_objects WHERE hash = ?", (obj_hash,)
            ).fetchone()
            if row is not None:
                freed += row["size"] or 0
            conn.execute("DELETE FROM inline_objects WHERE hash = ?", (obj_hash,))
        for prefix_dir in list(self.objects_dir.iterdir()):
            if prefix_dir.is_dir() and not any(prefix_dir.iterdir()):
                prefix_dir.rmdir()
        return freed

    def _row_to_commit(self, row: sqlite3.Row) -> Commit:
        output_ref = None
        if row["output_hash"]:
            tier = StorageTier(row["output_tier"]) if row["output_tier"] else StorageTier.BLOB
            output_ref = ObjectRef(
                hash=row["output_hash"],
                size=row["output_size"] or 0,
                tier=tier,
            )
        input_refs: list[ObjectRef] = []
        if row["input_refs"]:
            for h in json.loads(row["input_refs"]):
                input_refs.append(ObjectRef(hash=h))
        dep_versions: dict[str, str] = {}
        if row["dep_versions"]:
            dep_versions = json.loads(row["dep_versions"])
        tags: dict[str, str] = {}
        if row["tags"]:
            tags = json.loads(row["tags"])
        row_keys = set(row.keys())
        timeout_seconds = row["timeout_seconds"] if "timeout_seconds" in row_keys else None
        ttl_seconds = row["ttl_seconds"] if "ttl_seconds" in row_keys else None
        task_def = TaskDef(
            func_hash=row["func_hash"],
            func_name=row["func_name"],
            func_source=row["func_source"] or "",
            args_hash=row["args_hash"],
            args_snapshot=row["args_snapshot"] or b"",
            dep_versions=dep_versions,
            cache=bool(row["cache"]),
            tags=tags,
            retries=row["retries"] if "retries" in row_keys else 0,
            force=bool(row["force"]) if "force" in row_keys else False,
            timeout=timedelta(seconds=timeout_seconds) if timeout_seconds is not None else None,
            ttl=timedelta(seconds=ttl_seconds) if ttl_seconds is not None else None,
        )
        created_at = row["created_at"]
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        claimed_at = row["claimed_at"]
        if isinstance(claimed_at, str):
            claimed_at = datetime.fromisoformat(claimed_at)
        expires_at = row["expires_at"]
        if isinstance(expires_at, str):
            expires_at = datetime.fromisoformat(expires_at)
        return Commit(
            hash=row["hash"],
            task_def=task_def,
            input_refs=input_refs,
            output_ref=output_ref,
            parent_hash=row["parent_hash"],
            status=TaskStatus(row["status"]),
            created_at=created_at,
            claimed_at=claimed_at,
            error=row["error"],
            tags=tags,
            expires_at=expires_at,
        )


class _SQLiteFingerprintLock:
    def __init__(self, lock_path: str) -> None:
        self._state = _sqlite_lock_state(lock_path)

    async def __aenter__(self) -> None:
        await asyncio.to_thread(self._state.thread_lock.acquire)
        try:
            await asyncio.to_thread(self._state.file_lock.acquire)
        except Exception:
            self._state.thread_lock.release()
            raise

    async def __aexit__(self, *args: Any) -> None:
        try:
            await asyncio.to_thread(self._state.file_lock.release)
        finally:
            self._state.thread_lock.release()


class AsyncSQLiteStore:
    def __init__(self, root: Path) -> None:
        self._core = _SQLiteStoreCore(root)
        self._write_lock = asyncio.Lock()

    def _fingerprint_lock(self, fingerprint: str) -> _SQLiteFingerprintLock:
        import hashlib

        fp_hash = hashlib.sha256(fingerprint.encode()).hexdigest()[:16]
        return _SQLiteFingerprintLock(str(self._core.root / f".lock-{fp_hash}"))

    @property
    def root(self) -> Path:
        return self._core.root

    @property
    def objects_dir(self) -> Path:
        return self._core.objects_dir

    @property
    def db_path(self) -> Path:
        return self._core.db_path

    async def put_blob(self, data: bytes) -> ObjectRef:
        async with self._write_lock:
            return await asyncio.to_thread(self._core.put_blob, data)

    async def get_blob(self, ref: ObjectRef) -> bytes:
        return await asyncio.to_thread(self._core.get_blob, ref)

    async def put_commit(self, commit: Commit) -> None:
        async with self._write_lock:
            await asyncio.to_thread(self._core.put_commit, commit)

    async def get_commit(self, hash: str) -> Commit | None:
        return await asyncio.to_thread(self._core.get_commit, hash)

    async def find_by_fingerprint(self, fingerprint: str) -> Commit | None:
        return await asyncio.to_thread(self._core.find_by_fingerprint, fingerprint)

    async def find_running_by_fingerprint(self, fingerprint: str) -> Commit | None:
        return await asyncio.to_thread(self._core.find_running_by_fingerprint, fingerprint)

    async def list_commits(
        self,
        func_name: str | None = None,
        limit: int = 50,
        status: TaskStatus | None = None,
        tags: dict[str, str | None] | None = None,
    ) -> list[Commit]:
        return await asyncio.to_thread(
            self._core.list_commits,
            func_name=func_name,
            limit=limit,
            status=status,
            tags=tags,
        )

    async def get_history(self, hash: str) -> list[Commit]:
        return await asyncio.to_thread(self._core.get_history, hash)

    async def stats(self) -> dict[str, int]:
        return await asyncio.to_thread(self._core.stats)

    async def evict(self, older_than: datetime, max_size_bytes: int | None = None) -> int:
        async with self._write_lock:
            return await asyncio.to_thread(self._core.evict, older_than, max_size_bytes)

    async def delete_commit(self, hash: str) -> bool:
        async with self._write_lock:
            return await asyncio.to_thread(self._core.delete_commit, hash)

    async def delete_by_tags(self, tags: dict[str, str | None]) -> int:
        async with self._write_lock:
            return await asyncio.to_thread(self._core.delete_by_tags, tags)

    async def close(self) -> None:
        await asyncio.to_thread(self._core.close)


class SQLiteStore:
    def __init__(self, root: Path) -> None:
        self._async_store = AsyncSQLiteStore(root)
        self._runner = BlockingAsyncRunner()

    @classmethod
    def from_async(
        cls, async_store: AsyncSQLiteStore, *, runner: BlockingAsyncRunner | None = None
    ) -> SQLiteStore:
        instance = cls.__new__(cls)
        instance._async_store = async_store
        instance._runner = runner or BlockingAsyncRunner()
        return instance

    @property
    def root(self) -> Path:
        return self._async_store.root

    @property
    def objects_dir(self) -> Path:
        return self._async_store.objects_dir

    @property
    def db_path(self) -> Path:
        return self._async_store.db_path

    def _connect(self, *, immediate: bool = False) -> sqlite3.Connection:
        core: Any = self._async_store._core  # pyright: ignore[reportPrivateUsage]
        return core._connect(immediate=immediate)

    def blob_exists(self, hash: str) -> bool:
        core: Any = self._async_store._core  # pyright: ignore[reportPrivateUsage]
        return core.blob_exists(hash)

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
