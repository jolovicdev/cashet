from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import zlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cashet.models import Commit, ObjectRef, StorageTier, TaskDef, TaskStatus

_BLOB_COMPRESS_THRESHOLD = 256


class SQLiteStore:
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
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            self._tls.conn = conn
        if immediate:
            conn.execute("BEGIN IMMEDIATE")
        return conn

    def close(self) -> None:
        conn: sqlite3.Connection | None = getattr(self._tls, "conn", None)
        if conn is not None:
            conn.close()
            self._tls.conn = None

    def _init_db(self) -> None:
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
        self._migrate_claimed_at(conn)

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
        prefix = content_hash[:2]
        suffix = content_hash[2:]
        obj_path = self.objects_dir / prefix / suffix
        if obj_path.exists():
            return ObjectRef(hash=content_hash, size=len(data), tier=StorageTier.BLOB)
        obj_path.parent.mkdir(parents=True, exist_ok=True)
        stored = data
        if len(data) >= _BLOB_COMPRESS_THRESHOLD:
            compressed = zlib.compress(data, level=6)
            if len(compressed) < len(data):
                stored = compressed
        obj_path.write_bytes(stored)
        return ObjectRef(hash=content_hash, size=len(data), tier=StorageTier.BLOB)

    def get_blob(self, ref: ObjectRef) -> bytes:
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
            raise ValueError(f"Blob {ref.hash} integrity check failed")
        return raw

    def blob_exists(self, hash: str) -> bool:
        return (self.objects_dir / hash[:2] / hash[2:]).exists()

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
        conn.execute(
            """INSERT OR REPLACE INTO commits
               (hash, fingerprint, func_name, func_hash, args_hash, args_snapshot,
                func_source, dep_versions, cache, retries, input_refs, output_hash, output_size,
                output_tier, parent_hash, status, error, tags, created_at,
                claimed_at, last_accessed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
            ),
        )

    def find_by_fingerprint(self, fingerprint: str) -> Commit | None:
        conn = self._connect()
        row = conn.execute(
            """SELECT * FROM commits
               WHERE fingerprint = ? AND status IN ('completed', 'cached')
               ORDER BY created_at DESC LIMIT 1""",
            (fingerprint,),
        ).fetchone()
        if row is None:
            return None
        conn.execute(
            "UPDATE commits SET last_accessed_at = ? WHERE hash = ?",
            (datetime.now(UTC).isoformat(), row["hash"]),
        )
        return self._row_to_commit(row)

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
        if not hash:
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
        if not hash:
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
        return [self._row_to_commit(r) for r in rows]

    def stats(self) -> dict[str, int]:
        conn = self._connect()
        total = conn.execute("SELECT COUNT(*) FROM commits").fetchone()[0]
        completed = conn.execute(
            "SELECT COUNT(*) FROM commits WHERE status IN ('completed', 'cached')"
        ).fetchone()[0]
        obj_count = 0
        total_bytes = 0
        for p in self.objects_dir.iterdir():
            if p.is_dir():
                for f in p.iterdir():
                    if f.is_file():
                        obj_count += 1
                        total_bytes += f.stat().st_size
        return {
            "total_commits": total,
            "completed_commits": completed,
            "stored_objects": obj_count,
            "disk_bytes": total_bytes,
        }

    def evict(self, older_than: datetime) -> int:
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
                orphans = self._find_orphan_blobs(conn, candidates)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        if orphans:
            self._delete_orphan_blobs(orphans)
        return deleted

    def delete_commit(self, hash: str) -> bool:
        conn = self._connect(immediate=True)
        orphans: list[str] = []
        try:
            if len(hash) < 64:
                rows = conn.execute(
                    "SELECT * FROM commits WHERE hash LIKE ?", (hash + "%",)
                ).fetchall()
                if not rows:
                    return False
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
                    return False

            candidates: set[str] = set()
            if target["output_hash"]:
                candidates.add(target["output_hash"])
            if target["input_refs"]:
                for h in json.loads(target["input_refs"]):
                    candidates.add(h)

            conn.execute(
                "UPDATE commits SET parent_hash = NULL WHERE parent_hash = ?",
                (target["hash"],),
            )
            conn.execute("DELETE FROM commits WHERE hash = ?", (target["hash"],))

            if candidates:
                orphans = self._find_orphan_blobs(conn, candidates)

            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        if orphans:
            self._delete_orphan_blobs(orphans)
        return True

    def _find_orphan_blobs(self, conn: sqlite3.Connection, candidates: set[str]) -> list[str]:
        still_output: set[str] = set()
        for row in conn.execute("SELECT output_hash FROM commits WHERE output_hash IS NOT NULL"):
            still_output.add(row[0])
        still_input: set[str] = set()
        for row in conn.execute("SELECT input_refs FROM commits WHERE input_refs IS NOT NULL"):
            still_input.update(json.loads(row[0]))
        return [h for h in candidates if h not in still_output and h not in still_input]

    def _delete_orphan_blobs(self, orphans: list[str]) -> None:
        for blob_hash in orphans:
            blob_path = self.objects_dir / blob_hash[:2] / blob_hash[2:]
            if blob_path.exists():
                blob_path.unlink()
        for prefix_dir in list(self.objects_dir.iterdir()):
            if prefix_dir.is_dir() and not any(prefix_dir.iterdir()):
                prefix_dir.rmdir()

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
        task_def = TaskDef(
            func_hash=row["func_hash"],
            func_name=row["func_name"],
            func_source=row["func_source"] or "",
            args_hash=row["args_hash"],
            args_snapshot=row["args_snapshot"] or b"",
            dep_versions=dep_versions,
            cache=bool(row["cache"]),
            tags=tags,
            retries=row["retries"] if "retries" in row.keys() else 0,  # noqa: SIM118
        )
        created_at = row["created_at"]
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        claimed_at = row["claimed_at"]
        if isinstance(claimed_at, str):
            claimed_at = datetime.fromisoformat(claimed_at)
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
        )
