from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import threading
import zlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from cashet._ids import normalize_hash_prefix
from cashet._sqlite_schema import ensure_schema, put_commit_row, row_to_commit
from cashet.models import Commit, ObjectRef, StorageTier, TaskStatus

_BLOB_COMPRESS_THRESHOLD = 256
_INLINE_THRESHOLD = 1024
_ACCESS_BUMP_GRANULARITY = timedelta(hours=1)

logger = logging.getLogger("cashet")


class SQLiteStoreCore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.objects_dir = root / "objects"
        self.db_path = root / "meta.db"
        self.objects_dir.mkdir(parents=True, exist_ok=True)
        self._tls = threading.local()
        self._lock = threading.Lock()
        logger.debug("initializing sqlite store path=%s", str(self.db_path))
        ensure_schema(self._connect())

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
            # NORMAL under WAL cannot corrupt the database; a power loss may
            # drop the most recent commits, which a compute cache can recompute.
            conn.execute("PRAGMA synchronous=NORMAL")
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
        # Write-then-rename so a crash mid-write can never leave a partial file
        # at the content-addressed path, where the exists() dedup check above
        # would treat it as the real blob forever.
        tmp_path = obj_path.with_name(
            f"{obj_path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
        )
        tmp_path.write_bytes(stored)
        os.replace(tmp_path, obj_path)
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
            # The path is derived from the content hash, so a mismatched file is
            # corrupt; removing it lets the next put_blob store it again.
            obj_path.unlink(missing_ok=True)
            logger.error(
                "corrupt blob removed hash=%s",
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
            put_commit_row(conn, commit, now)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def find_by_fingerprint(self, fingerprint: str) -> Commit | None:
        conn = self._connect()
        now = datetime.now(UTC)
        now_iso = now.isoformat()
        row = conn.execute(
            """SELECT * FROM commits
               WHERE fingerprint = ? AND status IN ('completed', 'cached')
               AND (expires_at IS NULL OR expires_at > ?)
               ORDER BY created_at DESC
               LIMIT 1""",
            (fingerprint, now_iso),
        ).fetchone()
        if row is None:
            return None
        try:
            # Bump at most once per granularity window: eviction cutoffs are
            # measured in days, so hour-precision LRU keeps steady-state cache
            # hits free of write transactions.
            threshold = (now - _ACCESS_BUMP_GRANULARITY).isoformat()
            conn.execute(
                "UPDATE commits SET last_accessed_at = ? WHERE hash = ? "
                "AND (last_accessed_at IS NULL OR last_accessed_at < ?)",
                (now_iso, row["hash"], threshold),
            )
        except sqlite3.OperationalError:
            # Access-time bump only feeds LRU ordering; never fail a cache hit
            # because a concurrent writer holds the lock past busy_timeout.
            logger.debug("last_accessed_at bump skipped (db locked) hash=%s", row["hash"][:12])
        return row_to_commit(row)

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
        return row_to_commit(row)

    def get_commit(self, hash: str) -> Commit | None:
        normalized = normalize_hash_prefix(hash)
        if normalized is None:
            return None
        hash = normalized
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
            return row_to_commit(rows[0])
        row = conn.execute("SELECT * FROM commits WHERE hash = ?", (hash,)).fetchone()
        if row is None:
            return None
        return row_to_commit(row)

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
        return [row_to_commit(r) for r in rows]

    def get_history(self, hash: str) -> list[Commit]:
        normalized = normalize_hash_prefix(hash)
        if normalized is None:
            return []
        hash = normalized
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
            commit = row_to_commit(rows[0])
        else:
            row = conn.execute("SELECT * FROM commits WHERE hash = ?", (hash,)).fetchone()
            if row is None:
                return []
            commit = row_to_commit(row)
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
            c = row_to_commit(r)
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
        evictable = "(last_accessed_at < ? OR (expires_at IS NOT NULL AND expires_at <= ?))"
        try:
            params = (older_than.isoformat(), datetime.now(UTC).isoformat())
            candidates: set[str] = set()
            evicted_hashes = [
                row[0]
                for row in conn.execute(
                    f"SELECT hash FROM commits WHERE {evictable}", params
                )
            ]
            for row in conn.execute(
                f"SELECT output_hash FROM commits "
                f"WHERE {evictable} AND output_hash IS NOT NULL",
                params,
            ):
                candidates.add(row[0])
            for row in conn.execute(
                f"SELECT input_refs FROM commits "
                f"WHERE {evictable} AND input_refs IS NOT NULL",
                params,
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
                f"DELETE FROM commits WHERE {evictable}", params
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
            try:
                self._vacuum()
            except sqlite3.OperationalError:
                # Reclaiming space is best-effort; a concurrent writer holding the
                # lock must not undo an eviction whose deletes already committed.
                logger.debug("vacuum skipped (db busy) after eviction")
        else:
            logger.debug("eviction found no candidates")

        return deleted

    def _vacuum(self) -> None:
        conn = self._connect()
        mode = conn.execute("PRAGMA auto_vacuum").fetchone()[0]
        if mode == 2:
            conn.execute("PRAGMA incremental_vacuum")
        else:
            conn.execute("VACUUM")

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
        normalized = normalize_hash_prefix(hash)
        if normalized is None:
            return False
        hash = normalized
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
        query = "SELECT hash, output_hash, input_refs FROM commits WHERE 1=1"
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
        if not rows:
            conn.execute("ROLLBACK")
            return 0
        hashes = [r[0] for r in rows]
        candidates: set[str] = set()
        for r in rows:
            if r[1]:
                candidates.add(r[1])
            if r[2]:
                for h in json.loads(r[2]):
                    candidates.add(h)
        try:
            placeholders = ", ".join("?" for _ in hashes)
            conn.execute(
                f"UPDATE commits SET parent_hash = NULL WHERE parent_hash IN ({placeholders})",
                hashes,
            )
            conn.execute(
                f"DELETE FROM commits WHERE hash IN ({placeholders})", hashes
            )
            deleted = len(hashes)
            all_orphans = self._find_orphan_objects(conn, candidates) if candidates else []
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        if all_orphans:
            logger.info("orphan objects cleaned count=%d", len(all_orphans))
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
