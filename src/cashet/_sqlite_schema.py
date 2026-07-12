from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta

from cashet.models import Commit, ObjectRef, StorageTier, TaskDef, TaskStatus


def ensure_schema(conn: sqlite3.Connection) -> None:
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
    _migrate_last_accessed_at(conn)
    _migrate_retries(conn)
    _migrate_task_options(conn)
    _migrate_expires_at(conn)
    _migrate_ttl(conn)
    _migrate_claimed_at(conn)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS inline_objects (
            hash TEXT PRIMARY KEY,
            data BLOB NOT NULL
        )
    """)


def _migrate_last_accessed_at(conn: sqlite3.Connection) -> None:
    col_names = [r[1] for r in conn.execute("PRAGMA table_info(commits)").fetchall()]
    if "last_accessed_at" not in col_names:
        conn.execute(
            "ALTER TABLE commits ADD COLUMN last_accessed_at TEXT DEFAULT NULL"
        )
        conn.execute(
            "UPDATE commits SET last_accessed_at = created_at WHERE last_accessed_at IS NULL"
        )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_last_accessed_at ON commits(last_accessed_at)"
    )


def _migrate_retries(conn: sqlite3.Connection) -> None:
    col_names = [r[1] for r in conn.execute("PRAGMA table_info(commits)").fetchall()]
    if "retries" not in col_names:
        conn.execute(
            "ALTER TABLE commits ADD COLUMN retries INTEGER NOT NULL DEFAULT 0"
        )


def _migrate_task_options(conn: sqlite3.Connection) -> None:
    col_names = [r[1] for r in conn.execute("PRAGMA table_info(commits)").fetchall()]
    if "force" not in col_names:
        conn.execute(
            "ALTER TABLE commits ADD COLUMN force INTEGER NOT NULL DEFAULT 0"
        )
    if "timeout_seconds" not in col_names:
        conn.execute("ALTER TABLE commits ADD COLUMN timeout_seconds REAL")


def _migrate_expires_at(conn: sqlite3.Connection) -> None:
    col_names = [r[1] for r in conn.execute("PRAGMA table_info(commits)").fetchall()]
    if "expires_at" not in col_names:
        conn.execute("ALTER TABLE commits ADD COLUMN expires_at TEXT")


def _migrate_ttl(conn: sqlite3.Connection) -> None:
    col_names = [r[1] for r in conn.execute("PRAGMA table_info(commits)").fetchall()]
    if "ttl_seconds" not in col_names:
        conn.execute("ALTER TABLE commits ADD COLUMN ttl_seconds REAL")


def _migrate_claimed_at(conn: sqlite3.Connection) -> None:
    col_names = [r[1] for r in conn.execute("PRAGMA table_info(commits)").fetchall()]
    if "claimed_at" not in col_names:
        conn.execute(
            "ALTER TABLE commits ADD COLUMN claimed_at TEXT NOT NULL DEFAULT ''"
        )
        conn.execute(
            "UPDATE commits SET claimed_at = created_at WHERE claimed_at = ''"
        )


def put_commit_row(conn: sqlite3.Connection, commit: Commit, accessed_at: str) -> None:
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


def row_to_commit(row: sqlite3.Row) -> Commit:
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
