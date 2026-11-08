"""跨轮索引（SQLite）。

不变式（DESIGN §7.3）：

    fsync(数据文件) → 原子替换 → fsync(目录) → SQLite 提交

索引**只能落后，不能超前**。因此本模块不做任何“预写”：所有写入都由调用方在
目标文件已经落盘之后发起；崩溃时索引最多丢掉最后一个批次，下一轮按 stat/哈希
重新确认，结果幂等。

并发模型：连接开在 ``check_same_thread=False``，用一把可重入锁串行化所有访问。
拷贝线程只把结果交回主线程，索引写入始终单线程发生。
"""

from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .constants import INDEX_BATCH_ROWS, INDEX_BATCH_SECONDS

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sets (
  id         INTEGER PRIMARY KEY,
  name       TEXT NOT NULL UNIQUE,
  source_rel TEXT NOT NULL,
  target_rel TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
  set_id     INTEGER NOT NULL,
  path       TEXT    NOT NULL,
  kind       INTEGER NOT NULL,
  size       INTEGER NOT NULL,
  mtime_ns   INTEGER NOT NULL,
  hash       BLOB,
  region     INTEGER NOT NULL,
  synced_at  INTEGER NOT NULL,
  PRIMARY KEY (set_id, path)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS regions (
  set_id       INTEGER NOT NULL,
  path         TEXT    NOT NULL,
  chunk_hashes BLOB    NOT NULL,
  header       BLOB,
  PRIMARY KEY (set_id, path)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS pending (
  set_id INTEGER NOT NULL,
  path   TEXT    NOT NULL,
  reason TEXT    NOT NULL,
  at     INTEGER NOT NULL,
  PRIMARY KEY (set_id, path)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_pending_set ON pending(set_id);
"""

KIND_FILE = 0
KIND_DIR = 1


class IndexError_(Exception):
    """索引层错误。"""


class WorldIdMismatch(IndexError_):
    """索引属于另一个世界。"""


@dataclass
class FileRow:
    path: str
    kind: int = KIND_FILE
    size: int = 0
    mtime_ns: int = 0
    hash: Optional[bytes] = None
    region: bool = False
    synced_at: int = 0


class Index:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self.path), isolation_level=None, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA temp_store=MEMORY")
        self._in_tx = False
        self._ops = 0
        self._last_commit = time.monotonic()
        self._ensure_schema()

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #

    def _ensure_schema(self) -> None:
        with self._lock:
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            if self._in_tx:
                self._commit_locked()
            self._conn.close()

    def __enter__(self) -> "Index":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()

    # ------------------------------------------------------------------ #
    # 事务
    # ------------------------------------------------------------------ #

    def begin(self) -> None:
        with self._lock:
            self._begin_locked()

    def _begin_locked(self) -> None:
        if self._in_tx:
            return
        self._conn.execute("BEGIN IMMEDIATE")
        self._in_tx = True
        self._ops = 0
        self._last_commit = time.monotonic()

    def commit(self) -> None:
        with self._lock:
            self._commit_locked()

    def _commit_locked(self) -> None:
        if not self._in_tx:
            return
        self._conn.execute("COMMIT")
        self._in_tx = False
        self._ops = 0
        self._last_commit = time.monotonic()

    def rollback(self) -> None:
        with self._lock:
            if self._in_tx:
                self._conn.execute("ROLLBACK")
                self._in_tx = False
                self._ops = 0

    def _after_write(self) -> None:
        """批量提交：条数或秒数先到者。调用方须已持有锁。"""
        self._ops += 1
        if self._ops >= INDEX_BATCH_ROWS or (time.monotonic() - self._last_commit) >= INDEX_BATCH_SECONDS:
            self._commit_locked()
            self._begin_locked()

    # ------------------------------------------------------------------ #
    # meta
    # ------------------------------------------------------------------ #

    def get_meta(self, key: str, default: Optional[str] = None) -> Optional[str]:
        with self._lock:
            row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return default if row is None else row[0]

    def get_meta_int(self, key: str, default: int = 0) -> int:
        raw = self.get_meta(key)
        try:
            return int(raw) if raw is not None else default
        except (TypeError, ValueError):
            return default

    def set_meta(self, key: str, value: object) -> None:
        with self._lock:
            self._begin_locked()
            self._conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, str(value)),
            )
            self._after_write()

    # ------------------------------------------------------------------ #
    # world_id
    # ------------------------------------------------------------------ #

    def bind_world(self, world_id: str) -> None:
        """首次绑定；已绑定且不同则抛 WorldIdMismatch。"""
        with self._lock:
            current = self.get_meta("world_id")
            if current is None:
                self.set_meta("world_id", world_id)
                return
            if current != world_id:
                raise WorldIdMismatch(
                    f"索引记录的 world_id={current[:12]}… 与当前世界 {world_id[:12]}… 不一致；"
                    "请执行 !!mu index rebuild 重建索引"
                )

    def stored_world_id(self) -> Optional[str]:
        return self.get_meta("world_id")

    # ------------------------------------------------------------------ #
    # sets
    # ------------------------------------------------------------------ #

    def upsert_set(self, name: str, source_rel: str, target_rel: str) -> int:
        with self._lock:
            self._begin_locked()
            self._conn.execute(
                "INSERT INTO sets(name, source_rel, target_rel) VALUES(?, ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET source_rel = excluded.source_rel, "
                "target_rel = excluded.target_rel",
                (name, source_rel, target_rel),
            )
            row = self._conn.execute("SELECT id FROM sets WHERE name = ?", (name,)).fetchone()
            self._after_write()
            if row is None:
                raise IndexError_(f"写入 set {name!r} 失败")
            return int(row[0])

    def load_set_ids(self) -> Dict[str, int]:
        with self._lock:
            rows = self._conn.execute("SELECT name, id FROM sets").fetchall()
        return {name: int(sid) for name, sid in rows}

    # ------------------------------------------------------------------ #
    # files
    # ------------------------------------------------------------------ #

    def load_files(self, set_id: int) -> Dict[str, FileRow]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT path, kind, size, mtime_ns, hash, region, synced_at FROM files WHERE set_id = ?",
                (set_id,),
            ).fetchall()
        result: Dict[str, FileRow] = {}
        for path, kind, size, mtime_ns, digest, region, synced_at in rows:
            result[path] = FileRow(
                path=path,
                kind=int(kind),
                size=int(size),
                mtime_ns=int(mtime_ns),
                hash=bytes(digest) if digest is not None else None,
                region=bool(region),
                synced_at=int(synced_at),
            )
        return result

    def put_file(self, set_id: int, row: FileRow) -> None:
        with self._lock:
            self._begin_locked()
            self._conn.execute(
                "INSERT INTO files(set_id, path, kind, size, mtime_ns, hash, region, synced_at) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(set_id, path) DO UPDATE SET kind = excluded.kind, size = excluded.size, "
                "mtime_ns = excluded.mtime_ns, hash = excluded.hash, region = excluded.region, "
                "synced_at = excluded.synced_at",
                (
                    set_id,
                    row.path,
                    int(row.kind),
                    int(row.size),
                    int(row.mtime_ns),
                    row.hash,
                    1 if row.region else 0,
                    int(row.synced_at),
                ),
            )
            self._after_write()

    def delete_file(self, set_id: int, path: str) -> None:
        with self._lock:
            self._begin_locked()
            self._conn.execute("DELETE FROM files WHERE set_id = ? AND path = ?", (set_id, path))
            self._conn.execute("DELETE FROM regions WHERE set_id = ? AND path = ?", (set_id, path))
            self._after_write()

    def count_files(self, set_id: int) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) FROM files WHERE set_id = ?", (set_id,)).fetchone()
        return int(row[0]) if row else 0

    def total_stats(self) -> Tuple[int, int]:
        """返回 (文件数, 总字节数)。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(size), 0) FROM files WHERE kind = 0"
            ).fetchone()
        return (int(row[0]), int(row[1])) if row else (0, 0)

    # ------------------------------------------------------------------ #
    # regions（v0.2 使用；v0.1 仅保证表存在）
    # ------------------------------------------------------------------ #

    def get_region(self, set_id: int, path: str) -> Optional[Tuple[bytes, Optional[bytes]]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT chunk_hashes, header FROM regions WHERE set_id = ? AND path = ?",
                (set_id, path),
            ).fetchone()
        if row is None:
            return None
        return bytes(row[0]), (bytes(row[1]) if row[1] is not None else None)

    def put_region(self, set_id: int, path: str, chunk_hashes: bytes, header: Optional[bytes]) -> None:
        with self._lock:
            self._begin_locked()
            self._conn.execute(
                "INSERT INTO regions(set_id, path, chunk_hashes, header) VALUES(?, ?, ?, ?) "
                "ON CONFLICT(set_id, path) DO UPDATE SET chunk_hashes = excluded.chunk_hashes, "
                "header = excluded.header",
                (set_id, path, chunk_hashes, header),
            )
            self._after_write()

    # ------------------------------------------------------------------ #
    # pending
    # ------------------------------------------------------------------ #

    def put_pending(self, set_id: int, path: str, reason: str, at: int) -> None:
        with self._lock:
            self._begin_locked()
            self._conn.execute(
                "INSERT INTO pending(set_id, path, reason, at) VALUES(?, ?, ?, ?) "
                "ON CONFLICT(set_id, path) DO UPDATE SET reason = excluded.reason, at = excluded.at",
                (set_id, path, reason, at),
            )
            self._after_write()

    def clear_pending(self, set_id: int, path: str) -> None:
        with self._lock:
            self._begin_locked()
            self._conn.execute("DELETE FROM pending WHERE set_id = ? AND path = ?", (set_id, path))
            self._after_write()

    def purge_pending(self, valid_paths: Dict[int, set]) -> None:
        """清掉已经不再需要的 pending 行（文件已同步/已消失）。"""
        with self._lock:
            rows = self._conn.execute("SELECT set_id, path FROM pending").fetchall()
            stale = [(sid, path) for sid, path in rows if path not in valid_paths.get(int(sid), set())]
            if not stale:
                return
            self._begin_locked()
            self._conn.executemany("DELETE FROM pending WHERE set_id = ? AND path = ?", stale)
            self._commit_locked()

    def list_pending(self, set_id: Optional[int] = None) -> List[Tuple[int, str, str, int]]:
        with self._lock:
            if set_id is None:
                rows = self._conn.execute("SELECT set_id, path, reason, at FROM pending").fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT set_id, path, reason, at FROM pending WHERE set_id = ?", (set_id,)
                ).fetchall()
        return [(int(a), b, c, int(d)) for a, b, c, d in rows]

    # ------------------------------------------------------------------ #
    # 维护
    # ------------------------------------------------------------------ #

    def wipe(self) -> None:
        """清空全部数据（索引重建）。"""
        with self._lock:
            self._conn.executescript(
                "DELETE FROM pending; DELETE FROM regions; DELETE FROM files; DELETE FROM sets; DELETE FROM meta;"
            )
