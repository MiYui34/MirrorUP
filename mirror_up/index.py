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

import logging
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .constants import INDEX_BATCH_ROWS, INDEX_BATCH_SECONDS

logger = logging.getLogger("mirror_up")

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS targets (
  id   INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  root TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sets (
  id         INTEGER PRIMARY KEY,
  name       TEXT NOT NULL UNIQUE,
  source_rel TEXT NOT NULL,
  target_rel TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
  target_id  INTEGER NOT NULL,
  set_id     INTEGER NOT NULL,
  path       TEXT    NOT NULL,
  kind       INTEGER NOT NULL,
  size       INTEGER NOT NULL,
  mtime_ns   INTEGER NOT NULL,
  hash       BLOB,
  region     INTEGER NOT NULL,
  synced_at  INTEGER NOT NULL,
  PRIMARY KEY (target_id, set_id, path)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS regions (
  target_id    INTEGER NOT NULL,
  set_id       INTEGER NOT NULL,
  path         TEXT    NOT NULL,
  chunk_hashes BLOB    NOT NULL,
  header       BLOB,
  -- 上一次成功写入这个 region 的那一轮的开始时间（Unix 秒）。
  -- 下一轮用它当脏扫描下限：写失败/被取消的 region 不会把下限推过去，
  -- 否则那些 chunk 会被当成“没变”而永久漏同步。
  floor        INTEGER,
  PRIMARY KEY (target_id, set_id, path)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS pending (
  target_id INTEGER NOT NULL,
  set_id    INTEGER NOT NULL,
  path      TEXT    NOT NULL,
  reason    TEXT    NOT NULL,
  at        INTEGER NOT NULL,
  PRIMARY KEY (target_id, set_id, path)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS history (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  target     TEXT    NOT NULL,
  trigger    TEXT    NOT NULL,
  started_at INTEGER NOT NULL,
  finished_at INTEGER NOT NULL,
  ok         INTEGER NOT NULL,
  dry_run    INTEGER NOT NULL,
  applied    INTEGER NOT NULL,
  deleted    INTEGER NOT NULL,
  deferred   INTEGER NOT NULL,
  failed     INTEGER NOT NULL,
  bytes_written INTEGER NOT NULL,
  duration   REAL    NOT NULL,
  summary    TEXT    NOT NULL,
  detail     TEXT
);

CREATE INDEX IF NOT EXISTS idx_pending_set ON pending(target_id, set_id);
CREATE INDEX IF NOT EXISTS idx_history_time ON history(finished_at);
"""

#: 兼容 v0.4 及更早的单一目标索引：老数据全部归到这个目标下。
DEFAULT_TARGET_ID = 1

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
            self._migrate()
            self._conn.execute(
                "INSERT OR IGNORE INTO targets(id, name, root) VALUES(?, ?, ?)",
                (DEFAULT_TARGET_ID, "default", ""),
            )

    def _migrate(self) -> None:
        """v0.4 及更早的索引没有 target_id / targets / history，就地升级。"""
        columns = {row[1] for row in self._conn.execute("PRAGMA table_info(files)")}
        if "target_id" in columns:
            self._migrate_region_floor()
            return
        logger.info("[MirrorUP] 索引升级到 schema v5（多镜像目标）")
        self._conn.executescript(
            """
            ALTER TABLE files   RENAME TO files_v4;
            ALTER TABLE regions RENAME TO regions_v4;
            ALTER TABLE pending RENAME TO pending_v4;
            """
        )
        self._conn.executescript(SCHEMA)
        self._conn.execute("INSERT OR IGNORE INTO targets(id, name, root) VALUES(?, 'default', '')", (DEFAULT_TARGET_ID,))
        self._conn.execute(
            "INSERT INTO files(target_id, set_id, path, kind, size, mtime_ns, hash, region, synced_at) "
            "SELECT ?, set_id, path, kind, size, mtime_ns, hash, region, synced_at FROM files_v4",
            (DEFAULT_TARGET_ID,),
        )
        self._conn.execute(
            "INSERT INTO regions(target_id, set_id, path, chunk_hashes, header) "
            "SELECT ?, set_id, path, chunk_hashes, header FROM regions_v4",
            (DEFAULT_TARGET_ID,),
        )
        self._conn.execute(
            "INSERT INTO pending(target_id, set_id, path, reason, at) "
            "SELECT ?, set_id, path, reason, at FROM pending_v4",
            (DEFAULT_TARGET_ID,),
        )
        self._conn.executescript(
            "DROP TABLE files_v4; DROP TABLE regions_v4; DROP TABLE pending_v4;"
        )
        self._migrate_region_floor()

    def _migrate_region_floor(self) -> None:
        """v0.6 及更早的 regions 表没有 floor 列。"""
        columns = {row[1] for row in self._conn.execute("PRAGMA table_info(regions)")}
        if "floor" in columns:
            return
        logger.info("[MirrorUP] 索引升级到 schema v6（region 脏扫描下限）")
        self._conn.execute("ALTER TABLE regions ADD COLUMN floor INTEGER")

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
            existing = self._conn.execute(
                "SELECT id FROM sets WHERE name = ?", (name,)
            ).fetchone()
            if existing is None:
                # set 改名复用：源/目标相对路径都没变，只换了名字。把旧行改成新名字，
                # 这样 files/regions/pending 里的 set_id 全部保留，下一轮不用重扫。
                row = self._conn.execute(
                    "SELECT id FROM sets WHERE source_rel = ? AND target_rel = ? "
                    "ORDER BY id LIMIT 1",
                    (source_rel, target_rel),
                ).fetchone()
                if row is not None:
                    self._conn.execute(
                        "UPDATE sets SET name = ? WHERE id = ?", (name, int(row[0]))
                    )
                    self._after_write()
                    logger.info("[MirrorUP] set 改名复用索引：%r（id=%s）", name, row[0])
                    return int(row[0])
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

    # ------------------------------------------------------------------ #
    # targets
    # ------------------------------------------------------------------ #

    def upsert_target(self, name: str, root: str) -> int:
        with self._lock:
            self._begin_locked()
            self._conn.execute(
                "INSERT INTO targets(name, root) VALUES(?, ?) "
                "ON CONFLICT(name) DO UPDATE SET root = excluded.root",
                (name, root),
            )
            row = self._conn.execute("SELECT id FROM targets WHERE name = ?", (name,)).fetchone()
            self._after_write()
            if row is None:
                raise IndexError_(f"写入 target {name!r} 失败")
            return int(row[0])

    def load_targets(self) -> Dict[str, int]:
        with self._lock:
            rows = self._conn.execute("SELECT name, id FROM targets").fetchall()
        return {name: int(tid) for name, tid in rows}

    # ------------------------------------------------------------------ #
    # files
    # ------------------------------------------------------------------ #

    def load_files(
        self, set_id: int, target_id: int = DEFAULT_TARGET_ID
    ) -> Dict[str, FileRow]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT path, kind, size, mtime_ns, hash, region, synced_at FROM files "
                "WHERE target_id = ? AND set_id = ?",
                (target_id, set_id),
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

    def put_file(self, set_id: int, row: FileRow, target_id: int = DEFAULT_TARGET_ID) -> None:
        with self._lock:
            self._begin_locked()
            self._conn.execute(
                "INSERT INTO files(target_id, set_id, path, kind, size, mtime_ns, hash, region, synced_at) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(target_id, set_id, path) DO UPDATE SET kind = excluded.kind, "
                "size = excluded.size, mtime_ns = excluded.mtime_ns, hash = excluded.hash, "
                "region = excluded.region, synced_at = excluded.synced_at",
                (
                    target_id,
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

    def delete_file(self, set_id: int, path: str, target_id: int = DEFAULT_TARGET_ID) -> None:
        with self._lock:
            self._begin_locked()
            self._conn.execute(
                "DELETE FROM files WHERE target_id = ? AND set_id = ? AND path = ?",
                (target_id, set_id, path),
            )
            self._conn.execute(
                "DELETE FROM regions WHERE target_id = ? AND set_id = ? AND path = ?",
                (target_id, set_id, path),
            )
            self._after_write()

    def count_files(self, set_id: int, target_id: int = DEFAULT_TARGET_ID) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM files WHERE target_id = ? AND set_id = ?",
                (target_id, set_id),
            ).fetchone()
        return int(row[0]) if row else 0

    def total_stats(self, target_id: Optional[int] = None) -> Tuple[int, int]:
        """返回 (文件数, 总字节数)；``target_id=None`` 表示全部目标。"""
        with self._lock:
            if target_id is None:
                row = self._conn.execute(
                    "SELECT COUNT(*), COALESCE(SUM(size), 0) FROM files WHERE kind = 0"
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT COUNT(*), COALESCE(SUM(size), 0) FROM files "
                    "WHERE kind = 0 AND target_id = ?",
                    (target_id,),
                ).fetchone()
        return (int(row[0]), int(row[1])) if row else (0, 0)

    # ------------------------------------------------------------------ #
    # regions
    # ------------------------------------------------------------------ #

    def get_region(
        self, set_id: int, path: str, target_id: int = DEFAULT_TARGET_ID
    ) -> Optional[Tuple[bytes, Optional[bytes], Optional[int]]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT chunk_hashes, header, floor FROM regions "
                "WHERE target_id = ? AND set_id = ? AND path = ?",
                (target_id, set_id, path),
            ).fetchone()
        if row is None:
            return None
        return (
            bytes(row[0]),
            (bytes(row[1]) if row[1] is not None else None),
            (int(row[2]) if row[2] is not None else None),
        )

    def put_region(
        self,
        set_id: int,
        path: str,
        chunk_hashes: bytes,
        header: Optional[bytes],
        target_id: int = DEFAULT_TARGET_ID,
        floor: Optional[int] = None,
    ) -> None:
        with self._lock:
            self._begin_locked()
            self._conn.execute(
                "INSERT INTO regions(target_id, set_id, path, chunk_hashes, header, floor) "
                "VALUES(?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(target_id, set_id, path) DO UPDATE SET "
                "chunk_hashes = excluded.chunk_hashes, header = excluded.header, "
                "floor = COALESCE(excluded.floor, regions.floor)",
                (target_id, set_id, path, chunk_hashes, header, floor),
            )
            self._after_write()

    def delete_region(
        self, set_id: int, path: str, target_id: int = DEFAULT_TARGET_ID
    ) -> None:
        """忘掉一个 region 的 chunk 状态（下一轮会整份重扫）。"""
        with self._lock:
            self._begin_locked()
            self._conn.execute(
                "DELETE FROM regions WHERE target_id = ? AND set_id = ? AND path = ?",
                (target_id, set_id, path),
            )
            self._after_write()

    # ------------------------------------------------------------------ #
    # pending
    # ------------------------------------------------------------------ #

    def put_pending(
        self,
        set_id: int,
        path: str,
        reason: str,
        at: int,
        target_id: int = DEFAULT_TARGET_ID,
    ) -> None:
        with self._lock:
            self._begin_locked()
            self._conn.execute(
                "INSERT INTO pending(target_id, set_id, path, reason, at) VALUES(?, ?, ?, ?, ?) "
                "ON CONFLICT(target_id, set_id, path) DO UPDATE SET "
                "reason = excluded.reason, at = excluded.at",
                (target_id, set_id, path, reason, at),
            )
            self._after_write()

    def clear_pending(self, set_id: int, path: str, target_id: int = DEFAULT_TARGET_ID) -> None:
        with self._lock:
            self._begin_locked()
            self._conn.execute(
                "DELETE FROM pending WHERE target_id = ? AND set_id = ? AND path = ?",
                (target_id, set_id, path),
            )
            self._after_write()

    def purge_pending(self, valid_paths: Dict[Tuple[int, int], set]) -> None:
        """清掉已经不再需要的 pending 行（文件已同步/已消失）。"""
        with self._lock:
            rows = self._conn.execute("SELECT target_id, set_id, path FROM pending").fetchall()
            stale = [
                (tid, sid, path)
                for tid, sid, path in rows
                if path not in valid_paths.get((int(tid), int(sid)), set())
            ]
            if not stale:
                return
            self._begin_locked()
            self._conn.executemany(
                "DELETE FROM pending WHERE target_id = ? AND set_id = ? AND path = ?", stale
            )
            self._commit_locked()

    def list_pending(
        self, set_id: Optional[int] = None, target_id: Optional[int] = None
    ) -> List[Tuple[int, int, str, str, int]]:
        """返回 ``[(target_id, set_id, path, reason, at), ...]``。"""
        clauses: List[str] = []
        params: List[object] = []
        if set_id is not None:
            clauses.append("set_id = ?")
            params.append(set_id)
        if target_id is not None:
            clauses.append("target_id = ?")
            params.append(target_id)
        sql = "SELECT target_id, set_id, path, reason, at FROM pending"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        with self._lock:
            rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [(int(a), int(b), c, d, int(e)) for a, b, c, d, e in rows]

    # ------------------------------------------------------------------ #
    # history
    # ------------------------------------------------------------------ #

    def add_history(self, record: Dict[str, object]) -> None:
        with self._lock:
            self._begin_locked()
            self._conn.execute(
                "INSERT INTO history(target, trigger, started_at, finished_at, ok, dry_run, "
                "applied, deleted, deferred, failed, bytes_written, duration, summary, detail) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    record.get("target", ""),
                    record.get("trigger", ""),
                    int(record.get("started_at", 0)),
                    int(record.get("finished_at", 0)),
                    1 if record.get("ok") else 0,
                    1 if record.get("dry_run") else 0,
                    int(record.get("applied", 0)),
                    int(record.get("deleted", 0)),
                    int(record.get("deferred", 0)),
                    int(record.get("failed", 0)),
                    int(record.get("bytes_written", 0)),
                    float(record.get("duration", 0.0)),
                    str(record.get("summary", "")),
                    record.get("detail"),
                ),
            )
            self._after_write()

    def list_history(self, limit: int = 20) -> List[Dict[str, object]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, target, trigger, started_at, finished_at, ok, dry_run, applied, "
                "deleted, deferred, failed, bytes_written, duration, summary "
                "FROM history ORDER BY id DESC LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
        keys = (
            "id", "target", "trigger", "started_at", "finished_at", "ok", "dry_run",
            "applied", "deleted", "deferred", "failed", "bytes_written", "duration", "summary",
        )
        return [dict(zip(keys, row)) for row in rows]

    def prune_history(self, keep: int) -> None:
        with self._lock:
            self._begin_locked()
            self._conn.execute(
                "DELETE FROM history WHERE id NOT IN "
                "(SELECT id FROM history ORDER BY id DESC LIMIT ?)",
                (max(1, int(keep)),),
            )
            self._after_write()

    # ------------------------------------------------------------------ #
    # 维护
    # ------------------------------------------------------------------ #

    def wipe(self) -> None:
        """清空同步状态（索引重建）；保留历史与目标登记。"""
        with self._lock:
            self._conn.executescript(
                "DELETE FROM pending; DELETE FROM regions; DELETE FROM files; DELETE FROM sets; DELETE FROM meta;"
            )
