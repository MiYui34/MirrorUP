"""目标端落地（副本 / 原子替换 / 复验 / 重试）。

每个文件的写入流程是：

    stat(源) → 拷贝到同目录临时文件（边拷边算哈希）→ stat(源) 复核
      ├ 不一致 → 视为不稳定，删临时文件，退避重试
      └ 一致   → 哈希与索引相同？→ 内容没变，丢弃临时文件（不碰目标）
                            └ 不同 → fsync → os.replace → fsync 目录

``os.replace`` 是原子操作，所以目标文件永远要么是旧的完整版本、要么是新的完整版本，
不存在“写了一半”的可见状态。这是不做备份也能保证镜像不坏的前提。

注意：本类不持有任何跨调用的可变状态，可以被多个线程同时用于不同的文件。
"""

from __future__ import annotations

import errno
import logging
import os
import secrets
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, Tuple

from .config import SyncConfig
from .constants import IO_CHUNK, PENDING_BUSY, PENDING_UNSTABLE, PENDING_VANISHED
from .hashing import new_hasher
from .planner import Op, OpKind
from .region import (
    RegionExternalChunk,
    RegionFormatError,
    RegionUnstable,
    decode_state,
    encode_state,
    logical_digest,
    scan_region,
    write_region_delta,
    write_region_inplace,
)

logger = logging.getLogger("mirror_up")


class UnstableSource(Exception):
    """源文件在读取期间被改写。"""


class SourceVanished(Exception):
    """源文件在读取前/期间消失。"""


class TargetRetryable(Exception):
    """目标端暂时失败（被占用、共享冲突），可重试。"""


@dataclass
class ApplyResult:
    op: Op
    status: str  # applied / unchanged / deleted / deferred / failed
    digest: Optional[bytes] = None
    size: int = 0
    mtime_ns: int = 0
    detail: str = ""
    pending_reason: str = ""
    #: chunk 级写入后要落进索引的 region 状态 blob
    region_state: Optional[bytes] = None
    #: 实际写盘的字节数（-1 表示“就是文件大小”）
    physical: int = -1
    chunks_written: int = 0
    chunks_reused: int = 0


def _fsync_dir(path: Path) -> None:
    """尽力 fsync 目录项。Windows 上打开目录会失败，忽略即可（NTFS 日志能兜住）。"""
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _safe_join(root: Path, rel: str) -> Path:
    if rel.startswith("/") or ".." in rel.split("/"):
        raise ValueError(f"非法相对路径：{rel!r}")
    return root.joinpath(*rel.split("/"))


_RETRYABLE_ERRNOS = {
    errno.EACCES,
    errno.EPERM,
    errno.EBUSY,
    errno.EAGAIN,
    errno.ETXTBSY,
    getattr(errno, "EDEADLK", errno.EACCES),
}


class Applier:
    """单个 set 的写入器。"""

    def __init__(
        self,
        set_name: str,
        source_root: Path,
        target_root: Path,
        sync: SyncConfig,
        sleep: Callable[[float], None] = time.sleep,
        journal_dir: Optional[Path] = None,
    ):
        self.set_name = set_name
        self.source_root = Path(source_root)
        self.target_root = Path(target_root)
        self.sync = sync
        self._sleep = sleep
        self.journal_dir = Path(journal_dir) if journal_dir is not None else None

    # ------------------------------------------------------------------ #
    # 入口
    # ------------------------------------------------------------------ #

    def apply(self, op: Op) -> ApplyResult:
        try:
            if op.kind == OpKind.DELETE:
                return self._delete(op)
            if op.kind == OpKind.RMDIR:
                return self._rmdir(op)
            if op.kind == OpKind.MKDIR:
                return self._mkdir(op)
            if op.region and self.sync.region_delta:
                return self._write_region(op)
            return self._write(op)
        except ValueError as exc:
            return ApplyResult(op=op, status="failed", detail=str(exc))

    # ------------------------------------------------------------------ #
    # 写：region chunk 级
    # ------------------------------------------------------------------ #

    def _write_region(self, op: Op) -> ApplyResult:
        src = _safe_join(self.source_root, op.path)
        dst = _safe_join(self.target_root, op.path)
        previous = decode_state(op.region_state)
        attempts = max(1, self.sync.max_retry)
        last_detail = ""
        pending_reason = PENDING_UNSTABLE

        for attempt in range(1, attempts + 1):
            try:
                scan = scan_region(src, dirty_since=op.dirty_since, previous=previous)
                logical = logical_digest(scan.chunks)

                if (
                    not op.force_write
                    and op.old_hash is not None
                    and logical == op.old_hash
                    and dst.exists()
                ):
                    return ApplyResult(
                        op=op,
                        status="unchanged",
                        digest=logical,
                        size=scan.size,
                        mtime_ns=scan.mtime_ns,
                        detail="chunk 内容未变",
                        region_state=encode_state(scan.digests()),
                    )

                if self.sync.region_write_mode == "inplace":
                    result = write_region_inplace(
                        scan,
                        dst,
                        previous=previous,
                        force_full=op.force_write,
                        compaction_ratio=self.sync.region_compaction_ratio,
                        journal_dir=self.journal_dir,
                        rel_path=op.path,
                    )
                else:
                    result = write_region_delta(
                        scan,
                        dst,
                        previous=previous,
                        force_full=op.force_write,
                    )
                return ApplyResult(
                    op=op,
                    status="applied",
                    digest=result.logical,
                    size=scan.size,
                    mtime_ns=scan.mtime_ns,
                    detail=(
                        f"{result.chunks_written} chunk 写入 / {result.chunks_reused} 复用"
                        + ("（碎片整理）" if result.compacted else "")
                    ),
                    region_state=result.state,
                    physical=result.written_bytes,
                    chunks_written=result.chunks_written,
                    chunks_reused=result.chunks_reused,
                )
            except RegionExternalChunk as exc:
                self._log_fallback(op, str(exc))
                return self._write(op)
            except RegionFormatError as exc:
                self._log_fallback(op, str(exc))
                return self._write(op)
            except (RegionUnstable, SourceVanished) as exc:
                last_detail = str(exc)
                pending_reason = PENDING_UNSTABLE
                if attempt < attempts:
                    self._sleep(0.2 * attempt)
                    continue
            except TargetRetryable as exc:
                last_detail = str(exc)
                pending_reason = PENDING_BUSY
                if attempt < attempts:
                    self._sleep(0.2 * attempt)
                    continue
            except OSError as exc:
                if exc.errno in _RETRYABLE_ERRNOS and attempt < attempts:
                    last_detail = f"{type(exc).__name__}: {exc}"
                    pending_reason = PENDING_BUSY
                    self._sleep(0.2 * attempt)
                    continue
                return ApplyResult(op=op, status="failed", detail=f"{type(exc).__name__}: {exc}")

        if self.sync.defer_unstable:
            return ApplyResult(
                op=op,
                status="deferred",
                detail=last_detail or "重试耗尽",
                pending_reason=pending_reason,
            )
        return ApplyResult(op=op, status="failed", detail=last_detail or "重试耗尽")

    def _log_fallback(self, op: Op, reason: str) -> None:
        logger.warning("[MirrorUP] region %s 降级为整文件复制：%s", op.path, reason)

    # ------------------------------------------------------------------ #
    # 写
    # ------------------------------------------------------------------ #

    def _write(self, op: Op) -> ApplyResult:
        src = _safe_join(self.source_root, op.path)
        dst = _safe_join(self.target_root, op.path)
        last_detail = ""
        pending_reason = PENDING_UNSTABLE
        attempts = max(1, self.sync.max_retry)

        for attempt in range(1, attempts + 1):
            tmp: Optional[Path] = None
            try:
                tmp, digest, size, mtime_ns = self._copy_to_temp(src, dst)

                # 内容与索引一致、且目标本身没被改过：不写目标，只更新索引元数据
                if not op.force_write and op.old_hash is not None and digest == op.old_hash:
                    self._unlink_quiet(tmp)
                    return ApplyResult(
                        op=op,
                        status="unchanged",
                        digest=digest,
                        size=size,
                        mtime_ns=mtime_ns,
                        detail="内容未变",
                    )

                try:
                    os.replace(tmp, dst)
                except OSError as exc:
                    self._unlink_quiet(tmp)
                    raise TargetRetryable(f"替换目标文件失败：{exc}") from exc
                _fsync_dir(dst.parent)
                return ApplyResult(
                    op=op, status="applied", digest=digest, size=size, mtime_ns=mtime_ns
                )

            except SourceVanished:
                return ApplyResult(
                    op=op,
                    status="deferred",
                    detail="源文件在同步过程中消失",
                    pending_reason=PENDING_VANISHED,
                )
            except UnstableSource as exc:
                last_detail = str(exc)
                pending_reason = PENDING_UNSTABLE
                self._unlink_quiet(tmp)
                if attempt < attempts:
                    self._sleep(0.2 * attempt)
                    continue
            except TargetRetryable as exc:
                last_detail = str(exc)
                pending_reason = PENDING_BUSY
                self._unlink_quiet(tmp)
                if attempt < attempts:
                    self._sleep(0.2 * attempt)
                    continue
            except OSError as exc:
                self._unlink_quiet(tmp)
                if exc.errno in _RETRYABLE_ERRNOS and attempt < attempts:
                    last_detail = f"{type(exc).__name__}: {exc}"
                    pending_reason = PENDING_BUSY
                    self._sleep(0.2 * attempt)
                    continue
                return ApplyResult(op=op, status="failed", detail=f"{type(exc).__name__}: {exc}")

        if self.sync.defer_unstable:
            return ApplyResult(
                op=op,
                status="deferred",
                detail=last_detail or "重试耗尽",
                pending_reason=pending_reason,
            )
        return ApplyResult(op=op, status="failed", detail=last_detail or "重试耗尽")

    def _copy_to_temp(self, src: Path, dst: Path) -> Tuple[Path, bytes, int, int]:
        try:
            st0 = os.stat(src)
        except FileNotFoundError as exc:
            raise SourceVanished() from exc

        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = dst.parent / f".{dst.name}.mirrorup-{secrets.token_hex(4)}.tmp"
        hasher = new_hasher()
        written = 0

        try:
            with open(src, "rb") as fin, open(tmp, "wb") as fout:
                while True:
                    try:
                        block = fin.read(IO_CHUNK)
                    except OSError as exc:
                        raise SourceVanished(f"读取源文件失败：{exc}") from exc
                    if not block:
                        break
                    hasher.update(block)
                    fout.write(block)
                    written += len(block)
                fout.flush()
                os.fsync(fout.fileno())
        except BaseException:
            self._unlink_quiet(tmp)
            raise

        try:
            st1 = os.stat(src)
        except FileNotFoundError as exc:
            self._unlink_quiet(tmp)
            raise SourceVanished() from exc

        if (
            written != st0.st_size
            or st1.st_size != st0.st_size
            or st1.st_mtime_ns != st0.st_mtime_ns
        ):
            self._unlink_quiet(tmp)
            raise UnstableSource(
                f"源文件在读取期间被改写（{st0.st_size}B@{st0.st_mtime_ns} → "
                f"{st1.st_size}B@{st1.st_mtime_ns}，实际读 {written}B）"
            )

        return tmp, hasher.digest(), written, st1.st_mtime_ns

    @staticmethod
    def _unlink_quiet(path: Optional[Path]) -> None:
        if path is None:
            return
        try:
            os.unlink(path)
        except OSError:
            pass

    # ------------------------------------------------------------------ #
    # 删
    # ------------------------------------------------------------------ #

    def _delete(self, op: Op) -> ApplyResult:
        dst = _safe_join(self.target_root, op.path)
        try:
            os.unlink(dst)
        except FileNotFoundError:
            pass
        except OSError as exc:
            return ApplyResult(op=op, status="failed", detail=f"删除失败：{exc}")
        self._prune_empty_dirs(dst.parent)
        return ApplyResult(op=op, status="deleted")

    def _rmdir(self, op: Op) -> ApplyResult:
        dst = _safe_join(self.target_root, op.path)
        try:
            os.rmdir(dst)
        except OSError:
            # 非空（里面还有要保留的东西）或不存在：都不是错误
            pass
        return ApplyResult(op=op, status="deleted")

    def _mkdir(self, op: Op) -> ApplyResult:
        dst = _safe_join(self.target_root, op.path)
        try:
            dst.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            return ApplyResult(op=op, status="failed", detail=f"创建目录失败：{exc}")
        return ApplyResult(op=op, status="applied")

    def _prune_empty_dirs(self, directory: Path) -> None:
        """删完文件后顺手清掉空目录，但不越过 set 根。"""
        root = self.target_root
        current = directory
        while True:
            if current == root or root not in current.parents:
                return
            try:
                os.rmdir(current)
            except OSError:
                return
            current = current.parent
