"""镜像服启停编排。

v0.1 只有 ``manual`` 适配器：**只检查，不操作**。检查方式是尝试对镜像世界目录里的
``session.lock`` 加锁——服务端运行时 Java 会独占这个文件的锁，抢不到就说明它还活着。

`rcon` / `external` / `mcsm` 适配器在 v0.2 接入；当前版本遇到这些配置会退化为 manual
并在启动时给出警告。
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

SESSION_LOCK = "session.lock"


def _probe_lock(path: Path) -> bool:
    """返回 True 表示能拿到锁（= 没有进程持有），False 表示被占用或无法判断。"""
    try:
        fd = os.open(str(path), os.O_RDWR)
    except FileNotFoundError:
        return True
    except OSError:
        return False

    try:
        if os.name == "nt":
            import msvcrt

            try:
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            except OSError:
                return False
            try:
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
            return True
        else:
            import fcntl

            try:
                fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return False
            try:
                fcntl.lockf(fd, fcntl.LOCK_UN)
            except OSError:
                pass
            return True
    finally:
        os.close(fd)


def find_session_locks(root: Path, max_depth: int = 2) -> List[Path]:
    """在 set 目标根下有限深度地找 session.lock（每个维度一个世界目录）。"""
    root = Path(root)
    found: List[Path] = []
    if not root.exists():
        return found
    base_depth = len(root.parts)
    for dirpath, dirnames, filenames in os.walk(root):
        current = Path(dirpath)
        depth = len(current.parts) - base_depth
        if depth >= max_depth:
            dirnames[:] = []
        if SESSION_LOCK in filenames:
            found.append(current / SESSION_LOCK)
    return found


class ManualMirrorControl:
    """只检查镜像是否已停止；不提供 stop/start。"""

    name = "manual"

    def __init__(self, target_roots: Iterable[Path], max_depth: int = 2):
        self.target_roots = [Path(p) for p in target_roots]
        self.max_depth = max_depth

    def check(self) -> Tuple[bool, str]:
        """返回 (是否已停止, 说明)。"""
        locks: List[Path] = []
        for root in self.target_roots:
            locks.extend(find_session_locks(root, self.max_depth))

        if not locks:
            return True, "未发现 session.lock，视为镜像未运行"

        busy: List[Path] = [lock for lock in locks if not _probe_lock(lock)]
        if busy:
            shown = ", ".join(str(p) for p in busy[:3])
            more = "" if len(busy) <= 3 else f" 等 {len(busy)} 个"
            return False, f"镜像仍在运行（session.lock 被占用：{shown}{more}），请先关闭镜像服"
        return True, f"已确认镜像未运行（检查了 {len(locks)} 个 session.lock）"

    def start(self) -> bool:  # pragma: no cover - v0.2
        return False

    def wait_ready(self, timeout: float) -> bool:  # pragma: no cover - v0.2
        return False


def build_mirror_control(adapter: str, target_roots: Iterable[Path]) -> ManualMirrorControl:
    # v0.1：无论配置写什么，都用 manual
    return ManualMirrorControl(target_roots)
