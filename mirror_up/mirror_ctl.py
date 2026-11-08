"""镜像服启停编排。

同步要求"写目标时镜像处于关闭状态"，插件有三种做法：

* ``manual``  —— 只检查，不操作。检查方式是尝试给镜像世界里的 ``session.lock``
  加锁：服务端运行时 Java 独占这个文件，抢不到就说明它还活着。
* ``rcon``    —— 通过 RCON 发 ``stop``，等端口/文件锁释放；可选地再拉起启动命令。
* ``external``—— 完全交给用户的命令：``stop_cmd`` / ``start_cmd`` / ``probe_cmd``。
  ``probe_cmd`` 退出码 0 表示"正在运行"。

无论哪种适配器，``stop()`` 都必须等到镜像真的退出才返回；等不到就返回失败，
让引擎中止本轮（宁可这轮不写，也不能对着运行中的服务端改区块）。
"""

from __future__ import annotations

import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from .config import ExternalConfig, MirrorControlConfig, RconConfig
from .rcon import RconClient, RconError

logger = logging.getLogger("mirror_up")

SESSION_LOCK = "session.lock"
PROBE_INTERVAL = 0.5


# --------------------------------------------------------------------------- #
# 基础探测
# --------------------------------------------------------------------------- #


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


def port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    import socket

    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except OSError:
        return False


def _run(cmd: str, timeout: float) -> Tuple[int, str]:
    try:
        completed = subprocess.run(
            cmd,
            shell=True,
            timeout=timeout,
            capture_output=True,
            text=True,
            errors="replace",
        )
    except subprocess.TimeoutExpired:
        return 124, f"命令超时：{cmd}"
    except OSError as exc:
        return 127, f"无法执行：{exc}"
    output = ((completed.stdout or "") + (completed.stderr or "")).strip()
    return completed.returncode, output


def _spawn(cmd: str) -> Tuple[bool, str]:
    """拉起一个长期运行的进程（不阻塞、不等它退出）。"""
    flags = 0
    if os.name == "nt":
        flags = getattr(subprocess, "DETACHED_PROCESS", 0) | getattr(
            subprocess, "CREATE_NEW_PROCESS_GROUP", 0
        )
    try:
        subprocess.Popen(  # noqa: S602 - 用户显式配置的命令
            cmd,
            shell=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            creationflags=flags,
        )
    except OSError as exc:
        return False, f"启动命令失败：{exc}"
    return True, f"已执行启动命令：{cmd}"


# --------------------------------------------------------------------------- #
# 适配器
# --------------------------------------------------------------------------- #


class MirrorControl:
    name = "manual"

    def __init__(self, target_roots: Iterable[Path], max_depth: int = 2):
        self.target_roots = [Path(p) for p in target_roots]
        self.max_depth = max_depth

    # -- 供引擎调用 ----------------------------------------------------- #

    def check(self) -> Tuple[bool, str]:
        raise NotImplementedError

    def can_manage(self) -> bool:
        return False

    def stop(self, timeout: float, notify_sec: int = 0) -> Tuple[bool, str]:
        return False, f"{self.name} 适配器不支持自动停服"

    def start(self) -> Tuple[bool, str]:
        return False, f"{self.name} 适配器不支持自动启动"

    def wait_ready(self, timeout: float) -> bool:
        return False

    # -- 工具 ----------------------------------------------------------- #

    def _locks(self) -> List[Path]:
        locks: List[Path] = []
        for root in self.target_roots:
            locks.extend(find_session_locks(root, self.max_depth))
        return locks

    def _locks_busy(self) -> List[Path]:
        return [lock for lock in self._locks() if not _probe_lock(lock)]

    def _wait_locks_free(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not self._locks_busy():
                return True
            time.sleep(PROBE_INTERVAL)
        return not self._locks_busy()

    def _poll(self, predicate, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(PROBE_INTERVAL)
        return predicate()


class ManualMirrorControl(MirrorControl):
    """只检查镜像是否已停止；不提供 stop/start。"""

    name = "manual"

    def check(self) -> Tuple[bool, str]:
        locks = self._locks()
        if not locks:
            return True, "未发现 session.lock，视为镜像未运行"
        busy = self._locks_busy()
        if busy:
            shown = ", ".join(str(p) for p in busy[:3])
            more = "" if len(busy) <= 3 else f" 等 {len(busy)} 个"
            return False, f"镜像仍在运行（session.lock 被占用：{shown}{more}），请先关闭镜像服"
        return True, f"已确认镜像未运行（检查了 {len(locks)} 个 session.lock）"


class RconMirrorControl(ManualMirrorControl):
    """RCON 停服 + 可选命令启动。"""

    name = "rcon"

    def __init__(
        self,
        target_roots: Iterable[Path],
        rcon: RconConfig,
        start_cmd: str = "",
        max_depth: int = 2,
    ):
        super().__init__(target_roots, max_depth)
        self.rcon = rcon
        self.start_cmd = start_cmd
        self._client: Optional[RconClient] = None

    def can_manage(self) -> bool:
        return True

    def _rcon(self) -> RconClient:
        if self._client is None:
            self._client = RconClient(self.rcon.host, self.rcon.port, self.rcon.password)
            self._client.connect()
        return self._client

    def _running(self) -> bool:
        if self._locks_busy():
            return True
        return port_open(self.rcon.host, self.rcon.port)

    def check(self) -> Tuple[bool, str]:
        if self._locks_busy():
            return False, "镜像仍在运行（session.lock 被占用）"
        if port_open(self.rcon.host, self.rcon.port):
            return False, f"镜像 RCON 端口仍可连接（{self.rcon.host}:{self.rcon.port}），视为运行中"
        return True, "镜像未运行（session.lock 未被占用、RCON 端口关闭）"

    def stop(self, timeout: float, notify_sec: int = 0) -> Tuple[bool, str]:
        if not self._running():
            return True, "镜像本来就没有运行"
        if self.rcon.port <= 0 or not self.rcon.password:
            return False, "rcon 适配器需要 mirror_control.rcon.password 才能停服"
        try:
            client = self._rcon()
            if notify_sec > 0:
                try:
                    client.command(f"say [MirrorUP] 镜像服将在 {notify_sec} 秒后同步并重启")
                except RconError:
                    pass
            client.command("stop")
        except RconError as exc:
            self.close()
            return False, f"RCON 停服失败：{exc}"

        if self._poll(lambda: not self._running(), timeout):
            self.close()
            return True, "已通过 RCON 停止镜像服"
        self.close()
        return False, f"已发送 stop，但 {timeout:.0f} 秒内镜像仍未退出"

    def start(self) -> Tuple[bool, str]:
        if not self.start_cmd:
            return False, "未配置 mirror_control.external.start_cmd，无法自动启动"
        return _spawn(self.start_cmd)

    def wait_ready(self, timeout: float) -> bool:
        return self._poll(lambda: port_open(self.rcon.host, self.rcon.port), timeout)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None


class ExternalMirrorControl(MirrorControl):
    """完全由用户命令控制：stop_cmd / start_cmd / probe_cmd。"""

    name = "external"

    def __init__(
        self,
        target_roots: Iterable[Path],
        external: ExternalConfig,
        max_depth: int = 2,
    ):
        super().__init__(target_roots, max_depth)
        self.external = external

    def can_manage(self) -> bool:
        return True

    def _probe_running(self) -> Optional[bool]:
        if not self.external.probe_cmd:
            return None
        code, output = _run(self.external.probe_cmd, timeout=10)
        if code == 0:
            return True
        if code in (124, 127):
            logger.warning("[MirrorUP] probe_cmd 执行异常：%s", output)
            return None
        return False

    def check(self) -> Tuple[bool, str]:
        running = self._probe_running()
        if running is not None:
            if running:
                return False, "镜像仍在运行（probe_cmd 返回 0）"
            return True, "镜像未运行（probe_cmd 返回非 0）"
        # 没配 probe_cmd：退化成 session.lock 检查
        return super().check()

    def stop(self, timeout: float, notify_sec: int = 0) -> Tuple[bool, str]:
        if not self.external.stop_cmd:
            return False, "mirror_control.external.stop_cmd 为空，无法自动停服"
        code, output = _run(self.external.stop_cmd, timeout=min(timeout, 60))
        if code in (124, 127):
            return False, f"stop_cmd 执行失败（{code}）：{output}"

        def stopped() -> bool:
            running = self._probe_running()
            if running is None:
                return not self._locks_busy()
            return not running

        if self._poll(stopped, timeout):
            return True, f"已执行 stop_cmd（退出码 {code}）并确认镜像退出"
        return False, f"stop_cmd 已执行，但 {timeout:.0f} 秒内未确认镜像退出"

    def start(self) -> Tuple[bool, str]:
        if not self.external.start_cmd:
            return False, "mirror_control.external.start_cmd 为空，无法自动启动"
        return _spawn(self.external.start_cmd)

    def wait_ready(self, timeout: float) -> bool:
        def ready() -> bool:
            running = self._probe_running()
            if running is not None:
                return running
            return bool(self._locks_busy())

        return self._poll(ready, timeout)


def build_mirror_control(
    control: MirrorControlConfig, target_roots: Iterable[Path]
) -> MirrorControl:
    roots = [Path(p) for p in target_roots]
    adapter = (control.adapter or "manual").lower()
    if adapter == "rcon":
        return RconMirrorControl(roots, control.rcon, control.external.start_cmd)
    if adapter == "external":
        return ExternalMirrorControl(roots, control.external)
    # manual / mcsm（未实现）都退化为只检查
    return ManualMirrorControl(roots)
