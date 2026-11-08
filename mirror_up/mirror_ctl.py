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

import json
import logging
import os
import ssl
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Iterable, List, Optional, Tuple

from .config import ExternalConfig, McsmConfig, MirrorControlConfig, RconConfig
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


class SelfMirrorControl(MirrorControl):
    """控制「插件自己所在的那个 Minecraft 服务器」。

    和 rcon/external 的区别：这里不要端口、密码或外部命令，直接调用 MCDR 的
    ``ServerInterface``。它用于就地同步：镜像服自己也装了 MirrorUP，玩家在镜像上
    敲 ``!!mu sync`` 时，插件先停本服、写好世界、再拉起来。``server.stop()`` 只停
    服务端，MCDR 本身会继续运行，所以同步线程不会被一起停掉。
    """

    name = "self"

    def __init__(self, server: Any, target_roots: Iterable[Path], max_depth: int = 2):
        super().__init__(target_roots, max_depth)
        self.server = server

    # -- 探测 ----------------------------------------------------------- #

    def _server_running(self) -> bool:
        if self.server is None:
            return bool(self._locks_busy())
        try:
            return bool(self.server.is_server_running())
        except Exception:  # 状态读不到就退回文件锁判断
            logger.debug("[MirrorUP] 读取本服运行状态失败", exc_info=True)
            return bool(self._locks_busy())

    def check(self) -> Tuple[bool, str]:
        if self.server is None:
            if self._locks_busy():
                return False, "没有拿到 server 对象，且 session.lock 被占用"
            return True, "未发现 session.lock，视为本服未运行"
        if self._server_running():
            return False, "本服正在运行（插件所在服务器），将先停止它再写入"
        busy = self._locks_busy()
        if busy:
            shown = ", ".join(str(p) for p in busy[:3])
            return False, f"本服进程已退出，但 session.lock 仍被占用：{shown}"
        return True, "本服已停止，可以写入"

    def can_manage(self) -> bool:
        return self.server is not None

    # -- 启停 ----------------------------------------------------------- #

    def stop(self, timeout: float, notify_sec: int = 0) -> Tuple[bool, str]:
        if self.server is None:
            return False, "没有 server 对象，无法自动停止本服"
        if not self._server_running():
            return self._wait_locks_free(min(timeout, 10.0)), "本服本来就处于停止状态"
        if notify_sec > 0:
            self._notify(notify_sec)
        try:
            sent = bool(self.server.stop())
        except Exception as exc:
            return False, f"调用 server.stop() 失败：{exc}"
        if not sent:
            return False, "server.stop() 返回 False（服务器可能正在停止或未就绪）"
        if not self._poll(lambda: not self._server_running(), timeout):
            return False, (
                f"{timeout:.0f}s 内本服没有退出——它可能不响应 stop 命令（被包装脚本接管、"
                "卡在保存上，或者根本不是能被 MCDR 停止的服务端）。请手动关闭后再同步；本次不会写入任何文件。"
            )
        self._wait_locks_free(min(timeout, 10.0))
        return True, "本服已停止"

    def start(self) -> Tuple[bool, str]:
        if self.server is None:
            return False, "没有 server 对象，无法自动启动本服"
        if self._server_running():
            return True, "本服已在运行"
        try:
            started = bool(self.server.start())
        except Exception as exc:
            return False, f"调用 server.start() 失败：{exc}"
        if not started:
            return False, "server.start() 返回 False（可能已在启动，或 MCDR 状态不允许）"
        return True, "已发送启动命令"

    def wait_ready(self, timeout: float) -> bool:
        if self.server is None:
            return False
        return self._poll(self._server_running, timeout)

    # -- 内部 ----------------------------------------------------------- #

    def _notify(self, seconds: int) -> None:
        text = f"[MirrorUP] 本服将在 {seconds} 秒后停止以同步世界，稍后自动重启"
        try:
            say = getattr(self.server, "say", None)
            if callable(say):
                say(text)
            else:
                self.server.execute(f"say {text}")
        except Exception:
            logger.debug("[MirrorUP] 广播停服提示失败", exc_info=True)
        time.sleep(min(max(int(seconds), 0), 30))


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


class McsmError(Exception):
    """MCSM API 调用失败。"""


class _McsmClient:
    """最小 MCSManager HTTP 客户端。

    兼容两种鉴权：MCSM 9 的 ``apikey`` 查询参数，以及 MCSM 10 的登录 token
    （``POST /api/auth/login`` → ``Authorization: Bearer``）。实例操作沿用
    ``/api/protected_instance/*`` 端点。
    """

    def __init__(self, cfg: McsmConfig):
        self.cfg = cfg
        self.base_url = (cfg.base_url or "").rstrip("/")
        self._token: Optional[str] = None

    # -- 底层 ----------------------------------------------------------- #

    def _url(self, path: str, params: Optional[dict] = None) -> str:
        query = dict(params or {})
        if self.cfg.apikey:
            query.setdefault("apikey", self.cfg.apikey)
        url = f"{self.base_url}{path}"
        if query:
            url += "?" + urllib.parse.urlencode(query)
        return url

    def request(self, method: str, path: str, params: Optional[dict] = None, body: Optional[dict] = None) -> Any:
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(self._url(path, params), data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        if self._token:
            req.add_header("Authorization", f"Bearer {self._token}")
        context = ssl._create_unverified_context() if self.cfg.insecure_tls else None
        try:
            with urllib.request.urlopen(req, timeout=self.cfg.timeout_sec, context=context) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as exc:
            detail = ""
            try:
                detail = exc.read().decode("utf-8", "replace")[:200]
            except Exception:  # noqa: BLE001
                pass
            raise McsmError(f"HTTP {exc.code} {path} {detail}".strip()) from exc
        except (urllib.error.URLError, OSError) as exc:
            raise McsmError(f"连接 {self.base_url} 失败：{exc}") from exc
        if not raw:
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise McsmError(f"响应不是 JSON：{raw[:120]!r}") from exc

    def login(self) -> None:
        if self.cfg.apikey or self._token:
            return
        if not (self.cfg.username and self.cfg.password):
            raise McsmError("未配置 apikey，也没有 username/password")
        data = self.request(
            "POST", "/api/auth/login", body={"username": self.cfg.username, "password": self.cfg.password}
        )
        token = None
        if isinstance(data, dict):
            token = data.get("token") or (data.get("data") or {}).get("token")
        if not token:
            raise McsmError("登录 MCSM 失败：响应里没有 token")
        self._token = str(token)

    # -- 语义 ----------------------------------------------------------- #

    def _instance_params(self) -> dict:
        return {"uuid": self.cfg.uuid, "remote_uuid": self.cfg.remote_uuid}

    def status(self) -> Optional[int]:
        """返回 0=已停止、1=运行中、2=启动中、3=停止中；判断不了返回 None。

        MCSM 的响应是 ``{"status": 200, "data": 3}`` 这种信封格式，顶层的
        ``status`` 是 HTTP 风格的状态码，**不是**实例状态——所以先看 ``data``。
        """
        data = self.request("GET", "/api/protected_instance/status", self._instance_params())
        if not isinstance(data, dict):
            return None
        inner = data.get("data")
        value: object = None
        if isinstance(inner, dict):
            value = inner.get("status")
        elif isinstance(inner, (int, str)) and not isinstance(inner, bool):
            value = inner
        if value is None:
            value = data.get("status")
        text = str(value).strip() if value is not None else ""
        if not text.isdigit():
            return None
        code = int(text)
        # 200/400 这类是信封里的 HTTP 状态码，不能当实例状态用。
        return code if code in (0, 1, 2, 3) else None

    def stop(self) -> None:
        self.request("POST", "/api/protected_instance/stop", self._instance_params())

    def start(self) -> None:
        self.request("POST", "/api/protected_instance/start", self._instance_params())


class McsmMirrorControl(MirrorControl):
    """通过 MCSManager 面板停/启实例。"""

    name = "mcsm"

    def __init__(self, target_roots: Iterable[Path], mcsm: McsmConfig, max_depth: int = 2):
        super().__init__(target_roots, max_depth)
        self.mcsm = mcsm
        self._client: Optional[_McsmClient] = None

    def can_manage(self) -> bool:
        return True

    def _api(self) -> _McsmClient:
        if self._client is None:
            client = _McsmClient(self.mcsm)
            client.login()
            self._client = client
        return self._client

    def _status(self) -> Optional[int]:
        try:
            return self._api().status()
        except McsmError as exc:
            logger.warning("[MirrorUP] MCSM 状态查询失败：%s", exc)
            return None

    def _running(self) -> Optional[bool]:
        status = self._status()
        if status is not None:
            # 2=启动中、3=停止中，都算“还没停稳”，按运行中处理最安全。
            return status != 0
        if self._locks_busy():
            return True
        return None

    def check(self) -> Tuple[bool, str]:
        running = self._running()
        if running is None:
            return super().check()
        if running:
            return False, "镜像仍在运行（MCSM 实例状态为运行中）"
        return True, "镜像未运行（MCSM 实例状态为已停止）"

    def stop(self, timeout: float, notify_sec: int = 0) -> Tuple[bool, str]:
        try:
            client = self._api()
        except McsmError as exc:
            return False, f"MCSM 登录失败：{exc}"
        try:
            client.stop()
        except McsmError as exc:
            return False, f"MCSM 停服请求失败：{exc}"

        def stopped() -> bool:
            running = self._running()
            return running is False

        if self._poll(stopped, timeout):
            return True, "已通过 MCSM 停止镜像服"
        return False, f"MCSM 已发送停止请求，但 {timeout:.0f} 秒内实例仍在运行"

    def start(self) -> Tuple[bool, str]:
        try:
            self._api().start()
        except McsmError as exc:
            return False, f"MCSM 启动请求失败：{exc}"
        return True, "已通过 MCSM 启动镜像服"

    def wait_ready(self, timeout: float) -> bool:
        return self._poll(lambda: self._running() is True, timeout)


def build_mirror_control(
    control: MirrorControlConfig,
    target_roots: Iterable[Path],
    server: Any = None,
) -> MirrorControl:
    roots = [Path(p) for p in target_roots]
    adapter = (control.adapter or "manual").lower()
    if adapter == "self":
        return SelfMirrorControl(server, roots)
    if adapter == "rcon":
        return RconMirrorControl(roots, control.rcon, control.external.start_cmd)
    if adapter == "external":
        return ExternalMirrorControl(roots, control.external)
    if adapter == "mcsm":
        return McsmMirrorControl(roots, control.mcsm)
    return ManualMirrorControl(roots)
