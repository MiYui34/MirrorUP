"""给其它 MCDR 插件用的 Python API。

用法（在别的插件里）::

    from mcdreforged.api.all import PluginServerInterface

    def on_load(server: PluginServerInterface, prev_module):
        api = server.get_plugin_instance("mirror_up")   # MirrorUP 实例
        if api is None:
            return
        report = api.api.sync(dry_run=True)             # 同步执行一轮（阻塞）
        if report.ok():
            server.logger.info("镜像已是最新")

约定：

* ``sync()`` 是**阻塞**的，不要在 MCDR 主线程里直接调（会卡住服务器输入）。
  需要异步就用 ``sync_async()``，或者自己丢进线程。
* ``sync()`` 不会抛「已有同步在跑」，而是抛 :class:`SyncBusy`，调用方自己决定
  是排队还是忽略。
* ``verify()`` 只读，可以随时调。
* 没有 driver 身份（``role="exporter"``）时 ``sync()`` 抛 :class:`ApiError`。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .config import Config
from .engine import RoundReport, SyncBusy
from .index import Index
from .verify import VerifyReport, verify as run_verify


class ApiError(Exception):
    """API 使用错误（不是同步失败）。"""


@dataclass
class ApiInfo:
    version: str
    role: str
    busy: bool
    targets: List[str]
    sets: List[str]


class MirrorUPAPI:
    """薄封装：把插件内部对象暴露成稳定的调用面。"""

    def __init__(self, plugin) -> None:
        self._plugin = plugin

    # -- 只读属性 ------------------------------------------------------- #

    @property
    def config(self) -> Config:
        return self._plugin.config

    @property
    def index(self) -> Index:
        return self._plugin.index

    @property
    def version(self) -> str:
        return self._plugin.version

    def info(self) -> ApiInfo:
        cfg = self.config
        engine = getattr(self._plugin, "engine", None)
        return ApiInfo(
            version=self.version,
            role=cfg.role,
            busy=bool(engine and engine.is_busy()),
            targets=[t.name for t in cfg.enabled_targets()],
            sets=[s.name for s in cfg.enabled_sets()],
        )

    # -- 同步 ----------------------------------------------------------- #

    def sync(
        self,
        dry_run: bool = False,
        full: bool = False,
        force: bool = False,
        trigger: str = "api",
    ) -> RoundReport:
        """同步执行一轮。阻塞调用方线程；并发调用抛 :class:`SyncBusy`。"""
        engine = getattr(self._plugin, "engine", None)
        if engine is None:
            raise ApiError("当前节点是 exporter，不执行同步（请在 driver 上调用）")
        return engine.run(dry_run=dry_run, full=full, force=force, trigger=trigger)

    def sync_async(
        self,
        dry_run: bool = False,
        full: bool = False,
        force: bool = False,
        trigger: str = "api",
        callback=None,
    ) -> threading.Thread:
        """后台线程跑一轮，返回线程对象；``callback(report)`` 在结束时被调用。"""

        def _target() -> None:
            report: Optional[RoundReport] = None
            try:
                report = self.sync(dry_run=dry_run, full=full, force=force, trigger=trigger)
            except Exception as exc:  # noqa: BLE001 - 回调里再抛出会杀掉线程
                self._plugin.server.logger.error("[MirrorUP] API 同步失败：%s", exc)
            finally:
                if callback is not None:
                    try:
                        callback(report)
                    except Exception:  # noqa: BLE001
                        self._plugin.server.logger.exception("[MirrorUP] API 回调失败")

        thread = threading.Thread(target=_target, name="MirrorUP-api-sync", daemon=True)
        thread.start()
        return thread

    def cancel(self) -> bool:
        engine = getattr(self._plugin, "engine", None)
        if engine is None or not engine.is_busy():
            return False
        engine.cancel()
        return True

    # -- 校验与查询 ----------------------------------------------------- #

    def verify(self, deep: bool = False, targets: Optional[List[str]] = None) -> VerifyReport:
        return run_verify(
            self.config,
            self._plugin.base_dir,
            self.index,
            deep=deep,
            target_names=targets,
        )

    def status(self) -> List[str]:
        engine = getattr(self._plugin, "engine", None)
        return engine.describe() if engine is not None else ["exporter 节点"]

    def history(self, limit: int = 20) -> List[Dict[str, Any]]:
        return self.index.list_history(limit=max(1, int(limit)))


def get_api(plugin) -> MirrorUPAPI:
    """从 ``MirrorUP`` 实例拿 API 对象（内部只有一个实例，重复调用无副作用）。"""
    api = getattr(plugin, "api", None)
    if isinstance(api, MirrorUPAPI):
        return api
    api = MirrorUPAPI(plugin)
    try:
        plugin.api = api
    except AttributeError:
        pass
    return api
