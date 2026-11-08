"""源端保存窗口（save-off → save-all flush → 等待 → save-on）。

窗口只覆盖**扫描阶段**：我们只需要“源端在某一瞬间是静止的”，拿到一份一致的元数据
快照就够了；传输阶段玩家可以继续玩，不需要停服。

等待 ``Saved the game`` 依赖 MCDR 事件。这里不直接 import MCDR：flusher 由 entry 层
构造并注入，核心模块因此可以脱离 MCDR 做单元测试。
"""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass
from typing import Any, Optional

from .config import SourceFlushConfig


@dataclass
class FlushOutcome:
    ok: bool
    message: str
    skipped: bool = False


class NullSourceFlusher:
    """没有服务器接口时使用（单测、纯命令行场景）。"""

    def __init__(self, reason: str = "未连接 MCDR 服务器，跳过 save-all"):
        self.reason = reason
        self.active = False

    def begin(self) -> FlushOutcome:
        return FlushOutcome(ok=False, message=self.reason, skipped=True)

    def end(self) -> None:
        return None


class McdrSourceFlusher:
    """通过 MCDR 向源服发送保存指令，并监听控制台输出确认落盘。"""

    def __init__(self, server: Any, config: SourceFlushConfig):
        self.server = server
        self.config = config
        self._saved = threading.Event()
        self._regex = re.compile(config.saved_regex)
        self._listening = False
        self._auto_save_off = False
        self.active = False

    # ------------------------------------------------------------------ #
    # 事件
    # ------------------------------------------------------------------ #

    def _on_info(self, info: Any) -> None:
        try:
            if not getattr(info, "is_from_server", False):
                return
            content = getattr(info, "content", "") or ""
            if self._regex.search(content):
                self._saved.set()
        except Exception:  # 事件回调不能把异常抛回 MCDR
            pass

    def _ensure_listening(self) -> None:
        if self._listening:
            return
        from mcdreforged.plugin.plugin_event import MCDRPluginEvents  # 延迟导入

        self.server.register_event_listener(MCDRPluginEvents.GENERAL_INFO, self._on_info)
        self._listening = True

    # ------------------------------------------------------------------ #
    # 窗口
    # ------------------------------------------------------------------ #

    def begin(self) -> FlushOutcome:
        cfg = self.config
        if not cfg.enabled:
            return FlushOutcome(ok=False, message="source_flush 已关闭，直接扫描", skipped=True)
        try:
            if not self.server.is_server_running():
                return FlushOutcome(ok=False, message="源服未运行，跳过 save-all", skipped=True)
        except Exception:
            return FlushOutcome(ok=False, message="无法判断源服状态，跳过 save-all", skipped=True)

        self._ensure_listening()
        self.active = True
        self._saved.clear()

        try:
            if cfg.turn_off_auto_save:
                self.server.execute(cfg.commands["auto_save_off"])
                self._auto_save_off = True
            self.server.execute(cfg.commands["save_all"])
        except Exception as exc:
            self._restore_auto_save()
            return FlushOutcome(ok=False, message=f"发送保存指令失败：{exc}")

        if self._saved.wait(cfg.timeout_sec):
            return FlushOutcome(ok=True, message="源端已落盘")
        self._restore_auto_save()
        return FlushOutcome(
            ok=False,
            message=f"等待 {cfg.saved_regex!r} 超时（{cfg.timeout_sec}s），继续同步但源端可能不是最新状态",
        )

    def end(self) -> None:
        if not self.active:
            return
        self.active = False
        self._restore_auto_save()

    def _restore_auto_save(self) -> None:
        if not self._auto_save_off:
            return
        self._auto_save_off = False
        try:
            self.server.execute(self.config.commands["auto_save_on"])
        except Exception:
            pass


class SourceFlusher:
    """类型占位：begin/end 两个方法。"""

    def begin(self) -> FlushOutcome:  # pragma: no cover
        raise NotImplementedError

    def end(self) -> None:  # pragma: no cover
        raise NotImplementedError


def build_null_flusher() -> NullSourceFlusher:
    return NullSourceFlusher()
