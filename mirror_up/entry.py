"""MCDR 插件入口：把配置、索引、引擎和命令树粘在一起。

这个模块是唯一允许 import ``mcdreforged`` 的顶层模块（``commands`` 与 ``flush``
例外，它们在内部延迟导入）。核心同步逻辑（``engine`` 及以下）在纯 Python 下可单独
测试，见 ``tests/smoke_engine.py``。
"""

from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path
from typing import List, Optional

from mcdreforged.api.all import CommandSource, RColor, RText, RTextList, PluginServerInterface

from .agent import AgentError, AgentServer
from .api import MirrorUPAPI
from . import commands
from .config import (
    Config,
    ConfigError,
    load_config,
    save_config,
    validate_config,
)
from .constants import APP_NAME, CONFIG_FILENAME, INDEX_FILENAME, VERSION
from .engine import RoundReport, SyncBusy, SyncEngine
from .flush import McdrSourceFlusher, NullSourceFlusher
from .index import Index

logger = logging.getLogger("mirror_up")

_plugin: Optional["MirrorUP"] = None


class MirrorUP:
    def __init__(self, server: PluginServerInterface):
        self.server = server
        self.version = VERSION
        self.command_prefix = "!!mu"
        self.data_dir = Path(server.get_data_folder())
        self.data_dir.mkdir(parents=True, exist_ok=True)
        # 相对路径以 MCDR 根目录为基准：默认 source.root="./server" 正好指向
        # working_directory 里的那套服务端文件。
        self.base_dir = Path(os.getcwd())

        self.config, warnings = load_config(self.data_dir)
        warnings += validate_config(self.config, self.base_dir)
        for message in warnings:
            self.server.logger.warning("[%s] %s", APP_NAME, message)

        self.index = Index(self.data_dir / INDEX_FILENAME)
        self.is_exporter = self.config.role == "exporter"
        self.engine = None if self.is_exporter else self._build_engine()
        self.agent: Optional[AgentServer] = self._build_agent() if self.is_exporter else None
        #: 给其它插件用的 Python API（见 ``mirror_up.api``）。
        self.api = MirrorUPAPI(self)

        self._sync_thread: Optional[threading.Thread] = None
        self._scheduler_thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._next_due = 0.0

    # ------------------------------------------------------------------ #
    # 构建
    # ------------------------------------------------------------------ #

    def _build_engine(self) -> SyncEngine:
        if self.config.source_flush.enabled:
            flusher = McdrSourceFlusher(self.server, self.config.source_flush)
        else:
            flusher = NullSourceFlusher("配置里 source_flush.enabled=false")
        return SyncEngine(
            self.config,
            self.base_dir,
            self.index,
            flusher=flusher,
            log=self.server.logger,
            local_server=self.server,
        )

    def _build_agent(self) -> Optional[AgentServer]:
        agent = AgentServer(self.config, self.base_dir)
        try:
            agent.validate()
        except AgentError as exc:
            self.server.logger.error("[%s] exporter 无法启动：%s", APP_NAME, exc)
            return None
        return agent

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #

    def start_background(self) -> None:
        if self.is_exporter:
            if self.agent is None:
                return
            try:
                self.agent.start(background=True)
            except AgentError as exc:
                self.server.logger.error("[%s] agent 启动失败：%s", APP_NAME, exc)
                return
            self.server.logger.info("[%s] exporter 已监听 %s", APP_NAME, self.agent.url)
            return

        self._stop.clear()
        self._reschedule()
        self._scheduler_thread = threading.Thread(
            target=self._scheduler_loop, name="MirrorUP-scheduler", daemon=True
        )
        self._scheduler_thread.start()

    def shutdown(self) -> None:
        self._stop.set()
        self._wake.set()
        thread = self._scheduler_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=5.0)
        if self.engine is not None:
            self.engine.cancel()
        if self.agent is not None:
            try:
                self.agent.stop()
            except Exception:  # pragma: no cover
                pass
        try:
            self.index.close()
        except Exception:  # pragma: no cover
            pass

    # ------------------------------------------------------------------ #
    # 调度
    # ------------------------------------------------------------------ #

    def _reschedule(self) -> None:
        if self.config.schedule.auto_sync:
            self._next_due = time.time() + self.config.schedule.interval_seconds
        else:
            self._next_due = float("inf")

    def _scheduler_loop(self) -> None:
        while not self._stop.is_set():
            if not self.config.schedule.auto_sync:
                self._wake.wait(timeout=30.0)
                self._wake.clear()
                continue

            remaining = self._next_due - time.time()
            if remaining > 0:
                self._wake.wait(timeout=min(remaining, 30.0))
                self._wake.clear()
                if self._stop.is_set():
                    break
                continue

            if self.engine.is_busy():
                self._wake.wait(timeout=5.0)
                self._wake.clear()
                continue

            try:
                self.server.logger.info("[%s] 自动同步开始", APP_NAME)
                self._run_round(None, dry_run=False, full=False, force=False, trigger="schedule")
            except Exception:  # pragma: no cover - 调度线程不能死
                self.server.logger.exception("[%s] 自动同步异常", APP_NAME)
            finally:
                self._reschedule()

    # ------------------------------------------------------------------ #
    # 同步
    # ------------------------------------------------------------------ #

    def start_sync(
        self,
        source: Optional[CommandSource],
        dry_run: bool = False,
        full: bool = False,
        force: bool = False,
    ) -> None:
        if self.is_exporter or self.engine is None:
            self._reply(
                source,
                RText(f"[{APP_NAME}] 当前是 exporter 节点，不执行同步（同步请到 driver 上操作）", RColor.yellow),
            )
            return
        if self.engine.is_busy() or (self._sync_thread is not None and self._sync_thread.is_alive()):
            self._reply(source, RText(f"[{APP_NAME}] 已有同步正在进行", RColor.yellow))
            return

        self._sync_thread = threading.Thread(
            target=self._run_round,
            args=(source, dry_run, full, force, "manual"),
            name="MirrorUP-sync",
            daemon=True,
        )
        self._sync_thread.start()
        if not dry_run:
            self._reply(source, RText(f"[{APP_NAME}] 已开始同步，完成后另行通知", RColor.gray))

    def _run_round(
        self,
        source: Optional[CommandSource],
        dry_run: bool,
        full: bool,
        force: bool,
        trigger: str,
    ) -> None:
        if self.engine is None:
            self._reply(source, RText(f"[{APP_NAME}] 当前是 exporter 节点，不执行同步", RColor.yellow))
            return
        started = time.time()
        try:
            report = self.engine.run(dry_run=dry_run, full=full, force=force, trigger=trigger)
        except SyncBusy:
            self._reply(source, RText(f"[{APP_NAME}] 已有同步正在进行", RColor.yellow))
            return
        except ConfigError as exc:
            self._reply(source, RText(f"[{APP_NAME}] 配置错误：{exc}", RColor.red))
            self.server.logger.error("[%s] 配置错误：%s", APP_NAME, exc)
            return
        except Exception as exc:  # 保证命令/调度线程不炸
            self.server.logger.exception("[%s] 同步异常", APP_NAME)
            self._reply(source, RText(f"[{APP_NAME}] 同步异常：{exc}", RColor.red))
            return

        rendered = commands.render_plan(self, report) if dry_run else commands.render_report(report)
        self.server.logger.info(rendered.to_plain_text())
        self._reply(source, rendered)

        if not dry_run and trigger == "schedule":
            self._reschedule()

    def _reply(self, source: Optional[CommandSource], message) -> None:
        if source is None:
            return
        try:
            source.reply(message)
        except Exception:  # pragma: no cover - 玩家可能已离线
            self.server.logger.debug("[%s] 回复消息失败", APP_NAME, exc_info=True)

    def start_verify(self, source: Optional[CommandSource], deep: bool = False) -> None:
        """``!!mu verify``：只读校验，放后台线程跑（深校验可能读几分钟）。"""
        if self.is_exporter or self.engine is None:
            self._reply(
                source,
                RText(f"[{APP_NAME}] 当前是 exporter 节点，校验请到 driver 上执行", RColor.yellow),
            )
            return
        if self.engine.is_busy():
            self._reply(source, RText(f"[{APP_NAME}] 同步进行中，等本轮结束后再校验", RColor.yellow))
            return
        thread = threading.Thread(
            target=self._run_verify, args=(source, deep), name="MirrorUP-verify", daemon=True
        )
        thread.start()

    def _run_verify(self, source: Optional[CommandSource], deep: bool) -> None:
        if deep:
            self._reply(
                source,
                RText(f"[{APP_NAME}] 深校验开始，会读取源端与镜像的全部内容", RColor.gray),
            )
        try:
            report = self.api.verify(deep=deep)
        except Exception as exc:  # noqa: BLE001 - 校验失败要能回报
            self.server.logger.exception("[%s] 校验异常", APP_NAME)
            self._reply(source, RText(f"[{APP_NAME}] 校验失败：{exc}", RColor.red))
            return
        rendered = commands.render_verify(report)
        self.server.logger.info(rendered.to_plain_text())
        self._reply(source, rendered)

    def history(self, limit: int = 10) -> List[dict]:
        """最近几轮同步记录（``!!mu history`` 与 Python API 共用）。"""
        try:
            return self.index.list_history(limit=max(1, int(limit)))
        except Exception:  # pragma: no cover - 索引损坏时不要让命令炸
            self.server.logger.exception("[%s] 读取同步历史失败", APP_NAME)
            return []

    # ------------------------------------------------------------------ #
    # 命令层需要的接口
    # ------------------------------------------------------------------ #

    def status_lines(self) -> List[str]:
        lines = [f"{APP_NAME} v{self.version}（{self.config.role}）"]
        if self.is_exporter:
            if self.agent is not None and self.agent.url:
                lines.append(f"agent：监听中 {self.agent.url}")
            else:
                lines.append("agent：未启动（检查 agent_server.token / tls 配置）")
            sets = ", ".join(s.name for s in self.config.enabled_sets()) or "无"
            lines.append(f"对外提供：{sets}")
            lines.append(f"配置：{self.config.path}")
            return lines
        if self.engine is not None:
            lines.extend(self.engine.describe())
        if self.config.schedule.auto_sync:
            remaining = max(0, int(self._next_due - time.time()))
            lines.append(
                f"自动同步：开，间隔 {_human(self.config.schedule.interval_seconds)}，"
                f"下次约 {_human(remaining) if remaining else '即将'}后"
            )
        else:
            lines.append("自动同步：关")
        lines.append(f"配置：{self.config.path}")
        lines.append(f"索引：{self.data_dir / INDEX_FILENAME}")
        return lines

    def reload_config(self) -> RText:
        try:
            config, warnings = load_config(self.data_dir)
            warnings += validate_config(config, self.base_dir)
        except ConfigError as exc:
            self.server.logger.error("[%s] 配置重载失败：%s", APP_NAME, exc)
            return RText(f"[{APP_NAME}] 配置重载失败：{exc}", RColor.red)

        self.config = config
        self.is_exporter = config.role == "exporter"
        if self.agent is not None:
            try:
                self.agent.stop()
            except Exception:  # pragma: no cover
                pass
            self.agent = None
        if self.is_exporter:
            self.engine = None
            self.agent = self._build_agent()
            if self.agent is not None:
                try:
                    self.agent.start(background=True)
                except AgentError as exc:
                    self.server.logger.error("[%s] agent 启动失败：%s", APP_NAME, exc)
        else:
            self.engine = self._build_engine()
        self._reschedule()
        self._wake.set()
        for message in warnings:
            self.server.logger.warning("[%s] %s", APP_NAME, message)
        return RText(f"[{APP_NAME}] 配置已重载（{len(warnings)} 条提示）", RColor.green)

    def set_auto_sync(self, enabled: bool) -> None:
        self.config.schedule.auto_sync = enabled
        self._persist()
        self._reschedule()
        self._wake.set()

    def set_interval(self, seconds: int) -> int:
        limit = self.config.schedule.max_interval_seconds
        effective = max(60, min(int(seconds), limit))
        self.config.schedule.interval_seconds = effective
        self._persist()
        self._reschedule()
        self._wake.set()
        return effective

    def _persist(self) -> None:
        try:
            save_config(self.config, self.config.path)
        except OSError as exc:
            self.server.logger.error("[%s] 写回配置失败：%s", APP_NAME, exc)


def _human(seconds: int) -> str:
    seconds = max(0, int(seconds))
    if seconds >= 86400:
        return f"{seconds // 86400} 天"
    if seconds >= 3600:
        return f"{seconds // 3600} 小时"
    if seconds >= 60:
        return f"{seconds // 60} 分钟"
    return f"{seconds} 秒"


# --------------------------------------------------------------------------- #
# MCDR 生命周期
# --------------------------------------------------------------------------- #


def on_load(server: PluginServerInterface, prev_module) -> None:
    global _plugin
    try:
        _plugin = MirrorUP(server)
    except ConfigError as exc:
        server.logger.error("[%s] 加载失败，请检查配置：%s", APP_NAME, exc)
        server.logger.error("[%s] 配置文件位于 %s", APP_NAME, Path(server.get_data_folder()) / CONFIG_FILENAME)
        raise

    commands.register(server, _plugin)
    _plugin.start_background()
    server.logger.info("[%s] v%s 已加载（配置：%s）", APP_NAME, VERSION, _plugin.config.path)


def on_unload(server: PluginServerInterface) -> None:
    global _plugin
    if _plugin is not None:
        _plugin.shutdown()
        _plugin = None
