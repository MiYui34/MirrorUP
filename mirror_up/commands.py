"""``!!mu`` 命令树。

命令层只做三件事：解析参数、把请求交给 ``MirrorUP`` 实例、把结果渲染成人能读的文本。
同步本身在后台线程里跑，命令回调必须立刻返回，否则会卡住 MCDR 的主线程。
"""

from __future__ import annotations

from typing import List, Optional, Tuple

import time

from mcdreforged.api.all import CommandSource, GreedyText, Literal, RColor, RText, RTextList

from .config import ConfigError, save_config
from .constants import APP_NAME
from .engine import RoundReport, SyncAborted, SyncBusy


def _line(text: str, color: Optional[RColor] = None) -> RText:
    item = RText(text)
    if color is not None:
        item.set_color(color)
    return item


def _join(lines: List[RText]) -> RTextList:
    """把多行文本拼成一个带换行的 RTextList。

    ``RTextList`` 的 ``to_plain_text()`` 只是顺序拼接，不会自动换行，所以换行符
    必须显式塞进去；否则聊天栏里所有行会挤成一整行。
    """
    block = RTextList()
    for index, line in enumerate(lines):
        if index:
            block.append("\n")
        block.append(line)
    return block


def _deny(level: int):
    def predicate(source: CommandSource) -> bool:
        return source.has_permission(level)

    def message(source: CommandSource) -> RText:
        return _line(f"权限不足：该命令需要 MCDR 权限等级 {level}", RColor.red).set_color(RColor.red)

    return predicate, message


def _parse_flags(raw: str) -> Tuple[bool, bool, bool]:
    """解析 ``--dry-run`` / ``--full`` / ``--force``，未知参数忽略。"""
    dry_run = full = force = False
    for token in (raw or "").split():
        if token in ("--dry-run", "-n", "dry"):
            dry_run = True
        elif token in ("--full", "full"):
            full = True
        elif token in ("--force", "force", "-f"):
            force = True
    return dry_run, full, force


# --------------------------------------------------------------------------- #
# 输出
# --------------------------------------------------------------------------- #


def render_plan(plugin, report: RoundReport) -> RTextList:
    lines = [_line(f"[{APP_NAME}] 计划摘要（{report.duration:.2f}s）", RColor.aqua)]

    entries = list(report.plans.items()) if report.plans else []
    if not entries and report.plan is not None:
        entries = [("default", report.plan)]
    if not entries:
        lines.append(_line("  （没有生成计划）", RColor.gray))
        return _join(lines)

    multi = len(entries) > 1
    for name, plan in entries:
        prefix = f"  [{name}] " if multi else "  "
        lines.append(
            _line(
                f"{prefix}新增 {plan.add_count()} 文件 / {_mb(plan.add_bytes())}，"
                f"修改 {plan.modify_count()} 文件 / {_mb(plan.modify_bytes())}，"
                f"删除 {plan.delete_count()} 文件 / {_mb(plan.delete_bytes())}"
            )
        )
        lines.append(
            _line(
                f"{prefix}未变（跳过）{plan.unchanged_files()} 文件 / {_mb(plan.unchanged_bytes())}，"
                f"目录操作 {plan.dir_count()}",
                RColor.gray,
            )
        )
        if plugin.config.log.verbose_plan:
            for set_plan in plan.sets:
                for op in set_plan.ops[:20]:
                    lines.append(
                        _line(
                            f"      {op.kind:<7} {name}:{set_plan.set_name}:{op.path}  [{op.reason}]",
                            RColor.gray,
                        )
                    )
                if len(set_plan.ops) > 20:
                    lines.append(_line(f"      …… 还有 {len(set_plan.ops) - 20} 项", RColor.gray))

    if report.scan_errors:
        lines.append(_line(f"  扫描错误 {report.scan_errors} 项（相关路径本轮不参与删除）", RColor.yellow))
    for warning in report.warnings[:5]:
        lines.append(_line(f"  ! {warning}", RColor.yellow))
    if report.abort_reason:
        lines.append(_line(f"  已中止：{report.abort_reason}", RColor.red))
    if report.error:
        lines.append(_line(f"  失败：{report.error}", RColor.red))
    return _join(lines)


def render_report(report: RoundReport) -> RTextList:
    if report.abort_reason:
        return _join(
            [
                _line(f"[{APP_NAME}] 本轮中止：{report.abort_reason}", RColor.red),
                _line("  镜像一个字节都没有改动。", RColor.gray),
            ]
        )
    if report.error:
        return _join([_line(f"[{APP_NAME}] 同步失败：{report.error}", RColor.red)])

    head_color = RColor.green if report.ok() else RColor.yellow
    lines = [
        _line(
            f"[{APP_NAME}] 同步完成：新增/修改 {report.applied}，删除 {report.deleted}，"
            f"写入 {_mb(report.bytes_written)}，耗时 {report.duration:.2f}s",
            head_color,
        )
    ]
    if len(report.targets) > 1:
        for tr in report.targets:
            if tr.skipped:
                lines.append(_line(f"  [{tr.name}] 跳过（没有可用的 set）", RColor.gray))
                continue
            lines.append(
                _line(
                    f"  [{tr.name}] 写 {tr.applied} / 删 {tr.deleted} / 失败 {tr.failed}"
                    f" / {_mb(tr.bytes_written)}",
                    RColor.gray,
                )
            )
    else:
        detail = (
            f"  内容未变（仅更新索引）{report.unchanged_after_hash}，"
            f"目录 {report.dirs}，延后 {report.deferred}，失败 {report.failed}"
        )
        if report.regions_delta:
            detail += (
                f"\n  region 增量 {report.regions_delta} 个：写入 {report.chunks_written} 个 chunk，"
                f"复用 {report.chunks_reused} 个"
            )
        lines.append(_line(detail, RColor.gray))

    if report.cancelled:
        lines.append(_line("  本轮被取消，已完成的部分保持原样，下一轮会继续收敛。", RColor.yellow))
    for failure in report.failures[:5]:
        lines.append(_line(f"  ✗ {failure[0]}:{failure[1]} — {failure[2]}", RColor.red))
    if len(report.failures) > 5:
        lines.append(_line(f"  …… 另有 {len(report.failures) - 5} 个失败", RColor.red))
    if report.hint:
        lines.append(_line(f"  {report.hint}", RColor.gray))
    return _join(lines)


def _mb(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{value:.1f} GB"


# --------------------------------------------------------------------------- #
# 命令处理器
# --------------------------------------------------------------------------- #


def _help(plugin, source: CommandSource) -> None:
    lines = [
        _line(f"{APP_NAME} v{plugin.version} —— 增量镜像同步", RColor.aqua),
        _line("!!mu sync [--dry-run] [--full] [--force]   执行一次同步", RColor.gray),
        _line("!!mu diff                                  只规划不执行（摘要）", RColor.gray),
        _line("!!mu status                                查看索引/上次同步/待重试", RColor.gray),
        _line("!!mu verify [--deep]                       校验源/索引/镜像是否一致", RColor.gray),
        _line("!!mu history [条数]                        查看最近的同步记录", RColor.gray),
        _line("!!mu cancel                                取消进行中的同步", RColor.gray),
        _line("!!mu schedule on|off                       开关自动同步", RColor.gray),
        _line("!!mu schedule set <时长，如 30m/2h/3d>     设置自动同步间隔（上限 7 天）", RColor.gray),
        _line("!!mu index rebuild                         丢弃索引，下一轮全量重建", RColor.gray),
        _line("!!mu reload                                重新读取 config.json", RColor.gray),
    ]
    source.reply(_join(lines))


def _sync(plugin, source: CommandSource, raw: str = "") -> None:
    dry_run, full, force = _parse_flags(raw)
    plugin.start_sync(source, dry_run=dry_run, full=full, force=force)


def _diff(plugin, source: CommandSource) -> None:
    plugin.start_sync(source, dry_run=True)


def render_verify(report) -> RTextList:
    head_color = RColor.green if report.ok() else RColor.yellow
    lines = [
        _line(
            f"[{APP_NAME}] 校验完成（{'深校验' if report.deep else '浅校验'}）："
            f"检查 {report.files_checked} 文件 / {_mb(report.bytes_checked)}，"
            f"问题 {len(report.issues)} 项，耗时 {report.duration:.2f}s",
            head_color,
        )
    ]
    for issue in report.issues[:10]:
        lines.append(_line(f"  ✗ {issue.line()}", RColor.red))
    if len(report.issues) > 10:
        lines.append(_line(f"  …… 另有 {len(report.issues) - 10} 项", RColor.red))
    for warning in report.warnings[:5]:
        lines.append(_line(f"  ! {warning}", RColor.yellow))
    if report.ok():
        lines.append(_line("  源、索引、镜像一致。", RColor.gray))
    return _join(lines)


def _verify(plugin, source: CommandSource, raw: str = "") -> None:
    deep = "--deep" in (raw or "")
    plugin.start_verify(source, deep=deep)


def _history(plugin, source: CommandSource, raw: str = "") -> None:
    limit = 10
    token = (raw or "").strip()
    if token:
        try:
            limit = max(1, min(50, int(token)))
        except ValueError:
            source.reply(_line(f"看不懂的数量：{token!r}（示例：!!mu history 20）", RColor.red))
            return
    records = plugin.history(limit)
    if not records:
        source.reply(_line("还没有同步历史。", RColor.gray))
        return
    lines = [_line(f"{APP_NAME} 最近 {len(records)} 次同步", RColor.aqua)]
    for record in records:
        when = time.strftime("%m-%d %H:%M:%S", time.localtime(int(record.get("finished_at", 0)) / 1000))
        mark = "✓" if record.get("ok") else "✗"
        color = RColor.gray if record.get("ok") else RColor.yellow
        lines.append(
            _line(
                f"  {mark} {when} [{record.get('target', '?')}] {record.get('trigger', '?')}"
                f" {record.get('summary', '')}（{record.get('duration', 0)}s）",
                color,
            )
        )
    source.reply(_join(lines))


def _status(plugin, source: CommandSource) -> None:
    lines = [_line(f"{APP_NAME} 状态", RColor.aqua)]
    for entry in plugin.status_lines():
        color = RColor.yellow if entry.startswith(("⚠", "!")) else RColor.gray
        lines.append(_line(f"  {entry}", color))
    source.reply(_join(lines))


def _cancel(plugin, source: CommandSource) -> None:
    if not plugin.engine.is_busy():
        source.reply(_line("当前没有正在进行的同步。", RColor.gray))
        return
    plugin.engine.cancel()
    source.reply(_line("已请求取消，正在进行中的文件操作会先完成。", RColor.yellow))


def _reload(plugin, source: CommandSource) -> None:
    source.reply(plugin.reload_config())


def _index_rebuild(plugin, source: CommandSource) -> None:
    plugin.index.wipe()
    source.reply(_line("索引已清空，下一轮将全量重建（会重读所有文件）。", RColor.yellow))


def _schedule(plugin, source: CommandSource, raw: str) -> None:
    tokens = (raw or "").split()
    if not tokens:
        source.reply(_line("用法：!!mu schedule on|off|set <时长>", RColor.red))
        return
    action = tokens[0].lower()
    if action in ("on", "off"):
        plugin.set_auto_sync(action == "on")
        source.reply(_line(f"自动同步已{'开启' if action == 'on' else '关闭'}。", RColor.green))
        return
    if action == "set":
        if len(tokens) < 2:
            source.reply(_line("用法：!!mu schedule set 30m / 2h / 3d（上限 7 天）", RColor.red))
            return
        try:
            seconds = _parse_duration(tokens[1])
        except ValueError as exc:
            source.reply(_line(str(exc), RColor.red))
            return
        effective = plugin.set_interval(seconds)
        source.reply(_line(f"自动同步间隔已设为 {_human(effective)}。", RColor.green))
        return
    source.reply(_line("用法：!!mu schedule on|off|set <时长>", RColor.red))


def _parse_duration(raw: str) -> int:
    text = raw.strip().lower()
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    try:
        if text and text[-1] in units:
            value = float(text[:-1])
            seconds = int(value * units[text[-1]])
        else:
            seconds = int(float(text))
    except ValueError as exc:
        raise ValueError(f"看不懂的时长：{raw!r}（示例：30m、2h、3d）") from exc
    if seconds < 60:
        raise ValueError("间隔不能小于 60 秒")
    return seconds


def _human(seconds: int) -> str:
    if seconds % 86400 == 0:
        return f"{seconds // 86400} 天"
    if seconds % 3600 == 0:
        return f"{seconds // 3600} 小时"
    if seconds % 60 == 0:
        return f"{seconds // 60} 分钟"
    return f"{seconds} 秒"


# --------------------------------------------------------------------------- #
# 注册
# --------------------------------------------------------------------------- #


def register(server, plugin) -> None:
    perm_2, msg_2 = _deny(2)
    perm_3, msg_3 = _deny(3)

    root = Literal(plugin.command_prefix).runs(lambda src: _help(plugin, src))

    sync = Literal("sync").requires(perm_2, msg_2).runs(lambda src: _sync(plugin, src))
    sync.then(Literal("dry").runs(lambda src: _sync(plugin, src, "--dry-run")))
    sync.then(GreedyText("options").runs(lambda src, ctx: _sync(plugin, src, ctx["options"])))
    root.then(sync)

    root.then(Literal("diff").requires(perm_2, msg_2).runs(lambda src: _diff(plugin, src)))
    root.then(Literal("status").runs(lambda src: _status(plugin, src)))

    verify = Literal("verify").requires(perm_2, msg_2).runs(lambda src: _verify(plugin, src))
    verify.then(GreedyText("options").runs(lambda src, ctx: _verify(plugin, src, ctx["options"])))
    root.then(verify)

    history = Literal("history").runs(lambda src: _history(plugin, src))
    history.then(GreedyText("limit").runs(lambda src, ctx: _history(plugin, src, ctx["limit"])))
    root.then(history)
    root.then(Literal("cancel").requires(perm_2, msg_2).runs(lambda src: _cancel(plugin, src)))
    root.then(
        Literal("reload")
        .requires(perm_3, msg_3)
        .runs(lambda src: _reload(plugin, src))
    )

    index_node = Literal("index").requires(perm_3, msg_3)
    index_node.then(Literal("rebuild").runs(lambda src: _index_rebuild(plugin, src)))
    root.then(index_node)

    schedule = Literal("schedule").requires(perm_3, msg_3)
    schedule.then(Literal("on").runs(lambda src: _schedule(plugin, src, "on")))
    schedule.then(Literal("off").runs(lambda src: _schedule(plugin, src, "off")))
    schedule.then(GreedyText("args").runs(lambda src, ctx: _schedule(plugin, src, ctx["args"])))
    root.then(schedule)

    root.then(Literal("help").runs(lambda src: _help(plugin, src)))

    server.register_command(root)
    server.register_help_message(plugin.command_prefix, f"{APP_NAME} 增量镜像同步")
