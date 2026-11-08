"""命令层冒烟测试：不启动 MCDR，用假 server / 假 Source 跑通注册与渲染。

    python tests/smoke_commands.py
"""

from __future__ import annotations

import copy
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mirror_up import commands  # noqa: E402
from mirror_up.config import DEFAULT_CONFIG, parse_config, validate_config  # noqa: E402
from mirror_up.engine import RoundReport, SyncEngine  # noqa: E402
from mirror_up.flush import NullSourceFlusher  # noqa: E402
from mirror_up.index import Index  # noqa: E402


def check(label: str, condition: bool, extra: str = "") -> None:
    print(("  OK   " if condition else "  FAIL ") + label + (f"  [{extra}]" if extra else ""))
    if not condition:
        raise SystemExit(1)


class FakeSource:
    def __init__(self) -> None:
        self.messages: list = []

    def reply(self, message, **kwargs) -> None:
        self.messages.append(message)

    def has_permission(self, level: int) -> bool:
        return True

    def get_permission_level(self) -> int:
        return 4

    def text(self) -> str:
        out = []
        for item in self.messages:
            try:
                out.append(item.to_plain_text())
            except AttributeError:
                out.append(str(item))
        return "\n".join(out)


class FakeServer:
    def __init__(self) -> None:
        self.root = None
        self.help = []

    def register_command(self, root, *, allow_duplicates: bool = False) -> None:
        self.root = root

    def register_help_message(self, prefix, message, permission: int = 0) -> None:
        self.help.append((prefix, message, permission))


class FakeEngine:
    def __init__(self) -> None:
        self.busy = False
        self.cancelled = False

    def is_busy(self) -> bool:
        return self.busy

    def cancel(self) -> None:
        self.cancelled = True

    def describe(self):
        return ["空闲", "索引：0 个文件 / 0.0 MB"]


class FakeIndex:
    def __init__(self) -> None:
        self.wiped = False

    def wipe(self) -> None:
        self.wiped = True


class FakePlugin:
    def __init__(self, cfg) -> None:
        self.version = "0.1.0"
        self.command_prefix = "!!mu"
        self.config = cfg
        self.engine = FakeEngine()
        self.index = FakeIndex()
        self.calls: list = []

    def start_sync(self, source, dry_run=False, full=False, force=False) -> None:
        self.calls.append(("sync", dry_run, full, force))

    def status_lines(self):
        return self.engine.describe()

    def reload_config(self):
        self.calls.append(("reload",))
        return "reloaded"

    def set_auto_sync(self, enabled: bool) -> None:
        self.calls.append(("auto", enabled))

    def set_interval(self, seconds: int) -> int:
        self.calls.append(("interval", seconds))
        return seconds


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="mirrorup-cmd-"))
    print(f"workdir: {tmp}")
    try:
        src = tmp / "src"
        dst = tmp / "dst"
        (src / "world" / "region").mkdir(parents=True)
        (src / "world" / "level.dat").write_bytes(b"level")
        (src / "world" / "region" / "r.0.0.mca").write_bytes(b"x" * 8192)

        raw = copy.deepcopy(DEFAULT_CONFIG)
        raw["source"]["root"] = str(src)
        raw["target"]["root"] = str(dst)
        raw["sets"] = [s for s in raw["sets"] if s["name"] == "world"]
        cfg = parse_config(raw)
        cfg.source_flush.enabled = False
        cfg.log.verbose_plan = True
        validate_config(cfg, tmp)

        print("[1] 注册命令树")
        plugin = FakePlugin(cfg)
        server = FakeServer()
        commands.register(server, plugin)
        check("register 未抛异常", server.root is not None)
        check("注册了帮助信息", len(server.help) == 1, server.help[0][0] if server.help else "")

        print("[2] 参数解析")
        check("--dry-run", commands._parse_flags("--dry-run")[0] is True)
        check("--full --force", commands._parse_flags("x --full --force") == (False, True, True))
        check("30m", commands._parse_duration("30m") == 1800)
        check("2h", commands._parse_duration("2h") == 7200)
        check("3d", commands._parse_duration("3d") == 259200)
        try:
            commands._parse_duration("10s")
        except ValueError:
            check("10s 被拒绝（下限 60 秒）", True)
        else:
            check("10s 被拒绝（下限 60 秒）", False)

        print("[3] 处理器直接调用")
        source = FakeSource()
        commands._help(plugin, source)
        check("help 有 9 行", len(source.messages) == 1 and "!!mu sync" in source.text())

        source = FakeSource()
        commands._schedule(plugin, source, "set 30m")
        check("schedule set 落到 plugin", ("interval", 1800) in plugin.calls, str(plugin.calls[-1]))

        source = FakeSource()
        commands._status(plugin, source)
        check("status 有输出", "空闲" in source.text())

        source = FakeSource()
        commands._sync(plugin, source, "--dry-run --force")
        check("sync 参数透传", plugin.calls[-1] == ("sync", True, False, True), str(plugin.calls[-1]))

        print("[4] 真实引擎 + 渲染")
        index = Index(tmp / "index.sqlite3")
        engine = SyncEngine(cfg, tmp, index, flusher=NullSourceFlusher(), log=None)
        report = engine.run(dry_run=True)
        lines = commands.render_plan(plugin, report)
        text = lines.to_plain_text()
        check("计划里有新增数量", f"新增 {report.plan.add_count()}" in text, text[:60])
        check("多行渲染带换行", "\n" in text)
        check("dry-run 没写目标", not dst.exists())

        report = engine.run()
        check("真实同步成功", report.ok(), f"state={report.state} applied={report.applied}")
        check("目标已生成", (dst / "world" / "region" / "r.0.0.mca").exists())
        rendered = commands.render_report(report).to_plain_text()
        check("完成报告包含写入量", "写入" in rendered)

        print("[5] 导入 MCDR 入口模块")
        import mirror_up.entry as entry  # noqa: E402

        check("entry 有 on_load/on_unload", callable(entry.on_load) and callable(entry.on_unload))
        check("entry 暴露 MirrorUP", hasattr(entry, "MirrorUP"))

        # MCDR 的目录插件：插件 id 必须等于入口包名，且插件根目录不能有 __init__.py
        import mirror_up

        check("包级导出 on_load", callable(getattr(mirror_up, "on_load", None)))
        check("插件根目录没有 __init__.py", not (Path(__file__).resolve().parents[1] / "__init__.py").exists())

        print("全部通过")
    finally:
        import shutil

        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
