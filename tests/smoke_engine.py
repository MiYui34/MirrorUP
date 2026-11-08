"""端到端冒烟测试：不依赖 MCDR，直接用临时目录跑一轮真实同步。

    python tests/smoke_engine.py
"""

from __future__ import annotations

import copy
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mirror_up.config import DEFAULT_CONFIG, parse_config, validate_config  # noqa: E402
from mirror_up.engine import SyncEngine  # noqa: E402
from mirror_up.flush import NullSourceFlusher  # noqa: E402
from mirror_up.index import Index  # noqa: E402


def make_world(root: Path) -> None:
    (root / "world" / "region").mkdir(parents=True)
    (root / "world" / "data").mkdir(parents=True)
    (root / "mods").mkdir()
    (root / "world" / "level.dat").write_bytes(os.urandom(64))
    (root / "world" / "region" / "r.0.0.mca").write_bytes(os.urandom(4096 * 3))
    (root / "world" / "region" / "r.0.1.mca").write_bytes(os.urandom(4096 * 2))
    (root / "world" / "data" / "raids.dat").write_bytes(b"raids" * 200)
    (root / "mods" / "fabric-api.jar").write_bytes(b"jar" * 5000)
    (root / "mods" / "lithium.jar").write_bytes(b"lithium" * 3000)


def build(tmp: Path):
    src = tmp / "src"
    dst = tmp / "dst"
    make_world(src)
    raw = copy.deepcopy(DEFAULT_CONFIG)
    raw["source"]["root"] = str(src)
    raw["target"]["root"] = str(dst)
    cfg = parse_config(raw)
    cfg.source_flush.enabled = False
    warnings = validate_config(cfg, tmp)
    return src, dst, cfg, warnings


def check(label: str, condition: bool, extra: str = "") -> None:
    print(("  OK   " if condition else "  FAIL ") + label + (f"  [{extra}]" if extra else ""))
    if not condition:
        raise SystemExit(1)


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="mirrorup-smoke-"))
    print(f"workdir: {tmp}")
    try:
        src, dst, cfg, warnings = build(tmp)
        for w in warnings:
            print(f"  warn: {w}")

        index = Index(tmp / "index.sqlite3")
        engine = SyncEngine(cfg, tmp, index, flusher=NullSourceFlusher())

        print("\n[1] dry-run 首次")
        report = engine.run(dry_run=True, trigger="test")
        plan = report.plan
        check("dry-run 无错误", not report.error and not report.abort_reason, report.abort_reason or report.error)
        check("dry-run 不写目标", not dst.exists())
        check("计划包含新增", plan.add_count() >= 4, f"add={plan.add_count()} mkdir={plan.dir_count()}")
        check("计划没有删除", plan.delete_count() == 0)

        print("\n[2] 首次真实同步")
        report = engine.run(trigger="test")
        check("同步成功", report.ok(), report.abort_reason or report.error)
        check("写入了 6 个文件", report.applied == 6, f"applied={report.applied} dirs={report.dirs}")
        check("目标文件到位", (dst / "world" / "region" / "r.0.0.mca").exists())
        check(
            "内容一致",
            (dst / "world" / "region" / "r.0.0.mca").read_bytes()
            == (src / "world" / "region" / "r.0.0.mca").read_bytes(),
        )
        total_files, total_bytes = index.total_stats()
        check("索引已记录", total_files >= 6, f"rows={total_files} bytes={total_bytes}")
        check("无临时文件残留", not list(dst.rglob("*.mirrorup-*.tmp")))

        print("\n[3] 第二轮：没有变更")
        report = engine.run(trigger="test")
        check("同步成功", report.ok(), report.abort_reason or report.error)
        check("零写入", report.applied == 0, f"applied={report.applied}")
        check("零删除", report.deleted == 0)
        check("计划无变更", not report.plan.has_changes())

        print("\n[4] 修改一个文件")
        target = src / "world" / "data" / "raids.dat"
        target.write_bytes(b"changed" * 300)
        report = engine.run(trigger="test")
        check("只同步 1 个文件", report.applied == 1, f"applied={report.applied}")
        check("目标已更新", (dst / "world" / "data" / "raids.dat").read_bytes() == target.read_bytes())

        print("\n[5] 删除一个文件")
        (src / "mods" / "fabric-api.jar").unlink()
        report = engine.run(trigger="test")
        check("删除 1 个文件", report.deleted == 1, f"deleted={report.deleted}")
        check("目标已删除", not (dst / "mods" / "fabric-api.jar").exists())
        check("同目录其他文件保留", (dst / "mods" / "lithium.jar").exists())

        print("\n[6] 删空整个 set：默认中止，--force 才允许")
        (src / "mods" / "lithium.jar").unlink()
        report = engine.run(trigger="test")
        check("空源保护触发", bool(report.abort_reason), report.abort_reason)
        check("中止时不删目标", (dst / "mods" / "lithium.jar").exists())
        report = engine.run(force=True, trigger="test")
        check("force 后删除", report.deleted == 1, f"deleted={report.deleted}")
        check("目标目录已清空（set 根保留）", not any((dst / "mods").iterdir()))

        print("\n[7] mtime 回退但大小未变：靠 mtime 差异仍能同步")
        victim = src / "world" / "region" / "r.0.1.mca"
        old = time.time() - 86400
        victim.write_bytes(os.urandom(4096 * 2))  # 长度不变
        os.utime(victim, (old, old))
        report = engine.run(trigger="test")
        check("仍能同步", report.applied == 1, f"applied={report.applied}")
        check("内容已更新", (dst / "world" / "region" / "r.0.1.mca").read_bytes() == victim.read_bytes())

        print("\n[8] 还原嫌疑：大小变了且 mtime 比上次同步还早")
        victim.write_bytes(os.urandom(4096 * 5))  # 长度变化
        os.utime(victim, (old, old))
        report = engine.run(trigger="test")
        check("仍能同步", report.applied == 1, f"applied={report.applied}")
        check(
            "触发全量哈希复核",
            any(sp.forced_full_hash for sp in report.plan.sets),
            str([sp.forced_full_hash for sp in report.plan.sets]),
        )

        print("\n[9] 目标被外部改动后，下一轮应恢复")
        (dst / "world" / "data" / "raids.dat").write_bytes(b"tampered")
        report = engine.run(trigger="test")
        check("检测到大小不同并恢复", report.applied == 1, f"applied={report.applied}")
        check("内容已恢复", (dst / "world" / "data" / "raids.dat").read_bytes() == target.read_bytes())

        print("\n[10] 全新空源首轮")
        empty_src = tmp / "empty"
        (empty_src / "world").mkdir(parents=True)
        (empty_src / "mods").mkdir()
        raw = copy.deepcopy(DEFAULT_CONFIG)
        raw["source"]["root"] = str(empty_src)
        raw["target"]["root"] = str(tmp / "dst2")
        cfg2 = parse_config(raw)
        cfg2.source_flush.enabled = False
        index2 = Index(tmp / "index2.sqlite3")
        engine2 = SyncEngine(cfg2, tmp, index2, flusher=NullSourceFlusher())
        report = engine2.run(dry_run=True)
        check("空目录首轮不中止", not report.abort_reason, report.abort_reason)
        report = engine2.run(trigger="test")
        check("空目录真实同步成功", report.ok(), report.abort_reason or report.error)
        check("没有任何写入", report.applied == 0)

        print("\n全部通过")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()



