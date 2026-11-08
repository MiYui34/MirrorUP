"""只读校验：把「源 / 索引 / 镜像」三方对一遍，报告不一致。

两种深度：

* **浅校验**（默认，``deep=False``）：只 stat。检查索引是否还描述得清源端
  （源被删/被改），以及镜像是否还是索引以为的样子（镜像被改/被删）。不做
  任何哈希，几秒内跑完。
* **深校验**（``--deep``）：在浅校验基础上读内容算哈希。普通文件比哈希；
  ``.mca`` 比*逻辑* chunk 哈希（目标文件因为 inplace 碎片会比源大，比整个
  文件的字节哈希没有意义）。跨机时哈希在源端由 agent 计算，不传输文件内容。

校验**只读**：不改索引、不改目标、不写 journal。它回答的是“现在两边一致吗”，
修复手段仍然是下一次 ``!!mu sync``。

深校验的读量是整个世界（源端读一遍、目标端读一遍），10GB 的世界要按分钟算，
这是它只在 ``!!mu verify --deep`` 时跑的原因。
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from .config import Config, set_source_path, set_target_path_in, target_path
from .excludes import GlobMatcher
from .hashing import hash_file, is_region_name
from .index import DEFAULT_TARGET_ID, KIND_DIR, KIND_FILE, Index
from .region import RegionFormatError, logical_digest, scan_region
from .scanner import ScanResult, ScannedEntry, scan_tree
from .source import Source, SourceError, SourceUnavailable, build_source


@dataclass
class VerifyIssue:
    target: str
    set_name: str
    path: str
    kind: str
    detail: str = ""

    def line(self) -> str:
        where = f"{self.target}/{self.set_name}" if self.target else self.set_name
        suffix = f"（{self.detail}）" if self.detail else ""
        return f"[{where}] {self.path}: {self.kind}{suffix}"


@dataclass
class VerifyReport:
    deep: bool = False
    started_at: float = 0.0
    finished_at: float = 0.0
    files_checked: int = 0
    bytes_checked: int = 0
    hashes: int = 0
    issues: List[VerifyIssue] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def duration(self) -> float:
        return max(0.0, self.finished_at - self.started_at)

    def ok(self) -> bool:
        return not self.issues


def verify(
    config: Config,
    base_dir: Path,
    index: Index,
    deep: bool = False,
    target_names: Optional[Sequence[str]] = None,
    max_issues: int = 500,
) -> VerifyReport:
    base_dir = Path(base_dir)
    report = VerifyReport(deep=deep, started_at=time.time())
    matcher = GlobMatcher(config.exclude)

    targets = [
        t
        for t in config.enabled_targets()
        if target_names is None or t.name in target_names
    ]
    if not targets:
        report.warnings.append("没有启用的镜像目标")
        report.finished_at = time.time()
        return report

    target_ids = index.load_targets()
    source_scans: Dict[str, ScanResult] = {}
    sources: Dict[str, Source] = {}

    try:
        for item in config.enabled_sets():
            if (config.source.transport or "local").lower() != "agent":
                path = set_source_path(config, item, base_dir)
                if not path.is_dir():
                    report.warnings.append(f"set {item.name!r} 的源目录不存在，跳过：{path}")
                    continue
            try:
                src = build_source(config, item, base_dir)
            except SourceError as exc:
                report.warnings.append(f"set {item.name!r} 的源不可用，跳过：{exc}")
                continue
            try:
                source_scans[item.name] = src.scan(matcher)
            except SourceUnavailable as exc:
                raise VerifyError(f"源端不可用：{exc}") from exc
            sources[item.name] = src

        if not sources:
            report.warnings.append("没有任何可用的源 set")
            report.finished_at = time.time()
            return report

        for target in targets:
            target_id = target_ids.get(target.name, DEFAULT_TARGET_ID)
            dst_root = target_path(target, base_dir)
            for item in config.enabled_sets():
                src = sources.get(item.name)
                scan = source_scans.get(item.name)
                if src is None or scan is None:
                    continue
                if scan.root_missing:
                    report.warnings.append(f"{target.name}/{item.name}: 源根不可读，跳过")
                    continue

                dst = set_target_path_in(config, target, item, base_dir)
                target_scan = scan_tree(dst, matcher) if dst.is_dir() else None
                if target_scan is not None and (target_scan.root_missing or target_scan.errors):
                    report.warnings.append(
                        f"{target.name}/{item.name}: 镜像侧扫描不完整"
                        f"（{len(target_scan.errors)} 个错误），跳过该 set"
                    )
                    continue
                target_entries = target_scan.entries if target_scan is not None else {}

                rows = index.load_files(_set_id(index, item.name), target_id)
                _check_set(
                    report,
                    deep,
                    target.name,
                    item.name,
                    src,
                    scan,
                    target_entries,
                    rows,
                    dst,
                    max_issues,
                )
    finally:
        for src in sources.values():
            try:
                src.close()
            except Exception:  # noqa: BLE001 - 关闭失败不影响校验结果
                pass
        report.finished_at = time.time()

    return report


class VerifyError(Exception):
    """校验无法进行（源不可用等）。"""


def _set_id(index: Index, name: str) -> int:
    # verify 不改索引：只查已经登记的 set id，没有就返回 0（等价于“没有索引”）。
    try:
        return int(index.load_set_ids().get(name, 0))
    except Exception:  # noqa: BLE001 - 索引损坏时退化为无索引
        return 0


def _check_set(
    report: VerifyReport,
    deep: bool,
    target_name: str,
    set_name: str,
    src: Source,
    scan: ScanResult,
    target_entries: Dict[str, ScannedEntry],
    rows: Dict[str, object],
    dst_root: Path,
    max_issues: int,
) -> None:
    entries = scan.entries
    seen: set[str] = set()

    for path, row in rows.items():
        seen.add(path)
        entry = entries.get(path)
        if entry is None:
            report.issues.append(
                _issue(report, target_name, set_name, path, "source-missing", "源端已没有这个文件")
            )
            continue
        if entry.kind == KIND_DIR:
            continue
        report.files_checked += 1
        report.bytes_checked += entry.size
        if getattr(row, "kind", KIND_FILE) == KIND_DIR:
            report.issues.append(
                _issue(report, target_name, set_name, path, "kind-changed", "索引是目录，源端是文件")
            )
            continue
        if int(row.size) != entry.size:
            report.issues.append(
                _issue(
                    report,
                    target_name,
                    set_name,
                    path,
                    "index-stale",
                    f"索引 {row.size}B ≠ 源 {entry.size}B",
                )
            )
            continue

        target_entry = target_entries.get(path)
        if target_entry is None:
            report.issues.append(
                _issue(report, target_name, set_name, path, "target-missing", "镜像上没有这个文件")
            )
        elif target_entry.kind != KIND_DIR and target_entry.size != entry.size and not is_region_name(
            Path(path).name
        ):
            report.issues.append(
                _issue(
                    report,
                    target_name,
                    set_name,
                    path,
                    "target-size",
                    f"镜像 {target_entry.size}B ≠ 源 {entry.size}B",
                )
            )

        if not deep:
            continue

        digest = row.hash
        try:
            current = src.digest(path)
        except SourceError as exc:
            report.issues.append(
                _issue(report, target_name, set_name, path, "source-unreadable", str(exc))
            )
            continue
        if current is None:
            report.issues.append(
                _issue(report, target_name, set_name, path, "source-missing", "深校验时源文件消失")
            )
            continue
        report.hashes += 1
        src_hash, _size, _mtime = current

        if is_region_name(Path(path).name):
            _check_region(
                report, target_name, set_name, path, src, dst_root, digest, max_issues
            )
        elif digest is not None and bytes(src_hash) != bytes(digest):
            report.issues.append(
                _issue(
                    report,
                    target_name,
                    set_name,
                    path,
                    "source-changed",
                    "源内容与索引记录的哈希不一致",
                )
            )

        if target_entry is not None and is_region_name(Path(path).name):
            continue
        target_path_full = dst_root.joinpath(*path.split("/"))
        if target_entry is not None and target_path_full.is_file():
            try:
                target_hash = hash_file(target_path_full)
            except OSError as exc:
                report.issues.append(
                    _issue(report, target_name, set_name, path, "target-unreadable", str(exc))
                )
            else:
                if target_hash != bytes(src_hash):
                    report.issues.append(
                        _issue(
                            report,
                            target_name,
                            set_name,
                            path,
                            "target-content",
                            "镜像内容与源内容不一致",
                        )
                    )

    for path, entry in entries.items():
        if entry.kind == KIND_DIR or path in seen:
            continue
        report.issues.append(
            _issue(
                report,
                target_name,
                set_name,
                path,
                "source-new",
                "索引里没有，下一轮同步会新增",
            )
        )

    if len(report.issues) >= max_issues:
        report.warnings.append(f"问题数量达到上限 {max_issues}，后续未继续记录")


def _check_region(
    report: VerifyReport,
    target_name: str,
    set_name: str,
    path: str,
    src: Source,
    dst_root: Path,
    index_hash: Optional[bytes],
    max_issues: int,
) -> None:
    try:
        source_scan = src.scan_region(path, None, None)
    except (SourceError, RegionFormatError) as exc:
        report.issues.append(
            _issue(report, target_name, set_name, path, "source-unreadable", str(exc))
        )
        return
    logical = logical_digest(source_scan)

    if index_hash is not None and bytes(index_hash) != bytes(logical):
        report.issues.append(
            _issue(
                report,
                target_name,
                set_name,
                path,
                "source-changed",
                "源 region 的逻辑 chunk 哈希与索引不一致",
            )
        )

    target_file = dst_root.joinpath(*path.split("/"))
    if not target_file.is_file():
        return
    try:
        target_scan = scan_region(target_file)
    except (OSError, RegionFormatError) as exc:
        report.issues.append(
            _issue(report, target_name, set_name, path, "target-unreadable", str(exc))
        )
        return
    if logical_digest(target_scan) != logical:
        report.issues.append(
            _issue(
                report,
                target_name,
                set_name,
                path,
                "target-content",
                "镜像 region 的逻辑 chunk 哈希与源不一致",
            )
        )


def _issue(
    report: VerifyReport, target: str, set_name: str, path: str, kind: str, detail: str
) -> VerifyIssue:
    if len(report.issues) >= 500:
        report.warnings.append("问题数量过多，已截断")
    return VerifyIssue(target=target, set_name=set_name, path=path, kind=kind, detail=detail)
