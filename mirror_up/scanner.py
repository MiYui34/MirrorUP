"""目录扫描（L1 层）。

产出 set 内全部文件与目录的 ``(相对路径, size, mtime_ns)``。这一步**不读文件内容**，
只做目录遍历 + stat；因此无论世界多大，扫描成本都是一次元数据遍历。

被排除规则剪掉的目录会整棵跳过（例如 ``logs/**``）。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Tuple

from .excludes import GlobMatcher
from .index import KIND_DIR, KIND_FILE


@dataclass
class ScannedEntry:
    path: str
    kind: int
    size: int
    mtime_ns: int


@dataclass
class ScanResult:
    entries: Dict[str, ScannedEntry] = field(default_factory=dict)
    #: (相对路径, 错误信息) —— 这些路径本轮**不参与删除判定**，避免因读不到而误删
    errors: List[Tuple[str, str]] = field(default_factory=list)
    pruned_dirs: int = 0
    root_missing: bool = False


def _join(rel_dir: str, name: str) -> str:
    return f"{rel_dir}/{name}" if rel_dir else name


def scan_tree(root: Path, matcher: GlobMatcher) -> ScanResult:
    root = Path(root)
    result = ScanResult()
    if not root.exists():
        result.root_missing = True
        return result
    if not root.is_dir():
        result.errors.append(("", f"不是目录：{root}"))
        result.root_missing = True
        return result

    stack: List[Tuple[Path, str]] = [(root, "")]
    while stack:
        abs_dir, rel_dir = stack.pop()
        try:
            with os.scandir(abs_dir) as it:
                items = list(it)
        except OSError as exc:
            result.errors.append((rel_dir, f"读取目录失败：{exc}"))
            continue

        for entry in items:
            rel = _join(rel_dir, entry.name)
            try:
                is_dir = entry.is_dir(follow_symlinks=False)
                is_link = entry.is_symlink()
            except OSError as exc:  # 断链或权限问题
                result.errors.append((rel, f"stat 失败：{exc}"))
                continue

            if is_link:
                # 不跟随符号链接：目标可能指向同步根之外，跟随会破坏“镜像=源的副本”语义
                result.errors.append((rel, "跳过符号链接"))
                continue

            if is_dir:
                if matcher.match_dir(rel):
                    result.pruned_dirs += 1
                    continue
                try:
                    st = entry.stat(follow_symlinks=False)
                except OSError as exc:
                    result.errors.append((rel, f"stat 失败：{exc}"))
                    continue
                result.entries[rel] = ScannedEntry(
                    path=rel, kind=KIND_DIR, size=0, mtime_ns=st.st_mtime_ns
                )
                stack.append((Path(entry.path), rel))
                continue

            if matcher.match(rel):
                continue
            try:
                st = entry.stat(follow_symlinks=False)
            except OSError as exc:
                result.errors.append((rel, f"stat 失败：{exc}"))
                continue
            result.entries[rel] = ScannedEntry(
                path=rel, kind=KIND_FILE, size=st.st_size, mtime_ns=st.st_mtime_ns
            )

    return result
