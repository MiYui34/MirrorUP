"""计划生成（L2 层）。

输入：本轮扫描结果 + 索引中上一轮的记录；输出：一串待执行操作与统计。

判定的核心是“mtime 可以作为证据，但不能单独作为证据”：

- ``size`` 变了 → 一定变；
- ``mtime_ns`` 变了 → 疑似变，靠拷贝时顺带算出的哈希最终确认；
- 两者都没变，且 ``mtime_ns < 上次快照时间`` → 未变，**零读**；
- 两者都没变，但 ``mtime_ns >= 上次快照时间`` → 保守判为变（同一时刻的歧义）；
- ``mtime_ns`` 比上次快照还早、``size`` 却不同 → 疑似“备份还原/整目录拷贝”把
  时间戳抹旧了，此时整个 set 的“未变”结论都不可信，强制全量重哈希。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

from .hashing import is_region_name
from .index import KIND_DIR, KIND_FILE, FileRow
from .scanner import ScanResult, ScannedEntry


class OpKind:
    ADD = "add"
    MODIFY = "modify"
    DELETE = "delete"
    MKDIR = "mkdir"
    RMDIR = "rmdir"


_KIND_ORDER = {
    OpKind.ADD: 0,
    OpKind.MKDIR: 0,
    OpKind.MODIFY: 0,
    OpKind.DELETE: 1,
    OpKind.RMDIR: 1,
}


@dataclass
class Op:
    kind: str
    set_name: str
    set_id: int
    path: str
    size: int = 0
    mtime_ns: int = 0
    old_hash: Optional[bytes] = None
    reason: str = ""
    #: 源端哈希没变但镜像被改过时，必须真的写一次，不能用 old_hash 跳过。
    force_write: bool = False
    #: region 文件走 chunk 级增量（v0.2）
    region: bool = False
    #: 索引里上一轮的 chunk 状态 blob（``region.encode_state`` 的产物）
    region_state: Optional[bytes] = None
    #: 只有时间戳 >= 该值的 chunk 需要读；None 表示全部要读
    dirty_since: Optional[int] = None

    @property
    def is_write(self) -> bool:
        return self.kind in (OpKind.ADD, OpKind.MODIFY)

    @property
    def is_delete(self) -> bool:
        return self.kind in (OpKind.DELETE, OpKind.RMDIR)


@dataclass
class SetPlan:
    set_name: str
    set_id: int
    ops: List[Op] = field(default_factory=list)
    unchanged_files: int = 0
    unchanged_bytes: int = 0
    forced_full_hash: bool = False
    scan_errors: int = 0

    def count(self, *kinds: str) -> int:
        return sum(1 for op in self.ops if op.kind in kinds)

    def total_bytes(self, *kinds: str) -> int:
        return sum(op.size for op in self.ops if op.kind in kinds)


@dataclass
class Plan:
    snapshot_ns: int
    sets: List[SetPlan] = field(default_factory=list)
    scan_errors: int = 0
    pruned_dirs: int = 0

    # -------------------------------------------------------------- #
    # 汇总
    # -------------------------------------------------------------- #

    def all_ops(self) -> Iterable[Op]:
        for sp in self.sets:
            for op in sp.ops:
                yield op

    def add_count(self) -> int:
        return sum(sp.count(OpKind.ADD) for sp in self.sets)

    def add_bytes(self) -> int:
        return sum(sp.total_bytes(OpKind.ADD) for sp in self.sets)

    def modify_count(self) -> int:
        return sum(sp.count(OpKind.MODIFY) for sp in self.sets)

    def modify_bytes(self) -> int:
        return sum(sp.total_bytes(OpKind.MODIFY) for sp in self.sets)

    def delete_count(self) -> int:
        return sum(sp.count(OpKind.DELETE) for sp in self.sets)

    def delete_bytes(self) -> int:
        return sum(sp.total_bytes(OpKind.DELETE) for sp in self.sets)

    def dir_count(self) -> int:
        return sum(sp.count(OpKind.MKDIR, OpKind.RMDIR) for sp in self.sets)

    def unchanged_files(self) -> int:
        return sum(sp.unchanged_files for sp in self.sets)

    def unchanged_bytes(self) -> int:
        return sum(sp.unchanged_bytes for sp in self.sets)

    def has_changes(self) -> bool:
        return any(sp.ops for sp in self.sets)


def build_set_plan(
    set_name: str,
    set_id: int,
    scanned: ScanResult,
    index_rows: Dict[str, FileRow],
    snapshot_ns: int,
    trust_mtime: bool = True,
    full: bool = False,
    target_entries: Optional[Dict[str, ScannedEntry]] = None,
    last_sync_end_ns: int = 0,
) -> SetPlan:
    plan = SetPlan(set_name=set_name, set_id=set_id, scan_errors=len(scanned.errors))

    # 第一遍：找出“备份还原”嫌疑。命中则整个 set 的未变结论作废。
    forced_full = False
    if not full and trust_mtime:
        for path, idx in index_rows.items():
            entry = scanned.entries.get(path)
            if entry is None or entry.kind != KIND_FILE or idx.kind != KIND_FILE:
                continue
            if entry.size != idx.size and entry.mtime_ns < idx.synced_at:
                forced_full = True
                break
    plan.forced_full_hash = forced_full
    distrust_mtime = forced_full or full or not trust_mtime

    touched: set[str] = set()

    for path, entry in scanned.entries.items():
        touched.add(path)
        idx = index_rows.get(path)

        if idx is None or idx.kind != entry.kind:
            if entry.kind == KIND_DIR:
                plan.ops.append(
                    Op(OpKind.MKDIR, set_name, set_id, path, reason="new-dir")
                )
            else:
                plan.ops.append(
                    Op(
                        OpKind.ADD,
                        set_name,
                        set_id,
                        path,
                        size=entry.size,
                        mtime_ns=entry.mtime_ns,
                        reason="new",
                    )
                )
            continue

        if entry.kind == KIND_DIR:
            plan.unchanged_files += 0
            continue

        changed, reason = _file_changed(entry.size, entry.mtime_ns, idx, snapshot_ns, distrust_mtime)
        force_write = False
        if not changed and target_entries is not None:
            reason = _target_diverged(path, entry, target_entries, last_sync_end_ns) or ""
            changed = bool(reason)
            force_write = changed
        if changed:
            plan.ops.append(
                Op(
                    OpKind.MODIFY,
                    set_name,
                    set_id,
                    path,
                    size=entry.size,
                    mtime_ns=entry.mtime_ns,
                    old_hash=idx.hash,
                    reason=reason,
                    force_write=force_write,
                )
            )
        else:
            plan.unchanged_files += 1
            plan.unchanged_bytes += entry.size

    # 删除：扫描错误涉及的路径不参与，避免“读不到就删”
    error_paths = {p for p, _ in scanned.errors}
    error_prefixes = tuple(p + "/" for p in error_paths if p)
    for path, idx in index_rows.items():
        if path in touched:
            continue
        if path in error_paths or (error_prefixes and path.startswith(error_prefixes)):
            continue
        if idx.kind == KIND_DIR:
            plan.ops.append(Op(OpKind.RMDIR, set_name, set_id, path, reason="gone-dir"))
        else:
            plan.ops.append(
                Op(
                    OpKind.DELETE,
                    set_name,
                    set_id,
                    path,
                    size=idx.size,
                    old_hash=idx.hash,
                    reason="gone",
                )
            )

    plan.ops.sort(key=lambda op: (_KIND_ORDER.get(op.kind, 9), op.size, op.path))
    return plan


def _file_changed(
    size: int,
    mtime_ns: int,
    idx: FileRow,
    snapshot_ns: int,
    distrust_mtime: bool,
) -> tuple[bool, str]:
    if distrust_mtime:
        return True, "verify"
    if size != idx.size:
        return True, "size"
    if mtime_ns != idx.mtime_ns:
        return True, "mtime"
    if mtime_ns >= idx.synced_at:
        # 与上次快照同一时刻甚至更晚，说明时间戳不可靠
        return True, "mtime-not-older"
    if snapshot_ns <= idx.synced_at:
        return True, "clock"
    return False, "unchanged"


def _target_diverged(
    path: str,
    entry: ScannedEntry,
    target_entries: Dict[str, ScannedEntry],
    last_sync_end_ns: int,
) -> Optional[str]:
    """镜像侧校验（只 stat，不读内容）。

    源端没变，但目标文件与索引记录不符时，说明镜像被改过（玩家在镜像服玩过，
    或有人手改过），需要按源端覆盖回去。判据两条：

    - 目标不存在 / 大小与源端不符 → 一定不对；
    - 目标 mtime 晚于上一轮同步结束时间 → 上一轮结束后被写过。

    我们自己的写入发生在 ``last_sync_end`` 之前，所以不会自我误判；代价是每轮
    对目标多做一次 stat 扫描（元数据量级，不读内容）。
    """
    target = target_entries.get(path)
    if target is None or target.kind != KIND_FILE:
        return "target-missing"
    # region 文件的大小取决于目标端的 chunk 排布（inplace 会多出碎片扇区），
    # 不能拿源端大小去比；这类文件只靠 mtime 和时间窗口判断。
    if not is_region_name(path.rsplit("/", 1)[-1]) and target.size != entry.size:
        return "target-size"
    if last_sync_end_ns and target.mtime_ns > last_sync_end_ns:
        return "target-touched"
    return None
