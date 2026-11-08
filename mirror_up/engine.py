"""同步引擎：一轮同步的状态机。

    IDLE → PREPARING → FLUSHING → SNAPSHOT → PLANNING
        → （dry-run 在此返回）
        → STOPPING（逐个目标确认镜像已关闭）→ APPLYING → COMMITTING → IDLE

几个贯穿全程的规则：

1. **索引只能落后**。所有索引写入都发生在目标文件 fsync + 原子替换之后。
2. **任何情况下不因为“读不到源”而删目标**。扫描出错的路径不参与删除判定；
   源目录整体为空而索引非空时直接中止（防止路径写错/磁盘没挂载清空镜像）。
3. **中止发生在写之前**。镜像仍在运行、版本不一致、索引属于别的世界，都在
   STOPPING 之前就退出了，目标目录一个字节都不会动。
4. **源只扫一次，目标逐个应用**。多镜像目标共享同一份扫描结果与同一个索引
   文件（按 target_id 分区），避免 N 倍源端读取。
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from .applier import Applier, ApplyResult
from .config import (
    Config,
    SetConfig,
    TargetConfig,
    set_source_path,
    set_target_path_in,
    source_root,
    target_path,
)
from .constants import PENDING_UNSTABLE
from .excludes import GlobMatcher
from .flush import NullSourceFlusher
from .hashing import is_region_name
from .index import KIND_DIR, KIND_FILE, FileRow, Index, WorldIdMismatch
from .leveldat import LevelInfo, read_level_info, read_level_info_bytes
from .mirror_ctl import MirrorControl, build_mirror_control
from .planner import Op, OpKind, Plan, SetPlan, build_set_plan
from .region import recover_all_journals, supports_delta
from .scanner import ScanResult, ScannedEntry, scan_tree
from .source import (
    LocalSource,
    Source,
    SourceError,
    SourceUnavailable,
    SourceVanished,
    build_source,
)

logger = logging.getLogger("mirror_up")


class SyncBusy(Exception):
    """已有同步在跑。"""


class SyncAborted(Exception):
    """本轮同步被主动中止（不写目标）。"""


class SyncState:
    IDLE = "idle"
    PREPARING = "preparing"
    FLUSHING = "flushing"
    SNAPSHOT = "snapshot"
    PLANNING = "planning"
    STOPPING = "stopping"
    APPLYING = "applying"
    COMMITTING = "committing"
    ABORTED = "aborted"
    FAILED = "failed"


@dataclass
class SetSource:
    """源端的一个 set（与目标无关，扫一次给所有目标用）。"""

    cfg: SetConfig
    set_id: int
    src: Optional[Source]
    path: Optional[Path]
    scan: Optional[ScanResult] = None


@dataclass
class SetContext:
    """某个目标上的某个 set。"""

    cfg: SetConfig
    set_id: int
    target_id: int
    target_name: str
    target: Path
    index_rows: Dict[str, FileRow]
    applier: Applier
    src: Optional[Source] = None
    source: Optional[Path] = None
    scan: Optional[ScanResult] = None
    plan: Optional[SetPlan] = None


@dataclass
class TargetRound:
    """单个镜像目标在一轮里的账。"""

    name: str
    target_id: int
    root: Path
    applied: int = 0
    unchanged_after_hash: int = 0
    deleted: int = 0
    dirs: int = 0
    deferred: int = 0
    failed: int = 0
    bytes_written: int = 0
    bytes_deleted: int = 0
    regions_delta: int = 0
    chunks_written: int = 0
    chunks_reused: int = 0
    plan: Optional[Plan] = None
    hint: str = ""
    warnings: List[str] = field(default_factory=list)
    failures: List[Tuple[str, str, str]] = field(default_factory=list)
    stopped: bool = False
    restarted: bool = False
    skipped: bool = False

    @property
    def has_changes(self) -> bool:
        return bool(self.plan and self.plan.has_changes())


@dataclass
class RoundReport:
    dry_run: bool = False
    full: bool = False
    force: bool = False
    trigger: str = "manual"
    state: str = SyncState.IDLE
    started_at: float = 0.0
    finished_at: float = 0.0
    snapshot_ns: int = 0

    #: 单目标时等于 ``targets[0].plan``，便于老代码读取。
    plan: Optional[Plan] = None
    plans: Dict[str, Plan] = field(default_factory=dict)
    targets: List[TargetRound] = field(default_factory=list)

    applied: int = 0
    unchanged_after_hash: int = 0
    deleted: int = 0
    dirs: int = 0
    deferred: int = 0
    failed: int = 0
    bytes_written: int = 0
    bytes_deleted: int = 0
    scan_errors: int = 0
    pruned_dirs: int = 0
    #: chunk 级增量统计
    regions_delta: int = 0
    chunks_written: int = 0
    chunks_reused: int = 0

    warnings: List[str] = field(default_factory=list)
    failures: List[Tuple[str, str, str]] = field(default_factory=list)
    abort_reason: str = ""
    error: str = ""
    cancelled: bool = False
    hint: str = ""

    @property
    def duration(self) -> float:
        return max(0.0, self.finished_at - self.started_at)

    def ok(self) -> bool:
        return (
            self.state in (SyncState.IDLE,)
            and not self.abort_reason
            and not self.error
            and self.failed == 0
        )

    def merge_target(self, tr: TargetRound) -> None:
        self.applied += tr.applied
        self.unchanged_after_hash += tr.unchanged_after_hash
        self.deleted += tr.deleted
        self.dirs += tr.dirs
        self.deferred += tr.deferred
        self.failed += tr.failed
        self.bytes_written += tr.bytes_written
        self.bytes_deleted += tr.bytes_deleted
        self.regions_delta += tr.regions_delta
        self.chunks_written += tr.chunks_written
        self.chunks_reused += tr.chunks_reused
        self.warnings.extend(tr.warnings)
        self.failures.extend(tr.failures)
        for line in tr.hint.splitlines():
            if line.strip():
                self.hint = f"{self.hint}\n{line}" if self.hint else line


class SyncEngine:
    def __init__(
        self,
        config: Config,
        base_dir: Path,
        index: Index,
        flusher=None,
        log: Optional[logging.Logger] = None,
    ):
        self.config = config
        self.base_dir = Path(base_dir)
        self.index = index
        self.flusher = flusher or NullSourceFlusher()
        self.logger = log or logger

        self._run_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._cancel = threading.Event()
        self._state = SyncState.IDLE
        self._matcher = GlobMatcher(config.exclude)
        self._last_progress_log = 0.0

        self._sources: List[SetSource] = []
        self._controls: Dict[str, MirrorControl] = {}
        self._repair = True

    # ------------------------------------------------------------------ #
    # 状态
    # ------------------------------------------------------------------ #

    @property
    def state(self) -> str:
        with self._state_lock:
            return self._state

    def _set_state(self, state: str) -> None:
        with self._state_lock:
            self._state = state

    def is_busy(self) -> bool:
        return self._run_lock.locked()

    def cancel(self) -> None:
        self._cancel.set()

    # ------------------------------------------------------------------ #
    # 主流程
    # ------------------------------------------------------------------ #

    def run(
        self,
        dry_run: bool = False,
        full: bool = False,
        force: bool = False,
        trigger: str = "manual",
    ) -> RoundReport:
        if not self._run_lock.acquire(blocking=False):
            raise SyncBusy("已有同步正在进行")

        report = RoundReport(
            dry_run=dry_run, full=full, force=force, trigger=trigger, started_at=time.time()
        )
        self._cancel.clear()
        self._last_progress_log = 0.0
        self._sources = []
        self._controls = {}
        self._set_state(SyncState.PREPARING)

        try:
            self._run_inner(report, dry_run, full, force)
        except SyncAborted as exc:
            report.abort_reason = str(exc)
            self._set_state(SyncState.ABORTED)
            self.logger.warning("[MirrorUP] 本轮中止：%s", exc)
        except Exception as exc:  # 保留现场，交给命令层显示
            report.error = f"{type(exc).__name__}: {exc}"
            self._set_state(SyncState.FAILED)
            self.logger.exception("[MirrorUP] 同步失败")
        finally:
            self._close_flush_window()
            self._close_sources()
            report.finished_at = time.time()
            report.state = self.state
            self._set_state(SyncState.IDLE if report.ok() else report.state)
            self._run_lock.release()

        return report

    def _run_inner(self, report: RoundReport, dry_run: bool, full: bool, force: bool) -> None:
        sources = self._prepare_sources(report, repair=not dry_run)
        self._check_cancel(report)

        snapshot_ns = time.time_ns()
        report.snapshot_ns = snapshot_ns

        # 保存窗口只覆盖扫描阶段
        if not dry_run:
            self._open_flush_window(report)
        self._set_state(SyncState.SNAPSHOT)
        try:
            for item in sources:
                if item.src is None:
                    continue
                try:
                    item.scan = item.src.scan(self._matcher)
                except SourceVanished:
                    report.warnings.append(
                        f"set {item.cfg.name!r} 在源端不存在（agent 上没有这个 set），本轮跳过"
                    )
        except SourceUnavailable as exc:
            raise SyncAborted(f"源端不可用：{exc}")
        finally:
            if not dry_run:
                self._close_flush_window()

        self._set_state(SyncState.PLANNING)
        rounds: List[TargetRound] = []
        target_data: List[Tuple[TargetRound, List[SetContext]]] = []

        for target in self.config.enabled_targets():
            dst = target_path(target, self.base_dir)
            tr = TargetRound(name=target.name, target_id=0, root=dst)
            contexts = self._build_contexts(target, sources, report, repair=not dry_run)
            if not contexts:
                tr.skipped = True
                tr.warnings.append(f"目标 {target.name!r} 没有可用的 set，本轮跳过")
                rounds.append(tr)
                continue
            tr.target_id = contexts[0].target_id

            target_entries = (
                self._scan_target(target, contexts) if self.config.sync.verify_target else {}
            )
            plan = self._build_plan(
                contexts, snapshot_ns, full, force, report, target_entries, tr
            )
            tr.plan = plan
            report.plans[target.name] = plan
            rounds.append(tr)
            target_data.append((tr, contexts))

        report.targets = rounds
        if rounds and rounds[0].plan is not None:
            report.plan = rounds[0].plan
        if len(rounds) == 1:
            report.plan = rounds[0].plan

        if dry_run:
            for tr in rounds:
                if tr.plan is None:
                    continue
                self.logger.info(
                    "[MirrorUP] [%s] 计划：新增 %d / 修改 %d / 删除 %d（跳过未变 %d）",
                    tr.name,
                    tr.plan.add_count(),
                    tr.plan.modify_count(),
                    tr.plan.delete_count(),
                    tr.plan.unchanged_files(),
                )
                self._merge(tr, report)
            self._set_state(SyncState.IDLE)
            return

        for tr, contexts in target_data:
            plan = tr.plan
            assert plan is not None
            if not plan.has_changes():
                self.logger.info("[MirrorUP] [%s] 没有需要同步的变更", tr.name)
                self._merge(tr, report)
                self._commit_meta(report, tr, snapshot_ns)
                continue

            control = self._controls.get(tr.name)
            if control is None:
                control = build_mirror_control(
                    self.config.control_for(self._target_by_name(tr.name)),
                    [c.target for c in contexts],
                )
                self._controls[tr.name] = control
            self._set_state(SyncState.STOPPING)
            self._ensure_mirror_stopped(control, tr, report)
            self._apply(contexts, plan, report, tr, snapshot_ns)
            self._restart_mirror(control, tr, report)
            self._merge(tr, report)
            self._commit_meta(report, tr, snapshot_ns)

        self._set_state(SyncState.IDLE)

    def _target_by_name(self, name: str) -> TargetConfig:
        for target in self.config.enabled_targets():
            if target.name == name:
                return target
        return self.config.targets[0] if self.config.targets else self.config.target

    def _merge(self, tr: TargetRound, report: RoundReport) -> None:
        report.merge_target(tr)

    # ------------------------------------------------------------------ #
    # 镜像启停（每个目标独立）
    # ------------------------------------------------------------------ #

    def _ensure_mirror_stopped(
        self, ctl: MirrorControl, tr: TargetRound, report: RoundReport
    ) -> None:
        stopped, message = ctl.check()
        if stopped:
            self.logger.info("[MirrorUP] [%s] %s", tr.name, message)
            return
        if not ctl.can_manage():
            raise SyncAborted(f"[{tr.name}] {message}")

        control_cfg = self.config.control_for(self._target_by_name(tr.name))
        self.logger.warning(
            "[MirrorUP] [%s] %s；尝试自动停服（%s 适配器）", tr.name, message, ctl.name
        )
        ok, detail = ctl.stop(control_cfg.stop_timeout_sec, control_cfg.notify_players_sec)
        if not ok:
            raise SyncAborted(f"[{tr.name}] 自动停服失败：{detail}")
        tr.stopped = True
        tr.warnings.append(f"已自动停止镜像服 {tr.name!r}（{detail}）")

    def _restart_mirror(self, ctl: MirrorControl, tr: TargetRound, report: RoundReport) -> None:
        control_cfg = self.config.control_for(self._target_by_name(tr.name))
        if not (control_cfg.start_after_sync and ctl.can_manage()):
            tr.hint = "目标已更新完毕；镜像服仍是停止状态，确认后请手动启动"
            return

        ok, detail = ctl.start()
        if not ok:
            tr.warnings.append(f"同步完成，但自动启动失败：{detail}")
            tr.hint = "目标已更新完毕；镜像服仍处于停止状态，请手动启动"
            return

        if ctl.wait_ready(control_cfg.wait_ready_timeout_sec):
            tr.restarted = True
            tr.hint = f"目标已更新完毕，镜像服已启动（{detail}）"
        else:
            tr.warnings.append("镜像服启动后未确认就绪，请检查控制台")
            tr.hint = (
                f"目标已更新完毕，已执行启动命令（{detail}），"
                f"但 {control_cfg.wait_ready_timeout_sec:.0f}s 内未确认就绪"
            )

    def _close_sources(self) -> None:
        for item in self._sources:
            if item.src is not None:
                try:
                    item.src.close()
                except Exception:  # 关闭失败不影响结果
                    logger.debug("[MirrorUP] 关闭源失败", exc_info=True)
        self._sources = []

    # ------------------------------------------------------------------ #
    # 准备：源端
    # ------------------------------------------------------------------ #

    def _prepare_sources(self, report: RoundReport, repair: bool = True) -> List[SetSource]:
        self._repair = repair
        sources: List[SetSource] = []
        agent_mode = (self.config.source.transport or "local").lower() == "agent"
        for item in self.config.enabled_sets():
            src_path = set_source_path(self.config, item, self.base_dir)
            if not agent_mode:
                if not src_path.exists():
                    report.warnings.append(f"set {item.name!r} 的源目录不存在，本轮跳过：{src_path}")
                    continue
                if not src_path.is_dir():
                    report.warnings.append(f"set {item.name!r} 的源不是目录，本轮跳过：{src_path}")
                    continue
            try:
                src = build_source(self.config, item, self.base_dir)
            except SourceError as exc:
                report.warnings.append(f"set {item.name!r} 的源无法使用，本轮跳过：{exc}")
                continue
            set_id = self.index.upsert_set(item.name, item.source, item.target)
            sources.append(
                SetSource(
                    cfg=item,
                    set_id=set_id,
                    src=src,
                    path=None if agent_mode else src_path.resolve(),
                )
            )

        if not sources:
            raise SyncAborted(
                f"没有任何可用的 set：source.root={source_root(self.config, self.base_dir)}；"
                "请检查路径是否写对，改完后执行 !!mu reload"
            )

        world_id = self._compute_world_id(sources)
        try:
            self.index.bind_world(world_id)
        except WorldIdMismatch as exc:
            raise SyncAborted(str(exc)) from exc

        self._sources = sources
        return sources

    def _compute_world_id(self, sources: Sequence[SetSource]) -> str:
        """world_id 由源根 + 世界版本/种子构成，用于识别“索引套错了世界”。"""
        parts = [str(source_root(self.config, self.base_dir))]
        for item in sources:
            info = self._level_info(item)
            if info is not None:
                parts.extend(
                    [
                        info.version_name or "?",
                        str(info.seed) if info.seed is not None else "?",
                        info.level_name or "?",
                    ]
                )
                break
        digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
        return digest

    def _level_info(self, item: SetSource) -> Optional[LevelInfo]:
        """源端 level.dat 的版本/种子；跨机时通过 agent 读字节。"""
        if item.src is None:
            return None
        try:
            data = item.src.read_bytes("level.dat")
        except (SourceError, OSError):
            return None
        if not data:
            return None
        return read_level_info_bytes(data)

    # ------------------------------------------------------------------ #
    # 准备：目标端
    # ------------------------------------------------------------------ #

    def _build_contexts(
        self,
        target: TargetConfig,
        sources: Sequence[SetSource],
        report: RoundReport,
        repair: bool = True,
    ) -> List[SetContext]:
        contexts: List[SetContext] = []
        dst_root = target_path(target, self.base_dir)
        target_id = self.index.upsert_target(target.name, target.root)
        for item in sources:
            if item.scan is None:
                continue
            dst = set_target_path_in(self.config, target, item.cfg, self.base_dir)
            rows = self.index.load_files(item.set_id, target_id)
            journal_dir = (
                self.index.path.parent / "journal" / target.name / item.cfg.name
                if self.config.sync.journal
                else None
            )
            if repair and journal_dir is not None:
                restored = recover_all_journals(journal_dir, dst.resolve())
                # v0.4 的 journal 目录没有按目标分层，升级后第一次跑也可能有残留。
                legacy_journal_dir = self.index.path.parent / "journal" / item.cfg.name
                if restored == 0 and legacy_journal_dir.is_dir():
                    restored = recover_all_journals(legacy_journal_dir, dst.resolve())
                if restored:
                    report.warnings.append(
                        f"target {target.name!r} / set {item.cfg.name!r}：从 journal 恢复了 "
                        f"{restored} 个 region 的头（上次同步未正常结束，本轮会重做）"
                    )
            applier = Applier(
                set_name=item.cfg.name,
                target_root=dst.resolve(),
                sync=self.config.sync,
                source=item.src,
                journal_dir=journal_dir,
            )
            contexts.append(
                SetContext(
                    cfg=item.cfg,
                    set_id=item.set_id,
                    target_id=target_id,
                    target_name=target.name,
                    target=dst.resolve(),
                    index_rows=rows,
                    applier=applier,
                    src=item.src,
                    source=item.path,
                    scan=item.scan,
                    plan=None,
                )
            )
        _ = dst_root
        return contexts

    def _check_version_gate(
        self, contexts: Sequence[SetContext], report: RoundReport
    ) -> None:
        """源与镜像的 MC 版本不一致时拒绝写入（可用 force 覆盖）。"""
        for ctx in contexts:
            src_info: Optional[LevelInfo] = None
            for item in self._sources:
                if item.cfg.name == ctx.cfg.name:
                    src_info = self._level_info(item)
                    break
            if src_info is None or src_info.version_name is None:
                continue
            dst_info = read_level_info(ctx.target / "level.dat")
            if dst_info is None or dst_info.version_name is None:
                continue
            if src_info.version_name != dst_info.version_name:
                message = (
                    f"源世界版本 {src_info.version_name} 与镜像 {dst_info.version_name} 不一致；"
                    "版本不同写入区块会导致镜像坏档。确认要覆盖请使用 !!mu sync --force"
                )
                if not report.force:
                    raise SyncAborted(message)
                report.warnings.append(message + "（已按 --force 继续）")

    # ------------------------------------------------------------------ #
    # 计划
    # ------------------------------------------------------------------ #

    def _build_plan(
        self,
        contexts: Sequence[SetContext],
        snapshot_ns: int,
        full: bool,
        force: bool,
        report: RoundReport,
        target_entries: Dict[str, Optional[Dict[str, ScannedEntry]]],
        tr: TargetRound,
    ) -> Plan:
        self._check_version_gate(contexts, report)
        plan = Plan(snapshot_ns=snapshot_ns)
        last_sync_end_ns = self._meta_int_target("last_sync_end", tr.target_id, 0)
        last_sync_begin_ns = self._meta_int_target("last_sync_begin", tr.target_id, 0)
        dirty_since = last_sync_begin_ns // 1_000_000_000 if last_sync_begin_ns else None
        for ctx in contexts:
            scan = ctx.scan
            if scan is None:
                continue
            report.scan_errors += len(scan.errors)
            report.pruned_dirs += scan.pruned_dirs
            for path, message in scan.errors[:5]:
                report.warnings.append(f"{ctx.cfg.name}:{path}: {message}")

            if scan.root_missing:
                report.warnings.append(f"set {ctx.cfg.name!r} 源根不可读，本轮不参与同步/删除")
                continue

            if not scan.entries and ctx.index_rows and not force:
                raise SyncAborted(
                    f"set {ctx.cfg.name!r} 扫描到 0 个文件，但索引中有 {len(ctx.index_rows)} 条记录；"
                    "这通常意味着源路径写错或磁盘未挂载。为避免清空镜像已中止；确认无误可加 --force"
                )

            sp = build_set_plan(
                set_name=ctx.cfg.name,
                set_id=ctx.set_id,
                scanned=scan,
                index_rows=ctx.index_rows,
                snapshot_ns=snapshot_ns,
                trust_mtime=self.config.sync.trust_mtime,
                full=full,
                target_entries=target_entries.get(ctx.cfg.name),
                last_sync_end_ns=last_sync_end_ns,
            )
            if sp.forced_full_hash:
                report.warnings.append(
                    f"set {ctx.cfg.name!r} 检测到时间戳早于上次同步但大小不同的文件，"
                    "已对该 set 强制全量哈希复核"
                )
            ctx.plan = sp
            self._mark_region_ops(ctx, sp, dirty_since, distrust=full or sp.forced_full_hash)
            plan.sets.append(sp)
        _ = tr
        return plan

    def _mark_region_ops(
        self,
        ctx: SetContext,
        sp: SetPlan,
        dirty_since: Optional[int],
        distrust: bool,
    ) -> None:
        """给 region 文件的写操作挂上上一个目标的 chunk 状态和脏扫描起点。"""
        if not self.config.sync.region_delta:
            return
        for op in sp.ops:
            if not op.is_write:
                continue
            if not supports_delta(Path(op.path).name):
                continue
            op.region = True
            row = self.index.get_region(ctx.set_id, op.path, ctx.target_id)
            op.region_state = row[0] if row else None
            op.dirty_since = None if (distrust or op.force_write) else dirty_since

    # ------------------------------------------------------------------ #
    # 执行
    # ------------------------------------------------------------------ #

    def _scan_target(
        self, target: TargetConfig, contexts: Sequence[SetContext]
    ) -> Dict[str, Optional[Dict[str, ScannedEntry]]]:
        """stat 扫描镜像侧，用于发现“源没变但镜像被改过”的文件。

        目标读不全（有 OSError）时返回 None 表示放弃该 set 的校验——宁可漏检，
        也不能把“读不到”当成“缺失”然后重推整个 set。
        """
        result: Dict[str, Optional[Dict[str, ScannedEntry]]] = {}
        for ctx in contexts:
            if not ctx.target.exists():
                result[ctx.cfg.name] = {}
                continue
            scan = scan_tree(ctx.target, self._matcher)
            if scan.root_missing or scan.errors:
                self.logger.warning(
                    "[MirrorUP] 镜像侧 %s/%s 扫描不完整（%d 个错误），本轮跳过镜像校验",
                    target.name,
                    ctx.cfg.name,
                    len(scan.errors),
                )
                result[ctx.cfg.name] = None
            else:
                result[ctx.cfg.name] = scan.entries
        return result

    def _apply(
        self,
        contexts: Sequence[SetContext],
        plan: Plan,
        report: RoundReport,
        tr: TargetRound,
        snapshot_ns: int,
    ) -> None:
        self._set_state(SyncState.APPLYING)
        self.index.begin()
        started = time.monotonic()
        total = sum(len(sp.ops) for sp in plan.sets if sp.ops)
        done = 0

        try:
            with ThreadPoolExecutor(
                max_workers=self.config.sync.concurrency, thread_name_prefix="mirrorup"
            ) as pool:
                futures = {}
                for ctx in contexts:
                    if ctx.plan is None:
                        continue
                    for op in ctx.plan.ops:
                        futures[pool.submit(ctx.applier.apply, op)] = (ctx, op)

                for future in as_completed(futures):
                    ctx, op = futures[future]
                    if future.cancelled():
                        continue
                    try:
                        result = future.result()
                    except Exception as exc:  # 单文件异常不能拖垮整轮
                        result = ApplyResult(
                            op=op,
                            status="failed",
                            detail=f"{type(exc).__name__}: {exc}",
                        )

                    self._record(ctx, result, report, tr, snapshot_ns)
                    done += 1
                    self._log_progress(done, total, report, started)

                    if self._cancel.is_set() and not report.cancelled:
                        report.cancelled = True
                        self.logger.warning("[MirrorUP] 收到取消请求，停止派发剩余任务")
                        for pending in futures:
                            pending.cancel()
        finally:
            self.index.commit()

    def _record(
        self,
        ctx: SetContext,
        result: ApplyResult,
        report: RoundReport,
        tr: TargetRound,
        snapshot_ns: int,
    ) -> None:
        op = result.op
        if op is None:  # 理论上不会发生
            tr.failed += 1
            return

        if result.status == "applied":
            if op.kind == OpKind.MKDIR:
                tr.dirs += 1
                self.index.put_file(
                    ctx.set_id,
                    FileRow(
                        path=op.path,
                        kind=KIND_DIR,
                        size=0,
                        mtime_ns=op.mtime_ns,
                        hash=None,
                        region=False,
                        synced_at=snapshot_ns,
                    ),
                    ctx.target_id,
                )
            else:
                tr.applied += 1
                tr.bytes_written += result.physical if result.physical >= 0 else result.size
                if op.region:
                    tr.regions_delta += 1
                    tr.chunks_written += result.chunks_written
                    tr.chunks_reused += result.chunks_reused
                self.index.put_file(
                    ctx.set_id,
                    FileRow(
                        path=op.path,
                        kind=KIND_FILE,
                        # 索引记录的是**源端**状态：region 的 inplace 写入会让目标
                        # 文件比源文件大（碎片扇区），用 result.* 会把目标尺寸记成
                        # 源尺寸，下一轮误判“源变了”。
                        size=op.size,
                        mtime_ns=op.mtime_ns,
                        hash=result.digest,
                        region=is_region_name(Path(op.path).name),
                        synced_at=snapshot_ns,
                    ),
                    ctx.target_id,
                )
                if result.region_state is not None:
                    self.index.put_region(
                        ctx.set_id, op.path, result.region_state, None, ctx.target_id
                    )
            self.index.clear_pending(ctx.set_id, op.path, ctx.target_id)

        elif result.status == "unchanged":
            tr.unchanged_after_hash += 1
            if op.region:
                tr.chunks_reused += result.chunks_reused
            self.index.put_file(
                ctx.set_id,
                FileRow(
                    path=op.path,
                    kind=KIND_FILE,
                    size=op.size,
                    mtime_ns=op.mtime_ns,
                    hash=result.digest,
                    region=is_region_name(Path(op.path).name),
                    synced_at=snapshot_ns,
                ),
                ctx.target_id,
            )
            if result.region_state is not None:
                self.index.put_region(
                    ctx.set_id, op.path, result.region_state, None, ctx.target_id
                )
            self.index.clear_pending(ctx.set_id, op.path, ctx.target_id)

        elif result.status == "deleted":
            tr.deleted += 1
            tr.bytes_deleted += op.size
            self.index.delete_file(ctx.set_id, op.path, ctx.target_id)
            self.index.clear_pending(ctx.set_id, op.path, ctx.target_id)

        elif result.status == "deferred":
            tr.deferred += 1
            self.index.put_pending(
                ctx.set_id,
                op.path,
                result.pending_reason or PENDING_UNSTABLE,
                int(time.time()),
                ctx.target_id,
            )

        else:  # failed
            tr.failed += 1
            tr.failures.append((ctx.cfg.name, op.path, result.detail))

    def _meta_key(self, base: str, target_id: int) -> str:
        return f"{base}:{target_id}"

    def _meta_int_target(self, base: str, target_id: int, default: int = 0) -> int:
        value = self.index.get_meta_int(self._meta_key(base, target_id), -1)
        if value != -1:
            return value
        # 兼容 v0.4 及更早的全局键（默认目标）
        if target_id == 1:
            return self.index.get_meta_int(base, default)
        return default

    def _commit_meta(self, report: RoundReport, tr: TargetRound, snapshot_ns: int) -> None:
        now = time.time()
        self.index.set_meta(self._meta_key("last_snapshot_time", tr.target_id), snapshot_ns)
        self.index.set_meta(
            self._meta_key("last_sync_begin", tr.target_id),
            int(report.started_at * 1_000_000_000),
        )
        self.index.set_meta(self._meta_key("last_sync_end", tr.target_id), int(now * 1_000_000_000))
        summary = {
            "at": int(now),
            "target": tr.name,
            "dry_run": report.dry_run,
            "full": report.full,
            "applied": tr.applied,
            "deleted": tr.deleted,
            "deferred": tr.deferred,
            "failed": tr.failed,
            "bytes_written": tr.bytes_written,
            "unchanged": tr.unchanged_after_hash,
            "duration": round(report.duration, 3),
            "trigger": report.trigger,
        }
        self.index.set_meta(
            self._meta_key("last_round_json", tr.target_id),
            json.dumps(summary, ensure_ascii=False),
        )
        try:
            self.index.add_history(
                {
                    "target": tr.name,
                    "trigger": report.trigger,
                    "started_at": int(report.started_at * 1000),
                    "finished_at": int(now * 1000),
                    "ok": tr.failed == 0 and not report.abort_reason and not report.error,
                    "dry_run": report.dry_run,
                    "applied": tr.applied,
                    "deleted": tr.deleted,
                    "deferred": tr.deferred,
                    "failed": tr.failed,
                    "bytes_written": tr.bytes_written,
                    "duration": round(report.duration, 3),
                    "summary": (
                        f"写入 {tr.applied} / 删除 {tr.deleted} / 延后 {tr.deferred}"
                        f" / 失败 {tr.failed} / {_mb(tr.bytes_written)}"
                    ),
                    "detail": json.dumps(summary, ensure_ascii=False),
                }
            )
            self.index.prune_history(self.config.log.history_keep)
        except Exception:  # 历史写失败不影响本轮结果
            self.logger.exception("[MirrorUP] 写同步历史失败")
        self.index.commit()

    # ------------------------------------------------------------------ #
    # 辅助
    # ------------------------------------------------------------------ #

    def _open_flush_window(self, report: RoundReport) -> None:
        if (self.config.source.transport or "local").lower() == "agent":
            # 源端在另一台机器上，本机发的 save-off 只会打到镜像服。
            self.logger.info("[MirrorUP] 跨机模式：跳过 save-off（无法从本机控制源端保存）")
            return
        self._set_state(SyncState.FLUSHING)
        try:
            outcome = self.flusher.begin()
        except Exception as exc:
            report.warnings.append(f"开启保存窗口失败：{exc}")
            return
        if outcome.skipped:
            self.logger.info("[MirrorUP] %s", outcome.message)
            return
        if outcome.ok:
            self.logger.info("[MirrorUP] %s", outcome.message)
        else:
            report.warnings.append(outcome.message)
            self.logger.warning("[MirrorUP] %s", outcome.message)

    def _close_flush_window(self) -> None:
        try:
            self.flusher.end()
        except Exception:
            self.logger.exception("[MirrorUP] 关闭保存窗口失败")

    def _check_cancel(self, report: RoundReport) -> None:
        if self._cancel.is_set():
            report.cancelled = True
            raise SyncAborted("已取消")

    def _log_progress(self, done: int, total: int, report: RoundReport, started: float) -> None:
        now = time.monotonic()
        if done != total and (now - self._last_progress_log) < 5.0:
            return
        self._last_progress_log = now
        elapsed = max(0.001, now - started)
        rate = report.bytes_written / elapsed / (1024 * 1024)
        self.logger.info(
            "[MirrorUP] 进度 %d/%d，已写 %.1f MB（%.1f MB/s），失败 %d，延后 %d",
            done,
            total,
            report.bytes_written / (1024 * 1024),
            rate,
            report.failed,
            report.deferred,
        )

    # ------------------------------------------------------------------ #
    # 状态查询（给 !!mu status 用）
    # ------------------------------------------------------------------ #

    def describe(self) -> List[str]:
        lines: List[str] = []
        if self.is_busy():
            lines.append(f"同步进行中（{self.state}）")
        else:
            lines.append("空闲")

        files, total_bytes = self.index.total_stats()
        targets = self.config.enabled_targets()
        lines.append(
            f"索引：{files} 个文件 / {total_bytes / (1024 * 1024):.1f} MB（{len(targets)} 个目标）"
        )
        for target in targets:
            tid = self.index.load_targets().get(target.name)
            if tid is None:
                lines.append(f"  目标 {target.name}：尚未同步")
                continue
            last_end = self.index.get_meta_int(self._meta_key("last_sync_end", tid), 0)
            raw = self.index.get_meta(self._meta_key("last_round_json", tid))
            when = (
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(last_end / 1e9))
                if last_end
                else "从未"
            )
            detail = ""
            if raw:
                try:
                    summary = json.loads(raw)
                    detail = (
                        f"（写 {summary.get('applied', 0)} / 删 {summary.get('deleted', 0)}"
                        f" / 失败 {summary.get('failed', 0)}）"
                    )
                except (ValueError, TypeError):
                    detail = ""
            lines.append(f"  目标 {target.name}：上次同步 {when}{detail}")

        pending = self.index.list_pending()
        if pending:
            by_reason: Dict[str, int] = {}
            for _, _, _, reason, _ in pending:
                by_reason[reason] = by_reason.get(reason, 0) + 1
            detail = ", ".join(f"{k} {v}" for k, v in sorted(by_reason.items()))
            lines.append(f"待重试：{len(pending)} 项（{detail}）")

        stalled = [p for p in pending if p[3] == PENDING_UNSTABLE]
        if len(stalled) >= 3:
            lines.insert(0, f"⚠ {len(stalled)} 个对象连续多轮不稳定，见 !!mu status --pending")

        return lines


def _mb(size: int) -> str:
    if size >= 1024 * 1024:
        return f"{size / (1024 * 1024):.1f} MB"
    if size >= 1024:
        return f"{size / 1024:.1f} KB"
    return f"{size} B"
