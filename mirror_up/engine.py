"""同步引擎：一轮同步的状态机。

    IDLE → PREPARING → FLUSHING → SNAPSHOT → PLANNING
        → （dry-run 在此返回）
        → STOPPING（确认镜像已关闭）→ APPLYING → COMMITTING → IDLE

几条贯穿全程的规则：

1. **索引只能落后**。所有索引写入都发生在目标文件 fsync + 原子替换之后。
2. **任何情况下不因为“读不到源”而删目标**。扫描出错的路径不参与删除判定；
   源目录整体为空而索引非空时直接中止（防止路径写错/磁盘没挂载清空镜像）。
3. **中止发生在写之前**。镜像仍在运行、版本不一致、索引属于别的世界，都在
   STOPPING 之前就退出了，目标目录一个字节都不会动。
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
    set_source_path,
    set_target_path,
    source_root,
    target_root,
)
from .constants import PENDING_UNSTABLE
from .excludes import GlobMatcher
from .flush import NullSourceFlusher
from .hashing import is_region_name
from .index import KIND_DIR, KIND_FILE, FileRow, Index, WorldIdMismatch
from .leveldat import read_level_info
from .mirror_ctl import build_mirror_control
from .planner import Op, OpKind, Plan, SetPlan, build_set_plan
from .region import supports_delta
from .scanner import ScanResult, ScannedEntry, scan_tree

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
class SetContext:
    cfg: SetConfig
    set_id: int
    source: Path
    target: Path
    index_rows: Dict[str, FileRow]
    applier: Applier
    plan: Optional[SetPlan] = None


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

    plan: Optional[Plan] = None
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

        roots = [set_target_path(config, s, self.base_dir) for s in config.enabled_sets()]
        self.mirror_control = build_mirror_control(config.mirror_control.adapter, roots)

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
            report.finished_at = time.time()
            report.state = self.state
            self._set_state(SyncState.IDLE if report.ok() else report.state)
            self._run_lock.release()

        return report

    def _run_inner(self, report: RoundReport, dry_run: bool, full: bool, force: bool) -> None:
        contexts = self._prepare(report)
        self._check_cancel(report)

        snapshot_ns = time.time_ns()
        report.snapshot_ns = snapshot_ns

        # 保存窗口只覆盖扫描阶段
        if not dry_run:
            self._open_flush_window(report)
        self._set_state(SyncState.SNAPSHOT)
        scans: Dict[str, ScanResult] = {}
        try:
            for ctx in contexts:
                scans[ctx.cfg.name] = scan_tree(ctx.source, self._matcher)
        finally:
            if not dry_run:
                self._close_flush_window()

        self._set_state(SyncState.PLANNING)
        target_entries = self._scan_targets(contexts) if self.config.sync.verify_target else {}
        plan = self._build_plan(contexts, scans, snapshot_ns, full, force, report, target_entries)
        report.plan = plan

        if dry_run:
            self.logger.info(
                "[MirrorUP] 计划：新增 %d / 修改 %d / 删除 %d（跳过未变 %d）",
                plan.add_count(),
                plan.modify_count(),
                plan.delete_count(),
                plan.unchanged_files(),
            )
            self._set_state(SyncState.IDLE)
            return

        if not plan.has_changes():
            self.logger.info("[MirrorUP] 没有需要同步的变更")
        else:
            self._set_state(SyncState.STOPPING)
            stopped, message = self.mirror_control.check()
            if not stopped:
                raise SyncAborted(message)
            self.logger.info("[MirrorUP] %s", message)
            self._apply(contexts, plan, report, snapshot_ns)
            report.hint = "目标已更新完毕；镜像服仍是停止状态，确认后请手动启动"

        self._set_state(SyncState.COMMITTING)
        self._commit_meta(report, snapshot_ns)
        self._set_state(SyncState.IDLE)

    # ------------------------------------------------------------------ #
    # 准备
    # ------------------------------------------------------------------ #

    def _prepare(self, report: RoundReport) -> List[SetContext]:
        contexts: List[SetContext] = []
        for item in self.config.enabled_sets():
            src = set_source_path(self.config, item, self.base_dir)
            dst = set_target_path(self.config, item, self.base_dir)
            if not src.exists():
                report.warnings.append(f"set {item.name!r} 的源目录不存在，本轮跳过：{src}")
                continue
            if not src.is_dir():
                report.warnings.append(f"set {item.name!r} 的源不是目录，本轮跳过：{src}")
                continue
            set_id = self.index.upsert_set(item.name, item.source, item.target)
            rows = self.index.load_files(set_id)
            applier = Applier(
                set_name=item.name,
                source_root=src.resolve(),
                target_root=dst.resolve(),
                sync=self.config.sync,
            )
            contexts.append(
                SetContext(
                    cfg=item,
                    set_id=set_id,
                    source=src.resolve(),
                    target=dst.resolve(),
                    index_rows=rows,
                    applier=applier,
                )
            )

        if not contexts:
            raise SyncAborted(
                f"没有任何可用的 set：source.root={source_root(self.config, self.base_dir)}；"
                "请检查路径是否写对，改完后执行 !!mu reload"
            )

        world_id = self._compute_world_id(contexts)
        try:
            self.index.bind_world(world_id)
        except WorldIdMismatch as exc:
            raise SyncAborted(str(exc)) from exc

        self._check_version_gate(contexts, report)
        return contexts

    def _compute_world_id(self, contexts: Sequence[SetContext]) -> str:
        """world_id 由源根 + 世界版本/种子构成，用于识别“索引套错了世界”。"""
        parts = [str(source_root(self.config, self.base_dir))]
        for ctx in contexts:
            info = read_level_info(ctx.source / "level.dat")
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

    def _check_version_gate(self, contexts: Sequence[SetContext], report: RoundReport) -> None:
        """源与镜像的 MC 版本不一致时拒绝写入（可用 force 覆盖）。"""
        for ctx in contexts:
            src_info = read_level_info(ctx.source / "level.dat")
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
        scans: Dict[str, ScanResult],
        snapshot_ns: int,
        full: bool,
        force: bool,
        report: RoundReport,
        target_entries: Dict[str, Optional[Dict[str, ScannedEntry]]],
    ) -> Plan:
        plan = Plan(snapshot_ns=snapshot_ns)
        last_sync_end_ns = self.index.get_meta_int("last_sync_end", 0)
        last_sync_begin_ns = self.index.get_meta_int("last_sync_begin", 0)
        dirty_since = last_sync_begin_ns // 1_000_000_000 if last_sync_begin_ns else None
        for ctx in contexts:
            scan = scans[ctx.cfg.name]
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
        return plan

    def _mark_region_ops(
        self,
        ctx: SetContext,
        sp: SetPlan,
        dirty_since: Optional[int],
        distrust: bool,
    ) -> None:
        """给 region 文件的写操作挂上上一轮的 chunk 状态和脏扫描起点。

        源端时间戳不可信（``--full`` / 备份还原嫌疑 / 镜像被改）时 ``dirty_since``
        置 None，强制读取每个 chunk 重新算哈希。
        """
        if not self.config.sync.region_delta:
            return
        for op in sp.ops:
            if not op.is_write:
                continue
            if not supports_delta(Path(op.path).name):
                continue
            op.region = True
            row = self.index.get_region(ctx.set_id, op.path)
            op.region_state = row[0] if row else None
            op.dirty_since = None if (distrust or op.force_write) else dirty_since

    # ------------------------------------------------------------------ #
    # 执行
    # ------------------------------------------------------------------ #

    def _scan_targets(
        self, contexts: Sequence[SetContext]
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
                    "[MirrorUP] 镜像侧 %s 扫描不完整（%d 个错误），本轮跳过镜像校验",
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
                        futures[pool.submit(ctx.applier.apply, op)] = ctx

                for future in as_completed(futures):
                    ctx = futures[future]
                    if future.cancelled():
                        continue
                    try:
                        result = future.result()
                    except Exception as exc:  # 单文件异常不能拖垮整轮
                        op = getattr(future, "mirrorup_op", None)
                        result = ApplyResult(
                            op=op,  # type: ignore[arg-type]
                            status="failed",
                            detail=f"{type(exc).__name__}: {exc}",
                        )

                    self._record(ctx, result, report, snapshot_ns)
                    done += 1
                    self._log_progress(done, total, report, started)

                    if self._cancel.is_set() and not report.cancelled:
                        report.cancelled = True
                        self.logger.warning("[MirrorUP] 收到取消请求，停止派发剩余任务")
                        for pending in futures:
                            pending.cancel()
        finally:
            self.index.commit()

    def _record(self, ctx: SetContext, result: ApplyResult, report: RoundReport, snapshot_ns: int) -> None:
        op = result.op
        if op is None:  # 理论上不会发生
            report.failed += 1
            return

        if result.status == "applied":
            if op.kind == OpKind.MKDIR:
                report.dirs += 1
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
                )
            else:
                report.applied += 1
                report.bytes_written += result.physical if result.physical >= 0 else result.size
                if op.region:
                    report.regions_delta += 1
                    report.chunks_written += result.chunks_written
                    report.chunks_reused += result.chunks_reused
                self.index.put_file(
                    ctx.set_id,
                    FileRow(
                        path=op.path,
                        kind=KIND_FILE,
                        size=result.size,
                        mtime_ns=result.mtime_ns,
                        hash=result.digest,
                        region=is_region_name(Path(op.path).name),
                        synced_at=snapshot_ns,
                    ),
                )
                if result.region_state is not None:
                    self.index.put_region(ctx.set_id, op.path, result.region_state, None)
            self.index.clear_pending(ctx.set_id, op.path)

        elif result.status == "unchanged":
            report.unchanged_after_hash += 1
            if op.region:
                report.chunks_reused += result.chunks_reused
            self.index.put_file(
                ctx.set_id,
                FileRow(
                    path=op.path,
                    kind=KIND_FILE,
                    size=result.size,
                    mtime_ns=result.mtime_ns,
                    hash=result.digest,
                    region=is_region_name(Path(op.path).name),
                    synced_at=snapshot_ns,
                ),
            )
            if result.region_state is not None:
                self.index.put_region(ctx.set_id, op.path, result.region_state, None)
            self.index.clear_pending(ctx.set_id, op.path)

        elif result.status == "deleted":
            report.deleted += 1
            report.bytes_deleted += op.size
            self.index.delete_file(ctx.set_id, op.path)
            self.index.clear_pending(ctx.set_id, op.path)

        elif result.status == "deferred":
            report.deferred += 1
            self.index.put_pending(
                ctx.set_id,
                op.path,
                result.pending_reason or PENDING_UNSTABLE,
                int(time.time()),
            )

        else:  # failed
            report.failed += 1
            report.failures.append((ctx.cfg.name, op.path, result.detail))

    def _commit_meta(self, report: RoundReport, snapshot_ns: int) -> None:
        now = time.time()
        self.index.set_meta("last_snapshot_time", snapshot_ns)
        self.index.set_meta("last_sync_begin", int(report.started_at * 1_000_000_000))
        self.index.set_meta("last_sync_end", int(now * 1_000_000_000))
        summary = {
            "at": int(now),
            "dry_run": report.dry_run,
            "full": report.full,
            "applied": report.applied,
            "deleted": report.deleted,
            "deferred": report.deferred,
            "failed": report.failed,
            "bytes_written": report.bytes_written,
            "unchanged": report.unchanged_after_hash,
            "duration": round(report.duration, 3),
            "trigger": report.trigger,
        }
        self.index.set_meta("last_round_json", json.dumps(summary, ensure_ascii=False))
        self.index.commit()

    # ------------------------------------------------------------------ #
    # 辅助
    # ------------------------------------------------------------------ #

    def _open_flush_window(self, report: RoundReport) -> None:
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
        lines.append(f"索引：{files} 个文件 / {total_bytes / (1024 * 1024):.1f} MB")

        last_end = self.index.get_meta_int("last_sync_end", 0)
        if last_end:
            lines.append(f"上次同步：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_end / 1e9))}")
        else:
            lines.append("上次同步：从未（下一轮将全量）")

        raw = self.index.get_meta("last_round_json")
        if raw:
            try:
                summary = json.loads(raw)
                lines.append(
                    "上轮结果：写 {applied} / 删 {deleted} / 延后 {deferred} / 失败 {failed}".format(
                        applied=summary.get("applied", 0),
                        deleted=summary.get("deleted", 0),
                        deferred=summary.get("deferred", 0),
                        failed=summary.get("failed", 0),
                    )
                )
            except (ValueError, TypeError):
                pass

        pending = self.index.list_pending()
        if pending:
            by_reason: Dict[str, int] = {}
            for _, _, reason, _ in pending:
                by_reason[reason] = by_reason.get(reason, 0) + 1
            detail = ", ".join(f"{k} {v}" for k, v in sorted(by_reason.items()))
            lines.append(f"待重试：{len(pending)} 项（{detail}）")

        stalled = [p for p in pending if p[2] == PENDING_UNSTABLE]
        if len(stalled) >= 3:
            lines.insert(0, f"⚠ {len(stalled)} 个对象连续多轮不稳定，见 !!mu status --pending")

        return lines
