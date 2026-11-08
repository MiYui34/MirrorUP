"""配置加载、默认值生成与校验。

配置文件路径：``<MCDR 数据目录>/mirror_up/config.json``。
首次启动写默认值；升级时按 ``schema_version`` 补字段（只补不改，用户改动优先）。
"""

from __future__ import annotations

import copy
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .constants import CONFIG_FILENAME, SCHEMA_VERSION

DEFAULT_TARGET_ROOT = "./Mirror/server"

DEFAULT_EXCLUDE: List[str] = [
    "**/session.lock",
    "**/*.lock",
    "**/*.tmp",
    "**/*.mcc.lock",
    "logs/**",
    "crash-reports/**",
    "**/usercache.json",
]

DEFAULT_CONFIG: Dict[str, Any] = {
    "schema_version": SCHEMA_VERSION,
    "role": "driver",
    "source": {
        "root": "./server",
        "transport": "local",
        "agent": {"url": "http://127.0.0.1:25580", "token": "", "tls_pin_sha256": ""},
        "mtime_precision_sec": 0,
    },
    "target": {"root": "./Mirror/server"},
    "targets": [{"name": "default", "root": "./Mirror/server", "enabled": True}],
    "sets": [
        {"name": "world", "source": "world", "target": "world", "enabled": True, "mode": "strict"},
        {"name": "mods", "source": "mods", "target": "mods", "enabled": True, "mode": "strict"},
    ],
    "exclude": list(DEFAULT_EXCLUDE),
    "sync": {
        "hash": "xxh3-128",
        "concurrency": 4,
        "trust_mtime": True,
        "verify_target": True,
        "region_delta": True,
        "region_write_mode": "inplace",
        "region_compaction_ratio": 0.5,
        "journal": True,
        "clean_mcc_orphans": True,
        "max_retry": 3,
        "defer_unstable": True,
    },
    "source_flush": {
        "enabled": True,
        "turn_off_auto_save": True,
        "commands": {
            "auto_save_off": "save-off",
            "save_all": "save-all flush",
            "auto_save_on": "save-on",
        },
        "saved_regex": "^Saved the game$",
        "timeout_sec": 60,
    },
    "mirror_control": {
        "adapter": "manual",
        "rcon": {"host": "127.0.0.1", "port": 25575, "password": ""},
        "external": {"stop_cmd": "", "start_cmd": "", "probe_cmd": ""},
        "mcsm": {
            "base_url": "http://127.0.0.1:23333",
            "apikey": "",
            "uuid": "",
            "remote_uuid": "",
            "username": "",
            "password": "",
            "timeout_sec": 10,
            "insecure_tls": False,
        },
        "stop_timeout_sec": 120,
        "start_after_sync": True,
        "wait_ready_timeout_sec": 180,
        "notify_players_sec": 10,
    },
    "schedule": {
        "auto_sync": False,
        "interval_seconds": 3600,
        "max_interval_seconds": 604800,
        "skip_if_source_empty": False,
    },
    "agent_server": {"host": "127.0.0.1", "port": 25580, "token": "", "tls": {"enabled": False, "cert": "", "key": ""}},
    "log": {"history_keep": 20, "verbose_plan": False},
}


class ConfigError(Exception):
    """配置非法，启动阶段直接失败。"""


# --------------------------------------------------------------------------- #
# 数据模型
# --------------------------------------------------------------------------- #


@dataclass
class AgentConfig:
    url: str = "http://127.0.0.1:25580"
    token: str = ""
    tls_pin_sha256: str = ""


@dataclass
class SourceConfig:
    root: str = "./server"
    transport: str = "local"
    agent: AgentConfig = field(default_factory=AgentConfig)
    mtime_precision_sec: int = 0


@dataclass
class TargetConfig:
    """一个镜像目标。``mirror_control`` 为空时继承全局配置。"""

    name: str = "default"
    root: str = "./Mirror/server"
    enabled: bool = True
    mirror_control: Optional["MirrorControlConfig"] = None


@dataclass
class SetConfig:
    name: str
    source: str
    target: str
    enabled: bool = True
    mode: str = "strict"


@dataclass
class SyncConfig:
    hash: str = "xxh3-128"
    concurrency: int = 4
    trust_mtime: bool = True
    #: 每轮花一次 stat 扫描检查镜像侧是否被改过（被改过就重推）。
    verify_target: bool = True
    region_delta: bool = True
    region_write_mode: str = "inplace"
    region_compaction_ratio: float = 0.5
    journal: bool = True
    #: 目标上不再被任何 region 引用的 ``.mcc`` 外置 chunk 是否删除。
    clean_mcc_orphans: bool = True
    max_retry: int = 3
    defer_unstable: bool = True


@dataclass
class SourceFlushConfig:
    enabled: bool = True
    turn_off_auto_save: bool = True
    commands: Dict[str, str] = field(
        default_factory=lambda: {
            "auto_save_off": "save-off",
            "save_all": "save-all flush",
            "auto_save_on": "save-on",
        }
    )
    saved_regex: str = "^Saved the game$"
    timeout_sec: int = 60


@dataclass
class RconConfig:
    host: str = "127.0.0.1"
    port: int = 25575
    password: str = ""


@dataclass
class ExternalConfig:
    stop_cmd: str = ""
    start_cmd: str = ""
    probe_cmd: str = ""


@dataclass
class McsmConfig:
    """MCSManager 面板控制参数。

    MCSM 9 用 ``apikey`` 走查询参数；MCSM 10 用 ``username/password`` 登录拿
    token，再放到 ``Authorization: Bearer``。两种都配了时优先 apikey。
    """

    base_url: str = "http://127.0.0.1:23333"
    apikey: str = ""
    uuid: str = ""
    remote_uuid: str = ""
    username: str = ""
    password: str = ""
    timeout_sec: int = 10
    insecure_tls: bool = False


@dataclass
class MirrorControlConfig:
    adapter: str = "manual"
    rcon: RconConfig = field(default_factory=RconConfig)
    external: ExternalConfig = field(default_factory=ExternalConfig)
    mcsm: McsmConfig = field(default_factory=McsmConfig)
    stop_timeout_sec: int = 120
    start_after_sync: bool = True
    wait_ready_timeout_sec: int = 180
    notify_players_sec: int = 10


@dataclass
class ScheduleConfig:
    auto_sync: bool = False
    interval_seconds: int = 3600
    max_interval_seconds: int = 604800
    skip_if_source_empty: bool = False


@dataclass
class TlsConfig:
    enabled: bool = False
    cert: str = ""
    key: str = ""


@dataclass
class AgentServerConfig:
    host: str = "127.0.0.1"
    port: int = 25580
    token: str = ""
    tls: TlsConfig = field(default_factory=TlsConfig)


@dataclass
class LogConfig:
    history_keep: int = 20
    verbose_plan: bool = False


@dataclass
class Config:
    schema_version: int = SCHEMA_VERSION
    role: str = "driver"
    source: SourceConfig = field(default_factory=SourceConfig)
    #: 兼容 v0.4 及更早的单一目标写法；始终等于 ``targets[0]``。
    target: TargetConfig = field(default_factory=TargetConfig)
    targets: List[TargetConfig] = field(default_factory=list)
    sets: List[SetConfig] = field(default_factory=list)
    exclude: List[str] = field(default_factory=list)
    sync: SyncConfig = field(default_factory=SyncConfig)
    source_flush: SourceFlushConfig = field(default_factory=SourceFlushConfig)
    mirror_control: MirrorControlConfig = field(default_factory=MirrorControlConfig)
    schedule: ScheduleConfig = field(default_factory=ScheduleConfig)
    agent_server: AgentServerConfig = field(default_factory=AgentServerConfig)
    log: LogConfig = field(default_factory=LogConfig)

    #: 配置文件自身位置，便于 reload 与报错定位。
    path: Optional[Path] = None

    def enabled_sets(self) -> List[SetConfig]:
        return [s for s in self.sets if s.enabled]

    def enabled_targets(self) -> List[TargetConfig]:
        """启用的镜像目标；老配置没有 targets 时退化为单一 target。"""
        if not self.targets:
            return [self.target] if self.target.enabled else []
        return [t for t in self.targets if t.enabled]

    def control_for(self, target: TargetConfig) -> MirrorControlConfig:
        return target.mirror_control or self.mirror_control


# --------------------------------------------------------------------------- #
# 读取
# --------------------------------------------------------------------------- #


def _as_str(value: Any, default: str) -> str:
    return value if isinstance(value, str) else default


def _as_bool(value: Any, default: bool) -> bool:
    return value if isinstance(value, bool) else default


def _as_int(value: Any, default: int) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    return default


def _as_float(value: Any, default: float) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    return default


def _as_dict(value: Any, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    return dict(default or {})


def _as_str_dict(value: Any, default: Dict[str, str]) -> Dict[str, str]:
    if not isinstance(value, dict):
        return dict(default)
    result = dict(default)
    for key, val in value.items():
        if isinstance(key, str) and isinstance(val, str):
            result[key] = val
    return result


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """把 override 合并进 base 的副本：只覆盖它显式给出的键。"""
    result = copy.deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def _parse_set(raw: Any, index: int) -> SetConfig:
    if not isinstance(raw, dict):
        raise ConfigError(f"sets[{index}] 必须是对象")
    name = _as_str(raw.get("name"), "").strip()
    if not name:
        raise ConfigError(f"sets[{index}].name 不能为空")
    source = _as_str(raw.get("source"), "").strip()
    target = _as_str(raw.get("target"), "").strip()
    if not source or not target:
        raise ConfigError(f"set {name!r} 的 source/target 不能为空")
    mode = _as_str(raw.get("mode"), "strict")
    if mode not in ("strict", "lax"):
        raise ConfigError(f"set {name!r} 的 mode 只能是 strict / lax")
    return SetConfig(
        name=name,
        source=source,
        target=target,
        enabled=_as_bool(raw.get("enabled"), True),
        mode=mode,
    )


def _parse_control(raw: Any) -> MirrorControlConfig:
    """解析停服策略（全局段与每个目标各自的覆盖段共用）。"""
    control_raw = _as_dict(raw)
    rcon_raw = _as_dict(control_raw.get("rcon"))
    external_raw = _as_dict(control_raw.get("external"))
    mcsm_raw = _as_dict(control_raw.get("mcsm"))
    return MirrorControlConfig(
        adapter=_as_str(control_raw.get("adapter"), "manual"),
        rcon=RconConfig(
            host=_as_str(rcon_raw.get("host"), "127.0.0.1"),
            port=_as_int(rcon_raw.get("port"), 25575),
            password=_as_str(rcon_raw.get("password"), ""),
        ),
        external=ExternalConfig(
            stop_cmd=_as_str(external_raw.get("stop_cmd"), ""),
            start_cmd=_as_str(external_raw.get("start_cmd"), ""),
            probe_cmd=_as_str(external_raw.get("probe_cmd"), ""),
        ),
        mcsm=McsmConfig(
            base_url=_as_str(mcsm_raw.get("base_url"), "http://127.0.0.1:23333").rstrip("/"),
            apikey=_as_str(mcsm_raw.get("apikey"), ""),
            uuid=_as_str(mcsm_raw.get("uuid"), ""),
            remote_uuid=_as_str(mcsm_raw.get("remote_uuid"), ""),
            username=_as_str(mcsm_raw.get("username"), ""),
            password=_as_str(mcsm_raw.get("password"), ""),
            timeout_sec=max(1, _as_int(mcsm_raw.get("timeout_sec"), 10)),
            insecure_tls=_as_bool(mcsm_raw.get("insecure_tls"), False),
        ),
        stop_timeout_sec=max(1, _as_int(control_raw.get("stop_timeout_sec"), 120)),
        start_after_sync=_as_bool(control_raw.get("start_after_sync"), True),
        wait_ready_timeout_sec=max(1, _as_int(control_raw.get("wait_ready_timeout_sec"), 180)),
        notify_players_sec=max(0, _as_int(control_raw.get("notify_players_sec"), 10)),
    )


def _parse_targets(raw: Dict[str, Any], legacy_raw: Dict[str, Any]) -> List[TargetConfig]:
    """``targets`` 数组，兼容旧配置里单一的 ``target`` 对象。"""
    items = raw.get("targets")
    if items is None:
        return [
            TargetConfig(
                name="default",
                root=_as_str(legacy_raw.get("root"), "./Mirror/server"),
                enabled=True,
                mirror_control=None,
            )
        ]
    if not isinstance(items, list) or not items:
        raise ConfigError("targets 必须是非空数组")
    targets: List[TargetConfig] = []
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            raise ConfigError(f"targets[{i}] 必须是对象")
        name = _as_str(item.get("name"), f"target{i + 1}").strip() or f"target{i + 1}"
        control_raw = item.get("mirror_control")
        targets.append(
            TargetConfig(
                name=name,
                root=_as_str(item.get("root"), "./Mirror/server"),
                enabled=_as_bool(item.get("enabled"), True),
                mirror_control=_parse_control(control_raw) if control_raw is not None else None,
            )
        )

    # 兼容“只改了 target.root 没动 targets”的 v0.4 配置：默认生成的那个
    # targets[0] 如果没被用户改过，而 legacy target.root 改过，就以 legacy 为准。
    legacy_root = _as_str(legacy_raw.get("root"), "").strip()
    if (
        len(targets) == 1
        and targets[0].name == "default"
        and legacy_root
        and legacy_root != targets[0].root
        and targets[0].root == DEFAULT_TARGET_ROOT
    ):
        targets[0].root = legacy_root
    return targets


def parse_config(raw: Dict[str, Any]) -> Config:
    """把 JSON dict 解析为强类型 Config。缺失字段一律取默认值。"""
    if not isinstance(raw, dict):
        raise ConfigError("配置文件根节点必须是对象")

    source_raw = _as_dict(raw.get("source"))
    agent_raw = _as_dict(source_raw.get("agent"))
    target_raw = _as_dict(raw.get("target"))
    targets = _parse_targets(raw, target_raw)
    sync_raw = _as_dict(raw.get("sync"))
    flush_raw = _as_dict(raw.get("source_flush"))
    control_raw = _as_dict(raw.get("mirror_control"))
    schedule_raw = _as_dict(raw.get("schedule"))
    agent_server_raw = _as_dict(raw.get("agent_server"))
    agent_tls_raw = _as_dict(agent_server_raw.get("tls"))
    log_raw = _as_dict(raw.get("log"))

    sets_raw = raw.get("sets")
    if sets_raw is None:
        sets_raw = copy.deepcopy(DEFAULT_CONFIG["sets"])
    if not isinstance(sets_raw, list):
        raise ConfigError("sets 必须是数组")
    sets = [_parse_set(item, i) for i, item in enumerate(sets_raw)]

    exclude_raw = raw.get("exclude")
    if exclude_raw is None:
        exclude = list(DEFAULT_EXCLUDE)
    elif isinstance(exclude_raw, list):
        exclude = [p for p in exclude_raw if isinstance(p, str)]
    else:
        raise ConfigError("exclude 必须是字符串数组")

    max_interval = max(60, _as_int(schedule_raw.get("max_interval_seconds"), 604800))

    cfg = Config(
        schema_version=_as_int(raw.get("schema_version"), SCHEMA_VERSION),
        role=_as_str(raw.get("role"), "driver"),
        source=SourceConfig(
            root=_as_str(source_raw.get("root"), "./server"),
            transport=_as_str(source_raw.get("transport"), "local"),
            agent=AgentConfig(
                url=_as_str(agent_raw.get("url"), "http://127.0.0.1:25580"),
                token=_as_str(agent_raw.get("token"), ""),
                tls_pin_sha256=_as_str(agent_raw.get("tls_pin_sha256"), ""),
            ),
            mtime_precision_sec=max(0, _as_int(source_raw.get("mtime_precision_sec"), 0)),
        ),
        target=targets[0],
        targets=targets,
        sets=sets,
        exclude=exclude,
        sync=SyncConfig(
            hash=_as_str(sync_raw.get("hash"), "xxh3-128"),
            concurrency=max(1, min(32, _as_int(sync_raw.get("concurrency"), 4))),
            trust_mtime=_as_bool(sync_raw.get("trust_mtime"), True),
            verify_target=_as_bool(sync_raw.get("verify_target"), True),
            region_delta=_as_bool(sync_raw.get("region_delta"), True),
            region_write_mode=_as_str(sync_raw.get("region_write_mode"), "inplace"),
            region_compaction_ratio=_as_float(sync_raw.get("region_compaction_ratio"), 0.5),
            journal=_as_bool(sync_raw.get("journal"), True),
        clean_mcc_orphans=_as_bool(sync_raw.get("clean_mcc_orphans"), True),
            max_retry=max(1, _as_int(sync_raw.get("max_retry"), 3)),
            defer_unstable=_as_bool(sync_raw.get("defer_unstable"), True),
        ),
        source_flush=SourceFlushConfig(
            enabled=_as_bool(flush_raw.get("enabled"), True),
            turn_off_auto_save=_as_bool(flush_raw.get("turn_off_auto_save"), True),
            commands=_as_str_dict(
                flush_raw.get("commands"),
                {"auto_save_off": "save-off", "save_all": "save-all flush", "auto_save_on": "save-on"},
            ),
            saved_regex=_as_str(flush_raw.get("saved_regex"), "^Saved the game$"),
            timeout_sec=max(1, _as_int(flush_raw.get("timeout_sec"), 60)),
        ),
        mirror_control=_parse_control(control_raw),
        schedule=ScheduleConfig(
            auto_sync=_as_bool(schedule_raw.get("auto_sync"), False),
            interval_seconds=max(60, _as_int(schedule_raw.get("interval_seconds"), 3600)),
            max_interval_seconds=max_interval,
            skip_if_source_empty=_as_bool(schedule_raw.get("skip_if_source_empty"), False),
        ),
        agent_server=AgentServerConfig(
            host=_as_str(agent_server_raw.get("host"), "127.0.0.1"),
            port=max(0, min(65535, _as_int(agent_server_raw.get("port"), 25580))),
            token=_as_str(agent_server_raw.get("token"), ""),
            tls=TlsConfig(
                enabled=_as_bool(agent_tls_raw.get("enabled"), False),
                cert=_as_str(agent_tls_raw.get("cert"), ""),
                key=_as_str(agent_tls_raw.get("key"), ""),
            ),
        ),
        log=LogConfig(
            history_keep=max(1, _as_int(log_raw.get("history_keep"), 20)),
            verbose_plan=_as_bool(log_raw.get("verbose_plan"), False),
        ),
    )

    # 间隔硬上限：7 天。超限 clamp 并由调用方告警。
    if cfg.schedule.interval_seconds > cfg.schedule.max_interval_seconds:
        cfg.schedule.interval_seconds = cfg.schedule.max_interval_seconds

    return cfg


def load_config(data_dir: Path) -> Tuple[Config, List[str]]:
    """读取配置；不存在则写默认值。

    返回 ``(config, warnings)``。配置语法错误抛 ``ConfigError``（并保留原文件，
    不覆盖用户内容）。
    """
    path = Path(data_dir) / CONFIG_FILENAME
    warnings: List[str] = []
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(DEFAULT_CONFIG, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"配置文件不是合法 JSON: {path} ({exc})") from exc

    if not isinstance(raw, dict):
        raise ConfigError("配置文件根节点必须是对象")

    schema_version = _as_int(raw.get("schema_version"), 0)
    if schema_version < SCHEMA_VERSION:
        if 1 <= schema_version < 2:
            sync_raw = raw.get("sync")
            if isinstance(sync_raw, dict) and sync_raw.get("region_delta") is False:
                sync_raw["region_delta"] = True
                warnings.append(
                    "sync.region_delta 已打开（v0.2 起真正生效）；"
                    "如需退回整文件同步，请手动改回 false"
                )
        if 1 <= schema_version < 3:
            sync_raw = raw.get("sync")
            if isinstance(sync_raw, dict) and sync_raw.get("region_write_mode") in (None, "rebuild"):
                sync_raw["region_write_mode"] = "inplace"
                warnings.append(
                    "v0.3 起 region 默认用 inplace 写入（只追加脏 chunk）；"
                    "如需退回整文件替换，请把 sync.region_write_mode 改回 rebuild"
                )
        merged = _deep_merge(DEFAULT_CONFIG, raw)
        merged["schema_version"] = SCHEMA_VERSION
        backup = path.with_suffix(".json.bak")
        try:
            backup.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
        except OSError:
            pass
        raw = merged
        path.write_text(json.dumps(merged, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        warnings.append(f"配置已从 schema v{schema_version} 升级到 v{SCHEMA_VERSION}，旧文件备份为 {backup.name}")

    cfg = parse_config(raw)
    cfg.path = path
    warnings.extend(_normalize(cfg))

    return cfg, warnings


def _normalize(cfg: Config) -> List[str]:
    """把配置收敛到当前版本真正支持的取值，并返回给用户看的提示。"""
    warnings: List[str] = []
    if cfg.schedule.interval_seconds > cfg.schedule.max_interval_seconds:
        warnings.append(
            f"schedule.interval_seconds 超过上限，已 clamp 到 {cfg.schedule.max_interval_seconds} 秒"
        )
        cfg.schedule.interval_seconds = cfg.schedule.max_interval_seconds
    if cfg.sync.region_write_mode not in ("rebuild", "inplace"):
        cfg.sync.region_write_mode = "rebuild"
        warnings.append("未知的 region_write_mode，已回退为 rebuild")
    if cfg.sync.region_write_mode == "inplace" and not cfg.sync.journal:
        cfg.sync.region_write_mode = "rebuild"
        warnings.append(
            "journal=false 时 inplace 没有崩溃保护，已回退为 rebuild；"
            "要用 inplace 请把 journal 设回 true"
        )
    if cfg.sync.region_compaction_ratio < 0:
        cfg.sync.region_compaction_ratio = 0.0
    if cfg.source.mtime_precision_sec and cfg.sync.trust_mtime:
        warnings.append("mtime_precision_sec > 0 时不建议信任 mtime，已自动关闭 trust_mtime")
        cfg.sync.trust_mtime = False
    # 跨机时 driver 不在源服那台机器上，save-off/save-all 只会发给本机的镜像服，
    # 既刷新不了源端，还会把镜像服的自动保存关掉——直接关掉这个窗口。
    if (cfg.source.transport or "local").lower() == "agent" and cfg.source_flush.enabled:
        cfg.source_flush.enabled = False
        warnings.append(
            "source.transport=agent 时 source_flush 不生效（save-off 只会发给本机），"
            "已自动关闭；请确保 exporter 那台机器的世界已经落盘"
        )
    return warnings


# --------------------------------------------------------------------------- #
# 路径解析与校验
# --------------------------------------------------------------------------- #


def resolve_path(value: str, base: Path) -> Path:
    expanded = os.path.expanduser(value)
    path = Path(expanded)
    if not path.is_absolute():
        path = Path(base) / path
    return Path(os.path.normpath(str(path)))


def source_root(cfg: Config, base: Path) -> Path:
    return resolve_path(cfg.source.root, base)


def target_root(cfg: Config, base: Path) -> Path:
    """第一个（或唯一）镜像目标的根目录；多目标请用 ``target_path``。"""
    return target_path(cfg.enabled_targets()[0] if cfg.enabled_targets() else cfg.target, base)


def target_path(target: TargetConfig, base: Path) -> Path:
    return resolve_path(target.root, base)


def set_source_path(cfg: Config, item: SetConfig, base: Path) -> Path:
    return resolve_path(item.source, source_root(cfg, base))


def set_target_path(cfg: Config, item: SetConfig, base: Path) -> Path:
    return set_target_path_in(cfg, cfg.enabled_targets()[0] if cfg.enabled_targets() else cfg.target, item, base)


def set_target_path_in(cfg: Config, target: TargetConfig, item: SetConfig, base: Path) -> Path:
    return resolve_path(item.target, target_path(target, base))


def _is_within(child: Path, parent: Path) -> bool:
    try:
        child.relative_to(parent)
        return True
    except ValueError:
        return False


def validate_config(cfg: Config, base: Path) -> List[str]:
    """启动前校验。致命问题抛 ConfigError，可自动修正的问题返回 warning。"""
    warnings: List[str] = []

    if cfg.role not in ("driver", "exporter"):
        raise ConfigError(f"role 只能是 driver / exporter，当前为 {cfg.role!r}")

    src = source_root(cfg, base)
    all_targets = cfg.targets or [cfg.target]
    enabled_targets = cfg.enabled_targets()
    if not enabled_targets:
        raise ConfigError("没有启用任何镜像目标（targets 全部 enabled=false）")

    seen_names = set()
    seen_roots = set()
    for target in all_targets:
        if not target.name.strip():
            raise ConfigError("targets[].name 不能为空")
        if target.name in seen_names:
            raise ConfigError(f"target 名称重复：{target.name!r}")
        seen_names.add(target.name)
        dst = target_path(target, base)
        key = os.path.normcase(str(dst))
        if key in seen_roots:
            raise ConfigError(f"多个 target 指向同一目录：{dst}")
        seen_roots.add(key)
        if cfg.source.transport == "local" and (_is_within(src, dst) or _is_within(dst, src)):
            raise ConfigError(f"source.root 与 target {target.name!r} 不能互相包含：{src} / {dst}")

    if cfg.source.transport not in ("local", "agent"):
        raise ConfigError(f"source.transport 只能是 local / agent，当前为 {cfg.source.transport!r}")
    if cfg.source.transport == "agent":
        url = (cfg.source.agent.url or "").strip()
        if not url.lower().startswith(("http://", "https://")):
            raise ConfigError(f"source.agent.url 必须是 http(s) 地址，当前为 {url!r}")
        if len(cfg.source.agent.token or "") < 16:
            warnings.append(
                "source.transport=agent 但 source.agent.token 少于 16 个字符；"
                "agent 端会拒绝这种 token，请两边填同一个随机串"
            )
        if url.lower().startswith("http://") and not cfg.source.agent.tls_pin_sha256:
            warnings.append(
                "agent 走的是明文 HTTP；跨机部署建议上 TLS（https + tls_pin_sha256）"
            )
    if not src.exists() and cfg.source.transport == "local":
        # 不当成致命错误：路径可能还没建好，玩家也可能改完配置再 !!mu reload。
        # 真正执行同步时，_prepare 会因为“没有任何可用的 set”而中止。
        warnings.append(f"source.root 不存在：{src}（同步会中止，改好后 !!mu reload）")
    elif cfg.source.transport == "agent":
        warnings.append(f"源端为 agent（{cfg.source.agent.url}），不同步本地 {src}")

    names = set()
    for item in cfg.sets:
        if item.name in names:
            raise ConfigError(f"set 名称重复：{item.name!r}")
        names.add(item.name)

    for target in enabled_targets:
        seen_paths = set()
        for item in cfg.enabled_sets():
            dst_key = set_target_path_in(cfg, target, item, base).as_posix()
            if dst_key in seen_paths:
                raise ConfigError(
                    f"target {target.name!r} 上两个 set 指向同一目录：{item.target!r}"
                )
            seen_paths.add(dst_key)

    for item in cfg.enabled_sets():
        set_src = set_source_path(cfg, item, base)
        if cfg.source.transport == "agent":
            continue
        if not set_src.exists():
            warnings.append(f"set {item.name!r} 的源目录不存在，本轮跳过：{set_src}")
        elif not set_src.is_dir():
            raise ConfigError(f"set {item.name!r} 的源不是目录：{set_src}")

    if not cfg.enabled_sets():
        warnings.append("没有任何启用的 set，同步将什么都不做")

    def check_control(control: MirrorControlConfig, label: str) -> None:
        if control.adapter not in ("manual", "rcon", "external", "mcsm", "self"):
            raise ConfigError(f"未知的 {label}mirror_control.adapter：{control.adapter!r}")
        if control.adapter == "self" and cfg.role != "driver":
            raise ConfigError(f"{label}mirror_control.adapter=self 只能用在 role=driver 的节点上")
        if control.adapter == "rcon":
            if not control.rcon.password:
                warnings.append(f"{label}mirror_control.adapter=rcon 但 rcon.password 为空，停服命令会失败")
        elif control.adapter == "external":
            if not control.external.stop_cmd:
                warnings.append(f"{label}mirror_control.adapter=external 但 stop_cmd 为空，无法自动停镜像")
            if not control.external.start_cmd and control.start_after_sync:
                warnings.append(f"{label}start_after_sync=true 但 external.start_cmd 为空，同步后不会自动启动")
        elif control.adapter == "mcsm":
            mcsm = control.mcsm
            if not mcsm.base_url.startswith(("http://", "https://")):
                warnings.append(f"{label}mirror_control.mcsm.base_url 必须以 http:// 或 https:// 开头")
            if not mcsm.uuid or not mcsm.remote_uuid:
                warnings.append(f"{label}mirror_control.mcsm 需要 uuid 与 remote_uuid，否则无法定位实例")
            if not mcsm.apikey and not (mcsm.username and mcsm.password):
                warnings.append(
                    f"{label}mirror_control.mcsm 需要 apikey（MCSM 9）或 username+password（MCSM 10），否则无法停服"
                )

    check_control(cfg.mirror_control, "")
    for target in enabled_targets:
        if target.mirror_control is not None:
            check_control(target.mirror_control, f"target {target.name!r} 的 ")

    if cfg.role == "exporter" and len(cfg.agent_server.token or "") < 16:
        warnings.append(
            "role=exporter 但 agent_server.token 少于 16 个字符，agent 不会启动；"
            "请生成一个随机串（例如 python -c \"import secrets;print(secrets.token_hex(24))\"）"
        )

    return warnings


# --------------------------------------------------------------------------- #
# 写回（命令修改 schedule 等需要落盘）
# --------------------------------------------------------------------------- #


def _control_to_dict(control: MirrorControlConfig) -> Dict[str, Any]:
    return {
        "adapter": control.adapter,
        "rcon": {
            "host": control.rcon.host,
            "port": control.rcon.port,
            "password": control.rcon.password,
        },
        "external": {
            "stop_cmd": control.external.stop_cmd,
            "start_cmd": control.external.start_cmd,
            "probe_cmd": control.external.probe_cmd,
        },
        "mcsm": {
            "base_url": control.mcsm.base_url,
            "apikey": control.mcsm.apikey,
            "uuid": control.mcsm.uuid,
            "remote_uuid": control.mcsm.remote_uuid,
            "username": control.mcsm.username,
            "password": control.mcsm.password,
            "timeout_sec": control.mcsm.timeout_sec,
            "insecure_tls": control.mcsm.insecure_tls,
        },
        "stop_timeout_sec": control.stop_timeout_sec,
        "start_after_sync": control.start_after_sync,
        "wait_ready_timeout_sec": control.wait_ready_timeout_sec,
        "notify_players_sec": control.notify_players_sec,
    }


def _target_to_dict(target: TargetConfig) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "name": target.name,
        "root": target.root,
        "enabled": target.enabled,
    }
    if target.mirror_control is not None:
        data["mirror_control"] = _control_to_dict(target.mirror_control)
    return data


def config_to_dict(cfg: Config) -> Dict[str, Any]:
    """把强类型 Config 转回可写入的 dict（字段名与 config.json 一致）。"""
    return {
        "schema_version": cfg.schema_version,
        "role": cfg.role,
        "source": {
            "root": cfg.source.root,
            "transport": cfg.source.transport,
            "agent": {
                "url": cfg.source.agent.url,
                "token": cfg.source.agent.token,
                "tls_pin_sha256": cfg.source.agent.tls_pin_sha256,
            },
            "mtime_precision_sec": cfg.source.mtime_precision_sec,
        },
        "target": {"root": cfg.target.root},
        "targets": [_target_to_dict(t) for t in (cfg.targets or [cfg.target])],
        "sets": [
            {
                "name": item.name,
                "source": item.source,
                "target": item.target,
                "enabled": item.enabled,
                "mode": item.mode,
            }
            for item in cfg.sets
        ],
        "exclude": list(cfg.exclude),
        "sync": {
            "hash": cfg.sync.hash,
            "concurrency": cfg.sync.concurrency,
            "trust_mtime": cfg.sync.trust_mtime,
            "verify_target": cfg.sync.verify_target,
            "region_delta": cfg.sync.region_delta,
            "region_write_mode": cfg.sync.region_write_mode,
            "region_compaction_ratio": cfg.sync.region_compaction_ratio,
            "journal": cfg.sync.journal,
        "clean_mcc_orphans": cfg.sync.clean_mcc_orphans,
            "max_retry": cfg.sync.max_retry,
            "defer_unstable": cfg.sync.defer_unstable,
        },
        "source_flush": {
            "enabled": cfg.source_flush.enabled,
            "turn_off_auto_save": cfg.source_flush.turn_off_auto_save,
            "commands": dict(cfg.source_flush.commands),
            "saved_regex": cfg.source_flush.saved_regex,
            "timeout_sec": cfg.source_flush.timeout_sec,
        },
        "mirror_control": _control_to_dict(cfg.mirror_control),
        "schedule": {
            "auto_sync": cfg.schedule.auto_sync,
            "interval_seconds": cfg.schedule.interval_seconds,
            "max_interval_seconds": cfg.schedule.max_interval_seconds,
            "skip_if_source_empty": cfg.schedule.skip_if_source_empty,
        },
        "agent_server": {
            "host": cfg.agent_server.host,
            "port": cfg.agent_server.port,
            "token": cfg.agent_server.token,
            "tls": {
                "enabled": cfg.agent_server.tls.enabled,
                "cert": cfg.agent_server.tls.cert,
                "key": cfg.agent_server.tls.key,
            },
        },
        "log": {
            "history_keep": cfg.log.history_keep,
            "verbose_plan": cfg.log.verbose_plan,
        },
    }


def save_config(cfg: Config, path: Optional[Path] = None) -> Path:
    """原子写回配置（先写临时文件再替换）。"""
    target = Path(path or cfg.path or Path(CONFIG_FILENAME))
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    tmp.write_text(json.dumps(config_to_dict(cfg), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, target)
    cfg.path = target
    return target
