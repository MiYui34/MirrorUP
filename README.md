# MirrorUP

MCDR 镜像服**增量**同步插件：只传真正变了的文件，`.mca` 再细到 chunk。

## 特性

- **文件级增量**：跨轮索引记下每个文件的 `size / mtime / 哈希`，没变的文件不读、不传。
- **chunk 级增量**：region 文件只看脏 chunk（时间戳过滤 + 哈希确认），未变的 chunk 直接复用。
- **镜像纠偏**：镜像上被玩家改过的文件，下一轮会被发现并恢复。
- **原子写入**：先写临时文件再替换，镜像上不会出现半个文件；写入失败下一轮自动重试。
- **原地写 region**：`.mca` 默认 append-and-commit + 8KB 头 journal，一个 chunk 变了就只写它；碎片攒多了自动整理。
- **外置 chunk 也增量**：`.mcc` 外置容器的 region 只搬迁占位记录，内容变化按普通文件增量同步；不再被任何 region 引用的 `.mcc` 会被清理（`sync.clean_mcc_orphans`）。
- **跨机同步**：主服用 `exporter` 模式只读提供数据（HTTP + Bearer token，可上 TLS），镜像侧不用挂载主服目录。
- **自动停服**：支持 `rcon`、MCSManager（`mcsm`）与自定义命令（`external`）停/启镜像；停不下来就拒绝写入。
- **多镜像目标**：一份源、多个镜像目录，共享一个索引（按目标分区），每个目标可以有自己的停服方式。
- **校验与历史**：`!!mu verify` 核对源/镜像/索引三方是否一致（`--deep` 比哈希），`!!mu history` 查看每轮结果。
- **定时同步**：间隔 60 秒 ~ 7 天，可随时开关。

## 安装

需要 MCDR ≥ 2.12.0、Python ≥ 3.9。

1. 把插件目录放进 MCDR 的 `plugins/`（目录名建议 `MirrorUP`）。
2. 安装依赖：`pip install xxhash`。MCDR 只校验依赖、不会自动安装，缺依赖会在日志里报 `RequirementCheckFailure`。
3. 重启 MCDR，首次加载会生成 `config/mirror_up/config.json`。

## 快速开始

1. 编辑配置里的 `source.root`（主服目录）和 `target.root`（镜像服目录），相对路径以 **MCDR 根目录** 为基准。
2. **停掉镜像服**，在主服执行 `!!mu diff` 查看本轮计划。
3. 确认无误后执行 `!!mu sync`。
4. 看到"目标已更新完毕"后手动启动镜像服。
5. 需要自动同步：`!!mu schedule set 30m`，再 `!!mu schedule on`。

## 配置

首次启动生成 `config/mirror_up/config.json`，改完执行 `!!mu reload` 生效。

| 键 | 默认 | 说明 |
|---|---|---|
| `source.root` | `./server` | 主服目录（含 `world/`、`mods/`） |
| `source.transport` | `local` | `local` 直接读盘；`agent` 从主服的 exporter 拉取 |
| `source.agent.url` | `http://127.0.0.1:25580` | exporter 地址（`transport=agent` 时用） |
| `source.agent.token` | 空 | 与 exporter 的 `agent_server.token` 一致 |
| `source.agent.tls_pin_sha256` | 空 | 可选，固定自签证书指纹 |
| `role` | `driver` | `driver` 执行同步；`exporter` 只对外提供数据 |
| `target.root` | `./Mirror/server` | 镜像服目录（单目标时的简写；等价于 `targets[0]`） |
| `targets` | 一个 `default` | 多镜像目标：`[{"name","root","enabled","mirror_control"?}]`，每个目标可覆盖停服配置 |
| `sets` | `world`、`mods` | 同步哪些目录；`config/`、`server.properties` 默认不同步 |
| `exclude` | `session.lock`、`*.tmp`、`logs/**` 等 | glob 排除，命中目录整棵跳过 |
| `sync.concurrency` | `4` | 并行拷贝数 |
| `sync.verify_target` | `true` | 每轮检查镜像是否被改动 |
| `sync.region_delta` | `true` | `.mca` 走 chunk 级增量，`false` 退回整文件 |
| `sync.region_write_mode` | `inplace` | `inplace` 原地追加，`rebuild` 重建后替换 |
| `sync.journal` | `true` | `inplace` 的 region 头快照；`false` 会降级为 `rebuild` |
| `sync.clean_mcc_orphans` | `true` | 删除不再被任何 region 引用的 `.mcc` 外置 chunk |
| `log.history_keep` | `20` | `!!mu history` 保留多少轮 |
| `source_flush.enabled` | `true` | 扫描前 `save-off` + `save-all flush`，扫描后 `save-on` |
| `mirror_control.adapter` | `manual` | `manual` 只检查；`rcon` / `external` 可自动停启镜像 |
| `mirror_control.rcon` | `127.0.0.1:25575` | RCON 停服参数 |
| `mirror_control.external` | 空 | `stop_cmd` / `start_cmd` / `probe_cmd` 自定义命令 |
| `mirror_control.mcsm` | 空 | MCSManager 面板：`base_url`、`apikey`（v9）或 `username`/`password`（v10）、`uuid`、`remote_uuid` |
| `mirror_control.start_after_sync` | `true` | 同步后自动启动镜像并等端口就绪 |
| `agent_server` | `127.0.0.1:25580` | exporter 的监听地址、token 与 TLS 证书 |
| `schedule.interval_seconds` | `3600` | 自动同步间隔，60 ~ 604800 秒 |

## 跨机同步（可选）

主服与镜像不在同一台机器、或者不想让镜像侧挂载主服目录时，用 `exporter` + `agent`：

1. **主服**（MCDR 里装同一个插件）：
   - `role` 改成 `"exporter"`；
   - `agent_server.host` 改成 `0.0.0.0`（或内网 IP），`port` 默认 `25580`；
   - `agent_server.token` 填一个至少 16 字符的随机串；
   - 执行 `!!mu reload`（或重启 MCDR），日志出现「exporter 已监听」。
2. **镜像侧**（driver）：
   - `source.transport` 改成 `"agent"`；
   - `source.agent.url` 填 `http://<主服IP>:25580`；
   - `source.agent.token` 填同一个 token。
3. 公网部署请开 TLS（`agent_server.tls.enabled` + 证书），并在驱动端填 `tls_pin_sha256` 固定指纹。

exporter 只读：它不写任何东西，也不执行同步命令。`!!mu status` 在 exporter 上显示监听地址与对外提供的 set。

## 命令

| 命令 | 权限 | 说明 |
|---|---|---|
| `!!mu sync` | 2 | 同步一次 |
| `!!mu sync --dry-run` | 2 | 只出计划，不动镜像 |
| `!!mu sync --full` | 2 | 忽略索引，全量重算哈希 |
| `!!mu sync --force` | 2 | 跳过安全检查（空源保护、版本门禁） |
| `!!mu diff` | 2 | 同 `--dry-run` |
| `!!mu status` | 0 | 索引规模、各目标上次同步、待重试项 |
| `!!mu verify [--deep]` | 2 | 校验源/镜像/索引一致性；`--deep` 逐个比哈希（会读全部内容） |
| `!!mu history [N]` | 0 | 最近 N 轮同步结果（默认 10，最多 50） |
| `!!mu cancel` | 2 | 取消进行中的同步 |
| `!!mu schedule on\|off` | 3 | 开关自动同步 |
| `!!mu schedule set 30m` | 3 | 设置间隔（`s`/`m`/`h`/`d`） |
| `!!mu index rebuild` | 3 | 丢弃索引，下一轮全量 |
| `!!mu reload` | 3 | 重读配置 |

## 注意事项

- **镜像必须完全停止才能同步**。默认 `manual` 适配器用 `session.lock` 判断；`rcon` / `external` 可以自动停服——但停不下来时会中止同步，绝不在镜像还活着时写盘。
- **不做备份**。镜像上被覆盖的区块和模组没有历史版本。
- **写入量不等于读取量**。`.mca` 默认 `inplace`，改动一个 chunk 时目标端只写这个 chunk 加 16KB（新头 + journal）；`rebuild` 模式才会重写整个 region。
- **不覆盖配置文件**。`config/`、`server.properties` 不在默认同步范围内，手动加进去会覆盖镜像的配置。
- **空源保护**。源目录扫出 0 个文件但索引里有记录（路径写错、磁盘没挂载）时会中止，不会清空镜像；确认要清空请加 `--force`。
- **版本门禁**。源与镜像 `level.dat` 的 MC 版本不一致时拒绝写入。
- **跨机要保证 token 一致**。driver 与 exporter 的 token 不同会直接连不上；exporter 是只读的，不提供任何写入接口。

## 常见问题

- **"扫描到 0 个文件，但索引中有 N 条记录"**：源路径写错或磁盘未挂载。修好后重跑；确实要清空镜像再加 `--force`。
- **"版本不一致"**：镜像世界来自旧版本。先用同一版本启动一次镜像，或确认要覆盖后 `--force`。
- **"待重试 N 项"**：文件在拷贝途中一直被改写或被占用（常见于正在写盘的 region），下一轮会自动重试。
- **换了世界或改了源根**：`world_id` 对不上会拒绝旧索引，执行 `!!mu index rebuild`。

- **"源端不可用"**：跨机时连不上 exporter、token 不对，或主服没开 `role=exporter`。检查地址、端口、token 和主服日志里的「exporter 已监听」。
- **"自动停服失败"**：`rcon` 连不上或 `external` 脚本返回非零。镜像没停下来就不会写盘，修好适配器再同步。

## 版本

当前 `0.5.0`：文件级 + chunk 级增量、原地写 region（含头 journal 与碎片整理）、镜像纠偏、定时同步、跨机 agent 传输（含 TLS）、`.mcc` 外置 chunk 增量与孤儿清理、多镜像目标（共享索引）、校验与同步历史、MCSM/rcon/external 自动停服、Python API。

## 许可证

[GPL-3.0](LICENSE) © MiYui34。分发修改版需要同样以 GPL-3.0 开源，并保留版权声明。
