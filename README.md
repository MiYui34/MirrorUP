# MirrorUP

MCDR 镜像服**增量**同步插件：只传真正变了的文件，`.mca` 再细到 chunk。

## 特性

- **文件级增量**：跨轮索引记下每个文件的 `size / mtime / 哈希`，没变的文件不读、不传。
- **chunk 级增量**：region 文件只看脏 chunk（时间戳过滤 + 哈希确认），未变的 chunk 直接复用。
- **镜像纠偏**：镜像上被玩家改过的文件，下一轮会被发现并恢复。
- **原子写入**：先写临时文件再替换，镜像上不会出现半个文件；写入失败下一轮自动重试。
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
| `target.root` | `./Mirror/server` | 镜像服目录 |
| `sets` | `world`、`mods` | 同步哪些目录；`config/`、`server.properties` 默认不同步 |
| `exclude` | `session.lock`、`*.tmp`、`logs/**` 等 | glob 排除，命中目录整棵跳过 |
| `sync.concurrency` | `4` | 并行拷贝数 |
| `sync.verify_target` | `true` | 每轮检查镜像是否被改动 |
| `sync.region_delta` | `true` | `.mca` 走 chunk 级增量，`false` 退回整文件 |
| `source_flush.enabled` | `true` | 扫描前 `save-off` + `save-all flush`，扫描后 `save-on` |
| `schedule.interval_seconds` | `3600` | 自动同步间隔，60 ~ 604800 秒 |

## 命令

| 命令 | 权限 | 说明 |
|---|---|---|
| `!!mu sync` | 2 | 同步一次 |
| `!!mu sync --dry-run` | 2 | 只出计划，不动镜像 |
| `!!mu sync --full` | 2 | 忽略索引，全量重算哈希 |
| `!!mu sync --force` | 2 | 跳过安全检查（空源保护、版本门禁） |
| `!!mu diff` | 2 | 同 `--dry-run` |
| `!!mu status` | 0 | 索引规模、上次同步、待重试项 |
| `!!mu cancel` | 2 | 取消进行中的同步 |
| `!!mu schedule on\|off` | 3 | 开关自动同步 |
| `!!mu schedule set 30m` | 3 | 设置间隔（`s`/`m`/`h`/`d`） |
| `!!mu index rebuild` | 3 | 丢弃索引，下一轮全量 |
| `!!mu reload` | 3 | 重读配置 |

## 注意事项

- **镜像必须完全停止才能同步**。插件用 `session.lock` 判断镜像是否在运行；检测到运行中会拒绝写入。
- **不做备份**。镜像上被覆盖的区块和模组没有历史版本。
- **写入量不等于读取量**。当前写入方式是"重建 region 文件再替换"，改动一个 chunk 时目标端仍会重写整个 region；省下来的是源端读取和跨机传输（把写入也压到 O(脏 chunk) 的 `inplace` 模式在计划中）。
- **不覆盖配置文件**。`config/`、`server.properties` 不在默认同步范围内，手动加进去会覆盖镜像的配置。
- **空源保护**。源目录扫出 0 个文件但索引里有记录（路径写错、磁盘没挂载）时会中止，不会清空镜像；确认要清空请加 `--force`。
- **版本门禁**。源与镜像 `level.dat` 的 MC 版本不一致时拒绝写入。
- **外置 chunk 不增量**。使用 `.mcc` 外置容器的 region 会整文件复制。

## 常见问题

- **"扫描到 0 个文件，但索引中有 N 条记录"**：源路径写错或磁盘未挂载。修好后重跑；确实要清空镜像再加 `--force`。
- **"版本不一致"**：镜像世界来自旧版本。先用同一版本启动一次镜像，或确认要覆盖后 `--force`。
- **"待重试 N 项"**：文件在拷贝途中一直被改写或被占用（常见于正在写盘的 region），下一轮会自动重试。
- **换了世界或改了源根**：`world_id` 对不上会拒绝旧索引，执行 `!!mu index rebuild`。

## 版本

当前 `0.2.0`：文件级 + chunk 级增量、镜像纠偏、定时同步、手动停服检查。
