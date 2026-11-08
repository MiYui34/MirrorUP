# MirrorUP 设计文档

> 状态：草案 v0.1
> 定位：MCDReforged 镜像服**增量同步引擎**
> 本文只描述设计，不含实现代码。

---

## 0. 摘要

MirrorUP 是一个 MCDR 插件，负责把主服（source）的世界与模组文件同步到镜像服（target）。

与现有插件（[MirrorServerReforged](https://mcdreforged.com/zh-CN/plugin/mirror_server_reforged)、[MirrorMcsmcdR](https://mcdreforged.com/zh-CN/plugin/mirror_mcsmcdr/readme)）的根本差别在于**每轮同步的 IO 成本不是 O(世界大小)，而是 O(本轮真实变更量)**：

1. **跨轮索引**：持久化上一轮每个文件的 `(size, mtime, hash)`，命中即跳过，不读文件内容。
2. **region 内 chunk 级增量**：`.mca` 是"文件里的容器"，一个 8MB 的 region 只改一个区块时，只传输/写入那一个 chunk 记录。
3. **崩溃安全**：目标文件永远通过"临时文件 + 原子替换"或"先写数据、后提交头"落地，任何时刻崩溃都不会留下半写的 region。

支持**同机**（直接文件 IO）与**跨机**（源端 agent + 拉取式增量协议）两种拓扑。

---

## 1. 目标与非目标

### 1.1 目标

| 编号 | 目标 | 验收方式 |
|---|---|---|
| G1 | 未变更文件零读取、零写入 | 单轮 IO 计数器 |
| G2 | 变更文件只传输变更部分（region 下沉到 chunk） | 单轮传输字节数 |
| G3 | 任意时刻崩溃不产生半写文件 | `kill -9` 注入测试 + 全量校验 |
| G4 | 同机 / 跨机两种拓扑，配置切换 | 集成测试 |
| G5 | 可解释：干跑能列出将发生的每一个操作 | `!!mu diff` 输出 |
| G6 | 源端只读，绝不修改源服任何文件 | 代码审计 + 只读挂载测试 |

### 1.2 非目标

- **不做零停机**：镜像服在应用阶段必须停止（见 §10）。已确认接受。
- **不做镜像侧备份/回滚**：镜像上被覆盖的区块、被替换的模组文件直接丢失，不保留历史版本（见 §11）。
- **不做版本/区块格式转换**：源与镜像必须是同一 MC 版本、同一模组集合。
- **不解析区块 NBT**：chunk payload 视为不透明字节流，这是跨版本兼容的关键。
- **不做镜像服控制面板**：不实现 MCSM / screen / subprocess 的全套控制，只做可插拔的最小启停适配器。
- **不做 P2P 分发**：一个源到多个镜像时，每个镜像独立拉取（可在路线图中再评估）。
- **不同步 `config/` 与 `server.properties`**：这类"各服不同"的配置由用户手工维护，覆盖会让镜像起不来（见 D8）。

---

## 2. 术语

| 术语 | 含义 |
|---|---|
| source | 主服，数据的唯一真相来源，只读 |
| target | 镜像服，被覆盖写入 |
| set | 一个同步单元，形如 `源相对路径 → 目标相对路径`，如 `world → world` |
| round | 一次完整的同步轮次，从 flush 源服到重启镜像服结束 |
| snapshot | 本轮扫描得到的源端文件状态集合（路径 + size + mtime + hash） |
| index | 持久化在镜像侧的 SQLite 数据库，记录上一轮成功同步后的状态 |
| region | `.mca` 文件，32×32 区块的容器 |
| chunk record | region 内单个区块的磁盘记录：4B 长度 + 1B 压缩类型 + 压缩数据 |
| dirty | 相对 index 发生变化的对象（文件 / chunk） |
| plan | 由 index 与 snapshot 求差得到的有序操作列表 |
| apply | 把 plan 落到 target 磁盘的过程 |

---

## 3. 背景：现有插件的能力边界

| 方案 | 变更检测 | 传输粒度 | 每轮成本 |
|---|---|---|---|
| MirrorServerReforged 1.0.8 | 无（整目录拷贝） | 文件 | O(世界大小) 读 + 写 |
| MirrorMcsmcdR 1.7.0 | 每轮对两侧文件全量 `xxh128` 比对 | 文件 | **2×世界大小** 读 + O(变更) 写 |
| **MirrorUP** | 跨轮索引 + region 头/chunk 表 | **chunk** | **O(目录项) + O(真实变更)** |

关键区别不在"有没有哈希"，而在**有没有跨轮基线**：

- 只有 `size/mtime` 时，`mtime` 会说谎（备份还原、工具改时间戳、SMB 共享 2 秒精度、跨机时钟偏差），因此必须读内容算哈希才能确认——这一读就是整个世界的读。
- 有了基线哈希，"没变"这件事上一轮就已经证明过了，本轮只需比对目录项。
- 有了 region 头，"这个大文件里哪个区块变了"不需要读整个文件就能回答。

**为什么不用 rsync / rdiff？** 滚动校验和在 region 上不如 chunk 表精确（会有伪匹配），Windows 无原生 rsync，且 rdiff 仍需读完整文件。**为什么不用 btrfs/zfs send？** 平台不可控，作为可选加速在 §20 讨论。

**关于 `config/`**：两个现有插件都没有对配置目录做任何特殊处理——同步范围完全由配置里的目录列表决定（MirrorMcsmcdR 的 `sync.world` 默认只有 `world`），而 `source` 默认指向 `./server`，`config/` 位于 MCDR 根目录、天然在同步根之外。MirrorUP 沿用这个结论：**不同步 `config/` 与 `server.properties`**（见 D8）。

---

## 4. 关键决策（ADR）

### D1 — chunk 级增量用「容器格式」而不是「块级 diff」

**决策**：解析 region 的 8KB 头，按 chunk 记录哈希；不对整个文件做 4KB 块级 diff。

**理由**：块级 diff 更简单且完全不懂格式，但会产生两类问题：(a) 头部时间戳表每轮都变，8KB 头总是脏；(b) chunk 被重新分配到别的扇区时，块级 diff 会把整段数据判为变更，而 chunk 哈希能识别出"内容没变、只是搬家"。chunk 级还能直接复用源端的 `.mcc` 外置文件同步逻辑。

**代价**：我们开始拥有"写 region 文件"的代码，这是本项目最大的工程风险（见 §21-R1）。

### D2 — 不解析 NBT

**决策**：chunk payload 整体作为不透明字节；哈希只覆盖磁盘上的原始记录。

**理由**：区块格式随版本剧烈变化（1.13 扁平化、1.18 高度、1.20.5 外置 chunk），而容器格式十几年没大改。不解析 NBT 意味着 1.13 ~ 最新的世界都能同步，且实现量减少一个数量级。

**例外**：`level.dat` 需要读 3 个字段（`Data.Version.Name`、`Data.Seed`、`Data.LevelName`）用于世界身份识别，写入约 120 行的最小 NBT 读取器，不用于同步逻辑。

### D3 — 镜像侧停机，不做热补丁

**决策**：apply 阶段镜像服必须完全停止。

**理由**：服务端有 `RegionFileCache`，热改 `.mca` 它看不见，且会在下次保存时用内存数据盖回去；vanilla 没有任何可靠的区块重载手段。热补丁的收益（省掉 JVM 启动那 30–60 秒）远小于它带来的存档损坏风险。

### D4 — 索引只能"落后"于磁盘，不能"超前"

**决策**：先落盘（fsync + 原子替换），后提交索引；索引按批提交，允许落后。

**理由**：索引超前 = 下一轮会跳过实际未写入的文件 = 静默不一致。索引落后 = 下一轮重复应用同一内容 = 幂等、只是浪费一点 IO。方向必须是安全的那一侧。

### D5 — 不做镜像侧备份

**决策**：不保留被覆盖文件的旧版本，不提供"回滚到上一轮"。

**理由**：产品定位是镜像（可丢弃的副本），备份应由 QuickBackupM / PrimeBackup 之类专门的插件负责。
**边界**：崩溃安全所需的**瞬时事务记录**（如 region 头快照，每文件 8KB，成功后立即删除）不算备份，它只保证"不会写坏"，不提供"回到过去"的能力，且可通过配置关闭。

### D6 — 拉取式（pull），而不是推送式

**决策**：由镜像侧发起连接，源端只提供只读接口。

**理由**：防火墙只需要源端开一个入站端口；凭据存放在镜像侧（次要机器）而不是主服；源端天然只读，符合 G6。

### D7 — 只做同步引擎，不做控制面板

**决策**：启停通过适配器接口，v1 只提供 `manual`（要求已停止）与 `rcon`，其余留给社区或后续版本。

**理由**：MirrorMcsmcdR 已经覆盖 MCSM/screen/subprocess 四套控制，重复实现没有价值。对外提供 Python API，允许别的控制插件把 MirrorUP 当库调用。

### D8 — 不同步 `config/` 与 `server.properties`

**决策**：默认集合只有 `world` 与 `mods`。配置目录、服务端属性文件不在同步范围内，也不提供"默认开启"的开关；确需同步时必须由用户显式添加 set。

**理由**：
- 这些文件里的值**本来就该各服不同**（端口、`level-name`、`server-port`、`online-mode`、模组各自的门控参数）。照搬过去最典型的结果是镜像服端口被主服占用值顶掉、或者 MCDR 连不上自己的镜像。
- 一旦覆盖坏了，镜像起不来，而错误信息（"端口被占用"/"找不到世界"）离"同步覆盖了配置"这个真实原因很远，排查成本高。
- 现有插件（MirrorServerReforged、MirrorMcsmcdR）实际上都不管这些文件，说明社区共识是不管。

**边界**：不是技术上禁止。`sets` 是用户可编辑的，加一条 `{"source": "config", "target": "config"}` 就会同步；此时 `!!mu diff` 必须把"将覆盖配置文件"标红提示。

### D9 — 每轮顺手校验镜像侧（`verify_target`）

**背景**：镜像服的玩家不是只读观众。他们在镜像上挖过的方块、改过的模组配置都会留在镜像磁盘上；而增量同步只看“源变没变”——源没变就一个 op 都不产生，镜像被改坏的文件**永远不会被修回来**。

**决策**：每轮对目标做一次 **stat-only** 扫描（不读内容、不算哈希），与索引里的 `(size, mtime_ns)` 比对，发现分歧就产生一个 `force_write=True` 的 MODIFY，直接覆盖。默认开启，配置项 `sync.verify_target`。

**为什么不是“每轮对两侧都算全量哈希”**：那就是 MirrorMcsmcdR 的做法，稳态每轮要读 2×世界大小。stat 扫描的代价是 O(文件数) 次 `stat`（十几 GB 的世界约几万个文件，Windows 上量级是秒），换来的保证是“镜像的最终内容等于源”。这是本项目相对 MirrorMcsmcdR 的核心提速点，不能在实现时退化。

**误判防线**：
- 只比 `size` 与 `mtime_ns`；`mtime_ns > last_sync_end` 才算“被改过”。我们自己上一轮的写入都发生在 `last_sync_end` 之前，所以不会把自家写入误判成篡改。
- 目标根读不到（权限、路径错）时**放弃校验**，绝不把“读不到”当成“全部缺失”去删/建。
- 校验发现的差异只做覆盖，不做删除：镜像上多出来的文件由 L1 的正常删除逻辑处理，两条路径分开。

---

## 5. 架构总览

```
┌──────────────────────── 源端（主服，只读） ────────────────────────┐
│  Minecraft Server                                                  │
│      │ 日志/RCON                                                   │
│      ▼                                                             │
│  [Source Agent]  ── 扫描、哈希、region 头解析、只读 HTTP 服务        │
└───────────────────────────────┬────────────────────────────────────┘
                                │ HTTP + token（跨机）
                                │ 或直接文件 IO（同机）
┌───────────────────────────────▼────────────────────────────────────┐
│  [Sync Driver]（镜像侧，MCDR 插件）                                 │
│   Scheduler → Planner → Applier → Index(SQLite)                    │
│                    │            │                                  │
│                    │            └──→ 原子写 / region 补丁           │
│                    └──→ Mirror Control Adapter（stop/start）        │
└────────────────────────────────────────────────────────────────────┘
```

**同机拓扑**：`transport = local`，Source Agent 退化为进程内模块，直接 `os.scandir` / 随机读。
**跨机拓扑**：`transport = agent`，源端也安装 MirrorUP 并以 `role = exporter` 启动，只开 HTTP 服务，不做任何写入。

模块划分：

| 模块 | 职责 | 可独立测试 |
|---|---|---|
| `source` | 抽象：`scan() / stat() / copy_to() / scan_region() / get_records() / read_bytes()` | ✓ 用本地实现做基准 |
| `scanner` | 遍历 set，产出 snapshot | ✓ |
| `hasher` | 文件哈希、chunk 表计算 | ✓ |
| `region` | 头解析、chunk 记录定位与哈希、文件重建/补丁 | ✓ 重点 fuzz |
| `planner` | index × snapshot → plan | ✓ 纯函数 |
| `applier` | plan → 磁盘 + 索引提交 | ✓ |
| `mirror_ctl` | 启停适配器 | ✓ mock |
| `scheduler` | 定时、手动触发、互斥 | ✓ |
| `agent_http` | 只读 HTTP 服务 | ✓ |

---

## 6. 变更检测模型

三层，从便宜到贵，只有上一层命中才进入下一层。

### L1 — 路径集合（增 / 删）

index 里存着上一轮所有路径。本轮 snapshot 的路径集合与之求差：

- snapshot 有、index 无 → **新增**（必须读内容）
- index 有、snapshot 无 → **删除**
- 两边都有 → 进入 L2

成本：一次目录遍历。NTFS/ext4 上 `os.scandir` + `DirEntry.stat()` 走的是 `FindFirstFile` 缓存，15 万文件约 0.3–1s；文件系统元数据缓存热时更快。

### L2 — 文件元数据

| 情况 | 判定 | 动作 |
|---|---|---|
| `size` 不同 | 变更 | 读源文件，算哈希 |
| `mtime_ns` 不同 | **疑似**变更 | 读源文件，算哈希确认 |
| `size` 与 `mtime_ns` 都相同，且 `mtime_ns >= index.synced_at` | 变更 | 读源文件，算哈希确认 |
| 都相同，且 `mtime_ns < index.synced_at` | 未变 | **跳过**（零读） |
| `mtime_ns < index.synced_at` 但 `size` 不同 | 疑似"备份还原" | 强制全量重哈希该 set |

最后两条是防 `mtime` 说谎的核心：如果源端文件的时间戳**比我们上次同步的时间还早**，说明它不是正常写入产生的，必须怀疑。

`region` 文件在同 mtime 时不要指望它"没变"——服务端 `RegionFile.flush()` 会更新 mtime，所以正常写入一定会被 L2 捕获。

### L3 — region 内 chunk

只对**已经存在于 index 且文件级判定为脏**的 region 生效。

```
1. 读源端 region 头 8KB（位置表 + 时间戳表）
2. 对每个非空 chunk：
     if chunk_ts < last_snapshot_time:      # 保守过滤
         沿用 index 中的哈希（视为未变）
     else:
         定位 offset/sectors，读该 chunk record，算 xxh3-64
3. 与 index.regions.chunk_hashes 逐项比对 → 得到"变更 chunk 位图"
```

`last_snapshot_time` 取**上一轮开始扫描时的源端时间**，而不是"上一轮结束时间"。这保证过滤方向是保守的（多查，不会漏）：任何在上一轮快照之后写入的 chunk 都会被重新哈希。

首次同步、或 index 中无该 region 记录时，不做 chunk 级比较，直接整文件复制（顺序大 IO 最快）。

**外置 chunk（`.mcc`）的处理**：当压缩类型字节 `& 0x80` 时，region 内只有 5 字节占位记录，真实数据在同目录的 `c.<x>.<z>.mcc`。此时 chunk 哈希必须定义为：

```
H(5 字节占位记录 || H(mcc 文件内容))
```

否则所有外置 chunk 会因为占位记录相同而互相误判为"未变更"。`.mcc` 文件本身作为普通文件参与 L2 比对。

---

## 7. 索引

### 7.1 位置与生命周期

- 路径：`config/mirror_up/index.sqlite3`（镜像侧）
- `PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;`
- 可随时删除重建：删除后下一轮退化为全量（进度可见、可断点续传）。
- 每个 `world_id` 一份索引；`world_id` 不匹配时拒绝使用并要求重建，防止把 A 世界的索引套到 B 世界。

`world_id = sha256(source_root + Data.Version.Name + Data.Seed + Data.LevelName)`。

**不含 set 列表**（实现时修正）：早期版本写的是“首个 set 名”，但 set 的增删改名不该让整份索引失效——把 `mods` 加进配置就会触发全量重读，代价与收益不成比例。set 名只在 `sets` 表里做行级映射，改名等价于“旧 set 的行全部丢失 → 该 set 下一轮全量”，影响面被限制在一个 set 内。

### 7.2 Schema

```sql
CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
-- schema_version / world_id / mc_version / mods_digest
-- last_snapshot_time / last_sync_begin / last_sync_end / last_full_scan_at

CREATE TABLE sets (
  id         INTEGER PRIMARY KEY,
  name       TEXT NOT NULL UNIQUE,
  source_rel TEXT NOT NULL,
  target_rel TEXT NOT NULL
);

CREATE TABLE files (
  set_id     INTEGER NOT NULL,
  path       TEXT    NOT NULL,   -- 相对 set root，'/' 分隔，NFC 规范化
  kind       INTEGER NOT NULL,   -- 0 文件 / 1 目录（仅用于删除时清理空目录）
  size       INTEGER NOT NULL,
  mtime_ns   INTEGER NOT NULL,
  hash       BLOB,               -- 16B xxh3-128；NULL = 尚未计算
  region     INTEGER NOT NULL,   -- 1 = 该文件是 region，有 regions 行
  synced_at  INTEGER NOT NULL,   -- 本轮 snapshot_time
  PRIMARY KEY (set_id, path)
) WITHOUT ROWID;

CREATE TABLE regions (
  set_id       INTEGER NOT NULL,
  path         TEXT    NOT NULL,
  chunk_hashes BLOB    NOT NULL, -- 1024 × 8B xxh3-64，全 0 表示空 chunk
  header       BLOB,             -- 可选：8192B 源端头快照，用于 verify
  PRIMARY KEY (set_id, path)
) WITHOUT ROWID;

CREATE TABLE pending (
  set_id INTEGER NOT NULL,
  path   TEXT    NOT NULL,
  reason TEXT    NOT NULL,       -- unstable / busy / too_large / repr
  at     INTEGER NOT NULL,
  PRIMARY KEY (set_id, path)
) WITHOUT ROWID;
```

**v0.5 实际 schema（多目标）**：新增 `targets(id, name UNIQUE, root)`；`files` / `regions` / `pending` 三张表都加了 `target_id INTEGER NOT NULL` 并进入主键——同一个 set 不再只有一份索引，而是每个镜像目标各一份。新增 `history(id, target, trigger, started_at, finished_at, ok, dry_run, applied, deleted, deferred, failed, bytes_written, duration, summary, detail)` 支撑 `!!mu history`。`DEFAULT_TARGET_ID = 1`，单目标配置下行为与 v0.4 相同；旧库升级时三张表先改名 `*_v4`、按 `target_id = 1` 灌回再删旧表。`meta` 里每轮的时间戳按目标分区，键名后缀 `:<target_id>`（`target_id = 1` 时回退读旧的无后缀键，兼容 v0.4）。

体量估算：15 万文件 × 约 120B ≈ 18MB；3000 region × (8KB 哈希表) ≈ 24MB；加上 `header` 列约 48MB。可接受。关闭 `header` 列可省一半。

`pending` 表记录本轮因"文件正在被写、重试耗尽"而延后的对象，下一轮优先处理并在 `!!mu status` 中显示。

### 7.3 提交顺序不变式

```
fsync(数据文件) → 原子替换 → fsync(目录) → SQLite INSERT/UPDATE → COMMIT
```

批量提交（默认 200 条或 1 秒，取先到者），崩溃最多回退一个批次，重放幂等。
**禁止**先提交索引再写文件。

---

## 8. Region 容器与 chunk 增量

### 8.1 磁盘布局

```
偏移            内容
[0, 4096)       位置表：1024 × 4B（BE）
                  字节 0..2  = 扇区偏移（相对文件头，单位 4096B）
                  字节 3     = 扇区数（1..255）
[4096, 8192)    时间戳表：1024 × 4B（BE，Unix 秒）
[8192, EOF)     扇区区，每个 chunk record：
                  4B 长度（BE，含压缩类型字节）
                  1B 压缩类型
                  (长度-1)B 压缩数据
                补齐到 4096 边界
```

索引 `i = (x & 31) + 32 * (z & 31)`，`x = i % 32`，`z = i / 32`。

压缩类型：`1` gzip、`2` zlib、`3` 不压缩（1.15.1+）、`4` LZ4（24w04a+）、`127` 自定义（24w05a+）、`值|0x80` 表示外置 `.mcc`。

### 8.2 校验规则（不满足即判定文件异常）

```
offset >= 2                 # 不能落在头里
1 <= sectors <= 255
(offset + sectors) * 4096 <= file_size
length + 4 <= sectors * 4096
compression in {1,2,3,4,127} or (compression & 0x80)
文件大小 % 4096 == 0
```

任一条不满足 → 该 region 标记为 `corrupt`，降级为**整文件复制**并告警。绝不尝试"修复"。

### 8.3 chunk record 哈希

```python
def chunk_digest(f, offset, sectors):
    rec = pread(f, offset * 4096, 4)
    length = be32(rec)
    body = pread(f, offset * 4096 + 4, length)
    if body[0] & 0x80:
        return xxh3_64(rec + body + mcc_digest_of(offset))
    return xxh3_64(rec + body)
```

哈希覆盖**逻辑记录**，不含扇区填充字节——因此同一个 chunk 换了位置、填充长度不同，哈希不变。

### 8.4 目标侧写入：两种模式

#### 模式 `rebuild`（v0.3 之前的默认，最安全）

把"目标现有 chunk + 源端变更 chunk"拼成一个新文件，原子替换：

```
tmp = <dir>/.<name>.<round>.mca.tmp
write 8KB header 占位
for i in 0..1023:
    if bitmap[i]:  rec = 源端 chunk record
    else:          rec = 目标旧文件中该 chunk record
    if rec 为空: continue
    allocate sectors; write rec
回填 header（位置 + 时间戳）
fsync(tmp)
os.replace(tmp, target)      # 同卷，元数据操作，原子
```

- 优点：不可能产生半写文件，天然去碎片。
- 代价：每个脏 region 需 `读源 + 读旧目标 + 写新目标` ≈ 3× 文件大小。

#### 模式 `inplace`（v0.3 默认，append-and-commit）

```
1. 读目标头 → 位置表 + 时间戳表 + 空闲扇区位图
2. for 每个变更 chunk:
       标记其**原有扇区为保留**（本次绝不复用，避免覆盖尚未提交的旧数据）
       在空闲区/文件尾部找连续 sectors 个扇区
       write record + fsync
3. 一次性写新的位置表 + 时间戳表（各 4KB）
   fsync
4. 旧扇区在内存中标记为可回收；文件暂不截断
```

崩溃安全性：头提交之前，旧 chunk 的扇区**从未被覆盖**，所以旧头永远指向完好数据；新写入的只是孤儿数据，等下次 compaction 回收。

- 读写量：`8KB 头 + 变更 chunk 记录`，通常每轮每 region 几十 KB。
- 代价：文件会增长；当 `孤儿扇区 > 存活扇区 * 0.5` 时触发 compaction（即 `rebuild`）。
- 头部撕裂保护：提交前把旧 8KB 头写入 `config/mirror_up/journal/<set>/<sha1(path)>.hdr`，提交成功后删除。崩溃时若头校验失败，用该快照恢复（此时旧数据仍在，可完整恢复旧状态）。可通过 `journal: false` 关闭。

### 8.5 顺序约束

`.mcc` 文件必须在引用它的 region 头之前落盘，否则镜像启动时可能读到"头指向不存在的文件"。

```
写 .mcc（原子替换） → fsync → 提交 region 头
```

删除时相反：先删 region 中的引用（或整个 region），再删 `.mcc`。

> 注：v0.2 / v0.3 都没有实现 `.mcc` 的增量，见 §8.6 第 4 条。

### 8.6 v0.2 实现记录（与上文规范的差异）

v0.2 落地后，实现与 §8.3–§8.5 的差异如下。**以本节为准**，上文保留为设计意图。

1. **哈希是 xxh3-128（16B）**，不是文中的 8B。索引里的 `regions.chunk_hashes` 不是 1024×8B 定长表，而是 `MU2` 编码的稀疏 blob：`b"MU2" + >H count + (index >H, digest 16B)*`——只存非空 chunk。
2. **`rebuild` 的实际做法是"复制目标 + 补丁"**，不是逐 chunk 拼新文件：`shutil.copyfile(dst, tmp)` 之后，未变的 chunk 原样留在 tmp 上；变了的 chunk 若放得进原扇区就原地覆盖，放不下就从文件尾部追加；最后回填 8KB 头、`truncate` 到末扇区、`fsync`、`os.replace`。效果等价（目标文件从未被写过、不可能半写），但写量 = 复制目标 + 脏 chunk，见 §17 的说明。
3. **脏过滤用 `last_sync_begin`**（上一轮开始扫描的 Unix 秒，取自索引 `meta`），不是 `last_snapshot_time`。两者在实现里是同一个量级，但 `last_sync_begin` 保证包含"上一轮扫描期间写入的 chunk"。
4. **外置 `.mcc` 只降级、不实现**：读到压缩类型 `& 0x80` 直接抛 `RegionExternalChunk`，整文件复制；§8.5 的 `.mcc` 顺序约束与 `.mcc` 哈希拼接都未实现（v0.4 再说）。
5. **压缩类型不做白名单校验**：payload 当作不透明字节参与哈希，任何压缩类型都不影响正确性；只有 `& 0x80` 是语义信号。
6. **校验只做三条硬规则**：`offset >= 2`、`1 <= sectors <= 255`、`(offset+sectors)*4096 <= file_size`；`文件大小 % 4096 == 0` 未强制（截断文件会在读记录时因越界被拒）。
7. **读—复验**：读完所有脏 chunk 后复读 8KB 头并复 `stat`，任一变化抛 `RegionUnstable` 交给 applier 重试，与 §10.1 的乐观校验一致。
8. **索引只存 chunk 哈希、不存 offset/sectors**：目标端布局独立于源端，每次写都按目标现有头决定落点（这是 §8.3"哈希不含填充"能成立的前提）。

### 8.7 v0.3 实现记录（inplace / journal / compaction）

v0.3 落地后，`inplace` 与 §8.4 的设计差异如下，**以本节为准**：

1. **不做空闲扇区位图**：设计里的"在空闲区找位置"被简化为"一律追加到文件末尾"。因为不变式要求绝不覆盖正在替换的旧扇区，空闲区本来也只能用来说明碎片位置；而碎片回收交给 compaction 统一处理，运行时就不必维护位图。代价是每改一个 chunk 多占一个扇区，由 compaction 回收。
2. **未变的 chunk 只搬位置表、不搬数据**：新头里照抄旧头的 `(offset, sectors)`，这些扇区原样不动。
3. **写入顺序**：追加新 chunk 记录 → `fsync` → 写 journal（`os.replace` + `fsync`）→ 写新 8KB 头 → `fsync` → 删 journal。崩溃在"删 journal 之前"，下一轮恢复旧头并重做；崩溃在"删 journal 之后"，新头已提交，旧头不再被引用。
4. **journal 文件**：`config/mirror_up/journal/<set>/<sha1(相对路径)>.hdr`，格式 `MUJ1 | 4B 路径长度 | 路径 | 16B 头哈希 | 8192B 旧头`。路径必须是 set 内相对路径，拒绝 `..` 与绝对路径（防止被改坏的文件指到 set 外）。
5. **恢复时机**：每轮 `_prepare` 里、非 dry-run 时按 set 调 `recover_all_journals`，恢复条数作为 warning 报给用户；dry-run 不碰目标。
6. **`written_bytes` 口径**：数据扇区 + 8192（新头）+ 8192（journal）。所以"只改一个 chunk"的一轮写量是 20480 字节，不是 4096。
7. **compaction 条件**：`总扇区 > 存活扇区 * (1 + ratio)`（存活 = 2 个头扇区 + 各 chunk 占用），命中就走 `rebuild` 路径并标记 `compacted=True`，`!!mu` 报告里显示"（碎片整理）"。
8. **降级**：`journal=false` → `inplace` 降级为 `rebuild` 并告警；目标不存在、目标头解析失败、镜像被外部改动（`force_write`）→ 直接走 `rebuild`。
9. **仍未做**：`.mcc` 增量、RCON/external 启停（`.mcc` 仍整文件复制，`mirror_control` 仍是 manual）。

---

## 9. 同步流程

### 9.1 状态机

```
IDLE
 └─(触发)─→ PREPARING        校验配置、获取互斥锁、检查镜像状态
              └─→ FLUSHING    save-off → save-all flush → 等日志
                    └─→ SNAPSHOT    扫描源端，记录 snapshot_time
                          └─→ PLAN      与 index 求差，产出 plan
                                └─→ PLANNED  （dry-run 在此停止）
                                      └─→ STOPPING   停止镜像服
                                            └─→ APPLYING   执行 plan
                                                  └─→ VERIFYING  抽样/全量校验
                                                        └─→ STARTING   启动镜像服
                                                              └─→ COMMITTING  提交索引
                                                                    └─→ IDLE
```

任一步失败 → `FAILED`，执行 `finally`（`save-on`、必要时启动镜像服、释放锁），并把错误写入同步历史。

`!!mu cancel` 置取消标志，在 `APPLYING` 的每个操作之间检查；取消后进入 `FAILED(cancelled)`，已完成的部分保持原样（下一轮自然收敛）。

**v0.1 实现范围**：`PREPARING → FLUSHING → SNAPSHOT → PLANNING → STOPPING → APPLYING → COMMITTING`。
- `STOPPING` 在 v0.1 只是 `manual` 适配器的一次**检查**（探测 `session.lock` 能否被独占），不会替你停服，也不存在 `STARTING`。同步结束后提示用户手动启动镜像。
- `VERIFYING`（抽样校验已完成写入）推迟到 v0.3 与 `inplace` 一起做；v0.1 的保证来自“写入后 fsync + 原子替换 + 索引后提交”这三件事，而不是事后校验。
- **启动校验只报警、不阻断**：`source.root` 不存在时插件照常加载（打 warning），真正的拒绝发生在同步时——`_prepare` 发现没有任何可用的 set 就中止整轮。这样改路径的中间状态下，插件不会因为加载失败而从 `!!mu` 命令树里消失。
- **flush 超时不阻塞整轮**：等不到 `Saved the game` 时（`source_flush.timeout_sec`，默认 60s）记一条 warning 继续走，下一轮 flush 成功后自然会再同步一遍。把整轮卡死在没有 flush 确认上，比晚一轮更糟。

### 9.2 规划（plan）

```python
def plan(index, snapshot, cfg):
    ops = []
    for path in snapshot.paths - index.paths:
        ops.append(WholeFile(path, reason="new"))
    for path in index.paths - snapshot.paths:
        ops.append(Delete(path))
    for path in snapshot.paths & index.paths:
        if path in index.pending:            ops.append(WholeFile(path)); continue
        if is_region(path) and cfg.region_delta:
            change = region_diff(path)        # 见 §6-L3
            if change.bitmap.any():
                ops.append(RegionPatch(path, change))
            continue
        if file_changed(path):                # §6-L2
            ops.append(WholeFile(path))
    return order(ops)   # 小文件优先，region 按路径序，删除最后
```

排序原则：

1. **新增 → 修改 → 删除**（先补齐再删，避免中途出现"引用缺失"）。
2. region 与 `.mcc`：`.mcc` 先于 region。
3. 单文件大小升序（大文件在后，便于早期失败时少浪费）。
4. 可选并发：`concurrency` 个 worker 处理互不相关的文件；**同一 region 的补丁与 `.mcc` 写入必须串行**。

### 9.3 执行（apply）单文件

```python
def apply_whole(src, dst, entry, idx):
    st = src.stat()                      # 读前
    tmp = dst.with_suffix(dst.suffix + f".{round_id}.tmp")
    h = xxh3_128()
    with src.open("rb") as fi, tmp.open("wb") as fo:
        for block in iter_read(fi, 1 << 20):
            h.update(block); fo.write(block)
        fo.flush(); os.fsync(fo.fileno())
    st2 = src.stat()                     # 读后
    if (st.size, st.mtime_ns) != (st2.size, st2.mtime_ns) or h.digest() != entry.hash:
        tmp.unlink(); idx.defer(dst, "unstable"); return   # 由调度器重试，最多 max_retry 次
    os.replace(tmp, dst)
    fsync_dir(dst.parent)
    idx.commit(dst, size=st.size, mtime_ns=st.mtime_ns, hash=h.digest())
```

读前后各取一次元数据 + 与 snapshot 中的哈希交叉验证，是"源端在写"场景下唯一可靠的护栏。

---

## 10. 一致性模型

### 10.1 源端：没有写冻结，用乐观校验代替

**前提**：`/save-off` 只关闭**周期性自动保存**，**chunk 卸载时仍然会写盘**。玩家跑图时 region 文件一直在被改写，"我读的那一刻源端是静止的"这个假设不成立。

因此每轮都可能读到**撕裂数据**，有两种形态：

1. **文件级**：拷贝进行到一半源文件被重写 → 目标拿到的版本在源端**从未整体存在过**。
2. **chunk 级**：先读 8KB 头、再按位置表去读 payload，两次读之间该 chunk 被就地重写 → 头指向的扇区已经换了内容，读出的 payload 与头不对应。

#### 候选策略

| 方案 | 做法 | 代价 | 结论 |
|---|---|---|---|
| **A 长窗口** | `save-off` 覆盖扫描 + 传输 + 应用，最后才 `save-on` | 源服可能数分钟不落盘，进程崩溃丢这段时间的内存数据 | **否决** |
| **B 乐观校验** | 照常读，读完用版本戳复验，不通过就重读或延后 | 变更文件多一次 `stat` / 多读一次头表项 | **默认** |
| **C 文件系统快照** | btrfs/ZFS/LVM 快照后读快照 | 依赖平台，Windows 上基本不可用 | 可选加速，v0.5 之后 |

**选 B 的理由**：A 的风险是"源服崩溃丢世界数据"，比"镜像晚一轮"严重得多，而用户已明确接受镜像落后；B 的失败模式是"本轮少同步几个文件"，且与 D4（索引只能落后）天然一致——**没通过复验的对象不提交索引，目标保留上一版，镜像永远停在某个完整快照上**。

#### 机制

**文件级（v0.1）**：边读边算哈希，落地成功后重新 `stat`，比对 `(size, mtime_ns)`；变化即重传，最多 `sync.max_retry`（默认 3）次；超限标记 `deferred`，本轮不提交索引，下一轮重试。

为什么 `stat` 够用：MC 写 region 走 `RandomAccessFile.write`，落盘必然更新 `mtime`。漏检只可能发生在"同一时间戳粒度内被改写且长度不变"，窗口极窄，且下一轮 `mtime` 已经不同，会自然补上。

**chunk 级（v0.2+）**：seqlock 风格的读—复验：

1. 读 index `i` 的 `timestamp` 表项 `t0` 与位置表项 `(off, len)0`；
2. 读 payload，同时计算 chunk 哈希；
3. 复读 `(off, len)1` 与 `t1`；
4. `t0 != t1` 或 `(off, len)0 != (off, len)1` → 判定不稳定，重读（计入 `max_retry`）。

**已知盲区**：region 头的 `timestamp` 只有**秒级**精度，同一秒内、等长、原地覆盖的 chunk 检测不到。接受这个盲区——窗口极窄，且下一秒 `t` 变化会触发重新同步；不为它引入额外的全文件校验。

#### 不稳定对象的处理

- 本轮**不写目标、不提交索引**，记入 `pending(reason=unstable)`。
- `!!mu status` 显示"本轮跳过 N 个不稳定对象，下轮重试"，避免被误读成同步失败。
- 同一对象**连续 3 轮**仍不稳定 → 标记 `stalled` 并在 `!!mu status` 置顶提示。不自动停止同步（通常只是玩家还在跑图，下一轮就收敛了），但要让人能看见"某些区块已经三轮没同步上去"。
- `sync.defer_unstable: false` 时改为整轮失败，仅用于排障（确认读到的数据到底可不可信）。

#### 已否决：长窗口模式

把 `save-on` 推迟到 apply 结束之后再发（等价于让 `save-off` 覆盖整轮），能让"读到的每一字节都来自同一个静止状态"，彻底消除撕裂。**不实现**，原因：

- 代价落在**源服**身上：暂停保存期间崩进程，丢的是主服内存里的世界数据，而收益只是"镜像少落后一轮"。
- 收益可以用更便宜的方式拿到：不稳定对象延后一轮（见上），下一轮 `mtime`/`timestamp` 必变，自然重新同步，收敛性不受影响。
- 它会诱使运维把同步窗口拿去覆盖"反正没人玩"的长时段，反而放大风险。

结论写在这里是为了避免以后重复讨论——**MirrorUP 的一致性策略只有一条：乐观校验 + 延后一轮**，没有"更严格但更危险"的开关。

#### 与参考实现的差别

MirrorMcsmcdR 在调用同步**之前**就执行了 `save-on`（`auto_save_on` 位于 `world_sync.sync()` 之前），且拷贝全程没有任何读后复验。它把一致性完全押在"镜像服已停机"上，源端撕裂的数据会原样落到镜像。MirrorUP 的差别是把校验放在**读侧**，用一轮内少量重读换掉"长时间暂停保存"的风险。

### 10.2 语义承诺

- 镜像反映的是**快照时刻（`snapshot_time`）的世界状态**，允许落后，不允许半写。
- 同步是**收敛的**：任何被延后（`pending`）的对象下一轮必然被重新检查，不会永久丢失。
- 同步是**幂等**的：同一份源数据重复应用的结果相同。

### 10.3 目标端

- 镜像服必须在 `APPLYING` 前完全退出（进程消失，不是"正在停止"）。
- 停止判定：`mirror_ctl.is_stopped()` 为真，且端口不再监听，且 `session.lock` 可独占打开。
- `session.lock` 永远不同步。
- 启动后可选做一次"存活检查"（等待日志出现 `Done (` 或端口监听成功），失败则标记 `FAILED(startup)` 并保留索引（数据是对的，只是服务没起来）。

### 10.4 玩家体验

停止镜像服前 10 秒广播（title + chat），内容含"即将重启同步"。配置可关。

---

## 11. 崩溃恢复（无备份前提下）

因为没有备份，恢复策略只有一条：**保证磁盘上不存在半写文件，然后重跑**。

| 崩溃点 | 磁盘状态 | 下一轮行为 |
|---|---|---|
| 写 tmp 中断 | 目标原文件完好，留下 `.tmp` 垃圾 | 启动时清理陈旧 `.tmp`（匹配 `<name>.<round>.tmp`），重新同步 |
| `os.replace` 中断 | 原子操作，要么旧要么新 | 索引落后 → 重做该文件（幂等） |
| region 头提交前 | 旧头有效，新数据是孤儿 | 重算 chunk 表，孤儿数据被 compaction 回收 |
| region 头提交时撕裂 | 头损坏 | 用 `journal/*.hdr` 恢复；无 journal 则整文件从源重传 |
| 索引提交前 | 索引落后 | 重做，幂等 |
| 索引提交后 | 一致 | — |

启动自检（插件 `on_load`）：

1. `PRAGMA integrity_check`。
2. 清理 `*.tmp` 与过期 journal。
3. 若 `meta.last_sync_begin > meta.last_sync_end`（上一轮未正常结束），在 `!!mu status` 标记 `interrupted`，下一次同步强制 `full_hash`（重算所有文件哈希，不信 mtime）。

---

## 12. 传输层

### 12.1 抽象接口

```python
class Source(Protocol):                # v0.4 实际接口（原计划的 Transport 收敛成它）
    is_remote: bool
    def scan(self, matcher) -> ScanResult: ...              # 全量路径集合 + (size, mtime_ns)
    def stat(self, rel: str) -> Optional[tuple[int, int]]: ...
    def copy_to(self, rel: str, dst: Path) -> tuple[bytes, int, int]: ...   # 流式落盘，边写边哈希
    def scan_region(self, rel: str, dirty_since, previous) -> RegionScan: ...
    def get_records(self, rel: str, indices) -> dict[int, tuple[int, bytes, bytes]]: ...
    def read_bytes(self, rel: str, limit: int = 8 << 20) -> Optional[bytes]: ...
    def close(self) -> None: ...
```

`local` 与 `agent` 两个实现，上层（planner / applier / engine）完全一致。

**为什么没有按 offset 读的 `read_at`？** 写入端需要的是"某个 chunk 的完整记录"，而不是任意字节区间。整文件 + 按 chunk 索引取记录，比在 HTTP 上做 Range 请求少一层边界处理，也天然复用同一套 `read_record` 校验。代价是新增文件必须整文件传——这本来就是必需的量。

### 12.2 `local`

直接 `os.scandir` / 随机读。路径拼接前后都做规范化（`PurePosixPath`，拒绝 `..`、绝对路径、盘符），防止配错路径导致越界写入。

**跨平台注意**：`os.pread` 仅 Unix 可用，Windows 上需封装为 `open(file, 'rb', buffering=0)` + `seek/read`（或 `ctypes` 调 `ReadFile` 带 `OVERLAPPED`）。同理，`fsync(目录)` 在 Windows 上需要用 `FILE_FLAG_BACKUP_SEMANTICS` 打开目录句柄再 `FlushFileBuffers`，封装在 `platform_io` 模块里，上层不感知。

注意：**SMB / 网络共享也走 `local`**，但其 `mtime` 精度可能只有 2 秒，需在配置里显式声明 `mtime_precision = 2`，L2 判定相应放宽为"同精度区间内视为未变"。

### 12.3 `agent`

源端 MirrorUP 以 `role = exporter` 启动，只读，无任何写操作。

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/v1/info` | 协议版本、版本号、set 列表与各自 mc 版本 |
| GET | `/v1/manifest?set=<name>` | 二进制 manifest，含全量路径集合 |
| GET | `/v1/file?set=&path=` | 整文件流式下载（新增/修改文件） |
| GET | `/v1/stat?set=&path=` | JSON `{size, mtime_ns, is_dir}`，用于读后校验 |
| GET | `/v1/region?set=&path=&since=<unix秒>` | region 摘要：8KB 头 + 脏 chunk 的 `(index, timestamp, digest)` |
| GET / POST | `/v1/region/chunks?set=&path=&idx=a,b,c` | 指定 chunk 的记录（`length + 5B 头 + payload`） |

**为什么 manifest 要带全量路径集合？** 因为删除检测需要它。只发增量的话，客户端无法发现"源端删了文件"。路径名本身很便宜：20 万条路径经 varint 前缀压缩后约 1–2MB，一次 LAN 传输可忽略。

二进制 manifest 格式（大端，v0.4 实现）：

```
"MUPM" | u8 version | u32 count
repeat count times, 按路径字典序：
  varint shared_prefix_len
  varint suffix_len
  suffix bytes
  u64 size
  i64 mtime_ns
  u8  kind         # 0 = 文件, 1 = 目录
```

设计稿里的 gzip 外层与 hash 字段没有实现：manifest 在 LAN/本机场景下已经够小，hash 属于索引层、不进 manifest（驱动端本来就要按需重算）。差异记在 §12.5。

### 12.4 认证与安全

- `Authorization: Bearer <token>`，比较用 `hmac.compare_digest`；token 少于 16 字符直接拒绝启动 agent（少于 32 字符告警）。
- 默认监听 `127.0.0.1:25580`；跨机需显式改 `host`。
- 可选 TLS（自签证书 + 指纹固定 `tls_pin_sha256`，比对的是证书 DER 的 SHA-256）。
- 服务端校验：set 必须已配置、路径规范化后必须落在 set root 内、拒绝符号链接逃逸。
- 建议部署在 WireGuard / 内网，TLS 作为二层防护。
- **源端 agent 不提供任何写接口**，即使 token 泄露也无法修改主服。

### 12.5 v0.4 实现记录（与上文规范的差异）

1. **`Transport` 收敛为 `Source`**：没有 `read_at` / `region_meta` / `region_chunks(bitmap)`，改成 `scan / copy_to / scan_region / get_records`。理由见 §12.1。
2. **manifest 不压缩、不带 hash**；`flags` 简化成 `kind`（只有文件/目录两种）。
3. **chunk 选择用 `idx` 列表而不是 128B 位图**：驱动端每轮最多要几十到几百个 chunk，列表比位图省事；单次上限 1024。
4. **`/v1/region` 带 `since`**：服务端只对 `timestamp >= since` 的 chunk 算哈希，其余返回 `has_digest=0`，由客户端用上一轮索引补齐——这是跨机场景下"不读整个世界"的关键。
5. **`.mcc` 外置 chunk**：region 里只有 5 字节占位记录。逻辑哈希定义为 `H(占位记录 || H(mcc 内容))`，`H(mcc)` 由 `scan_region` 顺带算出并随摘要下发；驱动端把它当普通 chunk 搬 5 字节占位，真正内容由普通文件同步搬运（镜像停机期间，先后顺序无影响）。
6. **TLS**：`agent_server.tls.enabled` 时用 `ssl.SSLContext(PROTOCOL_TLS_SERVER)` 包一层；驱动端 `tls_pin_sha256` 对 `getpeercert(True)` 的 DER 做 SHA-256 后比对，证书链本身不校验（自签场景）。
7. **鉴权**：token ≥16 才允许启动 agent；设计稿写 ≥32，实现放宽并给出告警，方便本地联调。
8. **客户端连接**：每条请求新建 `http.client` 连接（`Connection: close`）。同步是批处理，连接复用带来的收益远小于状态管理成本；将来做长连接也不影响协议。

---

## 13. 镜像服启停编排

```python
class MirrorControl(Protocol):         # v0.4 实际接口
    def check(self) -> tuple[bool, str]: ...        # (已停止?, 说明)
    def can_manage(self) -> bool: ...               # 能不能自己停/启
    def stop(self, timeout: float, notify_sec: int = 0) -> tuple[bool, str]: ...
    def start(self) -> tuple[bool, str]: ...
    def wait_ready(self, timeout: float) -> bool: ...
```

| 适配器 | 说明 | 版本 |
|---|---|---|
| `manual` | 只检查，不操作；未停止则中止同步并提示 | v0.1（v0.4 仍是默认） |
| `rcon` | RCON `stop`（可选先 `say` 倒计时）+ TCP 端口探活 + 可选 `start_cmd` 启动 | v0.4 |
| `mcsm` | 调用 MCSManager API：v9 用 `apikey` 查询参数，v10 用 `/api/auth/login` 换 Bearer token；`/api/protected_instance/status|stop|start` | v0.5 |
| `external` | 调用用户提供的命令钩子（`stop_cmd` / `start_cmd` / `probe_cmd`） | v0.4 |

`external` 是万能兜底：用户写一个 shell 脚本就能接任何面板。

**实现细节（v0.4）**：

- 判断"正在运行"有三条独立证据：`session.lock` 被占用（`msvcrt.locking` / `fcntl.lockf` 试锁）、RCON 端口可连、`probe_cmd` 退出码为 0。任一为真即视为运行中——宁可多等一次，也不要在镜像还活着时改它的存档。
- `rcon` 的启动命令复用 `mirror_control.external.start_cmd`（RCON 只能停不能启）。
- `stop` 之后轮询到进程真的退出才返回；超时返回失败，同步中止（不会在镜像还在跑的时候写文件）。
- `start_after_sync=true` 时，同步完自动 `start()` 并 `wait_ready()`；只是"已执行"还是"已确认就绪"会写进 `report.hint`，不会假装成功。

---

## 14. 配置

路径：`config/mirror_up/config.json`。首次启动生成默认值；版本升级自动补默认字段（带 `schema_version` 迁移）。

```jsonc
{
  "schema_version": 5,
  "role": "driver",                      // driver | exporter

  "source": {
    "root": "./server",                  // 相对 MCDR 根目录
    "transport": "local",                // local | agent
    "agent": {
      "url": "http://127.0.0.1:25580",
      "token": "",
      "tls_pin_sha256": ""
    },
    "mtime_precision_sec": 0             // 网络共享设 2
  },

  "target": {
    "root": "./Mirror/server"            // 单目标简写，等价于 targets[0]
  },
  "targets": [                           // v0.5：可写多个；每个目标可覆盖 mirror_control
    { "name": "default", "root": "./Mirror/server", "enabled": true }
    // { "name": "mirror2", "root": "./Mirror2/server", "enabled": true,
    //   "mirror_control": { "adapter": "rcon", "rcon": { ... } } }
  ],

  // config/ 与 server.properties 不在默认集合内（见 D8）；确需同步时由用户自行添加 set。
  "sets": [
    { "name": "world",  "source": "world",  "target": "world",  "enabled": true,  "mode": "strict" },
    { "name": "mods",   "source": "mods",   "target": "mods",   "enabled": true,  "mode": "strict" }
  ],

  "exclude": [
    "**/session.lock",
    "**/*.lock",
    "**/*.tmp",
    "logs/**",
    "crash-reports/**",
    "**/usercache.json"
  ],

  "sync": {
    "hash": "xxh3-128",
    "concurrency": 4,
    "trust_mtime": true,                 // false = 每轮全量哈希（极慢，仅排障）
    "verify_target": true,               // 每轮 stat 扫描镜像，发现被改的文件就重推（D9）
    "region_delta": true,                // .mca 走 chunk 级增量（v0.2 已实现）；false 退回整文件
    "region_write_mode": "inplace",      // rebuild | inplace（inplace 从 v0.3 起是默认）
    "region_compaction_ratio": 0.5,      // 碎片扇区 > 存活扇区 * 0.5 时整文件重建
    "journal": true,                     // inplace 的头快照；false 会把写入模式降级为 rebuild
    "clean_mcc_orphans": true,           // 删除不再被任何 region 引用的 .mcc（v0.5）
    "max_retry": 3,
    "defer_unstable": true               // 读不稳定的文件延后到下一轮，而不是整轮失败
  },

  "source_flush": {
    "enabled": true,
    "turn_off_auto_save": true,
    "commands": {
      "auto_save_off": "save-off",
      "save_all": "save-all flush",
      "auto_save_on": "save-on"
    },
    "saved_regex": "^Saved the game$",
    "timeout_sec": 60
  },

  "mirror_control": {
    "adapter": "manual",
    "rcon": { "host": "127.0.0.1", "port": 25575, "password": "" },
    "external": { "stop_cmd": "", "start_cmd": "", "probe_cmd": "" },
    "mcsm": {                              // v0.5：MCSM 9 用 apikey，MCSM 10 用 username/password 登录
      "base_url": "http://127.0.0.1:23333",
      "apikey": "",
      "uuid": "",
      "remote_uuid": "",
      "username": "",
      "password": "",
      "timeout_sec": 10,
      "insecure_tls": false
    },
    "stop_timeout_sec": 120,
    "start_after_sync": true,
    "wait_ready_timeout_sec": 180,
    "notify_players_sec": 10
  },

  "schedule": {
    "auto_sync": false,
    "interval_seconds": 3600,
    "max_interval_seconds": 604800,       // 7 天，硬上限
    "skip_if_source_empty": false
  },

  "agent_server": {
    "host": "127.0.0.1",
    "port": 25580,
    "token": "",
    "tls": { "enabled": false, "cert": "", "key": "" }
  },

  "log": {
    "history_keep": 20,
    "verbose_plan": false
  }
}
```

校验规则：

- `schedule.interval_seconds` 超限直接 clamp 到 `max_interval_seconds` 并在控制台告警。
- `source.root` 与每个 `targets[].root` 解析为绝对路径后**不允许互相包含**（防止把镜像目录同步进自己）。
- 多目标：`targets[].name` 必须唯一、目录不能重复（`normcase` 后比较）、每个目标上的 set 目标路径不能重复。
- `sets[].target` 重名 → 拒绝启动。
- `region_write_mode=inplace` 从 v0.3 起可用且是默认；`journal=false` 时自动降级为 `rebuild` 并告警（没有头快照的 inplace 不提供崩溃保护）；`region_compaction_ratio < 0` 视为 0（只受"不缩小文件"限制，有碎片就整理）。`region_delta` 从 v0.2 起真正生效（v0.1 打开只会告警并忽略）。
- 跨机时 `agent.url` 指向自身地址 → 拒绝（防止自我同步）。
- `source.transport` 只能是 `local` / `agent`，其他值直接 `ConfigError`；`agent` 模式下 `agent.url` 必须是 http(s)，不再检查本地 `source.root` 是否存在（那是 exporter 的事）。
- `role=exporter` 时 `agent_server.token` 少于 16 字符 → 拒绝启动 agent；少于 32 字符 → 告警。
- `mirror_control.adapter=rcon` 但 `rcon.password` 为空 → 告警（RCON 通常会拒绝登录）；`adapter=external` 缺 `stop_cmd` 或 `start_cmd` → 告警；`adapter=mcsm` 要求 `base_url` 为 http(s)、`uuid`/`remote_uuid` 必填、`apikey` 或 `username`+`password` 至少一组，否则告警。以上检查对全局配置与每个目标的覆盖段各跑一遍。

---

## 15. 命令

前缀 `!!mu`（可配置），权限沿用 MCDR 权限等级。

| 命令 | 权限 | 说明 |
|---|---|---|
| `!!mu sync` | 2 | 执行一次同步 |
| `!!mu sync --dry-run` | 2 | 只规划不执行，输出 plan 摘要与明细（可折叠） |
| `!!mu sync --full` | 2 | 忽略索引，全量重哈希 + 全量比对（会重读整个世界，建议在维护窗口用） |
| `!!mu diff` | 2 | 同 `sync --dry-run`，但只显示摘要 |
| `!!mu status` | 0 | 索引状态、上次同步、下次定时、pending 数量 |
| `!!mu verify [--deep]` | 2 | 核对源/镜像/索引三方一致性；`--deep` 逐个比哈希（v0.5） |
| `!!mu history [N]` | 0 | 最近 N 轮结果（默认 10，上限 50）（v0.5） |
| `!!mu cancel` | 2 | 取消进行中的同步 |
| `!!mu schedule on\|off` | 3 | 开关自动同步 |
| `!!mu schedule set <1m\|2h\|3d>` | 3 | 设置间隔（下限 60 秒，上限 7 天） |
| `!!mu index rebuild` | 3 | 放弃索引，下一轮全量 |
| `!!mu reload` | 3 | 重载配置（不中断进行中的同步） |

`!!mu diff` 输出示例：

```
[MirrorUP] 计划摘要（set: world, mods）
  新增 12 文件 / 8.4 MB
  修改  3 文件 / 128 KB（其中 2 个 region，将补丁 47 个 chunk）
  删除  1 文件 / session.dat
  延后  0（pending 共 2）
  预计写入 9.1 MB / 读取 42.6 MB
  预计停机 ≈ 1.2s + 镜像启动时间
```

---

## 16. 可观测性

- **同步历史**：保留最近 N 轮（默认 20），记录开始/结束时间、各阶段耗时、读写字节数、文件/chunk 计数、错误。
- **每轮结束输出**：

```
[MirrorUP] 同步完成：world+mods
  扫描 152,339 项 / 0.8s
  变更 41 文件（37 修改 / 4 新增 / 0 删除）
  传输 9.2 MB，写入 9.4 MB（其中 chunk 级节省 312 MB）
  应用 1.3s，停机 1.9s，总计 48.2s（含镜像启动 44s）
  延后 2（unstable）
```

- **计数器**（供 `!!mu status` 与压测使用）：`files_scanned`、`files_read`、`bytes_read`、`bytes_written`、`regions_patched`、`chunks_copied`、`chunks_reused`、`deferred`。
- 日志分级：`INFO` 每轮一行摘要；`DEBUG` 每次文件操作；`TRACE` chunk 级。
- 所有日志带 `round_id`，便于把一次同步的全部输出串起来。

---

## 17. 性能预算

两档目标机器（均为 SATA SSD，单线程）：

**Profile A — 小型服：200 MB / 200 region / 5,000 文件**

| 场景 | 全量插件 | MirrorUP（inplace） |
|---|---|---|
| 首次同步 | ~2 s | ~2.5 s（多算一层哈希） |
| 稳态一轮（3 region 脏 / 20 chunk） | ~2 s | **< 0.1 s**（读 ~0.7 MB，写 ~0.7 MB） |

**Profile B — 大型服：12 GB / 3,000 region / 150,000 文件**

| 场景 | 全量插件 | 每轮重算哈希 | MirrorUP（inplace） |
|---|---|---|---|
| 首次同步 | 8–15 min | 12–20 min | 10–18 min |
| 稳态一轮（37 region 脏 / 300 chunk） | 8–15 min | 40–90 s | **2–5 s** |

预算约束：

- 扫描阶段不得读取文件内容（只 `stat`）。
- 稳态一轮读放大应 < 变更量的 5 倍。
- 单文件哈希用 1 MB 块读，避免小 IO 抖动。
- `concurrency` 默认 4；对 HDD 建议 1（并行反而降低顺序读效率）。

---

## 18. 兼容性与安全

### 18.1 兼容矩阵

| 维度 | 支持范围 | 说明 |
|---|---|---|
| MCDR | ≥ 2.10.0 | 需要 `server.start()/stop()`、`GreedyText` 命令 |
| Python | ≥ 3.9 | `sqlite3`、`os.scandir` |
| 服务端 | Vanilla / Paper / Purpur / Fabric / Forge / NeoForge | 只要 region 容器格式一致 |
| MC 版本 | 1.13 – 最新 | 容器头自 Beta 1.3 未变；1.20.5+ 的 `.mcc` 单独处理 |
| 操作系统 | Windows / Linux | Windows 长路径（`\\?\`）、无 reflink |
| 依赖 | `xxhash`（唯一第三方包） | 其余全部标准库 |

**前置条件（必须写进用户文档）**：源服与镜像服必须**同 MC 版本、同模组集合**。版本不一致时镜像会加载失败或损坏区块——我们不会检测到，因为我们不解析 NBT。

### 18.2 安全边界

- 源端**只读**：代码路径中不存在对 `source.root` 的写/删/改属性操作（`os.replace` 只用于 target）。
- 路径穿越防护：所有来自配置/协议的路径经 `normpath` + `commonpath` 校验，拒绝绝对路径、`..`、盘符、符号链接逃逸。
- 镜像侧覆盖写入是不可逆的，因此 `!!mu diff` 与 `--dry-run` 是默认推荐流程，`sync` 在权限 2 以上并要求二次确认（可关）。
- 不执行任何来自网络的命令；协议无 `eval`、无 shell 拼接。

---

## 19. 测试计划

| 层级 | 内容 | 判定 |
|---|---|---|
| 单元 | region 头解析：正常 / 越界 / 扇区为 0 / 长度溢出 / 压缩类型未知 | 全部返回 `corrupt` 而非异常 |
| 单元 | `chunk_digest` 对填充字节不敏感；对 `.mcc` 内容敏感 | 断言 |
| 单元 | planner 纯函数：新增/修改/删除/延后/region 位图 | 快照测试 |
| 属性测试 | 对随机生成的 region 执行 `patch` → 重新解析 → chunk 哈希集合与源一致 | Hypothesis，≥ 1000 例 |
| Fuzz | 随机字节流喂给解析器 | 不崩溃、不越界 |
| 差分 | 非 region 文件同步结果与 `robocopy /MIR` / `rsync -a --delete` 全等 | 文件树哈希一致 |
| 集成 | 真实 Paper 服务端 + 生成的 5 GB 世界，跑 10 轮混合变更 | 每轮后 `verify --deep` 通过 |
| 故障注入 | 在 apply 的各阶段 `kill -9`，重启后重跑 | 10 次内必定收敛，无损坏文件 |
| 性能 | Profile A/B 两档，记录计数器 | 满足 §17 预算 |

`verify --deep` 的实现：重新计算源端与目标端全部 chunk 哈希并逐一比对（不比对文件哈希，因为目标布局独立于源，见 §8.4）。

**v0.4 / v0.5 实际落地的测试**（`tests/`，不进仓库，随源码分发时可另行提供）：

| 脚本 | 覆盖 |
|---|---|
| `smoke_engine.py` | 10 个场景：dry-run、首轮、零写入、单文件变更、删除、空源保护与 `--force`、mtime 回退、还原嫌疑全量重算、镜像篡改重推、镜像变大后零写入 |
| `smoke_region.py` | region 首次构建、未变零读、只改一个 chunk、增删 chunk、坏文件/外置 chunk、rebuild 与 inplace 两种引擎路径、journal 恢复、compaction |
| `smoke_commands.py` | 命令树注册、参数解析、渲染、`entry` 导入与包级导出 |
| `smoke_v04.py` | wire 编解码、`AgentSource` 与 `LocalSource` 等价、跨机只写脏 chunk、`.mcc` 外置 chunk、external / rcon 适配器、配置 v4 校验 |
| `smoke_v05.py` | 双目标各自独立计划与恢复、同步历史、浅/深校验（含"同大小同 mtime 改内容"）、`mirror_up.api`、exporter 拒绝同步、set 改名复用索引、孤儿 `.mcc` 清理、MCSM 适配器（假面板双鉴权）、随机化往返（manifest 300 / region 头 200 组 / chunk state 30 组）、fuzz（随机字节喂 `parse_header` / `scan_region`，只允许已知异常）、与 `robocopy /MIR` 的差分（文件集合 + 逐字节内容） |

属性测试没有接 Hypothesis，用固定种子的随机往返代替；差分测试依赖 Windows 的 `robocopy`，没有该命令时自动跳过。

---

## 20. 路线图

| 版本 | 范围 | 验收标准 |
|---|---|---|
| **v0.1** ✅ | 索引 + 文件级增量 + 读后复验（§10.1）+ 原子替换 + plan/dry-run + 排除清单 + 断点续传 + `manual` 适配器 | Profile B 稳态一轮 < 10 s；崩溃注入不产生损坏文件 |
| **v0.2** ✅ | region chunk 级增量（`rebuild`）+ chunk 读—复验 + 定时 | Profile B 稳态一轮 < 5 s；源端读取量下降一个数量级（目标端写入量不变，见 §8.6 第 2 条） |
| **v0.3** ✅ | `inplace` append-and-commit + compaction + 头 journal（§8.7） | 写量降到"脏 chunk + 16KB"，崩溃注入后靠 journal 恢复旧头 |
| **v0.4** ✅ | 跨机 `agent` 传输 + TLS + `rcon`/`external` 停服 + `.mcc` 外置 chunk 增量 + 配置 schema v4 | 同机/跨机结果一致；`.mcc` 改内容能触发同步；停止不了镜像时拒绝写盘 |
| **v0.5** ✅ | `verify`（浅/深）、同步历史、多镜像目标共享索引（schema v5）、`mcsm` 适配器、`.mcc` 孤儿清理、set 改名复用索引、Python API（`mirror_up.api`） | 多目标各写各的、互不串写；深校验能发现"同大小同 mtime 的源内容变化"；第三方可 `get_api()` 调用 |
| 可选 | btrfs/zfs snapshot 加速后端、空闲触发、WebUI | — |

---

## 21. 风险登记

| ID | 风险 | 影响 | 缓解 |
|---|---|---|---|
| R1 | 自研 region 写入逻辑有 bug，写坏镜像存档 | 高（镜像可重建，源服不受影响） | 只写 target；`rebuild` 模式原子替换；`inplace` 用 append-and-commit + 头 journal；属性测试 + fuzz；`verify --deep` |
| R2 | `mtime` 不可靠导致漏同步 | 中 | L2 的"mtime 早于同步时间"规则；`--full` 强制重哈希；`verify` |
| R3 | 源端同步期间持续写入导致读到撕裂数据 | 中 | 乐观校验（§10.1）：读前后元数据比对 + 哈希交叉验证；不稳定对象延后到下一轮（长窗口模式已否决，不提供开关） |
| R4 | 跨机带宽不足，manifest 过大 | 中 | 二进制压缩 manifest；可选只发 since 之后路径 + 周期性全量核对 |
| R5 | 用户通过自定义 set 把 `server.properties` / `config` 纳入同步导致镜像起不来 | 中 | 默认集合不含它们（D8）；`diff` 高亮"将覆盖配置文件"；文档警示 |
| R6 | 源/镜像版本或模组不一致，区块无法加载 | 高（用户侧） | `info` 交换 `mc_version` 与 `mods_digest`，不一致时**默认拒绝同步**，可强制覆盖 |
| R7 | Windows 长路径 / 文件占用 | 低 | `\\?\` 前缀；停机后操作；`.tmp` 清理 |
| R8 | 索引损坏 | 低 | `integrity_check` 失败即自动重建（退化为全量） |
| R9 | 7 天间隔导致单轮变更接近全量 | 低 | 属预期；`diff` 提前告知；建议 > 24h 时提示 |

---

## 22. 未决问题

1. **`level.dat` 字段级同步**：整文件覆盖会把 `LevelName`/`LastPlayed` 也带过去，是否需要字段白名单？
2. **按需同步**：镜像无人时不启动，玩家进入前才同步——这会把 5 秒的增量变成"首次全量"，是否需要预热缓存？

已决：`config/` 是否同步 → **不同步**（D8，2026-11-08 确认）。多镜像目标 → **共享一个索引、按 `target_id` 分区**（每目标一份记录，schema v5，v0.5）；`.mcc` 孤儿清理 → **做了**，按 region 覆盖的 32×32 坐标范围删未被引用的 `.mcc`，开关 `sync.clean_mcc_orphans`（v0.5）；set 改名 → **复用索引**：`(source_rel, target_rel)` 相同而名字变了时 `UPDATE` 名字、保留 id 与全部行（v0.5）。

---

## 附录 A — 一次稳态同步的 IO 明细（Profile B 示例）

```
扫描        150,000 次 stat                    ≈ 0.8 s     0 B 内容读取
L2 命中     150,000 - 41 个跳过                 0 B
脏文件      41 个（37 改 / 4 新）
  ├ 4 个新增（mods jar 等）    读 9.0 MB  写 9.0 MB
  ├ 37 个 region（全部 chunk 级补丁）
  │   ├ 37 个 region 头        读 296 KB
  │   ├ 300 个 chunk 记录      读 9.2 MB
  │   └ inplace 写入           写 9.4 MB + 头 148 KB
  └ 删除 0
索引提交    41 行 + 内存位图    ≈ 20 ms
──────────────────────────────────────────────
合计        读 ≈ 18.5 MB   写 ≈ 9.5 MB
对照全量    读 12 GB      写 12 GB
```

## 附录 B — 相关规范与参考

- Region 容器格式：<https://minecraft.wiki/w/Region_file_format>（扇区、头、载荷、外置 chunk）
- MirrorServerReforged：<https://mcdreforged.com/zh-CN/plugin/mirror_server_reforged>
- MirrorMcsmcdR：<https://mcdreforged.com/zh-CN/plugin/mirror_mcsmcdr/readme>
- MCDReforged 插件文档：<https://docs.mcdreforged.com/zh-cn/latest/>
