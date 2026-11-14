"""全局常量。"""

from __future__ import annotations

import re

APP_ID = "mirror_up"
APP_NAME = "MirrorUP"
VERSION = "0.6.5"

#: 索引与配置的 schema 版本，迁移时使用。
#: v2：sync.region_delta 默认改为 true（v0.2 chunk 级增量真正生效）。
#: v3：sync.region_write_mode 默认 inplace（v0.3 追加式写入）。
#: v4：新增 agent_server 段与 source.transport=agent（v0.4 跨机传输）。
#: v5：多镜像目标（target_id）。
#: v6：regions 表新增 floor 列（region 脏扫描下限，v0.6.5）。
SCHEMA_VERSION = 6

#: 索引文件名（位于 MCDR 插件数据目录）。
INDEX_FILENAME = "index.sqlite3"
CONFIG_FILENAME = "config.json"

#: 索引批量提交阈值：条数或秒数先到者。
INDEX_BATCH_ROWS = 200
INDEX_BATCH_SECONDS = 1.0

#: 顺序读写的块大小。
IO_CHUNK = 1 << 20

#: region 文件命名：r.<x>.<z>.mca / r.<x>.<z>.mcr
REGION_NAME_RE = re.compile(r"^r\.(-?\d+)\.(-?\d+)\.(mca|mcr)$")

#: 读取重试的最大次数下限（配置里可以调大，但不能调到 0）。
MIN_RETRY = 1

#: pending 的原因取值。
PENDING_UNSTABLE = "unstable"
PENDING_BUSY = "busy"
PENDING_VANISHED = "vanished"
