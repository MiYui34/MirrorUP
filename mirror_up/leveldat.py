"""最小 NBT 读取器。

只做一件事：从 ``level.dat`` 里找出 ``Data.LevelName`` / ``Data.Version.Name`` /
``Data.Seed``，用于

1. 计算 ``world_id``（防止把 A 世界的索引套到 B 世界）；
2. 同步前比对源与镜像的 MC 版本（版本不一致写入区块会导致镜像坏档）。

刻意不实现 NBT 写入：本项目对 region/NBT 一律“当不透明字节搬运”，从不改写结构。
"""

from __future__ import annotations

import gzip
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Dict, Optional

TAG_END = 0
TAG_BYTE = 1
TAG_SHORT = 2
TAG_INT = 3
TAG_LONG = 4
TAG_FLOAT = 5
TAG_DOUBLE = 6
TAG_BYTE_ARRAY = 7
TAG_STRING = 8
TAG_LIST = 9
TAG_COMPOUND = 10
TAG_INT_ARRAY = 11
TAG_LONG_ARRAY = 12

_GZIP_MAGIC = b"\x1f\x8b"


class NbtError(Exception):
    pass


class _Reader:
    def __init__(self, fp: BinaryIO):
        self.fp = fp

    def read(self, n: int) -> bytes:
        data = self.fp.read(n)
        if len(data) != n:
            raise NbtError("NBT 数据提前结束")
        return data

    def skip(self, n: int) -> None:
        self.fp.seek(n, 1)

    def u8(self) -> int:
        return self.read(1)[0]

    def i8(self) -> int:
        return struct.unpack(">b", self.read(1))[0]

    def i16(self) -> int:
        return struct.unpack(">h", self.read(2))[0]

    def u16(self) -> int:
        return struct.unpack(">H", self.read(2))[0]

    def i32(self) -> int:
        return struct.unpack(">i", self.read(4))[0]

    def i64(self) -> int:
        return struct.unpack(">q", self.read(8))[0]

    def f32(self) -> float:
        return struct.unpack(">f", self.read(4))[0]

    def f64(self) -> float:
        return struct.unpack(">d", self.read(8))[0]

    def string(self) -> str:
        length = self.u16()
        return self.read(length).decode("utf-8", errors="replace")


def _read_payload(reader: _Reader, tag_type: int) -> Any:
    if tag_type == TAG_BYTE:
        return reader.i8()
    if tag_type == TAG_SHORT:
        return reader.i16()
    if tag_type == TAG_INT:
        return reader.i32()
    if tag_type == TAG_LONG:
        return reader.i64()
    if tag_type == TAG_FLOAT:
        return reader.f32()
    if tag_type == TAG_DOUBLE:
        return reader.f64()
    if tag_type == TAG_BYTE_ARRAY:
        length = reader.i32()
        if length < 0:
            raise NbtError("非法的数组长度")
        reader.skip(length)
        return None
    if tag_type == TAG_STRING:
        return reader.string()
    if tag_type == TAG_LIST:
        elem_type = reader.u8()
        length = reader.i32()
        if length <= 0:
            return []
        if elem_type == TAG_END:
            return []
        return [_read_payload(reader, elem_type) for _ in range(length)]
    if tag_type == TAG_COMPOUND:
        result: Dict[str, Any] = {}
        while True:
            child_type = reader.u8()
            if child_type == TAG_END:
                return result
            name = reader.string()
            result[name] = _read_payload(reader, child_type)
    if tag_type == TAG_INT_ARRAY:
        length = reader.i32()
        if length < 0:
            raise NbtError("非法的数组长度")
        reader.skip(length * 4)
        return None
    if tag_type == TAG_LONG_ARRAY:
        length = reader.i32()
        if length < 0:
            raise NbtError("非法的数组长度")
        reader.skip(length * 8)
        return None
    raise NbtError(f"未知的 NBT tag 类型：{tag_type}")


def parse_nbt_file(path: Path) -> Dict[str, Any]:
    """解析根 compound；兼容 gzip 与未压缩两种 level.dat。"""
    with open(path, "rb") as raw:
        magic = raw.read(2)
        raw.seek(0)
        stream: BinaryIO
        if magic == _GZIP_MAGIC:
            stream = gzip.GzipFile(fileobj=raw, mode="rb")
        else:
            stream = raw
        try:
            reader = _Reader(stream)
            root_type = reader.u8()
            if root_type != TAG_COMPOUND:
                raise NbtError(f"根标签不是 compound（{root_type}）")
            reader.string()  # 根名字，通常为空
            return _read_payload(reader, TAG_COMPOUND)
        except (OSError, EOFError, struct.error) as exc:
            raise NbtError(f"读取 NBT 失败：{exc}") from exc


@dataclass
class LevelInfo:
    level_name: Optional[str]
    version_name: Optional[str]
    seed: Optional[int]


def read_level_info(level_dat: Path) -> Optional[LevelInfo]:
    """读不到或结构不认识时返回 None —— 调用方按“未知”处理，不要猜。"""
    try:
        root = parse_nbt_file(level_dat)
    except (OSError, NbtError):
        return None
    data = root.get("Data")
    if not isinstance(data, dict):
        return None

    level_name = data.get("LevelName")
    if not isinstance(level_name, str):
        level_name = None

    version_name: Optional[str] = None
    version = data.get("Version")
    if isinstance(version, dict):
        name = version.get("Name")
        if isinstance(name, str):
            version_name = name
    elif isinstance(version, int):
        # 1.8 及更早只有数字版本（如 19133 表示区域格式版本），不当作 MC 版本用
        version_name = None

    seed = data.get("Seed")
    if not isinstance(seed, int):
        seed = None
    return LevelInfo(level_name=level_name, version_name=version_name, seed=seed)
