"""哈希工具。

算法固定为 xxh3-128（16 字节）。源端与目标端的哈希必须由同一实现产生，
因此这里不接受“算法可配置”——跨机器场景下两边装的是同一个插件版本。
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import BinaryIO, Optional

import xxhash

from .constants import IO_CHUNK, REGION_NAME_RE

HASH_SIZE = 16


def new_hasher() -> "xxhash.xxh3_128":
    return xxhash.xxh3_128()


def hash_stream(stream: BinaryIO) -> bytes:
    """把流读完并返回 16 字节摘要。"""
    hasher = new_hasher()
    while True:
        block = stream.read(IO_CHUNK)
        if not block:
            break
        hasher.update(block)
    return hasher.digest()


def hash_file(path: os.PathLike[str] | str) -> bytes:
    with open(path, "rb") as fp:
        return hash_stream(fp)


def hash_bytes(data: bytes) -> bytes:
    return xxhash.xxh3_128(data).digest()


def is_region_name(name: str) -> bool:
    """``r.0.0.mca`` / ``r.-1.2.mcr`` 判定（只看文件名）。"""
    return REGION_NAME_RE.match(name) is not None


def region_coords(name: str) -> Optional[tuple[int, int]]:
    m = REGION_NAME_RE.match(name)
    if m is None:
        return None
    return int(m.group(1)), int(m.group(2))


def hexdigest(digest: Optional[bytes]) -> str:
    return "-" if digest is None else digest.hex()


def stat_fingerprint(path: Path) -> tuple[int, int]:
    """返回 (size, mtime_ns)，用于稳定性判断。"""
    st = os.stat(path)
    return st.st_size, st.st_mtime_ns
