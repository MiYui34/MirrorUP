"""MirrorUP agent 协议（driver ↔ exporter）的二进制编码。

三个消息：

``MUPM`` manifest
    ``"MUPM" | u8 version | u32 count``，随后按路径字典序的条目：

    ``varint prefix_len | varint suffix_len | suffix | u64 size | i64 mtime_ns | u8 kind``

    ``prefix_len`` 是"与前一条路径的公共前缀长度"，用来把 20 万条路径压到 1~2MB。

``MUR1`` region 摘要
    ``"MUR1" | u64 size | i64 mtime_ns | u32 header_len | header |
    u32 count | (u16 index | u32 timestamp | u8 has_digest | [16B digest])*``

    服务端只对"脏" chunk 计算哈希；非脏 chunk 的哈希由客户端用上一轮索引补齐。

``MURC`` chunk 记录
    ``"MURC" | u32 count | (u16 index | u32 length | 5B head | (length-1)B body)*``

    记录原样搬运，不解压。

所有整数大端。协议只在自家客户端/服务端之间使用，不做向后兼容承诺；
格式变了就换魔数。
"""

from __future__ import annotations

import struct
from typing import Dict, Iterable, List, Mapping, Optional, Tuple, Union

from .scanner import ScannedEntry

MANIFEST_MAGIC = b"MUPM"
MANIFEST_VERSION = 1
REGION_MAGIC = b"MUR1"
CHUNKS_MAGIC = b"MURC"


def _put_varint(out: bytearray, value: int) -> None:
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return


def _get_varint(data: bytes, pos: int) -> Tuple[int, int]:
    value = 0
    shift = 0
    while True:
        if pos >= len(data):
            raise ValueError("varint 被截断")
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, pos
        shift += 7
        if shift > 63:
            raise ValueError("varint 过长")


def encode_manifest(
    entries: Union[Mapping[str, ScannedEntry], Iterable[ScannedEntry]],
) -> bytes:
    if isinstance(entries, Mapping):
        items: List[ScannedEntry] = list(entries.values())
    else:
        items = list(entries)
    items.sort(key=lambda e: e.path)
    out = bytearray(MANIFEST_MAGIC)
    out.append(MANIFEST_VERSION)
    out += struct.pack(">I", len(items))
    previous = ""
    for item in items:
        path = item.path
        common = 0
        limit = min(len(previous), len(path))
        while common < limit and previous[common] == path[common]:
            common += 1
        suffix = path[common:].encode("utf-8")
        _put_varint(out, common)
        _put_varint(out, len(suffix))
        out += suffix
        out += struct.pack(">QqB", item.size, item.mtime_ns, item.kind & 0xFF)
        previous = path
    return bytes(out)


def decode_manifest(data: bytes) -> Dict[str, ScannedEntry]:
    if not data.startswith(MANIFEST_MAGIC):
        raise ValueError("不是 MirrorUP manifest")
    pos = len(MANIFEST_MAGIC)
    if pos >= len(data):
        raise ValueError("manifest 缺少版本号")
    version = data[pos]
    pos += 1
    if version != MANIFEST_VERSION:
        raise ValueError(f"不支持的 manifest 版本：{version}")
    (count,) = struct.unpack_from(">I", data, pos)
    pos += 4
    result: Dict[str, ScannedEntry] = {}
    previous = ""
    for _ in range(count):
        prefix_len, pos = _get_varint(data, pos)
        suffix_len, pos = _get_varint(data, pos)
        if prefix_len > len(previous):
            raise ValueError("manifest 前缀长度非法")
        suffix = data[pos : pos + suffix_len].decode("utf-8")
        pos += suffix_len
        size, mtime_ns, kind = struct.unpack_from(">QqB", data, pos)
        pos += 17
        path = previous[:prefix_len] + suffix
        result[path] = ScannedEntry(path=path, kind=kind, size=size, mtime_ns=mtime_ns)
        previous = path
    return result


def encode_region_summary(
    size: int,
    mtime_ns: int,
    header: bytes,
    chunks: Union[
        Mapping[int, Tuple[int, Optional[bytes]]], Iterable[Tuple[int, int, Optional[bytes]]]
    ],
) -> bytes:
    if isinstance(chunks, Mapping):
        items = [(index, ts, digest) for index, (ts, digest) in chunks.items()]
    else:
        items = list(chunks)
    items.sort()
    out = bytearray(REGION_MAGIC)
    out += struct.pack(">QqI", size, mtime_ns, len(header))
    out += header
    out += struct.pack(">I", len(items))
    for index, timestamp, digest in items:
        out += struct.pack(">HIB", index, timestamp, 1 if digest is not None else 0)
        if digest is not None:
            if len(digest) != 16:
                raise ValueError("chunk 哈希必须是 16 字节")
            out += digest
    return bytes(out)


def decode_region_summary(
    data: bytes,
) -> Tuple[int, int, bytes, Dict[int, Tuple[int, Optional[bytes]]]]:
    if not data.startswith(REGION_MAGIC):
        raise ValueError("不是 MirrorUP region 摘要")
    pos = len(REGION_MAGIC)
    size, mtime_ns, header_len = struct.unpack_from(">QqI", data, pos)
    pos += 20
    header = data[pos : pos + header_len]
    pos += header_len
    (count,) = struct.unpack_from(">I", data, pos)
    pos += 4
    chunks: Dict[int, Tuple[int, Optional[bytes]]] = {}
    for _ in range(count):
        index, timestamp, has_digest = struct.unpack_from(">HIB", data, pos)
        pos += 7
        digest: Optional[bytes] = None
        if has_digest:
            digest = data[pos : pos + 16]
            pos += 16
        chunks[index] = (timestamp, digest)
    return size, mtime_ns, header, chunks


def encode_chunks(records: Dict[int, Tuple[int, bytes, bytes]]) -> bytes:
    items = sorted(records.items())
    out = bytearray(CHUNKS_MAGIC)
    out += struct.pack(">I", len(items))
    for index, (length, head, body) in items:
        if len(head) != 5:
            raise ValueError("chunk 记录头必须是 5 字节")
        if length != len(body) + 1:
            raise ValueError("chunk 长度与 payload 不一致")
        out += struct.pack(">HI", index, length)
        out += head
        out += body
    return bytes(out)


def decode_chunks(data: bytes) -> Dict[int, Tuple[int, bytes, bytes]]:
    if not data.startswith(CHUNKS_MAGIC):
        raise ValueError("不是 MirrorUP chunk 记录")
    pos = len(CHUNKS_MAGIC)
    (count,) = struct.unpack_from(">I", data, pos)
    pos += 4
    result: Dict[int, Tuple[int, bytes, bytes]] = {}
    for _ in range(count):
        index, length = struct.unpack_from(">HI", data, pos)
        pos += 6
        head = data[pos : pos + 5]
        pos += 5
        body = data[pos : pos + length - 1]
        pos += length - 1
        result[index] = (length, head, body)
    return result
