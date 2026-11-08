"""Region（``.mca``）容器解析与 chunk 级增量写入。

文件格式（全部大端）::

    0      .. 4096    位置表：1024 项 × 4B = 3B 扇区偏移 + 1B 扇区数
    4096   .. 8192    时间戳表：1024 项 × 4B（Unix 秒）
    8192   .. EOF     扇区区，每 4096B 一个扇区

每条 chunk 记录（`(cx & 31) + 32 * (cz & 31)` 号位置指向的扇区）::

    4B   长度 = 1B 压缩类型 + 压缩后的数据长度
    1B   压缩类型（1 gzip / 2 zlib / 3 原始 / 4 LZ4 / 127 自定义；|0x80 = 外置 .mcc）
    N B  数据
        然后补齐到 4096 的整数倍

本模块只做“搬运”：payload 原样拷贝，从不解压/重压缩。压缩算法对我们是不透明
字节，所以 1.13 到最新版本的 region 都能处理，遇到不认识的类型也不会坏。

增量写入的判据是 **chunk 记录的哈希**（含 4B 长度 + 1B 类型 + payload，不含扇区
填充）：同一个 chunk 换个扇区位置、填充长度变了，哈希不变。
"""

from __future__ import annotations

import os
import secrets
import shutil
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional, Set, Tuple

from .constants import REGION_NAME_RE
from .hashing import new_hasher

SECTOR_SIZE = 4096
HEADER_SECTORS = 2
HEADER_SIZE = SECTOR_SIZE * HEADER_SECTORS
CHUNK_COUNT = 1024
MAX_SECTORS = 255
EXTERNAL_FLAG = 0x80

#: 索引里 chunk 状态 blob 的魔数（改格式就换掉它，旧 blob 会被当成空）。
STATE_MAGIC = b"MU2"


class RegionFormatError(Exception):
    """region 文件结构不可信——调用方应降级为整文件复制，不要尝试修复。"""


class RegionExternalChunk(RegionFormatError):
    """包含外置（``.mcc``）chunk，v0.2 不处理，降级整文件复制。"""


class RegionUnstable(Exception):
    """读取期间源文件被改写。"""


def supports_delta(name: str) -> bool:
    """只有 ``.mca`` 走 chunk 级增量；``.mcr``（Region 旧格式）没有这套头。"""
    if REGION_NAME_RE.match(name) is None:
        return False
    return name.lower().endswith(".mca")


@dataclass(frozen=True)
class Slot:
    index: int
    offset: int      # 扇区偏移（相对文件头）
    sectors: int
    timestamp: int   # Unix 秒


@dataclass
class ChunkDelta:
    index: int
    offset: int
    sectors: int
    timestamp: int
    digest: Optional[bytes] = None
    read: bool = False

    def as_slot(self) -> Slot:
        return Slot(self.index, self.offset, self.sectors, self.timestamp)


@dataclass
class RegionScan:
    path: Path
    size: int
    mtime_ns: int
    header: bytes
    chunks: Dict[int, ChunkDelta]
    dirty: Set[int]

    def digests(self) -> Dict[int, bytes]:
        out: Dict[int, bytes] = {}
        for index, chunk in self.chunks.items():
            if chunk.digest is not None:
                out[index] = chunk.digest
        return out


# ---------------------------------------------------------------------- #
# 解析
# ---------------------------------------------------------------------- #

def parse_header(raw: bytes, file_size: int) -> Dict[int, Slot]:
    """解析 8KB 头。任何越界/零扇区都算 ``corrupt``，不做猜测。"""
    if len(raw) < HEADER_SIZE:
        raise RegionFormatError(f"region 头不足 8KB（{len(raw)}B）")
    max_sector = file_size // SECTOR_SIZE
    slots: Dict[int, Slot] = {}
    for index in range(CHUNK_COUNT):
        pos = index * 4
        offset = (raw[pos] << 16) | (raw[pos + 1] << 8) | raw[pos + 2]
        sectors = raw[pos + 3]
        if offset == 0 and sectors == 0:
            continue
        if offset < HEADER_SECTORS:
            raise RegionFormatError(f"chunk {index} 的扇区偏移越界：{offset}")
        if sectors == 0 or sectors > MAX_SECTORS:
            raise RegionFormatError(f"chunk {index} 的扇区数非法：{sectors}")
        if offset + sectors > max_sector:
            raise RegionFormatError(
                f"chunk {index} 超出文件末尾：扇区 {offset}+{sectors} > {max_sector}"
            )
        timestamp = struct.unpack_from(">I", raw, SECTOR_SIZE + index * 4)[0]
        slots[index] = Slot(index, offset, sectors, timestamp)
    return slots


def read_record(fp, slot: Slot) -> Tuple[int, bytes, bytes]:
    """读一条完整记录，返回 ``(length, head5, body)``。

    ``length`` 数的是"压缩类型字节 + payload"，所以读完 5 字节头之后只剩
    ``length - 1`` 字节；``head`` 的前 4 字节是长度、第 5 字节是压缩类型。
    记录尾部补到整扇区的零字节不属于内容，不读也不参与哈希。
    """
    fp.seek(slot.offset * SECTOR_SIZE)
    head = fp.read(5)
    if len(head) != 5:
        raise RegionFormatError(f"chunk {slot.index} 的记录头被截断")
    length = struct.unpack(">I", head[:4])[0]
    if length < 1:
        raise RegionFormatError(f"chunk {slot.index} 的长度非法：{length}")
    if 4 + length > slot.sectors * SECTOR_SIZE:
        raise RegionFormatError(
            f"chunk {slot.index} 的记录超出其扇区数（{length}B > {slot.sectors} 扇区）"
        )
    body = fp.read(length - 1)
    if len(body) != length - 1:
        raise RegionFormatError(f"chunk {slot.index} 的 payload 被截断")
    return length, head, body


def record_digest(head: bytes, body: bytes) -> bytes:
    hasher = new_hasher()
    hasher.update(head)
    hasher.update(body)
    return hasher.digest()


def _is_dirty(timestamp: int, since: Optional[int], prev_known: bool) -> bool:
    if since is None or not prev_known:
        return True
    if timestamp == 0:  # 时间戳被工具抹掉过，不能信
        return True
    return timestamp >= since


def scan_region(
    path: os.PathLike[str] | str,
    dirty_since: Optional[int] = None,
    previous: Optional[Dict[int, bytes]] = None,
) -> RegionScan:
    """扫描一个 region，只读取“可能需要”的 chunk。

    ``dirty_since`` 是上一轮同步开始的 Unix 秒；时间戳早于它的 chunk 视为未变，
    直接沿用 ``previous`` 里的哈希（因此稳态下源端只读 8KB 头）。
    """
    previous = previous or {}
    p = Path(path)
    st0 = os.stat(p)
    if st0.st_size < HEADER_SIZE:
        raise RegionFormatError(f"region 文件不足 8KB：{p}")

    chunks: Dict[int, ChunkDelta] = {}
    dirty: Set[int] = set()

    with open(p, "rb") as fp:
        header = fp.read(HEADER_SIZE)
        slots = parse_header(header, st0.st_size)
        for index, slot in slots.items():
            chunk = ChunkDelta(
                index=index,
                offset=slot.offset,
                sectors=slot.sectors,
                timestamp=slot.timestamp,
            )
            prev = previous.get(index)
            if _is_dirty(slot.timestamp, dirty_since, prev is not None):
                _length, head, body = read_record(fp, slot)
                if head[4] & EXTERNAL_FLAG:
                    raise RegionExternalChunk(
                        f"chunk {index} 使用外置 .mcc 容器，降级为整文件复制"
                    )
                chunk.digest = record_digest(head, body)
                chunk.read = True
                dirty.add(index)
            else:
                chunk.digest = prev
            chunks[index] = chunk
        fp.seek(0)
        header2 = fp.read(HEADER_SIZE)

    st1 = os.stat(p)
    if (
        header2 != header
        or st1.st_size != st0.st_size
        or st1.st_mtime_ns != st0.st_mtime_ns
    ):
        raise RegionUnstable(f"region 在读取期间被改写：{p}")

    return RegionScan(
        path=p,
        size=st1.st_size,
        mtime_ns=st1.st_mtime_ns,
        header=header,
        chunks=chunks,
        dirty=dirty,
    )


def logical_digest(chunks: Dict[int, ChunkDelta]) -> bytes:
    """region 的“内容身份”：所有 chunk 哈希的有序拼接，不含扇区布局。"""
    hasher = new_hasher()
    for index in sorted(chunks):
        digest = chunks[index].digest
        if digest is None:
            raise RegionFormatError(f"chunk {index} 缺少哈希，无法计算 region 摘要")
        hasher.update(struct.pack(">H", index))
        hasher.update(digest)
    return hasher.digest()


# ---------------------------------------------------------------------- #
# 索引状态编解码
# ---------------------------------------------------------------------- #

def encode_state(digests: Dict[int, bytes]) -> bytes:
    items = sorted(digests.items())
    out = bytearray(STATE_MAGIC)
    out += struct.pack(">H", len(items))
    for index, digest in items:
        if len(digest) != 16:
            raise ValueError("chunk 哈希必须是 16 字节")
        out += struct.pack(">H", index)
        out += digest
    return bytes(out)


def decode_state(blob: Optional[bytes]) -> Dict[int, bytes]:
    if not blob or len(blob) < 5 or not blob.startswith(STATE_MAGIC):
        return {}
    (count,) = struct.unpack_from(">H", blob, 3)
    out: Dict[int, bytes] = {}
    pos = 5
    for _ in range(count):
        if pos + 18 > len(blob):
            break
        (index,) = struct.unpack_from(">H", blob, pos)
        out[index] = bytes(blob[pos + 2 : pos + 18])
        pos += 18
    return out


# ---------------------------------------------------------------------- #
# 写入
# ---------------------------------------------------------------------- #

@dataclass
class RegionWriteResult:
    logical: bytes
    state: bytes
    written_bytes: int
    chunks_written: int
    chunks_reused: int
    chunks_dropped: int
    size: int
    mtime_ns: int


def build_header(entries: Dict[int, Tuple[int, int, int]]) -> bytes:
    locations = bytearray(SECTOR_SIZE)
    timestamps = bytearray(SECTOR_SIZE)
    for index, (offset, sectors, timestamp) in entries.items():
        base = index * 4
        locations[base] = (offset >> 16) & 0xFF
        locations[base + 1] = (offset >> 8) & 0xFF
        locations[base + 2] = offset & 0xFF
        locations[base + 3] = sectors & 0xFF
        struct.pack_into(">I", timestamps, index * 4, timestamp & 0xFFFFFFFF)
    return bytes(locations) + bytes(timestamps)


def write_region_delta(
    scan: RegionScan,
    dst_path: os.PathLike[str] | str,
    previous: Optional[Dict[int, bytes]] = None,
    force_full: bool = False,
) -> RegionWriteResult:
    """把 ``scan`` 描述的内容落到目标，复用目标里未变的 chunk 字节。

    ``force_full`` 时不信任目标内容：所有 chunk 都从源端重读重写（目标被玩家改过
    的场景）。否则先把目标文件整份复制成临时文件，再把变更的 chunk 覆盖上去，
    最后回填 8KB 头并原子替换。整个过程中目标文件本身从未被写过。
    """
    previous = previous or {}
    src = scan.path
    dst = Path(dst_path)
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.parent / f".{dst.name}.mirrorup-{secrets.token_hex(4)}.tmp"

    origin: Dict[int, Slot] = {}
    reuse_origin = False
    if not force_full and dst.exists():
        try:
            with open(dst, "rb") as fp:
                size = os.fstat(fp.fileno()).st_size
                origin = parse_header(fp.read(HEADER_SIZE), size)
            reuse_origin = True
        except (OSError, RegionFormatError):
            origin = {}
            reuse_origin = False

    if reuse_origin:
        try:
            shutil.copyfile(dst, tmp)
        except OSError:
            reuse_origin = False

    try:
        if not reuse_origin:
            with open(tmp, "wb") as fresh:
                fresh.write(b"\x00" * HEADER_SIZE)

        entries: Dict[int, Tuple[int, int, int]] = {}
        state: Dict[int, bytes] = {}
        written = os.path.getsize(tmp)  # 复制旧目标（或写 8KB 空头）的字节数
        reused = 0
        end_sector = max(HEADER_SECTORS, (os.path.getsize(tmp) + SECTOR_SIZE - 1) // SECTOR_SIZE)

        with open(tmp, "r+b") as fout, open(src, "rb") as fin:
            fout.seek(0, os.SEEK_END)

            for index in sorted(scan.chunks):
                chunk = scan.chunks[index]
                prev = previous.get(index)
                slot = origin.get(index)

                unchanged = False
                if reuse_origin and chunk.digest is not None:
                    if chunk.read:
                        unchanged = prev is not None and chunk.digest == prev
                    else:
                        unchanged = True

                if unchanged and slot is not None:
                    entries[index] = (slot.offset, slot.sectors, chunk.timestamp)
                    state[index] = chunk.digest  # type: ignore[assignment]
                    reused += 1
                    continue

                # 需要从源端搬这条记录
                length, head, body = read_record(fin, chunk.as_slot())
                digest = chunk.digest if chunk.digest is not None else record_digest(head, body)
                need = (4 + length + SECTOR_SIZE - 1) // SECTOR_SIZE
                if need > MAX_SECTORS:
                    raise RegionFormatError(f"chunk {index} 记录过大：{length}B")

                if slot is not None and need <= slot.sectors:
                    offset, sectors = slot.offset, slot.sectors
                else:
                    offset, sectors = end_sector, need
                    end_sector += need

                fout.seek(offset * SECTOR_SIZE)
                fout.write(head)
                fout.write(body)
                padding = sectors * SECTOR_SIZE - (4 + length)
                if padding > 0:
                    fout.write(b"\x00" * padding)

                entries[index] = (offset, sectors, chunk.timestamp)
                state[index] = digest
                written += sectors * SECTOR_SIZE

            dropped = sum(1 for index in origin if index not in entries)

            fout.seek(0)
            fout.write(build_header(entries))
            fout.truncate(end_sector * SECTOR_SIZE)
            fout.flush()
            os.fsync(fout.fileno())

        os.replace(tmp, dst)
        fsync_dir(dst.parent)
        stat = os.stat(dst)
        logical = new_hasher()
        for index in sorted(state):
            logical.update(struct.pack(">H", index))
            logical.update(state[index])

        return RegionWriteResult(
            logical=logical.digest(),
            state=encode_state(state),
            written_bytes=written,
            chunks_written=len(entries) - reused,
            chunks_reused=reused,
            chunks_dropped=dropped,
            size=stat.st_size,
            mtime_ns=stat.st_mtime_ns,
        )
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def fsync_dir(path: os.PathLike[str] | str) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)
