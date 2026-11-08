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

import contextlib
import hashlib
import os
import secrets
import shutil
import struct
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, Optional, Set, Tuple

from .constants import REGION_NAME_RE
from .hashing import new_hasher, region_coords

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
    """外置（``.mcc``）chunk 无法处理——例如 ``.mcc`` 文件缺失或路径不可解析。"""


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
    #: 数据在 ``.mcc`` 外置容器里（region 内只有 5 字节占位记录）
    external: bool = False
    #: ``.mcc`` 文件相对 set 根的路径（POSIX 风格）
    external_rel: Optional[str] = None

    def as_slot(self) -> Slot:
        return Slot(self.index, self.offset, self.sectors, self.timestamp)


@dataclass
class RegionScan:
    #: 本地扫描时是源文件路径；跨机扫描（agent）时为 ``None``
    path: Optional[Path]
    size: int
    mtime_ns: int
    header: bytes
    chunks: Dict[int, ChunkDelta]
    dirty: Set[int]
    #: 本轮实际读过的外置 chunk 文件（相对 set 根），供上层判断依赖
    external_files: Set[str] = field(default_factory=set)

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


def external_chunk_rel(region_path: os.PathLike[str] | str, index: int) -> str:
    """``r.X.Z.mca`` 里第 ``index`` 个 chunk 对应的 ``.mcc`` 文件名。

    chunk 坐标是全局的：``cx = X*32 + index % 32``、``cz = Z*32 + index // 32``。
    """
    name = Path(region_path).name
    coords = region_coords(name)
    if coords is None:
        raise RegionExternalChunk(f"无法从文件名解析 region 坐标：{name}")
    rx, rz = coords
    cx = rx * 32 + (index % 32)
    cz = rz * 32 + (index // 32)
    return f"c.{cx}.{cz}.mcc"


def _external_digest(
    region_path: Path,
    index: int,
    set_root: Optional[os.PathLike[str] | str],
) -> Tuple[bytes, str, int]:
    """外置 chunk 的身份 = H(5B 占位记录 || H(.mcc 内容))。

    占位记录进哈希是必须的：否则两个内容相同的 ``.mcc`` 会被当成同一个 chunk，
    而它们的压缩类型/长度字段可能不同。
    """
    if set_root is None:
        raise RegionExternalChunk("外置 chunk 需要 set 根目录才能定位 .mcc")
    rel_in_region = external_chunk_rel(region_path, index)
    root = Path(set_root)
    try:
        region_dir = region_path.parent.relative_to(root)
    except ValueError as exc:
        raise RegionExternalChunk(f"region 不在 set 根内：{region_path}") from exc
    rel = (region_dir / rel_in_region).as_posix()
    mcc_path = root / region_dir / rel_in_region

    hasher = new_hasher()
    total = 0
    with open(mcc_path, "rb") as fp:
        while True:
            block = fp.read(1 << 20)
            if not block:
                break
            hasher.update(block)
            total += len(block)
    return hasher.digest(), rel, total


def scan_region(
    path: os.PathLike[str] | str,
    dirty_since: Optional[int] = None,
    previous: Optional[Dict[int, bytes]] = None,
    set_root: Optional[os.PathLike[str] | str] = None,
) -> RegionScan:
    """扫描一个 region，只读取“可能需要”的 chunk。

    ``dirty_since`` 是上一轮同步开始的 Unix 秒；时间戳早于它的 chunk 视为未变，
    直接沿用 ``previous`` 里的哈希（因此稳态下源端只读 8KB 头）。

    外置（``.mcc``）chunk 只读占位记录和外部文件的内容哈希；它们的字节不在
    region 里，真正搬运由普通文件同步完成（镜像服在同步期间是停的，先后顺序
    不影响结果）。
    """
    previous = previous or {}
    p = Path(path)
    st0 = os.stat(p)
    if st0.st_size < HEADER_SIZE:
        raise RegionFormatError(f"region 文件不足 8KB：{p}")

    chunks: Dict[int, ChunkDelta] = {}
    dirty: Set[int] = set()
    external_files: Set[str] = set()

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
                    digest, rel, _size = _external_digest(p, index, set_root)
                    chunk.digest = record_digest(head, digest)
                    chunk.read = True
                    chunk.external = True
                    chunk.external_rel = rel
                    external_files.add(rel)
                else:
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
        external_files=external_files,
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
    #: 本轮是否因碎片过多而退化成整文件重写
    compacted: bool = False


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


def _read_chunk_payload(reader, fin, scan, chunk: ChunkDelta) -> Tuple[int, bytes, bytes, bytes]:
    """取一条 chunk 的 ``(length, head, body, digest)``。

    ``reader`` 非空时按 index 从外部取（跨机模式：HTTP 批量拉取）；否则从源
    region 文件读。外置 chunk 只能信扫描阶段算出的哈希（它的字节不在 region 里）。
    """
    if reader is not None:
        length, head, body = reader(chunk.index)
    else:
        if fin is None:
            raise RegionFormatError(f"chunk {chunk.index} 没有可用的数据来源")
        length, head, body = read_record(fin, chunk.as_slot())

    if chunk.external:
        if chunk.digest is None:
            raise RegionExternalChunk(f"chunk {chunk.index} 缺少外置内容哈希")
        return length, head, body, chunk.digest

    digest = record_digest(head, body)
    if chunk.digest is not None and chunk.digest != digest:
        raise RegionUnstable(f"chunk {chunk.index} 在扫描与写入之间被改写")
    return length, head, body, digest


def write_region_delta(
    scan: RegionScan,
    dst_path: os.PathLike[str] | str,
    previous: Optional[Dict[int, bytes]] = None,
    force_full: bool = False,
    get_record: Optional[Callable[[int], Tuple[int, bytes, bytes]]] = None,
) -> RegionWriteResult:
    """把 ``scan`` 描述的内容落到目标，复用目标里未变的 chunk 字节。

    ``force_full`` 时不信任目标内容：所有 chunk 都从源端重读重写（目标被玩家改过
    的场景）。否则先把目标文件整份复制成临时文件，再把变更的 chunk 覆盖上去，
    最后回填 8KB 头并原子替换。整个过程中目标文件本身从未被写过。
    """
    previous = previous or {}
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

        with contextlib.ExitStack() as stack:
            fout = stack.enter_context(open(tmp, "r+b"))
            fin = None
            if get_record is None:
                if scan.path is None:
                    raise RegionFormatError("region 扫描结果没有本地路径，且未提供 chunk 读取器")
                fin = stack.enter_context(open(scan.path, "rb"))
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
                length, head, body, digest = _read_chunk_payload(get_record, fin, scan, chunk)
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


# ---------------------------------------------------------------------- #
# 头 journal：inplace 模式唯一的崩溃保护
# ---------------------------------------------------------------------- #

#: journal 文件格式：``MUJ1 | 4B 路径长度 | 路径 | 16B 头哈希 | 8192B 旧头``。
JOURNAL_MAGIC = b"MUJ1"
JOURNAL_SUFFIX = ".hdr"


def journal_file(journal_dir: os.PathLike[str] | str, rel_path: str) -> Path:
    """每个 region 一个 journal 文件，文件名由目标相对路径决定。"""
    digest = hashlib.sha1(rel_path.encode("utf-8")).hexdigest()
    return Path(journal_dir) / (digest + JOURNAL_SUFFIX)


def write_journal(
    journal_dir: os.PathLike[str] | str, rel_path: str, header: bytes
) -> Path:
    """把旧 8KB 头落盘（临时文件 + 原子替换 + fsync）。"""
    if len(header) != HEADER_SIZE:
        raise ValueError("journal 只接受完整的 8KB region 头")
    path = journal_file(journal_dir, rel_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    raw_path = rel_path.encode("utf-8")
    payload = bytearray(JOURNAL_MAGIC)
    payload += struct.pack(">I", len(raw_path))
    payload += raw_path
    hasher = new_hasher()
    hasher.update(header)
    payload += hasher.digest()
    payload += header

    tmp = path.parent / f".{path.name}.{secrets.token_hex(4)}.tmp"
    with open(tmp, "wb") as fp:
        fp.write(bytes(payload))
        fp.flush()
        os.fsync(fp.fileno())
    os.replace(tmp, path)
    fsync_dir(path.parent)
    return path


def _read_journal(path: Path) -> Optional[Tuple[str, bytes]]:
    """读取并校验 journal；损坏的 journal 返回 ``None``（调用方应删除）。"""
    try:
        raw = path.read_bytes()
    except OSError:
        return None
    if len(raw) < 8 + 16 + HEADER_SIZE or not raw.startswith(JOURNAL_MAGIC):
        return None
    (path_len,) = struct.unpack_from(">I", raw, 4)
    if path_len <= 0 or path_len > 4096:
        return None
    start = 8
    if start + path_len + 16 + HEADER_SIZE > len(raw):
        return None
    rel = raw[start : start + path_len].decode("utf-8", "replace")
    if not rel or rel.startswith("/") or ".." in rel.split("/"):
        return None
    start += path_len
    digest = raw[start : start + 16]
    header = raw[start + 16 : start + 16 + HEADER_SIZE]
    hasher = new_hasher()
    hasher.update(header)
    if hasher.digest() != digest:
        return None
    return rel, header


def _restore_header(dst: Path, header: bytes) -> None:
    with open(dst, "r+b") as fp:
        fp.seek(0)
        fp.write(header)
        fp.flush()
        os.fsync(fp.fileno())


def recover_journal_at(
    journal_dir: os.PathLike[str] | str,
    rel_path: str,
    dst_path: os.PathLike[str] | str,
) -> bool:
    """写这个 region 之前先看有没有上一轮残留的 journal；有就把旧头写回去。

    ``inplace`` 只追加、不覆盖旧 chunk，所以旧头一旦恢复，文件立刻回到
    “完整可读”的状态；本轮该做的修改会在后面正常重做（幂等）。
    """
    path = journal_file(journal_dir, rel_path)
    if not path.exists():
        return False
    parsed = _read_journal(path)
    dst = Path(dst_path)
    restored = False
    if parsed is not None and parsed[0] == rel_path and dst.exists():
        try:
            _restore_header(dst, parsed[1])
            restored = True
        except OSError:
            return False  # 保留 journal，下一轮再试
    try:
        path.unlink()
    except OSError:
        pass
    fsync_dir(path.parent)
    return restored


def recover_all_journals(
    journal_dir: os.PathLike[str] | str, target_root: os.PathLike[str] | str
) -> int:
    """启动时扫一遍 journal 目录，把残留的头全部恢复（返回恢复的文件数）。"""
    root = Path(journal_dir)
    if not root.is_dir():
        return 0
    base = Path(target_root)
    restored = 0
    for path in sorted(root.glob("*" + JOURNAL_SUFFIX)):
        parsed = _read_journal(path)
        if parsed is not None:
            rel, header = parsed
            dst = base.joinpath(*rel.split("/"))
            if dst.exists():
                try:
                    _restore_header(dst, header)
                    restored += 1
                except OSError:
                    continue  # 恢复不了就留着，下次再试
        try:
            path.unlink()
        except OSError:
            pass
    fsync_dir(root)
    return restored


# ---------------------------------------------------------------------- #
# inplace：append-and-commit
# ---------------------------------------------------------------------- #

def _cleanup_tmp(dst: Path) -> None:
    for stale in dst.parent.glob(f".{dst.name}.mirrorup-*.tmp"):
        try:
            stale.unlink()
        except OSError:
            pass


def write_region_inplace(
    scan: RegionScan,
    dst_path: os.PathLike[str] | str,
    previous: Optional[Dict[int, bytes]] = None,
    force_full: bool = False,
    compaction_ratio: float = 0.5,
    journal_dir: Optional[os.PathLike[str] | str] = None,
    rel_path: str = "",
    get_record: Optional[Callable[[int], Tuple[int, bytes, bytes]]] = None,
) -> RegionWriteResult:
    """直接改目标文件：旧 chunk 原地保留，新 chunk 追加到末尾，最后提交 8KB 头。

    不变式：

    * 提交头之前**绝不覆盖旧 chunk 占用的扇区**，所以旧头始终指向完好数据；
    * 新数据先 ``fsync``，再写 journal（旧头），最后才写新头；
    * 头写到一半崩溃 → 下一轮用 journal 恢复旧头，region 回到完整状态；
    * 文件只增不减，孤儿扇区超过 ``compaction_ratio`` 时整文件重建一次。

    目标不存在、头结构不可信、或需要 compaction 时退回
    :func:`write_region_delta`（原子替换，不写 journal）。
    """
    previous = previous or {}
    dst = Path(dst_path)
    dst.parent.mkdir(parents=True, exist_ok=True)

    if journal_dir is not None:
        recover_journal_at(journal_dir, rel_path, dst)

    if force_full or not dst.exists():
        return write_region_delta(scan, dst, previous, force_full=True, get_record=get_record)

    try:
        with open(dst, "rb") as fp:
            size = os.fstat(fp.fileno()).st_size
            if size < HEADER_SIZE:
                raise RegionFormatError("目标 region 头不足 8KB")
            origin_header = fp.read(HEADER_SIZE)
        origin = parse_header(origin_header, size)
    except (OSError, RegionFormatError):
        return write_region_delta(scan, dst, previous, force_full=True, get_record=get_record)

    _cleanup_tmp(dst)

    total_sectors = (size + SECTOR_SIZE - 1) // SECTOR_SIZE
    live_sectors = HEADER_SECTORS + sum(slot.sectors for slot in origin.values())
    if total_sectors > live_sectors * (1.0 + max(0.0, float(compaction_ratio))):
        result = write_region_delta(scan, dst, previous, force_full=True, get_record=get_record)
        result.compacted = True
        return result

    entries: Dict[int, Tuple[int, int, int]] = {}
    state: Dict[int, bytes] = {}
    written = 0
    end_sector = (size + SECTOR_SIZE - 1) // SECTOR_SIZE

    with contextlib.ExitStack() as stack:
        fout = stack.enter_context(open(dst, "r+b"))
        fin = None
        if get_record is None:
            if scan.path is None:
                raise RegionFormatError("region 扫描结果没有本地路径，且未提供 chunk 读取器")
            fin = stack.enter_context(open(scan.path, "rb"))
        for index in sorted(scan.chunks):
            chunk = scan.chunks[index]
            prev = previous.get(index)
            slot = origin.get(index)

            unchanged = False
            if chunk.digest is not None:
                if chunk.read:
                    unchanged = prev is not None and chunk.digest == prev
                else:
                    unchanged = True

            if unchanged and slot is not None:
                entries[index] = (slot.offset, slot.sectors, chunk.timestamp)
                state[index] = chunk.digest  # type: ignore[assignment]
                continue

            # 需要从源端搬这条记录：校验“扫描时算的哈希 == 现在读到的字节”，
            # 防止扫描与写入之间源端又改了一次，把不一致的内容写进镜像。
            length, head, body, digest = _read_chunk_payload(get_record, fin, scan, chunk)
            need = (4 + length + SECTOR_SIZE - 1) // SECTOR_SIZE
            if need > MAX_SECTORS:
                raise RegionFormatError(f"chunk {index} 记录过大：{length}B")

            fout.seek(end_sector * SECTOR_SIZE)
            fout.write(head)
            fout.write(body)
            padding = need * SECTOR_SIZE - (4 + length)
            if padding > 0:
                fout.write(b"\x00" * padding)

            entries[index] = (end_sector, need, chunk.timestamp)
            state[index] = digest
            written += need * SECTOR_SIZE
            end_sector += need

        # 先把新数据落盘，再写 journal，最后才动头。
        fout.flush()
        os.fsync(fout.fileno())

        journaled = False
        if journal_dir is not None:
            write_journal(journal_dir, rel_path, origin_header)
            journaled = True

        fout.seek(0)
        fout.write(build_header(entries))
        fout.flush()
        os.fsync(fout.fileno())

    if journaled:
        try:
            journal_file(journal_dir, rel_path).unlink()  # type: ignore[arg-type]
        except OSError:
            pass
        fsync_dir(Path(journal_dir))  # type: ignore[arg-type]

    stat = os.stat(dst)
    logical = new_hasher()
    for index in sorted(state):
        logical.update(struct.pack(">H", index))
        logical.update(state[index])

    return RegionWriteResult(
        logical=logical.digest(),
        state=encode_state(state),
        written_bytes=written + HEADER_SIZE + (HEADER_SIZE if journaled else 0),
        chunks_written=len(entries) - sum(
            1 for index in entries if index in origin and entries[index][0] == origin[index].offset
        ),
        chunks_reused=sum(
            1 for index in entries if index in origin and entries[index][0] == origin[index].offset
        ),
        chunks_dropped=sum(1 for index in origin if index not in entries),
        size=stat.st_size,
        mtime_ns=stat.st_mtime_ns,
    )
