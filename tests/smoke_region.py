"""region chunk 级增量的冒烟测试（不依赖 MCDR）。

    python tests/smoke_region.py
"""

from __future__ import annotations

import copy
import os
import struct
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mirror_up.config import DEFAULT_CONFIG, parse_config, validate_config  # noqa: E402
from mirror_up.engine import SyncEngine  # noqa: E402
from mirror_up.flush import NullSourceFlusher  # noqa: E402
from mirror_up.index import Index  # noqa: E402
from mirror_up.region import (  # noqa: E402
    HEADER_SIZE,
    RegionExternalChunk,
    RegionFormatError,
    build_header,
    decode_state,
    encode_state,
    logical_digest,
    parse_header,
    read_record,
    scan_region,
    supports_delta,
    write_region_delta,
)


# ---------------------------------------------------------------------- #
# 造 region 文件
# ---------------------------------------------------------------------- #

def write_region(
    path: Path,
    chunks: dict[int, bytes],
    timestamp: int | dict[int, int],
    external: set[int] | None = None,
) -> None:
    """按给定 payload 造一个合法的 region 文件（压缩类型 3 = 未压缩）。

    ``timestamp`` 可以是一个整数（所有 chunk 同一时间），也可以是
    ``{index: 秒}``，这样能模拟"只改了一个 chunk、其余保留旧时间戳"。
    """
    external = external or set()
    entries = {}
    body = bytearray()
    next_offset = 2
    for index in sorted(chunks):
        payload = chunks[index]
        if index in external:
            record = struct.pack(">I", 1) + bytes([0x80 | 3])
        else:
            record = struct.pack(">I", 1 + len(payload)) + bytes([3]) + payload
        sectors = (len(record) + 4095) // 4096
        stamp = timestamp.get(index, 0) if isinstance(timestamp, dict) else timestamp
        entries[index] = (next_offset, sectors, stamp)
        body += record
        body += b"\x00" * (sectors * 4096 - len(record))
        next_offset += sectors
    Path(path).write_bytes(build_header(entries) + bytes(body))


def read_chunks(path: Path) -> dict[int, bytes]:
    with open(path, "rb") as fp:
        size = os.fstat(fp.fileno()).st_size
        slots = parse_header(fp.read(HEADER_SIZE), size)
        out = {}
        for index, slot in slots.items():
            _length, _head, body = read_record(fp, slot)
            out[index] = body
        return out


def check(label: str, condition: bool, extra: str = "") -> None:
    print(("  OK   " if condition else "  FAIL ") + label + (f"  [{extra}]" if extra else ""))
    if not condition:
        raise SystemExit(1)


# ---------------------------------------------------------------------- #
# 用例
# ---------------------------------------------------------------------- #

def case_scan_and_build(tmp: Path) -> dict[int, bytes]:
    print("\n[1] 首次构建：全部 chunk 都要读")
    src = tmp / "r.0.0.mca"
    dst = tmp / "out" / "r.0.0.mca"
    payloads = {0: b"a" * 100, 1: b"b" * 5000, 2: bytes(range(256)) * 40}
    write_region(src, payloads, timestamp=1000)

    scan = scan_region(src)
    check("三个 chunk 都算脏", scan.dirty == {0, 1, 2}, str(sorted(scan.dirty)))

    result = write_region_delta(scan, dst)
    check("目标文件生成", dst.exists())
    check("三个 chunk 都写入", result.chunks_written == 3, str(result.chunks_written))
    check("没有复用", result.chunks_reused == 0)
    check("目标内容与源一致", read_chunks(dst) == payloads)
    check("状态可编解码", decode_state(result.state) == scan.digests())
    return {"src": src, "dst": dst, "state": result.state, "payloads": payloads}


def case_no_change(ctx: dict) -> None:
    print("\n[2] 源没动：0 个 chunk 被读取")
    src, dst = ctx["src"], ctx["dst"]
    previous = decode_state(ctx["state"])
    scan = scan_region(src, dirty_since=5000, previous=previous)
    check("没有脏 chunk", not scan.dirty, str(sorted(scan.dirty)))
    check("复用上次的哈希", scan.digests() == previous)

    result = write_region_delta(scan, dst, previous=previous)
    check("0 个 chunk 写入", result.chunks_written == 0, str(result.chunks_written))
    check("3 个 chunk 复用", result.chunks_reused == 3, str(result.chunks_reused))
    check("内容仍然一致", read_chunks(dst) == ctx["payloads"])


def case_one_chunk_changed(ctx: dict) -> None:
    print("\n[3] 只改一个 chunk")
    src, dst = ctx["src"], ctx["dst"]
    payloads = dict(ctx["payloads"])
    payloads[1] = b"CHANGED" * 300
    write_region(src, payloads, timestamp={0: 1000, 1: 2000, 2: 1000})

    previous = decode_state(ctx["state"])
    scan = scan_region(src, dirty_since=1500, previous=previous)
    check("只有 1 号 chunk 是脏的", scan.dirty == {1}, str(sorted(scan.dirty)))
    check("1 号哈希发生变化", scan.chunks[1].digest != previous[1])

    result = write_region_delta(scan, dst, previous=previous)
    check("只写了 1 个 chunk", result.chunks_written == 1, str(result.chunks_written))
    check("复用了 2 个 chunk", result.chunks_reused == 2, str(result.chunks_reused))
    check("目标内容与源一致", read_chunks(dst) == payloads)
    ctx["payloads"] = payloads
    ctx["state"] = result.state


def case_add_and_remove(ctx: dict) -> None:
    print("\n[4] 一个 chunk 新增、一个删除")
    src, dst = ctx["src"], ctx["dst"]
    payloads = {1: ctx["payloads"][1], 2: ctx["payloads"][2], 7: b"new" * 700}
    write_region(src, payloads, timestamp={1: 2000, 2: 1000, 7: 3000})

    previous = decode_state(ctx["state"])
    scan = scan_region(src, dirty_since=2500, previous=previous)
    check("新增的 7 号被读取", 7 in scan.dirty, str(sorted(scan.dirty)))

    result = write_region_delta(scan, dst, previous=previous)
    check("删掉了 1 个 chunk 的记录", result.chunks_dropped == 1, str(result.chunks_dropped))
    check("目标内容与源一致", read_chunks(dst) == payloads)
    ctx["payloads"] = payloads
    ctx["state"] = result.state


def case_corrupt_and_external(tmp: Path) -> None:
    print("\n[5] 坏文件 / 外置 chunk 必须报错而不是猜")
    bad = tmp / "bad.mca"
    payloads = {0: b"x" * 10}
    write_region(bad, payloads, timestamp=1)
    raw = bytearray(bad.read_bytes())
    raw[0:3] = (99999).to_bytes(3, "big")  # 偏移远超文件
    bad.write_bytes(bytes(raw))
    try:
        scan_region(bad)
        raised = False
    except RegionFormatError:
        raised = True
    check("越界偏移被判定为 corrupt", raised)

    ext = tmp / "ext.mca"
    write_region(ext, {0: b"y" * 10}, timestamp=1, external={0})
    try:
        scan_region(ext)
        raised = False
    except RegionExternalChunk:
        raised = True
    check("外置 chunk 触发降级", raised)

    check(".mcr 不走增量", not supports_delta("r.0.0.mcr"))
    check(".mca 走增量", supports_delta("r.-1.2.mca"))


def case_engine(tmp: Path) -> None:
    print("\n[6] 引擎端到端：改一个 chunk，只有 1 个 chunk 被写")
    src_base = tmp / "engine-src"
    dst_base = tmp / "engine-dst"
    src_root = src_base / "world"
    dst_root = dst_base / "world"
    region_src = src_root / "region" / "r.0.0.mca"
    region_dst = dst_root / "region" / "r.0.0.mca"
    region_src.parent.mkdir(parents=True)
    (src_root / "level.dat").write_bytes(os.urandom(64))

    payloads = {index: bytes([index]) * (100 + index * 500) for index in range(4)}
    old_ts = int(time.time()) - 3600
    write_region(region_src, payloads, timestamp=old_ts)

    work = tmp / "engine"
    work.mkdir()
    raw = copy.deepcopy(DEFAULT_CONFIG)
    raw["source"]["root"] = str(src_base)
    raw["target"]["root"] = str(dst_base)
    raw["sets"] = [{"name": "world", "source": "world", "target": "world", "enabled": True}]
    cfg = parse_config(raw)
    cfg.source_flush.enabled = False
    validate_config(cfg, work)

    index = Index(work / "index.sqlite3")
    engine = SyncEngine(cfg, work, index, flusher=NullSourceFlusher())

    first = engine.run(trigger="test")
    check("首轮成功", first.ok(), first.abort_reason or first.error)
    check("首轮写入 4 个 chunk", first.chunks_written == 4, str(first.chunks_written))
    check("首轮内容一致", read_chunks(region_dst) == payloads)
    first_bytes = first.bytes_written

    payloads[2] = b"Z" * 900
    write_region(
        region_src,
        payloads,
        timestamp={0: old_ts, 1: old_ts, 2: int(time.time()), 3: old_ts},
    )

    second = engine.run(trigger="test")
    check("第二轮成功", second.ok(), second.abort_reason or second.error)
    check("只写 1 个 chunk", second.chunks_written == 1, str(second.chunks_written))
    check("复用 3 个 chunk", second.chunks_reused == 3, str(second.chunks_reused))
    check("目标内容与源一致", read_chunks(region_dst) == payloads)
    check(
        # v0.2 是 rebuild 模式：新增的写量 ≈ 临时文件复制 + 一个扇区，
        # 省下来的是源端读取（以及跨机时的传输），不是目标端写入。
        "第二轮只多写一个扇区",
        0 < second.bytes_written - first_bytes <= 4096,
        f"{second.bytes_written} vs {first_bytes}",
    )

    print("\n[7] 镜像被改：force 重建，不复用")
    tampered = bytearray(region_dst.read_bytes())
    tampered[-1] ^= 0xFF
    region_dst.write_bytes(bytes(tampered))
    os.utime(region_dst, (time.time() + 5, time.time() + 5))

    third = engine.run(trigger="test")
    check("第三轮成功", third.ok(), third.abort_reason or third.error)
    check("强制重建时 0 复用", third.chunks_reused == 0, str(third.chunks_reused))
    check("镜像被修复", read_chunks(region_dst) == payloads)

    index.close()


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="mirrorup-region-"))
    print(f"workdir: {tmp}")
    ctx = case_scan_and_build(tmp)
    case_no_change(ctx)
    case_one_chunk_changed(ctx)
    case_add_and_remove(ctx)
    case_corrupt_and_external(tmp)
    case_engine(tmp)
    print("\n全部通过")


if __name__ == "__main__":
    main()
