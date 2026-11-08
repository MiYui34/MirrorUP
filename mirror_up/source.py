"""源端读取抽象。

上层（engine / applier）只跟 :class:`Source` 打交道，于是"源在同机磁盘上"和
"源在另一台机器上、由 MirrorUP agent 提供服务"走同一套增量逻辑：

* :class:`LocalSource` —— 直接读本地文件（v0.1~v0.3 的行为，原样保留）。
* :class:`AgentSource` —— 通过 HTTP 读 exporter 暴露的只读接口。

两条实现都必须满足同一组语义：

* ``scan`` 只返回元数据，不读内容；
* ``copy_to`` 读完后复核源端 ``(size, mtime_ns)``，变了就抛
  :class:`UnstableSource`，让 applier 重试（而不是把半新半旧的内容写进镜像）；
* ``scan_region`` 只读"可能变过"的 chunk，其余沿用上一轮哈希；
* 所有路径都相对 set 根，实现自己负责把 ``..`` 之类挡在外面。
"""

from __future__ import annotations

import hashlib
import hmac
import http.client
import json
import os
import ssl
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterator, Optional, Sequence, Tuple
from urllib.parse import urlencode, urlparse

from .config import AgentConfig, Config, SetConfig, set_source_path
from .constants import IO_CHUNK
from .excludes import GlobMatcher
from .hashing import hash_file, new_hasher
from .region import (
    ChunkDelta,
    RegionFormatError,
    RegionScan,
    decode_state,
    parse_header,
    read_record,
    scan_region,
    supports_delta,
)
from .scanner import ScanResult, ScannedEntry, scan_tree
from .wire import (
    decode_chunks,
    decode_manifest,
    decode_region_summary,
)


class SourceError(Exception):
    """源端不可用或返回了非法数据。"""


class SourceUnavailable(SourceError):
    """连不上、鉴权失败、协议错误——本轮应中止而不是把源当成空。"""


class SourceVanished(SourceError):
    """文件在读取前/期间消失了（通常意味着它刚被删除）。"""


class UnstableSource(SourceError):
    """源文件在读取期间被改写（读到的内容不对应任何完整版本）。"""


def _as_rel(rel: str) -> str:
    rel = rel.replace("\\", "/").strip("/")
    if not rel or rel.startswith("/") or ".." in rel.split("/"):
        raise SourceError(f"非法相对路径：{rel!r}")
    return rel


class Source:
    """接口定义（不是 abc，避免跨机实现被迫继承无关方法）。"""

    label = "source"
    #: 源在另一台机器上时，region writer 需要按 chunk 拉记录（见 Applier._record_getter）
    is_remote = False

    def scan(self, matcher: GlobMatcher) -> ScanResult:  # pragma: no cover - 接口
        raise NotImplementedError

    def stat(self, rel: str) -> Optional[Tuple[int, int]]:  # pragma: no cover
        raise NotImplementedError

    def copy_to(self, rel: str, dst: Path) -> Tuple[bytes, int, int]:  # pragma: no cover
        raise NotImplementedError

    def digest(self, rel: str) -> Optional[Tuple[bytes, int, int]]:
        """只算哈希、不落盘：``(hash, size, mtime_ns)``，文件不存在返回 None。

        深校验用。跨机时由 agent 在源端计算，不产生文件传输。
        """
        raise NotImplementedError

    def scan_region(
        self, rel: str, dirty_since: Optional[int], previous: Optional[Dict[int, bytes]]
    ) -> RegionScan:  # pragma: no cover
        raise NotImplementedError

    def get_records(self, rel: str, indices: Sequence[int]) -> Dict[int, Tuple[int, bytes, bytes]]:
        raise NotImplementedError

    def read_bytes(self, rel: str, limit: int = 8 << 20) -> Optional[bytes]:  # pragma: no cover
        raise NotImplementedError

    def close(self) -> None:
        pass

    def __enter__(self) -> "Source":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


# --------------------------------------------------------------------------- #
# 本地
# --------------------------------------------------------------------------- #


class LocalSource(Source):
    def __init__(self, root: Path, set_name: str = ""):
        self.root = Path(root)
        self.set_name = set_name
        self.label = f"local:{self.root}"

    def scan(self, matcher: GlobMatcher) -> ScanResult:
        return scan_tree(self.root, matcher)

    def stat(self, rel: str) -> Optional[Tuple[int, int]]:
        try:
            st = os.stat(self.root.joinpath(*_as_rel(rel).split("/")))
        except FileNotFoundError:
            return None
        return st.st_size, st.st_mtime_ns

    def copy_to(self, rel: str, dst: Path) -> Tuple[bytes, int, int]:
        src = self.root.joinpath(*_as_rel(rel).split("/"))
        try:
            st0 = os.stat(src)
        except FileNotFoundError as exc:
            raise SourceVanished(f"源文件不存在：{rel}") from exc

        hasher = new_hasher()
        written = 0
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(src, "rb") as fin, open(dst, "wb") as fout:
                while True:
                    block = fin.read(IO_CHUNK)
                    if not block:
                        break
                    hasher.update(block)
                    fout.write(block)
                    written += len(block)
                fout.flush()
                os.fsync(fout.fileno())
        except FileNotFoundError as exc:
            raise SourceVanished(f"源文件在读取期间消失：{rel}") from exc
        except OSError as exc:
            raise SourceError(f"读取源文件失败：{rel}（{exc}）") from exc

        try:
            st1 = os.stat(src)
        except FileNotFoundError as exc:
            raise SourceVanished(f"源文件在读取期间消失：{rel}") from exc

        if (
            written != st0.st_size
            or st1.st_size != st0.st_size
            or st1.st_mtime_ns != st0.st_mtime_ns
        ):
            raise UnstableSource(
                f"源文件在读取期间被改写（{st0.st_size}B@{st0.st_mtime_ns} → "
                f"{st1.st_size}B@{st1.st_mtime_ns}，实际读 {written}B）"
            )
        return hasher.digest(), written, st1.st_mtime_ns

    def digest(self, rel: str) -> Optional[Tuple[bytes, int, int]]:
        src = self.root.joinpath(*_as_rel(rel).split("/"))
        try:
            st = os.stat(src)
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise SourceError(f"读取源文件失败：{rel}（{exc}）") from exc
        try:
            return hash_file(src), st.st_size, st.st_mtime_ns
        except OSError as exc:
            raise SourceError(f"读取源文件失败：{rel}（{exc}）") from exc

    def scan_region(
        self, rel: str, dirty_since: Optional[int], previous: Optional[Dict[int, bytes]]
    ) -> RegionScan:
        path = self.root.joinpath(*_as_rel(rel).split("/"))
        try:
            return scan_region(path, dirty_since, previous, set_root=self.root)
        except FileNotFoundError as exc:
            raise SourceVanished(f"源 region 不存在：{rel}") from exc

    def get_records(self, rel: str, indices: Sequence[int]) -> Dict[int, Tuple[int, bytes, bytes]]:
        path = self.root.joinpath(*_as_rel(rel).split("/"))
        result: Dict[int, Tuple[int, bytes, bytes]] = {}
        with open(path, "rb") as fp:
            size = os.fstat(fp.fileno()).st_size
            header = fp.read(8192)
            slots = parse_header(header, size)
            for index in indices:
                slot = slots.get(index)
                if slot is None:
                    continue
                result[index] = read_record(fp, slot)
        return result

    def read_bytes(self, rel: str, limit: int = 8 << 20) -> Optional[bytes]:
        path = self.root.joinpath(*_as_rel(rel).split("/"))
        try:
            with open(path, "rb") as fp:
                head = fp.read(4)
                if head[:2] == b"\x1f\x8b":
                    fp.seek(0)
                    import gzip

                    with gzip.GzipFile(fileobj=fp) as gz:
                        return gz.read(limit)
                fp.seek(0)
                return fp.read(limit)
        except FileNotFoundError:
            return None


# --------------------------------------------------------------------------- #
# 跨机（agent 客户端）
# --------------------------------------------------------------------------- #


class _HttpClient:
    def __init__(self, cfg: AgentConfig, timeout: float = 30.0):
        parsed = urlparse(cfg.url or "http://127.0.0.1:25580")
        self.scheme = (parsed.scheme or "http").lower()
        if self.scheme not in ("http", "https"):
            raise SourceError(f"不支持的 agent URL：{cfg.url!r}")
        self.host = parsed.hostname or "127.0.0.1"
        self.port = parsed.port or (443 if self.scheme == "https" else 80)
        self.base = parsed.path.rstrip("/")
        self.token = cfg.token or ""
        self.pin = (cfg.tls_pin_sha256 or "").strip().lower()
        self.timeout = timeout

    def _connection(self) -> http.client.HTTPConnection:
        if self.scheme == "https":
            context = ssl.create_default_context()
            if self.pin:
                # 自签证书场景：只能靠指纹固定（TOFU），关掉 CA 校验但保留指纹比对
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
            return http.client.HTTPSConnection(
                self.host, self.port, timeout=self.timeout, context=context
            )
        return http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)

    def _verify_pin(self, conn: http.client.HTTPConnection) -> None:
        if not self.pin or self.scheme != "https":
            return
        sock = getattr(conn, "sock", None)
        if sock is None:
            return
        der = sock.getpeercert(binary_form=True) or b""
        actual = hashlib.sha256(der).hexdigest()
        if not hmac.compare_digest(actual, self.pin):
            raise SourceUnavailable(
                f"agent 证书指纹不匹配（期望 {self.pin[:16]}…，实际 {actual[:16]}…）"
            )

    def _headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        headers = {"Accept": "application/octet-stream"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if extra:
            headers.update(extra)
        return headers

    @contextmanager
    def stream(self, path: str, params: Optional[Dict[str, str]] = None) -> Iterator[http.client.HTTPResponse]:
        url = path if not params else f"{path}?{urlencode(params)}"
        conn = self._connection()
        try:
            conn.request("GET", self.base + url, headers=self._headers())
            self._verify_pin(conn)
            resp = conn.getresponse()
            try:
                yield resp
            finally:
                resp.close()
        except (OSError, http.client.HTTPException) as exc:
            raise SourceUnavailable(f"无法连接 agent {self.host}:{self.port}：{exc}") from exc
        finally:
            conn.close()

    def get_with_status(
        self, path: str, params: Optional[Dict[str, str]] = None, body: Optional[bytes] = None
    ) -> Tuple[int, bytes]:
        """和 :meth:`get` 一样，但把状态码交给调用方判断（401 仍然直接抛）。"""
        method = "POST" if body is not None else "GET"
        url = path if not params else f"{path}?{urlencode(params)}"
        conn = self._connection()
        extra = {"Content-Type": "application/octet-stream"} if body is not None else None
        try:
            conn.request(method, self.base + url, body=body, headers=self._headers(extra))
            self._verify_pin(conn)
            resp = conn.getresponse()
            data = resp.read()
            status = resp.status
        except (OSError, http.client.HTTPException) as exc:
            raise SourceUnavailable(f"无法连接 agent {self.host}:{self.port}：{exc}") from exc
        finally:
            conn.close()

        if status == 401:
            raise SourceUnavailable("agent 鉴权失败（token 不匹配）")
        return status, data

    def get(
        self, path: str, params: Optional[Dict[str, str]] = None, body: Optional[bytes] = None
    ) -> bytes:
        status, data = self.get_with_status(path, params, body)
        if status == 404:
            raise SourceVanished(f"agent 上没有这个文件：{params.get('path') if params else path}")
        if status != 200:
            message = _error_message(data) or f"HTTP {status}"
            raise SourceError(f"agent 返回错误：{message}")
        return data


def _error_message(data: bytes) -> str:
    try:
        payload = json.loads(data.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return ""
    if isinstance(payload, dict):
        return str(payload.get("message") or payload.get("error") or "")
    return ""


class AgentSource(Source):
    is_remote = True

    def __init__(self, agent: AgentConfig, set_name: str, timeout: float = 30.0):
        self.client = _HttpClient(agent, timeout=timeout)
        self.set_name = set_name
        self.label = f"agent:{agent.url}#{set_name}"
        self._record_cache: Dict[Tuple[str, Tuple[int, ...]], Dict[int, Tuple[int, bytes, bytes]]] = {}

    # -- 元数据 --------------------------------------------------------- #

    def scan(self, matcher: GlobMatcher) -> ScanResult:
        data = self.client.get("/v1/manifest", {"set": self.set_name})
        try:
            entries = decode_manifest(data)
        except ValueError as exc:
            raise SourceError(f"agent manifest 解析失败：{exc}") from exc
        return ScanResult(entries=entries, errors=[], pruned_dirs=0, root_missing=False)

    def stat(self, rel: str) -> Optional[Tuple[int, int]]:
        try:
            data = self.client.get("/v1/stat", {"set": self.set_name, "path": _as_rel(rel)})
        except SourceVanished:
            return None
        try:
            payload = json.loads(data.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise SourceError(f"agent stat 返回非法 JSON：{exc}") from exc
        return int(payload["size"]), int(payload["mtime_ns"])

    def read_bytes(self, rel: str, limit: int = 8 << 20) -> Optional[bytes]:
        try:
            with self.client.stream(
                "/v1/file", {"set": self.set_name, "path": _as_rel(rel)}
            ) as resp:
                if resp.status == 404:
                    return None
                if resp.status != 200:
                    return None
                return resp.read(limit)
        except SourceVanished:
            return None

    def digest(self, rel: str) -> Optional[Tuple[bytes, int, int]]:
        rel = _as_rel(rel)
        try:
            data = self.client.get("/v1/hash", {"set": self.set_name, "path": rel})
        except SourceVanished:
            return None
        try:
            payload = json.loads(data.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise SourceError(f"agent hash 返回非法 JSON：{exc}") from exc
        return bytes.fromhex(payload["hash"]), int(payload["size"]), int(payload["mtime_ns"])

    # -- 文件 ----------------------------------------------------------- #

    def copy_to(self, rel: str, dst: Path) -> Tuple[bytes, int, int]:
        rel = _as_rel(rel)
        hasher = new_hasher()
        written = 0
        expected: Optional[Tuple[int, int]] = None
        dst.parent.mkdir(parents=True, exist_ok=True)
        with self.client.stream("/v1/file", {"set": self.set_name, "path": rel}) as resp:
            if resp.status == 404:
                raise SourceVanished(f"agent 上源文件不存在：{rel}")
            if resp.status != 200:
                raise SourceError(f"agent 读取 {rel} 失败：HTTP {resp.status}")
            try:
                expected = (
                    int(resp.getheader("X-MirrorUP-Size", "-1")),
                    int(resp.getheader("X-MirrorUP-Mtime-Ns", "-1")),
                )
            except (TypeError, ValueError):
                expected = None
            with open(dst, "wb") as fout:
                while True:
                    block = resp.read(IO_CHUNK)
                    if not block:
                        break
                    hasher.update(block)
                    fout.write(block)
                    written += len(block)
                fout.flush()
                os.fsync(fout.fileno())

        current = self.stat(rel)
        if current is None:
            raise SourceVanished(f"agent 上源文件在读取期间消失：{rel}")
        if expected is not None and expected != current:
            raise UnstableSource(
                f"源文件在读取期间被改写（{expected[0]}B@{expected[1]} → "
                f"{current[0]}B@{current[1]}）"
            )
        if written != current[0]:
            raise UnstableSource(f"读取字节数与源端不一致（{written} != {current[0]}）")
        return hasher.digest(), written, current[1]

    # -- region --------------------------------------------------------- #

    def scan_region(
        self, rel: str, dirty_since: Optional[int], previous: Optional[Dict[int, bytes]]
    ) -> RegionScan:
        rel = _as_rel(rel)
        previous = previous or {}
        status, data = self.client.get_with_status(
            "/v1/region",
            {
                "set": self.set_name,
                "path": rel,
                **({"since": str(int(dirty_since))} if dirty_since is not None else {}),
            },
        )
        if status == 404:
            raise SourceVanished(f"agent 上源文件不存在：{rel}")
        if status == 422:
            # 服务端也解析不了这个 region（损坏或非标准文件）：交给上层降级为整文件复制
            raise RegionFormatError(_error_message(data) or f"agent 无法解析 {rel}")
        if status != 200:
            message = _error_message(data) or f"HTTP {status}"
            raise SourceError(f"agent 返回错误：{message}")
        try:
            size, mtime_ns, header, served = decode_region_summary(data)
        except (ValueError, TypeError) as exc:
            raise SourceError(f"agent region 摘要解析失败：{exc}") from exc

        try:
            slots = parse_header(header, size)
        except Exception as exc:  # noqa: BLE001 - 协议数据不可信
            raise SourceError(f"agent 返回的 region 头非法：{exc}") from exc

        chunks: Dict[int, ChunkDelta] = {}
        dirty: set[int] = set()
        restart_needed = False
        for index, slot in slots.items():
            timestamp, digest = served.get(index, (slot.timestamp, None))
            chunk = ChunkDelta(
                index=index,
                offset=slot.offset,
                sectors=slot.sectors,
                timestamp=timestamp,
            )
            if digest is not None:
                chunk.digest = digest
                chunk.read = True
                dirty.add(index)
            else:
                chunk.digest = previous.get(index)
                if chunk.digest is None:
                    restart_needed = True
            chunks[index] = chunk

        if restart_needed:
            # 客户端没有这些 chunk 的基线：只能让服务端全部重读一次
            return self.scan_region(rel, None, {})

        return RegionScan(
            path=None,
            size=size,
            mtime_ns=mtime_ns,
            header=header,
            chunks=chunks,
            dirty=dirty,
        )

    def get_records(self, rel: str, indices: Sequence[int]) -> Dict[int, Tuple[int, bytes, bytes]]:
        rel = _as_rel(rel)
        wanted = tuple(sorted(set(int(i) for i in indices)))
        if not wanted:
            return {}
        key = (rel, wanted)
        cached = self._record_cache.get(key)
        if cached is not None:
            return cached
        data = self.client.get(
            "/v1/region/chunks",
            {"set": self.set_name, "path": rel, "idx": ",".join(str(i) for i in wanted)},
            body=b"",
        )
        try:
            records = decode_chunks(data)
        except (ValueError, TypeError) as exc:
            raise SourceError(f"agent chunk 记录解析失败：{exc}") from exc
        missing = [i for i in wanted if i not in records]
        if missing:
            raise SourceVanished(f"agent 缺少 chunk {missing[:5]}（{rel}）")
        self._record_cache[key] = records
        return records

    def close(self) -> None:
        self._record_cache.clear()


# --------------------------------------------------------------------------- #
# 工厂
# --------------------------------------------------------------------------- #


def build_source(config: Config, item: SetConfig, local_root: Path) -> Source:
    if config.source.transport == "agent":
        return AgentSource(config.source.agent, item.name)
    return LocalSource(set_source_path(config, item, local_root), item.name)


def is_region_path(rel: str) -> bool:
    return supports_delta(Path(rel).name)


__all__ = [
    "AgentSource",
    "LocalSource",
    "Source",
    "SourceError",
    "SourceUnavailable",
    "SourceVanished",
    "UnstableSource",
    "build_source",
    "decode_state",
    "is_region_path",
]
