"""只读 agent 服务端（跨机同步的 exporter 侧）。

把它跑在**源服**旁边，driver 侧的 MirrorUP 就能用 ``source.transport = "agent"``
读到 manifest / 文件 / region 摘要 —— 所有"哪些变了"的判断仍然在 driver 本地做，
agent 只负责按请求把字节发出去。

设计约束（对应 DESIGN §12.4）：

* 只读：没有任何写/删除端点；
* 每个请求都要 ``Authorization: Bearer <token>``；
* 路径必须落在配置里登记过的 set 根内，``..``、绝对路径、符号链接逃逸一律拒绝；
* 默认只监听 ``127.0.0.1``，跨机请自己改 ``agent_server.host`` 并配 TLS。

可以单独跑（``python -m mirror_up.agent --data-dir ...``），也可以在 MCDR 里以
``role = "exporter"`` 随插件启动。
"""

from __future__ import annotations

import argparse
import hmac
import json
import logging
import os
import ssl
import sys
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import parse_qs, urlparse

from .config import Config, load_config, set_source_path
from .constants import APP_NAME, VERSION, IO_CHUNK
from .excludes import GlobMatcher
from .leveldat import read_level_info
from .region import RegionExternalChunk, RegionFormatError, parse_header, read_record, scan_region
from .scanner import scan_tree
from .wire import encode_chunks, encode_manifest, encode_region_summary

logger = logging.getLogger("mirror_up.agent")

PROTOCOL_VERSION = 1
MAX_RECORD_REQUEST = 1024


class AgentError(Exception):
    def __init__(self, status: HTTPStatus, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


def _normalize_rel(rel: str) -> str:
    rel = (rel or "").replace("\\", "/").strip("/")
    if rel.startswith("/") or ".." in rel.split("/"):
        raise AgentError(HTTPStatus.FORBIDDEN, f"非法路径：{rel!r}")
    return rel


class AgentServer:
    """配置 + 生命周期；请求处理在 :class:`_Handler` 里。"""

    def __init__(
        self,
        config: Config,
        base_dir: Path,
        host: Optional[str] = None,
        port: Optional[int] = None,
        token: Optional[str] = None,
    ):
        self.config = config
        self.base_dir = Path(base_dir)
        server_cfg = config.agent_server
        self.host = host if host is not None else server_cfg.host
        self.port = port if port is not None else server_cfg.port
        self.token = token if token is not None else server_cfg.token
        self.tls_enabled = bool(server_cfg.tls.enabled)
        self.tls_cert = server_cfg.tls.cert
        self.tls_key = server_cfg.tls.key
        self.matcher = GlobMatcher(config.exclude)
        self.roots: Dict[str, Path] = {
            item.name: set_source_path(config, item, self.base_dir)
            for item in config.enabled_sets()
        }
        self._httpd: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    # -- 校验 ----------------------------------------------------------- #

    def validate(self) -> None:
        if len(self.token or "") < 16:
            raise AgentError(
                HTTPStatus.UNAUTHORIZED,
                "agent_server.token 至少要 16 个字符（建议 32 字节随机串），否则拒绝对外服务",
            )
        if not self.roots:
            raise AgentError(HTTPStatus.INTERNAL_SERVER_ERROR, "没有启用的 set，agent 无内容可服务")
        if self.tls_enabled:
            if not self.tls_cert or not self.tls_key:
                raise AgentError(
                    HTTPStatus.INTERNAL_SERVER_ERROR, "agent_server.tls 需要同时提供 cert 与 key"
                )
            for path in (self.tls_cert, self.tls_key):
                if not Path(path).is_file():
                    raise AgentError(
                        HTTPStatus.INTERNAL_SERVER_ERROR, f"TLS 文件不存在：{path}"
                    )

    # -- 资源解析 ------------------------------------------------------- #

    def root_for(self, set_name: str) -> Path:
        root = self.roots.get(set_name or "")
        if root is None:
            raise AgentError(HTTPStatus.NOT_FOUND, f"未知的 set：{set_name!r}")
        return root

    def resolve(self, set_name: str, rel: str) -> Path:
        root = self.root_for(set_name)
        rel = _normalize_rel(rel)
        target = root.joinpath(*rel.split("/")) if rel else root
        try:
            resolved = target.resolve()
            root_resolved = root.resolve()
        except OSError as exc:
            raise AgentError(HTTPStatus.NOT_FOUND, f"路径不可解析：{exc}") from exc
        if resolved != root_resolved and root_resolved not in resolved.parents:
            raise AgentError(HTTPStatus.FORBIDDEN, "路径逃出了 set 根目录")
        return resolved

    # -- 生命周期 ------------------------------------------------------- #

    def start(self, background: bool = True) -> None:
        self.validate()
        handler = _make_handler(self)
        httpd = _QuietServer((self.host, self.port), handler)
        httpd.daemon_threads = True
        if self.tls_enabled:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(self.tls_cert, self.tls_key)
            httpd.socket = context.wrap_socket(httpd.socket, server_side=True)
        self._httpd = httpd
        self.port = httpd.server_address[1]
        scheme = "https" if self.tls_enabled else "http"
        logger.info("[MirrorUP] agent 已监听 %s://%s:%d（%d 个 set）", scheme, self.host, self.port, len(self.roots))
        if background:
            self._thread = threading.Thread(
                target=httpd.serve_forever, name="mirrorup-agent", daemon=True
            )
            self._thread.start()

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    @property
    def url(self) -> str:
        scheme = "https" if self.tls_enabled else "http"
        host = "127.0.0.1" if self.host in ("0.0.0.0", "") else self.host
        return f"{scheme}://{host}:{self.port}"


class _QuietServer(ThreadingHTTPServer):
    """客户端提前断开是常态（driver 取消、进程退出），不要刷 traceback。"""

    daemon_threads = True

    def handle_error(self, request, client_address) -> None:  # type: ignore[no-untyped-def]
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
            logger.debug("[agent] 客户端断开：%s", client_address)
            return
        super().handle_error(request, client_address)


def _make_handler(server: AgentServer):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        server_ctx = server

        # -- 基础 ----------------------------------------------------- #

        def log_message(self, fmt: str, *args) -> None:  # noqa: A003 - 覆盖标准库
            logger.debug("[agent] %s - %s", self.address_string(), fmt % args)

        def _check_auth(self) -> None:
            expected = self.server_ctx.token
            header = self.headers.get("Authorization", "")
            if not header.startswith("Bearer "):
                raise AgentError(HTTPStatus.UNAUTHORIZED, "缺少 Bearer token")
            provided = header[len("Bearer ") :].strip()
            if not hmac.compare_digest(provided, expected):
                raise AgentError(HTTPStatus.UNAUTHORIZED, "token 不正确")

        def _query(self) -> Dict[str, str]:
            parsed = urlparse(self.path)
            raw = parse_qs(parsed.query, keep_blank_values=True)
            return {key: values[-1] for key, values in raw.items()}

        def _send_bytes(self, data: bytes, status: HTTPStatus = HTTPStatus.OK, content_type: str = "application/octet-stream") -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _send_error(self, exc: AgentError) -> None:
            payload = json.dumps({"error": exc.message, "status": int(exc.status)}).encode("utf-8")
            try:
                self._send_bytes(payload, exc.status, "application/json; charset=utf-8")
            except (BrokenPipeError, ConnectionResetError):
                pass

        def _dispatch(self) -> None:
            try:
                self._check_auth()
                parsed = urlparse(self.path)
                route = parsed.path.rstrip("/") or "/"
                handlers = {
                    "/v1/info": self._info,
                    "/v1/manifest": self._manifest,
                    "/v1/file": self._file,
                    "/v1/stat": self._stat,
                    "/v1/region": self._region,
                    "/v1/region/chunks": self._chunks,
                }
                handler = handlers.get(route)
                if handler is None:
                    raise AgentError(HTTPStatus.NOT_FOUND, f"未知端点：{route}")
                handler(self._query())
            except AgentError as exc:
                self._send_error(exc)
            except RegionFormatError as exc:
                self._send_error(AgentError(HTTPStatus.UNPROCESSABLE_ENTITY, f"region 无法解析：{exc}"))
            except (BrokenPipeError, ConnectionResetError):
                pass
            except Exception as exc:  # 兜底：不要让一个请求拖垮服务
                logger.exception("[agent] 处理请求失败：%s", self.path)
                self._send_error(AgentError(HTTPStatus.INTERNAL_SERVER_ERROR, f"{type(exc).__name__}: {exc}"))

        def do_GET(self) -> None:  # noqa: N802 - 标准库约定
            self._dispatch()

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length") or 0)
            if length:
                self.rfile.read(length)
            self._dispatch()

        # -- 端点 ----------------------------------------------------- #

        def _info(self, query: Dict[str, str]) -> None:
            sets = []
            for name, root in self.server_ctx.roots.items():
                info = read_level_info(root / "level.dat")
                sets.append(
                    {
                        "name": name,
                        "exists": root.is_dir(),
                        "mc_version": info.version_name if info else None,
                    }
                )
            payload = {
                "app": APP_NAME,
                "version": VERSION,
                "protocol": PROTOCOL_VERSION,
                "sets": sets,
            }
            self._send_bytes(
                json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                content_type="application/json; charset=utf-8",
            )

        def _manifest(self, query: Dict[str, str]) -> None:
            name = query.get("set", "")
            root = self.server_ctx.resolve(name, "")
            if not root.is_dir():
                raise AgentError(HTTPStatus.NOT_FOUND, f"set {name!r} 的目录不存在")
            scan = scan_tree(root, self.server_ctx.matcher)
            if scan.root_missing:
                raise AgentError(HTTPStatus.NOT_FOUND, f"set {name!r} 的目录不可读")
            self._send_bytes(encode_manifest(scan.entries.values()))

        def _stat(self, query: Dict[str, str]) -> None:
            name = query.get("set", "")
            rel = query.get("path", "")
            path = self.server_ctx.resolve(name, rel)
            try:
                st = os.stat(path)
            except FileNotFoundError as exc:
                raise AgentError(HTTPStatus.NOT_FOUND, f"文件不存在：{rel}") from exc
            payload = {
                "size": 0 if path.is_dir() else st.st_size,
                "mtime_ns": st.st_mtime_ns,
                "is_dir": path.is_dir(),
            }
            self._send_bytes(
                json.dumps(payload).encode("utf-8"),
                content_type="application/json; charset=utf-8",
            )

        def _file(self, query: Dict[str, str]) -> None:
            name = query.get("set", "")
            rel = query.get("path", "")
            path = self.server_ctx.resolve(name, rel)
            try:
                fp = open(path, "rb")
            except FileNotFoundError as exc:
                raise AgentError(HTTPStatus.NOT_FOUND, f"文件不存在：{rel}") from exc
            except OSError as exc:
                raise AgentError(HTTPStatus.FORBIDDEN, f"无法读取：{exc}") from exc
            with fp:
                st = os.fstat(fp.fileno())
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(st.st_size))
                self.send_header("X-MirrorUP-Size", str(st.st_size))
                self.send_header("X-MirrorUP-Mtime-Ns", str(st.st_mtime_ns))
                self.end_headers()
                while True:
                    block = fp.read(IO_CHUNK)
                    if not block:
                        break
                    self.wfile.write(block)

        def _region(self, query: Dict[str, str]) -> None:
            name = query.get("set", "")
            rel = query.get("path", "")
            root = self.server_ctx.root_for(name)
            path = self.server_ctx.resolve(name, rel)
            since_raw = query.get("since")
            try:
                since = int(since_raw) if since_raw not in (None, "") else None
            except ValueError as exc:
                raise AgentError(HTTPStatus.BAD_REQUEST, f"since 必须是整数：{since_raw!r}") from exc
            try:
                scan = scan_region(path, since, None, set_root=root)
            except FileNotFoundError as exc:
                raise AgentError(HTTPStatus.NOT_FOUND, f"region 不存在：{rel}") from exc
            chunks = [
                (chunk.index, chunk.timestamp, chunk.digest) for chunk in scan.chunks.values()
            ]
            self._send_bytes(encode_region_summary(scan.size, scan.mtime_ns, scan.header, chunks))

        def _chunks(self, query: Dict[str, str]) -> None:
            name = query.get("set", "")
            rel = query.get("path", "")
            raw = query.get("idx", "")
            try:
                indices = sorted({int(part) for part in raw.split(",") if part.strip()})
            except ValueError as exc:
                raise AgentError(HTTPStatus.BAD_REQUEST, f"idx 必须是逗号分隔的整数：{raw!r}") from exc
            if not indices:
                raise AgentError(HTTPStatus.BAD_REQUEST, "idx 不能为空")
            if len(indices) > MAX_RECORD_REQUEST:
                raise AgentError(HTTPStatus.BAD_REQUEST, f"一次最多请求 {MAX_RECORD_REQUEST} 个 chunk")
            if any(index < 0 or index >= 1024 for index in indices):
                raise AgentError(HTTPStatus.BAD_REQUEST, "chunk 下标必须在 0..1023")
            path = self.server_ctx.resolve(name, rel)
            try:
                with open(path, "rb") as fp:
                    size = os.fstat(fp.fileno()).st_size
                    slots = parse_header(fp.read(8192), size)
                    records = {}
                    for index in indices:
                        slot = slots.get(index)
                        if slot is None:
                            continue
                        records[index] = read_record(fp, slot)
            except FileNotFoundError as exc:
                raise AgentError(HTTPStatus.NOT_FOUND, f"region 不存在：{rel}") from exc
            self._send_bytes(encode_chunks(records))

    return Handler


# --------------------------------------------------------------------------- #
# 独立运行
# --------------------------------------------------------------------------- #


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="MirrorUP exporter（只读 agent）")
    parser.add_argument("--data-dir", required=True, help="包含 config.json 的 MCDR 数据目录")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--token", default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config, warnings = load_config(Path(args.data_dir))
    for warning in warnings:
        logger.warning("[MirrorUP] %s", warning)
    server = AgentServer(config, Path(os.getcwd()), args.host, args.port, args.token)
    try:
        server.start(background=False)
    except AgentError as exc:
        logger.error("[MirrorUP] agent 启动失败：%s", exc.message)
        return 2
    except KeyboardInterrupt:
        return 0
    finally:
        server.stop()
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
