"""最小 RCON 客户端（只为"让镜像服停/启"服务）。

协议（Source RCON，TCP）：

    包体 = int32 length | int32 request_id | int32 type | payload | 0x00 0x00
    type 3 = 登录，2 = 执行命令，0 = 命令回包

只实现登录 + 单条命令。多包回包用"短超时当结束符"处理——RCON 没有显式的
结束标记，这是所有实现的通行做法。
"""

from __future__ import annotations

import socket
import struct
from typing import Optional

SERVERDATA_AUTH = 3
SERVERDATA_AUTH_RESPONSE = 2
SERVERDATA_EXECCOMMAND = 2
SERVERDATA_RESPONSE_VALUE = 0


class RconError(Exception):
    pass


class RconClient:
    def __init__(self, host: str, port: int, password: str, timeout: float = 5.0):
        self.host = host
        self.port = int(port)
        self.password = password or ""
        self.timeout = timeout
        self._sock: Optional[socket.socket] = None
        self._request_id = 0

    # -- 连接 ----------------------------------------------------------- #

    def connect(self) -> None:
        try:
            self._sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        except OSError as exc:
            raise RconError(f"连接 RCON {self.host}:{self.port} 失败：{exc}") from exc
        self._sock.settimeout(self.timeout)
        self._authenticate()

    def _authenticate(self) -> None:
        self._request_id += 1
        request_id = self._request_id
        self._send(request_id, SERVERDATA_AUTH, self.password.encode("utf-8"))
        response_id, kind, _payload = self._read_packet()
        if kind == SERVERDATA_AUTH_RESPONSE and response_id == -1:
            raise RconError("RCON 密码错误")
        # 有的服务端会先发一个空的 RESPONSE_VALUE，再发 AUTH_RESPONSE
        if kind != SERVERDATA_AUTH_RESPONSE:
            response_id, kind, _payload = self._read_packet()
            if kind != SERVERDATA_AUTH_RESPONSE or response_id == -1:
                raise RconError("RCON 登录被拒绝")

    def __enter__(self) -> "RconClient":
        self.connect()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None

    # -- 命令 ----------------------------------------------------------- #

    def command(self, text: str) -> str:
        if self._sock is None:
            self.connect()
        self._request_id += 1
        request_id = self._request_id
        self._send(request_id, SERVERDATA_EXECCOMMAND, (text or "").encode("utf-8"))
        chunks = []
        self._sock.settimeout(self.timeout)  # type: ignore[union-attr]
        first = True
        while True:
            try:
                response_id, kind, payload = self._read_packet()
            except socket.timeout:
                if first:
                    raise RconError(f"RCON 命令无响应：{text!r}") from None
                break
            except RconError:
                if first:
                    raise
                break
            except OSError:
                break
            first = False
            if kind == SERVERDATA_RESPONSE_VALUE:
                chunks.append(payload)
            self._sock.settimeout(0.3)  # type: ignore[union-attr]
        return b"".join(chunks).decode("utf-8", errors="replace")

    # -- 收发 ----------------------------------------------------------- #

    def _send(self, request_id: int, kind: int, payload: bytes) -> None:
        body = struct.pack("<ii", request_id, kind) + payload + b"\x00\x00"
        packet = struct.pack("<i", len(body)) + body
        try:
            self._sock.sendall(packet)  # type: ignore[union-attr]
        except OSError as exc:
            raise RconError(f"发送 RCON 包失败：{exc}") from exc

    def _read_packet(self) -> tuple[int, int, bytes]:
        raw_length = self._recv_exact(4)
        (length,) = struct.unpack("<i", raw_length)
        if length < 10 or length > 1 << 20:
            raise RconError(f"RCON 包长度非法：{length}")
        data = self._recv_exact(length)
        request_id, kind = struct.unpack_from("<ii", data, 0)
        payload = data[8:-2] if len(data) >= 10 else b""
        return request_id, kind, payload

    def _recv_exact(self, count: int) -> bytes:
        if self._sock is None:
            raise RconError("RCON 未连接")
        chunks = []
        remaining = count
        while remaining > 0:
            block = self._sock.recv(remaining)
            if not block:
                raise RconError("RCON 连接被关闭")
            chunks.append(block)
            remaining -= len(block)
        return b"".join(chunks)
