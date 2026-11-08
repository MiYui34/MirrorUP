"""排除规则匹配。

配置里是一组 glob，匹配的是**相对 set 根的 posix 路径**。支持的语法刻意很小：

- ``*``      匹配路径段内任意字符（不跨 ``/``）
- ``?``      匹配路径段内单个字符
- ``**``     匹配任意多层目录；``**/x`` 也能匹配顶层的 ``x``
- ``dir/**`` 匹配 ``dir`` 下的所有内容，也匹配 ``dir`` 自身（用于剪枝）

不引入 ``pathlib.PurePath.match``：它在 ``**`` 上的语义随版本变化，且不匹配
``**/foo`` 与顶层 ``foo``，容易写出“看着对但漏一条”的规则。
"""

from __future__ import annotations

import re
from typing import Iterable, List, Pattern


def _translate(pattern: str) -> str:
    out: List[str] = []
    i = 0
    n = len(pattern)
    while i < n:
        ch = pattern[i]
        if ch == "*":
            if pattern.startswith("**", i):
                # '**/' 允许零层目录，因此 '**/session.lock' 能匹配 'session.lock'
                if i + 2 < n and pattern[i + 2] == "/":
                    out.append("(?:[^/]+/)*")
                    i += 3
                    continue
                out.append(".*")
                i += 2
                continue
            out.append("[^/]*")
            i += 1
        elif ch == "?":
            out.append("[^/]")
            i += 1
        elif ch == "/":
            out.append("/")
            i += 1
        else:
            out.append(re.escape(ch))
            i += 1
    return "".join(out)


class GlobMatcher:
    """把一组 glob 编译成正则；空规则永不匹配。"""

    def __init__(self, patterns: Iterable[str]):
        self.patterns = [p.strip() for p in patterns if p and p.strip()]
        self._regexes: List[Pattern[str]] = []
        for raw in self.patterns:
            pattern = raw.replace("\\", "/")
            while pattern.startswith("./"):
                pattern = pattern[2:]
            pattern = pattern.lstrip("/")
            if not pattern:
                continue
            if pattern.endswith("/"):
                pattern += "**"
            self._regexes.append(re.compile("^" + _translate(pattern) + "$"))

    def __bool__(self) -> bool:
        return bool(self._regexes)

    def match(self, rel_path: str) -> bool:
        if not self._regexes:
            return False
        path = rel_path.replace("\\", "/")
        return any(rx.match(path) is not None for rx in self._regexes)

    def match_dir(self, rel_path: str) -> bool:
        """目录是否命中。

        ``logs/**`` 这类规则要能剪掉整个目录，所以额外用 ``logs/`` 试一次：
        正则 ``logs/.*`` 对 ``logs/`` 是能匹配的（``.*`` 可以为空）。
        """
        if not self._regexes:
            return False
        path = rel_path.replace("\\", "/")
        probe = path + "/"
        return any(
            rx.match(path) is not None or rx.match(probe) is not None
            for rx in self._regexes
        )
