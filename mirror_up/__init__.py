"""MirrorUP —— 增量镜像同步插件。

本包内的核心模块（config / index / scanner / planner / applier / engine）
不依赖 MCDR，可脱离服务器单独测试；只有 ``entry`` 与 ``commands`` 依赖 MCDR，
所以这里用模块级 ``__getattr__`` 延迟导入，保证 ``import mirror_up.config``
不会把 MCDR 一起拉进来。

MCDR 把「目录插件里与插件 id 同名的包」当作入口模块：插件 id 必须是
``mirror_up``，``on_load`` / ``on_unload`` 从这个包导出。插件目录根部
**不要**放 ``__init__.py``——那会让 MCDR 把整个目录当成普通 Python 包，
从而认不出来（见 ``plugin_factory.__get_plugin_class_from_path``）。
"""

__version__ = "0.5.0"

__all__ = ["__version__", "on_load", "on_unload"]


def __getattr__(name: str):
    if name in ("on_load", "on_unload"):
        from . import entry

        return getattr(entry, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
