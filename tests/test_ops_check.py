# -*- coding: utf-8 -*-
"""ops_check 脚本测试（M3.2）

说明：注入假模块 redis 与 xtquant.xtdata，验证返回码。
"""
import sys
import types
import unittest


def _install_fake_redis(ok=True):
    r = types.ModuleType("redis")
    class R:
        def __init__(self, *a, **kw): pass
        def ping(self):
            if not ok:
                raise RuntimeError("ping fail")
    r.Redis = R
    sys.modules["redis"] = r


def _install_fake_xtquant(present=True):
    if present:
        pkg = types.ModuleType("xtquant")
        xtdata = types.ModuleType("xtquant.xtdata")
        pkg.xtdata = xtdata
        sys.modules["xtquant"] = pkg
        sys.modules["xtquant.xtdata"] = xtdata
    else:
        for k in ["xtquant", "xtquant.xtdata"]:
            sys.modules.pop(k, None)


class TestOpsCheck(unittest.TestCase):
    """类说明：环境自检脚本测试"""

    def test_success_path(self):
        """测试内容：依赖均正常
        目的：返回码应为 0
        输入：注入可用的 xtquant.xtdata 与 redis.Redis.ping
        预期输出：main() 返回 0
        """
        _install_fake_xtquant(True)
        _install_fake_redis(True)
        import importlib
        mod = importlib.import_module("scripts.ops_check")
        rc = mod.main()
        self.assertEqual(rc, 0)

    def test_fail_path(self):
        """测试内容：依赖异常
        目的：返回码应为 2
        输入：移除 xtquant 并让 redis.ping 抛异常
        预期输出：main() 返回 2
        """
        _install_fake_xtquant(False)
        _install_fake_redis(False)
        import importlib
        # 重新加载脚本模块，确保使用新的伪依赖
        sys.modules.pop("scripts.ops_check", None)
        mod = importlib.import_module("scripts.ops_check")
        rc = mod.main()
        self.assertEqual(rc, 2)