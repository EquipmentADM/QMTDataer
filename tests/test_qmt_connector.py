"""QMTConnector 单元测试（M2.5 版，修订）

修订点：
    - 为避免真实环境已安装 xtquant 导致“缺依赖”用例失效，加入 import 拦截（patch builtins.__import__）；
    - 为避免子模块属性缺失，给顶层 xtquant 绑定 xtdatacenter/xtdata 属性；
    - 为避免模块缓存污染，reload 改为先从 sys.modules 移除后再 import。

类说明：
    - 覆盖 mode='none' 跳过 listen、legacy 成功、legacy 失败、依赖缺失、ok 状态；
    - 上游：无；
    - 下游：被测对象 core.qmt_connector.QMTConnector。

注意：通过注入假 xtquant/xtdatacenter/xtdata 控制行为。
"""
import sys
import types
import importlib
import unittest
from unittest import mock


def _install_fake_xtquant(listen_side_effect=None):
    """安装假的 xtquant 包，并将子模块绑定到顶层属性。"""
    xtquant = types.ModuleType("xtquant")
    xtdc = types.ModuleType("xtquant.xtdatacenter")
    xtdata = types.ModuleType("xtquant.xtdata")

    def set_token(_):
        return None
    def init():
        return None
    def listen(port: int):
        if listen_side_effect:
            return listen_side_effect(port)
        return None

    xtdc.set_token = set_token
    xtdc.init = init
    xtdc.listen = listen

    # 绑定到顶层属性，避免 AttributeError: module 'xtquant' has no attribute 'xtdata'
    xtquant.xtdatacenter = xtdc
    xtquant.xtdata = xtdata

    sys.modules["xtquant"] = xtquant
    sys.modules["xtquant.xtdatacenter"] = xtdc
    sys.modules["xtquant.xtdata"] = xtdata


def _reload_qmt_connector_fresh():
    """从 sys.modules 移除后再导入，确保按当前注入的 xtquant 重新求值。"""
    sys.modules.pop("core.qmt_connector", None)
    import core.qmt_connector  # noqa


class TestQMTConnector(unittest.TestCase):
    """类说明：QMTConnector 行为测试（M2.5 修订）
    功能：mode 分支、异常路径与 ok 状态。
    上游：无。
    下游：QMTConnector。
    """

    def test_mode_none_skip_listen(self):
        """测试内容：mode='none' 跳过 listen
        目的：不发生 listen 调用但状态为连接成功；
        输入：注入假 xtdatacenter，未设置副作用；
        预期输出：connector.ok == True。
        """
        _install_fake_xtquant()
        _reload_qmt_connector_fresh()
        from core.qmt_connector import QMTConnector, QMTConfig
        conn = QMTConnector(QMTConfig(mode="none"))
        conn.listen_and_connect()
        self.assertTrue(conn.ok)

    def test_legacy_listen_success(self):
        """测试内容：legacy 模式 listen 成功
        目的：验证 legacy 分支可调用 listen 并成功；
        输入：listen 正常返回；
        预期输出：ok=True。
        """
        called = {"n": 0}
        def side_effect(port):
            called["n"] += 1
            self.assertIsInstance(port, int)
            return None
        _install_fake_xtquant(listen_side_effect=side_effect)
        _reload_qmt_connector_fresh()
        from core.qmt_connector import QMTConnector, QMTConfig
        conn = QMTConnector(QMTConfig(mode="legacy"))
        conn.listen_and_connect()
        self.assertTrue(conn.ok)
        self.assertEqual(called["n"], 1)

    def test_legacy_listen_fail(self):
        """测试内容：legacy 模式 listen 抛异常
        目的：触发异常路径；
        输入：listen 抛出 RuntimeError；
        预期输出：listen_and_connect 抛 RuntimeError。
        """
        def side_effect(_):
            raise RuntimeError("server only support xt user mode")
        _install_fake_xtquant(listen_side_effect=side_effect)
        _reload_qmt_connector_fresh()
        from core.qmt_connector import QMTConnector, QMTConfig
        conn = QMTConnector(QMTConfig(mode="legacy"))
        with self.assertRaises(RuntimeError):
            conn.listen_and_connect()

    def test_missing_xtquant_dependency(self):
        """测试内容：缺少 xtquant 依赖
        目的：验证构造函数在导入失败路径抛异常；
        输入：用 patch 拦截内建导入，使得任何 'xtquant*' 的导入抛 ImportError；
        预期输出：构造 QMTConnector 时抛 RuntimeError。
        """
        # 清理缓存，确保模块重新导入
        for k in ["xtquant", "xtquant.xtdatacenter", "xtquant.xtdata", "core.qmt_connector"]:
            sys.modules.pop(k, None)
        real_import = __import__
        def blocked(name, *a, **kw):
            if name.startswith("xtquant"):
                raise ImportError("No module named 'xtquant' (blocked by test)")
            return real_import(name, *a, **kw)
        with mock.patch("builtins.__import__", side_effect=blocked):
            import importlib
            qc = importlib.import_module("core.qmt_connector")
            with self.assertRaises(RuntimeError):
                qc.QMTConnector(qc.QMTConfig())

    def test_ok_property(self):
        """测试内容：ok 状态位
        目的：验证在 mode='none' 下连接后 ok=True；
        输入：正常路径；
        预期输出：ok 从 False → True。
        """
        _install_fake_xtquant()
        _reload_qmt_connector_fresh()
        from core.qmt_connector import QMTConnector, QMTConfig
        conn = QMTConnector(QMTConfig(mode="none"))
        self.assertFalse(conn.ok)
        conn.listen_and_connect()
        self.assertTrue(conn.ok)
