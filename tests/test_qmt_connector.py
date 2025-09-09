"""QMTConnector 单元测试（M2.5 版）

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


def _install_fake_xtquant(listen_side_effect=None):
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

    sys.modules["xtquant"] = xtquant
    sys.modules["xtquant.xtdatacenter"] = xtdc
    sys.modules["xtquant.xtdata"] = xtdata


def _reload_qmt_connector():
    if "core.qmt_connector" in sys.modules:
        importlib.reload(sys.modules["core.qmt_connector"])
    else:
        import core.qmt_connector  # noqa


class TestQMTConnector(unittest.TestCase):
    """类说明：QMTConnector 行为测试（M2.5）
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
        _reload_qmt_connector()
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
        _reload_qmt_connector()
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
        _reload_qmt_connector()
        from core.qmt_connector import QMTConnector, QMTConfig
        conn = QMTConnector(QMTConfig(mode="legacy"))
        with self.assertRaises(RuntimeError):
            conn.listen_and_connect()

    def test_missing_xtquant_dependency(self):
        """测试内容：缺少 xtquant 依赖
        目的：验证构造函数直接报错；
        输入：移除 sys.modules 中的 xtquant 相关条目并重载模块；
        预期输出：构造 QMTConnector 时抛 RuntimeError。
        """
        for k in ["xtquant", "xtquant.xtdatacenter", "xtquant.xtdata"]:
            sys.modules.pop(k, None)
        if "core.qmt_connector" in sys.modules:
            del sys.modules["core.qmt_connector"]
        import core.qmt_connector as qc
        with self.assertRaises(RuntimeError):
            qc.QMTConnector(qc.QMTConfig())

    def test_ok_property(self):
        """测试内容：ok 状态位
        目的：验证在 mode='none' 下连接后 ok=True；
        输入：正常路径；
        预期输出：ok 从 False → True。
        """
        _install_fake_xtquant()
        _reload_qmt_connector()
        from core.qmt_connector import QMTConnector, QMTConfig
        conn = QMTConnector(QMTConfig(mode="none"))
        self.assertFalse(conn.ok)
        conn.listen_and_connect()
        self.assertTrue(conn.ok)