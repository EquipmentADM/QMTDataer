"""QMTConnector 单元测试

类说明：
    - 覆盖核心连接流程、重试、依赖缺失与状态位。
    - 上游：无；
    - 下游：被测对象 core.qmt_connector.QMTConnector。

使用说明：
    - 通过在导入前向 sys.modules 注入假 xtquant/xtdatacenter/xtdata，
      控制不同分支行为；
    - 不连接真实 QMT。
"""
import sys
import types
import importlib
import unittest
from unittest import mock


def _install_fake_xtquant(listen_ok: bool = True, first_fail: bool = False):
    """安装假的 xtquant/xtdatacenter/xtdata 到 sys.modules。
    :param listen_ok: 最终 listen 是否成功。
    :param first_fail: 第一次 listen 抛异常，之后成功。
    """
    xtquant = types.ModuleType("xtquant")
    xtdc = types.ModuleType("xtquant.xtdatacenter")
    xtdata = types.ModuleType("xtquant.xtdata")

    def set_token(_):
        return None

    def init():
        return None

    state = {"count": 0}

    def listen(port: int):
        state["count"] += 1
        if first_fail and state["count"] == 1:
            raise RuntimeError("port in use")
        if not listen_ok:
            raise RuntimeError("listen failed")
        return None

    def get_server_config():
        return {"ok": True}

    def run():
        return None

    xtdc.set_token = set_token
    xtdc.init = init
    xtdc.listen = listen
    xtdata.get_server_config = get_server_config
    xtdata.run = run

    sys.modules["xtquant"] = xtquant
    sys.modules["xtquant.xtdatacenter"] = xtdc
    sys.modules["xtquant.xtdata"] = xtdata


def _reload_qmt_connector():
    if "core.qmt_connector" in sys.modules:
        importlib.reload(sys.modules["core.qmt_connector"])
    else:
        import core.qmt_connector  # noqa


class TestQMTConnector(unittest.TestCase):
    """类说明：QMTConnector 行为测试
    功能：验证连接成功、失败重试、依赖缺失以及 ok 状态位。
    上游：无。
    下游：QMTConnector。
    """

    def test_connect_success(self):
        """测试内容：正常连接路径
        目的：确保 listen_and_connect 能够设置连接状态为 True。
        输入：fake xtdc/xtdata，listen 正常，get_server_config 正常。
        预期输出：connector.ok == True，且不抛异常。
        """
        _install_fake_xtquant(listen_ok=True)
        _reload_qmt_connector()
        from core.qmt_connector import QMTConnector, QMTConfig
        conn = QMTConnector(QMTConfig(token="T"))
        conn.listen_and_connect()
        self.assertTrue(conn.ok)

    def test_connect_retry_once_then_success(self):
        """测试内容：首次监听失败，随后重试成功
        目的：验证退避重试逻辑生效。
        输入：listen 第一次抛异常，第二次成功。
        预期输出：最终连接成功，ok=True。
        """
        _install_fake_xtquant(listen_ok=True, first_fail=True)
        _reload_qmt_connector()
        from core.qmt_connector import QMTConnector, QMTConfig
        conn = QMTConnector(QMTConfig(token="T"))
        conn.listen_and_connect()
        self.assertTrue(conn.ok)

    def test_missing_xtquant_dependency(self):
        """测试内容：缺少 xtquant 依赖
        目的：构造依赖缺失的报错路径。
        输入：移除 sys.modules 中的 xtquant 相关条目。
        预期输出：构造 QMTConnector 时抛 RuntimeError。
        """
        for k in ["xtquant", "xtquant.xtdatacenter", "xtquant.xtdata"]:
            sys.modules.pop(k, None)
        if "core.qmt_connector" in sys.modules:
            del sys.modules["core.qmt_connector"]
        import core.qmt_connector as qc
        with self.assertRaises(RuntimeError):
            qc.QMTConnector(qc.QMTConfig(token="T"))

    def test_ok_flag_transitions(self):
        """测试内容：ok 状态位在连接前后变化
        目的：验证 ok 属性准确反映状态。
        输入：正常连接。
        预期输出：连接前 False，连接后 True。
        """
        _install_fake_xtquant(listen_ok=True)
        _reload_qmt_connector()
        from core.qmt_connector import QMTConnector, QMTConfig
        conn = QMTConnector(QMTConfig(token="T"))
        self.assertFalse(conn.ok)
        conn.listen_and_connect()
        self.assertTrue(conn.ok)