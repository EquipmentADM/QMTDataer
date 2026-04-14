# -*- coding: utf-8 -*-
import sys
from pathlib import Path
from unittest.mock import patch

import unittest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dev_dashboard import dashboard_plugin, dashboard_service


class TestDashboardPlugin(unittest.TestCase):
    def test_get_module_spec_fields(self):
        spec = dashboard_plugin.get_module_spec()
        self.assertEqual(spec["spec_version"], "1.0")
        self.assertEqual(spec["module_id"], "qmtdataer")
        self.assertEqual(spec["component_prefix"], "qmtdataer")
        self.assertTrue(callable(spec["check_available"]))
        self.assertTrue(callable(spec["build_layout"]))
        self.assertTrue(callable(spec["register_callbacks"]))
        self.assertTrue(callable(spec["get_status_detail"]))

    def test_default_probe_params(self):
        stock = dashboard_service.default_stock_probe_params()
        futures = dashboard_service.default_futures_probe_params()
        self.assertEqual(stock["symbol"], "510050.SH")
        self.assertEqual(futures["symbol"], "rb00.SF")
        self.assertIn(stock["period"], ("1d", "1m"))
        self.assertIn(futures["period"], ("1d", "1m"))

    @patch("dev_dashboard.dashboard_service._import_xtdata")
    def test_probe_history_empty(self, mock_import_xtdata):
        class FakeXtdata:
            @staticmethod
            def download_history_data(**kwargs):
                return True

            @staticmethod
            def get_market_data_ex(**kwargs):
                return {"510050.SH": None}

        mock_import_xtdata.return_value = FakeXtdata()
        result = dashboard_service.probe_history("510050.SH", "1d", "20260101", "20260131")
        self.assertFalse(result["ok"])
        self.assertEqual(result["rows"], 0)

    @patch("dev_dashboard.dashboard_service._import_xtdata")
    def test_check_available_import_failed(self, mock_import_xtdata):
        mock_import_xtdata.side_effect = RuntimeError("xtdata missing")
        with patch.dict(sys.modules, {"dash": object()}):
            result = dashboard_service.check_available()
        self.assertFalse(result["available"])
        self.assertEqual(result["status"], "xtdata_import_failed")


if __name__ == "__main__":
    unittest.main()
