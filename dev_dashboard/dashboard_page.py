# -*- coding: utf-8 -*-
"""
QMTDataer 控制台模块页面骨架。

第一阶段目标：
    - 只验证模块能被统一控制台挂载；
    - 提供股票 / 期货的最小历史取数测试按钮；
    - 不做落盘、不做任务持久化。
"""
from __future__ import annotations

import json
import traceback

from dev_dashboard.dashboard_service import (
    check_available,
    default_futures_probe_params,
    default_stock_probe_params,
    get_status_detail,
    probe_history,
)


MODULE_ID = "qmtdataer"
PREFIX = "qmtdataer-home"


def _components():
    """延迟导入 Dash 组件，避免模块导入期强依赖 Dash。"""
    from dash import Input, Output, State, callback_context, dcc, html

    return html, dcc, Input, Output, State, callback_context


def _json_text(data: object) -> str:
    """统一结果展示格式。"""
    return json.dumps(data, ensure_ascii=False, indent=2)


def build_layout():
    """构建模块页面布局。"""
    html, dcc, _, _, _, _ = _components()
    stock = default_stock_probe_params()
    futures = default_futures_probe_params()
    status = check_available()

    return html.Div(
        id=f"{PREFIX}-page",
        children=[
            html.H2("QMTDataer 模块"),
            html.P("当前为第一阶段控制台接入骨架，仅提供最小历史取数测试，不做保存。"),
            html.Div(
                id=f"{PREFIX}-status-box",
                children=[
                    html.H4("模块状态"),
                    html.Pre(_json_text(status), id=f"{PREFIX}-status-text"),
                    html.Button("刷新模块状态", id=f"{PREFIX}-refresh-status-btn", n_clicks=0),
                ],
            ),
            html.Hr(),
            html.Div(
                id=f"{PREFIX}-stock-box",
                children=[
                    html.H4("股票历史测试"),
                    dcc.Input(id=f"{PREFIX}-stock-symbol", type="text", value=stock["symbol"]),
                    dcc.Dropdown(
                        id=f"{PREFIX}-stock-period",
                        options=[{"label": p, "value": p} for p in ("1d", "1m")],
                        value=stock["period"],
                        clearable=False,
                    ),
                    dcc.Input(id=f"{PREFIX}-stock-start", type="text", value=stock["start"]),
                    dcc.Input(id=f"{PREFIX}-stock-end", type="text", value=stock["end"]),
                    html.Button("测试股票历史获取", id=f"{PREFIX}-stock-probe-btn", n_clicks=0),
                ],
            ),
            html.Div(
                id=f"{PREFIX}-futures-box",
                children=[
                    html.H4("期货历史测试"),
                    dcc.Input(id=f"{PREFIX}-futures-symbol", type="text", value=futures["symbol"]),
                    dcc.Dropdown(
                        id=f"{PREFIX}-futures-period",
                        options=[{"label": p, "value": p} for p in ("1d", "1m")],
                        value=futures["period"],
                        clearable=False,
                    ),
                    dcc.Input(id=f"{PREFIX}-futures-start", type="text", value=futures["start"]),
                    dcc.Input(id=f"{PREFIX}-futures-end", type="text", value=futures["end"]),
                    html.Button("测试期货历史获取", id=f"{PREFIX}-futures-probe-btn", n_clicks=0),
                ],
            ),
            html.Hr(),
            html.Div(
                id=f"{PREFIX}-result-box",
                children=[
                    html.H4("测试结果"),
                    html.Pre("尚未执行测试。", id=f"{PREFIX}-result-text"),
                ],
            ),
        ],
    )


def register_callbacks(app, module_context=None):
    """注册模块回调。"""
    _, _, Input, Output, State, callback_context = _components()

    @app.callback(
        Output(f"{PREFIX}-status-text", "children"),
        Input(f"{PREFIX}-refresh-status-btn", "n_clicks"),
        prevent_initial_call=True,
    )
    def _refresh_status(_n_clicks):
        return _json_text(get_status_detail())

    @app.callback(
        Output(f"{PREFIX}-result-text", "children"),
        Input(f"{PREFIX}-stock-probe-btn", "n_clicks"),
        Input(f"{PREFIX}-futures-probe-btn", "n_clicks"),
        State(f"{PREFIX}-stock-symbol", "value"),
        State(f"{PREFIX}-stock-period", "value"),
        State(f"{PREFIX}-stock-start", "value"),
        State(f"{PREFIX}-stock-end", "value"),
        State(f"{PREFIX}-futures-symbol", "value"),
        State(f"{PREFIX}-futures-period", "value"),
        State(f"{PREFIX}-futures-start", "value"),
        State(f"{PREFIX}-futures-end", "value"),
        prevent_initial_call=True,
    )
    def _run_probe(
        _stock_clicks,
        _futures_clicks,
        stock_symbol,
        stock_period,
        stock_start,
        stock_end,
        futures_symbol,
        futures_period,
        futures_start,
        futures_end,
    ):
        try:
            trigger = callback_context.triggered_id
            if trigger == f"{PREFIX}-stock-probe-btn":
                result = probe_history(stock_symbol, stock_period, stock_start, stock_end)
            elif trigger == f"{PREFIX}-futures-probe-btn":
                result = probe_history(futures_symbol, futures_period, futures_start, futures_end)
            else:
                result = {"ok": False, "message": "未知触发来源。"}
            return _json_text(result)
        except Exception as exc:
            return _json_text(
                {
                    "ok": False,
                    "message": "测试执行异常。",
                    "error": str(exc),
                    "traceback": traceback.format_exc(limit=3),
                }
            )
