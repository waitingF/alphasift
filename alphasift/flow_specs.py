# -*- coding: utf-8 -*-
"""Field constants and display metadata for capital-flow metrics."""

from __future__ import annotations

FLOW_LABELS: dict[str, str] = {
    "main_net_inflow": "主力净流入(万元)",
    "main_net_inflow_5d": "5日主力净流入(万元)",
    "main_net_inflow_10d": "10日主力净流入(万元)",
    "main_net_inflow_20d": "20日主力净流入(万元)",
    "main_inflow_streak": "连续净流入天数",
    "main_net_inflow_rate": "主力净流入占比",
    "main_net_inflow_zscore_20d": "20日净流入Z分数",
    "price_up_flow_out": "价涨量出(背离)",
    "price_down_flow_in": "价跌量入(吸筹)",
    "net_mf_amount": "L2主动净额(万元)",
    "close_pct": "涨跌幅(%)",
    "close": "收盘价",
}

DEFAULT_WINDOWS: tuple[int, ...] = (5, 10, 20)
ZSCORE_WINDOW = 20

TIER_BUY_AMOUNT_COLUMNS: tuple[str, ...] = (
    "buy_sm_amount",
    "buy_md_amount",
    "buy_lg_amount",
    "buy_elg_amount",
)

TIER_SELL_AMOUNT_COLUMNS: tuple[str, ...] = (
    "sell_sm_amount",
    "sell_md_amount",
    "sell_lg_amount",
    "sell_elg_amount",
)

MONEYFLOW_RAW_COLUMNS: tuple[str, ...] = (
    "ts_code",
    "trade_date",
    "buy_sm_vol",
    "buy_sm_amount",
    "sell_sm_vol",
    "sell_sm_amount",
    "buy_md_vol",
    "buy_md_amount",
    "sell_md_vol",
    "sell_md_amount",
    "buy_lg_vol",
    "buy_lg_amount",
    "sell_lg_vol",
    "sell_lg_amount",
    "buy_elg_vol",
    "buy_elg_amount",
    "sell_elg_vol",
    "sell_elg_amount",
    "net_mf_vol",
    "net_mf_amount",
    "main_net_inflow",
    "retail_net_inflow",
)

MONEYFLOW_FIELDS = (
    "ts_code,trade_date,"
    "buy_sm_vol,buy_sm_amount,sell_sm_vol,sell_sm_amount,"
    "buy_md_vol,buy_md_amount,sell_md_vol,sell_md_amount,"
    "buy_lg_vol,buy_lg_amount,sell_lg_vol,sell_lg_amount,"
    "buy_elg_vol,buy_elg_amount,sell_elg_vol,sell_elg_amount,"
    "net_mf_vol,net_mf_amount"
)
