from __future__ import annotations

import pandas as pd

from alphasift.flow_conditions import (
    FLOW_CONDITION_MAIN_INFLOW_STREAK,
    FLOW_CONDITION_MAIN_NET_INFLOW_5D_GT,
    FLOW_CONDITION_NO_PRICE_UP_FLOW_OUT,
    evaluate_flow_conditions,
)
from alphasift.flow_metrics import build_stock_flow_snapshot, enrich_moneyflow_frame


def make_moneyflow_rows(symbol: str, count: int = 30, *, start: str = "2024-01-02") -> pd.DataFrame:
    rows = []
    for index in range(count):
        trade_date = (pd.Timestamp(start) + pd.Timedelta(days=index)).strftime("%Y-%m-%d")
        main = 10.0 if index % 3 != 2 else -5.0
        rows.append(
            {
                "ts_code": symbol,
                "trade_date": trade_date,
                "buy_lg_amount": 100.0 + main,
                "buy_elg_amount": 50.0,
                "sell_lg_amount": 80.0,
                "sell_elg_amount": 20.0,
                "buy_sm_amount": 10.0,
                "buy_md_amount": 20.0,
                "sell_sm_amount": 15.0,
                "sell_md_amount": 12.0,
                "net_mf_amount": main,
                "main_net_inflow": main,
                "retail_net_inflow": 3.0,
            }
        )
    return pd.DataFrame(rows)


def make_daily_rows(count: int = 30, *, start: str = "2024-01-02") -> pd.DataFrame:
    rows = []
    for index in range(count):
        date = (pd.Timestamp(start) + pd.Timedelta(days=index)).strftime("%Y-%m-%d")
        close = 10.0 + index * 0.1
        rows.append(
            {
                "date": date,
                "open": close - 0.1,
                "high": close + 0.2,
                "low": close - 0.2,
                "close": close,
                "volume": 1000.0 + index * 10,
            }
        )
    return pd.DataFrame(rows)


def test_enrich_moneyflow_frame_computes_rolling_and_streak():
    moneyflow = make_moneyflow_rows("600519.SH", count=10)
    daily = make_daily_rows(count=10)

    enriched = enrich_moneyflow_frame(moneyflow, daily, windows=(5,))

    assert enriched["main_net_inflow_5d"].iloc[-1] == enriched["main_net_inflow"].tail(5).sum()
    assert int(enriched["main_inflow_streak"].iloc[-1]) >= 0
    assert "close_pct" in enriched.columns
    assert "price_up_flow_out" in enriched.columns


def test_build_stock_flow_snapshot_respects_as_of_date():
    moneyflow = make_moneyflow_rows("600519.SH", count=10)
    daily = make_daily_rows(count=10)

    snapshot = build_stock_flow_snapshot(
        moneyflow,
        daily,
        as_of_date="2024-01-05",
        windows=(5, 10, 20),
    )

    assert snapshot["as_of"] == "2024-01-05"
    assert snapshot.get("main_net_inflow") is not None
    assert "main_net_inflow_5d" in snapshot


def test_divergence_flags_detect_price_up_flow_out():
    moneyflow = pd.DataFrame([
        {
            "ts_code": "600519.SH",
            "trade_date": "2024-01-01",
            "main_net_inflow": -8.0,
            "net_mf_amount": -8.0,
        },
        {
            "ts_code": "600519.SH",
            "trade_date": "2024-01-02",
            "main_net_inflow": -6.0,
            "net_mf_amount": -6.0,
        },
    ])
    daily = pd.DataFrame([
        {"date": "2024-01-01", "open": 10.0, "high": 10.5, "low": 9.8, "close": 10.0, "volume": 1000.0},
        {"date": "2024-01-02", "open": 10.0, "high": 10.8, "low": 9.9, "close": 10.5, "volume": 1200.0},
    ])

    enriched = enrich_moneyflow_frame(moneyflow, daily, windows=(5,))
    last = enriched.iloc[-1]

    assert bool(last["price_up_flow_out"])
    assert not bool(last["price_down_flow_in"])


def test_enrich_moneyflow_frame_leaves_divergence_unset_without_daily():
    moneyflow = make_moneyflow_rows("600519.SH", count=5)

    enriched = enrich_moneyflow_frame(moneyflow, None, windows=(5,))
    last = enriched.iloc[-1]

    assert pd.isna(last["price_up_flow_out"])
    assert pd.isna(last["price_down_flow_in"])


def test_evaluate_flow_conditions_pass_and_fail():
    moneyflow = make_moneyflow_rows("600519.SH", count=10)
    daily = make_daily_rows(count=10)

    pass_result = evaluate_flow_conditions(
        moneyflow,
        daily,
        [
            {"id": FLOW_CONDITION_MAIN_INFLOW_STREAK, "params": {"days": 1}},
            {"id": FLOW_CONDITION_MAIN_NET_INFLOW_5D_GT, "params": {"threshold": -1000}},
            {"id": FLOW_CONDITION_NO_PRICE_UP_FLOW_OUT},
        ],
    )
    assert pass_result is not None

    fail_result = evaluate_flow_conditions(
        moneyflow,
        daily,
        [{"id": FLOW_CONDITION_MAIN_INFLOW_STREAK, "params": {"days": 999}}],
    )
    assert fail_result is None
