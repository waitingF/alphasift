from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from alphasift.flow_store import FlowBarStore, compute_flow_derived_columns


def _sample_moneyflow(ts_code: str = "600519.SH") -> pd.DataFrame:
    return pd.DataFrame([
        {
            "ts_code": ts_code,
            "trade_date": "2026-04-01",
            "buy_lg_amount": 100.0,
            "buy_elg_amount": 50.0,
            "sell_lg_amount": 80.0,
            "sell_elg_amount": 20.0,
            "buy_sm_amount": 10.0,
            "buy_md_amount": 20.0,
            "sell_sm_amount": 15.0,
            "sell_md_amount": 12.0,
            "net_mf_amount": 50.0,
        },
        {
            "ts_code": ts_code,
            "trade_date": "2026-04-02",
            "buy_lg_amount": 120.0,
            "buy_elg_amount": 60.0,
            "sell_lg_amount": 70.0,
            "sell_elg_amount": 30.0,
            "buy_sm_amount": 11.0,
            "buy_md_amount": 21.0,
            "sell_sm_amount": 16.0,
            "sell_md_amount": 13.0,
            "net_mf_amount": 80.0,
        },
    ])


def test_compute_flow_derived_columns():
    frame = _sample_moneyflow()
    result = compute_flow_derived_columns(frame)
    assert result["main_net_inflow"].iloc[0] == pytest.approx(50.0)
    assert result["retail_net_inflow"].iloc[0] == pytest.approx(3.0)


def test_flow_store_write_read_reconcile(tmp_path: Path):
    store = FlowBarStore(tmp_path / "flow_bars")
    store.write("600519", _sample_moneyflow())
    assert store.has_code("600519")

    read = store.read("600519", lookback_days=10)
    assert len(read) == 2
    assert read["trade_date"].iloc[-1] == "2026-04-02"

    remote = pd.DataFrame([
        {
            "ts_code": "600519.SH",
            "trade_date": "20260403",
            "buy_lg_amount": 130.0,
            "buy_elg_amount": 65.0,
            "sell_lg_amount": 60.0,
            "sell_elg_amount": 25.0,
            "buy_sm_amount": 12.0,
            "buy_md_amount": 22.0,
            "sell_sm_amount": 17.0,
            "sell_md_amount": 14.0,
            "net_mf_amount": 110.0,
        }
    ])
    stats = store.reconcile_and_write("600519.SH", remote)
    assert stats["added"] == 1
    merged = store.read("600519", lookback_days=10)
    assert merged["trade_date"].tolist() == ["2026-04-01", "2026-04-02", "2026-04-03"]


def test_flow_store_manifest_and_list_codes(tmp_path: Path):
    store = FlowBarStore(tmp_path / "flow_bars")
    store.write("600519", _sample_moneyflow())
    store.write_manifest({"version": 1, "last_trade_date": "20260402", "code_count": 1})
    manifest = store.manifest()
    assert manifest["last_trade_date"] == "20260402"
    assert store.list_codes() == ["600519.SH"]
