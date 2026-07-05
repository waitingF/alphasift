from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from alphasift.flow_store import FlowBarStore
from alphasift.flow_sync import init_flow_bars, sync_flow_bars


class FakePro:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def stock_basic(self, **kwargs):
        self.calls.append(("stock_basic", kwargs))
        return pd.DataFrame([
            {"ts_code": "600519.SH", "symbol": "600519", "name": "贵州茅台", "list_date": "20010827"},
            {"ts_code": "000001.SZ", "symbol": "000001", "name": "平安银行", "list_date": "19910403"},
        ])

    def trade_cal(self, **kwargs):
        self.calls.append(("trade_cal", kwargs))
        return pd.DataFrame({"cal_date": ["20260401", "20260402", "20260403"], "is_open": ["1", "1", "1"]})

    def moneyflow(self, **kwargs):
        self.calls.append(("moneyflow", kwargs))
        if "trade_date" in kwargs:
            trade_date = kwargs["trade_date"]
            return pd.DataFrame([
                {
                    "ts_code": "600519.SH",
                    "trade_date": trade_date,
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
            ])
        ts_code = kwargs["ts_code"]
        return pd.DataFrame([
            {
                "ts_code": ts_code,
                "trade_date": "20260401",
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
                "trade_date": "20260402",
                "buy_lg_amount": 110.0,
                "buy_elg_amount": 55.0,
                "sell_lg_amount": 75.0,
                "sell_elg_amount": 25.0,
                "buy_sm_amount": 11.0,
                "buy_md_amount": 21.0,
                "sell_sm_amount": 16.0,
                "sell_md_amount": 13.0,
                "net_mf_amount": 65.0,
            },
        ])


def _make_store(tmp_path: Path) -> FlowBarStore:
    store = FlowBarStore(tmp_path / "flow_bars")
    store.write_manifest({"version": 1, "last_trade_date": "20260401", "code_count": 0})
    return store


def test_init_flow_bars_writes_moneyflow(monkeypatch, tmp_path: Path):
    pro = FakePro()
    monkeypatch.setattr("tushare.pro_api", lambda token=None: pro)
    store = _make_store(tmp_path)

    stats = init_flow_bars(store, token="token", lookback_days=120, max_codes=1, workers=1)

    assert stats.updated_codes == 1
    assert store.has_code("600519")
    assert not (tmp_path / "flow_bars" / "meta" / "sync_progress.json").exists()


def test_sync_flow_bars_upserts_new_trade_date(monkeypatch, tmp_path: Path):
    pro = FakePro()
    monkeypatch.setattr("tushare.pro_api", lambda token=None: pro)
    store = _make_store(tmp_path)
    store.write("600519.SH", pd.DataFrame([
        {
            "ts_code": "600519.SH",
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
    ]))

    stats = sync_flow_bars(store, token="token")

    assert stats.updated_codes >= 1


def test_progress_resume(monkeypatch, tmp_path: Path):
    pro = FakePro()
    monkeypatch.setattr("tushare.pro_api", lambda token=None: pro)
    store = _make_store(tmp_path)
    progress_path = store.root / "meta" / "sync_progress.json"
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path.write_text(json.dumps({
        "signature": {"command": "init", "lookback_days": 120, "end_date": "20260403"},
        "next_index": 1,
        "symbols": ["600519.SH", "000001.SZ"],
        "updated": 1,
        "skipped": 0,
        "failed": 0,
        "rebuilt": 0,
        "last_symbol": "600519.SH",
        "errors": [],
        "api_stats": {"attempts": 1, "retries": 0, "failures": 0},
    }), encoding="utf-8")

    stats = init_flow_bars(store, token="token", lookback_days=120, max_codes=2, workers=1)

    assert stats.updated_codes == 1
