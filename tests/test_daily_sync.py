from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from alphasift.daily_store import DailyBarStore, adj_factor_rebuild_required
from alphasift.daily_sync import (
    TushareSyncClient,
    _SymbolProgressBar,
    _read_trade_cal_dates,
    init_daily_bars,
    sync_daily_bars,
)


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

    def daily(self, **kwargs):
        self.calls.append(("daily", kwargs))
        if "trade_date" in kwargs:
            trade_date = kwargs["trade_date"]
            return pd.DataFrame([
                {"ts_code": "600519.SH", "trade_date": trade_date, "open": 10.0, "high": 10.5, "low": 9.8, "close": 10.2, "vol": 1000.0, "amount": 10000.0},
            ])
        ts_code = kwargs["ts_code"]
        return pd.DataFrame([
            {"ts_code": ts_code, "trade_date": "20260401", "open": 10.0, "high": 10.5, "low": 9.8, "close": 10.2, "vol": 1000.0, "amount": 10000.0},
            {"ts_code": ts_code, "trade_date": "20260402", "open": 10.2, "high": 10.8, "low": 10.0, "close": 10.6, "vol": 1100.0, "amount": 11000.0},
        ])

    def adj_factor(self, **kwargs):
        self.calls.append(("adj_factor", kwargs))
        if "trade_date" in kwargs:
            trade_date = kwargs["trade_date"]
            return pd.DataFrame([
                {"ts_code": "600519.SH", "trade_date": trade_date, "adj_factor": 1.0},
            ])
        return pd.DataFrame([
            {"trade_date": "20260401", "adj_factor": 1.0},
            {"trade_date": "20260402", "adj_factor": 1.0},
        ])


def _make_store(tmp_path: Path) -> DailyBarStore:
    store = DailyBarStore(tmp_path / "daily_bars")
    store.write_manifest({"version": 1, "last_trade_date": "20260401", "code_count": 0})
    return store


def test_init_writes_raw_and_adj(monkeypatch, tmp_path: Path):
    pro = FakePro()
    monkeypatch.setattr("tushare.pro_api", lambda token=None: pro)
    store = _make_store(tmp_path)

    stats = init_daily_bars(store, token="token", lookback_days=120, max_codes=1, workers=1)

    assert stats.updated_codes == 1
    assert store.has_code("600519")
    assert not (tmp_path / "daily_bars" / "meta" / "sync_progress.json").exists()


def test_sync_upserts_new_trade_date(monkeypatch, tmp_path: Path):
    pro = FakePro()
    monkeypatch.setattr("tushare.pro_api", lambda token=None: pro)
    store = _make_store(tmp_path)
    store.replace_raw("600519.SH", pd.DataFrame([
        {"date": "20260401", "open": 10.0, "high": 10.5, "low": 9.8, "close": 10.2, "volume": 1000.0, "amount": 10000.0},
    ]))
    store.replace_adj_factor("600519.SH", pd.DataFrame([
        {"date": "20260401", "adj_factor": 1.0},
    ]))
    store.write_sidecar("600519.SH", {
        "ts_code": "600519.SH",
        "last_trade_date": "20260401",
        "latest_adj_factor": 1.0,
        "latest_adj_factor_date": "20260401",
        "adj_factor_fingerprint": "sha256:abc",
    })

    stats = sync_daily_bars(store, token="token", lookback_days=120)

    assert stats.updated_codes >= 1


def test_read_trade_cal_dates_rejects_empty_calendar():
    class FakePro:
        def trade_cal(self, **kwargs):
            import pandas as pd
            return pd.DataFrame()

    client = TushareSyncClient(FakePro(), requests_per_second=0)
    with pytest.raises(RuntimeError, match="no open trading days"):
        _read_trade_cal_dates(client, start_date="20260401", end_date="20260403")


def test_read_trade_cal_dates_accepts_trade_date_alias():
    class FakePro:
        def trade_cal(self, **kwargs):
            import pandas as pd
            return pd.DataFrame({"trade_date": ["20260402", "20260403"], "is_open": ["1", "1"]})

    client = TushareSyncClient(FakePro(), requests_per_second=0)
    dates = _read_trade_cal_dates(client, start_date="20260401", end_date="20260403")
    assert dates == ["20260402", "20260403"]


def test_init_uses_workers_parameter(monkeypatch, tmp_path: Path):
    pro = FakePro()
    monkeypatch.setattr("tushare.pro_api", lambda token=None: pro)
    store = _make_store(tmp_path)

    stats = init_daily_bars(
        store,
        token="token",
        lookback_days=120,
        max_codes=2,
        workers=2,
    )

    assert stats.updated_codes == 2


def test_symbol_progress_bar_disabled_when_show_progress_false():
    bar = _SymbolProgressBar(total=10, initial=0, enabled=False, desc="test")
    assert not bar.enabled
    bar.update(last_symbol="600519.SH", updated=1, skipped=0, failed=0)
    bar.close()

def test_adj_factor_rebuild_required_on_change(tmp_path: Path):
    store = _make_store(tmp_path)
    store.replace_adj_factor("600519.SH", pd.DataFrame([
        {"date": "20260401", "adj_factor": 1.0},
    ]))
    store.write_sidecar("600519.SH", {
        "ts_code": "600519.SH",
        "last_trade_date": "20260401",
        "latest_adj_factor": 1.0,
        "latest_adj_factor_date": "20260401",
    })
    assert adj_factor_rebuild_required(
        store,
        "600519.SH",
        {"adj_factor": 2.0, "trade_date": "20260402"},
    )


def test_transient_retry(monkeypatch):
    pro = FakePro()
    client = TushareSyncClient(pro, requests_per_second=0, retry=2, retry_interval=0)
    attempts = {"count": 0}

    def flaky():
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise ConnectionError("timeout")
        return "ok"

    assert client.call(flaky) == "ok"
    assert client.stats["retries"] == 1


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

    stats = init_daily_bars(store, token="token", lookback_days=120, max_codes=2, workers=1)

    assert stats.updated_codes == 1
