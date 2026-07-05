from __future__ import annotations

import sys
import types
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from alphasift.daily import _apply_tushare_adjustment, fetch_daily_history
from alphasift.daily_adjust import apply_adj
from alphasift.daily_store import (
    DailyBarStore,
    compute_adj_factor_fingerprint,
    format_date_iso,
    normalize_ts_code,
    require_pyarrow,
)

OHLCV_COLS = ("open", "high", "low", "close", "volume")
COMPARE_RTOL = 1e-6
COMPARE_ATOL = 1e-4

# Fixture with an ex-dividend event: adj_factor jumps on 20260404.
EX_DIVIDEND_RAW = pd.DataFrame([
    {"date": "20260401", "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 10000.0, "amount": 1000000.0},
    {"date": "20260402", "open": 100.5, "high": 102.0, "low": 100.0, "close": 101.5, "volume": 11000.0, "amount": 1100000.0},
    {"date": "20260403", "open": 101.5, "high": 103.0, "low": 101.0, "close": 102.5, "volume": 12000.0, "amount": 1200000.0},
    {"date": "20260404", "open": 68.0, "high": 69.0, "low": 67.5, "close": 68.5, "volume": 13000.0, "amount": 890000.0},
    {"date": "20260405", "open": 68.5, "high": 70.0, "low": 68.0, "close": 69.5, "volume": 14000.0, "amount": 970000.0},
])
EX_DIVIDEND_FACTORS = pd.DataFrame([
    {"date": "20260401", "adj_factor": 1.0},
    {"date": "20260402", "adj_factor": 1.0},
    {"date": "20260403", "adj_factor": 1.0},
    {"date": "20260404", "adj_factor": 1.5},
    {"date": "20260405", "adj_factor": 1.5},
])
EX_DIVIDEND_END_DATE = "20260405"


def _online_tushare_adjusted_history(
    raw: pd.DataFrame,
    factors: pd.DataFrame,
    *,
    end_date: str,
    lookback_days: int,
    adj: str,
) -> pd.DataFrame:
    """Expected OHLCV from raw+adj_factor using the same apply_adj path as online Tushare."""
    end = str(end_date)
    raw_slice = raw[raw["date"].astype(str) <= end].copy()
    factor_slice = factors[factors["date"].astype(str) <= end].copy()
    adjusted = apply_adj(raw_slice, factor_slice, adj=adj)
    adjusted["date"] = adjusted["date"].astype(str).map(format_date_iso)
    adjusted = adjusted.sort_values("date")
    end_iso = format_date_iso(end)
    adjusted = adjusted[adjusted["date"] <= end_iso]
    return adjusted.tail(max(int(lookback_days), 1)).reset_index(drop=True)


def _assert_ohlcv_frames_equal(left: pd.DataFrame, right: pd.DataFrame) -> None:
    assert left["date"].tolist() == right["date"].tolist()
    for col in OHLCV_COLS:
        pd.testing.assert_series_equal(
            pd.to_numeric(left[col]),
            pd.to_numeric(right[col]),
            check_names=False,
            rtol=COMPARE_RTOL,
            atol=COMPARE_ATOL,
        )


def _populate_ex_dividend_store(store_root: Path, *, adj: str = "qfq") -> DailyBarStore:
    store = DailyBarStore(store_root, adj=adj)
    store.replace_raw("000001", EX_DIVIDEND_RAW.copy())
    store.replace_adj_factor("000001", EX_DIVIDEND_FACTORS.copy())
    store.write_manifest({
        "version": 1,
        "provider": "tushare",
        "adj": adj,
        "last_trade_date": EX_DIVIDEND_END_DATE,
        "code_count": 1,
    })
    return store


@pytest.fixture
def store_root(tmp_path: Path) -> Path:
    root = tmp_path / "daily_bars"
    root.mkdir()
    return root


@pytest.fixture
def sample_store(store_root: Path) -> DailyBarStore:
    store = DailyBarStore(store_root, adj="qfq")
    raw = pd.DataFrame([
        {"date": "20260401", "open": 10.0, "high": 10.5, "low": 9.8, "close": 10.2, "volume": 1000.0, "amount": 10000.0},
        {"date": "20260402", "open": 10.2, "high": 10.8, "low": 10.0, "close": 10.6, "volume": 1100.0, "amount": 11000.0},
        {"date": "20260403", "open": 10.6, "high": 11.0, "low": 10.4, "close": 10.8, "volume": 1200.0, "amount": 12000.0},
    ])
    factors = pd.DataFrame([
        {"date": "20260401", "adj_factor": 1.0},
        {"date": "20260402", "adj_factor": 1.0},
        {"date": "20260403", "adj_factor": 2.0},
    ])
    store.replace_raw("600519", raw)
    store.replace_adj_factor("600519", factors)
    store.write_manifest({
        "version": 1,
        "provider": "tushare",
        "adj": "qfq",
        "last_trade_date": "20260403",
        "code_count": 1,
    })
    return store


def test_normalize_ts_code_variants():
    assert normalize_ts_code("600519") == "600519.SH"
    assert normalize_ts_code("600519.SH") == "600519.SH"
    assert normalize_ts_code("600519.sh") == "600519.SH"
    assert normalize_ts_code("000001") == "000001.SZ"
    assert normalize_ts_code("300750") == "300750.SZ"
    assert normalize_ts_code("688981") == "688981.SH"
    assert normalize_ts_code("830799") == "830799.BJ"
    assert normalize_ts_code("920593") == "920593.BJ"
    assert normalize_ts_code("1") == "000001.SZ"


def test_read_history_returns_tail_and_iso_dates(sample_store: DailyBarStore):
    hist = sample_store.read_history("600519", lookback_days=2, end_date="20260403")
    assert len(hist) == 2
    assert hist["date"].tolist() == ["2026-04-02", "2026-04-03"]
    assert hist.attrs["daily_source"] == "local"


def test_read_history_end_date_slice(sample_store: DailyBarStore):
    hist = sample_store.read_history("600519", lookback_days=10, end_date="20260402")
    assert hist["date"].tolist() == ["2026-04-01", "2026-04-02"]
    assert hist.attrs["daily_end_date"] == "20260402"


def test_apply_adj_matches_tushare_adjustment(monkeypatch):
    raw = pd.DataFrame({
        "date": ["20260428", "20260429"],
        "open": [10.0, 10.4],
        "high": [10.5, 10.6],
        "low": [9.9, 10.3],
        "close": [10.4, 10.5],
        "volume": [12345.0, 12300.0],
        "amount": [100000.0, 100500.0],
    })
    factors = pd.DataFrame({
        "date": ["20260429", "20260428"],
        "adj_factor": [2.0, 1.0],
    })

    class FakePro:
        def adj_factor(self, **kwargs):
            return factors.rename(columns={"date": "trade_date"})

    tushare_df = raw.rename(columns={"date": "trade_date", "volume": "vol"})
    adjusted_online = _apply_tushare_adjustment(
        tushare_df,
        pro=FakePro(),
        ts_code="000001.SZ",
        start_date="20260428",
        end_date="20260429",
        adj="qfq",
    )
    adjusted_local = apply_adj(raw, factors.sort_values("date"), adj="qfq")

    for col in ("open", "high", "low", "close"):
        pd.testing.assert_series_equal(
            pd.to_numeric(adjusted_local[col]),
            pd.to_numeric(adjusted_online[col]),
            check_names=False,
            rtol=1e-6,
            atol=1e-4,
        )


def test_missing_code_raises(sample_store: DailyBarStore):
    with pytest.raises(FileNotFoundError):
        sample_store.read_history("000001", lookback_days=5, end_date="20260403")


@pytest.mark.parametrize("adj", ["qfq", "hfq"])
@pytest.mark.parametrize("lookback_days", [3, 5])
def test_read_history_matches_online_tushare_path(
    store_root: Path,
    adj: str,
    lookback_days: int,
):
    """Read-time adj from local store matches mock online tushare for same window."""
    store = _populate_ex_dividend_store(store_root, adj=adj)
    local = store.read_history(
        "000001",
        lookback_days=lookback_days,
        end_date=EX_DIVIDEND_END_DATE,
    )
    online = _online_tushare_adjusted_history(
        EX_DIVIDEND_RAW,
        EX_DIVIDEND_FACTORS,
        end_date=EX_DIVIDEND_END_DATE,
        lookback_days=lookback_days,
        adj=adj,
    )
    _assert_ohlcv_frames_equal(local.reset_index(drop=True), online)


def test_fetch_daily_history_tushare_matches_local_store(
    store_root: Path,
    monkeypatch,
):
    """source=tushare and source=local return the same adjusted OHLCV for identical inputs."""
    class FakePro:
        def daily(self, **kwargs):
            return EX_DIVIDEND_RAW.rename(columns={"date": "trade_date", "volume": "vol"})

        def adj_factor(self, **kwargs):
            start = str(kwargs["start_date"])
            end = str(kwargs["end_date"])
            subset = EX_DIVIDEND_FACTORS[
                EX_DIVIDEND_FACTORS["date"].astype(str).between(start, end)
            ]
            return subset.rename(columns={"date": "trade_date"})

    class FakeTushare(types.SimpleNamespace):
        @staticmethod
        def pro_api(token=None):
            return FakePro()

    store = _populate_ex_dividend_store(store_root, adj="qfq")
    lookback_days = 5

    monkeypatch.setenv("TUSHARE_TOKEN", "token")
    monkeypatch.setenv("TUSHARE_DAILY_ADJ", "qfq")
    monkeypatch.setitem(sys.modules, "tushare", FakeTushare)

    fixed_now = datetime(2026, 4, 5, 15, 0, 0)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr("alphasift.daily.datetime", FixedDatetime)

    tushare = fetch_daily_history("000001", source="tushare", lookback_days=lookback_days, retries=0)
    local = fetch_daily_history(
        "000001",
        source="local",
        daily_bars_dir=store.root,
        end_date=EX_DIVIDEND_END_DATE,
        lookback_days=lookback_days,
    )

    tushare_dates = tushare["date"].astype(str).map(format_date_iso)
    tushare_norm = tushare.copy()
    tushare_norm["date"] = tushare_dates
    tushare_norm = tushare_norm.sort_values("date").reset_index(drop=True)
    local_norm = local.sort_values("date").reset_index(drop=True)

    _assert_ohlcv_frames_equal(local_norm, tushare_norm)


def test_fingerprint_stable(sample_store: DailyBarStore):
    factors = sample_store.local_adj_factor_series("600519")
    first = compute_adj_factor_fingerprint(factors)
    second = compute_adj_factor_fingerprint(factors)
    assert first == second
    assert first.startswith("sha256:")


def test_parquet_upsert_idempotent(sample_store: DailyBarStore):
    sample_store.upsert_raw_bar("600519", {
        "trade_date": "20260403",
        "open": 10.6,
        "high": 11.0,
        "low": 10.4,
        "close": 10.8,
        "vol": 1200.0,
        "amount": 12000.0,
    })
    hist = sample_store.read_history("600519", lookback_days=10, end_date="20260403")
    assert len(hist[hist["date"] == "2026-04-03"]) == 1


def test_require_pyarrow_import_error(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "pyarrow":
            raise ImportError("no pyarrow")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(RuntimeError, match="pip install"):
        require_pyarrow()
