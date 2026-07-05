from __future__ import annotations

import json
import time

import pandas as pd
import pytest

from alphasift.pattern.features import normalize_bars
from alphasift.pattern.search import list_store_codes, search_pattern


def _build_bars(symbol_seed: int, bars_count: int = 40) -> pd.DataFrame:
    rows = []
    for index in range(bars_count):
        base = 10 + symbol_seed + index * 0.1
        rows.append(
            {
                "date": (pd.Timestamp("2024-01-01") + pd.Timedelta(days=index)).strftime("%Y-%m-%d"),
                "open": round(base, 4),
                "high": round(base + 0.6, 4),
                "low": round(base - 0.4, 4),
                "close": round(base + ((index % 5) - 2) * 0.08, 4),
                "volume": float(1000 + symbol_seed * 10 + index * 15),
                "amount": float(10000 + symbol_seed * 100 + index * 150),
            }
        )
    return pd.DataFrame(rows)


def _write_daily_store(root, codes: list[str], *, bars_count: int = 80) -> None:
    from alphasift.daily_store import DailyBarStore

    store = DailyBarStore(root, adj="qfq")
    last_date = "20240331"
    for index, code in enumerate(codes):
        frame = _build_bars(index + 1, bars_count=bars_count)
        raw = frame.copy()
        raw["date"] = raw["date"].str.replace("-", "")
        last_date = str(raw["date"].iloc[-1])
        factors = pd.DataFrame({"date": raw["date"], "adj_factor": 1.0})
        store.replace_raw(code, raw)
        store.replace_adj_factor(code, factors)
    store.write_manifest({
        "version": 1,
        "provider": "test",
        "adj": "qfq",
        "last_trade_date": last_date,
        "code_count": len(codes),
    })


def test_list_store_codes_reads_parquet_directory(tmp_path):
    _write_daily_store(tmp_path, ["000001.SZ", "600519.SH"], bars_count=30)
    assert list_store_codes(tmp_path) == ["000001.SZ", "600519.SH"]


def test_search_pattern_finds_similar_window(tmp_path):
    codes = [f"60000{i}.SH" for i in range(1, 6)]
    _write_daily_store(tmp_path, codes, bars_count=80)
    query = _build_bars(1, bars_count=25).to_dict(orient="records")
    result = search_pattern(
        query,
        daily_bars_dir=tmp_path,
        metric="euclidean",
        top_k=5,
        codes=codes,
        lookback_days=80,
        max_workers=1,
    )
    assert result["summary"]["returned_matches"] >= 1
    assert result["matches"][0]["total_similarity"] >= 0.5


def test_search_pattern_scales_to_100_symbols_under_30_seconds(tmp_path):
    codes = [f"{idx:06d}.SZ" for idx in range(1, 101)]
    _write_daily_store(tmp_path, codes, bars_count=60)
    query = _build_bars(1, bars_count=20).to_dict(orient="records")
    started = time.monotonic()
    result = search_pattern(
        query,
        daily_bars_dir=tmp_path,
        metric="pearson",
        top_k=10,
        codes=codes,
        lookback_days=60,
        max_workers=4,
    )
    elapsed = time.monotonic() - started
    assert result["summary"]["scanned_symbols"] == 100
    assert elapsed < 30.0


def test_normalize_bars_accepts_chinese_columns():
    frame = pd.DataFrame(
        {
            "日期": ["2024-01-01", "2024-01-02"],
            "开盘": [10.0, 10.1],
            "最高": [10.5, 10.6],
            "最低": [9.8, 9.9],
            "收盘": [10.2, 10.3],
            "成交量": [1000, 1100],
        }
    )
    normalized = normalize_bars(frame)
    assert len(normalized) == 2
    assert "close" in normalized.columns
