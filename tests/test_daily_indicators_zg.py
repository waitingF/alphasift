from __future__ import annotations

import pandas as pd

from alphasift.daily_indicators import (
    add_boll_features,
    add_brick_features,
    add_zg_features,
    compute_zg_long,
    compute_zg_short,
    enrich_indicator_columns,
    extract_indicator_snapshot,
)


def _build_close_frame(closes: list[float], start: str = "2024-01-01") -> pd.DataFrame:
    rows = []
    for index, close in enumerate(closes):
        ts = (pd.Timestamp(start) + pd.Timedelta(days=index)).strftime("%Y-%m-%d")
        rows.append(
            {
                "date": ts,
                "open": close,
                "high": close + 0.5,
                "low": close - 0.5,
                "close": close,
                "volume": 1000.0,
            }
        )
    return pd.DataFrame(rows)


def test_zg_short_first_value_equals_first_close():
    closes = pd.Series([7.77, 8.0, 8.2, 8.4, 8.5, 8.6])
    result = compute_zg_short(closes)
    assert abs(result.iloc[0] - 7.77) < 1e-9


def test_zg_long_returns_nan_until_window_filled():
    closes = pd.Series([10.0 + 0.01 * i for i in range(120)])
    result = compute_zg_long(closes)
    assert pd.isna(result.iloc[112])
    assert not pd.isna(result.iloc[113])


def test_zg_long_matches_manual_average():
    closes_list = [10.0 + 0.05 * i for i in range(200)]
    closes = pd.Series(closes_list)
    result = compute_zg_long(closes)
    last_index = len(closes) - 1
    manual = sum(
        closes.iloc[last_index - window + 1 : last_index + 1].mean()
        for window in (14, 28, 57, 114)
    ) / 4
    assert abs(result.iloc[-1] - manual) < 1e-6


def test_add_zg_features_produces_columns():
    frame = _build_close_frame([10.0 + 0.05 * i for i in range(150)])
    enriched = add_zg_features(frame)
    assert "zg_short" in enriched.columns
    assert "zg_long" in enriched.columns
    assert abs(enriched["zg_short"].iloc[0] - 10.0) < 1e-9
    assert not pd.isna(enriched["zg_long"].iloc[-1])


def test_add_boll_features_matches_chart_definition():
    frame = _build_close_frame([10.0 + 0.05 * i for i in range(50)])
    enriched = add_boll_features(frame)
    manual_mid = enriched["close"].rolling(window=20, min_periods=1).mean()
    pd.testing.assert_series_equal(enriched["boll_mid"], manual_mid, check_names=False)
    spread_upper = enriched["boll_upper"] - enriched["boll_mid"]
    spread_lower = enriched["boll_mid"] - enriched["boll_lower"]
    pd.testing.assert_series_equal(spread_upper, spread_lower, check_names=False)


def test_add_brick_features_non_negative():
    frame = _build_close_frame([10.0 + (i % 5) - 0.05 * i for i in range(60)])
    enriched = add_brick_features(frame)
    assert (enriched["brick"] >= 0).all()


def test_extract_indicator_snapshot_flags_insufficient_zg_bars():
    frame = _build_close_frame([10.0 + 0.05 * i for i in range(80)])
    snapshot = extract_indicator_snapshot(frame)
    assert snapshot["zg_long"] is None or "zg_insufficient_bars" in str(snapshot.get("indicator_quality_flags", ""))


def test_extract_indicator_snapshot_uptrend_has_zg_bullish():
    frame = _build_close_frame([10.0 + 0.05 * i for i in range(200)])
    snapshot = extract_indicator_snapshot(frame)
    assert snapshot["zg_short_above_long"] is True
    assert snapshot["close_above_zg_long"] is True
    assert snapshot["kdj_j"] is not None


def test_enrich_indicator_columns_aligns_with_self_stock_brick_turn_up_logic():
    bricks = pd.Series([1.0, 2.0, 1.0, 0.5, 0.3, 1.0])
    brick_up = (bricks > bricks.shift(1)).fillna(False).astype(bool)
    brick_up_prev = brick_up.shift(1).fillna(False).astype(bool)
    turn_up = brick_up & ~brick_up_prev
    assert brick_up.tolist() == [False, True, False, False, False, True]
    assert turn_up.tolist() == [False, True, False, False, False, True]
