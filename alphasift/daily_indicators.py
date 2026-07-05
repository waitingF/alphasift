# -*- coding: utf-8 -*-
"""TDX-style daily indicators ported from self-stock-project (pure pandas, no I/O)."""

from __future__ import annotations

from typing import Any

import pandas as pd

ZG_SHORT_DEFAULT_SPAN = 10
ZG_LONG_DEFAULT_WINDOWS: tuple[int, ...] = (14, 28, 57, 114)
ZG_MIN_BARS = 114


def compute_zg_short(close: pd.Series, span: int = ZG_SHORT_DEFAULT_SPAN) -> pd.Series:
    """TDX formula: EMA(EMA(C,N),N), default N=10."""
    series = close.astype(float)
    ema1 = series.ewm(span=span, adjust=False).mean()
    return ema1.ewm(span=span, adjust=False).mean()


def compute_zg_long(
    close: pd.Series,
    windows: tuple[int, ...] = ZG_LONG_DEFAULT_WINDOWS,
) -> pd.Series:
    """TDX formula: (MA14 + MA28 + MA57 + MA114) / 4."""
    series = close.astype(float)
    ma_frame = pd.concat(
        [series.rolling(window=w, min_periods=w).mean() for w in windows],
        axis=1,
    )
    return ma_frame.mean(axis=1, skipna=False)


def add_kdj_features(
    df: pd.DataFrame,
    *,
    n: int = 9,
    k_period: int = 3,
    d_period: int = 3,
) -> pd.DataFrame:
    result = df.copy()
    low_min = result["low"].rolling(window=n, min_periods=1).min()
    high_max = result["high"].rolling(window=n, min_periods=1).max()
    denominator = (high_max - low_min).replace(0, 1.0)
    rsv = (result["close"] - low_min) / denominator * 100
    result["kdj_k"] = rsv.ewm(alpha=1 / max(k_period, 1), adjust=False).mean()
    result["kdj_d"] = result["kdj_k"].ewm(alpha=1 / max(d_period, 1), adjust=False).mean()
    result["kdj_j"] = 3 * result["kdj_k"] - 2 * result["kdj_d"]
    return result


def add_boll_features(
    df: pd.DataFrame,
    *,
    window: int = 20,
    num_std: float = 2.0,
) -> pd.DataFrame:
    result = df.copy()
    close = result["close"].astype(float)
    middle = close.rolling(window=window, min_periods=1).mean()
    deviation = close.rolling(window=window, min_periods=1).std(ddof=0).fillna(0.0)
    result["boll_mid"] = middle
    result["boll_upper"] = middle + num_std * deviation
    result["boll_lower"] = middle - num_std * deviation
    return result


def add_zg_features(
    df: pd.DataFrame,
    *,
    short_span: int = ZG_SHORT_DEFAULT_SPAN,
    long_windows: tuple[int, ...] = ZG_LONG_DEFAULT_WINDOWS,
) -> pd.DataFrame:
    result = df.copy()
    close = result["close"].astype(float)
    result["zg_short"] = compute_zg_short(close, span=short_span)
    result["zg_long"] = compute_zg_long(close, windows=long_windows)
    return result


def add_brick_features(
    df: pd.DataFrame,
    *,
    window: int = 4,
    sma_a: int = 4,
    sma_b: int = 6,
) -> pd.DataFrame:
    """TDX brick sub-chart indicator (SMA(X,N,M) as EWM(alpha=M/N))."""
    result = df.copy()
    high = result["high"].astype(float)
    low = result["low"].astype(float)
    close = result["close"].astype(float)

    hhv = high.rolling(window=window, min_periods=1).max()
    llv = low.rolling(window=window, min_periods=1).min()
    span = (hhv - llv).replace(0, 1.0)

    var1a = (hhv - close) / span * 100 - 90
    var3a = (close - llv) / span * 100

    var2a = var1a.ewm(alpha=1.0 / sma_a, adjust=False).mean() + 100
    var4a = var3a.ewm(alpha=1.0 / sma_b, adjust=False).mean()
    var5a = var4a.ewm(alpha=1.0 / sma_b, adjust=False).mean() + 100
    var6a = var5a - var2a

    brick = (var6a - 4).where(var6a > 4, 0.0)
    result["brick"] = brick

    brick_up = brick.gt(brick.shift(1)).fillna(False).astype(bool)
    brick_up_prev = brick_up.shift(1, fill_value=False).astype(bool)
    result["brick_up"] = brick_up
    result["brick_core_turn_up"] = brick_up & ~brick_up_prev
    return result


def add_brick_strategy_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Composite brick-turn-up signal aligned with self-stock XG selection."""
    result = frame.copy()
    brick = result["brick"].astype(float)
    prev_brick = brick.shift(1)
    prev_prev_brick = brick.shift(2)
    prev_close = result["close"].astype(float).shift(1)

    result["brick_today_red"] = brick.gt(prev_brick).fillna(False).astype(bool)
    result["brick_yesterday_green"] = prev_brick.lt(prev_prev_brick).fillna(False).astype(bool)
    result["brick_red_height"] = brick - prev_brick
    result["brick_green_height"] = prev_prev_brick - prev_brick
    result["brick_height_ok"] = (
        result["brick_red_height"].ge(result["brick_green_height"]).fillna(False).astype(bool)
    )

    daily_change = ((result["close"] - prev_close) / prev_close * 100).where(
        prev_close.notna() & prev_close.ne(0)
    )
    result["daily_change_pct"] = daily_change

    result["brick_turn_up"] = (
        result["brick_yesterday_green"]
        & result["brick_today_red"]
        & result["brick_height_ok"]
        & result["zg_short"].gt(result["zg_long"]).fillna(False)
        & daily_change.le(5).fillna(False)
    )
    return result


def enrich_indicator_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Attach KDJ/BOLL/ZG/brick indicator columns to a normalized OHLCV frame."""
    if df.empty or "close" not in df.columns:
        return df.copy()
    result = add_kdj_features(df)
    result = add_boll_features(result)
    result = add_zg_features(result)
    result = add_brick_features(result)
    return add_brick_strategy_features(result)


def _last_float(value: Any) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return round(float(value), 4)
    except (TypeError, ValueError):
        return None


def _last_bool(value: Any) -> bool | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return bool(value)
    except (TypeError, ValueError):
        return None


def extract_indicator_snapshot(df: pd.DataFrame) -> dict[str, object]:
    """Compute indicator columns and return last-bar snapshot for daily enrichment."""
    normalized = enrich_indicator_columns(df)
    if normalized.empty:
        return {}

    last = normalized.iloc[-1]
    prev = normalized.iloc[-2] if len(normalized) >= 2 else None

    prev_close = _last_float(prev["close"]) if prev is not None else None
    last_close = _last_float(last.get("close"))
    last_high = _last_float(last.get("high"))
    last_low = _last_float(last.get("low"))
    prev_volume = _last_float(prev.get("volume")) if prev is not None else None
    last_volume = _last_float(last.get("volume"))

    daily_change_pct = _last_float(last.get("daily_change_pct"))
    if daily_change_pct is None and prev_close not in (None, 0) and last_close is not None:
        daily_change_pct = round((last_close - prev_close) / prev_close * 100, 4)

    daily_amplitude_pct = None
    if prev_close not in (None, 0) and last_high is not None and last_low is not None:
        daily_amplitude_pct = round((last_high - last_low) / prev_close * 100, 4)

    zg_short = _last_float(last.get("zg_short"))
    zg_long = _last_float(last.get("zg_long"))
    kdj_k = _last_float(last.get("kdj_k"))
    kdj_d = _last_float(last.get("kdj_d"))
    kdj_j = _last_float(last.get("kdj_j"))
    prev_kdj_j = _last_float(prev.get("kdj_j")) if prev is not None else None
    prev_kdj_k = _last_float(prev.get("kdj_k")) if prev is not None else None
    prev_kdj_d = _last_float(prev.get("kdj_d")) if prev is not None else None
    boll_lower = _last_float(last.get("boll_lower"))
    boll_upper = _last_float(last.get("boll_upper"))

    zg_short_above_long = (
        zg_short is not None and zg_long is not None and zg_short > zg_long
    )
    close_above_zg_long = (
        last_close is not None and zg_long is not None and last_close > zg_long
    )
    close_below_boll_lower = (
        last_close is not None and boll_lower is not None and last_close < boll_lower
    )
    close_above_boll_upper = (
        last_close is not None and boll_upper is not None and last_close > boll_upper
    )
    kdj_golden_cross = (
        prev_kdj_k is not None
        and prev_kdj_d is not None
        and kdj_k is not None
        and kdj_d is not None
        and prev_kdj_k <= prev_kdj_d
        and kdj_k > kdj_d
    )
    volume_above_prev = (
        last_volume is not None and prev_volume is not None and last_volume > prev_volume
    )

    flags: list[str] = []
    close_series = pd.to_numeric(normalized["close"], errors="coerce").dropna()
    if len(close_series) < ZG_MIN_BARS or zg_long is None:
        flags.append("zg_insufficient_bars")

    return {
        "zg_short": zg_short,
        "zg_long": zg_long,
        "kdj_k": kdj_k,
        "kdj_d": kdj_d,
        "kdj_j": kdj_j,
        "prev_kdj_j": prev_kdj_j,
        "boll_mid": _last_float(last.get("boll_mid")),
        "boll_upper": boll_upper,
        "boll_lower": boll_lower,
        "brick": _last_float(last.get("brick")),
        "daily_change_pct": daily_change_pct,
        "daily_amplitude_pct": daily_amplitude_pct,
        "volume_above_prev": volume_above_prev,
        "zg_short_above_long": zg_short_above_long,
        "close_above_zg_long": close_above_zg_long,
        "close_below_boll_lower": close_below_boll_lower,
        "close_above_boll_upper": close_above_boll_upper,
        "kdj_golden_cross": kdj_golden_cross,
        "brick_turn_up": _last_bool(last.get("brick_turn_up")),
        "indicator_quality_flags": ";".join(flags),
    }
