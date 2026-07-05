# -*- coding: utf-8 -*-
"""Pure capital-flow derived metrics on local moneyflow frames."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from alphasift.flow_specs import (
    DEFAULT_WINDOWS,
    TIER_BUY_AMOUNT_COLUMNS,
    TIER_SELL_AMOUNT_COLUMNS,
    ZSCORE_WINDOW,
)


def enrich_moneyflow_frame(
    frame: pd.DataFrame,
    daily_bars: pd.DataFrame | None = None,
    *,
    windows: tuple[int, ...] = DEFAULT_WINDOWS,
) -> pd.DataFrame:
    """Append derived columns on a trade_date-ascending moneyflow frame."""
    if frame is None or frame.empty:
        return pd.DataFrame()

    result = frame.copy()
    result = _ensure_trade_date_sorted(result)
    result = _ensure_main_net_inflow(result)

    for window in windows:
        column = f"main_net_inflow_{window}d"
        result[column] = result["main_net_inflow"].rolling(window, min_periods=1).sum()

    result["main_inflow_streak"] = _compute_inflow_streak(result["main_net_inflow"])

    zscore_window = max(int(ZSCORE_WINDOW), 2)
    rolling_mean = result["main_net_inflow"].rolling(zscore_window, min_periods=zscore_window).mean()
    rolling_std = result["main_net_inflow"].rolling(zscore_window, min_periods=zscore_window).std()
    result["main_net_inflow_zscore_20d"] = np.where(
        rolling_std > 0,
        (result["main_net_inflow"] - rolling_mean) / rolling_std,
        np.nan,
    )

    result["turnover_amount"] = _approx_turnover_amount(result)
    has_daily = daily_bars is not None and not daily_bars.empty
    if has_daily:
        result = _join_daily_bars(result, daily_bars)
    else:
        result["close"] = np.nan
        result["close_pct"] = np.nan

    with np.errstate(divide="ignore", invalid="ignore"):
        result["main_net_inflow_rate"] = np.where(
            result["turnover_amount"] > 0,
            result["main_net_inflow"] / result["turnover_amount"],
            np.nan,
        )

    if has_daily:
        result["price_up_flow_out"] = (
            result["close_pct"].gt(0) & result["main_net_inflow"].lt(0)
        )
        result["price_down_flow_in"] = (
            result["close_pct"].lt(0) & result["main_net_inflow"].gt(0)
        )
    else:
        result["price_up_flow_out"] = np.nan
        result["price_down_flow_in"] = np.nan
    return result


def build_stock_flow_snapshot(
    moneyflow: pd.DataFrame,
    daily_bars: pd.DataFrame | None,
    *,
    as_of_date: str | None = None,
    windows: tuple[int, ...] = DEFAULT_WINDOWS,
) -> dict[str, Any]:
    """Build point-in-time snapshot fields for the latest or requested trade date."""
    enriched = enrich_moneyflow_frame(moneyflow, daily_bars, windows=windows)
    if enriched.empty:
        return {}

    if as_of_date:
        normalized = _normalize_trade_date(as_of_date)
        subset = enriched[enriched["trade_date"] <= normalized]
        if subset.empty:
            return {}
        row = subset.iloc[-1]
        resolved_as_of = str(row["trade_date"])
    else:
        row = enriched.iloc[-1]
        resolved_as_of = str(row["trade_date"])

    snapshot: dict[str, Any] = {"as_of": resolved_as_of}
    scalar_fields = [
        "main_net_inflow",
        "main_inflow_streak",
        "main_net_inflow_rate",
        "main_net_inflow_zscore_20d",
        "net_mf_amount",
    ]
    for window in windows:
        scalar_fields.append(f"main_net_inflow_{window}d")
    for field in scalar_fields:
        if field in row.index:
            snapshot[field] = _safe_float(row.get(field))

    for field in ("price_up_flow_out", "price_down_flow_in"):
        if field in row.index:
            value = row.get(field)
            snapshot[field] = bool(value) if pd.notna(value) else False

    return snapshot


def _ensure_trade_date_sorted(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    if "trade_date" not in result.columns:
        return result
    result["trade_date"] = result["trade_date"].map(_normalize_trade_date)
    result = result.dropna(subset=["trade_date"])
    return result.sort_values("trade_date").reset_index(drop=True)


def _ensure_main_net_inflow(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    if "main_net_inflow" in result.columns and result["main_net_inflow"].notna().any():
        result["main_net_inflow"] = pd.to_numeric(result["main_net_inflow"], errors="coerce")
        return result

    for column in (*TIER_BUY_AMOUNT_COLUMNS, *TIER_SELL_AMOUNT_COLUMNS):
        if column not in result.columns:
            result[column] = 0.0
        result[column] = pd.to_numeric(result[column], errors="coerce").fillna(0.0)

    result["main_net_inflow"] = (
        result["buy_lg_amount"]
        + result["buy_elg_amount"]
        - result["sell_lg_amount"]
        - result["sell_elg_amount"]
    )
    return result


def _compute_inflow_streak(series: pd.Series) -> pd.Series:
    streak_values: list[int] = []
    current = 0
    for value in pd.to_numeric(series, errors="coerce").fillna(0.0):
        if float(value) > 0:
            current += 1
        else:
            current = 0
        streak_values.append(current)
    return pd.Series(streak_values, index=series.index, dtype="int64")


def _approx_turnover_amount(frame: pd.DataFrame) -> pd.Series:
    buy_total = pd.Series(0.0, index=frame.index, dtype="float64")
    sell_total = pd.Series(0.0, index=frame.index, dtype="float64")
    for column in TIER_BUY_AMOUNT_COLUMNS:
        if column in frame.columns:
            buy_total = buy_total + pd.to_numeric(frame[column], errors="coerce").fillna(0.0)
    for column in TIER_SELL_AMOUNT_COLUMNS:
        if column in frame.columns:
            sell_total = sell_total + pd.to_numeric(frame[column], errors="coerce").fillna(0.0)
    turnover = (buy_total + sell_total) / 2.0
    return turnover.where(turnover > 0)


def _join_daily_bars(moneyflow: pd.DataFrame, daily_bars: pd.DataFrame) -> pd.DataFrame:
    bars = daily_bars.copy()
    date_col = "date" if "date" in bars.columns else "ts" if "ts" in bars.columns else None
    if date_col is None:
        return moneyflow.assign(close=np.nan, close_pct=np.nan)

    bars["trade_date"] = bars[date_col].map(_normalize_trade_date)
    bars["close"] = pd.to_numeric(bars.get("close"), errors="coerce")
    bars["volume"] = pd.to_numeric(bars.get("volume"), errors="coerce")
    bars = bars.dropna(subset=["trade_date"]).sort_values("trade_date")
    bars["close_pct"] = bars["close"].pct_change() * 100.0

    daily_turnover = bars["close"] * bars["volume"] / 10000.0
    bars["daily_turnover"] = daily_turnover.where(daily_turnover > 0)

    merged = moneyflow.merge(
        bars[["trade_date", "close", "close_pct", "daily_turnover"]],
        on="trade_date",
        how="left",
    )
    merged["turnover_amount"] = merged["daily_turnover"].combine_first(merged["turnover_amount"])
    return merged.drop(columns=["daily_turnover"])


def _normalize_trade_date(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return pd.to_datetime(text).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, (np.floating, np.integer)):
        if pd.isna(value):
            return None
        return round(float(value), 4)
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return round(parsed, 4)
