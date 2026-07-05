# -*- coding: utf-8 -*-
"""Feature extraction for pattern similarity."""

from __future__ import annotations

import numpy as np
import pandas as pd

from alphasift.pattern.math_utils import clip_frame, normalize_frame


def normalize_bars(bars: list[dict] | pd.DataFrame) -> pd.DataFrame:
    if isinstance(bars, pd.DataFrame):
        df = bars.copy()
    else:
        df = pd.DataFrame(list(bars))
    rename_map = {
        "ts": "date",
        "trade_date": "date",
        "日期": "date",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "volume",
    }
    df = df.rename(columns=rename_map)
    if "date" in df.columns:
        df = df.sort_values("date")
    for col in ("open", "high", "low", "close", "volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "close" not in df.columns:
        raise ValueError("bars must include close prices")
    df = df.dropna(subset=["close"]).copy()
    for col in ("open", "high", "low"):
        if col not in df.columns:
            df[col] = df["close"]
        else:
            df[col] = df[col].fillna(df["close"])
    if "volume" not in df.columns:
        df["volume"] = 0.0
    else:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
    return df.reset_index(drop=True)


def build_feature_frame(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    first_close = float(result["close"].iloc[0]) if not result.empty else 1.0
    base_close = first_close if first_close != 0 else 1.0

    result["close_return"] = result["close"] / base_close - 1.0
    result["body"] = (result["close"] - result["open"]) / base_close
    result["upper_shadow"] = (
        result["high"] - result[["open", "close"]].max(axis=1)
    ) / base_close
    result["lower_shadow"] = (
        result[["open", "close"]].min(axis=1) - result["low"]
    ) / base_close
    result["amplitude"] = (result["high"] - result["low"]) / base_close
    result["pct_change"] = result["close"].pct_change().fillna(0.0)
    result["log_volume"] = np.log1p(result["volume"].clip(lower=0))
    result["pct_change_mean"] = result["pct_change"].expanding(min_periods=1).mean()
    result["pct_change_std"] = result["pct_change"].expanding(min_periods=1).std(ddof=0).fillna(0.0)
    result["trend_slope"] = _rolling_trend_slope(result["close"], window=5)
    return result


def build_feature_matrix(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    feature_df = build_feature_frame(df)
    feature_config = config["feature"]
    ordered_columns = (
        feature_config["price_features"]
        + feature_config["volume_features"]
        + feature_config.get("stat_features", [])
    )
    available_columns = [column for column in ordered_columns if column in feature_df.columns]
    matrix = feature_df[available_columns].copy().fillna(0.0)
    matrix = normalize_frame(matrix, feature_config.get("normalize_method", "zscore"))
    matrix = clip_frame(matrix, feature_config.get("clip_outlier"))
    return matrix


def compare_feature_frames(left: pd.DataFrame, right: pd.DataFrame, config: dict) -> dict:
    from alphasift.pattern.metrics import compare_series

    feature_config = config["feature"]
    price_cols = _existing_columns(left, right, feature_config["price_features"])
    volume_cols = _existing_columns(left, right, feature_config["volume_features"])
    stat_cols = _existing_columns(left, right, feature_config.get("stat_features", []))

    price_result = _compare_group(left, right, price_cols, config)
    volume_result = _compare_group(left, right, volume_cols, config)
    stat_result = _compare_group(left, right, stat_cols, config)

    weights = config["similarity"]["weights"]
    total_similarity = (
        weights["price"] * price_result["similarity"]
        + weights["volume"] * volume_result["similarity"]
        + weights["stat"] * stat_result["similarity"]
    )
    return {
        "price_similarity": float(price_result["similarity"]),
        "volume_similarity": float(volume_result["similarity"]),
        "stat_similarity": float(stat_result["similarity"]),
        "total_similarity": float(total_similarity),
        "alignment_info": {
            "price": price_result.get("alignment_info"),
            "volume": volume_result.get("alignment_info"),
            "stat": stat_result.get("alignment_info"),
        },
    }


def _compare_group(left: pd.DataFrame, right: pd.DataFrame, columns: list[str], config: dict) -> dict:
    from alphasift.pattern.metrics import compare_series

    if not columns:
        return {"similarity": 0.0, "distance": 0.0, "alignment_info": None}
    left_values = left[columns].to_numpy(dtype=float)
    right_values = right[columns].to_numpy(dtype=float)
    return compare_series(left_values, right_values, config)


def _existing_columns(left: pd.DataFrame, right: pd.DataFrame, columns: list[str]) -> list[str]:
    return [column for column in columns if column in left.columns and column in right.columns]


def _rolling_trend_slope(series: pd.Series, window: int) -> pd.Series:
    if len(series) == 0:
        return pd.Series(dtype=float)
    slopes: list[float] = []
    values = series.to_numpy(dtype=float)
    for idx in range(len(values)):
        start = max(0, idx - window + 1)
        current = values[start : idx + 1]
        if len(current) < 2:
            slopes.append(0.0)
            continue
        x = np.arange(len(current), dtype=float)
        slope, _ = np.polyfit(x, current, 1)
        slopes.append(float(slope))
    return pd.Series(slopes, index=series.index, dtype=float)
