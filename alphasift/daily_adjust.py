# -*- coding: utf-8 -*-
"""Pure adjustment helpers for local daily bar storage."""

from __future__ import annotations

import pandas as pd


def apply_adj(
    raw: pd.DataFrame,
    factors: pd.DataFrame,
    *,
    adj: str,
) -> pd.DataFrame:
    """Apply Tushare-style forward/backward adjustment to unadjusted OHLCV.

    Numerically equivalent to ``alphasift.daily._apply_tushare_adjustment`` when
    ``raw`` uses a ``date`` column and ``factors`` uses ``date`` + ``adj_factor``.
    """
    if raw.empty:
        return raw.copy()

    raw_df = raw.copy()
    factor_df = factors.copy()
    date_col = "date"
    if date_col not in raw_df.columns:
        raise RuntimeError("raw daily history missing date column")
    if factor_df.empty or "adj_factor" not in factor_df.columns:
        raise RuntimeError("adj_factor series is empty")

    raw_df[date_col] = raw_df[date_col].astype(str)
    factor_df[date_col] = factor_df[date_col].astype(str)
    merged = raw_df.merge(factor_df[[date_col, "adj_factor"]], on=date_col, how="left")
    merged = merged.sort_values(date_col)
    merged["adj_factor"] = pd.to_numeric(merged["adj_factor"], errors="coerce").bfill()
    sorted_factors = factor_df.sort_values(date_col)
    valid_factors = pd.to_numeric(sorted_factors["adj_factor"], errors="coerce").dropna()
    if valid_factors.empty:
        raise RuntimeError("adj_factor invalid")
    latest_factor = float(valid_factors.iloc[-1])

    result = merged.drop(columns=["adj_factor"], errors="ignore").copy()
    for col in ("open", "high", "low", "close"):
        if col not in result.columns:
            continue
        result[col] = pd.to_numeric(result[col], errors="coerce")
        if adj == "hfq":
            result[col] = result[col] * merged["adj_factor"]
        else:
            result[col] = result[col] * merged["adj_factor"] / latest_factor
    return result
