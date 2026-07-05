# -*- coding: utf-8 -*-
"""Capital-flow condition evaluation for hard filtering."""

from __future__ import annotations

from typing import Any

import pandas as pd

from alphasift.flow_metrics import enrich_moneyflow_frame, _normalize_trade_date, _safe_float

FLOW_CONDITION_MAIN_INFLOW_STREAK = "main_inflow_streak_gte"
FLOW_CONDITION_MAIN_NET_INFLOW_5D_GT = "main_net_inflow_5d_gt"
FLOW_CONDITION_NO_PRICE_UP_FLOW_OUT = "no_price_up_flow_out"
FLOW_CONDITION_MAIN_NET_INFLOW_RATE_GTE = "main_net_inflow_rate_gte"


def evaluate_flow_conditions(
    moneyflow_frame: pd.DataFrame,
    daily_frame: pd.DataFrame | None,
    conditions: list[dict[str, Any]],
    *,
    as_of_date: str | None = None,
) -> dict[str, Any] | None:
    """Return flow snapshot when all conditions pass; None otherwise."""
    if not conditions:
        return None
    if moneyflow_frame is None or moneyflow_frame.empty:
        return None

    enriched = enrich_moneyflow_frame(moneyflow_frame, daily_frame)
    if enriched.empty:
        return None

    if as_of_date:
        normalized = _normalize_trade_date(as_of_date)
        if normalized:
            subset = enriched[enriched["trade_date"] <= normalized]
            if subset.empty:
                return None
            row = subset.iloc[-1]
            resolved_as_of = str(row["trade_date"])
        else:
            row = enriched.iloc[-1]
            resolved_as_of = str(row["trade_date"])
    else:
        row = enriched.iloc[-1]
        resolved_as_of = str(row["trade_date"])

    snapshot = _snapshot_from_row(row, resolved_as_of)
    for condition in conditions:
        if not _evaluate_one_condition(condition, snapshot):
            return None

    return {"snapshot": snapshot, "as_of": resolved_as_of}


def _evaluate_one_condition(condition: dict[str, Any], snapshot: dict[str, Any]) -> bool:
    cond_id = condition["id"]
    params = condition.get("params") or {}

    if cond_id == FLOW_CONDITION_MAIN_INFLOW_STREAK:
        days = int(params.get("days", 5))
        streak = snapshot.get("main_inflow_streak")
        return streak is not None and int(streak) >= days

    if cond_id == FLOW_CONDITION_MAIN_NET_INFLOW_5D_GT:
        threshold = float(params.get("threshold", 0))
        value = snapshot.get("main_net_inflow_5d")
        return value is not None and float(value) > threshold

    if cond_id == FLOW_CONDITION_NO_PRICE_UP_FLOW_OUT:
        return not bool(snapshot.get("price_up_flow_out"))

    if cond_id == FLOW_CONDITION_MAIN_NET_INFLOW_RATE_GTE:
        threshold = float(params.get("threshold", 0))
        value = snapshot.get("main_net_inflow_rate")
        return value is not None and float(value) >= threshold

    raise ValueError(f"未知资金面条件: {cond_id!r}")


def _snapshot_from_row(row: pd.Series, as_of: str) -> dict[str, Any]:
    snapshot: dict[str, Any] = {"as_of": as_of}
    for field in (
        "main_net_inflow",
        "main_net_inflow_5d",
        "main_net_inflow_10d",
        "main_net_inflow_20d",
        "main_inflow_streak",
        "main_net_inflow_rate",
        "main_net_inflow_zscore_20d",
        "net_mf_amount",
        "close_pct",
    ):
        if field in row.index:
            snapshot[field] = _safe_float(row.get(field))

    for field in ("price_up_flow_out", "price_down_flow_in"):
        if field in row.index:
            value = row.get(field)
            snapshot[field] = bool(value) if pd.notna(value) else False

    return snapshot
