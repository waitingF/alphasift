# -*- coding: utf-8 -*-
"""Flow feature enrichment for narrowed candidate pools."""

from __future__ import annotations

import pandas as pd

from alphasift.daily_store import DailyBarStore, normalize_ts_code
from alphasift.flow_metrics import enrich_moneyflow_frame
from alphasift.flow_store import FlowBarStore

_FLOW_FEATURE_DEFAULTS = {
    "main_net_inflow": pd.NA,
    "main_net_inflow_5d": pd.NA,
    "main_net_inflow_10d": pd.NA,
    "main_net_inflow_20d": pd.NA,
    "main_inflow_streak": pd.NA,
    "main_net_inflow_rate": pd.NA,
    "main_net_inflow_zscore_20d": pd.NA,
    "price_up_flow_out": pd.NA,
    "price_down_flow_in": pd.NA,
    "flow_as_of": "",
    "flow_quality_flags": "missing",
}


def enrich_flow_features(
    df: pd.DataFrame,
    *,
    flow_store: FlowBarStore,
    daily_store: DailyBarStore | None = None,
    lookback_days: int = 60,
    max_rows: int | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Per-row soft-fail flow enrichment; missing data flags rows for hard-filter rejection."""
    if df.empty:
        return df.copy()

    result = df.copy()
    _ensure_flow_columns(result)
    limit = max_rows if max_rows is not None else len(result)
    selected_index = list(result.index[:limit])
    flow_errors: list[str] = []
    quality_flag_counts: dict[str, int] = {}
    success_count = 0
    fetch_failed_codes: list[str] = []

    for idx in selected_index:
        raw_code = str(result.at[idx, "code"] if "code" in result.columns else "").strip()
        if not raw_code:
            _apply_flow_defaults(result, idx, flag="missing")
            quality_flag_counts["missing"] = quality_flag_counts.get("missing", 0) + 1
            continue

        code = raw_code.zfill(6) if raw_code.isdigit() else raw_code
        ts_code = normalize_ts_code(code)
        try:
            moneyflow = flow_store.read(code, lookback_days=lookback_days, end_date=end_date)
            daily_bars = None
            if daily_store is not None:
                try:
                    daily_bars = daily_store.read_history(
                        code,
                        lookback_days=lookback_days,
                        end_date=end_date,
                    )
                except (FileNotFoundError, OSError, RuntimeError):
                    daily_bars = None

            enriched = enrich_moneyflow_frame(moneyflow, daily_bars)
            if enriched.empty:
                raise RuntimeError("empty enriched moneyflow")

            row = enriched.iloc[-1]
            for col, default in _FLOW_FEATURE_DEFAULTS.items():
                if col in ("flow_as_of", "flow_quality_flags"):
                    continue
                result.at[idx, col] = row.get(col, default)

            result.at[idx, "flow_as_of"] = str(row.get("trade_date", ""))
            flags: list[str] = []
            if moneyflow.attrs.get("short_history"):
                flags.append("stale")
                quality_flag_counts["stale"] = quality_flag_counts.get("stale", 0) + 1
            result.at[idx, "flow_quality_flags"] = ";".join(flags)
            success_count += 1
        except Exception as exc:
            _apply_flow_defaults(result, idx, flag="missing")
            quality_flag_counts["missing"] = quality_flag_counts.get("missing", 0) + 1
            fetch_failed_codes.append(ts_code)
            flow_errors.append(f"{ts_code}: {exc}")

    result.attrs["flow_errors"] = flow_errors
    result.attrs["flow_success_count"] = success_count
    result.attrs["flow_fetch_failed_codes"] = fetch_failed_codes
    result.attrs["flow_quality_flag_counts"] = quality_flag_counts
    try:
        result.attrs["flow_store_manifest_last_trade_date"] = str(
            flow_store.manifest().get("last_trade_date", "")
        )
    except (OSError, RuntimeError):
        pass
    return result


def _ensure_flow_columns(frame: pd.DataFrame) -> None:
    for col, default in _FLOW_FEATURE_DEFAULTS.items():
        if col not in frame.columns:
            frame[col] = pd.NA if col not in {"flow_as_of", "flow_quality_flags"} else ""


def _apply_flow_defaults(frame: pd.DataFrame, idx: object, *, flag: str) -> None:
    for col, default in _FLOW_FEATURE_DEFAULTS.items():
        frame.at[idx, col] = default if col != "flow_quality_flags" else flag
