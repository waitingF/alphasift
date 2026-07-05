# -*- coding: utf-8 -*-
"""L1 hard filter — apply strategy hard_filters to snapshot DataFrame."""

import logging
from dataclasses import replace

import pandas as pd

from alphasift.models import HardFilterConfig

logger = logging.getLogger(__name__)
_DAILY_FILTER_DEFAULTS = {
    "change_60d_min": None,
    "change_60d_max": None,
    "require_ma_bullish": False,
    "require_price_above_ma20": False,
    "signal_score_min": None,
    "macd_status_whitelist": None,
    "rsi_status_whitelist": None,
    "breakout_20d_pct_min": None,
    "breakout_20d_pct_max": None,
    "range_20d_pct_max": None,
    "volume_ratio_20d_min": None,
    "volume_ratio_20d_max": None,
    "body_pct_min": None,
    "body_pct_max": None,
    "pullback_to_ma20_pct_min": None,
    "pullback_to_ma20_pct_max": None,
    "consolidation_days_20d_min": None,
    "consolidation_days_20d_max": None,
    "volatility_20d_pct_min": None,
    "volatility_20d_pct_max": None,
    "max_drawdown_20d_pct_min": None,
    "max_drawdown_20d_pct_max": None,
    "atr_20_pct_min": None,
    "atr_20_pct_max": None,
    "kdj_j_max": None,
    "kdj_j_min": None,
    "prev_kdj_j_max": None,
    "require_kdj_golden_cross": False,
    "require_zg_short_above_long": False,
    "require_close_above_zg_long": False,
    "require_close_below_boll_lower": False,
    "require_close_above_boll_upper": False,
    "daily_amplitude_max": None,
    "daily_change_min": None,
    "daily_change_max": None,
    "require_volume_above_prev": False,
    "require_brick_turn_up": False,
}
_FLOW_FILTER_DEFAULTS = {
    "main_inflow_streak_min": None,
    "main_net_inflow_5d_min": None,
    "main_net_inflow_min": None,
    "main_net_inflow_rate_min": None,
    "require_no_price_up_flow_out": False,
}


class SnapshotFieldMissingError(ValueError):
    """Raised when a configured hard filter cannot be evaluated safely."""


def apply_hard_filters(df: pd.DataFrame, filters: HardFilterConfig) -> pd.DataFrame:
    """Filter snapshot DataFrame by hard conditions. Returns filtered copy."""
    result = df.copy()
    if result.empty:
        return result

    mask = pd.Series(True, index=result.index)

    if filters.exclude_st:
        name_col = _find_col(result, ["name", "股票名称", "名称"]) if mask.any() else None
        if not name_col:
            raise SnapshotFieldMissingError(
                "Missing required snapshot column for exclude_st filter: name"
            )
        mask &= ~result[name_col].str.contains(r"ST|退", na=False)

    # Numeric filters — each is optional
    mask = _filter_min(result, mask, ["amount", "成交额"], filters.amount_min)
    mask = _filter_min(result, mask, ["price", "最新价", "现价"], filters.price_min)
    mask = _filter_max(result, mask, ["price", "最新价", "现价"], filters.price_max)
    mask = _filter_min(result, mask, ["total_mv", "总市值"], filters.market_cap_min)
    mask = _filter_max(result, mask, ["total_mv", "总市值"], filters.market_cap_max)
    mask = _filter_min(result, mask, ["pe_ratio", "市盈率"], filters.pe_ttm_min)
    mask = _filter_max(result, mask, ["pe_ratio", "市盈率"], filters.pe_ttm_max)
    mask = _filter_min(result, mask, ["pb_ratio", "市净率"], filters.pb_min)
    mask = _filter_max(result, mask, ["pb_ratio", "市净率"], filters.pb_max)
    mask = _filter_min(result, mask, ["volume_ratio", "量比"], filters.volume_ratio_min)
    mask = _filter_min(result, mask, ["turnover_rate", "换手率"], filters.turnover_rate_min)
    mask = _filter_min(result, mask, ["change_pct", "涨跌幅"], filters.change_pct_min)
    mask = _filter_max(result, mask, ["change_pct", "涨跌幅"], filters.change_pct_max)

    mask = _filter_min(result, mask, ["change_60d"], filters.change_60d_min)
    mask = _filter_max(result, mask, ["change_60d"], filters.change_60d_max)
    mask = _filter_bool_true(result, mask, "ma_bullish", filters.require_ma_bullish)
    mask = _filter_bool_true(result, mask, "price_above_ma20", filters.require_price_above_ma20)
    mask = _filter_min(result, mask, ["signal_score"], filters.signal_score_min)
    mask = _filter_in(result, mask, "macd_status", filters.macd_status_whitelist)
    mask = _filter_in(result, mask, "rsi_status", filters.rsi_status_whitelist)
    mask = _filter_min(result, mask, ["breakout_20d_pct"], filters.breakout_20d_pct_min)
    mask = _filter_max(result, mask, ["breakout_20d_pct"], filters.breakout_20d_pct_max)
    mask = _filter_max(result, mask, ["range_20d_pct"], filters.range_20d_pct_max)
    mask = _filter_min(result, mask, ["volume_ratio_20d"], filters.volume_ratio_20d_min)
    mask = _filter_max(result, mask, ["volume_ratio_20d"], filters.volume_ratio_20d_max)
    mask = _filter_min(result, mask, ["body_pct"], filters.body_pct_min)
    mask = _filter_max(result, mask, ["body_pct"], filters.body_pct_max)
    mask = _filter_min(result, mask, ["pullback_to_ma20_pct"], filters.pullback_to_ma20_pct_min)
    mask = _filter_max(result, mask, ["pullback_to_ma20_pct"], filters.pullback_to_ma20_pct_max)
    mask = _filter_min(result, mask, ["consolidation_days_20d"], filters.consolidation_days_20d_min)
    mask = _filter_max(result, mask, ["consolidation_days_20d"], filters.consolidation_days_20d_max)
    mask = _filter_min(result, mask, ["volatility_20d_pct"], filters.volatility_20d_pct_min)
    mask = _filter_max(result, mask, ["volatility_20d_pct"], filters.volatility_20d_pct_max)
    mask = _filter_min(result, mask, ["max_drawdown_20d_pct"], filters.max_drawdown_20d_pct_min)
    mask = _filter_max(result, mask, ["max_drawdown_20d_pct"], filters.max_drawdown_20d_pct_max)
    mask = _filter_min(result, mask, ["atr_20_pct"], filters.atr_20_pct_min)
    mask = _filter_max(result, mask, ["atr_20_pct"], filters.atr_20_pct_max)

    mask = _filter_max(result, mask, ["kdj_j"], filters.kdj_j_max)
    mask = _filter_min(result, mask, ["kdj_j"], filters.kdj_j_min)
    mask = _filter_max(result, mask, ["prev_kdj_j"], filters.prev_kdj_j_max)
    mask = _filter_bool_true(result, mask, "kdj_golden_cross", filters.require_kdj_golden_cross)
    mask = _filter_bool_true(result, mask, "zg_short_above_long", filters.require_zg_short_above_long)
    mask = _filter_bool_true(result, mask, "close_above_zg_long", filters.require_close_above_zg_long)
    mask = _filter_bool_true(result, mask, "close_below_boll_lower", filters.require_close_below_boll_lower)
    mask = _filter_bool_true(result, mask, "close_above_boll_upper", filters.require_close_above_boll_upper)
    mask = _filter_max(result, mask, ["daily_amplitude_pct"], filters.daily_amplitude_max)
    mask = _filter_min(result, mask, ["daily_change_pct"], filters.daily_change_min)
    mask = _filter_max(result, mask, ["daily_change_pct"], filters.daily_change_max)
    mask = _filter_bool_true(result, mask, "volume_above_prev", filters.require_volume_above_prev)
    mask = _filter_bool_true(result, mask, "brick_turn_up", filters.require_brick_turn_up)

    mask = _filter_min(result, mask, ["main_inflow_streak"], filters.main_inflow_streak_min)
    mask = _filter_min(result, mask, ["main_net_inflow_5d"], filters.main_net_inflow_5d_min)
    mask = _filter_min(result, mask, ["main_net_inflow"], filters.main_net_inflow_min)
    mask = _filter_min(result, mask, ["main_net_inflow_rate"], filters.main_net_inflow_rate_min)
    mask = _filter_bool_false(result, mask, "price_up_flow_out", filters.require_no_price_up_flow_out)

    return result.loc[mask].copy()


def hard_filter_rejection_summary(
    df: pd.DataFrame,
    filters: HardFilterConfig,
    *,
    limit: int = 8,
) -> list[str]:
    """Return compact sequential hard-filter rejection counts."""
    if df.empty or limit <= 0:
        return []

    mask = pd.Series(True, index=df.index)
    diagnostics: list[str] = []

    def record(label: str, next_mask: pd.Series) -> None:
        nonlocal mask
        before = int(mask.sum())
        after = int(next_mask.sum())
        removed = before - after
        if removed > 0 and len(diagnostics) < limit:
            diagnostics.append(f"{label} removed {removed} ({before}->{after})")
        mask = next_mask

    if filters.exclude_st:
        name_col = _find_col(df, ["name", "股票名称", "名称"]) if mask.any() else None
        if not name_col:
            raise SnapshotFieldMissingError(
                "Missing required snapshot column for exclude_st filter: name"
            )
        record("exclude_st", mask & ~df[name_col].str.contains(r"ST|退", na=False))

    def record_min(label: str, columns: list[str], value: float | None) -> None:
        if value is not None:
            record(label, _filter_min(df, mask, columns, value))

    def record_max(label: str, columns: list[str], value: float | None) -> None:
        if value is not None:
            record(label, _filter_max(df, mask, columns, value))

    record_min("amount_min", ["amount", "成交额"], filters.amount_min)
    record_min("price_min", ["price", "最新价", "现价"], filters.price_min)
    record_max("price_max", ["price", "最新价", "现价"], filters.price_max)
    record_min("market_cap_min", ["total_mv", "总市值"], filters.market_cap_min)
    record_max("market_cap_max", ["total_mv", "总市值"], filters.market_cap_max)
    record_min("pe_ttm_min", ["pe_ratio", "市盈率"], filters.pe_ttm_min)
    record_max("pe_ttm_max", ["pe_ratio", "市盈率"], filters.pe_ttm_max)
    record_min("pb_min", ["pb_ratio", "市净率"], filters.pb_min)
    record_max("pb_max", ["pb_ratio", "市净率"], filters.pb_max)
    record_min("volume_ratio_min", ["volume_ratio", "量比"], filters.volume_ratio_min)
    record_min("turnover_rate_min", ["turnover_rate", "换手率"], filters.turnover_rate_min)
    record_min("change_pct_min", ["change_pct", "涨跌幅"], filters.change_pct_min)
    record_max("change_pct_max", ["change_pct", "涨跌幅"], filters.change_pct_max)
    record_min("change_60d_min", ["change_60d"], filters.change_60d_min)
    record_max("change_60d_max", ["change_60d"], filters.change_60d_max)

    if filters.require_ma_bullish:
        record("require_ma_bullish", _filter_bool_true(df, mask, "ma_bullish", True))
    if filters.require_price_above_ma20:
        record("require_price_above_ma20", _filter_bool_true(df, mask, "price_above_ma20", True))

    record_min("signal_score_min", ["signal_score"], filters.signal_score_min)
    if filters.macd_status_whitelist:
        record("macd_status_whitelist", _filter_in(df, mask, "macd_status", filters.macd_status_whitelist))
    if filters.rsi_status_whitelist:
        record("rsi_status_whitelist", _filter_in(df, mask, "rsi_status", filters.rsi_status_whitelist))
    record_min("breakout_20d_pct_min", ["breakout_20d_pct"], filters.breakout_20d_pct_min)
    record_max("breakout_20d_pct_max", ["breakout_20d_pct"], filters.breakout_20d_pct_max)
    record_max("range_20d_pct_max", ["range_20d_pct"], filters.range_20d_pct_max)
    record_min("volume_ratio_20d_min", ["volume_ratio_20d"], filters.volume_ratio_20d_min)
    record_max("volume_ratio_20d_max", ["volume_ratio_20d"], filters.volume_ratio_20d_max)
    record_min("body_pct_min", ["body_pct"], filters.body_pct_min)
    record_max("body_pct_max", ["body_pct"], filters.body_pct_max)
    record_min("pullback_to_ma20_pct_min", ["pullback_to_ma20_pct"], filters.pullback_to_ma20_pct_min)
    record_max("pullback_to_ma20_pct_max", ["pullback_to_ma20_pct"], filters.pullback_to_ma20_pct_max)
    record_min("consolidation_days_20d_min", ["consolidation_days_20d"], filters.consolidation_days_20d_min)
    record_max("consolidation_days_20d_max", ["consolidation_days_20d"], filters.consolidation_days_20d_max)
    record_min("volatility_20d_pct_min", ["volatility_20d_pct"], filters.volatility_20d_pct_min)
    record_max("volatility_20d_pct_max", ["volatility_20d_pct"], filters.volatility_20d_pct_max)
    record_min("max_drawdown_20d_pct_min", ["max_drawdown_20d_pct"], filters.max_drawdown_20d_pct_min)
    record_max("max_drawdown_20d_pct_max", ["max_drawdown_20d_pct"], filters.max_drawdown_20d_pct_max)
    record_min("atr_20_pct_min", ["atr_20_pct"], filters.atr_20_pct_min)
    record_max("atr_20_pct_max", ["atr_20_pct"], filters.atr_20_pct_max)
    record_max("kdj_j_max", ["kdj_j"], filters.kdj_j_max)
    record_min("kdj_j_min", ["kdj_j"], filters.kdj_j_min)
    record_max("prev_kdj_j_max", ["prev_kdj_j"], filters.prev_kdj_j_max)
    if filters.require_kdj_golden_cross:
        record("require_kdj_golden_cross", _filter_bool_true(df, mask, "kdj_golden_cross", True))
    if filters.require_zg_short_above_long:
        record("require_zg_short_above_long", _filter_bool_true(df, mask, "zg_short_above_long", True))
    if filters.require_close_above_zg_long:
        record("require_close_above_zg_long", _filter_bool_true(df, mask, "close_above_zg_long", True))
    if filters.require_close_below_boll_lower:
        record("require_close_below_boll_lower", _filter_bool_true(df, mask, "close_below_boll_lower", True))
    if filters.require_close_above_boll_upper:
        record("require_close_above_boll_upper", _filter_bool_true(df, mask, "close_above_boll_upper", True))
    record_max("daily_amplitude_max", ["daily_amplitude_pct"], filters.daily_amplitude_max)
    record_min("daily_change_min", ["daily_change_pct"], filters.daily_change_min)
    record_max("daily_change_max", ["daily_change_pct"], filters.daily_change_max)
    if filters.require_volume_above_prev:
        record("require_volume_above_prev", _filter_bool_true(df, mask, "volume_above_prev", True))
    if filters.require_brick_turn_up:
        record("require_brick_turn_up", _filter_bool_true(df, mask, "brick_turn_up", True))
    record_min("main_inflow_streak_min", ["main_inflow_streak"], filters.main_inflow_streak_min)
    record_min("main_net_inflow_5d_min", ["main_net_inflow_5d"], filters.main_net_inflow_5d_min)
    record_min("main_net_inflow_min", ["main_net_inflow"], filters.main_net_inflow_min)
    record_min("main_net_inflow_rate_min", ["main_net_inflow_rate"], filters.main_net_inflow_rate_min)
    if filters.require_no_price_up_flow_out:
        record("require_no_price_up_flow_out", _filter_bool_false(df, mask, "price_up_flow_out", True))

    return diagnostics


def hard_filter_waterfall(
    df: pd.DataFrame,
    filters: HardFilterConfig,
    *,
    sample_limit: int = 3,
) -> list[dict[str, object]]:
    """Return sequential hard-filter counts plus rejected row samples."""
    if df.empty:
        return []

    mask = pd.Series(True, index=df.index)
    steps: list[dict[str, object]] = []

    def record(label: str, next_mask: pd.Series, value_columns: list[str] | None = None) -> None:
        nonlocal mask
        before = int(mask.sum())
        after = int(next_mask.sum())
        removed = before - after
        step: dict[str, object] = {
            "filter": label,
            "before": before,
            "after": after,
            "removed": removed,
            "removed_pct": round((removed / before * 100.0), 4) if before else 0.0,
        }
        if removed > 0 and sample_limit > 0:
            step["samples"] = _rejection_samples(
                df.loc[mask & ~next_mask],
                value_columns=value_columns or [],
                limit=sample_limit,
            )
        suggestion = _waterfall_suggestion(label, before, after, removed)
        if suggestion:
            step["suggestion"] = suggestion
        steps.append(step)
        mask = next_mask

    if filters.exclude_st:
        name_col = _find_col(df, ["name", "股票名称", "名称"]) if mask.any() else None
        if not name_col:
            raise SnapshotFieldMissingError(
                "Missing required snapshot column for exclude_st filter: name"
            )
        record("exclude_st", mask & ~df[name_col].str.contains(r"ST|退", na=False), [name_col])

    def record_min(label: str, columns: list[str], value: float | None) -> None:
        if value is not None:
            record(label, _filter_min(df, mask, columns, value), columns)

    def record_max(label: str, columns: list[str], value: float | None) -> None:
        if value is not None:
            record(label, _filter_max(df, mask, columns, value), columns)

    record_min("amount_min", ["amount", "成交额"], filters.amount_min)
    record_min("price_min", ["price", "最新价", "现价"], filters.price_min)
    record_max("price_max", ["price", "最新价", "现价"], filters.price_max)
    record_min("market_cap_min", ["total_mv", "总市值"], filters.market_cap_min)
    record_max("market_cap_max", ["total_mv", "总市值"], filters.market_cap_max)
    record_min("pe_ttm_min", ["pe_ratio", "市盈率"], filters.pe_ttm_min)
    record_max("pe_ttm_max", ["pe_ratio", "市盈率"], filters.pe_ttm_max)
    record_min("pb_min", ["pb_ratio", "市净率"], filters.pb_min)
    record_max("pb_max", ["pb_ratio", "市净率"], filters.pb_max)
    record_min("volume_ratio_min", ["volume_ratio", "量比"], filters.volume_ratio_min)
    record_min("turnover_rate_min", ["turnover_rate", "换手率"], filters.turnover_rate_min)
    record_min("change_pct_min", ["change_pct", "涨跌幅"], filters.change_pct_min)
    record_max("change_pct_max", ["change_pct", "涨跌幅"], filters.change_pct_max)
    record_min("change_60d_min", ["change_60d"], filters.change_60d_min)
    record_max("change_60d_max", ["change_60d"], filters.change_60d_max)

    if filters.require_ma_bullish:
        record("require_ma_bullish", _filter_bool_true(df, mask, "ma_bullish", True), ["ma_bullish"])
    if filters.require_price_above_ma20:
        record("require_price_above_ma20", _filter_bool_true(df, mask, "price_above_ma20", True), ["price_above_ma20"])

    record_min("signal_score_min", ["signal_score"], filters.signal_score_min)
    if filters.macd_status_whitelist:
        record("macd_status_whitelist", _filter_in(df, mask, "macd_status", filters.macd_status_whitelist), ["macd_status"])
    if filters.rsi_status_whitelist:
        record("rsi_status_whitelist", _filter_in(df, mask, "rsi_status", filters.rsi_status_whitelist), ["rsi_status"])
    record_min("breakout_20d_pct_min", ["breakout_20d_pct"], filters.breakout_20d_pct_min)
    record_max("breakout_20d_pct_max", ["breakout_20d_pct"], filters.breakout_20d_pct_max)
    record_max("range_20d_pct_max", ["range_20d_pct"], filters.range_20d_pct_max)
    record_min("volume_ratio_20d_min", ["volume_ratio_20d"], filters.volume_ratio_20d_min)
    record_max("volume_ratio_20d_max", ["volume_ratio_20d"], filters.volume_ratio_20d_max)
    record_min("body_pct_min", ["body_pct"], filters.body_pct_min)
    record_max("body_pct_max", ["body_pct"], filters.body_pct_max)
    record_min("pullback_to_ma20_pct_min", ["pullback_to_ma20_pct"], filters.pullback_to_ma20_pct_min)
    record_max("pullback_to_ma20_pct_max", ["pullback_to_ma20_pct"], filters.pullback_to_ma20_pct_max)
    record_min("consolidation_days_20d_min", ["consolidation_days_20d"], filters.consolidation_days_20d_min)
    record_max("consolidation_days_20d_max", ["consolidation_days_20d"], filters.consolidation_days_20d_max)
    record_min("volatility_20d_pct_min", ["volatility_20d_pct"], filters.volatility_20d_pct_min)
    record_max("volatility_20d_pct_max", ["volatility_20d_pct"], filters.volatility_20d_pct_max)
    record_min("max_drawdown_20d_pct_min", ["max_drawdown_20d_pct"], filters.max_drawdown_20d_pct_min)
    record_max("max_drawdown_20d_pct_max", ["max_drawdown_20d_pct"], filters.max_drawdown_20d_pct_max)
    record_min("atr_20_pct_min", ["atr_20_pct"], filters.atr_20_pct_min)
    record_max("atr_20_pct_max", ["atr_20_pct"], filters.atr_20_pct_max)
    record_max("kdj_j_max", ["kdj_j"], filters.kdj_j_max)
    record_min("kdj_j_min", ["kdj_j"], filters.kdj_j_min)
    record_max("prev_kdj_j_max", ["prev_kdj_j"], filters.prev_kdj_j_max)
    if filters.require_kdj_golden_cross:
        record("require_kdj_golden_cross", _filter_bool_true(df, mask, "kdj_golden_cross", True), ["kdj_golden_cross"])
    if filters.require_zg_short_above_long:
        record("require_zg_short_above_long", _filter_bool_true(df, mask, "zg_short_above_long", True), ["zg_short_above_long"])
    if filters.require_close_above_zg_long:
        record("require_close_above_zg_long", _filter_bool_true(df, mask, "close_above_zg_long", True), ["close_above_zg_long"])
    if filters.require_close_below_boll_lower:
        record("require_close_below_boll_lower", _filter_bool_true(df, mask, "close_below_boll_lower", True), ["close_below_boll_lower"])
    if filters.require_close_above_boll_upper:
        record("require_close_above_boll_upper", _filter_bool_true(df, mask, "close_above_boll_upper", True), ["close_above_boll_upper"])
    record_max("daily_amplitude_max", ["daily_amplitude_pct"], filters.daily_amplitude_max)
    record_min("daily_change_min", ["daily_change_pct"], filters.daily_change_min)
    record_max("daily_change_max", ["daily_change_pct"], filters.daily_change_max)
    if filters.require_volume_above_prev:
        record("require_volume_above_prev", _filter_bool_true(df, mask, "volume_above_prev", True), ["volume_above_prev"])
    if filters.require_brick_turn_up:
        record("require_brick_turn_up", _filter_bool_true(df, mask, "brick_turn_up", True), ["brick_turn_up"])

    return steps


def requires_daily_features(filters: HardFilterConfig) -> bool:
    """Return whether a hard-filter config needs daily K-line features."""
    return filters.require_no_price_up_flow_out or _has_non_default_filters(
        filters, _DAILY_FILTER_DEFAULTS
    )


def without_daily_filters(filters: HardFilterConfig) -> HardFilterConfig:
    """Return a copy with daily K-line filters disabled."""
    return replace(filters, **_DAILY_FILTER_DEFAULTS)


def requires_flow_features(filters: HardFilterConfig) -> bool:
    """Return whether a hard-filter config needs moneyflow features."""
    return _has_non_default_filters(filters, _FLOW_FILTER_DEFAULTS)


def without_flow_filters(filters: HardFilterConfig) -> HardFilterConfig:
    """Return a copy with moneyflow filters disabled."""
    return replace(filters, **_FLOW_FILTER_DEFAULTS)


def _has_non_default_filters(filters: HardFilterConfig, defaults: dict[str, object]) -> bool:
    return any(getattr(filters, key) != value for key, value in defaults.items())


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _filter_min(
    df: pd.DataFrame,
    mask: pd.Series,
    col_names: list[str],
    value: float | None,
) -> pd.Series:
    if value is None:
        return mask
    if not mask.any():
        return mask
    col = _find_col(df, col_names)
    if not col:
        raise SnapshotFieldMissingError(
            f"Missing required snapshot column for min filter {col_names}: "
            f"configured value={value}"
        )
    series = pd.to_numeric(df[col], errors="coerce")
    return mask & series.ge(value) & series.notna()


def _filter_max(
    df: pd.DataFrame,
    mask: pd.Series,
    col_names: list[str],
    value: float | None,
) -> pd.Series:
    if value is None:
        return mask
    if not mask.any():
        return mask
    col = _find_col(df, col_names)
    if not col:
        raise SnapshotFieldMissingError(
            f"Missing required snapshot column for max filter {col_names}: "
            f"configured value={value}"
        )
    series = pd.to_numeric(df[col], errors="coerce")
    return mask & series.le(value) & series.notna()


def _filter_bool_true(
    df: pd.DataFrame,
    mask: pd.Series,
    col_name: str,
    enabled: bool,
) -> pd.Series:
    if not enabled:
        return mask
    if not mask.any():
        return mask
    if col_name not in df.columns:
        raise SnapshotFieldMissingError(
            f"Missing required daily feature column for bool filter: {col_name}"
        )
    return mask & (df[col_name] == True)  # noqa: E712


def _filter_bool_false(
    df: pd.DataFrame,
    mask: pd.Series,
    col_name: str,
    enabled: bool,
) -> pd.Series:
    if not enabled:
        return mask
    if not mask.any():
        return mask
    if col_name not in df.columns:
        raise SnapshotFieldMissingError(
            f"Missing required flow feature column for bool filter: {col_name}"
        )
    series = df[col_name]
    return mask & series.notna() & (series != True)  # noqa: E712


def _filter_in(
    df: pd.DataFrame,
    mask: pd.Series,
    col_name: str,
    allowed: list[str] | None,
) -> pd.Series:
    if not allowed:
        return mask
    if not mask.any():
        return mask
    if col_name not in df.columns:
        raise SnapshotFieldMissingError(
            f"Missing required daily feature column for whitelist filter: {col_name}"
        )
    allowed_set = {str(item) for item in allowed}
    return mask & df[col_name].astype(str).isin(allowed_set)



def _rejection_samples(
    df: pd.DataFrame,
    *,
    value_columns: list[str],
    limit: int,
) -> list[dict[str, object]]:
    samples: list[dict[str, object]] = []
    name_col = _find_col(df, ["name", "股票名称", "名称"])
    code_col = _find_col(df, ["code", "代码", "symbol"])
    value_col = _find_col(df, value_columns)
    for _, row in df.head(limit).iterrows():
        item: dict[str, object] = {}
        if code_col:
            item["code"] = str(row.get(code_col, ""))
        if name_col:
            item["name"] = str(row.get(name_col, ""))
        if value_col:
            value = row.get(value_col)
            item["value"] = None if pd.isna(value) else value
        samples.append(item)
    return samples


def _waterfall_suggestion(label: str, before: int, after: int, removed: int) -> str:
    if before <= 0 or removed <= 0:
        return ""
    if after == 0:
        return f"{label} eliminated all remaining candidates; inspect threshold or data availability."
    if removed / before >= 0.9:
        return f"{label} removed over 90% of remaining candidates; consider reviewing this threshold."
    return ""
