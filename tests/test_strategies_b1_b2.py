from pathlib import Path

import pandas as pd
import pytest

from alphasift.daily import compute_daily_features
from alphasift.filter import requires_daily_features
from alphasift.strategy import load_strategy


def test_b1_strategy_yaml_loads_and_requires_daily_features():
    strat = load_strategy(Path("strategies/b1.yaml"))
    assert strat.name == "b1"
    assert strat.display_name == "B1（常规）"
    assert strat.screening.hard_filters.kdj_j_max == 13
    assert strat.screening.hard_filters.require_zg_short_above_long is True
    assert requires_daily_features(strat.screening.hard_filters)


def test_b1_above_long_adds_close_above_zg_long():
    strat = load_strategy(Path("strategies/b1_above_long.yaml"))
    assert strat.screening.hard_filters.require_close_above_zg_long is True


def test_b1_perfect_adds_amplitude_and_change_bounds():
    strat = load_strategy(Path("strategies/b1_perfect.yaml"))
    filters = strat.screening.hard_filters
    assert filters.daily_amplitude_max == 4.2
    assert filters.daily_change_min == -2.0
    assert filters.daily_change_max == 2.5


def test_b2_strategy_yaml_matches_self_stock_semantics():
    strat = load_strategy(Path("strategies/b2.yaml"))
    filters = strat.screening.hard_filters
    assert filters.prev_kdj_j_max == 13
    assert filters.daily_change_min == 3.95
    assert filters.require_volume_above_prev is True
    assert filters.kdj_j_max == 80
    assert filters.require_zg_short_above_long is True
    assert filters.require_close_above_zg_long is True


def test_brick_turn_up_strategy_yaml():
    strat = load_strategy(Path("strategies/brick_turn_up.yaml"))
    assert strat.screening.hard_filters.require_brick_turn_up is True


def test_compute_daily_features_includes_zg_and_kdj_fields():
    closes = [10.0 + 0.05 * i for i in range(200)]
    hist = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=200).astype(str),
            "open": closes,
            "high": [value + 0.5 for value in closes],
            "low": [value - 0.5 for value in closes],
            "close": closes,
            "volume": [1000.0] * 200,
        }
    )
    features = compute_daily_features(hist)
    assert features["zg_short"] is not None
    assert features["zg_long"] is not None
    assert features["kdj_j"] is not None
    assert features["zg_short_above_long"] is True
    assert features["close_above_zg_long"] is True
