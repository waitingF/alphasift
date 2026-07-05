from pathlib import Path

import pytest

from alphasift.filter import requires_daily_features, requires_flow_features
from alphasift.models import HardFilterConfig
from alphasift.screen_prerequisites import collect_screen_prerequisite_issues
from alphasift.strategy import load_strategy


@pytest.mark.parametrize(
    "strategy_name,needs_flow,needs_daily_store",
    [
        ("b1_main_inflow_5d", True, False),
        ("b1_main_inflow_5d_no_divergence", True, True),
        ("b2_main_inflow_5d", True, False),
    ],
)
def test_flow_combo_strategies_yaml(strategy_name, needs_flow, needs_daily_store, tmp_path):
    strat = load_strategy(Path("strategies") / f"{strategy_name}.yaml")
    filters = strat.screening.hard_filters
    assert requires_flow_features(filters) is True
    assert requires_daily_features(filters) is True
    assert filters.kdj_j_max == 13 or strategy_name.startswith("b2")
    issues = collect_screen_prerequisite_issues(
        hard_filters=filters,
        config=__import__("alphasift.config", fromlist=["Config"]).Config(
            data_dir=tmp_path / "data",
            flow_bars_dir=tmp_path / "data" / "flow_bars",
            daily_bars_dir=tmp_path / "data" / "daily_bars",
        ),
    )
    labels = {issue.label for issue in issues}
    assert "本地资金流库（flow-bars）" in labels
    if needs_daily_store:
        assert "本地日 K 库（daily-bars）" in labels


def test_b1_main_inflow_5d_adds_streak_filter():
    strat = load_strategy(Path("strategies/b1_main_inflow_5d.yaml"))
    assert strat.screening.hard_filters.main_inflow_streak_min == 5
    assert strat.screening.hard_filters.require_zg_short_above_long is True


def test_b1_main_inflow_no_divergence_adds_guard():
    strat = load_strategy(Path("strategies/b1_main_inflow_5d_no_divergence.yaml"))
    assert strat.screening.hard_filters.require_no_price_up_flow_out is True


def test_b2_main_inflow_5d_adds_net_inflow_filter():
    strat = load_strategy(Path("strategies/b2_main_inflow_5d.yaml"))
    assert strat.screening.hard_filters.main_net_inflow_5d_min == 0
    assert strat.screening.hard_filters.daily_change_min == 3.95
