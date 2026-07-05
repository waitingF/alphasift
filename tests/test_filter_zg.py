import pandas as pd
import pytest

from alphasift.filter import SnapshotFieldMissingError, apply_hard_filters
from alphasift.models import HardFilterConfig


def _sample_row(**overrides) -> dict:
    base = {
        "name": "示例A",
        "code": "000001",
        "price": 10.0,
        "amount": 100_000_000,
        "kdj_j": 10.0,
        "prev_kdj_j": 8.0,
        "zg_short": 11.0,
        "zg_long": 10.0,
        "zg_short_above_long": True,
        "close_above_zg_long": True,
        "daily_amplitude_pct": 3.0,
        "daily_change_pct": 1.5,
        "volume_above_prev": True,
        "brick_turn_up": False,
        "kdj_golden_cross": False,
        "close_below_boll_lower": False,
        "close_above_boll_upper": False,
    }
    base.update(overrides)
    return base


def test_b1_hard_filters_keep_matching_row():
    df = pd.DataFrame(
        [
            _sample_row(),
            _sample_row(name="示例B", kdj_j=20.0, zg_short_above_long=False),
        ]
    )
    filtered = apply_hard_filters(
        df,
        HardFilterConfig(kdj_j_max=13, require_zg_short_above_long=True),
    )
    assert filtered["name"].tolist() == ["示例A"]


def test_b1_perfect_filters_amplitude_and_daily_change():
    df = pd.DataFrame(
        [
            _sample_row(daily_amplitude_pct=3.5, daily_change_pct=1.0),
            _sample_row(name="示例B", daily_amplitude_pct=6.0, daily_change_pct=1.0),
            _sample_row(name="示例C", daily_amplitude_pct=3.0, daily_change_pct=4.0),
        ]
    )
    filtered = apply_hard_filters(
        df,
        HardFilterConfig(
            daily_amplitude_max=4.2,
            daily_change_min=-2.0,
            daily_change_max=2.5,
        ),
    )
    assert filtered["name"].tolist() == ["示例A"]


def test_b2_hard_filters_require_prev_kdj_volume_and_change():
    df = pd.DataFrame(
        [
            _sample_row(prev_kdj_j=10.0, daily_change_pct=4.5, volume_above_prev=True, kdj_j=60.0),
            _sample_row(name="示例B", prev_kdj_j=20.0, daily_change_pct=4.5, volume_above_prev=True),
            _sample_row(name="示例C", prev_kdj_j=10.0, daily_change_pct=2.0, volume_above_prev=True),
            _sample_row(name="示例D", prev_kdj_j=10.0, daily_change_pct=4.5, volume_above_prev=False),
        ]
    )
    filtered = apply_hard_filters(
        df,
        HardFilterConfig(
            prev_kdj_j_max=13,
            daily_change_min=3.95,
            require_volume_above_prev=True,
            kdj_j_max=80,
            require_zg_short_above_long=True,
            require_close_above_zg_long=True,
        ),
    )
    assert filtered["name"].tolist() == ["示例A"]


def test_brick_turn_up_filter():
    df = pd.DataFrame(
        [
            _sample_row(brick_turn_up=True),
            _sample_row(name="示例B", brick_turn_up=False),
        ]
    )
    filtered = apply_hard_filters(df, HardFilterConfig(require_brick_turn_up=True))
    assert filtered["name"].tolist() == ["示例A"]


def test_missing_kdj_column_raises():
    df = pd.DataFrame([{"name": "示例A", "amount": 100_000_000}])
    with pytest.raises(SnapshotFieldMissingError):
        apply_hard_filters(df, HardFilterConfig(kdj_j_max=13))
