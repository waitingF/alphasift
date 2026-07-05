from pathlib import Path

import pytest

from alphasift.config import Config
from alphasift.models import HardFilterConfig
from alphasift.pipeline import screen
from alphasift.screen_prerequisites import (
    ScreenPrerequisitesError,
    collect_screen_prerequisite_issues,
    validate_screen_prerequisites,
)


def test_main_inflow_momentum_requires_flow_and_daily_stores(tmp_path: Path):
    issues = collect_screen_prerequisite_issues(
        hard_filters=HardFilterConfig(
            main_inflow_streak_min=5,
            main_net_inflow_5d_min=0,
            require_no_price_up_flow_out=True,
        ),
        config=Config(
            data_dir=tmp_path / "data",
            flow_bars_dir=tmp_path / "data" / "flow_bars",
            daily_bars_dir=tmp_path / "data" / "daily_bars",
        ),
    )

    labels = {issue.label for issue in issues}
    assert "本地资金流库（flow-bars）" in labels
    assert "本地日 K 库（daily-bars）" in labels


def test_validate_screen_prerequisites_raises_with_actionable_message(tmp_path: Path):
    with pytest.raises(ScreenPrerequisitesError, match="前置条件未满足") as exc_info:
        validate_screen_prerequisites(
            strategy="main_inflow_momentum",
            hard_filters=HardFilterConfig(
                main_inflow_streak_min=5,
                require_no_price_up_flow_out=True,
            ),
            config=Config(
                data_dir=tmp_path / "data",
                flow_bars_dir=tmp_path / "data" / "flow_bars",
                daily_bars_dir=tmp_path / "data" / "daily_bars",
            ),
        )

    message = str(exc_info.value)
    assert "flow-bars init" in message
    assert "daily-bars init" in message


def _write_min_flow_store(root: Path) -> None:
    moneyflow = root / "moneyflow"
    moneyflow.mkdir(parents=True)
    (moneyflow / "600519.SH.parquet").write_bytes(b"placeholder")
    root.mkdir(parents=True, exist_ok=True)
    (root / "manifest.json").write_text(
        '{"last_trade_date":"20260402","dataset":"moneyflow"}',
        encoding="utf-8",
    )


def _write_min_daily_store(root: Path) -> None:
    raw = root / "bars" / "raw"
    raw.mkdir(parents=True)
    (raw / "600519.SH.parquet").write_bytes(b"placeholder")
    root.mkdir(parents=True, exist_ok=True)
    (root / "manifest.json").write_text(
        '{"last_trade_date":"20260402","dataset":"daily_bars"}',
        encoding="utf-8",
    )


def test_validate_screen_prerequisites_passes_when_stores_exist(tmp_path: Path):
    flow_dir = tmp_path / "data" / "flow_bars"
    daily_dir = tmp_path / "data" / "daily_bars"
    _write_min_flow_store(flow_dir)
    _write_min_daily_store(daily_dir)

    validate_screen_prerequisites(
        strategy="main_inflow_momentum",
        hard_filters=HardFilterConfig(
            main_inflow_streak_min=5,
            require_no_price_up_flow_out=True,
        ),
        config=Config(
            data_dir=tmp_path / "data",
            flow_bars_dir=flow_dir,
            daily_bars_dir=daily_dir,
        ),
    )


def test_screen_b1_main_inflow_5d_fails_fast_without_flow_store(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(
        "alphasift.pipeline.fetch_snapshot_with_fallback",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not fetch snapshot")),
    )

    with pytest.raises(ScreenPrerequisitesError, match="b1_main_inflow_5d"):
        screen(
            "b1_main_inflow_5d",
            use_llm=False,
            config=Config(
                llm_api_key="",
                snapshot_source_priority=["test"],
                strategies_dir=Path("strategies"),
                data_dir=tmp_path / "data",
                flow_bars_dir=tmp_path / "data" / "flow_bars",
                daily_bars_dir=tmp_path / "data" / "daily_bars",
                risk_enabled=False,
            ),
        )


def test_screen_main_inflow_momentum_fails_fast_without_prerequisites(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(
        "alphasift.pipeline.fetch_snapshot_with_fallback",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not fetch snapshot")),
    )

    with pytest.raises(ScreenPrerequisitesError, match="main_inflow_momentum"):
        screen(
            "main_inflow_momentum",
            use_llm=False,
            config=Config(
                llm_api_key="",
                snapshot_source_priority=["test"],
                strategies_dir=Path("strategies"),
                data_dir=tmp_path / "data",
                flow_bars_dir=tmp_path / "data" / "flow_bars",
                daily_bars_dir=tmp_path / "data" / "daily_bars",
                risk_enabled=False,
            ),
        )
