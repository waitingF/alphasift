from pathlib import Path

import pytest

from alphasift.config import Config
from alphasift.data_update import run_data_update
from alphasift.daily_sync import SyncStats


def _ready_daily_store(root: Path) -> None:
    raw = root / "bars" / "raw"
    raw.mkdir(parents=True)
    (raw / "600519.SH.parquet").write_bytes(b"placeholder")
    (root / "manifest.json").write_text('{"last_trade_date":"20260402"}', encoding="utf-8")


def _ready_flow_store(root: Path) -> None:
    moneyflow = root / "moneyflow"
    moneyflow.mkdir(parents=True)
    (moneyflow / "600519.SH.parquet").write_bytes(b"placeholder")
    (root / "manifest.json").write_text('{"last_trade_date":"20260402"}', encoding="utf-8")


def test_run_data_update_serializes_steps(monkeypatch, tmp_path: Path):
    calls: list[str] = []

    def fake_daily_sync(*args, **kwargs):
        calls.append("daily_sync")
        return SyncStats(updated_codes=1, added_rows=10)

    def fake_flow_sync(*args, **kwargs):
        calls.append("flow_sync")
        return SyncStats(updated_codes=2, added_rows=20)

    def fake_industry(max_boards: int):
        calls.append("industry")
        return {"600519": {"industry": "白酒", "concepts": "消费"}}, []

    def fake_save_industry(mapping, output):
        calls.append("save_industry")
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("code,industry\n600519,白酒\n", encoding="utf-8")
        return path

    def fake_hotspots(**kwargs):
        calls.append("hotspot")
        from alphasift.hotspot import HotspotResults, HotspotSummary

        return HotspotResults([HotspotSummary(topic="AI算力", heat_score=80.0)])

    def fake_save_hotspots(path, hotspots):
        calls.append("save_hotspot")
        return Path(path)

    def fake_append_history(path, hotspots, *, generated_at):
        calls.append("hotspot_history")
        return Path(path)

    monkeypatch.setenv("TUSHARE_TOKEN", "test-token")
    monkeypatch.setattr("alphasift.data_update.sync_daily_bars", fake_daily_sync)
    monkeypatch.setattr("alphasift.data_update.sync_flow_bars", fake_flow_sync)
    monkeypatch.setattr("alphasift.data_update.fetch_akshare_board_map", fake_industry)
    monkeypatch.setattr("alphasift.data_update.save_industry_map", fake_save_industry)
    monkeypatch.setattr("alphasift.data_update.discover_hotspots", fake_hotspots)
    monkeypatch.setattr("alphasift.data_update.save_hotspots_json", fake_save_hotspots)
    monkeypatch.setattr("alphasift.data_update.append_hotspot_history", fake_append_history)

    data_dir = tmp_path / "data"
    daily_dir = data_dir / "daily_bars"
    flow_dir = data_dir / "flow_bars"
    _ready_daily_store(daily_dir)
    _ready_flow_store(flow_dir)

    result = run_data_update(
        Config(data_dir=data_dir, daily_bars_dir=daily_dir, flow_bars_dir=flow_dir),
    )

    assert result.success
    assert [step.name for step in result.steps] == [
        "daily_bars",
        "flow_bars",
        "industry_cache",
        "hotspot_cache",
    ]
    assert calls == [
        "daily_sync",
        "flow_sync",
        "industry",
        "save_industry",
        "hotspot",
        "save_hotspot",
        "hotspot_history",
    ]


def test_run_data_update_stops_after_daily_failure(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("TUSHARE_TOKEN", "test-token")
    monkeypatch.setattr(
        "alphasift.data_update.sync_daily_bars",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("daily failed")),
    )

    data_dir = tmp_path / "data"
    daily_dir = data_dir / "daily_bars"
    _ready_daily_store(daily_dir)

    result = run_data_update(
        Config(data_dir=data_dir, daily_bars_dir=daily_dir, flow_bars_dir=data_dir / "flow_bars"),
        skip_industry=True,
        skip_hotspot=True,
    )

    assert result.had_failures
    assert len(result.steps) == 1
    assert result.steps[0].name == "daily_bars"
    assert result.steps[0].status == "failed"


def test_run_data_update_skips_tushare_without_token(tmp_path: Path):
    for key in ("TUSHARE_TOKEN", "TUSHARE_API_TOKEN"):
        import os

        os.environ.pop(key, None)

    result = run_data_update(
        Config(data_dir=tmp_path / "data"),
        skip_industry=True,
        skip_hotspot=True,
        init_if_missing=False,
    )

    assert result.success
    assert all(step.status == "skipped" for step in result.steps)


def test_cli_data_update_explain(monkeypatch, capsys):
    import sys

    from alphasift.cli import main
    from alphasift.data_update import DataUpdateResult

    monkeypatch.setattr(
        "alphasift.data_update.run_data_update",
        lambda config, **kwargs: DataUpdateResult(
            started_at="2026-04-28T10:00:00",
            finished_at="2026-04-28T10:00:01",
            steps=[],
        ),
    )
    monkeypatch.setattr(sys, "argv", ["alphasift", "data-update", "--skip-daily", "--skip-flow", "--explain"])

    with pytest.raises(SystemExit) as exc_info:
        main()

    assert exc_info.value.code == 0
    assert "data-update success=True" in capsys.readouterr().out
