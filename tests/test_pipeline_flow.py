from pathlib import Path

import pandas as pd

from alphasift.config import Config
from alphasift.filter import requires_flow_features, without_flow_filters
from alphasift.models import HardFilterConfig
from alphasift.pipeline import screen


def test_requires_flow_features_detects_configured_filters():
    filters = HardFilterConfig(main_inflow_streak_min=5)
    assert requires_flow_features(filters)
    stripped = without_flow_filters(filters)
    assert stripped.main_inflow_streak_min is None


def test_pipeline_applies_flow_hard_filters(monkeypatch, tmp_path: Path):
    df = pd.DataFrame([
        {
            "code": "600519",
            "name": "贵州茅台",
            "price": 180.0,
            "change_pct": 2.0,
            "amount": 500_000_000,
            "turnover_rate": 1.5,
            "volume_ratio": 1.2,
            "pe_ratio": 30.0,
            "pb_ratio": 8.0,
            "total_mv": 2_000_000_000_000,
        },
        {
            "code": "000001",
            "name": "平安银行",
            "price": 10.0,
            "change_pct": 2.5,
            "amount": 400_000_000,
            "turnover_rate": 2.0,
            "volume_ratio": 1.5,
            "pe_ratio": 8.0,
            "pb_ratio": 0.8,
            "total_mv": 200_000_000_000,
        },
    ])
    df.attrs["snapshot_source"] = "test"

    def fake_daily_enrich(frame, **kwargs):
        enriched = frame.copy()
        enriched.attrs["daily_success_count"] = len(enriched)
        return enriched

    def fake_enrich(frame, **kwargs):
        enriched = frame.copy()
        for idx, row in enriched.iterrows():
            is_target = row["code"] == "600519"
            enriched.at[idx, "main_inflow_streak"] = 6 if is_target else 2
            enriched.at[idx, "main_net_inflow_5d"] = 100.0 if is_target else -10.0
            enriched.at[idx, "main_net_inflow"] = 20.0 if is_target else -5.0
            enriched.at[idx, "main_net_inflow_rate"] = 0.08 if is_target else -0.02
            enriched.at[idx, "price_up_flow_out"] = False
            enriched.at[idx, "flow_as_of"] = "2026-04-02"
            enriched.at[idx, "flow_quality_flags"] = ""
        enriched.attrs["flow_success_count"] = len(enriched)
        return enriched

    monkeypatch.setattr("alphasift.pipeline.fetch_snapshot_with_fallback", lambda sources, **kwargs: df)
    monkeypatch.setattr("alphasift.pipeline.enrich_daily_features", fake_daily_enrich)
    monkeypatch.setattr("alphasift.pipeline.enrich_flow_features", fake_enrich)

    flow_dir = tmp_path / "flow_bars"
    daily_dir = tmp_path / "daily_bars"
    (flow_dir / "moneyflow").mkdir(parents=True)
    (flow_dir / "moneyflow" / "600519.SH.parquet").write_bytes(b"placeholder")
    (flow_dir / "manifest.json").write_text('{"last_trade_date":"20260402"}', encoding="utf-8")
    (daily_dir / "bars" / "raw").mkdir(parents=True)
    (daily_dir / "bars" / "raw" / "600519.SH.parquet").write_bytes(b"placeholder")
    (daily_dir / "manifest.json").write_text('{"last_trade_date":"20260402"}', encoding="utf-8")

    result = screen(
        "main_inflow_momentum",
        use_llm=False,
        config=Config(
            llm_api_key="",
            snapshot_source_priority=["test"],
            strategies_dir=Path("strategies"),
            flow_bars_dir=flow_dir,
            daily_bars_dir=daily_dir,
            risk_enabled=False,
        ),
    )

    assert result.flow_enriched is True
    assert result.daily_enriched is True
    assert result.after_filter_count == 1
    assert result.picks[0].code == "600519"
    assert any("Flow enrichment attempted" in item for item in result.degradation)
