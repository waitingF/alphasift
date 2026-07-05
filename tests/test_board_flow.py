from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

from alphasift.board_flow import (
    DEFAULT_METRIC,
    format_board_flow_explain,
    load_board_membership,
    rank_board_flow,
)
from alphasift.flow_store import FlowBarStore


def _write_moneyflow(store: FlowBarStore, ts_code: str, rows: list[dict[str, object]]) -> None:
    frame = pd.DataFrame(rows)
    frame["ts_code"] = ts_code
    store.write(ts_code.split(".")[0], frame)


def _sample_mapping(tmp_path: Path) -> Path:
    path = tmp_path / "industry_map.csv"
    pd.DataFrame([
        {"code": "600519", "industry": "白酒", "concepts": "消费,白酒"},
        {"code": "000001", "industry": "银行", "concepts": "金融,银行"},
        {"code": "600000", "industry": "银行", "concepts": "金融"},
    ]).to_csv(path, index=False)
    return path


def _sample_store(tmp_path: Path) -> FlowBarStore:
    store = FlowBarStore(tmp_path / "flow_bars")
    _write_moneyflow(store, "600519.SH", [
        {"trade_date": "2026-04-01", "buy_lg_amount": 100, "buy_elg_amount": 50, "sell_lg_amount": 80, "sell_elg_amount": 20, "net_mf_amount": 50},
        {"trade_date": "2026-04-02", "buy_lg_amount": 120, "buy_elg_amount": 60, "sell_lg_amount": 70, "sell_elg_amount": 30, "net_mf_amount": 80},
        {"trade_date": "2026-04-03", "buy_lg_amount": 130, "buy_elg_amount": 65, "sell_lg_amount": 60, "sell_elg_amount": 25, "net_mf_amount": 110},
        {"trade_date": "2026-04-04", "buy_lg_amount": 140, "buy_elg_amount": 70, "sell_lg_amount": 55, "sell_elg_amount": 20, "net_mf_amount": 135},
        {"trade_date": "2026-04-05", "buy_lg_amount": 150, "buy_elg_amount": 75, "sell_lg_amount": 50, "sell_elg_amount": 15, "net_mf_amount": 160},
    ])
    _write_moneyflow(store, "000001.SZ", [
        {"trade_date": "2026-04-01", "buy_lg_amount": 20, "buy_elg_amount": 10, "sell_lg_amount": 15, "sell_elg_amount": 5, "net_mf_amount": 10},
        {"trade_date": "2026-04-02", "buy_lg_amount": 25, "buy_elg_amount": 12, "sell_lg_amount": 14, "sell_elg_amount": 6, "net_mf_amount": 17},
        {"trade_date": "2026-04-03", "buy_lg_amount": 30, "buy_elg_amount": 15, "sell_lg_amount": 13, "sell_elg_amount": 7, "net_mf_amount": 25},
        {"trade_date": "2026-04-04", "buy_lg_amount": 35, "buy_elg_amount": 18, "sell_lg_amount": 12, "sell_elg_amount": 8, "net_mf_amount": 33},
        {"trade_date": "2026-04-05", "buy_lg_amount": 40, "buy_elg_amount": 20, "sell_lg_amount": 10, "sell_elg_amount": 9, "net_mf_amount": 41},
    ])
    _write_moneyflow(store, "600000.SH", [
        {"trade_date": "2026-04-01", "buy_lg_amount": 15, "buy_elg_amount": 8, "sell_lg_amount": 12, "sell_elg_amount": 4, "net_mf_amount": 7},
        {"trade_date": "2026-04-02", "buy_lg_amount": 18, "buy_elg_amount": 9, "sell_lg_amount": 11, "sell_elg_amount": 5, "net_mf_amount": 11},
        {"trade_date": "2026-04-03", "buy_lg_amount": 21, "buy_elg_amount": 10, "sell_lg_amount": 10, "sell_elg_amount": 6, "net_mf_amount": 15},
        {"trade_date": "2026-04-04", "buy_lg_amount": 24, "buy_elg_amount": 11, "sell_lg_amount": 9, "sell_elg_amount": 7, "net_mf_amount": 19},
        {"trade_date": "2026-04-05", "buy_lg_amount": 27, "buy_elg_amount": 12, "sell_lg_amount": 8, "sell_elg_amount": 8, "net_mf_amount": 23},
    ])
    return store


def test_load_board_membership_industry_and_concept(tmp_path: Path):
    mapping = _sample_mapping(tmp_path)
    membership = load_board_membership(mapping, board_types=["industry", "concept"])
    assert set(membership["board_type"]) == {"industry", "concept"}
    assert ("600519", "concept", "白酒") in set(map(tuple, membership[["code", "board_type", "board"]].values.tolist()))


def test_rank_board_flow_defaults_to_5d_metric(tmp_path: Path):
    store = _sample_store(tmp_path)
    mapping = _sample_mapping(tmp_path)
    result = rank_board_flow(
        store,
        mapping,
        board_types=["industry"],
        top_boards=5,
        top_stocks=5,
    )
    assert result.metric == DEFAULT_METRIC
    industry_boards = [board.board for board in result.boards if board.board_type == "industry"]
    assert industry_boards[0] == "白酒"
    assert result.constituents["industry:白酒"][0]["code"] == "600519"


def test_rank_board_flow_board_filter(tmp_path: Path):
    store = _sample_store(tmp_path)
    mapping = _sample_mapping(tmp_path)
    result = rank_board_flow(
        store,
        mapping,
        board_types=["industry"],
        board_filter="银行",
        top_stocks=5,
    )
    assert len(result.boards) == 1
    assert result.boards[0].board == "银行"
    codes = {item["code"] for item in result.constituents["industry:银行"]}
    assert codes == {"000001", "600000"}


def test_format_board_flow_explain_includes_sections(tmp_path: Path):
    store = _sample_store(tmp_path)
    mapping = _sample_mapping(tmp_path)
    result = rank_board_flow(store, mapping, board_types=["industry", "concept"], top_boards=3, top_stocks=2)
    text = format_board_flow_explain(result)
    assert "=== 行业板块 Top" in text
    assert "=== 概念板块 Top" in text
    assert "600519" in text


def test_board_flow_rank_cli_json(tmp_path, monkeypatch, capsys):
    store_root = tmp_path / "flow_bars"
    _sample_store(tmp_path)
    mapping = _sample_mapping(tmp_path)
    monkeypatch.setenv("ALPHASIFT_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", [
        "alphasift",
        "board-flow",
        "rank",
        "--mapping",
        str(mapping),
        "--board-type",
        "industry",
        "--top-boards",
        "2",
        "--top-stocks",
        "2",
        "--json",
    ])

    from alphasift.cli import main

    main()

    payload = json.loads(capsys.readouterr().out)
    assert payload["metric"] == "main_net_inflow_5d"
    assert payload["boards"]
    assert "industry:白酒" in payload["constituents"]


def test_board_flow_rank_cli_missing_mapping(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("ALPHASIFT_DATA_DIR", str(tmp_path))
    (tmp_path / "flow_bars").mkdir()
    monkeypatch.setattr(sys, "argv", ["alphasift", "board-flow", "rank", "--json"])

    from alphasift.cli import main

    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 1
    assert "industry mapping not found" in capsys.readouterr().err
