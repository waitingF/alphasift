# -*- coding: utf-8 -*-
"""Rank industry/concept boards and constituents by local Tushare moneyflow."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Literal

import pandas as pd

from alphasift.flow_metrics import _safe_float, build_stock_flow_snapshot
from alphasift.flow_store import FlowBarStore

BoardType = Literal["industry", "concept"]
MetricName = Literal[
    "main_net_inflow",
    "main_net_inflow_5d",
    "main_net_inflow_10d",
    "main_net_inflow_20d",
]
DEFAULT_METRIC: MetricName = "main_net_inflow_5d"
FLOW_DEFINITION = "buy_lg+buy_elg-sell_lg-sell_elg (万元)"


@dataclass
class StockFlowRow:
    code: str
    ts_code: str
    as_of: str
    main_net_inflow: float | None
    main_net_inflow_5d: float | None
    main_net_inflow_10d: float | None
    main_net_inflow_20d: float | None
    main_inflow_streak: int | None


@dataclass
class BoardRankRow:
    board_type: str
    board: str
    stock_count: int
    flow_sum: float
    flow_mean: float
    as_of: str


@dataclass
class BoardFlowRankResult:
    metric: str
    flow_definition: str
    lookback_days: int
    mapping_path: str
    flow_store_root: str
    stock_count: int
    membership_rows: int
    boards: list[BoardRankRow] = field(default_factory=list)
    constituents: dict[str, list[dict[str, object]]] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "metric": self.metric,
            "flow_definition": self.flow_definition,
            "lookback_days": self.lookback_days,
            "mapping_path": self.mapping_path,
            "flow_store_root": self.flow_store_root,
            "stock_count": self.stock_count,
            "membership_rows": self.membership_rows,
            "boards": [asdict(row) for row in self.boards],
            "constituents": self.constituents,
            "notes": self.notes,
        }


def load_stock_flow_frame(
    store: FlowBarStore,
    *,
    lookback_days: int = 60,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for ts_code in store.list_codes():
        symbol = ts_code.split(".")[0]
        try:
            snap = build_stock_flow_snapshot(
                store.read(symbol, lookback_days=lookback_days),
                daily_bars=None,
            )
        except Exception:
            continue
        if not snap:
            continue
        rows.append({
            "code": symbol.zfill(6),
            "ts_code": ts_code,
            "as_of": str(snap.get("as_of", "")),
            "main_net_inflow": snap.get("main_net_inflow"),
            "main_net_inflow_5d": snap.get("main_net_inflow_5d"),
            "main_net_inflow_10d": snap.get("main_net_inflow_10d"),
            "main_net_inflow_20d": snap.get("main_net_inflow_20d"),
            "main_inflow_streak": snap.get("main_inflow_streak"),
        })
    if not rows:
        return pd.DataFrame(columns=[
            "code", "ts_code", "as_of",
            "main_net_inflow", "main_net_inflow_5d", "main_net_inflow_10d", "main_net_inflow_20d",
            "main_inflow_streak",
        ])
    return pd.DataFrame(rows)


def load_board_membership(
    mapping_path: str | Path,
    *,
    board_types: list[BoardType] | None = None,
) -> pd.DataFrame:
    mapping = pd.read_csv(mapping_path, dtype={"code": str})
    mapping["code"] = mapping["code"].astype(str).str.zfill(6)
    selected = board_types or ["industry", "concept"]
    frames: list[pd.DataFrame] = []

    if "industry" in selected:
        industry = mapping[mapping["industry"].astype(str).str.strip() != ""].copy()
        if not industry.empty:
            industry["board_type"] = "industry"
            industry["board"] = industry["industry"].astype(str).str.strip()
            frames.append(industry[["code", "board_type", "board"]])

    if "concept" in selected:
        concept_rows: list[dict[str, str]] = []
        for _, row in mapping.iterrows():
            concepts = str(row.get("concepts", "") or "").replace("，", ",").replace("、", ",")
            for concept in concepts.split(","):
                concept = concept.strip()
                if concept:
                    concept_rows.append({
                        "code": row["code"],
                        "board_type": "concept",
                        "board": concept,
                    })
        if concept_rows:
            frames.append(pd.DataFrame(concept_rows))

    if not frames:
        return pd.DataFrame(columns=["code", "board_type", "board"])
    return pd.concat(frames, ignore_index=True).drop_duplicates(["code", "board_type", "board"])


def rank_board_flow(
    store: FlowBarStore,
    mapping_path: str | Path,
    *,
    board_types: list[BoardType] | None = None,
    metric: MetricName = DEFAULT_METRIC,
    top_boards: int = 15,
    top_stocks: int = 10,
    lookback_days: int = 60,
    board_filter: str | None = None,
) -> BoardFlowRankResult:
    mapping_path = Path(mapping_path)
    stock_flow = load_stock_flow_frame(store, lookback_days=lookback_days)
    membership = load_board_membership(mapping_path, board_types=board_types)

    notes: list[str] = []
    if stock_flow.empty:
        notes.append("flow store has no readable symbols; run `alphasift flow-bars init/sync` first")
    if membership.empty:
        notes.append(f"no board membership in {mapping_path}; run `alphasift industry-cache` first")
    if "concept" in (board_types or ["industry", "concept"]):
        notes.append("concept boards sum the same stock into each concept it belongs to")

    merged = membership.merge(stock_flow, on="code", how="inner")
    if board_filter:
        merged = merged[merged["board"].astype(str) == board_filter.strip()]

    boards: list[BoardRankRow] = []
    constituents: dict[str, list[dict[str, object]]] = {}

    if merged.empty:
        return BoardFlowRankResult(
            metric=metric,
            flow_definition=FLOW_DEFINITION,
            lookback_days=lookback_days,
            mapping_path=str(mapping_path),
            flow_store_root=str(store.root),
            stock_count=len(stock_flow),
            membership_rows=len(membership),
            boards=boards,
            constituents=constituents,
            notes=notes,
        )

    grouped = (
        merged.groupby(["board_type", "board"], as_index=False)
        .agg(
            stock_count=(metric, "count"),
            flow_sum=(metric, "sum"),
            flow_mean=(metric, "mean"),
            as_of=("as_of", "max"),
        )
        .sort_values(["board_type", "flow_sum"], ascending=[True, False])
    )

    selected_boards: list[tuple[str, str]] = []
    if board_filter:
        selected_boards = [
            (str(row.board_type), str(row.board))
            for row in grouped.itertuples(index=False)
        ]
    else:
        for board_type in sorted(grouped["board_type"].unique()):
            subset = grouped[grouped["board_type"] == board_type].head(max(int(top_boards), 1))
            selected_boards.extend(
                (str(row["board_type"]), str(row["board"]))
                for _, row in subset.iterrows()
            )

    for _, row in grouped.iterrows():
        key = (str(row["board_type"]), str(row["board"]))
        if key not in selected_boards:
            continue
        boards.append(BoardRankRow(
            board_type=str(row["board_type"]),
            board=str(row["board"]),
            stock_count=int(row["stock_count"]),
            flow_sum=round(float(row["flow_sum"]), 4),
            flow_mean=round(float(row["flow_mean"]), 4),
            as_of=str(row["as_of"]),
        ))

    for board_type, board in selected_boards:
        board_key = f"{board_type}:{board}"
        subset = merged[
            (merged["board_type"] == board_type)
            & (merged["board"] == board)
        ].sort_values(metric, ascending=False).head(max(int(top_stocks), 1))
        constituents[board_key] = [
            {
                "code": str(item["code"]),
                "ts_code": str(item["ts_code"]),
                "as_of": str(item["as_of"]),
                metric: _safe_float(item.get(metric)),
                "main_inflow_streak": _safe_int(item.get("main_inflow_streak")),
            }
            for item in subset.to_dict(orient="records")
        ]

    return BoardFlowRankResult(
        metric=metric,
        flow_definition=FLOW_DEFINITION,
        lookback_days=lookback_days,
        mapping_path=str(mapping_path),
        flow_store_root=str(store.root),
        stock_count=len(stock_flow),
        membership_rows=len(membership),
        boards=boards,
        constituents=constituents,
        notes=notes,
    )


def format_board_flow_explain(result: BoardFlowRankResult) -> str:
    lines = [
        (
            f"metric={result.metric} flow={result.flow_definition} "
            f"stocks={result.stock_count} membership={result.membership_rows}"
        ),
        f"flow_store={result.flow_store_root}",
        f"mapping={result.mapping_path}",
    ]
    for note in result.notes:
        lines.append(f"note: {note}")

    current_type = ""
    for board in result.boards:
        if board.board_type != current_type:
            current_type = board.board_type
            title = "行业" if current_type == "industry" else "概念"
            lines.append("")
            lines.append(f"=== {title}板块 Top ({result.metric} 合计, 万元) ===")
        lines.append(
            f"{board.board:<16} sum={board.flow_sum:>12.2f} "
            f"mean={board.flow_mean:>8.2f} count={board.stock_count} as_of={board.as_of}"
        )
        board_key = f"{board.board_type}:{board.board}"
        stocks = result.constituents.get(board_key, [])
        if stocks:
            lines.append(f"  {'code':<8}{'ts_code':<12}{result.metric:>14}  streak")
            for stock in stocks:
                metric_value = stock.get(result.metric)
                metric_text = "-" if metric_value is None else f"{float(metric_value):>14.2f}"
                streak = stock.get("main_inflow_streak")
                streak_text = "-" if streak is None else str(streak)
                lines.append(
                    f"  {stock['code']:<8}{stock['ts_code']:<12}{metric_text}  {streak_text}"
                )
    return "\n".join(lines).strip()


def _safe_int(value: object) -> int | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None
