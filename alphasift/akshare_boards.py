# -*- coding: utf-8 -*-
"""AkShare board list helpers with East Money -> Tonghuashun fallback."""

from __future__ import annotations

import time
from typing import Callable, Literal

import pandas as pd

from alphasift.normalize import safe_float as _safe_float
from alphasift.normalize import safe_text as _safe_text

BoardKind = Literal["concept", "industry"]
BoardBackend = Literal["em", "ths"]

BoardListResult = tuple[pd.DataFrame | None, BoardBackend | None, str | None]


def fetch_board_list_frame(
    board_kind: BoardKind,
    *,
    max_retries: int = 2,
    retry_interval: float = 1.0,
) -> BoardListResult:
    """Fetch a concept/industry board list, trying EM then THS."""
    em_frame, em_error = _fetch_with_retries(
        lambda: _fetch_em_board_frame(board_kind),
        max_retries=max_retries,
        retry_interval=retry_interval,
    )
    if em_frame is not None and not em_frame.empty:
        return em_frame, "em", None

    ths_frame, ths_error = _fetch_with_retries(
        lambda: _fetch_ths_board_frame(board_kind),
        max_retries=max_retries,
        retry_interval=retry_interval,
    )
    if ths_frame is not None and not ths_frame.empty:
        note = f"akshare {board_kind} board list fallback: ths"
        if em_error is not None:
            note = f"{note} (em failed: {em_error})"
        return ths_frame, "ths", note

    if em_error and ths_error:
        return None, None, f"em: {em_error}; ths: {ths_error}"
    if em_error:
        return None, None, f"em: {em_error}"
    return None, None, f"ths: {ths_error or 'empty board list'}"


def board_leader_name(frame: pd.DataFrame, board_name: str) -> str:
    """Return a leader stock name from a normalized THS industry summary frame."""
    if frame is None or frame.empty or "leader_name" not in frame.columns:
        return ""
    name_key = _safe_text(board_name)
    if not name_key:
        return ""
    for _, row in frame.iterrows():
        if _safe_text(row.get("board_name")) == name_key:
            return _safe_text(row.get("leader_name"))
    return ""


def board_leader_change_pct(frame: pd.DataFrame, board_name: str) -> float | None:
    if frame is None or frame.empty or "leader_change_pct" not in frame.columns:
        return None
    name_key = _safe_text(board_name)
    if not name_key:
        return None
    for _, row in frame.iterrows():
        if _safe_text(row.get("board_name")) == name_key:
            return _safe_float(row.get("leader_change_pct"))
    return None


def _fetch_with_retries(
    fetcher: Callable[[], pd.DataFrame | None],
    *,
    max_retries: int,
    retry_interval: float,
) -> tuple[pd.DataFrame | None, str | None]:
    attempts = max(int(max_retries), 0) + 1
    last_error: str | None = None
    for attempt in range(attempts):
        try:
            frame = fetcher()
        except Exception as exc:  # noqa: BLE001 - provider instability is degraded.
            last_error = str(exc)
            if attempt >= attempts - 1 or not _is_transient_error(exc):
                return None, last_error
            time.sleep(retry_interval * (2 ** attempt))
            continue
        if frame is not None and not frame.empty:
            return frame, None
        last_error = "empty board list"
        if attempt >= attempts - 1:
            return None, last_error
        time.sleep(retry_interval * (2 ** attempt))
    return None, last_error


def _fetch_em_board_frame(board_kind: BoardKind) -> pd.DataFrame | None:
    import akshare as ak

    if board_kind == "concept":
        return ak.stock_board_concept_name_em()
    return ak.stock_board_industry_name_em()


def _fetch_ths_board_frame(board_kind: BoardKind) -> pd.DataFrame | None:
    import akshare as ak

    if board_kind == "industry":
        raw = ak.stock_board_industry_summary_ths()
        if raw is None or raw.empty:
            return None
        rows: list[dict[str, object]] = []
        for idx, row in raw.iterrows():
            board_name = _safe_text(_row_value(row, ["板块", "board_name", "名称", "name"]))
            if not board_name:
                continue
            rank = _safe_float(_row_value(row, ["序号", "rank"]))
            if rank is None:
                rank = float(idx + 1)
            change_pct = _safe_float(_row_value(row, ["涨跌幅", "change_pct", "涨幅"]))
            rows.append({
                "board_name": board_name,
                "rank": rank,
                "change_pct": change_pct,
                "leader_name": _safe_text(_row_value(row, ["领涨股", "leader_name"])),
                "leader_change_pct": _safe_float(_row_value(row, ["领涨股-涨跌幅", "leader_change_pct"])),
            })
        return pd.DataFrame(rows)

    raw = ak.stock_board_concept_name_ths()
    if raw is None or raw.empty:
        return None
    rows = []
    for idx, row in raw.iterrows():
        board_name = _safe_text(_row_value(row, ["name", "名称", "板块名称"]))
        if not board_name:
            continue
        rows.append({
            "board_name": board_name,
            "rank": float(idx + 1),
            "change_pct": None,
            "leader_name": "",
            "leader_change_pct": None,
        })
    return pd.DataFrame(rows)


def _is_transient_error(exc: Exception) -> bool:
    text = str(exc).lower()
    patterns = (
        "connection aborted",
        "remote end closed connection",
        "connection reset",
        "timed out",
        "timeout",
        "temporarily unavailable",
        "too many requests",
        "429",
        "502",
        "503",
        "504",
    )
    return any(pattern in text for pattern in patterns)


def _row_value(row: pd.Series, columns: list[str]) -> object:
    for column in columns:
        if column in row:
            return row.get(column)
    return None
