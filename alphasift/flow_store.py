# -*- coding: utf-8 -*-
"""Local Tushare moneyflow storage (Parquet per ts_code)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from alphasift.daily_store import format_date_iso, normalize_date_yyyymmdd, normalize_ts_code, require_pyarrow
from alphasift.flow_specs import MONEYFLOW_RAW_COLUMNS, TIER_BUY_AMOUNT_COLUMNS, TIER_SELL_AMOUNT_COLUMNS
from alphasift.local_parquet_io import atomic_write_parquet, atomic_write_text, read_parquet


def compute_flow_derived_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Compute main/retail net inflow from tiered buy/sell amounts (万元)."""
    if frame is None or frame.empty:
        return frame

    result = frame.copy()
    for column in (*TIER_BUY_AMOUNT_COLUMNS, *TIER_SELL_AMOUNT_COLUMNS):
        if column not in result.columns:
            result[column] = 0.0
        result[column] = pd.to_numeric(result[column], errors="coerce").fillna(0.0)

    result["main_net_inflow"] = (
        result["buy_lg_amount"]
        + result["buy_elg_amount"]
        - result["sell_lg_amount"]
        - result["sell_elg_amount"]
    )
    result["retail_net_inflow"] = (
        result["buy_sm_amount"]
        + result["buy_md_amount"]
        - result["sell_sm_amount"]
        - result["sell_md_amount"]
    )
    return result


class FlowBarStore:
    """Read/write local per-code Parquet moneyflow bars."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self._moneyflow_dir = self.root / "moneyflow"
        self._meta_dir = self.root / "meta"

    def has_code(self, code: str) -> bool:
        ts_code = normalize_ts_code(code)
        return (self._moneyflow_dir / f"{ts_code}.parquet").is_file()

    def manifest(self) -> dict[str, object]:
        path = self.root / "manifest.json"
        if not path.is_file():
            raise RuntimeError(f"flow bar store manifest not found: {path}")
        return json.loads(path.read_text(encoding="utf-8"))

    def read(
        self,
        code: str,
        *,
        lookback_days: int = 60,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        require_pyarrow()
        ts_code = normalize_ts_code(code)
        path = self._moneyflow_dir / f"{ts_code}.parquet"
        if not path.is_file():
            raise FileNotFoundError(f"missing moneyflow bars for {ts_code}")

        frame = _sanitize_moneyflow(_read_parquet(path))
        end_iso = format_date_iso(end_date) if end_date else None
        if end_iso:
            frame = frame[frame["trade_date"].astype(str) <= end_iso]

        frame = frame.sort_values("trade_date")
        result = frame.tail(max(int(lookback_days), 1)).copy()
        result.attrs["flow_source"] = "local"
        result.attrs["flow_end_date"] = end_date or (
            normalize_date_yyyymmdd(str(result["trade_date"].iloc[-1])) if not result.empty else None
        )
        if len(frame) < lookback_days:
            result.attrs["short_history"] = True
        return result

    def write(self, code: str, frame: pd.DataFrame) -> None:
        """Reconcile and write moneyflow rows for one ts_code."""
        require_pyarrow()
        ts_code = normalize_ts_code(code)
        path = self._moneyflow_dir / f"{ts_code}.parquet"
        incoming = _sanitize_moneyflow(frame)
        existing = _read_parquet(path) if path.is_file() else pd.DataFrame(columns=list(MONEYFLOW_RAW_COLUMNS))
        merged = _merge_by_trade_date(existing, incoming)
        _atomic_write_parquet(path, merged)

    def reconcile_and_write(self, code: str, remote: pd.DataFrame | None) -> dict[str, int]:
        ts_code = normalize_ts_code(code)
        local = _sanitize_moneyflow(_read_parquet(self._moneyflow_dir / f"{ts_code}.parquet")) if self.has_code(code) else pd.DataFrame(columns=list(MONEYFLOW_RAW_COLUMNS))
        remote_frame = _sanitize_moneyflow(remote)
        stats = {"added": 0, "updated": 0, "unchanged": 0, "remote_total": int(len(remote_frame))}

        if remote_frame.empty:
            return stats

        indexed_local = local.set_index("trade_date") if not local.empty else None
        merged_rows: list[dict[str, object]] = []
        seen_dates: set[str] = set()

        for _, remote_row in remote_frame.iterrows():
            trade_date = str(remote_row["trade_date"])
            seen_dates.add(trade_date)
            record = remote_row.to_dict()
            if indexed_local is not None and trade_date in indexed_local.index:
                local_row = indexed_local.loc[trade_date]
                if isinstance(local_row, pd.DataFrame):
                    local_row = local_row.iloc[-1]
                if _rows_equal(local_row.to_dict(), record, ignore_keys={"trade_date"}):
                    stats["unchanged"] += 1
                else:
                    stats["updated"] += 1
            else:
                stats["added"] += 1
            merged_rows.append(record)

        if indexed_local is not None:
            for trade_date, local_row in indexed_local.iterrows():
                if str(trade_date) in seen_dates:
                    continue
                row_dict = local_row.to_dict()
                row_dict["trade_date"] = str(trade_date)
                merged_rows.append(row_dict)

        if not merged_rows:
            return stats

        merged = pd.DataFrame(merged_rows)
        merged = _sanitize_moneyflow(merged)
        if stats["added"] or stats["updated"] or not self.has_code(code):
            self.write(ts_code, merged)
        return stats

    def list_codes(self) -> list[str]:
        if not self._moneyflow_dir.is_dir():
            return []
        return sorted(path.stem for path in self._moneyflow_dir.glob("*.parquet"))

    def write_manifest(self, payload: dict[str, object]) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        _atomic_write_text(self.root / "manifest.json", json.dumps(payload, ensure_ascii=False, indent=2))

    def last_trade_date(self, code: str) -> str | None:
        ts_code = normalize_ts_code(code)
        path = self._moneyflow_dir / f"{ts_code}.parquet"
        if not path.is_file():
            return None
        frame = _sanitize_moneyflow(_read_parquet(path))
        if frame.empty:
            return None
        return str(frame["trade_date"].iloc[-1])


def _sanitize_moneyflow(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=list(MONEYFLOW_RAW_COLUMNS))

    frame = df.copy()
    if "trade_date" not in frame.columns and "date" in frame.columns:
        frame = frame.rename(columns={"date": "trade_date"})

    frame["trade_date"] = frame["trade_date"].astype(str).map(_normalize_trade_date_iso)
    frame = frame.dropna(subset=["trade_date"])

    numeric_cols = [
        col for col in frame.columns
        if col not in {"ts_code", "trade_date"}
    ]
    for col in numeric_cols:
        frame[col] = pd.to_numeric(frame[col], errors="coerce")

    frame = compute_flow_derived_columns(frame)
    for col in MONEYFLOW_RAW_COLUMNS:
        if col not in frame.columns:
            frame[col] = pd.NA

    frame = frame.drop_duplicates("trade_date", keep="last").sort_values("trade_date")
    return frame[list(MONEYFLOW_RAW_COLUMNS)].reset_index(drop=True)


def _normalize_trade_date_iso(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    normalized = normalize_date_yyyymmdd(text.replace("-", "")[:8] if "-" in text else text)
    if not normalized:
        return None
    return format_date_iso(normalized)


def _merge_by_trade_date(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return _sanitize_moneyflow(incoming)
    if incoming.empty:
        return _sanitize_moneyflow(existing)
    combined = pd.concat([existing, incoming], ignore_index=True)
    if combined.empty:
        return combined
    combined["trade_date"] = combined["trade_date"].astype(str)
    combined = combined.drop_duplicates("trade_date", keep="last").sort_values("trade_date")
    return _sanitize_moneyflow(combined.reset_index(drop=True))


def _rows_equal(
    local_row: dict[str, object],
    remote_row: dict[str, object],
    *,
    ignore_keys: set[str] | None = None,
) -> bool:
    skip = ignore_keys or set()
    for key, right in remote_row.items():
        if key in skip:
            continue
        left = local_row.get(key)
        left_na = left is None or (isinstance(left, float) and pd.isna(left)) or left is pd.NA
        right_na = right is None or (isinstance(right, float) and pd.isna(right)) or right is pd.NA
        if left_na and right_na:
            continue
        if left_na or right_na:
            return False
        if isinstance(left, float) or isinstance(right, float):
            try:
                if abs(float(left) - float(right)) > 1e-6:
                    return False
                continue
            except (ValueError, TypeError):
                pass
        if str(left) != str(right):
            return False
    return True


_read_parquet = read_parquet
_atomic_write_parquet = atomic_write_parquet
_atomic_write_text = atomic_write_text
