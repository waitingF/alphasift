# -*- coding: utf-8 -*-
"""Local Tushare daily bar storage (Parquet per ts_code)."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from alphasift.daily_adjust import apply_adj
from alphasift.local_parquet_io import (
    atomic_write_parquet,
    atomic_write_text,
    read_parquet,
    require_pyarrow,
)

_RAW_COLUMNS = ["date", "open", "high", "low", "close", "volume", "amount"]
_ADJ_COLUMNS = ["date", "adj_factor"]


def compute_adj_factor_fingerprint(
    factors: pd.DataFrame,
    *,
    window_days: int = 60,
) -> str:
    """Return ``sha256:<hex>`` over the canonical adj_factor tail window."""
    df = factors[["date", "adj_factor"]].copy()
    df["date"] = df["date"].astype(str)
    df["adj_factor"] = pd.to_numeric(df["adj_factor"], errors="coerce").round(6)
    df = df.dropna(subset=["adj_factor"]).drop_duplicates("date", keep="last")
    df = df.sort_values("date").tail(window_days)
    payload = json.dumps(
        list(zip(df["date"].tolist(), df["adj_factor"].tolist())),
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize_ts_code(code: str) -> str:
    raw = str(code).strip().upper()
    if "." in raw:
        return raw
    digits = raw.zfill(6)[-6:]
    if digits.startswith(("4", "8", "920")):
        return f"{digits}.BJ"
    if digits.startswith(("6", "9", "5")):
        return f"{digits}.SH"
    return f"{digits}.SZ"


def normalize_date_yyyymmdd(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if "-" in text:
        return text.replace("-", "")[:8]
    return text[:8]


def format_date_iso(value: str) -> str:
    text = normalize_date_yyyymmdd(value) or ""
    if len(text) != 8:
        return text
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}"


class DailyBarStore:
    """Read/write local per-code Parquet daily bars (raw + adj_factor)."""

    def __init__(self, root: str | Path, *, adj: str = "qfq") -> None:
        self.root = Path(root)
        self.adj = adj if adj in {"qfq", "hfq"} else "qfq"
        self._bars_dir = self.root / "bars"
        self._raw_dir = self._bars_dir / "raw"
        self._adj_dir = self._bars_dir / "adj_factor"
        self._meta_dir = self._bars_dir / "meta"

    def has_code(self, code: str) -> bool:
        ts_code = normalize_ts_code(code)
        return (self._raw_dir / f"{ts_code}.parquet").is_file()

    def manifest(self) -> dict[str, object]:
        path = self.root / "manifest.json"
        if not path.is_file():
            raise RuntimeError(f"daily bar store manifest not found: {path}")
        return json.loads(path.read_text(encoding="utf-8"))

    def read_history(
        self,
        code: str,
        *,
        lookback_days: int = 120,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        require_pyarrow()
        ts_code = normalize_ts_code(code)
        raw_path = self._raw_dir / f"{ts_code}.parquet"
        adj_path = self._adj_dir / f"{ts_code}.parquet"
        if not raw_path.is_file():
            raise FileNotFoundError(f"missing raw daily bars for {ts_code}")

        raw = _read_parquet(raw_path)
        factors = _read_parquet(adj_path) if adj_path.is_file() else pd.DataFrame(columns=_ADJ_COLUMNS)
        raw = _sanitize_bars(raw)
        factors = _sanitize_factors(factors)

        end_yyyymmdd = normalize_date_yyyymmdd(end_date)
        if end_yyyymmdd:
            raw = raw[raw["date"].astype(str) <= end_yyyymmdd]
            factors = factors[factors["date"].astype(str) <= end_yyyymmdd]

        adj_mismatch = False
        if not factors.empty:
            raw_dates = set(raw["date"].astype(str))
            factor_dates = set(factors["date"].astype(str))
            adj_mismatch = raw_dates != factor_dates and not factor_dates.issuperset(raw_dates)

        if factors.empty and self.adj in {"qfq", "hfq"}:
            raise RuntimeError(f"missing adj_factor for {ts_code}")

        adjusted = apply_adj(raw, factors, adj=self.adj) if self.adj in {"qfq", "hfq"} else raw.copy()
        adjusted["date"] = adjusted["date"].astype(str).map(format_date_iso)
        adjusted = adjusted.sort_values("date")
        if end_yyyymmdd:
            end_iso = format_date_iso(end_yyyymmdd)
            adjusted = adjusted[adjusted["date"] <= end_iso]

        short_history = len(adjusted) < lookback_days
        result = adjusted.tail(max(int(lookback_days), 1)).copy()
        result.attrs["daily_source"] = "local"
        result.attrs["daily_end_date"] = end_yyyymmdd or (
            normalize_date_yyyymmdd(str(result["date"].iloc[-1])) if not result.empty else None
        )
        if short_history:
            result.attrs["short_history"] = True
        if adj_mismatch:
            result.attrs["adj_mismatch"] = True
        return result

    def read_sidecar(self, code: str) -> dict[str, object]:
        ts_code = normalize_ts_code(code)
        path = self._meta_dir / f"{ts_code}.json"
        if not path.is_file():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def write_sidecar(self, code: str, payload: dict[str, object]) -> None:
        ts_code = normalize_ts_code(code)
        self._meta_dir.mkdir(parents=True, exist_ok=True)
        path = self._meta_dir / f"{ts_code}.json"
        _atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2))

    def upsert_raw_bar(self, ts_code: str, row: dict[str, object]) -> None:
        require_pyarrow()
        path = self._raw_dir / f"{normalize_ts_code(ts_code)}.parquet"
        incoming = pd.DataFrame([_normalize_raw_row(row)])
        existing = _read_parquet(path) if path.is_file() else pd.DataFrame(columns=_RAW_COLUMNS)
        merged = _merge_by_date(existing, incoming)
        _atomic_write_parquet(path, merged)
        self._update_sidecar_from_bars(normalize_ts_code(ts_code), merged)

    def replace_raw(self, ts_code: str, df: pd.DataFrame) -> None:
        require_pyarrow()
        path = self._raw_dir / f"{normalize_ts_code(ts_code)}.parquet"
        merged = _sanitize_bars(df)
        _atomic_write_parquet(path, merged)
        self._update_sidecar_from_bars(normalize_ts_code(ts_code), merged)

    def upsert_adj_factor_row(self, ts_code: str, row: dict[str, object]) -> None:
        require_pyarrow()
        path = self._adj_dir / f"{normalize_ts_code(ts_code)}.parquet"
        incoming = pd.DataFrame([_normalize_adj_row(row)])
        existing = _read_parquet(path) if path.is_file() else pd.DataFrame(columns=_ADJ_COLUMNS)
        merged = _merge_by_date(existing, incoming)
        _atomic_write_parquet(path, merged)
        self._update_sidecar_fingerprint(normalize_ts_code(ts_code), merged)

    def replace_adj_factor(self, ts_code: str, df: pd.DataFrame) -> None:
        require_pyarrow()
        path = self._adj_dir / f"{normalize_ts_code(ts_code)}.parquet"
        merged = _sanitize_factors(df)
        _atomic_write_parquet(path, merged)
        self._update_sidecar_fingerprint(normalize_ts_code(ts_code), merged)

    def write_manifest(self, payload: dict[str, object]) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        _atomic_write_text(self.root / "manifest.json", json.dumps(payload, ensure_ascii=False, indent=2))

    def last_raw_trade_date(self, code: str) -> str | None:
        ts_code = normalize_ts_code(code)
        path = self._raw_dir / f"{ts_code}.parquet"
        if not path.is_file():
            return None
        raw = _sanitize_bars(_read_parquet(path))
        if raw.empty:
            return None
        return str(raw["date"].iloc[-1])

    def local_adj_factor_series(self, code: str) -> pd.DataFrame:
        ts_code = normalize_ts_code(code)
        path = self._adj_dir / f"{ts_code}.parquet"
        if not path.is_file():
            return pd.DataFrame(columns=_ADJ_COLUMNS)
        return _sanitize_factors(_read_parquet(path))

    def _update_sidecar_from_bars(self, ts_code: str, raw: pd.DataFrame) -> None:
        if raw.empty:
            return
        sidecar = self.read_sidecar(ts_code)
        sidecar["ts_code"] = ts_code
        sidecar["last_trade_date"] = str(raw["date"].iloc[-1])
        adj_path = self._adj_dir / f"{ts_code}.parquet"
        if adj_path.is_file():
            factors = _sanitize_factors(_read_parquet(adj_path))
            if not factors.empty:
                sidecar["latest_adj_factor"] = float(factors["adj_factor"].iloc[-1])
                sidecar["latest_adj_factor_date"] = str(factors["date"].iloc[-1])
                sidecar["adj_factor_fingerprint"] = compute_adj_factor_fingerprint(factors)
        self.write_sidecar(ts_code, sidecar)

    def _update_sidecar_fingerprint(self, ts_code: str, factors: pd.DataFrame) -> None:
        if factors.empty:
            return
        sidecar = self.read_sidecar(ts_code)
        sidecar["ts_code"] = ts_code
        sidecar["latest_adj_factor"] = float(factors["adj_factor"].iloc[-1])
        sidecar["latest_adj_factor_date"] = str(factors["date"].iloc[-1])
        sidecar["adj_factor_fingerprint"] = compute_adj_factor_fingerprint(factors)
        sidecar["last_rebuild_at"] = datetime.now(timezone.utc).isoformat()
        self.write_sidecar(ts_code, sidecar)


def adj_factor_rebuild_required(
    store: DailyBarStore,
    ts_code: str,
    adj_row: dict[str, object] | None,
) -> bool:
    """Return True when local adj_factor history must be rebuilt for ts_code."""
    sidecar = store.read_sidecar(ts_code)
    local_factors = store.local_adj_factor_series(ts_code)
    if not sidecar or local_factors.empty:
        return True
    if adj_row is None:
        return True

    adj_t = round(float(adj_row.get("adj_factor", 0)), 6)
    latest = sidecar.get("latest_adj_factor")
    if latest is not None and round(float(latest), 6) != adj_t:
        return True

    fingerprint = sidecar.get("adj_factor_fingerprint")
    if fingerprint:
        recomputed = compute_adj_factor_fingerprint(local_factors)
        if fingerprint != recomputed:
            return True
    return False


def _normalize_raw_row(row: dict[str, object]) -> dict[str, object]:
    date_val = row.get("date") or row.get("trade_date")
    return {
        "date": normalize_date_yyyymmdd(str(date_val)),
        "open": float(row.get("open", 0)),
        "high": float(row.get("high", 0)),
        "low": float(row.get("low", 0)),
        "close": float(row.get("close", 0)),
        "volume": float(row.get("volume", row.get("vol", 0))),
        "amount": float(row.get("amount", 0) or 0),
    }


def _normalize_adj_row(row: dict[str, object]) -> dict[str, object]:
    date_val = row.get("date") or row.get("trade_date")
    return {
        "date": normalize_date_yyyymmdd(str(date_val)),
        "adj_factor": float(row.get("adj_factor", 0)),
    }


def _sanitize_bars(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=_RAW_COLUMNS)
    frame = df.copy()
    if "trade_date" in frame.columns and "date" not in frame.columns:
        frame = frame.rename(columns={"trade_date": "date"})
    if "vol" in frame.columns and "volume" not in frame.columns:
        frame = frame.rename(columns={"vol": "volume"})
    frame["date"] = frame["date"].astype(str).map(normalize_date_yyyymmdd)
    for col in ("open", "high", "low", "close", "volume", "amount"):
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame.dropna(subset=["open", "high", "low", "close"])
    frame = frame.drop_duplicates("date", keep="last").sort_values("date")
    return frame[_RAW_COLUMNS].reset_index(drop=True)


def _sanitize_factors(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=_ADJ_COLUMNS)
    frame = df.copy()
    if "trade_date" in frame.columns and "date" not in frame.columns:
        frame = frame.rename(columns={"trade_date": "date"})
    frame["date"] = frame["date"].astype(str).map(normalize_date_yyyymmdd)
    frame["adj_factor"] = pd.to_numeric(frame["adj_factor"], errors="coerce")
    frame = frame.dropna(subset=["adj_factor"])
    frame = frame.drop_duplicates("date", keep="last").sort_values("date")
    return frame[_ADJ_COLUMNS].reset_index(drop=True)


def _merge_by_date(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([existing, incoming], ignore_index=True)
    if "date" not in combined.columns:
        return incoming
    combined["date"] = combined["date"].astype(str)
    combined = combined.drop_duplicates("date", keep="last").sort_values("date")
    return combined.reset_index(drop=True)


_read_parquet = read_parquet
_atomic_write_parquet = atomic_write_parquet
_atomic_write_text = atomic_write_text
