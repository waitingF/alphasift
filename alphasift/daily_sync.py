# -*- coding: utf-8 -*-
"""Tushare daily bar sync orchestration for local DailyBarStore."""

from __future__ import annotations

import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Protocol

from alphasift.daily import _configure_tushare_client
from alphasift.daily_store import (
    DailyBarStore,
    adj_factor_rebuild_required,
    normalize_date_yyyymmdd,
    normalize_ts_code,
    require_pyarrow,
)

_TRANSIENT_PATTERNS = (
    "timeout",
    "timed out",
    "connection reset",
    "connection aborted",
    "temporarily unavailable",
    "请稍后再试",
    "too many requests",
)


class _LocalStoreRoot(Protocol):
    root: Path


class _LocalStoreWithManifest(Protocol):
    root: Path

    def manifest(self) -> dict[str, object]: ...


@dataclass
class SyncStats:
    added_rows: int = 0
    updated_codes: int = 0
    rebuilt_codes: list[str] = field(default_factory=list)
    failed_codes: list[str] = field(default_factory=list)
    source_errors: list[str] = field(default_factory=list)
    api_attempts: int = 0
    api_retries: int = 0
    api_failures: int = 0


@dataclass
class SyncProgress:
    signature: dict[str, object]
    next_index: int = 0
    symbols: list[str] = field(default_factory=list)
    updated: int = 0
    skipped: int = 0
    failed: int = 0
    rebuilt: int = 0
    last_symbol: str = ""
    errors: list[dict[str, str]] = field(default_factory=list)
    api_stats: dict[str, int] = field(default_factory=lambda: {"attempts": 0, "retries": 0, "failures": 0})

    def to_dict(self) -> dict[str, object]:
        return {
            "signature": self.signature,
            "next_index": self.next_index,
            "symbols": self.symbols,
            "updated": self.updated,
            "skipped": self.skipped,
            "failed": self.failed,
            "rebuilt": self.rebuilt,
            "last_symbol": self.last_symbol,
            "errors": self.errors,
            "api_stats": self.api_stats,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> SyncProgress:
        return cls(
            signature=dict(payload.get("signature", {}) or {}),
            next_index=int(payload.get("next_index", 0)),
            symbols=[str(item) for item in payload.get("symbols", []) or []],
            updated=int(payload.get("updated", 0)),
            skipped=int(payload.get("skipped", 0)),
            failed=int(payload.get("failed", 0)),
            rebuilt=int(payload.get("rebuilt", 0)),
            last_symbol=str(payload.get("last_symbol", "")),
            errors=[dict(item) for item in payload.get("errors", []) or []],
            api_stats=dict(payload.get("api_stats", {}) or {"attempts": 0, "retries": 0, "failures": 0}),
        )


class TushareSyncClient:
    """Shared rate limiter + retry wrapper for sync API calls."""

    def __init__(
        self,
        pro: object,
        *,
        requests_per_second: float = 2.0,
        retry: int = 3,
        retry_interval: float = 1.0,
    ) -> None:
        self.pro = pro
        self.requests_per_second = max(float(requests_per_second), 0.0)
        self.retry = max(int(retry), 0)
        self.retry_interval = max(float(retry_interval), 0.0)
        self._lock = threading.Lock()
        self._next_allowed = 0.0
        self.stats = {"attempts": 0, "retries": 0, "failures": 0}

    def call(self, fn: Callable[..., object], /, *args, **kwargs):
        attempts = self.retry + 1
        last_error: Exception | None = None
        for attempt in range(attempts):
            self._acquire()
            self.stats["attempts"] += 1
            try:
                return fn(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if not _is_transient_error(exc) or attempt >= attempts - 1:
                    self.stats["failures"] += 1
                    raise
                self.stats["retries"] += 1
                time.sleep(self.retry_interval * (2 ** attempt))
        raise last_error  # pragma: no cover

    def _acquire(self) -> None:
        if self.requests_per_second <= 0:
            return
        interval = 1.0 / self.requests_per_second
        with self._lock:
            now = time.monotonic()
            if now < self._next_allowed:
                time.sleep(self._next_allowed - now)
            self._next_allowed = max(now, self._next_allowed) + interval


class _SymbolProgressBar:
    """Single-line tqdm progress for per-symbol init/sync work."""

    def __init__(
        self,
        *,
        total: int,
        initial: int,
        enabled: bool,
        desc: str,
    ) -> None:
        self._bar = None
        if not enabled or total <= initial:
            return
        try:
            from tqdm import tqdm

            self._bar = tqdm(
                total=total,
                initial=initial,
                desc=desc,
                unit="code",
                dynamic_ncols=True,
                leave=True,
            )
        except ImportError:
            self._bar = None

    @property
    def enabled(self) -> bool:
        return self._bar is not None

    def update(
        self,
        *,
        last_symbol: str,
        updated: int,
        skipped: int,
        failed: int,
    ) -> None:
        if self._bar is None:
            return
        self._bar.update(1)
        self._bar.set_postfix(
            ok=updated,
            skip=skipped,
            fail=failed,
            last=last_symbol,
            refresh=False,
        )

    def close(self) -> None:
        if self._bar is not None:
            self._bar.close()
            self._bar = None

    @staticmethod
    def write(message: str) -> None:
        try:
            from tqdm import tqdm

            tqdm.write(message)
        except ImportError:
            import sys

            print(message, file=sys.stderr, flush=True)


def _make_sync_client(
    token: str,
    *,
    requests_per_second: float = 2.0,
    retry: int = 3,
    retry_interval: float = 1.0,
) -> tuple[object, TushareSyncClient]:
    require_pyarrow()
    import tushare as ts

    pro = ts.pro_api(token)
    _configure_tushare_client(pro, token=token)
    client = TushareSyncClient(
        pro,
        requests_per_second=requests_per_second,
        retry=retry,
        retry_interval=retry_interval,
    )
    return pro, client


def _fetch_and_replace_code(
    store: DailyBarStore,
    client: TushareSyncClient,
    pro: object,
    ts_code: str,
    start: str,
    end: str,
) -> int:
    raw = client.call(
        pro.daily,
        ts_code=ts_code,
        start_date=start,
        end_date=end,
        fields="ts_code,trade_date,open,high,low,close,vol,amount",
    )
    factors = client.call(
        pro.adj_factor,
        ts_code=ts_code,
        start_date=start,
        end_date=end,
        fields="trade_date,adj_factor",
    )
    if raw is None or raw.empty:
        raise RuntimeError("empty daily")
    if factors is None or factors.empty:
        raise RuntimeError("empty adj_factor")
    store.replace_raw(ts_code, raw)
    store.replace_adj_factor(ts_code, factors)
    return len(raw)


def init_daily_bars(
    store: DailyBarStore,
    *,
    token: str,
    lookback_days: int = 800,
    max_codes: int | None = None,
    workers: int = 4,
    include_st: bool = False,
    requests_per_second: float = 2.0,
    retry: int = 3,
    retry_interval: float = 1.0,
    progress_dir: Path | None = None,
    save_every: int = 50,
    save_interval: float = 15.0,
    reset_progress: bool = False,
    end_date: str | None = None,
    show_progress: bool = True,
) -> SyncStats:
    pro, client = _make_sync_client(
        token,
        requests_per_second=requests_per_second,
        retry=retry,
        retry_interval=retry_interval,
    )
    end = normalize_date_yyyymmdd(end_date) or _latest_trade_date(client)
    universe = _load_universe(client, include_st=include_st)
    if max_codes is not None:
        universe = universe[: max_codes]

    signature = {"command": "init", "lookback_days": lookback_days, "end_date": end}
    progress_path = _progress_path(store, progress_dir)
    progress = _load_or_reset_progress(progress_path, signature, reset_progress)
    if not progress.symbols:
        progress.symbols = [row["ts_code"] for row in universe]

    worker_count = max(1, int(workers))
    progress_bar = _SymbolProgressBar(
        total=len(progress.symbols),
        initial=progress.next_index,
        enabled=show_progress,
        desc="daily-bars init",
    )
    if show_progress and not progress_bar.enabled:
        _SymbolProgressBar.write(
            'tqdm not installed; install with: pip install "alphasift[daily-store]"'
        )

    stats = SyncStats()
    last_save = time.monotonic()
    progress_lock = threading.Lock()
    universe_by_code = {row["ts_code"]: row for row in universe}

    def process_symbol(index: int, ts_code: str) -> tuple[int, str, str | None, int, str | None]:
        meta = universe_by_code.get(ts_code)
        if meta is None:
            return index, ts_code, "skip", 0, None

        sidecar_date = store.read_sidecar(ts_code).get("last_trade_date")
        if sidecar_date and normalize_date_yyyymmdd(str(sidecar_date)) >= end:
            return index, ts_code, "skip", 0, None

        start = _init_start_date(meta.get("list_date"), end, lookback_days)
        try:
            rows = _fetch_and_replace_code(store, client, pro, ts_code, start, end)
            return index, ts_code, "ok", rows, None
        except Exception as exc:  # noqa: BLE001
            return index, ts_code, "fail", 0, str(exc)

    try:
        index = progress.next_index
        while index < len(progress.symbols):
            batch_end = min(index + worker_count, len(progress.symbols))
            batch = [(idx, progress.symbols[idx]) for idx in range(index, batch_end)]
            with ThreadPoolExecutor(max_workers=min(worker_count, len(batch))) as executor:
                futures = [
                    executor.submit(process_symbol, idx, ts_code)
                    for idx, ts_code in batch
                ]
                for future in as_completed(futures):
                    idx, ts_code, status, rows, error = future.result()
                    progress.last_symbol = ts_code
                    with progress_lock:
                        if status == "skip":
                            progress.skipped += 1
                        elif status == "ok":
                            stats.updated_codes += 1
                            stats.added_rows += rows
                            progress.updated += 1
                        else:
                            stats.failed_codes.append(ts_code)
                            progress.failed += 1
                            progress.errors.append({"ts_code": ts_code, "error": error or "unknown"})
                        progress_bar.update(
                            last_symbol=ts_code,
                            updated=progress.updated,
                            skipped=progress.skipped,
                            failed=progress.failed,
                        )
            progress.next_index = batch_end
            index = batch_end
            last_save = _maybe_save_progress(
                progress_path,
                progress,
                client,
                last_save,
                save_every=save_every,
                save_interval=save_interval,
            )
    except KeyboardInterrupt:
        progress_bar.close()
        _save_progress(progress_path, progress, client)
        _SymbolProgressBar.write(
            "daily-bars init interrupted; rerun the same command to resume "
            f"({progress.next_index}/{len(progress.symbols)} codes processed). "
            f"Progress saved to {progress_path}"
        )
        raise

    progress_bar.close()
    _finalize_manifest(store, stats, client, end_date=end, command="init")
    _delete_progress(progress_path)
    stats.api_attempts = client.stats["attempts"]
    stats.api_retries = client.stats["retries"]
    stats.api_failures = client.stats["failures"]
    return stats


def sync_daily_bars(
    store: DailyBarStore,
    *,
    token: str,
    requests_per_second: float = 2.0,
    retry: int = 3,
    retry_interval: float = 1.0,
    include_st: bool = False,
    lookback_days: int = 800,
    end_date: str | None = None,
) -> SyncStats:
    pro, client = _make_sync_client(
        token,
        requests_per_second=requests_per_second,
        retry=retry,
        retry_interval=retry_interval,
    )
    manifest = store.manifest()
    last_trade_date = normalize_date_yyyymmdd(str(manifest.get("last_trade_date", "")))
    end = normalize_date_yyyymmdd(end_date) or _latest_trade_date(client)
    if not last_trade_date:
        raise RuntimeError("manifest missing last_trade_date; run daily-bars init first")

    missing_dates = _read_trade_cal_dates(client, start_date=last_trade_date, end_date=end)[1:]
    stats = SyncStats()
    universe = {row["ts_code"] for row in _load_universe(client, include_st=include_st)}

    for trade_date in missing_dates:
        try:
            daily_t = client.call(
                pro.daily,
                trade_date=trade_date,
                fields="ts_code,trade_date,open,high,low,close,vol,amount",
            )
            adj_t = client.call(
                pro.adj_factor,
                trade_date=trade_date,
                fields="ts_code,trade_date,adj_factor",
            )
        except Exception as exc:  # noqa: BLE001
            stats.source_errors.append(f"{trade_date}: {exc}")
            stats.api_failures += 1
            break

        daily_rows = {} if daily_t is None or daily_t.empty else {
            str(row.ts_code): row for row in daily_t.itertuples(index=False)
        }
        adj_rows = {} if adj_t is None or adj_t.empty else {
            str(row.ts_code): row for row in adj_t.itertuples(index=False)
        }

        for ts_code, row in daily_rows.items():
            if ts_code not in universe:
                continue
            adj_row = adj_rows.get(ts_code)
            if adj_row is None:
                stats.failed_codes.append(ts_code)
                continue
            gap_start = _gap_fill_start(store, ts_code, trade_date, client)
            if gap_start:
                _rebuild_code_window(store, client, pro, ts_code, gap_start, trade_date, stats, lookback_days)
                continue

            store.upsert_raw_bar(
                ts_code,
                {
                    "trade_date": row.trade_date,
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "vol": row.vol,
                    "amount": getattr(row, "amount", 0),
                },
            )
            rebuild = adj_factor_rebuild_required(
                store,
                ts_code,
                {"adj_factor": adj_row.adj_factor, "trade_date": adj_row.trade_date},
            )
            if rebuild:
                start = _init_start_date(None, trade_date, lookback_days)
                _rebuild_code_window(store, client, pro, ts_code, start, trade_date, stats, lookback_days)
            else:
                store.upsert_adj_factor_row(
                    ts_code,
                    {"trade_date": adj_row.trade_date, "adj_factor": adj_row.adj_factor},
                )
                stats.updated_codes += 1
                stats.added_rows += 1

        manifest["last_trade_date"] = trade_date

    _finalize_manifest(store, stats, client, end_date=manifest.get("last_trade_date"), command="sync")
    stats.api_attempts = client.stats["attempts"]
    stats.api_retries = client.stats["retries"]
    stats.api_failures = client.stats.get("failures", 0)
    return stats


def fetch_daily_bars(
    store: DailyBarStore,
    codes: list[str],
    *,
    token: str,
    lookback_days: int = 120,
    requests_per_second: float = 2.0,
    retry: int = 3,
    retry_interval: float = 1.0,
    end_date: str | None = None,
) -> SyncStats:
    pro, client = _make_sync_client(
        token,
        requests_per_second=requests_per_second,
        retry=retry,
        retry_interval=retry_interval,
    )
    end = normalize_date_yyyymmdd(end_date) or _latest_trade_date(client)
    stats = SyncStats()
    for code in codes:
        ts_code = normalize_ts_code(code)
        start = _init_start_date(None, end, lookback_days)
        try:
            rows = _fetch_and_replace_code(store, client, pro, ts_code, start, end)
            stats.updated_codes += 1
            stats.added_rows += rows
        except Exception:  # noqa: BLE001
            stats.failed_codes.append(ts_code)
    _finalize_manifest(store, stats, client, end_date=end, command="fetch")
    stats.api_attempts = client.stats["attempts"]
    stats.api_retries = client.stats["retries"]
    stats.api_failures = client.stats["failures"]
    return stats


def status_daily_bars(store: DailyBarStore, *, effective_trade_date: str | None = None) -> dict[str, object]:
    raw_dir = store.root / "bars" / "raw"
    raw_count = len(list(raw_dir.glob("*.parquet"))) if raw_dir.is_dir() else 0
    return build_store_status_summary(
        store,
        effective_trade_date=effective_trade_date,
        file_count=raw_count,
        file_count_key="raw_file_count",
    )


def build_store_status_summary(
    store: _LocalStoreWithManifest,
    *,
    effective_trade_date: str | None,
    file_count: int,
    file_count_key: str,
) -> dict[str, object]:
    manifest: dict[str, object] = {}
    manifest_error: str | None = None
    try:
        manifest = store.manifest()
    except Exception as exc:  # noqa: BLE001
        manifest_error = str(exc)

    last = normalize_date_yyyymmdd(str(manifest.get("last_trade_date", "")))
    effective = normalize_date_yyyymmdd(effective_trade_date)
    stale = bool(effective and last and last < effective)
    ahead = bool(effective and last and last > effective)
    in_progress = load_sync_progress(store)
    return {
        "root": str(store.root),
        "manifest": manifest,
        "manifest_error": manifest_error,
        "last_trade_date": last,
        "effective_trade_date": effective,
        "stale_vs_effective": stale,
        "ahead_of_effective": ahead,
        "code_count": manifest.get("code_count", file_count),
        file_count_key: file_count,
        "failed_codes": list((manifest.get("sync_stats") or {}).get("failed_codes", [])),
        "in_progress": in_progress,
    }


def _rebuild_code_window(
    store: DailyBarStore,
    client: TushareSyncClient,
    pro: object,
    ts_code: str,
    start: str,
    end: str,
    stats: SyncStats,
    lookback_days: int,
) -> None:
    start = start or _init_start_date(None, end, lookback_days)
    try:
        rows = _fetch_and_replace_code(store, client, pro, ts_code, start, end)
    except Exception:  # noqa: BLE001
        stats.failed_codes.append(ts_code)
        return
    stats.rebuilt_codes.append(ts_code)
    stats.updated_codes += 1
    stats.added_rows += rows


def _gap_fill_start(store: DailyBarStore, ts_code: str, trade_date: str, client: TushareSyncClient) -> str | None:
    last = store.last_raw_trade_date(ts_code)
    if not last:
        return None
    dates = _read_trade_cal_dates(client, start_date=last, end_date=trade_date)
    if len(dates) > 2:
        return dates[1]
    return None


def _init_start_date(list_date: object, end: str, lookback_days: int) -> str:
    end_dt = datetime.strptime(end, "%Y%m%d").date()
    window_start = (end_dt - timedelta(days=max(int(lookback_days), 30))).strftime("%Y%m%d")
    if list_date:
        list_norm = normalize_date_yyyymmdd(str(list_date))
        if list_norm and list_norm > window_start:
            return list_norm
    return window_start


def _load_universe(
    client: TushareSyncClient,
    *,
    include_st: bool = False,
) -> list[dict[str, str]]:
    df = client.call(
        client.pro.stock_basic,
        exchange="",
        list_status="L",
        fields="ts_code,symbol,name,list_date",
    )
    rows: list[dict[str, str]] = []
    for row in df.itertuples(index=False):
        name = str(getattr(row, "name", ""))
        if not include_st and ("ST" in name.upper() or name.startswith("*")):
            continue
        rows.append({
            "ts_code": str(row.ts_code),
            "symbol": str(row.symbol),
            "name": name,
            "list_date": str(getattr(row, "list_date", "")),
        })
    return rows


def _latest_trade_date(client: TushareSyncClient) -> str:
    end = date.today()
    start = end - timedelta(days=30)
    dates = _read_trade_cal_dates(
        client,
        start_date=start.strftime("%Y%m%d"),
        end_date=end.strftime("%Y%m%d"),
    )
    return dates[-1]


def _read_trade_cal_dates(
    client: TushareSyncClient,
    *,
    start_date: str,
    end_date: str,
) -> list[str]:
    cal = client.call(
        client.pro.trade_cal,
        exchange="",
        start_date=start_date,
        end_date=end_date,
        is_open="1",
        fields="cal_date,is_open",
    )
    if cal is None or getattr(cal, "empty", True):
        raise RuntimeError(
            f"tushare trade_cal returned no open trading days for {start_date}..{end_date}"
        )
    date_col = None
    for candidate in ("cal_date", "trade_date", "date"):
        if candidate in cal.columns:
            date_col = candidate
            break
    if date_col is None:
        raise RuntimeError(
            "tushare trade_cal response missing cal_date column; "
            f"got columns={list(cal.columns)}"
        )
    dates = sorted(str(item) for item in cal[date_col].dropna().tolist())
    if not dates:
        raise RuntimeError(
            f"tushare trade_cal returned no open trading days for {start_date}..{end_date}"
        )
    return dates


def _progress_path(store: _LocalStoreRoot, progress_dir: Path | None) -> Path:
    base = progress_dir or store.root / "meta"
    return base / "sync_progress.json"


def _load_or_reset_progress(path: Path, signature: dict[str, object], reset: bool) -> SyncProgress:
    if reset or not path.is_file():
        return SyncProgress(signature=signature)
    payload = json.loads(path.read_text(encoding="utf-8"))
    progress = SyncProgress.from_dict(payload)
    if progress.signature != signature:
        return SyncProgress(signature=signature)
    return progress


def _save_progress(path: Path, progress: SyncProgress, client: TushareSyncClient) -> None:
    progress.api_stats = dict(client.stats)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(progress.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")


def _delete_progress(path: Path) -> None:
    if path.is_file():
        path.unlink()


def _maybe_save_progress(
    path: Path,
    progress: SyncProgress,
    client: TushareSyncClient,
    last_save: float,
    *,
    save_every: int,
    save_interval: float,
) -> float:
    now = time.monotonic()
    if progress.updated and progress.updated % save_every == 0:
        _save_progress(path, progress, client)
        return now
    if now - last_save >= save_interval:
        _save_progress(path, progress, client)
        return now
    return last_save


def _finalize_manifest(
    store: DailyBarStore,
    stats: SyncStats,
    client: TushareSyncClient,
    *,
    end_date: str | None,
    command: str,
) -> None:
    raw_count = len(list((store.root / "bars" / "raw").glob("*.parquet"))) if (store.root / "bars" / "raw").is_dir() else 0
    payload = {
        "version": 1,
        "provider": "tushare",
        "adj": store.adj,
        "storage_mode": "raw_plus_adj_factor",
        "lookback_cap_days": 800,
        "last_sync_at": datetime.now(timezone.utc).isoformat(),
        "last_trade_date": end_date,
        "code_count": raw_count,
        "schema": {
            "raw_columns": ["date", "open", "high", "low", "close", "volume", "amount"],
            "adj_factor_columns": ["date", "adj_factor"],
            "date_format": "YYYYMMDD",
        },
        "sync_stats": {
            "added_rows": stats.added_rows,
            "updated_codes": stats.updated_codes,
            "rebuilt_codes": stats.rebuilt_codes,
            "failed_codes": stats.failed_codes,
            "source_errors": stats.source_errors,
            "api_stats": dict(client.stats),
            "command": command,
        },
    }
    store.write_manifest(payload)


def _is_transient_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(pattern in text for pattern in _TRANSIENT_PATTERNS) or bool(
        re.search(r"\b(429|502|503|504)\b", text)
    )


def load_sync_progress(store: _LocalStoreRoot, progress_dir: Path | None = None) -> dict[str, object] | None:
    path = _progress_path(store, progress_dir)
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    total = len(payload.get("symbols") or [])
    next_index = int(payload.get("next_index", 0))
    percent = round(next_index / total * 100, 2) if total else 0.0
    return {
        "path": str(path),
        "signature": payload.get("signature"),
        "next_index": next_index,
        "total_symbols": total,
        "percent_complete": percent,
        "updated": int(payload.get("updated", 0)),
        "skipped": int(payload.get("skipped", 0)),
        "failed": int(payload.get("failed", 0)),
        "last_symbol": payload.get("last_symbol", ""),
        "api_stats": payload.get("api_stats", {}),
        "recent_errors": list(payload.get("errors") or [])[-5:],
    }

