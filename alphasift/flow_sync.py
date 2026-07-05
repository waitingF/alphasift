# -*- coding: utf-8 -*-
"""Tushare moneyflow sync orchestration for local FlowBarStore."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
import threading
import time

import pandas as pd

from alphasift.daily_sync import (
    SyncProgress,
    SyncStats,
    TushareSyncClient,
    _SymbolProgressBar,
    _delete_progress,
    _init_start_date,
    _latest_trade_date,
    _load_or_reset_progress,
    _load_universe,
    _make_sync_client,
    _maybe_save_progress,
    _progress_path,
    _read_trade_cal_dates,
    _save_progress,
    build_store_status_summary,
)
from alphasift.daily_store import normalize_date_yyyymmdd, require_pyarrow
from alphasift.flow_specs import MONEYFLOW_FIELDS
from alphasift.flow_store import FlowBarStore


def init_flow_bars(
    store: FlowBarStore,
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
    """Initialize per-symbol moneyflow history via Tushare pro.moneyflow(ts_code, ...)."""
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
        desc="flow-bars init",
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

        last = store.last_trade_date(ts_code)
        if last and normalize_date_yyyymmdd(last.replace("-", "")) >= end:
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
            "flow-bars init interrupted; rerun the same command to resume "
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


def sync_flow_bars(
    store: FlowBarStore,
    *,
    token: str,
    trade_date: str | None = None,
    requests_per_second: float = 2.0,
    retry: int = 3,
    retry_interval: float = 1.0,
    include_st: bool = False,
) -> SyncStats:
    """Incremental sync via pro.moneyflow(trade_date=T) full-market panel."""
    pro, client = _make_sync_client(
        token,
        requests_per_second=requests_per_second,
        retry=retry,
        retry_interval=retry_interval,
    )
    manifest = store.manifest()
    last_trade_date = normalize_date_yyyymmdd(str(manifest.get("last_trade_date", "")))
    end = normalize_date_yyyymmdd(trade_date) or _latest_trade_date(client)
    if not last_trade_date:
        raise RuntimeError("manifest missing last_trade_date; run flow-bars init first")

    missing_dates = _read_trade_cal_dates(client, start_date=last_trade_date, end_date=end)[1:]
    stats = SyncStats()
    universe = {row["ts_code"] for row in _load_universe(client, include_st=include_st)}

    for day in missing_dates:
        try:
            panel = client.call(
                pro.moneyflow,
                trade_date=day,
                fields=MONEYFLOW_FIELDS,
            )
        except Exception as exc:  # noqa: BLE001
            stats.source_errors.append(f"{day}: {exc}")
            stats.api_failures += 1
            break

        if panel is None or panel.empty:
            manifest["last_trade_date"] = day
            continue

        for ts_code, group in panel.groupby("ts_code", sort=False):
            if str(ts_code) not in universe:
                continue
            reconcile_stats = store.reconcile_and_write(str(ts_code), group.reset_index(drop=True))
            if reconcile_stats["added"] or reconcile_stats["updated"]:
                stats.updated_codes += 1
                stats.added_rows += reconcile_stats["added"] + reconcile_stats["updated"]

        manifest["last_trade_date"] = day

    _finalize_manifest(store, stats, client, end_date=manifest.get("last_trade_date"), command="sync")
    stats.api_attempts = client.stats["attempts"]
    stats.api_retries = client.stats["retries"]
    stats.api_failures = client.stats.get("failures", 0)
    return stats


def status_flow_bars(store: FlowBarStore, *, effective_trade_date: str | None = None) -> dict[str, object]:
    file_count = len(store.list_codes())
    return build_store_status_summary(
        store,
        effective_trade_date=effective_trade_date,
        file_count=file_count,
        file_count_key="moneyflow_file_count",
    )


def _fetch_and_replace_code(
    store: FlowBarStore,
    client: TushareSyncClient,
    pro: object,
    ts_code: str,
    start: str,
    end: str,
) -> int:
    raw = client.call(
        pro.moneyflow,
        ts_code=ts_code,
        start_date=start,
        end_date=end,
        fields=MONEYFLOW_FIELDS,
    )
    if raw is None or raw.empty:
        raise RuntimeError("empty moneyflow")
    store.write(ts_code, raw)
    return len(raw)


def _finalize_manifest(
    store: FlowBarStore,
    stats: SyncStats,
    client: TushareSyncClient,
    *,
    end_date: str | None,
    command: str,
) -> None:
    require_pyarrow()
    code_count = len(store.list_codes())
    payload = {
        "version": 1,
        "provider": "tushare",
        "dataset": "moneyflow",
        "schema_version": 1,
        "last_sync_at": datetime.now(timezone.utc).isoformat(),
        "last_trade_date": end_date,
        "code_count": code_count,
        "schema": {
            "date_format": "YYYY-MM-DD",
            "derived_columns": ["main_net_inflow", "retail_net_inflow"],
            "main_net_inflow_definition": "buy_lg+buy_elg-sell_lg-sell_elg (万元)",
        },
        "sync_stats": {
            "added_rows": stats.added_rows,
            "updated_codes": stats.updated_codes,
            "failed_codes": stats.failed_codes,
            "source_errors": stats.source_errors,
            "api_stats": dict(client.stats),
            "command": command,
        },
    }
    store.write_manifest(payload)
