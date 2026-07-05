# -*- coding: utf-8 -*-
"""Serial orchestration for refreshing local AlphaSift data stores."""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Literal

from alphasift.config import Config
from alphasift.daily_store import DailyBarStore
from alphasift.daily_sync import SyncStats, init_daily_bars, sync_daily_bars
from alphasift.flow_store import FlowBarStore
from alphasift.flow_sync import init_flow_bars, sync_flow_bars
from alphasift.hotspot import append_hotspot_history, discover_hotspots, save_hotspots_json
from alphasift.industry import fetch_akshare_board_map, save_industry_map

StepName = Literal["daily_bars", "flow_bars", "industry_cache", "hotspot_cache"]
StepStatus = Literal["ok", "skipped", "failed"]


@dataclass
class DataUpdateStepResult:
    name: StepName
    status: StepStatus
    message: str = ""
    details: dict[str, object] = field(default_factory=dict)


@dataclass
class DataUpdateResult:
    started_at: str
    finished_at: str = ""
    steps: list[DataUpdateStepResult] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return all(step.status != "failed" for step in self.steps)

    @property
    def had_failures(self) -> bool:
        return any(step.status == "failed" for step in self.steps)

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["success"] = self.success
        return payload


def run_data_update(
    config: Config,
    *,
    skip_daily: bool = False,
    skip_flow: bool = False,
    skip_industry: bool = False,
    skip_hotspot: bool = False,
    init_if_missing: bool = True,
    lookback_days: int = 800,
    include_st: bool = False,
    industry_max_boards: int | None = None,
    hotspot_top: int = 20,
    hotspot_max_boards: int | None = None,
    industry_output: str | Path | None = None,
    hotspot_output: str | Path | None = None,
    hotspot_history: str | Path | None = None,
) -> DataUpdateResult:
    """Refresh local datasets one after another (no parallelism)."""
    started_at = datetime.now().isoformat(timespec="seconds")
    steps: list[DataUpdateStepResult] = []
    token = _resolve_tushare_token()

    if not skip_daily:
        steps.append(
            _update_daily_bars(
                config,
                token=token,
                init_if_missing=init_if_missing,
                lookback_days=lookback_days,
                include_st=include_st,
            )
        )
        if steps[-1].status == "failed":
            return _finish(started_at, steps)

    if not skip_flow:
        steps.append(
            _update_flow_bars(
                config,
                token=token,
                init_if_missing=init_if_missing,
                lookback_days=lookback_days,
                include_st=include_st,
            )
        )
        if steps[-1].status == "failed":
            return _finish(started_at, steps)

    if not skip_industry:
        steps.append(
            _update_industry_cache(
                config,
                output_path=industry_output,
                max_boards=industry_max_boards,
            )
        )
        if steps[-1].status == "failed":
            return _finish(started_at, steps)

    if not skip_hotspot:
        steps.append(
            _update_hotspot_cache(
                config,
                output_path=hotspot_output,
                history_path=hotspot_history,
                top=hotspot_top,
                max_boards=hotspot_max_boards,
            )
        )

    return _finish(started_at, steps)


def format_data_update_explain(result: DataUpdateResult) -> str:
    lines = [
        f"data-update success={result.success} steps={len(result.steps)}",
        f"started_at={result.started_at} finished_at={result.finished_at}",
        "",
    ]
    for index, step in enumerate(result.steps, start=1):
        lines.append(f"{index}. {step.name} [{step.status}]")
        if step.message:
            lines.append(f"   {step.message}")
        for key, value in step.details.items():
            lines.append(f"   {key}={value}")
    return "\n".join(lines).strip()


def _finish(started_at: str, steps: list[DataUpdateStepResult]) -> DataUpdateResult:
    return DataUpdateResult(
        started_at=started_at,
        finished_at=datetime.now().isoformat(timespec="seconds"),
        steps=steps,
    )


def _resolve_tushare_token() -> str:
    return os.getenv("TUSHARE_TOKEN", "").strip() or os.getenv("TUSHARE_API_TOKEN", "").strip()


def _resolve_industry_output(config: Config, override: str | Path | None) -> Path:
    if override is not None:
        return Path(override)
    if config.industry_map_files:
        return Path(config.industry_map_files[0])
    return config.data_dir / "industry_map.csv"


def _resolve_hotspot_output(config: Config, override: str | Path | None) -> Path:
    return Path(override) if override is not None else config.data_dir / "hotspots.json"


def _resolve_hotspot_history(config: Config, override: str | Path | None) -> Path:
    return Path(override) if override is not None else config.data_dir / "hotspot.history.jsonl"


def _bar_store_ready(root: Path, parquet_parts: tuple[str, ...]) -> bool:
    if not root.is_dir() or not (root / "manifest.json").is_file():
        return False
    parquet_dir = root.joinpath(*parquet_parts)
    return parquet_dir.is_dir() and any(parquet_dir.glob("*.parquet"))


def _sync_stats_details(stats: SyncStats) -> dict[str, object]:
    return {
        "added_rows": stats.added_rows,
        "updated_codes": stats.updated_codes,
        "failed_codes": len(stats.failed_codes),
        "source_errors": len(stats.source_errors),
        "api_failures": stats.api_failures,
    }


def _daily_bars_root(config: Config) -> Path:
    return Path(config.daily_bars_dir or config.data_dir / "daily_bars")


def _flow_bars_root(config: Config) -> Path:
    return Path(config.flow_bars_dir or config.data_dir / "flow_bars")


def _update_daily_bars(
    config: Config,
    *,
    token: str,
    init_if_missing: bool,
    lookback_days: int,
    include_st: bool,
) -> DataUpdateStepResult:
    root = _daily_bars_root(config)
    if not token:
        return DataUpdateStepResult(
            name="daily_bars",
            status="skipped",
            message="缺少 TUSHARE_TOKEN，跳过 daily-bars",
        )

    store = DailyBarStore(root, adj=os.getenv("TUSHARE_DAILY_ADJ", "qfq"))
    ready = _bar_store_ready(root, ("bars", "raw"))
    try:
        if ready:
            stats = sync_daily_bars(
                store,
                token=token,
                requests_per_second=config.daily_sync_requests_per_second,
                retry=config.daily_sync_retry,
                retry_interval=config.daily_sync_retry_interval,
                include_st=include_st,
            )
            mode = "sync"
        elif init_if_missing:
            stats = init_daily_bars(
                store,
                token=token,
                lookback_days=lookback_days,
                requests_per_second=config.daily_sync_requests_per_second,
                retry=config.daily_sync_retry,
                retry_interval=config.daily_sync_retry_interval,
                save_every=config.daily_sync_progress_save_every,
                save_interval=config.daily_sync_progress_save_interval,
                include_st=include_st,
                show_progress=True,
            )
            mode = "init"
        else:
            return DataUpdateStepResult(
                name="daily_bars",
                status="skipped",
                message=f"本地库未初始化：{root}（可加 --init-if-missing 或先运行 daily-bars init）",
            )
    except Exception as exc:  # noqa: BLE001
        return DataUpdateStepResult(
            name="daily_bars",
            status="failed",
            message=str(exc),
        )

    details = _sync_stats_details(stats)
    details["mode"] = mode
    details["root"] = str(root)
    status: StepStatus = "failed" if stats.failed_codes or stats.source_errors else "ok"
    return DataUpdateStepResult(
        name="daily_bars",
        status=status,
        message=f"daily-bars {mode} 完成",
        details=details,
    )


def _update_flow_bars(
    config: Config,
    *,
    token: str,
    init_if_missing: bool,
    lookback_days: int,
    include_st: bool,
) -> DataUpdateStepResult:
    root = _flow_bars_root(config)
    if not token:
        return DataUpdateStepResult(
            name="flow_bars",
            status="skipped",
            message="缺少 TUSHARE_TOKEN，跳过 flow-bars",
        )

    store = FlowBarStore(root)
    ready = _bar_store_ready(root, ("moneyflow",))
    try:
        if ready:
            stats = sync_flow_bars(
                store,
                token=token,
                requests_per_second=config.flow_sync_requests_per_second,
                retry=config.flow_sync_retry,
                retry_interval=config.flow_sync_retry_interval,
                include_st=include_st,
            )
            mode = "sync"
        elif init_if_missing:
            stats = init_flow_bars(
                store,
                token=token,
                lookback_days=lookback_days,
                requests_per_second=config.flow_sync_requests_per_second,
                retry=config.flow_sync_retry,
                retry_interval=config.flow_sync_retry_interval,
                save_every=config.flow_sync_progress_save_every,
                save_interval=config.flow_sync_progress_save_interval,
                include_st=include_st,
                show_progress=True,
            )
            mode = "init"
        else:
            return DataUpdateStepResult(
                name="flow_bars",
                status="skipped",
                message=f"本地库未初始化：{root}（可加 --init-if-missing 或先运行 flow-bars init）",
            )
    except Exception as exc:  # noqa: BLE001
        return DataUpdateStepResult(
            name="flow_bars",
            status="failed",
            message=str(exc),
        )

    details = _sync_stats_details(stats)
    details["mode"] = mode
    details["root"] = str(root)
    status: StepStatus = "failed" if stats.failed_codes or stats.source_errors else "ok"
    return DataUpdateStepResult(
        name="flow_bars",
        status=status,
        message=f"flow-bars {mode} 完成",
        details=details,
    )


def _update_industry_cache(
    config: Config,
    *,
    output_path: str | Path | None,
    max_boards: int | None,
) -> DataUpdateStepResult:
    output = _resolve_industry_output(config, output_path)
    boards = max_boards if max_boards is not None else config.industry_provider_max_boards
    try:
        mapping, notes = fetch_akshare_board_map(max_boards=boards)
        saved_path = save_industry_map(mapping, output)
    except Exception as exc:  # noqa: BLE001
        return DataUpdateStepResult(
            name="industry_cache",
            status="failed",
            message=str(exc),
        )

    return DataUpdateStepResult(
        name="industry_cache",
        status="ok",
        message="industry-cache 完成",
        details={
            "path": str(saved_path),
            "rows": len(mapping),
            "notes": " | ".join(notes[:3]) if notes else "",
        },
    )


def _update_hotspot_cache(
    config: Config,
    *,
    output_path: str | Path | None,
    history_path: str | Path | None,
    top: int,
    max_boards: int | None,
) -> DataUpdateStepResult:
    output = _resolve_hotspot_output(config, output_path)
    history = _resolve_hotspot_history(config, history_path)
    boards = max_boards if max_boards is not None else config.industry_provider_max_boards
    try:
        hotspots = discover_hotspots(
            provider="akshare",
            max_boards=boards,
            history_path=history,
            fallback_cache_path=output,
            top=top,
        )
        fallback_used = bool(getattr(hotspots, "fallback_used", False))
        ths_fallback = bool(getattr(hotspots, "fallback_notes", []))
        if fallback_used and not output.exists():
            saved_path = save_hotspots_json(output, hotspots)
            history_saved = None
        elif fallback_used:
            saved_path = output
            history_saved = None
        else:
            saved_path = save_hotspots_json(output, hotspots)
            history_saved = append_hotspot_history(history, hotspots, generated_at=datetime.now().isoformat())
    except Exception as exc:  # noqa: BLE001
        return DataUpdateStepResult(
            name="hotspot_cache",
            status="failed",
            message=str(exc),
        )

    fallback_notes = list(getattr(hotspots, "fallback_notes", []) or [])
    status: StepStatus = "failed" if len(hotspots) == 0 else "ok"
    return DataUpdateStepResult(
        name="hotspot_cache",
        status=status,
        message="hotspot-cache 完成" if status == "ok" else "hotspot-cache 未获取到热点数据",
        details={
            "path": str(saved_path),
            "history_path": str(history_saved) if history_saved else "",
            "rows": len(hotspots),
            "fallback_used": fallback_used,
            "provider_fallback": ths_fallback,
            "source_errors": len(getattr(hotspots, "source_errors", []) or []),
            "notes": " | ".join(str(note) for note in fallback_notes[:3]),
        },
    )
