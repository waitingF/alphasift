# -*- coding: utf-8 -*-
"""Preflight checks before running strategies that need local daily/flow stores."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from alphasift.config import Config
from alphasift.filter import requires_daily_features, requires_flow_features
from alphasift.models import HardFilterConfig

_FLOW_BARS_REMEDY = "alphasift flow-bars init --lookback-days 800 && alphasift flow-bars sync"
_DAILY_BARS_REMEDY = "alphasift daily-bars init --lookback-days 800 && alphasift daily-bars sync"


class ScreenPrerequisitesError(RuntimeError):
    """Raised when a strategy's local data prerequisites are not satisfied."""


@dataclass(frozen=True)
class PrerequisiteIssue:
    label: str
    detail: str
    remedy: str


def validate_screen_prerequisites(
    *,
    strategy: str,
    hard_filters: HardFilterConfig,
    config: Config,
    daily_source: str | None = None,
) -> None:
    """Fail fast when required local stores are missing or empty."""
    issues = collect_screen_prerequisite_issues(
        hard_filters=hard_filters,
        config=config,
        daily_source=daily_source,
    )
    if not issues:
        return
    raise ScreenPrerequisitesError(format_prerequisite_issues(strategy, issues))


def collect_screen_prerequisite_issues(
    *,
    hard_filters: HardFilterConfig,
    config: Config,
    daily_source: str | None = None,
) -> list[PrerequisiteIssue]:
    issues: list[PrerequisiteIssue] = []
    daily_needed = requires_daily_features(hard_filters)
    effective_daily_source = str(daily_source or config.daily_source or "auto").strip().lower()

    if requires_flow_features(hard_filters):
        issues.extend(_check_flow_store(config.flow_bars_dir))

    daily_reasons: list[str] = []
    if hard_filters.require_no_price_up_flow_out:
        daily_reasons.append(
            "价涨量出 guard（require_no_price_up_flow_out）需在 flow enrich 时 join 本地日 K"
        )
    if daily_needed and effective_daily_source == "local":
        daily_reasons.append("策略含日 K 硬条件且 DAILY_SOURCE=local")
    if daily_reasons:
        issues.extend(_check_daily_store(config.daily_bars_dir, reason="；".join(daily_reasons)))

    return issues


def format_prerequisite_issues(strategy: str, issues: list[PrerequisiteIssue]) -> str:
    lines = [
        f"策略「{strategy}」前置条件未满足，请先完成以下步骤后再运行 screen：",
        "",
    ]
    for index, issue in enumerate(issues, start=1):
        lines.extend([
            f"{index}. {issue.label}",
            f"   {issue.detail}",
            f"   → {issue.remedy}",
        ])
    return "\n".join(lines)


def _check_flow_store(flow_bars_dir: Path | None) -> list[PrerequisiteIssue]:
    from alphasift.flow_store import FlowBarStore

    return _check_parquet_store(
        flow_bars_dir,
        label="本地资金流库（flow-bars）",
        parquet_parts=("moneyflow",),
        remedy=_FLOW_BARS_REMEDY,
        load_manifest=lambda root: FlowBarStore(root).manifest(),
    )


def _check_daily_store(daily_bars_dir: Path | None, *, reason: str) -> list[PrerequisiteIssue]:
    from alphasift.daily_store import DailyBarStore, require_pyarrow

    try:
        require_pyarrow()
    except RuntimeError as exc:
        return [
            PrerequisiteIssue(
                label="本地日 K 库（daily-bars）",
                detail=f"{reason}；{exc}",
                remedy=_DAILY_BARS_REMEDY,
            )
        ]

    return _check_parquet_store(
        daily_bars_dir,
        label="本地日 K 库（daily-bars）",
        parquet_parts=("bars", "raw"),
        remedy=_DAILY_BARS_REMEDY,
        reason=reason,
        load_manifest=lambda root: DailyBarStore(root).manifest(),
    )


def _check_parquet_store(
    store_dir: Path | None,
    *,
    label: str,
    parquet_parts: tuple[str, ...],
    remedy: str,
    load_manifest: Callable[[Path], dict[str, object]],
    reason: str = "",
) -> list[PrerequisiteIssue]:
    prefix = f"{reason}；" if reason else ""
    root = Path(store_dir) if store_dir is not None else None

    if root is None or not root.is_dir():
        return [
            PrerequisiteIssue(
                label=label,
                detail=f"{prefix}目录不存在：{root or '(未配置)'}",
                remedy=remedy,
            )
        ]

    manifest_path = root / "manifest.json"
    if not manifest_path.is_file():
        return [
            PrerequisiteIssue(
                label=label,
                detail=f"{prefix}缺少 manifest.json：{manifest_path}",
                remedy=remedy,
            )
        ]

    parquet_dir = root.joinpath(*parquet_parts)
    if not parquet_dir.is_dir() or not any(parquet_dir.glob("*.parquet")):
        return [
            PrerequisiteIssue(
                label=label,
                detail=f"{prefix}尚未写入任何 Parquet 数据：{parquet_dir}",
                remedy=remedy,
            )
        ]

    try:
        load_manifest(root)
    except (OSError, RuntimeError, ValueError) as exc:
        return [
            PrerequisiteIssue(
                label=label,
                detail=f"{prefix}manifest 无法读取：{exc}",
                remedy=remedy,
            )
        ]

    return []

