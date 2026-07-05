# -*- coding: utf-8 -*-
"""Pattern similarity search over local daily-bars store."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from heapq import nlargest
from pathlib import Path

import pandas as pd

from alphasift.daily_store import DailyBarStore, normalize_ts_code
from alphasift.pattern.config import merge_config
from alphasift.pattern.features import (
    build_feature_matrix,
    compare_feature_frames,
    normalize_bars,
)


def list_store_codes(daily_bars_dir: str | Path) -> list[str]:
    raw_dir = Path(daily_bars_dir) / "bars" / "raw"
    if not raw_dir.is_dir():
        return []
    return sorted(path.stem for path in raw_dir.glob("*.parquet"))


def search_pattern(
    query_bars: list[dict],
    *,
    daily_bars_dir: str | Path,
    metric: str = "euclidean",
    top_k: int = 20,
    codes: list[str] | None = None,
    exclude_codes: list[str] | None = None,
    lookback_days: int = 200,
    max_workers: int = 4,
    config_overrides: dict | None = None,
) -> dict:
    """Search local daily-bars universe for windows similar to query bars."""
    query_df = normalize_bars(query_bars)
    window_size = len(query_df)
    config = _build_runtime_config(metric, top_k, window_size, config_overrides)
    if len(query_df) < int(config["input"]["min_bars"]):
        raise ValueError(f"query bars too short: {len(query_df)}")

    query_features = build_feature_matrix(query_df, config)

    store = DailyBarStore(daily_bars_dir)
    universe = codes or list_store_codes(daily_bars_dir)
    excluded = {normalize_ts_code(code) for code in (exclude_codes or [])}
    universe = [code for code in universe if normalize_ts_code(code) not in excluded]
    if not universe:
        raise RuntimeError(f"no candidate codes found under {daily_bars_dir}")

    per_symbol_limit = int(config["retrieval"]["candidate_limit_per_symbol"])
    step = max(int(config["retrieval"].get("step", 1)), 1)

    def scan_one(code: str) -> list[dict]:
        try:
            history = store.read_history(code, lookback_days=lookback_days)
        except FileNotFoundError:
            return []
        candidate_df = normalize_bars(history)
        if len(candidate_df) < window_size:
            return []
        matches: list[dict] = []
        for start in range(0, len(candidate_df) - window_size + 1, step):
            end = start + window_size
            window = candidate_df.iloc[start:end].reset_index(drop=True)
            window_features = build_feature_matrix(window, config)
            similarity = compare_feature_frames(query_features, window_features, config)
            matches.append(
                _build_match(
                    code=code,
                    candidate_df=candidate_df,
                    start_idx=start,
                    end_idx=end - 1,
                    similarity=similarity,
                )
            )
        return nlargest(per_symbol_limit, matches, key=lambda item: item["total_similarity"])

    worker_limit = max(1, min(int(max_workers), len(universe)))
    all_matches: list[dict] = []
    if worker_limit <= 1 or len(universe) <= 1:
        for code in universe:
            all_matches.extend(scan_one(code))
    else:
        with ThreadPoolExecutor(max_workers=worker_limit) as executor:
            for batch in executor.map(scan_one, universe):
                all_matches.extend(batch)

    ranked = nlargest(top_k, all_matches, key=lambda item: item["total_similarity"])
    return {
        "query": {
            "bars_count": window_size,
            "start_date": str(query_df["date"].iloc[0]) if not query_df.empty else None,
            "end_date": str(query_df["date"].iloc[-1]) if not query_df.empty else None,
        },
        "config": {
            "distance_metric": metric,
            "top_k": top_k,
            "window_size": window_size,
            "lookback_days": lookback_days,
        },
        "summary": {
            "scanned_symbols": len(universe),
            "candidate_windows": len(all_matches),
            "returned_matches": len(ranked),
        },
        "matches": ranked,
    }


def _build_runtime_config(
    metric: str,
    top_k: int,
    window_size: int,
    overrides: dict | None,
) -> dict:
    payload = {
        "similarity": {"distance_metric": metric.lower()},
        "retrieval": {
            "top_k": top_k,
            "step": max(window_size // 15, 1),
        },
    }
    if overrides:
        payload = _deep_merge(payload, overrides)
    config = merge_config(payload)
    config["retrieval"]["step"] = max(int(config["retrieval"].get("step", 1)), 1)
    return config


def _build_match(
    *,
    code: str,
    candidate_df: pd.DataFrame,
    start_idx: int,
    end_idx: int,
    similarity: dict,
) -> dict:
    start_date = str(candidate_df.iloc[start_idx]["date"])
    end_date = str(candidate_df.iloc[end_idx]["date"])
    return {
        "code": normalize_ts_code(code),
        "match_id": f"{normalize_ts_code(code)}_{start_date}_{end_date}",
        "start_date": start_date,
        "end_date": end_date,
        "bars_count": end_idx - start_idx + 1,
        "price_similarity": similarity["price_similarity"],
        "volume_similarity": similarity["volume_similarity"],
        "stat_similarity": similarity["stat_similarity"],
        "total_similarity": similarity["total_similarity"],
    }


def _deep_merge(base: dict, override: dict) -> dict:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result
