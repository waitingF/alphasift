# -*- coding: utf-8 -*-
"""Pattern search configuration."""

from __future__ import annotations

from copy import deepcopy

from alphasift.pattern.metrics import SUPPORTED_METRICS

DEFAULT_CONFIG: dict = {
    "input": {
        "column_map": {
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        },
        "min_bars": 20,
    },
    "feature": {
        "price_features": [
            "close_return",
            "body",
            "upper_shadow",
            "lower_shadow",
            "amplitude",
        ],
        "volume_features": [
            "log_volume",
        ],
        "stat_features": [
            "pct_change_mean",
            "pct_change_std",
            "trend_slope",
        ],
        "normalize_method": "zscore",
        "clip_outlier": 5.0,
    },
    "similarity": {
        "distance_metric": "euclidean",
        "resample_length": 32,
        "warping_window": 0.2,
        "weights": {
            "price": 0.75,
            "volume": 0.15,
            "stat": 0.10,
        },
        "distance_to_similarity": {
            "method": "exp",
            "alpha": 1.0,
        },
    },
    "retrieval": {
        "window_mode": "fixed",
        "step": 1,
        "candidate_limit_per_symbol": 20,
        "top_k": 20,
    },
}


def get_default_config() -> dict:
    return deepcopy(DEFAULT_CONFIG)


def merge_config(user_config: dict | None = None) -> dict:
    merged = get_default_config()
    if user_config:
        _deep_update(merged, user_config)
    validate_config(merged)
    return merged


def validate_config(config: dict) -> None:
    metric = str(config["similarity"].get("distance_metric", "")).lower()
    if metric not in SUPPORTED_METRICS:
        raise ValueError(
            f"similarity.distance_metric must be one of {sorted(SUPPORTED_METRICS)}, got {metric!r}"
        )
    weights = config["similarity"]["weights"]
    if not {"price", "volume", "stat"} <= set(weights.keys()):
        raise ValueError("similarity.weights must define price, volume and stat")
    if int(config["retrieval"]["top_k"]) <= 0:
        raise ValueError("retrieval.top_k must be positive")


def _deep_update(target: dict, source: dict) -> dict:
    for key, value in source.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_update(target[key], value)
        else:
            target[key] = value
    return target
