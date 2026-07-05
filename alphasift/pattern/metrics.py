# -*- coding: utf-8 -*-
"""Pattern similarity metrics (aligned with self-stock-project)."""

from __future__ import annotations

from math import ceil

import numpy as np

from alphasift.pattern.math_utils import distance_to_similarity, resample_array

_RESAMPLE_METRICS = {"euclidean", "resample", "manhattan", "chebyshev"}
_CORRELATION_METRICS = {"pearson", "spearman"}
_ANGLE_METRICS = {"cosine"}
_WARPING_METRICS = {"dtw"}

SUPPORTED_METRICS = (
    _RESAMPLE_METRICS
    | _CORRELATION_METRICS
    | _ANGLE_METRICS
    | _WARPING_METRICS
)


def get_metric_descriptors() -> list[dict]:
    return [
        {"id": "euclidean", "label": "欧氏距离（重采样）", "category": "distance"},
        {"id": "manhattan", "label": "曼哈顿距离（重采样）", "category": "distance"},
        {"id": "chebyshev", "label": "切比雪夫距离（重采样）", "category": "distance"},
        {"id": "dtw", "label": "动态时间规整 DTW", "category": "warping"},
        {"id": "pearson", "label": "皮尔逊相关系数", "category": "correlation"},
        {"id": "spearman", "label": "斯皮尔曼秩相关", "category": "correlation"},
        {"id": "cosine", "label": "余弦相似度", "category": "angle"},
    ]


def compare_series(left: np.ndarray, right: np.ndarray, config: dict) -> dict:
    similarity_config = config["similarity"]
    metric = str(similarity_config.get("distance_metric", "euclidean")).lower()
    if metric not in SUPPORTED_METRICS:
        raise ValueError(
            f"Unsupported similarity metric: {metric!r}. Supported: {sorted(SUPPORTED_METRICS)}"
        )

    left_2d = _ensure_2d(left)
    right_2d = _ensure_2d(right)

    if metric in _RESAMPLE_METRICS:
        return _compare_lp(left_2d, right_2d, metric, similarity_config)
    if metric in _CORRELATION_METRICS:
        return _compare_correlation(left_2d, right_2d, metric, similarity_config)
    if metric in _ANGLE_METRICS:
        return _compare_cosine(left_2d, right_2d, similarity_config)
    if metric in _WARPING_METRICS:
        return _compare_dtw(left_2d, right_2d, similarity_config)
    raise ValueError(f"Metric dispatch failed for: {metric!r}")


def _ensure_2d(values: np.ndarray) -> np.ndarray:
    array = np.asarray(values, dtype=float)
    if array.ndim == 1:
        array = array.reshape(-1, 1)
    return array


def _resample_pair(left: np.ndarray, right: np.ndarray, target_length: int) -> tuple[np.ndarray, np.ndarray]:
    return (
        resample_array(left, target_length),
        resample_array(right, target_length),
    )


def _map_distance(similarity_config: dict, distance: float) -> float:
    mapping = similarity_config.get("distance_to_similarity", {})
    return float(
        distance_to_similarity(
            distance,
            method=mapping.get("method", "exp"),
            alpha=float(mapping.get("alpha", 1.0)),
        )
    )


def _compare_lp(left: np.ndarray, right: np.ndarray, metric: str, similarity_config: dict) -> dict:
    target_length = int(similarity_config.get("resample_length", 32))
    left_rs, right_rs = _resample_pair(left, right, target_length)
    diff = left_rs - right_rs

    if metric in {"euclidean", "resample"}:
        distance = float(np.linalg.norm(diff) / max(diff.shape[0], 1))
        norm = "l2"
    elif metric == "manhattan":
        distance = float(np.abs(diff).sum() / max(diff.shape[0], 1))
        norm = "l1"
    elif metric == "chebyshev":
        distance = float(np.abs(diff).max()) if diff.size else 0.0
        norm = "linf"
    else:
        raise ValueError(f"Unhandled Lp metric: {metric!r}")

    similarity = _map_distance(similarity_config, distance)
    return {
        "distance": distance,
        "similarity": similarity,
        "alignment_info": {"method": metric, "target_length": target_length, "norm": norm},
    }


def _compare_correlation(
    left: np.ndarray, right: np.ndarray, metric: str, similarity_config: dict
) -> dict:
    target_length = int(similarity_config.get("resample_length", 32))
    left_rs, right_rs = _resample_pair(left, right, target_length)

    corrs: list[float] = []
    for col in range(left_rs.shape[1]):
        a = left_rs[:, col]
        b = right_rs[:, col]
        if metric == "spearman":
            a = _rankdata(a)
            b = _rankdata(b)
        corrs.append(_safe_pearson(a, b))

    mean_corr = float(np.mean(corrs)) if corrs else 0.0
    similarity = float((mean_corr + 1.0) / 2.0)
    similarity = min(max(similarity, 0.0), 1.0)
    distance = float(1.0 - similarity)
    return {
        "distance": distance,
        "similarity": similarity,
        "alignment_info": {
            "method": metric,
            "target_length": target_length,
            "mean_correlation": mean_corr,
            "per_column": corrs,
        },
    }


def _compare_cosine(left: np.ndarray, right: np.ndarray, similarity_config: dict) -> dict:
    target_length = int(similarity_config.get("resample_length", 32))
    left_rs, right_rs = _resample_pair(left, right, target_length)
    a = left_rs.reshape(-1)
    b = right_rs.reshape(-1)
    norm_a = float(np.linalg.norm(a))
    norm_b = float(np.linalg.norm(b))
    if norm_a == 0.0 or norm_b == 0.0:
        cos = 0.0
    else:
        cos = float(np.dot(a, b) / (norm_a * norm_b))
    cos = max(min(cos, 1.0), -1.0)
    similarity = float((cos + 1.0) / 2.0)
    distance = float(1.0 - similarity)
    return {
        "distance": distance,
        "similarity": similarity,
        "alignment_info": {"method": "cosine", "target_length": target_length, "cosine": cos},
    }


def _compare_dtw(left: np.ndarray, right: np.ndarray, similarity_config: dict) -> dict:
    target_length = int(similarity_config.get("resample_length", 32))
    left_rs, right_rs = _resample_pair(left, right, target_length)

    warping_fraction = float(similarity_config.get("warping_window", 0.2))
    band = max(1, int(ceil(warping_fraction * max(len(left_rs), len(right_rs)))))

    distance_raw, path_length = _multivariate_dtw(left_rs, right_rs, band)
    distance = float(distance_raw / max(path_length, 1))
    similarity = _map_distance(similarity_config, distance)
    return {
        "distance": distance,
        "similarity": similarity,
        "alignment_info": {
            "method": "dtw",
            "target_length": target_length,
            "band": band,
            "path_length": path_length,
        },
    }


def _multivariate_dtw(left: np.ndarray, right: np.ndarray, band: int) -> tuple[float, int]:
    n = len(left)
    m = len(right)
    if n == 0 or m == 0:
        return 0.0, 0

    effective_band = max(band, abs(n - m))
    inf = float("inf")
    dp = np.full((n + 1, m + 1), inf, dtype=float)
    dp[0, 0] = 0.0

    for i in range(1, n + 1):
        j_start = max(1, i - effective_band)
        j_end = min(m, i + effective_band)
        for j in range(j_start, j_end + 1):
            diff = left[i - 1] - right[j - 1]
            cost = float(np.sqrt(np.dot(diff, diff)))
            dp[i, j] = cost + min(dp[i - 1, j], dp[i, j - 1], dp[i - 1, j - 1])

    distance = dp[n, m]
    i, j = n, m
    path_length = 0
    while i > 0 or j > 0:
        path_length += 1
        if i == 0:
            j -= 1
        elif j == 0:
            i -= 1
        else:
            step = int(np.argmin([dp[i - 1, j - 1], dp[i - 1, j], dp[i, j - 1]]))
            if step == 0:
                i -= 1
                j -= 1
            elif step == 1:
                i -= 1
            else:
                j -= 1
    return float(distance), int(path_length)


def _safe_pearson(a: np.ndarray, b: np.ndarray) -> float:
    if len(a) < 2:
        return 0.0
    a_centered = a - a.mean()
    b_centered = b - b.mean()
    denom = float(np.linalg.norm(a_centered) * np.linalg.norm(b_centered))
    if denom == 0.0:
        return 0.0
    value = float(np.dot(a_centered, b_centered) / denom)
    if np.isnan(value):
        return 0.0
    return max(min(value, 1.0), -1.0)


def _rankdata(values: np.ndarray) -> np.ndarray:
    order = np.argsort(values, kind="mergesort")
    ranks = np.empty_like(order, dtype=float)
    ranks[order] = np.arange(1, len(values) + 1, dtype=float)
    sorted_vals = values[order]
    i = 0
    n = len(values)
    while i < n:
        j = i + 1
        while j < n and sorted_vals[j] == sorted_vals[i]:
            j += 1
        if j - i > 1:
            avg = (ranks[order[i]] + ranks[order[j - 1]]) / 2.0
            for k in range(i, j):
                ranks[order[k]] = avg
        i = j
    return ranks
