# -*- coding: utf-8 -*-
"""Numeric helpers for pattern similarity."""

from __future__ import annotations

import numpy as np
import pandas as pd


def normalize_frame(frame: pd.DataFrame, method: str = "zscore") -> pd.DataFrame:
    if frame.empty or method == "none":
        return frame.copy()
    if method == "zscore":
        std = frame.std(ddof=0).replace(0, 1.0)
        return (frame - frame.mean()) / std
    if method == "minmax":
        denom = (frame.max() - frame.min()).replace(0, 1.0)
        return (frame - frame.min()) / denom
    if method == "robust":
        q1 = frame.quantile(0.25)
        q3 = frame.quantile(0.75)
        denom = (q3 - q1).replace(0, 1.0)
        return (frame - frame.median()) / denom
    raise ValueError(f"Unsupported normalize method: {method}")


def clip_frame(frame: pd.DataFrame, threshold: float | None) -> pd.DataFrame:
    if threshold is None:
        return frame
    return frame.clip(lower=-threshold, upper=threshold)


def resample_array(values: np.ndarray, target_length: int) -> np.ndarray:
    if len(values) == target_length:
        return values.astype(float, copy=True)
    if len(values) == 0:
        return np.zeros((target_length,) + values.shape[1:], dtype=float)
    source_index = np.linspace(0, len(values) - 1, num=len(values))
    target_index = np.linspace(0, len(values) - 1, num=target_length)
    if values.ndim == 1:
        return np.interp(target_index, source_index, values).astype(float)
    columns = [
        np.interp(target_index, source_index, values[:, idx])
        for idx in range(values.shape[1])
    ]
    return np.stack(columns, axis=1).astype(float)


def distance_to_similarity(distance: float, method: str = "exp", alpha: float = 1.0) -> float:
    if method == "exp":
        return float(np.exp(-alpha * max(distance, 0.0)))
    if method == "inverse":
        return float(1.0 / (1.0 + max(distance, 0.0)))
    raise ValueError(f"Unsupported distance_to_similarity method: {method}")
