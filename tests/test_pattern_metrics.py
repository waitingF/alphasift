from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

from alphasift.pattern.config import merge_config
from alphasift.pattern.metrics import SUPPORTED_METRICS, compare_series, get_metric_descriptors

SELF_STOCK_ROOT = Path("/Users/kongwei/stock/self-stock-project")


def _config_for(metric: str) -> dict:
    return merge_config({"similarity": {"distance_metric": metric}})


def _make_pair(seed: int = 0) -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    base = np.linspace(0.0, 1.0, 40).reshape(-1, 1)
    left = np.hstack([base, base ** 2, rng.normal(size=(40, 1)) * 0.01])
    right = left + rng.normal(scale=0.02, size=left.shape)
    return left, right


def test_supported_metrics_matches_descriptors():
    descriptor_ids = {item["id"] for item in get_metric_descriptors()}
    assert "resample" in SUPPORTED_METRICS
    assert "resample" not in descriptor_ids
    assert descriptor_ids.issubset(SUPPORTED_METRICS)


@pytest.mark.parametrize("metric", ["euclidean", "manhattan", "chebyshev", "dtw", "pearson", "spearman", "cosine"])
def test_identical_series_yield_high_similarity(metric: str):
    left, _ = _make_pair()
    result = compare_series(left, left, _config_for(metric))
    assert result["similarity"] >= 0.99


def test_dtw_tolerates_time_shift_better_than_euclidean():
    x = np.linspace(0, 4 * np.pi, 64)
    left = np.sin(x).reshape(-1, 1)
    right = np.sin(x - 0.4).reshape(-1, 1)
    dtw_score = compare_series(left, right, _config_for("dtw"))["similarity"]
    eu_score = compare_series(left, right, _config_for("euclidean"))["similarity"]
    assert dtw_score > eu_score


def test_resample_alias_maps_to_euclidean():
    left, right = _make_pair(seed=3)
    eu = compare_series(left, right, _config_for("euclidean"))
    rs = compare_series(left, right, _config_for("resample"))
    assert abs(eu["distance"] - rs["distance"]) < 1e-10
    assert abs(eu["similarity"] - rs["similarity"]) < 1e-10


@pytest.mark.skipif(not SELF_STOCK_ROOT.is_dir(), reason="self-stock-project not available")
def test_metrics_match_self_stock_reference():
    if str(SELF_STOCK_ROOT) not in sys.path:
        sys.path.insert(0, str(SELF_STOCK_ROOT))
    from stock_selector.config import merge_config as merge_self_stock_config
    from stock_selector.similarity.metrics import compare_series as self_stock_compare

    left, right = _make_pair(seed=7)
    for metric in ("euclidean", "manhattan", "chebyshev", "dtw", "pearson", "spearman", "cosine"):
        ours = compare_series(left, right, _config_for(metric))
        theirs = self_stock_compare(
            left,
            right,
            merge_self_stock_config({"similarity": {"distance_metric": metric}}),
        )
        assert abs(ours["distance"] - theirs["distance"]) < 1e-6, metric
        assert abs(ours["similarity"] - theirs["similarity"]) < 1e-6, metric
