# -*- coding: utf-8 -*-
"""Pattern similarity search (ported from self-stock-project stock_selector)."""

from alphasift.pattern.metrics import SUPPORTED_METRICS, compare_series, get_metric_descriptors
from alphasift.pattern.search import search_pattern

__all__ = [
    "SUPPORTED_METRICS",
    "compare_series",
    "get_metric_descriptors",
    "search_pattern",
]
