# -*- coding: utf-8 -*-
"""Shared atomic Parquet/text I/O for local bar stores."""

from __future__ import annotations

import os
import threading
from pathlib import Path

import pandas as pd


def require_pyarrow() -> None:
    try:
        import pyarrow  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            'pyarrow is required for local bar stores; install with: '
            'pip install "alphasift[daily-store]"'
        ) from exc


_WRITE_LOCKS: dict[str, threading.Lock] = {}
_WRITE_LOCKS_GUARD = threading.Lock()


def read_parquet(path: Path) -> pd.DataFrame:
    require_pyarrow()
    return pd.read_parquet(path)


def atomic_write_parquet(path: Path, df: pd.DataFrame) -> None:
    require_pyarrow()
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = _write_lock_for(path)
    with lock:
        tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}")
        df.to_parquet(tmp, index=False)
        with tmp.open("rb") as handle:
            handle.flush()
            os.fsync(handle.fileno())
        tmp.replace(path)


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    tmp.write_text(text, encoding="utf-8")
    with tmp.open("rb") as handle:
        handle.flush()
        os.fsync(handle.fileno())
    tmp.replace(path)


def _write_lock_for(path: Path) -> threading.Lock:
    key = str(path.resolve())
    with _WRITE_LOCKS_GUARD:
        if key not in _WRITE_LOCKS:
            _WRITE_LOCKS[key] = threading.Lock()
        return _WRITE_LOCKS[key]
