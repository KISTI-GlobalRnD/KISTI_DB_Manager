from __future__ import annotations

from typing import Any, Mapping


RUST_ARROW_DETAIL_TIMING_KEYS = (
    "rust_arrow.read_line",
    "rust_arrow.number_validate",
    "rust_arrow.table_assemble",
    "rust_arrow.columnar_merge",
    "rust_arrow.id_compaction",
    "rust_arrow.table_write",
    "rust_arrow.arrow_build",
    "rust_arrow.parquet_write",
    "rust_arrow.py_result_convert",
)
RUST_ARROW_UNACCOUNTED_DETAIL_KEYS = (
    "rust_arrow.read_line",
    "rust_arrow.json_parse",
    "rust_arrow.number_validate",
    "json.flatten",
    "rust_arrow.table_assemble",
    "rust_arrow.columnar_merge",
    "json.parquet.persist",
    "rust_arrow.arrow_build",
    "rust_arrow.parquet_write",
    "rust_arrow.py_result_convert",
)
RUST_ARROW_ID_COMPACTION_DEFAULT_PARALLEL_WORKERS = 8


def parallel_workers_was_explicit(data_config: Any) -> bool:
    try:
        marker = data_config.get("_parallel_workers_explicit")
    except Exception:
        marker = None
    if marker is not None:
        return bool(marker)
    try:
        return "parallel_workers" in data_config
    except Exception:
        return hasattr(data_config, "parallel_workers")


def timing_ms(timings: Mapping[str, Any], key: str) -> int:
    try:
        return int(timings.get(key, 0) or 0)
    except Exception:
        return 0


def rust_arrow_unaccounted_ms(timings: Mapping[str, Any]) -> int:
    total_ms = timing_ms(timings, "rust_arrow.total")
    if total_ms <= 0:
        return 0
    measured_ms = sum(timing_ms(timings, key) for key in RUST_ARROW_UNACCOUNTED_DETAIL_KEYS)
    return max(0, int(total_ms) - int(measured_ms))
