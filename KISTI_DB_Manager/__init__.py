"""
KISTI_DB_Manager

MariaDB/MySQL handling utilities for preprocessing, import/export, and management.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__version__ = "0.9.0"

__all__ = [
    "__version__",
    "cli",
    "config",
    "dataset_profile",
    "description_profile",
    "id_compaction",
    "load_data",
    "manage",
    "modes",
    "namemap",
    "naming",
    "openalex_reload_validate",
    "pipeline",
    "parquet_delta_merge",
    "parquet_finalize",
    "parquet_reload",
    "plot",
    "preview",
    "processing",
    "quarantine",
    "quarantine_summary",
    "report",
    "review",
    "review_diff",
    "review_preview",
    "review_schema",
    "target_db_preflight",
]


def __getattr__(name: str) -> Any:
    if name in {
        "cli",
        "config",
        "dataset_profile",
        "description_profile",
        "id_compaction",
        "load_data",
        "namemap",
        "naming",
        "openalex_reload_validate",
        "pipeline",
        "parquet_delta_merge",
        "parquet_finalize",
        "parquet_reload",
        "target_db_preflight",
        "manage",
        "modes",
        "plot",
        "preview",
        "processing",
        "quarantine",
        "quarantine_summary",
        "report",
        "review",
        "review_diff",
        "review_preview",
        "review_schema",
    }:
        return import_module(f"{__name__}.{name}")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
