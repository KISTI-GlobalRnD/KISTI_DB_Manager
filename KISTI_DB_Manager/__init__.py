"""
KISTI_DB_Manager

MariaDB/MySQL handling utilities for preprocessing, import/export, and management.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__version__ = "0.7.0"

__all__ = [
    "__version__",
    "cli",
    "config",
    "manage",
    "modes",
    "namemap",
    "naming",
    "pipeline",
    "plot",
    "preview",
    "processing",
    "quarantine",
    "quarantine_summary",
    "report",
    "review",
    "review_diff",
]


def __getattr__(name: str) -> Any:
    if name in {
        "cli",
        "config",
        "namemap",
        "naming",
        "pipeline",
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
    }:
        return import_module(f"{__name__}.{name}")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
