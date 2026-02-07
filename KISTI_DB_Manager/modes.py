from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class ModeSpec:
    name: str
    description: str
    data_overrides: dict[str, Any]
    stage_defaults: dict[str, bool]


def _stage(create: bool, load: bool, index: bool, optimize: bool) -> dict[str, bool]:
    return {"create": bool(create), "load": bool(load), "index": bool(index), "optimize": bool(optimize)}


MODES: dict[str, ModeSpec] = {
    "default": ModeSpec(
        name="default",
        description="Full pipeline (create+load+index+optimize) with config-driven behavior.",
        data_overrides={},
        stage_defaults=_stage(True, True, True, True),
    ),
    "ingest-fast": ModeSpec(
        name="ingest-fast",
        description="Max ingest throughput (create+load only). Uses LOAD DATA when available; skips index/optimize.",
        data_overrides={
            "db_load_method": "auto",
            "json_streaming_load": True,
            "parallel_workers": 4,
            "chunk_size": 20000,
            "fast_load_session": True,
        },
        stage_defaults=_stage(True, True, False, False),
    ),
    "ingest-fast-freeze": ModeSpec(
        name="ingest-fast-freeze",
        description="Ingest-first + schema freeze: avoid ALTER churn; store drift in __extra__ (create+load only).",
        data_overrides={
            "db_load_method": "auto",
            "json_streaming_load": True,
            "parallel_workers": 4,
            "chunk_size": 20000,
            "fast_load_session": True,
            "schema_mode": "freeze",
            "extra_column_name": "__extra__",
            "auto_alter_table": False,
        },
        stage_defaults=_stage(True, True, False, False),
    ),
    "ingest-fast-hybrid": ModeSpec(
        name="ingest-fast-hybrid",
        description="Ingest-first + hybrid schema: evolve during warmup batches, then freeze into __extra__ (create+load only).",
        data_overrides={
            "db_load_method": "auto",
            "json_streaming_load": True,
            "parallel_workers": 4,
            "chunk_size": 20000,
            "fast_load_session": True,
            "schema_mode": "hybrid",
            "extra_column_name": "__extra__",
            "schema_hybrid_warmup_batches": 1,
            "auto_alter_table": True,
        },
        stage_defaults=_stage(True, True, False, False),
    ),
    "ingest-safe": ModeSpec(
        name="ingest-safe",
        description="Portable ingest (no LOCAL INFILE). Uses pandas.to_sql; skips index/optimize.",
        data_overrides={
            "db_load_method": "to_sql",
            "json_streaming_load": False,
            "parallel_workers": 0,
            "chunk_size": 1000,
            "fast_load_session": False,
        },
        stage_defaults=_stage(True, True, False, False),
    ),
    "finalize": ModeSpec(
        name="finalize",
        description="Finalize DB after ingest (index+optimize only).",
        data_overrides={},
        stage_defaults=_stage(False, False, True, True),
    ),
}


def list_modes() -> list[ModeSpec]:
    return [MODES[k] for k in sorted(MODES)]


def apply_mode(mode: str | None, data_config: dict[str, Any], *, allow_unknown: bool = False) -> ModeSpec:
    """
    Apply a built-in mode preset to a mutable data_config dict.

    Precedence expectation (recommended):
      config < mode < explicit CLI overrides
    """
    name = str(mode or "").strip()
    if not name:
        name = str(data_config.get("mode") or "default").strip() or "default"

    spec = MODES.get(name)
    if spec is None:
        if allow_unknown:
            return MODES["default"]
        raise ValueError(f"Unknown mode: {name!r}. Available: {', '.join(sorted(MODES))}")

    for k, v in (spec.data_overrides or {}).items():
        data_config[k] = v
    data_config["mode"] = spec.name
    return spec


def resolve_mode_name(value: str | None, data_config: Mapping[str, Any] | None = None) -> str:
    name = str(value or "").strip()
    if name:
        return name
    if data_config is not None:
        cfg_name = str(data_config.get("mode") or "").strip()
        if cfg_name:
            return cfg_name
    return "default"
