from __future__ import annotations

import importlib
from typing import Any, Mapping

def _resolve_bool(value: bool | None, default: bool) -> bool:
    return bool(default) if value is None else bool(value)


class MissingDependencyError(RuntimeError):
    """Raised when an optional dependency group is required but not installed."""


class ConfigValidationError(ValueError):
    """Raised when CLI/config options resolve to an invalid pipeline combination."""


def _ensure_optional_deps(feature: str, modules: list[str], *, extras: list[str]) -> None:
    missing: list[str] = []
    for mod in modules:
        try:
            importlib.import_module(str(mod))
        except ModuleNotFoundError as e:
            name = getattr(e, "name", None) or str(mod)
            missing.append(str(name))

    if not missing:
        return

    extras_arg = ",".join(str(x) for x in extras if str(x))
    miss = ", ".join(sorted(set(missing)))
    raise MissingDependencyError(
        f"{feature} requires missing dependencies: {miss}. "
        f"Install with: pip install -e '.[{extras_arg}]'"
    )


def _validate_json_run_config(data_config: Mapping[str, Any], *, mode_name: str) -> None:
    streaming = bool(data_config.get("json_streaming_load", False))
    persist_parquet = bool(data_config.get("persist_parquet_files", False))
    persist_tsv = bool(data_config.get("persist_tsv_files", False))

    errors: list[str] = []
    if persist_parquet and streaming:
        errors.append(
            "persist_parquet_files=true cannot be combined with json_streaming_load=true; "
            "use --mode parse-parquet or disable one of the two options."
        )
    if persist_tsv and not streaming:
        errors.append(
            "persist_tsv_files=true requires json_streaming_load=true; "
            "use --mode ingest-fast or disable TSV persistence."
        )
    if persist_parquet and persist_tsv:
        errors.append(
            "persist_parquet_files=true cannot be combined with persist_tsv_files=true; "
            "choose one artifact strategy."
        )
    id_compaction: Mapping[str, Any] = {}
    try:
        from ..id_compaction import normalize_id_compaction_config, validate_id_compaction_config

        id_compaction = normalize_id_compaction_config(data_config)
        validate_id_compaction_config(id_compaction)
    except Exception as e:
        errors.append(f"invalid id_compaction config: {e}")
    flatten_backend = "auto"
    try:
        from ..rust_arrow_backend import normalize_flatten_backend

        flatten_backend = normalize_flatten_backend(data_config.get("flatten_backend", "auto"))
    except Exception as e:
        errors.append(f"invalid flatten_backend config: {e}")
    if errors:
        mode_hint = f" mode={mode_name!r}" if str(mode_name or "").strip() else ""
        raise ConfigValidationError("Invalid json run configuration" + mode_hint + ": " + " ".join(errors))
