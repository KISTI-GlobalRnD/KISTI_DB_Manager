from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any, Mapping, Sequence


BACKEND_AUTO = "auto"
BACKEND_PYTHON = "python"
BACKEND_RUST_ARROW = "rust-arrow"
BACKEND_CHOICES = (BACKEND_AUTO, BACKEND_PYTHON, BACKEND_RUST_ARROW)
DEFAULT_PROFILE_BACKENDS = (BACKEND_AUTO,)
PARSER_BACKEND_SERDE_JSON = "serde-json"
PARSER_BACKEND_SIMD_JSON = "simd-json"
PARSER_BACKEND_CHOICES = (PARSER_BACKEND_SERDE_JSON, PARSER_BACKEND_SIMD_JSON)


class RustArrowBackendUnavailable(RuntimeError):
    """Raised when the optional Rust Arrow backend cannot be used."""


def normalize_flatten_backend(value: Any) -> str:
    raw = str(value or BACKEND_AUTO).strip().lower().replace("_", "-")
    aliases = {
        "py": BACKEND_PYTHON,
        "pandas": BACKEND_PYTHON,
        "rust": BACKEND_RUST_ARROW,
        "rustarrow": BACKEND_RUST_ARROW,
        "rust-parquet": BACKEND_RUST_ARROW,
    }
    backend = aliases.get(raw, raw)
    if backend not in BACKEND_CHOICES:
        choices = ", ".join(BACKEND_CHOICES)
        raise ValueError(f"invalid flatten_backend {value!r}; expected one of: {choices}")
    return backend


def parse_backend_list(value: str | Sequence[str] | None) -> list[str]:
    if value is None:
        return list(DEFAULT_PROFILE_BACKENDS)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return list(DEFAULT_PROFILE_BACKENDS)
        raw_items: list[Any] = text.split(",")
    else:
        raw_items = list(value)

    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_items:
        item = str(raw).strip()
        if not item:
            raise ValueError("flatten backends must be a comma-separated list")
        backend = normalize_flatten_backend(item)
        if backend in seen:
            continue
        seen.add(backend)
        out.append(backend)
    if not out:
        raise ValueError("at least one flatten backend is required")
    return out


def normalize_parser_backend(value: Any) -> str:
    raw = str(value or PARSER_BACKEND_SERDE_JSON).strip().lower().replace("_", "-")
    aliases = {
        "serde": PARSER_BACKEND_SERDE_JSON,
        "serdejson": PARSER_BACKEND_SERDE_JSON,
        "simd": PARSER_BACKEND_SIMD_JSON,
        "simdjson": PARSER_BACKEND_SIMD_JSON,
    }
    backend = aliases.get(raw, raw)
    if backend not in PARSER_BACKEND_CHOICES:
        choices = ", ".join(PARSER_BACKEND_CHOICES)
        raise ValueError(f"invalid rust_parser_backend {value!r}; expected one of: {choices}")
    return backend


def _load_extension():
    try:
        return importlib.import_module("kisti_json_rs")
    except (ModuleNotFoundError, ImportError, OSError) as exc:
        raise RustArrowBackendUnavailable(
            "Rust Arrow backend is not installed. Build it with "
            "`python -m maturin develop --manifest-path crates/kisti_json_rs/Cargo.toml --release`."
        ) from exc


def rust_arrow_available() -> bool:
    try:
        _load_extension()
    except RustArrowBackendUnavailable:
        return False
    return True


def rust_arrow_unsupported_reason(
    *,
    requested_backend: str,
    persist_parquet_files: bool,
    create: bool,
    load: bool,
    index: bool,
    optimize: bool,
    emit_ddl: bool,
    custom_extract_fn: bool,
    id_compaction_enabled: bool,
    excepted_expand_dict_enabled: bool = False,
) -> str | None:
    backend = normalize_flatten_backend(requested_backend)
    if backend == BACKEND_PYTHON:
        return "python backend requested"
    if not persist_parquet_files:
        return "rust-arrow requires persist_parquet_files=true"
    if custom_extract_fn:
        return "custom extract_fn supplied"
    if excepted_expand_dict_enabled:
        return "rust-arrow does not support excepted_expand_dict=true without changing the Python parquet contract"
    return None


def _normalize_id_compaction_payload(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not value:
        return None
    from .id_compaction import RULES_VERSION, _rules_hash, normalize_id_compaction_config, validate_id_compaction_config

    raw = dict(value)
    cfg = normalize_id_compaction_config(raw if "id_compaction" in raw else {"id_compaction": raw})
    validate_id_compaction_config(cfg)
    if not cfg.get("enabled"):
        return None
    cfg["rules_version"] = RULES_VERSION
    cfg["rules_hash"] = _rules_hash(cfg)
    return cfg


def _persist_options(
    *,
    base_table: str,
    index_key: str,
    except_keys: Sequence[str] | None,
    excepted_expand_dict: bool,
    sep: str,
    parquet_dir: str | Path,
    batch_idx: int,
    index_offset: int,
    record_contexts: Sequence[Mapping[str, Any]] | None,
    parallel_workers: int,
    parallel_table_writes: bool = False,
    columnar_accumulator: bool = False,
    parser_backend: str | None = None,
    id_compaction: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    id_compaction_payload = _normalize_id_compaction_payload(id_compaction)
    options = {
        "base_table": str(base_table),
        "index_key": str(index_key),
        "except_keys": [str(k) for k in list(except_keys or []) if str(k)],
        "excepted_expand_dict": bool(excepted_expand_dict),
        "sep": str(sep or "__"),
        "parquet_dir": str(Path(parquet_dir)),
        "batch_idx": int(batch_idx),
        "index_offset": int(index_offset),
        "record_contexts": list(record_contexts or []),
        "parallel_workers": int(parallel_workers or 0),
        "parallel_table_writes": bool(parallel_table_writes),
        "columnar_accumulator": bool(columnar_accumulator),
        "parser_backend": normalize_parser_backend(parser_backend),
    }
    if id_compaction_payload:
        options["id_compaction"] = id_compaction_payload
    return options


def persist_json_batch_to_parquet(
    records: Sequence[Mapping[str, Any]],
    *,
    base_table: str,
    index_key: str,
    except_keys: Sequence[str] | None,
    excepted_expand_dict: bool,
    sep: str,
    parquet_dir: str | Path,
    batch_idx: int,
    index_offset: int,
    record_contexts: Sequence[Mapping[str, Any]] | None,
    parallel_workers: int,
    parallel_table_writes: bool = False,
    columnar_accumulator: bool = False,
    parser_backend: str | None = None,
    id_compaction: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    ext = _load_extension()
    options = _persist_options(
        base_table=base_table,
        index_key=index_key,
        except_keys=except_keys,
        excepted_expand_dict=excepted_expand_dict,
        sep=sep,
        parquet_dir=parquet_dir,
        batch_idx=batch_idx,
        index_offset=index_offset,
        record_contexts=record_contexts,
        parallel_workers=parallel_workers,
        parallel_table_writes=parallel_table_writes,
        columnar_accumulator=columnar_accumulator,
        parser_backend=parser_backend,
        id_compaction=id_compaction,
    )
    result = ext.persist_json_batch(records, options)
    if not isinstance(result, Mapping):
        raise RustArrowBackendUnavailable("Rust Arrow backend returned an invalid result")
    return dict(result)


def persist_json_lines_batch_to_parquet(
    lines: Sequence[str | bytes],
    *,
    base_table: str,
    index_key: str,
    except_keys: Sequence[str] | None,
    excepted_expand_dict: bool,
    sep: str,
    parquet_dir: str | Path,
    batch_idx: int,
    index_offset: int,
    record_contexts: Sequence[Mapping[str, Any]] | None,
    parallel_workers: int,
    parallel_table_writes: bool = False,
    columnar_accumulator: bool = False,
    parser_backend: str | None = None,
    id_compaction: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    ext = _load_extension()
    persist_fn = getattr(ext, "persist_json_lines_batch", None)
    if not callable(persist_fn):
        raise RustArrowBackendUnavailable("Rust Arrow backend does not support raw JSONL parsing")
    options = _persist_options(
        base_table=base_table,
        index_key=index_key,
        except_keys=except_keys,
        excepted_expand_dict=excepted_expand_dict,
        sep=sep,
        parquet_dir=parquet_dir,
        batch_idx=batch_idx,
        index_offset=index_offset,
        record_contexts=record_contexts,
        parallel_workers=parallel_workers,
        parallel_table_writes=parallel_table_writes,
        columnar_accumulator=columnar_accumulator,
        parser_backend=parser_backend,
        id_compaction=id_compaction,
    )
    payload = lines if isinstance(lines, list) else list(lines)
    result = persist_fn(payload, options)
    if not isinstance(result, Mapping):
        raise RustArrowBackendUnavailable("Rust Arrow raw JSONL backend returned an invalid result")
    return dict(result)


def persist_jsonl_sources_to_parquet(
    sources: Sequence[str | Path],
    *,
    base_table: str,
    index_key: str,
    except_keys: Sequence[str] | None,
    excepted_expand_dict: bool,
    sep: str,
    parquet_dir: str | Path,
    batch_idx: int,
    index_offset: int,
    parallel_workers: int,
    chunk_size: int,
    parquet_flush_records: int | None = None,
    parallel_table_writes: bool = False,
    columnar_accumulator: bool = False,
    parser_backend: str | None = None,
    max_records: int | None = None,
    id_compaction: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    ext = _load_extension()
    persist_fn = getattr(ext, "persist_jsonl_sources", None)
    if not callable(persist_fn):
        raise RustArrowBackendUnavailable("Rust Arrow backend does not support direct JSONL source parsing")
    options = _persist_options(
        base_table=base_table,
        index_key=index_key,
        except_keys=except_keys,
        excepted_expand_dict=excepted_expand_dict,
        sep=sep,
        parquet_dir=parquet_dir,
        batch_idx=batch_idx,
        index_offset=index_offset,
        record_contexts=None,
        parallel_workers=parallel_workers,
        parallel_table_writes=parallel_table_writes,
        columnar_accumulator=columnar_accumulator,
        parser_backend=parser_backend,
        id_compaction=id_compaction,
    )
    options["chunk_size"] = int(chunk_size or 1000)
    if parquet_flush_records is not None:
        options["parquet_flush_records"] = int(parquet_flush_records or 0)
    if max_records is not None:
        options["max_records"] = int(max_records)
    result = persist_fn([str(Path(source)) for source in sources], options)
    if not isinstance(result, Mapping):
        raise RustArrowBackendUnavailable("Rust Arrow direct JSONL source backend returned an invalid result")
    return dict(result)


def load_parquet_files_to_mysql(
    tables: Sequence[Mapping[str, Any]],
    *,
    db_config: Mapping[str, Any],
    batch_size: int = 1000,
    connect_timeout_s: int = 3,
    transaction: bool = True,
) -> dict[str, Any]:
    ext = _load_extension()
    load_fn = getattr(ext, "load_parquet_files_to_mysql", None)
    if not callable(load_fn):
        raise RustArrowBackendUnavailable("Rust Arrow backend was built without Rust MySQL loader support")
    payload = []
    for item in tables:
        payload.append(
            {
                "path": str(item.get("path") or ""),
                "table_sql": str(item.get("table_sql") or ""),
                "columns_original": [str(c) for c in list(item.get("columns_original") or [])],
                "columns_sql": [str(c) for c in list(item.get("columns_sql") or [])],
            }
        )
    result = load_fn(
        payload,
        {
            "db_config": dict(db_config or {}),
            "batch_size": int(batch_size or 1000),
            "connect_timeout_s": int(connect_timeout_s or 3),
            "transaction": bool(transaction),
        },
    )
    if not isinstance(result, Mapping):
        raise RustArrowBackendUnavailable("Rust MySQL loader returned an invalid result")
    return dict(result)
