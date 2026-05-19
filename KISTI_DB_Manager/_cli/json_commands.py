from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any
from ..modes import MODES as _MODES

from ..runstate import atomic_write_text
from .common import ConfigValidationError, _ensure_optional_deps, _resolve_bool, _validate_json_run_config

def _cmd_json_run(args: argparse.Namespace) -> int:
    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    data_config = cfg.get("data_config") or cfg.get("data") or {}
    db_config = cfg.get("db_config") or cfg.get("db") or {}
    parallel_workers_in_config = bool(isinstance(data_config, dict) and "parallel_workers" in data_config)
    from ..config import coerce_data_config, coerce_db_config

    from ..modes import apply_mode, resolve_mode_name

    data_config = coerce_data_config(data_config, inplace=isinstance(data_config, dict))
    db_config = coerce_db_config(db_config, inplace=isinstance(db_config, dict))
    mode_name = resolve_mode_name(getattr(args, "mode", None), data_config)
    mode_spec = apply_mode(mode_name, data_config)
    parallel_workers_explicit = bool(
        parallel_workers_in_config and "parallel_workers" not in (mode_spec.data_overrides or {})
    )

    if getattr(args, "fast_load_session", None) is not None:
        data_config["fast_load_session"] = bool(args.fast_load_session)
    if getattr(args, "schema_mode", None):
        data_config["schema_mode"] = str(args.schema_mode)
    if getattr(args, "schema_hybrid_warmup_batches", None) is not None:
        data_config["schema_hybrid_warmup_batches"] = int(args.schema_hybrid_warmup_batches)
    if getattr(args, "extra_column_name", None):
        data_config["extra_column_name"] = str(args.extra_column_name)
    if getattr(args, "db_load_method", None):
        data_config["db_load_method"] = args.db_load_method
    if getattr(args, "rust_db_load", None) is not None:
        data_config["rust_db_load"] = bool(args.rust_db_load)
    if getattr(args, "parallel_workers", None) is not None:
        data_config["parallel_workers"] = int(args.parallel_workers)
        parallel_workers_explicit = True
    if getattr(args, "flatten_backend", None):
        data_config["flatten_backend"] = str(args.flatten_backend)
    if getattr(args, "rust_raw_jsonl_parse", None) is not None:
        data_config["rust_raw_jsonl_parse"] = bool(args.rust_raw_jsonl_parse)
    if getattr(args, "rust_raw_jsonl_file_parse", None) is not None:
        data_config["rust_raw_jsonl_file_parse"] = bool(args.rust_raw_jsonl_file_parse)
    if getattr(args, "rust_parallel_table_writes", None) is not None:
        data_config["rust_parallel_table_writes"] = bool(args.rust_parallel_table_writes)
    if getattr(args, "rust_columnar_accumulator", None) is not None:
        data_config["rust_columnar_accumulator"] = bool(args.rust_columnar_accumulator)
    if getattr(args, "rust_parquet_flush_records", None) is not None:
        data_config["rust_parquet_flush_records"] = int(args.rust_parquet_flush_records)
    if getattr(args, "rust_parser_backend", None):
        data_config["rust_parser_backend"] = str(args.rust_parser_backend)
    if getattr(args, "db_load_parallel_tables", None) is not None:
        data_config["db_load_parallel_tables"] = int(args.db_load_parallel_tables)
    if getattr(args, "load_data_commit_strategy", None):
        data_config["load_data_commit_strategy"] = str(args.load_data_commit_strategy)
    if getattr(args, "overlap_batches", None) is not None:
        data_config["overlap_batches"] = bool(args.overlap_batches)
    if getattr(args, "json_streaming_load", None) is not None:
        data_config["json_streaming_load"] = bool(args.json_streaming_load)
    if getattr(args, "persist_parquet_files", None) is not None:
        data_config["persist_parquet_files"] = bool(args.persist_parquet_files)
    if getattr(args, "persist_parquet_dir", None):
        data_config["persist_parquet_dir"] = str(args.persist_parquet_dir)
    if getattr(args, "tsv_merge_union_schema", None) is not None:
        data_config["tsv_merge_union_schema"] = bool(args.tsv_merge_union_schema)
    if getattr(args, "tsv_union_merge_min_coverage", None) is not None:
        data_config["tsv_union_merge_min_coverage"] = float(args.tsv_union_merge_min_coverage)
    if getattr(args, "tsv_union_merge_max_union_cols", None) is not None:
        data_config["tsv_union_merge_max_union_cols"] = int(args.tsv_union_merge_max_union_cols)
    if getattr(args, "tsv_union_merge_max_missing_cols", None) is not None:
        data_config["tsv_union_merge_max_missing_cols"] = int(args.tsv_union_merge_max_missing_cols)
    if getattr(args, "persist_tsv_files", None) is not None:
        data_config["persist_tsv_files"] = bool(args.persist_tsv_files)
    if getattr(args, "persist_tsv_dir", None):
        data_config["persist_tsv_dir"] = str(args.persist_tsv_dir)
    if getattr(args, "chunk_size", None) is not None:
        data_config["chunk_size"] = int(args.chunk_size)
    if getattr(args, "auto_except", None) is not None:
        data_config["auto_except"] = bool(args.auto_except)
    if getattr(args, "auto_except_sample_records", None) is not None:
        data_config["auto_except_sample_records"] = int(args.auto_except_sample_records)
    if getattr(args, "auto_except_sample_max_sources", None) is not None:
        data_config["auto_except_sample_max_sources"] = int(args.auto_except_sample_max_sources)
    if getattr(args, "auto_except_seed", None) is not None:
        data_config["auto_except_seed"] = int(args.auto_except_seed)
    if getattr(args, "auto_except_unique_key_threshold", None) is not None:
        data_config["auto_except_unique_key_threshold"] = int(args.auto_except_unique_key_threshold)
    if getattr(args, "auto_except_min_observations", None) is not None:
        data_config["auto_except_min_observations"] = int(args.auto_except_min_observations)
    if getattr(args, "auto_except_novelty_threshold", None) is not None:
        data_config["auto_except_novelty_threshold"] = float(args.auto_except_novelty_threshold)

    def ensure_id_compaction_config() -> dict[str, Any]:
        if not isinstance(data_config.get("id_compaction"), dict):
            data_config["id_compaction"] = {}
        return data_config["id_compaction"]

    if getattr(args, "id_compaction", None) is not None:
        ensure_id_compaction_config()["enabled"] = bool(args.id_compaction)
    if getattr(args, "id_compaction_preset", None):
        ensure_id_compaction_config()["preset"] = str(args.id_compaction_preset)
    if getattr(args, "id_compaction_mode", None):
        ensure_id_compaction_config()["mode"] = str(args.id_compaction_mode)
    if getattr(args, "id_compaction_collision_policy", None):
        ensure_id_compaction_config()["collision_policy"] = str(args.id_compaction_collision_policy)
    if getattr(args, "id_compaction_namespace_conflict_policy", None):
        ensure_id_compaction_config()["namespace_conflict_policy"] = str(
            args.id_compaction_namespace_conflict_policy
        )

    create = _resolve_bool(getattr(args, "create", None), mode_spec.stage_defaults.get("create", True)) and not bool(args.dry_run)
    load = _resolve_bool(getattr(args, "load", None), mode_spec.stage_defaults.get("load", True)) and not bool(args.dry_run)
    index = _resolve_bool(getattr(args, "index", None), mode_spec.stage_defaults.get("index", True)) and not bool(args.dry_run)
    optimize = _resolve_bool(getattr(args, "optimize", None), mode_spec.stage_defaults.get("optimize", True)) and not bool(args.dry_run)

    _validate_json_run_config(data_config, mode_name=mode_spec.name)

    json_modules = ["numpy", "pandas", "tqdm", "orjson", "xmltodict"]
    if bool(data_config.get("persist_parquet_files", False)):
        json_modules.append("pyarrow")
    _ensure_optional_deps("json run", json_modules, extras=["json"])
    if create or load or index or optimize:
        db_mods = ["pymysql"]
        if load:
            db_mods.append("sqlalchemy")
        _ensure_optional_deps("json DB stages", db_mods, extras=["db"])

    from ..pipeline import run_json_pipeline
    from ..quarantine import QuarantineWriter
    from ..report import RunReport

    quarantine = QuarantineWriter(args.quarantine) if args.quarantine else None
    report = RunReport()

    report.set_artifact("mode", mode_spec.name)
    report.set_artifact(
        "json_execution_path",
        "streaming-load"
        if bool(data_config.get("json_streaming_load", False))
        else "parquet-first",
    )
    # Fast progress tracking: when --report is provided, also emit a small progress snapshot
    # periodically during the run. This makes it easy to locate the last shard/line after crashes.
    if args.report:
        try:
            data_config.setdefault("progress_path", str(args.report) + ".progress.json")
            data_config.setdefault("progress_interval_s", 10.0)
        except Exception:
            pass
    data_config["_parallel_workers_explicit"] = bool(parallel_workers_explicit)

    res = run_json_pipeline(
        data_config,
        db_config,
        index_key=args.index_key,
        except_keys=args.except_key or None,
        chunk_size=args.chunk_size,
        max_records=args.max_records,
        emit_ddl=args.print_ddl,
        create=create,
        load=load,
        index=index,
        optimize=optimize,
        continue_on_error=not args.fail_fast,
        report=report,
        quarantine=quarantine,
    )

    res.report.finish()

    if args.report:
        res.report.save_json(args.report)
        print(f"report: {args.report}")
    else:
        print(res.report.to_json())

    if args.print_namemap:
        print(json.dumps({k: v.to_dict() for k, v in res.name_maps.items()}, ensure_ascii=False, indent=2))

    if args.print_ddl:
        ddls = res.report.artifacts.get("create_table_sql_json")
        if ddls:
            print(json.dumps(ddls, ensure_ascii=False, indent=2))
        else:
            print("(no DDL available)")

    return 0


def _cmd_json_profile_parallel(args: argparse.Namespace) -> int:
    from ..json_parallel_profile import parse_worker_list, profile_parallel
    from ..rust_arrow_backend import parse_backend_list

    try:
        workers = parse_worker_list(args.workers)
    except ValueError as e:
        raise ConfigValidationError(str(e)) from e
    try:
        flatten_backends = parse_backend_list(args.flatten_backends)
    except ValueError as e:
        raise ConfigValidationError(str(e)) from e

    _ensure_optional_deps(
        "json profile-parallel",
        ["numpy", "pandas", "tqdm", "orjson", "xmltodict", "pyarrow"],
        extras=["json"],
    )

    result = profile_parallel(
        config_path=args.config,
        workers=workers,
        flatten_backends=flatten_backends,
        out_dir=args.out or None,
        max_records=args.max_records,
        chunk_size=args.chunk_size,
        mode=args.mode,
        keep_artifacts=bool(args.keep_artifacts),
        cleanup_parquet=bool(args.cleanup_parquet),
        index_key=args.index_key,
        except_keys=args.except_key or None,
        rust_raw_jsonl_parse=args.rust_raw_jsonl_parse,
        rust_raw_jsonl_file_parse=args.rust_raw_jsonl_file_parse,
        rust_parallel_table_writes=args.rust_parallel_table_writes,
        rust_columnar_accumulator=args.rust_columnar_accumulator,
        rust_parquet_flush_records=args.rust_parquet_flush_records,
        rust_parser_backend=args.rust_parser_backend,
        id_compaction=args.id_compaction,
        id_compaction_preset=args.id_compaction_preset,
        id_compaction_mode=args.id_compaction_mode,
        id_compaction_collision_policy=args.id_compaction_collision_policy,
        id_compaction_namespace_conflict_policy=args.id_compaction_namespace_conflict_policy,
        repeat=args.repeat,
        shuffle_order=bool(args.shuffle_order),
        seed=args.seed,
        issue_sample_limit=args.issue_sample_limit,
    )
    print(f"parallel_profile: {result.get('summary_json_path')}")
    print(f"parallel_profile_md: {result.get('summary_md_path')}")
    if "recommended_flatten_backend" in result:
        print(f"recommended_flatten_backend: {result.get('recommended_flatten_backend')}")
    print(f"recommended_parallel_workers: {result.get('recommended_parallel_workers')}")
    return 1 if result.get("status") == "failed" else 0


def _cmd_json_id_compaction_preflight(args: argparse.Namespace) -> int:
    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    data_config = cfg.get("data_config") or cfg.get("data") or {}

    from ..config import coerce_data_config

    data_config = coerce_data_config(data_config, inplace=isinstance(data_config, dict))

    def ensure_id_compaction_config() -> dict[str, Any]:
        if not isinstance(data_config.get("id_compaction"), dict):
            data_config["id_compaction"] = {}
        return data_config["id_compaction"]

    # This command is specifically for compaction, so default to enabled unless the
    # user explicitly disables it.
    ensure_id_compaction_config().setdefault("enabled", True)
    if getattr(args, "id_compaction", None) is not None:
        ensure_id_compaction_config()["enabled"] = bool(args.id_compaction)
    if getattr(args, "id_compaction_preset", None):
        ensure_id_compaction_config()["preset"] = str(args.id_compaction_preset)
    if getattr(args, "id_compaction_mode", None):
        ensure_id_compaction_config()["mode"] = str(args.id_compaction_mode)
    if getattr(args, "id_compaction_collision_policy", None):
        ensure_id_compaction_config()["collision_policy"] = str(args.id_compaction_collision_policy)
    if getattr(args, "id_compaction_namespace_conflict_policy", None):
        ensure_id_compaction_config()["namespace_conflict_policy"] = str(
            args.id_compaction_namespace_conflict_policy
        )

    _ensure_optional_deps("json id-compaction-preflight", ["numpy", "pandas"], extras=["json"])

    from ..id_compaction_preflight import run_id_compaction_preflight

    result = run_id_compaction_preflight(
        data_config,
        index_key=args.index_key,
        except_keys=args.except_key or None,
        max_records=args.max_records,
        max_examples_per_key=args.max_examples_per_key,
        force_enable=(False if getattr(args, "id_compaction", None) is False else True),
    )

    if args.report:
        atomic_write_text(
            args.report,
            json.dumps(result, ensure_ascii=False, indent=2, default=str),
            purpose="id compaction preflight report",
        )
        print(f"preflight: {args.report}")
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))

    if result.get("status") == "failed" and not bool(args.allow_issues):
        return 1
    return 0


def register_json_parser(sub) -> None:
    p_json = sub.add_parser("json", help="JSON -> DB pipeline helpers")
    json_sub = p_json.add_subparsers(dest="json_cmd", required=True)
    p_json_run = json_sub.add_parser("run", help="Run JSON flatten/create/load/index/optimize")
    p_json_run.add_argument("--config", required=True, help="JSON config file containing data_config and db_config")
    p_json_run.add_argument("--index-key", help="Override record id key (default: config or 'id')")
    p_json_run.add_argument("--except-key", action="append", help="Exclude a branch from flattening (repeatable)")
    p_json_run.add_argument("--chunk-size", type=int, help="Records per batch (default: config or 1000)")
    p_json_run.add_argument("--max-records", type=int, help="Stop after N records (useful for dry-run/preview)")
    p_json_run.add_argument("--fail-fast", action="store_true", help="Stop on first failure")
    p_json_run.add_argument("--dry-run", action="store_true", help="Prepare desc/namemap only (skip DB steps)")
    p_json_run.add_argument("--report", help="Write RunReport JSON to this path")
    p_json_run.add_argument("--quarantine", help="Write failures as JSONL to this path")
    p_json_run.add_argument("--print-namemap", action="store_true", help="Print NameMap JSON after run")
    p_json_run.add_argument("--print-ddl", action="store_true", help="Print CREATE TABLE DDLs after run")
    p_json_run.add_argument("--mode", choices=sorted(_MODES), help="Run mode preset (default: config.mode or default)")
    p_json_run.add_argument(
        "--schema-mode",
        choices=["evolve", "freeze", "hybrid"],
        help="Schema drift strategy (default: mode/config). freeze/hybrid store unknown fields into extra column.",
    )
    p_json_run.add_argument(
        "--schema-hybrid-warmup-batches",
        type=int,
        help="For schema_mode=hybrid: number of initial batches to allow ALTER (default: mode/config, usually 1).",
    )
    p_json_run.add_argument(
        "--extra-column-name",
        dest="extra_column_name",
        help="Extra column name used when schema_mode=freeze/hybrid (default: mode/config, usually '__extra__')",
    )
    p_json_run.add_argument(
        "--fast-load-session",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Best-effort DB session tuning for ingest speed (default: mode/config)",
    )
    p_json_run.add_argument(
        "--db-load-method",
        choices=["auto", "to_sql", "load_data"],
        help="DB load method override (default: config or 'auto')",
    )
    p_json_run.add_argument(
        "--rust-db-load",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Use the optional Rust MySQL loader for Rust parquet artifacts (default: config or false)",
    )
    p_json_run.add_argument(
        "--parallel-workers",
        type=int,
        help="ProcessPool workers for JSON flatten (default: config or 0/off)",
    )
    p_json_run.add_argument(
        "--flatten-backend",
        choices=["auto", "python", "rust-arrow"],
        help="JSON parse/parquet backend: auto, python, or rust-arrow (default: config or auto)",
    )
    p_json_run.add_argument(
        "--rust-raw-jsonl-parse",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Use Rust to parse raw JSONL/GZ lines in eligible parse/parquet-only rust-arrow runs (default: config or true).",
    )
    p_json_run.add_argument(
        "--rust-raw-jsonl-file-parse",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Use Rust to read and parse JSONL/NDJSON files directly in supported rust-arrow runs (default: config or false).",
    )
    p_json_run.add_argument(
        "--rust-parallel-table-writes",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Allow Rust to write per-table parquet artifacts in parallel when supported (default: config or false).",
    )
    p_json_run.add_argument(
        "--rust-columnar-accumulator",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Use the opt-in Rust columnar flatten/parquet accumulator when supported (default: config or false).",
    )
    p_json_run.add_argument(
        "--rust-parquet-flush-records",
        type=int,
        help="For direct Rust JSONL file parsing, write parquet after roughly N valid records instead of every chunk_size records (0/default: chunk_size).",
    )
    p_json_run.add_argument(
        "--rust-parser-backend",
        choices=["serde-json", "simd-json"],
        help="Experimental Rust raw JSONL parser backend: serde-json or simd-json (default: config or serde-json).",
    )
    p_json_run.add_argument(
        "--db-load-parallel-tables",
        type=int,
        help="Parallelize LOAD DATA across tables (default: config or 0/off)",
    )
    p_json_run.add_argument(
        "--load-data-commit-strategy",
        choices=["file", "table", "batch"],
        help="When using LOAD DATA: commit per file (default), per table, or per batch. 'batch' only applies to serial DB load (no overlap, db_load_parallel_tables<=1).",
    )
    p_json_run.add_argument(
        "--overlap-batches",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Overlap batch flattening with previous batch DB load (default: mode/config; requires streaming LOAD DATA)",
    )
    p_json_run.add_argument(
        "--json-streaming-load",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Use row-based streaming TSV load when LOAD DATA is enabled (default: config or false)",
    )
    p_json_run.add_argument(
        "--persist-parquet-files",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Save flattened parquet artifacts on local disk before DB load (default: config or true).",
    )
    p_json_run.add_argument(
        "--persist-parquet-dir",
        help="Directory to save parquet artifacts (default: runs/<table>_<run_id>/parquet).",
    )
    p_json_run.add_argument(
        "--tsv-merge-union-schema",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Rewrite TSV fragments to union schema to merge across schema drift (reduces LOAD DATA calls; may increase TSV size).",
    )
    p_json_run.add_argument(
        "--persist-tsv-files",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Keep JSON streaming TSV artifacts on local disk (default: config or false).",
    )
    p_json_run.add_argument(
        "--persist-tsv-dir",
        help="Directory to save persisted TSV artifacts (default: runs/<table>_<run_id>/tsv).",
    )
    p_json_run.add_argument(
        "--id-compaction",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable semantic ID compaction during JSON parsing (default: config or false).",
    )
    p_json_run.add_argument(
        "--id-compaction-preset",
        choices=["openalex"],
        help="ID compaction preset (default: openalex).",
    )
    p_json_run.add_argument(
        "--id-compaction-mode",
        choices=["semantic_column_strip"],
        help="ID compaction mode (default: semantic_column_strip).",
    )
    p_json_run.add_argument(
        "--id-compaction-collision-policy",
        choices=["error", "preserve"],
        help="How to handle nonblank source values that map to the same compacted column (default: error).",
    )
    p_json_run.add_argument(
        "--id-compaction-namespace-conflict-policy",
        choices=["error", "preserve"],
        help="How to handle URL namespace conflicts for semantic ID columns (default: error).",
    )
    p_json_run.add_argument(
        "--tsv-union-merge-min-coverage",
        type=float,
        help="Heuristic: attempt union merge when min(file_cols/union_cols) >= this (default: config or 0.8).",
    )
    p_json_run.add_argument(
        "--tsv-union-merge-max-union-cols",
        type=int,
        help="Heuristic: do not attempt union merge if union column count exceeds this (default: config or 256; 0 disables).",
    )
    p_json_run.add_argument(
        "--tsv-union-merge-max-missing-cols",
        type=int,
        help="Heuristic: attempt union merge when max missing columns per fragment is <= this (default: config or 32).",
    )
    p_json_run.add_argument(
        "--auto-except",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Auto-detect high-cardinality dict paths from random sample and append them to except_keys.",
    )
    p_json_run.add_argument(
        "--auto-except-sample-records",
        type=int,
        help="Auto-except random preflight sample size (records, default: config or 5000).",
    )
    p_json_run.add_argument(
        "--auto-except-sample-max-sources",
        type=int,
        help="Auto-except random source cap for sampling (default: config or 64).",
    )
    p_json_run.add_argument(
        "--auto-except-seed",
        type=int,
        help="Auto-except sampling seed (default: config or 42).",
    )
    p_json_run.add_argument(
        "--auto-except-unique-key-threshold",
        type=int,
        help="Detect path when unique dict subkeys exceed this threshold (default: config or 512).",
    )
    p_json_run.add_argument(
        "--auto-except-min-observations",
        type=int,
        help="Minimum dict-path observations to enable auto-except (default: config or 20).",
    )
    p_json_run.add_argument(
        "--auto-except-novelty-threshold",
        type=float,
        help="Detect path when unique_keys/observations >= this ratio (default: config or 2.0).",
    )
    p_json_run.add_argument(
        "--create",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable CREATE TABLE (default: mode preset)",
    )
    p_json_run.add_argument(
        "--load",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable LOAD/INSERT (default: mode preset)",
    )
    p_json_run.add_argument(
        "--index",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable INDEX creation (default: mode preset)",
    )
    p_json_run.add_argument(
        "--optimize",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable OPTIMIZE TABLE (default: mode preset)",
    )
    p_json_run.set_defaults(func=_cmd_json_run)

    p_json_profile_parallel = json_sub.add_parser(
        "profile-parallel",
        help="Compare JSON parse/parquet sample runs across parallel_workers values",
    )
    p_json_profile_parallel.add_argument("--config", required=True, help="JSON config file containing data_config and db_config")
    p_json_profile_parallel.add_argument("--index-key", help="Override record id key (default: config or 'id')")
    p_json_profile_parallel.add_argument("--except-key", action="append", help="Exclude a branch from flattening (repeatable)")
    p_json_profile_parallel.add_argument(
        "--workers",
        default="0,2,4,8",
        help="Comma-separated parallel_workers values to test (default: 0,2,4,8)",
    )
    p_json_profile_parallel.add_argument(
        "--flatten-backends",
        default="auto",
        help="Comma-separated flatten backends to test: auto, python, rust-arrow (default: auto)",
    )
    p_json_profile_parallel.add_argument(
        "--max-records",
        type=int,
        default=20000,
        help="Stop each sample run after N records (default: 20000; use 0 for full input)",
    )
    p_json_profile_parallel.add_argument(
        "--chunk-size",
        type=int,
        help="Records per batch override (default: selected mode/config)",
    )
    p_json_profile_parallel.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Run each worker setting N times and recommend by median throughput (default: 1)",
    )
    p_json_profile_parallel.add_argument(
        "--shuffle-order",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Shuffle worker/repeat execution order to reduce cache/order bias (default: true)",
    )
    p_json_profile_parallel.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for --shuffle-order (default: 42)",
    )
    p_json_profile_parallel.add_argument(
        "--issue-sample-limit",
        type=int,
        default=5,
        help="Max warning/error samples to embed per worker in the summary (default: 5)",
    )
    p_json_profile_parallel.add_argument(
        "--rust-raw-jsonl-parse",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="For rust-arrow profile runs, parse raw JSONL/NDJSON/GZ lines inside Rust (default: config or true).",
    )
    p_json_profile_parallel.add_argument(
        "--rust-raw-jsonl-file-parse",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="For rust-arrow profile runs, let Rust read JSONL/NDJSON source files directly when supported.",
    )
    p_json_profile_parallel.add_argument(
        "--rust-parallel-table-writes",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="For rust-arrow profile runs, allow parallel per-table parquet writes when supported.",
    )
    p_json_profile_parallel.add_argument(
        "--rust-columnar-accumulator",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="For rust-arrow profile runs, use the opt-in columnar flatten/parquet accumulator when supported.",
    )
    p_json_profile_parallel.add_argument(
        "--rust-parquet-flush-records",
        type=int,
        help="For rust-arrow direct JSONL profile runs, write parquet after roughly N valid records instead of every chunk_size records.",
    )
    p_json_profile_parallel.add_argument(
        "--rust-parser-backend",
        choices=["serde-json", "simd-json"],
        help="For rust-arrow raw JSONL profile runs, choose experimental parser backend.",
    )
    p_json_profile_parallel.add_argument(
        "--mode",
        choices=sorted(_MODES),
        default="parse-parquet-safe",
        help="Run mode preset to apply before profiling overrides (default: parse-parquet-safe)",
    )
    p_json_profile_parallel.add_argument(
        "--out",
        default="",
        help="Output directory (default: runs/profile_parallel_<timestamp>)",
    )
    p_json_profile_parallel.add_argument(
        "--keep-artifacts",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Keep per-worker parquet artifacts after inspection (default: true)",
    )
    p_json_profile_parallel.add_argument(
        "--cleanup-parquet",
        action="store_true",
        help="Delete per-worker parquet directories after artifact contract inspection",
    )
    p_json_profile_parallel.add_argument(
        "--id-compaction",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable semantic ID compaction during profile runs (default: config).",
    )
    p_json_profile_parallel.add_argument(
        "--id-compaction-preset",
        choices=["openalex"],
        help="ID compaction preset (default: openalex).",
    )
    p_json_profile_parallel.add_argument(
        "--id-compaction-mode",
        choices=["semantic_column_strip"],
        help="ID compaction mode (default: semantic_column_strip).",
    )
    p_json_profile_parallel.add_argument(
        "--id-compaction-collision-policy",
        choices=["error", "preserve"],
        help="How to handle nonblank source values that map to the same compacted column (default: error).",
    )
    p_json_profile_parallel.add_argument(
        "--id-compaction-namespace-conflict-policy",
        choices=["error", "preserve"],
        help="How to handle URL namespace conflicts for semantic ID columns (default: error).",
    )
    p_json_profile_parallel.set_defaults(func=_cmd_json_profile_parallel)

    p_json_id_preflight = json_sub.add_parser(
        "id-compaction-preflight",
        help="Scan JSON records for ID compaction collisions, namespace conflicts, and ambiguous URL-like columns",
    )
    p_json_id_preflight.add_argument("--config", required=True, help="JSON config file containing data_config")
    p_json_id_preflight.add_argument("--index-key", help="Override record id key (default: config or 'id')")
    p_json_id_preflight.add_argument("--except-key", action="append", help="Exclude a branch from flattening (repeatable)")
    p_json_id_preflight.add_argument(
        "--max-records",
        type=int,
        default=10000,
        help="Scan at most N records (default: 10000; use 0 for full scan)",
    )
    p_json_id_preflight.add_argument(
        "--max-examples-per-key",
        type=int,
        default=3,
        help="Store up to N source examples per issue key (default: 3)",
    )
    p_json_id_preflight.add_argument("--report", help="Write preflight JSON report to this path")
    p_json_id_preflight.add_argument(
        "--allow-issues",
        action="store_true",
        help="Return exit code 0 even when blocking ID compaction issues are found",
    )
    p_json_id_preflight.add_argument(
        "--id-compaction",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable semantic ID compaction for preflight (default: enabled).",
    )
    p_json_id_preflight.add_argument(
        "--id-compaction-preset",
        choices=["openalex"],
        help="ID compaction preset (default: openalex).",
    )
    p_json_id_preflight.add_argument(
        "--id-compaction-mode",
        choices=["semantic_column_strip"],
        help="ID compaction mode (default: semantic_column_strip).",
    )
    p_json_id_preflight.add_argument(
        "--id-compaction-collision-policy",
        choices=["error", "preserve"],
        help="Run policy to record in the report (preflight scans with preserve internally).",
    )
    p_json_id_preflight.add_argument(
        "--id-compaction-namespace-conflict-policy",
        choices=["error", "preserve"],
        help="Run policy to record in the report (preflight scans with preserve internally).",
    )
    p_json_id_preflight.set_defaults(func=_cmd_json_id_compaction_preflight)
