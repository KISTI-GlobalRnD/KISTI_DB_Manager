from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..runstate import atomic_write_text

def _cmd_parquet_reload(args: argparse.Namespace) -> int:
    from ..parquet_reload import run_reload_plan

    result = run_reload_plan(
        Path(args.plan),
        start_at=str(args.start_at or ""),
        only_table=str(args.only_table or ""),
        force_reload_completed=bool(args.force_reload_completed),
        skip_finalizer=bool(args.skip_finalizer),
        skip_preflight=bool(args.skip_preflight),
        dry_run=bool(args.dry_run),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def _cmd_parquet_preflight(args: argparse.Namespace) -> int:
    from ..target_db_preflight import run_target_db_preflight

    result = run_target_db_preflight(
        Path(args.plan),
        out_path=Path(args.out).expanduser().resolve() if args.out else None,
        table_names=[str(item).strip() for item in args.table if str(item).strip()] or None,
        require_reload_supported=bool(args.require_reload_supported),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result.get("status") == "failed" else 0


def _cmd_parquet_inspect(args: argparse.Namespace) -> int:
    from ..parquet_artifacts import artifact_contract_from_plan, inspect_parquet_artifact_contract

    table_names = [str(item).strip() for item in (args.table or []) if str(item).strip()] or None
    if args.plan:
        plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
        result = artifact_contract_from_plan(
            plan,
            table_names=table_names,
            require_schema_manifest=bool(args.require_schema_manifest),
            require_id_compaction=bool(args.require_id_compaction),
            strict_schema_manifest=bool(args.strict_schema_manifest),
        )
        result["plan"] = str(Path(args.plan).expanduser().resolve())
    else:
        if not args.parquet_root:
            raise SystemExit("parquet inspect requires --parquet-root or --plan")
        result = inspect_parquet_artifact_contract(
            Path(args.parquet_root),
            table_names=table_names,
            require_schema_manifest=bool(args.require_schema_manifest),
            require_id_compaction=bool(args.require_id_compaction),
            strict_schema_manifest=bool(args.strict_schema_manifest),
        )
    if args.out:
        out = Path(args.out).expanduser()
        atomic_write_text(out, json.dumps(result, ensure_ascii=False, indent=2), purpose="parquet inspect output")
        print(f"artifact_contract: {out}")
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if result.get("status") == "failed" else 0


def _cmd_parquet_mark_table_done(args: argparse.Namespace) -> int:
    from ..parquet_reload import mark_table_done_from_validation_report

    result = mark_table_done_from_validation_report(
        status_path=Path(args.status),
        table=str(args.table),
        validation_report=Path(args.validation_report),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def _cmd_parquet_finalize(args: argparse.Namespace) -> int:
    from ..parquet_finalize import run_finalize_plan

    result = run_finalize_plan(
        Path(args.plan),
        out_path=Path(args.out).expanduser().resolve() if args.out else None,
        strict_indexes=True if args.strict_indexes else None,
        no_unique_fallback=True if args.no_unique_fallback else None,
        skip_analyze=True if args.skip_analyze else None,
        skip_validation=True if args.skip_validation else None,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def register_parquet_parser(sub) -> None:
    p_parquet = sub.add_parser("parquet", help="Parquet materialize/reload/finalize helpers")
    parquet_sub = p_parquet.add_subparsers(dest="parquet_cmd", required=True)

    p_parquet_reload = parquet_sub.add_parser("reload", help="Run a config-driven parquet reload plan")
    p_parquet_reload.add_argument("--plan", required=True, help="Parquet reload plan JSON")
    p_parquet_reload.add_argument("--start-at", default="", help="Start from this table in the plan")
    p_parquet_reload.add_argument("--only-table", default="", help="Run only one table from the plan")
    p_parquet_reload.add_argument("--force-reload-completed", action="store_true")
    p_parquet_reload.add_argument("--skip-finalizer", action="store_true")
    p_parquet_reload.add_argument("--skip-preflight", action="store_true")
    p_parquet_reload.add_argument("--dry-run", action="store_true")
    p_parquet_reload.set_defaults(func=_cmd_parquet_reload)

    p_parquet_preflight = parquet_sub.add_parser("preflight", help="Inspect target DB compatibility before parquet reload")
    p_parquet_preflight.add_argument("--plan", required=True, help="Parquet reload plan JSON")
    p_parquet_preflight.add_argument("--out", default="", help="Preflight report path")
    p_parquet_preflight.add_argument("--table", action="append", default=[], help="Restrict preflight to selected plan table; repeatable")
    p_parquet_preflight.add_argument("--require-reload-supported", action="store_true")
    p_parquet_preflight.set_defaults(func=_cmd_parquet_preflight)

    p_parquet_inspect = parquet_sub.add_parser("inspect", help="Inspect parquet artifact contract and schema manifest")
    p_parquet_inspect.add_argument("--parquet-root", default="", help="Parquet root containing table directories")
    p_parquet_inspect.add_argument("--plan", default="", help="Plan JSON to derive parquet root and selected tables")
    p_parquet_inspect.add_argument("--out", default="", help="Write artifact contract report JSON to this path")
    p_parquet_inspect.add_argument("--table", action="append", default=[], help="Restrict inspection to selected table; repeatable")
    p_parquet_inspect.add_argument("--require-schema-manifest", action="store_true")
    p_parquet_inspect.add_argument("--require-id-compaction", action="store_true")
    p_parquet_inspect.add_argument("--strict-schema-manifest", action="store_true")
    p_parquet_inspect.set_defaults(func=_cmd_parquet_inspect)

    p_parquet_mark = parquet_sub.add_parser("mark-table-done", help="Recover status from a clean validation report")
    p_parquet_mark.add_argument("--status", required=True, help="parquet_reload_status JSON path")
    p_parquet_mark.add_argument("--table", required=True)
    p_parquet_mark.add_argument("--validation-report", required=True)
    p_parquet_mark.set_defaults(func=_cmd_parquet_mark_table_done)

    p_parquet_finalize = parquet_sub.add_parser("finalize", help="Run plan-driven DB indexes/analyze/validation")
    p_parquet_finalize.add_argument("--plan", required=True, help="Parquet reload plan JSON")
    p_parquet_finalize.add_argument("--out", default="", help="Finalizer report path")
    p_parquet_finalize.add_argument("--strict-indexes", action="store_true")
    p_parquet_finalize.add_argument("--no-unique-fallback", action="store_true")
    p_parquet_finalize.add_argument("--skip-analyze", action="store_true")
    p_parquet_finalize.add_argument("--skip-validation", action="store_true")
    p_parquet_finalize.set_defaults(func=_cmd_parquet_finalize)
