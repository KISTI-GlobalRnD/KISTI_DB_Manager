from __future__ import annotations

import argparse
import json
from pathlib import Path
from ..modes import MODES as _MODES

from .common import _ensure_optional_deps, _resolve_bool

def _cmd_tabular_run(args: argparse.Namespace) -> int:
    cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
    data_config = cfg.get("data_config") or cfg.get("data") or {}
    db_config = cfg.get("db_config") or cfg.get("db") or {}
    desc_params = cfg.get("desc_params") or None

    from ..modes import apply_mode, resolve_mode_name

    mode_name = resolve_mode_name(getattr(args, "mode", None), data_config)
    mode_spec = apply_mode(mode_name, data_config)

    if getattr(args, "fast_load_session", None) is not None:
        data_config["fast_load_session"] = bool(args.fast_load_session)
    if getattr(args, "db_load_method", None):
        data_config["db_load_method"] = args.db_load_method

    create = _resolve_bool(getattr(args, "create", None), mode_spec.stage_defaults.get("create", True)) and not bool(args.dry_run)
    load = _resolve_bool(getattr(args, "load", None), mode_spec.stage_defaults.get("load", True)) and not bool(args.dry_run)
    index = _resolve_bool(getattr(args, "index", None), mode_spec.stage_defaults.get("index", True)) and not bool(args.dry_run)
    optimize = _resolve_bool(getattr(args, "optimize", None), mode_spec.stage_defaults.get("optimize", True)) and not bool(args.dry_run)

    _ensure_optional_deps("tabular run", ["numpy", "pandas"], extras=["tabular"])
    if create or load or index or optimize:
        db_mods = ["pymysql"]
        if load:
            db_mods.append("sqlalchemy")
        _ensure_optional_deps("tabular DB stages", db_mods, extras=["db"])

    from ..pipeline import run_tabular_pipeline
    from ..quarantine import QuarantineWriter
    from ..report import RunReport

    quarantine = QuarantineWriter(args.quarantine) if args.quarantine else None
    report = RunReport()
    report.set_artifact("mode", mode_spec.name)

    res = run_tabular_pipeline(
        data_config,
        db_config,
        desc_params=desc_params,
        generate_desc=args.generate_desc,
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

    if args.print_namemap and res.name_map:
        print(json.dumps(res.name_map.to_dict(), ensure_ascii=False, indent=2))

    if args.print_ddl:
        ddl = res.report.artifacts.get("create_table_sql")
        if ddl:
            print(ddl)
        else:
            print("(no DDL available)")

    return 0


def register_tabular_parser(sub) -> None:
    p_tabular = sub.add_parser("tabular", help="Tabular -> DB pipeline helpers")
    tabular_sub = p_tabular.add_subparsers(dest="tabular_cmd", required=True)
    p_tabular_run = tabular_sub.add_parser("run", help="Run create/load/index/optimize for a tabular file")
    p_tabular_run.add_argument("--config", required=True, help="JSON config file containing data_config and db_config")
    p_tabular_run.add_argument("--generate-desc", action="store_true", help="Generate a new description CSV first")
    p_tabular_run.add_argument("--fail-fast", action="store_true", help="Stop on first failure")
    p_tabular_run.add_argument("--dry-run", action="store_true", help="Prepare desc/namemap only (skip DB steps)")
    p_tabular_run.add_argument("--report", help="Write RunReport JSON to this path")
    p_tabular_run.add_argument("--quarantine", help="Write failures as JSONL to this path")
    p_tabular_run.add_argument("--print-namemap", action="store_true", help="Print NameMap JSON after run")
    p_tabular_run.add_argument("--print-ddl", action="store_true", help="Print CREATE TABLE DDL after run")

    p_tabular_run.add_argument("--mode", choices=sorted(_MODES), help="Run mode preset (default: config.mode or default)")
    p_tabular_run.add_argument(
        "--fast-load-session",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Best-effort DB session tuning for ingest speed (default: mode/config)",
    )
    p_tabular_run.add_argument(
        "--db-load-method",
        choices=["auto", "to_sql", "load_data"],
        help="DB load method override (default: config or 'auto')",
    )
    p_tabular_run.add_argument(
        "--create",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable CREATE TABLE (default: mode preset)",
    )
    p_tabular_run.add_argument(
        "--load",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable LOAD/INSERT (default: mode preset)",
    )
    p_tabular_run.add_argument(
        "--index",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable INDEX creation (default: mode preset)",
    )
    p_tabular_run.add_argument(
        "--optimize",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable/disable OPTIMIZE TABLE (default: mode preset)",
    )
    p_tabular_run.set_defaults(func=_cmd_tabular_run)
