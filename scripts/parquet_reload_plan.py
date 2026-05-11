#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from KISTI_DB_Manager.parquet_reload import mark_table_done_from_validation_report, run_reload_plan
from KISTI_DB_Manager.target_db_preflight import run_target_db_preflight


def main() -> int:
    ap = argparse.ArgumentParser(description="Run a config-driven parquet reload plan.")
    sub = ap.add_subparsers(dest="cmd")

    run = sub.add_parser("run", help="Run materialize/validate/finalize from a plan JSON")
    run.add_argument("--plan", required=True)
    run.add_argument("--start-at", default="")
    run.add_argument("--only-table", default="")
    run.add_argument("--force-reload-completed", action="store_true")
    run.add_argument("--skip-finalizer", action="store_true")
    run.add_argument("--skip-preflight", action="store_true")
    run.add_argument("--dry-run", action="store_true")

    preflight = sub.add_parser("preflight", help="Inspect target DB compatibility before reload")
    preflight.add_argument("--plan", required=True)
    preflight.add_argument("--out", default="")
    preflight.add_argument("--table", action="append", default=[])
    preflight.add_argument("--require-reload-supported", action="store_true")

    mark = sub.add_parser("mark-table-done", help="Mark one table done from a clean validation report")
    mark.add_argument("--status", required=True, help="parquet_reload_status JSON path")
    mark.add_argument("--table", required=True)
    mark.add_argument("--validation-report", required=True)

    # Backward-friendly default: allow `scripts/parquet_reload_plan.py --plan ...`.
    ap.add_argument("--plan", default="")
    ap.add_argument("--start-at", default="")
    ap.add_argument("--only-table", default="")
    ap.add_argument("--force-reload-completed", action="store_true")
    ap.add_argument("--skip-finalizer", action="store_true")
    ap.add_argument("--skip-preflight", action="store_true")
    ap.add_argument("--dry-run", action="store_true")

    args = ap.parse_args()
    exit_code = 0
    if args.cmd == "mark-table-done":
        result = mark_table_done_from_validation_report(
            status_path=Path(args.status),
            table=str(args.table),
            validation_report=Path(args.validation_report),
        )
    elif args.cmd == "preflight":
        result = run_target_db_preflight(
            Path(args.plan),
            out_path=Path(args.out).expanduser().resolve() if args.out else None,
            table_names=[str(item).strip() for item in args.table if str(item).strip()] or None,
            require_reload_supported=bool(args.require_reload_supported),
        )
        exit_code = 1 if result.get("status") == "failed" else 0
    else:
        plan_path = getattr(args, "plan", "") or ""
        if not plan_path:
            ap.error("--plan is required")
        result = run_reload_plan(
            Path(plan_path),
            start_at=str(getattr(args, "start_at", "") or ""),
            only_table=str(getattr(args, "only_table", "") or ""),
            force_reload_completed=bool(getattr(args, "force_reload_completed", False)),
            skip_finalizer=bool(getattr(args, "skip_finalizer", False)),
            skip_preflight=bool(getattr(args, "skip_preflight", False)),
            dry_run=bool(getattr(args, "dry_run", False)),
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
