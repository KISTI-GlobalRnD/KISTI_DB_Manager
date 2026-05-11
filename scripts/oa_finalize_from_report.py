#!/usr/bin/env python3
"""
DB-only finalize helper for completed JSON ingest runs.

Why this exists:
- `json run --mode finalize` currently rebuilds name maps by re-reading source JSON.
- For very large OpenAlex runs, that extra scan is expensive when we already have
  stable `name_maps_json` stored in the completed `run_report.json`.

This helper loads:
- run_dir/config.json
- run_dir/run_report.json

and performs only DB-side post steps:
- create best-effort indexes on `index_key`
- optional OPTIMIZE TABLE
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from KISTI_DB_Manager import manage
from KISTI_DB_Manager.namemap import load_namemap


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_run_context(run_dir: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    cfg_path = run_dir / "config.json"
    report_path = run_dir / "run_report.json"
    if not cfg_path.exists():
        raise FileNotFoundError(f"missing config.json: {cfg_path}")
    if not report_path.exists():
        raise FileNotFoundError(f"missing run_report.json: {report_path}")
    return _read_json(cfg_path), _read_json(report_path)


def _load_name_maps(cfg: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    artifacts = report.get("artifacts") or {}
    name_maps = artifacts.get("name_maps_json")
    if isinstance(name_maps, dict) and name_maps:
        return dict(name_maps)

    dc = cfg.get("data_config") or {}
    name_maps = dc.get("_name_maps_json")
    if isinstance(name_maps, dict) and name_maps:
        return dict(name_maps)

    raise ValueError("no name_maps_json found in run_report.json artifacts or config.json data_config")


def _index_exists(db_config: dict[str, Any], *, table_name: str, column_name: str) -> bool:
    import pymysql

    conn = None
    cur = None
    try:
        conn = pymysql.connect(**db_config)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM information_schema.statistics
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name = %s
            LIMIT 1
            """,
            (db_config.get("database"), table_name, column_name),
        )
        return cur.fetchone() is not None
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir", help="runs/<run_id_dir>")
    ap.add_argument("--no-index", action="store_true", help="Skip index creation")
    ap.add_argument("--optimize", action="store_true", help="Run OPTIMIZE TABLE after indexes")
    ap.add_argument(
        "--table",
        action="append",
        default=[],
        help="Limit to one or more SQL table names (repeatable)",
    )
    args = ap.parse_args()

    run_dir = Path(args.run_dir).expanduser().resolve()
    cfg, report = _load_run_context(run_dir)
    db_config = manage.coerce_db_config(cfg.get("db_config") or {})
    data_config = manage.coerce_data_config(cfg.get("data_config") or {}, inplace=False)
    name_maps = _load_name_maps(cfg, report)
    selected_tables = {str(t).strip() for t in (args.table or []) if str(t).strip()}
    index_key = str((report.get("artifacts") or {}).get("index_key") or data_config.get("index_key") or "id").strip()
    prefix_len = int(data_config.get("index_prefix_len", 191) or 191)

    indexed = 0
    skipped = 0
    optimized = 0

    for table_original, nm_raw in name_maps.items():
        nm = load_namemap(nm_raw)
        if nm is None:
            print(f"[finalize] skip invalid namemap: {table_original}")
            skipped += 1
            continue
        if selected_tables and nm.table_sql not in selected_tables:
            continue

        if not args.no_index:
            if index_key not in nm.columns_original:
                print(f"[finalize] skip index {nm.table_sql}: missing column `{index_key}`")
                skipped += 1
            else:
                col_sql = nm.map_column(index_key)
                if _index_exists(db_config, table_name=nm.table_sql, column_name=col_sql):
                    print(f"[finalize] skip index {nm.table_sql}: `{col_sql}` already indexed")
                    skipped += 1
                else:
                    print(f"[finalize] index {nm.table_sql} ({col_sql})")
                    manage.set_index_simple(
                        db_config,
                        table_name=nm.table_sql,
                        column=index_key,
                        name_map=nm,
                        prefix_len=prefix_len,
                    )
                    indexed += 1

        if args.optimize:
            print(f"[finalize] optimize {nm.table_sql}")
            manage.optimize_table(
                db_config=db_config,
                data_config={"table_name": table_original},
                name_map=nm,
            )
            optimized += 1

    print(
        f"[finalize] done: indexed={indexed}, optimized={optimized}, skipped={skipped}, "
        f"tables_considered={len(name_maps)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
