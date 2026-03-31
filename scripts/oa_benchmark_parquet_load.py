#!/usr/bin/env python3
"""
Benchmark DB load-only speed from persisted parquet artifacts.

This is intended for operational comparison between:
- direct ingest-first (`json run --mode ingest-fast*`)
- parse-first (`json run --mode parse-parquet*`) followed by a later DB load

The script reads one or more parquet batches per table, creates temporary tables,
loads them via the existing KISTI_DB_Manager dataframe loader, records timings,
and drops the temporary tables by default.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

from KISTI_DB_Manager import manage
from KISTI_DB_Manager.config import coerce_data_config, coerce_db_config
from KISTI_DB_Manager.namemap import NameMap
from KISTI_DB_Manager.report import RunReport


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_env_like(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def _hydrate_db_password(db_config: dict[str, Any], *, dotenv_path: Path | None) -> dict[str, Any]:
    dbc = dict(db_config)
    password = str(dbc.get("password") or "")
    if password and password != "***":
        return dbc

    env = _read_env_like(dotenv_path) if dotenv_path is not None else {}
    user = str(dbc.get("user") or "").strip()
    candidate_keys: list[str] = []
    if user == "root":
        candidate_keys.append("MARIADB_ROOT_PASSWORD")
    candidate_keys.extend(["MARIADB_PASSWORD", "MYSQL_PASSWORD", "MYSQL_ROOT_PASSWORD"])

    for key in candidate_keys:
        value = str(env.get(key) or "").strip()
        if value:
            dbc["password"] = value
            return dbc

    raise RuntimeError(
        "DB password is masked or missing in config.json and could not be restored from dotenv. "
        "Pass --dotenv with a file containing the DB password."
    )


def _default_staging_dir() -> str:
    candidate = Path("/dev/shm")
    if candidate.exists() and candidate.is_dir():
        try:
            probe = candidate / ".kisti_bench_write_probe"
            probe.write_text("", encoding="utf-8")
            probe.unlink(missing_ok=True)
            return str(candidate)
        except Exception:
            pass
    return "/tmp"


def _connect_local_infile(db_config: dict[str, Any], *, fast_load_session: bool, report: RunReport):
    import pymysql
    from KISTI_DB_Manager.pipeline import _apply_fast_load_session_settings

    conn = pymysql.connect(
        host=db_config.get("host"),
        user=db_config.get("user"),
        password=db_config.get("password"),
        database=db_config.get("database"),
        port=int(db_config.get("port") or 3306),
        charset="utf8mb4",
        autocommit=False,
        local_infile=1,
        connect_timeout=5,
    )
    with conn.cursor() as cur:
        cur.execute("SELECT @@local_infile;")
        row = cur.fetchone()
    if row is not None and str(row[0]) in {"0", "OFF", "off", "False", "false"}:
        conn.close()
        raise RuntimeError("Server variable @@local_infile=0 (LOCAL INFILE disabled)")
    if fast_load_session:
        _apply_fast_load_session_settings(conn, report=report, stage="parquet_benchmark.fast_load_session")
    return conn


def _pick_table_dirs(root: Path, selected: list[str], max_tables: int | None) -> list[Path]:
    dirs = sorted([p for p in root.iterdir() if p.is_dir()])
    if selected:
        wanted = set(selected)
        dirs = [p for p in dirs if p.name in wanted]
    if max_tables is not None and max_tables >= 0:
        dirs = dirs[:max_tables]
    return dirs


def _pick_parquet_files(table_dir: Path, max_files_per_table: int, latest_first: bool) -> list[Path]:
    files = sorted(table_dir.glob("*.parquet"))
    if latest_first:
        files = list(reversed(files))
    return files[: max(0, int(max_files_per_table))]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("parquet_root", help="Root parquet artifact directory")
    ap.add_argument("--config", required=True, help="JSON config path containing db_config/data_config")
    ap.add_argument("--report", help="Output JSON report path")
    ap.add_argument("--dotenv", default=".env", help="dotenv-like file used to restore masked DB password")
    ap.add_argument("--table", action="append", default=[], help="Parquet table directory name to benchmark (repeatable)")
    ap.add_argument("--max-tables", type=int, default=4, help="Maximum number of table dirs to benchmark")
    ap.add_argument("--max-files-per-table", type=int, default=1, help="Number of parquet files per table to load")
    ap.add_argument("--limit-rows-per-file", type=int, default=0, help="Optional row cap after reading each parquet file")
    ap.add_argument("--table-prefix", default="bench_oa_parquet_", help="Temporary DB table prefix")
    ap.add_argument("--load-method", choices=["auto", "load_data", "to_sql"], default="load_data")
    ap.add_argument("--staging-writer", choices=["python", "duckdb"], default="duckdb")
    ap.add_argument("--staging-dir", default=None, help="Temp staging directory for LOAD DATA files")
    ap.add_argument("--latest-first", action="store_true", help="Pick newest parquet files first")
    ap.add_argument("--keep-tables", action="store_true", help="Keep benchmark tables instead of dropping them")
    ap.add_argument("--column-type", default="LONGTEXT")
    args = ap.parse_args()

    parquet_root = Path(args.parquet_root).expanduser().resolve()
    cfg = _read_json(Path(args.config).expanduser().resolve())
    db_config = _hydrate_db_password(
        coerce_db_config(cfg.get("db_config") or {}),
        dotenv_path=Path(args.dotenv).expanduser().resolve() if args.dotenv else None,
    )
    data_config = coerce_data_config(cfg.get("data_config") or {}, inplace=False)
    staging_dir = str(args.staging_dir or _default_staging_dir())

    report = RunReport()
    report.set_artifact("parquet_root", str(parquet_root))
    report.set_artifact("config_path", str(Path(args.config).expanduser().resolve()))
    report.set_artifact("load_method", args.load_method)
    report.set_artifact("staging_writer", args.staging_writer)
    report.set_artifact("staging_dir", staging_dir)
    report.set_artifact("latest_first", bool(args.latest_first))
    report.set_artifact("max_tables", int(args.max_tables))
    report.set_artifact("max_files_per_table", int(args.max_files_per_table))
    report.set_artifact("limit_rows_per_file", int(args.limit_rows_per_file))

    table_dirs = _pick_table_dirs(parquet_root, [str(t).strip() for t in args.table if str(t).strip()], args.max_tables)
    if not table_dirs:
        raise SystemExit(f"No parquet table directories selected under {parquet_root}")

    import pandas as pd

    fast_load_state = manage.FastLoadState(enabled=(args.load_method in {"auto", "load_data"}))
    local_infile_conn = None
    created_sql_tables: list[str] = []
    per_table: list[dict[str, Any]] = []

    try:
        if fast_load_state.enabled:
            local_infile_conn = _connect_local_infile(
                db_config,
                fast_load_session=True,
                report=report,
            )

        for table_dir in table_dirs:
            files = _pick_parquet_files(table_dir, args.max_files_per_table, args.latest_first)
            if not files:
                continue

            bench_table_original = f"{args.table_prefix}{table_dir.name}"
            table_summary: dict[str, Any] = {
                "parquet_dir": str(table_dir),
                "table_original": table_dir.name,
                "bench_table_original": bench_table_original,
                "files": [],
            }
            nm: NameMap | None = None
            existing_cols: set[str] | None = None

            for idx, parquet_file in enumerate(files):
                t0_read = time.perf_counter()
                df = pd.read_parquet(parquet_file)
                if args.limit_rows_per_file and int(args.limit_rows_per_file) > 0:
                    df = df.head(int(args.limit_rows_per_file)).copy()
                report.add_time_s("bench.parquet.read", time.perf_counter() - t0_read)
                report.bump("bench_parquet_files_read", 1)
                report.bump("bench_rows_read", int(len(df)))

                if nm is None:
                    nm = NameMap.build(
                        table_name=bench_table_original,
                        columns=list(df.columns),
                        key_sep=str(data_config.get("KEY_SEP") or "__"),
                    )
                    t0_create = time.perf_counter()
                    nm = manage.create_table_from_columns(
                        db_config,
                        table_name=bench_table_original,
                        columns=list(df.columns),
                        name_map=nm,
                        key_sep=str(data_config.get("KEY_SEP") or "__"),
                        column_type=str(args.column_type),
                    )
                    report.add_time_s("bench.db.create", time.perf_counter() - t0_create)
                    report.bump("bench_tables_created", 1)
                    created_sql_tables.append(nm.table_sql)

                t0_load = time.perf_counter()
                nm = manage.fill_table_from_dataframe(
                    df,
                    db_config,
                    table_name=nm.table_sql,
                    name_map=nm,
                    extra_column_name=str(data_config.get("extra_column_name") or "__extra__"),
                    auto_alter_table=False,
                    column_type=str(args.column_type),
                    fallback_on_insert_error=False,
                    report=report,
                    load_method=args.load_method,
                    fast_load_state=fast_load_state,
                    local_infile_conn=local_infile_conn,
                    existing_cols=existing_cols,
                    load_data_staging_writer=args.staging_writer,
                    load_data_staging_dir=staging_dir,
                )
                load_s = time.perf_counter() - t0_load
                if existing_cols is None:
                    existing_cols = set(nm.columns_sql)
                report.bump("bench_files_loaded", 1)
                report.bump("bench_rows_loaded", int(len(df)))
                table_summary["files"].append(
                    {
                        "path": str(parquet_file),
                        "rows": int(len(df)),
                        "load_seconds": round(load_s, 6),
                    }
                )

            if nm is not None:
                table_summary["bench_table_sql"] = nm.table_sql
            per_table.append(table_summary)

        report.set_artifact("per_table", per_table)
        report.finish()

        rows_loaded = int(report.stats.get("bench_rows_loaded", 0) or 0)
        duration_s = float(report.duration_s or 0.0)
        if duration_s > 0:
            report.set_artifact("throughput", {"bench_rows_loaded_per_s": rows_loaded / duration_s})

        out = report.to_dict()
        if args.report:
            Path(args.report).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"report: {args.report}")
        else:
            print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0
    finally:
        if local_infile_conn is not None:
            try:
                local_infile_conn.close()
            except Exception:
                pass
        if not args.keep_tables:
            for sql_table in reversed(created_sql_tables):
                try:
                    manage.drop_table(sql_table, db_config)
                except Exception:
                    pass


if __name__ == "__main__":
    raise SystemExit(main())
