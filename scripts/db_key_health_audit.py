#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pymysql


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def qi(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def read_env_like(path: Path | None) -> dict[str, str]:
    if path is None or not path.exists():
        return {}
    out: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def query_or_error(cur, sql: str, *, sample_limit: int) -> dict[str, Any]:
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        return {"status": "ok", "rows": [list(row) for row in rows[:sample_limit]]}
    except Exception as exc:
        return {"status": "error", "error_type": type(exc).__name__, "error": str(exc)}


def index_exists(cur, *, table: str, index_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND index_name = %s
        LIMIT 1
        """,
        (table, index_name),
    )
    return cur.fetchone() is not None


def ensure_helper_index(cur, *, table: str, key_column: str, index_name: str, prefix_length: int) -> dict[str, Any]:
    t0 = time.perf_counter()
    if index_exists(cur, table=table, index_name=index_name):
        return {"status": "exists", "elapsed_sec": round(time.perf_counter() - t0, 3), "index_name": index_name}
    try:
        cur.execute(
            f"CREATE INDEX {qi(index_name)} ON {qi(table)} ({qi(key_column)}({max(1, int(prefix_length))}))"
        )
        return {"status": "created", "elapsed_sec": round(time.perf_counter() - t0, 3), "index_name": index_name}
    except Exception as exc:
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "index_name": index_name,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def duckdb_query_or_error(con, sql: str, *, sample_limit: int) -> dict[str, Any]:
    t0 = time.perf_counter()
    try:
        rows = con.execute(sql).fetchall()
        return {
            "status": "ok",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "rows": [list(row) for row in rows[:sample_limit]],
        }
    except Exception as exc:
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def run_parquet_checks(args: argparse.Namespace, report: dict[str, Any]) -> None:
    import duckdb

    parquet_root = Path(args.parquet_root).expanduser().resolve()
    if not parquet_root.exists():
        raise FileNotFoundError(f"parquet root not found: {parquet_root}")
    parquet_glob = str(parquet_root / "*.parquet")
    temp_dir = Path(args.duckdb_temp_dir).expanduser().resolve() if args.duckdb_temp_dir else None
    if temp_dir:
        temp_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"PRAGMA threads={max(1, int(args.threads))}")
        con.execute(f"SET memory_limit={json.dumps(str(args.memory_limit))}")
        con.execute("SET preserve_insertion_order=false")
        if temp_dir:
            con.execute(f"SET temp_directory={json.dumps(str(temp_dir))}")
        table_expr = f"read_parquet({json.dumps(parquet_glob)}, union_by_name=true)"
        key = '"' + str(args.key_column).replace('"', '""') + '"'
        prefix_len = max(1, int(args.prefix_length))

        report["parquet_root"] = str(parquet_root)
        report["checks"]["parquet_summary"] = duckdb_query_or_error(
            con,
            f"""
            SELECT
              COUNT(*) AS rows_total,
              COUNT({key}) AS rows_with_key,
              SUM(CASE WHEN {key} IS NULL THEN 1 ELSE 0 END) AS key_null_rows,
              SUM(CASE WHEN {key} = 'NULL' THEN 1 ELSE 0 END) AS key_literal_null_rows,
              SUM(CASE WHEN COALESCE(TRIM({key}), '') = '' THEN 1 ELSE 0 END) AS key_blank_rows,
              SUM(CASE WHEN {key} IS NOT NULL AND NOT regexp_matches({key}, '^https://openalex\\.org/W[0-9]+$') THEN 1 ELSE 0 END)
                AS key_malformed_rows
            FROM {table_expr}
            """,
            sample_limit=int(args.sample_limit),
        )
        report["checks"]["parquet_bad_key_sample"] = duckdb_query_or_error(
            con,
            f"""
            SELECT {key} AS key_value, COUNT(*) AS row_count
            FROM {table_expr}
            WHERE {key} IS NULL
               OR {key} = 'NULL'
               OR COALESCE(TRIM({key}), '') = ''
               OR NOT regexp_matches({key}, '^https://openalex\\.org/W[0-9]+$')
            GROUP BY 1
            LIMIT {int(args.sample_limit)}
            """,
            sample_limit=int(args.sample_limit),
        )
        if args.parquet_duplicate_sample:
            report["checks"]["parquet_duplicate_key_sample"] = duckdb_query_or_error(
                con,
                f"""
                SELECT {key} AS key_value, COUNT(*) AS row_count
                FROM {table_expr}
                GROUP BY 1
                HAVING COUNT(*) > 1
                LIMIT {int(args.sample_limit)}
                """,
                sample_limit=int(args.sample_limit),
            )
        if args.parquet_prefix_collision_sample:
            report["checks"]["parquet_prefix_collision_sample"] = duckdb_query_or_error(
                con,
                f"""
                SELECT LEFT({key}, {prefix_len}) AS key_prefix,
                       COUNT(*) AS row_count,
                       COUNT(DISTINCT {key}) AS distinct_keys
                FROM {table_expr}
                WHERE {key} IS NOT NULL
                GROUP BY 1
                HAVING COUNT(DISTINCT {key}) > 1
                LIMIT {int(args.sample_limit)}
                """,
                sample_limit=int(args.sample_limit),
            )
    finally:
        con.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Bounded key health audit for large MariaDB tables.")
    ap.add_argument("--schema", default="")
    ap.add_argument("--table", default="")
    ap.add_argument("--key-column", default="id")
    ap.add_argument("--host", default=os.environ.get("MARIADB_BIND_IP", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("MARIADB_BIND_PORT", "3306")))
    ap.add_argument("--user", default=os.environ.get("MARIADB_USER", "root"))
    ap.add_argument("--password", default=os.environ.get("MARIADB_PASSWORD", ""))
    ap.add_argument("--dotenv", default=".env")
    ap.add_argument("--max-statement-time", type=int, default=60)
    ap.add_argument("--sample-limit", type=int, default=5)
    ap.add_argument("--duplicate-sample", action="store_true", help="Run a bounded GROUP BY duplicate sample")
    ap.add_argument("--ensure-helper-index", action="store_true", help="Create a non-unique prefix helper index before DB checks")
    ap.add_argument("--helper-index-name", default="", help="Helper index name, defaults to idx_<table>_<key>_audit")
    ap.add_argument("--parquet-root", default="", help="Optional parquet table directory to audit")
    ap.add_argument("--parquet-duplicate-sample", action="store_true", help="Run a deep full-parquet GROUP BY duplicate sample")
    ap.add_argument(
        "--parquet-prefix-collision-sample",
        action="store_true",
        help="Run a deep full-parquet prefix collision sample",
    )
    ap.add_argument("--prefix-length", type=int, default=64)
    ap.add_argument("--duckdb-temp-dir", default="")
    ap.add_argument("--threads", type=int, default=max(1, os.cpu_count() or 1))
    ap.add_argument("--memory-limit", default="64GB")
    ap.add_argument("--out", default="", help="Optional JSON output path")
    args = ap.parse_args()

    if not args.schema and not args.parquet_root:
        raise SystemExit("--schema or --parquet-root is required")
    if args.schema and not args.table:
        raise SystemExit("--table is required when --schema is provided")

    dotenv = read_env_like(Path(args.dotenv).expanduser().resolve() if args.dotenv else None)
    password = str(args.password or "").strip()
    if not password:
        candidate_keys: list[str] = []
        if str(args.user).strip() == "root":
            candidate_keys.append("MARIADB_ROOT_PASSWORD")
        candidate_keys.extend(["MARIADB_PASSWORD", "MYSQL_PASSWORD", "MYSQL_ROOT_PASSWORD"])
        for key in candidate_keys:
            if str(dotenv.get(key) or "").strip():
                password = str(dotenv[key]).strip()
                break
    if not password:
        raise SystemExit("password is required via --password, environment, or --dotenv")

    report: dict[str, Any] = {
        "generated_at": utc_now_iso(),
        "schema": str(args.schema),
        "table": str(args.table),
        "key_column": str(args.key_column),
        "max_statement_time": int(args.max_statement_time),
        "checks": {},
    }

    if args.schema:
        conn = pymysql.connect(
            host=args.host,
            user=args.user,
            password=password,
            port=int(args.port),
            database=str(args.schema),
            charset="utf8mb4",
            autocommit=True,
            read_timeout=max(30, int(args.max_statement_time) + 30),
            write_timeout=max(30, int(args.max_statement_time) + 30),
        )
        try:
            cur = conn.cursor()
            cur.execute(f"SET SESSION max_statement_time={int(args.max_statement_time)}")
            table = qi(args.table)
            key = qi(args.key_column)
            if args.ensure_helper_index:
                helper_name = str(args.helper_index_name or f"idx_{args.table}_{args.key_column}_audit")
                report["checks"]["helper_index"] = ensure_helper_index(
                    cur,
                    table=str(args.table),
                    key_column=str(args.key_column),
                    index_name=helper_name,
                    prefix_length=int(args.prefix_length),
                )
            report["checks"]["indexes"] = query_or_error(cur, f"SHOW INDEX FROM {table}", sample_limit=100)
            report["checks"]["null_sample"] = query_or_error(
                cur,
                f"SELECT {key} FROM {table} WHERE {key} IS NULL LIMIT {int(args.sample_limit)}",
                sample_limit=int(args.sample_limit),
            )
            report["checks"]["literal_null_sample"] = query_or_error(
                cur,
                f"SELECT {key} FROM {table} WHERE {key} = 'NULL' LIMIT {int(args.sample_limit)}",
                sample_limit=int(args.sample_limit),
            )
            if args.duplicate_sample:
                report["checks"]["duplicate_sample"] = query_or_error(
                    cur,
                    (
                        f"SELECT {key}, COUNT(*) AS duplicate_count "
                        f"FROM {table} GROUP BY {key} HAVING COUNT(*) > 1 "
                        f"LIMIT {int(args.sample_limit)}"
                    ),
                    sample_limit=int(args.sample_limit),
                )
        finally:
            conn.close()

    if args.parquet_root:
        run_parquet_checks(args, report)

    if args.out:
        write_json(Path(args.out).expanduser().resolve(), report)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
