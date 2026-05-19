#!/usr/bin/env python3
from __future__ import annotations

import argparse
import decimal
import json
import sys
import time
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable

from KISTI_DB_Manager.config import coerce_db_config
from KISTI_DB_Manager.runstate import atomic_write_json


DEFAULT_KEY_PATTERN = r"^https://openalex\.org/W[0-9]+$"
DEFAULT_CHECKSUM_COLUMNS = (
    "id",
    "doi",
    "title",
    "display_name",
    "publication_year",
    "type",
    "cited_by_count",
    "updated_date",
)
CHECKSUM_NULL_SENTINEL = "__KISTI_SQL_NULL__"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    payload["updated_at"] = _iso_now()
    atomic_write_json(path, payload)


def _read_env_like(path: Path | None) -> dict[str, str]:
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


def _hydrate_db_password(db_config: dict[str, Any], *, dotenv_path: Path | None) -> dict[str, Any]:
    dbc = dict(coerce_db_config(db_config, inplace=False))
    password = str(dbc.get("password") or "")
    if password and password != "***":
        return dbc
    env = _read_env_like(dotenv_path)
    for key in ("MARIADB_ROOT_PASSWORD", "MARIADB_PASSWORD", "MYSQL_PASSWORD", "MYSQL_ROOT_PASSWORD"):
        value = str(env.get(key) or "").strip()
        if value:
            dbc["password"] = value
            return dbc
    raise RuntimeError("Could not restore DB password from dotenv")


def _qi(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def _literal_marker_compare_sql(compare_mode: str) -> Callable[[str], str]:
    mode = str(compare_mode or "utf8mb4_bin").strip().lower()
    if mode == "utf8mb4_bin":
        return lambda column_sql: f"{column_sql} COLLATE utf8mb4_bin"
    if mode == "binary":
        return lambda column_sql: f"BINARY {column_sql}"
    raise ValueError(f"unsupported literal marker compare mode: {compare_mode}")


def _dq(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _duckdb_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _to_int(value: Any) -> int:
    return int(value or 0)


def _jsonable(value: Any) -> Any:
    if isinstance(value, decimal.Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8", errors="replace")
    return value


def _rows_as_lists(rows: list[tuple[Any, ...]], *, limit: int) -> list[list[Any]]:
    return [[_jsonable(value) for value in row] for row in rows[: max(0, int(limit))]]


def _key_health_summary_row(summary: dict[str, Any]) -> list[Any] | None:
    rows = summary.get("rows") if isinstance(summary, dict) else None
    if isinstance(rows, list) and rows and isinstance(rows[0], list):
        return rows[0]
    return None


def _key_health_metrics(summary: dict[str, Any]) -> dict[str, int]:
    row = _key_health_summary_row(summary)
    if row is None or len(row) < 7:
        return {}
    rows_with_key = _to_int(row[1])
    key_null_rows = _to_int(row[2])
    key_literal_null_rows = _to_int(row[3])
    key_blank_rows = _to_int(row[4])
    key_malformed_rows = _to_int(row[5])
    distinct_key_count = _to_int(row[6])
    return {
        "rows_total": _to_int(row[0]),
        "rows_with_key": rows_with_key,
        "key_null_rows": key_null_rows,
        "key_literal_null_rows": key_literal_null_rows,
        "key_blank_rows": key_blank_rows,
        "key_malformed_rows": key_malformed_rows,
        "bad_key_rows": key_null_rows + key_literal_null_rows + key_blank_rows + key_malformed_rows,
        "duplicate_key_rows": max(0, rows_with_key - distinct_key_count),
        "distinct_key_count": distinct_key_count,
    }


def _prune_key_samples_without_rows(check: dict[str, Any]) -> dict[str, Any]:
    metrics = _key_health_metrics(check.get("summary") or {})
    if not metrics:
        return check
    if metrics.get("bad_key_rows") == 0:
        check.pop("bad_key_sample", None)
    if metrics.get("duplicate_key_rows") == 0:
        check.pop("duplicate_key_sample", None)
        check.pop("duplicate_key_file_sample", None)
    return check


def _run_query(cur, sql: str, params: tuple[Any, ...] = (), *, sample_limit: int = 20) -> dict[str, Any]:
    t0 = time.perf_counter()
    try:
        cur.execute(sql, params)
        rows = cur.fetchall()
        return {
            "status": "ok",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "rows": _rows_as_lists(list(rows), limit=sample_limit),
        }
    except Exception as exc:
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def _load_table_specs(run_dir: Path, table_specs_path: Path | None) -> list[dict[str, Any]]:
    path = table_specs_path or (run_dir / "table_specs.json")
    if path.exists():
        payload = _read_json(path)
        specs = payload.get("specs")
        if not isinstance(specs, list):
            raise ValueError(f"Invalid table specs file: {path}")
        out: list[dict[str, Any]] = []
        for item in specs:
            if not isinstance(item, dict):
                continue
            target_table = str(item.get("target_table") or "").strip()
            source_dir = str(item.get("source_dir") or "").strip()
            if target_table and source_dir:
                out.append(dict(item))
        if out:
            return out
    layout_root = run_dir / "serving_parquet_root"
    if not layout_root.exists():
        raise FileNotFoundError(f"table_specs.json and serving_parquet_root are missing under {run_dir}")
    return [
        {"target_table": p.name, "source_table": p.name, "source_dir": str(p)}
        for p in sorted(layout_root.iterdir())
        if p.is_dir()
    ]


def _filter_specs(specs: list[dict[str, Any]], selected_tables: set[str]) -> list[dict[str, Any]]:
    if not selected_tables:
        return specs
    found = {str(spec.get("target_table") or "") for spec in specs}
    missing = sorted(selected_tables - found)
    if missing:
        raise SystemExit("Selected tables not found in specs: " + ", ".join(missing))
    return [spec for spec in specs if str(spec.get("target_table") or "") in selected_tables]


def _spec_signature(specs: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "target_table": str(spec.get("target_table") or ""),
            "source_table": str(spec.get("source_table") or ""),
            "source_dir": str(Path(str(spec.get("source_dir") or "")).expanduser().resolve()),
        }
        for spec in specs
    ]


def _validation_options(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "tables": sorted(str(item).strip() for item in args.table if str(item).strip()),
        "works_table": str(args.works_table),
        "key_column": str(args.key_column),
        "key_pattern": str(args.key_pattern),
        "prefix_length": int(args.prefix_length),
        "sample_limit": int(args.sample_limit),
        "literal_null_marker": str(args.literal_null_marker),
        "literal_null_marker_column_chunk_size": int(args.literal_null_marker_column_chunk_size),
        "db_literal_marker_compare_mode": str(args.literal_null_marker_compare_mode),
        "db_literal_marker_count_mode": str(args.literal_null_marker_count_mode),
        "db_literal_marker_columns": sorted(str(item).strip() for item in args.literal_null_marker_column if str(item).strip()),
        "skip_literal_null_marker_scan": bool(args.skip_literal_null_marker_scan),
        "skip_source_literal_null_marker_scan": bool(args.skip_source_literal_null_marker_scan),
        "skip_parquet_key_health": bool(args.skip_parquet_key_health),
        "skip_db_key_health": bool(args.skip_db_key_health),
        "skip_samples": bool(args.skip_samples),
        "skip_prefix_collision_sample": bool(args.skip_prefix_collision_sample),
        "skip_key_bucket_check": bool(args.skip_key_bucket_check),
        "key_bucket_prefix_length": int(args.key_bucket_prefix_length),
        "skip_orphans": bool(args.skip_orphans),
        "skip_sample_checksum": bool(args.skip_sample_checksum),
        "checksum_tables": sorted(str(item).strip() for item in args.checksum_table if str(item).strip()),
        "checksum_columns": [str(item).strip() for item in args.checksum_column if str(item).strip()],
        "checksum_sample_size": int(args.checksum_sample_size),
        "skip_row_bucket_checksum": bool(args.skip_row_bucket_checksum),
        "row_bucket_checksum_tables": sorted(str(item).strip() for item in args.row_bucket_checksum_table if str(item).strip()),
        "row_bucket_checksum_all_tables": bool(args.row_bucket_checksum_all_tables),
        "row_bucket_checksum_columns": [str(item).strip() for item in args.row_bucket_checksum_column if str(item).strip()],
        "row_bucket_prefix_length": int(args.row_bucket_prefix_length),
    }


def _resume_base_matches(
    previous_report: dict[str, Any],
    *,
    run_dir: Path,
    db_name: str,
    config_path: Path,
    table_specs_path: Path | None,
    spec_signature: list[dict[str, str]],
) -> bool:
    return (
        previous_report.get("run_dir") == str(run_dir)
        and previous_report.get("database") == str(db_name or "")
        and previous_report.get("config") == str(config_path)
        and previous_report.get("table_specs") == (str(table_specs_path) if table_specs_path else "")
        and previous_report.get("source_specs") == spec_signature
    )


def _sample_limit_compatible(previous_options: dict[str, Any], current_options: dict[str, Any]) -> bool:
    return int(previous_options.get("sample_limit") or 0) >= int(current_options.get("sample_limit") or 0)


def _check_resume_compatible(
    kind: str,
    previous_options: dict[str, Any],
    current_options: dict[str, Any],
) -> bool:
    if not previous_options:
        return False
    if kind == "table":
        if current_options.get("skip_literal_null_marker_scan"):
            return True
        compatible = (
            not previous_options.get("skip_literal_null_marker_scan")
            and previous_options.get("literal_null_marker") == current_options.get("literal_null_marker")
            and previous_options.get("db_literal_marker_compare_mode") == current_options.get("db_literal_marker_compare_mode")
        )
        if not compatible:
            return False
        if current_options.get("skip_source_literal_null_marker_scan"):
            return True
        return (
            not previous_options.get("skip_source_literal_null_marker_scan")
            and previous_options.get("key_column") == current_options.get("key_column")
        )
    if kind in {"works_key_parquet", "works_key_db"}:
        if previous_options.get("key_column") != current_options.get("key_column"):
            return False
        if previous_options.get("key_pattern") != current_options.get("key_pattern"):
            return False
        if not current_options.get("skip_samples"):
            if previous_options.get("skip_samples") or not _sample_limit_compatible(previous_options, current_options):
                return False
        if not current_options.get("skip_prefix_collision_sample"):
            if previous_options.get("skip_prefix_collision_sample"):
                return False
            if previous_options.get("prefix_length") != current_options.get("prefix_length"):
                return False
            if not _sample_limit_compatible(previous_options, current_options):
                return False
        return True
    if kind == "key_bucket":
        return (
            previous_options.get("key_column") == current_options.get("key_column")
            and previous_options.get("key_bucket_prefix_length") == current_options.get("key_bucket_prefix_length")
        )
    if kind == "orphan":
        return previous_options.get("key_column") == current_options.get("key_column")
    if kind == "sample_checksum":
        return (
            previous_options.get("key_column") == current_options.get("key_column")
            and previous_options.get("checksum_columns") == current_options.get("checksum_columns")
            and int(previous_options.get("checksum_sample_size") or 0) >= int(current_options.get("checksum_sample_size") or 0)
        )
    if kind == "row_bucket_checksum":
        return (
            previous_options.get("row_bucket_checksum_columns") == current_options.get("row_bucket_checksum_columns")
            and previous_options.get("row_bucket_prefix_length") == current_options.get("row_bucket_prefix_length")
        )
    return False


def _prune_reused_check(kind: str, check: dict[str, Any], current_options: dict[str, Any]) -> dict[str, Any]:
    out = json.loads(json.dumps(check))
    if kind == "table" and current_options.get("skip_literal_null_marker_scan"):
        out.pop("literal_null_marker_scan", None)
        out.pop("source_literal_null_marker_scan", None)
        out.pop("literal_null_marker_comparison", None)
    if kind == "table" and current_options.get("skip_source_literal_null_marker_scan"):
        out.pop("source_literal_null_marker_scan", None)
        out.pop("literal_null_marker_comparison", None)
    if kind in {"works_key_parquet", "works_key_db"}:
        out = _prune_key_samples_without_rows(out)
        if current_options.get("skip_samples"):
            out.pop("bad_key_sample", None)
            out.pop("duplicate_key_sample", None)
            out.pop("duplicate_key_file_sample", None)
        if current_options.get("skip_prefix_collision_sample"):
            out.pop("prefix_collision_sample", None)
    return out


def _reusable_previous_check(
    previous_checks: dict[str, Any],
    *,
    section: str,
    key: str,
    kind: str,
    previous_options: dict[str, Any],
    current_options: dict[str, Any],
) -> dict[str, Any] | None:
    section_checks = previous_checks.get(section)
    if not isinstance(section_checks, dict):
        return None
    check = section_checks.get(key)
    if not isinstance(check, dict):
        return None
    status = check.get("status")
    if status not in {None, "ok", "missing_key_column"}:
        return None
    if not _check_resume_compatible(kind, previous_options, current_options):
        return None
    return _prune_reused_check(kind, check, current_options)


@lru_cache(maxsize=None)
def _parquet_files(source_dir: Path) -> list[Path]:
    return sorted(path for path in source_dir.rglob("*.parquet") if path.is_file())


@lru_cache(maxsize=None)
def _parquet_row_count(source_dir: Path) -> dict[str, Any]:
    import pyarrow.parquet as pq

    t0 = time.perf_counter()
    if not source_dir.exists():
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "source_dir": str(source_dir),
            "file_count": 0,
            "rows": 0,
            "errors": [{"error_type": "FileNotFoundError", "error": f"source_dir not found: {source_dir}"}],
        }
    files = _parquet_files(source_dir)
    rows = 0
    errors: list[dict[str, Any]] = []
    for parquet_file in files:
        try:
            rows += int(pq.ParquetFile(parquet_file).metadata.num_rows)
        except Exception as exc:
            errors.append(
                {
                    "file": str(parquet_file),
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )
    status = "ok" if not errors else "error"
    return {
        "status": status,
        "elapsed_sec": round(time.perf_counter() - t0, 3),
        "source_dir": str(source_dir),
        "file_count": len(files),
        "rows": int(rows),
        "errors": errors,
    }


@lru_cache(maxsize=None)
def _parquet_columns(source_dir: Path) -> list[str]:
    import pyarrow.parquet as pq

    files = _parquet_files(source_dir)
    if not files:
        return []
    columns: list[str] = []
    seen: set[str] = set()
    for parquet_file in files:
        schema = pq.ParquetFile(parquet_file).schema_arrow
        for field in schema:
            column = str(field.name)
            if column not in seen:
                seen.add(column)
                columns.append(column)
    return columns


def _duckdb_query(con, sql: str, *, sample_limit: int) -> dict[str, Any]:
    t0 = time.perf_counter()
    try:
        rows = con.execute(sql).fetchall()
        return {
            "status": "ok",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "rows": _rows_as_lists(list(rows), limit=sample_limit),
        }
    except Exception as exc:
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def _duckdb_works_key_health(
    source_dir: Path,
    *,
    key_column: str,
    key_pattern: str,
    prefix_length: int,
    sample_limit: int,
    threads: int,
    memory_limit: str,
    temp_dir: Path | None,
    include_samples: bool,
    include_prefix_collision_sample: bool,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    import duckdb

    if not source_dir.exists():
        return {
            "status": "error",
            "source_dir": str(source_dir),
            "file_count": 0,
            "summary": {
                "status": "error",
                "error_type": "FileNotFoundError",
                "error": f"source_dir not found: {source_dir}",
            },
        }
    files = _parquet_files(source_dir)
    if not files:
        return {
            "status": "error",
            "source_dir": str(source_dir),
            "file_count": 0,
            "summary": {
                "status": "error",
                "error_type": "FileNotFoundError",
                "error": f"no parquet files under {source_dir}",
            },
        }

    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"PRAGMA threads={max(1, int(threads))}")
        con.execute(f"SET memory_limit={json.dumps(str(memory_limit))}")
        con.execute("SET preserve_insertion_order=false")
        if temp_dir is not None:
            temp_dir.mkdir(parents=True, exist_ok=True)
            con.execute(f"SET temp_directory={json.dumps(str(temp_dir))}")
        read_expr = f"read_parquet({json.dumps([str(path) for path in files])}, union_by_name=true)"
        read_expr_with_filename = f"read_parquet({json.dumps([str(path) for path in files])}, union_by_name=true, filename=true)"
        key = _dq(key_column)
        pattern = _duckdb_string(str(key_pattern))
        summary = _duckdb_query(
            con,
            f"""
            SELECT
              COUNT(*) AS rows_total,
              COUNT({key}) AS rows_with_key,
              SUM(CASE WHEN {key} IS NULL THEN 1 ELSE 0 END) AS key_null_rows,
              SUM(CASE WHEN {key} = 'NULL' THEN 1 ELSE 0 END) AS key_literal_null_rows,
              SUM(CASE WHEN {key} IS NOT NULL AND TRIM({key}) = '' THEN 1 ELSE 0 END) AS key_blank_rows,
              SUM(CASE
                    WHEN {key} IS NOT NULL
                     AND TRIM({key}) <> ''
                     AND {key} <> 'NULL'
                     AND NOT regexp_matches({key}, {pattern})
                    THEN 1 ELSE 0 END) AS key_malformed_rows,
              COUNT(DISTINCT {key}) AS distinct_key_count
            FROM {read_expr}
            """,
            sample_limit=1,
        )
        out: dict[str, Any] = {
            "status": "ok" if summary.get("status") == "ok" else "error",
            "source_dir": str(source_dir),
            "file_count": len(files),
            "summary": summary,
        }
        if progress_callback is not None:
            progress_callback(dict(out))
        metrics = _key_health_metrics(summary)
        if include_samples:
            if not metrics or int(metrics.get("bad_key_rows") or 0) > 0:
                out["bad_key_sample"] = _duckdb_query(
                    con,
                    f"""
                    SELECT {key} AS key_value, COUNT(*) AS row_count
                    FROM {read_expr}
                    WHERE {key} IS NULL
                       OR {key} = 'NULL'
                       OR ({key} IS NOT NULL AND TRIM({key}) = '')
                       OR (
                            {key} IS NOT NULL
                        AND TRIM({key}) <> ''
                        AND {key} <> 'NULL'
                        AND NOT regexp_matches({key}, {pattern})
                       )
                    GROUP BY 1
                    LIMIT {int(sample_limit)}
                    """,
                    sample_limit=sample_limit,
                )
                if progress_callback is not None:
                    progress_callback(dict(out))
            if not metrics or int(metrics.get("duplicate_key_rows") or 0) > 0:
                out["duplicate_key_sample"] = _duckdb_query(
                    con,
                    f"""
                    SELECT {key} AS key_value, COUNT(*) AS row_count
                    FROM {read_expr}
                    WHERE {key} IS NOT NULL
                    GROUP BY 1
                    HAVING COUNT(*) > 1
                    LIMIT {int(sample_limit)}
                    """,
                    sample_limit=sample_limit,
                )
                duplicate_keys = [
                    str(row[0])
                    for row in out["duplicate_key_sample"].get("rows", [])
                    if isinstance(row, list) and row and row[0] is not None
                ]
                if duplicate_keys:
                    duplicate_key_values = ", ".join(_duckdb_string(value) for value in duplicate_keys)
                    out["duplicate_key_file_sample"] = _duckdb_query(
                        con,
                        f"""
                        SELECT key_value,
                               COUNT(*) AS row_count,
                               COUNT(DISTINCT filename) AS file_count,
                               list(DISTINCT filename ORDER BY filename) AS files
                        FROM (
                            SELECT CAST({key} AS VARCHAR) AS key_value, filename
                            FROM {read_expr_with_filename}
                            WHERE {key} IS NOT NULL
                              AND CAST({key} AS VARCHAR) IN ({duplicate_key_values})
                        ) keyed
                        GROUP BY key_value
                        LIMIT {int(sample_limit)}
                        """,
                        sample_limit=sample_limit,
                    )
                if progress_callback is not None:
                    progress_callback(dict(out))
        if include_prefix_collision_sample:
            out["prefix_collision_sample"] = _duckdb_query(
                con,
                f"""
                SELECT LEFT({key}, {max(1, int(prefix_length))}) AS key_prefix,
                       COUNT(*) AS row_count,
                       COUNT(DISTINCT {key}) AS distinct_keys
                FROM {read_expr}
                WHERE {key} IS NOT NULL
                GROUP BY 1
                HAVING COUNT(DISTINCT {key}) > 1
                LIMIT {int(sample_limit)}
                """,
                sample_limit=sample_limit,
            )
            if progress_callback is not None:
                progress_callback(dict(out))
        return out
    finally:
        con.close()


def _db_table_exists(cur, *, table: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
          AND table_name = %s
        LIMIT 1
        """,
        (table,),
    )
    return cur.fetchone() is not None


def _db_column_exists(cur, *, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (table, column),
    )
    return cur.fetchone() is not None


def _db_columns(cur, *, table: str) -> list[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table,),
    )
    return [str(row[0]) for row in cur.fetchall()]


def _db_text_columns(cur, *, table: str) -> list[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND data_type IN (
            'char', 'varchar', 'tinytext', 'text', 'mediumtext', 'longtext',
            'json', 'enum', 'set'
          )
        ORDER BY ordinal_position
        """,
        (table,),
    )
    return [str(row[0]) for row in cur.fetchall()]


def _db_scalar_count(cur, *, table: str) -> dict[str, Any]:
    t0 = time.perf_counter()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {_qi(table)}")
        row = cur.fetchone()
        return {"status": "ok", "elapsed_sec": round(time.perf_counter() - t0, 3), "rows": _to_int(row[0] if row else 0)}
    except Exception as exc:
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def _db_literal_marker_scan(
    cur,
    *,
    table: str,
    marker: str,
    sample_limit: int,
    chunk_size: int = 32,
    compare_mode: str = "utf8mb4_bin",
    columns: list[str] | None = None,
    count_mode: str = "count",
) -> dict[str, Any]:
    t0 = time.perf_counter()
    table_sql = _qi(table)
    try:
        available_columns = _db_text_columns(cur, table=table)
        requested_columns = [str(column) for column in (columns or [])]
        if columns:
            wanted = {str(column) for column in columns}
            columns = [column for column in available_columns if column in wanted]
            missing_columns = sorted(wanted - set(columns))
        else:
            columns = available_columns
            missing_columns = []
        compare = _literal_marker_compare_sql(compare_mode)
        out: dict[str, Any] = {
            "status": "ok",
            "elapsed_sec": 0.0,
            "marker": marker,
            "comparison_mode": compare_mode,
            "count_mode": count_mode,
            "columns_scanned": len(columns),
            "requested_columns": requested_columns,
            "missing_columns": missing_columns,
            "nonzero_columns": {},
            "errors": [],
        }
        if str(count_mode) == "exists":
            for column in columns:
                try:
                    cur.execute(f"SELECT 1 FROM {table_sql} WHERE {compare(_qi(column))} = %s LIMIT 1", (marker,))
                    count = 1 if cur.fetchone() is not None else 0
                except Exception as exc:
                    out["errors"].append(
                        {
                            "columns": [column],
                            "error_type": type(exc).__name__,
                            "error": str(exc),
                        }
                    )
                    continue
                if count:
                    out["nonzero_columns"][column] = count
        else:
            for start in range(0, len(columns), max(1, int(chunk_size))):
                chunk = columns[start : start + max(1, int(chunk_size))]
                select_exprs = [
                    f"SUM(CASE WHEN {compare(_qi(column))} = %s THEN 1 ELSE 0 END) AS {_qi(f'c{idx}')}"
                    for idx, column in enumerate(chunk)
                ]
                try:
                    cur.execute(f"SELECT {', '.join(select_exprs)} FROM {table_sql}", tuple(marker for _ in chunk))
                    row = cur.fetchone() or ()
                except Exception as exc:
                    out["errors"].append(
                        {
                            "columns": chunk,
                            "error_type": type(exc).__name__,
                            "error": str(exc),
                        }
                    )
                    continue
                for idx, column in enumerate(chunk):
                    count = _to_int(row[idx] if idx < len(row) else 0)
                    if count:
                        out["nonzero_columns"][column] = count

        samples: dict[str, list[list[Any]]] = {}
        for column in list(out["nonzero_columns"])[: max(0, int(sample_limit))]:
            id_select = ", `id`" if column != "id" and _db_column_exists(cur, table=table, column="id") else ""
            sample = _run_query(
                cur,
                f"SELECT {_qi(column)} AS marker_value{id_select} "
                f"FROM {table_sql} "
                f"WHERE {compare(_qi(column))} = %s "
                f"LIMIT {int(sample_limit)}",
                (marker,),
                sample_limit=sample_limit,
            )
            if sample.get("status") == "ok":
                samples[column] = list(sample.get("rows") or [])
            else:
                out["errors"].append(
                    {
                        "columns": [column],
                        "error_type": sample.get("error_type"),
                        "error": sample.get("error"),
                    }
                )
        if samples:
            out["samples"] = samples
        if out["errors"]:
            out["status"] = "error"
        out["elapsed_sec"] = round(time.perf_counter() - t0, 3)
        return out
    except Exception as exc:
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "marker": marker,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def _duckdb_literal_marker_scan(
    source_dir: Path,
    *,
    columns: list[str],
    marker: str,
    key_column: str,
    sample_limit: int,
    threads: int,
    memory_limit: str,
    temp_dir: Path | None,
) -> dict[str, Any]:
    import duckdb

    t0 = time.perf_counter()
    files = _parquet_files(source_dir)
    if not files:
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "source_dir": str(source_dir),
            "file_count": 0,
            "marker": marker,
            "columns_scanned": 0,
            "nonzero_columns": {},
            "missing_columns": [str(column) for column in columns],
            "errors": [{"error_type": "FileNotFoundError", "error": f"no parquet files under {source_dir}"}],
        }

    parquet_columns = set(_parquet_columns(source_dir))
    selected_columns = [str(column) for column in columns if str(column) in parquet_columns]
    missing_columns = [str(column) for column in columns if str(column) not in parquet_columns]
    out: dict[str, Any] = {
        "status": "ok",
        "elapsed_sec": 0.0,
        "source_dir": str(source_dir),
        "file_count": len(files),
        "marker": marker,
        "columns_scanned": len(selected_columns),
        "requested_columns": [str(column) for column in columns],
        "missing_columns": missing_columns,
        "nonzero_columns": {},
        "errors": [],
    }
    if not selected_columns:
        out["elapsed_sec"] = round(time.perf_counter() - t0, 3)
        return out

    def _read_expr(*, filename: bool = False) -> str:
        filename_sql = ", filename=true" if filename else ""
        return f"read_parquet({json.dumps([str(path) for path in files])}, union_by_name=true{filename_sql})"

    marker_sql = _duckdb_string(str(marker))
    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"PRAGMA threads={max(1, int(threads))}")
        con.execute(f"SET memory_limit={json.dumps(str(memory_limit))}")
        con.execute("SET preserve_insertion_order=false")
        if temp_dir is not None:
            temp_dir.mkdir(parents=True, exist_ok=True)
            con.execute(f"SET temp_directory={json.dumps(str(temp_dir))}")

        select_exprs = [
            f"SUM(CASE WHEN {_dq(column)} = {marker_sql} THEN 1 ELSE 0 END) AS {_dq(f'c{idx}')}"
            for idx, column in enumerate(selected_columns)
        ]
        try:
            row = con.execute(f"SELECT {', '.join(select_exprs)} FROM {_read_expr()}").fetchone() or ()
        except Exception as exc:
            out["status"] = "error"
            out["errors"].append(
                {
                    "columns": selected_columns,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )
            out["elapsed_sec"] = round(time.perf_counter() - t0, 3)
            return out

        for idx, column in enumerate(selected_columns):
            count = _to_int(row[idx] if idx < len(row) else 0)
            if count:
                out["nonzero_columns"][column] = count

        samples: dict[str, list[list[Any]]] = {}
        has_key = str(key_column) in parquet_columns
        for column in list(out["nonzero_columns"])[: max(0, int(sample_limit))]:
            key_select = f", {_dq(key_column)} AS key_value" if has_key and column != key_column else ""
            sample_sql = (
                f"SELECT {_dq(column)} AS marker_value{key_select}, filename "
                f"FROM {_read_expr(filename=True)} "
                f"WHERE {_dq(column)} = {marker_sql} "
                f"LIMIT {int(sample_limit)}"
            )
            sample = _duckdb_query(con, sample_sql, sample_limit=sample_limit)
            if sample.get("status") == "ok":
                samples[column] = list(sample.get("rows") or [])
            else:
                out["errors"].append(
                    {
                        "columns": [column],
                        "error_type": sample.get("error_type"),
                        "error": sample.get("error"),
                    }
                )
        if samples:
            out["samples"] = samples
        if out["errors"]:
            out["status"] = "error"
        out["elapsed_sec"] = round(time.perf_counter() - t0, 3)
        return out
    except Exception as exc:
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "source_dir": str(source_dir),
            "file_count": len(files),
            "marker": marker,
            "columns_scanned": len(selected_columns),
            "requested_columns": [str(column) for column in columns],
            "missing_columns": missing_columns,
            "nonzero_columns": {},
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
    finally:
        try:
            con.close()
        except Exception:
            pass


def _literal_marker_comparison(
    *,
    db_scan: dict[str, Any],
    source_scan: dict[str, Any] | None,
) -> dict[str, Any]:
    marker = db_scan.get("marker")
    db_counts = db_scan.get("nonzero_columns") if isinstance(db_scan.get("nonzero_columns"), dict) else {}
    source_counts = (
        source_scan.get("nonzero_columns")
        if isinstance(source_scan, dict) and isinstance(source_scan.get("nonzero_columns"), dict)
        else {}
    )
    missing_columns = set()
    if isinstance(source_scan, dict) and isinstance(source_scan.get("missing_columns"), list):
        missing_columns = {str(column) for column in source_scan.get("missing_columns") or []}

    columns: dict[str, dict[str, Any]] = {}
    status = "ok"
    for column, db_count_raw in sorted(db_counts.items()):
        db_count = _to_int(db_count_raw)
        source_count = _to_int(source_counts.get(column) or 0)
        if column in missing_columns:
            column_status = "source_missing_column"
        elif not isinstance(source_scan, dict) or source_scan.get("status") != "ok":
            column_status = "source_scan_unavailable"
        elif db_count == source_count:
            column_status = "expected_source_literal"
        elif db_count > source_count:
            column_status = "db_excess_literal_marker"
        else:
            column_status = "db_missing_source_literal"

        if column_status != "expected_source_literal":
            status = "error"
        columns[str(column)] = {
            "status": column_status,
            "marker": marker,
            "db_count": db_count,
            "source_count": source_count,
            "match": column_status == "expected_source_literal",
        }

    return {
        "status": status,
        "marker": marker,
        "columns": columns,
    }


def _needs_source_literal_marker_scan(check: dict[str, Any], *, skip_source_scan: bool) -> bool:
    if bool(skip_source_scan):
        return False
    marker_scan = check.get("literal_null_marker_scan")
    if not isinstance(marker_scan, dict):
        return False
    nonzero = marker_scan.get("nonzero_columns")
    if not isinstance(nonzero, dict) or not any(_to_int(value) for value in nonzero.values()):
        return False
    comparison = check.get("literal_null_marker_comparison")
    return not isinstance(comparison, dict)


def _pick_checksum_columns(
    *,
    requested_columns: list[str],
    parquet_columns: list[str],
    db_columns: list[str],
) -> tuple[list[str], list[str]]:
    parquet_set = set(parquet_columns)
    db_set = set(db_columns)
    if requested_columns:
        selected = [column for column in requested_columns if column in parquet_set and column in db_set]
        missing = [column for column in requested_columns if column not in parquet_set or column not in db_set]
        return selected, missing
    selected = [column for column in DEFAULT_CHECKSUM_COLUMNS if column in parquet_set and column in db_set]
    return selected, []


def _checksum_expr_duckdb(columns: list[str]) -> str:
    parts = [f"md5(coalesce(CAST({_dq(column)} AS VARCHAR), {_duckdb_string(CHECKSUM_NULL_SENTINEL)}))" for column in columns]
    if not parts:
        return f"md5({_duckdb_string('')})"
    return "md5(" + " || '|' || ".join(parts) + ")"


def _checksum_expr_mysql(columns: list[str]) -> str:
    parts = [f"MD5(IFNULL(CAST({_qi(column)} AS CHAR), %s))" for column in columns]
    if not parts:
        return "MD5('')"
    return "MD5(CONCAT_WS('|', " + ", ".join(parts) + "))"


def _bucket_prefix_length(value: int) -> int:
    return max(1, min(32, int(value)))


def _duckdb_hex_sum_expr(hash_expr: str, *, start: int) -> str:
    return f"CAST(SUM(CAST('0x' || SUBSTR({hash_expr}, {int(start)}, 15) AS UBIGINT)) AS VARCHAR)"


def _mysql_hex_sum_expr(hash_expr: str, *, start: int) -> str:
    return f"CAST(SUM(CAST(CONV(SUBSTRING({hash_expr}, {int(start)}, 15), 16, 10) AS DECIMAL(38, 0))) AS CHAR)"


def _normalize_bucket_rows(rows: list[list[Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if len(row) < 4:
            continue
        out[str(row[0])] = {
            "row_count": _to_int(row[1]),
            "hash_sum_1": str(row[2] or "0"),
            "hash_sum_2": str(row[3] or "0"),
        }
    return out


def _compare_bucket_rows(
    parquet_rows: list[list[Any]],
    db_rows: list[list[Any]],
    *,
    sample_limit: int,
) -> dict[str, Any]:
    parquet = _normalize_bucket_rows(parquet_rows)
    db = _normalize_bucket_rows(db_rows)
    buckets = sorted(set(parquet) | set(db))
    mismatches: list[dict[str, Any]] = []
    for bucket in buckets:
        parquet_value = parquet.get(bucket)
        db_value = db.get(bucket)
        if parquet_value != db_value:
            mismatches.append({"bucket": bucket, "parquet": parquet_value, "db": db_value})
        if len(mismatches) >= max(0, int(sample_limit)):
            break
    return {
        "match": not mismatches and len(parquet) == len(db),
        "parquet_bucket_count": len(parquet),
        "db_bucket_count": len(db),
        "mismatch_count_sampled": len(mismatches),
        "mismatches": mismatches,
    }


def _duckdb_key_bucket_summary(
    source_dir: Path,
    *,
    key_column: str,
    bucket_prefix_length: int,
    threads: int,
    memory_limit: str,
    temp_dir: Path | None,
) -> dict[str, Any]:
    import duckdb

    t0 = time.perf_counter()
    files = _parquet_files(source_dir)
    if not files:
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "error_type": "FileNotFoundError",
            "error": f"no parquet files under {source_dir}",
            "rows": [],
        }

    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"PRAGMA threads={max(1, int(threads))}")
        con.execute(f"SET memory_limit={json.dumps(str(memory_limit))}")
        con.execute("SET preserve_insertion_order=false")
        if temp_dir is not None:
            temp_dir.mkdir(parents=True, exist_ok=True)
            con.execute(f"SET temp_directory={json.dumps(str(temp_dir))}")
        read_expr = f"read_parquet({json.dumps([str(path) for path in files])}, union_by_name=true)"
        key = _dq(key_column)
        key_hash = f"MD5(CAST({key} AS VARCHAR))"
        prefix_len = _bucket_prefix_length(bucket_prefix_length)
        rows = con.execute(
            f"""
            SELECT SUBSTR({key_hash}, 1, {prefix_len}) AS bucket,
                   COUNT(*) AS row_count,
                   {_duckdb_hex_sum_expr(key_hash, start=1)} AS hash_sum_1,
                   {_duckdb_hex_sum_expr(key_hash, start=17)} AS hash_sum_2
            FROM {read_expr}
            WHERE {key} IS NOT NULL
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchall()
        return {
            "status": "ok",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "rows": _rows_as_lists(list(rows), limit=len(rows)),
        }
    except Exception as exc:
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "error_type": type(exc).__name__,
            "error": str(exc),
            "rows": [],
        }
    finally:
        con.close()


def _db_key_bucket_summary(
    cur,
    *,
    table: str,
    key_column: str,
    bucket_prefix_length: int,
) -> dict[str, Any]:
    t0 = time.perf_counter()
    key = _qi(key_column)
    key_hash = f"MD5(CAST({key} AS CHAR))"
    prefix_len = _bucket_prefix_length(bucket_prefix_length)
    try:
        cur.execute(
            f"""
            SELECT SUBSTRING({key_hash}, 1, {prefix_len}) AS bucket,
                   COUNT(*) AS row_count,
                   {_mysql_hex_sum_expr(key_hash, start=1)} AS hash_sum_1,
                   {_mysql_hex_sum_expr(key_hash, start=17)} AS hash_sum_2
            FROM {_qi(table)}
            WHERE {key} IS NOT NULL
            GROUP BY 1
            ORDER BY 1
            """
        )
        rows = list(cur.fetchall())
        return {
            "status": "ok",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "rows": _rows_as_lists(rows, limit=len(rows)),
        }
    except Exception as exc:
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "error_type": type(exc).__name__,
            "error": str(exc),
            "rows": [],
        }


def _key_bucket_check(
    cur,
    *,
    table: str,
    source_dir: Path,
    key_column: str,
    bucket_prefix_length: int,
    sample_limit: int,
    threads: int,
    memory_limit: str,
    temp_dir: Path | None,
) -> dict[str, Any]:
    t0 = time.perf_counter()
    try:
        parquet_cols = _parquet_columns(source_dir)
        db_cols = _db_columns(cur, table=table)
        out: dict[str, Any] = {
            "status": "running",
            "source_dir": str(source_dir),
            "key_column": key_column,
            "bucket_prefix_length": _bucket_prefix_length(bucket_prefix_length),
        }
        if key_column not in parquet_cols or key_column not in db_cols:
            out["status"] = "error"
            out["error"] = f"key column {key_column!r} must exist in both Parquet and DB"
            out["elapsed_sec"] = round(time.perf_counter() - t0, 3)
            return out
        parquet = _duckdb_key_bucket_summary(
            source_dir,
            key_column=key_column,
            bucket_prefix_length=int(bucket_prefix_length),
            threads=int(threads),
            memory_limit=str(memory_limit),
            temp_dir=temp_dir,
        )
        db = _db_key_bucket_summary(
            cur,
            table=table,
            key_column=key_column,
            bucket_prefix_length=int(bucket_prefix_length),
        )
        out["parquet"] = parquet
        out["db"] = db
        if parquet.get("status") != "ok" or db.get("status") != "ok":
            out["status"] = "error"
        else:
            compare = _compare_bucket_rows(
                list(parquet.get("rows") or []),
                list(db.get("rows") or []),
                sample_limit=int(sample_limit),
            )
            out["comparison"] = compare
            out["status"] = "ok"
            out["match"] = bool(compare.get("match"))
        out["elapsed_sec"] = round(time.perf_counter() - t0, 3)
        return out
    except Exception as exc:
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "source_dir": str(source_dir),
            "key_column": key_column,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def _duckdb_row_bucket_checksum_summary(
    source_dir: Path,
    *,
    columns: list[str],
    bucket_prefix_length: int,
    threads: int,
    memory_limit: str,
    temp_dir: Path | None,
) -> dict[str, Any]:
    import duckdb

    t0 = time.perf_counter()
    files = _parquet_files(source_dir)
    if not files:
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "error_type": "FileNotFoundError",
            "error": f"no parquet files under {source_dir}",
            "rows": [],
        }

    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"PRAGMA threads={max(1, int(threads))}")
        con.execute(f"SET memory_limit={json.dumps(str(memory_limit))}")
        con.execute("SET preserve_insertion_order=false")
        if temp_dir is not None:
            temp_dir.mkdir(parents=True, exist_ok=True)
            con.execute(f"SET temp_directory={json.dumps(str(temp_dir))}")
        read_expr = f"read_parquet({json.dumps([str(path) for path in files])}, union_by_name=true)"
        row_hash = _checksum_expr_duckdb(columns)
        prefix_len = _bucket_prefix_length(bucket_prefix_length)
        rows = con.execute(
            f"""
            WITH row_hashes AS (
                SELECT {row_hash} AS row_hash
                FROM {read_expr}
            )
            SELECT SUBSTR(row_hash, 1, {prefix_len}) AS bucket,
                   COUNT(*) AS row_count,
                   {_duckdb_hex_sum_expr('row_hash', start=1)} AS hash_sum_1,
                   {_duckdb_hex_sum_expr('row_hash', start=17)} AS hash_sum_2
            FROM row_hashes
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchall()
        return {
            "status": "ok",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "rows": _rows_as_lists(list(rows), limit=len(rows)),
        }
    except Exception as exc:
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "error_type": type(exc).__name__,
            "error": str(exc),
            "rows": [],
        }
    finally:
        con.close()


def _db_row_bucket_checksum_summary(
    cur,
    *,
    table: str,
    columns: list[str],
    bucket_prefix_length: int,
) -> dict[str, Any]:
    t0 = time.perf_counter()
    row_hash = _checksum_expr_mysql(columns)
    prefix_len = _bucket_prefix_length(bucket_prefix_length)
    params = tuple(CHECKSUM_NULL_SENTINEL for _ in columns)
    try:
        cur.execute(
            f"""
            SELECT SUBSTRING(row_hash, 1, {prefix_len}) AS bucket,
                   COUNT(*) AS row_count,
                   {_mysql_hex_sum_expr('row_hash', start=1)} AS hash_sum_1,
                   {_mysql_hex_sum_expr('row_hash', start=17)} AS hash_sum_2
            FROM (
                SELECT {row_hash} AS row_hash
                FROM {_qi(table)}
            ) row_hashes
            GROUP BY 1
            ORDER BY 1
            """,
            params,
        )
        rows = list(cur.fetchall())
        return {
            "status": "ok",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "rows": _rows_as_lists(rows, limit=len(rows)),
        }
    except Exception as exc:
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "error_type": type(exc).__name__,
            "error": str(exc),
            "rows": [],
        }


def _row_bucket_checksum_check(
    cur,
    *,
    table: str,
    source_dir: Path,
    requested_columns: list[str],
    bucket_prefix_length: int,
    sample_limit: int,
    threads: int,
    memory_limit: str,
    temp_dir: Path | None,
) -> dict[str, Any]:
    t0 = time.perf_counter()
    try:
        parquet_cols = _parquet_columns(source_dir)
        db_cols = _db_columns(cur, table=table)
        if requested_columns:
            columns, missing_requested = _pick_checksum_columns(
                requested_columns=requested_columns,
                parquet_columns=parquet_cols,
                db_columns=db_cols,
            )
        else:
            db_set = set(db_cols)
            columns = [column for column in parquet_cols if column in db_set]
            missing_requested = []
        out: dict[str, Any] = {
            "status": "running",
            "source_dir": str(source_dir),
            "columns": columns,
            "missing_requested_columns": missing_requested,
            "bucket_prefix_length": _bucket_prefix_length(bucket_prefix_length),
        }
        if not columns:
            out["status"] = "error"
            out["error"] = "no common row-bucket checksum columns selected"
            out["elapsed_sec"] = round(time.perf_counter() - t0, 3)
            return out
        parquet = _duckdb_row_bucket_checksum_summary(
            source_dir,
            columns=columns,
            bucket_prefix_length=int(bucket_prefix_length),
            threads=int(threads),
            memory_limit=str(memory_limit),
            temp_dir=temp_dir,
        )
        db = _db_row_bucket_checksum_summary(
            cur,
            table=table,
            columns=columns,
            bucket_prefix_length=int(bucket_prefix_length),
        )
        out["parquet"] = parquet
        out["db"] = db
        if parquet.get("status") != "ok" or db.get("status") != "ok":
            out["status"] = "error"
        else:
            compare = _compare_bucket_rows(
                list(parquet.get("rows") or []),
                list(db.get("rows") or []),
                sample_limit=int(sample_limit),
            )
            out["comparison"] = compare
            out["status"] = "ok"
            out["match"] = bool(compare.get("match"))
        out["elapsed_sec"] = round(time.perf_counter() - t0, 3)
        return out
    except Exception as exc:
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "source_dir": str(source_dir),
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def _sample_checksum_duckdb(
    source_dir: Path,
    *,
    key_column: str,
    columns: list[str],
    sample_size: int,
    threads: int,
    memory_limit: str,
    temp_dir: Path | None,
) -> dict[str, Any]:
    import duckdb

    t0 = time.perf_counter()
    files = _parquet_files(source_dir)
    if not files:
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "error_type": "FileNotFoundError",
            "error": f"no parquet files under {source_dir}",
            "rows": [],
        }

    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"PRAGMA threads={max(1, int(threads))}")
        con.execute(f"SET memory_limit={json.dumps(str(memory_limit))}")
        con.execute("SET preserve_insertion_order=false")
        if temp_dir is not None:
            temp_dir.mkdir(parents=True, exist_ok=True)
            con.execute(f"SET temp_directory={json.dumps(str(temp_dir))}")
        read_expr = f"read_parquet({json.dumps([str(path) for path in files])}, union_by_name=true)"
        key = _dq(key_column)
        checksum_expr = _checksum_expr_duckdb(columns)
        rows = con.execute(
            f"""
            SELECT CAST({key} AS VARCHAR) AS key_value,
                   {checksum_expr} AS checksum
            FROM {read_expr}
            WHERE {key} IS NOT NULL
            ORDER BY {key}
            LIMIT {max(0, int(sample_size))}
            """
        ).fetchall()
        return {
            "status": "ok",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "rows": _rows_as_lists(list(rows), limit=sample_size),
        }
    except Exception as exc:
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "error_type": type(exc).__name__,
            "error": str(exc),
            "rows": [],
        }
    finally:
        con.close()


def _sample_checksum_mysql(
    cur,
    *,
    table: str,
    key_column: str,
    columns: list[str],
    sample_size: int,
) -> dict[str, Any]:
    t0 = time.perf_counter()
    key = _qi(key_column)
    checksum_expr = _checksum_expr_mysql(columns)
    try:
        cur.execute(
            f"""
            SELECT CAST({key} AS CHAR) AS key_value,
                   {checksum_expr} AS checksum
            FROM {_qi(table)}
            WHERE {key} IS NOT NULL
            ORDER BY {key}
            LIMIT {max(0, int(sample_size))}
            """,
            tuple(CHECKSUM_NULL_SENTINEL for _ in columns),
        )
        return {
            "status": "ok",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "rows": _rows_as_lists(list(cur.fetchall()), limit=sample_size),
        }
    except Exception as exc:
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "error_type": type(exc).__name__,
            "error": str(exc),
            "rows": [],
        }


def _compare_checksum_rows(
    parquet_rows: list[list[Any]],
    db_rows: list[list[Any]],
    *,
    sample_limit: int,
) -> dict[str, Any]:
    mismatches: list[dict[str, Any]] = []
    max_len = max(len(parquet_rows), len(db_rows))
    for idx in range(max_len):
        p_row = parquet_rows[idx] if idx < len(parquet_rows) else None
        d_row = db_rows[idx] if idx < len(db_rows) else None
        if p_row != d_row:
            mismatches.append({"index": idx, "parquet": p_row, "db": d_row})
        if len(mismatches) >= max(0, int(sample_limit)):
            break
    return {
        "match": not mismatches and len(parquet_rows) == len(db_rows),
        "parquet_sample_rows": len(parquet_rows),
        "db_sample_rows": len(db_rows),
        "mismatch_count_sampled": len(mismatches),
        "mismatches": mismatches,
    }


def _sample_checksum_check(
    cur,
    *,
    table: str,
    source_dir: Path,
    key_column: str,
    requested_columns: list[str],
    sample_size: int,
    sample_limit: int,
    threads: int,
    memory_limit: str,
    temp_dir: Path | None,
) -> dict[str, Any]:
    t0 = time.perf_counter()
    try:
        parquet_cols = _parquet_columns(source_dir)
        db_cols = _db_columns(cur, table=table)
        columns, missing_requested = _pick_checksum_columns(
            requested_columns=requested_columns,
            parquet_columns=parquet_cols,
            db_columns=db_cols,
        )
        out: dict[str, Any] = {
            "status": "running",
            "source_dir": str(source_dir),
            "key_column": key_column,
            "columns": columns,
            "missing_requested_columns": missing_requested,
            "sample_size": int(sample_size),
        }
        if key_column not in parquet_cols or key_column not in db_cols:
            out["status"] = "error"
            out["error"] = f"key column {key_column!r} must exist in both Parquet and DB"
            out["elapsed_sec"] = round(time.perf_counter() - t0, 3)
            return out
        if not columns:
            out["status"] = "error"
            out["error"] = "no common checksum columns selected"
            out["elapsed_sec"] = round(time.perf_counter() - t0, 3)
            return out
        parquet = _sample_checksum_duckdb(
            source_dir,
            key_column=key_column,
            columns=columns,
            sample_size=int(sample_size),
            threads=int(threads),
            memory_limit=str(memory_limit),
            temp_dir=temp_dir,
        )
        db = _sample_checksum_mysql(
            cur,
            table=table,
            key_column=key_column,
            columns=columns,
            sample_size=int(sample_size),
        )
        out["parquet"] = parquet
        out["db"] = db
        if parquet.get("status") != "ok" or db.get("status") != "ok":
            out["status"] = "error"
        else:
            compare = _compare_checksum_rows(
                list(parquet.get("rows") or []),
                list(db.get("rows") or []),
                sample_limit=int(sample_limit),
            )
            out["comparison"] = compare
            out["status"] = "ok"
            out["match"] = bool(compare.get("match"))
        out["elapsed_sec"] = round(time.perf_counter() - t0, 3)
        return out
    except Exception as exc:
        return {
            "status": "error",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
            "source_dir": str(source_dir),
            "key_column": key_column,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def _db_works_key_health(
    cur,
    *,
    table: str,
    key_column: str,
    key_pattern: str,
    prefix_length: int,
    sample_limit: int,
    include_samples: bool,
    include_prefix_collision_sample: bool,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    key = _qi(key_column)
    table_sql = _qi(table)
    summary = _run_query(
        cur,
        f"""
        SELECT
          COUNT(*) AS rows_total,
          COUNT({key}) AS rows_with_key,
          SUM(CASE WHEN {key} IS NULL THEN 1 ELSE 0 END) AS key_null_rows,
          SUM(CASE WHEN {key} = 'NULL' THEN 1 ELSE 0 END) AS key_literal_null_rows,
          SUM(CASE WHEN {key} IS NOT NULL AND TRIM({key}) = '' THEN 1 ELSE 0 END) AS key_blank_rows,
          SUM(CASE
                WHEN {key} IS NOT NULL
                 AND TRIM({key}) <> ''
                 AND {key} <> 'NULL'
                 AND {key} NOT REGEXP %s
                THEN 1 ELSE 0 END) AS key_malformed_rows,
          COUNT(DISTINCT {key}) AS distinct_key_count
        FROM {table_sql}
        """,
        (key_pattern,),
        sample_limit=1,
    )
    out: dict[str, Any] = {"status": "ok" if summary.get("status") == "ok" else "error", "summary": summary}
    if progress_callback is not None:
        progress_callback(dict(out))
    metrics = _key_health_metrics(summary)
    if include_samples:
        if not metrics or int(metrics.get("bad_key_rows") or 0) > 0:
            out["bad_key_sample"] = _run_query(
                cur,
                f"""
                SELECT {key} AS key_value, COUNT(*) AS row_count
                FROM {table_sql}
                WHERE {key} IS NULL
                   OR {key} = 'NULL'
                   OR ({key} IS NOT NULL AND TRIM({key}) = '')
                   OR (
                        {key} IS NOT NULL
                    AND TRIM({key}) <> ''
                    AND {key} <> 'NULL'
                    AND {key} NOT REGEXP %s
                   )
                GROUP BY 1
                LIMIT {int(sample_limit)}
                """,
                (key_pattern,),
                sample_limit=sample_limit,
            )
            if progress_callback is not None:
                progress_callback(dict(out))
        if not metrics or int(metrics.get("duplicate_key_rows") or 0) > 0:
            out["duplicate_key_sample"] = _run_query(
                cur,
                f"""
                SELECT {key} AS key_value, COUNT(*) AS row_count
                FROM {table_sql}
                WHERE {key} IS NOT NULL
                GROUP BY 1
                HAVING COUNT(*) > 1
                LIMIT {int(sample_limit)}
                """,
                sample_limit=sample_limit,
            )
            if progress_callback is not None:
                progress_callback(dict(out))
    if include_prefix_collision_sample:
        out["prefix_collision_sample"] = _run_query(
            cur,
            f"""
            SELECT LEFT({key}, {max(1, int(prefix_length))}) AS key_prefix,
                   COUNT(*) AS row_count,
                   COUNT(DISTINCT {key}) AS distinct_keys
            FROM {table_sql}
            WHERE {key} IS NOT NULL
            GROUP BY 1
            HAVING COUNT(DISTINCT {key}) > 1
            LIMIT {int(sample_limit)}
            """,
            sample_limit=sample_limit,
        )
        if progress_callback is not None:
            progress_callback(dict(out))
    return out


def _db_child_key_and_orphan_check(cur, *, table: str, parent_table: str, key_column: str) -> dict[str, Any]:
    key = _qi(key_column)
    child = _qi(table)
    parent = _qi(parent_table)
    out: dict[str, Any] = {}
    out["child_key_summary"] = _run_query(
        cur,
        f"""
        SELECT
          COUNT(*) AS rows_total,
          SUM(CASE WHEN {key} IS NULL THEN 1 ELSE 0 END) AS key_null_rows,
          SUM(CASE WHEN {key} = 'NULL' THEN 1 ELSE 0 END) AS key_literal_null_rows,
          SUM(CASE WHEN {key} IS NOT NULL AND TRIM({key}) = '' THEN 1 ELSE 0 END) AS key_blank_rows
        FROM {child}
        """,
        sample_limit=1,
    )
    out["orphans"] = _run_query(
        cur,
        f"""
        SELECT COUNT(*) AS orphan_rows
        FROM {child} c
        LEFT JOIN {parent} p ON p.{key} = c.{key}
        WHERE c.{key} IS NOT NULL
          AND TRIM(c.{key}) <> ''
          AND p.{key} IS NULL
        """,
        sample_limit=1,
    )
    return out


def _first_summary_row(section: dict[str, Any]) -> list[Any] | None:
    rows = section.get("rows")
    if isinstance(rows, list) and rows and isinstance(rows[0], list):
        return rows[0]
    return None


def _collect_issues(report: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    table_checks = ((report.get("checks") or {}).get("tables") or {})
    for table_name, check in table_checks.items():
        if not isinstance(check, dict):
            continue
        if check.get("status") != "ok":
            issues.append({"severity": "error", "table": table_name, "check": "table_status", "message": str(check.get("status"))})
            continue
        if int(check.get("parquet_file_count") or 0) <= 0:
            issues.append({"severity": "error", "table": table_name, "check": "parquet_file_count", "count": 0})
        if check.get("row_count_match") is False:
            issues.append(
                {
                    "severity": "error",
                    "table": table_name,
                    "check": "row_count_match",
                    "parquet_rows": check.get("parquet_rows"),
                    "db_rows": check.get("db_rows"),
                }
            )
        marker_scan = check.get("literal_null_marker_scan")
        if isinstance(marker_scan, dict):
            if marker_scan.get("status") != "ok":
                issues.append(
                    {
                        "severity": "error",
                        "table": table_name,
                        "check": "literal_null_marker_scan_status",
                        "message": str(marker_scan.get("error") or marker_scan.get("status")),
                    }
                )
            source_marker_scan = check.get("source_literal_null_marker_scan")
            if isinstance(source_marker_scan, dict) and source_marker_scan.get("status") != "ok":
                issues.append(
                    {
                        "severity": "error",
                        "table": table_name,
                        "check": "source_literal_null_marker_scan_status",
                        "message": str(source_marker_scan.get("error") or source_marker_scan.get("status")),
                    }
                )
            marker_comparison = check.get("literal_null_marker_comparison")
            comparison_columns = (
                marker_comparison.get("columns")
                if isinstance(marker_comparison, dict) and isinstance(marker_comparison.get("columns"), dict)
                else {}
            )
            nonzero = marker_scan.get("nonzero_columns")
            if isinstance(nonzero, dict):
                for column, count in sorted(nonzero.items()):
                    if _to_int(count):
                        comparison = comparison_columns.get(str(column)) if isinstance(comparison_columns, dict) else None
                        if isinstance(comparison, dict) and comparison.get("status") == "expected_source_literal":
                            continue
                        issues.append(
                            {
                                "severity": "error",
                                "table": table_name,
                                "column": str(column),
                                "check": (
                                    str(comparison.get("status"))
                                    if isinstance(comparison, dict) and comparison.get("status")
                                    else "literal_null_marker_rows"
                                ),
                                "count": _to_int(count),
                                "source_count": comparison.get("source_count") if isinstance(comparison, dict) else None,
                                "marker": marker_scan.get("marker"),
                            }
                        )

    key_checks = ((report.get("checks") or {}).get("works_key") or {})
    for origin in ("parquet", "db"):
        block = key_checks.get(origin)
        if not isinstance(block, dict):
            continue
        block_status = str(block.get("status") or "").strip()
        if block_status and block_status != "ok":
            issues.append(
                {
                    "severity": "error",
                    "table": "works",
                    "origin": origin,
                    "check": "works_key_status",
                    "message": block_status,
                }
            )
            continue
        summary = block.get("summary") if origin == "parquet" else (block.get("summary") or {})
        if isinstance(summary, dict) and summary.get("status") != "ok":
            issues.append({"severity": "error", "table": "works", "check": f"{origin}_works_key_summary", "message": str(summary.get("error"))})
            continue
        row = _first_summary_row(summary if isinstance(summary, dict) else {})
        if row is None or len(row) < 7:
            continue
        rows_total = _to_int(row[0])
        rows_with_key = _to_int(row[1])
        key_null_rows = _to_int(row[2])
        key_literal_null_rows = _to_int(row[3])
        key_blank_rows = _to_int(row[4])
        key_malformed_rows = _to_int(row[5])
        distinct_key_count = _to_int(row[6])
        duplicate_key_rows = max(0, rows_with_key - distinct_key_count)
        bad_counts = {
            "key_null_rows": key_null_rows,
            "key_literal_null_rows": key_literal_null_rows,
            "key_blank_rows": key_blank_rows,
            "key_malformed_rows": key_malformed_rows,
            "duplicate_key_rows": duplicate_key_rows,
        }
        for check_name, count in bad_counts.items():
            if int(count) != 0:
                issues.append(
                    {
                        "severity": "error",
                        "table": "works",
                        "origin": origin,
                        "check": check_name,
                        "count": int(count),
                    }
                )
        if rows_total != rows_with_key:
            issues.append(
                {
                    "severity": "error",
                    "table": "works",
                    "origin": origin,
                    "check": "rows_total_equals_rows_with_key",
                    "rows_total": rows_total,
                    "rows_with_key": rows_with_key,
                }
            )
        prefix_sample = block.get("prefix_collision_sample")
        if isinstance(prefix_sample, dict):
            if prefix_sample.get("status") != "ok":
                issues.append(
                    {
                        "severity": "error",
                        "table": "works",
                        "origin": origin,
                        "check": "prefix_collision_sample_status",
                        "message": str(prefix_sample.get("error") or prefix_sample.get("status")),
                    }
                )
            elif prefix_sample.get("rows"):
                issues.append(
                    {
                        "severity": "error",
                        "table": "works",
                        "origin": origin,
                        "check": "prefix_collision_sample",
                        "sample_count": len(prefix_sample.get("rows") or []),
                    }
                )
        duplicate_file_sample = block.get("duplicate_key_file_sample")
        if isinstance(duplicate_file_sample, dict):
            if duplicate_file_sample.get("status") != "ok":
                issues.append(
                    {
                        "severity": "error",
                        "table": "works",
                        "origin": origin,
                        "check": "duplicate_key_file_sample_status",
                        "message": str(duplicate_file_sample.get("error") or duplicate_file_sample.get("status")),
                    }
                )
            elif duplicate_file_sample.get("rows"):
                cross_file_count = 0
                for sample_row in duplicate_file_sample.get("rows") or []:
                    if len(sample_row) >= 3 and _to_int(sample_row[2]) > 1:
                        cross_file_count += 1
                issues.append(
                    {
                        "severity": "error",
                        "table": "works",
                        "origin": origin,
                        "check": "duplicate_key_file_sample",
                        "sample_count": len(duplicate_file_sample.get("rows") or []),
                        "cross_file_sample_count": cross_file_count,
                    }
                )

    orphan_checks = ((report.get("checks") or {}).get("orphan_checks") or {})
    for table_name, check in orphan_checks.items():
        if not isinstance(check, dict):
            continue
        check_status = str(check.get("status") or "").strip()
        if check_status and check_status != "ok":
            issues.append(
                {
                    "severity": "error",
                    "table": table_name,
                    "check": "orphan_check_status",
                    "message": check_status,
                }
            )
            continue
        child_summary = check.get("child_key_summary") or {}
        if isinstance(child_summary, dict) and child_summary.get("status") not in {None, "ok"}:
            issues.append(
                {
                    "severity": "error",
                    "table": table_name,
                    "check": "child_key_summary_status",
                    "message": str(child_summary.get("error") or child_summary.get("status")),
                }
            )
            continue
        orphan_summary = check.get("orphans") or {}
        if isinstance(orphan_summary, dict) and orphan_summary.get("status") not in {None, "ok"}:
            issues.append(
                {
                    "severity": "error",
                    "table": table_name,
                    "check": "orphan_query_status",
                    "message": str(orphan_summary.get("error") or orphan_summary.get("status")),
                }
            )
            continue
        child_row = _first_summary_row(check.get("child_key_summary") or {})
        if child_row is not None and len(child_row) >= 4:
            for idx, key_name in ((1, "child_key_null_rows"), (2, "child_key_literal_null_rows"), (3, "child_key_blank_rows")):
                count = _to_int(child_row[idx])
                if count:
                    issues.append({"severity": "error", "table": table_name, "check": key_name, "count": count})
        orphan_row = _first_summary_row(check.get("orphans") or {})
        if orphan_row is not None:
            count = _to_int(orphan_row[0])
            if count:
                issues.append({"severity": "error", "table": table_name, "check": "orphan_rows", "count": count})

    checksum_checks = ((report.get("checks") or {}).get("sample_checksums") or {})
    for table_name, check in checksum_checks.items():
        if not isinstance(check, dict):
            continue
        if check.get("status") != "ok":
            issues.append(
                {
                    "severity": "error",
                    "table": table_name,
                    "check": "sample_checksum_status",
                    "message": str(check.get("error") or check.get("status")),
                }
            )
            continue
        missing_requested = check.get("missing_requested_columns")
        if isinstance(missing_requested, list) and missing_requested:
            issues.append(
                {
                    "severity": "error",
                    "table": table_name,
                    "check": "sample_checksum_missing_requested_columns",
                    "columns": [str(column) for column in missing_requested],
                }
            )
        if check.get("match") is False:
            comparison = check.get("comparison") if isinstance(check.get("comparison"), dict) else {}
            issues.append(
                {
                    "severity": "error",
                    "table": table_name,
                    "check": "sample_checksum_mismatch",
                    "mismatch_count_sampled": comparison.get("mismatch_count_sampled"),
                    "parquet_sample_rows": comparison.get("parquet_sample_rows"),
                    "db_sample_rows": comparison.get("db_sample_rows"),
                }
            )
    bucket_checks = ((report.get("checks") or {}).get("key_buckets") or {})
    for table_name, check in bucket_checks.items():
        if not isinstance(check, dict):
            continue
        if check.get("status") != "ok":
            issues.append(
                {
                    "severity": "error",
                    "table": table_name,
                    "check": "key_bucket_status",
                    "message": str(check.get("error") or check.get("status")),
                }
            )
            continue
        if check.get("match") is False:
            comparison = check.get("comparison") if isinstance(check.get("comparison"), dict) else {}
            issues.append(
                {
                    "severity": "error",
                    "table": table_name,
                    "check": "key_bucket_mismatch",
                    "mismatch_count_sampled": comparison.get("mismatch_count_sampled"),
                    "parquet_bucket_count": comparison.get("parquet_bucket_count"),
                    "db_bucket_count": comparison.get("db_bucket_count"),
                }
            )

    row_bucket_checks = ((report.get("checks") or {}).get("row_bucket_checksums") or {})
    for table_name, check in row_bucket_checks.items():
        if not isinstance(check, dict):
            continue
        if check.get("status") != "ok":
            issues.append(
                {
                    "severity": "error",
                    "table": table_name,
                    "check": "row_bucket_checksum_status",
                    "message": str(check.get("error") or check.get("status")),
                }
            )
            continue
        missing_requested = check.get("missing_requested_columns")
        if isinstance(missing_requested, list) and missing_requested:
            issues.append(
                {
                    "severity": "error",
                    "table": table_name,
                    "check": "row_bucket_checksum_missing_requested_columns",
                    "columns": [str(column) for column in missing_requested],
                }
            )
        if check.get("match") is False:
            comparison = check.get("comparison") if isinstance(check.get("comparison"), dict) else {}
            issues.append(
                {
                    "severity": "error",
                    "table": table_name,
                    "check": "row_bucket_checksum_mismatch",
                    "mismatch_count_sampled": comparison.get("mismatch_count_sampled"),
                    "parquet_bucket_count": comparison.get("parquet_bucket_count"),
                    "db_bucket_count": comparison.get("db_bucket_count"),
                }
            )
    return issues


def run_validation(args: argparse.Namespace) -> int:
    run_dir = Path(args.run_dir).expanduser().resolve()
    config_path = Path(args.config).expanduser().resolve() if args.config else run_dir / "config.json"
    table_specs_path = Path(args.table_specs).expanduser().resolve() if args.table_specs else None
    dotenv_path = Path(args.dotenv).expanduser().resolve() if args.dotenv else None
    out_path = Path(args.out).expanduser().resolve() if args.out else run_dir / "reload_validation.json"

    cfg = _read_json(config_path)
    db_config = _hydrate_db_password(cfg.get("db_config") or {}, dotenv_path=dotenv_path)
    if str(args.db_name).strip():
        db_config["database"] = str(args.db_name).strip()

    selected_tables = {str(item).strip() for item in args.table if str(item).strip()}
    specs = _filter_specs(
        _load_table_specs(run_dir, table_specs_path),
        selected_tables,
    )
    spec_signature = _spec_signature(specs)
    validation_options = _validation_options(args)
    works_selected = not selected_tables or str(args.works_table) in selected_tables

    checks: dict[str, Any] = {
        "tables": {},
        "works_key": {},
        "orphan_checks": {},
        "sample_checksums": {},
        "key_buckets": {},
        "row_bucket_checksums": {},
    }
    previous_report: dict[str, Any] = {}
    previous_checks: dict[str, Any] = {}
    previous_options: dict[str, Any] = {}
    if args.resume and out_path.exists():
        try:
            previous_report = _read_json(out_path)
        except Exception:
            previous_report = {}
        if _resume_base_matches(
            previous_report,
            run_dir=run_dir,
            db_name=str(db_config.get("database") or ""),
            config_path=config_path,
            table_specs_path=table_specs_path,
            spec_signature=spec_signature,
        ):
            loaded_checks = previous_report.get("checks")
            loaded_options = previous_report.get("validation_options")
            if isinstance(loaded_checks, dict) and isinstance(loaded_options, dict):
                previous_checks = loaded_checks
                previous_options = loaded_options

    report: dict[str, Any] = {
        "status": "running",
        "generated_at": _iso_now(),
        "updated_at": None,
        "run_dir": str(run_dir),
        "config": str(config_path),
        "table_specs": str(table_specs_path) if table_specs_path else "",
        "database": str(db_config.get("database") or ""),
        "source_specs": spec_signature,
        "validation_options": validation_options,
        "resume": bool(args.resume),
        "current": None,
        "checks": checks,
        "issues": [],
    }
    _write_json(out_path, report)

    timeout_base = int(args.max_statement_time or 0)
    io_timeout = (timeout_base + 30) if timeout_base > 0 else 24 * 3600
    import pymysql

    conn = pymysql.connect(
        host=db_config.get("host"),
        user=db_config.get("user"),
        password=db_config.get("password"),
        database=db_config.get("database"),
        port=int(db_config.get("port") or 3306),
        charset="utf8mb4",
        autocommit=True,
        read_timeout=max(30, int(io_timeout)),
        write_timeout=max(30, int(io_timeout)),
    )
    try:
        with conn.cursor() as cur:
            if int(args.max_statement_time or 0) > 0:
                cur.execute(f"SET SESSION max_statement_time={int(args.max_statement_time)}")

            for spec in specs:
                table_name = str(spec.get("target_table") or "").strip()
                existing_entry = (
                    _reusable_previous_check(
                        previous_checks,
                        section="tables",
                        key=table_name,
                        kind="table",
                        previous_options=previous_options,
                        current_options=validation_options,
                    )
                    if args.resume
                    else None
                )
                if existing_entry is not None:
                    report["checks"]["tables"][table_name] = existing_entry
                    if _needs_source_literal_marker_scan(
                        existing_entry,
                        skip_source_scan=bool(args.skip_source_literal_null_marker_scan),
                    ):
                        source_dir = Path(str(spec.get("source_dir") or "")).expanduser().resolve()
                        marker_scan = existing_entry.get("literal_null_marker_scan") or {}
                        nonzero_columns = (
                            marker_scan.get("nonzero_columns")
                            if isinstance(marker_scan, dict) and isinstance(marker_scan.get("nonzero_columns"), dict)
                            else {}
                        )
                        report["current"] = {"phase": "source_literal_marker_scan", "table": table_name}
                        _write_json(out_path, report)
                        temp_dir = Path(args.duckdb_temp_dir).expanduser().resolve() if args.duckdb_temp_dir else None
                        existing_entry["source_literal_null_marker_scan"] = _duckdb_literal_marker_scan(
                            source_dir=source_dir,
                            columns=[str(column) for column in nonzero_columns],
                            marker=str(args.literal_null_marker),
                            key_column=str(args.key_column),
                            sample_limit=int(args.sample_limit),
                            threads=int(args.threads),
                            memory_limit=str(args.memory_limit),
                            temp_dir=temp_dir,
                        )
                        existing_entry["literal_null_marker_comparison"] = _literal_marker_comparison(
                            db_scan=marker_scan,
                            source_scan=existing_entry.get("source_literal_null_marker_scan"),
                        )
                        _write_json(out_path, report)
                    continue
                source_dir = Path(str(spec.get("source_dir") or "")).expanduser().resolve()
                entry: dict[str, Any] = {
                    "status": "running",
                    "source_dir": str(source_dir),
                    "source_table": str(spec.get("source_table") or ""),
                }
                report["current"] = {"phase": "table", "table": table_name}
                report["checks"]["tables"][table_name] = entry
                _write_json(out_path, report)

                parquet = _parquet_row_count(source_dir)
                entry["parquet"] = parquet
                entry["parquet_rows"] = parquet.get("rows")
                entry["parquet_file_count"] = parquet.get("file_count")

                if not _db_table_exists(cur, table=table_name):
                    entry["status"] = "missing_db_table"
                    _write_json(out_path, report)
                    continue
                db_count = _db_scalar_count(cur, table=table_name)
                entry["db"] = {"row_count": db_count}
                entry["db_rows"] = db_count.get("rows") if db_count.get("status") == "ok" else None
                entry["row_count_match"] = (
                    parquet.get("status") == "ok"
                    and db_count.get("status") == "ok"
                    and int(parquet.get("rows") or 0) == int(db_count.get("rows") or 0)
                )
                entry["status"] = "ok" if parquet.get("status") == "ok" and db_count.get("status") == "ok" else "error"
                if not args.skip_literal_null_marker_scan and db_count.get("status") == "ok":
                    entry["literal_null_marker_scan"] = _db_literal_marker_scan(
                        cur,
                        table=table_name,
                        marker=str(args.literal_null_marker),
                        sample_limit=int(args.sample_limit),
                        chunk_size=int(args.literal_null_marker_column_chunk_size),
                        compare_mode=str(args.literal_null_marker_compare_mode),
                        columns=[str(column).strip() for column in args.literal_null_marker_column if str(column).strip()],
                        count_mode=str(args.literal_null_marker_count_mode),
                    )
                    db_marker_scan = entry.get("literal_null_marker_scan")
                    nonzero_columns = (
                        db_marker_scan.get("nonzero_columns")
                        if isinstance(db_marker_scan, dict) and isinstance(db_marker_scan.get("nonzero_columns"), dict)
                        else {}
                    )
                    if nonzero_columns and not args.skip_source_literal_null_marker_scan:
                        report["current"] = {"phase": "source_literal_marker_scan", "table": table_name}
                        _write_json(out_path, report)
                        temp_dir = Path(args.duckdb_temp_dir).expanduser().resolve() if args.duckdb_temp_dir else None
                        entry["source_literal_null_marker_scan"] = _duckdb_literal_marker_scan(
                            source_dir=source_dir,
                            columns=[str(column) for column in nonzero_columns],
                            marker=str(args.literal_null_marker),
                            key_column=str(args.key_column),
                            sample_limit=int(args.sample_limit),
                            threads=int(args.threads),
                            memory_limit=str(args.memory_limit),
                            temp_dir=temp_dir,
                        )
                        entry["literal_null_marker_comparison"] = _literal_marker_comparison(
                            db_scan=db_marker_scan,
                            source_scan=entry.get("source_literal_null_marker_scan"),
                        )
                _write_json(out_path, report)

            works_table = str(args.works_table)
            works_spec = next((spec for spec in specs if str(spec.get("target_table") or "") == works_table), None)
            if works_spec is not None and not args.skip_parquet_key_health:
                existing_parquet_key = (
                    _reusable_previous_check(
                        previous_checks,
                        section="works_key",
                        key="parquet",
                        kind="works_key_parquet",
                        previous_options=previous_options,
                        current_options=validation_options,
                    )
                    if args.resume
                    else None
                )
                if existing_parquet_key is not None:
                    report["checks"]["works_key"]["parquet"] = existing_parquet_key
                else:
                    temp_dir = Path(args.duckdb_temp_dir).expanduser().resolve() if args.duckdb_temp_dir else None
                    report["current"] = {"phase": "works_key_parquet", "table": works_table}
                    _write_json(out_path, report)

                    def save_parquet_key_progress(check: dict[str, Any]) -> None:
                        report["checks"]["works_key"]["parquet"] = check
                        _write_json(out_path, report)

                    report["checks"]["works_key"]["parquet"] = _duckdb_works_key_health(
                        Path(str(works_spec.get("source_dir") or "")).expanduser().resolve(),
                        key_column=str(args.key_column),
                        key_pattern=str(args.key_pattern),
                        prefix_length=int(args.prefix_length),
                        sample_limit=int(args.sample_limit),
                        threads=int(args.threads),
                        memory_limit=str(args.memory_limit),
                        temp_dir=temp_dir,
                        include_samples=not bool(args.skip_samples),
                        include_prefix_collision_sample=not bool(args.skip_prefix_collision_sample),
                        progress_callback=save_parquet_key_progress,
                    )
                    _write_json(out_path, report)

            works_db_exists = _db_table_exists(cur, table=works_table)
            works_key_exists = works_db_exists and _db_column_exists(cur, table=works_table, column=str(args.key_column))
            if not works_db_exists:
                report["checks"]["works_key"]["db"] = {"status": "missing_works_table"}
                _write_json(out_path, report)
            elif not works_key_exists:
                report["checks"]["works_key"]["db"] = {
                    "status": "missing_key_column",
                    "table": works_table,
                    "key_column": str(args.key_column),
                }
                _write_json(out_path, report)
            else:
                if works_selected and not args.skip_db_key_health:
                    existing_db_key = (
                        _reusable_previous_check(
                            previous_checks,
                            section="works_key",
                            key="db",
                            kind="works_key_db",
                            previous_options=previous_options,
                            current_options=validation_options,
                        )
                        if args.resume
                        else None
                    )
                    if existing_db_key is not None:
                        report["checks"]["works_key"]["db"] = existing_db_key
                    else:
                        report["current"] = {"phase": "works_key_db", "table": works_table}
                        _write_json(out_path, report)

                        def save_db_key_progress(check: dict[str, Any]) -> None:
                            report["checks"]["works_key"]["db"] = check
                            _write_json(out_path, report)

                        report["checks"]["works_key"]["db"] = _db_works_key_health(
                            cur,
                            table=works_table,
                            key_column=str(args.key_column),
                            key_pattern=str(args.key_pattern),
                            prefix_length=int(args.prefix_length),
                            sample_limit=int(args.sample_limit),
                            include_samples=not bool(args.skip_samples),
                            include_prefix_collision_sample=not bool(args.skip_prefix_collision_sample),
                            progress_callback=save_db_key_progress,
                        )
                        _write_json(out_path, report)

                if works_selected and not args.skip_key_bucket_check:
                    existing_key_bucket = (
                        _reusable_previous_check(
                            previous_checks,
                            section="key_buckets",
                            key=works_table,
                            kind="key_bucket",
                            previous_options=previous_options,
                            current_options=validation_options,
                        )
                        if args.resume
                        else None
                    )
                    if existing_key_bucket is not None:
                        report["checks"]["key_buckets"][works_table] = existing_key_bucket
                    else:
                        if works_spec is None:
                            report["checks"]["key_buckets"][works_table] = {
                                "status": "missing_table_spec",
                                "table": works_table,
                            }
                        else:
                            temp_dir = Path(args.duckdb_temp_dir).expanduser().resolve() if args.duckdb_temp_dir else None
                            report["current"] = {"phase": "key_bucket", "table": works_table}
                            _write_json(out_path, report)
                            report["checks"]["key_buckets"][works_table] = _key_bucket_check(
                                cur,
                                table=works_table,
                                source_dir=Path(str(works_spec.get("source_dir") or "")).expanduser().resolve(),
                                key_column=str(args.key_column),
                                bucket_prefix_length=int(args.key_bucket_prefix_length),
                                sample_limit=int(args.sample_limit),
                                threads=int(args.threads),
                                memory_limit=str(args.memory_limit),
                                temp_dir=temp_dir,
                            )
                        _write_json(out_path, report)

                if not args.skip_orphans:
                    for spec in specs:
                        table_name = str(spec.get("target_table") or "").strip()
                        if not table_name or table_name == works_table:
                            continue
                        existing_orphan = (
                            _reusable_previous_check(
                                previous_checks,
                                section="orphan_checks",
                                key=table_name,
                                kind="orphan",
                                previous_options=previous_options,
                                current_options=validation_options,
                            )
                            if args.resume
                            else None
                        )
                        if existing_orphan is not None:
                            report["checks"]["orphan_checks"][table_name] = existing_orphan
                            continue
                        report["current"] = {"phase": "orphan_check", "table": table_name}
                        _write_json(out_path, report)
                        if _db_table_exists(cur, table=table_name) and not _db_column_exists(cur, table=table_name, column=str(args.key_column)):
                            report["checks"]["orphan_checks"][table_name] = {
                                "status": "missing_key_column",
                                "table": table_name,
                                "key_column": str(args.key_column),
                            }
                            _write_json(out_path, report)
                        elif _db_table_exists(cur, table=table_name):
                            report["checks"]["orphan_checks"][table_name] = _db_child_key_and_orphan_check(
                                cur,
                                table=table_name,
                                parent_table=works_table,
                                key_column=str(args.key_column),
                            )
                            _write_json(out_path, report)

            if not args.skip_sample_checksum and int(args.checksum_sample_size) > 0:
                spec_by_table = {str(spec.get("target_table") or ""): spec for spec in specs}
                checksum_tables = [str(item).strip() for item in args.checksum_table if str(item).strip()]
                if not checksum_tables and works_table in spec_by_table:
                    checksum_tables = [works_table]
                temp_dir = Path(args.duckdb_temp_dir).expanduser().resolve() if args.duckdb_temp_dir else None
                for table_name in checksum_tables:
                    spec = spec_by_table.get(table_name)
                    existing_checksum = (
                        _reusable_previous_check(
                            previous_checks,
                            section="sample_checksums",
                            key=table_name,
                            kind="sample_checksum",
                            previous_options=previous_options,
                            current_options=validation_options,
                        )
                        if args.resume
                        else None
                    )
                    if existing_checksum is not None:
                        report["checks"]["sample_checksums"][table_name] = existing_checksum
                        continue
                    if not spec:
                        report["checks"]["sample_checksums"][table_name] = {
                            "status": "missing_table_spec",
                            "table": table_name,
                        }
                        _write_json(out_path, report)
                        continue
                    if not _db_table_exists(cur, table=table_name):
                        report["checks"]["sample_checksums"][table_name] = {
                            "status": "missing_db_table",
                            "table": table_name,
                        }
                        _write_json(out_path, report)
                        continue
                    report["current"] = {"phase": "sample_checksum", "table": table_name}
                    _write_json(out_path, report)
                    report["checks"]["sample_checksums"][table_name] = _sample_checksum_check(
                        cur,
                        table=table_name,
                        source_dir=Path(str(spec.get("source_dir") or "")).expanduser().resolve(),
                        key_column=str(args.key_column),
                        requested_columns=[str(column).strip() for column in args.checksum_column if str(column).strip()],
                        sample_size=int(args.checksum_sample_size),
                        sample_limit=int(args.sample_limit),
                        threads=int(args.threads),
                        memory_limit=str(args.memory_limit),
                        temp_dir=temp_dir,
                    )
                    _write_json(out_path, report)

            if not args.skip_row_bucket_checksum:
                spec_by_table = {str(spec.get("target_table") or ""): spec for spec in specs}
                row_bucket_tables = [str(item).strip() for item in args.row_bucket_checksum_table if str(item).strip()]
                if args.row_bucket_checksum_all_tables:
                    row_bucket_tables = sorted(set(row_bucket_tables) | {table for table in spec_by_table if table})
                for table_name in row_bucket_tables:
                    existing_row_bucket = (
                        _reusable_previous_check(
                            previous_checks,
                            section="row_bucket_checksums",
                            key=table_name,
                            kind="row_bucket_checksum",
                            previous_options=previous_options,
                            current_options=validation_options,
                        )
                        if args.resume
                        else None
                    )
                    if existing_row_bucket is not None:
                        report["checks"]["row_bucket_checksums"][table_name] = existing_row_bucket
                        continue
                    spec = spec_by_table.get(table_name)
                    if not spec:
                        report["checks"]["row_bucket_checksums"][table_name] = {
                            "status": "missing_table_spec",
                            "table": table_name,
                        }
                        _write_json(out_path, report)
                        continue
                    if not _db_table_exists(cur, table=table_name):
                        report["checks"]["row_bucket_checksums"][table_name] = {
                            "status": "missing_db_table",
                            "table": table_name,
                        }
                        _write_json(out_path, report)
                        continue
                    temp_dir = Path(args.duckdb_temp_dir).expanduser().resolve() if args.duckdb_temp_dir else None
                    report["current"] = {"phase": "row_bucket_checksum", "table": table_name}
                    _write_json(out_path, report)
                    report["checks"]["row_bucket_checksums"][table_name] = _row_bucket_checksum_check(
                        cur,
                        table=table_name,
                        source_dir=Path(str(spec.get("source_dir") or "")).expanduser().resolve(),
                        requested_columns=[str(column).strip() for column in args.row_bucket_checksum_column if str(column).strip()],
                        bucket_prefix_length=int(args.row_bucket_prefix_length),
                        sample_limit=int(args.sample_limit),
                        threads=int(args.threads),
                        memory_limit=str(args.memory_limit),
                        temp_dir=temp_dir,
                    )
                    _write_json(out_path, report)

        issues = _collect_issues(report)
        report["issues"] = issues
        report["summary"] = {
            "table_count": len(specs),
            "issue_count": len(issues),
            "tables_checked": sorted(report["checks"]["tables"]),
        }
        final_status = "done" if not issues else "failed"
        report["status"] = final_status
        report["current"] = None
        report["finished_at"] = _iso_now()
        _write_json(out_path, report)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        if issues and not args.no_fail_on_issues:
            return 2
        return 0
    except Exception as exc:
        report["status"] = "error"
        report["failed_at"] = _iso_now()
        report["error_type"] = type(exc).__name__
        report["error"] = str(exc)
        _write_json(out_path, report)
        raise
    finally:
        conn.close()


def main(argv: list[str] | None = None, *, prog: str | None = None) -> int:
    from KISTI_DB_Manager._cli.openalex_reload_validate import build_parser

    parser = build_parser(prog=prog, key_pattern_default=DEFAULT_KEY_PATTERN)
    return run_validation(parser.parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
