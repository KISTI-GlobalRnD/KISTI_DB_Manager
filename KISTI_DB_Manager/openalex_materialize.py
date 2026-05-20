#!/usr/bin/env python3
"""
Materialize persisted OpenAlex parquet artifacts into MariaDB/MySQL tables.

MVP scope:
- input is a completed or in-progress parse-parquet run directory
- parquet files are loaded table-by-table, file-by-file
- progress is checkpointed to JSON for resume
- target tables are created on first seen parquet file

This intentionally reuses the existing KISTI_DB_Manager DB primitives instead of
introducing a second independent loader stack.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from KISTI_DB_Manager import load_data, manage
from KISTI_DB_Manager.config import coerce_data_config, coerce_db_config
from KISTI_DB_Manager.namemap import NameMap
from KISTI_DB_Manager.naming import canonicalize_column_names
from KISTI_DB_Manager.report import RunReport


MATERIALIZE_PRESETS: dict[str, dict[str, Any]] = {
    "openalex-idcompact-fast": {
        "description": "Benchmarked OpenAlex ID-compacted parquet materialization path",
        "load_method": "load_data",
        "staging_writer": "duckdb",
        "parallel_tables": 6,
        "parallel_files_per_table": 2,
        "require_schema_manifest": True,
        "require_id_compaction": True,
    },
}


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _default_staging_dir() -> str:
    candidate = Path("/dev/shm")
    if candidate.exists() and candidate.is_dir():
        try:
            probe = candidate / ".kisti_materialize_write_probe"
            probe.write_text("", encoding="utf-8")
            probe.unlink(missing_ok=True)
            return str(candidate)
        except Exception:
            pass
    return "/tmp"


def _read_parquet_schema_rows(parquet_file: Path) -> tuple[list[str], int]:
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(parquet_file)
    cols = [str(c) for c in pf.schema_arrow.names]
    rows = int(getattr(pf.metadata, "num_rows", 0) or 0)
    return cols, rows


def _inspect_parquet_tables(
    table_files: dict[str, list[Path]],
    *,
    key_sep: str = "__",
    extra_column_name: str | None = None,
    sample_limit: int = 20,
) -> dict[str, Any]:
    """
    Cheap preflight over selected Parquet files using only footers/schema metadata.

    This runs before destructive table reset/load work. It intentionally avoids data
    scans, but catches corrupt files, empty selections, schema variants, row totals,
    and the union schema needed to create a table that can accept every selected file.
    """

    out: dict[str, Any] = {
        "status": "ok",
        "tables": {},
        "errors": [],
    }
    extra_canon = str(extra_column_name or "").replace(".", key_sep) if extra_column_name else ""
    limit = max(0, int(sample_limit))

    for table_name, files in sorted(table_files.items()):
        union_columns: list[str] = []
        seen_columns: set[str] = set()
        schema_variants: dict[tuple[str, ...], dict[str, Any]] = {}
        duplicate_column_files: list[dict[str, Any]] = []
        zero_row_files: list[str] = []
        rows_total = 0
        file_count = 0

        for parquet_file in files:
            try:
                columns, rows = _read_parquet_schema_rows(parquet_file)
            except Exception as exc:
                out["errors"].append(
                    {
                        "table": str(table_name),
                        "file": str(parquet_file),
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }
                )
                continue

            file_count += 1
            rows_total += int(rows)
            normalized_columns = [str(column).replace(".", key_sep) for column in columns]
            canonical_columns = canonicalize_column_names(columns, key_sep=key_sep)
            counts: dict[str, int] = {}
            for column in normalized_columns:
                counts[column] = int(counts.get(column, 0) or 0) + 1
            for column in canonical_columns:
                if column not in seen_columns:
                    seen_columns.add(column)
                    union_columns.append(column)
            duplicate_columns = sorted(column for column, count in counts.items() if count > 1)
            if duplicate_columns and len(duplicate_column_files) < limit:
                duplicate_column_files.append(
                    {
                        "file": str(parquet_file),
                        "columns": duplicate_columns[:limit],
                    }
                )
            if int(rows) == 0 and len(zero_row_files) < limit:
                zero_row_files.append(str(parquet_file))

            signature = tuple(canonical_columns)
            item = schema_variants.setdefault(
                signature,
                {
                    "file_count": 0,
                    "rows": 0,
                    "columns": list(canonical_columns),
                    "example_file": str(parquet_file),
                },
            )
            item["file_count"] = int(item["file_count"]) + 1
            item["rows"] = int(item["rows"]) + int(rows)

        if extra_canon and extra_canon not in seen_columns:
            union_columns.append(extra_canon)

        variants = sorted(
            schema_variants.values(),
            key=lambda item: (-int(item.get("file_count") or 0), str(item.get("example_file") or "")),
        )
        table_status = "ok"
        if file_count != len(files) or duplicate_column_files:
            table_status = "error"
            out["status"] = "error"
        out["tables"][str(table_name)] = {
            "status": table_status,
            "file_count": int(file_count),
            "files_expected": int(len(files)),
            "rows_total": int(rows_total),
            "zero_row_file_count_sampled": len(zero_row_files),
            "zero_row_files_sample": zero_row_files,
            "schema_variant_count": int(len(schema_variants)),
            "schema_variants": variants[:limit],
            "union_column_count": int(len(union_columns)),
            "union_columns": union_columns,
            "duplicate_column_files_sample": duplicate_column_files,
        }

    return out


def _stage_parquet_with_duckdb(
    *,
    parquet_file: Path,
    columns_original: list[str],
    stage_path: str,
    limit_rows: int,
    offset_rows: int,
    report: RunReport,
) -> None:
    import duckdb

    def dq(ident: str) -> str:
        return '"' + str(ident).replace('"', '""') + '"'

    select_sql = ", ".join(dq(c) for c in columns_original)
    limit_sql = f" LIMIT {int(limit_rows)}" if int(limit_rows or 0) > 0 else ""
    offset_sql = f" OFFSET {int(offset_rows)}" if int(offset_rows or 0) > 0 else ""
    copy_sql = (
        f"COPY (SELECT {select_sql} FROM read_parquet({json.dumps(str(parquet_file))}){limit_sql}{offset_sql}) "
        f"TO {json.dumps(str(stage_path))} "
        f"{load_data.DUCKDB_LOAD_DATA_DIALECT.duckdb_copy_options_sql()};"
    )
    t0 = time.perf_counter()
    con = duckdb.connect(database=":memory:")
    try:
        con.execute(copy_sql)
    finally:
        con.close()
    ms = int(round((time.perf_counter() - t0) * 1000.0))
    report.add_time_ms("db.load_data.stage_write", ms)
    report.add_time_ms("db.load_data.duckdb_stage_write", ms)


def _load_parquet_file_via_duckdb_stage(
    *,
    conn,
    table_name: str,
    parquet_file: Path,
    columns_original: list[str],
    columns_sql: list[str],
    limit_rows: int,
    offset_rows: int,
    staging_dir: str,
    report: RunReport,
) -> int:
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        prefix="kisti_duck_stage_",
        suffix=".tsv",
        delete=False,
        dir=staging_dir,
    ) as f:
        stage_path = f.name
    try:
        _stage_parquet_with_duckdb(
            parquet_file=parquet_file,
            columns_original=columns_original,
            stage_path=stage_path,
            limit_rows=limit_rows,
            offset_rows=offset_rows,
            report=report,
        )
        return load_data.load_data_local_infile_tabular_file(
            conn=conn,
            table_name=table_name,
            file_path=stage_path,
            sep="\t",
            columns_expr=[f"`{str(c).replace('`', '``')}`" for c in columns_sql],
            ignore_lines=0,
            dialect=load_data.DUCKDB_LOAD_DATA_DIALECT,
            expected_rows=limit_rows,
            line_terminator="\n",
            report=report,
        )
    finally:
        try:
            os.remove(stage_path)
        except Exception:
            pass


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


def _connect_local_infile(db_config: dict[str, Any], *, fast_load_session: bool, report: RunReport | None = None):
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
        _apply_fast_load_session_settings(conn, report=report, stage="parquet_materialize.fast_load_session")
    return conn


def _should_run_load_data_preflight(*, load_method: str, staging_writer: str, skip_preflight: bool) -> bool:
    return (
        not bool(skip_preflight)
        and str(load_method) in {"auto", "load_data"}
        and str(staging_writer) == "duckdb"
    )


def _apply_materialize_preset(args) -> dict[str, Any]:
    preset_name = str(getattr(args, "materialize_preset", None) or "")
    preset = dict(MATERIALIZE_PRESETS.get(preset_name, {}))

    def resolve(name: str, default: Any) -> Any:
        value = getattr(args, name)
        if value is not None:
            return value
        return preset.get(name, default)

    args.load_method = str(resolve("load_method", "load_data"))
    args.staging_writer = str(resolve("staging_writer", "duckdb"))
    args.parallel_tables = int(resolve("parallel_tables", 1))
    args.parallel_files_per_table = int(resolve("parallel_files_per_table", 1))
    args.require_schema_manifest = bool(
        getattr(args, "require_schema_manifest", False) or preset.get("require_schema_manifest", False)
    )
    args.require_id_compaction = bool(
        getattr(args, "require_id_compaction", False) or preset.get("require_id_compaction", False)
    )
    return {
        "name": preset_name or None,
        "applied": bool(preset),
        "settings": preset,
    }


def _run_duckdb_load_data_preflight_on_conn(*, conn, staging_dir: str, report: RunReport | None = None) -> None:
    """
    Validate the runtime DuckDB COPY <-> MariaDB LOAD DATA dialect pairing.

    This deliberately runs against the target MariaDB connection before large
    materialization begins. A rowcount-only check is not enough: the old broken
    dialect could sometimes preserve rowcount while changing field contents.
    """
    import duckdb

    def qi(ident: str) -> str:
        return str(ident).replace("`", "``")

    rows = [
        ("W1", "plain"),
        ("W2", 'line1 "\nline2'),
        ("W3", None),
        ("W4", "NULL"),
        ("W5", "line1\r\nline2"),
        ("W6", 'backslash quote \\"\ninside'),
    ]
    table_name = f"kisti_load_data_preflight_{uuid.uuid4().hex[:12]}"
    stage_path = None

    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        prefix="kisti_load_data_preflight_",
        suffix=".tsv",
        delete=False,
        dir=staging_dir,
    ) as f:
        stage_path = f.name

    try:
        con = duckdb.connect(database=":memory:")
        try:
            con.execute("CREATE TABLE stage(id VARCHAR, txt VARCHAR)")
            con.executemany("INSERT INTO stage VALUES (?, ?)", rows)
            con.execute(
                f"COPY stage TO {json.dumps(str(stage_path))} "
                f"{load_data.DUCKDB_LOAD_DATA_DIALECT.duckdb_copy_options_sql()};"
            )
        finally:
            con.close()

        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TEMPORARY TABLE `{qi(table_name)}` ("
                "`id` VARCHAR(32) NOT NULL PRIMARY KEY, "
                "`txt` LONGTEXT NULL"
                ") CHARACTER SET utf8mb4"
            )

        loaded = load_data.load_data_local_infile_tabular_file(
            conn=conn,
            table_name=table_name,
            file_path=str(stage_path),
            sep="\t",
            columns_expr=["`id`", "`txt`"],
            ignore_lines=0,
            dialect=load_data.DUCKDB_LOAD_DATA_DIALECT,
            expected_rows=len(rows),
            line_terminator="\n",
            report=None,
        )
        if int(loaded) != len(rows):
            raise RuntimeError(f"LOAD DATA preflight inserted {loaded} rows, expected {len(rows)}")

        with conn.cursor() as cur:
            cur.execute(f"SELECT `id`, `txt` FROM `{qi(table_name)}` ORDER BY `id`")
            fetched = list(cur.fetchall())

        if fetched != rows:
            raise RuntimeError(
                "LOAD DATA preflight content mismatch; DuckDB COPY and MariaDB LOAD DATA dialects are not aligned"
            )

        if report is not None:
            try:
                report.set_artifact(
                    "load_data_preflight",
                    {
                        "status": "ok",
                        "dialect": load_data.DUCKDB_LOAD_DATA_DIALECT.name,
                        "rows": len(rows),
                    },
                )
            except Exception:
                pass
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TEMPORARY TABLE IF EXISTS `{qi(table_name)}`")
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        if stage_path:
            try:
                os.remove(stage_path)
            except Exception:
                pass


def _run_duckdb_load_data_preflight(
    db_config: dict[str, Any], *, staging_dir: str, report: RunReport | None = None
) -> None:
    conn = _connect_local_infile(db_config, fast_load_session=False, report=report)
    try:
        _run_duckdb_load_data_preflight_on_conn(conn=conn, staging_dir=staging_dir, report=report)
    finally:
        conn.close()


def _save_report(report: RunReport, report_path: Path) -> None:
    report.finish()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report.to_json(indent=2), encoding="utf-8")


def _drop_target_tables(db_config: dict[str, Any], table_names: list[str]) -> None:
    import pymysql

    if not table_names:
        return
    conn = pymysql.connect(
        host=db_config.get("host"),
        user=db_config.get("user"),
        password=db_config.get("password"),
        database=db_config.get("database"),
        port=int(db_config.get("port") or 3306),
        charset="utf8mb4",
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            for table_name in table_names:
                cur.execute(f"DROP TABLE IF EXISTS `{str(table_name).replace('`', '``')}`")
    finally:
        conn.close()


def _drop_confirmation_token(table_names: list[str]) -> str:
    return ",".join(str(table_name) for table_name in table_names)


def _require_drop_confirmation(table_names: list[str], confirmation: str) -> str:
    expected = _drop_confirmation_token(table_names)
    if str(confirmation or "").strip() != expected:
        raise SystemExit(
            "--reset-selected-tables drops existing DB tables before loading. "
            f"Re-run with --confirm-drop-tables {expected!r} to confirm the exact target table list."
        )
    return expected


def _load_progress(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "updated_at_utc": None,
            "parquet_root": None,
            "table_count": 0,
            "tables_completed": 0,
            "files_loaded": 0,
            "rows_loaded": 0,
            "table_file_counts": {},
            "active": {},
            "current": None,
            "completed_files": {},
            "partial_files": {},
        }
    try:
        return _read_json(path)
    except Exception:
        backup = path.with_name(path.name + f".corrupt.{int(time.time())}")
        try:
            shutil.move(str(path), str(backup))
        except Exception:
            pass
        return {
            "updated_at_utc": None,
            "parquet_root": None,
            "table_count": 0,
            "tables_completed": 0,
            "files_loaded": 0,
            "rows_loaded": 0,
            "table_file_counts": {},
            "active": {},
            "current": None,
            "completed_files": {},
            "partial_files": {},
        }


def _pick_table_dirs(root: Path, selected: list[str], max_tables: int | None) -> list[Path]:
    dirs = sorted([p for p in root.iterdir() if p.is_dir()])
    if selected:
        wanted = set(selected)
        found = {p.name for p in dirs}
        missing = sorted(wanted - found)
        if missing:
            available = ", ".join(p.name for p in dirs[:50])
            suffix = "" if len(dirs) <= 50 else f", ... ({len(dirs)} total)"
            raise SystemExit(
                "Selected parquet table directory not found: "
                + ", ".join(missing)
                + f". Available tables: {available}{suffix}"
            )
        dirs = [p for p in dirs if p.name in wanted]
    if max_tables is not None and int(max_tables) >= 0:
        dirs = dirs[: int(max_tables)]
    return dirs


def _pick_files(table_dir: Path, max_files_per_table: int | None, latest_first: bool) -> list[Path]:
    files = sorted(table_dir.glob("*.parquet"))
    if latest_first:
        files = list(reversed(files))
    if max_files_per_table is not None and int(max_files_per_table) >= 0:
        files = files[: int(max_files_per_table)]
    return files


def _append_bounded_history(state: dict[str, Any], key: str, item: dict[str, Any], *, limit: int = 10) -> None:
    history = state.get(key)
    if not isinstance(history, list):
        history = []
    history.append(item)
    state[key] = history[-max(1, int(limit)) :]


def _progress_selected_tables(state: dict[str, Any]) -> list[str]:
    session = state.get("session")
    if isinstance(session, dict):
        tables = session.get("tables")
        if isinstance(tables, list):
            return [str(item) for item in tables]
    file_counts = state.get("table_file_counts")
    if isinstance(file_counts, dict):
        return [str(item) for item in file_counts]
    return []


def _progress_refresh_session_counts(state: dict[str, Any]) -> None:
    table_file_counts = state.get("table_file_counts") if isinstance(state.get("table_file_counts"), dict) else {}
    completed_files = state.get("completed_files") if isinstance(state.get("completed_files"), dict) else {}
    selected_tables = _progress_selected_tables(state)

    files_total = sum(int(table_file_counts.get(table_name, 0) or 0) for table_name in selected_tables)
    files_completed = sum(len(completed_files.get(table_name, []) or []) for table_name in selected_tables)
    tables_completed = sum(
        1
        for table_name in selected_tables
        if int(table_file_counts.get(table_name, 0) or 0) > 0
        and len(completed_files.get(table_name, []) or []) >= int(table_file_counts.get(table_name, 0) or 0)
    )
    state["files_total"] = int(files_total)
    state["files_completed"] = int(files_completed)
    state["tables_completed"] = int(tables_completed)


def _progress_prepare_session(
    state: dict[str, Any],
    *,
    parquet_root: Path,
    table_files: dict[str, list[Path]],
    selected_tables: list[str],
    reset_tables: set[str] | None = None,
) -> None:
    now = _iso_now()
    session_id = f"{int(time.time())}-{os.getpid()}"
    table_file_counts = {str(k): len(v) for k, v in table_files.items()}
    session_tables = [str(k) for k in table_files]

    active = state.get("active")
    if isinstance(active, dict) and active:
        _append_bounded_history(
            state,
            "stale_active_history",
            {
                "cleared_at_utc": now,
                "session_id": session_id,
                "entries": active,
            },
        )

    completed_files = state.get("completed_files")
    if not isinstance(completed_files, dict):
        completed_files = {}
    partial_files = state.get("partial_files")
    if not isinstance(partial_files, dict):
        partial_files = {}

    pruned_completed: dict[str, dict[str, Any]] = {}
    pruned_partial: dict[str, dict[str, Any]] = {}
    reset_tables = set(reset_tables or set())
    for table_name, files in table_files.items():
        valid_names = {path.name for path in files}

        done_values = completed_files.get(table_name, [])
        if not isinstance(done_values, list):
            done_values = []
        filtered_done = [] if table_name in reset_tables else [str(item) for item in done_values if str(item) in valid_names]
        removed_done = sorted({str(item) for item in done_values} - set(filtered_done))
        if removed_done:
            pruned_completed[table_name] = {"count": len(removed_done), "examples": removed_done[:20]}
        completed_files[table_name] = filtered_done

        partial_values = partial_files.get(table_name, {})
        if not isinstance(partial_values, dict):
            partial_values = {}
        filtered_partial = (
            {}
            if table_name in reset_tables
            else {str(name): value for name, value in partial_values.items() if str(name) in valid_names}
        )
        removed_partial = sorted({str(name) for name in partial_values} - set(filtered_partial))
        if removed_partial:
            pruned_partial[table_name] = {"count": len(removed_partial), "examples": removed_partial[:20]}
        if filtered_partial:
            partial_files[table_name] = filtered_partial
        else:
            partial_files.pop(table_name, None)

    if pruned_completed or pruned_partial:
        _append_bounded_history(
            state,
            "progress_prune_history",
            {
                "pruned_at_utc": now,
                "session_id": session_id,
                "completed_files": pruned_completed,
                "partial_files": pruned_partial,
            },
        )

    state["parquet_root"] = str(parquet_root)
    state["updated_at_utc"] = now
    state["table_count"] = len(table_files)
    state["table_file_counts"] = table_file_counts
    state["completed_files"] = completed_files
    state["partial_files"] = partial_files
    state["active"] = {}
    state["current"] = None
    state["session"] = {
        "id": session_id,
        "started_at_utc": now,
        "parquet_root": str(parquet_root),
        "selected_tables_arg": list(selected_tables),
        "tables": session_tables,
        "files_total": int(sum(table_file_counts.values())),
    }
    state["files_loaded_session"] = 0
    state["rows_loaded_session"] = 0
    _progress_refresh_session_counts(state)
    state["files_completed_before_session"] = int(state.get("files_completed", 0) or 0)
    partial_rows = 0
    for table_name in session_tables:
        table_partials = partial_files.get(table_name, {})
        if not isinstance(table_partials, dict):
            continue
        for item in table_partials.values():
            if isinstance(item, dict):
                partial_rows += int(item.get("next_offset") or 0)
    state["partial_rows_before_session"] = int(partial_rows)
    state["files_loaded"] = int(state.get("files_completed", 0) or 0)
    state["rows_loaded"] = int(partial_rows)


def _progress_mark_reset_pending(state: dict[str, Any], *, target_tables: list[str]) -> None:
    state["reset"] = {
        "status": "pending",
        "target_tables": list(target_tables),
        "started_at_utc": _iso_now(),
    }


def _progress_mark_reset_completed(state: dict[str, Any]) -> None:
    reset = state.get("reset")
    if not isinstance(reset, dict):
        reset = {}
    reset["status"] = "completed"
    reset["completed_at_utc"] = _iso_now()
    state["reset"] = reset


def _require_no_pending_reset(state: dict[str, Any], *, progress_path: Path) -> None:
    reset = state.get("reset")
    if not isinstance(reset, dict) or str(reset.get("status") or "") != "pending":
        return
    target_tables = reset.get("target_tables")
    if not isinstance(target_tables, list):
        target_tables = []
    raise SystemExit(
        "Previous table reset is marked pending in progress state. "
        "Do not resume without reset confirmation; rerun with --reset-selected-tables "
        f"and --confirm-drop-tables {','.join(str(t) for t in target_tables)!r}, "
        f"or inspect {progress_path}."
    )


def _progress_sync_current(state: dict[str, Any]) -> None:
    active = state.get("active") or {}
    items = [v for _, v in sorted(active.items()) if isinstance(v, dict)]
    if not items:
        state["current"] = None
    elif len(items) == 1:
        state["current"] = items[0]
    else:
        state["current"] = items


def _progress_write(progress_path: Path, state: dict[str, Any], lock: threading.Lock) -> None:
    with lock:
        state["updated_at_utc"] = _iso_now()
        _progress_sync_current(state)
        _write_json(progress_path, state)


def _progress_activate(
    progress_path: Path,
    state: dict[str, Any],
    lock: threading.Lock,
    *,
    active_key: str,
    table_original: str,
    table_sql: str | None,
    parquet_file: Path,
    rows: int | None,
    offset_rows: int | None = None,
    total_rows: int | None = None,
) -> None:
    with lock:
        active = state.setdefault("active", {})
        active[active_key] = {
            "table_original": table_original,
            "table_sql": table_sql,
            "file_path": str(parquet_file),
            "rows": rows,
            "offset_rows": offset_rows,
            "total_rows": total_rows,
        }
        state["updated_at_utc"] = _iso_now()
        _progress_sync_current(state)
        _write_json(progress_path, state)


def _progress_mark_chunk_done(
    progress_path: Path,
    state: dict[str, Any],
    lock: threading.Lock,
    *,
    active_key: str,
    table_original: str,
    parquet_file: Path,
    rows: int,
    next_offset: int,
    total_rows: int,
    chunk_rows: int,
    file_complete: bool,
) -> None:
    with lock:
        state["rows_loaded"] = int(state.get("rows_loaded", 0) or 0) + int(rows)
        state["rows_loaded_session"] = int(state.get("rows_loaded_session", 0) or 0) + int(rows)
        partial_files = state.setdefault("partial_files", {})
        partial = partial_files.setdefault(table_original, {})
        if file_complete:
            partial.pop(parquet_file.name, None)
            if not partial:
                partial_files.pop(table_original, None)
            done = state.setdefault("completed_files", {}).setdefault(table_original, [])
            if parquet_file.name not in done:
                done.append(parquet_file.name)
                state["files_loaded"] = int(state.get("files_loaded", 0) or 0) + 1
                state["files_loaded_session"] = int(state.get("files_loaded_session", 0) or 0) + 1
        else:
            partial[parquet_file.name] = {
                "next_offset": int(next_offset),
                "total_rows": int(total_rows),
                "chunk_rows": int(chunk_rows),
            }

        _progress_refresh_session_counts(state)

        active = state.setdefault("active", {})
        active.pop(active_key, None)
        state["updated_at_utc"] = _iso_now()
        _progress_sync_current(state)
        _write_json(progress_path, state)


def _progress_mark_error(progress_path: Path, state: dict[str, Any], lock: threading.Lock, *, active_key: str) -> None:
    with lock:
        active = state.setdefault("active", {})
        active.pop(active_key, None)
        state["updated_at_utc"] = _iso_now()
        _progress_sync_current(state)
        _write_json(progress_path, state)


def _merge_worker_result(report: RunReport, result: dict[str, Any]) -> None:
    for key, value in (result.get("stats") or {}).items():
        report.bump(str(key), int(value or 0))
    for key, value in (result.get("timings_ms") or {}).items():
        report.add_time_ms(str(key), int(value or 0))
    for err in (result.get("errors") or []):
        report.error(
            stage=str(err.get("stage") or "parquet_materialize.file"),
            message=str(err.get("message") or "Worker error"),
            table_original=err.get("table_original"),
            parquet_file=err.get("parquet_file"),
            error=err.get("error"),
        )


def _merge_result_dict(dst: dict[str, Any], src: dict[str, Any]) -> None:
    dst_stats = dst.setdefault("stats", {})
    for key, value in (src.get("stats") or {}).items():
        dst_stats[str(key)] = int(dst_stats.get(str(key), 0) or 0) + int(value or 0)
    dst_timings = dst.setdefault("timings_ms", {})
    for key, value in (src.get("timings_ms") or {}).items():
        dst_timings[str(key)] = int(dst_timings.get(str(key), 0) or 0) + int(value or 0)
    dst.setdefault("errors", []).extend(list(src.get("errors") or []))


def _materialize_one_file(
    *,
    table_original: str,
    target_table_sql: str,
    parquet_file: Path,
    data_config: dict[str, Any],
    db_config: dict[str, Any],
    load_method: str,
    limit_rows_per_file: int,
    progress_path: Path,
    state: dict[str, Any],
    state_lock: threading.Lock,
    load_data_staging_writer: str,
    load_data_staging_dir: str | None,
    file_chunk_rows: int,
    nm: NameMap,
    existing_cols: set[str] | None,
) -> dict[str, Any]:
    local_report = RunReport()
    result: dict[str, Any] = {
        "table_original": table_original,
        "stats": {},
        "timings_ms": {},
        "errors": [],
        "files": [],
    }

    def bump(key: str, value: int = 1) -> None:
        result["stats"][key] = int(result["stats"].get(key, 0) or 0) + int(value)

    def add_ms(key: str, seconds: float) -> None:
        ms = int(round(float(seconds) * 1000.0))
        if ms > 0:
            result["timings_ms"][key] = int(result["timings_ms"].get(key, 0) or 0) + ms

    fast_load_state = manage.FastLoadState(enabled=(load_method in {"auto", "load_data"}))
    local_infile_conn = None
    engine = None
    extra_column_name = str(data_config.get("extra_column_name") or "__extra__")
    extra_canon = extra_column_name.replace(".", str(data_config.get("KEY_SEP") or "__")) if extra_column_name else ""
    active_key = f"{table_original}:{parquet_file.name}"
    partial_info = (((state.get("partial_files") or {}).get(table_original) or {}).get(parquet_file.name) or {})
    min_chunk_rows = 10_000

    def _is_lock_table_size_error(exc: Exception) -> bool:
        msg = str(exc).lower()
        return "lock table size" in msg or "(1206," in msg

    try:
        if fast_load_state.enabled:
            local_infile_conn = _connect_local_infile(db_config, fast_load_session=True, report=local_report)

        _progress_activate(
            progress_path,
            state,
            state_lock,
            active_key=active_key,
            table_original=table_original,
            table_sql=target_table_sql,
            parquet_file=parquet_file,
            rows=None,
        )

        t0 = time.perf_counter()
        table_columns, parquet_rows = _read_parquet_schema_rows(parquet_file)
        add_ms("parquet_materialize.inspect_parquet", time.perf_counter() - t0)
        load_rows = int(parquet_rows)
        if limit_rows_per_file and int(limit_rows_per_file) > 0:
            load_rows = min(load_rows, int(limit_rows_per_file))
        bump("parquet_files_read", 1)
        resume_offset = int(partial_info.get("next_offset") or 0)
        chunk_rows = int(file_chunk_rows or 0)
        if chunk_rows <= 0:
            chunk_rows = int(load_rows)

        current_nm = nm.with_additional_columns(table_columns, max_len=64)
        columns_original = [c for c in table_columns if c != extra_canon]
        columns_sql = [current_nm.map_column(c) for c in columns_original]
        offset_rows = int(resume_offset)
        if int(load_rows) <= 0 or int(offset_rows) >= int(load_rows):
            _progress_mark_chunk_done(
                progress_path,
                state,
                state_lock,
                active_key=active_key,
                table_original=table_original,
                parquet_file=parquet_file,
                rows=0,
                next_offset=int(load_rows),
                total_rows=int(load_rows),
                chunk_rows=0,
                file_complete=True,
            )
        while int(offset_rows) < int(load_rows):
            current_chunk_rows = min(int(chunk_rows), int(load_rows) - int(offset_rows))
            _progress_activate(
                progress_path,
                state,
                state_lock,
                active_key=active_key,
                table_original=table_original,
                table_sql=target_table_sql,
                parquet_file=parquet_file,
                rows=int(current_chunk_rows),
                offset_rows=int(offset_rows),
                total_rows=int(load_rows),
            )

            try:
                direct_duckdb_ok = False
                if (
                    str(load_data_staging_writer) == "duckdb"
                    and local_infile_conn is not None
                    and existing_cols is not None
                    and all(col in existing_cols for col in columns_sql)
                ):
                    t0 = time.perf_counter()
                    with local_report.timer("db.load_data.total"):
                        loaded_rows = _load_parquet_file_via_duckdb_stage(
                            conn=local_infile_conn,
                            table_name=target_table_sql,
                            parquet_file=parquet_file,
                            columns_original=columns_original,
                            columns_sql=columns_sql,
                            limit_rows=int(current_chunk_rows),
                            offset_rows=int(offset_rows),
                            staging_dir=str(load_data_staging_dir or _default_staging_dir()),
                            report=local_report,
                        )
                    if int(loaded_rows) != int(current_chunk_rows):
                        raise RuntimeError(
                            "LOAD DATA inserted row count mismatch "
                            f"for {parquet_file.name}: expected {int(current_chunk_rows)}, got {int(loaded_rows)}"
                        )
                    add_ms("parquet_materialize.load_file", time.perf_counter() - t0)
                    bump("load_data_ok", 1)
                    direct_duckdb_ok = True

                if not direct_duckdb_ok:
                    import pandas as pd

                    if engine is None:
                        from sqlalchemy import create_engine
                        from sqlalchemy.engine import URL

                        engine = create_engine(
                            URL.create(
                                "mysql+pymysql",
                                username=db_config.get("user"),
                                password=db_config.get("password"),
                                host=db_config.get("host"),
                                port=int(db_config.get("port") or 3306),
                                database=db_config.get("database"),
                            )
                        )
                    t0 = time.perf_counter()
                    df = pd.read_parquet(parquet_file)
                    add_ms("parquet_materialize.read_parquet", time.perf_counter() - t0)
                    if limit_rows_per_file and int(limit_rows_per_file) > 0:
                        df = df.head(int(limit_rows_per_file)).copy()
                    df = df.iloc[int(offset_rows): int(offset_rows) + int(current_chunk_rows)].copy()
                    t0 = time.perf_counter()
                    manage.fill_table_from_dataframe(
                        df,
                        db_config,
                        table_name=target_table_sql,
                        name_map=nm,
                        extra_column_name=extra_column_name,
                        auto_alter_table=False,
                        column_type="LONGTEXT",
                        fallback_on_insert_error=False,
                        report=local_report,
                        load_method=str(load_method),
                        fast_load_state=fast_load_state,
                        local_infile_conn=local_infile_conn,
                        existing_cols=existing_cols,
                        engine=engine,
                        load_data_staging_writer=load_data_staging_writer,
                        load_data_staging_dir=load_data_staging_dir,
                    )
                    add_ms("parquet_materialize.load_file", time.perf_counter() - t0)
            except Exception as exc:
                if _is_lock_table_size_error(exc) and int(current_chunk_rows) > int(min_chunk_rows):
                    next_chunk_rows = max(int(min_chunk_rows), int(current_chunk_rows) // 2)
                    bump("load_data_lock_table_retries", 1)
                    chunk_rows = int(next_chunk_rows)
                    continue
                raise

            bump("parquet_rows_read", int(current_chunk_rows))
            bump("rows_loaded", int(current_chunk_rows))
            _progress_mark_chunk_done(
                progress_path,
                state,
                state_lock,
                active_key=active_key,
                table_original=table_original,
                parquet_file=parquet_file,
                rows=int(current_chunk_rows),
                next_offset=int(offset_rows) + int(current_chunk_rows),
                total_rows=int(load_rows),
                chunk_rows=int(current_chunk_rows),
                file_complete=(int(offset_rows) + int(current_chunk_rows) >= int(load_rows)),
            )
            offset_rows = int(offset_rows) + int(current_chunk_rows)

        bump("files_loaded", 1)
        result["files"].append(
            {
                "path": str(parquet_file),
                "rows": int(load_rows),
                "table_sql": target_table_sql,
            }
        )
    except Exception as e:
        result["errors"].append(
            {
                "stage": "parquet_materialize.file",
                "message": "Failed to materialize parquet file",
                "table_original": table_original,
                "parquet_file": str(parquet_file),
                "error": str(e),
            }
        )
        _progress_mark_error(progress_path, state, state_lock, active_key=active_key)
        raise
    finally:
        for key, value in (local_report.stats or {}).items():
            result["stats"][str(key)] = int(result["stats"].get(str(key), 0) or 0) + int(value or 0)
        for key, value in (local_report.timings_ms or {}).items():
            result["timings_ms"][str(key)] = int(result["timings_ms"].get(str(key), 0) or 0) + int(value or 0)
        for issue in local_report.issues or []:
            result["errors"].append(
                {
                    "stage": issue.stage,
                    "message": issue.message,
                    "table_original": table_original,
                    "parquet_file": str(parquet_file),
                    "error": issue.exception_message or issue.context.get("error"),
                }
            )
        if engine is not None:
            try:
                engine.dispose()
            except Exception:
                pass
        if local_infile_conn is not None:
            try:
                local_infile_conn.close()
            except Exception:
                pass

    return result


def _materialize_one_table(
    *,
    table_original: str,
    files: list[Path],
    table_preflight: dict[str, Any] | None,
    completed_files: set[str],
    data_config: dict[str, Any],
    db_config: dict[str, Any],
    load_method: str,
    limit_rows_per_file: int,
    table_prefix: str,
    progress_path: Path,
    state: dict[str, Any],
    state_lock: threading.Lock,
    keep_going: bool,
    load_data_staging_writer: str,
    load_data_staging_dir: str | None,
    file_chunk_rows: int,
    parallel_files_per_table: int,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "table_original": table_original,
        "stats": {},
        "timings_ms": {},
        "errors": [],
        "files": [],
    }

    def bump(key: str, value: int = 1) -> None:
        result["stats"][key] = int(result["stats"].get(key, 0) or 0) + int(value)

    def add_ms(key: str, seconds: float) -> None:
        ms = int(round(float(seconds) * 1000.0))
        if ms > 0:
            result["timings_ms"][key] = int(result["timings_ms"].get(key, 0) or 0) + ms

    nm: NameMap | None = None
    existing_cols: set[str] | None = None

    try:
        target_table = f"{table_prefix}{table_original}"
        extra_column_name = str(data_config.get("extra_column_name") or "__extra__")
        extra_canon = extra_column_name.replace(".", str(data_config.get("KEY_SEP") or "__")) if extra_column_name else ""
        todo_files = [parquet_file for parquet_file in files if parquet_file.name not in completed_files]
        if not todo_files:
            return result

        t0 = time.perf_counter()
        preflight_columns = []
        if isinstance(table_preflight, dict):
            preflight_columns = [str(column) for column in (table_preflight.get("union_columns") or [])]
        if preflight_columns:
            first_columns = preflight_columns
        else:
            first_columns, _ = _read_parquet_schema_rows(todo_files[0])
        add_ms("parquet_materialize.inspect_parquet", time.perf_counter() - t0)
        if extra_canon and extra_canon not in first_columns:
            first_columns.append(extra_canon)
        nm = NameMap.build(
            table_name=target_table,
            columns=first_columns,
            key_sep=str(data_config.get("KEY_SEP") or "__"),
        )
        t0 = time.perf_counter()
        nm = manage.create_table_from_columns(
            db_config,
            table_name=target_table,
            columns=first_columns,
            name_map=nm,
            key_sep=str(data_config.get("KEY_SEP") or "__"),
            column_type="LONGTEXT",
        )
        add_ms("parquet_materialize.create_table", time.perf_counter() - t0)
        existing_cols = set(nm.columns_sql)
        result["table_sql"] = nm.table_sql

        file_workers = max(1, int(parallel_files_per_table or 1))
        if (
            str(load_data_staging_writer) != "duckdb"
            or file_workers <= 1
            or len(todo_files) <= 1
        ):
            file_workers = 1

        if file_workers <= 1:
            for parquet_file in todo_files:
                file_result: dict[str, Any] | None = None
                try:
                    file_result = _materialize_one_file(
                        table_original=table_original,
                        target_table_sql=nm.table_sql,
                        parquet_file=parquet_file,
                        data_config=data_config,
                        db_config=db_config,
                        load_method=load_method,
                        limit_rows_per_file=limit_rows_per_file,
                        progress_path=progress_path,
                        state=state,
                        state_lock=state_lock,
                        load_data_staging_writer=load_data_staging_writer,
                        load_data_staging_dir=load_data_staging_dir,
                        file_chunk_rows=file_chunk_rows,
                        nm=nm,
                        existing_cols=existing_cols,
                    )
                    _merge_result_dict(result, file_result)
                    result["files"].extend(file_result.get("files") or [])
                except Exception:
                    if file_result is not None:
                        _merge_result_dict(result, file_result)
                        result["files"].extend(file_result.get("files") or [])
                    if not keep_going:
                        raise
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=file_workers) as ex:
                fut_map = {
                    ex.submit(
                        _materialize_one_file,
                        table_original=table_original,
                        target_table_sql=nm.table_sql,
                        parquet_file=parquet_file,
                        data_config=data_config,
                        db_config=db_config,
                        load_method=load_method,
                        limit_rows_per_file=limit_rows_per_file,
                        progress_path=progress_path,
                        state=state,
                        state_lock=state_lock,
                        load_data_staging_writer=load_data_staging_writer,
                        load_data_staging_dir=load_data_staging_dir,
                        file_chunk_rows=file_chunk_rows,
                        nm=nm,
                        existing_cols=existing_cols,
                    ): parquet_file
                    for parquet_file in todo_files
                }
                for fut in concurrent.futures.as_completed(fut_map):
                    try:
                        file_result = fut.result()
                        _merge_result_dict(result, file_result)
                        result["files"].extend(file_result.get("files") or [])
                    except Exception as e:
                        result["errors"].append(
                            {
                                "stage": "parquet_materialize.file",
                                "message": "Failed to materialize parquet file",
                                "table_original": table_original,
                                "parquet_file": str(fut_map[fut]),
                                "error": str(e),
                            }
                        )
                        if not keep_going:
                            raise
        return result
    finally:
        pass


def main(argv: list[str] | None = None, *, prog: str | None = None) -> int:
    ap = argparse.ArgumentParser(prog=prog)
    ap.add_argument("run_dir", help="parse-parquet run directory containing config.json")
    ap.add_argument("--parquet-root", help="Override parquet root (defaults to config.data_config.persist_parquet_dir)")
    ap.add_argument("--progress", help="Progress JSON path (default: <run_dir>/parquet_materialize/progress.json)")
    ap.add_argument("--report", help="Report JSON path (default: <run_dir>/parquet_materialize/run_report.json)")
    ap.add_argument("--dotenv", default=".env", help="dotenv-like file used to restore masked DB password")
    ap.add_argument("--db-name", default=None, help="Override target database name from config.json")
    ap.add_argument("--table", action="append", default=[], help="Parquet table directory name to materialize (repeatable)")
    ap.add_argument("--max-tables", type=int, default=None)
    ap.add_argument("--max-files-per-table", type=int, default=None)
    ap.add_argument("--latest-first", action="store_true")
    ap.add_argument("--limit-rows-per-file", type=int, default=0)
    ap.add_argument("--table-prefix", default="", help="Optional target table prefix")
    ap.add_argument(
        "--materialize-preset",
        choices=sorted(MATERIALIZE_PRESETS),
        default=None,
        help="Apply a measured materialization preset while preserving explicit option overrides",
    )
    ap.add_argument("--load-method", choices=["auto", "load_data", "to_sql"], default=None)
    ap.add_argument("--parallel-tables", type=int, default=None, help="Number of tables to materialize in parallel")
    ap.add_argument("--parallel-files-per-table", type=int, default=None, help="Number of parquet files to load in parallel within a table")
    ap.add_argument("--file-chunk-rows", type=int, default=0, help="Chunk rows within a parquet file for finer-grained resume (0 disables)")
    ap.add_argument("--staging-writer", choices=["python", "duckdb"], default=None)
    ap.add_argument("--staging-dir", default=None, help="Temp staging directory for LOAD DATA files")
    ap.add_argument(
        "--skip-load-data-preflight",
        action="store_true",
        help="Skip the small target-DB LOAD DATA dialect round-trip check before loading",
    )
    ap.add_argument(
        "--reset-selected-tables",
        action="store_true",
        help="Drop selected target tables and clear their progress before loading",
    )
    ap.add_argument(
        "--confirm-drop-tables",
        default="",
        help="Required with --reset-selected-tables; exact comma-separated target table names after prefix",
    )
    ap.add_argument("--require-schema-manifest", action="store_true", help="Fail if schema_manifest.json is missing")
    ap.add_argument("--require-id-compaction", action="store_true", help="Fail if schema_manifest.json does not record enabled ID compaction")
    ap.add_argument("--strict-schema-manifest", action="store_true", help="Fail on manifest/parquet schema mismatches")
    ap.add_argument("--keep-going", action="store_true", help="Continue with next file on error")
    args = ap.parse_args(argv)
    preset_info = _apply_materialize_preset(args)

    run_dir = Path(args.run_dir).expanduser().resolve()
    cfg = _read_json(run_dir / "config.json")
    data_config = coerce_data_config(cfg.get("data_config") or {}, inplace=False)
    db_config = _hydrate_db_password(
        coerce_db_config(cfg.get("db_config") or {}, inplace=False),
        dotenv_path=Path(args.dotenv).expanduser().resolve() if args.dotenv else None,
    )
    if args.db_name:
        db_config["database"] = str(args.db_name).strip()

    parquet_root = Path(
        args.parquet_root
        or data_config.get("persist_parquet_dir")
        or ""
    ).expanduser().resolve()
    if not parquet_root.exists():
        raise SystemExit(f"parquet root not found: {parquet_root}")

    staging_dir = str(args.staging_dir or _default_staging_dir())
    Path(staging_dir).expanduser().resolve().mkdir(parents=True, exist_ok=True)

    work_dir = run_dir / "parquet_materialize"
    progress_path = Path(args.progress).expanduser().resolve() if args.progress else work_dir / "progress.json"
    report_path = Path(args.report).expanduser().resolve() if args.report else work_dir / "run_report.json"

    state = _load_progress(progress_path)

    selected_tables = [str(t).strip() for t in args.table if str(t).strip()]
    table_dirs = _pick_table_dirs(parquet_root, selected_tables, args.max_tables)
    if not table_dirs:
        raise SystemExit(f"No parquet table directories selected under {parquet_root}")

    report = RunReport()
    report.set_artifact("run_dir", str(run_dir))
    report.set_artifact("parquet_root", str(parquet_root))
    report.set_artifact("db_name", str(db_config.get("database") or ""))
    report.set_artifact("selected_tables", selected_tables)
    report.set_artifact("materialize_preset", preset_info.get("name"))
    report.set_artifact("materialize_preset_applied", bool(preset_info.get("applied")))
    report.set_artifact("materialize_preset_settings", preset_info.get("settings") or {})
    report.set_artifact("load_method", str(args.load_method))
    report.set_artifact("staging_writer", str(args.staging_writer))
    report.set_artifact("staging_dir", staging_dir)
    report.set_artifact("table_prefix", str(args.table_prefix))
    report.set_artifact("max_tables", args.max_tables)
    report.set_artifact("max_files_per_table", args.max_files_per_table)
    report.set_artifact("latest_first", bool(args.latest_first))
    report.set_artifact("limit_rows_per_file", int(args.limit_rows_per_file))
    report.set_artifact("parallel_tables", int(args.parallel_tables))
    report.set_artifact("parallel_files_per_table", int(args.parallel_files_per_table))
    report.set_artifact("file_chunk_rows", int(args.file_chunk_rows))
    report.set_artifact("skip_load_data_preflight", bool(args.skip_load_data_preflight))
    report.set_artifact("reset_selected_tables", bool(args.reset_selected_tables))
    report.set_artifact("confirm_drop_tables_provided", bool(str(args.confirm_drop_tables).strip()))

    state_lock = threading.Lock()
    state.setdefault("completed_files", {})
    state.setdefault("active", {})
    table_files: dict[str, list[Path]] = {
        table_dir.name: _pick_files(table_dir, args.max_files_per_table, bool(args.latest_first))
        for table_dir in table_dirs
    }
    empty_tables = sorted(table_name for table_name, files in table_files.items() if not files)
    if empty_tables:
        raise SystemExit("Selected parquet table directories contain no parquet files: " + ", ".join(empty_tables))
    try:
        from KISTI_DB_Manager.parquet_artifacts import inspect_parquet_artifact_contract

        artifact_contract = inspect_parquet_artifact_contract(
            parquet_root,
            table_names=sorted(table_files),
            require_schema_manifest=bool(args.require_schema_manifest),
            require_id_compaction=bool(args.require_id_compaction),
            strict_schema_manifest=bool(args.strict_schema_manifest),
        )
        report.set_artifact("parquet_artifact_contract", artifact_contract)
        if artifact_contract.get("status") == "failed":
            report.error(
                stage="parquet_materialize.artifact_contract",
                message="Selected parquet artifacts failed schema manifest contract preflight",
            )
            report.set_artifact("progress_path", str(progress_path))
            _save_report(report, report_path)
            raise SystemExit("Selected parquet artifacts failed schema manifest contract preflight; see report for details")
    except SystemExit:
        raise
    except Exception as exc:
        report.exception(
            stage="parquet_materialize.artifact_contract",
            message="Failed to inspect parquet artifact contract",
            exc=exc,
        )
        report.set_artifact("progress_path", str(progress_path))
        _save_report(report, report_path)
        raise
    parquet_preflight = _inspect_parquet_tables(
        table_files,
        key_sep=str(data_config.get("KEY_SEP") or "__"),
        extra_column_name=str(data_config.get("extra_column_name") or "__extra__"),
    )
    report.set_artifact("parquet_preflight", parquet_preflight)
    if parquet_preflight.get("status") != "ok":
        report.error(
            stage="parquet_materialize.parquet_preflight",
            message="Selected Parquet files failed metadata preflight; aborting before DB reset/load",
        )
        report.set_artifact("progress_path", str(progress_path))
        _save_report(report, report_path)
        raise SystemExit("Selected Parquet files failed metadata preflight; see report for details")
    reset_tables = {table_dir.name for table_dir in table_dirs} if bool(args.reset_selected_tables) else set()
    target_tables: list[str] = []
    if not reset_tables:
        _require_no_pending_reset(state, progress_path=progress_path)
    if reset_tables:
        if not selected_tables:
            raise SystemExit("--reset-selected-tables requires at least one --table")
        target_tables = [f"{str(args.table_prefix)}{table_name}" for table_name in sorted(reset_tables)]
        _require_drop_confirmation(target_tables, str(args.confirm_drop_tables))
        report.set_artifact("reset_selected_target_tables", target_tables)

    if _should_run_load_data_preflight(
        load_method=str(args.load_method),
        staging_writer=str(args.staging_writer),
        skip_preflight=bool(args.skip_load_data_preflight),
    ):
        try:
            _run_duckdb_load_data_preflight(db_config, staging_dir=staging_dir, report=report)
        except Exception as exc:
            report.exception(
                stage="parquet_materialize.load_data_preflight",
                message="LOAD DATA runtime dialect preflight failed",
                exc=exc,
                staging_writer=str(args.staging_writer),
                staging_dir=staging_dir,
            )
            report.set_artifact("progress_path", str(progress_path))
            _save_report(report, report_path)
            raise

    if reset_tables:
        _progress_prepare_session(
            state,
            parquet_root=parquet_root,
            table_files=table_files,
            selected_tables=selected_tables,
            reset_tables=reset_tables,
        )
        _progress_mark_reset_pending(state, target_tables=target_tables)
        _progress_write(progress_path, state, state_lock)
        _drop_target_tables(db_config, target_tables)
        _progress_mark_reset_completed(state)
        _progress_write(progress_path, state, state_lock)
    else:
        _progress_prepare_session(
            state,
            parquet_root=parquet_root,
            table_files=table_files,
            selected_tables=selected_tables,
            reset_tables=reset_tables,
        )
        _progress_write(progress_path, state, state_lock)

    session_tables_done: set[str] = set()
    worker_results: list[dict[str, Any]] = []

    try:
        if int(args.parallel_tables) <= 1:
            for table_dir in table_dirs:
                table_original = table_dir.name
                result = _materialize_one_table(
                    table_original=table_original,
                    files=table_files.get(table_original, []),
                    table_preflight=(parquet_preflight.get("tables") or {}).get(table_original, {}),
                    completed_files=set(state.get("completed_files", {}).get(table_original, [])),
                    data_config=data_config,
                    db_config=db_config,
                    load_method=str(args.load_method),
                    limit_rows_per_file=int(args.limit_rows_per_file),
                    table_prefix=str(args.table_prefix),
                    progress_path=progress_path,
                    state=state,
                    state_lock=state_lock,
                    keep_going=bool(args.keep_going),
                    load_data_staging_writer=str(args.staging_writer),
                    load_data_staging_dir=staging_dir,
                    file_chunk_rows=int(args.file_chunk_rows),
                    parallel_files_per_table=int(args.parallel_files_per_table),
                )
                worker_results.append(result)
                if result.get("files"):
                    session_tables_done.add(table_original)
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=int(args.parallel_tables)) as ex:
                fut_map = {
                    ex.submit(
                        _materialize_one_table,
                        table_original=table_dir.name,
                        files=table_files.get(table_dir.name, []),
                        table_preflight=(parquet_preflight.get("tables") or {}).get(table_dir.name, {}),
                        completed_files=set(state.get("completed_files", {}).get(table_dir.name, [])),
                        data_config=data_config,
                        db_config=db_config,
                        load_method=str(args.load_method),
                        limit_rows_per_file=int(args.limit_rows_per_file),
                        table_prefix=str(args.table_prefix),
                        progress_path=progress_path,
                        state=state,
                        state_lock=state_lock,
                        keep_going=bool(args.keep_going),
                        load_data_staging_writer=str(args.staging_writer),
                        load_data_staging_dir=staging_dir,
                        file_chunk_rows=int(args.file_chunk_rows),
                        parallel_files_per_table=int(args.parallel_files_per_table),
                    ): table_dir.name
                    for table_dir in table_dirs
                }
                for fut in concurrent.futures.as_completed(fut_map):
                    table_original = fut_map[fut]
                    result = fut.result()
                    worker_results.append(result)
                    if result.get("files"):
                        session_tables_done.add(table_original)

        for result in worker_results:
            _merge_worker_result(report, result)

        with state_lock:
            state["active"] = {}
            state["updated_at_utc"] = _iso_now()
            _progress_sync_current(state)
            _write_json(progress_path, state)

        report.set_artifact("tables_completed_session", sorted(session_tables_done))
        report.set_artifact("per_table", worker_results)
        report.set_artifact("progress_path", str(progress_path))
        _save_report(report, report_path)
        print(f"progress: {progress_path}")
        print(f"report: {report_path}")
        return 0
    finally:
        pass


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
