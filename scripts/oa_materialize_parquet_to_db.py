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
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from KISTI_DB_Manager import manage
from KISTI_DB_Manager.config import coerce_data_config, coerce_db_config
from KISTI_DB_Manager.namemap import NameMap
from KISTI_DB_Manager.report import RunReport


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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


def _stage_parquet_with_duckdb(
    *,
    parquet_file: Path,
    columns_original: list[str],
    stage_path: str,
    limit_rows: int,
    report: RunReport,
) -> None:
    import duckdb

    def dq(ident: str) -> str:
        return '"' + str(ident).replace('"', '""') + '"'

    select_sql = ", ".join(dq(c) for c in columns_original)
    limit_sql = f" LIMIT {int(limit_rows)}" if int(limit_rows or 0) > 0 else ""
    copy_sql = (
        f"COPY (SELECT {select_sql} FROM read_parquet({json.dumps(str(parquet_file))}){limit_sql}) "
        f"TO {json.dumps(str(stage_path))} "
        "(FORMAT CSV, HEADER FALSE, DELIMITER '\t', NULLSTR '\\N', QUOTE '\"', ESCAPE '\"');"
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
    staging_dir: str,
    report: RunReport,
) -> None:
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
            report=report,
        )
        manage._load_data_local_infile_tabular_file(
            conn=conn,
            table_name=table_name,
            file_path=stage_path,
            sep="\t",
            columns_expr=[f"`{str(c).replace('`', '``')}`" for c in columns_sql],
            ignore_lines=0,
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
        }
    return _read_json(path)


def _pick_table_dirs(root: Path, selected: list[str], max_tables: int | None) -> list[Path]:
    dirs = sorted([p for p in root.iterdir() if p.is_dir()])
    if selected:
        wanted = set(selected)
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
) -> None:
    with lock:
        active = state.setdefault("active", {})
        active[active_key] = {
            "table_original": table_original,
            "table_sql": table_sql,
            "file_path": str(parquet_file),
            "rows": rows,
        }
        state["updated_at_utc"] = _iso_now()
        _progress_sync_current(state)
        _write_json(progress_path, state)


def _progress_mark_done(
    progress_path: Path,
    state: dict[str, Any],
    lock: threading.Lock,
    *,
    active_key: str,
    table_original: str,
    parquet_file: Path,
    rows: int,
) -> None:
    with lock:
        done = state.setdefault("completed_files", {}).setdefault(table_original, [])
        if parquet_file.name not in done:
            done.append(parquet_file.name)
        state["files_loaded"] = int(state.get("files_loaded", 0) or 0) + 1
        state["rows_loaded"] = int(state.get("rows_loaded", 0) or 0) + int(rows)

        file_counts = state.get("table_file_counts") or {}
        completed_files = state.get("completed_files") or {}
        state["tables_completed"] = sum(
            1
            for table_name, total in file_counts.items()
            if int(total or 0) > 0 and len(completed_files.get(table_name, []) or []) >= int(total or 0)
        )

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
    nm: NameMap,
    existing_cols: set[str] | None,
) -> dict[str, Any]:
    import pandas as pd
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

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

    try:
        if fast_load_state.enabled:
            local_infile_conn = _connect_local_infile(db_config, fast_load_session=True, report=local_report)

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
        bump("parquet_rows_read", int(load_rows))

        _progress_activate(
            progress_path,
            state,
            state_lock,
            active_key=active_key,
            table_original=table_original,
            table_sql=target_table_sql,
            parquet_file=parquet_file,
            rows=int(load_rows),
        )

        direct_duckdb_ok = False
        current_nm = nm.with_additional_columns(table_columns, max_len=64)
        columns_original = [c for c in table_columns if c != extra_canon]
        columns_sql = [current_nm.map_column(c) for c in columns_original]
        if (
            str(load_data_staging_writer) == "duckdb"
            and local_infile_conn is not None
            and existing_cols is not None
            and all(col in existing_cols for col in columns_sql)
        ):
            t0 = time.perf_counter()
            with local_report.timer("db.load_data.total"):
                _load_parquet_file_via_duckdb_stage(
                    conn=local_infile_conn,
                    table_name=target_table_sql,
                    parquet_file=parquet_file,
                    columns_original=columns_original,
                    columns_sql=columns_sql,
                    limit_rows=int(limit_rows_per_file),
                    staging_dir=str(load_data_staging_dir or _default_staging_dir()),
                    report=local_report,
                )
            add_ms("parquet_materialize.load_file", time.perf_counter() - t0)
            bump("load_data_ok", 1)
            direct_duckdb_ok = True

        if not direct_duckdb_ok:
            t0 = time.perf_counter()
            df = pd.read_parquet(parquet_file)
            add_ms("parquet_materialize.read_parquet", time.perf_counter() - t0)
            if limit_rows_per_file and int(limit_rows_per_file) > 0:
                df = df.head(int(limit_rows_per_file)).copy()
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

        bump("files_loaded", 1)
        bump("rows_loaded", int(load_rows))
        result["files"].append(
            {
                "path": str(parquet_file),
                "rows": int(load_rows),
                "table_sql": target_table_sql,
            }
        )
        _progress_mark_done(
            progress_path,
            state,
            state_lock,
            active_key=active_key,
            table_original=table_original,
            parquet_file=parquet_file,
            rows=int(load_rows),
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
    parallel_files_per_table: int,
) -> dict[str, Any]:
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

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

    engine = None
    nm: NameMap | None = None
    existing_cols: set[str] | None = None

    try:
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

        target_table = f"{table_prefix}{table_original}"
        extra_column_name = str(data_config.get("extra_column_name") or "__extra__")
        extra_canon = extra_column_name.replace(".", str(data_config.get("KEY_SEP") or "__")) if extra_column_name else ""
        todo_files = [parquet_file for parquet_file in files if parquet_file.name not in completed_files]
        if not todo_files:
            return result

        t0 = time.perf_counter()
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
        if engine is not None:
            try:
                engine.dispose()
            except Exception:
                pass


def main() -> int:
    ap = argparse.ArgumentParser()
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
    ap.add_argument("--load-method", choices=["auto", "load_data", "to_sql"], default="load_data")
    ap.add_argument("--parallel-tables", type=int, default=1, help="Number of tables to materialize in parallel")
    ap.add_argument("--parallel-files-per-table", type=int, default=1, help="Number of parquet files to load in parallel within a table")
    ap.add_argument("--staging-writer", choices=["python", "duckdb"], default="duckdb")
    ap.add_argument("--staging-dir", default=None, help="Temp staging directory for LOAD DATA files")
    ap.add_argument("--keep-going", action="store_true", help="Continue with next file on error")
    args = ap.parse_args()

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

    work_dir = run_dir / "parquet_materialize"
    progress_path = Path(args.progress).expanduser().resolve() if args.progress else work_dir / "progress.json"
    report_path = Path(args.report).expanduser().resolve() if args.report else work_dir / "run_report.json"

    state = _load_progress(progress_path)
    state["parquet_root"] = str(parquet_root)
    state["updated_at_utc"] = _iso_now()

    selected_tables = [str(t).strip() for t in args.table if str(t).strip()]
    table_dirs = _pick_table_dirs(parquet_root, selected_tables, args.max_tables)
    state["table_count"] = len(table_dirs)

    report = RunReport()
    report.set_artifact("run_dir", str(run_dir))
    report.set_artifact("parquet_root", str(parquet_root))
    report.set_artifact("db_name", str(db_config.get("database") or ""))
    report.set_artifact("selected_tables", selected_tables)
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

    state_lock = threading.Lock()
    state.setdefault("completed_files", {})
    state.setdefault("active", {})
    table_files: dict[str, list[Path]] = {
        table_dir.name: _pick_files(table_dir, args.max_files_per_table, bool(args.latest_first))
        for table_dir in table_dirs
    }
    state["table_file_counts"] = {k: len(v) for k, v in table_files.items()}
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
        report.finish()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report.to_json(indent=2), encoding="utf-8")
        print(f"progress: {progress_path}")
        print(f"report: {report_path}")
        return 0
    finally:
        pass


if __name__ == "__main__":
    raise SystemExit(main())
