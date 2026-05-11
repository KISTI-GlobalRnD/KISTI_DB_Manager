#!/usr/bin/env python3
"""
Export OA GCC materialized tables to local parquet files.

Why this exists:
- The current GCC prep path materializes `openalex_works_meta`, `openalex_works_text`,
  and `openalex_refs` inside MariaDB.
- Downstream filtering / embedding / graph work is better served from local parquet.
- Re-reading the already materialized tables is the fastest path to local artifacts
  without restarting the raw OpenAlex transform from scratch.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pymysql

from oa_materialize_gcc_inputs import (
    META_COLUMNS,
    REFS_COLUMNS,
    TEXT_COLUMNS,
    _load_db_config,
    _read_state,
    _write_state,
)


DEFAULT_META_ROWS_PER_FILE = 500_000
DEFAULT_TEXT_ROWS_PER_FILE = 100_000
DEFAULT_REFS_ROWS_PER_FILE = 1_000_000


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _connect_dict(db_config: dict[str, Any]):
    kwargs = dict(db_config)
    kwargs["autocommit"] = True
    kwargs["charset"] = kwargs.get("charset") or "utf8mb4"
    kwargs["cursorclass"] = pymysql.cursors.DictCursor
    return pymysql.connect(**kwargs)


def _ensure_pyarrow() -> None:
    try:
        import pyarrow  # noqa: F401
    except Exception as exc:
        raise SystemExit(f"pyarrow is required for parquet export: {exc}") from exc


def _table_out_dir(export_root: Path, table: str) -> Path:
    path = export_root / table
    path.mkdir(parents=True, exist_ok=True)
    return path


def _part_path(table_dir: Path, part_number: int) -> Path:
    return table_dir / f"part-{int(part_number):06d}.parquet"


def _write_part(
    rows: list[dict[str, Any]],
    *,
    columns: list[str],
    dst: Path,
    compression: str,
) -> int:
    frame = pd.DataFrame.from_records(rows, columns=columns)
    tmp = dst.with_suffix(".parquet.tmp")
    frame.to_parquet(tmp, index=False, compression=compression)
    tmp.replace(dst)
    return int(len(frame))


def _export_ordered_single_key_table(
    conn,
    *,
    export_root: Path,
    state_path: Path,
    state_key: str,
    table: str,
    columns: list[str],
    key_column: str,
    rows_per_file: int,
    compression: str,
    limit_parts: int | None,
) -> None:
    state = _read_state(state_path)
    part_state = dict(state.get(state_key) or {})
    if part_state.get("done") is True:
        return

    table_dir = _table_out_dir(export_root, table)
    part_number = int(part_state.get("part_number") or 0)
    rows_written = int(part_state.get("rows_written") or 0)
    last_key = str(part_state.get("last_key") or "").strip()
    parts_written_this_run = 0

    col_sql = ", ".join(f"`{col}`" for col in columns)

    while True:
        if limit_parts is not None and parts_written_this_run >= int(limit_parts):
            break

        sql = f"SELECT {col_sql} FROM `{table}`"
        params: list[Any] = []
        if last_key:
            sql += f" WHERE `{key_column}` > %s"
            params.append(last_key)
        sql += f" ORDER BY `{key_column}` LIMIT %s"
        params.append(int(rows_per_file))

        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = list(cur.fetchall() or [])

        if not rows:
            part_state.update(
                {
                    "table": table,
                    "status": "done",
                    "done": True,
                    "part_number": part_number,
                    "rows_written": rows_written,
                    "last_key": last_key,
                    "updated_at": _utc_now(),
                }
            )
            state[state_key] = part_state
            _write_state(state_path, state)
            return

        part_number += 1
        dst = _part_path(table_dir, part_number)
        written = _write_part(rows, columns=columns, dst=dst, compression=compression)
        last_key = str(rows[-1].get(key_column) or "")
        rows_written += written
        parts_written_this_run += 1

        part_state.update(
            {
                "table": table,
                "status": "running",
                "done": False,
                "part_number": part_number,
                "rows_written": rows_written,
                "last_key": last_key,
                "last_file": str(dst),
                "rows_per_file": int(rows_per_file),
                "updated_at": _utc_now(),
            }
        )
        state[state_key] = part_state
        _write_state(state_path, state)
        print(
            json.dumps(
                {
                    "part": state_key,
                    "table": table,
                    "part_number": part_number,
                    "rows_written": rows_written,
                    "last_key": last_key,
                    "last_file": str(dst),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )


def _export_refs_table(
    conn,
    *,
    export_root: Path,
    state_path: Path,
    table: str,
    rows_per_file: int,
    compression: str,
    limit_parts: int | None,
) -> None:
    state = _read_state(state_path)
    part_state = dict(state.get("refs") or {})
    if part_state.get("done") is True:
        return

    table_dir = _table_out_dir(export_root, table)
    part_number = int(part_state.get("part_number") or 0)
    rows_written = int(part_state.get("rows_written") or 0)
    last_citing = str(part_state.get("last_citing_work_id") or "").strip()
    last_cited = str(part_state.get("last_cited_work_id") or "").strip()
    parts_written_this_run = 0

    col_sql = ", ".join(f"`{col}`" for col in REFS_COLUMNS)
    while True:
        if limit_parts is not None and parts_written_this_run >= int(limit_parts):
            break

        sql = f"SELECT {col_sql} FROM `{table}`"
        params: list[Any] = []
        if last_citing and last_cited:
            sql += (
                " WHERE (`citing_work_id` > %s)"
                "    OR (`citing_work_id` = %s AND `cited_work_id` > %s)"
            )
            params.extend([last_citing, last_citing, last_cited])
        sql += " ORDER BY `citing_work_id`, `cited_work_id` LIMIT %s"
        params.append(int(rows_per_file))

        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = list(cur.fetchall() or [])

        if not rows:
            part_state.update(
                {
                    "table": table,
                    "status": "done",
                    "done": True,
                    "part_number": part_number,
                    "rows_written": rows_written,
                    "last_citing_work_id": last_citing,
                    "last_cited_work_id": last_cited,
                    "updated_at": _utc_now(),
                }
            )
            state["refs"] = part_state
            _write_state(state_path, state)
            return

        part_number += 1
        dst = _part_path(table_dir, part_number)
        written = _write_part(rows, columns=REFS_COLUMNS, dst=dst, compression=compression)
        last_row = rows[-1]
        last_citing = str(last_row.get("citing_work_id") or "")
        last_cited = str(last_row.get("cited_work_id") or "")
        rows_written += written
        parts_written_this_run += 1

        part_state.update(
            {
                "table": table,
                "status": "running",
                "done": False,
                "part_number": part_number,
                "rows_written": rows_written,
                "last_citing_work_id": last_citing,
                "last_cited_work_id": last_cited,
                "last_file": str(dst),
                "rows_per_file": int(rows_per_file),
                "updated_at": _utc_now(),
            }
        )
        state["refs"] = part_state
        _write_state(state_path, state)
        print(
            json.dumps(
                {
                    "part": "refs",
                    "table": table,
                    "part_number": part_number,
                    "rows_written": rows_written,
                    "last_citing_work_id": last_citing,
                    "last_cited_work_id": last_cited,
                    "last_file": str(dst),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )


def main() -> int:
    _ensure_pyarrow()

    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir", help="runs/<run_id_dir>")
    ap.add_argument("--part", choices=["meta", "text", "refs", "all"], default="all")
    ap.add_argument("--export-root", default="", help="Root directory for parquet export output")
    ap.add_argument("--state-dir", default="", help="Directory for exporter progress and logs")
    ap.add_argument("--meta-table", default="openalex_works_meta")
    ap.add_argument("--text-table", default="openalex_works_text")
    ap.add_argument("--refs-table", default="openalex_refs")
    ap.add_argument("--meta-rows-per-file", type=int, default=DEFAULT_META_ROWS_PER_FILE)
    ap.add_argument("--text-rows-per-file", type=int, default=DEFAULT_TEXT_ROWS_PER_FILE)
    ap.add_argument("--refs-rows-per-file", type=int, default=DEFAULT_REFS_ROWS_PER_FILE)
    ap.add_argument("--compression", default="snappy")
    ap.add_argument("--limit-parts", type=int, default=None, help="Export at most N parquet files for the selected part(s)")
    args = ap.parse_args()

    run_dir = Path(args.run_dir).expanduser().resolve()
    export_root = Path(args.export_root).expanduser().resolve() if args.export_root else (run_dir / "gcc_parquet")
    state_dir = Path(args.state_dir).expanduser().resolve() if args.state_dir else (export_root / "_state")
    export_root.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)
    state_path = state_dir / "progress.json"

    state = _read_state(state_path)
    state.setdefault("_export", {})
    state["_export"].update(
        {
            "run_dir": str(run_dir),
            "export_root": str(export_root),
            "compression": str(args.compression),
            "updated_at": _utc_now(),
        }
    )
    _write_state(state_path, state)

    db_config = _load_db_config(run_dir)
    conn = _connect_dict(db_config)
    try:
        if args.part in {"meta", "all"}:
            _export_ordered_single_key_table(
                conn,
                export_root=export_root,
                state_path=state_path,
                state_key="meta",
                table=str(args.meta_table),
                columns=META_COLUMNS,
                key_column="work_id",
                rows_per_file=int(args.meta_rows_per_file),
                compression=str(args.compression),
                limit_parts=args.limit_parts,
            )
        if args.part in {"text", "all"}:
            _export_ordered_single_key_table(
                conn,
                export_root=export_root,
                state_path=state_path,
                state_key="text",
                table=str(args.text_table),
                columns=TEXT_COLUMNS,
                key_column="work_id",
                rows_per_file=int(args.text_rows_per_file),
                compression=str(args.compression),
                limit_parts=args.limit_parts,
            )
        if args.part in {"refs", "all"}:
            _export_refs_table(
                conn,
                export_root=export_root,
                state_path=state_path,
                table=str(args.refs_table),
                rows_per_file=int(args.refs_rows_per_file),
                compression=str(args.compression),
                limit_parts=args.limit_parts,
            )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
