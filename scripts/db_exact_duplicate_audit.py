#!/usr/bin/env python3
"""
Audit exact duplicate rows per table for a MariaDB schema.

Definition:
- "exact duplicate" means every column value in the row matches another row in the
  same table, with NULL semantics handled by SQL GROUP BY.

Output:
- writes one JSON object per table to the output file
- keeps a small state file so interrupted runs can resume
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pymysql


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(message: str) -> None:
    print(f"{_utc_now()} {message}", flush=True)


def _qi(name: str) -> str:
    return str(name).replace("`", "``")


def _connect(args: argparse.Namespace):
    return pymysql.connect(
        host=args.host,
        port=int(args.port),
        user=args.user,
        password=args.password,
        database=args.schema,
        autocommit=True,
        charset="utf8mb4",
    )


def _read_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp_path, path)


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _load_base_tables(cur, *, schema: str) -> list[tuple[str, int | None]]:
    cur.execute(
        """
        SELECT table_name, table_rows
        FROM information_schema.tables
        WHERE table_schema=%s AND table_type='BASE TABLE'
        ORDER BY table_name
        """,
        (schema,),
    )
    rows = []
    for table_name, table_rows in cur.fetchall():
        rows.append((str(table_name), int(table_rows) if table_rows is not None else None))
    return rows


def _load_columns(cur, *, schema: str, table: str) -> list[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    return [str(row[0]) for row in cur.fetchall()]


def _duplicate_summary(cur, *, table: str, columns: list[str]) -> tuple[int, int]:
    cols_sql = ", ".join(f"`{_qi(col)}`" for col in columns)
    cur.execute(
        f"""
        SELECT
          COALESCE(SUM(group_count - 1), 0) AS duplicate_rows,
          COUNT(*) AS duplicate_groups
        FROM (
          SELECT COUNT(*) AS group_count
          FROM `{_qi(table)}`
          GROUP BY {cols_sql}
          HAVING COUNT(*) > 1
        ) AS duplicate_groups
        """
    )
    row = cur.fetchone()
    duplicate_rows = int(row[0] or 0) if row else 0
    duplicate_groups = int(row[1] or 0) if row else 0
    return duplicate_rows, duplicate_groups


def _duplicate_sample(cur, *, table: str, columns: list[str], max_value_len: int = 200) -> dict[str, Any] | None:
    cols_sql = ", ".join(f"`{_qi(col)}`" for col in columns)
    cur.execute(
        f"""
        SELECT {cols_sql}, COUNT(*) AS duplicate_count
        FROM `{_qi(table)}`
        GROUP BY {cols_sql}
        HAVING COUNT(*) > 1
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if row is None:
        return None
    sample: dict[str, Any] = {}
    for idx, col in enumerate(columns):
        value = row[idx]
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", errors="replace")
        if isinstance(value, str) and len(value) > max_value_len:
            value = value[:max_value_len] + "...<truncated>"
        sample[col] = value
    sample["duplicate_count"] = int(row[len(columns)] or 0)
    return sample


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", required=True)
    ap.add_argument("--host", default=os.environ.get("MARIADB_BIND_IP", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("MARIADB_BIND_PORT", "3306")))
    ap.add_argument("--user", default=os.environ.get("MARIADB_USER", "root"))
    ap.add_argument("--password", default=os.environ.get("MARIADB_PASSWORD", ""))
    ap.add_argument("--output-jsonl", required=True)
    ap.add_argument("--state-file", required=True)
    ap.add_argument("--table", action="append", default=[], help="Only audit the given table name; repeatable")
    ap.add_argument("--sample", action="store_true", help="Fetch one duplicate sample row for tables that have duplicates")
    ap.add_argument("--reset", action="store_true", help="Reset state/output before starting")
    args = ap.parse_args()

    if not args.password:
        raise SystemExit("password is required via --password or environment")

    output_path = Path(args.output_jsonl).expanduser().resolve()
    state_path = Path(args.state_file).expanduser().resolve()

    if args.reset:
        if output_path.exists():
            output_path.unlink()
        if state_path.exists():
            state_path.unlink()

    state = _read_state(state_path)
    completed = set(str(x) for x in (state.get("completed_tables") or []))

    conn = _connect(args)
    cur = conn.cursor()
    try:
        tables = _load_base_tables(cur, schema=args.schema)
        if args.table:
            selected = set(str(x) for x in args.table)
            tables = [(table_name, est_rows) for table_name, est_rows in tables if table_name in selected]
        _log(f"[audit] schema={args.schema} base_tables={len(tables)}")
        for table_name, est_rows in tables:
            if table_name in completed:
                _log(f"[audit] skip completed table={table_name}")
                continue
            columns = _load_columns(cur, schema=args.schema, table=table_name)
            if not columns:
                payload = {
                    "schema": args.schema,
                    "table": table_name,
                    "estimated_rows": est_rows,
                    "status": "no_columns",
                    "audited_at_utc": _utc_now(),
                }
                _append_jsonl(output_path, payload)
                completed.add(table_name)
                state = {"schema": args.schema, "completed_tables": sorted(completed), "updated_at_utc": _utc_now()}
                _write_state(state_path, state)
                continue

            _log(f"[audit] start table={table_name} estimated_rows={est_rows}")
            duplicate_rows, duplicate_groups = _duplicate_summary(cur, table=table_name, columns=columns)
            payload = {
                "schema": args.schema,
                "table": table_name,
                "estimated_rows": est_rows,
                "column_count": len(columns),
                "duplicate_rows": duplicate_rows,
                "duplicate_groups": duplicate_groups,
                "audited_at_utc": _utc_now(),
            }
            if args.sample and duplicate_rows > 0:
                payload["sample"] = _duplicate_sample(cur, table=table_name, columns=columns)
            _append_jsonl(output_path, payload)
            completed.add(table_name)
            state = {"schema": args.schema, "completed_tables": sorted(completed), "updated_at_utc": _utc_now()}
            _write_state(state_path, state)
            _log(f"[audit] done table={table_name} duplicate_rows={duplicate_rows} duplicate_groups={duplicate_groups}")
        _log("[audit] completed")
        return 0
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
