#!/usr/bin/env python3
"""
Dedup helper for DB tables produced by JSON ingest.

Supported modes:
- exact mode:
  - create a temporary clone with the same schema
  - insert DISTINCT rows using all columns
  - atomically swap the table names
  - keep the original table as a backup unless explicitly dropped later
  - intended for small/medium tables
- keyed mode:
  - create a temporary clone with a UNIQUE index on a stable dedup key
  - copy rows in committed batches ordered by that key
  - support resume via a JSON state file
  - intended for very large tables where a single DISTINCT statement is unsafe
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pymysql

DEFAULT_BATCH_SIZE = 200_000


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_db_config(run_dir: Path) -> dict[str, Any]:
    cfg = _read_json(run_dir / "config.json")
    return dict(cfg.get("db_config") or {})


def _qi(name: str) -> str:
    return str(name).replace("`", "``")


def _name_with_suffix(base: str, suffix: str, *, max_len: int = 64) -> str:
    candidate = f"{base}{suffix}"
    if len(candidate) <= max_len:
        return candidate

    digest = hashlib.md5(candidate.encode("utf-8")).hexdigest()[:8]
    keep = max_len - len(suffix) - len(digest) - 1
    if keep < 8:
        trimmed = base[: max_len - len(digest) - 1].rstrip("_")
        return f"{trimmed}_{digest}"
    trimmed = base[:keep].rstrip("_")
    return f"{trimmed}_{digest}{suffix}"


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(message: str) -> None:
    print(f"{_utc_now()} {message}", flush=True)


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


def _column_names(cur, *, schema: str, table: str) -> list[str]:
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


def _count_rows(cur, *, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM `{_qi(table)}`")
    row = cur.fetchone()
    return int(row[0]) if row else 0


def _table_exists(cur, *, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema=%s AND table_name=%s
        LIMIT 1
        """,
        (schema, table),
    )
    return cur.fetchone() is not None


def _table_rows_estimate(cur, *, schema: str, table: str) -> int | None:
    cur.execute(
        """
        SELECT table_rows
        FROM information_schema.tables
        WHERE table_schema=%s AND table_name=%s
        """,
        (schema, table),
    )
    row = cur.fetchone()
    if not row:
        return None
    try:
        return int(row[0]) if row[0] is not None else None
    except Exception:
        return None


def _first_index_for_column(cur, *, schema: str, table: str, column: str) -> str | None:
    cur.execute(
        """
        SELECT index_name
        FROM information_schema.statistics
        WHERE table_schema=%s AND table_name=%s AND column_name=%s
        ORDER BY non_unique ASC, seq_in_index ASC, index_name ASC
        LIMIT 1
        """,
        (schema, table, column),
    )
    row = cur.fetchone()
    return str(row[0]) if row and row[0] else None


def _single_column_non_unique_indexes(cur, *, schema: str, table: str, column: str) -> list[str]:
    cur.execute(
        """
        SELECT index_name
        FROM information_schema.statistics
        WHERE table_schema=%s AND table_name=%s
        GROUP BY index_name
        HAVING MIN(non_unique) = 1
           AND COUNT(*) = 1
           AND MIN(column_name) = %s
        ORDER BY index_name
        """,
        (schema, table, column),
    )
    return [str(row[0]) for row in cur.fetchall() if row and row[0]]


def _count_blank_keys(cur, *, table: str, column: str, index_name: str) -> int:
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM `{_qi(table)}` FORCE INDEX (`{_qi(index_name)}`)
        WHERE `{_qi(column)}` IS NULL OR `{_qi(column)}` = ''
        """
    )
    row = cur.fetchone()
    return int(row[0]) if row else 0


def _next_key_batch_upper(
    cur,
    *,
    table: str,
    key_col: str,
    index_name: str,
    last_key: str | None,
    batch_size: int,
) -> tuple[str | None, int]:
    where_sql = ""
    params: list[Any] = []
    if last_key is not None:
        where_sql = f"WHERE `{_qi(key_col)}` > %s"
        params.append(last_key)
    params.append(int(batch_size))
    cur.execute(
        f"""
        SELECT MAX(`{_qi(key_col)}`) AS upper_key, COUNT(*) AS sampled_rows
        FROM (
            SELECT `{_qi(key_col)}`
            FROM `{_qi(table)}` FORCE INDEX (`{_qi(index_name)}`)
            {where_sql}
            ORDER BY `{_qi(key_col)}`
            LIMIT %s
        ) AS batch_keys
        """,
        tuple(params),
    )
    row = cur.fetchone()
    upper_key = str(row[0]) if row and row[0] is not None else None
    sampled_rows = int(row[1]) if row and row[1] is not None else 0
    return upper_key, sampled_rows


def _insert_key_batch(
    cur,
    *,
    base_table: str,
    tmp_table: str,
    cols_sql: str,
    key_col: str,
    index_name: str,
    last_key: str | None,
    upper_key: str,
) -> int:
    if last_key is None:
        where_sql = f"`{_qi(key_col)}` <= %s"
        params: tuple[Any, ...] = (upper_key,)
    else:
        where_sql = f"`{_qi(key_col)}` > %s AND `{_qi(key_col)}` <= %s"
        params = (last_key, upper_key)
    cur.execute(
        f"""
        INSERT IGNORE INTO `{_qi(tmp_table)}` ({cols_sql})
        SELECT {cols_sql}
        FROM `{_qi(base_table)}` FORCE INDEX (`{_qi(index_name)}`)
        WHERE {where_sql}
        ORDER BY `{_qi(key_col)}`
        """,
        params,
    )
    return int(cur.rowcount or 0)


def _dedup_exact(
    cur,
    *,
    schema: str,
    base_table: str,
    backup_table: str,
    tmp_table: str,
) -> int:
    cols = _column_names(cur, schema=schema, table=base_table)
    if not cols:
        raise ValueError(f"table not found or has no columns: {base_table}")

    cur.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema=%s AND table_name IN (%s, %s)
        """,
        (schema, backup_table, tmp_table),
    )
    if int(cur.fetchone()[0]) != 0:
        raise ValueError(f"backup/tmp table already exists: backup={backup_table}, tmp={tmp_table}")

    _log(f"[dedup] table={base_table}")
    _log("[dedup] count rows before")
    total_before = _count_rows(cur, table=base_table)
    col_sql = ", ".join(f"`{_qi(c)}`" for c in cols)

    _log(f"[dedup] rows_before={total_before}")
    _log(f"[dedup] create tmp table: {tmp_table}")
    cur.execute(f"CREATE TABLE `{_qi(tmp_table)}` LIKE `{_qi(base_table)}`")

    _log("[dedup] insert distinct rows")
    cur.execute(f"INSERT INTO `{_qi(tmp_table)}` ({col_sql}) SELECT DISTINCT {col_sql} FROM `{_qi(base_table)}`")

    total_after = _count_rows(cur, table=tmp_table)
    removed = int(total_before) - int(total_after)
    _log(f"[dedup] rows_after={total_after}")
    _log(f"[dedup] rows_removed={removed}")

    _log(f"[dedup] swap tables: {base_table} -> {backup_table}, {tmp_table} -> {base_table}")
    cur.execute(
        f"RENAME TABLE `{_qi(base_table)}` TO `{_qi(backup_table)}`, `{_qi(tmp_table)}` TO `{_qi(base_table)}`"
    )
    _log(f"[dedup] done: table={base_table}, backup={backup_table}, removed={removed}")
    return 0


def _dedup_keyed(
    cur,
    *,
    run_dir: Path,
    schema: str,
    base_table: str,
    backup_table: str,
    tmp_table: str,
    dedup_key: str,
    batch_size: int,
    exact_count_before: bool,
    max_batches: int | None,
    skip_swap: bool,
    state_path: Path,
    reset_tmp: bool,
) -> int:
    cols = _column_names(cur, schema=schema, table=base_table)
    if dedup_key not in cols:
        raise ValueError(f"dedup key column not found: {dedup_key}")
    if _table_exists(cur, schema=schema, table=backup_table):
        raise ValueError(f"backup table already exists: {backup_table}")

    index_name = _first_index_for_column(cur, schema=schema, table=base_table, column=dedup_key)
    if not index_name:
        raise ValueError(f"dedup key must be indexed for keyed mode: table={base_table}, key={dedup_key}")

    blank_keys = _count_blank_keys(cur, table=base_table, column=dedup_key, index_name=index_name)
    if blank_keys:
        raise ValueError(
            f"keyed dedup requires populated keys; found blank/null {dedup_key} rows={blank_keys}"
        )

    col_sql = ", ".join(f"`{_qi(c)}`" for c in cols)
    unique_index_name = _name_with_suffix(tmp_table, f"__uq_{dedup_key}")
    state = _read_state(state_path)

    _log(f"[dedup] table={base_table}")
    _log(f"[dedup] mode=keyed key={dedup_key} batch_size={batch_size} index={index_name}")
    _log(f"[dedup] state_file={state_path}")
    estimate_before = _table_rows_estimate(cur, schema=schema, table=base_table)
    if estimate_before is not None:
        _log(f"[dedup] rows_before_estimate={estimate_before}")

    if reset_tmp:
        if _table_exists(cur, schema=schema, table=tmp_table):
            _log(f"[dedup] drop existing tmp table: {tmp_table}")
            cur.execute(f"DROP TABLE `{_qi(tmp_table)}`")
        if state_path.exists():
            _log(f"[dedup] remove existing state file: {state_path}")
            state_path.unlink()
        state = {}

    if not _table_exists(cur, schema=schema, table=tmp_table):
        _log(f"[dedup] create tmp table: {tmp_table}")
        cur.execute(f"CREATE TABLE `{_qi(tmp_table)}` LIKE `{_qi(base_table)}`")
        for stale_index_name in _single_column_non_unique_indexes(
            cur,
            schema=schema,
            table=tmp_table,
            column=dedup_key,
        ):
            _log(f"[dedup] drop redundant tmp index: {stale_index_name}")
            cur.execute(f"ALTER TABLE `{_qi(tmp_table)}` DROP INDEX `{_qi(stale_index_name)}`")
        cur.execute(
            f"ALTER TABLE `{_qi(tmp_table)}` ADD UNIQUE KEY `{_qi(unique_index_name)}` (`{_qi(dedup_key)}`)"
        )
        state = {
            "schema": schema,
            "table": base_table,
            "tmp_table": tmp_table,
            "backup_table": backup_table,
            "strategy": "keyed",
            "dedup_key": dedup_key,
            "index_name": index_name,
            "batch_size": int(batch_size),
            "rows_before_estimate": estimate_before,
            "rows_before_exact": None,
            "batches_done": 0,
            "inserted_rows": 0,
            "last_key": None,
            "started_at_utc": _utc_now(),
            "updated_at_utc": _utc_now(),
            "status": "running",
        }
        _write_state(state_path, state)
    else:
        if not state:
            raise ValueError(f"tmp table exists without state file: tmp={tmp_table}, state={state_path}")
        if str(state.get("schema") or "") != schema:
            raise ValueError(f"state schema mismatch: {state_path}")
        if str(state.get("table") or "") != base_table:
            raise ValueError(f"state base table mismatch: {state_path}")
        if str(state.get("tmp_table") or "") != tmp_table:
            raise ValueError(f"state tmp table mismatch: {state_path}")
        if str(state.get("dedup_key") or "") != dedup_key:
            raise ValueError(f"state dedup key mismatch: {state_path}")
        _log(
            f"[dedup] resume tmp={tmp_table} batches_done={int(state.get('batches_done') or 0)} "
            f"last_key={state.get('last_key')!r}"
        )

    if exact_count_before and state.get("rows_before_exact") is None:
        _log("[dedup] count rows before")
        state["rows_before_exact"] = _count_rows(cur, table=base_table)
        state["updated_at_utc"] = _utc_now()
        _write_state(state_path, state)
        _log(f"[dedup] rows_before={state['rows_before_exact']}")

    last_key_raw = state.get("last_key")
    last_key = str(last_key_raw) if last_key_raw is not None else None
    batches_done = int(state.get("batches_done") or 0)
    inserted_rows = int(state.get("inserted_rows") or 0)
    batches_this_run = 0

    while True:
        if max_batches is not None and batches_this_run >= max_batches:
            state["updated_at_utc"] = _utc_now()
            state["status"] = "paused_max_batches"
            _write_state(state_path, state)
            _log(f"[dedup] paused after max_batches={max_batches}")
            return 0

        upper_key, sampled_rows = _next_key_batch_upper(
            cur,
            table=base_table,
            key_col=dedup_key,
            index_name=index_name,
            last_key=last_key,
            batch_size=batch_size,
        )
        if upper_key is None:
            break

        inserted = _insert_key_batch(
            cur,
            base_table=base_table,
            tmp_table=tmp_table,
            cols_sql=col_sql,
            key_col=dedup_key,
            index_name=index_name,
            last_key=last_key,
            upper_key=upper_key,
        )
        last_key = upper_key
        batches_done += 1
        batches_this_run += 1
        inserted_rows += inserted
        state.update(
            {
                "batches_done": batches_done,
                "inserted_rows": inserted_rows,
                "last_key": last_key,
                "last_batch_upper_key": upper_key,
                "last_batch_sampled_rows": int(sampled_rows),
                "last_batch_inserted_rows": int(inserted),
                "updated_at_utc": _utc_now(),
                "status": "running",
            }
        )
        _write_state(state_path, state)
        _log(
            f"[dedup] batch={batches_done} sampled_rows={sampled_rows} "
            f"inserted_rows={inserted} last_key={upper_key}"
        )

    _log("[dedup] count rows after")
    rows_after = _count_rows(cur, table=tmp_table)
    rows_before_exact = state.get("rows_before_exact")
    rows_removed = None
    if rows_before_exact is not None:
        rows_removed = int(rows_before_exact) - int(rows_after)
    state.update(
        {
            "rows_after_exact": rows_after,
            "rows_removed_exact": rows_removed,
            "finished_copy_at_utc": _utc_now(),
            "updated_at_utc": _utc_now(),
        }
    )
    _write_state(state_path, state)
    _log(f"[dedup] rows_after={rows_after}")
    if rows_removed is not None:
        _log(f"[dedup] rows_removed={rows_removed}")

    if skip_swap:
        state["status"] = "ready_to_swap"
        state["updated_at_utc"] = _utc_now()
        _write_state(state_path, state)
        _log("[dedup] skip swap enabled; tmp table is ready")
        return 0

    _log(f"[dedup] swap tables: {base_table} -> {backup_table}, {tmp_table} -> {base_table}")
    cur.execute(
        f"RENAME TABLE `{_qi(base_table)}` TO `{_qi(backup_table)}`, `{_qi(tmp_table)}` TO `{_qi(base_table)}`"
    )
    state["status"] = "swapped"
    state["swapped_at_utc"] = _utc_now()
    state["updated_at_utc"] = _utc_now()
    _write_state(state_path, state)
    _log(f"[dedup] done: table={base_table}, backup={backup_table}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir", help="runs/<run_id_dir>")
    ap.add_argument("table", help="SQL table name to deduplicate")
    ap.add_argument(
        "--backup-table",
        help="Explicit backup table name; default is <table>__bak_<utcstamp>",
    )
    ap.add_argument(
        "--tmp-table",
        help="Explicit temporary table name; default is <table>__dedup_tmp",
    )
    ap.add_argument("--dedup-key", default="", help="Use keyed batched dedup on this indexed column")
    ap.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Keyed dedup batch size in source-key rows")
    ap.add_argument("--max-batches", type=int, default=None, help="Stop after N keyed batches without swapping")
    ap.add_argument("--skip-swap", action="store_true", help="Populate tmp table but do not rename/swap")
    ap.add_argument("--reset-tmp", action="store_true", help="Drop existing tmp table and keyed state before starting")
    ap.add_argument(
        "--state-file",
        default="",
        help="Explicit state json path for keyed mode; default is <run_dir>/dedup_<table>.state.json",
    )
    ap.add_argument(
        "--exact-count-before",
        action="store_true",
        help="In keyed mode, do an exact COUNT(*) on the source table before copying",
    )
    args = ap.parse_args()

    run_dir = Path(args.run_dir).expanduser().resolve()
    db_config = _load_db_config(run_dir)
    schema = str(db_config.get("database") or "").strip()
    if not schema:
        raise ValueError("db_config.database is required")

    base_table = str(args.table).strip()
    if not base_table:
        raise ValueError("table is required")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_table = str(args.backup_table or _name_with_suffix(base_table, f"__bak_{stamp}"))
    tmp_table = str(args.tmp_table or _name_with_suffix(base_table, "__dedup_tmp"))
    dedup_key = str(args.dedup_key or "").strip()
    batch_size = int(args.batch_size)
    state_path = (
        Path(args.state_file).expanduser().resolve()
        if str(args.state_file or "").strip()
        else (run_dir / f"dedup_{base_table}.state.json")
    )

    if backup_table == base_table or tmp_table == base_table or backup_table == tmp_table:
        raise ValueError("table, backup table, and tmp table names must be distinct")
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    conn = pymysql.connect(**db_config, autocommit=True)
    cur = conn.cursor()
    try:
        if dedup_key:
            return _dedup_keyed(
                cur,
                run_dir=run_dir,
                schema=schema,
                base_table=base_table,
                backup_table=backup_table,
                tmp_table=tmp_table,
                dedup_key=dedup_key,
                batch_size=batch_size,
                exact_count_before=bool(args.exact_count_before),
                max_batches=args.max_batches,
                skip_swap=bool(args.skip_swap),
                state_path=state_path,
                reset_tmp=bool(args.reset_tmp),
            )
        return _dedup_exact(
            cur,
            schema=schema,
            base_table=base_table,
            backup_table=backup_table,
            tmp_table=tmp_table,
        )
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
