#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Iterable

import pymysql


DEFAULT_INDEX_SPECS: list[tuple[str, tuple[str, ...]]] = [
    ("idx_core_lang_year_work", ("source_is_core", "language", "publication_year", "work_id")),
    (
        "idx_cwts_exact_core",
        (
            "source_is_core",
            "language",
            "type",
            "source_type",
            "is_retracted",
            "is_paratext",
            "publication_year",
            "work_id",
        ),
    ),
    ("idx_doi_norm", ("doi_norm",)),
    ("idx_field_year", ("field_id", "publication_year", "work_id")),
]


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Add practical secondary indexes to openalex_works_meta.")
    ap.add_argument("run_dir", help="runs/<run_id_dir>")
    ap.add_argument("--table", default="openalex_works_meta")
    return ap.parse_args()


def _load_db_config(run_dir: Path) -> dict:
    config_path = run_dir / "config.json"
    data = json.loads(config_path.read_text(encoding="utf-8"))
    db_config = dict(data["db_config"])
    db_config.setdefault("charset", "utf8mb4")
    db_config.setdefault("autocommit", True)
    return db_config


def _index_exists(cur, *, table: str, index_name: str) -> bool:
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


def _table_exists(cur, *, table: str) -> bool:
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


def _emit(event: str, **payload) -> None:
    data = {"event": event, **payload}
    print(json.dumps(data, ensure_ascii=False), flush=True)


def _analyze_tables(cur, *, tables: Iterable[str]) -> None:
    for table in tables:
        started = time.time()
        _emit("analyze_table_start", table=table)
        cur.execute(f"ANALYZE TABLE `{table}`")
        _emit("analyze_table_done", table=table, seconds=round(time.time() - started, 3))


def _create_indexes(cur, *, table: str, specs: Iterable[tuple[str, tuple[str, ...]]]) -> None:
    for index_name, columns in specs:
        if _index_exists(cur, table=table, index_name=index_name):
            _emit("skip_index_exists", table=table, index_name=index_name)
            continue
        cols_sql = ", ".join(f"`{column}`" for column in columns)
        sql = f"CREATE INDEX `{index_name}` ON `{table}` ({cols_sql})"
        started = time.time()
        _emit("create_index_start", table=table, index_name=index_name, columns=list(columns), sql=sql)
        cur.execute(sql)
        _emit(
            "create_index_done",
            table=table,
            index_name=index_name,
            seconds=round(time.time() - started, 3),
        )


def main() -> int:
    args = _parse_args()
    run_dir = Path(args.run_dir).expanduser().resolve()
    db_config = _load_db_config(run_dir)

    conn = pymysql.connect(**db_config)
    try:
        with conn.cursor() as cur:
            if not _table_exists(cur, table=str(args.table)):
                raise SystemExit(f"Table not found: {args.table}")
            _emit("table_ready", table=str(args.table), database=db_config.get("database"))
            _create_indexes(cur, table=str(args.table), specs=DEFAULT_INDEX_SPECS)
            _analyze_tables(cur, tables=[str(args.table), "openalex_works_text"])
            _emit("all_done", table=str(args.table))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
