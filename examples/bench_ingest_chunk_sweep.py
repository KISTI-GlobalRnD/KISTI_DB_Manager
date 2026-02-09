#!/usr/bin/env python3
"""
Chunk-size / parallel_workers sweep runner for the JSON->DB pipeline.

Goals:
- Measure end-to-end wall time with consistent DB conditions (fresh DB per run)
- Compare serial (pw=0) vs parallel TSV backend (pw>1) under different chunk sizes

This script intentionally shells out to the CLI to keep behavior identical to
real runs (modes, flags, report generation).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _parse_int_list(s: str) -> list[int]:
    out: list[int] = []
    for part in (s or "").split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    return out


def _short_db_name(stamp: str, chunk: int, pw: int, rep: int) -> str:
    # Keep well under MariaDB's 64-char identifier limit.
    return f"kbs_{stamp}_c{chunk}_pw{pw}_r{rep}"


def _count_warnings(report: dict[str, Any]) -> int:
    n = 0
    for it in report.get("issues") or []:
        if isinstance(it, dict) and str(it.get("level") or "").lower() == "warning":
            n += 1
    return n


@dataclass(frozen=True)
class SweepRow:
    stamp: str
    input_path: str
    mode: str
    schema_mode: str
    chunk_size: int
    parallel_workers: int
    db_load_parallel_tables: int
    load_data_commit_strategy: str
    tsv_merge_union_schema: bool
    overlap_batches: bool
    rep: int
    db_name: str
    table_name: str
    report_path: str

    duration_s: float | None
    records_read: int | None
    batches_total: int | None
    pipeline_total_ms: int | None
    io_json_parse_ms: int | None
    json_flatten_ms: int | None
    db_tsv_write_ms: int | None
    flatten_plus_tsv_ms: int | None
    json_db_create_ms: int | None
    db_load_exec_ms: int | None
    db_load_total_ms: int | None
    warnings: int


def _db_admin_drop_create(
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
) -> None:
    import pymysql

    conn = pymysql.connect(
        host=host,
        port=int(port),
        user=user,
        password=password,
        autocommit=True,
        charset="utf8mb4",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS `{database}`")
            cur.execute(f"CREATE DATABASE `{database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")
    finally:
        conn.close()


def _db_admin_drop(
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
) -> None:
    import pymysql

    conn = pymysql.connect(
        host=host,
        port=int(port),
        user=user,
        password=password,
        autocommit=True,
        charset="utf8mb4",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS `{database}`")
    finally:
        conn.close()


def _run_one(
    *,
    repo_root: Path,
    input_path: Path,
    host: str,
    port: int,
    user: str,
    password: str,
    mode: str,
    schema_mode: str,
    chunk_size: int,
    parallel_workers: int,
    db_load_parallel_tables: int,
    load_data_commit_strategy: str,
    tsv_merge_union_schema: bool,
    overlap_batches: bool,
    rep: int,
    stamp: str,
    keep_db: bool,
    tmp_dir: Path,
) -> SweepRow:
    table_name = f"bench_sweep_c{int(chunk_size)}_pw{int(parallel_workers)}"
    db_name = _short_db_name(stamp, int(chunk_size), int(parallel_workers), int(rep))

    cfg_path = tmp_dir / f"bench_sweep_{db_name}_config.json"
    report_path = tmp_dir / f"bench_sweep_{db_name}_report.json"

    cfg = {
        "data_config": {
            "PATH": str(input_path.parent),
            "file_name": str(input_path.name),
            "file_type": "jsonl",
            "table_name": table_name,
            "KEY_SEP": "__",
            "index_key": "UID",
            "except_keys": [],
            "chunk_size": int(chunk_size),
            "db_load_method": "auto",
            "json_streaming_load": True,
            "fast_load_session": True,
            "db_load_parallel_tables": int(db_load_parallel_tables),
            "load_data_commit_strategy": str(load_data_commit_strategy),
            "tsv_merge_union_schema": bool(tsv_merge_union_schema),
            "overlap_batches": bool(overlap_batches),
        },
        "db_config": {
            "host": host,
            "user": user,
            "password": password,
            "database": db_name,
            "port": int(port),
        },
    }
    cfg_path.write_text(json.dumps(cfg, indent=2, sort_keys=False), encoding="utf-8")

    _db_admin_drop_create(host=host, port=int(port), user=user, password=password, database=db_name)

    cmd = [
        sys.executable,
        "-m",
        "KISTI_DB_Manager.cli",
        "json",
        "run",
        "--config",
        str(cfg_path),
        "--mode",
        str(mode),
        "--schema-mode",
        str(schema_mode),
        "--chunk-size",
        str(int(chunk_size)),
        "--parallel-workers",
        str(int(parallel_workers)),
        "--db-load-parallel-tables",
        str(int(db_load_parallel_tables)),
        "--load-data-commit-strategy",
        str(load_data_commit_strategy),
        "--tsv-merge-union-schema" if bool(tsv_merge_union_schema) else "--no-tsv-merge-union-schema",
        "--overlap-batches" if bool(overlap_batches) else "--no-overlap-batches",
        "--report",
        str(report_path),
    ]

    started = time.time()
    proc = subprocess.run(
        cmd,
        cwd=str(repo_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    wall_s = time.time() - started
    if proc.returncode != 0:
        # Keep failure context nearby; report may not exist.
        err_path = tmp_dir / f"bench_sweep_{db_name}_stderr.txt"
        out_path = tmp_dir / f"bench_sweep_{db_name}_stdout.txt"
        err_path.write_text(proc.stderr or "", encoding="utf-8", errors="replace")
        out_path.write_text(proc.stdout or "", encoding="utf-8", errors="replace")
        if not keep_db:
            try:
                _db_admin_drop(host=host, port=int(port), user=user, password=password, database=db_name)
            except Exception:
                pass
        raise RuntimeError(f"run_failed rc={proc.returncode} db={db_name} report={report_path}")

    report: dict[str, Any] = {}
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except Exception:
        report = {}

    t = report.get("timings_ms") or {}
    s = report.get("stats") or {}
    warnings = _count_warnings(report)

    json_flatten_ms = t.get("json.flatten")
    db_tsv_write_ms = t.get("db.load_data.tsv_write") or 0
    flatten_plus_tsv = None
    try:
        if json_flatten_ms is not None:
            flatten_plus_tsv = int(json_flatten_ms) + int(db_tsv_write_ms)
    except Exception:
        flatten_plus_tsv = None

    row = SweepRow(
        stamp=stamp,
        input_path=str(input_path),
        mode=str(mode),
        schema_mode=str(schema_mode),
        chunk_size=int(chunk_size),
        parallel_workers=int(parallel_workers),
        db_load_parallel_tables=int(db_load_parallel_tables),
        load_data_commit_strategy=str(load_data_commit_strategy),
        tsv_merge_union_schema=bool(tsv_merge_union_schema),
        overlap_batches=bool(overlap_batches),
        rep=int(rep),
        db_name=str(db_name),
        table_name=str(table_name),
        report_path=str(report_path),
        duration_s=float(report.get("duration_s") or wall_s),
        records_read=int(s.get("records_read")) if s.get("records_read") is not None else None,
        batches_total=int(s.get("batches_total")) if s.get("batches_total") is not None else None,
        pipeline_total_ms=int(t.get("pipeline.json.total")) if t.get("pipeline.json.total") is not None else None,
        io_json_parse_ms=int(t.get("io.json_parse")) if t.get("io.json_parse") is not None else None,
        json_flatten_ms=int(json_flatten_ms) if json_flatten_ms is not None else None,
        db_tsv_write_ms=int(db_tsv_write_ms) if db_tsv_write_ms is not None else None,
        flatten_plus_tsv_ms=int(flatten_plus_tsv) if flatten_plus_tsv is not None else None,
        json_db_create_ms=int(t.get("json.db.create")) if t.get("json.db.create") is not None else None,
        db_load_exec_ms=int(t.get("db.load_data.exec")) if t.get("db.load_data.exec") is not None else None,
        db_load_total_ms=int(t.get("db.load_data.total")) if t.get("db.load_data.total") is not None else None,
        warnings=int(warnings),
    )

    if not keep_db:
        _db_admin_drop(host=host, port=int(port), user=user, password=password, database=db_name)

    return row


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="/tmp/bench_wos_like_20000.jsonl", help="JSONL input path")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", default=3307, type=int)
    ap.add_argument("--user", default="root")
    ap.add_argument("--password", default="rootpass")
    ap.add_argument("--mode", default="ingest-fast", help="Run mode preset")
    ap.add_argument("--schema-mode", default="evolve", choices=["evolve", "freeze", "hybrid"])
    ap.add_argument("--chunk-sizes", default="20000,5000,1000")
    ap.add_argument("--workers", default="0,4,8")
    ap.add_argument("--db-load-parallel-tables", default=0, type=int)
    ap.add_argument(
        "--load-data-commit-strategy",
        default="file",
        choices=["file", "table", "batch"],
        help="When using LOAD DATA: commit per file/table/batch (default: file)",
    )
    ap.add_argument(
        "--tsv-merge-union-schema",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Rewrite TSV fragments to union schema to merge across schema drift (default: false)",
    )
    ap.add_argument(
        "--overlap-batches",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Overlap batch flattening with previous batch DB load (default: false)",
    )
    ap.add_argument("--reps", default=2, type=int)
    ap.add_argument("--keep-db", action="store_true", help="Keep databases (default: drop after each run)")
    ap.add_argument("--tmp-dir", default="/tmp")
    ap.add_argument("--out-json", default="", help="Write full sweep rows to this path (json)")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    input_path = Path(args.input).resolve()
    if not input_path.exists():
        raise SystemExit(f"Input not found: {input_path}")

    tmp_dir = Path(args.tmp_dir).resolve()
    tmp_dir.mkdir(parents=True, exist_ok=True)

    stamp = _utc_stamp()
    chunk_sizes = _parse_int_list(args.chunk_sizes)
    workers = _parse_int_list(args.workers)
    db_load_parallel_tables = int(args.db_load_parallel_tables or 0)
    load_data_commit_strategy = str(args.load_data_commit_strategy)
    tsv_merge_union_schema = bool(args.tsv_merge_union_schema)
    overlap_batches = bool(args.overlap_batches)
    reps = int(args.reps)

    rows: list[SweepRow] = []
    for chunk in chunk_sizes:
        for pw in workers:
            for rep in range(1, reps + 1):
                print(f"[run] chunk={chunk} pw={pw} rep={rep}/{reps}", flush=True)
                row = _run_one(
                    repo_root=repo_root,
                    input_path=input_path,
                    host=str(args.host),
                    port=int(args.port),
                    user=str(args.user),
                    password=str(args.password),
                    mode=str(args.mode),
                    schema_mode=str(args.schema_mode),
                    chunk_size=int(chunk),
                    parallel_workers=int(pw),
                    db_load_parallel_tables=int(db_load_parallel_tables),
                    load_data_commit_strategy=str(load_data_commit_strategy),
                    tsv_merge_union_schema=bool(tsv_merge_union_schema),
                    overlap_batches=bool(overlap_batches),
                    rep=int(rep),
                    stamp=stamp,
                    keep_db=bool(args.keep_db),
                    tmp_dir=tmp_dir,
                )
                rows.append(row)

    rows_sorted = sorted(rows, key=lambda r: (r.chunk_size, r.parallel_workers, r.rep))
    print("\nSummary (lower is better):")
    hdr = (
        "chunk  pw dpl  cmt  um ov rep  total_ms  flat+tsv_ms  db_exec_ms  batches  warn  report"
    )
    print(hdr)
    for r in rows_sorted:
        print(
            f"{r.chunk_size:5d} {r.parallel_workers:3d} {r.db_load_parallel_tables:3d} {str(r.load_data_commit_strategy)[:4]:>4s} {int(bool(r.tsv_merge_union_schema)):3d} {int(bool(r.overlap_batches)):2d} {r.rep:3d} "
            f"{(r.pipeline_total_ms or 0):9d} {(r.flatten_plus_tsv_ms or 0):11d} "
            f"{(r.db_load_exec_ms or 0):10d} {(r.batches_total or 0):7d} {r.warnings:4d} "
            f"{Path(r.report_path).name}"
        )

    if args.out_json:
        out_path = Path(args.out_json).resolve()
        out_path.write_text(json.dumps([asdict(r) for r in rows_sorted], indent=2), encoding="utf-8")
        print(f"\nWrote: {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
