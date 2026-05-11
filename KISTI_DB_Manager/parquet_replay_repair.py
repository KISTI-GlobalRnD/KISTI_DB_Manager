#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds
import pyarrow.parquet as pq
from KISTI_DB_Manager.runstate import JsonRunState, atomic_write_json, read_json, utc_now_iso


def _log(fp, message: str) -> None:
    line = f"[{datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')}] {message}"
    fp.write(line + "\n")
    fp.flush()


def _read_parquet_expr(source_expr: str, *, union_by_name: bool = False, filename: bool = False) -> str:
    options: list[str] = []
    if union_by_name:
        options.append("union_by_name=true")
    if filename:
        options.append("filename=true")
    if options:
        return f"read_parquet({source_expr}, {', '.join(options)})"
    return f"read_parquet({source_expr})"


def _parquet_source_expr(files: list[Path]) -> str:
    items = [str(p) for p in files]
    if not items:
        raise ValueError("files must not be empty")
    if len(items) == 1:
        return json.dumps(items[0])
    return "[" + ", ".join(json.dumps(x) for x in items) + "]"


def _count_rows_from_footers(table_dir: Path) -> tuple[int, int]:
    total_rows = 0
    file_count = 0
    for parquet_path in sorted(table_dir.glob("*.parquet")):
        total_rows += int(pq.ParquetFile(parquet_path).metadata.num_rows)
        file_count += 1
    return total_rows, file_count


def _cleanup_glob(root: Path, pattern: str) -> int:
    removed = 0
    for child in root.glob(pattern):
        if child.is_file():
            child.unlink(missing_ok=True)
            removed += 1
    return removed


def _link_or_copy(src: Path, dst: Path) -> str:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        return "exists"
    try:
        os.link(src, dst)
        return "hardlink"
    except Exception:
        shutil.copy2(src, dst)
        return "copy"


def _link_tree(src_dir: Path, dst_dir: Path) -> dict[str, int]:
    linked = 0
    copied = 0
    dst_dir.mkdir(parents=True, exist_ok=True)
    for src_file in sorted(p for p in src_dir.iterdir() if p.is_file()):
        mode = _link_or_copy(src_file, dst_dir / src_file.name)
        if mode == "hardlink":
            linked += 1
        elif mode == "copy":
            copied += 1
    return {"linked": linked, "copied": copied}


def _connect_duckdb(*, db_path: Path, temp_dir: Path | None, threads: int):
    import duckdb

    con = duckdb.connect(str(db_path))
    con.execute(f"PRAGMA threads={int(max(1, threads))};")
    chosen_temp = temp_dir
    if chosen_temp is not None:
        try:
            chosen_temp.mkdir(parents=True, exist_ok=True)
        except Exception:
            chosen_temp = db_path.parent / "_duckdb_tmp"
            chosen_temp.mkdir(parents=True, exist_ok=True)
    if chosen_temp:
        con.execute(f"PRAGMA temp_directory={json.dumps(str(chosen_temp))};")
    return con


def _write_dataset_from_sql(
    *,
    con,
    sql: str,
    out_dir: Path,
    basename_template: str,
    max_rows_per_file: int,
) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    _cleanup_glob(out_dir, basename_template.replace("{i}", "*"))
    reader = con.execute(sql).to_arrow_reader()
    ds.write_dataset(
        reader,
        base_dir=str(out_dir),
        format="parquet",
        basename_template=basename_template,
        existing_data_behavior="overwrite_or_ignore",
        max_rows_per_file=max_rows_per_file,
        max_rows_per_group=min(max_rows_per_file, 100_000),
    )
    return sum(1 for _ in out_dir.glob(basename_template.replace("{i}", "*")))


def repair_main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Repair replayed parquet rows by exact-row dedup on a known duplicate-id set."
    )
    ap.add_argument("--snapshot-root", required=True)
    ap.add_argument("--out-root", required=True)
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--duplicate-ids-parquet", required=True)
    ap.add_argument("--threads", type=int, default=max(1, os.cpu_count() or 1))
    ap.add_argument("--max-rows-per-file", type=int, default=1_000_000)
    ap.add_argument("--temp-dir", default="")
    ap.add_argument("--table", action="append", default=[])
    ap.add_argument("--exclude-table", action="append", default=[])
    ap.add_argument("--diagnose-only", action="store_true")
    args = ap.parse_args(argv)

    snapshot_root = Path(args.snapshot_root).expanduser().resolve()
    out_root = Path(args.out_root).expanduser().resolve()
    run_dir = Path(args.run_dir).expanduser().resolve()
    dup_ids_parquet = Path(args.duplicate_ids_parquet).expanduser().resolve()
    temp_dir = Path(args.temp_dir).expanduser().resolve() if str(args.temp_dir).strip() else None
    run_dir.mkdir(parents=True, exist_ok=True)
    out_root.mkdir(parents=True, exist_ok=True)

    selected_tables = set(args.table or [])
    excluded_tables = set(args.exclude_table or [])
    all_table_dirs = sorted(
        (p for p in snapshot_root.iterdir() if p.is_dir()),
        key=lambda p: (sum(1 for _ in p.glob("*.parquet")), p.name),
    )
    table_dirs = list(all_table_dirs)
    if selected_tables:
        table_dirs = [p for p in table_dirs if p.name in selected_tables]
    if excluded_tables:
        table_dirs = [p for p in table_dirs if p.name not in excluded_tables]
    passthrough_dirs = [p for p in all_table_dirs if p not in table_dirs]

    log_path = run_dir / "repair.log"
    progress_path = run_dir / "progress.json"
    report_path = run_dir / "report.json"
    initial_state: dict[str, Any] = {
        "generated_at": utc_now_iso(),
        "updated_at": None,
        "status": "running",
        "snapshot_root": str(snapshot_root),
        "out_root": str(out_root),
        "duplicate_ids_parquet": str(dup_ids_parquet),
        "diagnose_only": bool(args.diagnose_only),
        "tables_total": len(table_dirs),
        "tables_done": 0,
        "tables": {},
        "summary": {
            "touched_tables": 0,
            "rewritten_tables": 0,
            "rows_removed": 0,
            "linked_tables": 0,
            "passthrough_tables": 0,
        },
        "current_table": None,
    }
    if progress_path.exists():
        progress = JsonRunState(path=progress_path, payload=read_json(progress_path), timestamp_key="updated_at")
    else:
        progress = JsonRunState.create(progress_path, initial_state, timestamp_key="updated_at")
    state = progress.payload

    def save() -> None:
        progress.write(touch_timestamp=True)

    with log_path.open("a", encoding="utf-8") as log_fp:
        save()
        _log(log_fp, f"start snapshot_root={snapshot_root} out_root={out_root}")
        con = _connect_duckdb(db_path=run_dir / "repair.duckdb", temp_dir=temp_dir, threads=int(args.threads))
        try:
            con.execute(
                "CREATE OR REPLACE VIEW duplicate_ids AS "
                f"SELECT DISTINCT id FROM {_read_parquet_expr(json.dumps(str(dup_ids_parquet)))}"
            )

            if not args.diagnose_only:
                for passthrough_dir in passthrough_dirs:
                    link_stats = _link_tree(passthrough_dir, out_root / passthrough_dir.name)
                    state["tables"][passthrough_dir.name] = {
                        "status": "linked_passthrough",
                        "phase": "done",
                        "link_stats": link_stats,
                        "duration_sec": 0.0,
                    }
                    state["summary"]["passthrough_tables"] += 1
                if passthrough_dirs:
                    save()

            for table_dir in table_dirs:
                t0 = time.perf_counter()
                tstate: dict[str, Any] = {"status": "running", "phase": "inspect_schema"}
                state["current_table"] = table_dir.name
                state["tables"][table_dir.name] = tstate
                save()

                out_table_dir = out_root / table_dir.name
                parquet_files = sorted(table_dir.glob("*.parquet"))
                other_files = sorted(p for p in table_dir.iterdir() if p.is_file() and p.suffix != ".parquet")
                if not parquet_files:
                    link_stats = _link_tree(table_dir, out_table_dir)
                    tstate.update({"status": "linked_empty", "link_stats": link_stats, "duration_sec": round(time.perf_counter() - t0, 1)})
                    state["summary"]["linked_tables"] += 1
                    state["tables_done"] += 1
                    save()
                    continue

                table_glob = str(table_dir / "*.parquet")
                columns = [
                    row[0]
                    for row in con.execute(
                        f"DESCRIBE SELECT * FROM {_read_parquet_expr(json.dumps(table_glob), union_by_name=True)}"
                    ).fetchall()
                ]
                if "id" not in columns:
                    link_stats = _link_tree(table_dir, out_table_dir)
                    tstate.update(
                        {
                            "status": "linked_no_id",
                            "link_stats": link_stats,
                            "duration_sec": round(time.perf_counter() - t0, 1),
                        }
                    )
                    state["summary"]["linked_tables"] += 1
                    state["tables_done"] += 1
                    state["current_table"] = None
                    save()
                    continue

                tstate["phase"] = "scan_touched_files"
                save()
                touched_rows = con.execute(
                    "SELECT DISTINCT filename "
                    f"FROM {_read_parquet_expr(json.dumps(table_glob), union_by_name=True, filename=True)} t "
                    "SEMI JOIN duplicate_ids d USING (id)"
                ).fetchall()
                touched_files = [Path(row[0]).resolve() for row in touched_rows]
                touched_set = {p.resolve() for p in touched_files}

                if not touched_files:
                    link_stats = _link_tree(table_dir, out_table_dir)
                    tstate.update(
                        {
                            "status": "linked_untouched",
                            "phase": "done",
                            "link_stats": link_stats,
                            "touched_files": 0,
                            "duration_sec": round(time.perf_counter() - t0, 1),
                        }
                    )
                    state["summary"]["linked_tables"] += 1
                    state["tables_done"] += 1
                    state["current_table"] = None
                    save()
                    continue

                state["summary"]["touched_tables"] += 1
                touched_expr = _parquet_source_expr(sorted(touched_files))
                tstate["phase"] = "profile_affected_rows"
                save()
                pre_rows, pre_files = _count_rows_from_footers(table_dir)
                affected_rows = int(
                    con.execute(
                        "SELECT COUNT(*) "
                        f"FROM {_read_parquet_expr(touched_expr, union_by_name=True)} t "
                        "SEMI JOIN duplicate_ids d USING (id)"
                    ).fetchone()[0]
                )
                dedup_affected_rows = int(
                    con.execute(
                        "SELECT COUNT(*) FROM ("
                        "SELECT DISTINCT * "
                        f"FROM {_read_parquet_expr(touched_expr, union_by_name=True)} t "
                        "SEMI JOIN duplicate_ids d USING (id)"
                        ")"
                    ).fetchone()[0]
                )
                duplicate_row_excess = int(affected_rows - dedup_affected_rows)
                untouched_files = [p for p in parquet_files if p.resolve() not in touched_set]

                tstate.update(
                    {
                        "touched_files": len(touched_files),
                        "untouched_files": len(untouched_files),
                        "pre_rows": int(pre_rows),
                        "pre_files": int(pre_files),
                        "affected_rows": int(affected_rows),
                        "dedup_affected_rows": int(dedup_affected_rows),
                        "duplicate_row_excess": int(duplicate_row_excess),
                    }
                )

                if args.diagnose_only:
                    tstate.update({"status": "diagnosed", "phase": "done", "duration_sec": round(time.perf_counter() - t0, 1)})
                    state["tables_done"] += 1
                    state["current_table"] = None
                    save()
                    continue

                out_table_dir.mkdir(parents=True, exist_ok=True)
                tstate["phase"] = "link_untouched_files"
                save()
                linked = 0
                copied = 0
                for src_file in untouched_files:
                    mode = _link_or_copy(src_file, out_table_dir / src_file.name)
                    if mode == "hardlink":
                        linked += 1
                    elif mode == "copy":
                        copied += 1
                for src_file in other_files:
                    mode = _link_or_copy(src_file, out_table_dir / src_file.name)
                    if mode == "hardlink":
                        linked += 1
                    elif mode == "copy":
                        copied += 1

                if duplicate_row_excess > 0:
                    tstate["phase"] = "rewrite_touched_files"
                    save()
                    sql = (
                        "SELECT * "
                        f"FROM {_read_parquet_expr(touched_expr, union_by_name=True)} t "
                        "ANTI JOIN duplicate_ids d USING (id) "
                        "UNION ALL "
                        "SELECT DISTINCT * "
                        f"FROM {_read_parquet_expr(touched_expr, union_by_name=True)} t "
                        "SEMI JOIN duplicate_ids d USING (id)"
                    )
                    written_files = _write_dataset_from_sql(
                        con=con,
                        sql=sql,
                        out_dir=out_table_dir,
                        basename_template="repair-{i}.parquet",
                        max_rows_per_file=int(args.max_rows_per_file),
                    )
                    post_rows, post_files = _count_rows_from_footers(out_table_dir)
                    expected_rows = int(pre_rows - duplicate_row_excess)
                    valid = int(post_rows) == int(expected_rows)
                    tstate.update(
                        {
                            "status": "rewritten",
                            "written_files": int(written_files),
                            "post_rows": int(post_rows),
                            "post_files": int(post_files),
                            "expected_rows": int(expected_rows),
                            "rows_validated": bool(valid),
                            "phase": "done",
                            "link_stats": {"linked": linked, "copied": copied},
                        }
                    )
                    state["summary"]["rewritten_tables"] += 1
                    state["summary"]["rows_removed"] += int(duplicate_row_excess)
                else:
                    link_stats = _link_tree(table_dir, out_table_dir)
                    tstate.update(
                        {
                            "status": "linked_exact",
                            "phase": "done",
                            "link_stats": {
                                "linked": linked + int(link_stats["linked"]),
                                "copied": copied + int(link_stats["copied"]),
                            },
                        }
                    )
                    state["summary"]["linked_tables"] += 1

                tstate["duration_sec"] = round(time.perf_counter() - t0, 1)
                state["tables_done"] += 1
                state["current_table"] = None
                save()
                _log(
                    log_fp,
                    f"table={table_dir.name} status={tstate['status']} touched_files={len(touched_files)} rows_removed={duplicate_row_excess}",
                )

            state["status"] = "done"
            save()
            atomic_write_json(report_path, state)
            _log(log_fp, "repair complete")
        finally:
            con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(repair_main())
