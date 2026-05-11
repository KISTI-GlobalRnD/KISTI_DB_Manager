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


def _load_json(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    return read_json(path)


def _log(fp, message: str) -> None:
    line = f"[{datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')}] {message}"
    fp.write(line + "\n")
    fp.flush()


def _qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _count_rows_from_footers(table_dir: Path) -> tuple[int, int]:
    total_rows = 0
    file_count = 0
    for parquet_path in sorted(table_dir.glob("*.parquet")):
        total_rows += int(pq.ParquetFile(parquet_path).metadata.num_rows)
        file_count += 1
    return total_rows, file_count


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


def _connect_duckdb(*, threads: int, temp_dir: Path | None):
    import duckdb

    con = duckdb.connect()
    con.execute(f"PRAGMA threads={int(max(1, threads))};")
    chosen_temp = temp_dir
    if chosen_temp is not None:
        try:
            chosen_temp.mkdir(parents=True, exist_ok=True)
        except Exception:
            chosen_temp = None
    if chosen_temp is not None:
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
    for old_file in out_dir.glob(basename_template.replace("{i}", "*")):
        if old_file.is_file():
            old_file.unlink(missing_ok=True)
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


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Diagnose and rewrite merged OpenAlex main parquet with duplicate ids removed.")
    ap.add_argument("--snapshot-root", required=True)
    ap.add_argument("--main-table", required=True)
    ap.add_argument("--out-root", required=True)
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--threads", type=int, default=max(1, os.cpu_count() or 1))
    ap.add_argument("--max-rows-per-file", type=int, default=1_000_000)
    ap.add_argument("--temp-dir", default="")
    ap.add_argument("--merge-state")
    ap.add_argument("--base-root")
    ap.add_argument("--base-main-table")
    ap.add_argument("--delta-root")
    ap.add_argument("--delta-main-table")
    ap.add_argument("--sample-limit", type=int, default=50)
    ap.add_argument("--diagnose-only", action="store_true")
    ap.add_argument("--allow-nonexact", action="store_true")
    args = ap.parse_args(argv)

    snapshot_root = Path(args.snapshot_root).expanduser().resolve()
    out_root = Path(args.out_root).expanduser().resolve()
    run_dir = Path(args.run_dir).expanduser().resolve()
    temp_dir = Path(args.temp_dir).expanduser().resolve() if str(args.temp_dir).strip() else None
    run_dir.mkdir(parents=True, exist_ok=True)
    out_root.mkdir(parents=True, exist_ok=True)

    log_path = run_dir / "dedup.log"
    progress_path = run_dir / "progress.json"
    report_path = run_dir / "report.json"
    dup_ids_parquet = run_dir / "duplicate_ids.parquet"
    dup_sample_json = run_dir / "duplicate_id_samples.json"

    merge_state = _load_json(Path(args.merge_state).expanduser().resolve()) if args.merge_state else {}

    initial_state: dict[str, Any] = {
        "generated_at": utc_now_iso(),
        "updated_at": None,
        "status": "running",
        "snapshot_root": str(snapshot_root),
        "main_table": args.main_table,
        "out_root": str(out_root),
        "steps": {
            "diagnose": {"status": "pending"},
            "link_other_tables": {"status": "pending"},
            "rewrite_main": {"status": "pending"},
            "validate_rewrite": {"status": "pending"},
        },
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

        import duckdb

        con = _connect_duckdb(threads=int(args.threads), temp_dir=temp_dir)
        try:
            main_dir = snapshot_root / args.main_table
            main_glob = str(main_dir / "*.parquet")
            columns = [
                row[0]
                for row in con.execute(
                    f"DESCRIBE SELECT * FROM read_parquet({json.dumps(main_glob)}, union_by_name=true)"
                ).fetchall()
            ]
            hash_expr = "hash(" + ", ".join(_qi(col) for col in columns) + ")"

            _log(log_fp, "diagnose duplicate ids")
            t0 = time.perf_counter()
            con.execute(
                "CREATE OR REPLACE TEMP TABLE dup_ids AS "
                f"SELECT id, COUNT(*) AS merged_rows "
                f"FROM read_parquet({json.dumps(main_glob)}, union_by_name=true) "
                "GROUP BY id HAVING COUNT(*) > 1"
            )
            con.execute(
                f"COPY dup_ids TO {json.dumps(str(dup_ids_parquet))} (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            dup_id_count, dup_extra_rows = con.execute(
                "SELECT COUNT(*), COALESCE(SUM(merged_rows) - COUNT(*), 0) FROM dup_ids"
            ).fetchone()
            con.execute(
                "CREATE OR REPLACE TEMP TABLE dup_variants AS "
                "SELECT d.id, d.merged_rows, COUNT(DISTINCT "
                + hash_expr
                + ") AS distinct_row_variants "
                f"FROM read_parquet({json.dumps(main_glob)}, union_by_name=true) m "
                "JOIN dup_ids d USING (id) "
                "GROUP BY d.id, d.merged_rows"
            )
            exact_dup_ids, nonexact_dup_ids = con.execute(
                "SELECT "
                "COALESCE(SUM(CASE WHEN distinct_row_variants = 1 THEN 1 ELSE 0 END), 0), "
                "COALESCE(SUM(CASE WHEN distinct_row_variants > 1 THEN 1 ELSE 0 END), 0) "
                "FROM dup_variants"
            ).fetchone()

            source_breakdown: dict[str, Any] = {}
            if args.base_root and args.base_main_table and args.delta_root and args.delta_main_table:
                base_glob = str(Path(args.base_root).expanduser().resolve() / args.base_main_table / "*.parquet")
                delta_glob = str(Path(args.delta_root).expanduser().resolve() / args.delta_main_table / "*.parquet")
                con.execute(
                    "CREATE OR REPLACE TEMP TABLE dup_base_counts AS "
                    f"SELECT b.id, COUNT(*) AS base_rows "
                    f"FROM read_parquet({json.dumps(base_glob)}, union_by_name=true) b "
                    "SEMI JOIN dup_ids d USING (id) "
                    "GROUP BY b.id"
                )
                con.execute(
                    "CREATE OR REPLACE TEMP TABLE dup_delta_counts AS "
                    f"SELECT x.id, COUNT(*) AS delta_rows "
                    f"FROM read_parquet({json.dumps(delta_glob)}, union_by_name=true) x "
                    "SEMI JOIN dup_ids d USING (id) "
                    "GROUP BY x.id"
                )
                source_breakdown = dict(
                    zip(
                        [
                            "delta_source_dup_ids",
                            "base_source_dup_ids",
                            "mixed_source_dup_ids",
                            "unexplained_dup_ids",
                        ],
                        con.execute(
                            "SELECT "
                            "COALESCE(SUM(CASE WHEN COALESCE(delta_rows, 0) > 1 AND COALESCE(base_rows, 0) <= 1 THEN 1 ELSE 0 END), 0), "
                            "COALESCE(SUM(CASE WHEN COALESCE(delta_rows, 0) = 0 AND COALESCE(base_rows, 0) > 1 THEN 1 ELSE 0 END), 0), "
                            "COALESCE(SUM(CASE WHEN COALESCE(delta_rows, 0) > 1 AND COALESCE(base_rows, 0) > 1 THEN 1 ELSE 0 END), 0), "
                            "COALESCE(SUM(CASE WHEN COALESCE(delta_rows, 0) <= 1 AND COALESCE(base_rows, 0) <= 1 THEN 1 ELSE 0 END), 0) "
                            "FROM dup_ids d "
                            "LEFT JOIN dup_base_counts b USING (id) "
                            "LEFT JOIN dup_delta_counts x USING (id)"
                        ).fetchone(),
                    )
                )
                samples = con.execute(
                    "SELECT d.id, d.merged_rows, COALESCE(b.base_rows, 0) AS base_rows, "
                    "COALESCE(x.delta_rows, 0) AS delta_rows, v.distinct_row_variants "
                    "FROM dup_ids d "
                    "LEFT JOIN dup_base_counts b USING (id) "
                    "LEFT JOIN dup_delta_counts x USING (id) "
                    "LEFT JOIN dup_variants v USING (id, merged_rows) "
                    "ORDER BY d.merged_rows DESC, d.id "
                    f"LIMIT {int(max(1, args.sample_limit))}"
                ).fetchall()
            else:
                samples = con.execute(
                    "SELECT d.id, d.merged_rows, NULL AS base_rows, NULL AS delta_rows, v.distinct_row_variants "
                    "FROM dup_ids d "
                    "LEFT JOIN dup_variants v USING (id, merged_rows) "
                    "ORDER BY d.merged_rows DESC, d.id "
                    f"LIMIT {int(max(1, args.sample_limit))}"
                ).fetchall()

            sample_payload = [
                {
                    "id": row[0],
                    "merged_rows": int(row[1]),
                    "base_rows": None if row[2] is None else int(row[2]),
                    "delta_rows": None if row[3] is None else int(row[3]),
                    "distinct_row_variants": int(row[4]),
                }
                for row in samples
            ]
            atomic_write_json(dup_sample_json, {"rows": sample_payload})

            diagnose_elapsed = time.perf_counter() - t0
            state["steps"]["diagnose"] = {
                "status": "done",
                "duration_sec": round(diagnose_elapsed, 1),
                "duplicate_id_count": int(dup_id_count),
                "duplicate_row_excess": int(dup_extra_rows),
                "exact_duplicate_ids": int(exact_dup_ids),
                "nonexact_duplicate_ids": int(nonexact_dup_ids),
                "source_breakdown": {k: int(v) for k, v in source_breakdown.items()},
                "duplicate_ids_parquet": str(dup_ids_parquet),
                "duplicate_id_samples_json": str(dup_sample_json),
            }
            save()
            _log(log_fp, f"diagnose complete duplicate_id_count={dup_id_count} duplicate_row_excess={dup_extra_rows}")

            if int(dup_id_count) == 0:
                state["steps"]["link_other_tables"] = {"status": "skipped"}
                state["steps"]["rewrite_main"] = {"status": "skipped"}
                state["steps"]["validate_rewrite"] = {"status": "skipped"}
                state["status"] = "done"
                save()
                atomic_write_json(report_path, state)
                _log(log_fp, "no duplicate ids found")
                return 0

            if int(nonexact_dup_ids) > 0 and not args.allow_nonexact:
                state["status"] = "error"
                state["error"] = {
                    "message": "Non-exact duplicate ids detected; refusing rewrite without --allow-nonexact",
                    "nonexact_duplicate_ids": int(nonexact_dup_ids),
                }
                save()
                atomic_write_json(report_path, state)
                _log(log_fp, "abort due to non-exact duplicate ids")
                return 2

            if args.diagnose_only:
                state["steps"]["link_other_tables"] = {"status": "skipped"}
                state["steps"]["rewrite_main"] = {"status": "skipped"}
                state["steps"]["validate_rewrite"] = {"status": "skipped"}
                state["status"] = "done"
                save()
                atomic_write_json(report_path, state)
                _log(log_fp, "diagnose-only complete")
                return 0

            _log(log_fp, "link non-main table directories")
            link_stats: dict[str, Any] = {}
            pre_dedup_rows, pre_dedup_files = _count_rows_from_footers(main_dir)
            for table_dir in sorted(p for p in snapshot_root.iterdir() if p.is_dir() and p.name != args.main_table):
                link_stats[table_dir.name] = _link_tree(table_dir, out_root / table_dir.name)
                save()
            state["steps"]["link_other_tables"] = {
                "status": "done",
                "tables_linked": len(link_stats),
                "details": link_stats,
            }
            save()
            _log(log_fp, f"linked non-main tables count={len(link_stats)}")

            _log(log_fp, "rewrite deduped main table")
            rewrite_t0 = time.perf_counter()
            dedup_sql = (
                "SELECT * EXCLUDE (rn) FROM ("
                "SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS rn "
                f"FROM read_parquet({json.dumps(main_glob)}, union_by_name=true)"
                ") WHERE rn = 1"
            )
            written_files = _write_dataset_from_sql(
                con=con,
                sql=dedup_sql,
                out_dir=out_root / args.main_table,
                basename_template="part-{i}.parquet",
                max_rows_per_file=int(args.max_rows_per_file),
            )
            rewrite_elapsed = time.perf_counter() - rewrite_t0
            state["steps"]["rewrite_main"] = {
                "status": "done",
                "duration_sec": round(rewrite_elapsed, 1),
                "written_files": int(written_files),
            }
            save()
            _log(log_fp, f"rewrite complete files={written_files} duration_sec={rewrite_elapsed:.1f}")

            _log(log_fp, "validate rewritten main by footer counts")
            validate_t0 = time.perf_counter()
            dedup_rows, dedup_files = _count_rows_from_footers(out_root / args.main_table)
            validate_elapsed = time.perf_counter() - validate_t0
            expected_dedup_rows = int(pre_dedup_rows) - int(state["steps"]["diagnose"]["duplicate_row_excess"])
            state["steps"]["validate_rewrite"] = {
                "status": "done",
                "duration_sec": round(validate_elapsed, 1),
                "pre_dedup_rows": int(pre_dedup_rows),
                "pre_dedup_files": int(pre_dedup_files),
                "dedup_rows": int(dedup_rows),
                "dedup_files": int(dedup_files),
                "expected_rows_after_dedup": int(expected_dedup_rows),
            }
            footer_expected = None
            if merge_state.get("profile") and args.base_root and args.base_main_table and args.delta_root and args.delta_main_table:
                base_rows, _ = _count_rows_from_footers(Path(args.base_root).expanduser().resolve() / args.base_main_table)
                delta_rows, _ = _count_rows_from_footers(Path(args.delta_root).expanduser().resolve() / args.delta_main_table)
                footer_expected = base_rows - int(merge_state["profile"]["overlap_existing_ids"]) + int(
                    merge_state["profile"]["distinct_delta_ids"]
                )
                state["steps"]["validate_rewrite"]["pre_dedup_footer_expected"] = int(footer_expected)
            state["steps"]["validate_rewrite"]["dedup_matches_expected_rows"] = int(dedup_rows) == int(
                expected_dedup_rows
            )
            state["steps"]["validate_rewrite"]["rows_removed"] = int(
                state["steps"]["diagnose"]["duplicate_row_excess"]
            )
            state["steps"]["validate_rewrite"]["deduped_main_is_lower_by_duplicate_excess"] = (
                int(dedup_rows) + int(state["steps"]["diagnose"]["duplicate_row_excess"]) == int(pre_dedup_rows)
            )
            if footer_expected is not None:
                state["steps"]["validate_rewrite"]["footer_expected_minus_removed"] = int(footer_expected) - int(
                    state["steps"]["diagnose"]["duplicate_row_excess"]
                )
            save()
            _log(log_fp, f"validate complete dedup_rows={dedup_rows}")

            state["status"] = "done"
            save()
            atomic_write_json(report_path, state)
            _log(log_fp, "dedup main complete")
        finally:
            con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
