#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _log(fp, message: str) -> None:
    line = f"[{datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')}] {message}"
    fp.write(line + "\n")
    fp.flush()


def _count_rows_from_footers(table_dir: Path) -> tuple[int, int]:
    total_rows = 0
    file_count = 0
    for parquet_path in sorted(table_dir.glob("*.parquet")):
        total_rows += int(pq.ParquetFile(parquet_path).metadata.num_rows)
        file_count += 1
    return total_rows, file_count


def _table_glob(table_dir: Path) -> str:
    return str(table_dir / "*.parquet")


def _load_json(path: Path | None) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Validate a merged OpenAlex parquet snapshot.")
    ap.add_argument("--snapshot-root", required=True)
    ap.add_argument("--main-table", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--threads", type=int, default=max(1, os.cpu_count() or 1))
    ap.add_argument("--merge-state")
    ap.add_argument("--base-run-report")
    ap.add_argument("--base-root")
    ap.add_argument("--base-main-table")
    ap.add_argument("--delta-root")
    ap.add_argument("--delta-main-table")
    ap.add_argument("--child-table", action="append", default=[])
    ap.add_argument("--skip-main-distinct", action="store_true")
    ap.add_argument("--skip-child-orphans", action="store_true")
    args = ap.parse_args(argv)

    snapshot_root = Path(args.snapshot_root).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "validation_report.json"
    progress_path = out_dir / "progress.json"
    log_path = out_dir / "validation.log"

    merge_state = _load_json(Path(args.merge_state).expanduser().resolve()) if args.merge_state else {}
    base_run_report = _load_json(Path(args.base_run_report).expanduser().resolve()) if args.base_run_report else {}

    state: dict[str, Any] = {
        "generated_at": _utc_now_iso(),
        "updated_at": None,
        "status": "running",
        "snapshot_root": str(snapshot_root),
        "main_table": args.main_table,
        "steps": {
            "inventory": {"status": "pending"},
            "footer_counts": {"status": "pending"},
            "main_distinct": {"status": "pending"},
            "child_orphans": {"status": "pending"},
        },
    }

    with log_path.open("a", encoding="utf-8") as log_fp:
        def save() -> None:
            state["updated_at"] = _utc_now_iso()
            _write_json(progress_path, state)

        save()

        _log(log_fp, f"start snapshot_root={snapshot_root}")

        table_dirs = sorted([p for p in snapshot_root.iterdir() if p.is_dir()])
        state["steps"]["inventory"] = {
            "status": "done",
            "tables_found_total": len(table_dirs),
            "tables_found": [p.name for p in table_dirs],
        }
        save()
        _log(log_fp, f"inventory complete tables={len(table_dirs)}")

        footer_counts: dict[str, Any] = {}
        footer_t0 = time.perf_counter()
        for table_dir in table_dirs:
            rows, files = _count_rows_from_footers(table_dir)
            footer_counts[table_dir.name] = {"rows": rows, "files": files}
            save()
        footer_elapsed = time.perf_counter() - footer_t0

        footer_step: dict[str, Any] = {
            "status": "done",
            "duration_sec": round(footer_elapsed, 1),
            "tables": footer_counts,
        }

        if args.base_root and args.base_main_table:
            base_main_dir = Path(args.base_root).expanduser().resolve() / args.base_main_table
            base_rows, base_files = _count_rows_from_footers(base_main_dir)
            footer_step["base_main"] = {"rows": base_rows, "files": base_files}
        if args.delta_root and args.delta_main_table:
            delta_main_dir = Path(args.delta_root).expanduser().resolve() / args.delta_main_table
            delta_rows, delta_files = _count_rows_from_footers(delta_main_dir)
            footer_step["delta_main"] = {"rows": delta_rows, "files": delta_files}

        if merge_state.get("profile") and footer_step.get("base_main") and footer_step.get("delta_main"):
            overlap_existing_ids = int(merge_state["profile"]["overlap_existing_ids"])
            footer_step["expected_merged_main_rows_from_footers"] = (
                int(footer_step["base_main"]["rows"]) - overlap_existing_ids + int(footer_step["delta_main"]["rows"])
            )
        if base_run_report:
            base_main_rows_from_report = int(
                base_run_report.get("stats", {}).get("records_ok")
                or base_run_report.get("stats", {}).get("records_read")
                or 0
            )
            footer_step["base_main_rows_from_run_report"] = base_main_rows_from_report
            if merge_state.get("profile"):
                footer_step["expected_merged_main_rows_from_run_report"] = (
                    base_main_rows_from_report
                    - int(merge_state["profile"]["overlap_existing_ids"])
                    + int(merge_state["profile"]["distinct_delta_ids"])
                )

        state["steps"]["footer_counts"] = footer_step
        save()
        _log(log_fp, f"footer counts complete duration_sec={footer_elapsed:.1f}")

        if args.skip_main_distinct:
            state["steps"]["main_distinct"] = {"status": "skipped"}
            save()
        else:
            import duckdb

            main_dir = snapshot_root / args.main_table
            main_glob = _table_glob(main_dir)
            _log(log_fp, f"main distinct start table={args.main_table}")
            distinct_t0 = time.perf_counter()
            con = duckdb.connect()
            try:
                con.execute(f"PRAGMA threads={int(max(1, args.threads))};")
                merged_rows, distinct_ids = con.execute(
                    f"SELECT COUNT(*) AS rows, COUNT(DISTINCT id) AS distinct_ids "
                    f"FROM read_parquet({json.dumps(main_glob)}, union_by_name=true)"
                ).fetchone()
                distinct_elapsed = time.perf_counter() - distinct_t0
                state["steps"]["main_distinct"] = {
                    "status": "done",
                    "duration_sec": round(distinct_elapsed, 1),
                    "merged_rows": int(merged_rows),
                    "distinct_ids": int(distinct_ids),
                    "duplicate_id_rows": int(merged_rows) - int(distinct_ids),
                    "id_is_unique": int(merged_rows) == int(distinct_ids),
                }
                expected_from_footers = state["steps"]["footer_counts"].get("expected_merged_main_rows_from_footers")
                if expected_from_footers is not None:
                    state["steps"]["main_distinct"]["matches_footer_expected"] = int(merged_rows) == int(
                        expected_from_footers
                    )
                save()
                _log(log_fp, f"main distinct complete duration_sec={distinct_elapsed:.1f}")

                if args.skip_child_orphans or not args.child_table:
                    state["steps"]["child_orphans"] = {
                        "status": "skipped" if args.skip_child_orphans else "done",
                        "tables": {},
                    }
                    save()
                else:
                    _log(log_fp, "build merged_main_ids temp table")
                    con.execute(
                        "CREATE OR REPLACE TEMP TABLE merged_main_ids AS "
                        f"SELECT DISTINCT id FROM read_parquet({json.dumps(main_glob)}, union_by_name=true)"
                    )
                    orphan_results: dict[str, Any] = {}
                    orphan_t0 = time.perf_counter()
                    for table_name in args.child_table:
                        child_glob = _table_glob(snapshot_root / table_name)
                        _log(log_fp, f"child orphan start table={table_name}")
                        total_rows = con.execute(
                            f"SELECT COUNT(*) FROM read_parquet({json.dumps(child_glob)}, union_by_name=true)"
                        ).fetchone()[0]
                        orphan_rows = con.execute(
                            f"SELECT COUNT(*) "
                            f"FROM read_parquet({json.dumps(child_glob)}, union_by_name=true) c "
                            "ANTI JOIN merged_main_ids m USING (id)"
                        ).fetchone()[0]
                        orphan_results[table_name] = {
                            "rows": int(total_rows),
                            "orphan_rows": int(orphan_rows),
                            "all_ids_present_in_main": int(orphan_rows) == 0,
                        }
                        save()
                    orphan_elapsed = time.perf_counter() - orphan_t0
                    state["steps"]["child_orphans"] = {
                        "status": "done",
                        "duration_sec": round(orphan_elapsed, 1),
                        "tables": orphan_results,
                    }
                    save()
                    _log(log_fp, f"child orphan complete duration_sec={orphan_elapsed:.1f}")
            finally:
                con.close()

        state["status"] = "done"
        save()
        _write_json(report_path, state)
        _log(log_fp, "validation complete")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
