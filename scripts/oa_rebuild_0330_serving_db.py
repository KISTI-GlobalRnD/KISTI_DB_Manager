#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

from KISTI_DB_Manager.config import coerce_db_config
from KISTI_DB_Manager.openalex_serving import (
    AFFILIATION_AGG_TABLE,
    CANONICAL_PREFIX_0330,
    CORE_TABLE_ORDER,
    build_serving_symlink_layout,
)
from KISTI_DB_Manager.runstate import JsonRunState, atomic_write_json, read_json, utc_now_iso
import pymysql

DEFAULT_HDD_RUNS_ROOT = Path("/home/kimyoungjin06/Desktop/HDD/runs")
DEFAULT_HDD_TMP_ROOT = Path("/home/kimyoungjin06/Desktop/HDD/tmp")


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
    dbc = dict(coerce_db_config(db_config, inplace=False))
    password = str(dbc.get("password") or "")
    if password and password != "***":
        return dbc
    env = _read_env_like(dotenv_path) if dotenv_path else {}
    for key in ("MARIADB_ROOT_PASSWORD", "MARIADB_PASSWORD", "MYSQL_PASSWORD", "MYSQL_ROOT_PASSWORD"):
        value = str(env.get(key) or "").strip()
        if value:
            dbc["password"] = value
            return dbc
    raise RuntimeError("Could not restore DB password from dotenv")


def _mask_password(db_config: dict[str, Any]) -> dict[str, Any]:
    masked = dict(db_config)
    if "password" in masked and masked["password"]:
        masked["password"] = "***"
    return masked


def _reset_database(*, db_name: str, db_config: dict[str, Any]) -> None:
    conn = pymysql.connect(
        host=db_config.get("host"),
        user=db_config.get("user"),
        password=db_config.get("password"),
        port=int(db_config.get("port") or 3306),
        charset="utf8mb4",
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS `{db_name}`")
            cur.execute(f"CREATE DATABASE `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    finally:
        conn.close()


def _archive_materialize_progress_for_fresh_db(run_dir: Path) -> Path | None:
    progress_path = run_dir / "parquet_materialize" / "progress.json"
    if not progress_path.exists():
        return None
    ts = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S_%f")
    backup_path = progress_path.with_name(f"{progress_path.name}.fresh_db_reset.{ts}")
    shutil.move(str(progress_path), str(backup_path))
    return backup_path


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Clean rebuild of OpenAlex 0330 serving DB from canonical parquet.")
    ap.add_argument("--snapshot-root", default="/home/kimyoungjin06/Desktop/Disk/Raid/data/OpenAlex/parquet_exports/openalex_works_20260330_repairreplay_20260410_190630")
    ap.add_argument("--abstract-root", default="/home/kimyoungjin06/Desktop/HDD/Data/OpenAlex/reconstructed_abstract/openalex_works_abstract_reconstruct_20260330_20260412_231728/works_abstract_parquet")
    ap.add_argument("--materialize-config", default="runs/openalex_works_20260330_repairreplay_materialize_20260411_085900/config.json")
    ap.add_argument("--dotenv", default=".env")
    ap.add_argument("--db-name", default="openalex_20260330_raw_yjk")
    ap.add_argument("--run-dir", default="")
    ap.add_argument("--temp-dir", default="")
    ap.add_argument("--threads", type=int, default=16)
    ap.add_argument("--parallel-tables", type=int, default=1)
    ap.add_argument("--affiliation-agg-threads", type=int, default=4)
    ap.add_argument("--affiliation-agg-memory-limit", default="48GB")
    ap.add_argument("--affiliation-agg-source-batch-files", type=int, default=8)
    ap.add_argument("--affiliation-agg-bucket-count", type=int, default=256)
    ap.add_argument("--affiliation-agg-max-rows-per-file", type=int, default=1_000_000)
    ap.add_argument("--finalize-skip-unique-indexes", action="store_true")
    ap.add_argument("--finalize-strict-indexes", action="store_true")
    ap.add_argument("--skip-reload-validation", action="store_true")
    ap.add_argument("--reload-validation-max-statement-time", type=int, default=0)
    ap.add_argument("--reload-validation-skip-literal-null-marker-scan", action="store_true")
    ap.add_argument("--reload-validation-skip-samples", action="store_true")
    ap.add_argument("--reload-validation-skip-prefix-collision-sample", action="store_true")
    ap.add_argument("--reload-validation-skip-key-bucket-check", action="store_true")
    ap.add_argument("--reload-validation-key-bucket-prefix-length", type=int, default=1)
    ap.add_argument("--reload-validation-skip-orphans", action="store_true")
    ap.add_argument("--reload-validation-skip-sample-checksum", action="store_true")
    ap.add_argument("--reload-validation-checksum-sample-size", type=int, default=1000)
    ap.add_argument("--reload-validation-resume", action="store_true")
    ap.add_argument("--skip-drop-db", action="store_true")
    ap.add_argument("--skip-affiliation-agg", action="store_true")
    ap.add_argument("--prepare-only", action="store_true")
    return ap.parse_args()


def _report_completed(report_path: Path, *, table_name: str | None = None, db_name: str | None = None) -> bool:
    if not report_path.exists() or report_path.stat().st_size <= 0:
        return False
    try:
        payload = read_json(report_path)
    except Exception:
        return False
    if not payload.get("finished_at"):
        return False
    if payload.get("issues"):
        return False

    artifacts = payload.get("artifacts") or {}
    if db_name:
        report_db = str(artifacts.get("db_name") or "").strip()
        if report_db and report_db != str(db_name).strip():
            return False

    if table_name:
        expected = str(table_name).strip()
        selected_tables = artifacts.get("selected_tables")
        if isinstance(selected_tables, list) and selected_tables and expected not in {str(item) for item in selected_tables}:
            return False
        completed_tables = artifacts.get("tables_completed_session")
        if isinstance(completed_tables, list) and completed_tables and expected not in {str(item) for item in completed_tables}:
            return False
        per_table = artifacts.get("per_table")
        if isinstance(per_table, list) and per_table:
            matches = [item for item in per_table if isinstance(item, dict) and str(item.get("table_original") or "") == expected]
            if not matches:
                return False
            if not any(item.get("files") for item in matches):
                return False
    return True


def _summary_done(report_path: Path) -> bool:
    if not report_path.exists() or report_path.stat().st_size <= 0:
        return False
    try:
        payload = read_json(report_path)
    except Exception:
        return False
    return str(payload.get("status") or "") == "done"


def _run_subprocess(cmd: list[str], *, cwd: Path, log_path: Path) -> None:
    with log_path.open("a", encoding="utf-8") as log_fp:
        log_fp.write("$ " + " ".join(cmd) + "\n")
        log_fp.flush()
        proc = subprocess.Popen(cmd, cwd=str(cwd), stdout=log_fp, stderr=subprocess.STDOUT, env=dict(os.environ))
        rc = proc.wait()
        log_fp.write(f"[exit_code={rc}]\n")
        if rc != 0:
            raise RuntimeError(f"command failed ({rc}): {' '.join(cmd)}")


def _run_tracked_subprocess(
    cmd: list[str],
    *,
    cwd: Path,
    log_path: Path,
    progress: JsonRunState,
    phase: str,
) -> None:
    progress.update(current_step=str(phase))
    try:
        _run_subprocess(cmd, cwd=cwd, log_path=log_path)
    except Exception as exc:
        progress.set_status(
            "failed",
            failed_at=utc_now_iso(),
            failed_phase=str(phase),
            error_type=type(exc).__name__,
            error=str(exc),
        )
        raise


def main() -> int:
    args = _parse_args()
    repo_root = Path(__file__).resolve().parent.parent
    snapshot_root = Path(args.snapshot_root).expanduser().resolve()
    abstract_root = Path(args.abstract_root).expanduser().resolve()
    materialize_cfg_path = Path(args.materialize_config).expanduser().resolve()
    dotenv_path = Path(args.dotenv).expanduser().resolve() if args.dotenv else None
    ts = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    run_dir = Path(args.run_dir).expanduser().resolve() if args.run_dir else (DEFAULT_HDD_RUNS_ROOT / f"openalex_0330_serving_rebuild_{ts}")
    temp_dir = Path(args.temp_dir).expanduser().resolve() if args.temp_dir else (DEFAULT_HDD_TMP_ROOT / f"openalex_0330_serving_rebuild_{ts}")
    layout_root = run_dir / "serving_parquet_root"
    generated_root = run_dir / "generated"
    aff_agg_dir = generated_root / AFFILIATION_AGG_TABLE
    log_dir = run_dir / "logs"
    staging_dir = temp_dir / "staging"
    progress_path = run_dir / "progress.json"
    rebuild_log = log_dir / "rebuild.log"
    final_validation = run_dir / "final_validation.json"
    reload_validation = run_dir / "reload_validation.json"

    run_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    temp_dir.mkdir(parents=True, exist_ok=True)
    staging_dir.mkdir(parents=True, exist_ok=True)

    materialize_cfg = read_json(materialize_cfg_path)
    db_config_real = _hydrate_db_password(materialize_cfg.get("db_config") or {}, dotenv_path=dotenv_path)
    db_config_real["database"] = str(args.db_name).strip()
    db_config_masked = _mask_password(db_config_real)

    progress = JsonRunState.create(
        progress_path,
        {
            "status": "running",
            "generated_at": utc_now_iso(),
            "run_dir": str(run_dir),
            "snapshot_root": str(snapshot_root),
            "abstract_root": str(abstract_root),
            "db_name": str(args.db_name),
            "steps": {
                "layout": "pending",
                "db_reset": "pending",
                "core_load": "pending",
                "rest_load": "pending",
                "affiliation_agg_build": "pending",
                "affiliation_agg_load": "pending",
                "reload_validation": "pending",
                "finalize": "pending",
            },
        },
    )
    aff_agg_dir.mkdir(parents=True, exist_ok=True)

    if layout_root.exists():
        shutil.rmtree(layout_root)
    specs = build_serving_symlink_layout(
        snapshot_root=snapshot_root,
        abstract_parquet_dir=abstract_root,
        aff_agg_parquet_dir=aff_agg_dir,
        layout_root=layout_root,
        canonical_prefix=CANONICAL_PREFIX_0330,
    )
    serving_config = {
        "data_config": {
            "KEY_SEP": "__",
            "extra_column_name": "__extra__",
            "persist_parquet_dir": str(layout_root),
        },
        "db_config": db_config_masked,
    }
    atomic_write_json(run_dir / "config.json", serving_config)
    atomic_write_json(
        run_dir / "table_specs.json",
        {"generated_at": utc_now_iso(), "specs": [dict(spec.__dict__) for spec in specs]},
    )
    progress.payload["steps"]["layout"] = "done"
    progress.update(layout_root=str(layout_root))

    if args.prepare_only:
        progress.set_status("prepared")
        return 0

    if not args.skip_drop_db:
        materialize_progress_backup = _archive_materialize_progress_for_fresh_db(run_dir)
        if materialize_progress_backup is not None:
            progress.update(materialize_progress_backup=str(materialize_progress_backup))
        _reset_database(db_name=str(args.db_name), db_config=db_config_real)
        progress.payload["steps"]["db_reset"] = "done"
        progress.write(touch_timestamp=True)
    else:
        progress.payload["steps"]["db_reset"] = "skipped"
        progress.write(touch_timestamp=True)

    spec_map = {spec.target_table: spec for spec in specs}
    early_core_tables = [name for name in CORE_TABLE_ORDER if name in spec_map and name != AFFILIATION_AGG_TABLE]
    deferred_tables = {AFFILIATION_AGG_TABLE}
    rest_tables = [spec.target_table for spec in specs if spec.target_table not in set(early_core_tables) | deferred_tables]

    for phase_name, table_names in (("core_load", early_core_tables), ("rest_load", rest_tables)):
        for table_name in table_names:
            spec = spec_map[table_name]
            phase_report = run_dir / "reports" / f"{table_name}.json"
            phase_cmd = [
                str(repo_root / ".venv" / "bin" / "python"),
                "scripts/oa_materialize_parquet_to_db.py",
                str(run_dir),
                "--dotenv",
                str(dotenv_path or repo_root / ".env"),
                "--db-name",
                str(args.db_name),
                "--report",
                str(phase_report),
                "--parallel-tables",
                str(max(1, int(args.parallel_tables))),
                "--parallel-files-per-table",
                "1",
                "--file-chunk-rows",
                str(int(spec.file_chunk_rows)),
                "--staging-writer",
                str(spec.stage_writer),
                "--staging-dir",
                str(staging_dir),
                "--table",
                str(spec.target_table),
            ]
            progress.update(current_table=table_name)
            if args.skip_drop_db and _report_completed(
                phase_report,
                table_name=table_name,
                db_name=str(args.db_name),
            ):
                with rebuild_log.open("a", encoding="utf-8") as log_fp:
                    log_fp.write(f"[skip_completed_report] {table_name} -> {phase_report}\n")
                progress.add_list_item("completed_tables", table_name)
                continue
            _run_tracked_subprocess(
                phase_cmd,
                cwd=repo_root,
                log_path=rebuild_log,
                progress=progress,
                phase=f"{phase_name}:{table_name}",
            )
            progress.add_list_item("completed_tables", table_name)
        progress.payload["steps"][phase_name] = "done"
        progress.write(touch_timestamp=True)

    if not args.skip_affiliation_agg:
        progress.payload["current_table"] = AFFILIATION_AGG_TABLE
        progress.payload["steps"]["affiliation_agg_build"] = "running"
        progress.write(touch_timestamp=True)
        aff_build_report = run_dir / "reports" / f"{AFFILIATION_AGG_TABLE}.build.json"
        aff_build_cmd = [
            str(repo_root / ".venv" / "bin" / "python"),
            "scripts/oa_build_works_affiliation_agg.py",
            "--source-dir",
            str(snapshot_root / f"{CANONICAL_PREFIX_0330}__authorships"),
            "--out-dir",
            str(aff_agg_dir),
            "--temp-dir",
            str(temp_dir / "duckdb_affiliation_agg"),
            "--threads",
            str(max(1, int(args.affiliation_agg_threads))),
            "--memory-limit",
            str(args.affiliation_agg_memory_limit),
            "--max-rows-per-file",
            str(max(1, int(args.affiliation_agg_max_rows_per_file))),
            "--source-batch-files",
            str(max(1, int(args.affiliation_agg_source_batch_files))),
            "--bucket-count",
            str(max(1, int(args.affiliation_agg_bucket_count))),
            "--summary-out",
            str(aff_build_report),
        ]
        if args.skip_drop_db and _summary_done(aff_build_report):
            with rebuild_log.open("a", encoding="utf-8") as log_fp:
                log_fp.write(f"[skip_completed_build] {AFFILIATION_AGG_TABLE} -> {aff_build_report}\n")
            summary = read_json(aff_build_report)
        else:
            _run_tracked_subprocess(
                aff_build_cmd,
                cwd=repo_root,
                log_path=rebuild_log,
                progress=progress,
                phase="affiliation_agg_build",
            )
            summary = read_json(aff_build_report)
        progress.payload["steps"]["affiliation_agg_build"] = "done"
        progress.update(affiliation_agg=summary)

        spec = spec_map[AFFILIATION_AGG_TABLE]
        phase_report = run_dir / "reports" / f"{AFFILIATION_AGG_TABLE}.json"
        phase_cmd = [
            str(repo_root / ".venv" / "bin" / "python"),
            "scripts/oa_materialize_parquet_to_db.py",
            str(run_dir),
            "--dotenv",
            str(dotenv_path or repo_root / ".env"),
            "--db-name",
            str(args.db_name),
            "--report",
            str(phase_report),
            "--parallel-tables",
            str(max(1, int(args.parallel_tables))),
            "--parallel-files-per-table",
            "1",
            "--file-chunk-rows",
            str(int(spec.file_chunk_rows)),
            "--staging-writer",
            str(spec.stage_writer),
            "--staging-dir",
            str(staging_dir),
            "--table",
            str(spec.target_table),
        ]
        progress.payload["steps"]["affiliation_agg_load"] = "running"
        progress.write(touch_timestamp=True)
        if args.skip_drop_db and _report_completed(
            phase_report,
            table_name=AFFILIATION_AGG_TABLE,
            db_name=str(args.db_name),
        ):
            with rebuild_log.open("a", encoding="utf-8") as log_fp:
                log_fp.write(f"[skip_completed_report] {AFFILIATION_AGG_TABLE} -> {phase_report}\n")
        else:
            _run_tracked_subprocess(
                phase_cmd,
                cwd=repo_root,
                log_path=rebuild_log,
                progress=progress,
                phase="affiliation_agg_load",
            )
        progress.payload["steps"]["affiliation_agg_load"] = "done"
        progress.write(touch_timestamp=True)
    else:
        progress.payload["steps"]["affiliation_agg_build"] = "skipped"
        progress.payload["steps"]["affiliation_agg_load"] = "skipped"
        progress.write(touch_timestamp=True)

    if args.skip_reload_validation:
        progress.payload["steps"]["reload_validation"] = "skipped"
        progress.write(touch_timestamp=True)
    else:
        reload_validation_cmd = [
            str(repo_root / ".venv" / "bin" / "python"),
            "scripts/oa_validate_serving_reload.py",
            str(run_dir),
            "--dotenv",
            str(dotenv_path or repo_root / ".env"),
            "--db-name",
            str(args.db_name),
            "--out",
            str(reload_validation),
            "--max-statement-time",
            str(int(args.reload_validation_max_statement_time)),
            "--checksum-sample-size",
            str(int(args.reload_validation_checksum_sample_size)),
            "--key-bucket-prefix-length",
            str(int(args.reload_validation_key_bucket_prefix_length)),
        ]
        if args.reload_validation_resume:
            reload_validation_cmd.append("--resume")
        if args.reload_validation_skip_samples:
            reload_validation_cmd.append("--skip-samples")
        if args.reload_validation_skip_literal_null_marker_scan:
            reload_validation_cmd.append("--skip-literal-null-marker-scan")
        if args.reload_validation_skip_sample_checksum:
            reload_validation_cmd.append("--skip-sample-checksum")
        if args.reload_validation_skip_prefix_collision_sample:
            reload_validation_cmd.append("--skip-prefix-collision-sample")
        if args.reload_validation_skip_key_bucket_check:
            reload_validation_cmd.append("--skip-key-bucket-check")
        if args.reload_validation_skip_orphans:
            reload_validation_cmd.append("--skip-orphans")
        progress.payload["steps"]["reload_validation"] = "running"
        progress.payload.pop("current_table", None)
        progress.write(touch_timestamp=True)
        _run_tracked_subprocess(
            reload_validation_cmd,
            cwd=repo_root,
            log_path=rebuild_log,
            progress=progress,
            phase="reload_validation",
        )
        progress.payload["steps"]["reload_validation"] = "done"
        progress.payload["reload_validation"] = str(reload_validation)
        progress.write(touch_timestamp=True)

    finalize_cmd = [
        str(repo_root / ".venv" / "bin" / "python"),
        "scripts/oa_finalize_openalex_serving_db.py",
        "--config",
        str(run_dir / "config.json"),
        "--dotenv",
        str(dotenv_path or repo_root / ".env"),
        "--out",
        str(final_validation),
    ]
    if args.finalize_skip_unique_indexes:
        finalize_cmd.append("--skip-unique-indexes")
    if args.finalize_strict_indexes:
        finalize_cmd.append("--strict-indexes")
    progress.payload["steps"]["finalize"] = "running"
    progress.payload.pop("current_table", None)
    progress.write(touch_timestamp=True)
    _run_tracked_subprocess(
        finalize_cmd,
        cwd=repo_root,
        log_path=rebuild_log,
        progress=progress,
        phase="finalize",
    )
    progress.payload["steps"]["finalize"] = "done"
    progress.payload["final_validation"] = str(final_validation)
    progress.payload.pop("current_table", None)
    progress.write(touch_timestamp=True)

    progress.set_status("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
