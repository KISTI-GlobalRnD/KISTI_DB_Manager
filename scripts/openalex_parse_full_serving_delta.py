#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from KISTI_DB_Manager.openalex_snapshot import (
    FULL_SERVING_CORE_ENTITIES,
    FULL_SERVING_ENTITIES,
    FULL_SERVING_REFERENCE_ENTITIES,
)

DEFAULT_HDD_RUNS_ROOT = Path("/home/kimyoungjin06/Desktop/HDD/runs")


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Parse non-works OpenAlex full-serving delta slices into parquet artifacts.")
    ap.add_argument(
        "--source-root",
        default="/home/kimyoungjin06/Desktop/HDD/Data/OpenAlex/openalex-snapshot-delta-full(20260330)",
        help="Root containing per-entity updated_date directories",
    )
    ap.add_argument(
        "--parquet-root",
        required=True,
        help="Destination root for parsed parquet artifacts (one subdir per entity table)",
    )
    ap.add_argument("--run-dir", default="", help="Run directory for configs/logs/progress")
    ap.add_argument("--group", choices=["full", "core", "reference"], default="full")
    ap.add_argument("--skip-works", action="store_true", default=True)
    ap.add_argument("--target-date", default="20260330", help="Suffix used in generated table names")
    ap.add_argument("--start-entity", default="", help="Optional entity name to resume from")
    return ap.parse_args()


def _entities_for_group(group: str) -> list[str]:
    if group == "core":
        return list(FULL_SERVING_CORE_ENTITIES)
    if group == "reference":
        return list(FULL_SERVING_REFERENCE_ENTITIES)
    return list(FULL_SERVING_ENTITIES)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _report_completed(report_path: Path) -> bool:
    if not report_path.exists() or report_path.stat().st_size <= 0:
        return False
    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    return bool(payload.get("finished_at"))


def _resume_cursor_from_progress(progress_path: Path) -> dict | None:
    if not progress_path.exists() or progress_path.stat().st_size <= 0:
        return None
    try:
        payload = json.loads(progress_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    loaded = payload.get("loaded")
    if isinstance(loaded, dict):
        cursor = loaded.get("cursor")
        if isinstance(cursor, dict) and cursor.get("source_path"):
            return dict(cursor)
    cursor = payload.get("cursor")
    if isinstance(cursor, dict) and cursor.get("source_path"):
        return dict(cursor)
    return None


def _build_config(*, source_root: Path, parquet_dir: Path, entity: str, target_date: str) -> dict:
    return {
        "data_config": {
            "PATH": str(source_root),
            "file_type": "gz",
            "file_glob": f"{entity}/updated_date=*/part_*.gz",
            "table_name": f"openalex_{entity}_{target_date}_delta",
            "KEY_SEP": "__",
            "index_key": "id",
            "except_keys": [],
            "schema_mode": "hybrid",
            "schema_hybrid_warmup_batches": 1,
            "extra_column_name": "__extra__",
            "db_load_method": "auto",
            "json_streaming_load": False,
            "chunk_size": 5000,
            "parallel_workers": 0,
            "db_load_parallel_tables": 0,
            "overlap_batches": False,
            "tsv_merge_union_schema": True,
            "excepted_expand_dict": False,
            "auto_except": True,
            "auto_except_sample_records": 12000,
            "auto_except_sample_max_sources": 128,
            "auto_except_seed": 42,
            "auto_except_unique_key_threshold": 512,
            "auto_except_min_observations": 20,
            "auto_except_novelty_threshold": 2.0,
            "persist_parquet_files": True,
            "persist_parquet_dir": str(parquet_dir),
            "persist_tsv_files": False,
        },
        "db_config": {},
    }


def main() -> int:
    args = parse_args()
    source_root = Path(args.source_root).expanduser().resolve()
    parquet_root = Path(args.parquet_root).expanduser().resolve()
    ts = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    run_dir = Path(args.run_dir).expanduser().resolve() if args.run_dir else (DEFAULT_HDD_RUNS_ROOT / f"openalex_full_serving_parse_{ts}")
    configs_dir = run_dir / "configs"
    logs_dir = run_dir / "logs"
    reports_dir = run_dir / "reports"
    entities = _entities_for_group(args.group)
    if args.skip_works:
        entities = [entity for entity in entities if entity != "works"]
    if args.start_entity:
        if args.start_entity not in entities:
            raise SystemExit(f"--start-entity not in selected group: {args.start_entity}")
        entities = entities[entities.index(args.start_entity) :]

    progress = {
        "status": "running",
        "generated_at": _now_utc(),
        "source_root": str(source_root),
        "parquet_root": str(parquet_root),
        "group": args.group,
        "target_date": args.target_date,
        "entities": {entity: {"status": "pending"} for entity in entities},
        "current_entity": None,
    }
    _write_json(run_dir / "progress.json", progress)
    configs_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    cli = ROOT / ".venv" / "bin" / "python"
    if not cli.exists():
        raise SystemExit(f"missing interpreter: {cli}")

    for entity in entities:
        entity_source_dir = source_root / entity
        entity_files = sorted(entity_source_dir.glob("updated_date=*/part_*.gz"))
        progress["current_entity"] = entity
        progress["generated_at"] = _now_utc()
        progress["entities"][entity] = {
            "status": "running",
            "source_dir": str(entity_source_dir),
            "source_file_count": len(entity_files),
        }
        _write_json(run_dir / "progress.json", progress)

        if not entity_files:
            progress["entities"][entity]["status"] = "skipped_empty"
            progress["generated_at"] = _now_utc()
            _write_json(run_dir / "progress.json", progress)
            continue

        entity_run_dir = run_dir / entity
        entity_run_dir.mkdir(parents=True, exist_ok=True)
        parquet_dir = parquet_root / f"openalex_{entity}_{args.target_date}_delta"
        config = _build_config(source_root=source_root, parquet_dir=parquet_dir, entity=entity, target_date=args.target_date)
        config_path = configs_dir / f"{entity}.config.json"
        report_path = reports_dir / f"{entity}.report.json"
        report_progress_path = reports_dir / f"{entity}.report.json.progress.json"
        quarantine_path = reports_dir / f"{entity}.quarantine.jsonl"
        log_path = logs_dir / f"{entity}.parse.log"
        resume_cursor = _resume_cursor_from_progress(report_progress_path)
        if resume_cursor:
            config["data_config"]["resume_cursor"] = resume_cursor
        _write_json(config_path, config)

        if _report_completed(report_path):
            progress["entities"][entity] = {
                "status": "done",
                "source_dir": str(entity_source_dir),
                "source_file_count": len(entity_files),
                "config": str(config_path),
                "parquet_dir": str(parquet_dir),
                "report": str(report_path),
                "quarantine": str(quarantine_path),
                "log": str(log_path),
            }
            progress["generated_at"] = _now_utc()
            _write_json(run_dir / "progress.json", progress)
            continue

        cmd = [
            str(cli),
            "-m",
            "KISTI_DB_Manager.cli",
            "json",
            "run",
            "--config",
            str(config_path),
            "--mode",
            "parse-parquet-safe",
            "--no-create",
            "--no-load",
            "--no-index",
            "--no-optimize",
            "--report",
            str(report_path),
            "--quarantine",
            str(quarantine_path),
        ]

        env = dict(os.environ)
        env["PYTHONUNBUFFERED"] = "1"
        with log_path.open("w", encoding="utf-8") as log:
            log.write(json.dumps({"entity": entity, "cmd": cmd}, ensure_ascii=False) + "\n")
            log.flush()
            subprocess.run(cmd, cwd=str(ROOT), env=env, stdout=log, stderr=subprocess.STDOUT, check=True)

        progress["entities"][entity] = {
            "status": "done",
            "source_dir": str(entity_source_dir),
            "source_file_count": len(entity_files),
            "config": str(config_path),
            "parquet_dir": str(parquet_dir),
            "report": str(report_path),
            "quarantine": str(quarantine_path),
            "log": str(log_path),
        }
        progress["generated_at"] = _now_utc()
        _write_json(run_dir / "progress.json", progress)

    progress["status"] = "done"
    progress["current_entity"] = None
    progress["generated_at"] = _now_utc()
    _write_json(run_dir / "progress.json", progress)
    print(json.dumps({"done": True, "run_dir": str(run_dir), "parquet_root": str(parquet_root)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
