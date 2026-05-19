from __future__ import annotations

import fcntl
import json
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .runstate import atomic_write_json, open_append_text, prepare_output_file_path


REPO_DIR = Path(__file__).resolve().parents[1]


class ReloadPlanError(ValueError):
    pass


class ReloadStepFailed(RuntimeError):
    pass


VALIDATION_PROFILE_DEFAULTS: dict[str, dict[str, Any]] = {
    "large": {
        "skip_parquet_key_health": True,
        "skip_db_key_health": True,
        "skip_key_bucket_check": True,
        "skip_orphans": True,
        "skip_sample_checksum": True,
        "skip_row_bucket_checksum": True,
        "skip_source_literal_null_marker_scan": True,
        "skip_literal_null_marker_scan": True,
        "literal_null_marker_count_mode": "count",
    },
    "smoke": {
        "skip_parquet_key_health": True,
        "skip_db_key_health": True,
        "skip_key_bucket_check": True,
        "skip_orphans": True,
        "skip_sample_checksum": True,
        "skip_row_bucket_checksum": True,
        "skip_source_literal_null_marker_scan": True,
        "skip_literal_null_marker_scan": False,
        "literal_null_marker_count_mode": "exists",
    },
    "deep": {
        "skip_parquet_key_health": False,
        "skip_db_key_health": False,
        "skip_key_bucket_check": False,
        "skip_orphans": False,
        "skip_sample_checksum": False,
        "skip_row_bucket_checksum": True,
        "skip_source_literal_null_marker_scan": False,
        "skip_literal_null_marker_scan": False,
        "literal_null_marker_count_mode": "count",
    },
}


@dataclass(frozen=True)
class ReloadTableSpec:
    name: str
    writer: str = "duckdb"
    chunk_rows: int = 0
    reset: bool = True
    expected_rows: int | None = None
    validation_profile: str = "large"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    payload["updated_at"] = utc_now_iso()
    atomic_write_json(path, payload)


def command_to_str(cmd: list[str]) -> str:
    return shlex.join(cmd)


def load_plan(plan_path: Path) -> dict[str, Any]:
    plan = read_json(Path(plan_path).expanduser().resolve())
    if not isinstance(plan, dict):
        raise ReloadPlanError("plan must be a JSON object")
    return plan


def _path_from_plan(plan: dict[str, Any], key: str, default: str = "") -> Path:
    value = str(plan.get(key) or default).strip()
    if not value:
        raise ReloadPlanError(f"plan.{key} is required")
    return Path(value).expanduser().resolve()


def _path_from_plan_no_resolve(plan: dict[str, Any], key: str, default: Path | str = "") -> Path:
    value = str(plan.get(key) or default).strip()
    if not value:
        raise ReloadPlanError(f"plan.{key} is required")
    expanded = Path(value).expanduser()
    if expanded.is_absolute():
        return expanded
    return Path.cwd() / expanded


def db_name_from_plan(plan: dict[str, Any]) -> str:
    value = str(plan.get("db_name") or "").strip()
    if value:
        return value
    db = plan.get("db")
    if isinstance(db, dict):
        value = str(db.get("name") or db.get("database") or "").strip()
        if value:
            return value
    try:
        run_dir = _path_from_plan(plan, "run_dir")
        config_path = Path(str(plan.get("config") or run_dir / "config.json")).expanduser().resolve()
        cfg = read_json(config_path)
        value = str(((cfg.get("db_config") or {}).get("database")) or "").strip()
        if value:
            return value
    except Exception:
        pass
    return ""


def bool_from_plan(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def normalize_table_specs(plan: dict[str, Any]) -> list[ReloadTableSpec]:
    raw = plan.get("tables")
    if not isinstance(raw, list) or not raw:
        raise ReloadPlanError("plan.tables must be a non-empty list")
    out: list[ReloadTableSpec] = []
    seen: set[str] = set()
    materialize = plan.get("materialize") if isinstance(plan.get("materialize"), dict) else {}
    default_writer = str(materialize.get("staging_writer") or "duckdb")
    default_chunk = int(materialize.get("file_chunk_rows") or 0)
    for item in raw:
        if not isinstance(item, dict):
            raise ReloadPlanError(f"table spec must be an object: {item!r}")
        name = str(item.get("name") or item.get("table") or "").strip()
        if not name:
            raise ReloadPlanError(f"table spec is missing name: {item}")
        if name in seen:
            raise ReloadPlanError(f"duplicate table in plan: {name}")
        seen.add(name)
        writer = str(item.get("writer") or item.get("staging_writer") or default_writer)
        if writer not in {"duckdb", "python"}:
            raise ReloadPlanError(f"invalid writer for {name}: {writer}")
        chunk_rows = int(item.get("chunk_rows") if item.get("chunk_rows") is not None else default_chunk)
        expected_raw = item.get("expected_rows")
        expected_rows = int(expected_raw) if expected_raw not in (None, "") else None
        out.append(
            ReloadTableSpec(
                name=name,
                writer=writer,
                chunk_rows=chunk_rows,
                reset=bool_from_plan(item.get("reset"), default=True),
                expected_rows=expected_rows,
                validation_profile=str(item.get("validation_profile") or "large"),
            )
        )
    return out


def validate_plan(plan: dict[str, Any]) -> None:
    _path_from_plan(plan, "run_dir")
    if not db_name_from_plan(plan):
        raise ReloadPlanError("plan.db_name or plan.db.name is required")
    specs = normalize_table_specs(plan)
    for spec in specs:
        if int(spec.chunk_rows or 0) <= 0:
            raise ReloadPlanError(f"chunk_rows must be positive for table {spec.name}")
        if spec.validation_profile not in VALIDATION_PROFILE_DEFAULTS:
            raise ReloadPlanError(f"invalid validation_profile for {spec.name}: {spec.validation_profile}")
    finalize = plan.get("finalize") if isinstance(plan.get("finalize"), dict) else {}
    if bool(finalize.get("enabled", False)) and not isinstance(finalize.get("indexes") or [], list):
        raise ReloadPlanError("plan.finalize.indexes must be a list")


def plan_paths(plan: dict[str, Any], plan_path: Path | None = None) -> dict[str, Path]:
    run_dir = _path_from_plan(plan, "run_dir")
    reports_dir = Path(str(plan.get("reports_dir") or run_dir / "reports")).expanduser().resolve()
    logs_dir = Path(str(plan.get("logs_dir") or run_dir / "logs")).expanduser().resolve()
    staging_dir = Path(str(plan.get("staging_dir") or run_dir / "staging")).expanduser().resolve()
    tag = str(plan.get("tag") or "parquet").strip()
    status_path = _path_from_plan_no_resolve(plan, "status_path", reports_dir / f"parquet_reload_status_{tag}.json")
    lock_path = _path_from_plan_no_resolve(plan, "lock_path", run_dir / f"parquet_reload_{tag}.lock")
    progress_path = Path(str(plan.get("progress_path") or run_dir / "parquet_materialize" / "progress.json")).expanduser().resolve()
    duckdb_temp_dir = Path(str(plan.get("duckdb_temp_dir") or staging_dir / "duckdb_tmp")).expanduser().resolve()
    return {
        "plan_path": Path(plan_path).expanduser().resolve() if plan_path else Path(""),
        "run_dir": run_dir,
        "reports_dir": reports_dir,
        "logs_dir": logs_dir,
        "staging_dir": staging_dir,
        "status_path": status_path,
        "lock_path": lock_path,
        "progress_path": progress_path,
        "duckdb_temp_dir": duckdb_temp_dir,
    }


def load_status(path: Path, *, plan: dict[str, Any], plan_path: Path) -> dict[str, Any]:
    if path.exists():
        try:
            payload = read_json(path)
            if isinstance(payload, dict):
                return payload
        except Exception:
            pass
    return {
        "status": "pending",
        "tag": str(plan.get("tag") or "parquet"),
        "plan": str(Path(plan_path).expanduser().resolve()),
        "run_dir": str(_path_from_plan(plan, "run_dir")),
        "database": db_name_from_plan(plan),
        "started_at": utc_now_iso(),
        "updated_at": None,
        "completed": [],
        "tables": {},
        "finalizer": {},
    }


def save_status(path: Path, payload: dict[str, Any], **updates: Any) -> None:
    if updates:
        payload.update(updates)
    write_json(path, payload)


def run_logged(cmd: list[str], *, log_path: Path, dry_run: bool = False) -> int:
    with open_append_text(log_path, purpose="parquet reload log") as log:
        log.write(f"\n[{utc_now_iso()}] $ {command_to_str(cmd)}\n")
        log.flush()
        if dry_run:
            log.write(f"[{utc_now_iso()}] dry-run: command not executed\n")
            return 0
        proc = subprocess.Popen(cmd, cwd=REPO_DIR, stdout=log, stderr=subprocess.STDOUT, text=True)
        rc = proc.wait()
        log.write(f"[{utc_now_iso()}] exit_code={rc}\n")
        return int(rc)


def selected_specs(specs: list[ReloadTableSpec], *, start_at: str = "", only_table: str = "") -> list[ReloadTableSpec]:
    selected = list(specs)
    if start_at:
        names = [spec.name for spec in selected]
        if start_at not in names:
            raise ReloadPlanError(f"--start-at table not in plan: {start_at}")
        selected = selected[names.index(start_at) :]
    if only_table:
        selected = [spec for spec in selected if spec.name == only_table]
        if not selected:
            raise ReloadPlanError(f"--only-table table not in plan: {only_table}")
    return selected


def materialize_cmd(plan: dict[str, Any], spec: ReloadTableSpec, *, report_path: Path) -> list[str]:
    paths = plan_paths(plan)
    materialize = plan.get("materialize") if isinstance(plan.get("materialize"), dict) else {}
    table_prefix = str(materialize.get("table_prefix") or "")
    cmd = [
        sys.executable,
        "-m",
        "KISTI_DB_Manager.openalex_materialize",
        str(paths["run_dir"]),
        "--dotenv",
        str(plan.get("dotenv") or ".env"),
        "--db-name",
        db_name_from_plan(plan),
        "--report",
        str(report_path),
        "--parallel-tables",
        str(int(materialize.get("parallel_tables") or 1)),
        "--parallel-files-per-table",
        str(int(materialize.get("parallel_files_per_table") or 1)),
        "--file-chunk-rows",
        str(int(spec.chunk_rows)),
        "--staging-writer",
        spec.writer,
        "--staging-dir",
        str(paths["staging_dir"]),
        "--table",
        spec.name,
    ]
    if str(plan.get("parquet_root") or "").strip():
        cmd.extend(["--parquet-root", str(Path(str(plan.get("parquet_root"))).expanduser().resolve())])
    if table_prefix:
        cmd.extend(["--table-prefix", table_prefix])
    preflight = plan.get("preflight") if isinstance(plan.get("preflight"), dict) else {}
    artifact_contract = preflight.get("artifact_contract") if isinstance(preflight.get("artifact_contract"), dict) else {}
    if bool(artifact_contract.get("require_schema_manifest", False)):
        cmd.append("--require-schema-manifest")
    if bool(artifact_contract.get("require_id_compaction", False)):
        cmd.append("--require-id-compaction")
    if bool(artifact_contract.get("strict_schema_manifest", False)):
        cmd.append("--strict-schema-manifest")
    if spec.reset:
        cmd.extend(["--reset-selected-tables", "--confirm-drop-tables", f"{table_prefix}{spec.name}"])
    return cmd


def validation_cmd(plan: dict[str, Any], spec: ReloadTableSpec, *, report_path: Path) -> list[str]:
    paths = plan_paths(plan)
    validation = plan.get("validation") if isinstance(plan.get("validation"), dict) else {}
    profile = str(validation.get("profile") or spec.validation_profile or "large").strip().lower()
    if profile not in VALIDATION_PROFILE_DEFAULTS:
        profile = "large"
    defaults = dict(VALIDATION_PROFILE_DEFAULTS[profile])
    literal_marker = validation.get("literal_marker") if isinstance(validation.get("literal_marker"), dict) else {}
    marker_mode = str(literal_marker.get("mode") or validation.get("literal_marker_mode") or "").strip().lower()
    if marker_mode == "off":
        defaults["skip_literal_null_marker_scan"] = True
        defaults["skip_source_literal_null_marker_scan"] = True
    elif marker_mode in {"full", "columns"}:
        defaults["skip_literal_null_marker_scan"] = False
        defaults["literal_null_marker_count_mode"] = "count"
    elif marker_mode == "sample":
        defaults["skip_literal_null_marker_scan"] = False
        defaults["skip_source_literal_null_marker_scan"] = True
        defaults["literal_null_marker_count_mode"] = "exists"
    config_path = Path(str(plan.get("config") or paths["run_dir"] / "config.json")).expanduser().resolve()
    cmd = [
        sys.executable,
        "scripts/oa_validate_serving_reload.py",
        str(paths["run_dir"]),
        "--config",
        str(config_path),
        "--dotenv",
        str(plan.get("dotenv") or ".env"),
        "--db-name",
        db_name_from_plan(plan),
        "--out",
        str(report_path),
        "--table",
        spec.name,
        "--literal-null-marker",
        str(validation.get("literal_null_marker", r"\N")),
        "--literal-null-marker-compare-mode",
        str(validation.get("literal_marker_compare_mode") or validation.get("literal_null_marker_compare_mode") or "utf8mb4_bin"),
        "--literal-null-marker-count-mode",
        str(validation.get("literal_null_marker_count_mode") or literal_marker.get("count_mode") or defaults.get("literal_null_marker_count_mode") or "count"),
        "--duckdb-temp-dir",
        str(paths["duckdb_temp_dir"]),
        "--threads",
        str(int(validation.get("threads") or 4)),
        "--memory-limit",
        str(validation.get("memory_limit") or "64GB"),
    ]
    option_map = {
        "skip_parquet_key_health": "--skip-parquet-key-health",
        "skip_db_key_health": "--skip-db-key-health",
        "skip_key_bucket_check": "--skip-key-bucket-check",
        "skip_orphans": "--skip-orphans",
        "skip_sample_checksum": "--skip-sample-checksum",
        "skip_row_bucket_checksum": "--skip-row-bucket-checksum",
        "skip_source_literal_null_marker_scan": "--skip-source-literal-null-marker-scan",
        "skip_literal_null_marker_scan": "--skip-literal-null-marker-scan",
    }
    for key, flag in option_map.items():
        if bool(validation.get(key, defaults[key])):
            cmd.append(flag)
    marker_columns = literal_marker.get("columns") or validation.get("literal_null_marker_columns") or []
    if isinstance(marker_columns, str):
        marker_columns = [item.strip() for item in marker_columns.split(",") if item.strip()]
    marker_columns_iter = marker_columns if isinstance(marker_columns, list) else []
    for column in marker_columns_iter:
        if str(column).strip():
            cmd.extend(["--literal-null-marker-column", str(column).strip()])
    if bool(validation.get("resume", False)):
        cmd.append("--resume")
    return cmd


def preflight_enabled(plan: dict[str, Any], *, skip_preflight: bool = False) -> bool:
    if skip_preflight:
        return False
    preflight = plan.get("preflight") if isinstance(plan.get("preflight"), dict) else {}
    return bool(preflight.get("enabled", True))


def finalizer_cmd(plan_path: Path, plan: dict[str, Any], *, report_path: Path) -> list[str]:
    finalize = plan.get("finalize") if isinstance(plan.get("finalize"), dict) else {}
    cmd = [sys.executable, "scripts/parquet_finalize_db.py", "--plan", str(Path(plan_path).expanduser().resolve()), "--out", str(report_path)]
    if bool(finalize.get("strict_indexes", False)):
        cmd.append("--strict-indexes")
    if bool(finalize.get("no_unique_fallback", False)):
        cmd.append("--no-unique-fallback")
    if bool(finalize.get("skip_analyze", True)):
        cmd.append("--skip-analyze")
    if bool(finalize.get("skip_validation", True)):
        cmd.append("--skip-validation")
    return cmd


def verify_materialize_report(plan: dict[str, Any], spec: ReloadTableSpec, report_path: Path) -> dict[str, Any]:
    if not report_path.exists():
        raise ReloadStepFailed(f"materialize report missing: {report_path}")
    report = read_json(report_path)
    issues = report.get("issues") or []
    if issues:
        raise ReloadStepFailed(f"materialize report has issues for {spec.name}: {issues[:2]}")
    artifacts = report.get("artifacts") if isinstance(report.get("artifacts"), dict) else {}
    if artifacts.get("selected_tables") != [spec.name]:
        raise ReloadStepFailed(f"materialize report selected_tables mismatch for {spec.name}")
    if spec.reset:
        materialize = plan.get("materialize") if isinstance(plan.get("materialize"), dict) else {}
        expected_target = f"{str(materialize.get('table_prefix') or '')}{spec.name}"
        if artifacts.get("reset_selected_tables") is not True:
            raise ReloadStepFailed(f"reset_selected_tables was not true for {spec.name}")
        if artifacts.get("reset_selected_target_tables") != [expected_target]:
            raise ReloadStepFailed(f"reset target mismatch for {spec.name}")
    if spec.name not in (artifacts.get("tables_completed_session") or []):
        raise ReloadStepFailed(f"materialize report did not complete {spec.name}")
    stats = report.get("stats") if isinstance(report.get("stats"), dict) else {}
    rows_loaded = int(stats.get("rows_loaded") or 0)
    files_loaded = int(stats.get("files_loaded") or 0)
    if files_loaded <= 0:
        raise ReloadStepFailed(f"empty materialize file stats for {spec.name}: {stats}")
    if spec.expected_rows is not None and rows_loaded != int(spec.expected_rows):
        raise ReloadStepFailed(f"materialize row mismatch for {spec.name}: rows_loaded={rows_loaded}, expected={spec.expected_rows}")
    for item in artifacts.get("per_table") or []:
        if isinstance(item, dict) and item.get("errors"):
            raise ReloadStepFailed(f"materialize errors for {spec.name}: {item.get('errors')[:2]}")
    return {
        "materialize_status": "done",
        "rows_loaded": rows_loaded,
        "files_loaded": files_loaded,
        "materialize_duration_s": report.get("duration_s"),
    }


def verify_progress(progress_path: Path, table: str) -> dict[str, Any]:
    progress = read_json(progress_path)
    if progress.get("active"):
        raise ReloadStepFailed(f"progress active is not empty for {table}: {progress.get('active')}")
    if progress.get("current") is not None:
        raise ReloadStepFailed(f"progress current is not cleared for {table}: {progress.get('current')}")
    file_counts = progress.get("table_file_counts") if isinstance(progress.get("table_file_counts"), dict) else {}
    expected_files = int(file_counts.get(table) or 0)
    completed_files = progress.get("completed_files") if isinstance(progress.get("completed_files"), dict) else {}
    completed_count = len(completed_files.get(table) or [])
    if expected_files <= 0 or completed_count != expected_files:
        raise ReloadStepFailed(f"progress mismatch for {table}: completed={completed_count}, expected={expected_files}")
    return {
        "progress_status": "done",
        "progress_files_completed": completed_count,
        "progress_rows_loaded_session": int(progress.get("rows_loaded_session") or 0),
    }


def verify_validation_report(table: str, report_path: Path) -> dict[str, Any]:
    if not report_path.exists():
        raise ReloadStepFailed(f"validation report missing: {report_path}")
    report = read_json(report_path)
    if report.get("status") != "done":
        raise ReloadStepFailed(f"validation status for {table} is {report.get('status')}: {report.get('issues')}")
    if report.get("issues"):
        raise ReloadStepFailed(f"validation issues for {table}: {report.get('issues')[:3]}")
    table_check = (((report.get("checks") or {}).get("tables") or {}).get(table) or {})
    if table_check.get("status") != "ok":
        raise ReloadStepFailed(f"validation table status for {table}: {table_check.get('status')}")
    if table_check.get("row_count_match") is not True:
        raise ReloadStepFailed(f"row mismatch for {table}: parquet={table_check.get('parquet_rows')} db={table_check.get('db_rows')}")
    marker_scan = table_check.get("literal_null_marker_scan") or {}
    if marker_scan and marker_scan.get("status") != "ok":
        raise ReloadStepFailed(f"literal marker scan failed for {table}: {marker_scan}")
    return {
        "validation_status": "done",
        "parquet_rows": int(table_check.get("parquet_rows") or 0),
        "db_rows": int(table_check.get("db_rows") or 0),
        "literal_marker_nonzero_columns": marker_scan.get("nonzero_columns") or {},
        "literal_marker_comparison": table_check.get("literal_null_marker_comparison") or {},
    }


def verify_finalizer(plan: dict[str, Any], report_path: Path) -> dict[str, Any]:
    if not report_path.exists():
        raise ReloadStepFailed(f"finalizer report missing: {report_path}")
    report = read_json(report_path)
    finalize = plan.get("finalize") if isinstance(plan.get("finalize"), dict) else {}
    strict = bool(finalize.get("strict_indexes", False))
    if report.get("status") not in {"done", "done_with_index_errors"}:
        raise ReloadStepFailed(f"finalizer status is {report.get('status')}: {report.get('error')}")
    indexes = report.get("indexes") if isinstance(report.get("indexes"), list) else []
    failed = [item for item in indexes if isinstance(item, dict) and item.get("status") in {"failed", "mismatch"}]
    if failed and strict:
        raise ReloadStepFailed(f"finalizer index failures: {failed}")
    required = {
        (str(item.get("table")), str(item.get("index_name") or item.get("name")))
        for item in ((plan.get("finalize") or {}).get("indexes") or [])
        if isinstance(item, dict)
    }
    observed = {
        (str(item.get("table")), str(item.get("index_name")))
        for item in indexes
        if isinstance(item, dict) and item.get("status") in {"created", "exists"}
    }
    missing = sorted(required - observed)
    if missing and strict:
        raise ReloadStepFailed(f"missing required index success entries: {missing}")
    return {"status": report.get("status"), "indexes": indexes, "missing_success_entries": missing}


def verify_preflight_report(report: dict[str, Any]) -> dict[str, Any]:
    if report.get("status") == "failed":
        raise ReloadStepFailed(f"target DB preflight failed: {(report.get('issues') or [])[:3]}")
    return {
        "status": report.get("status"),
        "driver": report.get("driver"),
        "driver_inferred": report.get("driver_inferred"),
        "database": report.get("database"),
        "issue_count": len(report.get("issues") or []),
        "warning_count": len(report.get("warnings") or []),
        "unsupported_features": report.get("unsupported_features") or [],
    }


def all_planned_tables_done(status: dict[str, Any], specs: list[ReloadTableSpec]) -> tuple[bool, list[str]]:
    completed = set(status.get("completed") or [])
    missing: list[str] = []
    tables = status.get("tables") if isinstance(status.get("tables"), dict) else {}
    for spec in specs:
        table_state = tables.get(spec.name) if isinstance(tables.get(spec.name), dict) else {}
        if spec.name not in completed or table_state.get("status") != "done":
            missing.append(spec.name)
    return not missing, missing


def acquire_lock(path: Path):
    path = prepare_output_file_path(path, purpose="parquet reload lock")
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags, 0o600)
    fh = os.fdopen(fd, "w", encoding="utf-8")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        fh.close()
        raise ReloadPlanError(f"another parquet reload driver holds {path}") from exc
    except Exception:
        fh.close()
        raise
    fh.write(f"pid={os.getpid()}\nstarted_at={utc_now_iso()}\n")
    fh.flush()
    return fh


def run_reload_plan(
    plan_path: Path,
    *,
    start_at: str = "",
    only_table: str = "",
    force_reload_completed: bool = False,
    skip_finalizer: bool = False,
    skip_preflight: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    plan_path = Path(plan_path).expanduser().resolve()
    plan = load_plan(plan_path)
    validate_plan(plan)
    paths = plan_paths(plan, plan_path)
    for key in ("reports_dir", "logs_dir", "staging_dir", "duckdb_temp_dir"):
        paths[key].mkdir(parents=True, exist_ok=True)
    all_specs = normalize_table_specs(plan)
    specs = selected_specs(all_specs, start_at=str(start_at).strip(), only_table=str(only_table).strip())
    lock_fh = acquire_lock(paths["lock_path"])
    status = load_status(paths["status_path"], plan=plan, plan_path=plan_path)
    completed = set(status.get("completed") or [])
    tag = str(plan.get("tag") or "parquet").strip()
    try:
        save_status(paths["status_path"], status, status="running", current={"phase": "start", "pid": os.getpid()})
        if preflight_enabled(plan, skip_preflight=bool(skip_preflight)):
            preflight_report = paths["reports_dir"] / f"target_db_preflight_{tag}.json"
            status["preflight"] = {"status": "running", "report": str(preflight_report), "started_at": utc_now_iso()}
            save_status(paths["status_path"], status, current={"phase": "preflight", "pid": os.getpid()})
            if dry_run:
                status["preflight"].update({"status": "skipped_dry_run", "finished_at": utc_now_iso()})
            else:
                from .target_db_preflight import run_target_db_preflight

                preflight_report_payload = run_target_db_preflight(
                    plan_path,
                    out_path=preflight_report,
                    table_names=[spec.name for spec in specs],
                    require_reload_supported=True,
                )
                status["preflight"].update(verify_preflight_report(preflight_report_payload))
                status["preflight"]["finished_at"] = utc_now_iso()
            save_status(paths["status_path"], status, current={"phase": "preflight_done", "pid": os.getpid()})
        else:
            status["preflight"] = {"status": "skipped", "reason": "disabled_or_cli_skip", "finished_at": utc_now_iso()}
            save_status(paths["status_path"], status)
        for spec in specs:
            table_state = dict((status.get("tables") or {}).get(spec.name) or {})
            if spec.name in completed and not force_reload_completed and table_state.get("status") == "done":
                continue
            reload_report = paths["reports_dir"] / f"reload_{spec.name}_{tag}.json"
            reload_log = paths["logs_dir"] / f"reload_{spec.name}_{tag}.log"
            validate_report = paths["reports_dir"] / f"validate_{spec.name}_{tag}.json"
            validate_log = paths["logs_dir"] / f"validate_{spec.name}_{tag}.log"
            table_state = {
                "status": "materializing",
                "expected_rows": spec.expected_rows,
                "writer": spec.writer,
                "chunk_rows": spec.chunk_rows,
                "reset": spec.reset,
                "materialize_report": str(reload_report),
                "materialize_log": str(reload_log),
                "validation_report": str(validate_report),
                "validation_log": str(validate_log),
                "started_at": utc_now_iso(),
            }
            status.setdefault("tables", {})[spec.name] = table_state
            save_status(paths["status_path"], status, current={"phase": "materialize", "table": spec.name, "pid": os.getpid()})
            rc = run_logged(materialize_cmd(plan, spec, report_path=reload_report), log_path=reload_log, dry_run=bool(dry_run))
            if rc != 0:
                raise ReloadStepFailed(f"materialize command failed for {spec.name}; see {reload_log}")
            if dry_run:
                continue
            table_state.update(verify_materialize_report(plan, spec, reload_report))
            table_state.update(verify_progress(paths["progress_path"], spec.name))
            table_state["status"] = "validating"
            save_status(paths["status_path"], status, current={"phase": "validate", "table": spec.name, "pid": os.getpid()})
            rc = run_logged(validation_cmd(plan, spec, report_path=validate_report), log_path=validate_log, dry_run=False)
            if rc != 0:
                raise ReloadStepFailed(f"validation command failed for {spec.name}; see {validate_log}")
            table_state.update(verify_validation_report(spec.name, validate_report))
            table_state["status"] = "done"
            table_state["finished_at"] = utc_now_iso()
            if spec.name not in completed:
                status.setdefault("completed", []).append(spec.name)
                completed.add(spec.name)
            save_status(paths["status_path"], status, current={"phase": "table_done", "table": spec.name, "pid": os.getpid()})

        if dry_run:
            save_status(paths["status_path"], status, status="dry_run_done", current=None)
            return status

        finalize = plan.get("finalize") if isinstance(plan.get("finalize"), dict) else {}
        if bool(finalize.get("enabled", False)) and not skip_finalizer and not only_table:
            complete, missing = all_planned_tables_done(status, all_specs)
            if not complete:
                raise ReloadStepFailed(f"finalizer blocked until all planned tables are done; missing={missing[:10]}")
            final_report = paths["reports_dir"] / f"finalize_{tag}.json"
            final_log = paths["logs_dir"] / f"finalize_{tag}.log"
            status["finalizer"] = {"status": "running", "report": str(final_report), "log": str(final_log), "started_at": utc_now_iso()}
            save_status(paths["status_path"], status, current={"phase": "finalizer", "pid": os.getpid()})
            rc = run_logged(finalizer_cmd(plan_path, plan, report_path=final_report), log_path=final_log, dry_run=False)
            if rc != 0:
                raise ReloadStepFailed(f"finalizer command failed; see {final_log}")
            status["finalizer"].update(verify_finalizer(plan, final_report))
            status["finalizer"]["finished_at"] = utc_now_iso()

        save_status(paths["status_path"], status, status="done", current=None, finished_at=utc_now_iso())
        return status
    except Exception as exc:
        save_status(
            paths["status_path"],
            status,
            status="failed",
            current=None,
            failed_at=utc_now_iso(),
            error_type=type(exc).__name__,
            error=str(exc),
        )
        raise
    finally:
        try:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
        finally:
            lock_fh.close()


def mark_table_done_from_validation_report(*, status_path: Path, table: str, validation_report: Path) -> dict[str, Any]:
    status_path = Path(status_path).expanduser()
    if not status_path.is_absolute():
        status_path = Path.cwd() / status_path
    table = str(table).strip()
    if not table:
        raise ReloadPlanError("table is required")
    status = read_json(status_path)
    result = verify_validation_report(table, Path(validation_report).expanduser().resolve())
    state = dict((status.get("tables") or {}).get(table) or {})
    state.update(result)
    state["status"] = "done"
    state["finished_at"] = utc_now_iso()
    status.setdefault("tables", {})[table] = state
    completed = list(status.get("completed") or [])
    if table not in completed:
        completed.append(table)
    status["completed"] = completed
    status["status"] = "running"
    status["current"] = {"phase": "table_done", "table": table, "pid": os.getpid()}
    for key in ("failed_at", "error_type", "error", "finished_at"):
        status.pop(key, None)
    write_json(status_path, status)
    return status
