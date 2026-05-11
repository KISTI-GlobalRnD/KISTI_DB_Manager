from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


DEFAULT_WORKERS = (0, 2, 4, 8)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_out_dir() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    return Path("runs") / f"profile_parallel_{stamp}"


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def parse_worker_list(value: str | Sequence[int] | None) -> list[int]:
    if value is None:
        return list(DEFAULT_WORKERS)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return list(DEFAULT_WORKERS)
        raw_items: list[Any] = text.split(",")
    else:
        raw_items = list(value)

    workers: list[int] = []
    seen: set[int] = set()
    for raw in raw_items:
        item = str(raw).strip()
        if not item:
            raise ValueError("workers must be a comma-separated list of non-negative integers")
        try:
            worker = int(item)
        except Exception as exc:
            raise ValueError(f"invalid worker value: {item!r}") from exc
        if worker < 0:
            raise ValueError(f"worker values must be non-negative: {worker}")
        if worker in seen:
            continue
        seen.add(worker)
        workers.append(worker)

    if not workers:
        raise ValueError("at least one worker value is required")
    return workers


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _issue_counts(run_report: Mapping[str, Any]) -> dict[str, int]:
    issues = list(run_report.get("issues") or [])
    error_count = 0
    warning_count = 0
    for issue in issues:
        if not isinstance(issue, Mapping):
            error_count += 1
            continue
        level = str(issue.get("level") or "error").strip().lower()
        if level == "warning":
            warning_count += 1
        else:
            error_count += 1
    return {
        "issue_count": len(issues),
        "error_count": int(error_count),
        "warning_count": int(warning_count),
    }


def _duration_s(run_report: Mapping[str, Any]) -> float | None:
    duration = _as_float(run_report.get("duration_s"), 0.0)
    if duration > 0:
        return duration
    timings = run_report.get("timings_ms") if isinstance(run_report.get("timings_ms"), Mapping) else {}
    total_ms = _as_int(timings.get("pipeline.json.total"), 0)
    if total_ms <= 0:
        total_ms = sum(_as_int(v, 0) for v in timings.values())
    if total_ms > 0:
        return float(total_ms) / 1000.0
    return None


def _records_for_rate(run_report: Mapping[str, Any]) -> int:
    stats = run_report.get("stats") if isinstance(run_report.get("stats"), Mapping) else {}
    records = _as_int(stats.get("records_read"), 0)
    if records <= 0:
        records = _as_int(stats.get("records_ok"), 0)
    return max(0, int(records))


def _records_per_s(run_report: Mapping[str, Any]) -> float | None:
    duration = _duration_s(run_report)
    if duration is None or duration <= 0:
        return None
    return float(_records_for_rate(run_report)) / float(duration)


def _run_report_profile(run_report: Mapping[str, Any], *, top: int = 8) -> dict[str, Any]:
    try:
        from .cli import _build_run_report_profile

        return _build_run_report_profile(dict(run_report), top=top)
    except Exception:
        return {}


def _artifact_contract_failed(contract: Mapping[str, Any]) -> bool:
    return str(contract.get("status") or "").strip().lower() == "failed"


def _row_status(*, run_failed: bool, counts: Mapping[str, int], contract: Mapping[str, Any]) -> str:
    if run_failed or int(counts.get("error_count") or 0) > 0 or _artifact_contract_failed(contract):
        return "failed"
    if int(counts.get("warning_count") or 0) > 0 or str(contract.get("status") or "") == "done_with_warnings":
        return "done_with_warnings"
    return "done"


def _summarize_worker_run(
    *,
    worker: int,
    run_dir: Path,
    parquet_dir: Path,
    report_path: Path,
    quarantine_path: Path,
    artifact_contract_path: Path,
    run_report: Mapping[str, Any],
    artifact_contract: Mapping[str, Any],
    run_failed: bool,
    cleanup_requested: bool,
    profile_top: int,
) -> dict[str, Any]:
    stats = run_report.get("stats") if isinstance(run_report.get("stats"), Mapping) else {}
    timings = run_report.get("timings_ms") if isinstance(run_report.get("timings_ms"), Mapping) else {}
    counts = _issue_counts(run_report)
    row = {
        "workers": int(worker),
        "status": _row_status(run_failed=run_failed, counts=counts, contract=artifact_contract),
        "run_dir": str(run_dir),
        "parquet_dir": str(parquet_dir),
        "report_path": str(report_path),
        "quarantine_path": str(quarantine_path),
        "artifact_contract_path": str(artifact_contract_path),
        "duration_s": _duration_s(run_report),
        "records_per_s": _records_per_s(run_report),
        "records_read": _as_int(stats.get("records_read"), 0),
        "records_ok": _as_int(stats.get("records_ok"), 0),
        "parquet_files_persisted": _as_int(stats.get("parquet_files_persisted"), 0),
        "parquet_rows_emitted": _as_int(stats.get("parquet_rows_emitted"), 0),
        "timings_ms": {
            "io.json_parse": _as_int(timings.get("io.json_parse"), 0),
            "json.flatten": _as_int(timings.get("json.flatten"), 0),
            "json.parquet.persist": _as_int(timings.get("json.parquet.persist"), 0),
        },
        "issue_count": int(counts["issue_count"]),
        "error_count": int(counts["error_count"]),
        "warning_count": int(counts["warning_count"]),
        "artifact_contract_status": artifact_contract.get("status"),
        "artifact_contract_issue_count": len(artifact_contract.get("issues") or []),
        "artifact_contract_warning_count": len(artifact_contract.get("warnings") or []),
        "run_report_profile": _run_report_profile(run_report, top=profile_top),
        "parquet_cleaned": bool(cleanup_requested),
    }
    return row


def recommend_parallel_workers(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("status") or "") == "failed":
            continue
        if _as_int(row.get("error_count"), 0) > 0:
            continue
        if str(row.get("artifact_contract_status") or "") == "failed":
            continue
        rps = row.get("records_per_s")
        try:
            rps_f = float(rps)
        except Exception:
            continue
        if rps_f < 0:
            continue
        candidates.append({"workers": _as_int(row.get("workers"), 0), "records_per_s": rps_f})

    if not candidates:
        return {
            "status": "failed",
            "recommended_parallel_workers": None,
            "recommendation_reason": "No successful worker run was eligible for recommendation.",
            "eligible_workers": [],
        }

    candidates.sort(key=lambda item: (float(item["records_per_s"]), -int(item["workers"])), reverse=True)
    by_worker = {int(item["workers"]): item for item in candidates}
    baseline = by_worker.get(0)
    parallel_candidates = [item for item in candidates if int(item["workers"]) > 0]
    if baseline is not None and parallel_candidates:
        base_rate = float(baseline["records_per_s"])
        if all(float(item["records_per_s"]) <= base_rate for item in parallel_candidates):
            return {
                "status": "done",
                "recommended_parallel_workers": 0,
                "recommendation_reason": (
                    "All successful parallel worker settings were slower than workers=0; "
                    "parallel flatten is not recommended for this data/config sample."
                ),
                "eligible_workers": [int(item["workers"]) for item in sorted(candidates, key=lambda x: int(x["workers"]))],
            }

    best = max(candidates, key=lambda item: (float(item["records_per_s"]), -int(item["workers"])))
    best_rate = float(best["records_per_s"])
    threshold = best_rate * 0.95
    within_tolerance = [item for item in candidates if float(item["records_per_s"]) >= threshold]
    chosen = min(within_tolerance, key=lambda item: int(item["workers"]))
    chosen_worker = int(chosen["workers"])
    best_worker = int(best["workers"])
    if chosen_worker != best_worker:
        reason = (
            f"workers={chosen_worker} is within 5% of the fastest throughput "
            f"({best_rate:.3f} records/s at workers={best_worker}) and has lower orchestration overhead."
        )
    else:
        reason = f"workers={best_worker} had the highest eligible throughput ({best_rate:.3f} records/s)."
    return {
        "status": "done",
        "recommended_parallel_workers": chosen_worker,
        "recommendation_reason": reason,
        "eligible_workers": [int(item["workers"]) for item in sorted(candidates, key=lambda x: int(x["workers"]))],
    }


def _render_parallel_profile_markdown(summary: Mapping[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# JSON Parallel Profile")
    lines.append("")
    lines.append(f"- status: `{summary.get('status')}`")
    lines.append(f"- config: `{summary.get('config')}`")
    lines.append(f"- mode: `{summary.get('mode')}`")
    lines.append(f"- max_records: `{summary.get('max_records')}`")
    lines.append(f"- recommended_parallel_workers: `{summary.get('recommended_parallel_workers')}`")
    lines.append(f"- recommendation_reason: {summary.get('recommendation_reason')}")
    lines.append("")
    lines.append("## Worker Runs")
    lines.append("")
    lines.append(
        "| workers | status | duration_s | records_per_s | io.json_parse_ms | json.flatten_ms | "
        "json.parquet.persist_ms | issues | errors | warnings | artifact_contract |"
    )
    lines.append("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|")
    for row in summary.get("runs") or []:
        timings = row.get("timings_ms") if isinstance(row.get("timings_ms"), Mapping) else {}
        duration = row.get("duration_s")
        rps = row.get("records_per_s")
        duration_s = "" if duration is None else f"{float(duration):.3f}"
        rps_s = "" if rps is None else f"{float(rps):.3f}"
        lines.append(
            "| "
            f"{row.get('workers')} | {row.get('status')} | {duration_s} | {rps_s} | "
            f"{_as_int(timings.get('io.json_parse'), 0)} | "
            f"{_as_int(timings.get('json.flatten'), 0)} | "
            f"{_as_int(timings.get('json.parquet.persist'), 0)} | "
            f"{_as_int(row.get('issue_count'), 0)} | "
            f"{_as_int(row.get('error_count'), 0)} | "
            f"{_as_int(row.get('warning_count'), 0)} | "
            f"{row.get('artifact_contract_status')} |"
        )
    lines.append("")
    lines.append("## Artifacts")
    lines.append("")
    lines.append(f"- summary_json: `{summary.get('summary_json_path')}`")
    lines.append(f"- summary_md: `{summary.get('summary_md_path')}`")
    lines.append("")
    return "\n".join(lines)


def _load_profile_config(config_path: Path, *, mode: str) -> tuple[dict[str, Any], dict[str, Any], str]:
    from .config import coerce_data_config, coerce_db_config
    from .modes import apply_mode

    cfg = _read_json(config_path)
    data_config = coerce_data_config(cfg.get("data_config") or cfg.get("data") or {}, inplace=False)
    db_config = coerce_db_config(cfg.get("db_config") or cfg.get("db") or {}, inplace=False)
    mode_spec = apply_mode(mode, data_config)
    return data_config, db_config, mode_spec.name


def _ensure_id_compaction_config(data_config: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(data_config.get("id_compaction"), dict):
        data_config["id_compaction"] = {}
    return data_config["id_compaction"]


def _apply_id_compaction_overrides(
    data_config: dict[str, Any],
    *,
    id_compaction: bool | None,
    id_compaction_preset: str | None,
    id_compaction_mode: str | None,
    id_compaction_collision_policy: str | None,
    id_compaction_namespace_conflict_policy: str | None,
) -> bool:
    from .id_compaction import normalize_id_compaction_config, validate_id_compaction_config

    nested = _ensure_id_compaction_config(data_config)
    if id_compaction is not None:
        nested["enabled"] = bool(id_compaction)
    if id_compaction_preset:
        nested["preset"] = str(id_compaction_preset)
    if id_compaction_mode:
        nested["mode"] = str(id_compaction_mode)
    if id_compaction_collision_policy:
        nested["collision_policy"] = str(id_compaction_collision_policy)
    if id_compaction_namespace_conflict_policy:
        nested["namespace_conflict_policy"] = str(id_compaction_namespace_conflict_policy)

    normalized = normalize_id_compaction_config(data_config)
    validate_id_compaction_config(normalized)
    data_config["id_compaction"] = normalized
    return bool(normalized.get("enabled"))


def _failed_contract(parquet_dir: Path, *, require_schema_manifest: bool, require_id_compaction: bool, exc: BaseException) -> dict[str, Any]:
    return {
        "status": "failed",
        "generated_at": _utc_now_iso(),
        "parquet_root": str(parquet_dir.expanduser().resolve()),
        "input": {
            "require_schema_manifest": bool(require_schema_manifest),
            "require_id_compaction": bool(require_id_compaction),
        },
        "schema_manifest": {},
        "summary": {},
        "tables": {},
        "issues": [
            {
                "severity": "error",
                "check": "artifact_contract_exception",
                "message": str(exc),
                "error_type": type(exc).__name__,
            }
        ],
        "warnings": [],
        "finished_at": _utc_now_iso(),
    }


def profile_parallel(
    *,
    config_path: str | Path,
    workers: str | Sequence[int] | None = None,
    out_dir: str | Path | None = None,
    max_records: int | None = 20000,
    chunk_size: int | None = None,
    mode: str = "parse-parquet-safe",
    keep_artifacts: bool = True,
    cleanup_parquet: bool = False,
    index_key: str | None = None,
    except_keys: Sequence[str] | None = None,
    id_compaction: bool | None = None,
    id_compaction_preset: str | None = None,
    id_compaction_mode: str | None = None,
    id_compaction_collision_policy: str | None = None,
    id_compaction_namespace_conflict_policy: str | None = None,
    profile_top: int = 8,
) -> dict[str, Any]:
    from . import pipeline
    from .parquet_artifacts import inspect_parquet_artifact_contract
    from .quarantine import QuarantineWriter
    from .report import RunReport

    config = Path(config_path).expanduser().resolve()
    worker_values = parse_worker_list(workers)
    out = Path(out_dir).expanduser().resolve() if out_dir else _default_out_dir().resolve()
    out.mkdir(parents=True, exist_ok=True)

    cleanup_requested = bool(cleanup_parquet) or not bool(keep_artifacts)
    max_records_arg = None
    if max_records is not None:
        max_records_int = int(max_records)
        max_records_arg = max_records_int if max_records_int > 0 else None

    rows: list[dict[str, Any]] = []
    for worker in worker_values:
        run_dir = out / f"w{int(worker)}"
        parquet_dir = run_dir / "parquet"
        report_path = run_dir / "run_report.json"
        quarantine_path = run_dir / "quarantine.jsonl"
        artifact_contract_path = run_dir / "artifact_contract.json"
        run_dir.mkdir(parents=True, exist_ok=True)

        data_config, db_config, mode_name = _load_profile_config(config, mode=mode)
        data_config["json_streaming_load"] = False
        data_config["persist_parquet_files"] = True
        data_config["persist_tsv_files"] = False
        data_config["persist_parquet_dir"] = str(parquet_dir)
        data_config["parallel_workers"] = int(worker)
        data_config["progress_path"] = str(report_path) + ".progress.json"
        data_config["progress_interval_s"] = 10.0
        if chunk_size is not None:
            data_config["chunk_size"] = int(chunk_size)
        effective_chunk_size = _as_int(data_config.get("chunk_size"), 0) or None
        effective_id_compaction = _apply_id_compaction_overrides(
            data_config,
            id_compaction=id_compaction,
            id_compaction_preset=id_compaction_preset,
            id_compaction_mode=id_compaction_mode,
            id_compaction_collision_policy=id_compaction_collision_policy,
            id_compaction_namespace_conflict_policy=id_compaction_namespace_conflict_policy,
        )

        report = RunReport()
        report.set_artifact("mode", mode_name)
        report.set_artifact("json_execution_path", "parquet-first")
        report.set_artifact(
            "parallel_profile",
            {
                "workers": int(worker),
                "out_dir": str(out),
                "run_dir": str(run_dir),
                "max_records": max_records_arg,
            },
        )
        run_failed = False
        try:
            result = pipeline.run_json_pipeline(
                data_config,
                db_config,
                index_key=index_key,
                except_keys=list(except_keys or []) or None,
                chunk_size=effective_chunk_size,
                max_records=max_records_arg,
                create=False,
                load=False,
                index=False,
                optimize=False,
                continue_on_error=True,
                report=report,
                quarantine=QuarantineWriter(quarantine_path),
            )
            run_report_obj = result.report
        except Exception as exc:
            run_failed = True
            report.exception(
                stage="json.profile_parallel",
                message="Parallel profile worker run failed",
                exc=exc,
                workers=int(worker),
            )
            run_report_obj = report

        run_report_obj.finish()
        run_report_obj.save_json(str(report_path))
        run_report = run_report_obj.to_dict()

        require_schema_manifest = bool(effective_id_compaction)
        require_id_compaction = bool(effective_id_compaction)
        try:
            artifact_contract = inspect_parquet_artifact_contract(
                parquet_dir,
                require_schema_manifest=require_schema_manifest,
                require_id_compaction=require_id_compaction,
            )
        except Exception as exc:
            artifact_contract = _failed_contract(
                parquet_dir,
                require_schema_manifest=require_schema_manifest,
                require_id_compaction=require_id_compaction,
                exc=exc,
            )
        _write_json(artifact_contract_path, artifact_contract)

        row = _summarize_worker_run(
            worker=int(worker),
            run_dir=run_dir,
            parquet_dir=parquet_dir,
            report_path=report_path,
            quarantine_path=quarantine_path,
            artifact_contract_path=artifact_contract_path,
            run_report=run_report,
            artifact_contract=artifact_contract,
            run_failed=run_failed,
            cleanup_requested=cleanup_requested,
            profile_top=profile_top,
        )
        rows.append(row)

        if cleanup_requested and parquet_dir.exists():
            shutil.rmtree(parquet_dir, ignore_errors=True)

    recommendation = recommend_parallel_workers(rows)
    status = str(recommendation.get("status") or "failed")
    if status != "failed" and any(str(row.get("status") or "") != "done" for row in rows):
        status = "done_with_warnings"

    summary_json_path = out / "parallel_profile.json"
    summary_md_path = out / "parallel_profile.md"
    summary: dict[str, Any] = {
        "status": status,
        "generated_at": _utc_now_iso(),
        "config": str(config),
        "out_dir": str(out),
        "mode": str(mode),
        "workers": worker_values,
        "max_records": max_records_arg,
        "chunk_size": chunk_size,
        "keep_artifacts": bool(keep_artifacts),
        "cleanup_parquet": bool(cleanup_requested),
        "runs": rows,
        "recommended_parallel_workers": recommendation.get("recommended_parallel_workers"),
        "recommendation_reason": recommendation.get("recommendation_reason"),
        "eligible_workers": recommendation.get("eligible_workers") or [],
        "summary_json_path": str(summary_json_path),
        "summary_md_path": str(summary_md_path),
    }
    _write_json(summary_json_path, summary)
    summary_md_path.write_text(_render_parallel_profile_markdown(summary) + "\n", encoding="utf-8")
    return summary
