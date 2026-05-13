from __future__ import annotations

import json
import os
import random
import shutil
import statistics
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence


DEFAULT_WORKERS = (0, 2, 4, 8)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_out_dir() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%fZ")
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


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, (int, float)):
        try:
            return int(value) != 0
        except Exception:
            return bool(default)
    text = str(value).strip().lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off", ""}:
        return False
    return bool(default)


def _median(values: Sequence[float]) -> float | None:
    vals = [float(v) for v in values]
    if not vals:
        return None
    return float(statistics.median(vals))


def _mean(values: Sequence[float]) -> float | None:
    vals = [float(v) for v in values]
    if not vals:
        return None
    return float(statistics.fmean(vals))


def _stdev(values: Sequence[float]) -> float | None:
    vals = [float(v) for v in values]
    if len(vals) < 2:
        return None
    return float(statistics.stdev(vals))


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


def _backend_sort_rank(backend: str) -> int:
    backend_s = str(backend or "python")
    if backend_s == "python":
        return 0
    if backend_s == "auto":
        return 1
    if backend_s == "rust-arrow":
        return 2
    return 9


def _row_backend(row: Mapping[str, Any]) -> str:
    value = row.get("flatten_backend")
    if value is None:
        value = row.get("requested_flatten_backend")
    if value is None:
        value = row.get("effective_backend")
    return str(value or "python")


def _recommendation_backend(row: Mapping[str, Any]) -> str:
    requested = _row_backend(row)
    effective = str(row.get("effective_backend") or requested)
    if requested == "auto" and effective in {"python", "rust-arrow"}:
        return effective
    return requested


def _eligible_workers(candidates: Sequence[Mapping[str, Any]]) -> list[int]:
    return sorted({int(item.get("workers", 0) or 0) for item in candidates})


def _dedupe_recommendation_candidates(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    best_by_key: dict[tuple[str, int], dict[str, Any]] = {}
    for item in candidates:
        backend = str(item.get("flatten_backend") or "python")
        worker = int(item.get("workers", 0) or 0)
        current = dict(item)
        key = (backend, worker)
        previous = best_by_key.get(key)
        if previous is None:
            best_by_key[key] = current
            continue
        prev_rate = float(previous.get("records_per_s") or 0.0)
        cur_rate = float(current.get("records_per_s") or 0.0)
        if cur_rate > prev_rate:
            best_by_key[key] = current
    return list(best_by_key.values())


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _validate_profile_file_path(root: Path, path: Path, *, purpose: str) -> None:
    _assert_profile_child_path(root, path, purpose=purpose)
    try:
        path.lstat()
    except FileNotFoundError:
        return
    if path.is_symlink() or path.is_dir():
        raise RuntimeError(f"{purpose} path already exists and is not a safe file: {path}")


def _safe_write_text(root: Path, path: Path, text: str, *, purpose: str) -> None:
    parent = path.parent
    _assert_profile_child_path(root, parent, purpose=f"{purpose} parent")
    parent.mkdir(parents=True, exist_ok=True)
    _assert_profile_child_path(root, parent, purpose=f"{purpose} parent")
    if parent.is_symlink() or not parent.is_dir():
        raise RuntimeError(f"{purpose} parent is not a safe directory: {parent}")
    _validate_profile_file_path(root, path, purpose=purpose)

    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        _validate_profile_file_path(root, path, purpose=purpose)
        os.replace(tmp_name, path)
    except Exception:
        try:
            tmp_path = Path(tmp_name)
            _assert_profile_child_path(root, tmp_path, purpose=f"{purpose} temporary file")
            if not tmp_path.is_symlink() and tmp_path.is_file():
                tmp_path.unlink()
        except Exception:
            pass
        raise


def _safe_write_json(root: Path, path: Path, payload: Mapping[str, Any], *, purpose: str) -> None:
    _safe_write_text(
        root,
        path,
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        purpose=purpose,
    )


def _safe_json_value(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except TypeError:
        return repr(value)


@dataclass
class _ProfileQuarantineWriter:
    root: Path
    path: Path
    flush: bool = True
    _lines: list[str] = field(default_factory=list, init=False)

    def __enter__(self) -> "_ProfileQuarantineWriter":
        _validate_profile_file_path(self.root, self.path, purpose="profile quarantine")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        if not self._lines:
            return
        _safe_write_text(
            self.root,
            self.path,
            "".join(self._lines),
            purpose="profile quarantine",
        )
        self._lines = []

    def write(
        self,
        *,
        stage: str,
        record: Any,
        index: int | None = None,
        exc: BaseException | None = None,
        **context: Any,
    ) -> None:
        entry: dict[str, Any] = {
            "timestamp": _utc_now_iso(),
            "stage": stage,
            "index": index,
            "record": _safe_json_value(record),
            "context": {k: _safe_json_value(v) for k, v in context.items()},
        }
        if exc is not None:
            entry["exception_type"] = type(exc).__name__
            entry["exception_message"] = str(exc)
        self._lines.append(json.dumps(entry, ensure_ascii=False) + "\n")


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _absolute_no_resolve(path: Path) -> Path:
    expanded = path.expanduser()
    if expanded.is_absolute():
        return expanded
    return Path.cwd() / expanded


def _assert_no_symlink_components(path: Path, *, purpose: str) -> Path:
    path_abs = _absolute_no_resolve(path)
    current = Path(path_abs.anchor)
    normalized_parts: list[str] = []
    parts = path_abs.parts[1:] if path_abs.anchor else path_abs.parts
    for part in parts:
        if part in {"", "."}:
            continue
        if part == "..":
            if normalized_parts:
                normalized_parts.pop()
            current = Path(path_abs.anchor).joinpath(*normalized_parts)
            continue
        current = current / part
        try:
            is_link = current.is_symlink()
        except OSError as exc:
            raise RuntimeError(f"failed to inspect {purpose} path component: {current}") from exc
        if is_link:
            raise RuntimeError(f"{purpose} path contains a symlink component: {current}")
        normalized_parts.append(part)
    return Path(path_abs.anchor).joinpath(*normalized_parts)


def _prepare_profile_output_dir(out_dir: str | Path | None) -> Path:
    out_raw = Path(out_dir).expanduser() if out_dir else _default_out_dir()
    out_abs = _assert_no_symlink_components(out_raw, purpose="profile output")
    if out_abs.exists():
        if out_abs.is_symlink() or not out_abs.is_dir():
            raise RuntimeError(f"profile output path already exists and is not a directory: {out_abs}")
    out_abs.mkdir(parents=True, exist_ok=True)
    out_abs = _assert_no_symlink_components(out_abs, purpose="profile output")
    if out_abs.is_symlink() or not out_abs.is_dir():
        raise RuntimeError(f"profile output path is not a safe directory: {out_abs}")
    return out_abs.resolve(strict=True)


def _assert_profile_child_path(root: Path, path: Path, *, purpose: str) -> None:
    root_raw = _assert_no_symlink_components(root, purpose="profile output")
    root_abs = root_raw.resolve(strict=False)
    path_abs = _assert_no_symlink_components(path, purpose=purpose)
    if not _is_relative_to(path_abs, root_abs):
        raise RuntimeError(f"{purpose} path escapes the profile output directory: {path}")

    resolved = path_abs.resolve(strict=False)
    if not _is_relative_to(resolved, root_abs):
        raise RuntimeError(f"{purpose} path resolves outside the profile output directory: {path}")


def _validate_profile_run_dir(root: Path, run_dir: Path) -> None:
    _assert_profile_child_path(root, run_dir, purpose="profile run")
    if run_dir.exists():
        if not run_dir.is_dir():
            raise RuntimeError(f"profile run path already exists and is not a directory: {run_dir}")
        try:
            non_empty = any(run_dir.iterdir())
        except OSError as exc:
            raise RuntimeError(f"failed to inspect existing profile run directory: {run_dir}") from exc
        if non_empty:
            raise RuntimeError(
                "profile run directory already exists and is not empty; choose a fresh --out directory "
                f"or remove the stale run directory: {run_dir}"
            )


def _prepare_profile_run_dir(root: Path, run_dir: Path) -> None:
    _validate_profile_run_dir(root, run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    _assert_profile_child_path(root, run_dir, purpose="profile run")


def _prepare_profile_run_dirs(root: Path, run_dirs: Sequence[Path]) -> None:
    unique: list[Path] = []
    seen: set[str] = set()
    for run_dir in run_dirs:
        key = str(run_dir.expanduser().absolute())
        if key in seen:
            continue
        seen.add(key)
        unique.append(run_dir)
    for run_dir in unique:
        _validate_profile_run_dir(root, run_dir)
    for run_dir in unique:
        _prepare_profile_run_dir(root, run_dir)


def _safe_remove_profile_parquet_dir(root: Path, parquet_dir: Path) -> bool:
    _assert_profile_child_path(root, parquet_dir, purpose="profile parquet cleanup")
    if not parquet_dir.exists():
        return False
    if parquet_dir.is_symlink() or not parquet_dir.is_dir():
        raise RuntimeError(f"refusing to delete non-directory or symlink parquet path: {parquet_dir}")
    if not getattr(shutil.rmtree, "avoids_symlink_attacks", False):
        raise RuntimeError("refusing to delete parquet path because shutil.rmtree is not symlink-attack resistant")
    shutil.rmtree(parquet_dir)
    return True


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


def _sample_run_report_issues(run_report: Mapping[str, Any], *, limit: int) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    if limit <= 0:
        return samples
    for issue in list(run_report.get("issues") or []):
        if not isinstance(issue, Mapping):
            continue
        samples.append(
            {
                "source": "run_report",
                "level": issue.get("level"),
                "stage": issue.get("stage"),
                "message": issue.get("message"),
                "exception_type": issue.get("exception_type"),
                "exception_message": issue.get("exception_message"),
            }
        )
        if len(samples) >= limit:
            break
    return samples


def _sample_artifact_contract_issues(contract: Mapping[str, Any], *, limit: int) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    if limit <= 0:
        return samples
    for kind in ("issues", "warnings"):
        for issue in list(contract.get(kind) or []):
            if not isinstance(issue, Mapping):
                continue
            samples.append(
                {
                    "source": f"artifact_contract.{kind}",
                    "level": issue.get("severity") or ("warning" if kind == "warnings" else "error"),
                    "check": issue.get("check"),
                    "message": issue.get("message"),
                    "table": issue.get("table"),
                }
            )
            if len(samples) >= limit:
                return samples
    return samples


def _artifact_contract_failed(contract: Mapping[str, Any]) -> bool:
    return str(contract.get("status") or "").strip().lower() == "failed"


def _row_status(*, run_failed: bool, counts: Mapping[str, int], contract: Mapping[str, Any]) -> str:
    if run_failed or int(counts.get("error_count") or 0) > 0 or _artifact_contract_failed(contract):
        return "failed"
    if int(counts.get("warning_count") or 0) > 0 or str(contract.get("status") or "") == "done_with_warnings":
        return "done_with_warnings"
    return "done"


def _execution_status(rows: Sequence[Mapping[str, Any]]) -> str:
    statuses = [str(row.get("status") or "") for row in rows]
    if not statuses or all(status == "failed" or not status for status in statuses):
        return "failed"
    if any(status == "failed" or status != "done" for status in statuses):
        return "done_with_warnings"
    return "done"


def _summarize_worker_run(
    *,
    worker: int,
    flatten_backend: str,
    repeat_index: int,
    execution_order: int,
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
    issue_sample_limit: int,
) -> dict[str, Any]:
    stats = run_report.get("stats") if isinstance(run_report.get("stats"), Mapping) else {}
    timings = run_report.get("timings_ms") if isinstance(run_report.get("timings_ms"), Mapping) else {}
    artifacts = run_report.get("artifacts") if isinstance(run_report.get("artifacts"), Mapping) else {}
    counts = _issue_counts(run_report)
    row = {
        "workers": int(worker),
        "flatten_backend": str(flatten_backend),
        "effective_backend": str(artifacts.get("flatten_backend_effective") or artifacts.get("flatten_backend") or flatten_backend),
        "repeat_index": int(repeat_index),
        "execution_order": int(execution_order),
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
        "parquet_batches_total": _as_int(stats.get("parquet_batches_total"), 0),
        "timings_ms": {
            "io.json_parse": _as_int(timings.get("io.json_parse"), 0),
            "rust_arrow.json_parse": _as_int(timings.get("rust_arrow.json_parse"), 0),
            "rust_arrow.py_to_json": _as_int(timings.get("rust_arrow.py_to_json"), 0),
            "json.flatten": _as_int(timings.get("json.flatten"), 0),
            "json.parquet.persist": _as_int(timings.get("json.parquet.persist"), 0),
            "rust_arrow.total": _as_int(timings.get("rust_arrow.total"), 0),
        },
        "issue_count": int(counts["issue_count"]),
        "error_count": int(counts["error_count"]),
        "warning_count": int(counts["warning_count"]),
        "artifact_contract_status": artifact_contract.get("status"),
        "artifact_contract_issue_count": len(artifact_contract.get("issues") or []),
        "artifact_contract_warning_count": len(artifact_contract.get("warnings") or []),
        "flatten_backend_fallback_reason": artifacts.get("flatten_backend_fallback_reason"),
        "flatten_backend_auto_disabled_reason": artifacts.get("flatten_backend_auto_disabled_reason"),
        "python_fallback_active": bool(artifacts.get("python_fallback_active", False)),
        "rust_raw_jsonl_parse_requested": bool(artifacts.get("rust_raw_jsonl_parse_requested", False)),
        "rust_raw_jsonl_parse_effective": bool(artifacts.get("rust_raw_jsonl_parse_effective", False)),
        "rust_raw_jsonl_parse_disabled_reason": artifacts.get("rust_raw_jsonl_parse_disabled_reason"),
        "rust_raw_jsonl_file_parse_requested": bool(artifacts.get("rust_raw_jsonl_file_parse_requested", False)),
        "rust_raw_jsonl_file_parse_effective": bool(artifacts.get("rust_raw_jsonl_file_parse_effective", False)),
        "rust_raw_jsonl_file_parse_disabled_reason": artifacts.get("rust_raw_jsonl_file_parse_disabled_reason"),
        "rust_parallel_table_writes": bool(artifacts.get("rust_parallel_table_writes", False)),
        "rust_columnar_accumulator": bool(artifacts.get("rust_columnar_accumulator", False)),
        "rust_parquet_flush_records": _as_int(artifacts.get("rust_parquet_flush_records"), 0),
        "rust_arrow_failed_batches": _as_int(
            artifacts.get("rust_arrow_failed_batches"),
            _as_int(artifacts.get("flatten_backend_fallback_batches"), 0),
        ),
        "flatten_backend_fallback_batches": _as_int(artifacts.get("flatten_backend_fallback_batches"), 0),
        "issue_samples": (
            _sample_run_report_issues(run_report, limit=issue_sample_limit)
            + _sample_artifact_contract_issues(artifact_contract, limit=issue_sample_limit)
        )[: max(0, int(issue_sample_limit))],
        "run_report_profile": _run_report_profile(run_report, top=profile_top),
        "parquet_cleaned": False,
        "cleanup_error": None,
    }
    if _as_int(row.get("flatten_backend_fallback_batches"), 0) <= 0:
        row["flatten_backend_fallback_batches"] = _as_int(row.get("rust_arrow_failed_batches"), 0)
    return row


def _eligible_for_recommendation(row: Mapping[str, Any]) -> bool:
    if str(row.get("status") or "") == "failed":
        return False
    if _as_int(row.get("error_count"), 0) > 0:
        return False
    if str(row.get("artifact_contract_status") or "") == "failed":
        return False
    if str(row.get("effective_backend") or "") == "mixed":
        return False
    if _as_int(row.get("rust_arrow_failed_batches"), _as_int(row.get("flatten_backend_fallback_batches"), 0)) > 0:
        return False
    if "eligible_attempt_count" in row and _as_int(row.get("eligible_attempt_count"), 0) <= 0:
        return False
    if _as_int(row.get("failed_attempt_count"), 0) > 0:
        return False
    return True


def _aggregate_artifact_contract_status(attempts: Sequence[Mapping[str, Any]]) -> str:
    statuses = [str(item.get("artifact_contract_status") or "") for item in attempts]
    statuses = [status for status in statuses if status]
    if not statuses:
        return "failed"
    if any(status == "failed" for status in statuses):
        return "failed"
    if any(status == "done_with_warnings" for status in statuses):
        return "done_with_warnings"
    unique = sorted(set(statuses))
    if len(unique) == 1:
        return unique[0]
    return "mixed"


def recommend_parallel_workers(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for row in rows:
        if not _eligible_for_recommendation(row):
            continue
        rps = row.get("records_per_s")
        try:
            rps_f = float(rps)
        except Exception:
            continue
        if rps_f < 0:
            continue
        backend = _recommendation_backend(row)
        candidates.append(
            {
                "flatten_backend": backend,
                "effective_backend": str(row.get("effective_backend") or backend),
                "workers": _as_int(row.get("workers"), 0),
                "records_per_s": rps_f,
                "basis": row.get("records_per_s_basis") or "single_run",
            }
        )

    if not candidates:
        return {
            "status": "failed",
            "recommended_flatten_backend": None,
            "recommended_parallel_workers": None,
            "recommendation_reason": "No successful worker run was eligible for recommendation.",
            "eligible_workers": [],
            "eligible_configurations": [],
        }

    candidates = _dedupe_recommendation_candidates(candidates)
    candidates.sort(
        key=lambda item: (
            float(item["records_per_s"]),
            -_backend_sort_rank(str(item.get("effective_backend") or item.get("flatten_backend") or "")),
            -int(item["workers"]),
        ),
        reverse=True,
    )
    by_worker = {int(item["workers"]): item for item in candidates}
    baseline = by_worker.get(0)
    parallel_candidates = [item for item in candidates if int(item["workers"]) > 0]
    backend_count = len({str(item.get("flatten_backend") or "") for item in candidates})
    if backend_count == 1 and baseline is not None and parallel_candidates:
        base_rate = float(baseline["records_per_s"])
        if all(float(item["records_per_s"]) <= base_rate for item in parallel_candidates):
            backend = str(baseline.get("flatten_backend") or "python")
            return {
                "status": "done",
                "recommended_flatten_backend": backend,
                "recommended_parallel_workers": 0,
                "recommendation_reason": (
                    "All successful parallel worker settings were slower than workers=0; "
                    "parallel flatten is not recommended for this data/config sample."
                ),
                "eligible_workers": _eligible_workers(candidates),
                "eligible_configurations": [
                    {"flatten_backend": str(item.get("flatten_backend")), "workers": int(item["workers"])}
                    for item in sorted(candidates, key=lambda x: (_backend_sort_rank(str(x.get("effective_backend"))), int(x["workers"])))
                ],
            }

    best = max(
        candidates,
        key=lambda item: (
            float(item["records_per_s"]),
            -_backend_sort_rank(str(item.get("effective_backend") or item.get("flatten_backend") or "")),
            -int(item["workers"]),
        ),
    )
    best_rate = float(best["records_per_s"])
    threshold = best_rate * 0.95
    within_tolerance = [item for item in candidates if float(item["records_per_s"]) >= threshold]
    chosen = min(
        within_tolerance,
        key=lambda item: (
            _backend_sort_rank(str(item.get("effective_backend") or item.get("flatten_backend") or "")),
            int(item["workers"]),
        ),
    )
    chosen_worker = int(chosen["workers"])
    best_worker = int(best["workers"])
    chosen_backend = str(chosen.get("flatten_backend") or "python")
    best_backend = str(best.get("flatten_backend") or "python")
    if chosen_worker != best_worker or chosen_backend != best_backend:
        reason = (
            f"backend={chosen_backend}, workers={chosen_worker} is within 5% of the fastest eligible throughput "
            f"({best_rate:.3f} records/s at backend={best_backend}, workers={best_worker}) and has lower operational overhead."
        )
    else:
        reason = (
            f"backend={best_backend}, workers={best_worker} had the highest eligible throughput "
            f"({best_rate:.3f} records/s)."
        )
    return {
        "status": "done",
        "recommended_flatten_backend": chosen_backend,
        "recommended_parallel_workers": chosen_worker,
        "recommendation_reason": reason,
        "eligible_workers": _eligible_workers(candidates),
        "eligible_configurations": [
            {"flatten_backend": str(item.get("flatten_backend")), "workers": int(item["workers"])}
            for item in sorted(candidates, key=lambda x: (_backend_sort_rank(str(x.get("effective_backend"))), int(x["workers"])))
        ],
    }


def _aggregate_worker_attempts(
    *,
    worker: int,
    flatten_backend: str,
    attempts: Sequence[Mapping[str, Any]],
    issue_sample_limit: int,
) -> dict[str, Any]:
    if not attempts:
        return {
            "workers": int(worker),
            "flatten_backend": str(flatten_backend),
            "effective_backend": str(flatten_backend),
            "status": "failed",
            "run_dir": None,
            "parquet_dir": None,
            "report_path": None,
            "quarantine_path": None,
            "artifact_contract_path": None,
            "duration_s": None,
            "records_per_s": None,
            "records_per_s_basis": "none",
            "records_per_s_median": None,
            "records_per_s_min": None,
            "records_per_s_max": None,
            "records_per_s_mean": None,
            "records_per_s_stdev": None,
            "attempt_count": 0,
            "eligible_attempt_count": 0,
            "failed_attempt_count": 0,
            "recommendation_ineligible_attempt_count": 0,
            "records_read": 0,
            "records_ok": 0,
            "parquet_files_persisted": 0,
            "parquet_rows_emitted": 0,
            "parquet_batches_total": 0,
            "timings_ms": {},
            "issue_count": 0,
            "error_count": 0,
            "warning_count": 0,
            "artifact_contract_status": "failed",
            "artifact_contract_issue_count": 0,
            "artifact_contract_warning_count": 0,
            "python_fallback_active": False,
            "rust_raw_jsonl_parse_requested": False,
            "rust_raw_jsonl_parse_effective": False,
            "rust_raw_jsonl_parse_disabled_reason": None,
            "rust_raw_jsonl_file_parse_requested": False,
            "rust_raw_jsonl_file_parse_effective": False,
            "rust_raw_jsonl_file_parse_disabled_reason": None,
            "rust_parallel_table_writes": False,
            "rust_columnar_accumulator": False,
            "rust_parquet_flush_records": 0,
            "rust_arrow_failed_batches": 0,
            "flatten_backend_fallback_reason": None,
            "flatten_backend_auto_disabled_reason": None,
            "flatten_backend_fallback_batches": 0,
            "issue_samples": [],
            "run_report_profile": {},
            "parquet_cleaned": False,
            "attempts": [],
        }
    if len(attempts) == 1:
        row = dict(attempts[0])
        row["flatten_backend"] = str(flatten_backend)
        row.setdefault("effective_backend", row.get("flatten_backend"))
        row["attempt_count"] = 1
        row["eligible_attempt_count"] = 1 if _eligible_for_recommendation(row) else 0
        row["failed_attempt_count"] = 1 if str(row.get("status") or "") == "failed" else 0
        row["recommendation_ineligible_attempt_count"] = 0 if row["eligible_attempt_count"] else 1
        row["records_per_s_basis"] = "single_run"
        row["records_per_s_median"] = row.get("records_per_s")
        row["records_per_s_min"] = row.get("records_per_s")
        row["records_per_s_max"] = row.get("records_per_s")
        row["records_per_s_mean"] = row.get("records_per_s")
        row["records_per_s_stdev"] = None
        row["attempts"] = [dict(attempts[0])]
        return row

    attempt_rows = [dict(item) for item in attempts]
    eligible = [item for item in attempt_rows if _eligible_for_recommendation(item)]
    nonfailed = [item for item in attempt_rows if str(item.get("status") or "") != "failed"]
    metric_rows = eligible if eligible else nonfailed
    rates = [float(item["records_per_s"]) for item in metric_rows if item.get("records_per_s") is not None]
    durations = [float(item["duration_s"]) for item in metric_rows if item.get("duration_s") is not None]
    status = _execution_status(attempt_rows)
    records_per_s = _median(rates)

    issue_samples: list[dict[str, Any]] = []
    issue_seen: set[tuple[str, str, str, str]] = set()
    for item in attempt_rows:
        for sample in list(item.get("issue_samples") or []):
            if not isinstance(sample, Mapping):
                continue
            payload = dict(sample)
            payload.setdefault("repeat_index", item.get("repeat_index"))
            key = (
                str(payload.get("source") or ""),
                str(payload.get("level") or ""),
                str(payload.get("stage") or payload.get("check") or ""),
                str(payload.get("message") or ""),
            )
            if key in issue_seen:
                continue
            issue_seen.add(key)
            issue_samples.append(payload)
            if len(issue_samples) >= max(0, int(issue_sample_limit)):
                break
        if len(issue_samples) >= max(0, int(issue_sample_limit)):
            break

    timing_keys = (
        "io.json_parse",
        "rust_arrow.json_parse",
        "rust_arrow.py_to_json",
        "json.flatten",
        "json.parquet.persist",
        "rust_arrow.total",
    )
    timings: dict[str, int] = {}
    for key in timing_keys:
        values = []
        for item in metric_rows:
            item_timings = item.get("timings_ms") if isinstance(item.get("timings_ms"), Mapping) else {}
            values.append(float(_as_int(item_timings.get(key), 0)))
        med = _median(values)
        timings[key] = int(round(med or 0))

    run_dir = str(Path(str(attempt_rows[0].get("run_dir") or ".")).parent)
    effective_backends = sorted({str(item.get("effective_backend") or flatten_backend) for item in attempt_rows})
    artifact_contract_status = _aggregate_artifact_contract_status(attempt_rows)
    fallback_reasons = [
        str(item.get("flatten_backend_fallback_reason"))
        for item in attempt_rows
        if item.get("flatten_backend_fallback_reason")
    ]
    raw_jsonl_disabled_reasons = [
        str(item.get("rust_raw_jsonl_parse_disabled_reason"))
        for item in attempt_rows
        if item.get("rust_raw_jsonl_parse_disabled_reason")
    ]
    raw_jsonl_file_disabled_reasons = [
        str(item.get("rust_raw_jsonl_file_parse_disabled_reason"))
        for item in attempt_rows
        if item.get("rust_raw_jsonl_file_parse_disabled_reason")
    ]
    auto_disabled_reasons = [
        str(item.get("flatten_backend_auto_disabled_reason"))
        for item in attempt_rows
        if item.get("flatten_backend_auto_disabled_reason")
    ]
    rust_arrow_failed_batches = sum(
        _as_int(item.get("rust_arrow_failed_batches"), _as_int(item.get("flatten_backend_fallback_batches"), 0))
        for item in attempt_rows
    )
    return {
        "workers": int(worker),
        "flatten_backend": str(flatten_backend),
        "effective_backend": effective_backends[0] if len(effective_backends) == 1 else "mixed",
        "status": status,
        "run_dir": run_dir,
        "parquet_dir": None,
        "report_path": None,
        "quarantine_path": None,
        "artifact_contract_path": None,
        "duration_s": _median(durations),
        "records_per_s": records_per_s,
        "records_per_s_basis": "median",
        "records_per_s_median": _median(rates),
        "records_per_s_min": min(rates) if rates else None,
        "records_per_s_max": max(rates) if rates else None,
        "records_per_s_mean": _mean(rates),
        "records_per_s_stdev": _stdev(rates),
        "attempt_count": len(attempt_rows),
        "eligible_attempt_count": len(eligible),
        "failed_attempt_count": sum(1 for item in attempt_rows if str(item.get("status") or "") == "failed"),
        "recommendation_ineligible_attempt_count": len(attempt_rows) - len(eligible),
        "records_read": sum(_as_int(item.get("records_read"), 0) for item in attempt_rows),
        "records_ok": sum(_as_int(item.get("records_ok"), 0) for item in attempt_rows),
        "parquet_files_persisted": sum(_as_int(item.get("parquet_files_persisted"), 0) for item in attempt_rows),
        "parquet_rows_emitted": sum(_as_int(item.get("parquet_rows_emitted"), 0) for item in attempt_rows),
        "parquet_batches_total": sum(_as_int(item.get("parquet_batches_total"), 0) for item in attempt_rows),
        "timings_ms": timings,
        "issue_count": sum(_as_int(item.get("issue_count"), 0) for item in attempt_rows),
        "error_count": sum(_as_int(item.get("error_count"), 0) for item in attempt_rows),
        "warning_count": sum(_as_int(item.get("warning_count"), 0) for item in attempt_rows),
        "artifact_contract_status": artifact_contract_status,
        "artifact_contract_issue_count": sum(_as_int(item.get("artifact_contract_issue_count"), 0) for item in attempt_rows),
        "artifact_contract_warning_count": sum(_as_int(item.get("artifact_contract_warning_count"), 0) for item in attempt_rows),
        "python_fallback_active": any(bool(item.get("python_fallback_active")) for item in attempt_rows),
        "rust_raw_jsonl_parse_requested": any(bool(item.get("rust_raw_jsonl_parse_requested")) for item in attempt_rows),
        "rust_raw_jsonl_parse_effective": any(bool(item.get("rust_raw_jsonl_parse_effective")) for item in attempt_rows),
        "rust_raw_jsonl_parse_disabled_reason": (
            "; ".join(sorted(set(raw_jsonl_disabled_reasons))) if raw_jsonl_disabled_reasons else None
        ),
        "rust_raw_jsonl_file_parse_requested": any(
            bool(item.get("rust_raw_jsonl_file_parse_requested")) for item in attempt_rows
        ),
        "rust_raw_jsonl_file_parse_effective": any(
            bool(item.get("rust_raw_jsonl_file_parse_effective")) for item in attempt_rows
        ),
        "rust_raw_jsonl_file_parse_disabled_reason": (
            "; ".join(sorted(set(raw_jsonl_file_disabled_reasons))) if raw_jsonl_file_disabled_reasons else None
        ),
        "rust_parallel_table_writes": any(bool(item.get("rust_parallel_table_writes")) for item in attempt_rows),
        "rust_columnar_accumulator": any(bool(item.get("rust_columnar_accumulator")) for item in attempt_rows),
        "rust_parquet_flush_records": max(_as_int(item.get("rust_parquet_flush_records"), 0) for item in attempt_rows),
        "rust_arrow_failed_batches": rust_arrow_failed_batches,
        "flatten_backend_fallback_reason": "; ".join(sorted(set(fallback_reasons))) if fallback_reasons else None,
        "flatten_backend_auto_disabled_reason": "; ".join(sorted(set(auto_disabled_reasons))) if auto_disabled_reasons else None,
        "flatten_backend_fallback_batches": rust_arrow_failed_batches,
        "issue_samples": issue_samples,
        "run_report_profile": {},
        "parquet_cleaned": any(bool(item.get("parquet_cleaned")) for item in attempt_rows),
        "attempts": attempt_rows,
    }


def _execution_plan(
    *,
    workers: Sequence[int],
    flatten_backends: Sequence[str],
    repeat: int,
    shuffle_order: bool,
    seed: int | None,
) -> list[tuple[str, int, int]]:
    plan = [
        (str(backend), int(worker), int(repeat_index))
        for repeat_index in range(1, repeat + 1)
        for backend in flatten_backends
        for worker in workers
    ]
    if shuffle_order and len(plan) > 1:
        rng = random.Random(seed)
        rng.shuffle(plan)
    return plan


def _markdown_table_cell(value: Any) -> str:
    return str(value or "").replace("\n", " ").replace("|", r"\|")


def _render_parallel_profile_markdown(summary: Mapping[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# JSON Parallel Profile")
    lines.append("")
    lines.append(f"- status: `{summary.get('status')}`")
    if "execution_status" in summary:
        lines.append(f"- execution_status: `{summary.get('execution_status')}`")
    if "recommendation_status" in summary:
        lines.append(f"- recommendation_status: `{summary.get('recommendation_status')}`")
    lines.append(f"- config: `{summary.get('config')}`")
    lines.append(f"- mode: `{summary.get('mode')}`")
    lines.append(f"- max_records: `{summary.get('max_records')}`")
    lines.append(f"- flatten_backends: `{','.join(str(x) for x in (summary.get('flatten_backends') or []))}`")
    lines.append(f"- rust_raw_jsonl_parse: `{summary.get('rust_raw_jsonl_parse')}`")
    lines.append(f"- rust_raw_jsonl_file_parse: `{summary.get('rust_raw_jsonl_file_parse')}`")
    lines.append(f"- rust_parallel_table_writes: `{summary.get('rust_parallel_table_writes')}`")
    lines.append(f"- rust_columnar_accumulator: `{summary.get('rust_columnar_accumulator')}`")
    lines.append(f"- rust_parquet_flush_records: `{summary.get('rust_parquet_flush_records')}`")
    lines.append(f"- repeat: `{summary.get('repeat')}`")
    lines.append(f"- records_per_s_basis: `{summary.get('records_per_s_basis')}`")
    lines.append(f"- recommended_flatten_backend: `{summary.get('recommended_flatten_backend')}`")
    lines.append(f"- recommended_parallel_workers: `{summary.get('recommended_parallel_workers')}`")
    lines.append(f"- recommendation_reason: {summary.get('recommendation_reason')}")
    lines.append("")
    lines.append("## Worker Runs")
    lines.append("")
    lines.append(
        "| backend | effective | raw_jsonl | file_jsonl | table_write_parallel | columnar | workers | status | attempts | eligible | duration_s | records_per_s | rps_min | rps_max | "
        "io.json_parse_ms | rust_arrow.json_parse_ms | rust_arrow.py_to_json_ms | json.flatten_ms | "
        "json.parquet.persist_ms | rust_arrow.total_ms | "
        "issues | errors | warnings | artifact_contract | rust_arrow_failed_batches | fallback_reason |"
    )
    lines.append(
        "|---|---|---|---|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---|"
    )
    for row in summary.get("runs") or []:
        timings = row.get("timings_ms") if isinstance(row.get("timings_ms"), Mapping) else {}
        duration = row.get("duration_s")
        rps = row.get("records_per_s")
        duration_s = "" if duration is None else f"{float(duration):.3f}"
        rps_s = "" if rps is None else f"{float(rps):.3f}"
        rps_min = row.get("records_per_s_min")
        rps_max = row.get("records_per_s_max")
        rps_min_s = "" if rps_min is None else f"{float(rps_min):.3f}"
        rps_max_s = "" if rps_max is None else f"{float(rps_max):.3f}"
        lines.append(
            "| "
            f"{row.get('flatten_backend')} | {row.get('effective_backend')} | "
            f"{'yes' if row.get('rust_raw_jsonl_parse_effective') else 'no'} | "
            f"{'yes' if row.get('rust_raw_jsonl_file_parse_effective') else 'no'} | "
            f"{'yes' if row.get('rust_parallel_table_writes') else 'no'} | "
            f"{'yes' if row.get('rust_columnar_accumulator') else 'no'} | "
            f"{row.get('workers')} | {row.get('status')} | "
            f"{_as_int(row.get('attempt_count'), 1)} | {_as_int(row.get('eligible_attempt_count'), 0)} | "
            f"{duration_s} | {rps_s} | {rps_min_s} | {rps_max_s} | "
            f"{_as_int(timings.get('io.json_parse'), 0)} | "
            f"{_as_int(timings.get('rust_arrow.json_parse'), 0)} | "
            f"{_as_int(timings.get('rust_arrow.py_to_json'), 0)} | "
            f"{_as_int(timings.get('json.flatten'), 0)} | "
            f"{_as_int(timings.get('json.parquet.persist'), 0)} | "
            f"{_as_int(timings.get('rust_arrow.total'), 0)} | "
            f"{_as_int(row.get('issue_count'), 0)} | "
            f"{_as_int(row.get('error_count'), 0)} | "
            f"{_as_int(row.get('warning_count'), 0)} | "
            f"{row.get('artifact_contract_status')} | "
            f"{_as_int(row.get('rust_arrow_failed_batches'), _as_int(row.get('flatten_backend_fallback_batches'), 0))} | "
            f"{_markdown_table_cell(row.get('flatten_backend_fallback_reason'))} |"
        )
    lines.append("")
    issue_rows: list[tuple[Any, Mapping[str, Any]]] = []
    for row in summary.get("runs") or []:
        for sample in list(row.get("issue_samples") or []):
            if isinstance(sample, Mapping):
                issue_rows.append((f"{row.get('flatten_backend')}/w{row.get('workers')}", sample))
    if issue_rows:
        lines.append("## Issue Samples")
        lines.append("")
        lines.append("| run | source | level | stage/check | message |")
        lines.append("|---|---|---|---|---|")
        for run_label, sample in issue_rows:
            stage = sample.get("stage") or sample.get("check") or ""
            message = sample.get("message") or ""
            lines.append(
                "| "
                f"{_markdown_table_cell(run_label)} | "
                f"{_markdown_table_cell(sample.get('source'))} | "
                f"{_markdown_table_cell(sample.get('level'))} | "
                f"{_markdown_table_cell(stage)} | "
                f"{_markdown_table_cell(message)} |"
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
        "parquet_root": str(_absolute_no_resolve(parquet_dir.expanduser())),
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
    flatten_backends: str | Sequence[str] | None = None,
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
    rust_raw_jsonl_parse: bool | None = None,
    rust_raw_jsonl_file_parse: bool | None = None,
    rust_parallel_table_writes: bool | None = None,
    rust_columnar_accumulator: bool | None = None,
    rust_parquet_flush_records: int | None = None,
    profile_top: int = 8,
    repeat: int = 1,
    shuffle_order: bool = True,
    seed: int | None = 42,
    issue_sample_limit: int = 5,
) -> dict[str, Any]:
    from . import pipeline
    from .parquet_artifacts import inspect_parquet_artifact_contract
    from .report import RunReport
    from .rust_arrow_backend import parse_backend_list

    config = Path(config_path).expanduser().resolve()
    worker_values = parse_worker_list(workers)
    backend_values = parse_backend_list(flatten_backends)
    out = _prepare_profile_output_dir(out_dir)
    summary_json_path = out / "parallel_profile.json"
    summary_md_path = out / "parallel_profile.md"
    _validate_profile_file_path(out, summary_json_path, purpose="profile summary json")
    _validate_profile_file_path(out, summary_md_path, purpose="profile summary markdown")
    repeat_count = max(1, int(repeat or 1))
    sample_limit = max(0, int(issue_sample_limit or 0))

    cleanup_requested = bool(cleanup_parquet) or not bool(keep_artifacts)
    max_records_arg = None
    if max_records is not None:
        max_records_int = int(max_records)
        max_records_arg = max_records_int if max_records_int > 0 else None

    attempts_by_config: dict[tuple[str, int], list[dict[str, Any]]] = {
        (str(backend), int(worker)): [] for backend in backend_values for worker in worker_values
    }
    plan = _execution_plan(
        workers=worker_values,
        flatten_backends=backend_values,
        repeat=repeat_count,
        shuffle_order=bool(shuffle_order),
        seed=seed,
    )
    execution_order: list[dict[str, Any]] = []
    use_backend_dirs = len(backend_values) > 1
    planned_run_dirs: list[Path] = []
    for flatten_backend, worker, repeat_index in plan:
        backend_root = out / str(flatten_backend).replace("-", "_") if use_backend_dirs else out
        worker_root = backend_root / f"w{int(worker)}"
        run_dir = worker_root if repeat_count == 1 else worker_root / f"r{int(repeat_index)}"
        planned_run_dirs.append(run_dir)
    _prepare_profile_run_dirs(out, planned_run_dirs)

    for order, (flatten_backend, worker, repeat_index) in enumerate(plan, start=1):
        backend_root = out / str(flatten_backend).replace("-", "_") if use_backend_dirs else out
        worker_root = backend_root / f"w{int(worker)}"
        run_dir = worker_root if repeat_count == 1 else worker_root / f"r{int(repeat_index)}"
        parquet_dir = run_dir / "parquet"
        report_path = run_dir / "run_report.json"
        quarantine_path = run_dir / "quarantine.jsonl"
        artifact_contract_path = run_dir / "artifact_contract.json"
        _assert_profile_child_path(out, run_dir, purpose="profile run")
        execution_order.append(
            {
                "execution_order": int(order),
                "flatten_backend": str(flatten_backend),
                "workers": int(worker),
                "repeat_index": int(repeat_index),
                "run_dir": str(run_dir),
            }
        )

        data_config, db_config, mode_name = _load_profile_config(config, mode=mode)
        data_config["json_streaming_load"] = False
        data_config["persist_parquet_files"] = True
        data_config["persist_tsv_files"] = False
        data_config["persist_parquet_dir"] = str(parquet_dir)
        data_config["parallel_workers"] = int(worker)
        data_config["flatten_backend"] = str(flatten_backend)
        rust_raw_requested = (
            _as_bool(rust_raw_jsonl_parse, default=False)
            if rust_raw_jsonl_parse is not None
            else _as_bool(data_config.get("rust_raw_jsonl_parse", False), default=False)
        )
        data_config["rust_raw_jsonl_parse"] = bool(rust_raw_requested and str(flatten_backend) == "rust-arrow")
        rust_file_requested = (
            _as_bool(rust_raw_jsonl_file_parse, default=False)
            if rust_raw_jsonl_file_parse is not None
            else _as_bool(data_config.get("rust_raw_jsonl_file_parse", False), default=False)
        )
        data_config["rust_raw_jsonl_file_parse"] = bool(rust_file_requested and str(flatten_backend) == "rust-arrow")
        parallel_table_writes_requested = (
            _as_bool(rust_parallel_table_writes, default=False)
            if rust_parallel_table_writes is not None
            else _as_bool(data_config.get("rust_parallel_table_writes", False), default=False)
        )
        data_config["rust_parallel_table_writes"] = bool(
            parallel_table_writes_requested and str(flatten_backend) == "rust-arrow"
        )
        columnar_requested = (
            _as_bool(rust_columnar_accumulator, default=False)
            if rust_columnar_accumulator is not None
            else _as_bool(data_config.get("rust_columnar_accumulator", False), default=False)
        )
        data_config["rust_columnar_accumulator"] = bool(columnar_requested and str(flatten_backend) == "rust-arrow")
        flush_requested = (
            max(0, int(rust_parquet_flush_records))
            if rust_parquet_flush_records is not None
            else _as_int(data_config.get("rust_parquet_flush_records"), 0)
        )
        data_config["rust_parquet_flush_records"] = int(flush_requested if str(flatten_backend) == "rust-arrow" else 0)
        data_config["progress_path"] = ""
        data_config["progress_interval_s"] = 0.0
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
                "flatten_backend": str(flatten_backend),
                "rust_raw_jsonl_parse": bool(data_config.get("rust_raw_jsonl_parse", False)),
                "rust_raw_jsonl_file_parse": bool(data_config.get("rust_raw_jsonl_file_parse", False)),
                "rust_parallel_table_writes": bool(data_config.get("rust_parallel_table_writes", False)),
                "rust_columnar_accumulator": bool(data_config.get("rust_columnar_accumulator", False)),
                "rust_parquet_flush_records": int(data_config.get("rust_parquet_flush_records", 0) or 0),
                "repeat_index": int(repeat_index),
                "execution_order": int(order),
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
                quarantine=_ProfileQuarantineWriter(out, quarantine_path),
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
        try:
            _safe_write_text(
                out,
                report_path,
                run_report_obj.to_json(),
                purpose="profile run report",
            )
        except Exception as exc:
            run_failed = True
            run_report_obj.exception(
                stage="json.profile_parallel.report_write",
                message="Failed to save profile run report",
                exc=exc,
                path=str(report_path),
            )
        run_report = run_report_obj.to_dict()

        require_schema_manifest = bool(effective_id_compaction)
        require_id_compaction = bool(effective_id_compaction)
        try:
            _assert_profile_child_path(out, parquet_dir, purpose="profile parquet inspect")
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
        try:
            _safe_write_json(
                out,
                artifact_contract_path,
                artifact_contract,
                purpose="profile artifact contract",
            )
        except Exception as exc:
            artifact_contract = dict(artifact_contract)
            issues = list(artifact_contract.get("issues") or [])
            issues.append(
                {
                    "severity": "error",
                    "check": "artifact_contract_write_failed",
                    "message": str(exc),
                    "error_type": type(exc).__name__,
                }
            )
            artifact_contract["issues"] = issues
            artifact_contract["status"] = "failed"

        row = _summarize_worker_run(
            worker=int(worker),
            flatten_backend=str(flatten_backend),
            repeat_index=int(repeat_index),
            execution_order=int(order),
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
            issue_sample_limit=sample_limit,
        )

        if cleanup_requested:
            try:
                row["parquet_cleaned"] = bool(_safe_remove_profile_parquet_dir(out, parquet_dir))
            except Exception as exc:
                cleanup_sample = {
                    "source": "profile_cleanup",
                    "level": "error",
                    "stage": "json.profile_parallel.cleanup",
                    "message": str(exc),
                    "exception_type": type(exc).__name__,
                }
                row["status"] = "failed"
                row["issue_count"] = int(row.get("issue_count") or 0) + 1
                row["error_count"] = int(row.get("error_count") or 0) + 1
                row["cleanup_error"] = cleanup_sample
                samples = list(row.get("issue_samples") or [])
                if sample_limit > 0 and len(samples) < sample_limit:
                    samples.append(cleanup_sample)
                row["issue_samples"] = samples[:sample_limit] if sample_limit > 0 else []

        attempts_by_config[(str(flatten_backend), int(worker))].append(row)

    rows = [
        _aggregate_worker_attempts(
            worker=int(worker),
            flatten_backend=str(backend),
            attempts=attempts_by_config.get((str(backend), int(worker)), []),
            issue_sample_limit=sample_limit,
        )
        for backend in backend_values
        for worker in worker_values
    ]
    recommendation = recommend_parallel_workers(rows)
    execution_status = _execution_status(rows)
    recommendation_status = str(recommendation.get("status") or "failed")
    status = execution_status
    if status == "done" and recommendation_status == "failed":
        status = "done_with_warnings"

    summary: dict[str, Any] = {
        "status": status,
        "execution_status": execution_status,
        "recommendation_status": recommendation_status,
        "generated_at": _utc_now_iso(),
        "config": str(config),
        "out_dir": str(out),
        "mode": str(mode),
        "workers": worker_values,
        "flatten_backends": backend_values,
        "rust_raw_jsonl_parse": bool(_as_bool(rust_raw_jsonl_parse, default=False)) if rust_raw_jsonl_parse is not None else None,
        "rust_raw_jsonl_file_parse": (
            bool(_as_bool(rust_raw_jsonl_file_parse, default=False)) if rust_raw_jsonl_file_parse is not None else None
        ),
        "rust_parallel_table_writes": (
            bool(_as_bool(rust_parallel_table_writes, default=False)) if rust_parallel_table_writes is not None else None
        ),
        "rust_columnar_accumulator": (
            bool(_as_bool(rust_columnar_accumulator, default=False)) if rust_columnar_accumulator is not None else None
        ),
        "rust_parquet_flush_records": (
            max(0, int(rust_parquet_flush_records)) if rust_parquet_flush_records is not None else None
        ),
        "repeat": int(repeat_count),
        "shuffle_order": bool(shuffle_order),
        "seed": seed,
        "max_records": max_records_arg,
        "chunk_size": chunk_size,
        "records_per_s_basis": "median" if repeat_count > 1 else "single_run",
        "keep_artifacts": bool(keep_artifacts),
        "cleanup_parquet": bool(cleanup_requested),
        "execution_order": execution_order,
        "runs": rows,
        "recommended_flatten_backend": recommendation.get("recommended_flatten_backend"),
        "recommended_parallel_workers": recommendation.get("recommended_parallel_workers"),
        "recommendation_reason": recommendation.get("recommendation_reason"),
        "eligible_workers": recommendation.get("eligible_workers") or [],
        "eligible_configurations": recommendation.get("eligible_configurations") or [],
        "summary_json_path": str(summary_json_path),
        "summary_md_path": str(summary_md_path),
    }
    _safe_write_json(out, summary_json_path, summary, purpose="profile summary json")
    _safe_write_text(
        out,
        summary_md_path,
        _render_parallel_profile_markdown(summary) + "\n",
        purpose="profile summary markdown",
    )
    return summary
