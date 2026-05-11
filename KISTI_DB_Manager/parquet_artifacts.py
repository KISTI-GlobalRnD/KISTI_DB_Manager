from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


OPENALEX_COMPACT_SUFFIXES = ("_openalex_id",)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _read_parquet_schema_rows(path: Path) -> tuple[list[str], int]:
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(path)
    return [str(name) for name in pf.schema_arrow.names], int(getattr(pf.metadata, "num_rows", 0) or 0)


def _status_for(issues: list[dict[str, Any]], warnings: list[dict[str, Any]]) -> str:
    if any(str(item.get("severity") or "error") == "error" for item in issues):
        return "failed"
    if warnings or issues:
        return "done_with_warnings"
    return "done"


def _issue(items: list[dict[str, Any]], *, check: str, message: str, severity: str = "error", **extra: Any) -> None:
    items.append({"severity": severity, "check": check, "message": message, **extra})


def _warning(items: list[dict[str, Any]], *, check: str, message: str, **extra: Any) -> None:
    items.append({"check": check, "message": message, **extra})


def _table_dirs(parquet_root: Path, table_names: list[str] | None) -> tuple[dict[str, Path], list[str]]:
    if table_names:
        out: dict[str, Path] = {}
        missing: list[str] = []
        for name in table_names:
            name_s = str(name).strip()
            if not name_s:
                continue
            path = parquet_root / name_s
            if path.is_dir():
                out[name_s] = path
            else:
                missing.append(name_s)
        return out, missing
    return {path.name: path for path in sorted(parquet_root.iterdir()) if path.is_dir()}, []


def _manifest_summary(manifest: Mapping[str, Any] | None, manifest_path: Path) -> dict[str, Any]:
    if not isinstance(manifest, Mapping):
        return {
            "path": str(manifest_path),
            "exists": False,
            "id_compaction": {"enabled": False},
            "table_count": 0,
            "compacted_column_count": 0,
        }
    idc = manifest.get("id_compaction") if isinstance(manifest.get("id_compaction"), Mapping) else {}
    tables = manifest.get("tables") if isinstance(manifest.get("tables"), Mapping) else {}
    compacted_count = 0
    for table_info in tables.values():
        if not isinstance(table_info, Mapping):
            continue
        cols = table_info.get("columns") if isinstance(table_info.get("columns"), Mapping) else {}
        for info in cols.values():
            if isinstance(info, Mapping) and str(info.get("source_column") or ""):
                compacted_count += 1
    return {
        "path": str(manifest_path),
        "exists": True,
        "generated_at": manifest.get("generated_at"),
        "id_compaction": {
            "enabled": bool(idc.get("enabled")),
            "preset": idc.get("preset"),
            "mode": idc.get("mode"),
            "rules_version": idc.get("rules_version"),
            "rules_hash": idc.get("rules_hash"),
            "column_count": len(idc.get("columns") or []) if isinstance(idc.get("columns"), list) else 0,
            "collision_count": len(idc.get("collisions") or {}) if isinstance(idc.get("collisions"), Mapping) else 0,
            "namespace_conflict_count": (
                len(idc.get("namespace_conflicts") or {})
                if isinstance(idc.get("namespace_conflicts"), Mapping)
                else 0
            ),
            "ambiguous_column_count": (
                len(idc.get("ambiguous_columns") or {}) if isinstance(idc.get("ambiguous_columns"), Mapping) else 0
            ),
        },
        "table_count": len(tables),
        "compacted_column_count": compacted_count,
    }


def _manifest_table_columns(manifest: Mapping[str, Any] | None, table: str) -> dict[str, dict[str, Any]]:
    if not isinstance(manifest, Mapping):
        return {}
    tables = manifest.get("tables")
    if not isinstance(tables, Mapping):
        return {}
    table_info = tables.get(table)
    if not isinstance(table_info, Mapping):
        return {}
    cols = table_info.get("columns")
    if not isinstance(cols, Mapping):
        return {}
    return {str(k): dict(v) for k, v in cols.items() if isinstance(v, Mapping)}


def _looks_like_compacted_column(column: str) -> bool:
    col = str(column)
    return any(col.endswith(suffix) for suffix in OPENALEX_COMPACT_SUFFIXES) or col in {
        "ror_id",
        "doi_id",
        "orcid_id",
    } or col.endswith("__openalex_id")


def inspect_parquet_artifact_contract(
    parquet_root: str | Path,
    *,
    table_names: list[str] | None = None,
    require_schema_manifest: bool = False,
    require_id_compaction: bool = False,
    strict_schema_manifest: bool = False,
    sample_limit: int = 20,
) -> dict[str, Any]:
    """
    Inspect the contract between parquet artifacts and schema_manifest.json.

    This is intentionally footer/metadata based. It does not scan parquet data values.
    """
    root = Path(parquet_root).expanduser().resolve()
    issues: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    report: dict[str, Any] = {
        "status": "running",
        "generated_at": utc_now_iso(),
        "parquet_root": str(root),
        "input": {
            "table_names": list(table_names or []),
            "require_schema_manifest": bool(require_schema_manifest),
            "require_id_compaction": bool(require_id_compaction),
            "strict_schema_manifest": bool(strict_schema_manifest),
        },
        "schema_manifest": {},
        "summary": {},
        "tables": {},
        "issues": issues,
        "warnings": warnings,
    }
    if not root.exists() or not root.is_dir():
        _issue(issues, check="parquet_root_missing", message=f"parquet root not found: {root}", parquet_root=str(root))
        report["schema_manifest"] = _manifest_summary(None, root / "schema_manifest.json")
        report["status"] = _status_for(issues, warnings)
        report["finished_at"] = utc_now_iso()
        return report

    manifest_path = root / "schema_manifest.json"
    manifest: dict[str, Any] | None = None
    if manifest_path.exists():
        try:
            manifest = _read_json(manifest_path)
        except Exception as exc:
            _issue(
                issues,
                check="schema_manifest_parse_error",
                message=f"failed to parse schema_manifest.json: {exc}",
                error_type=type(exc).__name__,
            )
    else:
        msg = "schema_manifest.json is missing from parquet root"
        if require_schema_manifest or require_id_compaction or strict_schema_manifest:
            _issue(issues, check="schema_manifest_missing", message=msg, parquet_root=str(root))
        else:
            _warning(warnings, check="schema_manifest_missing", message=msg, parquet_root=str(root))

    manifest_info = _manifest_summary(manifest, manifest_path)
    report["schema_manifest"] = manifest_info
    idc_info = manifest_info.get("id_compaction") if isinstance(manifest_info.get("id_compaction"), Mapping) else {}
    if require_id_compaction and not bool(idc_info.get("enabled")):
        _issue(
            issues,
            check="id_compaction_required",
            message="id_compaction is required but schema manifest is missing it or marks it disabled",
        )

    dirs, missing_tables = _table_dirs(root, table_names)
    for table in missing_tables:
        _issue(issues, check="parquet_table_missing", message=f"selected parquet table directory not found: {table}", table=table)
    if not dirs:
        _issue(issues, check="no_parquet_tables", message="no parquet table directories selected", parquet_root=str(root))

    total_files = 0
    total_rows = 0
    tables_with_manifest = 0
    tables_with_compacted_columns = 0
    limit = max(0, int(sample_limit))

    for table, table_dir in sorted(dirs.items()):
        files = sorted(table_dir.glob("*.parquet"))
        entry: dict[str, Any] = {
            "status": "running",
            "path": str(table_dir),
            "file_count": len(files),
            "row_count": 0,
            "union_column_count": 0,
            "union_columns_sample": [],
            "schema_variant_count": 0,
            "manifest_column_count": 0,
            "manifest_compacted_column_count": 0,
            "missing_manifest_columns_in_parquet": [],
            "parquet_columns_missing_from_manifest": [],
            "source_and_compacted_columns": [],
            "errors": [],
        }
        if not files:
            _issue(issues, check="parquet_table_empty", message=f"parquet table has no files: {table}", table=table)
            entry["status"] = "failed"
            report["tables"][table] = entry
            continue

        union_columns: list[str] = []
        seen: set[str] = set()
        schema_variants: set[tuple[str, ...]] = set()
        for path in files:
            try:
                cols, rows = _read_parquet_schema_rows(path)
            except Exception as exc:
                error = {"file": str(path), "error_type": type(exc).__name__, "error": str(exc)}
                entry["errors"].append(error)
                _issue(issues, check="parquet_footer_error", message=f"failed to read parquet footer: {path}", table=table, **error)
                continue
            total_files += 1
            total_rows += int(rows)
            entry["row_count"] = int(entry["row_count"]) + int(rows)
            schema_variants.add(tuple(cols))
            for col in cols:
                col_s = str(col)
                if col_s not in seen:
                    seen.add(col_s)
                    union_columns.append(col_s)

        manifest_cols = _manifest_table_columns(manifest, table)
        manifest_col_names = set(manifest_cols)
        actual_col_names = set(union_columns)
        compacted_manifest_cols = {
            col
            for col, info in manifest_cols.items()
            if str(info.get("source_column") or "") and str(info.get("source_column")) != str(col)
        }
        source_and_compacted: list[dict[str, str]] = []
        for col, info in sorted(manifest_cols.items()):
            source_col = str(info.get("source_column") or "")
            if not source_col or source_col == col:
                continue
            if col in actual_col_names and source_col in actual_col_names:
                source_and_compacted.append({"source_column": source_col, "compacted_column": col})

        if manifest_cols:
            tables_with_manifest += 1
        if compacted_manifest_cols or any(_looks_like_compacted_column(col) for col in union_columns):
            tables_with_compacted_columns += 1

        missing_in_parquet = sorted(manifest_col_names - actual_col_names)
        missing_in_manifest = sorted(actual_col_names - manifest_col_names) if manifest_cols else []
        entry.update(
            {
                "status": "done",
                "union_column_count": len(union_columns),
                "union_columns_sample": union_columns[:limit],
                "schema_variant_count": len(schema_variants),
                "manifest_column_count": len(manifest_cols),
                "manifest_compacted_column_count": len(compacted_manifest_cols),
                "missing_manifest_columns_in_parquet": missing_in_parquet[:limit],
                "missing_manifest_columns_in_parquet_count": len(missing_in_parquet),
                "parquet_columns_missing_from_manifest": missing_in_manifest[:limit],
                "parquet_columns_missing_from_manifest_count": len(missing_in_manifest),
                "source_and_compacted_columns": source_and_compacted[:limit],
                "source_and_compacted_column_count": len(source_and_compacted),
            }
        )

        if manifest is not None and not manifest_cols:
            msg = f"table is not present in schema_manifest.json: {table}"
            if strict_schema_manifest:
                _issue(issues, check="table_missing_from_schema_manifest", message=msg, table=table)
                entry["status"] = "failed"
            else:
                _warning(warnings, check="table_missing_from_schema_manifest", message=msg, table=table)
        if missing_in_parquet:
            msg = f"schema_manifest columns are missing from parquet table: {table}"
            if strict_schema_manifest:
                _issue(
                    issues,
                    check="manifest_columns_missing_from_parquet",
                    message=msg,
                    table=table,
                    count=len(missing_in_parquet),
                    sample=missing_in_parquet[:limit],
                )
                entry["status"] = "failed"
            else:
                _warning(
                    warnings,
                    check="manifest_columns_missing_from_parquet",
                    message=msg,
                    table=table,
                    count=len(missing_in_parquet),
                    sample=missing_in_parquet[:limit],
                )
        if source_and_compacted:
            msg = f"source and compacted ID columns coexist in parquet table: {table}"
            if strict_schema_manifest or require_id_compaction:
                _issue(
                    issues,
                    check="mixed_compacted_and_source_columns",
                    message=msg,
                    table=table,
                    sample=source_and_compacted[:limit],
                )
                entry["status"] = "failed"
            else:
                _warning(
                    warnings,
                    check="mixed_compacted_and_source_columns",
                    message=msg,
                    table=table,
                    sample=source_and_compacted[:limit],
                )
        report["tables"][table] = entry

    report["summary"] = {
        "table_count": len(dirs),
        "tables_with_manifest": int(tables_with_manifest),
        "tables_with_compacted_columns": int(tables_with_compacted_columns),
        "file_count": int(total_files),
        "row_count": int(total_rows),
        "schema_manifest_exists": bool(manifest is not None),
        "id_compaction_enabled": bool(idc_info.get("enabled")),
        "id_compaction_rules_version": idc_info.get("rules_version"),
        "id_compaction_rules_hash": idc_info.get("rules_hash"),
        "id_compaction_column_count": int(idc_info.get("column_count") or 0),
    }
    report["status"] = _status_for(issues, warnings)
    report["finished_at"] = utc_now_iso()
    return report


def artifact_contract_from_plan(
    plan: Mapping[str, Any],
    *,
    table_names: list[str] | None = None,
    require_schema_manifest: bool | None = None,
    require_id_compaction: bool | None = None,
    strict_schema_manifest: bool | None = None,
) -> dict[str, Any]:
    run_dir = Path(str(plan.get("run_dir") or ".")).expanduser().resolve()
    parquet_root_value = str(plan.get("parquet_root") or "").strip()
    if not parquet_root_value:
        config_path = Path(str(plan.get("config") or run_dir / "config.json")).expanduser().resolve()
        try:
            cfg = _read_json(config_path)
            parquet_root_value = str((cfg.get("data_config") or {}).get("persist_parquet_dir") or "").strip()
        except Exception:
            parquet_root_value = ""
    parquet_root = Path(parquet_root_value).expanduser().resolve() if parquet_root_value else run_dir / "parquet"
    preflight = plan.get("preflight") if isinstance(plan.get("preflight"), Mapping) else {}
    artifact_cfg = preflight.get("artifact_contract") if isinstance(preflight.get("artifact_contract"), Mapping) else {}
    return inspect_parquet_artifact_contract(
        parquet_root,
        table_names=table_names,
        require_schema_manifest=(
            bool(require_schema_manifest)
            if require_schema_manifest is not None
            else bool(artifact_cfg.get("require_schema_manifest", False))
        ),
        require_id_compaction=(
            bool(require_id_compaction)
            if require_id_compaction is not None
            else bool(artifact_cfg.get("require_id_compaction", False))
        ),
        strict_schema_manifest=(
            bool(strict_schema_manifest)
            if strict_schema_manifest is not None
            else bool(artifact_cfg.get("strict_schema_manifest", False))
        ),
    )
