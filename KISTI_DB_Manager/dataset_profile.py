from __future__ import annotations

import glob
import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .namemap import NameMap, load_namemap
from .naming import MYSQL_IDENTIFIER_MAX_LEN, truncate_table_name


DATASET_PROFILE_SCHEMA_VERSION = "1.0"


@dataclass(frozen=True)
class DatasetProfileResult:
    dataset_profile_path: Path
    profile: dict[str, Any]


@dataclass(frozen=True)
class _TableProfile:
    path: Path
    table: dict[str, Any]
    columns: tuple[dict[str, Any], ...]
    name_map: NameMap | None


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


def _round_ratio(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(float(numerator) / float(denominator), 6)


def resolve_profile_paths(
    patterns: Iterable[str] = (),
    *,
    profiles: Iterable[str] = (),
) -> list[Path]:
    """Resolve directories, glob patterns, and explicit profile paths deterministically."""
    found: list[Path] = []

    def add_candidate(candidate: str | Path) -> None:
        path = Path(candidate).expanduser()
        if path.is_dir():
            found.extend(sorted(path.glob("*_profile.json")))
            return
        matches = sorted(glob.glob(str(path)))
        if matches:
            found.extend(Path(match).expanduser() for match in matches)
            return
        found.append(path)

    for pattern in patterns:
        add_candidate(pattern)
    for profile in profiles:
        add_candidate(profile)

    deduped: dict[str, Path] = {}
    for path in found:
        resolved = path.resolve()
        deduped[str(resolved)] = resolved
    return [deduped[key] for key in sorted(deduped)]


def _load_profile(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"profile must be a JSON object: {path}")
    return data


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _warning_flags(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return sorted({str(item).strip() for item in value if str(item).strip()})
    text = str(value).strip()
    if not text:
        return []
    return sorted({part.strip() for part in text.replace(",", ";").split(";") if part.strip()})


def _count_values(values: Iterable[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value).strip()
        if not key:
            continue
        counts[key] = int(counts.get(key, 0)) + 1
    return {key: counts[key] for key in sorted(counts)}


def _column_sql(column: Mapping[str, Any]) -> str:
    return str(column.get("sql_column") or column.get("source_column") or "")


def _profile_columns(profile: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    columns = profile.get("columns") or []
    if not isinstance(columns, Sequence) or isinstance(columns, (str, bytes)):
        return ()
    out: list[dict[str, Any]] = []
    for column in columns:
        if isinstance(column, Mapping):
            out.append(dict(column))
    return tuple(out)


def _table_names(profile: Mapping[str, Any], path: Path) -> tuple[str, str, NameMap | None]:
    nm = load_namemap(profile.get("name_map"))
    if nm is not None:
        return nm.table_original, nm.table_sql, nm

    source = profile.get("source") if isinstance(profile.get("source"), Mapping) else {}
    table_original = str(source.get("table_name") or path.stem.removesuffix("_profile") or "table")
    table_sql = truncate_table_name(table_original, max_len=MYSQL_IDENTIFIER_MAX_LEN)
    return table_original, table_sql, None


def _summarize_table(profile: Mapping[str, Any], path: Path) -> _TableProfile:
    table_original, table_sql, nm = _table_names(profile, path)
    source = profile.get("source") if isinstance(profile.get("source"), Mapping) else {}
    columns = _profile_columns(profile)
    key_candidates = sorted(
        {
            _column_sql(column)
            for column in columns
            if _column_sql(column) and _boolish(column.get("is_key_candidate"))
        }
    )
    index_recommended = sorted(
        {
            _column_sql(column)
            for column in columns
            if _column_sql(column) and _boolish(column.get("index_recommended"))
        }
    )
    warnings = sorted({str(item) for item in profile.get("warnings") or [] if str(item)})
    table = {
        "table_sql": table_sql,
        "table_original": table_original,
        "source_file": str(source.get("file") or ""),
        "row_count": _int_or_none(source.get("row_count")),
        "column_count": len(columns),
        "key_candidates": key_candidates,
        "index_recommended_columns": index_recommended,
        "warnings": warnings,
    }
    return _TableProfile(path=path, table=table, columns=columns, name_map=nm)


def _infer_base_table(tables: Sequence[_TableProfile], *, key_sep: str) -> str:
    if not tables:
        return ""
    return min(
        (str(table.table.get("table_original") or table.table.get("table_sql") or "") for table in tables),
        key=lambda name: (name.count(key_sep), len(name), name),
    )


def _find_column(table: _TableProfile, column_name: str) -> dict[str, Any] | None:
    for column in table.columns:
        if str(column.get("sql_column") or "") == column_name:
            return column
        if str(column.get("source_column") or "") == column_name:
            return column
    return None


def _source_column_for_sql(table: _TableProfile, column_sql: str) -> str:
    column = _find_column(table, column_sql)
    if column is None:
        return column_sql
    return str(column.get("source_column") or column.get("sql_column") or column_sql)


def _column_identity_names(column: Mapping[str, Any]) -> tuple[str, ...]:
    names = {
        str(column.get("sql_column") or "").strip(),
        str(column.get("source_column") or "").strip(),
    }
    return tuple(sorted(name for name in names if name))


def _is_parent_key_column(column: Mapping[str, Any]) -> bool:
    if _boolish(column.get("is_key_candidate")):
        return True
    unique_ratio = _float_or_none(column.get("unique_ratio"))
    null_ratio = _float_or_none(column.get("null_ratio"))
    return bool(
        unique_ratio is not None
        and unique_ratio >= 0.95
        and (null_ratio is None or null_ratio <= 0.05)
    )


def _relationship_key_pair(
    parent: _TableProfile,
    child: _TableProfile,
) -> tuple[dict[str, Any], dict[str, Any], str] | None:
    parent_id = _find_column(parent, "id")
    child_id = _find_column(child, "id")
    if parent_id is not None and child_id is not None:
        return parent_id, child_id, "exact_id"

    child_by_name: dict[str, dict[str, Any]] = {}
    for child_column in child.columns:
        for name in _column_identity_names(child_column):
            child_by_name.setdefault(name, child_column)

    candidates: list[tuple[tuple[int, float, str], dict[str, Any], dict[str, Any], str]] = []
    for parent_column in parent.columns:
        if not _is_parent_key_column(parent_column):
            continue
        for name in _column_identity_names(parent_column):
            child_column = child_by_name.get(name)
            if child_column is None:
                continue
            child_null_ratio = _float_or_none(child_column.get("null_ratio"))
            if child_null_ratio is not None and child_null_ratio > 0.2:
                continue
            lower = name.lower()
            name_rank = 0 if lower == "id" else 1 if lower.endswith("id") else 2
            parent_unique_ratio = _float_or_none(parent_column.get("unique_ratio")) or 0.0
            candidates.append(
                (
                    (name_rank, -parent_unique_ratio, name),
                    parent_column,
                    child_column,
                    name,
                )
            )

    if not candidates:
        return None

    _, parent_column, child_column, _name = sorted(candidates, key=lambda item: item[0])[0]
    return parent_column, child_column, "shared_parent_key"


def _find_parent_table(
    child: _TableProfile,
    *,
    by_original: Mapping[str, _TableProfile],
    by_sql: Mapping[str, _TableProfile],
    base_table: str,
    key_sep: str,
) -> _TableProfile | None:
    child_original = str(child.table.get("table_original") or "")
    parts = [part for part in child_original.split(key_sep) if part]
    for end in range(len(parts) - 1, 0, -1):
        parent_original = key_sep.join(parts[:end])
        parent = by_original.get(parent_original)
        if parent is not None:
            return parent

    child_sql = str(child.table.get("table_sql") or "")
    sql_parts = [part for part in child_sql.split(key_sep) if part]
    for end in range(len(sql_parts) - 1, 0, -1):
        parent_sql = key_sep.join(sql_parts[:end])
        parent = by_sql.get(parent_sql)
        if parent is not None:
            return parent

    for sub_prefix in (f"{base_table}-SUB{key_sep}", f"{base_table}_SUB{key_sep}"):
        if child_original.startswith(sub_prefix):
            return by_original.get(base_table) or by_sql.get(truncate_table_name(base_table))
    return None


def _relationship_warnings(parent_id: Mapping[str, Any], child_id: Mapping[str, Any]) -> list[str]:
    warnings: list[str] = []
    parent_unique_ratio = _float_or_none(parent_id.get("unique_ratio"))
    child_null_ratio = _float_or_none(child_id.get("null_ratio"))
    parent_type_family = str(parent_id.get("type_family") or "").strip()
    child_type_family = str(child_id.get("type_family") or "").strip()
    if parent_unique_ratio is not None and parent_unique_ratio < 0.95:
        warnings.append("parent_id_unique_ratio_below_0_95")
    if child_null_ratio is not None and child_null_ratio > 0.2:
        warnings.append("child_id_null_ratio_above_0_2")
    if parent_type_family and child_type_family and parent_type_family != child_type_family:
        warnings.append("relationship_column_type_family_mismatch")
    return warnings


def _confidence_bucket(confidence: float | None) -> str:
    value = float(confidence or 0.0)
    if value >= 0.75:
        return "high"
    if value >= 0.5:
        return "medium"
    return "low"


def _candidate_review_priority(confidence: float | None, warnings: Sequence[str]) -> str:
    value = float(confidence or 0.0)
    if value < 0.5 or len(warnings) >= 2:
        return "high_risk"
    if warnings or value < 0.75:
        return "review"
    return "accept_hint"


def _annotate_candidate(candidate: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(candidate)
    warnings = _warning_flags(out.get("warnings"))
    confidence = _float_or_none(out.get("confidence")) or 0.0
    risk_score = min(1.0, max(0.0, 1.0 - confidence) + (0.15 * len(warnings)))
    out["warnings"] = warnings
    out["confidence_bucket"] = _confidence_bucket(confidence)
    out["review_priority"] = _candidate_review_priority(confidence, warnings)
    out["risk_score"] = round(risk_score, 6)
    return out


def _source_path_for_table(table: _TableProfile) -> Path | None:
    raw = str(table.table.get("source_file") or "").strip()
    if not raw:
        return None
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = table.path.parent / path
    return path


def _read_source_column_values(path: Path, column: str, *, max_rows: int) -> list[Any]:
    suffix = path.suffix.lower()
    limit = max(1, int(max_rows or 1))
    if suffix == ".parquet":
        try:
            import pyarrow.parquet as pq  # type: ignore

            pf = pq.ParquetFile(path)
            out: list[Any] = []
            remaining = limit
            for idx in range(pf.num_row_groups):
                if remaining <= 0:
                    break
                table = pf.read_row_group(idx, columns=[column])
                values = table.column(column).to_pylist()
                out.extend(values[:remaining])
                remaining = limit - len(out)
            return out
        except ImportError:
            pass

    import pandas as pd  # type: ignore

    if suffix in {".csv", ".tsv"}:
        sep = "\t" if suffix == ".tsv" else ","
        frame = pd.read_csv(path, usecols=[column], nrows=limit, sep=sep)
        return frame[column].tolist()
    if suffix in {".feather", ".ftr"}:
        frame = pd.read_feather(path, columns=[column])
        return frame[column].head(limit).tolist()
    if suffix == ".parquet":
        frame = pd.read_parquet(path, columns=[column])
        return frame[column].head(limit).tolist()
    raise ValueError(f"unsupported_profile_source_file:{path.name}")


def _normalize_key_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except Exception:
            value = value.hex()
    if isinstance(value, (list, tuple, dict)):
        value = json.dumps(value, ensure_ascii=False, sort_keys=True, default=_json_safe)
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _value_set(values: Iterable[Any]) -> set[str]:
    return {text for text in (_normalize_key_value(value) for value in values) if text}


def _relationship_value_overlap(
    *,
    parent: _TableProfile,
    child: _TableProfile,
    parent_column_sql: str,
    child_column_sql: str,
    max_rows: int,
) -> dict[str, Any]:
    parent_path = _source_path_for_table(parent)
    child_path = _source_path_for_table(child)
    if parent_path is None or child_path is None:
        return {
            "status": "error",
            "reason": "missing_source_file",
            "sampled_max_rows": int(max_rows),
        }
    if not parent_path.exists() or not child_path.exists():
        return {
            "status": "error",
            "reason": "source_file_not_found",
            "parent_source_file": str(parent_path),
            "child_source_file": str(child_path),
            "sampled_max_rows": int(max_rows),
        }

    parent_source_column = _source_column_for_sql(parent, parent_column_sql)
    child_source_column = _source_column_for_sql(child, child_column_sql)
    try:
        parent_values = _read_source_column_values(parent_path, parent_source_column, max_rows=max_rows)
        child_values = _read_source_column_values(child_path, child_source_column, max_rows=max_rows)
    except Exception as exc:
        return {
            "status": "error",
            "reason": str(exc),
            "parent_source_file": str(parent_path),
            "child_source_file": str(child_path),
            "parent_source_column": parent_source_column,
            "child_source_column": child_source_column,
            "sampled_max_rows": int(max_rows),
        }

    parent_set = _value_set(parent_values)
    child_set = _value_set(child_values)
    overlap = parent_set & child_set
    orphan = child_set - parent_set
    child_distinct = len(child_set)
    parent_distinct = len(parent_set)
    orphan_ratio = _round_ratio(len(orphan), child_distinct)
    overlap_ratio = _round_ratio(len(overlap), child_distinct)
    parent_coverage_ratio = _round_ratio(len(overlap), parent_distinct)
    if child_distinct == 0:
        status = "sampled_no_child_values"
    elif orphan_ratio is not None and orphan_ratio <= 0.01:
        status = "sampled_passed_hint"
    elif overlap_ratio is not None and overlap_ratio >= 0.5:
        status = "sampled_partial_overlap"
    else:
        status = "sampled_needs_review"
    return {
        "status": status,
        "sampled_max_rows": int(max_rows),
        "parent_source_file": str(parent_path),
        "child_source_file": str(child_path),
        "parent_source_column": parent_source_column,
        "child_source_column": child_source_column,
        "parent_sampled_rows": len(parent_values),
        "child_sampled_rows": len(child_values),
        "parent_distinct_count": parent_distinct,
        "child_non_null_count": len([value for value in child_values if _normalize_key_value(value)]),
        "child_distinct_count": child_distinct,
        "overlap_distinct_count": len(overlap),
        "orphan_distinct_count": len(orphan),
        "overlap_ratio": overlap_ratio,
        "orphan_ratio": orphan_ratio,
        "parent_coverage_ratio": parent_coverage_ratio,
    }


def _apply_value_overlap_validation(
    candidates: Sequence[Mapping[str, Any]],
    *,
    tables_by_sql: Mapping[str, _TableProfile],
    enabled: bool,
    max_rows: int,
    max_candidates: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not enabled:
        return [dict(candidate) for candidate in candidates], {
            "status": "not_computed",
            "reason": "disabled_by_default",
        }

    out: list[dict[str, Any]] = []
    computed = 0
    skipped = 0
    error_count = 0
    status_counts: list[str] = []
    limit = max(0, int(max_candidates or 0))
    for idx, candidate in enumerate(candidates):
        item = dict(candidate)
        if limit and idx >= limit:
            skipped += 1
            item["value_overlap"] = {
                "status": "skipped",
                "reason": "candidate_limit_reached",
                "sampled_max_rows": int(max_rows),
            }
            out.append(item)
            continue
        parent_sql = str(item.get("parent_table_sql") or "")
        child_sql = str(item.get("child_table_sql") or "")
        parent = tables_by_sql.get(parent_sql)
        child = tables_by_sql.get(child_sql)
        if parent is None or child is None:
            result = {
                "status": "error",
                "reason": "table_profile_not_found",
                "sampled_max_rows": int(max_rows),
            }
        else:
            result = _relationship_value_overlap(
                parent=parent,
                child=child,
                parent_column_sql=str(item.get("parent_column_sql") or "id"),
                child_column_sql=str(item.get("child_column_sql") or "id"),
                max_rows=max_rows,
            )
        item["value_overlap"] = result
        status = str(result.get("status") or "")
        status_counts.append(status)
        if status == "error":
            error_count += 1
        if status in {"sampled_needs_review", "sampled_partial_overlap", "error"}:
            warnings = _warning_flags(item.get("warnings"))
            warnings.append(f"value_overlap_{status}")
            item["warnings"] = sorted(set(warnings))
            item = _annotate_candidate(item)
        computed += 1
        out.append(item)

    return out, {
        "status": "computed",
        "mode": "candidate_key_sample",
        "candidate_count": len(candidates),
        "computed_candidate_count": computed,
        "skipped_candidate_count": skipped,
        "error_count": error_count,
        "sampled_max_rows": int(max_rows),
        "max_candidates": int(max_candidates or 0),
        "status_counts": _count_values(status_counts),
    }


def _skipped_candidate(
    *,
    child: _TableProfile,
    parent: _TableProfile | None,
    reason: str,
) -> dict[str, Any]:
    return {
        "source": "table_name_path",
        "reason": reason,
        "parent_table_sql": str(parent.table.get("table_sql") or "") if parent is not None else "",
        "child_table_sql": str(child.table.get("table_sql") or ""),
        "parent_table_original": str(parent.table.get("table_original") or "") if parent is not None else "",
        "child_table_original": str(child.table.get("table_original") or ""),
        "expected_parent_column_sql": "id",
        "expected_child_column_sql": "id",
    }


def _infer_relationship_candidates_with_audit(
    tables: Sequence[_TableProfile],
    *,
    base_table: str,
    key_sep: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_original = {str(table.table.get("table_original") or ""): table for table in tables}
    by_sql = {str(table.table.get("table_sql") or ""): table for table in tables}
    candidates: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for child in sorted(tables, key=lambda item: str(item.table.get("table_sql") or "")):
        child_original = str(child.table.get("table_original") or "")
        child_sql_name = str(child.table.get("table_sql") or "")
        if child_original == base_table or child_sql_name == truncate_table_name(base_table):
            continue
        parent = _find_parent_table(
            child,
            by_original=by_original,
            by_sql=by_sql,
            base_table=base_table,
            key_sep=key_sep,
        )
        if parent is None or parent is child:
            if key_sep in child_original or key_sep in child_sql_name:
                skipped.append(_skipped_candidate(child=child, parent=None, reason="missing_parent_table"))
            continue

        key_pair = _relationship_key_pair(parent, child)
        if key_pair is None:
            parent_id = _find_column(parent, "id")
            child_id = _find_column(child, "id")
            missing = []
            if parent_id is None:
                missing.append("missing_parent_id")
            if child_id is None:
                missing.append("missing_child_id")
            skipped.append(_skipped_candidate(child=child, parent=parent, reason="_and_".join(missing)))
            continue

        parent_id, child_id, key_match_source = key_pair
        parent_sql = str(parent.table["table_sql"])
        child_sql = str(child.table["table_sql"])
        warnings = _relationship_warnings(parent_id, child_id)
        confidence = 0.8 if not warnings else 0.6
        candidates.append(
            _annotate_candidate(
                {
                    "parent_table_sql": parent_sql,
                    "child_table_sql": child_sql,
                    "parent_column_sql": str(parent_id.get("sql_column") or "id"),
                    "child_column_sql": str(child_id.get("sql_column") or "id"),
                    "relationship_type": "naming_parent_child",
                    "confidence": confidence,
                    "evidence": {
                        "source": "table_name_path",
                        "parent_table_original": str(parent.table.get("table_original") or ""),
                        "child_table_original": str(child.table.get("table_original") or ""),
                        "parent_unique_ratio": _float_or_none(parent_id.get("unique_ratio")),
                        "child_null_ratio": _float_or_none(child_id.get("null_ratio")),
                        "shared_column_name": True,
                        "key_match_source": key_match_source,
                    },
                    "warnings": warnings,
                    "status": "candidate",
                }
            )
        )

    return (
        sorted(candidates, key=lambda item: (item["parent_table_sql"], item["child_table_sql"])),
        sorted(skipped, key=lambda item: (item["child_table_sql"], item["reason"])),
    )


def _infer_relationship_candidates(
    tables: Sequence[_TableProfile],
    *,
    base_table: str,
    key_sep: str,
) -> list[dict[str, Any]]:
    candidates, _skipped = _infer_relationship_candidates_with_audit(
        tables,
        base_table=base_table,
        key_sep=key_sep,
    )
    return candidates


def _build_dataset_audit(
    *,
    tables: Sequence[_TableProfile],
    candidates: Sequence[Mapping[str, Any]],
    skipped_candidates: Sequence[Mapping[str, Any]],
    value_overlap: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    candidate_warnings = [
        warning
        for candidate in candidates
        for warning in _warning_flags(candidate.get("warnings"))
    ]
    table_warnings = [
        warning
        for table in tables
        for warning in _warning_flags(table.table.get("warnings"))
    ]
    column_warnings = [
        warning
        for table in tables
        for column in table.columns
        for warning in _warning_flags(column.get("warnings"))
    ]
    review_counts = _count_values(str(candidate.get("review_priority") or "") for candidate in candidates)
    value_overlap_payload = dict(value_overlap or {"status": "not_computed", "reason": "disabled_by_default"})
    return {
        "mode": "profile_only",
        "data_scan": "sampled" if value_overlap_payload.get("status") == "computed" else "not_performed",
        "candidate_count": len(candidates),
        "confidence_buckets": _count_values(str(candidate.get("confidence_bucket") or "") for candidate in candidates),
        "review_priority_counts": review_counts,
        "candidate_warning_count": len(candidate_warnings),
        "warning_counts": _count_values(candidate_warnings),
        "skipped_candidate_count": len(skipped_candidates),
        "skip_reason_counts": _count_values(str(item.get("reason") or "") for item in skipped_candidates),
        "skipped_candidates": list(skipped_candidates),
        "table_warning_counts": _count_values(table_warnings),
        "column_warning_counts": _count_values(column_warnings),
        "value_overlap": value_overlap_payload,
    }


def build_dataset_profile(
    profile_paths: Iterable[str | Path],
    *,
    base_table: str | None = None,
    key_sep: str = "__",
    generated_at: str | None = None,
    validate_relationships: bool = False,
    validation_max_rows: int = 100000,
    validation_max_candidates: int = 100,
) -> dict[str, Any]:
    paths = [Path(path).expanduser().resolve() for path in profile_paths]
    if not paths:
        raise ValueError("at least one profile path is required")

    warnings: list[str] = []
    tables: list[_TableProfile] = []
    seen_table_sql: set[str] = set()
    for path in sorted(paths):
        profile = _load_profile(path)
        if str(profile.get("schema_version") or "") != "2.0":
            warnings.append(f"profile_schema_version_not_2_0:{path.name}")
        table = _summarize_table(profile, path)
        table_sql = str(table.table.get("table_sql") or "")
        if table_sql in seen_table_sql:
            warnings.append(f"duplicate_table_profile:{table_sql}")
            continue
        seen_table_sql.add(table_sql)
        tables.append(table)

    tables = sorted(tables, key=lambda item: str(item.table.get("table_sql") or ""))
    base = str(base_table or _infer_base_table(tables, key_sep=key_sep))
    candidates, skipped_candidates = _infer_relationship_candidates_with_audit(
        tables,
        base_table=base,
        key_sep=key_sep,
    )
    tables_by_sql = {str(table.table.get("table_sql") or ""): table for table in tables}
    candidates, value_overlap_summary = _apply_value_overlap_validation(
        candidates,
        tables_by_sql=tables_by_sql,
        enabled=bool(validate_relationships),
        max_rows=int(validation_max_rows or 100000),
        max_candidates=int(validation_max_candidates or 100),
    )
    audit = _build_dataset_audit(
        tables=tables,
        candidates=candidates,
        skipped_candidates=skipped_candidates,
        value_overlap=value_overlap_summary,
    )

    return {
        "schema_version": DATASET_PROFILE_SCHEMA_VERSION,
        "generated_at": generated_at or _iso_now(),
        "backend": "python",
        "source": {
            "profile_count": len(tables),
            "profile_paths": [str(path) for path in sorted(paths)],
        },
        "dataset": {
            "base_table": base,
            "base_table_sql": truncate_table_name(base, max_len=MYSQL_IDENTIFIER_MAX_LEN) if base else "",
            "key_sep": key_sep,
        },
        "tables": [table.table for table in tables],
        "relationship_candidates": candidates,
        "audit": audit,
        "warnings": sorted(set(warnings)),
    }


def write_dataset_profile(
    profile_paths: Iterable[str | Path],
    *,
    out_path: str | Path,
    base_table: str | None = None,
    key_sep: str = "__",
    validate_relationships: bool = False,
    validation_max_rows: int = 100000,
    validation_max_candidates: int = 100,
) -> DatasetProfileResult:
    profile = build_dataset_profile(
        profile_paths,
        base_table=base_table,
        key_sep=key_sep,
        validate_relationships=validate_relationships,
        validation_max_rows=validation_max_rows,
        validation_max_candidates=validation_max_candidates,
    )
    path = Path(out_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(profile, ensure_ascii=False, indent=2, default=_json_safe) + "\n", encoding="utf-8")
    return DatasetProfileResult(dataset_profile_path=path, profile=profile)
