from __future__ import annotations

import glob
import json
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
    if parent_unique_ratio is not None and parent_unique_ratio < 0.95:
        warnings.append("parent_id_unique_ratio_below_0_95")
    if child_null_ratio is not None and child_null_ratio > 0.2:
        warnings.append("child_id_null_ratio_above_0_2")
    return warnings


def _infer_relationship_candidates(
    tables: Sequence[_TableProfile],
    *,
    base_table: str,
    key_sep: str,
) -> list[dict[str, Any]]:
    by_original = {str(table.table.get("table_original") or ""): table for table in tables}
    by_sql = {str(table.table.get("table_sql") or ""): table for table in tables}
    candidates: list[dict[str, Any]] = []

    for child in sorted(tables, key=lambda item: str(item.table.get("table_sql") or "")):
        parent = _find_parent_table(
            child,
            by_original=by_original,
            by_sql=by_sql,
            base_table=base_table,
            key_sep=key_sep,
        )
        if parent is None or parent is child:
            continue

        parent_id = _find_column(parent, "id")
        child_id = _find_column(child, "id")
        if parent_id is None or child_id is None:
            continue

        parent_sql = str(parent.table["table_sql"])
        child_sql = str(child.table["table_sql"])
        warnings = _relationship_warnings(parent_id, child_id)
        confidence = 0.8 if not warnings else 0.6
        candidates.append(
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
                },
                "warnings": warnings,
                "status": "candidate",
            }
        )

    return sorted(candidates, key=lambda item: (item["parent_table_sql"], item["child_table_sql"]))


def build_dataset_profile(
    profile_paths: Iterable[str | Path],
    *,
    base_table: str | None = None,
    key_sep: str = "__",
    generated_at: str | None = None,
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
    candidates = _infer_relationship_candidates(tables, base_table=base, key_sep=key_sep)

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
        "warnings": sorted(set(warnings)),
    }


def write_dataset_profile(
    profile_paths: Iterable[str | Path],
    *,
    out_path: str | Path,
    base_table: str | None = None,
    key_sep: str = "__",
) -> DatasetProfileResult:
    profile = build_dataset_profile(profile_paths, base_table=base_table, key_sep=key_sep)
    path = Path(out_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(profile, ensure_ascii=False, indent=2, default=_json_safe) + "\n", encoding="utf-8")
    return DatasetProfileResult(dataset_profile_path=path, profile=profile)
