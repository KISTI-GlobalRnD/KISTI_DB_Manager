from __future__ import annotations

import json
from typing import Any, Iterable, Mapping

from ..namemap import load_namemap
from ..naming import MYSQL_IDENTIFIER_MAX_LEN, truncate_table_name
from .core import TableInfo
from .schema_graph import (
    fallback_join_sql,
    infer_table_role,
    quote_mysql_identifier,
    relationship_join_sql,
    table_depth,
    table_display_label,
)


def _human_bytes(value: int | None) -> str:
    if value is None:
        return "n/a"
    try:
        n = float(int(value))
    except Exception:
        return str(value)
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    idx = 0
    while n >= 1024.0 and idx < len(units) - 1:
        n /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(n)} {units[idx]}"
    return f"{n:.1f} {units[idx]}"


def _qi(name: str) -> str:
    return quote_mysql_identifier(name)


def _collect_predicted_columns_by_sql(report: Mapping[str, Any] | None) -> dict[str, list[dict[str, Any]]]:
    artifacts = (report or {}).get("artifacts") or {}
    result: dict[str, list[dict[str, Any]]] = {}
    name_maps_json = artifacts.get("name_maps_json")
    if isinstance(name_maps_json, Mapping):
        for _table_original, nm_dict in name_maps_json.items():
            nm = load_namemap(nm_dict)
            if nm is None:
                continue
            cols = []
            for col in nm.columns_sql:
                cols.append(
                    {
                        "name": str(col),
                        "data_type": "longtext",
                        "column_type": "LONGTEXT",
                        "is_nullable": "YES",
                        "column_key": "PRI" if str(col) == "id" else "",
                        "extra": "",
                    }
                )
            result[nm.table_sql] = cols
    else:
        nm = load_namemap(artifacts.get("name_map"))
        if nm is not None:
            result[nm.table_sql] = [
                {
                    "name": str(col),
                    "data_type": "longtext",
                    "column_type": "LONGTEXT",
                    "is_nullable": "YES",
                    "column_key": "PRI" if str(col) == "id" else "",
                    "extra": "",
                }
                for col in nm.columns_sql
            ]
    return result


def _apply_predicted_columns(
    table_infos: list[TableInfo],
    predicted_by_sql: Mapping[str, list[dict[str, Any]]],
) -> list[TableInfo]:
    out: list[TableInfo] = []
    for ti in table_infos:
        cols = ti.columns if ti.columns else predicted_by_sql.get(ti.name_sql)
        out.append(
            TableInfo(
                name_sql=ti.name_sql,
                name_original=ti.name_original,
                row_count=ti.row_count,
                row_count_exact=ti.row_count_exact,
                table_rows_estimate=ti.table_rows_estimate,
                data_length=ti.data_length,
                index_length=ti.index_length,
                engine=ti.engine,
                collation=ti.collation,
                columns=cols,
                indexes=ti.indexes,
            )
        )
    return out


def _description_profile_table_sql(profile: Mapping[str, Any] | None) -> str:
    if not isinstance(profile, Mapping):
        return ""
    nm = load_namemap(profile.get("name_map"))
    if nm is not None:
        return nm.table_sql
    source = profile.get("source") if isinstance(profile.get("source"), Mapping) else {}
    return str(source.get("table_name") or "")


def _compact_column_profile(row: Mapping[str, Any]) -> dict[str, Any]:
    keys = [
        "source_column",
        "sql_column",
        "suggested_type",
        "type_family",
        "type_confidence",
        "type_reason",
        "null_ratio",
        "empty_string_ratio",
        "unique_ratio",
        "top_freq_ratio",
        "is_key_candidate",
        "index_recommended",
        "warnings",
    ]
    return {key: row.get(key) for key in keys if key in row}


def _description_profile_columns_by_sql(profile: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(profile, Mapping):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for raw in profile.get("columns") or []:
        if not isinstance(raw, Mapping):
            continue
        sql_col = str(raw.get("sql_column") or raw.get("source_column") or raw.get("name") or "").strip()
        if not sql_col:
            continue
        out[sql_col] = _compact_column_profile(raw)
    return out


def _columns_from_description_profile(profile: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(profile, Mapping):
        return []
    cols: list[dict[str, Any]] = []
    for raw in profile.get("columns") or []:
        if not isinstance(raw, Mapping):
            continue
        name = str(raw.get("sql_column") or raw.get("source_column") or "").strip()
        if not name:
            continue
        suggested_type = str(raw.get("suggested_type") or raw.get("Type") or "LONGTEXT")
        null_ratio = raw.get("null_ratio")
        try:
            nullable = float(null_ratio) > 0
        except Exception:
            nullable = True
        column_key = str(raw.get("column_key") or "").strip().upper()
        if column_key not in {"PRI", "UNI", "MUL"}:
            column_key = "MUL" if bool(raw.get("index_recommended") or raw.get("is_key_candidate") or raw.get("is_key")) else ""
        cols.append(
            {
                "name": name,
                "data_type": suggested_type.lower(),
                "column_type": suggested_type,
                "is_nullable": "YES" if nullable else "NO",
                "column_key": column_key,
                "extra": "",
                "description_profile": _compact_column_profile(raw),
            }
        )
    return cols


def _merge_description_profile_columns(
    columns: list[dict[str, Any]],
    profile_by_sql: Mapping[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    for col in columns:
        item = dict(col)
        name = str(item.get("name") or "").strip()
        profile = profile_by_sql.get(name)
        if profile:
            item["description_profile"] = profile
        merged.append(item)
    return merged


def _description_profile_summary(profile: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(profile, Mapping):
        return None
    source = profile.get("source") if isinstance(profile.get("source"), Mapping) else {}
    return {
        "schema_version": profile.get("schema_version"),
        "backend": profile.get("backend"),
        "table_sql": _description_profile_table_sql(profile),
        "source_file": source.get("file"),
        "source_row_count": source.get("row_count"),
        "column_count": len(profile.get("columns") or []),
        "warnings": profile.get("warnings") or [],
    }


def _compact_relationship_candidate(candidate: Mapping[str, Any]) -> dict[str, Any]:
    keys = [
        "parent_table_sql",
        "child_table_sql",
        "parent_column_sql",
        "child_column_sql",
        "relationship_type",
        "confidence",
        "confidence_bucket",
        "review_priority",
        "risk_score",
        "status",
        "warnings",
        "evidence",
    ]
    return {key: candidate.get(key) for key in keys if key in candidate}


def _normalize_relationship_decision(value: Any) -> str:
    decision = str(value or "").strip().lower().replace("-", "_")
    aliases = {
        "accept": "accepted",
        "accepted": "accepted",
        "approve": "accepted",
        "approved": "accepted",
        "reject": "rejected",
        "rejected": "rejected",
        "deny": "rejected",
        "denied": "rejected",
        "review": "needs_review",
        "needs_review": "needs_review",
        "needs review": "needs_review",
        "defer": "deferred",
        "deferred": "deferred",
        "hold": "deferred",
    }
    return aliases.get(decision, decision or "undecided")


def _compact_relationship_decision(decision: Mapping[str, Any]) -> dict[str, Any]:
    keys = [
        "parent_table_sql",
        "child_table_sql",
        "parent_column_sql",
        "child_column_sql",
        "decision",
        "reason",
        "reviewed_by",
        "reviewed_at",
        "source",
        "notes",
    ]
    item = {key: decision.get(key) for key in keys if key in decision}
    item["decision"] = _normalize_relationship_decision(decision.get("decision") or decision.get("status"))
    return item


def _dataset_relationship_candidates(profile: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(profile, Mapping):
        return []
    raw = profile.get("relationship_candidates") or []
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for candidate in raw:
        if isinstance(candidate, Mapping):
            out.append(_compact_relationship_candidate(candidate))
    return out


def _relationship_decisions(decisions_profile: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(decisions_profile, Mapping):
        return []
    raw = decisions_profile.get("decisions") or decisions_profile.get("relationship_decisions") or []
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for decision in raw:
        if not isinstance(decision, Mapping):
            continue
        item = _compact_relationship_decision(decision)
        if item.get("parent_table_sql") and item.get("child_table_sql"):
            out.append(item)
    return out


def _dataset_candidate_key(candidate: Mapping[str, Any]) -> tuple[str, str]:
    return (
        str(candidate.get("parent_table_sql") or ""),
        str(candidate.get("child_table_sql") or ""),
    )


def _relationship_decision_key(decision: Mapping[str, Any]) -> tuple[str, str]:
    return (
        str(decision.get("parent_table_sql") or ""),
        str(decision.get("child_table_sql") or ""),
    )


def _relationship_decision_summary(decisions: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    decision_list = list(decisions)
    counts = _count_values(
        _normalize_relationship_decision(decision.get("decision"))
        for decision in decision_list
    )
    status = ""
    if len(counts) == 1:
        status = next(iter(counts))
    elif len(counts) > 1:
        status = "mixed"
    return {
        "relationship_decisions": decision_list,
        "relationship_decision_count": len(decision_list),
        "relationship_decision_counts": counts,
        "relationship_decision_status": status,
        "relationship_operator_reviewed": len(decision_list) > 0,
    }


def _candidate_confidence(candidate: Mapping[str, Any]) -> float:
    try:
        return float(candidate.get("confidence") or 0.0)
    except Exception:
        return 0.0


def _candidate_warning_count(candidate: Mapping[str, Any]) -> int:
    warnings = candidate.get("warnings")
    if isinstance(warnings, list):
        return len([item for item in warnings if str(item)])
    if warnings:
        return 1
    return 0


def _count_values(values: Iterable[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value).strip()
        if not key:
            continue
        counts[key] = int(counts.get(key, 0)) + 1
    return {key: counts[key] for key in sorted(counts)}


def _sum_count_dicts(items: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        for key, value in item.items():
            if not str(key).strip():
                continue
            try:
                n = int(value or 0)
            except Exception:
                n = 0
            counts[str(key)] = int(counts.get(str(key), 0)) + n
    return {key: counts[key] for key in sorted(counts)}


def _candidate_review_priority(candidate: Mapping[str, Any]) -> str:
    priority = str(candidate.get("review_priority") or "").strip()
    if priority:
        return priority
    if _candidate_warning_count(candidate) > 0:
        return "review"
    return ""


def _review_priority_rank(priority: str) -> int:
    return {
        "accept_hint": 0,
        "review": 1,
        "high_risk": 2,
    }.get(str(priority), -1)


def _candidate_key_match_source(candidate: Mapping[str, Any]) -> str:
    evidence = candidate.get("evidence")
    if not isinstance(evidence, Mapping):
        return ""
    return str(evidence.get("key_match_source") or "").strip()


def _relationship_candidate_summary(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    priorities = [_candidate_review_priority(candidate) for candidate in candidates]
    priority_counts = _count_values(priorities)
    worst_priority = ""
    if priority_counts:
        worst_priority = max(priority_counts, key=_review_priority_rank)
    key_sources = sorted(
        {
            key_source
            for key_source in (_candidate_key_match_source(candidate) for candidate in candidates)
            if key_source
        }
    )
    return {
        "relationship_review_priority": worst_priority,
        "relationship_review_priority_counts": priority_counts,
        "relationship_needs_review": worst_priority in {"review", "high_risk"},
        "relationship_key_match_sources": key_sources,
        "relationship_primary_key_match_source": key_sources[0] if key_sources else "",
    }


def _candidate_column_sql(candidate: Mapping[str, Any], key: str) -> str:
    value = str(candidate.get(key) or "").strip()
    return value or "id"


def _dataset_profile_summary(
    profile: Mapping[str, Any] | None,
    *,
    source_file: str | None = None,
) -> dict[str, Any] | None:
    if not isinstance(profile, Mapping):
        return None
    source = profile.get("source") if isinstance(profile.get("source"), Mapping) else {}
    dataset = profile.get("dataset") if isinstance(profile.get("dataset"), Mapping) else {}
    tables = profile.get("tables") if isinstance(profile.get("tables"), list) else []
    candidates = _dataset_relationship_candidates(profile)
    audit = profile.get("audit") if isinstance(profile.get("audit"), Mapping) else {}
    status_counts: dict[str, int] = {}
    for candidate in candidates:
        status = str(candidate.get("status") or "unknown")
        status_counts[status] = int(status_counts.get(status, 0)) + 1
    audit_summary = {
        key: audit.get(key)
        for key in (
            "mode",
            "data_scan",
            "candidate_count",
            "confidence_buckets",
            "review_priority_counts",
            "candidate_warning_count",
            "warning_counts",
            "skipped_candidate_count",
            "skip_reason_counts",
            "value_overlap",
        )
        if key in audit
    }
    return {
        "schema_version": profile.get("schema_version"),
        "backend": profile.get("backend"),
        "source_file": source_file or "",
        "source_profile_count": source.get("profile_count"),
        "base_table": dataset.get("base_table"),
        "base_table_sql": dataset.get("base_table_sql"),
        "key_sep": dataset.get("key_sep"),
        "table_count": len(tables),
        "relationship_candidate_count": len(candidates),
        "relationship_candidate_status_counts": status_counts,
        "audit": audit_summary,
        "warnings": profile.get("warnings") or [],
    }


def _relationship_decisions_profile_summary(
    decisions_profile: Mapping[str, Any] | None,
    *,
    source_file: str | None = None,
) -> dict[str, Any] | None:
    if not isinstance(decisions_profile, Mapping):
        return None
    decisions = _relationship_decisions(decisions_profile)
    return {
        "schema_version": decisions_profile.get("schema_version"),
        "source_file": source_file or "",
        "decision_count": len(decisions),
        "decision_counts": _count_values(decision.get("decision") for decision in decisions),
        "warnings": decisions_profile.get("warnings") or [],
    }


def prepare_schema_table_infos(
    *,
    report: Mapping[str, Any] | None,
    table_infos: list[TableInfo],
) -> list[TableInfo]:
    predicted_columns = _collect_predicted_columns_by_sql(report)
    ordered = sorted(table_infos, key=lambda ti: ti.name_sql)
    return _apply_predicted_columns(ordered, predicted_columns)


def collect_schema_ddls_by_sql(
    *,
    report: Mapping[str, Any] | None,
    table_infos: Iterable[TableInfo],
) -> dict[str, str]:
    artifacts = (report or {}).get("artifacts") or {}
    ddls: dict[str, str] = {}
    raw = artifacts.get("create_table_sql_json")
    if isinstance(raw, Mapping):
        for key, value in raw.items():
            if isinstance(value, str) and value.strip():
                ddls[str(key)] = value
    single = artifacts.get("create_table_sql")
    if isinstance(single, str) and single.strip():
        for ti in table_infos:
            if ti.name_sql not in ddls:
                ddls[ti.name_sql] = single
                break
    if ddls:
        return ddls

    synthesized: dict[str, str] = {}
    for ti in table_infos:
        if not ti.columns:
            continue
        lines: list[str] = []
        pk_cols: list[str] = []
        for col in ti.columns:
            name = str(col.get("name") or "").strip()
            if not name:
                continue
            column_type = str(col.get("column_type") or col.get("data_type") or "LONGTEXT")
            nullable = str(col.get("is_nullable") or "YES").upper() == "YES"
            extra = str(col.get("extra") or "").strip()
            if str(col.get("column_key") or "").upper() == "PRI":
                pk_cols.append(name)
            part = f"  `{_qi(name)}` {column_type}"
            if not nullable:
                part += " NOT NULL"
            if extra:
                part += f" {extra}"
            lines.append(part)
        if pk_cols:
            cols_sql = ", ".join(f"`{_qi(col)}`" for col in pk_cols)
            lines.append(f"  PRIMARY KEY ({cols_sql})")
        if not lines:
            continue
        synthesized[ti.name_sql] = (
            f"CREATE TABLE `{_qi(ti.name_sql)}` (\n"
            + ",\n".join(lines)
            + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
        )
    return synthesized


def _collect_issue_counts_by_sql(
    *,
    issues: list[dict[str, Any]] | None,
    table_infos: Iterable[TableInfo],
) -> dict[str, dict[str, int]]:
    known_sql = {ti.name_sql for ti in table_infos}
    by_original = {ti.name_original: ti.name_sql for ti in table_infos if ti.name_original}
    counts: dict[str, dict[str, int]] = {}
    for issue in issues or []:
        if not isinstance(issue, Mapping):
            continue
        level = str(issue.get("level") or "").strip().lower()
        if not level:
            continue
        context = issue.get("context") or {}
        table = None
        if isinstance(context, Mapping):
            for key in ("table", "table_name", "table_sql"):
                value = context.get(key)
                if value:
                    table = str(value)
                    break
        if not table:
            continue
        table_sql = table
        if table_sql not in known_sql:
            table_sql = by_original.get(table) or truncate_table_name(table, max_len=MYSQL_IDENTIFIER_MAX_LEN)
        if table_sql not in known_sql:
            continue
        bucket = counts.setdefault(table_sql, {"error": 0, "warning": 0})
        bucket[level] = int(bucket.get(level, 0)) + 1
    return counts


def _collect_quarantine_counts_by_sql(
    *,
    quarantine_path: str | None,
    report: Mapping[str, Any] | None,
    table_infos: Iterable[TableInfo],
) -> tuple[dict[str, int], int, str | None]:
    if not quarantine_path:
        return {}, 0, None
    known_sql = {ti.name_sql for ti in table_infos}
    sql_by_original: dict[str, str] = {}
    try:
        artifacts = (report or {}).get("artifacts") or {}
        nm_by_table = artifacts.get("name_maps_json") or {}
        if isinstance(nm_by_table, Mapping):
            for _k, nm in nm_by_table.items():
                if isinstance(nm, Mapping) and nm.get("table_original") and nm.get("table_sql"):
                    sql_by_original[str(nm.get("table_original"))] = str(nm.get("table_sql"))
    except Exception:
        sql_by_original = {}

    counts: dict[str, int] = {}
    total = 0
    error: str | None = None
    try:
        with open(quarantine_path, encoding="utf-8") as handle:
            for line in handle:
                raw = line.strip()
                if not raw:
                    continue
                try:
                    entry = json.loads(raw)
                except Exception:
                    continue
                total += 1
                context = entry.get("context") or {}
                record = entry.get("record") or {}
                table = None
                if isinstance(context, Mapping):
                    for key in ("table", "table_name", "table_sql"):
                        value = context.get(key)
                        if value:
                            table = str(value)
                            break
                if table is None and isinstance(record, Mapping):
                    for key in ("table", "table_name", "table_sql"):
                        value = record.get(key)
                        if value:
                            table = str(value)
                            break
                if not table:
                    continue
                table_sql = table
                if table_sql not in known_sql:
                    table_sql = sql_by_original.get(table) or truncate_table_name(
                        table,
                        max_len=MYSQL_IDENTIFIER_MAX_LEN,
                    )
                if table_sql not in known_sql:
                    continue
                counts[table_sql] = int(counts.get(table_sql, 0)) + 1
    except Exception as exc:
        error = str(exc)
    return counts, total, error


def build_schema_viewer_payload(
    *,
    config_path: str,
    report_path: str | None,
    quarantine_path: str | None,
    report: Mapping[str, Any] | None,
    issues: list[dict[str, Any]] | None,
    table_infos: list[TableInfo],
    base_table: str,
    base_table_sql: str,
    base_table_graph: str,
    key_sep: str,
    db_config: Mapping[str, Any],
    db_masked: Mapping[str, Any] | None,
    db_enabled: bool,
    db_error: str | None,
    samples_by_table: Mapping[str, list[dict[str, Any]]],
    edges: list[tuple[str, str, str]],
    generated_at: str,
    description_profile: Mapping[str, Any] | None = None,
    dataset_profile: Mapping[str, Any] | None = None,
    dataset_profile_path: str | None = None,
    relationship_decisions: Mapping[str, Any] | None = None,
    relationship_decisions_path: str | None = None,
    dataset_table_profile_count: int = 0,
    dataset_table_profile_column_count: int = 0,
) -> dict[str, Any]:
    ddls_by_sql = collect_schema_ddls_by_sql(report=report, table_infos=table_infos)
    issue_counts_by_sql = _collect_issue_counts_by_sql(issues=issues, table_infos=table_infos)
    quarantine_counts_by_sql, quarantine_total, quarantine_error = _collect_quarantine_counts_by_sql(
        quarantine_path=quarantine_path,
        report=report,
        table_infos=table_infos,
    )

    info_by_graph_name = {ti.name_original or ti.name_sql: ti for ti in table_infos}
    info_by_sql = {ti.name_sql: ti for ti in table_infos if ti.name_sql}
    edges_payload: list[dict[str, Any]] = []
    parent_edges_by_child_sql: dict[str, list[dict[str, Any]]] = {}
    child_edges_by_parent_sql: dict[str, list[dict[str, Any]]] = {}
    dataset_candidates = _dataset_relationship_candidates(dataset_profile)
    dataset_candidates_by_edge: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for candidate in dataset_candidates:
        key = _dataset_candidate_key(candidate)
        if all(key):
            dataset_candidates_by_edge.setdefault(key, []).append(candidate)
    relationship_decision_items = _relationship_decisions(relationship_decisions)
    relationship_decisions_by_edge: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for decision in relationship_decision_items:
        key = _relationship_decision_key(decision)
        if all(key):
            relationship_decisions_by_edge.setdefault(key, []).append(decision)
    matched_dataset_candidate_count = 0
    matched_dataset_candidate_keys: set[tuple[str, str]] = set()
    matched_relationship_decision_count = 0
    matched_relationship_decision_keys: set[tuple[str, str]] = set()
    for parent, child, label in edges:
        parent_info = info_by_graph_name.get(parent)
        child_info = info_by_graph_name.get(child)
        parent_sql = (
            parent_info.name_sql
            if parent_info is not None
            else truncate_table_name(parent, max_len=MYSQL_IDENTIFIER_MAX_LEN)
        )
        child_sql = (
            child_info.name_sql
            if child_info is not None
            else truncate_table_name(child, max_len=MYSQL_IDENTIFIER_MAX_LEN)
        )
        item = {
            "parent": parent,
            "child": child,
            "label": label,
            "parent_sql": parent_sql,
            "child_sql": child_sql,
            "parent_display": table_display_label(base_table_graph, key_sep, parent),
            "child_display": table_display_label(base_table_graph, key_sep, child),
            "join_sql": relationship_join_sql(parent_sql=parent_sql, child_sql=child_sql),
            "relationship_source": "structural_naming",
            "relationship_status": "structural",
            "relationship_type": "structural_naming",
            "relationship_candidate_count": 0,
            "relationship_warning_count": 0,
            "relationship_review_priority": "",
            "relationship_review_priority_counts": {},
            "relationship_needs_review": False,
            "relationship_key_match_sources": [],
            "relationship_primary_key_match_source": "",
        }
        relationship_candidates = dataset_candidates_by_edge.get((parent_sql, child_sql)) or []
        if relationship_candidates:
            primary_candidate = max(relationship_candidates, key=_candidate_confidence)
            statuses = sorted({str(candidate.get("status") or "unknown") for candidate in relationship_candidates})
            types = sorted(
                {
                    str(candidate.get("relationship_type") or "relationship")
                    for candidate in relationship_candidates
                }
            )
            item["relationship_candidates"] = relationship_candidates
            item["relationship_candidate_count"] = len(relationship_candidates)
            item["relationship_confidence_max"] = max(
                _candidate_confidence(candidate)
                for candidate in relationship_candidates
            )
            item["relationship_statuses"] = statuses
            item["relationship_source"] = "dataset_profile"
            item["relationship_status"] = statuses[0] if len(statuses) == 1 else "mixed"
            item["relationship_type"] = types[0] if len(types) == 1 else "mixed"
            item["relationship_warning_count"] = sum(
                _candidate_warning_count(candidate)
                for candidate in relationship_candidates
            )
            item["parent_column_sql"] = _candidate_column_sql(primary_candidate, "parent_column_sql")
            item["child_column_sql"] = _candidate_column_sql(primary_candidate, "child_column_sql")
            item.update(_relationship_candidate_summary(relationship_candidates))
            item["join_sql"] = relationship_join_sql(
                parent_sql=parent_sql,
                child_sql=child_sql,
                parent_column_sql=item["parent_column_sql"],
                child_column_sql=item["child_column_sql"],
            )
            matched_dataset_candidate_count += len(relationship_candidates)
            matched_dataset_candidate_keys.add((parent_sql, child_sql))
        edge_decisions = relationship_decisions_by_edge.get((parent_sql, child_sql)) or []
        if edge_decisions:
            item.update(_relationship_decision_summary(edge_decisions))
            matched_relationship_decision_count += len(edge_decisions)
            matched_relationship_decision_keys.add((parent_sql, child_sql))
        edges_payload.append(item)
        parent_edges_by_child_sql.setdefault(child_sql, []).append(item)
        child_edges_by_parent_sql.setdefault(parent_sql, []).append(item)

    for (parent_sql, child_sql), relationship_candidates in sorted(dataset_candidates_by_edge.items()):
        if (parent_sql, child_sql) in matched_dataset_candidate_keys:
            continue
        parent_info = info_by_sql.get(parent_sql)
        child_info = info_by_sql.get(child_sql)
        if parent_info is None or child_info is None or parent_sql == child_sql:
            continue
        primary_candidate = max(relationship_candidates, key=_candidate_confidence)
        statuses = sorted({str(candidate.get("status") or "unknown") for candidate in relationship_candidates})
        types = sorted(
            {
                str(candidate.get("relationship_type") or "relationship")
                for candidate in relationship_candidates
            }
        )
        parent_graph = parent_info.name_original or parent_info.name_sql
        child_graph = child_info.name_original or child_info.name_sql
        item = {
            "parent": parent_graph,
            "child": child_graph,
            "label": "candidate",
            "parent_sql": parent_sql,
            "child_sql": child_sql,
            "parent_display": table_display_label(base_table_graph, key_sep, parent_graph),
            "child_display": table_display_label(base_table_graph, key_sep, child_graph),
            "join_sql": relationship_join_sql(
                parent_sql=parent_sql,
                child_sql=child_sql,
                parent_column_sql=_candidate_column_sql(primary_candidate, "parent_column_sql"),
                child_column_sql=_candidate_column_sql(primary_candidate, "child_column_sql"),
            ),
            "relationship_source": "dataset_profile",
            "relationship_status": statuses[0] if len(statuses) == 1 else "mixed",
            "relationship_type": types[0] if len(types) == 1 else "mixed",
            "relationship_candidates": relationship_candidates,
            "relationship_candidate_count": len(relationship_candidates),
            "relationship_confidence_max": max(
                _candidate_confidence(candidate)
                for candidate in relationship_candidates
            ),
            "relationship_statuses": statuses,
            "relationship_warning_count": sum(
                _candidate_warning_count(candidate)
                for candidate in relationship_candidates
            ),
            "parent_column_sql": _candidate_column_sql(primary_candidate, "parent_column_sql"),
            "child_column_sql": _candidate_column_sql(primary_candidate, "child_column_sql"),
        }
        item.update(_relationship_candidate_summary(relationship_candidates))
        edge_decisions = relationship_decisions_by_edge.get((parent_sql, child_sql)) or []
        if edge_decisions:
            item.update(_relationship_decision_summary(edge_decisions))
            matched_relationship_decision_count += len(edge_decisions)
            matched_relationship_decision_keys.add((parent_sql, child_sql))
        edges_payload.append(item)
        parent_edges_by_child_sql.setdefault(child_sql, []).append(item)
        child_edges_by_parent_sql.setdefault(parent_sql, []).append(item)
        matched_dataset_candidate_count += len(relationship_candidates)
        matched_dataset_candidate_keys.add((parent_sql, child_sql))

    table_payloads: list[dict[str, Any]] = []
    totals = {"rows": 0, "columns": 0, "size_bytes": 0}
    depth_groups: dict[int, list[str]] = {}
    issue_tables = 0
    profile_table_sql = _description_profile_table_sql(description_profile)
    profile_columns_by_sql = _description_profile_columns_by_sql(description_profile)
    description_profile_summary = _description_profile_summary(description_profile)
    dataset_profile_summary = _dataset_profile_summary(dataset_profile, source_file=dataset_profile_path)
    relationship_decisions_summary = _relationship_decisions_profile_summary(
        relationship_decisions,
        source_file=relationship_decisions_path,
    )
    if dataset_profile_summary is not None:
        dataset_profile_summary = {
            **dataset_profile_summary,
            "table_profile_count_loaded": int(dataset_table_profile_count or 0),
            "table_profile_column_count_loaded": int(dataset_table_profile_column_count or 0),
        }
    for ti in table_infos:
        graph_name = ti.name_original or ti.name_sql
        depth = table_depth(base_table_graph, key_sep, graph_name)
        is_base = str(graph_name) == str(base_table_graph)
        role = infer_table_role(depth, is_base=is_base)
        cols = list(ti.columns or [])
        table_profile = None
        if profile_table_sql and ti.name_sql == profile_table_sql:
            table_profile = description_profile_summary
            if not cols:
                cols = _columns_from_description_profile(description_profile)
            else:
                cols = _merge_description_profile_columns(cols, profile_columns_by_sql)
        idxs = list(ti.indexes or [])
        rows_sort = ti.row_count if ti.row_count is not None else ti.table_rows_estimate
        size_bytes = int((ti.data_length or 0) + (ti.index_length or 0)) if (
            ti.data_length is not None or ti.index_length is not None
        ) else 0
        issue_counts = issue_counts_by_sql.get(ti.name_sql) or {}
        quarantine_count = int(quarantine_counts_by_sql.get(ti.name_sql) or 0)
        if issue_counts or quarantine_count:
            issue_tables += 1
        totals["rows"] += int(rows_sort or 0)
        totals["columns"] += len(cols)
        totals["size_bytes"] += int(size_bytes or 0)
        depth_groups.setdefault(depth, []).append(ti.name_sql)
        display_short = table_display_label(base_table_graph, key_sep, graph_name)
        parent_edges = parent_edges_by_child_sql.get(ti.name_sql) or []
        child_edges = child_edges_by_parent_sql.get(ti.name_sql) or []
        relationship_edges = parent_edges + child_edges
        relationship_priority_counts = _sum_count_dicts(
            edge.get("relationship_review_priority_counts") or {}
            for edge in relationship_edges
            if isinstance(edge.get("relationship_review_priority_counts"), Mapping)
        )
        relationship_key_sources = sorted(
            {
                str(source)
                for edge in relationship_edges
                for source in edge.get("relationship_key_match_sources", [])
                if str(source)
            }
        )
        relationship_decision_counts = _sum_count_dicts(
            edge.get("relationship_decision_counts") or {}
            for edge in relationship_edges
            if isinstance(edge.get("relationship_decision_counts"), Mapping)
        )
        if parent_edges:
            join_sql = str(parent_edges[0].get("join_sql") or "")
        elif ti.name_sql != base_table_sql:
            join_sql = fallback_join_sql(base_table_sql=base_table_sql, table_sql=ti.name_sql)
        else:
            join_sql = fallback_join_sql(base_table_sql=base_table_sql, table_sql=base_table_sql)
        ddl = ddls_by_sql.get(ti.name_sql) or ddls_by_sql.get(graph_name) or ""
        payload = {
            "name_sql": ti.name_sql,
            "name_original": ti.name_original,
            "display_short": display_short,
            "display_full": graph_name,
            "role": role,
            "role_label": "BASE" if role == "base" else ("SUB" if role == "sub" else "NESTED"),
            "depth": depth,
            "rows_sort": int(rows_sort or 0),
            "rows_label": ti.rows_label(),
            "column_count": len(cols),
            "index_count": len(idxs),
            "size_bytes": int(size_bytes or 0),
            "size_label": _human_bytes(size_bytes),
            "engine": ti.engine,
            "collation": ti.collation,
            "columns": cols,
            "description_profile": table_profile,
            "indexes": idxs,
            "samples": samples_by_table.get(ti.name_sql) or [],
            "sample_count": len(samples_by_table.get(ti.name_sql) or []),
            "ddl": ddl,
            "join_sql": join_sql,
            "parent_edges": parent_edges,
            "child_edges": child_edges,
            "relationship_count": len(parent_edges) + len(child_edges),
            "relationship_candidate_count": sum(
                int(edge.get("relationship_candidate_count") or 0)
                for edge in relationship_edges
            ),
            "relationship_warning_count": sum(
                int(edge.get("relationship_warning_count") or 0)
                for edge in relationship_edges
            ),
            "relationship_review_priority_counts": relationship_priority_counts,
            "relationship_needs_review_count": int(relationship_priority_counts.get("review") or 0)
            + int(relationship_priority_counts.get("high_risk") or 0),
            "relationship_key_match_sources": relationship_key_sources,
            "relationship_decision_count": sum(
                int(edge.get("relationship_decision_count") or 0)
                for edge in relationship_edges
            ),
            "relationship_decision_counts": relationship_decision_counts,
            "is_disconnected": not parent_edges and not child_edges,
            "issue_error_count": int(issue_counts.get("error") or 0),
            "issue_warning_count": int(issue_counts.get("warning") or 0),
            "quarantine_count": quarantine_count,
        }
        payload["search_blob"] = " ".join(
            [
                str(payload.get("name_sql") or ""),
                str(payload.get("name_original") or ""),
                str(payload.get("display_short") or ""),
                ddl,
                " ".join(str(col.get("name") or "") for col in cols),
                " ".join(
                    str((col.get("description_profile") or {}).get("warnings") or "")
                    for col in cols
                    if isinstance(col.get("description_profile"), Mapping)
                ),
                " ".join(str(ix.get("index_name") or "") for ix in idxs),
                " ".join(
                    str(candidate.get("relationship_type") or "")
                    for edge in relationship_edges
                    for candidate in edge.get("relationship_candidates", [])
                    if isinstance(candidate, Mapping)
                ),
                " ".join(
                    str(candidate.get("review_priority") or "")
                    for edge in relationship_edges
                    for candidate in edge.get("relationship_candidates", [])
                    if isinstance(candidate, Mapping)
                ),
                " ".join(
                    str((candidate.get("evidence") or {}).get("key_match_source") or "")
                    for edge in relationship_edges
                    for candidate in edge.get("relationship_candidates", [])
                    if isinstance(candidate, Mapping) and isinstance(candidate.get("evidence"), Mapping)
                ),
                " ".join(
                    str(candidate.get("warnings") or "")
                    for edge in relationship_edges
                    for candidate in edge.get("relationship_candidates", [])
                    if isinstance(candidate, Mapping)
                ),
                " ".join(
                    str(decision.get("decision") or "")
                    for edge in relationship_edges
                    for decision in edge.get("relationship_decisions", [])
                    if isinstance(decision, Mapping)
                ),
                " ".join(
                    str(decision.get("reason") or "")
                    for edge in relationship_edges
                    for decision in edge.get("relationship_decisions", [])
                    if isinstance(decision, Mapping)
                ),
            ]
        ).lower()
        table_payloads.append(payload)

    groups = []
    for depth in sorted(depth_groups):
        if depth == 0:
            label = "Depth 0 · Base"
            description = "메인 테이블"
        elif depth == 1:
            label = "Depth 1 · First-level subtables"
            description = "base 바로 아래에서 분기된 첫 번째 subtable"
        else:
            label = f"Depth {depth} · Nested subtables"
            description = f"경로 깊이 {depth} 단계의 nested subtable"
        groups.append(
            {
                "depth": depth,
                "label": label,
                "description": description,
                "table_sqls": sorted(depth_groups[depth]),
            }
        )

    meta_payload = {
        "generated_at": generated_at,
        "config": config_path,
        "report": report_path or "",
        "database": db_masked.get("database") if db_masked else db_config.get("database"),
        "db_enabled": bool(db_enabled),
        "db_error": db_error,
        "quarantine": quarantine_path or "",
        "quarantine_entries": quarantine_total,
        "quarantine_error": quarantine_error,
        "base_table": base_table,
        "base_table_sql": base_table_sql,
        "key_sep": key_sep,
        "mode": "schema-viewer",
        "description_profile": description_profile_summary,
    }
    summary_payload = {
        "table_count": len(table_payloads),
        "rows_total": int(totals["rows"]),
        "columns_total": int(totals["columns"]),
        "size_bytes_total": int(totals["size_bytes"]),
        "flagged_table_count": int(issue_tables),
        "edge_count": len(edges_payload),
    }
    dataset_profile_payload = None
    if dataset_profile_summary is not None:
        meta_payload["dataset_profile"] = dataset_profile_summary
        summary_payload.update(
            {
                "relationship_candidate_count": len(dataset_candidates),
                "relationship_candidates_on_edges": matched_dataset_candidate_count,
                "structural_edge_count": len(edges),
                "candidate_only_edge_count": max(0, len(edges_payload) - len(edges)),
                "unmatched_relationship_candidate_count": max(
                    0,
                    len(dataset_candidates) - matched_dataset_candidate_count,
                ),
            }
        )
        dataset_profile_payload = {
            **dataset_profile_summary,
            "unmatched_relationship_candidates": [
                candidate
                for candidate in dataset_candidates
                if _dataset_candidate_key(candidate) not in matched_dataset_candidate_keys
            ],
        }
    relationship_decisions_payload = None
    if relationship_decisions_summary is not None:
        meta_payload["relationship_decisions"] = relationship_decisions_summary
        summary_payload.update(
            {
                "relationship_decision_count": len(relationship_decision_items),
                "relationship_decisions_on_edges": matched_relationship_decision_count,
                "unmatched_relationship_decision_count": max(
                    0,
                    len(relationship_decision_items) - matched_relationship_decision_count,
                ),
                "relationship_decision_counts": relationship_decisions_summary.get("decision_counts") or {},
            }
        )
        relationship_decisions_payload = {
            **relationship_decisions_summary,
            "unmatched_relationship_decisions": [
                decision
                for decision in relationship_decision_items
                if _relationship_decision_key(decision) not in matched_relationship_decision_keys
            ],
        }

    result = {
        "meta": meta_payload,
        "summary": summary_payload,
        "tables": table_payloads,
        "groups": groups,
        "edges": edges_payload,
        "description_profile": description_profile_summary,
    }
    if dataset_profile_payload is not None:
        result["dataset_profile"] = dataset_profile_payload
    if relationship_decisions_payload is not None:
        result["relationship_decisions"] = relationship_decisions_payload
    return result
