from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from .config import coerce_data_config, coerce_db_config
from ._review.schema_html import render_schema_viewer_html
from ._review.schema_payload import build_schema_viewer_payload, prepare_schema_table_infos
from .namemap import load_namemap
from .naming import MYSQL_IDENTIFIER_MAX_LEN, truncate_table_name
from .review import (
    DBIntrospector,
    TableInfo,
    _collect_table_infos_from_db_prefix,
    _collect_table_infos_from_report,
    _load_json,
    _mask_db_config,
    _maybe_svg_to_png,
    _merge_db_details,
    _parse_formats,
    _utc_now_iso,
    _write_text,
    build_table_edges,
    render_mermaid,
    render_simple_svg,
)


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None


def _collect_table_infos_from_dataset_profile(dataset_profile: Mapping[str, Any]) -> list[TableInfo]:
    tables = dataset_profile.get("tables") or []
    if not isinstance(tables, list):
        return []
    out: list[TableInfo] = []
    seen: set[str] = set()
    for table in tables:
        if not isinstance(table, Mapping):
            continue
        table_sql = str(table.get("table_sql") or "").strip()
        if not table_sql or table_sql in seen:
            continue
        seen.add(table_sql)
        table_original = str(table.get("table_original") or "").strip() or None
        out.append(
            TableInfo(
                name_sql=table_sql,
                name_original=table_original,
                row_count=_int_or_none(table.get("row_count")),
            )
        )
    return sorted(out, key=lambda item: item.name_sql)


def _dataset_profile_profile_paths(
    *,
    dataset_profile: Mapping[str, Any],
    dataset_profile_path: str | None,
) -> list[Path]:
    source = dataset_profile.get("source") if isinstance(dataset_profile.get("source"), Mapping) else {}
    raw_paths = source.get("profile_paths") if isinstance(source, Mapping) else None
    base_dir = Path(dataset_profile_path).expanduser().parent if dataset_profile_path else None
    paths: list[Path] = []
    if isinstance(raw_paths, list):
        for raw in raw_paths:
            if not raw:
                continue
            path = Path(str(raw)).expanduser()
            if not path.is_absolute() and base_dir is not None:
                path = base_dir / path
            paths.append(path)
    elif base_dir is not None and base_dir.exists():
        paths.extend(sorted(base_dir.glob("*_profile.json")))

    deduped: dict[str, Path] = {}
    for path in paths:
        if path.name == "dataset_profile.json":
            continue
        try:
            resolved = path.resolve()
        except Exception:
            resolved = path
        deduped[str(resolved)] = resolved
    return [deduped[key] for key in sorted(deduped)]


def _table_sql_from_profile(profile: Mapping[str, Any], path: Path) -> str:
    nm = load_namemap(profile.get("name_map"))
    if nm is not None:
        return nm.table_sql
    source = profile.get("source") if isinstance(profile.get("source"), Mapping) else {}
    table = str(source.get("table_name") or path.stem.removesuffix("_profile") or "").strip()
    if not table:
        return ""
    return truncate_table_name(table, max_len=MYSQL_IDENTIFIER_MAX_LEN)


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


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _columns_from_table_profile(profile: Mapping[str, Any]) -> list[dict[str, Any]]:
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
            column_key = "MUL" if _boolish(raw.get("index_recommended")) or _boolish(raw.get("is_key_candidate")) else ""
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


def _profile_columns_by_sql(profile: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for raw in profile.get("columns") or []:
        if not isinstance(raw, Mapping):
            continue
        sql_col = str(raw.get("sql_column") or raw.get("source_column") or "").strip()
        if sql_col:
            out[sql_col] = _compact_column_profile(raw)
    return out


def _merge_table_columns_with_profile(
    columns: list[dict[str, Any]],
    profile: Mapping[str, Any],
) -> list[dict[str, Any]]:
    profile_by_sql = _profile_columns_by_sql(profile)
    merged: list[dict[str, Any]] = []
    for column in columns:
        item = dict(column)
        name = str(item.get("name") or "").strip()
        if name in profile_by_sql:
            item["description_profile"] = profile_by_sql[name]
        merged.append(item)
    return merged


def _load_dataset_table_profiles(
    *,
    dataset_profile: Mapping[str, Any] | None,
    dataset_profile_path: str | None,
) -> dict[str, dict[str, Any]]:
    if not isinstance(dataset_profile, Mapping):
        return {}
    profiles: dict[str, dict[str, Any]] = {}
    for path in _dataset_profile_profile_paths(
        dataset_profile=dataset_profile,
        dataset_profile_path=dataset_profile_path,
    ):
        if not path.exists():
            continue
        try:
            profile = _load_json(str(path))
        except Exception:
            continue
        if not isinstance(profile, Mapping) or str(profile.get("schema_version") or "") != "2.0":
            continue
        table_sql = _table_sql_from_profile(profile, path)
        if table_sql:
            profiles[table_sql] = dict(profile)
    return profiles


def _apply_dataset_table_profiles(
    table_infos: list[TableInfo],
    profiles_by_sql: Mapping[str, Mapping[str, Any]],
) -> list[TableInfo]:
    if not profiles_by_sql:
        return table_infos
    out: list[TableInfo] = []
    for ti in table_infos:
        profile = profiles_by_sql.get(ti.name_sql)
        if profile is None:
            out.append(ti)
            continue
        profile_source = profile.get("source") if isinstance(profile.get("source"), Mapping) else {}
        row_count = ti.row_count
        if row_count is None:
            row_count = _int_or_none(profile_source.get("row_count"))
        columns = (
            _merge_table_columns_with_profile(list(ti.columns or []), profile)
            if ti.columns
            else _columns_from_table_profile(profile)
        )
        out.append(
            TableInfo(
                name_sql=ti.name_sql,
                name_original=ti.name_original,
                row_count=row_count,
                row_count_exact=ti.row_count_exact,
                table_rows_estimate=ti.table_rows_estimate,
                data_length=ti.data_length,
                index_length=ti.index_length,
                engine=ti.engine,
                collation=ti.collation,
                columns=columns,
                indexes=ti.indexes,
            )
        )
    return out


def _dataset_relationship_candidates(dataset_profile: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(dataset_profile, Mapping):
        return []
    raw = dataset_profile.get("relationship_candidates") or []
    if not isinstance(raw, list):
        return []
    return [dict(item) for item in raw if isinstance(item, Mapping)]


def _candidate_confidence(candidate: Mapping[str, Any]) -> float:
    try:
        return float(candidate.get("confidence") or 0.0)
    except Exception:
        return 0.0


def _candidate_only_graph_edges(
    *,
    dataset_profile: Mapping[str, Any] | None,
    table_infos: list[TableInfo],
    base_table_graph: str,
    key_sep: str,
) -> list[tuple[str, str, str]]:
    if not isinstance(dataset_profile, Mapping):
        return []
    graph_by_sql = {
        ti.name_sql: ti.name_original or ti.name_sql
        for ti in table_infos
        if ti.name_sql
    }
    if not graph_by_sql:
        return []
    structural_keys: set[tuple[str, str]] = set()
    structural_edges = build_table_edges(
        base_table=base_table_graph,
        tables=[ti.name_original or ti.name_sql for ti in table_infos],
        key_sep=key_sep,
    )
    sql_by_graph = {
        ti.name_original or ti.name_sql: ti.name_sql
        for ti in table_infos
        if ti.name_sql
    }
    for parent_graph, child_graph, _label in structural_edges:
        parent_sql = sql_by_graph.get(parent_graph, parent_graph)
        child_sql = sql_by_graph.get(child_graph, child_graph)
        structural_keys.add((str(parent_sql), str(child_sql)))

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for candidate in _dataset_relationship_candidates(dataset_profile):
        parent_sql = str(candidate.get("parent_table_sql") or "").strip()
        child_sql = str(candidate.get("child_table_sql") or "").strip()
        key = (parent_sql, child_sql)
        if (
            not parent_sql
            or not child_sql
            or parent_sql == child_sql
            or key in structural_keys
            or parent_sql not in graph_by_sql
            or child_sql not in graph_by_sql
        ):
            continue
        grouped.setdefault(key, []).append(candidate)

    edges: list[tuple[str, str, str]] = []
    for (parent_sql, child_sql), candidates in sorted(grouped.items()):
        primary = max(candidates, key=_candidate_confidence)
        label = str(primary.get("status") or "candidate").strip() or "candidate"
        edges.append((graph_by_sql[parent_sql], graph_by_sql[child_sql], label))
    return edges


def generate_schema_viewer(
    *,
    config_path: str,
    out_dir: str,
    report_path: str | None = None,
    quarantine_path: str | None = None,
    formats: str | None = None,
    db_enabled: bool = True,
    exact_counts: bool = False,
    sample_rows: int | None = None,
    sample_max_tables: int = 20,
    description_profile_path: str | None = None,
    dataset_profile_path: str | None = None,
    relationship_decisions_path: str | None = None,
) -> dict[str, Any]:
    cfg = _load_json(config_path)
    data_config = coerce_data_config(cfg.get("data_config") or cfg.get("data") or {})
    db_config = coerce_db_config(cfg.get("db_config") or cfg.get("db") or {})

    base_table = str(data_config.get("table_name") or "").strip()
    if not base_table:
        raise ValueError("data_config.table_name is required")
    key_sep = str(data_config.get("KEY_SEP", "__"))
    base_table_sql = truncate_table_name(base_table, max_len=MYSQL_IDENTIFIER_MAX_LEN)

    report = _load_json(report_path) if report_path else None
    issues = (report.get("issues") if isinstance(report, Mapping) else None) or None
    fmt = _parse_formats(formats) if formats is not None else {"html", "svg", "mmd"}
    description_profile = None
    if description_profile_path:
        description_profile = _load_json(description_profile_path)
    else:
        profile_path = Path(data_config["PATH"]) / f"{data_config['table_name']}_profile.json"
        if profile_path.exists():
            description_profile = _load_json(str(profile_path))
    dataset_profile = None
    resolved_dataset_profile_path = ""
    if dataset_profile_path:
        resolved_dataset_profile_path = str(Path(dataset_profile_path).expanduser())
        dataset_profile = _load_json(resolved_dataset_profile_path)
    else:
        profile_path = Path(data_config["PATH"]) / "dataset_profile.json"
        if profile_path.exists():
            resolved_dataset_profile_path = str(profile_path)
            dataset_profile = _load_json(str(profile_path))
    relationship_decisions = None
    resolved_relationship_decisions_path = ""
    if relationship_decisions_path:
        resolved_relationship_decisions_path = str(Path(relationship_decisions_path).expanduser())
        relationship_decisions = _load_json(resolved_relationship_decisions_path)
    else:
        decisions_path = Path(data_config["PATH"]) / "relationship_decisions.json"
        if decisions_path.exists():
            resolved_relationship_decisions_path = str(decisions_path)
            relationship_decisions = _load_json(str(decisions_path))
    dataset_table_profiles_by_sql = _load_dataset_table_profiles(
        dataset_profile=dataset_profile if isinstance(dataset_profile, Mapping) else None,
        dataset_profile_path=resolved_dataset_profile_path,
    )

    table_infos = (
        _collect_table_infos_from_report(base_table=base_table, report=report)
        if isinstance(report, Mapping)
        else []
    )
    if not table_infos and isinstance(dataset_profile, Mapping):
        table_infos = _collect_table_infos_from_dataset_profile(dataset_profile)
    if not table_infos:
        table_infos = [TableInfo(name_sql=base_table_sql, name_original=base_table)]

    db_masked = None
    db_error = None
    samples_by_table: dict[str, list[dict[str, Any]]] = {}
    if db_enabled:
        db_masked = _mask_db_config(db_config)
        try:
            db = DBIntrospector(db_config)
            if not table_infos or (
                len(table_infos) == 1
                and table_infos[0].name_sql == base_table_sql
                and not report_path
            ):
                table_infos = _collect_table_infos_from_db_prefix(db=db, base_table=base_table)
                if not table_infos:
                    table_infos = [TableInfo(name_sql=base_table_sql, name_original=base_table)]
            table_infos = _merge_db_details(db=db, table_infos=table_infos, exact_counts=bool(exact_counts))
            if sample_rows is not None and int(sample_rows) > 0:
                sr = int(sample_rows)
                mt = max(1, int(sample_max_tables or 1))
                ordered = sorted(table_infos, key=lambda t: (0 if t.name_sql == base_table_sql else 1, t.name_sql))
                for ti in ordered[:mt]:
                    try:
                        samples_by_table[ti.name_sql] = db.sample_rows(table_name=ti.name_sql, limit=sr)
                    except Exception:
                        continue
        except Exception as exc:
            db_enabled = False
            db_error = str(exc)

    table_infos = prepare_schema_table_infos(
        report=report if isinstance(report, Mapping) else None,
        table_infos=table_infos,
    )
    table_infos = _apply_dataset_table_profiles(table_infos, dataset_table_profiles_by_sql)
    use_original_names = any(ti.name_original for ti in table_infos)
    base_table_graph = base_table if use_original_names else base_table_sql
    edges = build_table_edges(
        base_table=base_table_graph,
        tables=[ti.name_original or ti.name_sql for ti in table_infos],
        key_sep=key_sep,
    )
    candidate_only_edges = _candidate_only_graph_edges(
        dataset_profile=dataset_profile if isinstance(dataset_profile, Mapping) else None,
        table_infos=table_infos,
        base_table_graph=base_table_graph,
        key_sep=key_sep,
    )
    mermaid = render_mermaid(
        base_table=base_table_graph,
        table_infos=table_infos,
        key_sep=key_sep,
        extra_edges=candidate_only_edges,
    )
    svg_text = render_simple_svg(
        base_table=base_table_graph,
        table_infos=table_infos,
        key_sep=key_sep,
        extra_edges=candidate_only_edges,
    )
    payload = build_schema_viewer_payload(
        config_path=config_path,
        report_path=report_path,
        quarantine_path=quarantine_path,
        report=report if isinstance(report, Mapping) else None,
        issues=issues if isinstance(issues, list) else None,
        table_infos=table_infos,
        base_table=base_table,
        base_table_sql=base_table_sql,
        base_table_graph=base_table_graph,
        key_sep=key_sep,
        db_config=db_config,
        db_masked=db_masked,
        db_enabled=db_enabled,
        db_error=db_error,
        samples_by_table=samples_by_table,
        edges=edges,
        generated_at=_utc_now_iso(),
        description_profile=description_profile if isinstance(description_profile, Mapping) else None,
        dataset_profile=dataset_profile if isinstance(dataset_profile, Mapping) else None,
        dataset_profile_path=resolved_dataset_profile_path,
        relationship_decisions=relationship_decisions if isinstance(relationship_decisions, Mapping) else None,
        relationship_decisions_path=resolved_relationship_decisions_path,
        dataset_table_profile_count=len(dataset_table_profiles_by_sql),
        dataset_table_profile_column_count=sum(
            len(profile.get("columns") or [])
            for profile in dataset_table_profiles_by_sql.values()
        ),
    )

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    mermaid_path = out_path / "schema.mmd"
    svg_path = out_path / "schema.svg"
    html_path = out_path / "schema_viewer.html"
    json_path = out_path / "schema_viewer.json"
    png_path = out_path / "schema.png"

    _write_text(mermaid_path, mermaid)
    _write_text(svg_path, svg_text)
    png_written = False
    if "png" in fmt:
        png_written = _maybe_svg_to_png(svg_text, png_path)

    html_text = render_schema_viewer_html(
        title=f"Schema Viewer: {base_table}",
        base_table=base_table,
        meta=payload["meta"],
        svg_text=svg_text,
        payload=payload,
    )
    _write_text(html_path, html_text)
    _write_text(json_path, json.dumps(payload, ensure_ascii=False, indent=2))

    return {
        "out_dir": str(out_path),
        "schema_viewer_html": str(html_path),
        "schema_viewer_json": str(json_path),
        "schema_svg": str(svg_path),
        "schema_png": str(png_path) if png_written else None,
        "schema_mmd": str(mermaid_path),
    }
