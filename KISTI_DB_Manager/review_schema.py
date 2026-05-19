from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from .config import coerce_data_config, coerce_db_config
from ._review.schema_html import render_schema_viewer_html
from ._review.schema_payload import build_schema_viewer_payload, prepare_schema_table_infos
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

    table_infos = (
        _collect_table_infos_from_report(base_table=base_table, report=report)
        if isinstance(report, Mapping)
        else []
    )
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
    use_original_names = any(ti.name_original for ti in table_infos)
    base_table_graph = base_table if use_original_names else base_table_sql
    mermaid = render_mermaid(base_table=base_table_graph, table_infos=table_infos, key_sep=key_sep)
    svg_text = render_simple_svg(base_table=base_table_graph, table_infos=table_infos, key_sep=key_sep)
    edges = build_table_edges(
        base_table=base_table_graph,
        tables=[ti.name_original or ti.name_sql for ti in table_infos],
        key_sep=key_sep,
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
