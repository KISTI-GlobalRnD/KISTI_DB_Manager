from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from ..config import coerce_data_config, coerce_db_config
from ..namemap import load_namemap
from ..naming import MYSQL_IDENTIFIER_MAX_LEN, truncate_table_name
from .common import _load_json, _parse_formats, _utc_now_iso, _write_text
from .core import TableInfo
from .report_html import _render_html
from .report_markdown import _render_plan_markdown
from .schema_render import _maybe_svg_to_png, render_mermaid, render_simple_svg


def generate_review_plan(
    *,
    config_path: str,
    out_dir: str,
    formats: str | None = None,
    max_records: int | None = 1000,
    generate_desc: bool = False,
    data_overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Generate a pre-load review plan (no DB writes).

    - For JSON inputs: runs `run_json_pipeline(..., create/load/index/optimize=False)` on up to max_records
      and emits predicted NameMaps + DDL + schema diagrams.
    - For tabular inputs: can optionally generate a description CSV (generate_desc=True), then emits NameMap + DDL.
    - data_overrides: optional runtime overrides for data_config (e.g., auto-except knobs).
    """
    from ..pipeline import run_json_pipeline, run_tabular_pipeline
    from ..quarantine import NullQuarantineWriter
    from ..report import RunReport

    cfg = _load_json(config_path)
    data_config = coerce_data_config(cfg.get("data_config") or cfg.get("data") or {})
    if data_overrides:
        for k, v in data_overrides.items():
            if v is None:
                continue
            data_config[str(k)] = v
    db_config = coerce_db_config(cfg.get("db_config") or cfg.get("db") or {})

    base_table = str(data_config.get("table_name") or "").strip()
    if not base_table:
        raise ValueError("data_config.table_name is required")
    key_sep = str(data_config.get("KEY_SEP", "__"))
    base_table_sql = truncate_table_name(base_table, max_len=MYSQL_IDENTIFIER_MAX_LEN)

    file_name = str(data_config.get("file_name") or "")
    file_type = str(data_config.get("file_type") or "").lower()

    fmt = _parse_formats(formats) if formats is not None else {"md", "html", "svg", "mmd"}
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    report = RunReport()
    ddls: dict[str, str] = {}
    table_infos: list[TableInfo] = []
    rd: dict[str, Any] = {}

    if file_type in {"jsonl", "ndjson", "jsonlines", "json", "gz", "zip"}:
        res = run_json_pipeline(
            data_config,
            db_config,
            emit_ddl=True,
            create=False,
            load=False,
            index=False,
            optimize=False,
            continue_on_error=True,
            report=report,
            quarantine=NullQuarantineWriter(),
            max_records=max_records,
        )
        report.finish()
        rd = res.report.to_dict()
        ddls = dict((rd.get("artifacts") or {}).get("create_table_sql_json") or {})

        nm_by_table = (rd.get("artifacts") or {}).get("name_maps_json") or {}
        if isinstance(nm_by_table, Mapping):
            for table_original, nm_dict in nm_by_table.items():
                nm = load_namemap(nm_dict)
                if nm is None:
                    continue
                cols = [{"name": c, "column_type": "LONGTEXT"} for c in nm.columns_sql]
                table_infos.append(TableInfo(name_sql=nm.table_sql, name_original=nm.table_original, columns=cols))

    else:
        # Tabular: reuse the tabular pipeline preparation path (no DB steps)
        res = run_tabular_pipeline(
            data_config,
            db_config,
            generate_desc=bool(generate_desc),
            emit_ddl=True,
            create=False,
            load=False,
            index=False,
            optimize=False,
            continue_on_error=True,
            report=report,
            quarantine=NullQuarantineWriter(),
        )
        res.report.finish()
        rd = res.report.to_dict()
        ddl = (rd.get("artifacts") or {}).get("create_table_sql")
        if isinstance(ddl, str) and ddl.strip():
            ddls = {base_table: ddl}
        nm = load_namemap((rd.get("artifacts") or {}).get("name_map"))
        if nm is not None:
            cols = [{"name": c, "column_type": None} for c in nm.columns_sql]
            table_infos.append(TableInfo(name_sql=nm.table_sql, name_original=nm.table_original, columns=cols))

    # Normalize
    table_infos = sorted(table_infos, key=lambda t: t.name_sql)
    for ti in table_infos:
        if ti.name_original == base_table:
            base_table_sql = ti.name_sql
            break

    use_original_names = any(ti.name_original for ti in table_infos)
    base_table_graph = base_table if use_original_names else base_table_sql

    # Artifacts: schema
    mermaid = render_mermaid(base_table=base_table_graph, table_infos=table_infos, key_sep=key_sep)
    svg_text = render_simple_svg(base_table=base_table_graph, table_infos=table_infos, key_sep=key_sep)

    mermaid_path = out_path / "schema.mmd"
    svg_path = out_path / "schema.svg"
    _write_text(mermaid_path, mermaid)
    _write_text(svg_path, svg_text)

    png_path = out_path / "schema.png"
    png_written = False
    if "png" in fmt:
        png_written = _maybe_svg_to_png(svg_text, png_path)

    # DDL files
    ddl_json_path = out_path / "ddl.json"
    ddl_sql_path = out_path / "ddl.sql"
    _write_text(ddl_json_path, json.dumps(ddls, ensure_ascii=False, indent=2))
    ddl_sql_concat = "\n".join([s.rstrip() for s in ddls.values() if isinstance(s, str)]) + ("\n" if ddls else "")
    _write_text(ddl_sql_path, ddl_sql_concat)

    # Report JSON (plan run)
    report_path = out_path / "plan_run_report.json"
    _write_text(report_path, report.to_json())

    generated_at = _utc_now_iso()
    plan_md_path = out_path / "PLAN.md"
    plan_html_path = out_path / "plan.html"

    md = _render_plan_markdown(
        generated_at=generated_at,
        config_path=config_path,
        base_table=base_table,
        base_table_sql=base_table_sql,
        key_sep=key_sep,
        file_name=file_name,
        file_type=file_type,
        max_records=int(max_records) if max_records is not None else None,
        stats=report.stats,
        timings_ms=report.timings_ms,
        artifacts=(rd.get("artifacts") if isinstance(rd, Mapping) else {}) or {},
        issues=[it.to_dict() for it in report.issues] if hasattr(report, "issues") else None,
        table_infos=table_infos,
        formats=fmt,
    )
    _write_text(plan_md_path, md)

    html_text = _render_html(
        title=f"Review Plan: {base_table}",
        markdown_path="PLAN.md",
        schema_svg_path="schema.svg",
        schema_svg_text=svg_text,
        mermaid_path="schema.mmd",
        meta={
            "generated_at": generated_at,
            "config": config_path,
            "input": file_name,
            "file_type": file_type,
            "base_table": base_table,
            "base_table_sql": base_table_sql,
            "key_sep": key_sep,
            "mode": "plan",
            "max_records": max_records if max_records is not None else "",
        },
        table_infos=table_infos,
        issues=[it.to_dict() for it in report.issues] if hasattr(report, "issues") else None,
        timings_ms=getattr(report, "timings_ms", None),
        stats=getattr(report, "stats", None),
    )
    _write_text(plan_html_path, html_text)

    plan_json = {
        "generated_at": generated_at,
        "config": config_path,
        "base_table": base_table,
        "base_table_sql": base_table_sql,
        "key_sep": key_sep,
        "mode": "plan",
        "max_records": int(max_records) if max_records is not None else None,
        "stats": dict(report.stats),
        "timings_ms": dict(report.timings_ms),
        "issues": [it.to_dict() for it in report.issues],
        "auto_except": ((rd.get("artifacts") or {}).get("auto_except") if isinstance(rd, Mapping) else None),
        "id_compaction": ((rd.get("artifacts") or {}).get("id_compaction") if isinstance(rd, Mapping) else None),
        "ddl_json": "ddl.json",
        "ddl_sql": "ddl.sql",
        "tables": [
            {
                "name_sql": ti.name_sql,
                "name_original": ti.name_original,
                "columns": ti.columns,
            }
            for ti in table_infos
        ],
        "artifacts": {
            "schema_svg": "schema.svg",
            "schema_png": "schema.png" if png_written else None,
            "schema_mmd": "schema.mmd",
            "plan_md": "PLAN.md",
            "plan_html": "plan.html",
            "plan_run_report": "plan_run_report.json",
        },
    }
    _write_text(out_path / "plan.json", json.dumps(plan_json, ensure_ascii=False, indent=2))

    return {
        "out_dir": str(out_path),
        "plan_md": str(plan_md_path),
        "plan_html": str(plan_html_path),
        "schema_svg": str(svg_path),
        "schema_png": str(png_path) if png_written else None,
        "schema_mmd": str(mermaid_path),
        "ddl_json": str(ddl_json_path),
        "ddl_sql": str(ddl_sql_path),
        "plan_json": str(out_path / "plan.json"),
        "plan_run_report": str(report_path),
    }
