from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from ..config import coerce_data_config, coerce_db_config
from ..naming import MYSQL_IDENTIFIER_MAX_LEN, truncate_table_name
from .common import _load_json, _mask_db_config, _parse_formats, _utc_now_iso, _write_text
from .core import (
    DBIntrospector,
    _collect_table_infos_from_db_prefix,
    _collect_table_infos_from_report,
    _merge_db_details,
)
from .report_html import _render_html
from .report_markdown import _render_markdown
from .schema_render import _maybe_svg_to_png, render_mermaid, render_simple_svg


def generate_review_pack(
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
) -> dict[str, Any]:
    """
    Generate a review pack for a v2 run.

    Inputs:
    - config_path: JSON containing {data_config, db_config}
    - report_path: optional RunReport JSON to enrich mapping + issues
    - db_enabled: when True, attempt to introspect DB (requires `.[db]`)
    - exact_counts: when True, uses COUNT(*) per table (slow on large tables)

    Outputs (in out_dir):
    - REVIEW.md
    - review.html
    - schema.mmd
    - schema.svg
    - schema.png (best-effort; requires cairosvg and formats include png)
    - review.json (machine-readable summary)
    """
    cfg = _load_json(config_path)
    data_config = coerce_data_config(cfg.get("data_config") or cfg.get("data") or {})
    db_config = coerce_db_config(cfg.get("db_config") or cfg.get("db") or {})

    base_table = str(data_config.get("table_name") or "").strip()
    if not base_table:
        raise ValueError("data_config.table_name is required")
    key_sep = str(data_config.get("KEY_SEP", "__"))
    base_table_sql = truncate_table_name(base_table, max_len=MYSQL_IDENTIFIER_MAX_LEN)

    report = _load_json(report_path) if report_path else None
    issues = (report.get("issues") if report else None) or None

    fmt = _parse_formats(formats)

    # 1) Seed table list
    table_infos = _collect_table_infos_from_report(base_table=base_table, report=report) if report else []

    # 2) Optional DB introspection (fills rows/cols/indexes + optional samples)
    db_masked = None
    db_error = None
    samples_by_table: dict[str, list[dict[str, Any]]] = {}
    if db_enabled:
        db_masked = _mask_db_config(db_config)
        try:
            db = DBIntrospector(db_config)
            if not table_infos:
                table_infos = _collect_table_infos_from_db_prefix(db=db, base_table=base_table)
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
        except Exception as e:
            # Keep best-effort: still produce pack with non-DB info.
            db_enabled = False
            db_error = str(e)

    # Normalize ordering
    table_infos = sorted(table_infos, key=lambda t: t.name_sql)
    for ti in table_infos:
        if ti.name_original == base_table:
            base_table_sql = ti.name_sql
            break

    use_original_names = any(ti.name_original for ti in table_infos)
    base_table_graph = base_table if use_original_names else base_table_sql

    # Optional: quarantine overlay counts (best-effort).
    quarantine_counts_by_table: dict[str, int] = {}
    quarantine_total = 0
    quarantine_error: str | None = None
    if quarantine_path:
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

        known_sql = {ti.name_sql for ti in table_infos}
        try:
            with open(quarantine_path, encoding="utf-8") as f:
                for line in f:
                    raw = line.strip()
                    if not raw:
                        continue
                    try:
                        entry = json.loads(raw)
                    except Exception:
                        continue
                    quarantine_total += 1
                    table = None
                    ctx = entry.get("context") or {}
                    rec = entry.get("record") or {}
                    if isinstance(ctx, Mapping):
                        for k in ("table", "table_name", "table_sql"):
                            v = ctx.get(k)
                            if v:
                                table = str(v)
                                break
                    if table is None and isinstance(rec, Mapping):
                        for k in ("table", "table_name", "table_sql"):
                            v = rec.get(k)
                            if v:
                                table = str(v)
                                break
                    if not table:
                        continue

                    # Normalize to SQL table name where possible.
                    if table in known_sql:
                        table_sql = table
                    elif table in sql_by_original and sql_by_original[table] in known_sql:
                        table_sql = sql_by_original[table]
                    else:
                        # Best-effort: try truncation to 64.
                        table_sql = truncate_table_name(table, max_len=MYSQL_IDENTIFIER_MAX_LEN)
                        if table_sql not in known_sql:
                            continue

                    quarantine_counts_by_table[table_sql] = int(quarantine_counts_by_table.get(table_sql, 0)) + 1
        except Exception as e:
            quarantine_error = str(e)

    generated_at = _utc_now_iso()
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # Mermaid
    mermaid = render_mermaid(base_table=base_table_graph, table_infos=table_infos, key_sep=key_sep)
    mermaid_path = out_path / "schema.mmd"
    if "mmd" in fmt or "mermaid" in fmt:
        _write_text(mermaid_path, mermaid)
    else:
        # Always write for HTML linking.
        _write_text(mermaid_path, mermaid)

    # SVG (+ optional PNG)
    svg_text = render_simple_svg(base_table=base_table_graph, table_infos=table_infos, key_sep=key_sep)
    svg_path = out_path / "schema.svg"
    if "svg" in fmt:
        _write_text(svg_path, svg_text)
    else:
        _write_text(svg_path, svg_text)

    png_path = out_path / "schema.png"
    png_written = False
    if "png" in fmt:
        png_written = _maybe_svg_to_png(svg_text, png_path)

    # Markdown
    md_path = out_path / "REVIEW.md"
    md = _render_markdown(
        generated_at=generated_at,
        base_table=base_table,
        base_table_sql=base_table_sql,
        key_sep=key_sep,
        formats=fmt,
        config_path=config_path,
        report_path=report_path,
        db_enabled=db_enabled,
        exact_counts=bool(exact_counts),
        db_config_masked=db_masked,
        db_error=db_error,
        mermaid_text=mermaid,
        table_infos=table_infos,
        issues=issues,
    )
    if "md" in fmt:
        _write_text(md_path, md)
    else:
        _write_text(md_path, md)

    # HTML
    html_path = out_path / "review.html"
    title = f"Review Pack: {base_table}"
    html_text = _render_html(
        title=title,
        markdown_path="REVIEW.md",
        schema_svg_path="schema.svg",
        schema_svg_text=svg_text,
        mermaid_path="schema.mmd",
        meta={
            "generated_at": generated_at,
            "config": config_path,
            "report": report_path or "",
            "quarantine": quarantine_path or "",
            "quarantine_entries": int(quarantine_total) if quarantine_path else "",
            "quarantine_error": quarantine_error or "",
            "base_table": base_table,
            "base_table_sql": base_table_sql,
            "key_sep": key_sep,
            "db_enabled": db_enabled,
            "db_error": db_error or "",
            "row_count": "exact" if exact_counts else "estimated",
            "sample_rows": int(sample_rows) if sample_rows is not None else "",
            "sample_max_tables": int(sample_max_tables),
        },
        table_infos=table_infos,
        issues=issues,
        samples_by_table=samples_by_table or None,
        table_badges=({t: {"quarantine": n} for t, n in quarantine_counts_by_table.items()} if quarantine_counts_by_table else None),
        timings_ms=(report.get("timings_ms") if report else None),
        stats=(report.get("stats") if report else None),
    )
    if "html" in fmt:
        _write_text(html_path, html_text)
    else:
        _write_text(html_path, html_text)

    # Machine-readable summary
    review_json: dict[str, Any] = {
        "generated_at": generated_at,
        "config": config_path,
        "report": report_path,
        "base_table": base_table,
        "base_table_sql": base_table_sql,
        "key_sep": key_sep,
        "db_enabled": bool(db_enabled),
        "db_error": db_error,
        "row_count_exact": bool(exact_counts),
        "table_name_namespace": "original" if use_original_names else "sql",
        "sample_rows": int(sample_rows) if sample_rows is not None and int(sample_rows) > 0 else None,
        "sample_max_tables": int(sample_max_tables),
        "db_config": _mask_db_config(db_config),
        "tables": [
            {
                "name_sql": ti.name_sql,
                "name_original": ti.name_original,
                "rows": ti.row_count,
                "rows_estimate": ti.table_rows_estimate,
                "rows_exact": ti.row_count_exact,
                "data_length": ti.data_length,
                "index_length": ti.index_length,
                "engine": ti.engine,
                "collation": ti.collation,
                "columns": ti.columns,
                "indexes": ti.indexes,
            }
            for ti in table_infos
        ],
        "issues": issues or [],
        "artifacts": {
            "schema_svg": "schema.svg",
            "schema_png": "schema.png" if png_written else None,
            "schema_mmd": "schema.mmd",
            "review_md": "REVIEW.md",
            "review_html": "review.html",
        },
    }
    if samples_by_table:
        _write_text(out_path / "samples.json", json.dumps(samples_by_table, ensure_ascii=False, indent=2))
        review_json["artifacts"]["samples_json"] = "samples.json"
    _write_text(out_path / "review.json", json.dumps(review_json, ensure_ascii=False, indent=2))

    return {
        "out_dir": str(out_path),
        "review_md": str(md_path),
        "review_html": str(html_path),
        "schema_svg": str(svg_path),
        "schema_png": str(png_path) if png_written else None,
        "schema_mmd": str(mermaid_path),
        "review_json": str(out_path / "review.json"),
    }
