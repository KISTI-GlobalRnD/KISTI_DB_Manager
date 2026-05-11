#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from KISTI_DB_Manager.review import TableInfo, _utc_now_iso, _write_text, build_table_edges, render_mermaid, render_simple_svg
from KISTI_DB_Manager.review_preview import write_review_preview_report
from KISTI_DB_Manager.review_schema import _collect_ddls_by_sql, _render_schema_viewer_html
from KISTI_DB_Manager.naming import truncate_table_name


try:
    import pyarrow.parquet as pq
except Exception as exc:  # pragma: no cover
    raise RuntimeError("pyarrow is required for parquet schema bundle generation") from exc


@dataclass(frozen=True)
class ErdJob:
    name: str
    base_table: str
    source_dir: Path
    pattern: str
    mode: str  # parsed | tabular


@dataclass(frozen=True)
class PreviewJob:
    name: str
    config: dict[str, Any]


def _slugify(value: str) -> str:
    text = str(value or "").strip()
    text = re.sub(r"^\d+(?:[._]\d+)?__", "", text)
    text = text.replace("__MAIN", "").replace("__SUB__", "__")
    text = re.sub(r"[^0-9A-Za-z가-힣_]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text.lower() or "table"


def _read_desc_columns(desc_path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    with desc_path.open("r", encoding="utf-8-sig", newline="") as fp:
        reader = csv.DictReader(fp)
        fieldnames = list(reader.fieldnames or [])
        if not fieldnames:
            return [], []
        name_key = fieldnames[0]
        columns: list[dict[str, Any]] = []
        indexes: list[dict[str, Any]] = []
        seq = 1
        for row in reader:
            raw_name = str(row.get(name_key) or "").strip()
            if not raw_name:
                continue
            column_type = str(row.get("Type") or "LONGTEXT").strip() or "LONGTEXT"
            if column_type.lower() == "unknown":
                column_type = "LONGTEXT"
            is_key = str(row.get("is_key") or "").strip().lower() == "true"
            columns.append(
                {
                    "name": raw_name,
                    "data_type": column_type.lower(),
                    "column_type": column_type,
                    "is_nullable": "YES",
                    "column_key": "PRI" if is_key else "",
                    "extra": "",
                }
            )
            if is_key:
                indexes.append(
                    {
                        "index_name": "PRIMARY",
                        "column_name": raw_name,
                        "seq_in_index": seq,
                        "non_unique": 0,
                    }
                )
                seq += 1
        return columns, indexes


def _parquet_columns(parquet_path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    pf = pq.ParquetFile(parquet_path)
    columns = []
    for field in pf.schema_arrow:
        columns.append(
            {
                "name": str(field.name),
                "data_type": str(field.type),
                "column_type": str(field.type),
                "is_nullable": "YES" if field.nullable else "NO",
                "column_key": "",
                "extra": "",
            }
        )
    return columns, []


def _table_info_from_parquet(parquet_path: Path, *, table_sql: str) -> TableInfo:
    desc_path = parquet_path.with_name(f"{parquet_path.stem}_Desc.csv")
    if desc_path.exists():
        columns, indexes = _read_desc_columns(desc_path)
    else:
        columns, indexes = _parquet_columns(parquet_path)
    pf = pq.ParquetFile(parquet_path)
    md = pf.metadata
    return TableInfo(
        name_sql=truncate_table_name(table_sql, max_len=64),
        row_count=int(md.num_rows) if md is not None else None,
        row_count_exact=True,
        data_length=int(parquet_path.stat().st_size),
        columns=columns,
        indexes=indexes,
    )


def _stem_to_table_sql(stem: str, *, base_table: str, mode: str) -> str:
    base_sql = truncate_table_name(_slugify(base_table), max_len=64)
    if mode == "parsed":
        core = re.sub(r"^\d+(?:[._]\d+)?__", "", stem)
        if core.endswith("__MAIN"):
            base = core[: -len("__MAIN")]
            return truncate_table_name(_slugify(base), max_len=64)
        if "__SUB__" in core:
            base, suffix = core.split("__SUB__", 1)
            return truncate_table_name(f"{_slugify(base)}__{_slugify(suffix)}", max_len=64)
        return truncate_table_name(f"{base_sql}__{_slugify(core)}", max_len=64)
    return truncate_table_name(f"{base_sql}__{_slugify(stem)}", max_len=64)


def _generate_folder_schema_viewer(*, job: ErdJob, out_dir: Path) -> dict[str, Any]:
    parquet_paths = sorted(job.source_dir.glob(job.pattern))
    if not parquet_paths:
        raise FileNotFoundError(f"No parquet files matched: {job.source_dir}/{job.pattern}")

    base_sql = truncate_table_name(_slugify(job.base_table), max_len=64)
    table_infos: list[TableInfo] = []
    source_map: list[dict[str, Any]] = []
    has_base = False

    for parquet_path in parquet_paths:
        table_sql = _stem_to_table_sql(parquet_path.stem, base_table=job.base_table, mode=job.mode)
        ti = _table_info_from_parquet(parquet_path, table_sql=table_sql)
        table_infos.append(ti)
        source_map.append(
            {
                "table_sql": ti.name_sql,
                "source_parquet": str(parquet_path),
                "source_desc": str(parquet_path.with_name(f"{parquet_path.stem}_Desc.csv"))
                if parquet_path.with_name(f"{parquet_path.stem}_Desc.csv").exists()
                else "",
            }
        )
        if ti.name_sql == base_sql:
            has_base = True

    if not has_base:
        table_infos.insert(0, TableInfo(name_sql=base_sql, row_count=None, columns=[], indexes=[]))

    table_infos = sorted(table_infos, key=lambda ti: (0 if ti.name_sql == base_sql else 1, ti.name_sql))
    mermaid = render_mermaid(base_table=base_sql, table_infos=table_infos, key_sep="__")
    svg_text = render_simple_svg(base_table=base_sql, table_infos=table_infos, key_sep="__")
    edges = build_table_edges(base_table=base_sql, tables=[ti.name_sql for ti in table_infos], key_sep="__")
    ddls_by_sql = _collect_ddls_by_sql(report=None, table_infos=table_infos)

    payload_tables: list[dict[str, Any]] = []
    total_rows = 0
    total_columns = 0
    total_bytes = 0
    for ti in table_infos:
        row_count = ti.row_count if ti.row_count is not None else 0
        total_rows += int(row_count or 0)
        total_columns += len(ti.columns or [])
        total_bytes += int(ti.data_length or 0)
        payload_tables.append(
            {
                "name_sql": ti.name_sql,
                "name_original": None,
                "display_short": ti.name_sql,
                "display_full": ti.name_sql,
                "role": "base" if ti.name_sql == base_sql else "sub",
                "role_label": "BASE" if ti.name_sql == base_sql else "SUB",
                "depth": 0 if ti.name_sql == base_sql else max(1, ti.name_sql.count("__")),
                "rows_sort": int(row_count or 0),
                "rows_label": ti.rows_label(),
                "column_count": len(ti.columns or []),
                "index_count": len(ti.indexes or []),
                "size_bytes": int(ti.data_length or 0),
                "size_label": f"{int(ti.data_length or 0):,} B" if ti.data_length is not None else "n/a",
                "engine": None,
                "collation": None,
                "columns": list(ti.columns or []),
                "indexes": list(ti.indexes or []),
                "samples": [],
                "sample_count": 0,
                "ddl": ddls_by_sql.get(ti.name_sql) or "",
                "join_sql": "",
                "issue_error_count": 0,
                "issue_warning_count": 0,
                "quarantine_count": 0,
                "search_blob": " ".join(
                    [
                        ti.name_sql,
                        " ".join(str(col.get("name") or "") for col in (ti.columns or [])),
                        " ".join(str(ix.get("index_name") or "") for ix in (ti.indexes or [])),
                    ]
                ).lower(),
            }
        )

    payload = {
        "meta": {
            "generated_at": _utc_now_iso(),
            "config": "",
            "report": "",
            "database": "",
            "db_enabled": False,
            "db_error": None,
            "quarantine": "",
            "quarantine_entries": 0,
            "quarantine_error": None,
            "base_table": base_sql,
            "base_table_sql": base_sql,
            "key_sep": "__",
            "mode": "schema-viewer",
            "source_dir": str(job.source_dir),
            "pattern": job.pattern,
            "bundle_job": job.name,
        },
        "summary": {
            "table_count": len(payload_tables),
            "rows_total": int(total_rows),
            "columns_total": int(total_columns),
            "size_bytes_total": int(total_bytes),
            "flagged_table_count": 0,
            "edge_count": len(edges),
        },
        "tables": payload_tables,
        "groups": [],
        "edges": [{"parent": parent, "child": child, "label": label} for parent, child, label in edges],
        "sources": source_map,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    _write_text(out_dir / "schema.mmd", mermaid)
    _write_text(out_dir / "schema.svg", svg_text)
    _write_text(out_dir / "schema_viewer.json", json.dumps(payload, ensure_ascii=False, indent=2))
    _write_text(
        out_dir / "schema_viewer.html",
        _render_schema_viewer_html(
            title=f"Schema Viewer: {base_sql}",
            base_table=base_sql,
            meta=payload["meta"],
            svg_text=svg_text,
            payload=payload,
        ),
    )
    return {
        "job": job.name,
        "kind": "erd",
        "base_table": base_sql,
        "out_dir": str(out_dir),
        "schema_viewer_html": str(out_dir / "schema_viewer.html"),
        "schema_viewer_json": str(out_dir / "schema_viewer.json"),
        "schema_svg": str(out_dir / "schema.svg"),
        "schema_mmd": str(out_dir / "schema.mmd"),
        "table_count": len(payload_tables),
    }


def _write_preview_config(out_dir: Path, config: dict[str, Any]) -> Path:
    path = out_dir / "preview_config.json"
    _write_text(path, json.dumps(config, ensure_ascii=False, indent=2))
    return path


def _run_preview(*, job: PreviewJob, out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = _write_preview_config(out_dir, config=job.config)
    res = write_review_preview_report(config_path=str(cfg_path), out_dir=str(out_dir), max_records=3, max_nodes=5000, max_union_nodes=20000)
    return {
        "job": job.name,
        "kind": "preview",
        "config": str(cfg_path),
        **res,
    }


def _default_jobs() -> tuple[list[ErdJob], list[PreviewJob]]:
    hdd = Path("/home/kimyoungjin06/Desktop/HDD/Data/Funding")
    erd_jobs = [
        ErdJob(name="nih", base_table="nih_reporter_raw", source_dir=hdd / "US_NIH", pattern="*_raw.parquet", mode="tabular"),
        ErdJob(name="nsf", base_table="nsf_20250524_raw", source_dir=hdd / "US_NSF", pattern="*.parquet", mode="parsed"),
        ErdJob(name="ntis", base_table="ntis_202504_raw", source_dir=hdd / "NTIS" / "202504", pattern="*.parquet", mode="tabular"),
        ErdJob(name="cordis_projects_xml", base_table="eu_cordis_1_projects_xml", source_dir=hdd / "EU_CORDIS", pattern="1_*.parquet", mode="parsed"),
        ErdJob(name="cordis_projects_json", base_table="eu_cordis_projects_json", source_dir=hdd / "EU_CORDIS", pattern="2_*.parquet", mode="tabular"),
        ErdJob(name="cordis_deliverables", base_table="eu_cordis_deliverables", source_dir=hdd / "EU_CORDIS", pattern="3_*.parquet", mode="tabular"),
        ErdJob(name="cordis_publications", base_table="eu_cordis_publications", source_dir=hdd / "EU_CORDIS", pattern="4_*.parquet", mode="tabular"),
        ErdJob(name="cordis_irps", base_table="eu_cordis_irps", source_dir=hdd / "EU_CORDIS", pattern="5_*.parquet", mode="tabular"),
    ]
    preview_jobs = [
        PreviewJob(
            name="nsf_json_preview",
            config={
                "data_config": {
                    "PATH": str(hdd / "US_NSF" / "Data"),
                    "file_name": "2025.zip",
                    "file_type": "zip",
                    "table_name": "nsf_20250524_raw",
                    "KEY_SEP": "__",
                },
                "db_config": {},
            },
        ),
        PreviewJob(
            name="cordis_json_preview",
            config={
                "data_config": {
                    "PATH": str(hdd / "EU_CORDIS" / "2_Projects_json" / "cordis-HORIZONprojects-json"),
                    "file_name": "project.json",
                    "file_type": "json",
                    "table_name": "eu_cordis_projects_json",
                    "KEY_SEP": "__",
                },
                "db_config": {},
            },
        ),
        PreviewJob(
            name="cordis_xml_preview",
            config={
                "data_config": {
                    "PATH": str(hdd / "EU_CORDIS" / "1_Projects_xml"),
                    "file_name": "cordis-HORIZONprojects-xml.zip",
                    "file_type": "zip",
                    "xml_file_name": "project-rcn-238520_en.xml",
                    "table_name": "eu_cordis_1_projects_xml",
                    "KEY_SEP": "__",
                },
                "db_config": {},
            },
        ),
    ]
    return erd_jobs, preview_jobs


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate funding dataset ERD/preview bundles for NIH/NSF/NTIS/CORDIS")
    parser.add_argument("--out-root", default=str(Path("runs") / "funding_review_bundle_20260407"), help="Output root directory")
    args = parser.parse_args()

    out_root = Path(args.out_root)
    out_root.mkdir(parents=True, exist_ok=True)
    erd_jobs, preview_jobs = _default_jobs()

    manifest: dict[str, Any] = {
        "generated_at": _utc_now_iso(),
        "out_root": str(out_root),
        "erd": [],
        "preview": [],
    }

    for job in erd_jobs:
        manifest["erd"].append(_generate_folder_schema_viewer(job=job, out_dir=out_root / job.name / "erd"))

    for job in preview_jobs:
        manifest["preview"].append(_run_preview(job=job, out_dir=out_root / job.name / "preview"))

    manifest_path = out_root / "manifest.json"
    _write_text(manifest_path, json.dumps(manifest, ensure_ascii=False, indent=2))

    lines = [
        "# Funding Review Bundle",
        "",
        f"- generated_at: `{manifest['generated_at']}`",
        f"- out_root: `{out_root}`",
        "",
        "## ERD",
        "",
    ]
    for item in manifest["erd"]:
        lines.append(f"- `{item['job']}` → `{item['schema_viewer_html']}`")
    lines.extend(["", "## Preview", ""])
    for item in manifest["preview"]:
        lines.append(f"- `{item['job']}` → `{item['preview_html']}`")
    lines.append("")
    _write_text(out_root / "README.md", "\n".join(lines))
    print(str(manifest_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
