#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pymysql


TABLE_DOCS: dict[str, dict[str, str]] = {
    "works": {
        "title": "OpenAlex 20260330 works",
        "grain": "One row per OpenAlex work (`id`).",
        "description": (
            "Main work-level flat table for the repaired 2026-03-30 OpenAlex snapshot. "
            "It keeps bibliographic metadata, counts, OA/location fields, primary topic fields, "
            "and selected identifier/APC fields in a single wide row."
        ),
        "notes": (
            "This serving schema is ingest-oriented: most columns are stored as `LONGTEXT`, and "
            "schema drift is preserved in `__extra__` instead of altering table shape during load."
        ),
    },
    "works_topics": {
        "title": "OpenAlex 20260330 works_topics",
        "grain": "One row per work-topic assignment.",
        "description": (
            "Exploded child table from the `topics` array under each work. "
            "Each row links a work id to one topic plus its domain/field/subfield labels and score."
        ),
        "notes": (
            "Use this for topic-level joins or filters without re-parsing nested arrays from the main `works` table."
        ),
    },
    "works_referenced_works": {
        "title": "OpenAlex 20260330 works_referenced_works",
        "grain": "One row per outgoing reference edge from a work to another work id.",
        "description": (
            "Exploded child table from the `referenced_works` array under each work. "
            "Each row records a citation/reference edge using the source work `id` and one referenced OpenAlex work id."
        ),
        "notes": (
            "This is the practical edge list for citation graph work. It is much cheaper to consume than re-reading the nested array from `works`."
        ),
    },
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _human_bytes(value: int | None) -> str:
    if value is None:
        return "n/a"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    n = float(value)
    idx = 0
    while n >= 1024.0 and idx < len(units) - 1:
        n /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(n)} {units[idx]}"
    return f"{n:.1f} {units[idx]}"


def _column_group(column_name: str) -> str:
    if column_name == "__extra__":
        return "extra"
    if "__" in column_name:
        return column_name.split("__", 1)[0]
    if column_name in {"id", "doi", "title", "display_name", "publication_date", "publication_year", "language", "type"}:
        return "core"
    if column_name.endswith("_count") or column_name in {"fwci"}:
        return "metrics"
    return "core"


def _column_hint(table: str, column_name: str) -> str:
    if table == "works":
        hints = {
            "id": "OpenAlex work id (`https://openalex.org/W...`).",
            "doi": "DOI string when present.",
            "title": "Preferred work title.",
            "display_name": "OpenAlex display title.",
            "publication_date": "Publication date as string from source snapshot.",
            "publication_year": "Publication year as string from source snapshot.",
            "language": "Language code.",
            "type": "OpenAlex work type.",
            "authors_count": "Author count for the work.",
            "locations_count": "Location count for the work.",
            "countries_distinct_count": "Distinct country count across locations/authorship context.",
            "institutions_distinct_count": "Distinct institution count.",
            "referenced_works_count": "Number of referenced works.",
            "cited_by_count": "OpenAlex cited-by count.",
            "fwci": "Field-weighted citation impact.",
            "updated_date": "Last updated_date in OpenAlex snapshot.",
            "__extra__": "Schema-drift payload preserved during ingest.",
        }
        return hints.get(column_name, "")
    if table == "works_topics":
        hints = {
            "id": "Source OpenAlex work id.",
            "topics__id": "Topic id for this assignment.",
            "topics__display_name": "Topic label.",
            "topics__score": "Topic score for this work.",
            "topics__domain__id": "Domain id of the topic.",
            "topics__domain__display_name": "Domain label of the topic.",
            "topics__field__id": "Field id of the topic.",
            "topics__field__display_name": "Field label of the topic.",
            "topics__subfield__id": "Subfield id of the topic.",
            "topics__subfield__display_name": "Subfield label of the topic.",
            "__extra__": "Schema-drift payload preserved during ingest.",
        }
        return hints.get(column_name, "")
    if table == "works_referenced_works":
        hints = {
            "id": "Source OpenAlex work id.",
            "referenced_works": "Referenced OpenAlex work id from the source work.",
            "__extra__": "Schema-drift payload preserved during ingest.",
        }
        return hints.get(column_name, "")
    return ""


def _load_reports(report_dir: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if not report_dir.exists():
        return out
    for path in sorted(report_dir.glob("*.json")):
        out[path.stem] = _read_json(path)
    return out


def _load_serving_specs(serving_manifest_path: Path) -> list[dict[str, Any]]:
    payload = _read_json(serving_manifest_path)
    specs = payload.get("specs") or []
    return [spec for spec in specs if isinstance(spec, dict)]


def _connect(args: argparse.Namespace):
    return pymysql.connect(
        host=args.host,
        port=int(args.port),
        user=args.user,
        password=args.password,
        database=args.database,
        charset="utf8mb4",
        autocommit=True,
    )


def _fetch_table_create(cur, table_name: str) -> str:
    cur.execute(f"SHOW CREATE TABLE `{table_name}`")
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"SHOW CREATE TABLE returned no row for {table_name}")
    return str(row[1])


def _fetch_columns(cur, database: str, table_name: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT column_name, column_type, is_nullable, column_key, extra, ordinal_position
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        ORDER BY ordinal_position
        """,
        (database, table_name),
    )
    out = []
    for row in cur.fetchall():
        out.append(
            {
                "column_name": str(row[0]),
                "column_type": str(row[1]),
                "is_nullable": str(row[2]),
                "column_key": str(row[3] or ""),
                "extra": str(row[4] or ""),
                "ordinal_position": int(row[5]),
            }
        )
    return out


def _fetch_table_stats(cur, database: str, table_name: str) -> dict[str, Any]:
    cur.execute(
        """
        SELECT table_rows, data_length, index_length, engine, table_collation
        FROM information_schema.tables
        WHERE table_schema=%s AND table_name=%s
        """,
        (database, table_name),
    )
    row = cur.fetchone()
    if not row:
        return {}
    return {
        "table_rows_estimate": int(row[0] or 0),
        "data_length": int(row[1] or 0),
        "index_length": int(row[2] or 0),
        "engine": str(row[3] or ""),
        "collation": str(row[4] or ""),
    }


def _fetch_all_table_stats(cur, database: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT table_name, table_rows, data_length, index_length, engine, table_collation
        FROM information_schema.tables
        WHERE table_schema=%s
        ORDER BY table_name
        """,
        (database,),
    )
    out: list[dict[str, Any]] = []
    for row in cur.fetchall():
        out.append(
            {
                "table_name": str(row[0]),
                "table_rows_estimate": int(row[1] or 0),
                "data_length": int(row[2] or 0),
                "index_length": int(row[3] or 0),
                "engine": str(row[4] or ""),
                "collation": str(row[5] or ""),
            }
        )
    return out


def _write_columns_csv(path: Path, *, table_name: str, columns: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=[
                "table_name",
                "ordinal_position",
                "column_name",
                "column_group",
                "column_type",
                "is_nullable",
                "column_key",
                "extra",
                "hint",
            ],
        )
        writer.writeheader()
        for col in columns:
            writer.writerow(
                {
                    "table_name": table_name,
                    "ordinal_position": col["ordinal_position"],
                    "column_name": col["column_name"],
                    "column_group": _column_group(col["column_name"]),
                    "column_type": col["column_type"],
                    "is_nullable": col["is_nullable"],
                    "column_key": col["column_key"],
                    "extra": col["extra"],
                    "hint": _column_hint(table_name, col["column_name"]),
                }
            )


def _render_table_md(
    *,
    table_name: str,
    source_table: str,
    source_dir: str,
    create_sql: str,
    stats: dict[str, Any],
    report: dict[str, Any] | None,
    columns: list[dict[str, Any]],
) -> str:
    doc = TABLE_DOCS.get(table_name, {})
    lines: list[str] = [
        f"# {doc.get('title', table_name)}",
        "",
        f"- DB table: `{table_name}`",
        f"- Canonical source table/folder: `{source_table}`",
        f"- Source parquet dir: `{source_dir}`",
        f"- Grain: {doc.get('grain', 'n/a')}",
        "",
        doc.get("description", ""),
        "",
    ]
    if doc.get("notes"):
        lines.extend(["## Notes", "", doc["notes"], ""])

    lines.extend(
        [
            "## Load Summary",
            "",
            f"- Rows loaded (report exact): `{((report or {}).get('stats') or {}).get('rows_loaded', 'n/a')}`",
            f"- Parquet rows read: `{((report or {}).get('stats') or {}).get('parquet_rows_read', 'n/a')}`",
            f"- Parquet files read: `{((report or {}).get('stats') or {}).get('parquet_files_read', 'n/a')}`",
            f"- Files loaded: `{((report or {}).get('stats') or {}).get('files_loaded', 'n/a')}`",
            f"- Duration: `{report.get('duration_s') if report else 'n/a'}` seconds",
            f"- Estimated DB rows (`information_schema`): `{stats.get('table_rows_estimate', 'n/a')}`",
            f"- Estimated data length: `{_human_bytes(stats.get('data_length'))}`",
            f"- Estimated index length: `{_human_bytes(stats.get('index_length'))}`",
            "",
            "## Column Summary",
            "",
            "| # | Column | Group | Type | Nullable | Key | Hint |",
            "|---:|---|---|---|---|---|---|",
        ]
    )
    for col in columns:
        lines.append(
            f"| {col['ordinal_position']} | `{col['column_name']}` | `{_column_group(col['column_name'])}` | "
            f"`{col['column_type']}` | `{col['is_nullable']}` | `{col['column_key'] or ''}` | "
            f"{_column_hint(table_name, col['column_name'])} |"
        )
    lines.extend(["", "## RDB DDL", "", "```sql", create_sql.rstrip(), "```", ""])
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Build a documentation bundle for selected OpenAlex serving tables.")
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--database", required=True)
    ap.add_argument("--report-dir", required=True)
    ap.add_argument("--serving-manifest", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument(
        "--tables",
        nargs="+",
        default=["works", "works_topics", "works_referenced_works"],
    )
    ap.add_argument("--integrated-report-pdf", default="output/pdf/openalex_20260330_integrated_report/openalex_20260330_integrated_report.pdf")
    args = ap.parse_args()

    out_dir = Path(args.out).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    reports = _load_reports(Path(args.report_dir).expanduser().resolve())
    specs = _load_serving_specs(Path(args.serving_manifest).expanduser().resolve())
    spec_by_target = {str(spec.get("target_table")): spec for spec in specs}

    schema_sql_chunks: list[str] = []
    overview_rows: list[dict[str, Any]] = []
    summary: dict[str, Any] = {
        "generated_at": _utc_now_iso(),
        "database": args.database,
        "selected_tables": list(args.tables),
        "integrated_report_pdf": str(Path(args.integrated_report_pdf).expanduser().resolve()),
        "tables": {},
        "serving_manifest": str(Path(args.serving_manifest).expanduser().resolve()),
    }

    with _connect(args) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema=%s
                """,
                (args.database,),
            )
            total_tables_in_db = int(cur.fetchone()[0] or 0)
            summary["tables_in_db_now"] = total_tables_in_db
            all_db_tables = _fetch_all_table_stats(cur, args.database)
            summary["all_db_tables"] = all_db_tables

            full_schema_chunks: list[str] = []
            for row in all_db_tables:
                full_schema_chunks.append(f"-- {row['table_name']}\n{_fetch_table_create(cur, row['table_name'])};\n")
            _write_text(out_dir / "current_db_schema.sql", "\n".join(full_schema_chunks))

            for table_name in args.tables:
                spec = spec_by_target.get(table_name, {})
                create_sql = _fetch_table_create(cur, table_name)
                columns = _fetch_columns(cur, args.database, table_name)
                stats = _fetch_table_stats(cur, args.database, table_name)
                report = reports.get(table_name)
                source_dir = str(spec.get("source_dir") or "")

                table_dir = out_dir / "tables"
                md_path = table_dir / f"{table_name}.md"
                csv_path = table_dir / f"{table_name}_columns.csv"
                _write_columns_csv(csv_path, table_name=table_name, columns=columns)
                _write_text(
                    md_path,
                    _render_table_md(
                        table_name=table_name,
                        source_table=str(spec.get("source_table") or ""),
                        source_dir=source_dir,
                        create_sql=create_sql,
                        stats=stats,
                        report=report,
                        columns=columns,
                    ),
                )

                schema_sql_chunks.append(f"-- {table_name}\n{create_sql};\n")
                summary["tables"][table_name] = {
                    "source_dir": source_dir,
                    "report_path": str((Path(args.report_dir) / f"{table_name}.json").resolve()) if report else None,
                    "row_count_exact_from_report": ((report or {}).get("stats") or {}).get("rows_loaded"),
                    "parquet_files_read": ((report or {}).get("stats") or {}).get("parquet_files_read"),
                    "db_table_rows_estimate": stats.get("table_rows_estimate"),
                    "data_length": stats.get("data_length"),
                    "index_length": stats.get("index_length"),
                    "column_count": len(columns),
                    "columns_csv": str(csv_path),
                    "table_doc": str(md_path),
                }
                overview_rows.append(
                    {
                        "target_table": table_name,
                        "source_table": str(spec.get("source_table") or ""),
                        "source_dir": source_dir,
                        "generated": bool(spec.get("generated")),
                        "stage_writer": str(spec.get("stage_writer") or ""),
                        "file_chunk_rows": int(spec.get("file_chunk_rows") or 0),
                        "row_count_exact_from_report": ((report or {}).get("stats") or {}).get("rows_loaded"),
                        "db_table_rows_estimate": stats.get("table_rows_estimate"),
                        "data_length_bytes": stats.get("data_length"),
                        "index_length_bytes": stats.get("index_length"),
                    }
                )

    _write_text(out_dir / "schema.sql", "\n".join(schema_sql_chunks))
    _write_json(out_dir / "bundle_summary.json", summary)

    with (out_dir / "serving_table_overview.csv").open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=[
                "target_table",
                "source_table",
                "source_dir",
                "generated",
                "stage_writer",
                "file_chunk_rows",
                "row_count_exact_from_report",
                "db_table_rows_estimate",
                "data_length_bytes",
                "index_length_bytes",
            ],
        )
        writer.writeheader()
        for row in overview_rows:
            writer.writerow(row)

    with (out_dir / "current_db_tables.csv").open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=[
                "table_name",
                "table_rows_estimate",
                "data_length",
                "index_length",
                "engine",
                "collation",
            ],
        )
        writer.writeheader()
        for row in summary["all_db_tables"]:
            writer.writerow(row)

    with (out_dir / "planned_serving_tables.csv").open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=[
                "target_table",
                "source_table",
                "source_dir",
                "generated",
                "selected",
                "stage_writer",
                "file_chunk_rows",
                "report_exists",
            ],
        )
        writer.writeheader()
        for spec in specs:
            target_table = str(spec.get("target_table") or "")
            writer.writerow(
                {
                    "target_table": target_table,
                    "source_table": str(spec.get("source_table") or ""),
                    "source_dir": str(spec.get("source_dir") or ""),
                    "generated": bool(spec.get("generated")),
                    "selected": bool(spec.get("selected")),
                    "stage_writer": str(spec.get("stage_writer") or ""),
                    "file_chunk_rows": int(spec.get("file_chunk_rows") or 0),
                    "report_exists": (Path(args.report_dir) / f"{target_table}.json").exists(),
                }
            )

    integrated_pdf = Path(args.integrated_report_pdf).expanduser().resolve()
    copied_pdf = None
    if integrated_pdf.exists():
        copied_pdf = out_dir / integrated_pdf.name
        shutil.copy2(integrated_pdf, copied_pdf)

    readme_lines = [
        "# OpenAlex 20260330 Export Bundle",
        "",
        "This bundle is intended to travel with the copied parquet directories for:",
        "",
        "- `works`",
        "- `works_topics`",
        "- `works_referenced_works`",
        "",
        "## Final 20260330 lineage",
        "",
        "- Base raw snapshot used for the 0330 works merge: `openalex-snapshot(20260225)`",
        "- Parsed base parquet used for merge: `openalex_works_20260225_raw_20260331_031932`",
        "- 0330 delta parquet source: `openalex_works_20260330_delta_20260407_212041`",
        "- Final repaired canonical works snapshot: `openalex_works_20260330_repairreplay_20260410_190630`",
        "",
        "## Bundle contents",
        "",
        "- `schema.sql`: current RDB DDL for the selected tables",
        "- `current_db_schema.sql`: current RDB DDL for all tables loaded so far into `openalex_20260330_raw_yjk`",
        "- `current_db_tables.csv`: current DB table inventory and estimated storage",
        "- `planned_serving_tables.csv`: planned 0330 serving tables from the serving manifest",
        "- `serving_table_overview.csv`: source-to-serving mapping and row/storage summary",
        "- `bundle_summary.json`: machine-readable summary",
        "- `tables/*.md`: per-table description docs",
        "- `tables/*_columns.csv`: per-table column inventory",
    ]
    if copied_pdf is not None:
        readme_lines.append(f"- `{copied_pdf.name}`: integrated 20260330 lineage/change report PDF")
    readme_lines.extend(
        [
            "",
            "## Serving schema notes",
            "",
            "- The serving DB is `openalex_20260330_raw_yjk`.",
            "- The ingest strategy is wide and tolerant: most columns are `LONGTEXT`.",
            "- `__extra__` captures schema drift instead of forcing online table alteration during bulk load.",
            "- Exact rows for the selected tables come from per-table load reports in the rebuild run.",
            "",
            "## Selected tables",
            "",
            "| DB table | Canonical source folder | Row grain | Exact rows from load report |",
            "|---|---|---|---:|",
        ]
    )
    for table_name in args.tables:
        info = summary["tables"][table_name]
        grain = TABLE_DOCS.get(table_name, {}).get("grain", "")
        source_table = ""
        for row in overview_rows:
            if row["target_table"] == table_name:
                source_table = str(row["source_table"] or "")
                break
        readme_lines.append(
            f"| `{table_name}` | `{source_table}` | {grain} | {info.get('row_count_exact_from_report', 'n/a')} |"
        )
    readme_lines.append("")
    _write_text(out_dir / "README.md", "\n".join(readme_lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
