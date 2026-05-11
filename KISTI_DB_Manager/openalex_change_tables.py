from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from KISTI_DB_Manager.openalex_change_report import _connect_duckdb, _log, _read_parquet_expr
from KISTI_DB_Manager.runstate import JsonRunState, atomic_write_json, utc_now_iso


def _write_query_to_dataset(
    *,
    con,
    sql: str,
    out_dir: Path,
    basename_template: str,
    max_rows_per_file: int,
) -> int:
    import pyarrow.dataset as ds

    out_dir.mkdir(parents=True, exist_ok=True)
    for child in out_dir.glob("*.parquet"):
        child.unlink(missing_ok=True)
    reader = con.execute(sql).to_arrow_reader()
    ds.write_dataset(
        reader,
        base_dir=str(out_dir),
        format="parquet",
        existing_data_behavior="overwrite_or_ignore",
        basename_template=basename_template,
        max_rows_per_file=max_rows_per_file,
        max_rows_per_group=min(max_rows_per_file, 100_000),
    )
    return sum(1 for _ in out_dir.glob("*.parquet"))


def build_openalex_change_tables(
    *,
    base_root: Path,
    final_root: Path,
    delta_ids_parquet: Path,
    run_dir: Path,
    base_prefix: str,
    final_prefix: str,
    threads: int = 8,
    temp_dir: Path | None = None,
    max_rows_per_file: int = 1_000_000,
) -> dict[str, Any]:
    run_dir.mkdir(parents=True, exist_ok=True)
    diff_root = run_dir / "diff_tables"
    diff_root.mkdir(parents=True, exist_ok=True)
    db_path = run_dir / "change_tables.duckdb"
    log_path = run_dir / "change_tables.log"
    summary_json = run_dir / "change_tables_summary.json"
    summary_csv = run_dir / "change_tables_summary.csv"
    progress_path = run_dir / "progress.json"

    base_main_dir = base_root / base_prefix
    final_main_dir = final_root / final_prefix
    base_abs_dir = base_root / f"{base_prefix}__excepted__abstract_inverted_index"
    final_abs_dir = final_root / f"{final_prefix}__excepted__abstract_inverted_index"
    base_auth_dir = base_root / f"{base_prefix}__authorships"
    final_auth_dir = final_root / f"{final_prefix}__authorships"

    progress = JsonRunState.create(
        progress_path,
        {
        "status": "running",
        "generated_at": utc_now_iso(),
        "base_root": str(base_root),
        "final_root": str(final_root),
        "delta_ids_parquet": str(delta_ids_parquet),
        "steps": {
            "new_work_ids": "pending",
            "updated_main_fields": "pending",
            "abstract_changes": "pending",
            "authorship_changes": "pending",
            "summary": "pending",
        },
    },
    )

    queries: dict[str, str] = {
        "new_work_ids": """
            SELECT
              f.id,
              f.title,
              f.publication_year,
              f.publication_date,
              f.type,
              f.language,
              f.updated_date
            FROM final_main f
            ANTI JOIN base_main b USING (id)
            SEMI JOIN delta_ids d USING (id)
        """,
        "updated_main_fields": """
            SELECT
              b.id,
              b.title AS base_title,
              f.title AS final_title,
              b.doi AS base_doi,
              f.doi AS final_doi,
              b.publication_date AS base_publication_date,
              f.publication_date AS final_publication_date,
              b.publication_year AS base_publication_year,
              f.publication_year AS final_publication_year,
              b.language AS base_language,
              f.language AS final_language,
              b.type AS base_type,
              f.type AS final_type,
              b.authors_count AS base_authors_count,
              f.authors_count AS final_authors_count,
              b.institutions_distinct_count AS base_institutions_distinct_count,
              f.institutions_distinct_count AS final_institutions_distinct_count,
              b.countries_distinct_count AS base_countries_distinct_count,
              f.countries_distinct_count AS final_countries_distinct_count,
              b.locations_count AS base_locations_count,
              f.locations_count AS final_locations_count,
              b.referenced_works_count AS base_referenced_works_count,
              f.referenced_works_count AS final_referenced_works_count,
              b.cited_by_count AS base_cited_by_count,
              f.cited_by_count AS final_cited_by_count,
              b.fwci AS base_fwci,
              f.fwci AS final_fwci,
              b.has_fulltext AS base_has_fulltext,
              f.has_fulltext AS final_has_fulltext,
              b.updated_date AS base_updated_date,
              f.updated_date AS final_updated_date,
              b.title IS DISTINCT FROM f.title AS title_changed,
              b.doi IS DISTINCT FROM f.doi AS doi_changed,
              b.publication_date IS DISTINCT FROM f.publication_date AS publication_date_changed,
              b.publication_year IS DISTINCT FROM f.publication_year AS publication_year_changed,
              b.language IS DISTINCT FROM f.language AS language_changed,
              b.type IS DISTINCT FROM f.type AS type_changed,
              b.authors_count IS DISTINCT FROM f.authors_count AS authors_count_changed,
              b.institutions_distinct_count IS DISTINCT FROM f.institutions_distinct_count AS institutions_distinct_count_changed,
              b.countries_distinct_count IS DISTINCT FROM f.countries_distinct_count AS countries_distinct_count_changed,
              b.locations_count IS DISTINCT FROM f.locations_count AS locations_count_changed,
              b.referenced_works_count IS DISTINCT FROM f.referenced_works_count AS referenced_works_count_changed,
              b.cited_by_count IS DISTINCT FROM f.cited_by_count AS cited_by_count_changed,
              b.fwci IS DISTINCT FROM f.fwci AS fwci_changed,
              b.has_fulltext IS DISTINCT FROM f.has_fulltext AS has_fulltext_changed
            FROM base_main b
            JOIN final_main f USING (id)
            SEMI JOIN overlap_ids o USING (id)
            WHERE
              b.title IS DISTINCT FROM f.title
              OR b.doi IS DISTINCT FROM f.doi
              OR b.publication_date IS DISTINCT FROM f.publication_date
              OR b.publication_year IS DISTINCT FROM f.publication_year
              OR b.language IS DISTINCT FROM f.language
              OR b.type IS DISTINCT FROM f.type
              OR b.authors_count IS DISTINCT FROM f.authors_count
              OR b.institutions_distinct_count IS DISTINCT FROM f.institutions_distinct_count
              OR b.countries_distinct_count IS DISTINCT FROM f.countries_distinct_count
              OR b.locations_count IS DISTINCT FROM f.locations_count
              OR b.referenced_works_count IS DISTINCT FROM f.referenced_works_count
              OR b.cited_by_count IS DISTINCT FROM f.cited_by_count
              OR b.fwci IS DISTINCT FROM f.fwci
              OR b.has_fulltext IS DISTINCT FROM f.has_fulltext
        """,
        "abstract_changes": """
            SELECT
              o.id,
              CASE
                WHEN COALESCE(b.value, '') = '' AND COALESCE(f.value, '') <> '' THEN 'added'
                WHEN COALESCE(b.value, '') <> '' AND COALESCE(f.value, '') = '' THEN 'removed'
                WHEN COALESCE(b.value, '') <> '' AND COALESCE(f.value, '') <> '' AND b.value IS DISTINCT FROM f.value THEN 'changed'
                ELSE 'unchanged'
              END AS change_type,
              COALESCE(b.value, '') <> '' AS base_has_payload,
              COALESCE(f.value, '') <> '' AS final_has_payload,
              LENGTH(COALESCE(b.value, '')) AS base_payload_bytes,
              LENGTH(COALESCE(f.value, '')) AS final_payload_bytes,
              md5(COALESCE(b.value, '')) AS base_payload_md5,
              md5(COALESCE(f.value, '')) AS final_payload_md5
            FROM overlap_ids o
            LEFT JOIN base_abs b USING (id)
            LEFT JOIN final_abs f USING (id)
            WHERE
              (COALESCE(b.value, '') = '' AND COALESCE(f.value, '') <> '')
              OR (COALESCE(b.value, '') <> '' AND COALESCE(f.value, '') = '')
              OR (COALESCE(b.value, '') <> '' AND COALESCE(f.value, '') <> '' AND b.value IS DISTINCT FROM f.value)
        """,
        "authorship_changes": """
            WITH base_counts AS (
              SELECT id, COUNT(*) AS base_authorship_rows
              FROM __BASE_AUTH__
              SEMI JOIN overlap_ids USING (id)
              GROUP BY 1
            ),
            final_counts AS (
              SELECT id, COUNT(*) AS final_authorship_rows
              FROM __FINAL_AUTH__
              SEMI JOIN overlap_ids USING (id)
              GROUP BY 1
            )
            SELECT
              o.id,
              COALESCE(b.base_authorship_rows, 0) AS base_authorship_rows,
              COALESCE(f.final_authorship_rows, 0) AS final_authorship_rows,
              COALESCE(f.final_authorship_rows, 0) - COALESCE(b.base_authorship_rows, 0) AS authorship_row_delta,
              CASE
                WHEN COALESCE(b.base_authorship_rows, 0) = 0 AND COALESCE(f.final_authorship_rows, 0) > 0 THEN 'added_from_zero'
                WHEN COALESCE(f.final_authorship_rows, 0) > COALESCE(b.base_authorship_rows, 0) THEN 'increased'
                WHEN COALESCE(f.final_authorship_rows, 0) < COALESCE(b.base_authorship_rows, 0) THEN 'decreased'
                ELSE 'unchanged'
              END AS change_type
            FROM overlap_ids o
            LEFT JOIN base_counts b USING (id)
            LEFT JOIN final_counts f USING (id)
            WHERE COALESCE(b.base_authorship_rows, 0) IS DISTINCT FROM COALESCE(f.final_authorship_rows, 0)
        """,
    }

    summary_rows: list[dict[str, Any]] = []

    with log_path.open("a", encoding="utf-8") as log_fp:
        _log(log_fp, f"base_root={base_root}")
        _log(log_fp, f"final_root={final_root}")
        _log(log_fp, f"delta_ids_parquet={delta_ids_parquet}")
        con = _connect_duckdb(db_path=db_path, temp_dir=temp_dir, threads=threads)
        try:
            con.execute(
                "CREATE OR REPLACE VIEW delta_ids AS "
                f"SELECT id FROM read_parquet({json.dumps(str(delta_ids_parquet))});"
            )
            con.execute("CREATE OR REPLACE VIEW base_main AS " f"SELECT * FROM {_read_parquet_expr(base_main_dir)};")
            con.execute("CREATE OR REPLACE VIEW final_main AS " f"SELECT * FROM {_read_parquet_expr(final_main_dir)};")
            con.execute("CREATE OR REPLACE VIEW overlap_ids AS SELECT b.id FROM base_main b SEMI JOIN delta_ids d USING (id);")
            con.execute("CREATE OR REPLACE VIEW base_abs AS " f"SELECT id, value FROM {_read_parquet_expr(base_abs_dir)};")
            con.execute("CREATE OR REPLACE VIEW final_abs AS " f"SELECT id, value FROM {_read_parquet_expr(final_abs_dir)};")

            for name, sql in queries.items():
                if name == "authorship_changes":
                    sql = sql.replace("__BASE_AUTH__", _read_parquet_expr(base_auth_dir)).replace(
                        "__FINAL_AUTH__", _read_parquet_expr(final_auth_dir)
                    )
                out_dir = diff_root / name
                _log(log_fp, f"start {name}")
                file_count = _write_query_to_dataset(
                    con=con,
                    sql=sql,
                    out_dir=out_dir,
                    basename_template=f"{name}-{{i}}.parquet",
                    max_rows_per_file=max_rows_per_file,
                )
                con.execute(
                    f"CREATE OR REPLACE VIEW {name} AS "
                    f"SELECT * FROM read_parquet({json.dumps(str(out_dir / '*.parquet'))}, union_by_name=true);"
                )
                row_count = int(con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0])
                summary_rows.append(
                    {
                        "table_name": name,
                        "row_count": row_count,
                        "parquet_files": file_count,
                        "out_dir": str(out_dir),
                    }
                )
                progress.payload["steps"][name] = "done"
                progress.update(**{name: {"row_count": row_count, "parquet_files": file_count, "out_dir": str(out_dir)}})
                _log(log_fp, f"done {name}: rows={row_count}, files={file_count}")

            with summary_csv.open("w", encoding="utf-8", newline="") as fp:
                writer = csv.DictWriter(fp, fieldnames=["table_name", "row_count", "parquet_files", "out_dir"])
                writer.writeheader()
                writer.writerows(summary_rows)
            summary = {
                "generated_at": utc_now_iso(),
                "base_root": str(base_root),
                "final_root": str(final_root),
                "delta_ids_parquet": str(delta_ids_parquet),
                "diff_root": str(diff_root),
                "tables": summary_rows,
            }
            atomic_write_json(summary_json, summary)
            progress.payload["steps"]["summary"] = "done"
            progress.set_status("done")
            _log(log_fp, "summary done")
            return summary
        finally:
            con.close()
