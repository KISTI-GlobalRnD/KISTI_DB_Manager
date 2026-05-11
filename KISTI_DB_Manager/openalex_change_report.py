from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from KISTI_DB_Manager.runstate import JsonRunState, atomic_write_json, utc_now_iso

def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _log(fp, message: str) -> None:
    line = f"[{datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')}] {message}"
    fp.write(line + "\n")
    fp.flush()


def _read_parquet_expr(path: Path) -> str:
    return f"read_parquet({json.dumps(str(path / '*.parquet'))}, union_by_name=true)"


def _connect_duckdb(*, db_path: Path, temp_dir: Path | None, threads: int):
    import duckdb

    con = duckdb.connect(str(db_path))
    con.execute(f"PRAGMA threads={int(max(1, threads))};")
    chosen_temp = temp_dir
    if chosen_temp is not None:
        try:
            chosen_temp.mkdir(parents=True, exist_ok=True)
        except Exception:
            chosen_temp = db_path.parent / "_duckdb_tmp"
            chosen_temp.mkdir(parents=True, exist_ok=True)
        con.execute(f"PRAGMA temp_directory={json.dumps(str(chosen_temp))};")
    return con


def _query_one(con, sql: str) -> dict[str, Any]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    row = cur.fetchone()
    if row is None:
        return {}
    return {cols[i]: row[i] for i in range(len(cols))}


def _query_rows(con, sql: str) -> list[dict[str, Any]]:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return [{cols[i]: row[i] for i in range(len(cols))} for row in rows]


def build_openalex_change_report(
    *,
    base_root: Path,
    final_root: Path,
    delta_ids_parquet: Path,
    run_dir: Path,
    base_prefix: str,
    final_prefix: str,
    threads: int = 8,
    temp_dir: Path | None = None,
) -> dict[str, Any]:
    run_dir.mkdir(parents=True, exist_ok=True)
    db_path = run_dir / "change_report.duckdb"
    log_path = run_dir / "change_report.log"
    json_path = run_dir / "change_report.json"
    md_path = run_dir / "change_report.md"
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
            "profile": "pending",
            "main_field_counts": "pending",
            "abstract_counts": "pending",
            "authorship_counts": "pending",
            "samples": "pending",
        },
    },
    )

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
            con.execute(
                "CREATE OR REPLACE VIEW base_main AS "
                f"SELECT * FROM {_read_parquet_expr(base_main_dir)};"
            )
            con.execute(
                "CREATE OR REPLACE VIEW final_main AS "
                f"SELECT * FROM {_read_parquet_expr(final_main_dir)};"
            )
            con.execute(
                "CREATE OR REPLACE VIEW overlap_ids AS "
                "SELECT b.id FROM base_main b SEMI JOIN delta_ids d USING (id);"
            )
            con.execute(
                "CREATE OR REPLACE VIEW base_abs AS "
                f"SELECT id, value FROM {_read_parquet_expr(base_abs_dir)};"
            )
            con.execute(
                "CREATE OR REPLACE VIEW final_abs AS "
                f"SELECT id, value FROM {_read_parquet_expr(final_abs_dir)};"
            )
            con.execute(
                "CREATE OR REPLACE VIEW base_auth AS "
                f"SELECT id FROM {_read_parquet_expr(base_auth_dir)};"
            )
            con.execute(
                "CREATE OR REPLACE VIEW final_auth AS "
                f"SELECT id FROM {_read_parquet_expr(final_auth_dir)};"
            )
            _log(log_fp, "views ready")

            profile = _query_one(
                con,
                """
                SELECT
                  (SELECT COUNT(*) FROM delta_ids) AS distinct_delta_ids,
                  (SELECT COUNT(*) FROM overlap_ids) AS overlap_existing_ids,
                  (SELECT COUNT(*) FROM delta_ids) - (SELECT COUNT(*) FROM overlap_ids) AS new_ids
                """,
            )
            _log(log_fp, f"profile={profile}")
            progress.payload["steps"]["profile"] = "done"
            progress.update(profile=profile)

            main_field_counts = _query_one(
                con,
                """
                SELECT
                  COUNT(*) AS compared_rows,
                  COUNT(*) FILTER (WHERE b.title IS DISTINCT FROM f.title) AS title_changed,
                  COUNT(*) FILTER (WHERE b.display_name IS DISTINCT FROM f.display_name) AS display_name_changed,
                  COUNT(*) FILTER (WHERE b.doi IS DISTINCT FROM f.doi) AS doi_changed,
                  COUNT(*) FILTER (WHERE b.publication_date IS DISTINCT FROM f.publication_date) AS publication_date_changed,
                  COUNT(*) FILTER (WHERE b.publication_year IS DISTINCT FROM f.publication_year) AS publication_year_changed,
                  COUNT(*) FILTER (WHERE b.language IS DISTINCT FROM f.language) AS language_changed,
                  COUNT(*) FILTER (WHERE b.type IS DISTINCT FROM f.type) AS type_changed,
                  COUNT(*) FILTER (WHERE b.authors_count IS DISTINCT FROM f.authors_count) AS authors_count_changed,
                  COUNT(*) FILTER (WHERE b.institutions_distinct_count IS DISTINCT FROM f.institutions_distinct_count) AS institutions_distinct_count_changed,
                  COUNT(*) FILTER (WHERE b.countries_distinct_count IS DISTINCT FROM f.countries_distinct_count) AS countries_distinct_count_changed,
                  COUNT(*) FILTER (WHERE b.locations_count IS DISTINCT FROM f.locations_count) AS locations_count_changed,
                  COUNT(*) FILTER (WHERE b.referenced_works_count IS DISTINCT FROM f.referenced_works_count) AS referenced_works_count_changed,
                  COUNT(*) FILTER (WHERE b.cited_by_count IS DISTINCT FROM f.cited_by_count) AS cited_by_count_changed,
                  COUNT(*) FILTER (WHERE b.fwci IS DISTINCT FROM f.fwci) AS fwci_changed,
                  COUNT(*) FILTER (WHERE b.has_fulltext IS DISTINCT FROM f.has_fulltext) AS has_fulltext_changed,
                  COUNT(*) FILTER (WHERE b.updated_date IS DISTINCT FROM f.updated_date) AS updated_date_changed
                FROM base_main b
                JOIN final_main f USING (id)
                SEMI JOIN overlap_ids o USING (id)
                """,
            )
            _log(log_fp, "main field counts done")
            progress.payload["steps"]["main_field_counts"] = "done"
            progress.update(main_field_counts=main_field_counts)

            abstract_counts = _query_one(
                con,
                """
                WITH pairs AS (
                  SELECT
                    o.id,
                    b.value AS base_value,
                    f.value AS final_value
                  FROM overlap_ids o
                  LEFT JOIN base_abs b USING (id)
                  LEFT JOIN final_abs f USING (id)
                )
                SELECT
                  COUNT(*) AS overlap_ids_checked,
                  COUNT(*) FILTER (
                    WHERE COALESCE(base_value, '') <> '' AND COALESCE(final_value, '') <> ''
                  ) AS both_have_payload,
                  COUNT(*) FILTER (
                    WHERE COALESCE(base_value, '') = '' AND COALESCE(final_value, '') <> ''
                  ) AS abstract_added,
                  COUNT(*) FILTER (
                    WHERE COALESCE(base_value, '') <> '' AND COALESCE(final_value, '') = ''
                  ) AS abstract_removed,
                  COUNT(*) FILTER (
                    WHERE COALESCE(base_value, '') <> '' AND COALESCE(final_value, '') <> ''
                      AND base_value IS DISTINCT FROM final_value
                  ) AS abstract_payload_changed
                FROM pairs
                """,
            )
            _log(log_fp, "abstract counts done")
            progress.payload["steps"]["abstract_counts"] = "done"
            progress.update(abstract_counts=abstract_counts)

            authorship_counts = _query_one(
                con,
                """
                WITH base_counts AS (
                  SELECT id, COUNT(*) AS n
                  FROM base_auth
                  SEMI JOIN overlap_ids USING (id)
                  GROUP BY 1
                ),
                final_counts AS (
                  SELECT id, COUNT(*) AS n
                  FROM final_auth
                  SEMI JOIN overlap_ids USING (id)
                  GROUP BY 1
                )
                SELECT
                  COUNT(*) AS overlap_ids_checked,
                  COUNT(*) FILTER (WHERE COALESCE(b.n, 0) IS DISTINCT FROM COALESCE(f.n, 0)) AS authorship_rowcount_changed,
                  COUNT(*) FILTER (WHERE COALESCE(b.n, 0) = 0 AND COALESCE(f.n, 0) > 0) AS authorship_added_from_zero,
                  COUNT(*) FILTER (WHERE COALESCE(b.n, 0) > 0 AND COALESCE(f.n, 0) = 0) AS authorship_removed_to_zero
                FROM overlap_ids o
                LEFT JOIN base_counts b USING (id)
                LEFT JOIN final_counts f USING (id)
                """,
            )
            _log(log_fp, "authorship counts done")
            progress.payload["steps"]["authorship_counts"] = "done"
            progress.update(authorship_counts=authorship_counts)

            samples = {
                "new_ids": _query_rows(
                    con,
                    """
                    SELECT f.id, f.title, f.publication_year, f.updated_date
                    FROM final_main f
                    ANTI JOIN base_main b USING (id)
                    SEMI JOIN delta_ids d USING (id)
                    ORDER BY f.updated_date DESC NULLS LAST, f.id
                    LIMIT 10
                    """,
                ),
                "title_changed": _query_rows(
                    con,
                    """
                    SELECT b.id, b.title AS base_title, f.title AS final_title, b.updated_date AS base_updated_date, f.updated_date AS final_updated_date
                    FROM base_main b
                    JOIN final_main f USING (id)
                    SEMI JOIN overlap_ids o USING (id)
                    WHERE b.title IS DISTINCT FROM f.title
                    ORDER BY f.updated_date DESC NULLS LAST, b.id
                    LIMIT 10
                    """,
                ),
                "abstract_changed": _query_rows(
                    con,
                    """
                    SELECT o.id, b.value AS base_value, f.value AS final_value
                    FROM overlap_ids o
                    LEFT JOIN base_abs b USING (id)
                    LEFT JOIN final_abs f USING (id)
                    WHERE COALESCE(b.value, '') <> '' AND COALESCE(f.value, '') <> ''
                      AND b.value IS DISTINCT FROM f.value
                    LIMIT 10
                    """,
                ),
                "institutions_distinct_count_changed": _query_rows(
                    con,
                    """
                    SELECT b.id, b.title, b.institutions_distinct_count AS base_count, f.institutions_distinct_count AS final_count
                    FROM base_main b
                    JOIN final_main f USING (id)
                    SEMI JOIN overlap_ids o USING (id)
                    WHERE b.institutions_distinct_count IS DISTINCT FROM f.institutions_distinct_count
                    ORDER BY f.updated_date DESC NULLS LAST, b.id
                    LIMIT 10
                    """,
                ),
            }
            _log(log_fp, "samples done")
            progress.payload["steps"]["samples"] = "done"
            progress.update(samples={k: len(v) for k, v in samples.items()})
        finally:
            con.close()

    report = {
        "generated_at": utc_now_iso(),
        "base_root": str(base_root),
        "final_root": str(final_root),
        "delta_ids_parquet": str(delta_ids_parquet),
        "profile": profile,
        "main_field_counts": main_field_counts,
        "abstract_counts": abstract_counts,
        "authorship_counts": authorship_counts,
        "samples": samples,
    }
    atomic_write_json(json_path, report)
    progress.set_status("done")

    md = []
    md.append("# OpenAlex Increment Change Report")
    md.append("")
    md.append(f"- Generated at: `{report['generated_at']}`")
    md.append(f"- Base root: `{base_root}`")
    md.append(f"- Final root: `{final_root}`")
    md.append("")
    md.append("## Profile")
    md.append("")
    md.append(f"- Distinct delta ids: `{profile['distinct_delta_ids']}`")
    md.append(f"- Existing ids updated: `{profile['overlap_existing_ids']}`")
    md.append(f"- New ids: `{profile['new_ids']}`")
    md.append("")
    md.append("## Main Field Changes")
    md.append("")
    for key, value in main_field_counts.items():
        md.append(f"- {key}: `{value}`")
    md.append("")
    md.append("## Abstract Changes")
    md.append("")
    for key, value in abstract_counts.items():
        md.append(f"- {key}: `{value}`")
    md.append("")
    md.append("## Authorship Changes")
    md.append("")
    for key, value in authorship_counts.items():
        md.append(f"- {key}: `{value}`")
    md.append("")
    for name, rows in samples.items():
        md.append(f"## Samples: {name}")
        md.append("")
        if not rows:
            md.append("- none")
            md.append("")
            continue
        for row in rows:
            md.append(f"- `{json.dumps(row, ensure_ascii=False)}`")
        md.append("")
    _write_text(md_path, "\n".join(md))
    return report
