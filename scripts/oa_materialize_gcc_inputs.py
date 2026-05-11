#!/usr/bin/env python3
"""
Materialize deduplicated OA GCC input tables from a loaded OpenAlex works snapshot.

Why this exists:
- The raw `openalex_works_202602*` tables may contain duplicate rows from resumed ingest.
- For the OA GCC phase-1 work we only need three stable, deduplicated inputs:
  - `openalex_works_meta`
  - `openalex_works_text`
  - `openalex_refs`
- These target tables are indexed for the downstream filter / GCC pipeline.

Current design:
- source rows are streamed in `id` order (requires source `id` index for speed)
- target tables use PK / unique keys so `LOAD DATA ... IGNORE` deduplicates on load
- progress is checkpointed to JSON so interrupted runs can resume
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any

import pymysql

from KISTI_DB_Manager import load_data


META_COLUMNS = [
    "work_id",
    "doi_norm",
    "publication_date",
    "publication_year",
    "type",
    "language",
    "has_abstract",
    "has_fulltext",
    "authors_count",
    "institutions_distinct_count",
    "countries_distinct_count",
    "locations_count",
    "cited_by_count",
    "referenced_works_count",
    "fwci",
    "citation_normalized_percentile_value",
    "citation_normalized_percentile_top_1",
    "citation_normalized_percentile_top_10",
    "is_retracted",
    "is_paratext",
    "source_id",
    "source_is_core",
    "source_type",
    "source_issn_l",
    "primary_topic_id",
    "primary_topic_name",
    "primary_topic_score",
    "domain_id",
    "domain_name",
    "field_id",
    "field_name",
    "subfield_id",
    "subfield_name",
]

TEXT_COLUMNS = [
    "work_id",
    "title",
    "abstract",
    "has_abstract",
]

REFS_COLUMNS = [
    "citing_work_id",
    "cited_work_id",
]

TEXT_LIKE_TYPES = {"tinytext", "text", "mediumtext", "longtext"}
OA_WORK_ID_PREFIX = "https://openalex.org/W"
OA_WORK_ID_SENTINEL = OA_WORK_ID_PREFIX + ":"
RANGE_BUCKET_MAX_EST_ROWS = 1_000_000
RANGE_BUCKET_MAX_DIGITS = 5


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_db_config(run_dir: Path) -> dict[str, Any]:
    cfg = _read_json(run_dir / "config.json")
    db = dict(cfg.get("db_config") or {})
    db.setdefault("charset", "utf8mb4")
    db.setdefault("autocommit", True)
    return db


def _connect(db_config: dict[str, Any], *, local_infile: bool = False):
    kwargs = dict(db_config)
    kwargs["local_infile"] = bool(local_infile)
    kwargs["autocommit"] = True
    kwargs["charset"] = kwargs.get("charset") or "utf8mb4"
    return pymysql.connect(**kwargs)


def _apply_fast_load_session_settings(conn) -> None:
    if conn is None:
        return
    settings = [
        "SET SESSION unique_checks=0",
        "SET SESSION foreign_key_checks=0",
        "SET SESSION sql_log_bin=0",
    ]
    try:
        with conn.cursor() as cur:
            for sql in settings:
                try:
                    cur.execute(sql)
                except Exception:
                    pass
    except Exception:
        pass


def _strip_oa_prefix(value: Any) -> str:
    s = str(value or "").strip()
    pref = "https://openalex.org/"
    if s.startswith(pref):
        return s[len(pref) :]
    return s


def _normalize_doi(value: Any) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    low = s.lower()
    for pref in ("https://doi.org/", "http://doi.org/", "doi.org/"):
        if low.startswith(pref):
            s = s[len(pref) :]
            low = s.lower()
            break
    if low.startswith("doi:"):
        s = s.split(":", 1)[1].strip()
    return s.strip().lower()


def _to_bool_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1 if value else 0
    s = str(value).strip().lower()
    if not s:
        return None
    if s in {"1", "true", "t", "yes", "y"}:
        return 1
    if s in {"0", "false", "f", "no", "n"}:
        return 0
    return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    s = str(value).strip()
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _to_text(value: Any, *, lower: bool = False) -> str:
    s = str(value or "").strip()
    return s.lower() if lower else s


def _tsv_escape(value: Any) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    return load_data.mysql_escape_load_data_value(value)


def _write_tsv(path: Path, *, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        for row in rows:
            f.write("\t".join(_tsv_escape(row.get(col)) for col in columns))
            f.write("\n")


def _load_tsv(conn, *, path: Path, table: str, columns: list[str]) -> int:
    return load_data.load_data_local_infile_tabular_file(
        conn=conn,
        table_name=table,
        file_path=str(path),
        sep="\t",
        columns_expr=[f"`{str(c).replace('`', '``')}`" for c in columns],
        ignore_lines=0,
        dialect=load_data.MYSQL_GENERATED_TSV_DIALECT,
        ignore_duplicates=True,
        line_terminator="\n",
    )


def _sql_quote(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _sql_trim(expr: str) -> str:
    return f"TRIM(COALESCE({expr}, ''))"


def _sql_text(expr: str, *, lower: bool = False) -> str:
    base = _sql_trim(expr)
    if lower:
        base = f"LOWER({base})"
    return f"NULLIF({base}, '')"


def _sql_strip_oa(expr: str) -> str:
    return f"NULLIF(REPLACE({_sql_trim(expr)}, 'https://openalex.org/', ''), '')"


def _sql_bool(expr: str) -> str:
    base = _sql_trim(expr)
    lowered = f"LOWER({base})"
    return (
        "CASE "
        f"WHEN {base} = '' THEN NULL "
        f"WHEN {lowered} IN ('1', 'true', 't', 'yes', 'y') THEN 1 "
        f"WHEN {lowered} IN ('0', 'false', 'f', 'no', 'n') THEN 0 "
        "ELSE NULL END"
    )


def _sql_int(expr: str) -> str:
    base = _sql_trim(expr)
    return (
        "CASE "
        f"WHEN {base} = '' THEN NULL "
        f"WHEN {base} REGEXP '^-?[0-9]+$' THEN CAST({base} AS SIGNED) "
        f"WHEN {base} REGEXP '^-?[0-9]+\\.[0-9]+$' THEN CAST(CAST({base} AS DECIMAL(30,10)) AS SIGNED) "
        "ELSE NULL END"
    )


def _sql_float(expr: str) -> str:
    base = _sql_trim(expr)
    return (
        "CASE "
        f"WHEN {base} = '' THEN NULL "
        f"WHEN {base} REGEXP '^-?[0-9]+(\\.[0-9]+)?$' THEN CAST({base} AS DOUBLE) "
        "ELSE NULL END"
    )


def _sql_date(expr: str) -> str:
    base = _sql_trim(expr)
    return (
        "CASE "
        f"WHEN {base} REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$' "
        f"THEN STR_TO_DATE({base}, '%Y-%m-%d') "
        "ELSE NULL END"
    )


def _sql_doi(primary_expr: str, fallback_expr: str) -> str:
    base = f"LOWER({_sql_trim(f'COALESCE({primary_expr}, {fallback_expr})')})"
    return (
        "NULLIF("
        "CASE "
        f"WHEN {base} = '' THEN '' "
        f"WHEN {base} LIKE 'https://doi.org/%' THEN SUBSTRING({base}, 17) "
        f"WHEN {base} LIKE 'http://doi.org/%' THEN SUBSTRING({base}, 16) "
        f"WHEN {base} LIKE 'doi.org/%' THEN SUBSTRING({base}, 9) "
        f"WHEN {base} LIKE 'doi:%' THEN TRIM(SUBSTRING({base}, 5)) "
        f"ELSE {base} END"
        ", '')"
    )


def _id_index_name(cur, *, table: str) -> str | None:
    cur.execute(
        """
        SELECT index_name
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND column_name = 'id'
        ORDER BY index_name, seq_in_index
        LIMIT 1
        """,
        (table,),
    )
    row = cur.fetchone()
    return str(row[0]) if row and row[0] else None


def _column_type(cur, *, table: str, column: str) -> str | None:
    cur.execute(
        """
        SELECT DATA_TYPE
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (table, column),
    )
    row = cur.fetchone()
    return str(row[0]).lower() if row and row[0] else None


def _select_strategy(cur, *, table: str, column: str) -> str:
    data_type = _column_type(cur, table=table, column=column)
    if data_type in TEXT_LIKE_TYPES and column == "id":
        return "range_buckets" if _id_index_name(cur, table=table) else "full_scan"
    if data_type in TEXT_LIKE_TYPES:
        return "full_scan"
    return "cursor"


def _next_digit_upper_bound(digits: str) -> str:
    chars = list(digits)
    for idx in range(len(chars) - 1, -1, -1):
        if chars[idx] != "9":
            chars[idx] = str(int(chars[idx]) + 1)
            return OA_WORK_ID_PREFIX + "".join(chars[: idx + 1])
    return OA_WORK_ID_SENTINEL


def _estimate_range_rows(cur, *, table: str, index_name: str, lower: str, upper: str) -> int:
    cur.execute(
        f"EXPLAIN SELECT `id` FROM `{table}` FORCE INDEX (`{index_name}`) WHERE `id` >= %s AND `id` < %s",
        (lower, upper),
    )
    row = cur.fetchone()
    if not row or len(row) < 9:
        return 0
    try:
        return int(row[8] or 0)
    except Exception:
        return 0


def _build_range_buckets(cur, *, table: str, index_name: str, max_est_rows: int = RANGE_BUCKET_MAX_EST_ROWS, max_digits: int = RANGE_BUCKET_MAX_DIGITS) -> list[dict[str, Any]]:
    buckets: list[dict[str, Any]] = []

    def recurse(digits: str) -> None:
        lower = OA_WORK_ID_PREFIX + digits
        upper = _next_digit_upper_bound(digits)
        est_rows = _estimate_range_rows(cur, table=table, index_name=index_name, lower=lower, upper=upper)
        if est_rows <= max_est_rows or len(digits) >= max_digits:
            buckets.append(
                {
                    "key": digits,
                    "lower": lower,
                    "upper": upper,
                    "estimated_rows": est_rows,
                }
            )
            return

        exact_upper = lower + "0"
        exact_rows = _estimate_range_rows(cur, table=table, index_name=index_name, lower=lower, upper=exact_upper)
        if exact_rows > 0:
            buckets.append(
                {
                    "key": f"{digits}_exact",
                    "lower": lower,
                    "upper": exact_upper,
                    "estimated_rows": exact_rows,
                }
            )
        for digit in "0123456789":
            recurse(digits + digit)

    for digit in "0123456789":
        recurse(digit)
    return buckets


def _slice_range_buckets(
    buckets: list[dict[str, Any]],
    *,
    start_index: int | None,
    end_index: int | None,
) -> tuple[int, int, list[dict[str, Any]]]:
    total = len(buckets)
    start = max(0, int(start_index or 0))
    end = total if end_index is None else min(total, max(start, int(end_index)))
    return start, end, buckets[start:end]


def _meta_row(row: dict[str, Any]) -> dict[str, Any]:
    has_abstract = _to_bool_int(row.get("has_abstract"))
    if has_abstract is None:
        has_abstract = 1 if _to_text(row.get("abstract")) else 0
    return {
        "work_id": _strip_oa_prefix(row.get("id")),
        "doi_norm": _normalize_doi(row.get("doi") or row.get("ids__doi")),
        "publication_date": _to_text(row.get("publication_date")) or None,
        "publication_year": _to_int(row.get("publication_year")),
        "type": _to_text(row.get("type"), lower=True),
        "language": _to_text(row.get("language"), lower=True),
        "has_abstract": has_abstract,
        "has_fulltext": _to_bool_int(row.get("has_fulltext")),
        "authors_count": _to_int(row.get("authors_count")),
        "institutions_distinct_count": _to_int(row.get("institutions_distinct_count")),
        "countries_distinct_count": _to_int(row.get("countries_distinct_count")),
        "locations_count": _to_int(row.get("locations_count")),
        "cited_by_count": _to_int(row.get("cited_by_count")),
        "referenced_works_count": _to_int(row.get("referenced_works_count")),
        "fwci": _to_float(row.get("fwci")),
        "citation_normalized_percentile_value": _to_float(row.get("citation_normalized_percentile__value")),
        "citation_normalized_percentile_top_1": _to_bool_int(row.get("citation_normalized_percentile__is_in_top_1_percent")),
        "citation_normalized_percentile_top_10": _to_bool_int(row.get("citation_normalized_percentile__is_in_top_10_percent")),
        "is_retracted": _to_bool_int(row.get("is_retracted")),
        "is_paratext": _to_bool_int(row.get("is_paratext")),
        "source_id": _strip_oa_prefix(row.get("primary_location__source__id")),
        "source_is_core": _to_bool_int(row.get("primary_location__source__is_core")),
        "source_type": _to_text(row.get("primary_location__source__type"), lower=True),
        "source_issn_l": _to_text(row.get("primary_location__source__issn_l")),
        "primary_topic_id": _strip_oa_prefix(row.get("primary_topic__id")),
        "primary_topic_name": _to_text(row.get("primary_topic__display_name")),
        "primary_topic_score": _to_float(row.get("primary_topic__score")),
        "domain_id": _strip_oa_prefix(row.get("primary_topic__domain__id")),
        "domain_name": _to_text(row.get("primary_topic__domain__display_name")),
        "field_id": _strip_oa_prefix(row.get("primary_topic__field__id")),
        "field_name": _to_text(row.get("primary_topic__field__display_name")),
        "subfield_id": _strip_oa_prefix(row.get("primary_topic__subfield__id")),
        "subfield_name": _to_text(row.get("primary_topic__subfield__display_name")),
    }


def _text_row(row: dict[str, Any]) -> dict[str, Any]:
    title = _to_text(row.get("display_name"))
    abstract = _to_text(row.get("abstract"))
    has_abstract = _to_bool_int(row.get("has_abstract"))
    if has_abstract is None:
        has_abstract = 1 if abstract else 0
    return {
        "work_id": _strip_oa_prefix(row.get("id")),
        "title": title,
        "abstract": abstract,
        "has_abstract": has_abstract,
    }


def _refs_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "citing_work_id": _strip_oa_prefix(row.get("id")),
        "cited_work_id": _strip_oa_prefix(row.get("referenced_works")),
    }


def _ensure_meta_table(cur, *, table: str) -> None:
    cur.execute(
        f"""
CREATE TABLE IF NOT EXISTS `{table}` (
  `work_id` VARCHAR(32) NOT NULL,
  `doi_norm` VARCHAR(255) NULL,
  `publication_date` DATE NULL,
  `publication_year` INT NULL,
  `type` VARCHAR(32) NULL,
  `language` VARCHAR(16) NULL,
  `has_abstract` TINYINT NULL,
  `has_fulltext` TINYINT NULL,
  `authors_count` INT NULL,
  `institutions_distinct_count` INT NULL,
  `countries_distinct_count` INT NULL,
  `locations_count` INT NULL,
  `cited_by_count` INT NULL,
  `referenced_works_count` INT NULL,
  `fwci` DOUBLE NULL,
  `citation_normalized_percentile_value` DOUBLE NULL,
  `citation_normalized_percentile_top_1` TINYINT NULL,
  `citation_normalized_percentile_top_10` TINYINT NULL,
  `is_retracted` TINYINT NULL,
  `is_paratext` TINYINT NULL,
  `source_id` VARCHAR(32) NULL,
  `source_is_core` TINYINT NULL,
  `source_type` VARCHAR(32) NULL,
  `source_issn_l` VARCHAR(16) NULL,
  `primary_topic_id` VARCHAR(32) NULL,
  `primary_topic_name` VARCHAR(255) NULL,
  `primary_topic_score` DOUBLE NULL,
  `domain_id` VARCHAR(32) NULL,
  `domain_name` VARCHAR(255) NULL,
  `field_id` VARCHAR(32) NULL,
  `field_name` VARCHAR(255) NULL,
  `subfield_id` VARCHAR(32) NULL,
  `subfield_name` VARCHAR(255) NULL,
  PRIMARY KEY (`work_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""
    )


def _ensure_text_table(cur, *, table: str) -> None:
    cur.execute(
        f"""
CREATE TABLE IF NOT EXISTS `{table}` (
  `work_id` VARCHAR(32) NOT NULL,
  `title` TEXT NULL,
  `abstract` MEDIUMTEXT NULL,
  `has_abstract` TINYINT NULL,
  PRIMARY KEY (`work_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""
    )


def _ensure_refs_table(cur, *, table: str) -> None:
    cur.execute(
        f"""
CREATE TABLE IF NOT EXISTS `{table}` (
  `citing_work_id` VARCHAR(32) NOT NULL,
  `cited_work_id` VARCHAR(32) NOT NULL,
  PRIMARY KEY (`citing_work_id`, `cited_work_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""
    )


def _named_index_exists(cur, *, table: str, index_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND index_name = %s
        LIMIT 1
        """,
        (table, index_name),
    )
    return cur.fetchone() is not None


def _ensure_meta_secondary_indexes(cur, *, table: str) -> None:
    specs = [
        ("idx_pub_year", ["publication_year"]),
        ("idx_type_lang", ["type", "language"]),
        ("idx_source_core", ["source_is_core"]),
        ("idx_source_type", ["source_type"]),
        ("idx_field", ["field_id"]),
    ]
    for index_name, columns in specs:
        if _named_index_exists(cur, table=table, index_name=index_name):
            continue
        cols_sql = ", ".join(f"`{col}`" for col in columns)
        cur.execute(f"CREATE INDEX `{index_name}` ON `{table}` ({cols_sql})")


def _ensure_refs_secondary_indexes(cur, *, table: str) -> None:
    if _named_index_exists(cur, table=table, index_name="idx_cited"):
        return
    cur.execute(f"CREATE INDEX `idx_cited` ON `{table}` (`cited_work_id`)")


def _index_exists(cur, *, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (table, column),
    )
    return cur.fetchone() is not None


def _read_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _fetch_batches(cur, *, sql: str, params: tuple[Any, ...]):
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _materialize_works(
    src_conn,
    dst_conn,
    *,
    source_table: str,
    meta_table: str,
    text_table: str,
    batch_size: int,
    state_path: Path,
    work_dir: Path,
    limit_batches: int | None,
) -> None:
    state = _read_state(state_path)
    part_state = dict(state.get("works") or {})
    last_id = str(part_state.get("last_id") or "").strip()
    batches_done = int(part_state.get("batches_done") or 0)
    rows_seen = int(part_state.get("rows_seen") or 0)
    rows_loaded_meta = int(part_state.get("rows_loaded_meta") or 0)
    rows_loaded_text = int(part_state.get("rows_loaded_text") or 0)

    with dst_conn.cursor() as cur_dst:
        _ensure_meta_table(cur_dst, table=meta_table)
        _ensure_text_table(cur_dst, table=text_table)

    select_cols = [
        "id",
        "doi",
        "ids__doi",
        "publication_date",
        "publication_year",
        "type",
        "language",
        "display_name",
        "abstract",
        "has_abstract",
        "has_fulltext",
        "authors_count",
        "institutions_distinct_count",
        "countries_distinct_count",
        "locations_count",
        "cited_by_count",
        "referenced_works_count",
        "fwci",
        "citation_normalized_percentile__value",
        "citation_normalized_percentile__is_in_top_1_percent",
        "citation_normalized_percentile__is_in_top_10_percent",
        "is_retracted",
        "is_paratext",
        "primary_location__source__id",
        "primary_location__source__is_core",
        "primary_location__source__type",
        "primary_location__source__issn_l",
        "primary_topic__id",
        "primary_topic__display_name",
        "primary_topic__score",
        "primary_topic__domain__id",
        "primary_topic__domain__display_name",
        "primary_topic__field__id",
        "primary_topic__field__display_name",
        "primary_topic__subfield__id",
        "primary_topic__subfield__display_name",
    ]
    cols_sql = ", ".join(f"`{c}`" for c in select_cols)

    while True:
        if limit_batches is not None and batches_done >= limit_batches:
            break
        sql = f"SELECT {cols_sql} FROM `{source_table}`"
        params: list[Any] = []
        if last_id:
            sql += " WHERE `id` > %s"
            params.append(last_id)
        sql += " ORDER BY `id` LIMIT %s"
        params.append(int(batch_size))

        with src_conn.cursor() as cur_src:
            rows = _fetch_batches(cur_src, sql=sql, params=tuple(params))
        if not rows:
            break

        meta_rows: list[dict[str, Any]] = []
        text_rows: list[dict[str, Any]] = []
        for row in rows:
            wid = _strip_oa_prefix(row.get("id"))
            if not wid:
                continue
            meta_rows.append(_meta_row(row))
            text_rows.append(_text_row(row))

        batch_tag = f"works_b{batches_done + 1:06d}"
        meta_path = work_dir / f"{batch_tag}_meta.tsv"
        text_path = work_dir / f"{batch_tag}_text.tsv"
        _write_tsv(meta_path, columns=META_COLUMNS, rows=meta_rows)
        _write_tsv(text_path, columns=TEXT_COLUMNS, rows=text_rows)

        rows_loaded_meta += _load_tsv(dst_conn, path=meta_path, table=meta_table, columns=META_COLUMNS)
        rows_loaded_text += _load_tsv(dst_conn, path=text_path, table=text_table, columns=TEXT_COLUMNS)

        try:
            meta_path.unlink()
        except FileNotFoundError:
            pass
        try:
            text_path.unlink()
        except FileNotFoundError:
            pass

        last_id = str(rows[-1].get("id") or "")
        batches_done += 1
        rows_seen += len(rows)
        state["works"] = {
            "last_id": last_id,
            "batches_done": batches_done,
            "rows_seen": rows_seen,
            "rows_loaded_meta": rows_loaded_meta,
            "rows_loaded_text": rows_loaded_text,
            "done": False,
        }
        _write_state(state_path, state)
        print(
            json.dumps(
                {
                    "part": "works",
                    "batch": batches_done,
                    "last_id": last_id,
                    "rows_seen": rows_seen,
                    "rows_loaded_meta": rows_loaded_meta,
                    "rows_loaded_text": rows_loaded_text,
                },
                ensure_ascii=False,
            )
        )

    state["works"] = {
        "last_id": last_id,
        "batches_done": batches_done,
        "rows_seen": rows_seen,
        "rows_loaded_meta": rows_loaded_meta,
        "rows_loaded_text": rows_loaded_text,
        "done": True,
    }
    _write_state(state_path, state)


def _materialize_works_full_scan(
    dst_conn,
    *,
    source_table: str,
    meta_table: str,
    text_table: str,
    state_path: Path,
) -> None:
    state = _read_state(state_path)
    part_state = dict(state.get("works") or {})
    rows_loaded_meta = int(part_state.get("rows_loaded_meta") or 0)
    rows_loaded_text = int(part_state.get("rows_loaded_text") or 0)
    meta_done = bool(part_state.get("meta_done"))
    text_done = bool(part_state.get("text_done"))

    work_id_sql = _sql_strip_oa("`id`")
    has_abstract_sql = f"COALESCE({_sql_bool('`has_abstract`')}, CASE WHEN {_sql_text('`abstract`')} IS NULL THEN 0 ELSE 1 END)"

    with dst_conn.cursor() as cur_dst:
        _ensure_meta_table(cur_dst, table=meta_table)
        _ensure_text_table(cur_dst, table=text_table)

        if not meta_done:
            state["works"] = {
                "strategy": "full_scan",
                "phase": "meta",
                "status": "running",
                "meta_done": meta_done,
                "text_done": text_done,
                "rows_loaded_meta": rows_loaded_meta,
                "rows_loaded_text": rows_loaded_text,
                "done": False,
            }
            _write_state(state_path, state)
            meta_sql = f"""
INSERT IGNORE INTO `{meta_table}` (
  `work_id`, `doi_norm`, `publication_date`, `publication_year`, `type`, `language`,
  `has_abstract`, `has_fulltext`, `authors_count`, `institutions_distinct_count`,
  `countries_distinct_count`, `locations_count`, `cited_by_count`, `referenced_works_count`,
  `fwci`, `citation_normalized_percentile_value`, `citation_normalized_percentile_top_1`,
  `citation_normalized_percentile_top_10`, `is_retracted`, `is_paratext`, `source_id`,
  `source_is_core`, `source_type`, `source_issn_l`, `primary_topic_id`, `primary_topic_name`,
  `primary_topic_score`, `domain_id`, `domain_name`, `field_id`, `field_name`,
  `subfield_id`, `subfield_name`
)
SELECT
  {work_id_sql},
  {_sql_doi('`doi`', '`ids__doi`')},
  {_sql_date('`publication_date`')},
  {_sql_int('`publication_year`')},
  {_sql_text('`type`', lower=True)},
  {_sql_text('`language`', lower=True)},
  {has_abstract_sql},
  {_sql_bool('`has_fulltext`')},
  {_sql_int('`authors_count`')},
  {_sql_int('`institutions_distinct_count`')},
  {_sql_int('`countries_distinct_count`')},
  {_sql_int('`locations_count`')},
  {_sql_int('`cited_by_count`')},
  {_sql_int('`referenced_works_count`')},
  {_sql_float('`fwci`')},
  {_sql_float('`citation_normalized_percentile__value`')},
  {_sql_bool('`citation_normalized_percentile__is_in_top_1_percent`')},
  {_sql_bool('`citation_normalized_percentile__is_in_top_10_percent`')},
  {_sql_bool('`is_retracted`')},
  {_sql_bool('`is_paratext`')},
  {_sql_strip_oa('`primary_location__source__id`')},
  {_sql_bool('`primary_location__source__is_core`')},
  {_sql_text('`primary_location__source__type`', lower=True)},
  {_sql_text('`primary_location__source__issn_l`')},
  {_sql_strip_oa('`primary_topic__id`')},
  {_sql_text('`primary_topic__display_name`')},
  {_sql_float('`primary_topic__score`')},
  {_sql_strip_oa('`primary_topic__domain__id`')},
  {_sql_text('`primary_topic__domain__display_name`')},
  {_sql_strip_oa('`primary_topic__field__id`')},
  {_sql_text('`primary_topic__field__display_name`')},
  {_sql_strip_oa('`primary_topic__subfield__id`')},
  {_sql_text('`primary_topic__subfield__display_name`')}
FROM `{source_table}`
WHERE {work_id_sql} IS NOT NULL
"""
            cur_dst.execute(meta_sql)
            rows_loaded_meta = int(cur_dst.rowcount or 0)
            meta_done = True
            state["works"] = {
                "strategy": "full_scan",
                "phase": "meta",
                "status": "done",
                "meta_done": meta_done,
                "text_done": text_done,
                "rows_loaded_meta": rows_loaded_meta,
                "rows_loaded_text": rows_loaded_text,
                "done": False,
            }
            _write_state(state_path, state)
            print(json.dumps({"part": "works", "phase": "meta", "strategy": "full_scan", "rows_loaded_meta": rows_loaded_meta}, ensure_ascii=False), flush=True)

        if not text_done:
            state["works"] = {
                "strategy": "full_scan",
                "phase": "text",
                "status": "running",
                "meta_done": meta_done,
                "text_done": text_done,
                "rows_loaded_meta": rows_loaded_meta,
                "rows_loaded_text": rows_loaded_text,
                "done": False,
            }
            _write_state(state_path, state)
            text_sql = f"""
INSERT IGNORE INTO `{text_table}` (
  `work_id`, `title`, `abstract`, `has_abstract`
)
SELECT
  {work_id_sql},
  {_sql_text('`display_name`')},
  {_sql_text('`abstract`')},
  {has_abstract_sql}
FROM `{source_table}`
WHERE {work_id_sql} IS NOT NULL
"""
            cur_dst.execute(text_sql)
            rows_loaded_text = int(cur_dst.rowcount or 0)
            text_done = True
            state["works"] = {
                "strategy": "full_scan",
                "phase": "text",
                "status": "done",
                "meta_done": meta_done,
                "text_done": text_done,
                "rows_loaded_meta": rows_loaded_meta,
                "rows_loaded_text": rows_loaded_text,
                "done": False,
            }
            _write_state(state_path, state)
            print(json.dumps({"part": "works", "phase": "text", "strategy": "full_scan", "rows_loaded_text": rows_loaded_text}, ensure_ascii=False), flush=True)

    state["works"] = {
        "strategy": "full_scan",
        "phase": "complete",
        "status": "done",
        "meta_done": meta_done,
        "text_done": text_done,
        "rows_loaded_meta": rows_loaded_meta,
        "rows_loaded_text": rows_loaded_text,
        "done": bool(meta_done and text_done),
    }
    _write_state(state_path, state)


def _materialize_works_range_buckets(
    dst_conn,
    *,
    source_table: str,
    meta_table: str,
    text_table: str,
    state_path: Path,
    range_bucket_max_est_rows: int = RANGE_BUCKET_MAX_EST_ROWS,
    bucket_start_index: int | None = None,
    bucket_end_index: int | None = None,
) -> None:
    state = _read_state(state_path)
    part_state = dict(state.get("works") or {})
    rows_loaded_meta = int(part_state.get("rows_loaded_meta") or 0)
    rows_loaded_text = int(part_state.get("rows_loaded_text") or 0)
    meta_done = bool(part_state.get("meta_done"))
    text_done = bool(part_state.get("text_done"))
    meta_bucket_index = int(part_state.get("meta_bucket_index") or 0)
    text_bucket_index = int(part_state.get("text_bucket_index") or 0)

    work_id_sql = _sql_strip_oa("`id`")
    has_abstract_sql = f"COALESCE({_sql_bool('`has_abstract`')}, CASE WHEN {_sql_text('`abstract`')} IS NULL THEN 0 ELSE 1 END)"

    with dst_conn.cursor() as cur_dst:
        _ensure_meta_table(cur_dst, table=meta_table)
        _ensure_text_table(cur_dst, table=text_table)
        index_name = _id_index_name(cur_dst, table=source_table)
        if not index_name:
            raise SystemExit(f"Missing required source index for range buckets: {source_table}.id")
        all_buckets = _build_range_buckets(
            cur_dst,
            table=source_table,
            index_name=index_name,
            max_est_rows=int(range_bucket_max_est_rows),
        )
        bucket_start_global, bucket_end_global, buckets = _slice_range_buckets(
            all_buckets,
            start_index=bucket_start_index,
            end_index=bucket_end_index,
        )
        source_hint = f" FORCE INDEX (`{index_name}`)"

        if not buckets:
            state["works"] = {
                "strategy": "range_buckets",
                "phase": "complete",
                "status": "done",
                "meta_done": True,
                "text_done": True,
                "meta_bucket_index": 0,
                "text_bucket_index": 0,
                "bucket_count": 0,
                "bucket_count_total": len(all_buckets),
                "bucket_start_index_global": bucket_start_global,
                "bucket_end_index_global": bucket_end_global,
                "rows_loaded_meta": rows_loaded_meta,
                "rows_loaded_text": rows_loaded_text,
                "done": True,
            }
            _write_state(state_path, state)
            return

        if not meta_done:
            for bucket_idx in range(meta_bucket_index, len(buckets)):
                bucket = buckets[bucket_idx]
                bucket_lower_sql = _sql_quote(str(bucket["lower"]))
                bucket_upper_sql = _sql_quote(str(bucket["upper"]))
                state["works"] = {
                    "strategy": "range_buckets",
                    "phase": "meta",
                    "status": "running",
                    "meta_done": meta_done,
                    "text_done": text_done,
                    "meta_bucket_index": bucket_idx,
                    "text_bucket_index": text_bucket_index,
                    "bucket_count": len(buckets),
                    "bucket_count_total": len(all_buckets),
                    "bucket_start_index_global": bucket_start_global,
                    "bucket_end_index_global": bucket_end_global,
                    "bucket_index_global": bucket_start_global + bucket_idx,
                    "bucket_key": bucket["key"],
                    "bucket_lower": bucket["lower"],
                    "bucket_upper": bucket["upper"],
                    "bucket_estimated_rows": int(bucket.get("estimated_rows") or 0),
                    "rows_loaded_meta": rows_loaded_meta,
                    "rows_loaded_text": rows_loaded_text,
                    "done": False,
                }
                _write_state(state_path, state)
                meta_sql = f"""
INSERT IGNORE INTO `{meta_table}` (
  `work_id`, `doi_norm`, `publication_date`, `publication_year`, `type`, `language`,
  `has_abstract`, `has_fulltext`, `authors_count`, `institutions_distinct_count`,
  `countries_distinct_count`, `locations_count`, `cited_by_count`, `referenced_works_count`,
  `fwci`, `citation_normalized_percentile_value`, `citation_normalized_percentile_top_1`,
  `citation_normalized_percentile_top_10`, `is_retracted`, `is_paratext`, `source_id`,
  `source_is_core`, `source_type`, `source_issn_l`, `primary_topic_id`, `primary_topic_name`,
  `primary_topic_score`, `domain_id`, `domain_name`, `field_id`, `field_name`,
  `subfield_id`, `subfield_name`
)
SELECT
  {work_id_sql},
  {_sql_doi('`doi`', '`ids__doi`')},
  {_sql_date('`publication_date`')},
  {_sql_int('`publication_year`')},
  {_sql_text('`type`', lower=True)},
  {_sql_text('`language`', lower=True)},
  {has_abstract_sql},
  {_sql_bool('`has_fulltext`')},
  {_sql_int('`authors_count`')},
  {_sql_int('`institutions_distinct_count`')},
  {_sql_int('`countries_distinct_count`')},
  {_sql_int('`locations_count`')},
  {_sql_int('`cited_by_count`')},
  {_sql_int('`referenced_works_count`')},
  {_sql_float('`fwci`')},
  {_sql_float('`citation_normalized_percentile__value`')},
  {_sql_bool('`citation_normalized_percentile__is_in_top_1_percent`')},
  {_sql_bool('`citation_normalized_percentile__is_in_top_10_percent`')},
  {_sql_bool('`is_retracted`')},
  {_sql_bool('`is_paratext`')},
  {_sql_strip_oa('`primary_location__source__id`')},
  {_sql_bool('`primary_location__source__is_core`')},
  {_sql_text('`primary_location__source__type`', lower=True)},
  {_sql_text('`primary_location__source__issn_l`')},
  {_sql_strip_oa('`primary_topic__id`')},
  {_sql_text('`primary_topic__display_name`')},
  {_sql_float('`primary_topic__score`')},
  {_sql_strip_oa('`primary_topic__domain__id`')},
  {_sql_text('`primary_topic__domain__display_name`')},
  {_sql_strip_oa('`primary_topic__field__id`')},
  {_sql_text('`primary_topic__field__display_name`')},
  {_sql_strip_oa('`primary_topic__subfield__id`')},
  {_sql_text('`primary_topic__subfield__display_name`')}
FROM `{source_table}`{source_hint}
WHERE `id` >= {bucket_lower_sql}
  AND `id` < {bucket_upper_sql}
"""
                cur_dst.execute(meta_sql)
                rows_loaded_meta += int(cur_dst.rowcount or 0)
                state["works"] = {
                    "strategy": "range_buckets",
                    "phase": "meta",
                    "status": "running",
                    "meta_done": meta_done,
                    "text_done": text_done,
                    "meta_bucket_index": bucket_idx + 1,
                    "text_bucket_index": text_bucket_index,
                    "bucket_count": len(buckets),
                    "bucket_count_total": len(all_buckets),
                    "bucket_start_index_global": bucket_start_global,
                    "bucket_end_index_global": bucket_end_global,
                    "bucket_index_global": bucket_start_global + bucket_idx + 1,
                    "bucket_key": bucket["key"],
                    "bucket_lower": bucket["lower"],
                    "bucket_upper": bucket["upper"],
                    "bucket_estimated_rows": int(bucket.get("estimated_rows") or 0),
                    "rows_loaded_meta": rows_loaded_meta,
                    "rows_loaded_text": rows_loaded_text,
                    "done": False,
                }
                _write_state(state_path, state)
                print(
                    json.dumps(
                        {
                            "part": "works",
                            "phase": "meta",
                            "strategy": "range_buckets",
                            "bucket_index": bucket_idx + 1,
                            "bucket_count": len(buckets),
                            "bucket_index_global": bucket_start_global + bucket_idx + 1,
                            "bucket_count_total": len(all_buckets),
                            "bucket_key": bucket["key"],
                            "rows_loaded_meta": rows_loaded_meta,
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
            meta_done = True
            meta_bucket_index = len(buckets)

        if not text_done:
            for bucket_idx in range(text_bucket_index, len(buckets)):
                bucket = buckets[bucket_idx]
                bucket_lower_sql = _sql_quote(str(bucket["lower"]))
                bucket_upper_sql = _sql_quote(str(bucket["upper"]))
                state["works"] = {
                    "strategy": "range_buckets",
                    "phase": "text",
                    "status": "running",
                    "meta_done": meta_done,
                    "text_done": text_done,
                    "meta_bucket_index": meta_bucket_index,
                    "text_bucket_index": bucket_idx,
                    "bucket_count": len(buckets),
                    "bucket_count_total": len(all_buckets),
                    "bucket_start_index_global": bucket_start_global,
                    "bucket_end_index_global": bucket_end_global,
                    "bucket_index_global": bucket_start_global + bucket_idx,
                    "bucket_key": bucket["key"],
                    "bucket_lower": bucket["lower"],
                    "bucket_upper": bucket["upper"],
                    "bucket_estimated_rows": int(bucket.get("estimated_rows") or 0),
                    "rows_loaded_meta": rows_loaded_meta,
                    "rows_loaded_text": rows_loaded_text,
                    "done": False,
                }
                _write_state(state_path, state)
                text_sql = f"""
INSERT IGNORE INTO `{text_table}` (
  `work_id`, `title`, `abstract`, `has_abstract`
)
SELECT
  {work_id_sql},
  {_sql_text('`display_name`')},
  {_sql_text('`abstract`')},
  {has_abstract_sql}
FROM `{source_table}`{source_hint}
WHERE `id` >= {bucket_lower_sql}
  AND `id` < {bucket_upper_sql}
"""
                cur_dst.execute(text_sql)
                rows_loaded_text += int(cur_dst.rowcount or 0)
                state["works"] = {
                    "strategy": "range_buckets",
                    "phase": "text",
                    "status": "running",
                    "meta_done": meta_done,
                    "text_done": text_done,
                    "meta_bucket_index": meta_bucket_index,
                    "text_bucket_index": bucket_idx + 1,
                    "bucket_count": len(buckets),
                    "bucket_count_total": len(all_buckets),
                    "bucket_start_index_global": bucket_start_global,
                    "bucket_end_index_global": bucket_end_global,
                    "bucket_index_global": bucket_start_global + bucket_idx + 1,
                    "bucket_key": bucket["key"],
                    "bucket_lower": bucket["lower"],
                    "bucket_upper": bucket["upper"],
                    "bucket_estimated_rows": int(bucket.get("estimated_rows") or 0),
                    "rows_loaded_meta": rows_loaded_meta,
                    "rows_loaded_text": rows_loaded_text,
                    "done": False,
                }
                _write_state(state_path, state)
                print(
                    json.dumps(
                        {
                            "part": "works",
                            "phase": "text",
                            "strategy": "range_buckets",
                            "bucket_index": bucket_idx + 1,
                            "bucket_count": len(buckets),
                            "bucket_index_global": bucket_start_global + bucket_idx + 1,
                            "bucket_count_total": len(all_buckets),
                            "bucket_key": bucket["key"],
                            "rows_loaded_text": rows_loaded_text,
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
            text_done = True
            text_bucket_index = len(buckets)

    state["works"] = {
        "strategy": "range_buckets",
        "phase": "complete",
        "status": "done",
        "meta_done": meta_done,
        "text_done": text_done,
        "meta_bucket_index": meta_bucket_index,
        "text_bucket_index": text_bucket_index,
        "bucket_count": len(buckets),
        "bucket_count_total": len(all_buckets),
        "bucket_start_index_global": bucket_start_global,
        "bucket_end_index_global": bucket_end_global,
        "rows_loaded_meta": rows_loaded_meta,
        "rows_loaded_text": rows_loaded_text,
        "done": bool(meta_done and text_done),
    }
    _write_state(state_path, state)


def _materialize_refs(
    src_conn,
    dst_conn,
    *,
    source_table: str,
    refs_table: str,
    batch_size: int,
    state_path: Path,
    work_dir: Path,
    limit_batches: int | None,
) -> None:
    state = _read_state(state_path)
    part_state = dict(state.get("refs") or {})
    last_id = str(part_state.get("last_id") or "").strip()
    batches_done = int(part_state.get("batches_done") or 0)
    rows_seen = int(part_state.get("rows_seen") or 0)
    rows_loaded = int(part_state.get("rows_loaded") or 0)

    with dst_conn.cursor() as cur_dst:
        _ensure_refs_table(cur_dst, table=refs_table)

    while True:
        if limit_batches is not None and batches_done >= limit_batches:
            break
        sql = f"SELECT `id`, `referenced_works` FROM `{source_table}`"
        params: list[Any] = []
        if last_id:
            sql += " WHERE `id` > %s"
            params.append(last_id)
        sql += " ORDER BY `id` LIMIT %s"
        params.append(int(batch_size))

        with src_conn.cursor() as cur_src:
            rows = _fetch_batches(cur_src, sql=sql, params=tuple(params))
        if not rows:
            break

        ref_rows: list[dict[str, Any]] = []
        for row in rows:
            citing = _strip_oa_prefix(row.get("id"))
            cited = _strip_oa_prefix(row.get("referenced_works"))
            if not citing or not cited:
                continue
            ref_rows.append(_refs_row(row))

        batch_tag = f"refs_b{batches_done + 1:06d}"
        refs_path = work_dir / f"{batch_tag}.tsv"
        _write_tsv(refs_path, columns=REFS_COLUMNS, rows=ref_rows)

        rows_loaded += _load_tsv(dst_conn, path=refs_path, table=refs_table, columns=REFS_COLUMNS)

        try:
            refs_path.unlink()
        except FileNotFoundError:
            pass

        last_id = str(rows[-1].get("id") or "")
        batches_done += 1
        rows_seen += len(rows)
        state["refs"] = {
            "last_id": last_id,
            "batches_done": batches_done,
            "rows_seen": rows_seen,
            "rows_loaded": rows_loaded,
            "done": False,
        }
        _write_state(state_path, state)
        print(
            json.dumps(
                {
                    "part": "refs",
                    "batch": batches_done,
                    "last_id": last_id,
                    "rows_seen": rows_seen,
                    "rows_loaded": rows_loaded,
                },
                ensure_ascii=False,
            )
        )

    state["refs"] = {
        "last_id": last_id,
        "batches_done": batches_done,
        "rows_seen": rows_seen,
        "rows_loaded": rows_loaded,
        "done": True,
    }
    _write_state(state_path, state)


def _materialize_refs_full_scan(
    dst_conn,
    *,
    source_table: str,
    refs_table: str,
    state_path: Path,
) -> None:
    state = _read_state(state_path)
    part_state = dict(state.get("refs") or {})
    if part_state.get("done"):
        return

    with dst_conn.cursor() as cur_dst:
        _ensure_refs_table(cur_dst, table=refs_table)
        state["refs"] = {
            "strategy": "full_scan",
            "status": "running",
            "done": False,
        }
        _write_state(state_path, state)
        citing_sql = _sql_strip_oa("`id`")
        cited_sql = _sql_strip_oa("`referenced_works`")
        refs_sql = f"""
INSERT IGNORE INTO `{refs_table}` (
  `citing_work_id`, `cited_work_id`
)
SELECT
  `citing_work_id`,
  `cited_work_id`
FROM (
  SELECT
    {citing_sql} AS `citing_work_id`,
    {cited_sql} AS `cited_work_id`
  FROM `{source_table}`
) AS `normalized_refs`
WHERE `citing_work_id` IS NOT NULL
  AND `cited_work_id` IS NOT NULL
"""
        cur_dst.execute(refs_sql)
        rows_loaded = int(cur_dst.rowcount or 0)

    state["refs"] = {
        "strategy": "full_scan",
        "status": "done",
        "rows_loaded": rows_loaded,
        "done": True,
    }
    _write_state(state_path, state)
    print(json.dumps({"part": "refs", "strategy": "full_scan", "rows_loaded": rows_loaded}, ensure_ascii=False), flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir", help="runs/<run_id_dir>")
    ap.add_argument("--part", choices=["works", "refs", "all"], default="all")
    ap.add_argument("--source-table", default="openalex_works_202602")
    ap.add_argument("--source-refs-table", default="openalex_works_202602__referenced_works")
    ap.add_argument("--meta-table", default="openalex_works_meta")
    ap.add_argument("--text-table", default="openalex_works_text")
    ap.add_argument("--refs-table", default="openalex_refs")
    ap.add_argument("--batch-size", type=int, default=50000)
    ap.add_argument("--limit-batches", type=int, default=None)
    ap.add_argument("--state-dir", default="", help="Directory for progress JSON and temp TSV batches")
    ap.add_argument("--range-bucket-max-est-rows", type=int, default=RANGE_BUCKET_MAX_EST_ROWS, help="Target estimated rows per range bucket")
    ap.add_argument("--bucket-start-index", type=int, default=None, help="Global start bucket index for range_buckets strategy")
    ap.add_argument("--bucket-end-index", type=int, default=None, help="Global exclusive end bucket index for range_buckets strategy")
    ap.add_argument("--finalize-indexes", action="store_true", help="Create deferred secondary indexes on target tables and exit")
    args = ap.parse_args()

    run_dir = Path(args.run_dir).expanduser().resolve()
    db_config = _load_db_config(run_dir)

    state_dir = Path(args.state_dir).expanduser().resolve() if args.state_dir else (run_dir / "gcc_materialize")
    state_dir.mkdir(parents=True, exist_ok=True)
    state_path = state_dir / "progress.json"
    tmp_dir = state_dir / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    src_conn = _connect(db_config, local_infile=False)
    dst_conn = _connect(db_config, local_infile=True)
    _apply_fast_load_session_settings(dst_conn)
    try:
        if args.finalize_indexes:
            with dst_conn.cursor() as cur:
                if args.part in {"works", "all"}:
                    _ensure_meta_table(cur, table=str(args.meta_table))
                    _ensure_text_table(cur, table=str(args.text_table))
                    _ensure_meta_secondary_indexes(cur, table=str(args.meta_table))
                if args.part in {"refs", "all"}:
                    _ensure_refs_table(cur, table=str(args.refs_table))
                    _ensure_refs_secondary_indexes(cur, table=str(args.refs_table))
            print(json.dumps({"ok": True, "finalize_indexes": True}, ensure_ascii=False), flush=True)
            return 0

        with src_conn.cursor() as cur:
            works_strategy = _select_strategy(cur, table=args.source_table, column="id")
            refs_strategy = _select_strategy(cur, table=args.source_refs_table, column="id")
            if args.part in {"works", "all"} and works_strategy in {"cursor", "range_buckets"} and not _index_exists(cur, table=args.source_table, column="id"):
                raise SystemExit(f"Missing required source index: {args.source_table}.id")
            if args.part in {"refs", "all"} and refs_strategy in {"cursor", "range_buckets"} and not _index_exists(cur, table=args.source_refs_table, column="id"):
                raise SystemExit(f"Missing required source index: {args.source_refs_table}.id")

        if args.part in {"works", "all"}:
            print(json.dumps({"part": "works", "strategy": works_strategy}, ensure_ascii=False), flush=True)
            if works_strategy == "range_buckets":
                _materialize_works_range_buckets(
                    dst_conn,
                    source_table=str(args.source_table),
                    meta_table=str(args.meta_table),
                    text_table=str(args.text_table),
                    state_path=state_path,
                    range_bucket_max_est_rows=int(args.range_bucket_max_est_rows),
                    bucket_start_index=args.bucket_start_index,
                    bucket_end_index=args.bucket_end_index,
                )
            elif works_strategy == "full_scan":
                _materialize_works_full_scan(
                    dst_conn,
                    source_table=str(args.source_table),
                    meta_table=str(args.meta_table),
                    text_table=str(args.text_table),
                    state_path=state_path,
                )
            else:
                _materialize_works(
                    src_conn,
                    dst_conn,
                    source_table=str(args.source_table),
                    meta_table=str(args.meta_table),
                    text_table=str(args.text_table),
                    batch_size=int(args.batch_size),
                    state_path=state_path,
                    work_dir=tmp_dir,
                    limit_batches=args.limit_batches,
                )

        if args.part in {"refs", "all"}:
            print(json.dumps({"part": "refs", "strategy": refs_strategy}, ensure_ascii=False), flush=True)
            if refs_strategy == "full_scan":
                _materialize_refs_full_scan(
                    dst_conn,
                    source_table=str(args.source_refs_table),
                    refs_table=str(args.refs_table),
                    state_path=state_path,
                )
            else:
                _materialize_refs(
                    src_conn,
                    dst_conn,
                    source_table=str(args.source_refs_table),
                    refs_table=str(args.refs_table),
                    batch_size=int(args.batch_size),
                    state_path=state_path,
                    work_dir=tmp_dir,
                    limit_batches=args.limit_batches,
                )
    finally:
        src_conn.close()
        dst_conn.close()

    print(json.dumps({"ok": True, "state_path": str(state_path)}, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
