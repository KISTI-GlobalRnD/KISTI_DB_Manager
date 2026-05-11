#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pymysql


SERVING_TABLE = "openalex_core_works"
AGG_YEAR_TABLE = "openalex_core_works_year_counts"
AGG_FIELD_YEAR_TABLE = "openalex_core_works_field_year_counts"
AGG_SOURCE_YEAR_TABLE = "openalex_core_works_source_year_counts"
LOCK_TABLE_ERROR_CODES = {1206}
RETRYABLE_ERROR_CODES = {1205}
RETRY_SLEEP_SECONDS = 10

CORE_TYPE_VALUES = ("article", "review", "book-chapter", "book chapter")
CORE_SOURCE_TYPE_VALUES = ("journal", "book series", "book_series")


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _emit(event: str, **payload: Any) -> None:
    print(json.dumps({"event": event, **payload}, ensure_ascii=False), flush=True)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _load_db_config(run_dir: Path) -> dict[str, Any]:
    cfg = _read_json(run_dir / "config.json")
    db = dict(cfg.get("db_config") or {})
    db.setdefault("charset", "utf8mb4")
    db.setdefault("autocommit", True)
    return db


def _connect(db_config: dict[str, Any]):
    kwargs = dict(db_config)
    kwargs["autocommit"] = True
    kwargs["charset"] = kwargs.get("charset") or "utf8mb4"
    conn = pymysql.connect(**kwargs)
    with conn.cursor() as cur:
        cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    return conn


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build a typed serving table for fast OpenAlex-like filters.")
    ap.add_argument("run_dir", help="runs/<run_id_dir>")
    ap.add_argument("--source-table", default="openalex_works_meta")
    ap.add_argument("--target-table", default=SERVING_TABLE)
    ap.add_argument("--state-dir", default="", help="Default: <run_dir>/serving_core_works")
    ap.add_argument("--bucket-start", type=int, default=10)
    ap.add_argument("--bucket-end", type=int, default=99)
    ap.add_argument("--skip-aggregates", action="store_true")
    ap.add_argument("--skip-finalize-indexes", action="store_true")
    return ap.parse_args()


def _ensure_table(cur, *, table: str) -> None:
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{table}` (
          `work_id` VARCHAR(32) NOT NULL,
          `doi_norm` VARCHAR(255) NULL,
          `publication_date` DATE NULL,
          `publication_year` INT NULL,
          `type` VARCHAR(32) NULL,
          `language` VARCHAR(16) NULL,
          `source_id` VARCHAR(32) NULL,
          `source_type` VARCHAR(32) NULL,
          `source_is_core` TINYINT NULL,
          `source_issn_l` VARCHAR(16) NULL,
          `primary_topic_id` VARCHAR(32) NULL,
          `domain_id` VARCHAR(32) NULL,
          `field_id` VARCHAR(32) NULL,
          `subfield_id` VARCHAR(32) NULL,
          `authors_count` INT NULL,
          `institutions_distinct_count` INT NULL,
          `countries_distinct_count` INT NULL,
          `locations_count` INT NULL,
          `cited_by_count` INT NULL,
          `referenced_works_count` INT NULL,
          `has_abstract` TINYINT NULL,
          `has_fulltext` TINYINT NULL,
          `is_retracted` TINYINT NULL,
          `is_paratext` TINYINT NULL,
          `has_refs` TINYINT NULL,
          `has_affiliation` TINYINT NULL,
          `is_core_type` TINYINT NULL,
          `is_core_source_type` TINYINT NULL,
          `is_cwts_exact_core` TINYINT NULL,
          PRIMARY KEY (`work_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci
        """
    )


def _index_exists(cur, *, table: str, index_name: str) -> bool:
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


def _ensure_secondary_indexes(cur, *, table: str) -> None:
    specs = [
        ("idx_cwts_year_work", ("is_cwts_exact_core", "publication_year", "work_id")),
        ("idx_sourcecore_year_work", ("source_is_core", "publication_year", "work_id")),
        ("idx_field_year_work", ("field_id", "publication_year", "work_id")),
        ("idx_source_year_work", ("source_id", "publication_year", "work_id")),
        ("idx_type_lang_year_work", ("type", "language", "publication_year", "work_id")),
        ("idx_doi_norm", ("doi_norm",)),
    ]
    for index_name, columns in specs:
        if _index_exists(cur, table=table, index_name=index_name):
            _emit("skip_index_exists", table=table, index_name=index_name)
            continue
        started = time.time()
        cols_sql = ", ".join(f"`{col}`" for col in columns)
        _emit("create_index_start", table=table, index_name=index_name, columns=list(columns))
        cur.execute(f"CREATE INDEX `{index_name}` ON `{table}` ({cols_sql})")
        _emit("create_index_done", table=table, index_name=index_name, seconds=round(time.time() - started, 3))


def _build_buckets(start: int, end: int) -> list[tuple[int, str, str]]:
    if start < 10 or end > 99 or start > end:
        raise ValueError("bucket range must be within 10..99")
    buckets: list[tuple[int, str, str]] = []
    for prefix in range(start, end + 1):
        lo = f"W{prefix}"
        hi = f"W{prefix + 1}" if prefix < 99 else "W:"
        buckets.append((prefix, lo, hi))
    return buckets


def _make_bucket_spec(*, label: str, lower_bound: str, upper_bound: str, coarse_prefix: int, depth: int) -> dict[str, Any]:
    return {
        "label": label,
        "lower_bound": lower_bound,
        "upper_bound": upper_bound,
        "coarse_prefix": coarse_prefix,
        "depth": depth,
    }


def _build_bucket_specs(start: int, end: int) -> list[dict[str, Any]]:
    return [
        _make_bucket_spec(
            label=str(prefix),
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            coarse_prefix=prefix,
            depth=0,
        )
        for prefix, lower_bound, upper_bound in _build_buckets(start, end)
    ]


def _split_bucket(bucket: dict[str, Any]) -> list[dict[str, Any]]:
    lower_bound = str(bucket["lower_bound"])
    upper_bound = str(bucket["upper_bound"])
    label = str(bucket["label"])
    coarse_prefix = int(bucket["coarse_prefix"])
    depth = int(bucket.get("depth", 0)) + 1
    children: list[dict[str, Any]] = []
    for digit in range(10):
        child_lower = f"{lower_bound}{digit}"
        child_upper = upper_bound if digit == 9 else f"{lower_bound}{digit + 1}"
        children.append(
            _make_bucket_spec(
                label=f"{label}.{digit}",
                lower_bound=child_lower,
                upper_bound=child_upper,
                coarse_prefix=coarse_prefix,
                depth=depth,
            )
        )
    return children


def _load_state(path: Path, *, buckets_total: int) -> dict[str, Any]:
    if path.exists():
        state = _read_json(path)
    else:
        state = {
            "started_at": _utc_now(),
            "buckets_total": buckets_total,
            "bucket_index": 0,
            "rows_loaded": 0,
            "load_complete": False,
            "indexes_complete": False,
            "aggregates_complete": False,
        }
    return state


def _normalize_state_for_queue(
    state: dict[str, Any],
    *,
    bucket_specs: list[dict[str, Any]],
) -> dict[str, Any]:
    if state.get("pending_buckets"):
        state.setdefault("ranges_done", int(state.get("bucket_index", 0)))
        state.setdefault("ranges_total", int(state.get("ranges_done", 0)) + len(state["pending_buckets"]))
        return state

    bucket_index = int(state.get("bucket_index", 0))
    pending = [dict(spec) for spec in bucket_specs[bucket_index:]]
    state["pending_buckets"] = pending
    state["ranges_done"] = bucket_index
    state["ranges_total"] = bucket_index + len(pending)
    return state


def _persist_state(path: Path, state: dict[str, Any]) -> None:
    state["updated_at"] = _utc_now()
    _write_json(path, state)


def _insert_bucket(cur, *, source_table: str, target_table: str, lower_bound: str, upper_bound: str) -> int:
    type_values = ", ".join(repr(v) for v in CORE_TYPE_VALUES)
    source_type_values = ", ".join(repr(v) for v in CORE_SOURCE_TYPE_VALUES)
    sql = f"""
    INSERT IGNORE INTO `{target_table}` (
      `work_id`, `doi_norm`, `publication_date`, `publication_year`, `type`, `language`,
      `source_id`, `source_type`, `source_is_core`, `source_issn_l`,
      `primary_topic_id`, `domain_id`, `field_id`, `subfield_id`,
      `authors_count`, `institutions_distinct_count`, `countries_distinct_count`, `locations_count`,
      `cited_by_count`, `referenced_works_count`, `has_abstract`, `has_fulltext`,
      `is_retracted`, `is_paratext`,
      `has_refs`, `has_affiliation`, `is_core_type`, `is_core_source_type`, `is_cwts_exact_core`
    )
    SELECT
      `work_id`, `doi_norm`, `publication_date`, `publication_year`, `type`, `language`,
      `source_id`, `source_type`, `source_is_core`, `source_issn_l`,
      `primary_topic_id`, `domain_id`, `field_id`, `subfield_id`,
      `authors_count`, `institutions_distinct_count`, `countries_distinct_count`, `locations_count`,
      `cited_by_count`, `referenced_works_count`, `has_abstract`, `has_fulltext`,
      `is_retracted`, `is_paratext`,
      CASE WHEN COALESCE(`referenced_works_count`, 0) >= 1 THEN 1 ELSE 0 END AS `has_refs`,
      CASE WHEN COALESCE(`institutions_distinct_count`, 0) >= 1 THEN 1 ELSE 0 END AS `has_affiliation`,
      CASE WHEN LOWER(COALESCE(`type`, '')) IN ({type_values}) THEN 1 ELSE 0 END AS `is_core_type`,
      CASE WHEN LOWER(COALESCE(`source_type`, '')) IN ({source_type_values}) THEN 1 ELSE 0 END AS `is_core_source_type`,
      CASE
        WHEN COALESCE(`source_is_core`, 0) = 1
         AND LOWER(COALESCE(`language`, '')) = 'en'
         AND LOWER(COALESCE(`type`, '')) IN ({type_values})
         AND LOWER(COALESCE(`source_type`, '')) IN ({source_type_values})
         AND COALESCE(`referenced_works_count`, 0) >= 1
         AND COALESCE(`institutions_distinct_count`, 0) >= 1
         AND COALESCE(`is_retracted`, 0) = 0
         AND COALESCE(`is_paratext`, 0) = 0
        THEN 1 ELSE 0
      END AS `is_cwts_exact_core`
    FROM `{source_table}` FORCE INDEX (`PRIMARY`)
    WHERE `work_id` >= %s AND `work_id` < %s
    """
    cur.execute(sql, (lower_bound, upper_bound))
    return int(cur.rowcount or 0)


def _rebuild_aggregates(cur, *, serving_table: str) -> None:
    started = time.time()
    _emit("aggregate_start", table=AGG_YEAR_TABLE)
    cur.execute(f"DROP TABLE IF EXISTS `{AGG_YEAR_TABLE}`")
    cur.execute(
        f"""
        CREATE TABLE `{AGG_YEAR_TABLE}` AS
        SELECT
          `publication_year`,
          COUNT(*) AS `n_total`,
          SUM(COALESCE(`is_cwts_exact_core`, 0)) AS `n_cwts_exact_core`,
          SUM(COALESCE(`has_abstract`, 0)) AS `n_has_abstract`
        FROM `{serving_table}`
        WHERE `publication_year` IS NOT NULL
        GROUP BY `publication_year`
        """
    )
    cur.execute(f"ALTER TABLE `{AGG_YEAR_TABLE}` ADD PRIMARY KEY (`publication_year`)")
    _emit("aggregate_done", table=AGG_YEAR_TABLE, seconds=round(time.time() - started, 3))

    started = time.time()
    _emit("aggregate_start", table=AGG_FIELD_YEAR_TABLE)
    cur.execute(f"DROP TABLE IF EXISTS `{AGG_FIELD_YEAR_TABLE}`")
    cur.execute(
        f"""
        CREATE TABLE `{AGG_FIELD_YEAR_TABLE}` AS
        SELECT
          `field_id`,
          `publication_year`,
          COUNT(*) AS `n_total`,
          SUM(COALESCE(`is_cwts_exact_core`, 0)) AS `n_cwts_exact_core`
        FROM `{serving_table}`
        WHERE `field_id` IS NOT NULL AND `field_id` <> ''
          AND `publication_year` IS NOT NULL
        GROUP BY `field_id`, `publication_year`
        """
    )
    cur.execute(
        f"ALTER TABLE `{AGG_FIELD_YEAR_TABLE}` ADD PRIMARY KEY (`field_id`, `publication_year`)"
    )
    _emit("aggregate_done", table=AGG_FIELD_YEAR_TABLE, seconds=round(time.time() - started, 3))

    started = time.time()
    _emit("aggregate_start", table=AGG_SOURCE_YEAR_TABLE)
    cur.execute(f"DROP TABLE IF EXISTS `{AGG_SOURCE_YEAR_TABLE}`")
    cur.execute(
        f"""
        CREATE TABLE `{AGG_SOURCE_YEAR_TABLE}` AS
        SELECT
          `source_id`,
          `publication_year`,
          COUNT(*) AS `n_total`,
          SUM(COALESCE(`is_cwts_exact_core`, 0)) AS `n_cwts_exact_core`
        FROM `{serving_table}`
        WHERE `source_id` IS NOT NULL AND `source_id` <> ''
          AND `publication_year` IS NOT NULL
        GROUP BY `source_id`, `publication_year`
        """
    )
    cur.execute(
        f"ALTER TABLE `{AGG_SOURCE_YEAR_TABLE}` ADD PRIMARY KEY (`source_id`, `publication_year`)"
    )
    _emit("aggregate_done", table=AGG_SOURCE_YEAR_TABLE, seconds=round(time.time() - started, 3))


def main() -> int:
    args = _parse_args()
    run_dir = Path(args.run_dir).expanduser().resolve()
    db_config = _load_db_config(run_dir)
    state_dir = Path(args.state_dir).expanduser().resolve() if args.state_dir else (run_dir / "serving_core_works")
    state_dir.mkdir(parents=True, exist_ok=True)
    state_path = state_dir / "progress.json"

    bucket_specs = _build_bucket_specs(args.bucket_start, args.bucket_end)
    state = _load_state(state_path, buckets_total=len(bucket_specs))
    state = _normalize_state_for_queue(state, bucket_specs=bucket_specs)
    _persist_state(state_path, state)

    conn = _connect(db_config)
    try:
        with conn.cursor() as cur:
            _ensure_table(cur, table=str(args.target_table))

        if not state.get("load_complete"):
            while state["pending_buckets"]:
                bucket = dict(state["pending_buckets"][0])
                prefix = int(bucket["coarse_prefix"])
                lower_bound = str(bucket["lower_bound"])
                upper_bound = str(bucket["upper_bound"])
                label = str(bucket["label"])
                depth = int(bucket.get("depth", 0))
                started = time.time()
                _emit(
                    "bucket_start",
                    bucket_index=int(state.get("ranges_done", 0)),
                    bucket_prefix=prefix,
                    bucket_label=label,
                    lower_bound=lower_bound,
                    upper_bound=upper_bound,
                    depth=depth,
                )
                try:
                    with conn.cursor() as cur:
                        rows_loaded = _insert_bucket(
                            cur,
                            source_table=str(args.source_table),
                            target_table=str(args.target_table),
                            lower_bound=lower_bound,
                            upper_bound=upper_bound,
                        )
                except pymysql.err.OperationalError as exc:
                    errno = int(exc.args[0]) if exc.args else 0
                    if errno in RETRYABLE_ERROR_CODES:
                        state["last_error"] = {
                            "code": errno,
                            "message": str(exc),
                            "bucket_label": label,
                            "lower_bound": lower_bound,
                            "upper_bound": upper_bound,
                        }
                        _persist_state(state_path, state)
                        _emit(
                            "bucket_retry",
                            bucket_index=int(state.get("ranges_done", 0)),
                            bucket_prefix=prefix,
                            bucket_label=label,
                            depth=depth,
                            error_code=errno,
                            error_message=str(exc),
                            sleep_seconds=RETRY_SLEEP_SECONDS,
                        )
                        time.sleep(RETRY_SLEEP_SECONDS)
                        continue
                    if errno not in LOCK_TABLE_ERROR_CODES:
                        raise
                    split_buckets = _split_bucket(bucket)
                    state["pending_buckets"] = split_buckets + list(state["pending_buckets"][1:])
                    state["ranges_total"] = int(state.get("ranges_total", 0)) + len(split_buckets) - 1
                    state["last_error"] = {
                        "code": errno,
                        "message": str(exc),
                        "bucket_label": label,
                        "lower_bound": lower_bound,
                        "upper_bound": upper_bound,
                    }
                    _persist_state(state_path, state)
                    started = time.time()
                    _emit(
                        "bucket_split",
                        bucket_index=int(state.get("ranges_done", 0)),
                        bucket_prefix=prefix,
                        bucket_label=label,
                        depth=depth,
                        split_children=[child["label"] for child in split_buckets],
                        error_code=errno,
                        error_message=str(exc),
                    )
                    continue

                state["pending_buckets"] = list(state["pending_buckets"][1:])
                state["ranges_done"] = int(state.get("ranges_done", 0)) + 1
                state["bucket_index"] = state["ranges_done"]
                state["rows_loaded"] = int(state.get("rows_loaded", 0)) + rows_loaded
                state["last_bucket_prefix"] = prefix
                state["last_bucket_label"] = label
                state.pop("last_error", None)
                _persist_state(state_path, state)
                _emit(
                    "bucket_done",
                    bucket_index=int(state.get("ranges_done", 0)) - 1,
                    bucket_prefix=prefix,
                    bucket_label=label,
                    rows_loaded=rows_loaded,
                    cumulative_rows=state["rows_loaded"],
                    seconds=round(time.time() - started, 3),
                )

            state["load_complete"] = True
            _persist_state(state_path, state)
            _emit("load_complete", table=str(args.target_table), rows_loaded=state["rows_loaded"])

        if not args.skip_finalize_indexes and not state.get("indexes_complete"):
            with conn.cursor() as cur:
                _ensure_secondary_indexes(cur, table=str(args.target_table))
                _emit("analyze_table_start", table=str(args.target_table))
                cur.execute(f"ANALYZE TABLE `{args.target_table}`")
                _emit("analyze_table_done", table=str(args.target_table))
            state["indexes_complete"] = True
            _persist_state(state_path, state)

        if not args.skip_aggregates and not state.get("aggregates_complete"):
            with conn.cursor() as cur:
                _rebuild_aggregates(cur, serving_table=str(args.target_table))
            state["aggregates_complete"] = True
            _persist_state(state_path, state)

        state["finished_at"] = _utc_now()
        _persist_state(state_path, state)
        _emit("all_done", table=str(args.target_table), state_path=str(state_path))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
