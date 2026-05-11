#!/usr/bin/env python3
"""
Audit exact duplicate rows from Overton parsed parquet files.

Why this exists:
- DB-side `GROUP BY all columns` is too slow for the larger Overton tables.
- The parsed parquet artifacts are the closer-to-source representation we want to validate.
- We want resumable, table-by-table duplicate checks with machine-readable output.

Definition:
- "exact duplicate" means every audited column value in the row matches another row
  in the same parquet file.
- By default, technical columns prefixed with `__` are excluded from the audit.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq


SUFFIX_TO_SHORT_NAME: list[tuple[str, str]] = [
    ("__main", "docs"),
    ("__sub__authors", "authors"),
    ("__sub__topics", "topics"),
    ("__sub__source_tags", "src_tags"),
    ("__sub__sdgcategories", "sdg_cats"),
    ("__sub__classifications", "classifications"),
    ("__sub__entities", "entities"),
    ("__sub__policy_source_region", "policy_src_region"),
    ("__sub__policy_source_country", "policy_src_country"),
    ("__sub__policy_source_type", "policy_src_type"),
    ("__sub__policy_document_ids_cited", "policy_doc_ids_cited"),
    ("__sub__dois_cited", "cited_dois"),
    ("__sub__self_identifiers", "self_ids"),
    ("__sub__mentions_people", "mentions_people"),
    ("__sub__policy_source_country_iso_codes", "policy_src_country_iso"),
    ("__sub__ref_contexts", "ref_ctx"),
    ("__sub__cited_policy_document_dois", "cited_policy_dois"),
    ("__sub__source_function", "src_function"),
    ("__sub__source_sector", "src_sector"),
    ("__sub__source_type", "src_type"),
]

CANONICAL_NAMES = {short for _, short in SUFFIX_TO_SHORT_NAME}


@dataclass(frozen=True)
class AuditTarget:
    table: str
    parquet_path: Path
    parquet_stem: str
    estimated_rows: int
    physical_columns: list[str]
    audit_columns: list[str]
    excluded_columns: list[str]


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(message: str) -> None:
    print(f"{_utc_now()} {message}", flush=True)


def _read_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp_path, path)


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _canonical_name_for_stem(stem: str) -> str:
    normalized = stem.strip().lower()
    if normalized in CANONICAL_NAMES:
        return normalized
    hits = [short for suffix, short in SUFFIX_TO_SHORT_NAME if normalized.endswith(suffix)]
    if len(hits) == 1:
        return hits[0]
    return stem


def _dq(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _should_exclude(col: str, prefixes: list[str]) -> bool:
    return any(col.startswith(prefix) for prefix in prefixes if prefix)


def _discover_targets(parquet_dir: Path, *, exclude_prefixes: list[str], selected_tables: set[str]) -> list[AuditTarget]:
    targets: list[AuditTarget] = []
    for path in sorted(parquet_dir.glob("*.parquet")):
        stem = path.stem
        table = _canonical_name_for_stem(stem)
        if selected_tables and table not in selected_tables:
            continue
        pf = pq.ParquetFile(path)
        physical_columns = list(pf.schema_arrow.names)
        audit_columns = [col for col in physical_columns if not _should_exclude(col, exclude_prefixes)]
        excluded_columns = [col for col in physical_columns if col not in audit_columns]
        estimated_rows = int(pf.metadata.num_rows) if pf.metadata is not None else 0
        targets.append(
            AuditTarget(
                table=table,
                parquet_path=path.resolve(),
                parquet_stem=stem,
                estimated_rows=estimated_rows,
                physical_columns=physical_columns,
                audit_columns=audit_columns,
                excluded_columns=excluded_columns,
            )
        )
    targets.sort(key=lambda item: (item.estimated_rows, item.table))
    return targets


def _connect_duckdb(*, threads: int, temp_dir: Path | None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={int(threads)}")
    if temp_dir is not None:
        temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute("SET temp_directory = ?", [str(temp_dir)])
    return con


def _duplicate_summary(con: duckdb.DuckDBPyConnection, *, parquet_path: Path, columns: list[str]) -> tuple[int, int, int]:
    cols_sql = ", ".join(_dq(col) for col in columns)
    row = con.execute(
        f"""
        SELECT
          COALESCE(SUM(group_count - 1), 0) AS duplicate_rows,
          COUNT(*) AS duplicate_groups,
          COALESCE(SUM(group_count), 0) AS rows_in_duplicate_groups
        FROM (
          SELECT COUNT(*) AS group_count
          FROM read_parquet(?)
          GROUP BY {cols_sql}
          HAVING COUNT(*) > 1
        ) AS duplicate_groups
        """,
        [str(parquet_path)],
    ).fetchone()
    duplicate_rows = int(row[0] or 0) if row else 0
    duplicate_groups = int(row[1] or 0) if row else 0
    rows_in_duplicate_groups = int(row[2] or 0) if row else 0
    return duplicate_rows, duplicate_groups, rows_in_duplicate_groups


def _duplicate_sample(
    con: duckdb.DuckDBPyConnection,
    *,
    parquet_path: Path,
    columns: list[str],
    max_value_len: int = 200,
) -> dict[str, Any] | None:
    cols_sql = ", ".join(_dq(col) for col in columns)
    row = con.execute(
        f"""
        SELECT {cols_sql}, COUNT(*) AS duplicate_count
        FROM read_parquet(?)
        GROUP BY {cols_sql}
        HAVING COUNT(*) > 1
        LIMIT 1
        """,
        [str(parquet_path)],
    ).fetchone()
    if row is None:
        return None
    sample: dict[str, Any] = {}
    for idx, col in enumerate(columns):
        value = row[idx]
        if isinstance(value, str) and len(value) > max_value_len:
            value = value[:max_value_len] + "...<truncated>"
        sample[col] = value
    sample["duplicate_count"] = int(row[len(columns)] or 0)
    return sample


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet-dir", required=True, help="Directory containing one parquet file per Overton table")
    ap.add_argument("--output-jsonl", required=True)
    ap.add_argument("--state-file", required=True)
    ap.add_argument("--table", action="append", default=[], help="Only audit the given canonical table name; repeatable")
    ap.add_argument("--exclude-prefix", action="append", default=["__"], help="Column prefix to exclude from duplicate checks; repeatable")
    ap.add_argument("--threads", type=int, default=max(1, min(8, (os.cpu_count() or 4))))
    ap.add_argument("--temp-dir", default="", help="DuckDB temp spill directory")
    ap.add_argument("--sample", action="store_true", help="Fetch one duplicate sample row per table when duplicates exist")
    ap.add_argument("--reset", action="store_true", help="Reset state/output before starting")
    args = ap.parse_args()

    parquet_dir = Path(args.parquet_dir).expanduser().resolve()
    output_path = Path(args.output_jsonl).expanduser().resolve()
    state_path = Path(args.state_file).expanduser().resolve()
    temp_dir = Path(args.temp_dir).expanduser().resolve() if args.temp_dir else None

    if not parquet_dir.is_dir():
        raise SystemExit(f"parquet dir not found: {parquet_dir}")

    if args.reset:
        if output_path.exists():
            output_path.unlink()
        if state_path.exists():
            state_path.unlink()

    state = _read_state(state_path)
    completed = set(str(x) for x in (state.get("completed_tables") or []))
    selected_tables = {str(x).strip() for x in args.table if str(x).strip()}
    exclude_prefixes = [str(x) for x in args.exclude_prefix if str(x)]
    targets = _discover_targets(parquet_dir, exclude_prefixes=exclude_prefixes, selected_tables=selected_tables)

    _log(f"[audit] parquet_dir={parquet_dir} tables={len(targets)} threads={args.threads}")
    con = _connect_duckdb(threads=args.threads, temp_dir=temp_dir)
    try:
        for target in targets:
            if target.table in completed:
                _log(f"[audit] skip completed table={target.table}")
                continue
            if not target.audit_columns:
                payload = {
                    "table": target.table,
                    "parquet_path": str(target.parquet_path),
                    "parquet_stem": target.parquet_stem,
                    "estimated_rows": target.estimated_rows,
                    "physical_column_count": len(target.physical_columns),
                    "audit_column_count": 0,
                    "excluded_columns": target.excluded_columns,
                    "status": "no_audit_columns",
                    "audited_at_utc": _utc_now(),
                }
                _append_jsonl(output_path, payload)
                completed.add(target.table)
                _write_state(
                    state_path,
                    {
                        "parquet_dir": str(parquet_dir),
                        "completed_tables": sorted(completed),
                        "updated_at_utc": _utc_now(),
                    },
                )
                continue

            _log(
                f"[audit] start table={target.table} rows={target.estimated_rows} "
                f"audit_cols={len(target.audit_columns)} excluded={len(target.excluded_columns)}"
            )
            duplicate_rows, duplicate_groups, rows_in_duplicate_groups = _duplicate_summary(
                con,
                parquet_path=target.parquet_path,
                columns=target.audit_columns,
            )
            payload: dict[str, Any] = {
                "table": target.table,
                "parquet_path": str(target.parquet_path),
                "parquet_stem": target.parquet_stem,
                "estimated_rows": target.estimated_rows,
                "physical_column_count": len(target.physical_columns),
                "audit_column_count": len(target.audit_columns),
                "excluded_columns": target.excluded_columns,
                "duplicate_rows": duplicate_rows,
                "duplicate_groups": duplicate_groups,
                "rows_in_duplicate_groups": rows_in_duplicate_groups,
                "audited_at_utc": _utc_now(),
            }
            if args.sample and duplicate_rows > 0:
                payload["sample"] = _duplicate_sample(
                    con,
                    parquet_path=target.parquet_path,
                    columns=target.audit_columns,
                )
            _append_jsonl(output_path, payload)
            completed.add(target.table)
            _write_state(
                state_path,
                {
                    "parquet_dir": str(parquet_dir),
                    "completed_tables": sorted(completed),
                    "updated_at_utc": _utc_now(),
                },
            )
            _log(
                f"[audit] done table={target.table} duplicate_rows={duplicate_rows} "
                f"duplicate_groups={duplicate_groups}"
            )
        _log("[audit] completed")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
