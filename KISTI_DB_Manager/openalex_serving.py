from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .bucketed_jobs import BucketedDuckDBJobSpec, BucketedPairSpec, run_bucketed_duckdb_job
from .runstate import atomic_write_json, utc_now_iso


CANONICAL_PREFIX_0330 = "openalex_works_20260330"
ABSTRACT_INDEX_TABLE = f"{CANONICAL_PREFIX_0330}__excepted__abstract_inverted_index"
RECONSTRUCTED_ABSTRACT_TABLE = "works_abstract"
AFFILIATION_AGG_TABLE = "works_affiliation_agg"
CORE_TABLE_ORDER = (
    "works",
    "works_abstract",
    "works_authorships",
    "works_affiliation_agg",
)
TEXT_HEAVY_TABLES = {
    "works",
    "works_abstract",
    "works_authorships",
    "works_affiliation_agg",
}


@dataclass(frozen=True)
class ServingTableSpec:
    source_table: str
    target_table: str
    source_dir: str
    stage_writer: str
    file_chunk_rows: int
    generated: bool = False
    selected: bool = True


def canonical_to_serving_table_name(
    table_name: str,
    *,
    canonical_prefix: str = CANONICAL_PREFIX_0330,
) -> str | None:
    name = str(table_name).strip()
    if not name:
        raise ValueError("table_name is required")
    if name == canonical_prefix:
        return "works"
    if name == f"{canonical_prefix}__excepted__abstract_inverted_index":
        return None
    needle = f"{canonical_prefix}__"
    if not name.startswith(needle):
        raise ValueError(f"unexpected canonical table name: {name}")
    suffix = name[len(needle) :]
    return f"works_{suffix.replace('__', '_')}"


def table_has_nested_fields(table_dir: Path) -> bool:
    import pyarrow.parquet as pq

    first_parquet = next(iter(sorted(table_dir.glob("*.parquet"))), None)
    if first_parquet is None:
        return False
    schema = pq.ParquetFile(first_parquet).schema_arrow
    for field in schema:
        if field.type.num_fields > 0 or getattr(field.type, "id", None) in {
            "list",
            "large_list",
            "fixed_size_list",
            "struct",
            "map",
        }:
            return True
        text = str(field.type).lower()
        if text.startswith(("list<", "large_list<", "struct<", "map<")):
            return True
    return False


def preferred_stage_writer(table_dir: Path, *, target_table: str) -> str:
    if target_table in {"works", RECONSTRUCTED_ABSTRACT_TABLE, AFFILIATION_AGG_TABLE}:
        return "duckdb"
    return "python" if table_has_nested_fields(table_dir) else "duckdb"


def preferred_file_chunk_rows(*, target_table: str) -> int:
    if target_table == RECONSTRUCTED_ABSTRACT_TABLE:
        return 50_000
    if target_table in TEXT_HEAVY_TABLES:
        return 100_000
    return 250_000


def build_serving_table_specs(
    *,
    snapshot_root: Path,
    abstract_parquet_dir: Path,
    aff_agg_parquet_dir: Path,
    canonical_prefix: str = CANONICAL_PREFIX_0330,
) -> list[ServingTableSpec]:
    specs: list[ServingTableSpec] = []
    for table_dir in sorted([p for p in snapshot_root.iterdir() if p.is_dir()]):
        target_table = canonical_to_serving_table_name(table_dir.name, canonical_prefix=canonical_prefix)
        if not target_table:
            continue
        specs.append(
            ServingTableSpec(
                source_table=table_dir.name,
                target_table=target_table,
                source_dir=str(table_dir),
                stage_writer=preferred_stage_writer(table_dir, target_table=target_table),
                file_chunk_rows=preferred_file_chunk_rows(target_table=target_table),
            )
        )

    specs.append(
        ServingTableSpec(
            source_table="works_abstract_parquet",
            target_table=RECONSTRUCTED_ABSTRACT_TABLE,
            source_dir=str(abstract_parquet_dir),
            stage_writer="duckdb",
            file_chunk_rows=preferred_file_chunk_rows(target_table=RECONSTRUCTED_ABSTRACT_TABLE),
            generated=True,
        )
    )
    specs.append(
        ServingTableSpec(
            source_table=AFFILIATION_AGG_TABLE,
            target_table=AFFILIATION_AGG_TABLE,
            source_dir=str(aff_agg_parquet_dir),
            stage_writer="duckdb",
            file_chunk_rows=preferred_file_chunk_rows(target_table=AFFILIATION_AGG_TABLE),
            generated=True,
        )
    )

    def sort_key(spec: ServingTableSpec) -> tuple[int, str]:
        try:
            return (0, str(CORE_TABLE_ORDER.index(spec.target_table)).zfill(2))
        except ValueError:
            return (1, spec.target_table)

    return sorted(specs, key=sort_key)


def build_serving_symlink_layout(
    *,
    snapshot_root: Path,
    abstract_parquet_dir: Path,
    aff_agg_parquet_dir: Path,
    layout_root: Path,
    canonical_prefix: str = CANONICAL_PREFIX_0330,
) -> list[ServingTableSpec]:
    layout_root.mkdir(parents=True, exist_ok=True)
    specs = build_serving_table_specs(
        snapshot_root=snapshot_root,
        abstract_parquet_dir=abstract_parquet_dir,
        aff_agg_parquet_dir=aff_agg_parquet_dir,
        canonical_prefix=canonical_prefix,
    )
    for spec in specs:
        target_path = layout_root / spec.target_table
        if target_path.exists() or target_path.is_symlink():
            target_path.unlink()
        target_path.symlink_to(Path(spec.source_dir).resolve(), target_is_directory=True)
    atomic_write_json(
        layout_root / "serving_manifest.json",
        {
            "generated_at": utc_now_iso(),
            "layout_root": str(layout_root),
            "specs": [asdict(spec) for spec in specs],
        },
    )
    return specs


def build_works_affiliation_agg(
    *,
    source_dir: Path,
    out_dir: Path,
    temp_dir: Path | None = None,
    threads: int = 8,
    memory_limit: str = "64GB",
    max_rows_per_file: int = 1_000_000,
    source_batch_files: int = 8,
    bucket_count: int = 256,
    resume: bool = True,
) -> dict[str, Any]:
    def _build_inst_query(batch_files: list[Path], normalized_bucket_count: int) -> str:
        read_expr = f"read_parquet({json.dumps([str(path) for path in batch_files])}, union_by_name=true)"
        return f"""
            WITH source_rows AS (
              SELECT
                id,
                authorships__institutions
              FROM {read_expr}
            )
            SELECT
              CAST(hash(id) % {normalized_bucket_count} AS INTEGER) AS bucket,
              id,
              inst.display_name AS display_name
            FROM source_rows, UNNEST(authorships__institutions) AS u(inst)
            WHERE COALESCE(TRIM(inst.display_name), '') <> ''
            GROUP BY 1, 2, 3
        """

    def _build_raw_query(batch_files: list[Path], normalized_bucket_count: int) -> str:
        read_expr = f"read_parquet({json.dumps([str(path) for path in batch_files])}, union_by_name=true)"
        return f"""
            WITH source_rows AS (
              SELECT
                id,
                authorships__raw_affiliation_strings
              FROM {read_expr}
            )
            SELECT
              CAST(hash(id) % {normalized_bucket_count} AS INTEGER) AS bucket,
              id,
              raw_aff
            FROM source_rows, UNNEST(authorships__raw_affiliation_strings) AS u(raw_aff)
            WHERE COALESCE(TRIM(raw_aff), '') <> ''
            GROUP BY 1, 2, 3
        """

    def _build_reduce_query(bucket_inputs: dict[str, str | None]) -> str:
        inst_glob = bucket_inputs["inst"]
        raw_glob = bucket_inputs["raw"]
        inst_sql = (
            f"""
              SELECT
                id,
                string_agg(DISTINCT display_name, '; ' ORDER BY display_name) AS institution_names,
                count(DISTINCT display_name) AS institutions_distinct_count
              FROM read_parquet({json.dumps(inst_glob)}, union_by_name=true)
              GROUP BY 1
            """
            if inst_glob
            else """
              SELECT
                CAST(NULL AS VARCHAR) AS id,
                CAST(NULL AS VARCHAR) AS institution_names,
                CAST(NULL AS BIGINT) AS institutions_distinct_count
              WHERE FALSE
            """
        )
        raw_sql = (
            f"""
              SELECT
                id,
                string_agg(DISTINCT raw_aff, '; ' ORDER BY raw_aff) AS raw_affiliation_strings
              FROM read_parquet({json.dumps(raw_glob)}, union_by_name=true)
              GROUP BY 1
            """
            if raw_glob
            else """
              SELECT
                CAST(NULL AS VARCHAR) AS id,
                CAST(NULL AS VARCHAR) AS raw_affiliation_strings
              WHERE FALSE
            """
        )
        return f"""
            WITH inst_agg AS (
              {inst_sql}
            ),
            raw_agg AS (
              {raw_sql}
            )
            SELECT
              COALESCE(i.id, r.id) AS id,
              regexp_extract(COALESCE(i.id, r.id), '[^/]+$') AS oaid_w,
              COALESCE(NULLIF(i.institution_names, ''), r.raw_affiliation_strings) AS institution_names,
              r.raw_affiliation_strings,
              COALESCE(i.institutions_distinct_count, 0) AS institutions_distinct_count
            FROM inst_agg i
            FULL OUTER JOIN raw_agg r USING (id)
            WHERE COALESCE(NULLIF(i.institution_names, ''), NULLIF(r.raw_affiliation_strings, '')) IS NOT NULL
        """

    job = BucketedDuckDBJobSpec(
        source_dir=source_dir,
        out_dir=out_dir,
        temp_dir=temp_dir,
        threads=int(threads),
        memory_limit=str(memory_limit),
        max_rows_per_file=int(max_rows_per_file),
        source_batch_files=int(source_batch_files),
        bucket_count=int(bucket_count),
        resume=bool(resume),
        pair_specs=(
            BucketedPairSpec(name="inst", build_batch_query=_build_inst_query),
            BucketedPairSpec(name="raw", build_batch_query=_build_raw_query),
        ),
        build_reduce_query=_build_reduce_query,
    )
    return run_bucketed_duckdb_job(job)
