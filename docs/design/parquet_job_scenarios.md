# Scenario-Based Large Parquet Jobs

!!! note "Design status"
    This is a scenario design note for large parquet jobs. It is useful background for shared execution engines,
    but current operator-facing commands remain in [OpenAlex Example Workflow](../manual/openalex-workflow.md)
    and [CLI Quick Reference](../reference/cli.md).

This note defines the execution model we want for large parquet processing jobs in this repository.
The key rule is that the framework should be scenario-driven, not dataset-driven.

## Problem

Several operational scripts in this repo process datasets that are too large for naive global queries.
The failure mode is consistent:

- one global `read_parquet(...)` query over too many files
- large `UNNEST`, `DISTINCT`, or `GROUP BY`
- heavy spill
- weak restart points
- poor observability when a long-running subprocess disappears

This showed up most clearly in the first `works_affiliation_agg` implementation, but the pattern is not specific to OpenAlex.

## Principle

The reusable layer should express **job scenarios**.
The dataset-specific module should only provide:

- source locations
- key columns
- query fragments or adapters
- output naming

The engine should own:

- batching
- bucketing
- temp/spill policy
- progress / failure reporting
- resumability boundaries

## Scenario Types

### 1. `direct_materialize`

Use for straightforward parquet to DB or parquet to parquet transfer.

Characteristics:

- no heavy explode
- no global dedup
- no bucket stage

Current examples:

- `scripts/oa_materialize_parquet_to_db.py`
- most `works_*` child table loads

### 2. `dedup_by_key`

Use when exact or keyed duplicate removal is the main operation.

Characteristics:

- full scan
- key-based filtering or anti-join
- one rewritten output dataset

Current examples:

- `scripts/oa_dedup_merged_main.py`
- `KISTI_DB_Manager/parquet_replay_repair.py`

### 3. `delta_merge`

Use when a base snapshot must be updated from a delta snapshot.

Characteristics:

- base + delta keyed reconciliation
- row replacement / carry-forward semantics
- partition or table-wise execution

Current examples:

- `KISTI_DB_Manager/parquet_delta_merge.py`

### 4. `bucketed_transform`

Use when the source must be flattened or transformed before the final aggregation step.

Characteristics:

- process source files in batches
- emit intermediate bucketed parquet
- defer expensive grouping to bucket-local queries

Current examples:

- `works_affiliation_agg`

### 5. `bucketed_compare`

Use for large diff/audit workloads where key-aligned comparison is too large for a single global query.

Characteristics:

- source A / source B spill by key bucket
- bucket-local comparison
- merge summaries at the end

Likely future examples:

- materialized change tables
- very large orphan / consistency audits

## Generic Engine Boundary

The generic module should expose a small number of primitives.

### Run State

Shared concerns:

- atomic `progress.json`
- final `summary.json`
- failure payload with phase and counters
- stale-run detection

This should move into a reusable run-state utility.

### Bucketed Job Engine

Shared concerns:

- source file batching
- `hash(key) % bucket_count` routing
- temp pair dataset writing
- bucket-local reduce
- final output writing

The job definition should be parameterized by:

- `source_dir`
- `out_dir`
- `temp_dir`
- `threads`
- `memory_limit`
- `source_batch_files`
- `bucket_count`
- `max_rows_per_file`
- one or more phase-1 query builders
- one phase-2 reduce query builder

The reduce query builder must also tolerate **missing bucket inputs**.
For example, a given bucket may contain `inst` rows but no `raw_aff` rows.
The generic engine should pass that information explicitly instead of assuming every intermediate dataset exists for every bucket.

## Current Refactor

The first reusable engine has been introduced as:

- `KISTI_DB_Manager/bucketed_jobs.py`

It provides:

- `BucketedPairSpec`
- `BucketedDuckDBJobSpec`
- `run_bucketed_duckdb_job(...)`

The OpenAlex-specific `works_affiliation_agg` builder now delegates to this engine instead of holding its own execution loop.

## Mapping of Existing Scripts

| Script / Module | Scenario |
| --- | --- |
| `scripts/oa_materialize_parquet_to_db.py` | `direct_materialize` |
| `scripts/oa_dedup_merged_main.py` | `dedup_by_key` |
| `KISTI_DB_Manager/parquet_replay_repair.py` | `dedup_by_key` |
| `KISTI_DB_Manager/parquet_delta_merge.py` | `delta_merge` |
| `KISTI_DB_Manager/openalex_serving.py::build_works_affiliation_agg` | `bucketed_transform` |
| `KISTI_DB_Manager/openalex_change_tables.py` | candidate for `bucketed_compare` |
| `KISTI_DB_Manager/openalex_change_report.py` | candidate for `bucketed_compare` |

## Next Refactors

Priority order:

1. extract shared run-state helpers
2. move another heavy job onto the bucketed engine
3. formalize job profiles for scenario selection
4. add operational thresholds for spill/temp growth

The goal is not to make SQL generic.
The goal is to make **execution strategy generic**.
