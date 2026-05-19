# JSON Pipeline Architecture

The JSON pipeline has two major operational directions.

## DB-First

DB-first modes prioritize completing MariaDB/MySQL ingest.

- `ingest-fast`
- `ingest-fast-freeze`
- `ingest-fast-hybrid`
- `ingest-safe`
- `finalize`

Use this direction when DB completion time is the primary goal and local parquet reuse is secondary.

## Artifact-First

Artifact-first modes prioritize local parquet artifacts.

- `parse-parquet`
- `parse-parquet-safe`

Use this direction when restartability, downstream reuse, inspection, or staged DB materialization matters.

## Shared Responsibilities

Both directions use the same core responsibilities:

- JSON source iteration
- nested flattening into base/subtable structures
- name mapping and MySQL identifier truncation
- schema drift handling
- run reports and quarantine output

The artifact-first path additionally produces parquet artifacts and schema manifests before DB materialization.

## Where to Look in Code

- `KISTI_DB_Manager/pipeline.py`: top-level pipeline orchestration
- `KISTI_DB_Manager/_pipeline/`: runtime and JSON source helpers
- `KISTI_DB_Manager/rust_arrow_backend.py`: optional Rust Arrow bridge
- `KISTI_DB_Manager/json_parallel_profile.py`: profile and comparison workflow

For mode selection, see [JSON Modes](../reference/modes.md).
