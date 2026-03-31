# OpenAlex Parquet Materializer Plan (2026-03-31)

- Goal: keep `parse-parquet*` parquet artifacts as the canonical raw layer, and run `MariaDB/MySQL` load as a separate materialization stage afterward.
- Background: the package is strong at `raw JSON -> flatten -> DB`, but it did not have an independent `parquet -> DB` module. That made it difficult to satisfy both local-first work and full raw DB loading.

## Conclusion

- Run `raw -> flatten/parquet` only once.
- From that canonical parquet output:
  - start local analytical work immediately
  - let the `parquet materializer` handle DB loading later
- In other words, parsing happens once and materialization has two consumers.

## Goals

- Allow DB loading from parquet without reparsing raw JSON
- Support resume at table/batch granularity
- Allow selective loading of only some tables
- Treat generated parquet as the canonical artifact
- Record load results and progress in JSON report/progress files

## Non-Goals

- Making MariaDB read parquet directly
- Optimizing parquet and MariaDB row-store in the same single pass
- Replacing the full existing `json run` path immediately

## Why a Separate Module Is Needed

A naive `parquet -> DB` implementation currently becomes:

1. read parquet
2. build a `DataFrame`
3. generate a temporary TSV
4. `LOAD DATA LOCAL INFILE`

This works functionally, but it is expensive at full raw OpenAlex scale. The `parquet materializer` therefore needs to take explicit responsibility for two things:

- performing DB loading without reparsing raw JSON
- managing load units at table/file level so restart and selective loading stay practical

## MVP Scope

### Inputs

- parquet root
- `config.json` from the original run
- optional `.env` path
- optional table allowlist

### Outputs

- target DB tables
- `progress.json`
- `run_report.json`

### Behavior

1. Scan table directories under the parquet root
2. Process `b*.parquet` files in each table directory in order
3. Create the target table from the first file
4. Read each parquet file and load it batch by batch
5. Save file-level checkpoints
6. On restart, resume after the last completed file

## Operational Requirements

- If `config.json` masks the password as `***`, it must be recoverable from `.env`
- Support target table prefixes
- Default to append rather than overwrite
- Provide `--table` for partial loading
- Do not `DROP/RECREATE` by default

## Recommended Progress Contract

Example `progress.json`:

```json
{
  "updated_at_utc": "2026-03-31T00:00:00+00:00",
  "parquet_root": "/raid/.../parquet_exports/openalex_works_20260225_raw_xxx",
  "table_count": 22,
  "tables_completed": 3,
  "files_loaded": 128,
  "rows_loaded": 12345678,
  "current": {
    "table_original": "openalex_works_20260225__concepts",
    "table_sql": "openalex_works_20260225__concepts",
    "file_path": "/raid/.../concepts/b000127.parquet",
    "rows": 35439
  },
  "completed_files": {
    "openalex_works_20260225": ["b000000.parquet", "b000001.parquet"]
  }
}
```

## MVP Implementation Principle

- existing `manage.create_table_from_columns`
- existing `manage.fill_table_from_dataframe`
- existing `LOAD DATA LOCAL INFILE`

should all be reused.

So the MVP is not a completely new DB loader.
It is **a materializer that reorganizes the existing load logic around parquet artifacts**.

## Later Optimization Stages

### Phase 2

- go beyond file-level resume and optimize per-table high-watermarks
- add a path that reads `name_maps_json` directly from the final report
- parallelize table loading
- add large-table priority policy

### Phase 3

- reduce memory use through parquet row-group/chunk handling
- experiment with DuckDB/pyarrow staged CSV/TSV generation
- promote the materializer to a dedicated CLI subcommand

## Decision Rule

- If local work is the priority, keep `parse-parquet*`
- If DB completion speed is the priority, `ingest-fast*` remains stronger
- If both need to be satisfied, the package needs a `parquet materializer`

The conclusion of this document is simple:

- `parquet -> MariaDB` is no longer just a consumer-side convenience; it should be treated as a core module capability.
