# Chapter 6. Restart & Recovery

This chapter explains which checkpoints exist, what resume granularity is guaranteed, and what replay still happens after interruption.

## Parse stage

`parse-parquet*` runs leave progress in the run directory.
The important files are:

- `run_report.json.progress.json`
- optional external progress snapshots if you run external probes around the job

Operationally, parse resume is best understood as:

- source file or shard aware
- batch aware
- not row-level inside a batch

That means a restart usually replays at most the current batch rather than the full dataset.

## Materialize stage

`oa_materialize_parquet_to_db.py` stores progress at:

- `runs/<openalex_parse_run_dir>/parquet_materialize/progress.json`

The materializer now supports two resume granularities:

1. parquet file level by default
2. parquet file internal chunk level when `--file-chunk-rows N` is used

## Chunk-level resume

Example:

```bash
python scripts/oa_materialize_parquet_to_db.py \
  runs/<openalex_parse_run_dir> \
  --dotenv path/to/.env \
  --file-chunk-rows 5000
```

With this enabled, `progress.json` stores partial progress like:

- `partial_files.<table>.<file>.next_offset`
- `partial_files.<table>.<file>.chunk_rows`
- `partial_files.<table>.<file>.total_rows`

On restart, the loader continues from `next_offset` instead of replaying the whole parquet file.

## Practical guidance

- For small parquet files, chunk resume adds little value
- For large OpenAlex subtables, chunk resume is worth using
- Keep `file_chunk_rows` moderate so checkpoint frequency is useful without adding too much overhead

## Plan-driven reload stage

For multi-table operational reloads, prefer a plan file:

- template: `configs/parquet_reload_plan.template.json`
- run snapshot: `runs/<run_dir>/plans/parquet_reload_plan.json`
- status: `runs/<run_dir>/reports/parquet_reload_status_<tag>.json`

Run:

```bash
kisti-db-manager parquet preflight --plan runs/<run_dir>/plans/parquet_reload_plan.json
kisti-db-manager parquet reload --plan runs/<run_dir>/plans/parquet_reload_plan.json
```

The reload command also runs preflight by default. A failed preflight should be treated as a hard stop because it checks the target DB driver, permissions, object shape, planned reset risks, existing index definitions, and DuckDB-to-DB `LOAD DATA` round-trip behavior before any selected table is dropped.

Resume from a later table:

```bash
kisti-db-manager parquet reload --plan runs/<run_dir>/plans/parquet_reload_plan.json --start-at table_name
```

Finalization is blocked until every planned table is marked done in the reload status. If a partial resume is intentional, complete or mark the skipped tables before allowing the finalizer to run.

If materialization completed but validation had to be rerun manually, mark the table complete from a clean validation report:

```bash
kisti-db-manager parquet mark-table-done \
  --status runs/<run_dir>/reports/parquet_reload_status_<tag>.json \
  --table table_name \
  --validation-report runs/<run_dir>/reports/validate_table_name_<tag>.json
```
