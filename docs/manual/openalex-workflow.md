# Chapter 4. OpenAlex Example Workflow

This chapter uses OpenAlex as the public example dataset for the package: preflight, parse-parquet, materialize, and the current best-known load path.

## Recommended default

For current OpenAlex work, use a two-stage flow.

1. `parse-parquet-safe` to create canonical local parquet artifacts
2. `oa_materialize_parquet_to_db.py` to load selected tables or the full set into MariaDB later

## Why this split exists

OpenAlex has large nested branches and some branches are operationally better treated as local analytical artifacts first.
A canonical parquet layer makes restart, downstream local work, and later DB materialization much easier.

## Preflight for explosive dict branches

If a path looks like `abstract_inverted_index`, run a plan first.

```bash
kisti-db-manager review plan \
  --config path/to/json_config.json \
  --auto-except \
  --auto-except-sample-records 5000 \
  --auto-except-sample-max-sources 64 \
  --out plan_out
```

Then run the parse:

```bash
kisti-db-manager json run \
  --config path/to/json_config.json \
  --mode parse-parquet-safe \
  --auto-except
```

## Materialize into DB later

```bash
python scripts/oa_materialize_parquet_to_db.py \
  runs/<parse_parquet_run_dir> \
  --dotenv .env \
  --db-name openalex_20260225_raw_yjk \
  --staging-writer duckdb \
  --parallel-tables 4 \
  --parallel-files-per-table 4 \
  --file-chunk-rows 5000
```

## Current best-known DB load path

In this repository, the fastest practical materialization path is:

`parquet -> DuckDB staging -> LOAD DATA LOCAL INFILE`

Use:

- `--staging-writer duckdb`
- `--parallel-tables N`
- `--parallel-files-per-table N`
- `--file-chunk-rows N` for finer restart granularity on large parquet files

## When to go DB-first instead

If the real target is simply “finish raw DB ingest as soon as possible,” then `ingest-fast*` is still the simpler operational path.
If the real target is “local artifacts first, DB later,” keep the parquet-first flow.
