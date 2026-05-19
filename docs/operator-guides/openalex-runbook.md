# OpenAlex Runbook

This chapter uses OpenAlex as the public example dataset for the package: preflight, parse-parquet, materialize, and the current best-known load path.

## Recommended default

For current OpenAlex work, use a two-stage flow.

1. `parse-parquet-safe` to create canonical local parquet artifacts
2. `oa_materialize_parquet_to_db.py` to load selected tables or the full set into MariaDB later

For the current Rust ID-compacted artifact path, start with:

```bash
kisti-db-manager json run \
  --config path/to/openalex_config.json \
  --mode parse-parquet-safe \
  --flatten-backend rust-arrow \
  --id-compaction \
  --chunk-size 10000
```

This keeps `parallel_workers` omitted so the scoped `rust-arrow` ID-compaction default applies (`8`), while using the retained 100k chunk-size check's file-count recommendation.

## Why this split exists

OpenAlex has large nested branches and some branches are operationally better treated as local analytical artifacts first.
A canonical parquet layer makes restart, downstream local work, and later DB materialization much easier.

## Preflight for explosive dict branches

If a path looks like `abstract_inverted_index`, run a plan first.

```bash
kisti-db-manager review plan \
  --config path/to/openalex_config.json \
  --auto-except \
  --auto-except-sample-records 5000 \
  --auto-except-sample-max-sources 64 \
  --out plan_out
```

Then run the parse:

```bash
kisti-db-manager json run \
  --config path/to/openalex_config.json \
  --mode parse-parquet-safe \
  --auto-except
```

## Materialize into DB later

```bash
uv run python scripts/oa_materialize_parquet_to_db.py \
  runs/<openalex_parse_run_dir> \
  --dotenv path/to/.env \
  --db-name target_openalex_db \
  --materialize-preset openalex-idcompact-fast
```

For repeated operational reloads, use the plan-driven wrapper instead of a one-off shell loop:

```bash
kisti-db-manager parquet inspect \
  --parquet-root runs/<openalex_parse_run_dir>/parquet \
  --require-schema-manifest \
  --require-id-compaction
kisti-db-manager parquet preflight \
  --plan runs/<openalex_parse_run_dir>/plans/parquet_reload_plan.json
kisti-db-manager parquet reload \
  --plan runs/<openalex_parse_run_dir>/plans/parquet_reload_plan.json
```

Use `configs/openalex_20260330_nonworks_reload_plan.example.json` as the OpenAlex 20260330 example and store the concrete run copy under the run directory.
The reload wrapper runs preflight by default and includes the parquet artifact contract check in that preflight. For ID-compacted runs, set `preflight.artifact_contract.require_schema_manifest=true` and `require_id_compaction=true` in the plan so old non-compacted parquet cannot be loaded by mistake. The wrapper uses the large validation profile, which keeps post-load validation to row counts unless a literal marker scan is explicitly enabled.

## Current best-known DB load path

In this repository, the fastest practical materialization path is:

`parquet -> DuckDB staging -> LOAD DATA LOCAL INFILE`

Use:

- `--materialize-preset openalex-idcompact-fast` for the current OpenAlex ID-compacted starting point
- `--staging-writer duckdb`
- `--parallel-tables N`
- `--parallel-files-per-table N`
- `--file-chunk-rows N` for finer restart granularity on large parquet files

The retained 100k OpenAlex source materializer comparison loaded `4,733,457` rows across `194` parquet files. The serial materializer took `23.64s`; `--parallel-tables 6 --parallel-files-per-table 2` took `7.99s`; increasing file parallelism to `3` regressed to `8.63s`. Treat the preset as an operational starting point, not a universal DB default; shared or smaller DBs should still profile locally.

## Delta snapshot merge

For snapshot refresh work, keep the merge logic inside the package rather than as one-off shell code.

- Package submodule: `KISTI_DB_Manager.parquet_delta_merge`
- Thin wrappers:
  - `scripts/oa_merge_parquet_snapshot_delta.py`
  - `scripts/oa_watch_delta_parse_and_merge.py`

Current recommendation for OpenAlex delta refresh is:

1. finish `parse-parquet-safe` for the new `updated_date` range
2. build `delta_ids`
3. merge by `id` at the parquet layer
4. materialize to DB only after the merged parquet snapshot is stable

For large OpenAlex snapshots, prefer `tablewise` merge instead of `filewise`.
The filewise path is only appropriate when most base parquet files are guaranteed to be untouched.

## When to go DB-first instead

If the real target is simply “finish raw DB ingest as soon as possible,” then `ingest-fast*` is still the simpler operational path.
If the real target is “local artifacts first, DB later,” keep the parquet-first flow.
