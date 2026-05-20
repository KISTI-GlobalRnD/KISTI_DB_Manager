# CLI Quick Reference

This page lists the supported `kisti-db-manager` command surface. Source-checkout
scripts are compatibility or maintainer tools; see
[CLI and Script Boundaries](script-boundaries.md) for the distinction.

## General

```bash
kisti-db-manager version
kisti-db-manager modes
```

## Reports

```bash
kisti-db-manager report summary path/to/run_report.json
kisti-db-manager report diff before.json after.json --out diff.md
kisti-db-manager report profile path/to/run_report.json --top 10
```

## Quarantine

```bash
kisti-db-manager quarantine summary path/to/quarantine.jsonl --out quarantine_out
```

## Review

```bash
kisti-db-manager review pack --config path/to/config.json --report run_report.json --out review_out
kisti-db-manager review schema-viewer --config path/to/config.json --report run_report.json --out schema_viewer_out
kisti-db-manager review schema-viewer --config path/to/config.json --description-profile path/to/table_profile.json --out schema_viewer_out
kisti-db-manager review diff before_review.json after_review.json --out-dir review_diff_out
kisti-db-manager review preview --config path/to/config.json --out preview_out
kisti-db-manager review plan --config path/to/openalex_config.json --out plan_out
```

`review schema-viewer` automatically reads `<PATH>/<table_name>_profile.json`
when present, or accepts `--description-profile` explicitly. The profile
enriches column metadata with type confidence, null/unique ratios,
key/index recommendations, and warning badges.

## Tabular

```bash
kisti-db-manager tabular describe --config path/to/config.json
kisti-db-manager tabular profile-dataset --profiles "path/to/*_profile.json" --base-table works --out dataset_profile.json
kisti-db-manager tabular run --config path/to/config.json --report run_report.json
```

`tabular describe` writes a v2 `*_Desc.csv` plus `*_profile.json`. The CSV keeps
the compatibility fields used by tabular DB creation while adding type
confidence, null/empty ratios, length statistics, uniqueness, key/index
recommendations, and warnings. The JSON profile keeps source metadata, NameMap,
and detailed evidence for review and future RDB visualization.

`tabular profile-dataset` reads multiple per-table `*_profile.json` files and
writes `dataset_profile.json`. The v1 output is DB-free and conservative: it
summarizes tables and emits naming-path relationship candidates as review hints,
not confirmed foreign keys. `review schema-viewer --dataset-profile
dataset_profile.json` overlays those candidates on matching relationship cards;
if omitted, the viewer auto-detects `<PATH>/dataset_profile.json` when present.
The Schema Viewer overview summarizes table roles, candidate-backed
relationships, unmatched candidates, relation warnings, and disconnected
non-base tables. Candidate relationships between known tables that are not
covered by the structural naming tree are drawn as dashed candidate edges in
the SVG and Mermaid outputs.

## JSON

```bash
kisti-db-manager json run --config path/to/openalex_config.json --mode ingest-fast
kisti-db-manager json run --config path/to/openalex_config.json --mode finalize
kisti-db-manager json run --config path/to/openalex_config.json --mode parse-parquet-safe
kisti-db-manager json run --config path/to/openalex_config.json --mode parse-parquet-safe --id-compaction
kisti-db-manager json run --config path/to/openalex_config.json --mode parse-parquet-safe --flatten-backend rust-arrow
kisti-db-manager json run --config path/to/openalex_config.json --mode parse-parquet-safe --flatten-backend rust-arrow --id-compaction --chunk-size 10000
kisti-db-manager json run --config path/to/openalex_config.json --mode parse-parquet-safe --flatten-backend rust-arrow --rust-raw-jsonl-file-parse --rust-columnar-accumulator --chunk-size 500 --rust-parquet-flush-records 10000 --rust-parser-backend serde-json
kisti-db-manager json profile-parallel \
  --config path/to/openalex_config.json \
  --flatten-backends python,rust-arrow \
  --workers 0,2,4,8 \
  --max-records 20000 \
  --chunk-size 5000 \
  --repeat 3 \
  --out runs/profile_parallel_test
kisti-db-manager json id-compaction-preflight --config path/to/openalex_config.json --report id_compaction_preflight.json
```

`json run --flatten-backend auto|python|rust-arrow` selects the JSON parse/parquet backend. `auto` is the default: it uses the optional Rust Arrow/Parquet extension for supported parquet artifact runs when installed and falls back to Python otherwise. Explicit `rust-arrow` parse/parquet-only runs now use the Rust raw JSONL/GZ parser by default when the input is eligible; use `--no-rust-raw-jsonl-parse` to compare against the older Python JSON decoding path. For explicit `rust-arrow` parquet-first runs with `--id-compaction`, omitted `parallel_workers` defaults to `8`; pass `--parallel-workers 0` or another count to override it. For OpenAlex ID-compacted artifact runs, `--chunk-size 10000` is the current runbook recommendation because it halves parquet file count versus `5000` with essentially unchanged runtime in the retained 100k check. Add `--rust-raw-jsonl-file-parse` to let Rust read source JSONL/NDJSON files directly when supported. `--rust-parser-backend serde-json|simd-json` is an experimental parser selector for Rust raw JSONL paths; the default stays `serde-json`, and `simd-json` requires rebuilding with `python -m maturin develop --manifest-path crates/kisti_json_rs/Cargo.toml --release --features simd-json`. `--rust-columnar-accumulator` tests the opt-in Rust columnar accumulator, which avoids materializing full row maps before Arrow arrays are built; keep it profile-driven and disabled for ID compaction runs. `--rust-parquet-flush-records` lets direct Rust JSONL runs use a small `--chunk-size` for parser/flatten cache locality while writing parquet files after a larger record count. `--rust-parallel-table-writes` explicitly tests per-table parquet write parallelism; keep it evidence-based because it can trade lower parquet write time for more I/O contention. `--rust-db-load` opts into the experimental Rust MySQL insert path for Rust parquet artifacts while keeping table creation/schema mapping/index/optimize in Python. Build the optional extension with `pip install -e '.[json,db,rust]'` and `python -m maturin develop --manifest-path crates/kisti_json_rs/Cargo.toml --release`. For backend policy, limitations, smoke tests, and benchmarks, see [Rust Backend and Profiling](../architecture/rust-backend.md).

Operational Rust DB checks:

```bash
kisti-db-manager smoke rust-db-load --dotenv .env
kisti-db-manager openalex benchmark-load runs/example/parquet \
  --config runs/example/config.json \
  --loader rust-mysql \
  --report runs/example/rust_mysql_load_benchmark.json
kisti-db-manager openalex validate-reload runs/example \
  --config runs/example/config.json \
  --table works \
  --out runs/example/reload_validation.json
```

`json profile-parallel` compares JSON parse/parquet-only sample runs across `parallel_workers` values and optional `--flatten-backends` values. It disables DB stages, writes artifacts under `<out>/w<workers>/` for one backend or `<out>/<backend>/w<workers>/` for multiple backends, runs `parquet inspect`-style artifact contract checks, and creates `parallel_profile.json` plus `parallel_profile.md`. Use `--repeat N` to run each worker/backend setting multiple times; recommendations use median `records_per_s`. Eligible `rust-arrow` profile runs include the Rust raw JSONL/GZ parser by default; add `--no-rust-raw-jsonl-parse` to compare the older Python JSON decoding path, add `--rust-raw-jsonl-file-parse` to include direct Rust source-file reading, add `--rust-parser-backend simd-json` to run the experimental parser variant after building the Rust extension with the `simd-json` Cargo feature, add `--rust-columnar-accumulator` to test the columnar Rust accumulator, add `--rust-parquet-flush-records` to test larger parquet output flushes with smaller chunks, and add `--rust-parallel-table-writes` to test per-table parquet write parallelism. Rust profile summaries include detailed timing keys for source reading, number validation, table assembly, columnar merge, ID compaction, table write dispatch, Arrow array build, parquet file writing, `rust_arrow.unaccounted_ms` for remaining unattributed Rust time, and `rust_parser_fallbacks` for simd-to-serde parser fallback attempts. Add `--cleanup-parquet` when you want to keep reports/contracts but remove sample parquet directories.

`--id-compaction` currently supports the OpenAlex semantic column strip mode. It keeps compacted schemas stable across URL, bare ID, and null values, can run with `--parallel-workers`, and fails fast on conflicting nonblank values that would collapse into the same output column. Explicit `rust-arrow` parquet-first runs apply the scoped `parallel_workers=8` default only when no worker count was supplied. Use `--chunk-size 10000` for the current OpenAlex ID-compacted Rust artifact runbook; `20000` should stay profile-only because it was slower in the retained 100k check. The conflict policies can be set with `--id-compaction-collision-policy error|preserve` and `--id-compaction-namespace-conflict-policy error|preserve`.

`json id-compaction-preflight` scans JSON input before a long run and reports compacted-column collisions, namespace conflicts, ambiguous URL-like columns, source examples, and the effective production policies. Use `--max-records 0` for a full scan.

## Parquet materialize helper

```bash
kisti-db-manager openalex materialize \
  runs/<openalex_parse_run_dir> \
  --dotenv path/to/.env \
  --db-name target_openalex_db \
  --materialize-preset openalex-idcompact-fast
```

`--materialize-preset openalex-idcompact-fast` expands to the measured OpenAlex ID-compacted starting point: DuckDB staging, `LOAD DATA LOCAL INFILE`, `--parallel-tables 6`, `--parallel-files-per-table 2`, `--require-schema-manifest`, and `--require-id-compaction`. Explicit CLI options still override the preset's load/staging/parallel values.

## Parquet reload plan

```bash
kisti-db-manager parquet inspect \
  --parquet-root runs/<run_dir>/parquet \
  --require-schema-manifest \
  --require-id-compaction
kisti-db-manager parquet preflight --plan runs/<run_dir>/plans/parquet_reload_plan.json
kisti-db-manager parquet reload --plan runs/<run_dir>/plans/parquet_reload_plan.json
kisti-db-manager parquet reload --plan runs/<run_dir>/plans/parquet_reload_plan.json --start-at table_name
kisti-db-manager parquet mark-table-done \
  --status runs/<run_dir>/reports/parquet_reload_status_<tag>.json \
  --table table_name \
  --validation-report runs/<run_dir>/reports/validate_table_name_<tag>.json
kisti-db-manager parquet finalize --plan runs/<run_dir>/plans/parquet_reload_plan.json
```

`parquet inspect` validates the parquet artifact contract before DB work: `schema_manifest.json`, ID compaction provenance, `rules_hash`, selected table schemas, and mixed source/compacted ID columns. Add `--strict-schema-manifest` to fail on manifest/parquet mismatches.

Source-checkout compatibility script wrappers are also available, but new
operator instructions should prefer the `kisti-db-manager` commands above:

```bash
python scripts/oa_materialize_parquet_to_db.py runs/<openalex_parse_run_dir> --dotenv path/to/.env
python scripts/oa_validate_serving_reload.py runs/<run_dir> --table works --out runs/<run_dir>/reload_validation.json
python scripts/parquet_reload_plan.py run --plan runs/<run_dir>/plans/parquet_reload_plan.json
python scripts/parquet_preflight_db.py --plan runs/<run_dir>/plans/parquet_reload_plan.json
python scripts/parquet_finalize_db.py --plan runs/<run_dir>/plans/parquet_reload_plan.json
```

`parquet reload` runs target DB preflight by default. Use `--skip-preflight` only after separately reviewing a clean preflight report.
The preflight report includes the same parquet artifact contract check under `checks.parquet_artifacts`; plan-level `preflight.artifact_contract.require_schema_manifest`, `require_id_compaction`, and `strict_schema_manifest` make these checks blocking.
For large reload plans, `validation.profile=large` defaults to row-count validation and skips expensive literal marker full scans unless `validation.literal_marker.mode` is set to `full` or `columns`.
`db.driver=postgresql` is supported for preflight diagnostics only; reload/finalize remain MariaDB/MySQL-backed.
