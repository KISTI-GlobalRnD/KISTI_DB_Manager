# Chapter 4. Rust Backend and Profiling

This chapter covers the optional Rust extension for JSON parse/parquet work and the experimental Rust MySQL loader.
Use it when you need evidence-based backend selection or want to test the Rust load path before using it on production data.

## What Rust Owns

The Rust extension is intentionally scoped.

- JSON record flattening for supported parquet artifact runs
- Optional raw JSONL parsing inside Rust for parse/parquet-only runs
- Optional direct JSONL/NDJSON source-file reading inside Rust for supported parse/parquet-only runs
- Optional per-table parallel parquet writes inside Rust
- Optional columnar accumulator for Rust flatten/parquet output
- Parquet writing through Arrow
- OpenAlex `semantic_column_strip` ID compaction in the Rust path
- Optional parquet-to-MySQL batch insert when `--rust-db-load` is enabled

The Python pipeline still owns table creation, name mapping, schema comments, index creation, optimize, reports, quarantine, and fallback policy.

## Build

Install Python dependencies and build the local Rust extension:

```bash
pip install -e '.[json,db,rust]'
python -m maturin develop --manifest-path crates/kisti_json_rs/Cargo.toml --release
```

During development, the non-release build is enough for correctness checks:

```bash
python -m maturin develop --manifest-path crates/kisti_json_rs/Cargo.toml
```

## Backend Selection

`json run` accepts three backend values:

- `auto`: default; use Rust when supported, otherwise fall back to Python
- `python`: force the mature Python path
- `rust-arrow`: force Rust and fail if the Rust path is unsupported

For parquet-first runs:

```bash
kisti-db-manager json run \
  --config path/to/openalex_config.json \
  --mode parse-parquet-safe \
  --flatten-backend rust-arrow
```

For plain JSONL/NDJSON parse/parquet-only runs, you can also bypass Python JSON decoding and parse raw lines in Rust:

```bash
kisti-db-manager json run \
  --config path/to/openalex_config.json \
  --mode parse-parquet-safe \
  --flatten-backend rust-arrow \
  --rust-raw-jsonl-parse
```

This path is opt-in and intentionally narrow. It requires `flatten_backend=rust-arrow`, `persist_parquet_files=true`, no DB/create/index/optimize stages, no `records_key`, and plain `.jsonl`/`.ndjson` input. Reports include `rust_raw_jsonl_parse_effective` and the Rust parser timing key `rust_arrow.json_parse` when measurable.

Rust run reports also expose lower-level timing keys when measurable: `rust_arrow.read_line`, `rust_arrow.number_validate`, `rust_arrow.columnar_merge`, `rust_arrow.arrow_build`, `rust_arrow.parquet_write`, and `rust_arrow.py_result_convert`. Use these with `json.flatten`, `json.parquet.persist`, and `rust_arrow.total` to identify whether the next bottleneck is source reading, JSON decoding, flattening, Arrow array construction, parquet output, or Python result conversion.

For the narrowest fast path, add `--rust-raw-jsonl-file-parse` as well. That keeps the same parse/parquet-only constraints and bypasses Python's line-reading loop. It currently falls back to the batch raw parser when ID compaction is enabled.

`--rust-parallel-table-writes` is also available for profiling table-level parquet write parallelism. Keep it opt-in: on some inputs it lowers `json.parquet.persist` but can still reduce total throughput due to I/O contention.

`--rust-columnar-accumulator` is an opt-in Rust parquet path that accumulates column data directly instead of materializing full row maps before Arrow arrays are built. It is intended for profile-driven optimization of parse/parquet-only `rust-arrow` runs. Keep it disabled for ID compaction runs; the current ID compaction path still uses the mature row accumulator so manifest and compaction accounting stay unchanged.

For direct Rust JSONL file parsing, `--rust-parquet-flush-records N` decouples the parser/flatten micro-batch size from parquet output batch size. For example, `--chunk-size 500 --rust-parquet-flush-records 10000` keeps the small-cache-friendly flatten path but writes far fewer parquet files than flushing every 500 records. The option only affects the direct Rust JSONL file path; `0` or omission keeps the legacy `chunk_size` flush behavior.

For backend comparison:

```bash
kisti-db-manager json profile-parallel \
  --config path/to/openalex_config.json \
  --flatten-backends python,rust-arrow \
  --workers 0,2,4,8 \
  --max-records 20000 \
  --chunk-size 5000 \
  --repeat 3 \
  --out runs/profile_parallel_test
```

`json profile-parallel` keeps per-run reports and parquet artifacts by default.
Add `--rust-raw-jsonl-parse --rust-raw-jsonl-file-parse` when you want the `rust-arrow` profile runs to include the raw JSONL parser and direct file reader fast paths.
Add `--rust-parallel-table-writes` only when you want to test table-level parquet write parallelism explicitly.
Add `--rust-columnar-accumulator` to compare the opt-in columnar Rust accumulator against the existing row accumulator before using it in a real run.
Add `--rust-parquet-flush-records` when profiling small `--chunk-size` values so the speed benefit does not imply thousands of parquet files.
It also writes:

- `parallel_profile.json`
- `parallel_profile.md`
- per-run `artifact_contract.json`

Recommendations compare only successful runs. If the fastest setting is less than 5% faster than a lower-overhead setting, the lower-overhead setting is recommended.

## ID Compaction Contract

The Rust backend supports the OpenAlex `semantic_column_strip` preset.
It should produce the same parquet contract as the Python path for supported data:

- compacted OpenAlex/DOI/ROR/ORCID values
- semantic column names such as `author_openalex_id`
- `schema_manifest.json` with rules, descriptions, and counts
- ambiguous URL-like columns recorded as warnings instead of compacted blindly

Use `parquet inspect` after a parse run:

```bash
kisti-db-manager parquet inspect \
  --parquet-root runs/<run_dir>/parquet \
  --require-schema-manifest \
  --require-id-compaction
```

## Rust MySQL Loader

`--rust-db-load` opts into the experimental Rust MySQL insert path after Rust parquet files are written:

```bash
kisti-db-manager json run \
  --config path/to/openalex_config.json \
  --mode ingest-safe \
  --flatten-backend rust-arrow \
  --rust-db-load
```

This path keeps table creation and schema mapping in Python, then asks the Rust extension to read the generated parquet files and batch insert into MySQL/MariaDB.

The loader runs each batch inside a DB transaction by default.
If an insert fails, that batch is rolled back rather than left partially inserted.

Use the Python DB bridge instead of `--rust-db-load` when you need mature per-row fallback or schema-freeze extra-column behavior.

## Smoke Test

Before relying on the Rust DB loader, run the live smoke script against a disposable or development MariaDB database:

```bash
python scripts/smoke_rust_db_load.py --dotenv .env
```

The script:

- creates a unique `kisti_rust_db_smoke_*` table family
- writes OpenAlex-like JSONL input
- runs Rust parquet generation and Rust MySQL load
- validates row counts and artifact contract
- drops the smoke tables by default

Keep the tables only for manual inspection:

```bash
python scripts/smoke_rust_db_load.py --dotenv .env --keep-tables
```

The cleanup rule is intentionally narrow: it drops only the exact base table or tables beginning with `base__`.

## DB Load Benchmark

To compare DB load-only throughput from existing parquet artifacts:

```bash
python scripts/oa_benchmark_parquet_load.py runs/example/parquet \
  --config runs/example/config.json \
  --loader rust-mysql \
  --report runs/example/rust_mysql_load_benchmark.json
```

The default loader remains Python:

```bash
python scripts/oa_benchmark_parquet_load.py runs/example/parquet \
  --config runs/example/config.json \
  --loader python \
  --report runs/example/python_load_benchmark.json
```

Use the same parquet root and table subset when comparing loaders.

## Current Limits

The Rust path is intentionally conservative.

- `excepted_expand_dict=true` remains Python-only
- `persist_parquet_files=false` cannot use Rust
- custom Python `extract_fn` disables Rust
- raw JSONL parsing is limited to explicit `rust-arrow` parse/parquet-only runs
- direct Rust JSONL file parsing currently excludes ID compaction and uses the batch raw parser instead
- schema-freeze extra-column mode disables direct Rust DB load
- DB indexes and optimize remain Python-managed

When `flatten_backend=auto`, unsupported Rust cases fall back to Python and record the fallback reason in the run report.
When `flatten_backend=rust-arrow`, unsupported cases are treated as errors.

## Verification Commands

Before merging Rust backend changes:

```bash
cargo fmt --manifest-path crates/kisti_json_rs/Cargo.toml --check
cargo check --manifest-path crates/kisti_json_rs/Cargo.toml
cargo test --manifest-path crates/kisti_json_rs/Cargo.toml
python -m unittest discover -s tests -q
python scripts/smoke_rust_db_load.py --dotenv .env
```

For DB smoke runs, verify no temporary tables remain unless `--keep-tables` was used.
