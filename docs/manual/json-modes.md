# Chapter 3. JSON Modes

This chapter maps each `json run` mode to its operational purpose so you can choose the correct execution path before a long ingest starts.

## Rule of thumb

- `ingest-fast`: fastest DB-oriented path
- `ingest-fast-freeze`: fast path when schema drift is severe
- `ingest-fast-hybrid`: warm up schema, then freeze
- `ingest-safe`: fallback when `LOCAL INFILE` cannot be used
- `finalize`: index and optimize after ingest
- `parse-parquet`: parquet-first path
- `parse-parquet-safe`: conservative parquet-first path for large nested sources

## Operational meaning

| Mode | Primary goal | Load path | Good fit |
|---|---|---|---|
| `ingest-fast` | fastest DB ingest | streaming `LOAD DATA` | stable DB-first ingest |
| `ingest-fast-freeze` | avoid ALTER churn | streaming `LOAD DATA` + `__extra__` | strong schema drift |
| `ingest-fast-hybrid` | early evolve, later freeze | streaming `LOAD DATA` | partial schema discovery |
| `ingest-safe` | compatibility | `to_sql` fallback style | `LOCAL INFILE` blocked |
| `finalize` | post-load index/optimize | no load | after ingest |
| `parse-parquet` | local artifacts first | parquet-first | artifact-driven workflows |
| `parse-parquet-safe` | safer parquet-first | parquet-first | OpenAlex-like nested sources |

## Important constraint

These options are now validated strictly:

- `persist_parquet_files=true` and `json_streaming_load=true` cannot be enabled together
- `persist_tsv_files=true` is only valid for the streaming path

In production, prefer an explicit mode instead of relying on `default`.

## OpenAlex ID compaction

For OpenAlex-scale JSON parsing, ID compaction can be enabled explicitly:

```bash
kisti-db-manager json run \
  --config path/to/openalex_config.json \
  --mode parse-parquet-safe \
  --id-compaction
```

The current OpenAlex preset removes repeated URL prefixes from known ID values and stores namespace meaning in column names and descriptions. For example, `author_id=https://openalex.org/A123` becomes `author_openalex_id=A123`, while base `id` remains named `id` and stores `W...`. Semantic columns are renamed consistently even when a row has `null` or an already compact bare ID value, preventing mixed schemas such as both `author_id` and `author_openalex_id`.

ID compaction can be used with `--parallel-workers`. Worker processes flatten JSON slices, then the parent applies compaction and merges the resulting schema summary. Compaction collisions and namespace conflicts still fail immediately by default rather than being hidden by the sequential fallback path.

If two nonblank source values map to the same compacted output column, compaction raises an error by default instead of silently choosing one. The run report and `schema_manifest.json` record the removed prefix, source column, new column, and description; schema-evolved DB columns also receive the same column comments when the target supports them.

Operational policy flags:

- `--id-compaction-collision-policy error|preserve`
- `--id-compaction-namespace-conflict-policy error|preserve`

Keep the default `error` for production loads where data loss must stop the run. Use `preserve` only when you intentionally want the original conflicting column retained for later review.

Before a long production run, scan the input first:

```bash
kisti-db-manager json id-compaction-preflight \
  --config path/to/openalex_config.json \
  --max-records 100000 \
  --report id_compaction_preflight.json
```

The preflight scanner uses `preserve` internally so it can collect all findings in the scan window instead of stopping at the first bad row. The report still records the production policies configured for `json run`. It returns exit code `1` when collisions, namespace conflicts, or scan errors are found; add `--allow-issues` for review-only automation.

## Rust backend and profiling

Backend selection is separate from mode selection.
Use `--flatten-backend auto|python|rust-arrow` to choose the parser/parquet backend, and use `json profile-parallel` to compare settings before changing production runs.

Detailed Rust backend, Rust DB loader, smoke-test, and benchmark guidance is in [Chapter 4. Rust Backend and Profiling](json-rust-backend.md).
