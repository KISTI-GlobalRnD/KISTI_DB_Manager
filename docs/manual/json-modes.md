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
