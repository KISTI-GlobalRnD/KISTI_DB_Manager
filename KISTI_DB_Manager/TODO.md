# KISTI_DB_Manager Todo (Prioritized)

## P0 (Core robustness)
- Improve excepted-branch handling (store raw JSON for unstructured parts)
- Add integration smoke-run docs (real DB example configs)

## Done
- Type widening on insert failures (tabular/json): widen/add column and retry
- `run_json_pipeline()` (json/jsonl/gz/zip) with per-record quarantine + RunReport stats
- Schema drift handling: `ALTER TABLE ADD COLUMN` best-effort during load
- CLI parity: `json run --dry-run/--print-ddl` (+ NameMap artifacts)

## P1 (Quality & maintainability)
- Make heavy dependencies optional (extras: `tabular`, `json`, `db`, `viz`) and raise friendly errors when missing
- Add structured logging (stdio JSON logs option) + deterministic run directory layout
- Add integration tests (dockerized MariaDB) for create/load/index/optimize end-to-end

## P2 (Performance & UX)
- Chunked/streaming ingest for large CSV/JSONL (avoid full pandas load)
- Parallel flatten/ingest (process pool) with bounded memory
- Schema visualization improvements (NameMap-aware, drift-aware)
