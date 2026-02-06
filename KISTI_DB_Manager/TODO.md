# KISTI_DB_Manager Todo (Prioritized)

## P0 (Core robustness)

## Done
- Type widening on insert failures (tabular/json): widen/add column and retry
- `run_json_pipeline()` (json/jsonl/gz/zip) with per-record quarantine + RunReport stats
- Multi-input JSON ingest: `file_names`/`file_glob` + ZIP multi-member (`json_file_names`) support
- Excepted branch preservation: store raw JSON + path/type + source context in excepted tables
- Integration smoke-run docs: real DB templates + `examples/smoke_real_db.sh`
- Optional deps split (`tabular/json/db/viz`) + friendly CLI error on missing extras
- Schema drift handling: `ALTER TABLE ADD COLUMN` best-effort during load
- CLI parity: `json run --dry-run/--print-ddl` (+ NameMap artifacts)

## P1 (Quality & maintainability)
- Add structured logging (stdio JSON logs option) + deterministic run directory layout
- Add integration tests (dockerized MariaDB) for create/load/index/optimize end-to-end

## P2 (Performance & UX)
- Chunked/streaming ingest for large CSV/JSONL (avoid full pandas load)
- Parallel flatten/ingest (process pool) with bounded memory
- Schema visualization improvements (NameMap-aware, drift-aware)
