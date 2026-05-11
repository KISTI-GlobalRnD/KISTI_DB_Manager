# CLI Quick Reference

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
kisti-db-manager review diff before_review.json after_review.json --out-dir review_diff_out
kisti-db-manager review preview --config path/to/config.json --out preview_out
kisti-db-manager review plan --config path/to/openalex_config.json --out plan_out
```

## Tabular

```bash
kisti-db-manager tabular run --config path/to/config.json --report run_report.json
```

## JSON

```bash
kisti-db-manager json run --config path/to/openalex_config.json --mode ingest-fast
kisti-db-manager json run --config path/to/openalex_config.json --mode finalize
kisti-db-manager json run --config path/to/openalex_config.json --mode parse-parquet-safe
kisti-db-manager json run --config path/to/openalex_config.json --mode parse-parquet-safe --id-compaction
kisti-db-manager json id-compaction-preflight --config path/to/openalex_config.json --report id_compaction_preflight.json
```

`--id-compaction` currently supports the OpenAlex semantic column strip mode. It keeps compacted schemas stable across URL, bare ID, and null values, can run with `--parallel-workers`, and fails fast on conflicting nonblank values that would collapse into the same output column. The conflict policies can be set with `--id-compaction-collision-policy error|preserve` and `--id-compaction-namespace-conflict-policy error|preserve`.

`json id-compaction-preflight` scans JSON input before a long run and reports compacted-column collisions, namespace conflicts, ambiguous URL-like columns, source examples, and the effective production policies. Use `--max-records 0` for a full scan.

## Parquet materialize helper

```bash
python scripts/oa_materialize_parquet_to_db.py \
  runs/<openalex_parse_run_dir> \
  --dotenv path/to/.env \
  --db-name target_openalex_db \
  --staging-writer duckdb \
  --parallel-tables 4 \
  --parallel-files-per-table 4 \
  --file-chunk-rows 5000
```

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

Equivalent script wrappers are available:

```bash
python scripts/parquet_reload_plan.py run --plan runs/<run_dir>/plans/parquet_reload_plan.json
python scripts/parquet_preflight_db.py --plan runs/<run_dir>/plans/parquet_reload_plan.json
python scripts/parquet_finalize_db.py --plan runs/<run_dir>/plans/parquet_reload_plan.json
```

`parquet reload` runs target DB preflight by default. Use `--skip-preflight` only after separately reviewing a clean preflight report.
The preflight report includes the same parquet artifact contract check under `checks.parquet_artifacts`; plan-level `preflight.artifact_contract.require_schema_manifest`, `require_id_compaction`, and `strict_schema_manifest` make these checks blocking.
For large reload plans, `validation.profile=large` defaults to row-count validation and skips expensive literal marker full scans unless `validation.literal_marker.mode` is set to `full` or `columns`.
`db.driver=postgresql` is supported for preflight diagnostics only; reload/finalize remain MariaDB/MySQL-backed.
