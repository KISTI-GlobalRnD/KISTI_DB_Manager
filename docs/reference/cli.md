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
```

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
