# Artifacts Reference

This page summarizes the artifact types operators should recognize.

## Run Reports

`run_report.json` records timing, counters, issues, backend choices, and artifact paths.
Use it as the first source when diagnosing a run.

```bash
kisti-db-manager report summary path/to/run_report.json
kisti-db-manager report profile path/to/run_report.json --top 20
```

## Quarantine

`quarantine.jsonl` stores records that failed parsing or normalization when continue-on-error behavior is active.

```bash
kisti-db-manager quarantine summary path/to/quarantine.jsonl --out quarantine_out
```

## Parquet Artifacts

Parquet-first runs produce table directories under a run-specific parquet root.
OpenAlex ID-compacted runs should also include `schema_manifest.json`.

```bash
kisti-db-manager parquet inspect \
  --parquet-root runs/<run_dir>/parquet \
  --require-schema-manifest \
  --require-id-compaction
```

## Review Artifacts

Review commands generate:

- Markdown reports
- standalone HTML reports
- `schema.svg`
- `schema.mmd`
- JSON payloads for review/schema viewers

Keep generated review artifacts outside `docs/` unless they are selected public documentation examples.

## Public Documentation Artifacts

The public OpenAlex preview and schema SVG under `docs/examples/` and `docs/assets/` are intentionally checked in.
Refresh instructions live in [Review and Visualization](../architecture/review-visualization.md#regenerating-the-checked-in-openalex-schema-svg).
