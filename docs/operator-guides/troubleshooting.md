# Troubleshooting

Use this page as the first triage pass before changing production settings.

## Review the Run Report First

```bash
kisti-db-manager report summary path/to/run_report.json
kisti-db-manager report profile path/to/run_report.json --top 20
```

Check:

- failed stages
- slow timing keys
- parquet artifact paths
- backend fallback reasons
- schema drift warnings

## Check Quarantine

```bash
kisti-db-manager quarantine summary path/to/quarantine.jsonl --out quarantine_out
```

Large quarantine counts should be inspected before rerunning with broader fallback behavior.

## Check Parquet Artifacts Before Reload

```bash
kisti-db-manager parquet inspect \
  --parquet-root runs/<run_dir>/parquet \
  --require-schema-manifest
```

For ID-compacted OpenAlex runs, also require ID compaction:

```bash
kisti-db-manager parquet inspect \
  --parquet-root runs/<run_dir>/parquet \
  --require-schema-manifest \
  --require-id-compaction
```

## Common Stop Signals

- `persist_parquet_files=true` and `json_streaming_load=true` are both enabled
- ID compaction preflight reports collisions or namespace conflicts
- reload preflight reports target DB reset or permission risks
- profile results show no meaningful speedup from extra workers

When a stop signal appears, prefer a focused preflight/profile run over changing several production options at once.
