# Parquet-First Workflow

Use this workflow when local artifacts, restartability, and downstream reuse matter more than immediate DB completion.

## Parse First

```bash
kisti-db-manager json run \
  --config path/to/openalex_config.json \
  --mode parse-parquet-safe
```

For the current OpenAlex Rust ID-compacted path:

```bash
kisti-db-manager json run \
  --config path/to/openalex_config.json \
  --mode parse-parquet-safe \
  --flatten-backend rust-arrow \
  --id-compaction \
  --chunk-size 10000
```

## Inspect the Artifact Contract

```bash
kisti-db-manager parquet inspect \
  --parquet-root runs/<run_dir>/parquet \
  --require-schema-manifest \
  --require-id-compaction
```

Treat a failed contract check as a stop signal before DB work.

## Materialize Later

```bash
uv run python scripts/oa_materialize_parquet_to_db.py \
  runs/<run_dir> \
  --dotenv path/to/.env \
  --db-name target_openalex_db \
  --materialize-preset openalex-idcompact-fast
```

For repeated reloads, prefer the plan-driven wrapper:

```bash
kisti-db-manager parquet preflight --plan runs/<run_dir>/plans/parquet_reload_plan.json
kisti-db-manager parquet reload --plan runs/<run_dir>/plans/parquet_reload_plan.json
```

## Artifact Policy

- Keep generated parquet, reports, and validation outputs under `runs/` or another ignored run directory.
- Do not copy generated artifacts into `docs/` unless they are intentionally selected public examples.
- Preserve profile artifacts when comparing workers, backends, or materializers; recommendations should be based on measured median throughput.
