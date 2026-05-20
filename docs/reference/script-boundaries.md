# CLI and Script Boundaries

This page separates the supported installed CLI surface from source-checkout
helper scripts. Use it when deciding whether a workflow should become a
packaged command.

## Supported CLI Surface

These commands are the product-facing entrypoints. They should work from an
installed wheel when the required optional dependencies are installed.

Core commands:

- `kisti-db-manager version`
- `kisti-db-manager modes`
- `kisti-db-manager tabular run`
- `kisti-db-manager json run`
- `kisti-db-manager json profile-parallel`
- `kisti-db-manager json id-compaction-preflight`

Review and reporting:

- `kisti-db-manager review plan`
- `kisti-db-manager review pack`
- `kisti-db-manager review preview`
- `kisti-db-manager review schema-viewer`
- `kisti-db-manager review diff`
- `kisti-db-manager report summary`
- `kisti-db-manager report diff`
- `kisti-db-manager report profile`
- `kisti-db-manager quarantine summary`

Parquet operations:

- `kisti-db-manager parquet inspect`
- `kisti-db-manager parquet preflight`
- `kisti-db-manager parquet reload`
- `kisti-db-manager parquet mark-table-done`
- `kisti-db-manager parquet finalize`

OpenAlex operational examples:

- `kisti-db-manager openalex materialize`
- `kisti-db-manager openalex benchmark-load`
- `kisti-db-manager openalex validate-reload`
- `kisti-db-manager smoke rust-db-load`

## Compatibility Wrappers

Some scripts remain as source-checkout compatibility wrappers for existing
operators and old runbooks. They should forward to packaged modules or CLI
commands, not carry independent implementation logic.

- `scripts/oa_materialize_parquet_to_db.py`
- `scripts/oa_benchmark_parquet_load.py`
- `scripts/oa_validate_serving_reload.py`
- `scripts/smoke_rust_db_load.py`
- `scripts/parquet_finalize_db.py`
- `scripts/parquet_preflight_db.py`
- `scripts/parquet_reload_plan.py`

Treat these as migration aids. New documentation should prefer
`kisti-db-manager ...` unless it is explicitly describing source-checkout
compatibility.

## Source-Checkout Tools

Other scripts are maintainer or dataset-operation tools. They are allowed to be
more specific, but they are not part of the stable installed command contract.

Examples:

- local maintainer release validation such as `scripts/release_check.py`
- one-off OpenAlex serving rebuild orchestration such as
  `scripts/oa_rebuild_0330_serving_db.py`
- OpenAlex 20260330 serving table assembly helpers such as
  `scripts/oa_build_works_affiliation_agg.py` and
  `scripts/oa_finalize_openalex_serving_db.py`
- GCC/OpenAlex watch shell scripts under `scripts/run_oa_gcc_*.sh`
- Overton export/validation shell scripts under `scripts/run_overton_*.sh`
- historical report and bundle builders under `scripts/`

These scripts may call package modules, but they do not need to become CLI
subcommands unless the workflow becomes dataset-neutral, repeatable, and useful
outside the original operating context.

## Promotion Criteria

Promote a script into the supported CLI surface only when all of these are true:

- The workflow is repeatable for more than one run or dataset variant.
- Inputs and outputs can be described as stable files, directories, or reports.
- The command can provide useful `--help` without requiring heavy optional
  dependencies to import.
- Tests can cover parser dispatch and the core behavior without live production
  data.
- The command does not encode a one-off table list, snapshot date, or local
  machine path as core behavior.

If a script fails those checks, keep it under `scripts/` and document it as a
source-checkout tool rather than a product command.
