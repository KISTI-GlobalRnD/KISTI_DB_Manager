# Overton Validation Queries

This page documents the query bundle for validating `overton_202601_raw` after parquet finalize, DB import, index build, and short-name rename.

## Files

- `sql/overton_validation_quick.sql`
- `sql/overton_validation_deep.sql`
- `scripts/run_overton_validation.sh`

## Run

Quick validation:

```bash
scripts/run_overton_validation.sh --schema overton_202601_raw --mode quick
```

Deep validation:

```bash
scripts/run_overton_validation.sh --schema overton_202601_raw --mode deep
```

## Quick mode

`quick` is the default operational check.
It focuses on fast structural verification:

- base table / view inventory
- estimated row counts and table sizes from `information_schema`
- index inventory
- canonical short-name tables and compatibility views

Use this right after import or after a recovery resume.

## Deep mode

`deep` is the slower audit pass.
It focuses on content-level validation:

- exact `COUNT(*)` for all 20 canonical base tables
- extra-row checks by `policy_document_id` for scalar tables
- exact duplicate probes for the known variant-sensitive scalar tables
- orphan row counts in child tables relative to `docs`
- top fan-out documents for `topics` and `ref_ctx`

Use this when you want a stronger confidence check before handing the DB to downstream users.

## Notes

- The SQL files target the canonical short-name schema layout:
  - `docs`, `authors`, `topics`, `ref_ctx`, `src_type`, ...
- The same schema still exposes long-name compatibility `VIEW`s.
- `quick` is safe for frequent execution.
- `deep` is intentionally heavier and should be treated as an audit step, not a heartbeat check.
