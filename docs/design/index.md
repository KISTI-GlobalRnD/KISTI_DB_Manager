# Design Notes Overview

This section contains design and performance notes that sit one level below the operator guides and architecture pages.
It focuses less on how to run the package and more on why the package is structured this way and where the real bottlenecks were found.

## Current note groups

- ingest tuning
- schema display / review UX benchmark
- description profile v2 and future RDB visualization inputs
- parquet materializer design
- scenario-based large parquet jobs
- dataset-specific operational notes

Durable maintenance decisions live in [Decisions](../decisions/index.md).

## Reading rule

- Read the Operator Guides first
- Come here when you need implementation rationale or performance evidence

## Maintenance rule

Design notes may mention internal modules, but avoid line-number references for active code.
The package now keeps compatibility facades such as `KISTI_DB_Manager.review` and `KISTI_DB_Manager.cli` thin, while implementation lives under `_review/`, `_cli/`, and `_pipeline/`.
Use module/function names in design notes so the docs remain useful after refactors.
