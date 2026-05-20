# KISTI_DB_Manager

`KISTI_DB_Manager` supports high-volume MariaDB/MySQL ingest, nested JSON/XML flattening, parquet-first workflows, and review artifacts for large operational datasets.

This documentation is the source of truth for maintained workflows.
GitHub Wiki should be treated as scratch space; durable runbooks and reference pages belong here.

## Documentation Map

### Getting Started

- [Install and first run](getting-started/index.md)

Use this when you are new to the repository or deciding between DB-first and parquet-first operation.

### Operator Guides

- [OpenAlex Runbook](operator-guides/openalex-runbook.md)
- [Parquet-First Workflow](operator-guides/parquet-first-workflow.md)
- [Restart and Recovery](operator-guides/restart-recovery.md)
- [Troubleshooting](operator-guides/troubleshooting.md)
- [Release Checklist](operator-guides/release-checklist.md)

Use these pages when you need command sequences and operational stop signals.

### Reference

- [CLI Quick Reference](reference/cli.md)
- [Migration Notes](reference/migration.md)
- [JSON Modes](reference/modes.md)
- [Artifacts Reference](reference/artifacts.md)
- [Examples](reference/examples.md)
- [Overton Validation Queries](reference/overton-validation.md)

Use these pages when you need option names, artifacts, or examples quickly.

### Architecture

- [Package Layout and Architecture](architecture/package-layout.md)
- [JSON Pipeline Architecture](architecture/json-pipeline.md)
- [Rust Backend and Profiling](architecture/rust-backend.md)
- [Review and Visualization](architecture/review-visualization.md)

Use these pages when you need implementation context or maintainer-facing structure.

### Korean Runbooks

- [한국어 운영 문서](ko/index.md)

Korean pages summarize day-to-day operator procedures. The English pages remain the canonical technical reference.

### Decisions

- [ADR 0001: MkDocs is the documentation source of truth](decisions/adr-0001-docs-source-of-truth.md)

Use decisions when a maintenance policy should survive beyond one cleanup pass.

### Design and Performance Notes

- [Design Notes Overview](design/index.md)
- [Description Profile v2](design/description-profile-v2.md)
- [Dataset Profile v1](design/dataset-profile-v1.md)
- [Scenario-Based Parquet Jobs](design/parquet_job_scenarios.md)
- historical performance and benchmark notes under `docs/performance/`

## Recommended Reading Order

1. [Getting Started](getting-started/index.md)
2. [OpenAlex Runbook](operator-guides/openalex-runbook.md) if you are running OpenAlex
3. [JSON Modes](reference/modes.md) when choosing execution mode
4. [Rust Backend and Profiling](architecture/rust-backend.md) when choosing Python vs Rust
5. [Restart and Recovery](operator-guides/restart-recovery.md) before long operational runs

## Scope

Commercial dataset runbooks and generated local artifacts are intentionally kept out of the public docs surface.
Generated artifacts should be checked in only when explicitly selected as public examples.
