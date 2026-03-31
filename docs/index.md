# KISTI_DB_Manager

`KISTI_DB_Manager` is the repository for high-volume MariaDB/MySQL ingest, flattening, and review workflows.

This documentation site is intentionally manual-first.
The priority is operational guidance for large, messy datasets rather than auto-generated API pages.

## What this site covers

- How to install the package with the right extras
- Which `json run` mode to choose for a given ingest goal
- How to run OpenAlex in `parse-parquet` and `materialize` stages
- How restart and resume checkpoints behave in production
- Where to find deeper design notes and external specifications

## Recommended reading order

1. Start with [Getting Started](manual/getting-started.md)
2. Read [JSON Modes](manual/json-modes.md)
3. If you are working with OpenAlex, go to [OpenAlex Workflow](manual/openalex-workflow.md)
4. For operational interruptions and restart policy, read [Restart & Recovery](manual/restart-recovery.md)

## Scope

This site does not try to replace every repository document.
Instead, it organizes the operationally important parts and links the existing design notes under `docs/performance/` and `docs/external/`.

## Detailed ops guide

A more detailed Korean operations guide remains in the repository at:

- [`KISTI_DB_Manager/GUIDE_KO.md`](https://github.com/KISTI-GlobalRnD/KISTI_DB_Manager/blob/main/KISTI_DB_Manager/GUIDE_KO.md)
