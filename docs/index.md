# KISTI_DB_Manager

`KISTI_DB_Manager` is the repository for high-volume MariaDB/MySQL ingest, flattening, and review workflows.

This documentation site is intentionally manual-first.
The priority is operational guidance for large, messy datasets rather than auto-generated API pages.

## Chapter Map

### Manual

- [Manual Overview](manual/index.md)
- [Chapter 1. Package Overview](manual/package-overview.md)
- [Chapter 2. Getting Started](manual/getting-started.md)
- [Chapter 3. JSON Modes](manual/json-modes.md)
- [Chapter 4. OpenAlex Example Workflow](manual/openalex-workflow.md)
- [Chapter 5. Review and Visualization](manual/review-visualization.md)
- [Chapter 6. Restart & Recovery](manual/restart-recovery.md)

### Reference

- [Reference Overview](reference/index.md)
- [CLI Quick Reference](reference/cli.md)
- [Examples](reference/examples.md)

### Design Notes

- [Design Notes Overview](design/index.md)
- performance and benchmark notes under `docs/performance/`

## Recommended reading order

1. Start with [Manual Overview](manual/index.md)
2. Read [Chapter 1. Package Overview](manual/package-overview.md)
3. Read [Chapter 2. Getting Started](manual/getting-started.md)
4. Read [Chapter 3. JSON Modes](manual/json-modes.md)
5. If you are working with OpenAlex, go to [Chapter 4. OpenAlex Example Workflow](manual/openalex-workflow.md)
6. Review package-side inspection outputs in [Chapter 5. Review and Visualization](manual/review-visualization.md)
7. For operational interruptions and restart policy, read [Chapter 6. Restart & Recovery](manual/restart-recovery.md)

## Scope

This site does not try to replace every repository document.
Instead, it organizes the operationally important public parts and links the design notes that are safe to expose.

Commercial dataset runbooks and generated local artifacts are intentionally kept out of the public docs surface.

## Detailed ops guide

A more detailed Korean operations guide remains in the repository at:

- [`KISTI_DB_Manager/GUIDE_KO.md`](https://github.com/KISTI-GlobalRnD/KISTI_DB_Manager/blob/main/KISTI_DB_Manager/GUIDE_KO.md)
