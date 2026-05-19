# Changelog

## 0.9.0 - 2026-05-19

### Added

- Promoted supported OpenAlex operational commands into the installed CLI:
  `openalex materialize`, `openalex benchmark-load`, `openalex validate-reload`,
  and `smoke rust-db-load`.
- Added packaged command modules for OpenAlex materialization, DB load
  benchmarking, reload validation, and Rust DB smoke testing so supported
  operations no longer depend on `scripts/` paths.
- Added CI wheel/sdist smoke coverage and an optional workflow-dispatch MariaDB
  smoke job for the Rust-backed DB load path.
- Added package layout, script-boundary, OpenAlex runbook, and release-checklist
  documentation for the supported CLI surface.

### Changed

- Updated parquet reload finalization/validation helpers to call packaged
  modules rather than repository-local scripts.
- Refreshed OpenAlex schema visualization assets from the current packaged
  review code path.
- Split the OpenAlex reload validator parser into the `_cli` package while
  keeping validation behavior unchanged.

## 0.8.0 - 2026-05-13

### Added

- Added backend-aware `json profile-parallel` reporting for Python and Rust Arrow comparison.
- Added optional Rust Arrow JSON/parquet backend for supported parse/parquet runs.
- Added OpenAlex ID compaction parity for the Rust backend.
- Added experimental opt-in Rust MySQL parquet loader.
- Added Rust DB smoke and load benchmark scripts.

### Changed

- Hardened output path handling, including symlink-safe write and cleanup helpers.
- Consolidated Rust backend, profiling, smoke, and benchmark documentation.
- Refreshed legacy operational docs and marked historical design/performance notes explicitly.

### Fixed

- Reduced risk around accidental artifact deletion and unsafe output path reuse.
- Improved profile/report summaries so operators can choose backend and worker settings from measured runs.
