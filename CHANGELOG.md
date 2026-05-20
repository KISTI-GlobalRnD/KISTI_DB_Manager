# Changelog

## Unreleased

### Added

- Added Description Profile v2 outputs for richer per-column database profiling,
  including SQL column mapping, key/index hints, type families, and quality
  warnings.
- Added Dataset Profile v1 generation through `tabular profile-dataset` to
  summarize multiple table profiles and produce early relationship candidates.
- Added stable raw-column alias metadata to `NameMap` so dot-normalized column
  collisions can be traced across schema drift and reload paths.
- Added migration notes for Description Profile v2, Dataset Profile v1, and
  NameMap alias compatibility.
- Added golden artifact regression tests for Description Profile v2 and Dataset
  Profile v1 outputs.
- Added Schema Viewer artifact contract coverage for JSON payload, Mermaid,
  SVG relationship metadata, and HTML markers.
- Added Rust crate unit tests for parser backend selection, JSONL parsing,
  number validation, and identifier namespace mapping.
- Added a source-checkout `scripts/release_check.py` runner for the local
  release validation gate.

### Changed

- Switched CI's Python test gate from `unittest discover` to `pytest -q` and
  added an explicit `test` optional dependency group.
- Split the Rust PyO3 extension feature so default extension builds still work
  while crate unit tests can run without `extension-module` linking.
- Reworked column canonicalization and truncation to preserve collision hints
  such as `__dot`, `__raw`, and `__dup2` instead of opaque numeric suffixes.
- Routed tabular load, row load, TSV load, description-profile, and JSON
  pipeline schema drift handling through the same NameMap-aware input
  canonicalization path.
- Updated OpenAlex parquet preflight diagnostics to report normalized column
  collisions while keeping union schema output deterministic.
- Refined README as a short documentation portal and moved release-facing
  details into MkDocs pages.

### Fixed

- Prevented raw input aliases from silently collapsing repeated raw labels.
- Prevented a later raw column such as `a__b` from being merged into a prior
  `a.b -> a__b` alias during schema drift.
- Preserved raw forced-key resolution in Description Profile v2 when raw and
  dot-normalized names collide.

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
