# Changelog

## Unreleased

### Added

- Added Dataset Profile overlays to `review schema-viewer` through
  `--dataset-profile` or automatic `<PATH>/dataset_profile.json` detection,
  with candidate evidence attached to existing relationship cards.
- Added Schema Viewer relational overview panels for table roles,
  relationship evidence, coverage gaps, relationship warnings, and
  disconnected-table filtering.
- Added dashed Schema Viewer candidate-only edges for Dataset Profile
  relationships between known tables that are not covered by structural naming.
- Added Schema Viewer relationship review badges, key-source badges, and a
  needs-review filter for Dataset Profile-backed relationship cards.
- Added a Schema Viewer Relationship Catalog with relationship search,
  priority/key-source filters, sorting, join SQL previews, and parent/child
  table jump actions.
- Added optional `relationship_decisions.json` overlays for Schema Viewer so
  operator accepted/rejected relationship decisions stay separate from inferred
  Dataset Profile evidence.
- Added opt-in sampled value-overlap validation for `tabular profile-dataset`
  relationship candidates, with overlap/orphan ratios attached as bounded
  review evidence.
- Added Schema Viewer auto-loading of per-table `*_profile.json` files
  referenced by `dataset_profile.json`, so no-DB Dataset Profile views can show
  column catalogs, type hints, and key/index badges.
- Added profile-only Dataset Profile audit summaries for confidence buckets,
  review priority, warning counts, skipped naming hints, and no-scan value
  overlap status.
- Added shared parent-key relationship hints for profile-only Dataset Profiles
  so WoS-style `UID` parent-child tables are not dropped when `id` is absent.

### Changed

- Replaced the legacy WoS Feather `Data_Sample` fixture with a parquet-first
  OpenAlex works sample and refreshed the sample schema generator/docs.

## 0.10.0 - 2026-05-20

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
- Added Rust Python extension smoke coverage for raw JSONL parquet output and
  explicit `simd-json` feature gating.
- Added a source-checkout `scripts/release_check.py` runner for the local
  release validation gate.

### Changed

- Switched CI's Python test gate from `unittest discover` to `pytest -q` and
  added an explicit `test` optional dependency group.
- Split the Rust PyO3 extension feature so default extension builds still work
  while crate unit tests can run without `extension-module` linking.
- Extended CI and the local release check to validate the Rust `simd-json`
  feature and installed extension path.
- Declared the DuckDB Python package in the JSON optional dependencies used by
  parquet/OpenAlex DuckDB paths.
- Switched Rust extension CI smoke validation to build and install the extension
  wheel instead of relying on a pre-existing virtual environment.
- Added source distribution checks for the Rust crate files needed to rebuild
  the optional extension from packaged source.
- Opted CI and docs workflows into GitHub Actions' Node.js 24 runtime ahead of
  the June 2026 default runner transition.
- Updated GitHub Actions checkout, Python setup, and Pages actions to current
  Node.js 24-compatible major versions.
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
