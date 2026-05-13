# Changelog

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
