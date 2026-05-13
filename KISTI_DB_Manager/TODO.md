# KISTI_DB_Manager Roadmap

This file keeps only current engineering follow-ups. Historical incident notes and completed run logs should stay in git history or dated docs under `docs/performance/`.

## Current Status

Completed in the current development batch:

- output/path safety helpers and symlink-safe write/delete wrappers
- backend-aware `json profile-parallel`
- Rust Arrow JSON/parquet backend
- OpenAlex ID compaction parity for the Rust path
- experimental Rust MySQL parquet loader with transactional batch insert
- Rust DB smoke and load benchmark scripts
- consolidated Rust backend documentation

## P0 Before Release

- Push the current commit stack and confirm CI/build behavior on a clean checkout.
- Build an sdist/wheel and verify the Rust crate files are included while `crates/kisti_json_rs/target` is excluded.
- Run one representative real-data profile with `python,rust-arrow` backends and record the recommendation artifact.
- Run one representative DB load benchmark comparing Python loader vs `--loader rust-mysql` from the same parquet root.

## P1 Hardening

- Add CI coverage for Rust extension build when the environment supports Rust and maturin.
- Expand Rust MySQL loader type coverage only when a real parquet contract needs it.
- Add a rollback/partial-insert integration test around a deliberately failing Rust DB load batch.
- Promote common DB smoke env parsing into a shared helper if more live smoke scripts are added.

## P2 Operations

- Keep public docs manual-first and move dated design notes under explicit historical labels.
- Add benchmark result templates for `parallel_profile.json` and Rust DB load-only reports.
- Keep OpenAlex-specific orchestration thin and move reusable execution strategy into package modules.
