# Release Checklist

Use this checklist before committing documentation or operator-facing workflow changes.

## CI Gate

Pull requests and pushes to `main` run `.github/workflows/ci.yml`.
That workflow blocks on:

- Python 3.10, 3.11, and 3.12 unit tests
- `mkdocs build --strict`
- Rust `fmt`, `check`, and `test`
- wheel/sdist build plus a built-wheel CLI smoke test

## Documentation

```bash
git diff --check
mkdocs build --strict
```

Check that generated public examples are intentionally selected and reproducible.
Run-specific artifacts should stay outside `docs/` unless they are part of the public examples.

## Python Tests

For code changes, run the unit suite:

```bash
python -m unittest discover -s tests -q
```

## Package Build

Before release tagging or handoff, verify that the source distribution and wheel build:

```bash
python -m pip install build
python -m build --sdist --wheel
```

The build writes `build/`, `dist/`, and `*.egg-info/` directories.
They are ignored by git and can be removed after inspection unless you are publishing the artifacts.

## Rust Backend Changes

When Rust code or Rust backend policy changes:

```bash
cargo fmt --manifest-path crates/kisti_json_rs/Cargo.toml --check
cargo check --manifest-path crates/kisti_json_rs/Cargo.toml
cargo test --manifest-path crates/kisti_json_rs/Cargo.toml
```

Run the DB smoke test only against a disposable or development MariaDB target:

```bash
python scripts/smoke_rust_db_load.py --dotenv .env
```

## Operator-Facing Changes

- Update the relevant operator guide, not only the reference page.
- Update Korean runbooks when the day-to-day command sequence changes.
- Keep README as a short portal; move operational detail into MkDocs.
