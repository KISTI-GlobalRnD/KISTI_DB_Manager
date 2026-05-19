# Release Checklist

Use this checklist before committing documentation or operator-facing workflow changes.

## Documentation

```bash
git diff --check
uv run mkdocs build --strict
```

Check that generated public examples are intentionally selected and reproducible.
Run-specific artifacts should stay outside `docs/` unless they are part of the public examples.

## Python Tests

For code changes, run the unit suite:

```bash
uv run python -m unittest discover -s tests -q
```

## Rust Backend Changes

When Rust code or Rust backend policy changes:

```bash
cargo fmt --manifest-path crates/kisti_json_rs/Cargo.toml --check
cargo check --manifest-path crates/kisti_json_rs/Cargo.toml
cargo test --manifest-path crates/kisti_json_rs/Cargo.toml
```

Run the DB smoke test only against a disposable or development MariaDB target:

```bash
uv run python scripts/smoke_rust_db_load.py --dotenv .env
```

## Operator-Facing Changes

- Update the relevant operator guide, not only the reference page.
- Update Korean runbooks when the day-to-day command sequence changes.
- Keep README as a short portal; move operational detail into MkDocs.
