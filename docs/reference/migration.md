# Migration Notes

This page records compatibility notes for operator-facing artifact changes.
Use it with `CHANGELOG.md` and the
[release checklist](../operator-guides/release-checklist.md) before refreshing
production artifacts or cutting a release.

## Unreleased

### NameMap Column Aliases

`NameMap` now includes optional `column_aliases` metadata.
The field maps unambiguous raw input column names to canonical column names.

Example:

```json
{
  "columns_original": ["a__b"],
  "columns_sql": ["a__b"],
  "column_aliases": {
    "a.b": "a__b"
  }
}
```

Compatibility behavior:

- Existing NameMap JSON without `column_aliases` still loads.
- New NameMap JSON may include `column_aliases`.
- Raw aliases are stored only when the raw input label is unique.
- Internal calls should use canonical column names from `columns_original`.
- Raw input loaders should resolve incoming headers through the active NameMap
  before extending schema.

Why this changed:

- A raw `a.b` column and a raw `a__b` column can normalize to the same base.
- Schema drift can expose the collision later, not only in the first chunk.
- Alias metadata lets loaders distinguish raw input headers from already
  canonical internal column names.

### Stable Collision Hints

Column canonicalization now uses descriptive collision hints instead of opaque
numeric suffixes where possible.

Examples:

```text
a.b, a__b
=> a__b__dot, a__b__raw

title, title
=> title, title__dup2
```

Compatibility behavior:

- Existing databases and artifacts keep their stored column names.
- Regenerated NameMaps can produce different names for collision cases.
- Downstream tools should read `NameMap.column_map` rather than reconstructing
  SQL names from source names.
- OpenAlex parquet artifacts scanned locally did not contain dot-normalized
  column collisions, so existing OpenAlex SVG/Desc outputs are not expected to
  change because of this naming policy alone.

### Description Profile v2

Description Profile v2 is the current per-table profile contract.
It adds SQL mapping and database-understanding metrics while keeping the CSV
surface suitable for inspection.

Important fields include:

- `source_column`
- `sql_column`
- `type_family`
- `suggested_type`
- `is_key_candidate`
- `index_recommended`
- `warnings`

Compatibility behavior:

- Treat v1-style Desc CSV files as legacy inputs.
- New visualizations and dataset-level summaries should prefer v2 profile JSON
  when available.
- Forced keys are resolved against raw input columns first, then through
  canonical naming.

### Dataset Profile v1

Dataset Profile v1 summarizes multiple per-table profile JSON files.
It is intended to support schema-level review and visualization rather than
replace per-table Description Profiles.

Current scope:

- table summaries
- column role/type summaries
- early relationship candidates
- dataset-level warnings

Compatibility behavior:

- Dataset Profile v1 is additive.
- Existing tabular and JSON ingest paths do not require it.
- Visualization code should treat relationship candidates as advisory until
  validated by row-level key checks or operator review.

## Release Migration Checks

Before publishing a release that changes artifact contracts:

1. Regenerate representative Description Profile and Dataset Profile fixtures.
2. Confirm `NameMap` round-trips with and without `column_aliases`.
3. Confirm legacy `_1`-style NameMaps still load.
4. Run the unit suite.
5. Run `mkdocs build --strict`.
6. If DB load behavior changed, run the MariaDB smoke path.
