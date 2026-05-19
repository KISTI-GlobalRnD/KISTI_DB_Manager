# Description Profile v2

`*_Desc.csv` started as a compact table description file for tabular DB loads.
For production use it should become the first artifact in a broader DB
understanding pipeline, not a place to store every possible statistic.

## Scope

`*_Desc.csv` v2 is a column-level DB design summary. It keeps the compatibility
fields needed by existing create/index code while adding the small set of
signals needed to judge column type, nullability, key candidacy, and index risk.

The richer evidence lives in `*_profile.json`. Multi-table relationship
inference belongs in a future `dataset_profile.json`, because foreign-key and
normalization hints are unsafe to infer from one table in isolation.

## Artifact Boundary

| Artifact | Purpose |
| --- | --- |
| `*_Desc.csv` | Human-readable and DB-loader-compatible column design table. |
| `*_profile.json` | Detailed evidence, source metadata, profile backend, warnings, and NameMap. |
| `dataset_profile.json` | Future multi-table relationship candidates for RDB/schema visualization. |

## v2 CSV Columns

The v2 CSV includes these primary fields:

- `source_column`, `sql_column`, `description`
- `suggested_type`, `type_family`, `type_confidence`, `type_reason`
- `row_count`, `non_null_count`, `null_count`, `null_ratio`
- `empty_string_count`, `empty_string_ratio`
- `min_len`, `max_len`, `p95_len`, `max_byte_len`
- `numeric_min`, `numeric_max`, `date_min`, `date_max`
- `unique_count`, `unique_ratio`, `top_value`, `top_freq_ratio`
- `is_key_candidate`, `index_recommended`, `warnings`

It also preserves compatibility aliases:

- `Type` mirrors `suggested_type`
- `Null_ratio` mirrors `null_ratio`
- `is_key` mirrors `is_key_candidate`
- `Description` mirrors `description`

## Viewer Integration

The RDB/schema viewer should consume the artifacts in stages:

1. Read `*_Desc.csv` to display compact column-level badges: type confidence,
   null ratio, unique ratio, key/index recommendation, and warning state.
2. Read `*_profile.json` for inspector details when an operator selects a
   column.
3. Later, read `dataset_profile.json` to draw relationship-candidate edges and
   expose value-overlap evidence.

The current schema viewer can already overlay one table-level `*_profile.json`
through `--description-profile`, and it auto-detects
`<PATH>/<table_name>_profile.json` when present. This is intentionally limited
to column-level badges and payload enrichment. It does not yet infer
cross-table relationships.

Relationship candidates must be rendered as candidates, not confirmed foreign
keys, unless a DB constraint or operator-provided rule confirms them.

## Rust Boundary

Rust should accelerate profile calculation, not own the artifact contract. The
Python layer remains responsible for CLI behavior, compatibility fields, NameMap
integration, and JSON/CSV writing. This keeps `tabular describe --backend auto`
able to fall back to Python when the Rust extension is unavailable.
