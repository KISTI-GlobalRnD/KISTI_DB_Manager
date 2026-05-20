# Dataset Profile v1

`dataset_profile.json` is the dataset-level companion to per-table
`*_profile.json` files. Its job is to help operators understand a group of
tables together without pretending that inferred relationships are confirmed
database constraints.

## Goal

The first version should answer three questions:

- Which table profiles belong to this dataset?
- Which relationship candidates are worth showing in the schema viewer?
- What evidence and warning flags explain each candidate?

It should not replace DB introspection, migration tooling, or confirmed foreign
key metadata. Confirmed constraints should still come from the live database or
operator-provided rules.

## Inputs

The v1 builder accepts a directory, glob pattern, or explicit file list containing
`*_profile.json` artifacts produced by `tabular describe`.

Optional inputs:

- a run report containing `name_maps_json`
- a base table name and `KEY_SEP`
- a DB/schema viewer payload for future enrichment

The initial implementation should work without a DB connection.

## Artifact Shape

Top-level fields:

```json
{
  "schema_version": "1.0",
  "generated_at": "2026-05-20T00:00:00+00:00",
  "backend": "python",
  "source": {
    "profile_count": 3,
    "profile_paths": ["works_profile.json"]
  },
  "dataset": {
    "base_table": "works",
    "key_sep": "__"
  },
  "tables": [],
  "relationship_candidates": [],
  "warnings": []
}
```

Table entries should stay compact:

```json
{
  "table_sql": "works__authorships",
  "table_original": "works__authorships",
  "source_file": "/path/to/authorships.csv",
  "row_count": 1000,
  "column_count": 8,
  "key_candidates": ["id"],
  "index_recommended_columns": ["id", "work_id"],
  "warnings": ["contains_nulls"]
}
```

Relationship candidate entries should preserve evidence:

```json
{
  "parent_table_sql": "works",
  "child_table_sql": "works__authorships",
  "parent_column_sql": "id",
  "child_column_sql": "id",
  "relationship_type": "naming_parent_child",
  "confidence": 0.8,
  "evidence": {
    "source": "table_name_path",
    "parent_unique_ratio": 1.0,
    "child_null_ratio": 0.0,
    "shared_column_name": true,
    "key_match_source": "exact_id"
  },
  "warnings": [],
  "status": "candidate"
}
```

`status` must be one of:

- `candidate`: inferred from profiles or naming conventions
- `confirmed`: confirmed by a DB constraint or operator rule
- `rejected`: retained for audit after an operator rejects it

The first implementation should emit only `candidate`.

## Candidate Rules

Use conservative, explainable rules first.

1. Naming path relationship
   - If table names follow the known flattened naming pattern, such as
     `base__child` or `base__child__nested`, create a parent-child candidate.
   - Prefer `id` to `id` when both sides expose the column.
   - If `id` is absent, allow an exact shared column name only when the parent
     column is a key candidate or is nearly unique/non-null in its table profile.
     This keeps WoS-style `UID` relationships visible without scanning source
     values.
   - Confidence starts high enough to display, but still remains a candidate.

2. Explicit key-like columns
   - If a child table has `parent_id`, `<parent>_id`, or a profile column marked
     `index_recommended`, create a lower-confidence candidate to a parent table
     with an `id` key candidate.
   - Do not create a candidate when null ratio is high or type families differ.

3. Value-overlap candidates
   - Keep this out of v1.
   - Only revisit it as an explicit opt-in feature with hard budgets for rows,
     columns, candidate pairs, and output size.
   - The current `*_profile.json` contains top values but not enough
     distribution evidence to safely infer overlap.

## Viewer Integration

The schema viewer should consume `dataset_profile.json` in addition to the
existing one-table `*_profile.json` overlay. This is the next phase after the
v1 artifact builder.

Initial viewer behavior:

- add `--dataset-profile path/to/dataset_profile.json`
- auto-detect `<PATH>/dataset_profile.json` when present
- add candidate metadata to existing edges when names match
- draw new candidate edges only when no structural naming edge already exists
- label candidate edges clearly as candidates, not constraints

The table inspector should show candidate evidence and warnings next to the
relationship SQL. The top-level overview should also distinguish structural
naming edges from Dataset Profile evidence, surface unmatched candidates, and
make disconnected non-base tables visible without forcing operators to inspect
every table card.

## CLI Shape

Recommended command:

```bash
kisti-db-manager tabular profile-dataset \
  --profiles "path/to/*_profile.json" \
  --base-table works \
  --key-sep "__" \
  --out path/to/dataset_profile.json
```

Support an explicit repeated form later if shell globbing is inconvenient:

```bash
kisti-db-manager tabular profile-dataset \
  --profile works_profile.json \
  --profile works__authorships_profile.json \
  --out dataset_profile.json
```

## Implementation Phases

Phase 1: artifact builder (implemented)

- load and validate multiple `*_profile.json` files
- summarize tables
- infer naming-path relationship candidates
- write deterministic `dataset_profile.json`
- add focused unit tests

Phase 2: viewer overlay (initial implemented)

- add `--dataset-profile`
- merge candidate evidence into schema viewer payload
- render relationship candidate badges and warnings
- summarize table roles, candidate-backed relationships, coverage gaps, and
  disconnected tables in the viewer overview
- draw candidate-only SVG/Mermaid edges when both endpoint tables exist and no
  structural naming edge already covers the pair
- add no-DB viewer tests

The overlay attaches Dataset Profile candidate evidence to existing structural
relationship cards and draws dashed candidate-only edges for known table pairs
that are not already covered by naming structure.

Phase 3a: profile-only candidate audit (implemented)

- score emitted candidates into confidence buckets and review priorities
- count candidate warning types without scanning source data
- record skipped naming-path hints such as missing parent/child `id` columns
- write `value_overlap.status=not_computed` explicitly to avoid implying
  sampled value evidence exists
- surface review-priority and skipped-hint counts in the schema viewer overview

Phase 3b: bounded optional evidence

- add optional bounded value sketches or sampled hashes behind explicit flags
- support low-confidence key-like column candidates only after pruning the
  candidate-pair search space
- expose candidate filtering in the viewer

Default budgets should stay conservative:

- never compare all column pairs across all tables by default
- only compare key-like/index-recommended columns
- cap candidate pairs before reading values
- cap sampled rows per table
- cap sampled hashes per column
- write budget/exclusion counts into `dataset_profile.json`

Phase 4: operator decisions

- implemented for Schema Viewer overlays through `relationship_decisions.json`
- keep confirmed/rejected decisions as a separate overlay artifact
- never overwrite inferred evidence with operator decisions

## Non-goals for v1

- automatic FK creation
- DB migration generation
- full value-overlap scans over large tables
- all-pairs column comparison across all tables
- relationship claims based only on matching column names
- Rust ownership of the artifact contract
