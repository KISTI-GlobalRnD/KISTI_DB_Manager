# Artifacts Reference

This page summarizes the artifact types operators should recognize.

## Run Reports

`run_report.json` records timing, counters, issues, backend choices, and artifact paths.
Use it as the first source when diagnosing a run.

```bash
kisti-db-manager report summary path/to/run_report.json
kisti-db-manager report profile path/to/run_report.json --top 20
```

## Quarantine

`quarantine.jsonl` stores records that failed parsing or normalization when continue-on-error behavior is active.

```bash
kisti-db-manager quarantine summary path/to/quarantine.jsonl --out quarantine_out
```

## Tabular Description Profiles

`tabular describe` writes two related artifacts:

- `*_Desc.csv`: a v2 column-level DB design table with compatibility fields
  (`Type`, `Null_ratio`, `is_key`, `Description`) plus type confidence,
  null/empty ratios, length statistics, uniqueness, key/index recommendations,
  and warning flags.
- `*_profile.json`: richer evidence for review and future RDB visualization,
  including schema version, source file metadata, backend, sampling policy,
  NameMap, per-column evidence, and warnings.

```bash
kisti-db-manager tabular describe --config path/to/config.json
```

The CSV is intentionally compact and loader-compatible. Multi-table
relationship candidates belong in a future dataset-level profile rather than in
each table's `*_Desc.csv`.

`review schema-viewer` can overlay this profile through `--description-profile`
or auto-detect `<PATH>/<table_name>_profile.json` when that file exists.

## Dataset Profiles

`dataset_profile.json` is the multi-table companion to per-table profiles. It
summarizes the table profiles that belong to one dataset and stores conservative
relationship candidates with their evidence.

```bash
kisti-db-manager tabular profile-dataset \
  --profiles "path/to/*_profile.json" \
  --base-table works \
  --out path/to/dataset_profile.json
```

Candidate relationships are not confirmed foreign keys. Operators should treat
them as review hints until they are backed by a DB constraint or an explicit
operator decision. v1 uses profile metadata and table naming paths only: it
prefers `id`-to-`id`, then falls back to an exact shared parent key candidate
such as WoS-style `UID`. Bounded value-overlap evidence is intentionally left
for the opt-in `--validate-relationships` path. The v1 design contract is documented in
[Dataset Profile v1](../design/dataset-profile-v1.md).
The artifact includes a profile-only `audit` block with confidence buckets,
review-priority counts, warning counts, skipped naming hints, and an explicit
`value_overlap.status=not_computed` marker by default. When relationship
validation is enabled, the audit switches to `data_scan=sampled` and records a
bounded `candidate_key_sample` summary. Each validated relationship candidate
receives `value_overlap` evidence with sampled parent/child rows, distinct key
counts, overlap ratio, orphan ratio, and validation status.

`review schema-viewer` can overlay this artifact through `--dataset-profile` or
auto-detect `<PATH>/dataset_profile.json`. When the dataset profile references
per-table `*_profile.json` paths, the viewer loads them too, so no-DB schema
views can still show column catalogs, type hints, key/index recommendations,
and warning badges. The viewer attaches candidate confidence, review priority,
key-match source, status, warnings, and evidence to matching relationship
cards. Its overview panels also summarize table roles, candidate-backed
relationships, accepted hints, review-needed relationships, key sources,
unmatched candidates, relation warnings, and disconnected non-base tables.
Tables can be filtered to review-needed relationship hints. The Relationship
Catalog lists parent-child routes, join-column hints, review priority, key
source, warnings, sampled value-overlap evidence, candidate evidence, and join
SQL in one searchable review surface. When both endpoint
tables exist but no structural naming edge covers a candidate, the SVG and
Mermaid outputs draw it as a dashed candidate edge.

## Relationship Decisions

`relationship_decisions.json` is an optional operator overlay for relationship
reviews. It should not replace or rewrite `dataset_profile.json`; it records
human decisions about inferred relationship candidates so later profile
regeneration does not erase review state.

```json
{
  "schema_version": "1.0",
  "decisions": [
    {
      "parent_table_sql": "works",
      "child_table_sql": "works__authorships",
      "parent_column_sql": "id",
      "child_column_sql": "id",
      "decision": "accepted",
      "reason": "Child rows carry the parent work id.",
      "reviewed_by": "operator",
      "reviewed_at": "2026-05-20"
    }
  ]
}
```

Supported decision values are normalized to `accepted`, `rejected`,
`needs_review`, `deferred`, or `undecided`. `review schema-viewer` can overlay
this file through `--relationship-decisions` or auto-detect
`<PATH>/relationship_decisions.json`.

## Parquet Artifacts

Parquet-first runs produce table directories under a run-specific parquet root.
OpenAlex ID-compacted runs should also include `schema_manifest.json`.

```bash
kisti-db-manager parquet inspect \
  --parquet-root runs/<run_dir>/parquet \
  --require-schema-manifest \
  --require-id-compaction
```

## Review Artifacts

Review commands generate:

- Markdown reports
- standalone HTML reports
- `schema.svg`
- `schema.mmd`
- JSON payloads for review/schema viewers

Keep generated review artifacts outside `docs/` unless they are selected public documentation examples.

## Public Documentation Artifacts

The public OpenAlex preview and schema SVG under `docs/examples/` and `docs/assets/` are intentionally checked in.
Refresh instructions live in [Review and Visualization](../architecture/review-visualization.md#regenerating-the-checked-in-openalex-schema-svg).
