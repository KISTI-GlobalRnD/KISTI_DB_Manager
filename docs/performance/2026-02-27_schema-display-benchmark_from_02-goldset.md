# Historical Benchmark: Schema Display from a Standalone Contract Viewer

!!! note "Historical status"
    This page is retained as UI/design background for schema display work.
    Current review commands and outputs are documented in
    [Review and Visualization](../manual/review-visualization.md).

## Reference implementation
- A separate standalone schema-contract viewer implementation used as the UI/UX benchmark.

## What the reference viewer does well
- Builds a single payload JSON first, then renders a standalone HTML viewer from the payload.
- Splits responsibilities cleanly:
- payload builder (`_build_payload`)
- HTML renderer (`_render_html`)
- CLI entrypoint (`main`) to regenerate reports in batch
- Includes rich schema context in one place:
- metrics
- table/column groupings
- logical schema cards (grain, key note, fk notes, ddl preview)
- interactive table controls (filter/sort/export)

## Key reference files
- `modules/02_golden_set/scripts/reporting/build_phase2_schema_visualization.py:670`
- `modules/02_golden_set/scripts/reporting/build_phase2_schema_visualization.py:808`
- `modules/02_golden_set/scripts/reporting/build_phase2_schema_visualization.py:2813`
- `data/metadata/phase2_schema_visualization_payload_20260224.json`
- `outputs/reports/phase2_curation_schema_contract_view_20260224.html`

## Current KISTI_DB_Manager insertion points

The current code keeps `KISTI_DB_Manager.review` and `KISTI_DB_Manager.cli` as compatibility facades.
Implementation now lives in smaller modules:

- DB introspection layer:
  - `KISTI_DB_Manager/_review/core.py` (`DBIntrospector`, `TableInfo`, table-info merge helpers)
- Review assembly:
  - `KISTI_DB_Manager/_review/plan.py` (`generate_review_plan`)
  - `KISTI_DB_Manager/_review/pack.py` (`generate_review_pack`)
- HTML/Markdown rendering:
  - `KISTI_DB_Manager/_review/report_html.py` (`_render_html`)
  - `KISTI_DB_Manager/_review/report_markdown.py` (`_render_markdown`, `_render_plan_markdown`)
- Schema rendering and viewer:
  - `KISTI_DB_Manager/_review/schema_render.py` (Mermaid/SVG rendering)
  - `KISTI_DB_Manager/_review/schema_payload.py` and `schema_html.py` (standalone schema viewer)
- CLI entrypoints:
  - `KISTI_DB_Manager/_cli/review.py` (`_cmd_review_pack`, `_cmd_review_plan`, `_cmd_review_preview`, `_cmd_review_schema_viewer`, `_cmd_review_diff`)

## Benchmark takeaway for this repo
- Adopt the same two-step model:
- Step 1: build a canonical `schema_preview_payload.json`
- Step 2: render one standalone HTML from that payload
- Keep payload stable so downstream tools can consume it without parsing HTML.
- Reuse existing introspection (`DBIntrospector`) and `TableInfo` merge path instead of adding new DB queries.

## Minimal implementation slice (safe while ingest is running)

The original minimal slice has been implemented and generalized:

- `review pack` emits review JSON, Markdown, HTML, Mermaid, SVG, and optional PNG artifacts.
- `review schema-viewer` emits a canonical viewer JSON payload plus standalone HTML.
- The viewer payload includes table list, row estimates, columns, indexes, relationship hints, DDL, and issue/quarantine counts where available.
- CLI output paths are printed by `_cli/review.py`.

## Notes
- The reference viewer uses static artifact generation (not a live web server); this matches the current batch-style architecture and is low-risk to port.
