# Schema Display Benchmark from 02_GoldSet

## Source project
- `/home/kimyoungjin06/Desktop/Workspace/1.2.8.TwinPaper_Module02_GoldenSet`

## What 02_GoldSet does well
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
- DB introspection layer:
- `KISTI_DB_Manager/review.py:80` (`DBIntrospector`)
- `KISTI_DB_Manager/review.py:100` (`list_tables_like`)
- `KISTI_DB_Manager/review.py:205` (`table_columns`)
- `KISTI_DB_Manager/review.py:238` (`table_indexes`)
- Review assembly:
- `KISTI_DB_Manager/review.py:2658` (`generate_review_plan`)
- `KISTI_DB_Manager/review.py:2886` (`generate_review_pack`)
- HTML rendering path:
- `KISTI_DB_Manager/review.py:917` (`_render_html`)
- CLI entrypoints:
- `KISTI_DB_Manager/cli.py:623` (`_cmd_review_pack`)
- `KISTI_DB_Manager/cli.py:654` (`_cmd_review_plan`)
- `KISTI_DB_Manager/cli.py:701` (`_cmd_review_preview`)

## Benchmark takeaway for this repo
- Adopt the same two-step model:
- Step 1: build a canonical `schema_preview_payload.json`
- Step 2: render one standalone HTML from that payload
- Keep payload stable so downstream tools can consume it without parsing HTML.
- Reuse existing introspection (`DBIntrospector`) and `TableInfo` merge path instead of adding new DB queries.

## Minimal implementation slice (safe while ingest is running)
- Add payload writer in `generate_review_pack`:
- include table list, row estimates, columns, indexes, and join hints
- Add HTML renderer adapter in `review.py`:
- consume payload and show summary cards + table metadata grid
- Expose output paths in CLI:
- print `schema_preview_json` and `schema_preview_html` next to existing schema artifacts

## Notes
- 02_GoldSet uses static artifact generation (not a live web server); this matches current batch-style architecture and is low-risk to port.
