# OpenAlex Sample

This directory contains a small parquet-first OpenAlex works sample used by the
example schema and profile workflows.

- Base table: `openalex_works_20260225`
- Tables: 22
- Rows: 20,288 total across the parquet files
- Sampling: up to 1,000 rows per table from a local OpenAlex parquet-first probe
- Relationship key: `id` on the base and sub tables

The sample replaces the older WoS Feather files so the checked-in example data
matches the repository's parquet-first operational path.

Regenerate the sample schema image:

```bash
uv run --all-extras python examples/generate_data_sample_schema.py
```
