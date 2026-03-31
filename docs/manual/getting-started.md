# Chapter 2. Getting Started

This chapter covers install, base CLI, and the first decision point between DB-first and parquet-first operation.

## Install

Base install keeps only the lightweight commands.
For real ingest work, install extras explicitly.

```bash
pip install -e .
pip install -e ".[json,db]"
pip install -e ".[json,db,viz,review]"
```

Recommended combinations:

- tabular ingest only: `pip install -e ".[tabular,db]"`
- JSON/XML ingest only: `pip install -e ".[json,db]"`
- review and visualization: `pip install -e ".[json,db,viz,review]"`
- docs site build: `pip install -e ".[docs]"`

## Core CLI

```bash
kisti-db-manager version
kisti-db-manager modes
kisti-db-manager report profile path/to/run_report.json --top 10
kisti-db-manager json run --config path/to/openalex_config.json --mode ingest-fast
kisti-db-manager json run --config path/to/openalex_config.json --mode finalize
```

## Recommended large-data flow

For DB-first ingest:

```bash
kisti-db-manager json run --config path/to/openalex_config.json --mode ingest-fast
kisti-db-manager json run --config path/to/openalex_config.json --mode finalize
```

For local parquet-first workflows:

```bash
kisti-db-manager json run --config path/to/openalex_config.json --mode parse-parquet-safe
```

Then materialize later:

```bash
python scripts/oa_materialize_parquet_to_db.py \
  runs/<openalex_parse_run_dir> \
  --dotenv path/to/.env
```

## Decision rule

Use `ingest-fast*` when DB completion time is the priority.
Use `parse-parquet*` when local artifacts and resumable downstream work are the priority.
