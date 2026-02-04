# KISTI_DB_Manager

![Logo](Image/KISTI_DB_Manager.svg)

MariaDB/MySQL handling utilities for preprocessing, import/export, and management.

## Versioning note (0.7.0)

Starting from **0.7.0**, this repository keeps a single implementation:
- **`KISTI_DB_Manager` is the “v2” codebase** (refactor + robustness + performance).
- The old v1-only implementation has been removed from the working tree (available in git history).

## Goals

- Keep the “never fail the whole run” philosophy for messy/heterogeneous data
- Make table/column naming constraints consistent across create/load/index steps
- Make large JSON/XML ingestion fast (bulk load + streaming) and observable (RunReport timings)

## What’s in the box

- **One-shot pipelines**
  - `tabular run`: Description → CREATE → LOAD → INDEX → OPTIMIZE
  - `json run`: records → flatten(main+subs) → CREATE/ALTER → LOAD → INDEX → OPTIMIZE
- **Schema drift handling**
  - New columns: best-effort `ALTER TABLE ADD COLUMN`
  - Insert failures: best-effort widen/add failing column (default `LONGTEXT`) and retry
  - Optional **schema freeze**: keep base schema stable and store unknown fields into `__extra__`
- **Performance**
  - `LOAD DATA LOCAL INFILE` fast path for bulk ingest (tabular + JSON streaming rows)
  - Chunk/batch controls, parallel JSON flattening, and stage timings/throughput in `RunReport`
- **Operational safety**
  - `RunReport` JSON + `Quarantine` JSONL for continue-on-error ingestion
- **Review/visualization**
  - Review pack generation (md/html/svg) and schema diagrams (optional extras)

## Install

```bash
pip install -e .
```

Optional extras:

```bash
pip install -e ".[db]"
pip install -e ".[viz]"
pip install -e ".[review]"
pip install -e ".[db,viz]"
pip install -e ".[db,review]"
```

## CLI

```bash
kisti-db-manager version
kisti-db-manager modes
kisti-db-manager report summary path/to/run_report.json
kisti-db-manager report diff path/to/before.json path/to/after.json --out diff.md
kisti-db-manager quarantine summary path/to/quarantine.jsonl --out quarantine_out
kisti-db-manager tabular run --config path/to/config.json --report run_report.json --quarantine quarantine.jsonl
kisti-db-manager json run --config path/to/json_config.json --report json_report.json --quarantine quarantine.jsonl
```

### Modes (presets)

Large data (recommended flow):

```bash
# 1) ingest only (skip index/optimize)
kisti-db-manager json run --config path/to/json_config.json --mode ingest-fast

# 2) build indexes + optimize after ingest
kisti-db-manager json run --config path/to/json_config.json --mode finalize
```

Schema drift heavy + ALTER is too expensive:

```bash
kisti-db-manager json run --config path/to/json_config.json --mode ingest-fast-freeze
```

Korean ops guide (decision rules + checklist):
- `KISTI_DB_Manager/GUIDE_KO.md`

## Python API (v1-style usage)

Most v1-style notebooks can keep the same import:

```python
from KISTI_DB_Manager import manage, preview

flist = sorted([x for x in os.listdir(data_config["PATH"]) if x.endswith(".csv")])
for f in flist:
    data_config = preview.update_data_config(f, data_config)
    manage.create_table(data_config, db_config)
    manage.fill_table_from_file(data_config, db_config)
    manage.set_index(db_config, data_config)
    manage.optimize_table(db_config, data_config)
```

## Smoke test (Docker MariaDB)

We ship a reproducible smoke test under `KISTI_DB_Manager/examples/`.

```bash
cd KISTI_DB_Manager/examples
docker compose up --build --abort-on-container-exit smoke
docker compose down
```

Or on host (requires deps installed):

```bash
bash KISTI_DB_Manager/examples/smoke.sh
```
