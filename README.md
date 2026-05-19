# KISTI_DB_Manager

![Logo](Image/KISTI_DB_Manager.svg)

MariaDB/MySQL ingest, flattening, parquet artifact, and review tooling for large tabular and nested JSON/XML datasets.

## Documentation

The maintained documentation lives in `docs/` and is published through MkDocs.

- Start here: [docs/index.md](docs/index.md)
- Install and first run: [docs/getting-started/index.md](docs/getting-started/index.md)
- OpenAlex operations: [docs/operator-guides/openalex-runbook.md](docs/operator-guides/openalex-runbook.md)
- CLI reference: [docs/reference/cli.md](docs/reference/cli.md)
- Architecture notes: [docs/architecture/index.md](docs/architecture/index.md)
- Korean operator notes: [docs/ko/index.md](docs/ko/index.md)

README is intentionally only a portal. Detailed workflows, benchmarks, and maintainer notes belong in the MkDocs pages so they are versioned and checked by `mkdocs build --strict`.

## Install

Create and activate a Python environment first:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

```bash
pip install -e .
```

Recommended extras:

```bash
pip install -e ".[tabular,db]"
pip install -e ".[json,db]"
pip install -e ".[json,db,viz,review]"
pip install -e ".[docs]"
```

For the optional Rust backend:

```bash
pip install -e ".[json,db,rust]"
python -m maturin develop --manifest-path crates/kisti_json_rs/Cargo.toml --release
```

## Quick Start

DB-first ingest:

```bash
kisti-db-manager json run --config path/to/openalex_config.json --mode ingest-fast
kisti-db-manager json run --config path/to/openalex_config.json --mode finalize
```

Parquet-first OpenAlex-style run:

```bash
kisti-db-manager json run \
  --config path/to/openalex_config.json \
  --mode parse-parquet-safe \
  --flatten-backend rust-arrow \
  --id-compaction \
  --chunk-size 10000
```

Review before a large nested run:

```bash
kisti-db-manager review plan \
  --config path/to/openalex_config.json \
  --auto-except \
  --auto-except-sample-records 5000 \
  --auto-except-sample-max-sources 64 \
  --out plan_out
```

Build docs locally:

```bash
pip install -e ".[docs]"
mkdocs build --strict
```

## Smoke Test

Docker MariaDB smoke:

```bash
cd examples
docker compose up --build --abort-on-container-exit smoke
docker compose down
```

Host smoke:

```bash
bash examples/smoke.sh
```
