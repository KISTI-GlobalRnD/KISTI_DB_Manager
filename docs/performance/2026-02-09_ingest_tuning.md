# Performance Improvement Report (2026-02-09) — JSON Flatten + MariaDB LOAD DATA Tuning

- Focus: reducing end-to-end wall time for JSON flattening (= TSV generation) plus `LOAD DATA LOCAL INFILE`
- Bench runner: `examples/bench_ingest_chunk_sweep.py` (the DB is `DROP/CREATE`d on every run to keep conditions fixed)

## Test Environment

| Item | Value |
|---|---|
| OS | Ubuntu 24.04.1 (kernel `6.14.0-37-generic`) |
| CPU | AMD Ryzen Threadripper PRO 5975WX (64 vCPU) |
| RAM | 995GiB |
| Python | 3.12.3 |
| Package | `kisti-db-manager==0.7.0` |
| DB | MariaDB 10.11.15 (Docker, `127.0.0.1:3307`) |
| DB vars | `local_infile=ON`, `innodb_flush_log_at_trx_commit=2`, `sync_binlog=0` |

Notes:
- `innodb_flush_log_at_trx_commit=2` is applied on a best-effort basis when `fast_load_session=true` is enabled. This is a durability tradeoff, so use `--no-fast-load-session` or adjust the DB setting if your operational policy requires it.

## Benchmark Data

This tuning pass used synthetic JSONL shaped similarly to WoS-like nested records.

| Name | Path | Notes |
|---|---|---|
| wos_like_20000 | `/tmp/bench_wos_like_20000.jsonl` | 20,000 records, 12 tables, `rows_loaded=160,000` |
| driftcols | `/tmp/bench_wos_like_20000_driftcols.jsonl` | induces column drift across parallel flatten slices |
| (repo sample) | `examples/data/bench_20lists_20000.jsonl` | fixed sample included in the repository (not tuned separately) |

## Result 1) `wos_like_20000` (end-to-end)

### Compared Configurations

Baseline (single-path):
- `--mode ingest-fast --chunk-size 20000 --parallel-workers 0 --db-load-parallel-tables 0`
- `--no-overlap-batches --no-tsv-merge-union-schema`

Tuned (parallel + overlap):
- `--mode ingest-fast-parallel --chunk-size 5000 --parallel-workers 8 --db-load-parallel-tables 8`
- `--overlap-batches --tsv-merge-union-schema`

### Total Time (`pipeline.json.total`, 2 repetitions)

| Variant | reps (ms) | avg (ms) | avg (s) | speedup |
|---|---:|---:|---:|---:|
| baseline | 5078, 6237 | 5658 | 5.658 | 1.00x |
| tuned | 1925, 1950 | 1938 | 1.938 | 2.92x |

Improvement (average basis):
- total: **-65.8%**, **2.9x** faster

Interpretation note:
- In the tuned run, `chunk_size=5000` produces 4 batches, so `tables_loaded/load_data_ok=48` is expected (= 12 tables x 4 batches). The baseline uses a single batch, so it stays at 12.

### Representative RunReport Diff Highlights

The example below uses one representative before/after pair. The reports were written under local `/tmp/`.

- before: `/tmp/bench_sweep_kbs_20260209T104033Z_c20000_pw0_r1_report.json`
- after: `/tmp/bench_sweep_kbs_20260209T104229Z_c5000_pw8_r1_report.json`

Key changes (same `rows_loaded=160,000`):
- `json.flatten`: 1371ms → 396ms (-975ms)
- `db.load_data.exec`: 3008ms → 849ms (-2159ms)
- When `overlap_batches=true`, also inspect `db.load_data.exec.wall` to understand the actual DB-side wall time (after=1391ms).

## Result 2) `driftcols` (union-merge effect under schema drift)

Condition (same base config):
- `--mode ingest-fast-parallel --chunk-size 5000 --parallel-workers 8 --db-load-parallel-tables 8 --overlap-batches`

| Variant | load_data_ok | total_ms (rep1) | Notes |
|---|---:|---:|---|
| `--no-tsv-merge-union-schema` | 76 | 1981 | schema fragments diverge, so `LOAD DATA` is called more often |
| `--tsv-merge-union-schema` | 48 | 1967 | `tsv.merge.union=57ms`, and union-merge occurs across 4 tables |

Conclusion:
- For data where parallel flatten produces schema fragments per worker slice, `tsv_merge_union_schema` reduces the number of `LOAD DATA` calls and lowers DB-side variability.
- However, if the union schema becomes excessively wide and sparse, it can backfire. The default heuristics (`coverage`, `union cols`, `missing cols`) should remain enabled.

## Tuning Rules from This Benchmark (Operational Guidance)

- If DB ingest is the bottleneck, raising `db_load_parallel_tables` to 4~8 gives the largest win first.
- `overlap_batches` materially reduces end-to-end wall time when both flattening and DB load matter.
- `parallel_workers` helps only when flattening or TSV generation is the bottleneck. If the DB is the bottleneck, its effect is limited.
- `load_data_commit_strategy` did not show a consistent benefit in this environment, so the default (`file`) remains the recommendation.

## Reproduction

Baseline:

```bash
.venv/bin/python examples/bench_ingest_chunk_sweep.py \
  --input /tmp/bench_wos_like_20000.jsonl \
  --mode ingest-fast --schema-mode evolve \
  --chunk-sizes 20000 --workers 0 --db-load-parallel-tables 0 \
  --load-data-commit-strategy file \
  --no-tsv-merge-union-schema --no-overlap-batches \
  --reps 2 --tmp-dir /tmp --out-json /tmp/sweep_woslike_baseline.json
```

Tuned:

```bash
.venv/bin/python examples/bench_ingest_chunk_sweep.py \
  --input /tmp/bench_wos_like_20000.jsonl \
  --mode ingest-fast-parallel --schema-mode evolve \
  --chunk-sizes 5000 --workers 8 --db-load-parallel-tables 8 \
  --load-data-commit-strategy file \
  --tsv-merge-union-schema --overlap-batches \
  --reps 2 --tmp-dir /tmp --out-json /tmp/sweep_woslike_tuned.json
```

Profile and diff commands (use the `report_path` emitted by the sweep output):

```bash
kisti-db-manager report profile /tmp/<run_report.json> --top 12
kisti-db-manager report diff /tmp/<before.json> /tmp/<after.json> --out diff.md
```
