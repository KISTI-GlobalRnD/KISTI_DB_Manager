# 성능 개선 리포트 (2026-02-09) — JSON Flatten + MariaDB LOAD DATA 튜닝

- Focus: JSON flattening(=TSV 생성) + `LOAD DATA LOCAL INFILE` 적재 end-to-end wall time 단축
- Bench runner: `examples/bench_ingest_chunk_sweep.py` (매 실행마다 DB `DROP/CREATE`로 조건 고정)

## 실험 환경

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
- `innodb_flush_log_at_trx_commit=2`는 `fast_load_session=true`에서 best-effort로 적용됨(내구성 trade-off). 운영 정책에 맞게 `--no-fast-load-session` 또는 DB 설정을 조정 권장.

## 벤치 데이터

이번 튜닝은 WoS와 유사한 형태의 synthetic JSONL을 대상으로 수행.

| Name | Path | Notes |
|---|---|---|
| wos_like_20000 | `/tmp/bench_wos_like_20000.jsonl` | 20,000 records, 12 tables, `rows_loaded=160,000` |
| driftcols | `/tmp/bench_wos_like_20000_driftcols.jsonl` | parallel flatten slice별 컬럼 드리프트를 유도 |
| (repo sample) | `examples/data/bench_20lists_20000.jsonl` | 레포에 포함된 고정 샘플(별도 튜닝 미실시) |

## 결과 1) wos_like_20000 (end-to-end)

### 비교 설정

Baseline(싱글):
- `--mode ingest-fast --chunk-size 20000 --parallel-workers 0 --db-load-parallel-tables 0`
- `--no-overlap-batches --no-tsv-merge-union-schema`

Tuned(병렬 + 오버랩):
- `--mode ingest-fast-parallel --chunk-size 5000 --parallel-workers 8 --db-load-parallel-tables 8`
- `--overlap-batches --tsv-merge-union-schema`

### 총 시간 (`pipeline.json.total`, 2회 반복)

| Variant | reps (ms) | avg (ms) | avg (s) | speedup |
|---|---:|---:|---:|---:|
| baseline | 5078, 6237 | 5658 | 5.658 | 1.00x |
| tuned | 1925, 1950 | 1938 | 1.938 | 2.92x |

개선 폭(평균 기준):
- total: **-65.8%**, **2.9x** 빠름

해석 시 주의:
- tuned는 `chunk_size=5000`으로 4 batch라서 `tables_loaded/load_data_ok`가 48(=12 tables x 4 batches)로 증가하는 것이 정상임. (baseline은 1 batch라서 12)

### 대표 RunReport diff 하이라이트

아래는 “대표 1회(before/after)” 기준. (리포트는 로컬 `/tmp/`에 생성됨)

- before: `/tmp/bench_sweep_kbs_20260209T104033Z_c20000_pw0_r1_report.json`
- after: `/tmp/bench_sweep_kbs_20260209T104229Z_c5000_pw8_r1_report.json`

핵심 변화(동일 `rows_loaded=160,000`):
- `json.flatten`: 1371ms → 396ms (-975ms)
- `db.load_data.exec`: 3008ms → 849ms (-2159ms)
- `overlap_batches=true`일 때 실제 DB 로딩 wall-time은 `db.load_data.exec.wall`로도 함께 확인 권장 (after=1391ms)

## 결과 2) driftcols (schema drift에서 union-merge 효과)

조건(동일):
- `--mode ingest-fast-parallel --chunk-size 5000 --parallel-workers 8 --db-load-parallel-tables 8 --overlap-batches`

| Variant | load_data_ok | total_ms (rep1) | Notes |
|---|---:|---:|---|
| `--no-tsv-merge-union-schema` | 76 | 1981 | schema fragment가 갈라져 `LOAD DATA` 호출 증가 |
| `--tsv-merge-union-schema` | 48 | 1967 | `tsv.merge.union=57ms`, union-merge가 4개 테이블에서 발생 |

결론:
- parallel flatten에서 “worker slice별로 스키마 조각이 생기는” 데이터는 `tsv_merge_union_schema`가 `LOAD DATA` 호출 수를 줄여 DB 적재의 변동성을 낮춰줌.
- 단, union schema가 과도하게 넓어지는(매우 sparse) 케이스에서는 역효과가 날 수 있어 기본 휴리스틱(coverage/union cols/missing cols) 유지 권장.

## 이번 벤치 기준 튜닝 규칙(실무 적용용)

- DB ingest가 병목이면 `db_load_parallel_tables`를 먼저 4~8로 올리는 것이 가장 큰 이득.
- `overlap_batches`는 flatten과 DB load가 둘 다 의미 있을 때 end-to-end wall time을 크게 줄임.
- `parallel_workers`는 flatten/TSV 생성이 병목일 때만 이득. DB가 병목이면 효과가 제한적.
- `load_data_commit_strategy`는 이번 데이터/환경에서는 일관된 개선이 없어서 기본(`file`) 유지.

## 재현 방법

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

프로파일/디프(리포트 파일 경로는 sweep 출력의 `report_path` 참고):

```bash
kisti-db-manager report profile /tmp/<run_report.json> --top 12
kisti-db-manager report diff /tmp/<before.json> /tmp/<after.json> --out diff.md
```

