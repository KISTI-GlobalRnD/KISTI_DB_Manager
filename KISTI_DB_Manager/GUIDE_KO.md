<!--
현행 운영 요약 문서입니다. 상세 영문 매뉴얼은 docs/manual/ 아래를 기준으로 유지합니다.
-->

# KISTI_DB_Manager 운영 가이드

이 문서는 실무 실행 순서를 빠르게 고르기 위한 한국어 요약입니다. 자세한 기준과 최신 명령은 `docs/manual/`과 `docs/reference/cli.md`를 우선 확인하세요.

## 기본 원칙

- 운영에서는 `default` 모드에 기대지 말고 목적에 맞는 mode를 명시합니다.
- 대용량 JSON은 먼저 `review plan` 또는 작은 `json profile-parallel` 샘플로 병목을 확인합니다.
- 장시간 작업은 `RunReport`, quarantine, parquet artifact, progress 파일을 남기도록 구성합니다.
- 테스트 DB smoke는 고유 prefix를 쓰고, cleanup 대상이 정확히 해당 prefix인지 확인합니다.

## 설치

```bash
# JSON/XML + DB ingest
pip install -e '.[json,db]'

# 리뷰/시각화 포함
pip install -e '.[json,db,viz,review]'

# Rust backend 개발/검증
pip install -e '.[json,db,rust]'
python -m maturin develop --manifest-path crates/kisti_json_rs/Cargo.toml --release
```

## 실행 흐름 선택

### DB-first

DB 적재 완료 시간이 최우선이면 `ingest-fast -> finalize`를 기본으로 둡니다.

```bash
kisti-db-manager json run --config path/to/config.json --mode ingest-fast
kisti-db-manager json run --config path/to/config.json --mode finalize
```

`LOCAL INFILE`이 막혀 있거나 환경 호환성이 더 중요하면 `ingest-safe`를 사용합니다.

```bash
kisti-db-manager json run --config path/to/config.json --mode ingest-safe
```

스키마 drift가 심하고 `ALTER TABLE`이 병목이면 freeze/hybrid를 검토합니다.

```bash
kisti-db-manager json run --config path/to/config.json --mode ingest-fast-freeze
kisti-db-manager json run --config path/to/config.json --mode ingest-fast-hybrid --schema-hybrid-warmup-batches 3
```

### Artifact-first

parquet artifact를 표준 중간 산출물로 남기고 DB 적재를 나중에 하고 싶다면 `parse-parquet-safe`를 사용합니다.

```bash
kisti-db-manager json run --config path/to/config.json --mode parse-parquet-safe
```

이후 DB 적재는 parquet materialize/reload 경로를 사용합니다.

```bash
python scripts/oa_materialize_parquet_to_db.py \
  runs/<parse_run_dir> \
  --dotenv path/to/.env \
  --db-name target_db
```

또는 plan 기반 reload를 사용합니다.

```bash
kisti-db-manager parquet inspect \
  --parquet-root runs/<parse_run_dir>/parquet \
  --require-schema-manifest \
  --require-id-compaction
kisti-db-manager parquet preflight --plan runs/<parse_run_dir>/plans/parquet_reload_plan.json
kisti-db-manager parquet reload --plan runs/<parse_run_dir>/plans/parquet_reload_plan.json
```

## Rust backend

Python/Rust backend 선택은 mode 선택과 별개입니다. 운영 전에는 샘플 profile로 먼저 비교합니다.

```bash
kisti-db-manager json profile-parallel \
  --config path/to/config.json \
  --flatten-backends python,rust-arrow \
  --workers 0,2,4,8 \
  --max-records 20000 \
  --chunk-size 5000 \
  --repeat 3 \
  --out runs/profile_parallel_test
```

Rust parquet backend를 강제하려면 다음처럼 실행합니다.

```bash
kisti-db-manager json run \
  --config path/to/config.json \
  --mode parse-parquet-safe \
  --flatten-backend rust-arrow
```

plain JSONL/NDJSON의 parse/parquet-only 실행에서는 JSON line decoding도 Rust로 넘길 수 있습니다.

```bash
kisti-db-manager json run \
  --config path/to/config.json \
  --mode parse-parquet-safe \
  --flatten-backend rust-arrow \
  --rust-raw-jsonl-parse
```

지원되는 경우 `--rust-raw-jsonl-file-parse`를 추가하면 Python line loop 없이 Rust가 JSONL/NDJSON 파일을 직접 읽습니다. ID compaction이 켜진 실행은 현재 batch raw parser 경로를 사용합니다.

Rust MySQL loader는 아직 명시 opt-in입니다. 테이블 생성/스키마 매핑은 Python이 유지하고, Rust가 parquet를 읽어 batch insert합니다.

```bash
kisti-db-manager json run \
  --config path/to/config.json \
  --mode ingest-safe \
  --flatten-backend rust-arrow \
  --rust-db-load
```

실제 DB smoke는 다음 스크립트를 사용합니다. 기본적으로 테스트 테이블을 삭제합니다.

```bash
python scripts/smoke_rust_db_load.py --dotenv .env
```

## OpenAlex ID compaction

OpenAlex-scale run은 긴 작업 전에 preflight를 먼저 권장합니다.

```bash
kisti-db-manager json id-compaction-preflight \
  --config path/to/openalex_config.json \
  --max-records 100000 \
  --report id_compaction_preflight.json
```

실행 시에는 명시적으로 켭니다.

```bash
kisti-db-manager json run \
  --config path/to/openalex_config.json \
  --mode parse-parquet-safe \
  --id-compaction
```

기본 policy는 충돌을 숨기지 않고 실패시키는 것입니다. 충돌을 보존해 검토하려는 경우에만 `preserve` 정책을 사용하세요.

## 검증 체크리스트

```bash
python -m unittest discover -s tests -q
cargo fmt --manifest-path crates/kisti_json_rs/Cargo.toml --check
cargo check --manifest-path crates/kisti_json_rs/Cargo.toml
cargo test --manifest-path crates/kisti_json_rs/Cargo.toml
mkdocs build --strict
```

DB smoke 후에는 임시 테이블이 남지 않았는지 확인합니다.

## 상세 문서

- `docs/manual/json-modes.md`: mode 선택 기준
- `docs/manual/json-rust-backend.md`: Rust backend/profile/smoke/benchmark
- `docs/manual/openalex-workflow.md`: OpenAlex parquet-first 운영 흐름
- `docs/manual/restart-recovery.md`: 재시작/복구 정책
- `docs/reference/cli.md`: 빠른 명령 reference
