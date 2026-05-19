# OpenAlex 운영 절차

현재 OpenAlex 운영은 parquet-first 흐름을 기본으로 둡니다.

## 기본 실행

```bash
kisti-db-manager json run \
  --config path/to/openalex_config.json \
  --mode parse-parquet-safe \
  --flatten-backend rust-arrow \
  --id-compaction \
  --chunk-size 10000
```

이 설정은 Rust Arrow 경로와 ID compaction을 사용하고, `parallel_workers`를 명시하지 않으면 현재 scoped default인 `8`을 사용합니다.

## 사전 점검

`abstract_inverted_index`처럼 폭발적인 dict branch가 있으면 먼저 review plan을 실행합니다.

```bash
kisti-db-manager review plan \
  --config path/to/openalex_config.json \
  --auto-except \
  --auto-except-sample-records 5000 \
  --auto-except-sample-max-sources 64 \
  --out plan_out
```

## Parquet 계약 확인

```bash
kisti-db-manager parquet inspect \
  --parquet-root runs/<run_dir>/parquet \
  --require-schema-manifest \
  --require-id-compaction
```

이 단계가 실패하면 DB 적재로 넘어가지 않습니다.

## DB 적재

```bash
uv run python scripts/oa_materialize_parquet_to_db.py \
  runs/<run_dir> \
  --dotenv path/to/.env \
  --db-name target_openalex_db \
  --materialize-preset openalex-idcompact-fast
```

반복 운영에서는 `parquet preflight`와 `parquet reload`를 사용하는 plan-driven 경로를 우선합니다.
