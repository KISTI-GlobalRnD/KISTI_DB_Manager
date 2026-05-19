# 재시작 및 복구

중단 후에는 먼저 어느 단계에서 중단됐는지 확인합니다.

## Parse 단계

확인할 파일:

- `run_report.json.progress.json`
- run directory의 parquet 출력 상태

`parse-parquet*` 재시작은 보통 현재 batch 일부를 다시 처리할 수 있습니다.
row 단위 정확 재개가 아니라 source/batch 단위 재개로 이해해야 합니다.

## Materialize 단계

확인할 파일:

- `runs/<run_dir>/parquet_materialize/progress.json`

큰 parquet 파일은 `--file-chunk-rows N`을 사용하면 파일 내부 offset 기준으로 재개할 수 있습니다.

```bash
uv run python scripts/oa_materialize_parquet_to_db.py \
  runs/<run_dir> \
  --dotenv path/to/.env \
  --file-chunk-rows 5000
```

## Reload 단계

plan-driven reload는 preflight를 먼저 실행합니다.

```bash
kisti-db-manager parquet preflight --plan runs/<run_dir>/plans/parquet_reload_plan.json
kisti-db-manager parquet reload --plan runs/<run_dir>/plans/parquet_reload_plan.json
```

preflight 실패는 hard stop으로 봅니다.
target DB reset, 권한, schema manifest, ID compaction 계약을 다시 확인한 뒤 재시도합니다.
