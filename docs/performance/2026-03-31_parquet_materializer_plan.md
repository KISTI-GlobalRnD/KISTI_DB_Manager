# OpenAlex Parquet Materializer Plan (2026-03-31)

- 목적: `parse-parquet*`로 생성한 parquet 아티팩트를 canonical raw layer로 두고, 이후 `MariaDB/MySQL` 적재를 별도 materialization 단계로 수행한다.
- 배경: 현재는 `raw JSON -> flatten -> DB` 경로는 강하지만, `parquet -> DB`는 독립 모듈이 없다. 따라서 local-first 작업과 full raw DB 적재를 동시에 만족시키기 어렵다.

## 결론

- `raw -> flatten/parquet`는 한 번만 수행한다.
- 그 결과물(parquet)을 기준으로:
  - 로컬 분석은 바로 시작한다.
  - DB 적재는 `parquet materializer`가 담당한다.
- 즉 파싱은 1회, materialization은 2종이다.

## 목표

- raw JSON 재파싱 없이 parquet만으로 DB 적재 가능
- 테이블/배치 단위 resume 가능
- 일부 테이블만 선택 적재 가능
- 생성된 parquet를 canonical artifact로 취급
- 적재 결과와 진행률을 JSON report/progress로 남김

## 비목표

- MariaDB가 parquet를 직접 읽게 만드는 것
- single-pass로 parquet와 MariaDB row-store를 동시에 최적화하는 것
- 기존 `json run` 전체 경로를 당장 대체하는 것

## 왜 별도 모듈이 필요한가

현재 `parquet -> DB`를 단순히 구현하면 내부적으로 다음으로 흐른다.

1. parquet 읽기
2. `DataFrame` 생성
3. 임시 TSV 생성
4. `LOAD DATA LOCAL INFILE`

이 방식은 기능은 되지만, full raw OpenAlex 규모에서는 end-to-end 비용이 크다. 따라서 `parquet materializer`는 다음 두 가지를 명시적으로 책임져야 한다.

- raw JSON 재파싱 없이 DB 적재만 수행
- 적재 단위를 테이블/파일 수준으로 관리하여 재개와 선별 적재를 지원

## MVP 범위

### 입력

- parquet root
- 기존 run의 `config.json`
- optional `.env` path
- optional table allowlist

### 출력

- target DB tables
- `progress.json`
- `run_report.json`

### 동작

1. parquet root의 table 디렉터리 스캔
2. 각 table 디렉터리의 `b*.parquet` 순서대로 처리
3. 첫 파일 기준으로 target table 생성
4. 각 parquet 파일을 읽어 batch 단위로 적재
5. 파일 단위 checkpoint 저장
6. 재실행 시 마지막 완료 파일 이후부터 resume

## 운영 요구사항

- `config.json`에 password가 `***`로 마스킹된 경우 `.env`에서 복원 가능해야 함
- target table prefix 지원
- overwrite/append 중 append 기본
- 일부 table만 적재하는 `--table` 옵션 필요
- 기본적으로 DROP/RECREATE 하지 않음

## 추천 progress 계약

`progress.json` 예시:

```json
{
  "updated_at_utc": "2026-03-31T00:00:00+00:00",
  "parquet_root": "/raid/.../parquet_exports/openalex_works_20260225_raw_xxx",
  "table_count": 22,
  "tables_completed": 3,
  "files_loaded": 128,
  "rows_loaded": 12345678,
  "current": {
    "table_original": "openalex_works_20260225__concepts",
    "table_sql": "openalex_works_20260225__concepts",
    "file_path": "/raid/.../concepts/b000127.parquet",
    "rows": 35439
  },
  "completed_files": {
    "openalex_works_20260225": ["b000000.parquet", "b000001.parquet"]
  }
}
```

## MVP 구현 원칙

- 기존 `manage.create_table_from_columns`
- 기존 `manage.fill_table_from_dataframe`
- 기존 `LOAD DATA LOCAL INFILE`

를 재사용한다.

즉 MVP는 “완전히 새로운 DB loader”가 아니라,
**기존 적재 로직을 parquet artifact 기준으로 재배치한 materializer**다.

## 이후 최적화 단계

### Phase 2

- file-level resume를 넘어 per-table high-watermark 최적화
- `name_maps_json`를 최종 report에서 직접 읽는 경로 추가
- 병렬 table loading
- large table 우선순위 정책

### Phase 3

- parquet row-group/chunk 기반 메모리 절감
- DuckDB/pyarrow 기반 staged CSV/TSV 생성 실험
- materializer 전용 CLI subcommand 편입

## 판단 기준

- 로컬 작업 우선이면 `parse-parquet*`는 유지
- DB 완주 속도 우선이면 `ingest-fast*`가 여전히 강함
- 둘을 함께 만족시키려면 `parquet materializer`가 필요

즉 이 문서의 결론은 하나다.

- `parquet -> MariaDB`는 소비자 기능이 아니라, 이제 모듈의 핵심 기능으로 봐야 한다.
