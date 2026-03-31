<!--
이 문서는 “속도 최우선 + 스키마 불확실” 데이터를 다루는 실무 관점 안내서입니다.
-->

# KISTI_DB_Manager 운영 가이드 (상황별 추천)

## 공통 전제

- 입력 포맷/스키마가 출처마다 다르고, 일부는 스키마가 최신화되지 않았거나 레코드가 스키마와 불일치할 수 있음
- 목표는 **1-pass(1 loop look)** 에 가깝게 “최소 비용으로 스키마를 결정/적재”하고, **전체 작업이 중단되지 않게** 하는 것
- 대용량(수십~100GB+ 압축 JSON/XML) 기준으로 **속도가 최우선**

## 설치 (extras 권장)

기본 설치(`pip install -e .`)는 경량 명령(`version`, `modes`, `report`, `quarantine` 등) 중심입니다.
실제 적재/리뷰 작업은 목적별 extras 설치를 권장합니다.

```bash
# Tabular ingest
pip install -e ".[tabular,db]"

# JSON/XML ingest
pip install -e ".[json,db]"

# 리뷰/시각화까지 포함
pip install -e ".[json,db,viz,review]"
```

누락된 의존성이 있으면 CLI가 traceback 대신 설치 명령을 안내합니다.

## 빠르게 시작하기 (추천 워크플로우)

```bash
# 0) 모드 목록 확인
kisti-db-manager modes

# (선택) 최근 실행 리포트 병목 요약
kisti-db-manager report profile path/to/run_report.json --top 10

# 1) ingest only (index/optimize 생략)
kisti-db-manager json run --config path/to/json_config.json --mode ingest-fast

# 2) ingest 이후 index/optimize만 수행
kisti-db-manager json run --config path/to/json_config.json --mode finalize
```

실DB 통합 스모크(도커 DB 미사용) 템플릿/스크립트:
- `examples/configs/tabular_config_realdb.template.json`
- `examples/configs/json_config_realdb.template.json`
- `examples/configs/json_config_multifile_realdb.template.json`
- `bash examples/smoke_real_db.sh <tabular_config> <json_config>`

## 상황별 모드 선택

### 1) LOCAL INFILE 사용 가능 + 속도 최우선 (대부분 추천)

- 추천: `--mode ingest-fast`
- 특징:
  - 가능하면 `LOAD DATA LOCAL INFILE`로 적재(실패 시 to_sql로 폴백)
  - `chunk_size`를 크게(기본 20000) 잡아 `LOAD DATA` 호출 횟수를 줄임
  - ingest 단계에서 `index/optimize`는 생략(후처리로 분리)

### 2) 스키마 드리프트가 잦고 ALTER 비용이 치명적

- 추천: `--mode ingest-fast-freeze`
- 특징:
  - `ALTER TABLE`을 사실상 금지(`auto_alter_table=false`)
  - 예상 못한 필드는 `__extra__`(기본) 컬럼에 JSON 문자열로 저장
  - 장점: 대용량에서 “새 컬럼 등장 → ALTER 반복” 병목을 크게 줄임
  - 단점: 이후 분석에서 `__extra__`를 다시 풀어 컬럼으로 승격하는 후처리가 필요할 수 있음

### 2.5) 드리프트가 잦지만, 초반에는 컬럼을 최대한 확보하고 싶다 (hybrid)

- 추천: `--mode ingest-fast-hybrid`
- 특징:
  - 초반 `schema_hybrid_warmup_batches` 만큼은 evolve처럼 `ALTER TABLE`로 컬럼을 확보
  - warmup 이후에는 freeze처럼 `ALTER`을 중단하고 unknown 필드는 `__extra__`로 보존
  - 운영상 “초반 스키마 자동 확장 + 후반 ALTER 폭발 방지” 타협안

```bash
kisti-db-manager json run --config path/to/json_config.json --mode ingest-fast-hybrid

# warmup 배치 수 조정(예: 3 배치까지 evolve 후 freeze)
kisti-db-manager json run --config path/to/json_config.json --mode ingest-fast-hybrid --schema-hybrid-warmup-batches 3
```

### 3) 보안/권한 문제로 LOCAL INFILE이 불가 (혹은 서버 정책상 금지)

- 추천: `--mode ingest-safe`
- 특징:
  - `pandas.to_sql`로 적재(느리지만 범용)
  - 모드 자체가 “완전 속도”가 아니라 “어쨌든 적재 성공”에 초점

### 4) ingest는 끝났고 인덱스/최적화만 수행하고 싶다

- 추천: `--mode finalize`
- 특징:
  - CREATE/LOAD 없이 `INDEX + OPTIMIZE`만 수행
  - 대용량에서 시간이 오래 걸릴 수 있으므로 ingest와 분리하는 것이 유리

## v2 트레이드오프(=단점)와 운영 권장안

v2는 v1 대비 성능/견고성(무중단 ingest)은 개선됐지만, 대신 “상황별로 모드를 고르는 운영 복잡도”가 생길 수 있음.
아래는 실제 운영에서 자주 발생하는 트레이드오프와 이를 최소화하는 권장 규칙임.

### 트레이드오프(단점)

- **선택지/인지 부하 증가**: 모드/옵션이 늘어서 팀 표준이 없으면 실행마다 설정이 달라질 수 있음
- **환경/정책 의존성**: `LOCAL INFILE`이 서버/클라이언트 정책상 막혀 있으면 고속 경로(`LOAD DATA LOCAL INFILE`)를 못 쓰고 `to_sql`로 폴백되어 느려질 수 있음
- **내구성(안전성) vs 속도**: `fast_load_session=true`는 속도에 유리하지만(세션 변수 튜닝) 장애 시 데이터 손실 위험/정책 충돌 가능성이 있음
- **스키마 진화 vs 속도**: `schema_mode=freeze`는 `ALTER TABLE` 반복 병목을 줄이지만, 대신 unknown 필드가 `__extra__`에 쌓여 후처리가 필요할 수 있음
- **2-step 운용 필요**: 대용량에서는 `ingest-fast → finalize`처럼 ingest와 인덱스/최적화를 분리하는 편이 실무적으로 안전/빠름(단일 명령보다 운영 단계가 늘어남)

### 팀 운영 표준(추천)

아래 규칙만 따르면 대부분 “고민 없이” 실행 가능하도록 설계함.

1) 기본값: `ingest-fast → finalize`

```bash
kisti-db-manager json run --config path/to/json_config.json --mode ingest-fast
kisti-db-manager json run --config path/to/json_config.json --mode finalize
```

2) DB 정책상 세션 튜닝이 위험/불가하면: `ingest-fast`는 유지하되 세션 튜닝만 끄기

```bash
kisti-db-manager json run --config path/to/json_config.json --mode ingest-fast --no-fast-load-session
```

3) `LOCAL INFILE`이 막혀 있으면: `ingest-safe → finalize`

```bash
kisti-db-manager json run --config path/to/json_config.json --mode ingest-safe
kisti-db-manager json run --config path/to/json_config.json --mode finalize
```

4) 스키마 드리프트가 심하고 ALTER가 병목이면: `ingest-fast-freeze → finalize`

```bash
kisti-db-manager json run --config path/to/json_config.json --mode ingest-fast-freeze
kisti-db-manager json run --config path/to/json_config.json --mode finalize
```

## 자주 겪는 문제 체크리스트

- `LOCAL INFILE` 관련:
  - 서버 변수 `@@local_infile=0`이면 fast load 불가 → `ingest-safe`로 전환(또는 서버 설정/권한 조정)
  - 클라이언트 드라이버에서 `local_infile=1`이 필요(환경에 따라 차단될 수 있음)
- 속도가 기대보다 느릴 때:
  - `chunk_size`가 너무 작으면 `LOAD DATA` 호출/커밋 횟수 증가로 느려질 수 있음
  - 드리프트가 심한데 evolve(ALTER 허용)로 돌리면 `ALTER TABLE`이 병목이 될 수 있음 → `freeze` 고려
  - `parallel_workers`는 주로 **flatten/TSV 생성(CPU+I/O)** 단계에 영향(=DB load가 병목이면 효과 제한적)
    - `json_streaming_load=true` + `LOAD DATA` 경로에서는 워커가 TSV를 생성하고(IPC 최소화) 부모가 `LOAD DATA`를 수행
    - 동일 스키마(컬럼 순서) TSV 조각은 테이블별로 병합해 `LOAD DATA` 호출 수를 줄임
  - `db_load_parallel_tables`는 **테이블 단위로 `LOAD DATA`를 병렬화**해 DB ingest 구간(`db.load_data.exec`)을 줄일 수 있음
    - 테이블별로 별도 `LOCAL INFILE` 커넥션을 사용(쓰레드 기반)
    - WoS-like처럼 테이블 수가 많고(`tables_loaded`가 큼) `db.load_data.exec` 비중이 높을 때 효과가 큼
    - `kisti-db-manager report profile run_report.json`로 `json.flatten` / `db.load_data.*` 비중을 먼저 확인 권장
  - `overlap_batches`는 **배치 N의 DB load와 배치 N+1의 flatten을 오버랩**해 end-to-end wall time을 줄이는 옵션
    - `json.flatten`과 `db.load_data.exec`가 둘 다 큰 경우(둘이 번갈아 병목인 경우) 특히 효과가 큼
    - 오버랩이 켜져 있으면 `json.db.load`/`db.load_data.exec`는 “대기(스톨) 시간” 위주로 기록되며, 실제 DB 로딩 wall-time은 `db.load_data.exec.wall`도 함께 확인 권장

## 중요한 옵션(요약)

### `chunk_size` (JSON)

- 작을수록 `LOAD DATA` 호출이 많아지고(=커밋/파싱 오버헤드 증가) 느려질 수 있음
- 벤치(JSON20, 21테이블/420k rows) 기준:
  - `chunk_size=1000` → `LOAD DATA` 420회로 느려짐
  - `chunk_size>=5000`부터 거의 포화(병목은 DB ingest/commit 쪽)

### 다중 입력 (`file_names` / `file_glob` / `json_file_names`)

- 사전 병합 없이 여러 JSON 조각 파일을 순차 ingest 가능
  - `file_names`: 명시 목록 입력
  - `file_glob`: 패턴 매칭 입력 (예: `**/*.jsonl`)
- ZIP 입력에서 내부 멤버를 여러 개 지정 가능
  - `json_file_names`: ZIP 내부 JSON/JSONL 멤버 목록
- 입력 우선순위:
  - `file_names`/`input_paths` → `file_glob` → `file_name`

### `fast_load_session` (속도↑ / 내구성↓ 가능)

- `ingest-fast*` 모드는 기본적으로 DB 세션 변수 튜닝을 “best-effort”로 시도함
  - 예: `unique_checks=0`, `foreign_key_checks=0`, `innodb_flush_log_at_trx_commit=2`, `sql_log_bin=0`
- 서버 권한/정책에 따라 실패할 수 있으며, 실패해도 작업은 계속 진행됨
- 속도/리스크 트레이드오프이므로 필요 시 `--no-fast-load-session`으로 끌 수 있음
- 참고: MariaDB에서는 일부 변수(예: `innodb_flush_log_at_trx_commit`)가 **GLOBAL-only**일 수 있음
  - 이 경우 `fast_load_session`에서 해당 설정이 경고로 기록될 수 있으며, 실제로 적용하려면 서버/권한 범위에서 `SET GLOBAL ...` 또는 서버 시작 옵션으로 설정해야 함

### `db_load_parallel_tables` (DB ingest 병렬화)

- `LOAD DATA`를 테이블 단위로 병렬 실행해 wall-time을 줄이는 옵션
  - 예: 배치당 12개 테이블을 순차 로딩(12번) → 8개 쓰레드로 동시 로딩(대략 12/8 라운드)
- 권장: `2~8` 범위에서 벤치로 결정(너무 크게 잡으면 DB/디스크가 포화되어 오히려 느려질 수 있음)
- CLI에서 바로 덮어쓰려면: `--db-load-parallel-tables N`
- 참고: `parallel_workers=0`(싱글 flatten)이어도 `schema_mode=evolve`(또는 hybrid warmup)에서는 테이블 병렬 로딩이 적용됨
- 참고: 병렬 로딩에서는 `db.load_data.exec`가 “합계”가 아니라 “wall-time”으로 기록됨(프로파일 share_pct 해석을 위해)

### `overlap_batches` (배치 오버랩)

- 배치 N의 `LOAD DATA`가 진행되는 동안 배치 N+1의 flatten/TSV 생성을 먼저 수행해 파이프라인 wall-time을 줄임
- 조건/주의:
  - `db_load_method=auto/load_data` + `json_streaming_load=true` 경로에서 주로 의미가 큼(=LOAD DATA 기반)
  - 오버랩이 켜진 경우 프로파일의 `json.db.load`/`db.load_data.exec`는 “대기(스톨) 시간” 성격이 강함
    - 실제 DB 로딩 wall-time은 `db.load_data.exec.wall`도 함께 확인 권장
- CLI: `--overlap-batches` / `--no-overlap-batches`

### `persist_parquet_files` (기본 로컬 parquet 아티팩트)

- 기본값: `true`
- 목적:
  - JSON flatten 결과를 배치별 parquet 파일로 먼저 남기고 그 다음 DB 적재
- 저장 위치:
  - `persist_parquet_dir`를 지정하지 않으면 `runs/<table>_<run_id>/parquet`
- 주의:
  - `persist_parquet_files=true`면 streaming `LOAD DATA` 경로 대신 DataFrame/parquet-first 경로를 사용
  - 운영에서는 `default`에 기대기보다 `--mode parse-parquet` 또는 `--mode parse-parquet-safe`를 명시하는 편이 안전
  - 즉, 최고 속도가 목적이면 `ingest-fast*` 모드 또는 `--no-persist-parquet-files --json-streaming-load` 조합이 더 맞음

후속 DB 적재(MVP):

```bash
python scripts/oa_materialize_parquet_to_db.py \
  runs/<parse_parquet_run_dir> \
  --dotenv .env
```

- 이 스크립트는 `parse-parquet*` run이 만든 parquet를 입력으로 받아 DB 적재만 별도로 수행한다.
- 진행 상태는 `runs/<parse_parquet_run_dir>/parquet_materialize/progress.json`에 남는다.
- `--db-name openalex_20260225_raw_yjk`로 원래 parse config를 바꾸지 않고 대상 DB만 덮어쓸 수 있다.
- `--parallel-tables N`으로 서로 다른 parquet table 디렉터리를 병렬 적재할 수 있다.
- `--parallel-files-per-table N`으로 하나의 큰 parquet table 안에서 여러 parquet batch를 동시에 적재할 수 있다.
- 기본 staging은 `--staging-writer duckdb`이며, 가능하면 `/dev/shm`에 staging 파일을 만들고 `LOAD DATA LOCAL INFILE`로 적재한다.
- parquet 스키마가 안정적이면 pandas를 거치지 않고 `parquet -> DuckDB staging -> MariaDB`로 바로 간다. 파일별 스키마 드리프트가 생기면 DataFrame 경로로 자동 fallback 한다.

### `persist_tsv_files` (로컬 백업용 TSV 아티팩트)

- 기본값: `false`
- 목적:
  - streaming `LOAD DATA` 경로에서 생성된 TSV 조각을 로컬 디스크에 남겨 재실행/감사/부분 재적재에 활용
- 저장 위치:
  - `persist_tsv_dir`를 지정하지 않으면 `runs/<table>_<run_id>/tsv`
- 주의:
  - `persist_tsv_files=true`는 streaming 경로(`json_streaming_load=true`)에서만 허용
  - 현재 구현에서는 안전한 파일 보존을 우선해서 `persist_tsv_files=true`면 `overlap_batches`와 `db_load_parallel_tables`를 비활성화함
  - 최고 속도 경로에서는 기본적으로 꺼져 있음. 필요할 때만 `--persist-tsv-files`로 켜는 게 맞음

### `schema_mode=freeze` + `extra_column_name`

- 스키마가 계속 바뀌는 데이터에서 “ALTER 반복” 병목을 피하기 위한 전략
- 테이블에 없는 필드는 지정한 extra 컬럼에 JSON 문자열로 보존
- `parallel_workers>0`(병렬 flatten/TSV)에서도 freeze/hybrid(frozen)에서 unknown 필드를 `__extra__`로 패킹한 뒤 `LOAD DATA`로 적재 가능

### `except_keys` (원본 보존 강화)

- excepted 테이블에는 제외 브랜치 원문과 컨텍스트를 함께 저장
  - 기본 동작: 제외된 dict/list는 컬럼으로 펼치지 않고 JSON 문자열(`value` + `__except_raw_json__`)로 보존 (`excepted_expand_dict=false`)
  - `__except_raw_json__`, `__except_path__`, `__except_raw_type__`
  - 가능하면 소스 추적 컬럼(`__source_path__`, `__source_member__`, `__line_no__`, `__record_index__`)
  - 레거시 호환이 필요하면 `excepted_expand_dict=true`로 dict 키 확장 가능(대용량에서는 컬럼 폭증 주의)

### `auto_except` (랜덤 샘플 기반 자동 제외)

- 목적: `abstract_inverted_index`처럼 서브키 카디널리티가 큰 dict 경로를 자동 탐지해 `except_keys`에 추가
- 동작: JSON run 시작 전에 랜덤 소스 샘플로 dict-path 통계를 수집하고 후보를 자동 제외
- 주요 설정:
  - `auto_except` (기본 `false`)
  - `auto_except_sample_records` (기본 `5000`)
  - `auto_except_sample_max_sources` (기본 `64`)
  - `auto_except_seed` (기본 `42`)
  - `auto_except_unique_key_threshold` (기본 `512`)
  - `auto_except_min_observations` (기본 `20`)
  - `auto_except_novelty_threshold` (기본 `2.0`, `unique_keys/observations`)
- 리포트(`run_report.artifacts.auto_except`)에 탐지 키, 샘플 프로파일, ETA 추정(소스/바이트 기준) 기록

## 추가 팁 (대용량/운영)

- ingest 단계에서는 `index/optimize`를 분리(`--mode ingest-fast` → `--mode finalize`)
- 스키마 드리프트가 “가끔”이면 evolve(기본)도 충분히 효율적일 수 있음
- 드리프트가 “자주/많이”면 freeze가 운영상 안정적(속도/성공률 관점)
