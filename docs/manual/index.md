# Manual Overview

이 섹션은 운영 매뉴얼입니다.
핵심 목표는 "어떤 데이터를 어떤 모드로 어떤 순서로 돌릴지"를 빠르게 판단하게 하는 것입니다.

## Recommended reading order

1. [Chapter 1. Package Overview](package-overview.md)
2. [Chapter 2. Getting Started](getting-started.md)
3. [Chapter 3. JSON Modes](json-modes.md)
4. [Chapter 4. OpenAlex Example Workflow](openalex-workflow.md)
5. [Chapter 5. Review and Visualization](review-visualization.md)
6. [Chapter 6. Restart & Recovery](restart-recovery.md)

## Audience

- 신규 운영자: 설치, mode 선택, 기본 실행 흐름 파악
- 대용량 운영 담당자: restart/resume, parquet/materialize 분리 이해
- 분석 파이프라인 담당자: OpenAlex 예시와 review artifact 흐름 확인

## Decision map

- DB 완주 속도가 우선이면 `ingest-fast*`
- 로컬 artifact와 재사용성이 우선이면 `parse-parquet*`
- OpenAlex는 기본적으로 `parse-parquet-safe -> materialize`
- schema inspection이 목적이면 `review plan / pack / preview / schema-viewer`
