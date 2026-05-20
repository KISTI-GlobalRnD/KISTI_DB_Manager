# 릴리스 체크리스트

문서 또는 운영 절차를 변경했다면 최소한 아래 검증을 실행합니다.
PR과 `main` push는 `.github/workflows/ci.yml`에서 Python 3.10/3.11/3.12 테스트, MkDocs 빌드, Rust 기본/simd-json 검증, Rust 확장 smoke, 패키지 빌드를 자동으로 확인합니다.
같은 workflow를 수동 실행할 때 `db-smoke` 입력을 켜면 임시 MariaDB 서비스에서 Rust DB load smoke도 실행합니다.

소스 체크아웃에서 같은 로컬 게이트를 한 번에 실행하려면 다음 명령을 사용합니다.

```bash
python scripts/release_check.py
```

릴리스 태그 직전에는 `--require-clean`을 추가해 미커밋 변경이 남아 있으면 실패하도록 할 수 있습니다.

## 버전과 변경 기록

릴리스 태그 전에는 다음 항목을 먼저 맞춥니다.

1. `pyproject.toml`과 `KISTI_DB_Manager/__init__.py`의 버전을 같은 값으로 갱신합니다.
2. `CHANGELOG.md`에 날짜가 있는 릴리스 항목을 추가합니다.
3. NameMap JSON, Description Profile, Dataset Profile, CLI 출력, report JSON
   같은 저장 산출물 계약이 바뀌었다면 `docs/reference/migration.md`를
   갱신합니다.
4. 로컬에서 문서와 패키지를 빌드하고, 빌드된 wheel의 `kisti-db-manager version` 출력이 의도한 버전인지 확인합니다.
5. 릴리스 커밋의 CI가 통과한 뒤에만 git tag를 생성합니다.

```bash
git diff --check
mkdocs build --strict
```

코드 변경이 있으면 Python 테스트를 실행합니다.

```bash
python -m pytest -q
```

산출물 계약이 바뀌었다면 관련 테스트를 명시적으로 확인합니다.

```bash
python -m pytest tests/test_naming.py tests/test_namemap.py tests/test_cli_tabular.py tests/test_dataset_profile.py tests/test_profile_artifact_contracts.py tests/test_review_schema_artifact_contracts.py -q
```

확인할 항목:

- 기존 NameMap JSON이 계속 로드되는지
- 새 NameMap JSON의 선택 필드가 하위 호환인지
- Description Profile과 Dataset Profile에 `schema_version`이 포함되는지
- Schema Viewer JSON/SVG/Mermaid 산출물이 고정 contract와 일치하는지
- 문서에 포함된 SVG/HTML 예제가 의도적으로 갱신된 것인지

릴리스 태그 또는 배포 전에는 wheel/sdist 빌드도 확인합니다.

```bash
python -m pip install build
python -m build --sdist --wheel
```

빌드 산출물인 `build/`, `dist/`, `*.egg-info/`는 git에서 제외되며, 배포하지 않을 때는 검사 후 삭제합니다.

Rust backend 변경이 있으면 Rust 검증도 실행합니다.

```bash
cargo fmt --manifest-path crates/kisti_json_rs/Cargo.toml --check
cargo check --manifest-path crates/kisti_json_rs/Cargo.toml
cargo test --manifest-path crates/kisti_json_rs/Cargo.toml --no-default-features
cargo check --manifest-path crates/kisti_json_rs/Cargo.toml --features simd-json
cargo test --manifest-path crates/kisti_json_rs/Cargo.toml --no-default-features --features simd-json
python -m maturin build --manifest-path crates/kisti_json_rs/Cargo.toml --release --out build/rust-extension-wheels
python -m pip install --force-reinstall --no-deps build/rust-extension-wheels/kisti_json_rs-*.whl
python -m pytest tests/test_rust_arrow_extension_smoke.py -q
```

Rust DB load나 DB 의존성이 바뀌었다면 disposable MariaDB 대상에서 smoke도 확인합니다.

```bash
kisti-db-manager smoke rust-db-load --dotenv .env
```

운영 명령 순서가 바뀌면 다음 문서를 같이 갱신합니다.

- 영문 operator guide
- 한국어 runbook
- CLI reference 또는 artifacts reference
- migration notes
- README의 링크가 바뀐 경우 README
