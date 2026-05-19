# 릴리스 체크리스트

문서 또는 운영 절차를 변경했다면 최소한 아래 검증을 실행합니다.
PR과 `main` push는 `.github/workflows/ci.yml`에서 Python 3.10/3.11/3.12 테스트, MkDocs 빌드, Rust 검증, 패키지 빌드를 자동으로 확인합니다.

```bash
git diff --check
mkdocs build --strict
```

코드 변경이 있으면 Python 테스트를 실행합니다.

```bash
python -m unittest discover -s tests -q
```

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
cargo test --manifest-path crates/kisti_json_rs/Cargo.toml
```

운영 명령 순서가 바뀌면 다음 문서를 같이 갱신합니다.

- 영문 operator guide
- 한국어 runbook
- CLI reference 또는 artifacts reference
- README의 링크가 바뀐 경우 README
