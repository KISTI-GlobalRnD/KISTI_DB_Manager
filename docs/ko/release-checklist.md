# 릴리스 체크리스트

문서 또는 운영 절차를 변경했다면 최소한 아래 검증을 실행합니다.

```bash
git diff --check
uv run mkdocs build --strict
```

코드 변경이 있으면 Python 테스트를 실행합니다.

```bash
uv run python -m unittest discover -s tests -q
```

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
