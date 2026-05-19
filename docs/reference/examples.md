# Examples

## Smoke test

Docker:

```bash
cd examples
docker compose up --build --abort-on-container-exit smoke
docker compose down
```

Host:

```bash
bash examples/smoke.sh
```

## Real DB smoke

```bash
cp examples/configs/tabular_config_realdb.template.json examples/configs/tabular_config_realdb.local.json
cp examples/configs/json_config_realdb.template.json examples/configs/json_config_realdb.local.json
bash examples/smoke_real_db.sh \
  examples/configs/tabular_config_realdb.local.json \
  examples/configs/json_config_realdb.local.json
```

## Preview outputs

There are two example surfaces:

- `examples/`: smoke-test fixtures, sample configs, and historical screenshot assets.
- `docs/examples/` and `docs/assets/`: public documentation examples published by MkDocs.

Current public review examples:

- OpenAlex raw-vs-flatten preview HTML: [`../examples/openalex_preview/preview.html`](../examples/openalex_preview/preview.html)
- OpenAlex raw-vs-flatten preview JSON: [`../examples/openalex_preview/preview.json`](../examples/openalex_preview/preview.json)
- OpenAlex predicted schema SVG: [`../assets/openalex_schema_example.svg`](../assets/openalex_schema_example.svg)

For the maintainer refresh command, see [Review and Visualization](../manual/review-visualization.md#regenerating-the-checked-in-openalex-schema-svg).

Generated run-specific review artifacts should usually stay outside `docs/`.
Copy or link them into `docs/examples/` only when the artifact is intentionally part of the public documentation.
