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

Representative screenshots and preview examples remain in `examples/README.md` and `examples/assets/`.
Use those when you need visual validation of review output or raw-vs-flatten preview behavior.
