# Examples (smoke + previews)

## Run smoke test

Docker (recommended):

```bash
cd examples
docker compose up --build --abort-on-container-exit smoke
docker compose down
```

Host (requires deps + docker):

```bash
bash examples/smoke.sh
```

## Output previews

These are **representative snapshots**. Regenerate locally with the smoke test and check `examples/out/`.

### JSON 20-lists schema diagram

![JSON 20-lists schema diagram](assets/json_20lists_schema.svg)

### Review HTML (rendered)

![Review preview](assets/json_20lists_review.png)

### Review Diff HTML (rendered)

![Review diff preview](assets/json_20lists_review_diff.png)

### Raw vs Flatten preview (HTML)

This helps validate whether flattening matches the raw record structure (missing/extra keys).

```bash
kisti-db-manager review preview --config examples/configs/json_preview_20lists.json --out preview_out
```

![Raw vs Flatten preview](assets/json_20lists_preview.png)

## Data_Sample schema (WoS)

We also ship a real-ish multi-table sample under `Data_Sample/` (repo root).

Generate/update the schema image:

```bash
python3 examples/generate_data_sample_schema.py
```

Result:

![WoS sample schema](../Image/Schema_WoS_Sample.png)
