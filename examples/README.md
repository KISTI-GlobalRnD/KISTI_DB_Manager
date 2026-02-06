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

## Real DB smoke-run (integration)

Use this when you want to validate against an existing MariaDB/MySQL instance (non-docker DB).

1. Create local configs from templates:

```bash
cp examples/configs/tabular_config_realdb.template.json examples/configs/tabular_config_realdb.local.json
cp examples/configs/json_config_realdb.template.json examples/configs/json_config_realdb.local.json
```

2. Edit `db_config` and `data_config` paths in the `.local.json` files.

3. Run integration smoke:

```bash
bash examples/smoke_real_db.sh \
  examples/configs/tabular_config_realdb.local.json \
  examples/configs/json_config_realdb.local.json
```

Multi-file JSON input template:
- `examples/configs/json_config_multifile_realdb.template.json`

## Output previews

These are **representative snapshots**. Regenerate locally with the smoke test and check `examples/out/`.

### JSON 20-lists schema diagram

![JSON 20-lists schema diagram](assets/json_20lists_schema.svg)

### Review HTML (rendered)

![Review preview](assets/json_20lists_review.png)

### Review Diff HTML (rendered)

![Review diff preview](assets/json_20lists_review_diff.png)

### Raw vs Flatten preview (HTML)

This helps validate whether flattening matches the raw record structure (missing/extra keys), and also provides a **union view**
to spot low-coverage / type-drift branches across sampled records.

```bash
kisti-db-manager review preview --config examples/configs/json_preview_20lists.json --out preview_out
```

![Raw vs Flatten preview](assets/json_20lists_preview.png)

Union structure (scrolled to `#union` in the same page):

![Union structure preview](assets/json_20lists_preview_union.png)

Expanded view (new tab):

```bash
# macOS:
open "preview_out/preview.html?view=expanded&record=0"
# Linux:
xdg-open "preview_out/preview.html?view=expanded&record=0"
```

![Raw vs Flatten expanded view](assets/json_20lists_preview_expanded.png)

Diagram view (new tab, overview):

```bash
# macOS:
open "preview_out/preview.html?view=diagram&record=0"
# Linux:
xdg-open "preview_out/preview.html?view=diagram&record=0"
```

![Raw vs Flatten diagram view](assets/json_20lists_preview_diagram.png)

Tip: in diagram view, **double-click** a node to open the expanded view focused on that path/subtable.
Use **Shift + double-click** to open in the same tab.

### Raw vs Flatten preview (schema drift / complex)

This is a **synthetic WoS-like JSONL** sample intentionally containing:
- low coverage branches (only some records contain a path)
- type drift (same path is dict vs list vs value, etc.)

```bash
kisti-db-manager review preview --config examples/configs/json_preview_wos_like.json --out preview_out_wos_like --max-records 6
```

![Raw vs Flatten preview (wos-like)](assets/json_wos_like_preview.png)

![Union structure preview (wos-like)](assets/json_wos_like_preview_union.png)

![Raw vs Flatten expanded view (wos-like)](assets/json_wos_like_preview_expanded.png)

![Raw vs Flatten diagram view (wos-like)](assets/json_wos_like_preview_diagram.png)

## Data_Sample schema (WoS)

We also ship a real-ish multi-table sample under `Data_Sample/` (repo root).

Generate/update the schema image:

```bash
python3 examples/generate_data_sample_schema.py
```

Result:

![WoS sample schema](../Image/Schema_WoS_Sample.png)
