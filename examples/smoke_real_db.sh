#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXAMPLES="$ROOT/examples"
OUT="$EXAMPLES/out/realdb_smoke"

TABULAR_CONFIG="${1:-$EXAMPLES/configs/tabular_config_realdb.local.json}"
JSON_CONFIG="${2:-$EXAMPLES/configs/json_config_realdb.local.json}"

mkdir -p "$OUT"

if command -v kisti-db-manager >/dev/null 2>&1; then
  KISTI_CMD=(kisti-db-manager)
else
  KISTI_CMD=(python3 -m KISTI_DB_Manager.cli)
fi

if [[ ! -f "$TABULAR_CONFIG" ]]; then
  echo "tabular config not found: $TABULAR_CONFIG"
  echo "copy template and fill DB/data values:"
  echo "  cp $EXAMPLES/configs/tabular_config_realdb.template.json $EXAMPLES/configs/tabular_config_realdb.local.json"
  exit 1
fi

if [[ ! -f "$JSON_CONFIG" ]]; then
  echo "json config not found: $JSON_CONFIG"
  echo "copy template and fill DB/data values:"
  echo "  cp $EXAMPLES/configs/json_config_realdb.template.json $EXAMPLES/configs/json_config_realdb.local.json"
  exit 1
fi

echo "[1/4] tabular run"
"${KISTI_CMD[@]}" tabular run \
  --config "$TABULAR_CONFIG" \
  --report "$OUT/tabular_report.json" \
  --quarantine "$OUT/tabular_quarantine.jsonl"

echo "[2/4] tabular report check"
python3 - "$OUT/tabular_report.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as f:
    report = json.load(f)

errors = int(report.get("stats", {}).get("issues_error", 0) or 0)
if errors:
    raise SystemExit(f"tabular smoke failed: issues_error={errors} (see {path})")
print(f"tabular ok: {path}")
PY

echo "[3/4] json run (ingest-fast)"
"${KISTI_CMD[@]}" json run \
  --config "$JSON_CONFIG" \
  --mode ingest-fast \
  --report "$OUT/json_report_ingest.json" \
  --quarantine "$OUT/json_quarantine_ingest.jsonl"

echo "[4/4] json finalize"
"${KISTI_CMD[@]}" json run \
  --config "$JSON_CONFIG" \
  --mode finalize \
  --report "$OUT/json_report_finalize.json" \
  --quarantine "$OUT/json_quarantine_finalize.jsonl"

python3 - "$OUT/json_report_ingest.json" "$OUT/json_report_finalize.json" <<'PY'
import json
import sys

for path in sys.argv[1:]:
    with open(path, encoding="utf-8") as f:
        report = json.load(f)
    errors = int(report.get("stats", {}).get("issues_error", 0) or 0)
    if errors:
        raise SystemExit(f"json smoke failed: issues_error={errors} (see {path})")
    print(f"json ok: {path}")
PY

echo "done:"
echo "  $OUT/tabular_report.json"
echo "  $OUT/json_report_ingest.json"
echo "  $OUT/json_report_finalize.json"
