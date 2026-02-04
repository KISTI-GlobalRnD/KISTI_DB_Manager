#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EXAMPLES="$ROOT/KISTI_DB_Manager/examples"
OUT="$EXAMPLES/out"

mkdir -p "$OUT"

python3 -c "import pandas, numpy, sqlalchemy, pymysql" >/dev/null 2>&1 || {
  echo "Missing deps. Install first (example):"
  echo "  python3 -m venv .venv && source .venv/bin/activate"
  echo "  pip install -e \".[db]\""
  exit 1
}

if command -v kisti-db-manager >/dev/null 2>&1; then
  KISTI_CMD=(kisti-db-manager)
else
  KISTI_CMD=(python3 -m KISTI_DB_Manager.cli)
fi

cd "$EXAMPLES"
docker compose up -d mariadb

echo "Waiting for MariaDB..."
for _ in $(seq 1 60); do
  if docker compose exec -T mariadb mariadb-admin ping -uroot -prootpass --silent >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

cd "$ROOT"
"${KISTI_CMD[@]}" tabular run \
  --config "$EXAMPLES/configs/tabular_config.json" \
  --report "$OUT/tabular_report.json" \
  --quarantine "$OUT/tabular_quarantine.jsonl"

python3 - "$OUT/tabular_report.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as f:
    report = json.load(f)

errors = int(report.get("stats", {}).get("issues_error", 0) or 0)
if errors:
    raise SystemExit(f"tabular smoke failed: issues_error={errors} (see {path})")
PY

"${KISTI_CMD[@]}" json run \
  --config "$EXAMPLES/configs/json_config.json" \
  --report "$OUT/json_report.json" \
  --quarantine "$OUT/json_quarantine.jsonl"

python3 - "$OUT/json_report.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as f:
    report = json.load(f)

errors = int(report.get("stats", {}).get("issues_error", 0) or 0)
if errors:
    raise SystemExit(f"json smoke failed: issues_error={errors} (see {path})")
PY

cd "$EXAMPLES"
docker compose down

echo "Done. Outputs:"
echo "  $OUT/tabular_report.json"
echo "  $OUT/tabular_quarantine.jsonl"
echo "  $OUT/json_report.json"
echo "  $OUT/json_quarantine.jsonl"
