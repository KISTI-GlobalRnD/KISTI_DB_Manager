#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: $0 RUN_DIR [STATE_DIR]" >&2
  exit 2
fi

RUN_DIR="$1"
STATE_DIR="${2:-$RUN_DIR/gcc_materialize}"

cd "$(dirname "$0")/.."
mkdir -p "$STATE_DIR"

while true; do
  if ./.venv/bin/python - "$RUN_DIR" <<'PY'
import json
import sys
from pathlib import Path
import pymysql

run_dir = Path(sys.argv[1]).expanduser().resolve()
cfg = json.loads((run_dir / "config.json").read_text())
db = cfg["db_config"]
conn = pymysql.connect(
    host=db["host"],
    user=db["user"],
    password=db["password"],
    database=db.get("database") or None,
    port=int(db.get("port", 3306)),
    charset="utf8mb4",
    autocommit=True,
)
try:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.statistics "
            "WHERE table_schema=DATABASE() AND table_name=%s AND column_name=%s LIMIT 1",
            ("openalex_works_202602", "id"),
        )
        sys.exit(0 if cur.fetchone() else 1)
finally:
    conn.close()
PY
  then
    break
  fi
  sleep 30
done

./.venv/bin/python scripts/oa_materialize_gcc_inputs.py \
  "$RUN_DIR" \
  --part works \
  --state-dir "$STATE_DIR" \
  --batch-size 50000
