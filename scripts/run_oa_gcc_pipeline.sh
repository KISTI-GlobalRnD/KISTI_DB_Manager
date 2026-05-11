#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: $0 RUN_DIR [STATE_DIR]" >&2
  exit 2
fi

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RUN_DIR_INPUT="$1"
STATE_DIR_INPUT="${2:-}"
RUN_NAME="$(basename "$RUN_DIR_INPUT")"

RUN_DIR="$(cd "$ROOT_DIR" && python3 - <<'PY' "$RUN_DIR_INPUT"
from pathlib import Path
import sys
print(Path(sys.argv[1]).expanduser().resolve())
PY
)"

if [ -n "$STATE_DIR_INPUT" ]; then
  STATE_DIR="$(cd "$ROOT_DIR" && python3 - <<'PY' "$STATE_DIR_INPUT"
from pathlib import Path
import sys
print(Path(sys.argv[1]).expanduser().resolve())
PY
)"
else
  STATE_DIR="$RUN_DIR/gcc_materialize"
fi

mkdir -p "$STATE_DIR"
LOG_FILE="$STATE_DIR/pipeline_watch.log"
PID_FILE="$STATE_DIR/pipeline_watch.pid"

cd "$ROOT_DIR"
echo "$$" > "$PID_FILE"

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*" >> "$LOG_FILE"
}

part_done() {
  local part="$1"
  ./.venv/bin/python - "$RUN_DIR" "$STATE_DIR" "$part" <<'PY'
import json
import sys
from pathlib import Path
import pymysql

run_dir = Path(sys.argv[1])
state_dir = Path(sys.argv[2])
part = sys.argv[3]

cfg = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
db = dict(cfg["db_config"])
db.setdefault("charset", "utf8mb4")
db.setdefault("autocommit", True)

state_path = state_dir / "progress.json"
state = {}
if state_path.exists():
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        state = {}

part_state = dict(state.get(part) or {})
if part_state.get("done") is True:
    raise SystemExit(0)

conn = pymysql.connect(**db)
try:
    with conn.cursor() as cur:
        if part == "works":
            cur.execute(
                """
                SELECT 1
                FROM information_schema.processlist
                WHERE db = DATABASE()
                  AND (
                    info LIKE 'INSERT IGNORE INTO `openalex_works_meta`%%'
                    OR info LIKE 'INSERT IGNORE INTO `openalex_works_text`%%'
                  )
                LIMIT 1
                """
            )
            if cur.fetchone():
                raise SystemExit(1)
            raise SystemExit(1)

        if part == "refs":
            cur.execute(
                """
                SELECT 1
                FROM information_schema.processlist
                WHERE db = DATABASE()
                  AND info LIKE 'INSERT IGNORE INTO `openalex_refs`%%'
                LIMIT 1
                """
            )
            if cur.fetchone():
                raise SystemExit(1)
            raise SystemExit(1)

        raise SystemExit(2)
finally:
    conn.close()
PY
}

part_db_active() {
  local part="$1"
  ./.venv/bin/python - "$RUN_DIR" "$part" <<'PY'
import json
import sys
from pathlib import Path
import pymysql

run_dir = Path(sys.argv[1])
part = sys.argv[2]

cfg = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
db = dict(cfg["db_config"])
db.setdefault("charset", "utf8mb4")
db.setdefault("autocommit", True)

conn = pymysql.connect(**db)
try:
    with conn.cursor() as cur:
        if part == "works":
            cur.execute(
                """
                SELECT 1
                FROM information_schema.processlist
                WHERE db = DATABASE()
                  AND (
                    info LIKE 'INSERT IGNORE INTO `openalex_works_meta`%%'
                    OR info LIKE 'INSERT IGNORE INTO `openalex_works_text`%%'
                  )
                LIMIT 1
                """
            )
            raise SystemExit(0 if cur.fetchone() else 1)

        if part == "refs":
            cur.execute(
                """
                SELECT 1
                FROM information_schema.processlist
                WHERE db = DATABASE()
                  AND info LIKE 'INSERT IGNORE INTO `openalex_refs`%%'
                LIMIT 1
                """
            )
            raise SystemExit(0 if cur.fetchone() else 1)

        raise SystemExit(2)
finally:
    conn.close()
PY
}

part_pid() {
  local part="$1"
  pgrep -fn -f "scripts/oa_materialize_gcc_inputs.py .*${RUN_NAME}.* --part ${part}" || true
}

start_part() {
  local part="$1"
  local log_name
  if [ "$part" = "works" ]; then
    log_name="works_extract.log"
  else
    log_name="refs_extract.log"
  fi
  log "starting $part extraction"
  nohup ./.venv/bin/python scripts/oa_materialize_gcc_inputs.py \
    "$RUN_DIR" \
    --part "$part" \
    --state-dir "$STATE_DIR" \
    >> "$STATE_DIR/$log_name" 2>&1 &
  echo "$!" > "$STATE_DIR/$part.pid"
  log "started $part pid=$!"
}

wait_part() {
  local part="$1"
  while true; do
    if part_done "$part"; then
      log "$part already complete"
      return 0
    fi

    local pid
    pid="$(part_pid "$part")"
    if [ -n "$pid" ]; then
      log "$part running pid=$pid"
      sleep 60
      continue
    fi

    if part_db_active "$part"; then
      log "$part waiting on DB-side work/rollback"
      sleep 60
      continue
    fi

    start_part "$part"
    sleep 10
  done
}

log "watcher started run_dir=$RUN_DIR state_dir=$STATE_DIR"
wait_part works
wait_part refs
log "pipeline complete"
