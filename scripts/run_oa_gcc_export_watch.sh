#!/usr/bin/env bash
set -uo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: $0 RUN_DIR [STATE_DIR] [EXPORT_ROOT]" >&2
  exit 2
fi

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RUN_DIR_INPUT="$1"
STATE_DIR_INPUT="${2:-}"
EXPORT_ROOT_INPUT="${3:-}"

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

RUN_NAME="$(basename "$RUN_DIR")"
DEFAULT_EXPORT_ROOT="/home/kimyoungjin06/Desktop/Disk/Raid/dumps/KISTI_DB_Manager/exports/$RUN_NAME/gcc_parquet"
if [ -n "$EXPORT_ROOT_INPUT" ]; then
  EXPORT_ROOT="$(cd "$ROOT_DIR" && python3 - <<'PY' "$EXPORT_ROOT_INPUT"
from pathlib import Path
import sys
print(Path(sys.argv[1]).expanduser().resolve())
PY
)"
else
  EXPORT_ROOT="$DEFAULT_EXPORT_ROOT"
fi

cd "$ROOT_DIR"
mkdir -p "$STATE_DIR" "$EXPORT_ROOT"

LOG_FILE="$STATE_DIR/export_watch.log"
PID_FILE="$STATE_DIR/export_watch.pid"
EXPORT_STATE_DIR="$EXPORT_ROOT/_state"
mkdir -p "$EXPORT_STATE_DIR"
echo "$$" > "$PID_FILE"

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*" >> "$LOG_FILE"
}

py_db() {
  ./.venv/bin/python - "$RUN_DIR" "$@" <<'PY'
import json
import sys
from pathlib import Path
import pymysql

run_dir = Path(sys.argv[1])
mode = sys.argv[2]
cfg = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
db = dict(cfg["db_config"])
db.setdefault("charset", "utf8mb4")
db.setdefault("autocommit", True)
conn = pymysql.connect(**db)
try:
    with conn.cursor() as cur:
        if mode == "works_active":
            cur.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.processlist
                WHERE db = DATABASE()
                  AND (
                    info LIKE 'INSERT IGNORE INTO `openalex_works_meta`%%'
                    OR info LIKE 'INSERT IGNORE INTO `openalex_works_text`%%'
                  )
                """
            )
            print(int(cur.fetchone()[0] or 0))
        elif mode == "refs_active":
            cur.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.processlist
                WHERE db = DATABASE()
                  AND info LIKE 'INSERT IGNORE INTO `openalex_refs`%%'
                """
            )
            print(int(cur.fetchone()[0] or 0))
        else:
            raise SystemExit(2)
finally:
    conn.close()
PY
}

export_done() {
  ./.venv/bin/python - "$EXPORT_STATE_DIR/progress.json" "$1" <<'PY'
import json
import sys
from pathlib import Path

state_path = Path(sys.argv[1])
part = sys.argv[2]
if not state_path.exists():
    raise SystemExit(1)
try:
    state = json.loads(state_path.read_text(encoding="utf-8"))
except Exception:
    raise SystemExit(1)
part_state = dict(state.get(part) or {})
raise SystemExit(0 if part_state.get("done") is True else 1)
PY
}

works_meta_ready() {
  ./.venv/bin/python - "$STATE_DIR/parallel_works" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
shards = sorted(p for p in root.glob("w*") if p.is_dir())
if not shards:
    raise SystemExit(1)
for shard in shards:
    state_path = shard / "progress.json"
    if not state_path.exists():
        raise SystemExit(1)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    works = dict(state.get("works") or {})
    if not bool(works.get("meta_done")):
        raise SystemExit(1)
raise SystemExit(0)
PY
}

works_text_ready() {
  ./.venv/bin/python - "$STATE_DIR/parallel_works" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
shards = sorted(p for p in root.glob("w*") if p.is_dir())
if not shards:
    raise SystemExit(1)
for shard in shards:
    state_path = shard / "progress.json"
    if not state_path.exists():
        raise SystemExit(1)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    works = dict(state.get("works") or {})
    if not bool(works.get("done")):
        raise SystemExit(1)
raise SystemExit(0)
PY
}

refs_ready() {
  ./.venv/bin/python - "$STATE_DIR/parallel_refs/progress.json" <<'PY'
import json
import sys
from pathlib import Path

state_path = Path(sys.argv[1])
if not state_path.exists():
    raise SystemExit(1)
state = json.loads(state_path.read_text(encoding="utf-8"))
refs = dict(state.get("refs") or {})
raise SystemExit(0 if refs.get("done") is True else 1)
PY
}

part_pid() {
  local part="$1"
  local pid_file="$EXPORT_STATE_DIR/${part}.pid"
  if [ -f "$pid_file" ]; then
    cat "$pid_file" 2>/dev/null || true
  fi
}

start_export() {
  local part="$1"
  log "starting parquet export part=$part export_root=$EXPORT_ROOT"
  if command -v ionice >/dev/null 2>&1; then
    nohup ionice -c2 -n7 nice -n 10 ./.venv/bin/python scripts/oa_export_gcc_tables_parquet.py \
      "$RUN_DIR" \
      --part "$part" \
      --export-root "$EXPORT_ROOT" \
      --state-dir "$EXPORT_STATE_DIR" \
      >> "$EXPORT_STATE_DIR/${part}.log" 2>&1 &
  else
    nohup nice -n 10 ./.venv/bin/python scripts/oa_export_gcc_tables_parquet.py \
      "$RUN_DIR" \
      --part "$part" \
      --export-root "$EXPORT_ROOT" \
      --state-dir "$EXPORT_STATE_DIR" \
      >> "$EXPORT_STATE_DIR/${part}.log" 2>&1 &
  fi
  echo "$!" > "$EXPORT_STATE_DIR/${part}.pid"
  log "started parquet export part=$part pid=$!"
}

all_done=0
log "export watcher started run_dir=$RUN_DIR export_root=$EXPORT_ROOT"
while [ "$all_done" -eq 0 ]; do
  meta_status="waiting"
  text_status="waiting"
  refs_status="waiting"

  if export_done meta; then
    meta_status="done"
  elif works_meta_ready; then
    meta_status="ready"
    pid="$(part_pid meta)"
    if [ -z "${pid:-}" ] || ! kill -0 "$pid" 2>/dev/null; then
      start_export meta
      meta_status="started"
    else
      meta_status="running"
    fi
  fi

  works_active="$(py_db works_active)"
  if export_done text; then
    text_status="done"
  elif works_text_ready && [ "$works_active" -eq 0 ]; then
    text_status="ready"
    pid="$(part_pid text)"
    if [ -z "${pid:-}" ] || ! kill -0 "$pid" 2>/dev/null; then
      start_export text
      text_status="started"
    else
      text_status="running"
    fi
  elif [ "$works_active" -gt 0 ]; then
    text_status="source_running"
  fi

  refs_active="$(py_db refs_active)"
  if export_done refs; then
    refs_status="done"
  elif refs_ready && [ "$refs_active" -eq 0 ]; then
    refs_status="ready"
    pid="$(part_pid refs)"
    if [ -z "${pid:-}" ] || ! kill -0 "$pid" 2>/dev/null; then
      start_export refs
      refs_status="started"
    else
      refs_status="running"
    fi
  elif [ "$refs_active" -gt 0 ]; then
    refs_status="source_running"
  fi

  log "status meta=$meta_status text=$text_status refs=$refs_status works_active=$works_active refs_active=$refs_active"

  if export_done meta && export_done text && export_done refs; then
    all_done=1
    break
  fi
  sleep 60
done

log "export watcher complete"
