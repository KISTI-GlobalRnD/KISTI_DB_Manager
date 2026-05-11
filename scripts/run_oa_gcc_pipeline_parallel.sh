#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: $0 RUN_DIR [STATE_DIR] [WORKERS] [BUCKET_TARGET]" >&2
  exit 2
fi

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RUN_DIR_INPUT="$1"
STATE_DIR_INPUT="${2:-}"
WORKERS="${3:-4}"
BUCKET_TARGET_INPUT="${4:-}"
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

cd "$ROOT_DIR"

if ! [[ "$WORKERS" =~ ^[0-9]+$ ]] || [ "$WORKERS" -lt 1 ]; then
  echo "WORKERS must be a positive integer" >&2
  exit 2
fi

if [ -n "$BUCKET_TARGET_INPUT" ] && { ! [[ "$BUCKET_TARGET_INPUT" =~ ^[0-9]+$ ]] || [ "$BUCKET_TARGET_INPUT" -lt 1 ]; }; then
  echo "BUCKET_TARGET must be a positive integer" >&2
  exit 2
fi

mkdir -p "$STATE_DIR"
WORKS_ROOT="$STATE_DIR/parallel_works"
REFS_ROOT="$STATE_DIR/parallel_refs"
mkdir -p "$WORKS_ROOT" "$REFS_ROOT"
LOG_FILE="$STATE_DIR/pipeline_parallel.log"
PID_FILE="$STATE_DIR/pipeline_parallel.pid"

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

stop_existing_works() {
  local pids
  pids="$(pgrep -f "scripts/oa_materialize_gcc_inputs.py .*${RUN_NAME}.* --part works" || true)"
  if [ -n "$pids" ]; then
    log "stopping existing works extractors pids=$(echo "$pids" | tr '\n' ' ')"
    kill $pids || true
  fi

  local watch_pids
  watch_pids="$(pgrep -f "scripts/run_oa_gcc_pipeline.sh .*${RUN_NAME}" || true)"
  if [ -n "$watch_pids" ]; then
    log "stopping existing sequential watcher pids=$(echo "$watch_pids" | tr '\n' ' ')"
    kill $watch_pids || true
  fi

  while true; do
    local active
    active="$(py_db works_active)"
    if [ "$active" -eq 0 ]; then
      break
    fi
    log "waiting for works DB activity/rollback to clear active=$active"
    sleep 30
  done
}

bucket_plan() {
  ./.venv/bin/python - "$RUN_DIR" "$WORKERS" "${BUCKET_TARGET_INPUT:-}" <<'PY'
import importlib.util
import json
import sys
from pathlib import Path
import pymysql

run_dir = Path(sys.argv[1])
workers = int(sys.argv[2])
bucket_target_arg = sys.argv[3]

spec = importlib.util.spec_from_file_location("oa_materialize_gcc_inputs", str(Path("scripts/oa_materialize_gcc_inputs.py")))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

cfg = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
db = dict(cfg["db_config"])
db.setdefault("charset", "utf8mb4")
db.setdefault("autocommit", True)

bucket_target = int(bucket_target_arg) if bucket_target_arg else int(mod.RANGE_BUCKET_MAX_EST_ROWS)

conn = pymysql.connect(**db)
try:
    with conn.cursor() as cur:
        index_name = mod._id_index_name(cur, table="openalex_works_202602")
        if not index_name:
            raise SystemExit("missing source id index for openalex_works_202602")
        buckets = mod._build_range_buckets(
            cur,
            table="openalex_works_202602",
            index_name=index_name,
            max_est_rows=bucket_target,
        )
finally:
    conn.close()

bucket_count = len(buckets)
chunk = (bucket_count + workers - 1) // workers
payload = {
    "bucket_count": bucket_count,
    "bucket_target": bucket_target,
    "workers": workers,
    "ranges": [],
}
for idx in range(workers):
    start = idx * chunk
    end = min(bucket_count, start + chunk)
    if start >= end:
        continue
    payload["ranges"].append({"worker": idx + 1, "start": start, "end": end})
print(json.dumps(payload, ensure_ascii=False))
PY
}

start_shard() {
  local worker="$1"
  local start="$2"
  local end="$3"
  local bucket_target="$4"
  local shard_name
  shard_name="$(printf 'w%02d' "$worker")"
  local shard_dir="$WORKS_ROOT/$shard_name"
  mkdir -p "$shard_dir"
  log "starting works shard=$shard_name range=${start}:${end} bucket_target=$bucket_target"
  nohup ./.venv/bin/python scripts/oa_materialize_gcc_inputs.py \
    "$RUN_DIR" \
    --part works \
    --state-dir "$shard_dir" \
    --bucket-start-index "$start" \
    --bucket-end-index "$end" \
    --range-bucket-max-est-rows "$bucket_target" \
    >> "$WORKS_ROOT/$shard_name.log" 2>&1 &
  echo "$!" > "$WORKS_ROOT/$shard_name.pid"
  log "started works shard=$shard_name pid=$!"
}

shard_done() {
  ./.venv/bin/python - "$1" <<'PY'
import json
import sys
from pathlib import Path

state_path = Path(sys.argv[1]) / "progress.json"
if not state_path.exists():
    raise SystemExit(1)
try:
    state = json.loads(state_path.read_text(encoding="utf-8"))
except Exception:
    raise SystemExit(1)
works = dict(state.get("works") or {})
raise SystemExit(0 if works.get("done") is True else 1)
PY
}

summarize_shards() {
  ./.venv/bin/python - "$WORKS_ROOT" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
summary = {
    "done": 0,
    "total": 0,
    "rows_loaded_meta": 0,
    "rows_loaded_text": 0,
    "workers": [],
}
for shard_dir in sorted(p for p in root.glob("w*") if p.is_dir()):
    state_path = shard_dir / "progress.json"
    state = {}
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            state = {}
    works = dict(state.get("works") or {})
    done = bool(works.get("done"))
    summary["total"] += 1
    summary["done"] += 1 if done else 0
    summary["rows_loaded_meta"] += int(works.get("rows_loaded_meta") or 0)
    summary["rows_loaded_text"] += int(works.get("rows_loaded_text") or 0)
    summary["workers"].append(
        {
            "name": shard_dir.name,
            "done": done,
            "phase": works.get("phase"),
            "status": works.get("status"),
            "meta_bucket_index": int(works.get("meta_bucket_index") or 0),
            "text_bucket_index": int(works.get("text_bucket_index") or 0),
            "bucket_count": int(works.get("bucket_count") or 0),
            "bucket_count_total": int(works.get("bucket_count_total") or 0),
            "bucket_start_index_global": int(works.get("bucket_start_index_global") or 0),
            "bucket_end_index_global": int(works.get("bucket_end_index_global") or 0),
        }
    )
print(json.dumps(summary, ensure_ascii=False))
PY
}

refs_done() {
  ./.venv/bin/python - "$REFS_ROOT" <<'PY'
import json
import sys
from pathlib import Path

state_path = Path(sys.argv[1]) / "progress.json"
if not state_path.exists():
    raise SystemExit(1)
try:
    state = json.loads(state_path.read_text(encoding="utf-8"))
except Exception:
    raise SystemExit(1)
refs = dict(state.get("refs") or {})
raise SystemExit(0 if refs.get("done") is True else 1)
PY
}

start_refs() {
  mkdir -p "$REFS_ROOT"
  log "starting refs extraction"
  nohup ./.venv/bin/python scripts/oa_materialize_gcc_inputs.py \
    "$RUN_DIR" \
    --part refs \
    --state-dir "$REFS_ROOT" \
    >> "$STATE_DIR/refs_extract.log" 2>&1 &
  echo "$!" > "$STATE_DIR/refs.pid"
  log "started refs pid=$!"
}

monitor_works() {
  while true; do
    local summary_json
    summary_json="$(summarize_shards)"
    local done total rows_meta rows_text
    read -r done total rows_meta rows_text < <(
      ./.venv/bin/python - "$summary_json" <<'PY'
import json
import sys
payload = json.loads(sys.argv[1])
print(payload["done"], payload["total"], payload["rows_loaded_meta"], payload["rows_loaded_text"])
PY
    )

    local started=0
    while IFS=$'\t' read -r worker start end bucket_target; do
      [ -n "$worker" ] || continue
      local shard_name
      shard_name="$(printf 'w%02d' "$worker")"
      local pid_file="$WORKS_ROOT/$shard_name.pid"
      local pid=""
      if [ -f "$pid_file" ]; then
        pid="$(cat "$pid_file" 2>/dev/null || true)"
      fi
      if shard_done "$WORKS_ROOT/$shard_name"; then
        continue
      fi
      if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        continue
      fi
      start_shard "$worker" "$start" "$end" "$bucket_target"
      started=1
      sleep 2
    done < <(
      ./.venv/bin/python - "$PLAN_JSON" <<'PY'
import json
import sys
plan = json.loads(sys.argv[1])
for item in plan["ranges"]:
    print(f'{item["worker"]}\t{item["start"]}\t{item["end"]}\t{plan["bucket_target"]}')
PY
    )

    local active
    active="$(py_db works_active)"
    log "works parallel status done=$done/$total rows_meta=$rows_meta rows_text=$rows_text active_queries=$active restarted=$started"
    if [ "$done" -eq "$total" ] && [ "$active" -eq 0 ] && [ "$total" -gt 0 ]; then
      break
    fi
    sleep 60
  done
}

monitor_refs() {
  while true; do
    if refs_done; then
      log "refs complete"
      return 0
    fi
    local pid=""
    if [ -f "$STATE_DIR/refs.pid" ]; then
      pid="$(cat "$STATE_DIR/refs.pid" 2>/dev/null || true)"
    fi
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      log "refs running pid=$pid"
      sleep 60
      continue
    fi
    local active
    active="$(py_db refs_active)"
    if [ "$active" -gt 0 ]; then
      log "refs waiting on DB-side work/rollback active=$active"
      sleep 60
      continue
    fi
    start_refs
    sleep 10
  done
}

log "parallel watcher started run_dir=$RUN_DIR state_dir=$STATE_DIR workers=$WORKERS"
PLAN_JSON="$(bucket_plan)"
log "works bucket plan $PLAN_JSON"
if [ "${SKIP_STOP_EXISTING:-0}" = "1" ]; then
  log "skipping stop_existing_works due to SKIP_STOP_EXISTING=1"
else
  stop_existing_works
fi
monitor_works
log "works parallel complete"
monitor_refs
log "pipeline complete"
