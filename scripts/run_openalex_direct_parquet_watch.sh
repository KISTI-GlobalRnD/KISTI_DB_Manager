#!/usr/bin/env bash
set -uo pipefail

if [ "$#" -lt 2 ]; then
  echo "usage: $0 TEXT_OUT_DIR REFS_OUT_DIR" >&2
  exit 2
fi

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TEXT_OUT="$(python3 - <<'PY' "$1"
from pathlib import Path
import sys
print(Path(sys.argv[1]).expanduser().resolve())
PY
)"
REFS_OUT="$(python3 - <<'PY' "$2"
from pathlib import Path
import sys
print(Path(sys.argv[1]).expanduser().resolve())
PY
)"

LOG_DIR="$TEXT_OUT/../_parquet_watch_logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/$(basename "$TEXT_OUT")__$(basename "$REFS_OUT").log"
PID_FILE="$LOG_DIR/$(basename "$TEXT_OUT")__$(basename "$REFS_OUT").pid"
echo "$$" > "$PID_FILE"

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$*" >> "$LOG_FILE"
}

pid_alive() {
  local pid_file="$1"
  if [ ! -f "$pid_file" ]; then
    return 1
  fi
  local pid
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  [ -n "$pid" ] || return 1
  kill -0 "$pid" 2>/dev/null
}

start_convert() {
  local input_tsv="$1"
  local out_dir="$2"
  local tag="$3"
  local chunksize="$4"
  log "starting parquet convert tag=$tag input=$input_tsv out=$out_dir"
  if command -v ionice >/dev/null 2>&1; then
    nohup ionice -c2 -n7 nice -n 10 "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/tsv_to_parquet_chunks.py" \
      --input-tsv "$input_tsv" \
      --output-dir "$out_dir" \
      --chunksize "$chunksize" \
      >> "$out_dir.convert.log" 2>&1 &
  else
    nohup nice -n 10 "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/tsv_to_parquet_chunks.py" \
      --input-tsv "$input_tsv" \
      --output-dir "$out_dir" \
      --chunksize "$chunksize" \
      >> "$out_dir.convert.log" 2>&1 &
  fi
  echo "$!" > "$out_dir.convert.pid"
}

reset_failed_convert() {
  local out_dir="$1"
  python3 - <<'PY' "$out_dir"
from pathlib import Path
import shutil
import sys

root = Path(sys.argv[1])
for child in root.glob("part-*.parquet"):
    child.unlink(missing_ok=True)
for name in ("progress.json",):
    (root / name).unlink(missing_ok=True)
PY
  rm -f "$out_dir.convert.pid"
}

log "watch started text_out=$TEXT_OUT refs_out=$REFS_OUT"
while true; do
  text_done=0
  refs_done=0
  text_source_done=0
  refs_source_done=0

  if [ -f "$TEXT_OUT/openalex_works_text.tsv" ] && [ -f "$TEXT_OUT/openalex_works_text_extract_meta.json" ] && ! pid_alive "$TEXT_OUT/pid"; then
    text_source_done=1
    if [ -f "$TEXT_OUT/parquet/progress.json" ] && ! grep -q '"done": true' "$TEXT_OUT/parquet/progress.json" && ! pid_alive "$TEXT_OUT/parquet.convert.pid"; then
      log "resetting failed parquet convert tag=text"
      reset_failed_convert "$TEXT_OUT/parquet"
    fi
    if [ ! -f "$TEXT_OUT/parquet/progress.json" ]; then
      mkdir -p "$TEXT_OUT/parquet"
      start_convert "$TEXT_OUT/openalex_works_text.tsv" "$TEXT_OUT/parquet" "text" "100000"
    fi
    if [ -f "$TEXT_OUT/parquet/progress.json" ] && grep -q '"done": true' "$TEXT_OUT/parquet/progress.json"; then
      text_done=1
    fi
  fi

  if [ -f "$REFS_OUT/openalex_refs.tsv" ] && [ -f "$REFS_OUT/openalex_refs_extract_meta.json" ] && ! pid_alive "$REFS_OUT/pid"; then
    refs_source_done=1
    if [ -f "$REFS_OUT/parquet/progress.json" ] && ! grep -q '"done": true' "$REFS_OUT/parquet/progress.json" && ! pid_alive "$REFS_OUT/parquet.convert.pid"; then
      log "resetting failed parquet convert tag=refs"
      reset_failed_convert "$REFS_OUT/parquet"
    fi
    if [ ! -f "$REFS_OUT/parquet/progress.json" ]; then
      mkdir -p "$REFS_OUT/parquet"
      start_convert "$REFS_OUT/openalex_refs.tsv" "$REFS_OUT/parquet" "refs" "1000000"
    fi
    if [ -f "$REFS_OUT/parquet/progress.json" ] && grep -q '"done": true' "$REFS_OUT/parquet/progress.json"; then
      refs_done=1
    fi
  fi

  log "status text_source_done=$text_source_done refs_source_done=$refs_source_done text_done=$text_done refs_done=$refs_done text_pid_alive=$(pid_alive "$TEXT_OUT/pid" && echo 1 || echo 0) refs_pid_alive=$(pid_alive "$REFS_OUT/pid" && echo 1 || echo 0)"
  if [ "$text_done" -eq 1 ] && [ "$refs_done" -eq 1 ]; then
    break
  fi
  sleep 60
done

log "watch complete"
