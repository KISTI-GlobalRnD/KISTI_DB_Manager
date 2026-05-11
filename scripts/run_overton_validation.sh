#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  scripts/run_overton_validation.sh [--schema <name>] [--mode quick|deep]

Defaults:
  --schema overton_202601_raw
  --mode quick
EOF
}

schema="overton_202601_raw"
mode="quick"

while [ $# -gt 0 ]; do
  case "$1" in
    --schema) schema="$2"; shift 2 ;;
    --mode) mode="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "ERROR: unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

case "$schema" in
  [A-Za-z0-9_]* ) ;;
  * ) echo "ERROR: unsafe schema: $schema" >&2; exit 2 ;;
esac

case "$mode" in
  quick|deep) ;;
  *) echo "ERROR: mode must be quick or deep" >&2; exit 2 ;;
esac

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sql_file="$repo_root/sql/overton_validation_${mode}.sql"
docker_env="$repo_root/../1.0.0.GlobRnD/MariaDB_Migration/docker/.env"

if [ ! -f "$sql_file" ]; then
  echo "ERROR: missing SQL file: $sql_file" >&2
  exit 1
fi

if [ ! -f "$docker_env" ]; then
  echo "ERROR: missing docker env: $docker_env" >&2
  exit 1
fi

set -a
source "$docker_env"
set +a

if [ -z "${MARIADB_ROOT_PASSWORD:-}" ]; then
  echo "ERROR: MARIADB_ROOT_PASSWORD is not set" >&2
  exit 1
fi

sed "s/__OVERTON_SCHEMA__/$schema/g" "$sql_file" \
  | MYSQL_PWD="$MARIADB_ROOT_PASSWORD" docker exec -i -e MYSQL_PWD "${MARIADB_CONTAINER_NAME:-mariadb-kisti}" \
      mariadb -uroot --table --default-character-set=utf8mb4
