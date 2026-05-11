#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class DumpPlan:
    dump_dir: str
    source_schema: str
    target_schema: str
    schema_file: str
    index_file: str | None
    table_count: int
    total_bytes: int
    tables: list[str]
    data_files: list[str]
    created_at_utc: str


def load_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip("'").strip('"')
    return env


def discover_dump(dump_dir: Path, source_schema: str, selected_tables: set[str] | None) -> DumpPlan:
    schema_file = dump_dir / "schemas_noindex" / f"schema__{source_schema}.sql.zst"
    if not schema_file.exists():
        raise FileNotFoundError(f"Missing schema file: {schema_file}")
    index_file = dump_dir / "indexes" / f"indexes__{source_schema}.sql"
    if not index_file.exists():
        index_file = None

    prefix = f"data__{source_schema}__"
    data_files = sorted((dump_dir / "tables").glob(f"{prefix}*.sql.zst"))
    if selected_tables:
        filtered: list[Path] = []
        for path in data_files:
            table_name = path.name[len(prefix) : -len(".sql.zst")]
            if table_name in selected_tables:
                filtered.append(path)
        data_files = filtered
    if not data_files:
        raise FileNotFoundError(f"No data files found for schema={source_schema}")

    tables = [path.name[len(prefix) : -len(".sql.zst")] for path in data_files]
    total_bytes = sum(path.stat().st_size for path in data_files)
    return DumpPlan(
        dump_dir=str(dump_dir),
        source_schema=source_schema,
        target_schema=source_schema,
        schema_file=str(schema_file),
        index_file=str(index_file) if index_file else None,
        table_count=len(data_files),
        total_bytes=total_bytes,
        tables=tables,
        data_files=[str(path) for path in data_files],
        created_at_utc=datetime.now(timezone.utc).isoformat(),
    )


def run_shell(command: str, env: dict[str, str]) -> None:
    subprocess.run(["bash", "-lc", command], check=True, env=env)


def sql_stream_command(
    file_path: Path,
    *,
    source_schema: str,
    target_schema: str,
    container: str,
    root_password: str,
    kind: str,
) -> str:
    mysql_cmd = (
        f"MYSQL_PWD={shlex.quote(root_password)} docker exec -i -e MYSQL_PWD "
        f"{shlex.quote(container)} mariadb -uroot --binary-mode --max-allowed-packet=1G"
    )
    if kind == "schema":
        transform = (
            f"zstd -dc {shlex.quote(str(file_path))} | "
            f"python3 -c {shlex.quote(schema_rewrite_program(source_schema, target_schema))}"
        )
        return f"{transform} | {mysql_cmd}"
    if kind == "indexes":
        transform = (
            f"cat {shlex.quote(str(file_path))} | "
            f"python3 -c {shlex.quote(schema_rewrite_program(source_schema, target_schema))}"
        )
        return f"{transform} | {mysql_cmd}"
    preamble = (
        f"printf 'USE `{target_schema}`;\\nSET FOREIGN_KEY_CHECKS=0;\\n' ; "
        f"zstd -dc {shlex.quote(str(file_path))}"
    )
    return f"({preamble}) | {mysql_cmd}"


def schema_rewrite_program(source_schema: str, target_schema: str) -> str:
    return (
        "import sys; "
        f"src={source_schema!r}; dst={target_schema!r}; "
        "data=sys.stdin.buffer.read().decode('utf-8', 'replace'); "
        "data=data.replace(f'`{src}`', f'`{dst}`'); "
        "data=data.replace(f'USE {src};', f'USE {dst};'); "
        "sys.stdout.write(data)"
    )


def execute_restore(
    plan: DumpPlan,
    *,
    target_schema: str,
    container: str,
    root_password: str,
    drop_target: bool,
    schema_only: bool,
    skip_indexes: bool,
    output_dir: Path | None,
) -> None:
    env = os.environ.copy()
    env["MYSQL_PWD"] = root_password

    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "restore_plan.json").write_text(
            json.dumps({**asdict(plan), "target_schema": target_schema}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    if drop_target:
        drop_sql = f"DROP DATABASE IF EXISTS `{target_schema}`;"
        subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                "-e",
                "MYSQL_PWD",
                container,
                "mariadb",
                "-uroot",
                "--binary-mode",
                "--max-allowed-packet=1G",
                "-e",
                drop_sql,
            ],
            check=True,
            env=env,
        )

    run_shell(
        sql_stream_command(
            Path(plan.schema_file),
            source_schema=plan.source_schema,
            target_schema=target_schema,
            container=container,
            root_password=root_password,
            kind="schema",
        ),
        env,
    )

    if schema_only:
        return

    for file_path in plan.data_files:
        run_shell(
            sql_stream_command(
                Path(file_path),
                source_schema=plan.source_schema,
                target_schema=target_schema,
                container=container,
                root_password=root_password,
                kind="data",
            ),
            env,
        )

    if plan.index_file and not skip_indexes:
        run_shell(
            sql_stream_command(
                Path(plan.index_file),
                source_schema=plan.source_schema,
                target_schema=target_schema,
                container=container,
                root_password=root_password,
                kind="indexes",
            ),
            env,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Restore one schema from a tablewise sql.zst dump directory.")
    parser.add_argument("dump_dir", help="Path to dump_*/tablewise directory")
    parser.add_argument("--schema", required=True, help="Source schema name inside the dump")
    parser.add_argument("--target-schema", help="Target schema name in MariaDB (default: same as source)")
    parser.add_argument("--tables", help="Comma-separated subset of tables to restore")
    parser.add_argument("--dotenv", default=".env", help="dotenv file with MariaDB credentials")
    parser.add_argument("--container", help="Docker container name (default from env or mariadb-kisti)")
    parser.add_argument("--dry-run", action="store_true", help="Only emit restore plan")
    parser.add_argument("--drop-target", action="store_true", help="Drop target schema before restore")
    parser.add_argument("--schema-only", action="store_true", help="Restore schema DDL only")
    parser.add_argument("--skip-indexes", action="store_true", help="Skip index restore")
    parser.add_argument("--output-dir", help="Optional output directory for restore plan JSON")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dump_dir = Path(args.dump_dir).expanduser().resolve()
    dotenv_path = Path(args.dotenv).expanduser().resolve()
    env_file = load_env(dotenv_path)
    root_password = env_file.get("MARIADB_ROOT_PASSWORD") or os.environ.get("MARIADB_ROOT_PASSWORD")
    if not root_password:
        raise RuntimeError(f"Could not restore DB password from dotenv: {dotenv_path}")
    container = args.container or env_file.get("MARIADB_CONTAINER_NAME") or os.environ.get("MARIADB_CONTAINER_NAME") or "mariadb-kisti"

    selected_tables = None
    if args.tables:
        selected_tables = {value.strip() for value in args.tables.split(",") if value.strip()}
    plan = discover_dump(dump_dir, args.schema, selected_tables)
    target_schema = args.target_schema or args.schema
    plan.target_schema = target_schema

    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "restore_plan.json").write_text(json.dumps(asdict(plan), ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        output_dir = None

    if args.dry_run:
        json.dump(asdict(plan), sys.stdout, ensure_ascii=False, indent=2)
        sys.stdout.write("\n")
        return 0

    execute_restore(
        plan,
        target_schema=target_schema,
        container=container,
        root_password=root_password,
        drop_target=args.drop_target,
        schema_only=args.schema_only,
        skip_indexes=args.skip_indexes,
        output_dir=output_dir,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
