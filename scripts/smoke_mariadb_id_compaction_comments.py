#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from KISTI_DB_Manager import manage
from KISTI_DB_Manager.namemap import NameMap


def _read_env_like(path: Path | None) -> dict[str, str]:
    if path is None or not path.exists():
        return {}
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        key, value = s.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            out[key] = value
    return out


def _pick(env: dict[str, str], *names: str, default: str = "") -> str:
    for name in names:
        value = os.environ.get(name) or env.get(name)
        if value:
            return value
    return default


def _db_config(dotenv_path: Path | None, database: str | None = None) -> dict:
    env = _read_env_like(dotenv_path)
    user = _pick(env, "MARIADB_USER", "MYSQL_USER", default="root")
    password = _pick(env, "MARIADB_PASSWORD", "MYSQL_PASSWORD", default="")
    if user == "root":
        password = _pick(
            env,
            "MARIADB_ROOT_PASSWORD",
            "MYSQL_ROOT_PASSWORD",
            "MARIADB_PASSWORD",
            "MYSQL_PASSWORD",
            default=password,
        )
    return {
        "host": _pick(env, "KISTI_TEST_DB_HOST", "KISTI_SMOKE_DB_HOST", "MARIADB_BIND_IP", default="127.0.0.1"),
        "port": int(_pick(env, "KISTI_TEST_DB_PORT", "KISTI_SMOKE_DB_PORT", "MARIADB_BIND_PORT", default="3306")),
        "user": user,
        "password": password,
        "database": database or _pick(env, "KISTI_TEST_DB_NAME", "KISTI_SMOKE_DB_NAME", "MARIADB_DATABASE"),
    }


def _qi(ident: str) -> str:
    return str(ident).replace("`", "``")


def _fetch_column_comment(conn, *, database: str, table: str, column: str) -> str:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COLUMN_COMMENT
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s
            """,
            (database, table, column),
        )
        row = cur.fetchone()
    return str(row[0] if row else "")


def run_smoke(*, dotenv_path: Path | None, database: str | None, keep_table: bool, table_prefix: str) -> dict:
    try:
        import pymysql
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(f"pymysql is required for this smoke test: {exc}") from exc

    cfg = _db_config(dotenv_path, database=database)
    if not cfg.get("database"):
        raise RuntimeError("No database configured. Set MARIADB_DATABASE or pass --database.")

    table = f"{table_prefix}_{uuid4().hex[:12]}"
    url = URL.create(
        "mysql+pymysql",
        username=cfg["user"],
        password=cfg["password"],
        host=cfg["host"],
        port=int(cfg["port"]),
        database=cfg["database"],
    )
    engine = create_engine(url)
    conn = pymysql.connect(
        host=cfg["host"],
        port=int(cfg["port"]),
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        charset="utf8mb4",
        local_infile=True,
        autocommit=False,
    )

    result = {
        "status": "failed",
        "database": cfg["database"],
        "table": table,
        "checks": {},
    }

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT @@local_infile")
            local_infile = str(cur.fetchone()[0])
            result["checks"]["local_infile"] = local_infile
            if local_infile in {"0", "OFF", "off", "False", "false"}:
                raise RuntimeError("@@local_infile is disabled")

            cur.execute(f"DROP TABLE IF EXISTS `{_qi(table)}`")
            cur.execute(
                f"CREATE TABLE `{_qi(table)}` ("
                "`id` VARCHAR(32) NOT NULL PRIMARY KEY"
                ") CHARACTER SET utf8mb4"
            )
        conn.commit()

        nm = NameMap.build(table_name=table, columns=["id"], key_sep="__", max_len=64)
        description = "OpenAlex Author ID. Original URL prefix https://openalex.org/ removed during JSON parsing."
        manage.fill_table_from_rows(
            [
                {"id": "W1", "author_openalex_id": "A1"},
                {"id": "W2", "author_openalex_id": None},
            ],
            cfg,
            table_name=table,
            name_map=nm,
            columns_original=["id", "author_openalex_id"],
            auto_alter_table=True,
            column_type="LONGTEXT",
            load_method="load_data",
            engine=engine,
            local_infile_conn=conn,
            column_descriptions={"author_openalex_id": description},
        )

        author_comment = _fetch_column_comment(
            conn,
            database=str(cfg["database"]),
            table=table,
            column="author_openalex_id",
        )
        result["checks"]["add_column_comment"] = author_comment
        if author_comment != description:
            raise RuntimeError(f"ADD COLUMN comment mismatch: {author_comment!r}")

        with conn.cursor() as cur:
            cur.execute(f"SELECT `id`, `author_openalex_id` FROM `{_qi(table)}` ORDER BY `id`")
            rows = list(cur.fetchall())
        result["checks"]["loaded_rows"] = rows
        if rows != [("W1", "A1"), ("W2", None)]:
            raise RuntimeError(f"Loaded rows mismatch: {rows!r}")

        with conn.cursor() as cur:
            cur.execute(f"ALTER TABLE `{_qi(table)}` ADD COLUMN `legacy_id` VARCHAR(8) COMMENT 'Existing legacy comment'")
            cur.execute(
                f"ALTER TABLE `{_qi(table)}` MODIFY COLUMN `legacy_id` "
                f"{manage._column_type_with_comment('LONGTEXT', 'legacy_id', {}, {'legacy_id': 'Existing legacy comment'})}"
            )
        conn.commit()

        legacy_comment = _fetch_column_comment(
            conn,
            database=str(cfg["database"]),
            table=table,
            column="legacy_id",
        )
        result["checks"]["modify_preserves_existing_comment"] = legacy_comment
        if legacy_comment != "Existing legacy comment":
            raise RuntimeError(f"MODIFY COLUMN comment mismatch: {legacy_comment!r}")

        result["status"] = "passed"
        return result
    finally:
        if not keep_table:
            try:
                with conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS `{_qi(table)}`")
                conn.commit()
            except Exception:
                pass
            result["table_dropped"] = True
        else:
            result["table_dropped"] = False
        conn.close()
        engine.dispose()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Smoke-test MariaDB ID compaction column comments and LOAD DATA.")
    ap.add_argument("--dotenv", default=".env", help="dotenv-like file containing MARIADB_* settings")
    ap.add_argument("--database", default="", help="Override target database")
    ap.add_argument("--keep-table", action="store_true", help="Keep the temporary smoke table for inspection")
    ap.add_argument("--table-prefix", default="kisti_id_compaction_smoke")
    args = ap.parse_args(argv)

    try:
        result = run_smoke(
            dotenv_path=Path(args.dotenv).expanduser().resolve() if args.dotenv else None,
            database=str(args.database or "") or None,
            keep_table=bool(args.keep_table),
            table_prefix=str(args.table_prefix or "kisti_id_compaction_smoke"),
        )
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": {"type": type(exc).__name__, "message": str(exc)}}, ensure_ascii=False, indent=2))
        return 1

    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get("status") == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
