from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import coerce_db_config
from .runstate import atomic_write_json


_IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    payload["updated_at"] = utc_now_iso()
    atomic_write_json(path, payload)


def read_env_like(path: Path | None) -> dict[str, str]:
    if path is None or not path.exists():
        return {}
    out: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def hydrate_db_password(db_config: dict[str, Any], *, dotenv_path: Path | None) -> dict[str, Any]:
    dbc = dict(coerce_db_config(db_config, inplace=False))
    password = str(dbc.get("password") or "")
    if password and password != "***":
        return dbc
    env = read_env_like(dotenv_path)
    user = str(dbc.get("user") or "").strip()
    keys = ["MARIADB_PASSWORD", "MYSQL_PASSWORD", "MYSQL_ROOT_PASSWORD", "MARIADB_ROOT_PASSWORD"]
    if user == "root":
        keys = ["MARIADB_ROOT_PASSWORD", "MYSQL_ROOT_PASSWORD", "MARIADB_PASSWORD", "MYSQL_PASSWORD"]
    for key in keys:
        value = str(env.get(key) or "").strip()
        if value:
            dbc["password"] = value
            return dbc
    raise RuntimeError("Could not restore DB password from dotenv")


def quote_ident(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def load_plan(plan_path: Path) -> dict[str, Any]:
    plan = read_json(Path(plan_path).expanduser().resolve())
    if not isinstance(plan, dict):
        raise ValueError("plan must be a JSON object")
    return plan


def plan_path_value(plan: dict[str, Any], key: str, default: str = "") -> Path:
    value = str(plan.get(key) or default).strip()
    if not value:
        raise ValueError(f"plan.{key} is required")
    return Path(value).expanduser().resolve()


def db_name_from_plan(plan: dict[str, Any]) -> str:
    value = str(plan.get("db_name") or "").strip()
    if value:
        return value
    db = plan.get("db")
    if isinstance(db, dict):
        value = str(db.get("name") or db.get("database") or "").strip()
        if value:
            return value
    try:
        config_path = plan_path_value(plan, "config", str(plan_path_value(plan, "run_dir") / "config.json"))
        cfg = read_json(config_path)
        value = str(((cfg.get("db_config") or {}).get("database")) or "").strip()
        if value:
            return value
    except Exception:
        pass
    return ""


def db_config_from_plan(plan: dict[str, Any]) -> dict[str, Any]:
    config_path = plan_path_value(plan, "config", str(plan_path_value(plan, "run_dir") / "config.json"))
    cfg = read_json(config_path)
    db_config = hydrate_db_password(
        cfg.get("db_config") or {},
        dotenv_path=Path(str(plan.get("dotenv") or ".env")).expanduser().resolve() if plan.get("dotenv", ".env") else None,
    )
    db_name = db_name_from_plan(plan)
    if db_name:
        db_config["database"] = db_name
    return db_config


def normalize_index_columns(raw: Any) -> list[tuple[str, int | None]]:
    if not isinstance(raw, list) or not raw:
        raise ValueError("index.columns must be a non-empty list")
    out: list[tuple[str, int | None]] = []
    for item in raw:
        if isinstance(item, str):
            out.append((item, None))
            continue
        if isinstance(item, dict):
            name = str(item.get("name") or item.get("column") or "").strip()
            if not name:
                raise ValueError(f"index column is missing name: {item}")
            prefix_raw = item.get("prefix_len")
            prefix_len = int(prefix_raw) if prefix_raw not in (None, "", 0) else None
            out.append((name, prefix_len))
            continue
        raise ValueError(f"invalid index column spec: {item!r}")
    return out


def normalize_indexes(plan: dict[str, Any]) -> list[dict[str, Any]]:
    finalize = plan.get("finalize") if isinstance(plan.get("finalize"), dict) else {}
    raw_indexes = finalize.get("indexes") or plan.get("indexes") or []
    if not isinstance(raw_indexes, list):
        raise ValueError("finalize.indexes must be a list")
    indexes: list[dict[str, Any]] = []
    for item in raw_indexes:
        if not isinstance(item, dict):
            raise ValueError(f"index spec must be an object: {item!r}")
        table = str(item.get("table") or "").strip()
        index_name = str(item.get("index_name") or item.get("name") or "").strip()
        if not table or not index_name:
            raise ValueError(f"index spec requires table and index_name: {item}")
        indexes.append(
            {
                "table": table,
                "index_name": index_name,
                "columns": normalize_index_columns(item.get("columns")),
                "unique": bool(item.get("unique", False)),
            }
        )
    return indexes


def table_exists(cur, *, table: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
          AND table_name = %s
        LIMIT 1
        """,
        (table,),
    )
    return cur.fetchone() is not None


def index_exists(cur, *, table: str, index_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND index_name = %s
        LIMIT 1
        """,
        (table, index_name),
    )
    return cur.fetchone() is not None


def index_definition(cur, *, table: str, index_name: str) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT column_name, sub_part, non_unique, seq_in_index
        FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND index_name = %s
        ORDER BY seq_in_index
        """,
        (table, index_name),
    )
    rows = list(cur.fetchall())
    if not rows:
        return None
    return {
        "columns": [(str(row[0]), int(row[1]) if row[1] not in (None, "") else None) for row in rows],
        "unique": int(rows[0][2]) == 0,
    }


def index_definition_matches(*, expected_columns: list[tuple[str, int | None]], expected_unique: bool, observed: dict[str, Any] | None) -> bool:
    if observed is None:
        return False
    return list(observed.get("columns") or []) == list(expected_columns) and bool(observed.get("unique")) == bool(expected_unique)


def column_sql(columns: list[tuple[str, int | None]]) -> str:
    parts = []
    for name, prefix_len in columns:
        if prefix_len:
            parts.append(f"{quote_ident(name)}({int(prefix_len)})")
        else:
            parts.append(quote_ident(name))
    return ", ".join(parts)


def create_index(cur, *, table: str, index_name: str, columns: list[tuple[str, int | None]], unique: bool) -> None:
    verb = "CREATE UNIQUE INDEX" if unique else "CREATE INDEX"
    cur.execute(f"{verb} {quote_ident(index_name)} ON {quote_ident(table)} ({column_sql(columns)})")


def add_fallback_index(
    cur,
    report: dict[str, Any],
    *,
    out_path: Path,
    table: str,
    index_name: str,
    columns: list[tuple[str, int | None]],
    failures: list[dict[str, Any]],
) -> None:
    fallback_name = f"{index_name}_nonunique"
    entry = {"table": table, "index_name": fallback_name, "status": "running", "fallback_for": index_name}
    report["indexes"].append(entry)
    write_json(out_path, report | {"current_step": "index_fallback", "current_table": table, "current_index": fallback_name})
    try:
        if index_exists(cur, table=table, index_name=fallback_name):
            entry["status"] = "exists"
        else:
            create_index(cur, table=table, index_name=fallback_name, columns=columns, unique=False)
            entry["status"] = "created"
    except Exception as exc:
        entry["status"] = "failed"
        entry["error_type"] = type(exc).__name__
        entry["error"] = str(exc)
        failures.append(dict(entry))
    write_json(out_path, report)


def run_finalize_plan(
    plan_path: Path,
    *,
    out_path: Path | None = None,
    strict_indexes: bool | None = None,
    no_unique_fallback: bool | None = None,
    skip_analyze: bool | None = None,
    skip_validation: bool | None = None,
) -> dict[str, Any]:
    plan = load_plan(plan_path)
    run_dir = plan_path_value(plan, "run_dir")
    finalize = plan.get("finalize") if isinstance(plan.get("finalize"), dict) else {}
    out = Path(out_path).expanduser().resolve() if out_path else run_dir / "reports" / "parquet_finalize.json"
    db_config = db_config_from_plan(plan)
    indexes = normalize_indexes(plan)
    strict = bool(finalize.get("strict_indexes", False)) if strict_indexes is None else bool(strict_indexes)
    no_fallback = bool(finalize.get("no_unique_fallback", False)) if no_unique_fallback is None else bool(no_unique_fallback)
    do_analyze = not (bool(finalize.get("skip_analyze", True)) if skip_analyze is None else bool(skip_analyze))
    do_validation = not (bool(finalize.get("skip_validation", True)) if skip_validation is None else bool(skip_validation))

    report: dict[str, Any] = {
        "status": "running",
        "generated_at": utc_now_iso(),
        "updated_at": None,
        "plan": str(Path(plan_path).expanduser().resolve()),
        "database": str(db_config.get("database") or ""),
        "indexes": [],
        "analyze": [],
        "validation": {},
        "current_step": "start",
    }
    write_json(out, report)
    failures: list[dict[str, Any]] = []
    import pymysql

    conn = pymysql.connect(
        host=db_config.get("host"),
        user=db_config.get("user"),
        password=db_config.get("password"),
        database=db_config.get("database"),
        port=int(db_config.get("port") or 3306),
        charset="utf8mb4",
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            for spec in indexes:
                table = str(spec["table"])
                index_name = str(spec["index_name"])
                columns = list(spec["columns"])
                unique = bool(spec["unique"])
                report["current_step"] = "index"
                report["current_table"] = table
                report["current_index"] = index_name
                write_json(out, report)
                if not table_exists(cur, table=table):
                    report["indexes"].append({"table": table, "index_name": index_name, "status": "skipped_missing"})
                    write_json(out, report)
                    continue
                observed = index_definition(cur, table=table, index_name=index_name)
                existed = observed is not None
                entry = {"table": table, "index_name": index_name, "status": "exists" if existed else "running"}
                report["indexes"].append(entry)
                write_json(out, report)
                if existed and not index_definition_matches(expected_columns=columns, expected_unique=unique, observed=observed):
                    entry["status"] = "mismatch"
                    entry["expected"] = {"columns": columns, "unique": unique}
                    entry["observed"] = observed
                    failures.append(dict(entry))
                    write_json(out, report)
                    continue
                if not existed:
                    try:
                        create_index(cur, table=table, index_name=index_name, columns=columns, unique=unique)
                        entry["status"] = "created"
                    except Exception as exc:
                        entry["status"] = "failed"
                        entry["error_type"] = type(exc).__name__
                        entry["error"] = str(exc)
                        failures.append(dict(entry))
                        write_json(out, report)
                        if unique and not no_fallback:
                            add_fallback_index(
                                cur,
                                report,
                                out_path=out,
                                table=table,
                                index_name=index_name,
                                columns=columns,
                                failures=failures,
                            )
                        continue
                write_json(out, report)

            analyze_tables = list(finalize.get("analyze_tables") or sorted({item["table"] for item in indexes}))
            if not do_analyze:
                report["analyze"].append({"status": "skipped", "reason": "skip_analyze", "tables": analyze_tables})
                write_json(out, report)
            else:
                for table in analyze_tables:
                    report["current_step"] = "analyze"
                    report["current_table"] = table
                    report["current_index"] = None
                    entry = {"table": table, "status": "running"}
                    report["analyze"].append(entry)
                    write_json(out, report)
                    if table_exists(cur, table=table):
                        cur.execute(f"ANALYZE TABLE {quote_ident(table)}")
                        entry["status"] = "done"
                    else:
                        entry["status"] = "skipped_missing"
                    write_json(out, report)

            if not do_validation:
                report["validation"] = {"status": "skipped", "reason": "skip_validation"}
                write_json(out, report)
            else:
                validation_tables = list(finalize.get("validation_tables") or sorted({item["table"] for item in indexes}))
                validation = {"status": "done", "tables": {}}
                for table in validation_tables:
                    report["current_step"] = "validation"
                    report["current_table"] = table
                    write_json(out, report)
                    if table_exists(cur, table=table):
                        cur.execute(f"SELECT COUNT(*) FROM {quote_ident(table)}")
                        row = cur.fetchone()
                        validation["tables"][table] = {"row_count": int(row[0] or 0)}
                    else:
                        validation["tables"][table] = {"status": "skipped_missing"}
                report["validation"] = validation
                write_json(out, report)

            if failures and strict:
                raise RuntimeError(f"{len(failures)} index operation(s) failed")
    except Exception as exc:
        report["status"] = "failed"
        report["failed_at"] = utc_now_iso()
        report["error_type"] = type(exc).__name__
        report["error"] = str(exc)
        write_json(out, report)
        raise
    finally:
        conn.close()

    for key in ("current_table", "current_index"):
        report.pop(key, None)
    report["status"] = "done_with_index_errors" if failures else "done"
    report["current_step"] = "done"
    report["finished_at"] = utc_now_iso()
    write_json(out, report)
    return report


def main(argv: list[str] | None = None, *, prog: str | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog=prog,
        description="Create plan-driven DB indexes/analyze/validation for parquet reloads.",
    )
    ap.add_argument("--plan", required=True)
    ap.add_argument("--out", default="")
    ap.add_argument("--strict-indexes", action="store_true")
    ap.add_argument("--no-unique-fallback", action="store_true")
    ap.add_argument("--skip-analyze", action="store_true")
    ap.add_argument("--skip-validation", action="store_true")
    args = ap.parse_args(argv)

    report = run_finalize_plan(
        Path(args.plan),
        out_path=Path(args.out).expanduser().resolve() if args.out else None,
        strict_indexes=True if args.strict_indexes else None,
        no_unique_fallback=True if args.no_unique_fallback else None,
        skip_analyze=True if args.skip_analyze else None,
        skip_validation=True if args.skip_validation else None,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
