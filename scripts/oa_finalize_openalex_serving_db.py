#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pymysql

from KISTI_DB_Manager import manage
from KISTI_DB_Manager.config import coerce_db_config


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _read_env_like(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def _hydrate_db_password(db_config: dict[str, Any], *, dotenv_path: Path | None) -> dict[str, Any]:
    dbc = dict(coerce_db_config(db_config, inplace=False))
    password = str(dbc.get("password") or "")
    if password and password != "***":
        return dbc
    env = _read_env_like(dotenv_path) if dotenv_path else {}
    for key in ("MARIADB_ROOT_PASSWORD", "MARIADB_PASSWORD", "MYSQL_PASSWORD", "MYSQL_ROOT_PASSWORD"):
        value = str(env.get(key) or "").strip()
        if value:
            dbc["password"] = value
            return dbc
    raise RuntimeError("Could not restore DB password from dotenv")


def _index_exists(cur, *, table: str, index_name: str) -> bool:
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


def _table_exists(cur, *, table: str) -> bool:
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


def _add_index(cur, *, table: str, index_name: str, columns: list[tuple[str, int | None]], unique: bool) -> None:
    if not _table_exists(cur, table=table):
        return
    if _index_exists(cur, table=table, index_name=index_name):
        return
    cols_sql = []
    for col_name, prefix_len in columns:
        if prefix_len:
            cols_sql.append(f"`{col_name}`({int(prefix_len)})")
        else:
            cols_sql.append(f"`{col_name}`")
    verb = "CREATE UNIQUE INDEX" if unique else "CREATE INDEX"
    cur.execute(f"{verb} `{index_name}` ON `{table}` ({', '.join(cols_sql)})")


def _scalar(cur, sql: str) -> int:
    cur.execute(sql)
    row = cur.fetchone()
    if not row:
        return 0
    return int(row[0] or 0)


def main() -> int:
    ap = argparse.ArgumentParser(description="Finalize OpenAlex 0330 serving DB indexes and validation.")
    ap.add_argument("--config", required=True, help="JSON config containing db_config")
    ap.add_argument("--dotenv", default=".env")
    ap.add_argument("--out", required=True, help="Validation report path")
    ap.add_argument("--skip-unique-indexes", action="store_true", help="Skip unique indexes and create fallback non-unique indexes")
    ap.add_argument("--no-unique-fallback", action="store_true", help="Do not create non-unique fallback indexes for skipped/failed unique indexes")
    ap.add_argument("--strict-indexes", action="store_true", help="Exit non-zero if any index creation fails")
    ap.add_argument("--table", action="append", default=[], help="Finalize only selected table(s); repeatable")
    ap.add_argument("--skip-analyze", action="store_true", help="Skip ANALYZE TABLE")
    ap.add_argument("--skip-validation", action="store_true", help="Skip post-index validation queries")
    args = ap.parse_args()

    cfg = _read_json(Path(args.config).expanduser().resolve())
    db_config = _hydrate_db_password(cfg.get("db_config") or {}, dotenv_path=Path(args.dotenv).expanduser().resolve())
    out_path = Path(args.out).expanduser().resolve()
    conn = pymysql.connect(
        host=db_config.get("host"),
        user=db_config.get("user"),
        password=db_config.get("password"),
        database=db_config.get("database"),
        port=int(db_config.get("port") or 3306),
        charset="utf8mb4",
        autocommit=True,
    )
    report: dict[str, Any] = {
        "status": "running",
        "generated_at": _iso_now(),
        "updated_at": None,
        "database": str(db_config.get("database") or ""),
        "indexes": [],
        "analyze": [],
        "validation": {},
    }
    index_failures: list[dict[str, Any]] = []

    def save_report(**updates: Any) -> None:
        if updates:
            report.update(updates)
        report["updated_at"] = _iso_now()
        _write_json(out_path, report)

    try:
        save_report(current_step="start")
        with conn.cursor() as cur:
            selected_tables = {str(item).strip() for item in args.table if str(item).strip()}
            specs = [
                ("works", "uk_works_id", [("id", 64)], True),
                ("works_abstract", "uk_works_abstract_oaid_w", [("oaid_w", 32)], True),
                ("works_abstract", "idx_works_abstract_id", [("id", 64)], False),
                ("works_authorships", "idx_works_authorships_id", [("id", 64)], False),
                ("works_affiliation_agg", "uk_works_affiliation_agg_oaid_w", [("oaid_w", 32)], True),
                ("works_affiliation_agg", "idx_works_affiliation_agg_id", [("id", 64)], False),
            ]
            if selected_tables:
                known_tables = {table for table, _index_name, _columns, _unique in specs} | {
                    "works_topics",
                    "works_locations",
                    "works_referenced_works",
                }
                unknown = sorted(selected_tables - known_tables)
                if unknown:
                    raise SystemExit("Selected finalize table(s) are not known: " + ", ".join(unknown))
                specs = [item for item in specs if item[0] in selected_tables]
            def add_nonunique_fallback(table: str, index_name: str, columns: list[tuple[str, int | None]]) -> None:
                fallback_name = f"{index_name}_nonunique"
                fallback_entry = {"table": table, "index_name": fallback_name, "status": "running", "fallback_for": index_name}
                report["indexes"].append(fallback_entry)
                save_report(current_step="index_fallback", current_table=table, current_index=fallback_name)
                try:
                    if _index_exists(cur, table=table, index_name=fallback_name):
                        fallback_entry["status"] = "exists"
                    else:
                        _add_index(cur, table=table, index_name=fallback_name, columns=columns, unique=False)
                        fallback_entry["status"] = "created"
                except Exception as exc:
                    fallback_entry["status"] = "failed"
                    fallback_entry["error_type"] = type(exc).__name__
                    fallback_entry["error"] = str(exc)
                    index_failures.append(dict(fallback_entry))
                save_report()

            for table, index_name, columns, unique in specs:
                save_report(current_step="index", current_table=table, current_index=index_name)
                if not _table_exists(cur, table=table):
                    report["indexes"].append({"table": table, "index_name": index_name, "status": "skipped_missing"})
                    save_report()
                    continue
                if unique and args.skip_unique_indexes:
                    report["indexes"].append({"table": table, "index_name": index_name, "status": "skipped_unique"})
                    save_report()
                    if not args.no_unique_fallback:
                        add_nonunique_fallback(table, index_name, columns)
                    continue
                existed = _index_exists(cur, table=table, index_name=index_name)
                entry = {"table": table, "index_name": index_name, "status": "exists" if existed else "running"}
                report["indexes"].append(entry)
                save_report()
                if not existed:
                    try:
                        _add_index(cur, table=table, index_name=index_name, columns=columns, unique=unique)
                        entry["status"] = "created"
                    except Exception as exc:
                        entry["status"] = "failed"
                        entry["error_type"] = type(exc).__name__
                        entry["error"] = str(exc)
                        index_failures.append(dict(entry))
                        save_report()
                        if unique and not args.no_unique_fallback:
                            add_nonunique_fallback(table, index_name, columns)
                        continue
                save_report()

            tables_to_analyze = ["works", "works_abstract", "works_authorships", "works_affiliation_agg"]
            if selected_tables:
                tables_to_analyze = [table for table in tables_to_analyze if table in selected_tables]
            if args.skip_analyze:
                report["analyze"].append({"status": "skipped", "reason": "--skip-analyze", "tables": tables_to_analyze})
                save_report()
            else:
                for table in tables_to_analyze:
                    save_report(current_step="analyze", current_table=table, current_index=None)
                    if _table_exists(cur, table=table):
                        entry = {"table": table, "status": "running"}
                        report["analyze"].append(entry)
                        save_report()
                        cur.execute(f"ANALYZE TABLE `{table}`")
                        entry["status"] = "done"
                        save_report()

            validation = report["validation"]
            save_report(current_step="validation", current_table=None)
            if args.skip_validation:
                validation["status"] = "skipped"
                validation["reason"] = "--skip-validation"
                save_report(current_validation="skipped")
            elif (not selected_tables or "works" in selected_tables) and _table_exists(cur, table="works"):
                validation["works_rows"] = _scalar(cur, "SELECT COUNT(*) FROM `works`")
                save_report(current_validation="works_rows")
                validation["works_distinct_id"] = _scalar(cur, "SELECT COUNT(DISTINCT `id`) FROM `works`")
                save_report(current_validation="works_distinct_id")
            if args.skip_validation:
                pass
            elif (not selected_tables or "works_abstract" in selected_tables) and _table_exists(cur, table="works_abstract"):
                validation["works_abstract_rows"] = _scalar(cur, "SELECT COUNT(*) FROM `works_abstract`")
                save_report(current_validation="works_abstract_rows")
                validation["works_abstract_orphans"] = _scalar(
                    cur,
                    """
                    SELECT COUNT(*)
                    FROM `works_abstract` a
                    LEFT JOIN `works` w ON w.`id` = a.`id`
                    WHERE w.`id` IS NULL
                    """,
                )
                save_report(current_validation="works_abstract_orphans")
            if args.skip_validation:
                pass
            elif (not selected_tables or "works_affiliation_agg" in selected_tables) and _table_exists(cur, table="works_affiliation_agg"):
                validation["works_affiliation_agg_rows"] = _scalar(cur, "SELECT COUNT(*) FROM `works_affiliation_agg`")
                save_report(current_validation="works_affiliation_agg_rows")
                validation["works_affiliation_agg_distinct_oaid_w"] = _scalar(
                    cur, "SELECT COUNT(DISTINCT `oaid_w`) FROM `works_affiliation_agg`"
                )
                save_report(current_validation="works_affiliation_agg_distinct_oaid_w")
                validation["works_affiliation_agg_orphans"] = _scalar(
                    cur,
                    """
                    SELECT COUNT(*)
                    FROM `works_affiliation_agg` a
                    LEFT JOIN `works` w ON w.`id` = a.`id`
                    WHERE w.`id` IS NULL
                    """,
                )
                save_report(current_validation="works_affiliation_agg_orphans")
            if args.skip_validation:
                pass
            elif (not selected_tables or "works_authorships" in selected_tables) and _table_exists(cur, table="works_authorships"):
                validation["works_authorships_rows"] = _scalar(cur, "SELECT COUNT(*) FROM `works_authorships`")
                save_report(current_validation="works_authorships_rows")
                validation["works_authorships_orphans"] = _scalar(
                    cur,
                    """
                    SELECT COUNT(*)
                    FROM `works_authorships` a
                    LEFT JOIN `works` w ON w.`id` = a.`id`
                    WHERE w.`id` IS NULL
                    """,
                )
                save_report(current_validation="works_authorships_orphans")
            child_tables = ("works_topics", "works_locations", "works_referenced_works")
            if selected_tables:
                child_tables = tuple(table for table in child_tables if table in selected_tables)
            for child_table in child_tables:
                if args.skip_validation:
                    break
                if _table_exists(cur, table=child_table):
                    save_report(current_validation=f"{child_table}_orphans")
                    validation[f"{child_table}_orphans"] = _scalar(
                        cur,
                        f"""
                        SELECT COUNT(*)
                        FROM `{child_table}` c
                        LEFT JOIN `works` w ON w.`id` = c.`id`
                        WHERE w.`id` IS NULL
                        """,
                    )
                    save_report()
            if index_failures and args.strict_indexes:
                raise RuntimeError(f"{len(index_failures)} index operation(s) failed")
    except Exception as exc:
        save_report(
            status="failed",
            failed_at=_iso_now(),
            error_type=type(exc).__name__,
            error=str(exc),
        )
        raise
    finally:
        conn.close()

    report.pop("current_index", None)
    report.pop("current_table", None)
    report.pop("current_validation", None)
    final_status = "done_with_index_errors" if index_failures else "done"
    save_report(status=final_status, finished_at=_iso_now(), current_step="done")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
