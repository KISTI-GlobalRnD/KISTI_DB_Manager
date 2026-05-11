from __future__ import annotations

import json
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import coerce_db_config
from .runstate import atomic_write_json


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


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


def db_name_from_plan(plan: dict[str, Any]) -> str:
    value = str(plan.get("db_name") or "").strip()
    if value:
        return value
    db = plan.get("db")
    if isinstance(db, dict):
        value = str(db.get("name") or db.get("database") or "").strip()
        if value:
            return value
    return ""


def hydrate_db_password(db_config: dict[str, Any], *, dotenv_path: Path | None) -> dict[str, Any]:
    dbc = dict(db_config)
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


def plan_path_value(plan: dict[str, Any], key: str, default: str = "") -> Path:
    value = str(plan.get(key) or default).strip()
    if not value:
        raise ValueError(f"plan.{key} is required")
    return Path(value).expanduser().resolve()


def db_config_from_plan(plan: dict[str, Any]) -> dict[str, Any]:
    run_dir = plan_path_value(plan, "run_dir")
    config_path = plan_path_value(plan, "config", str(run_dir / "config.json"))
    cfg = read_json(config_path)
    db_raw = dict(cfg.get("db_config") or {})
    db_section = plan.get("db") if isinstance(plan.get("db"), dict) else {}
    if db_section:
        for key in ("driver", "dialect", "host", "port", "user", "password", "database", "dbname", "schema"):
            if key in db_section and db_section.get(key) not in (None, ""):
                db_raw[key] = db_section.get(key)
    db_name = db_name_from_plan(plan)
    if db_name:
        db_raw["database"] = db_name
    driver, _ = db_driver_from_values(plan, db_raw)
    if driver in {"mariadb", "mysql"}:
        db_config = coerce_db_config(db_raw, inplace=False)
    else:
        db_config = dict(db_raw)
        db_config.setdefault("port", 5432)
    return hydrate_db_password(
        db_config,
        dotenv_path=Path(str(plan.get("dotenv") or ".env")).expanduser().resolve() if plan.get("dotenv", ".env") else None,
    )


def db_driver_from_values(plan: dict[str, Any], db_config: dict[str, Any]) -> tuple[str, bool]:
    db = plan.get("db") if isinstance(plan.get("db"), dict) else {}
    raw = str(db.get("driver") or db.get("dialect") or db_config.get("driver") or db_config.get("dialect") or "").strip().lower()
    if raw:
        if raw in {"postgres", "postgresql", "pg"}:
            return "postgresql", False
        if raw in {"mariadb", "mysql"}:
            return raw, False
        return raw, False
    port = int(db_config.get("port") or 0)
    if port == 5432:
        return "postgresql", True
    return "mariadb", True


def materialize_table_prefix(plan: dict[str, Any]) -> str:
    materialize = plan.get("materialize") if isinstance(plan.get("materialize"), dict) else {}
    return str(materialize.get("table_prefix") or "")


def bool_from_plan(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def planned_tables(plan: dict[str, Any], *, table_names: list[str] | None = None) -> list[dict[str, Any]]:
    selected = {str(name).strip() for name in (table_names or []) if str(name).strip()}
    prefix = materialize_table_prefix(plan)
    out: list[dict[str, Any]] = []
    raw = plan.get("tables") or []
    if not isinstance(raw, list):
        return out
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or item.get("table") or "").strip()
        if not name or (selected and name not in selected):
            continue
        out.append(
            {
                "name": name,
                "target_table": f"{prefix}{name}",
                "reset": bool_from_plan(item.get("reset"), default=True),
            }
        )
    return out


def normalize_index_columns(raw: Any) -> list[tuple[str, int | None]]:
    out: list[tuple[str, int | None]] = []
    if not isinstance(raw, list):
        return out
    for item in raw:
        if isinstance(item, str):
            out.append((item, None))
            continue
        if isinstance(item, dict):
            name = str(item.get("name") or item.get("column") or "").strip()
            if not name:
                continue
            prefix = item.get("prefix_len")
            out.append((name, int(prefix) if prefix not in (None, "", 0) else None))
    return out


def planned_indexes(plan: dict[str, Any]) -> list[dict[str, Any]]:
    finalize = plan.get("finalize") if isinstance(plan.get("finalize"), dict) else {}
    raw = finalize.get("indexes") or plan.get("indexes") or []
    out: list[dict[str, Any]] = []
    if not isinstance(raw, list):
        return out
    for item in raw:
        if not isinstance(item, dict):
            continue
        table = str(item.get("table") or "").strip()
        name = str(item.get("index_name") or item.get("name") or "").strip()
        if not table or not name:
            continue
        out.append(
            {
                "table": table,
                "index_name": name,
                "columns": normalize_index_columns(item.get("columns")),
                "unique": bool(item.get("unique", False)),
            }
        )
    return out


def quote_mysql(name: str) -> str:
    return "`" + str(name).replace("`", "``") + "`"


def quote_pg(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def add_issue(report: dict[str, Any], *, check: str, message: str, severity: str = "error", **extra: Any) -> None:
    report.setdefault("issues", []).append({"severity": severity, "check": check, "message": message, **extra})


def add_warning(report: dict[str, Any], *, check: str, message: str, **extra: Any) -> None:
    report.setdefault("warnings", []).append({"check": check, "message": message, **extra})


def mysql_index_definition(cur, *, table: str, index_name: str) -> dict[str, Any] | None:
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


def index_definition_matches(expected: dict[str, Any], observed: dict[str, Any] | None) -> bool:
    if observed is None:
        return False
    return list(expected.get("columns") or []) == list(observed.get("columns") or []) and bool(expected.get("unique")) == bool(observed.get("unique"))


def mysql_load_data_roundtrip(conn, *, staging_dir: Path) -> dict[str, Any]:
    from . import load_data
    import duckdb

    rows = [
        ("W1", "plain"),
        ("W2", 'line1 "\nline2'),
        ("W3", None),
        ("W4", "NULL"),
        ("W5", "line1\r\nline2"),
        ("W6", 'backslash quote \\"\ninside'),
    ]
    table_name = f"kisti_preflight_load_{uuid.uuid4().hex[:12]}"
    stage_path = ""
    staging_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        prefix="kisti_preflight_load_",
        suffix=".tsv",
        delete=False,
        dir=str(staging_dir),
    ) as f:
        stage_path = f.name
    try:
        con = duckdb.connect(database=":memory:")
        try:
            con.execute("CREATE TABLE stage(id VARCHAR, txt VARCHAR)")
            con.executemany("INSERT INTO stage VALUES (?, ?)", rows)
            con.execute(
                f"COPY stage TO {json.dumps(str(stage_path))} "
                f"{load_data.DUCKDB_LOAD_DATA_DIALECT.duckdb_copy_options_sql()};"
            )
        finally:
            con.close()
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TEMPORARY TABLE {quote_mysql(table_name)} ("
                "`id` VARCHAR(32) NOT NULL PRIMARY KEY, "
                "`txt` LONGTEXT NULL"
                ") CHARACTER SET utf8mb4"
            )
        loaded = load_data.load_data_local_infile_tabular_file(
            conn=conn,
            table_name=table_name,
            file_path=str(stage_path),
            sep="\t",
            columns_expr=["`id`", "`txt`"],
            ignore_lines=0,
            dialect=load_data.DUCKDB_LOAD_DATA_DIALECT,
            expected_rows=len(rows),
            line_terminator="\n",
        )
        with conn.cursor() as cur:
            cur.execute(f"SELECT `id`, `txt` FROM {quote_mysql(table_name)} ORDER BY `id`")
            fetched = list(cur.fetchall())
        return {"status": "ok", "loaded_rows": int(loaded), "content_match": fetched == rows}
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TEMPORARY TABLE IF EXISTS {quote_mysql(table_name)}")
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        if stage_path:
            try:
                os.remove(stage_path)
            except Exception:
                pass


def mysql_permission_probe(conn) -> dict[str, Any]:
    table = f"kisti_preflight_perm_{uuid.uuid4().hex[:12]}"
    steps: list[str] = []
    result: dict[str, Any] = {"status": "ok", "steps": steps}
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {quote_mysql(table)} (`id` INT NOT NULL)")
            steps.append("create_table")
            cur.execute(f"INSERT INTO {quote_mysql(table)} (`id`) VALUES (1)")
            steps.append("insert")
            cur.execute(f"SELECT COUNT(*) FROM {quote_mysql(table)}")
            steps.append("select")
            cur.execute(f"ALTER TABLE {quote_mysql(table)} ADD COLUMN `txt` VARCHAR(16) NULL")
            steps.append("alter")
            cur.execute(f"CREATE INDEX `idx_txt` ON {quote_mysql(table)} (`txt`)")
            steps.append("create_index")
        return result
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {quote_mysql(table)}")
            steps.append("drop_table")
        except Exception as exc:
            result["status"] = "error"
            result["cleanup_error_type"] = type(exc).__name__
            result["cleanup_error"] = str(exc)


def run_mariadb_preflight(
    report: dict[str, Any],
    *,
    db_config: dict[str, Any],
    plan: dict[str, Any],
    table_names: list[str] | None,
    staging_dir: Path,
) -> None:
    import pymysql

    conn = pymysql.connect(
        host=db_config.get("host"),
        user=db_config.get("user"),
        password=db_config.get("password"),
        database=db_config.get("database"),
        port=int(db_config.get("port") or 3306),
        charset="utf8mb4",
        local_infile=1,
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT VERSION(), @@version_comment, DATABASE(),
                       @@character_set_database, @@collation_database,
                       @@sql_mode, @@lower_case_table_names, @@local_infile
                """
            )
            row = cur.fetchone() or ()
            report["server"] = {
                "version": str(row[0] if len(row) > 0 else ""),
                "version_comment": str(row[1] if len(row) > 1 else ""),
                "database": str(row[2] if len(row) > 2 else ""),
                "character_set_database": str(row[3] if len(row) > 3 else ""),
                "collation_database": str(row[4] if len(row) > 4 else ""),
                "sql_mode": str(row[5] if len(row) > 5 else ""),
                "lower_case_table_names": int(row[6] or 0) if len(row) > 6 else None,
                "local_infile": str(row[7] if len(row) > 7 else ""),
            }
            report["capabilities"]["local_infile"] = str(report["server"].get("local_infile")) not in {"0", "OFF", "off", "False", "false"}
            if not report["capabilities"]["local_infile"]:
                add_issue(report, check="local_infile", message="Server variable @@local_infile is disabled")

            try:
                report["checks"]["permission_probe"] = mysql_permission_probe(conn)
            except Exception as exc:
                report["checks"]["permission_probe"] = {"status": "error", "error_type": type(exc).__name__, "error": str(exc)}
                add_issue(report, check="permission_probe", message=str(exc), error_type=type(exc).__name__)

            preflight = plan.get("preflight") if isinstance(plan.get("preflight"), dict) else {}
            load_data_probe = bool(preflight.get("load_data_roundtrip", True))
            materialize = plan.get("materialize") if isinstance(plan.get("materialize"), dict) else {}
            needs_load_data = str(materialize.get("staging_writer") or "duckdb") == "duckdb"
            if load_data_probe and needs_load_data:
                try:
                    result = mysql_load_data_roundtrip(conn, staging_dir=staging_dir)
                    report["checks"]["load_data_roundtrip"] = result
                    if result.get("status") != "ok" or result.get("content_match") is not True:
                        add_issue(report, check="load_data_roundtrip", message="DuckDB staging and MariaDB LOAD DATA content did not match")
                except Exception as exc:
                    report["checks"]["load_data_roundtrip"] = {"status": "error", "error_type": type(exc).__name__, "error": str(exc)}
                    add_issue(report, check="load_data_roundtrip", message=str(exc), error_type=type(exc).__name__)

            tables = planned_tables(plan, table_names=table_names)
            target_names = [item["target_table"] for item in tables]
            table_checks: dict[str, Any] = {}
            for item in tables:
                target = str(item["target_table"])
                reset = bool(item.get("reset"))
                cur.execute(
                    """
                    SELECT table_type, engine, table_collation
                    FROM information_schema.tables
                    WHERE table_schema = DATABASE()
                      AND table_name = %s
                    """,
                    (target,),
                )
                row = cur.fetchone()
                entry: dict[str, Any] = {"target_table": target, "reset": reset, "exists": row is not None}
                if row is not None:
                    entry.update({"table_type": row[0], "engine": row[1], "table_collation": row[2]})
                    if reset and str(row[0]).upper() != "BASE TABLE":
                        add_issue(report, check="reset_non_base_table", message=f"reset target is not a base table: {target}", table=target)
                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM information_schema.triggers
                        WHERE trigger_schema = DATABASE()
                          AND event_object_table = %s
                        """,
                        (target,),
                    )
                    triggers = int((cur.fetchone() or (0,))[0] or 0)
                    entry["trigger_count"] = triggers
                    if reset and triggers:
                        add_issue(report, check="reset_table_has_triggers", message=f"reset target has triggers: {target}", table=target, count=triggers)
                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM information_schema.key_column_usage
                        WHERE table_schema = DATABASE()
                          AND (
                            (table_name = %s AND referenced_table_name IS NOT NULL)
                            OR referenced_table_name = %s
                          )
                        """,
                        (target, target),
                    )
                    fk_count = int((cur.fetchone() or (0,))[0] or 0)
                    entry["foreign_key_reference_count"] = fk_count
                    if reset and fk_count:
                        add_issue(report, check="reset_table_fk_related", message=f"reset target is foreign-key related: {target}", table=target, count=fk_count)
                table_checks[target] = entry
            report["checks"]["tables"] = table_checks
            report["capabilities"]["planned_target_tables"] = target_names

            index_checks: dict[str, Any] = {}
            for spec in planned_indexes(plan):
                table = str(spec["table"])
                index_name = str(spec["index_name"])
                observed = mysql_index_definition(cur, table=table, index_name=index_name)
                match = index_definition_matches(spec, observed) if observed is not None else None
                index_checks[f"{table}.{index_name}"] = {
                    "table": table,
                    "index_name": index_name,
                    "expected": {"columns": spec["columns"], "unique": bool(spec["unique"])},
                    "observed": observed,
                    "match": match,
                }
                if observed is not None and match is not True:
                    add_issue(
                        report,
                        check="index_definition_mismatch",
                        message=f"existing index definition does not match plan: {table}.{index_name}",
                        table=table,
                        index=index_name,
                    )
            report["checks"]["indexes"] = index_checks
    finally:
        conn.close()


def connect_postgresql(db_config: dict[str, Any]):
    try:
        import psycopg

        return psycopg.connect(
            host=db_config.get("host"),
            port=int(db_config.get("port") or 5432),
            user=db_config.get("user"),
            password=db_config.get("password"),
            dbname=db_config.get("database") or db_config.get("dbname"),
            autocommit=True,
        )
    except ImportError:
        try:
            import psycopg2

            conn = psycopg2.connect(
                host=db_config.get("host"),
                port=int(db_config.get("port") or 5432),
                user=db_config.get("user"),
                password=db_config.get("password"),
                dbname=db_config.get("database") or db_config.get("dbname"),
            )
            conn.autocommit = True
            return conn
        except ImportError as exc:
            raise RuntimeError("PostgreSQL preflight requires psycopg or psycopg2") from exc


def run_postgresql_preflight(
    report: dict[str, Any],
    *,
    db_config: dict[str, Any],
    plan: dict[str, Any],
    table_names: list[str] | None,
    require_reload_supported: bool,
) -> None:
    report["capabilities"]["preflight_supported"] = True
    report["capabilities"]["reload_supported"] = False
    report["capabilities"]["finalize_supported"] = False
    report.setdefault("unsupported_features", []).extend(["postgresql_parquet_reload", "postgresql_finalize"])
    conn = connect_postgresql(db_config)
    db_section = plan.get("db") if isinstance(plan.get("db"), dict) else {}
    schema = str(db_config.get("schema") or db_section.get("schema") or "public")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database(), version(), current_setting('server_encoding'), current_setting('lc_collate'), current_setting('lc_ctype'), current_schema()")
            row = cur.fetchone() or ()
            report["server"] = {
                "database": str(row[0] if len(row) > 0 else ""),
                "version": str(row[1] if len(row) > 1 else ""),
                "server_encoding": str(row[2] if len(row) > 2 else ""),
                "lc_collate": str(row[3] if len(row) > 3 else ""),
                "lc_ctype": str(row[4] if len(row) > 4 else ""),
                "current_schema": str(row[5] if len(row) > 5 else ""),
                "target_schema": schema,
            }
            table_checks: dict[str, Any] = {}
            for item in planned_tables(plan, table_names=table_names):
                target = str(item["target_table"])
                cur.execute(
                    """
                    SELECT table_type
                    FROM information_schema.tables
                    WHERE table_schema = %s
                      AND table_name = %s
                    """,
                    (schema, target),
                )
                row = cur.fetchone()
                entry = {"schema": schema, "target_table": target, "reset": bool(item.get("reset")), "exists": row is not None}
                if row is not None:
                    entry["table_type"] = row[0]
                table_checks[target] = entry
            report["checks"]["tables"] = table_checks
    finally:
        conn.close()
    if require_reload_supported and not bool((plan.get("preflight") or {}).get("allow_unsupported", False)):
        add_issue(
            report,
            check="unsupported_reload_backend",
            message="PostgreSQL preflight is supported, but parquet reload is only implemented for MariaDB/MySQL",
        )


def report_final_status(report: dict[str, Any], *, hard_fail: bool) -> str:
    errors = [item for item in report.get("issues") or [] if item.get("severity", "error") == "error"]
    if errors and hard_fail:
        return "failed"
    if errors:
        return "done_with_issues"
    if report.get("warnings"):
        return "done_with_warnings"
    return "done"


def run_target_db_preflight(
    plan_path: Path,
    *,
    out_path: Path | None = None,
    table_names: list[str] | None = None,
    require_reload_supported: bool = False,
) -> dict[str, Any]:
    plan_path = Path(plan_path).expanduser().resolve()
    plan = read_json(plan_path)
    run_dir = plan_path_value(plan, "run_dir")
    reports_dir = Path(str(plan.get("reports_dir") or run_dir / "reports")).expanduser().resolve()
    out = Path(out_path).expanduser().resolve() if out_path else reports_dir / "target_db_preflight.json"
    preflight = plan.get("preflight") if isinstance(plan.get("preflight"), dict) else {}
    failure_policy = str(preflight.get("failure_policy") or "hard").strip().lower()
    hard_fail = failure_policy != "warn"
    db_config = db_config_from_plan(plan)
    driver, inferred = db_driver_from_values(plan, db_config)
    staging_dir = Path(str(plan.get("staging_dir") or run_dir / "staging")).expanduser().resolve()
    report: dict[str, Any] = {
        "status": "running",
        "generated_at": utc_now_iso(),
        "updated_at": None,
        "plan": str(plan_path),
        "driver": driver,
        "driver_inferred": bool(inferred),
        "database": str(db_config.get("database") or db_config.get("dbname") or ""),
        "failure_policy": failure_policy,
        "capabilities": {"preflight_supported": True, "reload_supported": driver in {"mariadb", "mysql"}, "finalize_supported": driver in {"mariadb", "mysql"}},
        "checks": {},
        "issues": [],
        "warnings": [],
        "unsupported_features": [],
    }
    write_json(out, report)
    try:
        artifact_cfg = preflight.get("artifact_contract") if isinstance(preflight.get("artifact_contract"), dict) else {}
        if bool(artifact_cfg.get("enabled", True)):
            try:
                from .parquet_artifacts import artifact_contract_from_plan

                artifact_report = artifact_contract_from_plan(plan, table_names=table_names)
                report["checks"]["parquet_artifacts"] = artifact_report
                for item in artifact_report.get("issues") or []:
                    add_issue(
                        report,
                        check="parquet_artifact_contract." + str(item.get("check") or "issue"),
                        message=str(item.get("message") or "parquet artifact contract issue"),
                        severity=str(item.get("severity") or "error"),
                        artifact_issue=item,
                    )
                for item in artifact_report.get("warnings") or []:
                    add_warning(
                        report,
                        check="parquet_artifact_contract." + str(item.get("check") or "warning"),
                        message=str(item.get("message") or "parquet artifact contract warning"),
                        artifact_warning=item,
                    )
            except Exception as exc:
                add_issue(
                    report,
                    check="parquet_artifact_contract.exception",
                    message=str(exc),
                    error_type=type(exc).__name__,
                )
        if driver in {"mariadb", "mysql"}:
            run_mariadb_preflight(report, db_config=db_config, plan=plan, table_names=table_names, staging_dir=staging_dir)
        elif driver == "postgresql":
            run_postgresql_preflight(report, db_config=db_config, plan=plan, table_names=table_names, require_reload_supported=require_reload_supported)
        else:
            report["capabilities"]["preflight_supported"] = False
            add_issue(report, check="unsupported_driver", message=f"unsupported db driver: {driver}")
    except Exception as exc:
        add_issue(report, check="preflight_exception", message=str(exc), error_type=type(exc).__name__)
    report["status"] = report_final_status(report, hard_fail=hard_fail)
    report["finished_at"] = utc_now_iso()
    write_json(out, report)
    return report
