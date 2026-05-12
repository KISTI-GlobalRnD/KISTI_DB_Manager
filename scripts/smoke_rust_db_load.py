#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4

from KISTI_DB_Manager.parquet_artifacts import inspect_parquet_artifact_contract
from KISTI_DB_Manager.pipeline import run_json_pipeline


def _read_env_like(path: Path | None) -> dict[str, str]:
    if path is None or not path.exists():
        return {}
    out: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        key, value = line.split("=", 1)
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


def _db_config(dotenv_path: Path | None, database: str | None = None) -> dict[str, Any]:
    env = _read_env_like(dotenv_path)
    user = _pick(env, "KISTI_TEST_DB_USER", "KISTI_SMOKE_DB_USER", "MARIADB_USER", "MYSQL_USER", "DB_USER", default="root")
    password = _pick(
        env,
        "KISTI_TEST_DB_PASSWORD",
        "KISTI_SMOKE_DB_PASSWORD",
        "MARIADB_PASSWORD",
        "MYSQL_PASSWORD",
        "DB_PASSWORD",
        default="",
    )
    if user == "root":
        password = _pick(
            env,
            "MARIADB_ROOT_PASSWORD",
            "MYSQL_ROOT_PASSWORD",
            "KISTI_TEST_DB_PASSWORD",
            "KISTI_SMOKE_DB_PASSWORD",
            "MARIADB_PASSWORD",
            "MYSQL_PASSWORD",
            "DB_PASSWORD",
            default=password,
        )
    return {
        "host": _pick(
            env,
            "KISTI_TEST_DB_HOST",
            "KISTI_SMOKE_DB_HOST",
            "MARIADB_HOST",
            "MYSQL_HOST",
            "DB_HOST",
            "MARIADB_BIND_IP",
            default="127.0.0.1",
        ),
        "port": int(
            _pick(
                env,
                "KISTI_TEST_DB_PORT",
                "KISTI_SMOKE_DB_PORT",
                "MARIADB_PORT",
                "MYSQL_PORT",
                "DB_PORT",
                "MARIADB_BIND_PORT",
                default="3306",
            )
        ),
        "user": user,
        "password": password,
        "database": database
        or _pick(
            env,
            "KISTI_TEST_DB_NAME",
            "KISTI_SMOKE_DB_NAME",
            "MARIADB_DATABASE",
            "MYSQL_DATABASE",
            "DB_NAME",
            "DATABASE",
            default="",
        ),
    }


def _safe_identifier_prefix(value: str) -> str:
    safe = re.sub(r"[^0-9A-Za-z_]+", "_", value.strip())
    safe = re.sub(r"_+", "_", safe).strip("_")
    if not safe:
        safe = "kisti_rust_db_smoke"
    if safe[0].isdigit():
        safe = f"t_{safe}"
    return safe[:40]


def _qi(identifier: str) -> str:
    return str(identifier).replace("`", "``")


def _smoke_drop_targets(candidates: list[str], *, base_table: str) -> list[str]:
    return sorted(str(t) for t in candidates if str(t) == base_table or str(t).startswith(base_table + "__"))


def _drop_smoke_tables(db_config: dict[str, Any], *, base_table: str) -> list[str]:
    import pymysql

    conn = pymysql.connect(
        host=db_config["host"],
        port=int(db_config.get("port") or 3306),
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"],
        connect_timeout=5,
        autocommit=True,
        charset="utf8mb4",
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW TABLES LIKE %s", (base_table + "%",))
            candidates = [str(row[0]) for row in cur.fetchall()]
            targets = _smoke_drop_targets(candidates, base_table=base_table)
            for table in targets:
                cur.execute(f"DROP TABLE IF EXISTS `{_qi(table)}`")
            return targets
    finally:
        conn.close()


def _fetch_table_summary(db_config: dict[str, Any], name_maps: Mapping[str, Any]) -> list[dict[str, Any]]:
    import pymysql

    conn = pymysql.connect(
        host=db_config["host"],
        port=int(db_config.get("port") or 3306),
        user=db_config["user"],
        password=db_config["password"],
        database=db_config["database"],
        connect_timeout=5,
        autocommit=True,
        charset="utf8mb4",
    )
    tables: list[dict[str, Any]] = []
    try:
        with conn.cursor() as cur:
            for original, nm in sorted(name_maps.items()):
                sql_name = nm.table_sql
                cur.execute(f"SELECT COUNT(*) FROM `{_qi(sql_name)}`")
                rows = int(cur.fetchone()[0])
                cur.execute(f"SHOW COLUMNS FROM `{_qi(sql_name)}`")
                columns = [str(row[0]) for row in cur.fetchall()]
                tables.append(
                    {
                        "original": str(original),
                        "sql": str(sql_name),
                        "rows": rows,
                        "columns": columns,
                    }
                )
    finally:
        conn.close()
    return tables


def _sample_records(count: int) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for i in range(1, count + 1):
        records.append(
            {
                "id": f"https://openalex.org/W{i}",
                "doi": f"https://doi.org/10.1000/{i}",
                "display_name": f"Rust DB smoke record {i}",
                "publication_year": 2020 + (i % 4),
                "is_retracted": False,
                "homepage_url": f"https://example.org/value-like-but-not-id/{i}",
                "primary_location": {
                    "landing_page_url": f"https://doi.org/10.1000/{i}",
                    "source": {
                        "id": f"https://openalex.org/S{i % 3}",
                        "display_name": f"Journal {i % 3}",
                    },
                },
                "authorships": [
                    {
                        "author_position": "first",
                        "author": {"id": f"https://openalex.org/A{i}", "display_name": f"Author {i}"},
                        "institutions": [
                            {
                                "id": f"https://openalex.org/I{i % 2}",
                                "display_name": f"Institute {i % 2}",
                            }
                        ],
                    },
                    {
                        "author_position": "last",
                        "author": {
                            "id": f"https://openalex.org/A{i + 10}",
                            "display_name": f"Author {i + 10}",
                        },
                        "institutions": [],
                    },
                ],
                "topics": [
                    {
                        "id": f"https://openalex.org/T{i % 3}",
                        "display_name": f"Topic {i % 3}",
                        "score": round(0.5 + i / 20, 3),
                    },
                    {
                        "id": f"https://openalex.org/T{(i + 1) % 3}",
                        "display_name": f"Topic {(i + 1) % 3}",
                        "score": round(0.4 + i / 20, 3),
                    },
                ],
                "referenced_works": [
                    f"https://openalex.org/W{i + 100}",
                    f"https://openalex.org/W{i + 200}",
                ],
            }
        )
    return records


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def run_smoke(args: argparse.Namespace) -> dict[str, Any]:
    db_config = _db_config(
        Path(args.dotenv).expanduser().resolve() if args.dotenv else None,
        database=str(args.database or "") or None,
    )
    if not db_config.get("database"):
        raise RuntimeError("No database configured. Set MARIADB_DATABASE or pass --database.")

    out_dir = Path(args.out).expanduser().resolve() if args.out else Path(tempfile.mkdtemp(prefix="kisti_rust_db_smoke_"))
    input_dir = out_dir / "input"
    parquet_dir = out_dir / "parquet"
    base_prefix = _safe_identifier_prefix(str(args.table_prefix or "kisti_rust_db_smoke"))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base_table = f"{base_prefix}_{stamp}_{uuid4().hex[:8]}"
    jsonl_path = input_dir / "openalex_smoke.jsonl"
    report_path = out_dir / "run_report.json"
    contract_path = out_dir / "artifact_contract.json"
    summary_path = out_dir / "summary.json"

    _write_jsonl(jsonl_path, _sample_records(max(1, int(args.records))))

    data_config = {
        "PATH": str(input_dir),
        "file_name": jsonl_path.name,
        "file_type": "jsonl",
        "table_name": base_table,
        "KEY_SEP": "__",
        "chunk_size": int(args.chunk_size),
        "flatten_backend": "rust-arrow",
        "rust_db_load": True,
        "rust_db_load_batch_size": int(args.rust_batch_size),
        "persist_parquet_files": True,
        "persist_parquet_dir": str(parquet_dir),
        "schema_mode": "evolve",
        "id_compaction": {
            "enabled": True,
            "preset": "openalex",
            "mode": "semantic_column_strip",
            "description_policy": "required",
            "apply_to_excepted_raw_json": False,
            "collision_policy": "error",
            "namespace_conflict_policy": "error",
        },
    }

    result: dict[str, Any] = {
        "status": "failed",
        "database": db_config.get("database"),
        "base_table": base_table,
        "work_dir": str(out_dir),
        "paths": {
            "input": str(jsonl_path),
            "parquet_dir": str(parquet_dir),
            "run_report": str(report_path),
            "artifact_contract": str(contract_path),
            "summary": str(summary_path),
        },
    }
    dropped: list[str] = []
    try:
        res = run_json_pipeline(
            data_config,
            db_config,
            create=True,
            load=True,
            index=False,
            optimize=False,
            continue_on_error=False,
            chunk_size=int(args.chunk_size),
            max_records=int(args.records),
        )
        res.report.finish()
        report_path.write_text(res.report.to_json(indent=2), encoding="utf-8")
        contract = inspect_parquet_artifact_contract(
            parquet_dir,
            require_schema_manifest=True,
            require_id_compaction=True,
        )
        contract_path.write_text(json.dumps(contract, ensure_ascii=False, indent=2), encoding="utf-8")
        tables = _fetch_table_summary(db_config, res.name_maps)
        errors = [issue.to_dict() for issue in res.report.issues if issue.level == "error"]
        if errors:
            raise RuntimeError(f"run report contains error-level issues: {len(errors)}")
        if contract.get("status") != "done":
            raise RuntimeError(f"artifact contract failed: {contract.get('status')}")
        result.update(
            {
                "status": "passed",
                "tables": tables,
                "stats": dict(res.report.stats),
                "timings_ms": dict(res.report.timings_ms),
                "artifacts": {
                    "flatten_backend_effective": res.report.artifacts.get("flatten_backend_effective"),
                    "rust_arrow_db_bridge": res.report.artifacts.get("rust_arrow_db_bridge"),
                    "rust_db_load": res.report.artifacts.get("rust_db_load"),
                    "rust_db_load_effective": res.report.artifacts.get("rust_db_load_effective"),
                    "latest_rust_db_load": res.report.artifacts.get("latest_rust_db_load"),
                    "id_compaction": res.report.artifacts.get("id_compaction"),
                },
                "issues": [issue.to_dict() for issue in res.report.issues],
                "artifact_contract_status": contract.get("status"),
            }
        )
        return result
    finally:
        if not args.keep_tables:
            try:
                dropped = _drop_smoke_tables(db_config, base_table=base_table)
            except Exception as exc:
                result["cleanup_error"] = {"type": type(exc).__name__, "message": str(exc)}
            result["dropped_tables"] = dropped
        else:
            result["dropped_tables"] = []
        summary_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Smoke-test Rust Arrow parquet + Rust MySQL JSON DB load.")
    ap.add_argument("--dotenv", default=".env", help="dotenv-like file containing DB settings")
    ap.add_argument("--database", default="", help="Override target database")
    ap.add_argument("--out", default="", help="Output directory for smoke artifacts")
    ap.add_argument("--table-prefix", default="kisti_rust_db_smoke", help="Temporary table prefix")
    ap.add_argument("--records", type=int, default=6)
    ap.add_argument("--chunk-size", type=int, default=3)
    ap.add_argument("--rust-batch-size", type=int, default=1000)
    ap.add_argument("--keep-tables", action="store_true", help="Keep temporary DB tables for inspection")
    args = ap.parse_args(argv)

    try:
        result = run_smoke(args)
    except Exception as exc:
        print(
            json.dumps(
                {"status": "failed", "error": {"type": type(exc).__name__, "message": str(exc)}},
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1

    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0 if result.get("status") == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
