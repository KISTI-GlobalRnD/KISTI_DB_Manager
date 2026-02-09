from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .config import coerce_data_config, coerce_db_config
from .namemap import NameMap, load_namemap
from .quarantine import NullQuarantineWriter, QuarantineWriter
from .report import RunReport


def _mask_db_config(db_config: Mapping[str, Any]) -> dict[str, Any]:
    masked = dict(db_config)
    if "password" in masked and masked["password"]:
        masked["password"] = "***"
    return masked


def _apply_fast_load_session_settings(conn, *, report: RunReport | None, stage: str) -> None:
    """
    Best-effort session tuning for bulk ingest.

    WARNING: Some variables may require elevated privileges or may not be supported by the server.
    We treat all failures as non-fatal and simply continue.
    """
    if conn is None:
        return

    settings = [
        ("SET SESSION unique_checks=0", "unique_checks=0"),
        ("SET SESSION foreign_key_checks=0", "foreign_key_checks=0"),
        # Faster commits; higher risk of data loss on crash (trade-off for ingest speed).
        ("SET SESSION innodb_flush_log_at_trx_commit=2", "innodb_flush_log_at_trx_commit=2"),
        # Disable binlog for this session (may require SUPER / SYSTEM_VARIABLES_ADMIN; often fails on managed DBs).
        ("SET SESSION sql_log_bin=0", "sql_log_bin=0"),
    ]

    try:
        with conn.cursor() as cur:
            for sql, label in settings:
                try:
                    cur.execute(sql)
                except Exception as e:
                    if report is not None:
                        try:
                            report.warn(
                                stage=stage,
                                message="Failed to apply fast-load session setting (ignored)",
                                setting=label,
                                error=str(e),
                            )
                        except Exception:
                            pass
    except Exception as e:
        if report is not None:
            try:
                report.warn(stage=stage, message="Failed to apply fast-load session settings (ignored)", error=str(e))
            except Exception:
                pass


@dataclass(frozen=True)
class TabularRunResult:
    name_map: NameMap | None
    report: RunReport


@dataclass(frozen=True)
class JsonRunResult:
    name_maps: dict[str, NameMap]
    report: RunReport


def _json_loads_factory():
    try:
        import orjson

        def loads(obj):
            if isinstance(obj, str):
                obj = obj.encode("utf-8")
            return orjson.loads(obj)

        return loads
    except Exception:
        import json

        def loads(obj):
            if isinstance(obj, (bytes, bytearray, memoryview)):
                obj = bytes(obj).decode("utf-8")
            return json.loads(obj)

        return loads


def _iter_json_records(
    data_config: Mapping[str, Any],
    *,
    report: RunReport | None = None,
    max_records: int | None = None,
    with_context: bool = False,
):
    """
    Yield JSON records from one or more inputs described by data_config.

    Supports:
    - jsonl/ndjson
    - json (single object or array; optionally records_key within a dict)
    - gz (jsonl by default; json if records_key is used and file contains a JSON object/array)
    - zip (json member(s) by name, or auto-pick all .jsonl/.ndjson/.json members)

    Input selection priority:
    1) file_names / input_paths (list)
    2) file_glob / file_patterns
    3) file_name (single, backward compatible)
    """
    import glob
    from pathlib import Path

    from .config import join_path

    loads = _json_loads_factory()

    dc = coerce_data_config(data_config)
    base_path = Path(str(dc.get("PATH", "") or ""))
    configured_file_type = str(dc.get("file_type") or "").strip().lower()
    records_key = dc.get("records_key") or dc.get("json_records_key")
    json_member_value = dc.get("json_file_names")
    if json_member_value is None:
        json_member_value = dc.get("json_file_name")
    if json_member_value is None:
        json_member_value = dc.get("inner_file_name")

    max_records = int(max_records) if max_records is not None and int(max_records) > 0 else None
    yielded = 0

    def _as_string_list(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, (str, Path)):
            s = str(value).strip()
            return [s] if s else []
        if isinstance(value, (list, tuple, set)):
            out: list[str] = []
            for item in value:
                if item is None:
                    continue
                s = str(item).strip()
                if s:
                    out.append(s)
            return out
        s = str(value).strip()
        return [s] if s else []

    def _resolve_path(value: str) -> Path:
        p = Path(str(value))
        if p.is_absolute():
            return p
        return Path(join_path(base_path, str(p)))

    source_specs: list[tuple[str, Path]] = []
    for value in _as_string_list(dc.get("file_names") or dc.get("input_paths")):
        source_specs.append(("file_names", _resolve_path(value)))

    glob_values = _as_string_list(dc.get("file_glob") or dc.get("file_patterns") or dc.get("file_pattern"))
    for pattern in glob_values:
        pattern_path = Path(pattern)
        if pattern_path.is_absolute():
            pattern_abs = pattern
        else:
            pattern_abs = str(base_path / pattern)
        matches = sorted(Path(x) for x in glob.glob(pattern_abs, recursive=True))
        if not matches and report is not None:
            try:
                report.warn(stage="iter_json_records", message="No files matched file_glob pattern", pattern=pattern)
            except Exception:
                pass
        for m in matches:
            source_specs.append((f"file_glob:{pattern}", m))

    if not source_specs:
        file_name = str(dc.get("file_name") or "").strip()
        if not file_name:
            raise ValueError("JSON pipeline input is missing. Set one of: file_name, file_names, file_glob")
        source_specs.append(("file_name", _resolve_path(file_name)))

    deduped_sources: list[tuple[str, Path]] = []
    seen_sources: set[str] = set()
    for origin, path in source_specs:
        key = str(path)
        if key in seen_sources:
            continue
        seen_sources.add(key)
        deduped_sources.append((origin, path))

    if not deduped_sources:
        raise FileNotFoundError("No input files found from file_names/file_glob configuration")

    for origin, path in deduped_sources:
        if not path.exists():
            raise FileNotFoundError(f"Input file not found ({origin}): {path}")
        if not path.is_file():
            raise FileNotFoundError(f"Input path is not a file ({origin}): {path}")

    def _bump_bytes(n: int) -> None:
        if report is None:
            return
        try:
            report.bump("io_bytes_read", int(n))
        except Exception:
            return

    def _add_parse_time(dt_s: float) -> None:
        if report is None:
            return
        try:
            report.add_time_s("io.json_parse", float(dt_s))
        except Exception:
            return

    def _add_read_time(dt_s: float) -> None:
        if report is None:
            return
        try:
            report.add_time_s("io.read", float(dt_s))
        except Exception:
            return

    def _can_yield_one() -> bool:
        nonlocal yielded
        if max_records is not None and yielded >= max_records:
            return False
        yielded += 1
        return True

    def _record_output(record: Any, context: Mapping[str, Any] | None):
        if with_context:
            return record, dict(context or {})
        return record

    def emit(obj, *, context: Mapping[str, Any] | None = None):
        if isinstance(obj, list):
            for item in obj:
                if not _can_yield_one():
                    return
                yield _record_output(item, context)
            return
        if isinstance(obj, dict) and records_key and isinstance(obj.get(records_key), list):
            for item in obj.get(records_key) or []:
                if not _can_yield_one():
                    return
                yield _record_output(item, context)
            return
        if not _can_yield_one():
            return
        yield _record_output(obj, context)

    def iter_jsonl_fileobj(f, *, source_label: str, source_member: str | None = None):
        import time

        line_no = 0
        for raw in f:
            line_no += 1
            if max_records is not None and yielded >= max_records:
                return
            line = raw.strip()
            if not line:
                continue
            try:
                _bump_bytes(len(raw))
            except Exception:
                pass
            try:
                t0 = time.perf_counter()
                obj = loads(line)
                _add_parse_time(time.perf_counter() - t0)
            except Exception as e:
                if report:
                    report.exception(
                        stage="iter_json_records",
                        message="Failed to parse JSONL line",
                        exc=e,
                        source=source_label,
                    )
                continue

            if not _can_yield_one():
                return
            context: dict[str, Any] = {"source_path": source_label, "line_no": int(line_no)}
            if source_member:
                context["source_member"] = str(source_member)
            yield _record_output(obj, context)

    def iter_one_source(path: Path, *, source_label: str):
        import time

        file_type = configured_file_type or path.suffix.lstrip(".").lower()

        if file_type in {"jsonl", "ndjson", "jsonlines"}:
            with open(path, "rb") as f:
                yield from iter_jsonl_fileobj(f, source_label=source_label)
            return

        if file_type == "json":
            with open(path, "rb") as f:
                t0 = time.perf_counter()
                raw = f.read()
                _add_read_time(time.perf_counter() - t0)
                _bump_bytes(len(raw))
                t1 = time.perf_counter()
                obj = loads(raw)
                _add_parse_time(time.perf_counter() - t1)
            yield from emit(obj, context={"source_path": source_label})
            return

        if file_type == "gz":
            import gzip

            with gzip.open(path, "rb") as f:
                if records_key:
                    # Try parsing full JSON (dict/array). If it fails, fall back to JSONL.
                    try:
                        t0 = time.perf_counter()
                        raw = f.read()
                        _add_read_time(time.perf_counter() - t0)
                        _bump_bytes(len(raw))
                        t1 = time.perf_counter()
                        obj = loads(raw)
                        _add_parse_time(time.perf_counter() - t1)
                    except Exception:
                        f.seek(0)
                        yield from iter_jsonl_fileobj(f, source_label=source_label)
                    else:
                        yield from emit(obj, context={"source_path": source_label})
                else:
                    yield from iter_jsonl_fileobj(f, source_label=source_label)
            return

        if file_type == "zip":
            import io
            import zipfile

            with zipfile.ZipFile(path, "r") as zf:
                names = [n for n in zf.namelist() if not str(n).endswith("/")]

                requested_members = _as_string_list(json_member_value)
                if requested_members:
                    members = [m for m in requested_members if m in names]
                    missing = [m for m in requested_members if m not in names]
                    for m in missing:
                        if report is not None:
                            try:
                                report.warn(
                                    stage="iter_json_records",
                                    message="Requested ZIP member was not found",
                                    source=source_label,
                                    zip_member=m,
                                )
                            except Exception:
                                pass
                    if not members:
                        raise FileNotFoundError(
                            f"No requested JSON/JSONL member found in ZIP: {path} (requested={requested_members})"
                        )
                else:
                    members = [n for n in names if n.lower().endswith((".jsonl", ".ndjson", ".json"))]
                    members.sort()
                    if not members:
                        raise FileNotFoundError(
                            "No JSON/JSONL file found in ZIP "
                            f"(source={path}; set data_config.json_file_name/json_file_names)"
                        )

                for member in members:
                    if max_records is not None and yielded >= max_records:
                        return
                    with zf.open(member, "r") as fp:
                        member_label = f"{source_label}::{member}"
                        suffix = Path(member).suffix.lower()
                        if suffix in {".jsonl", ".ndjson"}:
                            yield from iter_jsonl_fileobj(
                                fp, source_label=source_label, source_member=str(member)
                            )
                            continue

                        t0 = time.perf_counter()
                        raw = fp.read()
                        _add_read_time(time.perf_counter() - t0)
                        _bump_bytes(len(raw))
                        if not raw:
                            continue
                        if records_key:
                            t1 = time.perf_counter()
                            obj = loads(raw)
                            _add_parse_time(time.perf_counter() - t1)
                            yield from emit(
                                obj,
                                context={"source_path": source_label, "source_member": str(member)},
                            )
                        else:
                            # Heuristic: if it looks like JSON array/object, parse once; otherwise treat as JSONL.
                            head = raw.lstrip()[:1]
                            if head in (b"{", b"["):
                                t1 = time.perf_counter()
                                obj = loads(raw)
                                _add_parse_time(time.perf_counter() - t1)
                                yield from emit(
                                    obj,
                                    context={"source_path": source_label, "source_member": str(member)},
                                )
                            else:
                                yield from iter_jsonl_fileobj(
                                    io.BytesIO(raw),
                                    source_label=source_label,
                                    source_member=str(member),
                                )
            return

        raise ValueError(f"Unsupported file_type={file_type!r} for JSON pipeline (source={path})")

    for _origin, _path in deduped_sources:
        if max_records is not None and yielded >= max_records:
            break
        yield from iter_one_source(_path, source_label=str(_path))


def run_tabular_pipeline(
    data_config: Mapping[str, Any],
    db_config: Mapping[str, Any],
    *,
    df_desc=None,
    name_map: NameMap | dict | None = None,
    desc_params: dict[str, Any] | None = None,
    generate_desc: bool = False,
    emit_ddl: bool = False,
    create: bool = True,
    load: bool = True,
    index: bool = True,
    optimize: bool = True,
    continue_on_error: bool = True,
    report: RunReport | None = None,
    quarantine: QuarantineWriter | None = None,
) -> TabularRunResult:
    """
    One-shot pipeline: (optional) profile -> create -> load -> index -> optimize.

    - Ensures NameMap is shared across all DB operations.
    - Collects errors in RunReport and optionally writes failures to Quarantine JSONL.
    """
    from . import manage

    report = report or RunReport()
    quarantine_cm = quarantine or NullQuarantineWriter()
    import time

    t_total0 = time.perf_counter()

    dc = coerce_data_config(data_config, inplace=isinstance(data_config, dict))
    dbc = coerce_db_config(db_config)

    report.bump("runs_tabular", 1)
    report.bump("tables_total", 1)

    load_method = str(dc.get("db_load_method") or "to_sql").strip().lower()
    fast_load_state = manage.FastLoadState(enabled=(load_method in {"auto", "load_data"}))
    local_infile_conn = None
    if load and fast_load_state.enabled:
        try:
            import pymysql

            local_infile_conn = pymysql.connect(
                host=dbc.get("host"),
                user=dbc.get("user"),
                password=dbc.get("password"),
                database=dbc.get("database"),
                port=int(dbc.get("port") or 3306),
                charset="utf8mb4",
                autocommit=False,
                local_infile=1,
                connect_timeout=3,
            )
            with local_infile_conn.cursor() as cur:
                cur.execute("SELECT @@local_infile;")
                row = cur.fetchone()
            if row is not None and str(row[0]) in {"0", "OFF", "off", "False", "false"}:
                raise RuntimeError("Server variable @@local_infile=0 (LOCAL INFILE disabled)")
            if bool(dc.get("fast_load_session", False)):
                _apply_fast_load_session_settings(
                    local_infile_conn,
                    report=report,
                    stage="tabular_pipeline.fast_load_session",
                )
        except Exception as e:
            fast_load_state.disable(reason="conn_failed", error=str(e))
            try:
                report.warn(
                    stage="tabular_pipeline.load_data",
                    message="Fast load disabled; falling back to pandas.to_sql",
                    error=str(e),
                )
            except Exception:
                pass
            if local_infile_conn is not None:
                try:
                    local_infile_conn.close()
                except Exception:
                    pass
                local_infile_conn = None

    try:
        with quarantine_cm as q:
            try:
                with report.timer("tabular.prepare"):
                    if generate_desc:
                        # Lazy import: preview pulls in pandas/numpy
                        from . import preview

                        report.bump("desc_generated", 1)
                        params = desc_params or {}
                        df_desc = preview.get_Table_Description(dc, params=params, sep=dc.get("KEY_SEP", "__"))
                    elif df_desc is None:
                        df_desc = manage.read_Description(dc)

                    cols = list(getattr(df_desc, "index", []))
                    nm = load_namemap(name_map) or load_namemap(dc.get("_name_map"))
                    if nm is None and cols:
                        nm = NameMap.build(table_name=dc["table_name"], columns=cols, key_sep=dc.get("KEY_SEP", "__"))
                        if isinstance(data_config, dict):
                            data_config["_name_map"] = nm.to_dict()

                    if nm is not None:
                        report.set_artifact("name_map", nm.to_dict())
                        if emit_ddl:
                            try:
                                ddl = manage.generate_create_table_sql(dc, df_desc=df_desc, name_map=nm)
                                report.set_artifact("create_table_sql", ddl)
                            except Exception as e:
                                report.warn(stage="tabular_pipeline", message="Failed to generate DDL artifact", exc=str(e))

            except Exception as e:
                report.exception(
                    stage="tabular_pipeline",
                    message="Failed to prepare description/namemap",
                    exc=e,
                    data_config=dict(dc),
                    db_config=_mask_db_config(dbc),
                )
                q.write(stage="tabular_pipeline.prepare", record=dict(dc), exc=e)
                return TabularRunResult(name_map=None, report=report)

            # DB steps
            def step(fn, *args, stage: str, **kwargs):
                t0 = time.perf_counter()
                try:
                    result = fn(*args, **kwargs)
                    report.bump(f"{stage}_ok", 1)
                    return result
                except Exception as e:
                    report.exception(
                        stage=stage,
                        message=f"Failed at stage: {stage}",
                        exc=e,
                        data_config=dict(dc),
                        db_config=_mask_db_config(dbc),
                    )
                    q.write(stage=stage, record=dict(dc), exc=e)
                    report.bump(f"{stage}_failed", 1)
                    if continue_on_error:
                        return None
                    raise
                finally:
                    report.add_time_s(f"tabular.db.{stage}", time.perf_counter() - t0)

            if create:
                # create_table returns NameMap
                try:
                    t0 = time.perf_counter()
                    nm = manage.create_table(dc, dbc, df_desc=df_desc, name_map=nm)
                    report.add_time_s("tabular.db.create", time.perf_counter() - t0)
                    report.bump("create_ok", 1)
                except Exception as e:
                    report.exception(
                        stage="create",
                        message="Failed to create table",
                        exc=e,
                        data_config=dict(dc),
                        db_config=_mask_db_config(dbc),
                    )
                    q.write(stage="create", record=dict(dc), exc=e)
                    report.bump("create_failed", 1)
                    if not continue_on_error:
                        raise

            if load:
                nm_loaded = step(
                    manage.fill_table_from_file,
                    dc,
                    dbc,
                    stage="load",
                    df_desc=df_desc,
                    name_map=nm,
                    report=report,
                    load_method=load_method,
                    fast_load_state=fast_load_state,
                    local_infile_conn=local_infile_conn,
                )
                if isinstance(nm_loaded, NameMap):
                    nm = nm_loaded

            if index:
                step(
                    manage.set_index,
                    stage="index",
                    db_config=dbc,
                    data_config=dc,
                    df_desc=df_desc,
                    name_map=nm,
                )

            if optimize:
                step(
                    manage.optimize_table,
                    stage="optimize",
                    db_config=dbc,
                    data_config=dc,
                    name_map=nm,
                )

            return TabularRunResult(name_map=nm, report=report)
    finally:
        report.add_time_s("pipeline.tabular.total", time.perf_counter() - t_total0)
        if local_infile_conn is not None:
            try:
                local_infile_conn.close()
            except Exception:
                pass


def run_json_pipeline(
    data_config: Mapping[str, Any],
    db_config: Mapping[str, Any],
    *,
    index_key: str | None = None,
    except_keys: list[str] | None = None,
    chunk_size: int | None = None,
    max_records: int | None = None,
    key_sep: str | None = None,
    emit_ddl: bool = False,
    create: bool = True,
    load: bool = True,
    index: bool = True,
    optimize: bool = True,
    continue_on_error: bool = True,
    report: RunReport | None = None,
    quarantine: QuarantineWriter | None = None,
    extract_fn=None,
    index_prefix_len: int | None = 191,
    column_type: str = "LONGTEXT",
) -> JsonRunResult:
    """
    JSON one-shot pipeline:
      records -> flatten(main + sub tables) -> create -> load -> index -> optimize

    Key features:
    - Per-record quarantine via processing.extract_data_from_jsons(report/quarantine)
    - NameMap per table to keep naming stable across drift
    - Best-effort schema drift handling (missing columns auto-added during load)
    """
    from . import manage

    report = report or RunReport()
    quarantine_cm = quarantine or NullQuarantineWriter()
    import time

    t_total0 = time.perf_counter()

    dc = coerce_data_config(data_config, inplace=isinstance(data_config, dict))
    dbc = coerce_db_config(db_config)

    base_table = str(dc.get("table_name") or "").strip()
    if not base_table:
        raise ValueError("data_config.table_name is required for JSON pipeline")

    key_sep = str(key_sep or dc.get("KEY_SEP", "__"))
    index_key = str(index_key or dc.get("index_key") or "id")
    except_keys = list(except_keys if except_keys is not None else (dc.get("except_keys") or []))
    chunk_size = int(chunk_size or dc.get("chunk_size") or dc.get("batch_size") or 1000)
    if max_records is None:
        max_records = dc.get("max_records")
    max_records = int(max_records) if max_records is not None and int(max_records) > 0 else None
    index_prefix_len = int(dc.get("index_prefix_len", index_prefix_len if index_prefix_len is not None else 191))
    fallback_on_insert_error = bool(dc.get("fallback_on_insert_error", True))
    fallback_column_type = str(dc.get("fallback_column_type", "LONGTEXT"))
    insert_retry_max = int(dc.get("insert_retry_max", 5) or 0)
    schema_mode = str(dc.get("schema_mode") or "evolve").strip().lower()
    if schema_mode in {"evolve_then_freeze", "evolve-then-freeze", "evolve_to_freeze"}:
        schema_mode = "hybrid"
    if schema_mode not in {"evolve", "freeze", "hybrid"}:
        schema_mode = "evolve"
    extra_column_name = str(dc.get("extra_column_name") or "__extra__")
    use_extra_column = schema_mode in {"freeze", "hybrid"}
    hybrid_warmup_batches = int(dc.get("schema_hybrid_warmup_batches", 1) or 0)
    if hybrid_warmup_batches < 0:
        hybrid_warmup_batches = 0
    auto_alter_table_cfg = bool(dc.get("auto_alter_table", True))
    report.set_artifact("schema_mode", schema_mode)
    if use_extra_column:
        report.set_artifact("extra_column_name", extra_column_name)
    if schema_mode == "hybrid":
        report.set_artifact("schema_hybrid_warmup_batches", int(hybrid_warmup_batches))
    report.set_artifact("auto_alter_table", bool(auto_alter_table_cfg))
    report.set_artifact("fast_load_session", bool(dc.get("fast_load_session", False)))

    if extract_fn is None:
        try:
            from .processing import extract_data_from_jsons as extract_fn
        except Exception as e:
            report.exception(
                stage="json_pipeline",
                message="Failed to import JSON processing backend (missing deps?)",
                exc=e,
                data_config=dict(dc),
                db_config=_mask_db_config(dbc),
            )
            return JsonRunResult(name_maps={}, report=report)

    report.bump("runs_json", 1)
    report.bump("tables_total", 1)
    report.set_artifact("index_key", index_key)
    report.set_artifact("chunk_size", chunk_size)
    report.set_artifact("except_keys", list(except_keys))
    report.set_artifact("max_records", max_records)
    try:
        report.set_artifact("parallel_workers", int(dc.get("parallel_workers") or 0))
    except Exception:
        pass

    to_sql_chunksize = dc.get("to_sql_chunksize")
    if to_sql_chunksize is not None:
        try:
            to_sql_chunksize = int(to_sql_chunksize)
        except Exception:
            to_sql_chunksize = None
    to_sql_method = dc.get("to_sql_method")
    sanitize_nan_strings = bool(dc.get("sanitize_nan_strings", False))
    convert_nan_to_none = bool(dc.get("convert_nan_to_none", False))

    name_maps: dict[str, NameMap] = {}
    created_tables: set[str] = set()
    ddl_by_table: dict[str, str] = {}

    def _table_for_sub(sub_key: str) -> str:
        return f"{base_table}{key_sep}{sub_key}"

    def _table_for_excepted(ex_key: str) -> str:
        return f"{base_table}{key_sep}excepted{key_sep}{ex_key}"

    def step(stage: str, fn, *args, **kwargs):
        t0 = time.perf_counter()
        try:
            result = fn(*args, **kwargs)
            report.bump(f"{stage}_ok", 1)
            return result
        except Exception as e:
            report.exception(
                stage=stage,
                message=f"Failed at stage: {stage}",
                exc=e,
                data_config=dict(dc),
                db_config=_mask_db_config(dbc),
            )
            q.write(stage=stage, record={"table_name": base_table}, exc=e)
            report.bump(f"{stage}_failed", 1)
            if continue_on_error:
                return None
            raise
        finally:
            report.add_time_s(f"json.db.{stage}", time.perf_counter() - t0)

    def ensure_name_map(table_original: str, columns: list[str]) -> NameMap:
        columns_norm = [str(c).replace(".", key_sep) for c in columns]
        nm = name_maps.get(table_original)
        if nm is None:
            nm = NameMap.build(table_name=table_original, columns=columns_norm, key_sep=key_sep, max_len=64)
        else:
            nm = nm.with_additional_columns(columns_norm, max_len=64)
        name_maps[table_original] = nm
        if isinstance(data_config, dict):
            data_config.setdefault("_name_maps_json", {})
            data_config["_name_maps_json"][table_original] = nm.to_dict()
        return nm

    def maybe_update_artifacts():
        report.set_artifact("name_maps_json", {k: v.to_dict() for k, v in name_maps.items()})
        if emit_ddl:
            report.set_artifact("create_table_sql_json", dict(ddl_by_table))

    engine = None
    inspector = None
    existing_cols_cache: dict[str, set[str]] = {}
    if load:
        try:
            from sqlalchemy import create_engine, inspect as sa_inspect
            from sqlalchemy.engine import URL

            url = URL.create(
                "mysql+pymysql",
                username=dbc.get("user"),
                password=dbc.get("password"),
                host=dbc.get("host"),
                port=dbc.get("port"),
                database=dbc.get("database"),
            )
            engine = create_engine(url)
            inspector = sa_inspect(engine)
        except Exception:
            engine = None
            inspector = None

    def _get_existing_cols(table_sql: str) -> set[str] | None:
        cols = existing_cols_cache.get(table_sql)
        if cols is not None:
            return cols
        if inspector is None:
            return None
        try:
            got = {c.get("name") for c in inspector.get_columns(table_sql)}
        except Exception:
            return None
        got.discard(None)
        cols = set(got)
        existing_cols_cache[table_sql] = cols
        return cols

    db_load_method = str(dc.get("db_load_method") or "to_sql").strip().lower()
    fast_load_state = manage.FastLoadState(enabled=(db_load_method in {"auto", "load_data"}))
    local_infile_conn = None
    if load and fast_load_state.enabled:
        try:
            import pymysql

            local_infile_conn = pymysql.connect(
                host=dbc.get("host"),
                user=dbc.get("user"),
                password=dbc.get("password"),
                database=dbc.get("database"),
                port=int(dbc.get("port") or 3306),
                charset="utf8mb4",
                autocommit=False,
                local_infile=1,
                connect_timeout=3,
            )
            with local_infile_conn.cursor() as cur:
                cur.execute("SELECT @@local_infile;")
                row = cur.fetchone()
            if row is not None and str(row[0]) in {"0", "OFF", "off", "False", "false"}:
                raise RuntimeError("Server variable @@local_infile=0 (LOCAL INFILE disabled)")
            if bool(dc.get("fast_load_session", False)):
                _apply_fast_load_session_settings(
                    local_infile_conn,
                    report=report,
                    stage="json_pipeline.fast_load_session",
                )
        except Exception as e:
            fast_load_state.disable(reason="conn_failed", error=str(e))
            try:
                report.warn(
                    stage="json_pipeline.load_data",
                    message="Fast load disabled; falling back to pandas.to_sql",
                    error=str(e),
                )
            except Exception:
                pass
            if local_infile_conn is not None:
                try:
                    local_infile_conn.close()
                except Exception:
                    pass
                local_infile_conn = None

    try:
        with quarantine_cm as q:
            batch: list[dict] = []
            batch_contexts: list[dict[str, Any]] = []

            import inspect

            try:
                extract_sig = inspect.signature(extract_fn)
                extract_params = set(extract_sig.parameters.keys())
            except Exception:
                extract_params = set()

            parallel_workers = dc.get("parallel_workers")
            try:
                parallel_workers = int(parallel_workers) if parallel_workers is not None else 0
            except Exception:
                parallel_workers = 0

            db_load_parallel_tables = dc.get("db_load_parallel_tables", dc.get("load_parallel_tables", 0))
            try:
                db_load_parallel_tables = int(db_load_parallel_tables) if db_load_parallel_tables is not None else 0
            except Exception:
                db_load_parallel_tables = 0
            if db_load_parallel_tables < 0:
                db_load_parallel_tables = 0
            try:
                report.set_artifact("db_load_parallel_tables", int(db_load_parallel_tables))
            except Exception:
                pass

            json_streaming_load = bool(dc.get("json_streaming_load", True))
            use_streaming_rows = (
                bool(load)
                and bool(json_streaming_load)
                and str(db_load_method or "").strip().lower() in {"auto", "load_data"}
                and local_infile_conn is not None
            )

            global_index = 0
            batch_no = 0
            hybrid_freeze_started = False

            def flush_batch(
                batch_records: list[dict],
                *,
                index_offset: int,
                record_contexts: list[dict[str, Any]] | None = None,
            ) -> None:
                if not batch_records:
                    return

                nonlocal batch_no, hybrid_freeze_started
                batch_idx = int(batch_no)
                batch_no += 1

                is_hybrid_warmup = schema_mode == "hybrid" and batch_idx < int(hybrid_warmup_batches)
                if schema_mode == "evolve":
                    auto_alter_table_effective = bool(auto_alter_table_cfg)
                elif schema_mode == "freeze":
                    auto_alter_table_effective = False
                else:
                    auto_alter_table_effective = bool(auto_alter_table_cfg) and bool(is_hybrid_warmup)
                    if not is_hybrid_warmup and not hybrid_freeze_started:
                        hybrid_freeze_started = True
                        try:
                            report.bump("schema_hybrid_freeze_started", 1)
                        except Exception:
                            pass
                        report.set_artifact("schema_hybrid_freeze_started_batch", int(batch_idx))
                        report.set_artifact("schema_hybrid_freeze_started_record", int(index_offset))

                if schema_mode == "hybrid":
                    try:
                        report.bump("schema_hybrid_batches_warmup" if is_hybrid_warmup else "schema_hybrid_batches_frozen", 1)
                    except Exception:
                        pass

                report.bump("batches_total", 1)

                if use_streaming_rows and fast_load_state.enabled:
                    try:
                        from .processing import extract_rows_from_jsons, _safe_flatten_jsons_to_tsv_worker
                    except Exception as e:
                        report.exception(
                            stage="json_pipeline.flatten",
                            message="Failed to import extract_rows_from_jsons; falling back to DataFrame path",
                            exc=e,
                        )
                    else:
                        def _is_nullish(v) -> bool:
                            import math

                            if v is None:
                                return True
                            try:
                                if type(v).__name__ == "NAType":
                                    return True
                            except Exception:
                                pass
                            try:
                                return isinstance(v, float) and math.isnan(v)
                            except Exception:
                                return False

                        # Parallel TSV backend (workers write TSV; parent does LOAD DATA) to minimize IPC.
                        if parallel_workers and int(parallel_workers) > 1 and len(batch_records) >= 2 and bool(auto_alter_table_effective):
                            import os
                            import shutil
                            from concurrent.futures import ProcessPoolExecutor

                            pw = min(int(parallel_workers), int(len(batch_records)))
                            # Split the batch into pw contiguous chunks to keep LOAD DATA calls bounded.
                            n = int(len(batch_records))
                            base = n // pw
                            rem = n % pw
                            slices: list[tuple[int, int]] = []
                            start = 0
                            for wi in range(pw):
                                extra = 1 if wi < rem else 0
                                end = start + base + extra
                                if end > start:
                                    slices.append((start, end))
                                start = end

                            tmp_dir = str(dc.get("tmp_dir") or "/tmp")
                            workdirs: list[str] = []
                            results: list[dict] = []
                            class _ParallelTSVFailed(Exception):
                                pass
                            try:
                                try:
                                    with report.timer("json.flatten"):
                                        with ProcessPoolExecutor(max_workers=pw) as ex:
                                            futs = []
                                            for s, e in slices:
                                                ctx_slice = None
                                                if isinstance(record_contexts, (list, tuple)):
                                                    ctx_slice = list(record_contexts[s:e])
                                                futs.append(
                                                    ex.submit(
                                                        _safe_flatten_jsons_to_tsv_worker,
                                                        (
                                                            int(index_offset) + int(s),
                                                            list(batch_records[s:e]),
                                                            index_key,
                                                            tuple(except_keys or ()),
                                                            key_sep,
                                                            tmp_dir,
                                                            ctx_slice,
                                                        ),
                                                    )
                                                )
                                            for fut in futs:
                                                results.append(fut.result())
                                except Exception as e:
                                    try:
                                        report.warn(
                                            stage="json_pipeline.flatten.parallel_tsv",
                                            message="Parallel TSV backend failed; falling back to serial rows backend",
                                            error={"type": type(e).__name__, "message": str(e)},
                                        )
                                    except Exception:
                                        pass
                                    raise _ParallelTSVFailed() from e

                                tables_files: dict[str, list[dict]] = {}
                                load_groups: list[dict[str, Any]] = []

                                def _add_file(table: str, fi: dict | None) -> None:
                                    if not isinstance(fi, dict):
                                        return
                                    if not fi.get("path") or not fi.get("columns") or not fi.get("rows"):
                                        return
                                    tables_files.setdefault(str(table), []).append(fi)

                                for res in results:
                                    if not isinstance(res, dict) or not res.get("ok"):
                                        err = None
                                        if isinstance(res, dict):
                                            err = res.get("error")
                                        report.warn(
                                            stage="json_pipeline.flatten.parallel_tsv",
                                            message="Worker failed to produce TSV artifacts; skipping chunk",
                                            error=err,
                                        )
                                        if not continue_on_error:
                                            raise RuntimeError(f"parallel_tsv_worker_failed: {err}")
                                        continue

                                    try:
                                        report.bump("records_ok", int(res.get("records_ok", 0) or 0))
                                        report.bump("records_failed", int(res.get("records_failed", 0) or 0))
                                    except Exception:
                                        pass

                                    tms = res.get("timings_ms") or {}
                                    try:
                                        report.add_time_ms("json.flatten.workersum", int(tms.get("flatten_ms", 0) or 0))
                                        report.add_time_ms("json.flatten.tsv_write.workersum", int(tms.get("tsv_write_ms", 0) or 0))
                                    except Exception:
                                        pass

                                    wd = res.get("workdir")
                                    if isinstance(wd, str) and wd:
                                        workdirs.append(wd)

                                    _add_file(base_table, res.get("main"))
                                    for sub_key, fi in (res.get("subs") or {}).items():
                                        _add_file(_table_for_sub(str(sub_key).replace(".", key_sep)), fi)
                                    for ex_key, fi in (res.get("excepted") or {}).items():
                                        _add_file(_table_for_excepted(str(ex_key).replace(".", key_sep)), fi)

                                    for err in (res.get("errors") or [])[:20]:
                                        try:
                                            report.warn(
                                                stage="json_pipeline.flatten.parallel_tsv.record",
                                                message="Record failed in parallel TSV worker",
                                                error=err,
                                            )
                                        except Exception:
                                            pass
                                        try:
                                            q.write(
                                                stage="json_pipeline.flatten.record",
                                                record={"table_name": base_table, "error": err},
                                                exc=RuntimeError(str(err.get("message") if isinstance(err, dict) else err)),
                                            )
                                        except Exception:
                                            pass

                                for table_original, files in tables_files.items():
                                    if not files:
                                        continue

                                    # Create table schema once using union of all file columns for this batch.
                                    cols_non_null: set[str] = set()
                                    for fi in files:
                                        for c in fi.get("columns") or []:
                                            cols_non_null.add(str(c))
                                    cols_non_null.add(index_key)
                                    cols = [index_key] + sorted([c for c in cols_non_null if c != index_key])
                                    if use_extra_column and extra_column_name not in set(cols):
                                        cols.append(extra_column_name)
                                    if not cols:
                                        continue

                                    nm = ensure_name_map(table_original, cols)
                                    if emit_ddl:
                                        try:
                                            ddl, _nm2 = manage.generate_create_table_sql_from_columns(
                                                table_name=table_original,
                                                columns=list(nm.columns_original),
                                                name_map=nm,
                                                key_sep=key_sep,
                                                column_type=column_type,
                                            )
                                            ddl_by_table[table_original] = ddl
                                        except Exception as e:
                                            report.warn(stage="json_pipeline", message="Failed to generate DDL artifact", exc=str(e))

                                    if create and table_original not in created_tables:
                                        nm_created = step(
                                            "create",
                                            manage.create_table_from_columns,
                                            dbc,
                                            table_name=table_original,
                                            columns=list(nm.columns_original),
                                            name_map=nm,
                                            key_sep=key_sep,
                                            column_type=column_type,
                                        )
                                        if isinstance(nm_created, NameMap):
                                            name_maps[table_original] = nm_created
                                            nm = nm_created
                                        if nm_created is not None:
                                            created_tables.add(table_original)
                                            report.bump("tables_created", 1)
                                            existing_cols_cache[nm.table_sql] = set(nm.columns_sql)

                                    if load:
                                        # Reduce LOAD DATA calls by concatenating files with identical schemas.
                                        import tempfile
                                        import uuid

                                        groups: dict[tuple[str, ...], list[dict]] = {}
                                        for fi in files:
                                            cols_key = tuple(fi.get("columns") or ())
                                            if not cols_key:
                                                continue
                                            groups.setdefault(cols_key, []).append(fi)

                                        merged_entries: list[dict] = []
                                        for cols_key, group_files in groups.items():
                                            if len(group_files) == 1:
                                                merged_entries.append(group_files[0])
                                                continue

                                            # Merge to a single TSV to amortize LOAD DATA overhead.
                                            tmp_path = None
                                            with tempfile.NamedTemporaryFile(
                                                mode="wb",
                                                prefix=f"kisti_merge_{uuid.uuid4().hex[:8]}_",
                                                suffix=".tsv",
                                                delete=False,
                                                dir=tmp_dir,
                                            ) as out:
                                                tmp_path = out.name
                                                merged_path = tmp_path
                                                for gf in group_files:
                                                    try:
                                                        with open(str(gf.get("path")), "rb") as inp:
                                                            shutil.copyfileobj(inp, out, length=1024 * 1024)
                                                    except Exception:
                                                        # Best-effort: if merge fails, fall back to loading files individually.
                                                        merged_path = None
                                                        break

                                            if not merged_path:
                                                if tmp_path:
                                                    try:
                                                        os.remove(str(tmp_path))
                                                    except Exception:
                                                        pass
                                                merged_entries.extend(group_files)
                                                continue

                                            # Delete originals after successful merge.
                                            for gf in group_files:
                                                try:
                                                    os.remove(str(gf.get("path")))
                                                except Exception:
                                                    pass

                                            merged_entries.append(
                                                {
                                                    "path": str(merged_path),
                                                    "columns": list(cols_key),
                                                    "rows": int(sum(int(gf.get("rows") or 0) for gf in group_files)),
                                                }
                                            )

                                        load_groups.append(
                                            {
                                                "nm": nm,
                                                "table_sql": nm.table_sql,
                                                "entries": merged_entries,
                                                "existing_cols": set(_get_existing_cols(nm.table_sql) or []) if inspector is not None else None,
                                            }
                                        )

                                # Execute LOAD DATA for this batch (optionally parallel across tables).
                                if load and load_groups:
                                    # Serial load: keep existing behavior/timings.
                                    if not db_load_parallel_tables or int(db_load_parallel_tables) <= 1 or len(load_groups) <= 1:
                                        for g in load_groups:
                                            nm = g.get("nm")
                                            table_sql = g.get("table_sql")
                                            entries = g.get("entries") or []
                                            if not nm or not table_sql or not entries:
                                                continue
                                            loaded_any = False
                                            for fi in entries:
                                                path = fi.get("path")
                                                file_cols = fi.get("columns") or []
                                                if not path or not file_cols:
                                                    continue
                                                load_res = step(
                                                    "load",
                                                    manage.fill_table_from_tsv_file,
                                                    str(path),
                                                    dbc,
                                                    table_name=str(table_sql),
                                                    name_map=nm,
                                                    # TSV backend does not populate __extra__; keep file columns exact.
                                                    extra_column_name=None,
                                                    columns_original=list(file_cols),
                                                    auto_alter_table=bool(auto_alter_table_effective),
                                                    column_type=column_type,
                                                    report=report,
                                                    engine=engine,
                                                    existing_cols=_get_existing_cols(str(table_sql)),
                                                    load_method=db_load_method,
                                                    fast_load_state=fast_load_state,
                                                    local_infile_conn=local_infile_conn,
                                                )
                                                if load_res is not None:
                                                    loaded_any = True
                                                    try:
                                                        report.bump("rows_loaded", int(fi.get("rows") or 0))
                                                    except Exception:
                                                        pass
                                                try:
                                                    os.remove(str(path))
                                                except Exception:
                                                    pass
                                            if loaded_any:
                                                report.bump("tables_loaded", 1)
                                        return

                                    # Parallel load across tables: one local_infile connection per thread.
                                    try:
                                        import threading
                                        import time
                                        from concurrent.futures import ThreadPoolExecutor, as_completed

                                        import pymysql

                                        tls = threading.local()
                                        conns: list[Any] = []
                                        conns_lock = threading.Lock()

                                        def _thread_init():
                                            conn = pymysql.connect(
                                                host=dbc.get("host"),
                                                user=dbc.get("user"),
                                                password=dbc.get("password"),
                                                database=dbc.get("database"),
                                                port=int(dbc.get("port") or 3306),
                                                charset="utf8mb4",
                                                autocommit=False,
                                                local_infile=1,
                                                connect_timeout=3,
                                            )
                                            if bool(dc.get("fast_load_session", False)):
                                                try:
                                                    _apply_fast_load_session_settings(
                                                        conn,
                                                        report=None,
                                                        stage="json_pipeline.fast_load_session",
                                                    )
                                                except Exception:
                                                    pass
                                            tls.conn = conn
                                            with conns_lock:
                                                conns.append(conn)

                                        def _get_conn():
                                            return getattr(tls, "conn", None)

                                        def _load_group(g: dict) -> dict:
                                            nm = g.get("nm")
                                            table_sql = str(g.get("table_sql") or "")
                                            entries = g.get("entries") or []
                                            existing_cols = g.get("existing_cols")
                                            res = {
                                                "table_sql": table_sql,
                                                "ok": True,
                                                "loaded_any": False,
                                                "rows_loaded": 0,
                                                "load_ok": 0,
                                                "load_failed": 0,
                                                "load_data_ok": 0,
                                                "existing_cols": existing_cols,
                                                "errors": [],
                                            }
                                            conn = _get_conn()
                                            if conn is None:
                                                res["ok"] = False
                                                res["errors"].append({"type": "RuntimeError", "message": "missing thread connection"})
                                                return res

                                            for fi in entries:
                                                path = fi.get("path")
                                                file_cols = fi.get("columns") or []
                                                if not path or not file_cols:
                                                    continue
                                                try:
                                                    manage.fill_table_from_tsv_file(
                                                        str(path),
                                                        dbc,
                                                        table_name=table_sql,
                                                        name_map=nm,
                                                        extra_column_name=None,
                                                        columns_original=list(file_cols),
                                                        auto_alter_table=bool(auto_alter_table_effective),
                                                        column_type=column_type,
                                                        report=None,  # avoid thread-unsafe RunReport mutations
                                                        engine=engine,
                                                        existing_cols=existing_cols,
                                                        load_method=db_load_method,
                                                        fast_load_state=None,
                                                        local_infile_conn=conn,
                                                    )
                                                    res["loaded_any"] = True
                                                    res["load_ok"] += 1
                                                    res["load_data_ok"] += 1
                                                    try:
                                                        res["rows_loaded"] += int(fi.get("rows") or 0)
                                                    except Exception:
                                                        pass
                                                except Exception as e:
                                                    res["ok"] = False
                                                    res["load_failed"] += 1
                                                    res["errors"].append({"type": type(e).__name__, "message": str(e)})
                                                    if not continue_on_error:
                                                        break
                                                finally:
                                                    try:
                                                        os.remove(str(path))
                                                    except Exception:
                                                        pass
                                            return res

                                        pl = min(int(db_load_parallel_tables), int(len(load_groups)))
                                        t0 = time.perf_counter()
                                        with report.timer("json.db.load"):
                                            with ThreadPoolExecutor(
                                                max_workers=pl,
                                                thread_name_prefix="kisti_load",
                                                initializer=_thread_init,
                                            ) as ex:
                                                futs = [ex.submit(_load_group, g) for g in load_groups]
                                                for fut in as_completed(futs):
                                                    r = fut.result()
                                                    if not isinstance(r, dict):
                                                        continue
                                                    # Update existing column cache from the worker's view (best-effort).
                                                    if r.get("existing_cols") is not None:
                                                        try:
                                                            existing_cols_cache[str(r.get("table_sql") or "")] = set(r.get("existing_cols") or [])
                                                        except Exception:
                                                            pass
                                                    try:
                                                        report.bump("rows_loaded", int(r.get("rows_loaded") or 0))
                                                    except Exception:
                                                        pass
                                                    try:
                                                        report.bump("load_ok", int(r.get("load_ok") or 0))
                                                        report.bump("load_failed", int(r.get("load_failed") or 0))
                                                        report.bump("load_data_ok", int(r.get("load_data_ok") or 0))
                                                    except Exception:
                                                        pass
                                                    if r.get("loaded_any"):
                                                        report.bump("tables_loaded", 1)
                                                    if not r.get("ok"):
                                                        report.warn(
                                                            stage="json_pipeline.load.parallel_tables",
                                                            message="LOAD DATA failed for table in parallel loader",
                                                            table=str(r.get("table_sql") or ""),
                                                            errors=list(r.get("errors") or [])[:3],
                                                        )
                                                        if not continue_on_error:
                                                            raise RuntimeError(f"parallel_table_load_failed: {r.get('table_sql')}")
                                        dt = time.perf_counter() - t0
                                        # For parallel load, report wall time to keep share_pct meaningful.
                                        report.add_time_s("db.load_data.exec", dt)
                                        report.add_time_s("db.load_data.total", dt)
                                    finally:
                                        for c in conns:
                                            try:
                                                c.close()
                                            except Exception:
                                                pass
                                return
                            except _ParallelTSVFailed:
                                # Fall back to serial rows backend below.
                                pass
                            finally:
                                for wd in workdirs:
                                    try:
                                        shutil.rmtree(wd, ignore_errors=True)
                                    except Exception:
                                        pass

                        # Serial rows backend (rows -> TSV -> LOAD DATA) for maximal correctness/compatibility.
                        try:
                            with report.timer("json.flatten"):
                                rows_main, sub_rows_tot, excepted = extract_rows_from_jsons(
                                    batch_records,
                                    index_key=index_key,
                                    except_keys=except_keys,
                                    sep=key_sep,
                                    report=report,
                                    quarantine=q,
                                    index_offset=int(index_offset),
                                    record_contexts=record_contexts,
                                    parallel_workers=None,
                                )
                        except Exception as e:
                            report.exception(stage="json_pipeline.flatten", message="Failed to flatten JSON batch", exc=e)
                            q.write(stage="json_pipeline.flatten", record={"table_name": base_table}, exc=e)
                            if not continue_on_error:
                                raise
                            return

                        tables_rows: dict[str, list[dict]] = {}
                        if rows_main:
                            tables_rows[base_table] = rows_main
                        for sub_key, rows in (sub_rows_tot or {}).items():
                            if rows:
                                tables_rows[_table_for_sub(str(sub_key).replace(".", key_sep))] = rows
                        for ex_key, items in (excepted or {}).items():
                            if items:
                                tables_rows[_table_for_excepted(str(ex_key).replace(".", key_sep))] = list(items)

                        for table_original, rows in tables_rows.items():
                            if not rows:
                                continue

                            cols_non_null: set[str] = set()
                            for r in rows:
                                if not isinstance(r, dict):
                                    continue
                                for k, v in r.items():
                                    if k == index_key or not _is_nullish(v):
                                        cols_non_null.add(str(k))
                            cols_non_null.add(index_key)
                            cols = [index_key] + sorted([c for c in cols_non_null if c != index_key])
                            if use_extra_column and extra_column_name not in set(cols):
                                cols.append(extra_column_name)
                            if not cols:
                                continue

                            nm = ensure_name_map(table_original, cols)
                            if emit_ddl:
                                try:
                                    ddl, _nm2 = manage.generate_create_table_sql_from_columns(
                                        table_name=table_original,
                                        columns=list(nm.columns_original),
                                        name_map=nm,
                                        key_sep=key_sep,
                                        column_type=column_type,
                                    )
                                    ddl_by_table[table_original] = ddl
                                except Exception as e:
                                    report.warn(stage="json_pipeline", message="Failed to generate DDL artifact", exc=str(e))

                            if create and table_original not in created_tables:
                                nm_created = step(
                                    "create",
                                    manage.create_table_from_columns,
                                    dbc,
                                    table_name=table_original,
                                    columns=list(nm.columns_original),
                                    name_map=nm,
                                    key_sep=key_sep,
                                    column_type=column_type,
                                )
                                if isinstance(nm_created, NameMap):
                                    name_maps[table_original] = nm_created
                                    nm = nm_created
                                if nm_created is not None:
                                    created_tables.add(table_original)
                                    report.bump("tables_created", 1)
                                    existing_cols_cache[nm.table_sql] = set(nm.columns_sql)

                            if load:
                                load_res = step(
                                    "load",
                                    manage.fill_table_from_rows,
                                    rows,
                                    dbc,
                                    table_name=nm.table_sql,
                                    name_map=nm,
                                    extra_column_name=extra_column_name if use_extra_column else None,
                                    columns_original=cols,
                                    auto_alter_table=bool(auto_alter_table_effective),
                                    column_type=column_type,
                                    report=report,
                                    engine=engine,
                                    existing_cols=_get_existing_cols(nm.table_sql),
                                    load_method=db_load_method,
                                    fast_load_state=fast_load_state,
                                    local_infile_conn=local_infile_conn,
                                )
                                if load_res is not None:
                                    report.bump("tables_loaded", 1)
                                    try:
                                        report.bump("rows_loaded", int(len(rows)))
                                    except Exception:
                                        pass
                        return

                try:
                    extract_kwargs = {
                        "index_key": index_key,
                        "except_keys": except_keys,
                        "sep": key_sep,
                        "report": report,
                        "quarantine": q,
                    }
                    if "index_offset" in extract_params:
                        extract_kwargs["index_offset"] = int(index_offset)
                    if record_contexts is not None and "record_contexts" in extract_params:
                        extract_kwargs["record_contexts"] = list(record_contexts)
                    if parallel_workers and parallel_workers > 1 and "parallel_workers" in extract_params:
                        extract_kwargs["parallel_workers"] = int(parallel_workers)

                    with report.timer("json.flatten"):
                        df_main, df_subs, excepted = extract_fn(batch_records, **extract_kwargs)
                except Exception as e:
                    report.exception(stage="json_pipeline.flatten", message="Failed to flatten JSON batch", exc=e)
                    q.write(stage="json_pipeline.flatten", record={"table_name": base_table}, exc=e)
                    if not continue_on_error:
                        raise
                    return

                tables: dict[str, Any] = {}
                if df_main is not None:
                    tables[base_table] = df_main
                for sub_key, sub_df in (df_subs or {}).items():
                    tables[_table_for_sub(str(sub_key).replace(".", key_sep))] = sub_df

                if excepted:
                    try:
                        import pandas as pd
                    except Exception:
                        pd = None
                    if pd is not None:
                        for ex_key, items in excepted.items():
                            if not items:
                                continue
                            try:
                                tables[_table_for_excepted(str(ex_key).replace(".", key_sep))] = pd.DataFrame(items)
                            except Exception as e:
                                report.exception(
                                    stage="json_pipeline.excepted",
                                    message="Failed to build excepted DataFrame",
                                    exc=e,
                                    except_key=str(ex_key),
                                )

                for table_original, df in tables.items():
                    cols = list(getattr(df, "columns", []))
                    if not cols:
                        continue
                    if use_extra_column and extra_column_name not in set(cols):
                        cols = list(cols) + [extra_column_name]

                    nm = ensure_name_map(table_original, [str(c) for c in cols])
                    if emit_ddl:
                        try:
                            ddl, _nm2 = manage.generate_create_table_sql_from_columns(
                                table_name=table_original,
                                columns=list(nm.columns_original),
                                name_map=nm,
                                key_sep=key_sep,
                                column_type=column_type,
                            )
                            ddl_by_table[table_original] = ddl
                        except Exception as e:
                            report.warn(stage="json_pipeline", message="Failed to generate DDL artifact", exc=str(e))

                    if create and table_original not in created_tables:
                        nm_created = step(
                            "create",
                            manage.create_table_from_columns,
                            dbc,
                            table_name=table_original,
                            columns=list(nm.columns_original),
                            name_map=nm,
                            key_sep=key_sep,
                            column_type=column_type,
                        )
                        if isinstance(nm_created, NameMap):
                            name_maps[table_original] = nm_created
                            nm = nm_created
                        if nm_created is not None:
                            created_tables.add(table_original)
                            report.bump("tables_created", 1)
                            existing_cols_cache[nm.table_sql] = set(nm.columns_sql)

                    if load:
                        load_res = step(
                            "load",
                            manage.fill_table_from_dataframe,
                            df,
                            dbc,
                            table_name=nm.table_sql,
                            name_map=nm,
                            extra_column_name=extra_column_name if use_extra_column else None,
                            auto_alter_table=bool(auto_alter_table_effective),
                            column_type=column_type,
                            fallback_on_insert_error=fallback_on_insert_error,
                            fallback_column_type=fallback_column_type,
                            insert_retry_max=insert_retry_max,
                            report=report,
                            chunksize=to_sql_chunksize,
                            to_sql_method=to_sql_method,
                            sanitize_nan_strings=sanitize_nan_strings,
                            convert_nan_to_none=convert_nan_to_none,
                            engine=engine,
                            existing_cols=_get_existing_cols(nm.table_sql),
                            load_method=db_load_method,
                            fast_load_state=fast_load_state,
                            local_infile_conn=local_infile_conn,
                        )
                        if load_res is not None:
                            report.bump("tables_loaded", 1)
                            try:
                                report.bump("rows_loaded", int(len(df)))
                            except Exception:
                                pass

            batch_index_offset = 0
            for rec_out in _iter_json_records(dc, report=report, max_records=max_records, with_context=True):
                report.bump("records_read", 1)
                rec_ctx: dict[str, Any] = {}
                if isinstance(rec_out, tuple) and len(rec_out) == 2:
                    record, ctx = rec_out
                    if isinstance(ctx, Mapping):
                        rec_ctx = dict(ctx)
                else:
                    record = rec_out
                if not isinstance(record, dict):
                    report.warn(stage="json_pipeline", message="Non-dict JSON record encountered; skipping", dtype=type(record).__name__)
                    continue
                rec_ctx.setdefault("record_index", int(global_index))
                if not batch:
                    batch_index_offset = global_index
                batch.append(record)
                batch_contexts.append(rec_ctx)
                global_index += 1
                if len(batch) >= chunk_size:
                    flush_batch(batch, index_offset=batch_index_offset, record_contexts=batch_contexts)
                    batch = []
                    batch_contexts = []

            if batch:
                flush_batch(batch, index_offset=batch_index_offset, record_contexts=batch_contexts)

            # Post steps: indexes + optimize
            if index:
                for table_original, nm in name_maps.items():
                    if index_key not in nm.columns_original:
                        continue
                    step(
                        "index",
                        manage.set_index_simple,
                        dbc,
                        table_name=nm.table_sql,
                        column=index_key,
                        name_map=nm,
                        prefix_len=index_prefix_len,
                    )

            if optimize:
                for table_original, nm in name_maps.items():
                    step(
                        "optimize",
                        manage.optimize_table,
                        db_config=dbc,
                        data_config={"table_name": table_original},
                        name_map=nm,
                    )

        report.stats["tables_total"] = int(len(name_maps))
        try:
            # Basic throughput helpers (best-effort)
            tp: dict[str, Any] = {}

            parse_ms = int(report.timings_ms.get("io.json_parse", 0) or 0)
            bytes_read = int(report.stats.get("io_bytes_read", 0) or 0)
            records_read = int(report.stats.get("records_read", 0) or 0)
            records_ok = int(report.stats.get("records_ok", 0) or 0)
            rows_loaded = int(report.stats.get("rows_loaded", 0) or 0)

            def per_s(count: int, ms: int) -> float | None:
                if ms <= 0:
                    return None
                return float(count) / (float(ms) / 1000.0)

            if parse_ms > 0:
                tp["io.json_parse.records_per_s"] = per_s(records_read, parse_ms)
                tp["io.json_parse.mb_per_s"] = (float(bytes_read) / (1024.0 * 1024.0)) / (float(parse_ms) / 1000.0)

            flatten_ms = int(report.timings_ms.get("json.flatten", 0) or 0)
            if flatten_ms > 0:
                tp["json.flatten.records_per_s"] = per_s(records_ok or records_read, flatten_ms)

            load_ms = int(report.timings_ms.get("json.db.load", 0) or 0)
            if load_ms > 0:
                tp["json.db.load.rows_per_s"] = per_s(rows_loaded, load_ms)

            load_exec_ms = int(report.timings_ms.get("db.load_data.exec", 0) or 0)
            if load_exec_ms > 0:
                tp["db.load_data.exec.rows_per_s"] = per_s(rows_loaded, load_exec_ms)

            report.set_artifact("throughput", {k: v for k, v in tp.items() if v is not None})
        except Exception:
            pass
        maybe_update_artifacts()
        return JsonRunResult(name_maps=name_maps, report=report)
    finally:
        report.add_time_s("pipeline.json.total", time.perf_counter() - t_total0)
        if local_infile_conn is not None:
            try:
                local_infile_conn.close()
            except Exception:
                pass
