from __future__ import annotations

import re
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
        # Session-only toggles (safe defaults for ingest speed; automatically reset when connection closes).
        ("SET SESSION unique_checks=0", "unique_checks=0"),
        ("SET SESSION foreign_key_checks=0", "foreign_key_checks=0"),
        # Global-only in MariaDB/MySQL: keep durability trade-off explicit.
        ("SET GLOBAL innodb_flush_log_at_trx_commit=2", "GLOBAL innodb_flush_log_at_trx_commit=2"),
        # Disable binlog for this session (may require SUPER / SYSTEM_VARIABLES_ADMIN; often fails on managed DBs).
        ("SET SESSION sql_log_bin=0", "sql_log_bin=0"),
    ]

    applied: list[str] = []
    skipped: list[dict] = []

    try:
        with conn.cursor() as cur:
            for sql, label in settings:
                try:
                    cur.execute(sql)
                    applied.append(label)
                except Exception as e:
                    # Avoid spamming warnings for expected privilege/scope limitations; record as artifacts instead.
                    skipped.append({"setting": label, "error": str(e)})
    except Exception as e:
        skipped.append({"setting": "<cursor>", "error": str(e)})

    if report is not None:
        try:
            if applied:
                report.set_artifact("fast_load_session.applied", list(applied))
            if skipped:
                report.set_artifact("fast_load_session.skipped", list(skipped))
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


_POSSIBLE_LONG_POSITIVE_JSON_INTEGER_RE = re.compile(r"\d{20,}")
_POSSIBLE_LONG_NEGATIVE_JSON_INTEGER_RE = re.compile(r"-\d{19,}")
_POSSIBLE_LONG_POSITIVE_JSON_INTEGER_RE_BYTES = re.compile(rb"\d{20,}")
_POSSIBLE_LONG_NEGATIVE_JSON_INTEGER_RE_BYTES = re.compile(rb"-\d{19,}")
_JSON_DIGIT_BYTES = b"0123456789"
_LONG_JSON_INTEGER_RE = re.compile(
    r"(?:(?:^|[\[\{,:])\s*)(?:-\d{19,}|\d{20,})(?=\s*(?:[,}\]]|$)|[.eE])"
)
_LONG_JSON_INTEGER_RE_BYTES = re.compile(
    rb"(?:(?:^|[\[\{,:])\s*)(?:-\d{19,}|\d{20,})(?=\s*(?:[,}\]]|$)|[.eE])"
)
_RUST_ARROW_DETAIL_TIMING_KEYS = (
    "rust_arrow.read_line",
    "rust_arrow.number_validate",
    "rust_arrow.table_assemble",
    "rust_arrow.columnar_merge",
    "rust_arrow.id_compaction",
    "rust_arrow.table_write",
    "rust_arrow.arrow_build",
    "rust_arrow.parquet_write",
    "rust_arrow.py_result_convert",
)
_RUST_ARROW_UNACCOUNTED_DETAIL_KEYS = (
    "rust_arrow.read_line",
    "rust_arrow.json_parse",
    "rust_arrow.number_validate",
    "json.flatten",
    "rust_arrow.table_assemble",
    "rust_arrow.columnar_merge",
    "json.parquet.persist",
    "rust_arrow.arrow_build",
    "rust_arrow.parquet_write",
    "rust_arrow.py_result_convert",
)
_RUST_ARROW_ID_COMPACTION_DEFAULT_PARALLEL_WORKERS = 8


def _parallel_workers_was_explicit(data_config: Any) -> bool:
    try:
        marker = data_config.get("_parallel_workers_explicit")
    except Exception:
        marker = None
    if marker is not None:
        return bool(marker)
    try:
        return "parallel_workers" in data_config
    except Exception:
        return hasattr(data_config, "parallel_workers")


def _timing_ms(timings: Mapping[str, Any], key: str) -> int:
    try:
        return int(timings.get(key, 0) or 0)
    except Exception:
        return 0


def _rust_arrow_unaccounted_ms(timings: Mapping[str, Any]) -> int:
    total_ms = _timing_ms(timings, "rust_arrow.total")
    if total_ms <= 0:
        return 0
    measured_ms = sum(_timing_ms(timings, key) for key in _RUST_ARROW_UNACCOUNTED_DETAIL_KEYS)
    return max(0, int(total_ms) - int(measured_ms))


def _json_loads_factory():
    import json

    def _loads_stdlib(obj):
        if isinstance(obj, (bytes, bytearray, memoryview)):
            obj = bytes(obj).decode("utf-8")
        return json.loads(obj)

    def _maybe_has_integer_outside_u64(obj) -> bool:
        """
        orjson converts integer literals outside i64/u64 range to float. Keep the
        fast path for normal records, but use stdlib json when a suspicious numeric
        literal appears so Python's arbitrary-size int semantics are preserved.
        """
        if isinstance(obj, str):
            if _POSSIBLE_LONG_POSITIVE_JSON_INTEGER_RE.search(obj) is None and (
                "-" not in obj or _POSSIBLE_LONG_NEGATIVE_JSON_INTEGER_RE.search(obj) is None
            ):
                return False
            return _LONG_JSON_INTEGER_RE.search(obj) is not None
        elif isinstance(obj, (bytes, bytearray, memoryview)):
            raw = bytes(obj)
            digit_count = len(raw) - len(raw.translate(None, _JSON_DIGIT_BYTES))
            if digit_count < 19:
                return False
            if _POSSIBLE_LONG_POSITIVE_JSON_INTEGER_RE_BYTES.search(raw) is None and (
                b"-" not in raw or _POSSIBLE_LONG_NEGATIVE_JSON_INTEGER_RE_BYTES.search(raw) is None
            ):
                return False
            return _LONG_JSON_INTEGER_RE_BYTES.search(raw) is not None
        else:
            return False

    try:
        import orjson

        def loads(obj):
            if _maybe_has_integer_outside_u64(obj):
                return _loads_stdlib(obj)
            if isinstance(obj, str):
                obj = obj.encode("utf-8")
            return orjson.loads(obj)

        return loads
    except Exception:
        return _loads_stdlib


def _as_string_list(value: Any) -> list[str]:
    from pathlib import Path

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


def _coerce_bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, (int, float)):
        try:
            return int(value) != 0
        except Exception:
            return bool(default)
    s = str(value).strip().lower()
    if s in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "f", "no", "n", "off", ""}:
        return False
    return bool(default)


def _coerce_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _resolve_json_sources(
    data_config: Mapping[str, Any],
    *,
    report: RunReport | None = None,
    apply_sampling: bool = True,
) -> list[tuple[str, Any]]:
    import glob
    import random
    from pathlib import Path

    from .config import join_path

    dc = coerce_data_config(data_config)
    base_path = Path(str(dc.get("PATH", "") or ""))

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

    if apply_sampling:
        sample_randomize = _coerce_bool(
            dc.get("sample_randomize_sources", dc.get("sample_random_sources", False)),
            default=False,
        )
        sample_max_sources = _coerce_int(dc.get("sample_max_sources", dc.get("sample_source_limit", 0)), default=0)
        if sample_max_sources < 0:
            sample_max_sources = 0
        if sample_randomize:
            seed_raw = dc.get("sample_seed", dc.get("sample_random_seed"))
            seed = None
            if seed_raw not in (None, ""):
                try:
                    seed = int(seed_raw)
                except Exception:
                    seed = None
            rnd = random.Random(seed)
            rnd.shuffle(deduped_sources)
        if sample_max_sources > 0:
            deduped_sources = deduped_sources[:sample_max_sources]

    return list(deduped_sources)


def _iter_json_records(
    data_config: Mapping[str, Any],
    *,
    report: RunReport | None = None,
    quarantine: QuarantineWriter | NullQuarantineWriter | None = None,
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
    from pathlib import Path

    loads = _json_loads_factory()

    dc = coerce_data_config(data_config)
    resume_cursor_raw = dc.get("_resume_cursor")
    if resume_cursor_raw is None:
        resume_cursor_raw = dc.get("resume_cursor")
    resume_source_path: str | None = None
    resume_source_member: str | None = None
    resume_line_no: int | None = None
    resume_skip_until_line_no: int | None = None
    resume_idx: int | None = None
    try:
        resume_backtrack_lines = int(dc.get("resume_backtrack_lines", 0) or 0)
    except Exception:
        resume_backtrack_lines = 0
    if resume_backtrack_lines < 0:
        resume_backtrack_lines = 0
    if isinstance(resume_cursor_raw, Mapping):
        resume_source_path = str(resume_cursor_raw.get("source_path") or "").strip() or None
        resume_source_member = str(resume_cursor_raw.get("source_member") or "").strip() or None
        try:
            ln = resume_cursor_raw.get("line_no")
            resume_line_no = int(ln) if ln not in (None, "") else None
        except Exception:
            resume_line_no = None
    if resume_line_no is not None and resume_line_no >= 0:
        resume_skip_until_line_no = max(0, int(resume_line_no) - int(resume_backtrack_lines))

    configured_file_type = str(dc.get("file_type") or "").strip().lower()
    records_key = dc.get("records_key") or dc.get("json_records_key")
    json_member_value = dc.get("json_file_names")
    if json_member_value is None:
        json_member_value = dc.get("json_file_name")
    if json_member_value is None:
        json_member_value = dc.get("inner_file_name")

    source_infos = _resolve_json_sources(dc, report=report, apply_sampling=True)
    if resume_source_path:
        try:
            resume_abs = str(Path(resume_source_path).expanduser().resolve())
        except Exception:
            resume_abs = resume_source_path
        for i, (_origin, p) in enumerate(source_infos):
            try:
                if str(p.resolve()) == resume_abs:
                    resume_idx = int(i)
                    break
            except Exception:
                if str(p) == resume_abs:
                    resume_idx = int(i)
                    break
        if resume_idx is not None and resume_idx > 0:
            source_infos = source_infos[resume_idx:]
        if resume_idx is None:
            # Safety: if we can't find the resume file in the configured sources, ignore resume cursor.
            resume_skip_until_line_no = None

    max_records = int(max_records) if max_records is not None and int(max_records) > 0 else None
    yielded = 0

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

    def iter_jsonl_fileobj(
        f,
        *,
        source_label: str,
        source_member: str | None = None,
        skip_until_line_no: int | None = None,
    ):
        import time

        line_no = 0
        for raw in f:
            line_no += 1
            if max_records is not None and yielded >= max_records:
                return
            if skip_until_line_no is not None and int(skip_until_line_no) > 0 and int(line_no) <= int(skip_until_line_no):
                try:
                    _bump_bytes(len(raw))
                except Exception:
                    pass
                continue
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
                if quarantine is not None:
                    q_context: dict[str, Any] = {"source": source_label, "line_no": int(line_no)}
                    if source_member:
                        q_context["source_member"] = str(source_member)
                    try:
                        raw_text = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
                        quarantine.write(
                            stage="iter_json_records",
                            record=raw_text.rstrip("\r\n"),
                            index=int(line_no),
                            exc=e,
                            **q_context,
                        )
                    except Exception:
                        pass
                continue

            if not _can_yield_one():
                return
            context: dict[str, Any] = {"source_path": source_label, "line_no": int(line_no)}
            if source_member:
                context["source_member"] = str(source_member)
            yield _record_output(obj, context)

    def iter_one_source(
        path: Path,
        *,
        source_label: str,
        skip_until_line_no: int | None = None,
        skip_member: str | None = None,
    ):
        import time

        file_type = configured_file_type or path.suffix.lstrip(".").lower()

        if file_type in {"jsonl", "ndjson", "jsonlines"}:
            with open(path, "rb") as f:
                yield from iter_jsonl_fileobj(f, source_label=source_label, skip_until_line_no=skip_until_line_no)
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
                        yield from iter_jsonl_fileobj(f, source_label=source_label, skip_until_line_no=skip_until_line_no)
                    else:
                        yield from emit(obj, context={"source_path": source_label})
                else:
                    yield from iter_jsonl_fileobj(f, source_label=source_label, skip_until_line_no=skip_until_line_no)
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
                            member_skip = None
                            if skip_until_line_no is not None and skip_member and str(skip_member) == str(member):
                                member_skip = int(skip_until_line_no)
                            yield from iter_jsonl_fileobj(
                                fp,
                                source_label=source_label,
                                source_member=str(member),
                                skip_until_line_no=member_skip,
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
                                member_skip = None
                                if skip_until_line_no is not None and skip_member and str(skip_member) == str(member):
                                    member_skip = int(skip_until_line_no)
                                yield from iter_jsonl_fileobj(
                                    io.BytesIO(raw),
                                    source_label=source_label,
                                    source_member=str(member),
                                    skip_until_line_no=member_skip,
                                )
            return

        raise ValueError(f"Unsupported file_type={file_type!r} for JSON pipeline (source={path})")

    skip_next = None
    if resume_idx is not None and resume_skip_until_line_no is not None and int(resume_skip_until_line_no) > 0:
        skip_next = int(resume_skip_until_line_no)

    for _origin, _path in source_infos:
        if max_records is not None and yielded >= max_records:
            break
        yield from iter_one_source(
            _path,
            source_label=str(_path),
            skip_until_line_no=skip_next,
            skip_member=resume_source_member,
        )
        # Apply resume skipping only to the first matching source.
        skip_next = None


def _iter_dict_path_stats(
    record: Any,
    *,
    key_sep: str,
    stats: dict[str, dict[str, Any]],
    unique_cap: int,
) -> None:
    stack: list[tuple[Any, str]] = [(record, "")]
    while stack:
        cur, path = stack.pop()
        if type(cur) is dict or isinstance(cur, dict):
            if path:
                st = stats.get(path)
                if st is None:
                    st = {"observations": 0, "dict_keys_total": 0, "unique_keys": set()}
                    stats[path] = st
                st["observations"] = int(st.get("observations", 0)) + 1
                key_count = len(cur)
                st["dict_keys_total"] = int(st.get("dict_keys_total", 0)) + int(key_count)
                uniq: set[str] = st.get("unique_keys") or set()
                if len(uniq) < int(unique_cap):
                    for k in cur.keys():
                        uniq.add(str(k))
                        if len(uniq) >= int(unique_cap):
                            break
                    st["unique_keys"] = uniq

            for k, v in cur.items():
                ks = k if type(k) is str else str(k)
                child = f"{path}{key_sep}{ks}" if path else ks
                if type(v) is dict or isinstance(v, dict):
                    stack.append((v, child))
                elif type(v) is list or isinstance(v, list):
                    stack.append((v, child))
            continue

        if type(cur) is list or isinstance(cur, list):
            for it in cur:
                if type(it) is dict or isinstance(it, dict):
                    # list item dicts share the same logical path
                    stack.append((it, path))
                elif type(it) is list or isinstance(it, list):
                    stack.append((it, path))


def _auto_detect_except_keys(
    data_config: Mapping[str, Any],
    *,
    existing_except_keys: list[str] | None,
) -> tuple[list[str], dict[str, Any]]:
    import time

    dc = coerce_data_config(data_config)
    existing = [str(k).strip() for k in (existing_except_keys or []) if str(k).strip()]
    existing_set = set(existing)
    key_sep = str(dc.get("KEY_SEP", "__"))

    sample_records = _coerce_int(dc.get("auto_except_sample_records", 5000), default=5000)
    if sample_records < 1:
        sample_records = 1
    sample_max_sources = _coerce_int(dc.get("auto_except_sample_max_sources", 64), default=64)
    if sample_max_sources < 1:
        sample_max_sources = 1
    seed = _coerce_int(dc.get("auto_except_seed", 42), default=42)
    unique_threshold = _coerce_int(dc.get("auto_except_unique_key_threshold", 512), default=512)
    if unique_threshold < 2:
        unique_threshold = 2
    min_observations = _coerce_int(dc.get("auto_except_min_observations", 20), default=20)
    if min_observations < 1:
        min_observations = 1
    try:
        novelty_threshold = float(dc.get("auto_except_novelty_threshold", 2.0) or 2.0)
    except Exception:
        novelty_threshold = 2.0
    if novelty_threshold < 0.0:
        novelty_threshold = 0.0
    profile_topn = _coerce_int(dc.get("auto_except_profile_topn", 30), default=30)
    if profile_topn < 1:
        profile_topn = 1
    unique_cap = _coerce_int(dc.get("auto_except_unique_key_cap", 200000), default=200000)
    if unique_cap < 1024:
        unique_cap = 1024

    all_sources = _resolve_json_sources(dc, apply_sampling=False)
    total_source_count = int(len(all_sources))
    total_source_bytes = 0
    for _origin, path in all_sources:
        try:
            total_source_bytes += int(path.stat().st_size)
        except Exception:
            continue

    sample_dc = dict(dc)
    sample_dc["sample_randomize_sources"] = True
    sample_dc["sample_seed"] = int(seed)
    sample_dc["sample_max_sources"] = int(sample_max_sources)

    stats: dict[str, dict[str, Any]] = {}
    sampled_records = 0
    sampled_sources: set[str] = set()
    t0 = time.perf_counter()
    for out in _iter_json_records(sample_dc, report=None, max_records=sample_records, with_context=True):
        record, context = out
        if isinstance(context, Mapping):
            sp = context.get("source_path")
            if sp is not None:
                sampled_sources.add(str(sp))
        _iter_dict_path_stats(record, key_sep=key_sep, stats=stats, unique_cap=unique_cap)
        sampled_records += 1
    sample_duration_s = float(time.perf_counter() - t0)

    profile_rows: list[dict[str, Any]] = []
    detected: list[str] = []
    for path, st in stats.items():
        obs = int(st.get("observations", 0))
        uniq_keys = st.get("unique_keys") or set()
        unique_count = int(len(uniq_keys))
        avg_keys = float(st.get("dict_keys_total", 0)) / float(obs) if obs > 0 else 0.0
        novelty = float(unique_count) / float(obs) if obs > 0 else 0.0
        is_candidate = bool(
            obs >= min_observations and unique_count >= unique_threshold and novelty >= novelty_threshold
        )
        if is_candidate:
            if path not in existing_set and str(path).split(key_sep)[-1] not in existing_set:
                detected.append(path)
        profile_rows.append(
            {
                "path": str(path),
                "observations": int(obs),
                "unique_keys": int(unique_count),
                "avg_dict_keys": float(round(avg_keys, 3)),
                "novelty_ratio": float(round(novelty, 3)),
                "auto_except_candidate": bool(is_candidate),
            }
        )

    profile_rows.sort(key=lambda x: (int(x.get("unique_keys", 0)), float(x.get("novelty_ratio", 0.0))), reverse=True)

    effective = list(existing)
    seen_effective = set(existing)
    for k in sorted(set(detected)):
        if k not in seen_effective:
            effective.append(k)
            seen_effective.add(k)

    sampled_source_bytes = 0
    for sp in sampled_sources:
        try:
            from pathlib import Path

            sampled_source_bytes += int(Path(sp).stat().st_size)
        except Exception:
            continue

    eta_by_source_s = None
    eta_by_bytes_s = None
    if sample_duration_s > 0.0 and sampled_sources and total_source_count > 0:
        try:
            eta_by_source_s = float(sample_duration_s * (float(total_source_count) / float(len(sampled_sources))))
        except Exception:
            eta_by_source_s = None
    if sample_duration_s > 0.0 and sampled_source_bytes > 0 and total_source_bytes > 0:
        try:
            eta_by_bytes_s = float(sample_duration_s * (float(total_source_bytes) / float(sampled_source_bytes)))
        except Exception:
            eta_by_bytes_s = None

    eta_candidates = [x for x in [eta_by_source_s, eta_by_bytes_s] if isinstance(x, (int, float)) and x > 0]
    eta_range_s = None
    if eta_candidates:
        eta_range_s = [float(min(eta_candidates)), float(max(eta_candidates))]

    meta = {
        "enabled": True,
        "sample": {
            "records_requested": int(sample_records),
            "records_sampled": int(sampled_records),
            "max_sources_requested": int(sample_max_sources),
            "sources_sampled": int(len(sampled_sources)),
            "seed": int(seed),
            "duration_s": float(round(sample_duration_s, 6)),
        },
        "thresholds": {
            "unique_key_threshold": int(unique_threshold),
            "min_observations": int(min_observations),
            "novelty_threshold": float(novelty_threshold),
        },
        "input": {
            "total_sources": int(total_source_count),
            "total_source_bytes": int(total_source_bytes),
            "sampled_source_bytes": int(sampled_source_bytes),
        },
        "estimate": {
            "eta_seconds_by_source": eta_by_source_s,
            "eta_seconds_by_bytes": eta_by_bytes_s,
            "eta_seconds_range": eta_range_s,
        },
        "detected_except_keys": sorted(set(detected)),
        "except_keys_effective": list(effective),
        "dict_path_profile_top": list(profile_rows[: int(profile_topn)]),
    }
    return effective, meta


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
    parallel_workers_explicit = _parallel_workers_was_explicit(data_config)

    dc = coerce_data_config(data_config, inplace=isinstance(data_config, dict))
    dbc = coerce_db_config(db_config)

    base_table = str(dc.get("table_name") or "").strip()
    if not base_table:
        raise ValueError("data_config.table_name is required for JSON pipeline")

    key_sep = str(key_sep or dc.get("KEY_SEP", "__"))
    index_key = str(index_key or dc.get("index_key") or "id")
    except_keys_input = list(except_keys if except_keys is not None else (dc.get("except_keys") or []))
    except_keys = list(except_keys_input)
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
    rust_db_load = _coerce_bool(dc.get("rust_db_load", False), default=False)
    rust_db_load_batch_size = int(dc.get("rust_db_load_batch_size", dc.get("to_sql_chunksize") or 1000) or 1000)
    if rust_db_load_batch_size <= 0:
        rust_db_load_batch_size = 1000
    report.set_artifact("rust_db_load", bool(rust_db_load))
    rust_parallel_table_writes = _coerce_bool(dc.get("rust_parallel_table_writes", False), default=False)
    report.set_artifact("rust_parallel_table_writes", bool(rust_parallel_table_writes))
    rust_columnar_accumulator = _coerce_bool(dc.get("rust_columnar_accumulator", False), default=False)
    report.set_artifact("rust_columnar_accumulator", bool(rust_columnar_accumulator))
    rust_parquet_flush_records = int(dc.get("rust_parquet_flush_records", 0) or 0)
    if rust_parquet_flush_records < 0:
        rust_parquet_flush_records = 0
    report.set_artifact("rust_parquet_flush_records", int(rust_parquet_flush_records))
    persist_parquet_files = _coerce_bool(dc.get("persist_parquet_files", True), default=True)
    persist_parquet_dir = str(dc.get("persist_parquet_dir", "") or "").strip()
    if persist_parquet_files and not persist_parquet_dir:
        from pathlib import Path

        persist_parquet_dir = str(Path("runs") / f"{base_table}_{report.run_id}" / "parquet")
    report.set_artifact("persist_parquet_files", bool(persist_parquet_files))
    if persist_parquet_files:
        report.set_artifact("persist_parquet_dir", persist_parquet_dir)

    persist_tsv_files = _coerce_bool(dc.get("persist_tsv_files", False), default=False)
    persist_tsv_dir = str(dc.get("persist_tsv_dir", dc.get("save_local_dir", "")) or "").strip()
    if persist_tsv_files and not persist_tsv_dir:
        from pathlib import Path

        persist_tsv_dir = str(Path("runs") / f"{base_table}_{report.run_id}" / "tsv")
    report.set_artifact("persist_tsv_files", bool(persist_tsv_files))
    if persist_tsv_files:
        report.set_artifact("persist_tsv_dir", persist_tsv_dir)

    from .id_compaction import IdCompactor

    id_compactor = IdCompactor.from_config(dc, sep=key_sep, index_key=index_key)
    if id_compactor.enabled:
        if str(id_compactor.config.get("preset")) != "openalex":
            raise ValueError("id_compaction preset currently supported: openalex")
        if str(id_compactor.config.get("mode")) != "semantic_column_strip":
            raise ValueError("id_compaction mode currently supported: semantic_column_strip")
    report.set_artifact("id_compaction", id_compactor.summary())

    from .rust_arrow_backend import (
        BACKEND_AUTO,
        BACKEND_PYTHON,
        BACKEND_RUST_ARROW,
        RustArrowBackendUnavailable,
        load_parquet_files_to_mysql,
        normalize_flatten_backend,
        normalize_parser_backend,
        persist_json_batch_to_parquet,
        persist_json_lines_batch_to_parquet,
        persist_jsonl_sources_to_parquet,
        rust_arrow_unsupported_reason,
    )

    flatten_backend_requested = normalize_flatten_backend(dc.get("flatten_backend", BACKEND_AUTO))
    report.set_artifact("flatten_backend", flatten_backend_requested)
    report.set_artifact("flatten_backend_requested", flatten_backend_requested)
    rust_parser_backend = normalize_parser_backend(dc.get("rust_parser_backend", "serde-json"))
    report.set_artifact("rust_parser_backend", rust_parser_backend)
    rust_raw_jsonl_parse_requested = bool(
        _coerce_bool(dc.get("rust_raw_jsonl_parse", True), default=True)
        and flatten_backend_requested == BACKEND_RUST_ARROW
    )
    rust_raw_jsonl_file_parse_requested = _coerce_bool(dc.get("rust_raw_jsonl_file_parse", False), default=False)
    report.set_artifact("rust_raw_jsonl_parse_requested", bool(rust_raw_jsonl_parse_requested))
    report.set_artifact("rust_raw_jsonl_file_parse_requested", bool(rust_raw_jsonl_file_parse_requested))
    parallel_workers = dc.get("parallel_workers")
    try:
        parallel_workers = int(parallel_workers) if parallel_workers is not None else 0
    except Exception:
        parallel_workers = 0
    json_streaming_load = _coerce_bool(dc.get("json_streaming_load", False), default=False)
    parallel_workers_auto_default_source = None
    if (
        not parallel_workers_explicit
        and parallel_workers <= 0
        and flatten_backend_requested == BACKEND_RUST_ARROW
        and id_compactor.enabled
        and persist_parquet_files
        and not json_streaming_load
    ):
        parallel_workers = _RUST_ARROW_ID_COMPACTION_DEFAULT_PARALLEL_WORKERS
        dc["parallel_workers"] = int(parallel_workers)
        parallel_workers_auto_default_source = "rust_arrow_id_compaction"
    report.set_artifact("parallel_workers", int(parallel_workers or 0))
    report.set_artifact("parallel_workers_explicit", bool(parallel_workers_explicit))
    if parallel_workers_auto_default_source:
        report.set_artifact("parallel_workers_default_source", parallel_workers_auto_default_source)

    # Best-effort progress checkpointing (for crash recovery / quick shard detection).
    # This intentionally writes a tiny JSON snapshot periodically without waiting for the final report.
    progress_path = str(dc.get("progress_path") or "").strip()
    if progress_path:
        try:
            from pathlib import Path

            def _normalize_progress_path_no_resolve(value: str) -> Path:
                expanded = Path(str(value)).expanduser()
                path_abs = expanded if expanded.is_absolute() else Path.cwd() / expanded
                current = Path(path_abs.anchor)
                normalized_parts: list[str] = []
                parts = path_abs.parts[1:] if path_abs.anchor else path_abs.parts
                for part in parts:
                    if part in {"", "."}:
                        continue
                    if part == "..":
                        if normalized_parts:
                            normalized_parts.pop()
                        current = Path(path_abs.anchor).joinpath(*normalized_parts)
                        continue
                    current = current / part
                    if current.is_symlink():
                        raise RuntimeError(f"progress path contains a symlink component: {current}")
                    normalized_parts.append(part)
                return Path(path_abs.anchor).joinpath(*normalized_parts)

            progress_path = str(_normalize_progress_path_no_resolve(progress_path))
        except Exception as e:
            report.warn(
                stage="json_pipeline.progress",
                message="Progress checkpointing disabled because progress_path is unsafe",
                path=progress_path,
                error={"type": type(e).__name__, "message": str(e)},
            )
            progress_path = ""
    try:
        progress_interval_s = float(dc.get("progress_interval_s", 10.0) or 0.0)
    except Exception:
        progress_interval_s = 0.0
    if progress_interval_s < 0:
        progress_interval_s = 0.0
    last_progress_write_t = 0.0
    last_loaded_snapshot: dict[str, Any] | None = None
    if progress_path:
        # Preserve the last known "loaded" cursor across restarts until we produce a new one.
        try:
            from pathlib import Path

            import json as _json

            p = Path(progress_path)
            if p.exists():
                prev = _json.loads(p.read_text(encoding="utf-8") or "{}")
                loaded = prev.get("loaded")
                if isinstance(loaded, dict) and isinstance(loaded.get("cursor"), dict) and loaded.get("cursor", {}).get("source_path"):
                    last_loaded_snapshot = dict(loaded)
        except Exception:
            last_loaded_snapshot = None

    def _write_progress_snapshot(
        *,
        stage: str,
        ctx: Mapping[str, Any] | None = None,
        extra: Mapping[str, Any] | None = None,
        force: bool = False,
    ) -> None:
        nonlocal last_progress_write_t
        nonlocal last_loaded_snapshot
        if not progress_path:
            return

        now_s = time.time()
        if not force:
            if progress_interval_s <= 0:
                return
            if (now_s - float(last_progress_write_t)) < float(progress_interval_s):
                return
        last_progress_write_t = float(now_s)

        try:
            import os
            from datetime import datetime, timezone
            from pathlib import Path

            payload: dict[str, Any] = {
                "run_id": getattr(report, "run_id", None),
                "table": base_table,
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "stage": str(stage),
                "pid": int(os.getpid()),
                "cursor": {
                    "source_path": None if ctx is None else ctx.get("source_path"),
                    "source_member": None if ctx is None else ctx.get("source_member"),
                    "line_no": None if ctx is None else ctx.get("line_no"),
                    "record_index": None if ctx is None else ctx.get("record_index"),
                },
                "stats": {
                    "records_read": int(report.stats.get("records_read", 0)),
                    "records_ok": int(report.stats.get("records_ok", 0)),
                    "records_failed": int(report.stats.get("records_failed", 0)),
                    "batches_total": int(report.stats.get("batches_total", 0)),
                    "rows_loaded": int(report.stats.get("rows_loaded", 0)),
                    "tables_loaded": int(report.stats.get("tables_loaded", 0)),
                    "parquet_batches_total": int(report.stats.get("parquet_batches_total", 0)),
                    "parquet_files_persisted": int(report.stats.get("parquet_files_persisted", 0)),
                    "parquet_rows_emitted": int(report.stats.get("parquet_rows_emitted", 0)),
                },
                "timings_ms": {
                    "json.flatten": int(report.timings_ms.get("json.flatten", 0) or 0),
                    "json.parquet.persist": int(report.timings_ms.get("json.parquet.persist", 0) or 0),
                    "json.db.load": int(report.timings_ms.get("json.db.load", 0) or 0),
                },
            }
            if extra:
                payload["extra"] = dict(extra)

            # Track the last fully-loaded cursor separately so crash recovery can skip within a shard.
            if str(stage) == "loaded" and (payload.get("cursor") or {}).get("source_path"):
                last_loaded_snapshot = {
                    "updated_at_utc": payload.get("updated_at_utc"),
                    "cursor": dict(payload.get("cursor") or {}),
                    "stats": dict(payload.get("stats") or {}),
                }
                if "extra" in payload:
                    try:
                        last_loaded_snapshot["extra"] = dict(payload.get("extra") or {})
                    except Exception:
                        pass
            if last_loaded_snapshot:
                payload["loaded"] = last_loaded_snapshot

            import tempfile

            out = _normalize_progress_path_no_resolve(progress_path)
            parent = _normalize_progress_path_no_resolve(str(out.parent))
            parent.mkdir(parents=True, exist_ok=True)
            parent = _normalize_progress_path_no_resolve(str(parent))
            if parent.is_symlink() or not parent.is_dir():
                raise RuntimeError(f"progress path parent is not a safe directory: {parent}")
            out = _normalize_progress_path_no_resolve(str(out))
            fd, tmp = tempfile.mkstemp(prefix=f".{out.name}.", suffix=".tmp", dir=str(parent))
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                import json as _json

                f.write(_json.dumps(payload, ensure_ascii=False, indent=2))
            try:
                out = _normalize_progress_path_no_resolve(str(out))
                os.replace(tmp, out)
            except Exception:
                try:
                    tmp_path = _normalize_progress_path_no_resolve(str(tmp))
                    if not tmp_path.is_symlink() and tmp_path.is_file():
                        tmp_path.unlink()
                except Exception:
                    pass
                raise
        except Exception:
            # Never fail the ingest because checkpointing failed.
            return

    custom_extract_fn = extract_fn is not None
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

    auto_except_enabled = _coerce_bool(dc.get("auto_except", False), default=False)
    auto_except_meta: dict[str, Any] | None = None
    if auto_except_enabled:
        try:
            with report.timer("json.auto_except.preflight"):
                except_keys, auto_except_meta = _auto_detect_except_keys(
                    dc,
                    existing_except_keys=list(except_keys),
                )
        except Exception as e:
            report.warn(
                stage="json.auto_except",
                message="Auto-except preflight failed; falling back to configured except_keys",
                error={"type": type(e).__name__, "message": str(e)},
            )
            except_keys = list(except_keys_input)
            auto_except_meta = {
                "enabled": True,
                "failed": True,
                "error": {"type": type(e).__name__, "message": str(e)},
                "detected_except_keys": [],
                "except_keys_effective": list(except_keys),
            }
    else:
        auto_except_meta = {"enabled": False, "detected_except_keys": [], "except_keys_effective": list(except_keys)}

    report.bump("runs_json", 1)
    report.bump("tables_total", 1)
    report.set_artifact("index_key", index_key)
    report.set_artifact("chunk_size", chunk_size)
    report.set_artifact("except_keys_input", list(except_keys_input))
    report.set_artifact("except_keys", list(except_keys))
    report.set_artifact("auto_except", dict(auto_except_meta or {}))
    report.set_artifact("max_records", max_records)

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
        report.set_artifact("id_compaction", id_compactor.summary())
        if emit_ddl:
            report.set_artifact("create_table_sql_json", dict(ddl_by_table))

    def _column_descriptions_for(table_original: str) -> dict[str, str]:
        return id_compactor.column_descriptions(table_original) if id_compactor.enabled else {}

    parquet_manifest_table_columns: dict[str, list[str]] = {}

    def _record_parquet_manifest_columns(table_original: Any, columns: Any) -> None:
        table = str(table_original or "")
        if not table:
            return
        out = parquet_manifest_table_columns.setdefault(table, [])
        seen = set(out)
        for column in list(columns or []):
            col = str(column or "")
            if not col or col in seen:
                continue
            out.append(col)
            seen.add(col)

    def _json_absolute_no_resolve(path: Any):
        from pathlib import Path

        expanded = Path(str(path)).expanduser()
        if expanded.is_absolute():
            return expanded
        return Path.cwd() / expanded

    def _json_assert_no_symlink_components(path: Any, *, purpose: str):
        from pathlib import Path

        path_abs = _json_absolute_no_resolve(path)
        current = Path(path_abs.anchor)
        normalized_parts: list[str] = []
        parts = path_abs.parts[1:] if path_abs.anchor else path_abs.parts
        for part in parts:
            if part in {"", "."}:
                continue
            if part == "..":
                if normalized_parts:
                    normalized_parts.pop()
                current = Path(path_abs.anchor).joinpath(*normalized_parts)
                continue
            current = current / part
            if current.is_symlink():
                raise RuntimeError(f"{purpose} path contains a symlink component: {current}")
            normalized_parts.append(part)
        return Path(path_abs.anchor).joinpath(*normalized_parts)

    def _write_schema_manifest() -> None:
        if not persist_parquet_files or int(report.stats.get("parquet_files_persisted", 0) or 0) <= 0:
            return
        try:
            import json as _json
            import os
            import tempfile

            root = _json_assert_no_symlink_components(persist_parquet_dir, purpose="schema manifest root")
            root.mkdir(parents=True, exist_ok=True)
            root = _json_assert_no_symlink_components(root, purpose="schema manifest root")
            if root.is_symlink() or not root.is_dir():
                raise RuntimeError(f"schema manifest root is not a safe directory: {root}")
            path = root / "schema_manifest.json"
            _json_assert_no_symlink_components(path, purpose="schema manifest")
            try:
                path.lstat()
            except FileNotFoundError:
                pass
            else:
                if path.is_symlink() or path.is_dir():
                    raise RuntimeError(f"schema manifest path already exists and is not a safe file: {path}")
            _scan_parquet_manifest_columns(root)
            payload = id_compactor.schema_manifest(
                name_maps=name_maps,
                table_columns=parquet_manifest_table_columns,
            )
            fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(root))
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    handle.write(_json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
                _json_assert_no_symlink_components(path, purpose="schema manifest")
                try:
                    path.lstat()
                except FileNotFoundError:
                    pass
                else:
                    if path.is_symlink() or path.is_dir():
                        raise RuntimeError(f"schema manifest path already exists and is not a safe file: {path}")
                os.replace(tmp_name, path)
            except Exception:
                try:
                    tmp_path = _json_assert_no_symlink_components(tmp_name, purpose="schema manifest temporary file")
                    if not tmp_path.is_symlink() and tmp_path.is_file():
                        tmp_path.unlink()
                except Exception:
                    pass
                raise
            report.set_artifact("schema_manifest", str(path))
        except Exception as e:
            report.warn(
                stage="id_compaction.schema_manifest",
                message="Failed to write parquet schema manifest",
                error={"type": type(e).__name__, "message": str(e)},
            )

    def _scan_parquet_manifest_columns(root: Any) -> None:
        try:
            import pyarrow.parquet as _pq
        except Exception:
            return

        try:
            children = sorted(root.iterdir())
        except Exception:
            return

        for table_dir in children:
            try:
                if table_dir.is_symlink() or not table_dir.is_dir():
                    continue
            except Exception:
                continue
            table = str(table_dir.name)
            for path in sorted(table_dir.glob("*.parquet")):
                try:
                    if path.is_symlink() or not path.is_file():
                        continue
                    pf = _pq.ParquetFile(path)
                    _record_parquet_manifest_columns(table, list(pf.schema_arrow.names))
                except Exception:
                    continue

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

    # Lazily created per-run executors (to amortize pool/connection startup across batches).
    flatten_executor = None
    parallel_tsv_disabled = False
    load_executor = None
    load_tls = None
    load_conns: list[Any] = []
    load_conns_lock = None
    load_parallel_disabled = False

    try:
        with quarantine_cm as q:
            batch: list[dict] = []
            batch_contexts: list[dict[str, Any]] = []

            import inspect

            try:
                extract_sig = inspect.signature(extract_fn)
                extract_params = set(extract_sig.parameters.keys())
                extract_accepts_kwargs = any(
                    p.kind == inspect.Parameter.VAR_KEYWORD for p in extract_sig.parameters.values()
                )
            except Exception:
                extract_params = set()
                extract_accepts_kwargs = False

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
            if persist_tsv_files and db_load_parallel_tables > 0:
                db_load_parallel_tables = 0
                report.warn(
                    stage="json_pipeline.tsv_persist",
                    message="persist_tsv_files=true disables parallel table loading; using serial table load",
                )
            try:
                report.set_artifact("db_load_parallel_tables", int(db_load_parallel_tables))
            except Exception:
                pass

            def _coerce_bool_local(v) -> bool:
                if isinstance(v, bool):
                    return bool(v)
                if v is None:
                    return False
                if isinstance(v, (int, float)):
                    try:
                        return int(v) != 0
                    except Exception:
                        return False
                s = str(v).strip().lower()
                if s in {"1", "true", "t", "yes", "y", "on"}:
                    return True
                if s in {"0", "false", "f", "no", "n", "off", ""}:
                    return False
                return bool(s)

            overlap_batches = _coerce_bool_local(
                dc.get("overlap_batches", dc.get("pipeline_overlap_batches", dc.get("pipeline_overlap", False)))
            )
            if persist_tsv_files and overlap_batches:
                overlap_batches = False
                report.warn(
                    stage="json_pipeline.tsv_persist",
                    message="persist_tsv_files=true disables overlapped batch loading; using non-overlap mode",
                )
            try:
                report.set_artifact("overlap_batches", bool(overlap_batches))
            except Exception:
                pass

            # excepted branch handling:
            # - False (default): keep raw payload under `value` + metadata (prevents column explosion)
            # - True: expand dict keys in excepted rows (legacy behavior)
            excepted_expand_dict = _coerce_bool_local(dc.get("excepted_expand_dict", dc.get("except_expand_dict", False)))
            try:
                report.set_artifact("excepted_expand_dict", bool(excepted_expand_dict))
            except Exception:
                pass

            load_data_commit_strategy = str(
                dc.get(
                    "load_data_commit_strategy",
                    dc.get("db_load_commit_strategy", dc.get("load_data_commit", "file")),
                )
            ).strip().lower()
            if load_data_commit_strategy not in {"file", "table", "batch"}:
                load_data_commit_strategy = "file"
            # 'batch' commit only makes sense for serial (single-connection) loading. If we
            # overlap batches or parallelize loads across tables (multiple connections), we
            # degrade 'batch' to 'table' (per-connection transactional bundling).
            load_data_commit_strategy_effective = load_data_commit_strategy
            if load_data_commit_strategy == "batch" and (bool(overlap_batches) or int(db_load_parallel_tables or 0) > 1):
                load_data_commit_strategy_effective = "table"
            try:
                report.set_artifact("load_data_commit_strategy", str(load_data_commit_strategy))
                if load_data_commit_strategy_effective != load_data_commit_strategy:
                    report.set_artifact("load_data_commit_strategy_effective", str(load_data_commit_strategy_effective))
            except Exception:
                pass

            tsv_merge_union_schema = _coerce_bool_local(
                dc.get("tsv_merge_union_schema", dc.get("tsv_union_merge", dc.get("load_data_union_merge", False)))
            )
            try:
                report.set_artifact("tsv_merge_union_schema", bool(tsv_merge_union_schema))
            except Exception:
                pass
            try:
                tsv_union_merge_min_coverage = float(dc.get("tsv_union_merge_min_coverage", 0.8) or 0.8)
            except Exception:
                tsv_union_merge_min_coverage = 0.8
            if tsv_union_merge_min_coverage < 0.0:
                tsv_union_merge_min_coverage = 0.0
            if tsv_union_merge_min_coverage > 1.0:
                tsv_union_merge_min_coverage = 1.0
            try:
                tsv_union_merge_max_union_cols = int(dc.get("tsv_union_merge_max_union_cols", 256) or 256)
            except Exception:
                tsv_union_merge_max_union_cols = 256
            if tsv_union_merge_max_union_cols < 0:
                tsv_union_merge_max_union_cols = 0
            try:
                tsv_union_merge_max_missing_cols = int(dc.get("tsv_union_merge_max_missing_cols", 32) or 32)
            except Exception:
                tsv_union_merge_max_missing_cols = 32
            if tsv_union_merge_max_missing_cols < 0:
                tsv_union_merge_max_missing_cols = 0

            json_streaming_load = bool(dc.get("json_streaming_load", False))
            if persist_parquet_files and json_streaming_load:
                json_streaming_load = False
                report.warn(
                    stage="json_pipeline.parquet_persist",
                    message="persist_parquet_files=true disables streaming LOAD DATA; using DataFrame parquet-first path",
                )
            export_tsv_only = bool(persist_tsv_files) and (not bool(load))
            use_streaming_rows = (
                bool(json_streaming_load)
                and (
                    (
                        bool(load)
                        and str(db_load_method or "").strip().lower() in {"auto", "load_data"}
                        and local_infile_conn is not None
                    )
                    or bool(export_tsv_only)
                )
            )

            global_index = 0
            batch_no = 0
            rust_auto_disabled_reason: str | None = None
            rust_auto_success_batches = 0
            rust_auto_fallback_batches = 0
            rust_explicit_failed = False
            hybrid_freeze_started = False
            # Frozen schema support (schema_mode=freeze or hybrid frozen):
            # table_original -> allowed canonical columns (unknowns should be packed into extra).
            frozen_allowed_cols_by_table_original: dict[str, set[str]] = {}
            pending_load_futures: list[Any] = []
            pending_load_workdirs: list[str] = []
            pending_load_tsv_files: list[str] = []
            pending_load_submitted_at: float | None = None
            pending_load_checkpoint_ctx: dict[str, Any] | None = None
            pending_load_checkpoint_extra: dict[str, Any] | None = None

            def _cleanup_workdirs(workdirs: list[str]) -> None:
                if not workdirs:
                    return
                import shutil
                from pathlib import Path

                if not getattr(shutil.rmtree, "avoids_symlink_attacks", False):
                    report.warn(
                        stage="json_pipeline.flatten.parallel_tsv.cleanup",
                        message="Skipping TSV worker directory cleanup because shutil.rmtree is not symlink-attack resistant",
                    )
                    return

                for wd in workdirs:
                    try:
                        path = Path(str(wd))
                        if not path.is_absolute() or not path.name.startswith("kisti_flatten_"):
                            report.warn(
                                stage="json_pipeline.flatten.parallel_tsv.cleanup",
                                message="Skipping unsafe TSV worker cleanup path",
                                path=str(wd),
                            )
                            continue
                        try:
                            path.lstat()
                        except FileNotFoundError:
                            continue
                        if path.is_symlink() or not path.is_dir():
                            report.warn(
                                stage="json_pipeline.flatten.parallel_tsv.cleanup",
                                message="Skipping non-directory TSV worker cleanup path",
                                path=str(path),
                            )
                            continue
                        if bool(persist_tsv_files):
                            try:
                                has_unpersisted_tsv = any(
                                    child.is_file() and not child.is_symlink() and child.suffix == ".tsv"
                                    for child in path.iterdir()
                                )
                            except Exception:
                                has_unpersisted_tsv = True
                            if has_unpersisted_tsv:
                                report.warn(
                                    stage="json_pipeline.flatten.parallel_tsv.cleanup",
                                    message="Leaving TSV worker directory in place because it still contains unpersisted TSV files",
                                    path=str(path),
                                )
                                continue
                        shutil.rmtree(str(path), ignore_errors=True)
                    except Exception as e:
                        try:
                            report.warn(
                                stage="json_pipeline.flatten.parallel_tsv.cleanup",
                                message="Failed to clean TSV worker directory",
                                path=str(wd),
                                error={"type": type(e).__name__, "message": str(e)},
                            )
                        except Exception:
                            pass

            def _cleanup_parent_tsv_files(paths: list[str]) -> None:
                if not paths or bool(persist_tsv_files):
                    return
                from pathlib import Path

                for raw in paths:
                    try:
                        path = Path(str(raw))
                        if not path.is_absolute() or not path.name.startswith(("kisti_merge_", "kisti_union_")):
                            report.warn(
                                stage="json_pipeline.flatten.parallel_tsv.cleanup",
                                message="Skipping unsafe parent TSV cleanup path",
                                path=str(raw),
                            )
                            continue
                        try:
                            path.lstat()
                        except FileNotFoundError:
                            continue
                        if path.is_symlink() or not path.is_file():
                            report.warn(
                                stage="json_pipeline.flatten.parallel_tsv.cleanup",
                                message="Skipping non-file parent TSV cleanup path",
                                path=str(path),
                            )
                            continue
                        path.unlink()
                    except Exception as e:
                        try:
                            report.warn(
                                stage="json_pipeline.flatten.parallel_tsv.cleanup",
                                message="Failed to clean parent TSV file",
                                path=str(raw),
                                error={"type": type(e).__name__, "message": str(e)},
                            )
                        except Exception:
                            pass

            def _drain_pending_loads() -> None:
                """
                Wait for any in-flight LOAD DATA futures and aggregate results into the report.

                When overlap_batches is enabled, we submit per-table LOAD DATA futures and
                let the main thread continue flattening the next batch. Before any DB DDL/load
                work in the next batch, we drain these futures so schema/cache stay consistent.
                """
                nonlocal pending_load_futures, pending_load_workdirs, pending_load_tsv_files, pending_load_submitted_at
                nonlocal pending_load_checkpoint_ctx, pending_load_checkpoint_extra
                if not pending_load_futures:
                    pending_load_checkpoint_ctx = None
                    pending_load_checkpoint_extra = None
                    # Best-effort cleanup if a caller attached workdirs without futures.
                    if pending_load_workdirs:
                        _cleanup_workdirs(pending_load_workdirs)
                        pending_load_workdirs = []
                    if pending_load_tsv_files:
                        _cleanup_parent_tsv_files(pending_load_tsv_files)
                        pending_load_tsv_files = []
                    pending_load_submitted_at = None
                    return

                import time
                from concurrent.futures import as_completed

                futures = list(pending_load_futures)
                workdirs = list(pending_load_workdirs)
                tsv_files = list(pending_load_tsv_files)
                submitted_at = pending_load_submitted_at
                checkpoint_ctx = pending_load_checkpoint_ctx
                checkpoint_extra = pending_load_checkpoint_extra

                pending_load_futures = []
                pending_load_workdirs = []
                pending_load_tsv_files = []
                pending_load_submitted_at = None
                pending_load_checkpoint_ctx = None
                pending_load_checkpoint_extra = None

                t0 = time.perf_counter()
                first_failed_table: str | None = None
                try:
                    with report.timer("json.db.load"):
                        for fut in as_completed(futures):
                            try:
                                r = fut.result()
                            except Exception as e:
                                r = {
                                    "table_sql": "",
                                    "ok": False,
                                    "loaded_any": False,
                                    "rows_loaded": 0,
                                    "load_ok": 0,
                                    "load_failed": 1,
                                    "load_data_ok": 0,
                                    "existing_cols": None,
                                    "errors": [{"type": type(e).__name__, "message": str(e)}],
                                }

                            if not isinstance(r, dict):
                                continue

                            table_sql = str(r.get("table_sql") or "")
                            if table_sql and r.get("existing_cols") is not None:
                                try:
                                    existing_cols_cache[table_sql] = set(r.get("existing_cols") or [])
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
                                try:
                                    report.bump("tables_loaded", 1)
                                except Exception:
                                    pass

                            if not r.get("ok"):
                                try:
                                    report.warn(
                                        stage="json_pipeline.load.overlap",
                                        message="LOAD DATA failed for table in overlapped loader",
                                        table=table_sql,
                                        errors=list(r.get("errors") or [])[:3],
                                    )
                                except Exception:
                                    pass
                                if not continue_on_error and first_failed_table is None:
                                    first_failed_table = table_sql
                        if first_failed_table is not None:
                            raise RuntimeError(f"overlapped_table_load_failed: {first_failed_table}")
                finally:
                    t1 = time.perf_counter()
                    wait_dt = t1 - t0
                    # For overlapped loads, record the *stall* time (critical path), not worker sum.
                    report.add_time_s("db.load_data.exec", wait_dt)
                    report.add_time_s("db.load_data.total", wait_dt)
                    if submitted_at is not None:
                        wall_dt = t1 - float(submitted_at)
                        report.add_time_s("db.load_data.exec.wall", wall_dt)
                        report.add_time_s("db.load_data.total.wall", wall_dt)

                    _cleanup_workdirs(workdirs)
                    _cleanup_parent_tsv_files(tsv_files)
                    if checkpoint_ctx:
                        _write_progress_snapshot(stage="loaded", ctx=checkpoint_ctx, extra=checkpoint_extra, force=True)

            def flush_batch(
                batch_records: list[dict],
                *,
                index_offset: int,
                record_contexts: list[dict[str, Any]] | None = None,
                raw_json_lines: list[bytes | str] | None = None,
            ) -> None:
                batch_record_count = len(raw_json_lines) if raw_json_lines is not None else len(batch_records)
                if batch_record_count <= 0:
                    return

                import time

                nonlocal batch_no, hybrid_freeze_started
                nonlocal flatten_executor, parallel_tsv_disabled
                nonlocal load_executor, load_tls, load_conns, load_conns_lock, load_parallel_disabled
                nonlocal pending_load_futures, pending_load_workdirs, pending_load_tsv_files, pending_load_submitted_at
                nonlocal pending_load_checkpoint_ctx, pending_load_checkpoint_extra
                nonlocal frozen_allowed_cols_by_table_original
                nonlocal rust_auto_disabled_reason, rust_auto_success_batches, rust_auto_fallback_batches, rust_explicit_failed
                batch_idx = int(batch_no)
                batch_no += 1
                if record_contexts:
                    _write_progress_snapshot(
                        stage="flush_batch",
                        ctx=record_contexts[-1],
                        extra={
                            "batch_idx": int(batch_idx),
                            "index_offset": int(index_offset),
                            "batch_records": int(batch_record_count),
                            "raw_jsonl": raw_json_lines is not None,
                        },
                        force=True,
                    )
                if flatten_backend_requested == BACKEND_RUST_ARROW and rust_explicit_failed:
                    report.warn(
                        stage="json_pipeline.rust_arrow",
                        message="Skipping batch after earlier explicit Rust Arrow backend failure",
                        batch_idx=int(batch_idx),
                    )
                    return

                def _batch_progress_extra(
                    *,
                    mode: str,
                    parquet: Mapping[str, Any] | None = None,
                ) -> dict[str, Any]:
                    payload: dict[str, Any] = {
                        "batch_idx": int(batch_idx),
                        "index_offset": int(index_offset),
                        "batch_records": int(batch_record_count),
                        "mode": str(mode),
                    }
                    if parquet:
                        payload["parquet"] = dict(parquet)
                    return payload

                parquet_progress: dict[str, Any] | None = None

                from pathlib import Path

                def _slug(value: str, *, max_len: int = 96) -> str:
                    s = "".join(ch if (ch.isalnum() or ch in {"_", "-", "."}) else "_" for ch in str(value or ""))
                    s = s.strip("._")
                    if not s:
                        s = "unknown"
                    return s[:max_len]

                def _absolute_no_resolve(path: Any) -> Path:
                    expanded = Path(str(path)).expanduser()
                    if expanded.is_absolute():
                        return expanded
                    return Path.cwd() / expanded

                def _assert_no_symlink_components(path: Any, *, purpose: str) -> Path:
                    path_abs = _absolute_no_resolve(path)
                    current = Path(path_abs.anchor)
                    normalized_parts: list[str] = []
                    parts = path_abs.parts[1:] if path_abs.anchor else path_abs.parts
                    for part in parts:
                        if part in {"", "."}:
                            continue
                        if part == "..":
                            if normalized_parts:
                                normalized_parts.pop()
                            current = Path(path_abs.anchor).joinpath(*normalized_parts)
                            continue
                        current = current / part
                        try:
                            is_link = current.is_symlink()
                        except OSError as exc:
                            raise RuntimeError(f"failed to inspect {purpose} path component: {current}") from exc
                        if is_link:
                            raise RuntimeError(f"{purpose} path contains a symlink component: {current}")
                        normalized_parts.append(part)
                    return Path(path_abs.anchor).joinpath(*normalized_parts)

                def _is_relative_to(path: Path, parent: Path) -> bool:
                    try:
                        path.relative_to(parent)
                        return True
                    except ValueError:
                        return False

                def _prepare_safe_artifact_dir(root_value: Any, child_name: str, *, purpose: str) -> Path:
                    root = _assert_no_symlink_components(root_value, purpose=f"{purpose} root")
                    root.mkdir(parents=True, exist_ok=True)
                    root = _assert_no_symlink_components(root, purpose=f"{purpose} root")
                    if root.is_symlink() or not root.is_dir():
                        raise RuntimeError(f"{purpose} root is not a safe directory: {root}")

                    child = root / child_name
                    _assert_no_symlink_components(child, purpose=purpose)
                    child.mkdir(parents=True, exist_ok=True)
                    child = _assert_no_symlink_components(child, purpose=purpose)
                    if child.is_symlink() or not child.is_dir():
                        raise RuntimeError(f"{purpose} path is not a safe directory: {child}")

                    root_resolved = root.resolve(strict=True)
                    child_resolved = child.resolve(strict=True)
                    if not _is_relative_to(child_resolved, root_resolved):
                        raise RuntimeError(f"{purpose} path resolves outside root: {child}")
                    return child_resolved

                def _path_occupied(path: Path) -> bool:
                    try:
                        path.lstat()
                        return True
                    except FileNotFoundError:
                        return False

                def _allocate_artifact_path(table_dir: Path, *, base: str, ext: str) -> Path:
                    for i in range(100_000):
                        suffix = "" if i == 0 else f"_{i}"
                        candidate = table_dir / f"{base}{suffix}{ext}"
                        if not _path_occupied(candidate):
                            return candidate
                    raise RuntimeError(f"failed to allocate artifact path: {table_dir / (base + ext)}")

                safe_tsv_workdirs: set[Path] = set()
                safe_tsv_files: set[Path] = set()
                finalized_tsv_source_paths: set[str] = set()

                def _safe_worker_tsv_source(path_value: Any) -> Path:
                    path_text = str(path_value or "").strip()
                    if not path_text:
                        raise RuntimeError("empty TSV worker file path")
                    src = _absolute_no_resolve(path_text)
                    try:
                        src.lstat()
                    except FileNotFoundError as exc:
                        raise RuntimeError(f"TSV worker file does not exist: {src}") from exc
                    if src.is_symlink() or not src.is_file():
                        raise RuntimeError(f"TSV worker file is not a safe regular file: {src}")
                    src_resolved = src.resolve(strict=True)
                    if src_resolved in safe_tsv_files:
                        return src_resolved
                    if not safe_tsv_workdirs:
                        raise RuntimeError("no safe TSV worker directory registered")
                    if not any(_is_relative_to(src_resolved, wd) for wd in safe_tsv_workdirs):
                        raise RuntimeError(f"TSV worker file resolves outside registered workdirs: {src}")
                    return src_resolved

                def _finalize_tsv_file(fi: dict | None, *, table_original: str, phase: str) -> None:
                    import shutil

                    if not isinstance(fi, dict):
                        return
                    if bool(fi.get("_tsv_finalized")):
                        return
                    path = str(fi.get("path") or "").strip()
                    if not path:
                        return

                    src_for_cleanup: Path | None = None
                    src_key: str | None = None
                    if not persist_tsv_files:
                        try:
                            src_for_cleanup = _safe_worker_tsv_source(path)
                            src_key = str(src_for_cleanup)
                            if src_key in finalized_tsv_source_paths:
                                fi["_tsv_finalized"] = True
                                return
                            src_for_cleanup.unlink()
                            finalized_tsv_source_paths.add(src_key)
                            fi["_tsv_finalized"] = True
                        except Exception as e:
                            report.warn(
                                stage="json_pipeline.tsv_persist",
                                message="Failed to remove TSV worker file",
                                path=path,
                                error={"type": type(e).__name__, "message": str(e)},
                            )
                        return

                    try:
                        src = _safe_worker_tsv_source(path)
                        src_for_cleanup = src
                        src_key = str(src)
                        if src_key in finalized_tsv_source_paths:
                            fi["_tsv_finalized"] = True
                            return
                        table_dir = _prepare_safe_artifact_dir(
                            persist_tsv_dir,
                            _slug(table_original, max_len=120),
                            purpose="TSV artifact",
                        )

                        ext = src.suffix if src.suffix else ".tsv"
                        base = f"b{int(batch_idx):06d}_{_slug(phase, max_len=24)}_{_slug(src.stem, max_len=120)}"
                        dst = _allocate_artifact_path(table_dir, base=base, ext=ext)

                        shutil.move(str(src), str(dst))
                        fi["path"] = str(dst)
                        fi["_tsv_finalized"] = True
                        finalized_tsv_source_paths.add(src_key)

                        report.bump("tsv_files_persisted", 1)
                        try:
                            report.bump("rows_emitted", int(fi.get("rows") or 0))
                        except Exception:
                            pass
                    except Exception as e:
                        report.warn(
                            stage="json_pipeline.tsv_persist",
                            message="Failed to persist TSV artifact; leaving temporary file in place",
                            path=path,
                            error={"type": type(e).__name__, "message": str(e)},
                        )

                def _finalize_remaining_tsv_artifacts(groups: list[dict[str, Any]] | None) -> None:
                    if not persist_tsv_files or not groups:
                        return
                    for group in groups:
                        if not isinstance(group, dict):
                            continue
                        table_original = str(group.get("table_sql") or group.get("table_original") or "")
                        for fi in list(group.get("entries") or []):
                            _finalize_tsv_file(fi, table_original=table_original, phase="load")

                def _persist_parquet_table(df: Any, *, table_original: str) -> None:
                    import json
                    import math
                    import os
                    import uuid
                    from numbers import Number

                    base = f"b{int(batch_idx):06d}"
                    table_dir = _prepare_safe_artifact_dir(
                        persist_parquet_dir,
                        _slug(table_original, max_len=120),
                        purpose="parquet artifact",
                    )
                    dst = _allocate_artifact_path(table_dir, base=base, ext=".parquet")

                    parquet_df = df
                    reset_index = getattr(parquet_df, "reset_index", None)
                    if callable(reset_index):
                        try:
                            parquet_df = reset_index(drop=True)
                        except Exception:
                            parquet_df = df

                    def _is_parquet_nullish(value: Any) -> bool:
                        if value is None:
                            return True
                        try:
                            if type(value).__name__ == "NAType":
                                return True
                        except Exception:
                            pass
                        try:
                            return isinstance(value, float) and math.isnan(value)
                        except Exception:
                            return False

                    def _parquet_value_kind(value: Any) -> str | None:
                        if _is_parquet_nullish(value):
                            return None
                        if isinstance(value, dict):
                            return "dict"
                        if isinstance(value, list):
                            return "list"
                        if isinstance(value, tuple):
                            return "tuple"
                        if isinstance(value, set):
                            return "set"
                        if isinstance(value, (str, bytes, bytearray)):
                            return "string"
                        if isinstance(value, bool):
                            return "bool"
                        if isinstance(value, int) and not (-(2**63) <= value <= (2**64 - 1)):
                            return "large_int"
                        if isinstance(value, Number):
                            return "number"
                        return type(value).__name__

                    def _stringify_parquet_value(value: Any) -> str | None:
                        if _is_parquet_nullish(value):
                            return None
                        if isinstance(value, (bytes, bytearray)):
                            return bytes(value).decode("utf-8", errors="replace")
                        if isinstance(value, bool):
                            return "true" if value else "false"
                        if isinstance(value, (dict, list, tuple, set)):
                            return json.dumps(value, ensure_ascii=False, default=str)
                        return str(value)

                    def _coerce_object_columns(frame: Any, *, force: bool = False) -> Any:
                        columns = getattr(frame, "columns", None)
                        if columns is None:
                            return frame
                        out_frame = None
                        for col in list(columns):
                            try:
                                series = frame[col]
                            except Exception:
                                continue
                            dtype_name = str(getattr(series, "dtype", ""))
                            if not force and dtype_name != "object":
                                continue
                            kinds: set[str] = set()
                            try:
                                values = series.tolist()
                            except Exception:
                                try:
                                    values = list(series)
                                except Exception:
                                    continue
                            for value in values:
                                kind = _parquet_value_kind(value)
                                if kind:
                                    kinds.add(kind)
                                if len(kinds) > 1 and not force:
                                    break
                            if not kinds:
                                continue
                            if (
                                not force
                                and len(kinds) <= 1
                                and "large_int" not in kinds
                                and not (kinds & {"dict", "list", "tuple", "set"})
                            ):
                                continue
                            if out_frame is None:
                                try:
                                    out_frame = frame.copy()
                                except Exception:
                                    return frame
                            out_frame[col] = series.map(_stringify_parquet_value)
                        return out_frame if out_frame is not None else frame

                    def _write_parquet_file(frame: Any) -> None:
                        tmp = dst.with_name(f".{dst.name}.{uuid.uuid4().hex}.tmp")
                        try:
                            try:
                                frame.to_parquet(str(tmp), index=False)
                            except TypeError:
                                frame.to_parquet(str(tmp))
                            os.replace(str(tmp), str(dst))
                        except Exception:
                            try:
                                tmp.unlink()
                            except FileNotFoundError:
                                pass
                            raise

                    parquet_df = _coerce_object_columns(parquet_df, force=False)
                    try:
                        _write_parquet_file(parquet_df)
                    except Exception:
                        fallback_df = _coerce_object_columns(parquet_df, force=True)
                        if fallback_df is parquet_df:
                            raise
                        _write_parquet_file(fallback_df)

                    _record_parquet_manifest_columns(table_original, list(getattr(parquet_df, "columns", [])))
                    report.bump("parquet_files_persisted", 1)
                    try:
                        report.bump("parquet_rows_emitted", int(len(df)))
                    except Exception:
                        pass

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
                        # Ensure warmup batch loads/ALTERs are fully applied before we freeze schema.
                        try:
                            _drain_pending_loads()
                        except Exception as e:
                            report.warn(
                                stage="json_pipeline.schema_hybrid_freeze",
                                message="Failed to drain pending loads at hybrid freeze transition",
                                error={"type": type(e).__name__, "message": str(e)},
                            )
                            if not continue_on_error:
                                raise

                        # Snapshot allowed columns for each known table at the time of freezing.
                        try:
                            extra_canon = str(extra_column_name).replace(".", key_sep) if extra_column_name else "__extra__"
                            for table_original, nm in name_maps.items():
                                allowed: set[str] = set()
                                existing_sql = _get_existing_cols(nm.table_sql)
                                if existing_sql:
                                    sql_to_orig = {nm.columns_sql[i]: nm.columns_original[i] for i in range(len(nm.columns_sql))}
                                    for c in existing_sql:
                                        o = sql_to_orig.get(str(c))
                                        if o:
                                            allowed.add(o)
                                else:
                                    allowed = set(nm.columns_original)
                                allowed.add(index_key)
                                allowed.add(extra_canon)
                                frozen_allowed_cols_by_table_original[table_original] = allowed
                            report.set_artifact("frozen_schema_tables", int(len(frozen_allowed_cols_by_table_original)))
                        except Exception as e:
                            report.warn(
                                stage="json_pipeline.schema_hybrid_freeze",
                                message="Failed to snapshot frozen schema columns; unknown fields may be dropped",
                                error={"type": type(e).__name__, "message": str(e)},
                            )
                            if not continue_on_error:
                                raise

                if schema_mode == "hybrid":
                    try:
                        report.bump("schema_hybrid_batches_warmup" if is_hybrid_warmup else "schema_hybrid_batches_frozen", 1)
                    except Exception:
                        pass

                report.bump("batches_total", 1)

                if use_streaming_rows and (fast_load_state.enabled or export_tsv_only):
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

                        # TSV backend (worker(s) write TSV; parent does LOAD DATA) to minimize IPC.
                        #
                        # - parallel_workers>1: ProcessPool writes multiple TSV fragments per batch (minimize IPC)
                        # - parallel_workers<=1: run the same TSV worker in-process (still enables parallel DB load across tables)
                        tsv_extra_column_name = extra_column_name if use_extra_column else None
                        tsv_freeze_active = bool(use_extra_column) and (not bool(auto_alter_table_effective))
                        tsv_allowed_cols_by_table = None
                        if tsv_freeze_active and frozen_allowed_cols_by_table_original:
                            try:
                                tsv_allowed_cols_by_table = {
                                    k: tuple(sorted(v)) for k, v in frozen_allowed_cols_by_table_original.items()
                                }
                            except Exception:
                                tsv_allowed_cols_by_table = {
                                    str(k): list(v) for k, v in frozen_allowed_cols_by_table_original.items()
                                }
                        if (
                            (not parallel_tsv_disabled)
                            and len(batch_records) >= 1
                            and (bool(auto_alter_table_effective) or bool(tsv_freeze_active))
                        ):
                            import os
                            import shutil
                            import uuid

                            tmp_dir = str(dc.get("tmp_dir") or "/tmp")
                            worker_workdir_token = uuid.uuid4().hex[:12]
                            workdirs: list[str] = []
                            results: list[dict] = []
                            class _ParallelTSVFailed(Exception):
                                pass
                            defer_workdir_cleanup = False

                            def _register_tsv_workdir(wd: Any) -> str | None:
                                try:
                                    workdir = _absolute_no_resolve(str(wd or ""))
                                    tmp_root = _absolute_no_resolve(tmp_dir)
                                    try:
                                        workdir.lstat()
                                    except FileNotFoundError as exc:
                                        raise RuntimeError(f"TSV worker directory does not exist: {workdir}") from exc
                                    if workdir.is_symlink() or not workdir.is_dir():
                                        raise RuntimeError(f"TSV worker path is not a safe directory: {workdir}")
                                    workdir_resolved = workdir.resolve(strict=True)
                                    tmp_root_resolved = tmp_root.resolve(strict=True)
                                    expected_prefix = f"kisti_flatten_{worker_workdir_token}_"
                                    if not workdir_resolved.name.startswith(expected_prefix):
                                        raise RuntimeError(
                                            f"TSV worker directory has unexpected name: {workdir_resolved.name}"
                                        )
                                    if not _is_relative_to(workdir_resolved, tmp_root_resolved):
                                        raise RuntimeError(
                                            f"TSV worker directory resolves outside tmp_dir: {workdir}"
                                        )
                                    safe_tsv_workdirs.add(workdir_resolved)
                                    return str(workdir_resolved)
                                except Exception as e:
                                    report.warn(
                                        stage="json_pipeline.flatten.parallel_tsv.workdir",
                                        message="Ignoring unsafe TSV worker directory",
                                        path=str(wd),
                                        error={"type": type(e).__name__, "message": str(e)},
                                    )
                                    if not continue_on_error:
                                        raise RuntimeError(f"unsafe_tsv_worker_directory: {e}") from e
                                    return None

                            def _register_parent_tsv_file(path_value: Any, *, expected_prefix: str) -> Path:
                                src = _absolute_no_resolve(str(path_value or ""))
                                try:
                                    src.lstat()
                                except FileNotFoundError as exc:
                                    raise RuntimeError(f"parent TSV file does not exist: {src}") from exc
                                if src.is_symlink() or not src.is_file():
                                    raise RuntimeError(f"parent TSV path is not a safe regular file: {src}")
                                src_resolved = src.resolve(strict=True)
                                tmp_root_resolved = _absolute_no_resolve(tmp_dir).resolve(strict=True)
                                if not src_resolved.name.startswith(expected_prefix):
                                    raise RuntimeError(
                                        f"parent TSV file has unexpected name: {src_resolved.name}"
                                    )
                                if not _is_relative_to(src_resolved, tmp_root_resolved):
                                    raise RuntimeError(f"parent TSV file resolves outside tmp_dir: {src}")
                                safe_tsv_files.add(src_resolved)
                                return src_resolved

                            try:
                                use_pool = bool(parallel_workers) and int(parallel_workers) > 1 and len(batch_records) >= 2
                                if use_pool:
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

                                    if flatten_executor is None:
                                        flatten_executor = ProcessPoolExecutor(max_workers=int(parallel_workers))

                                    try:
                                        with report.timer("json.flatten"):
                                            futs = []
                                            for s, e in slices:
                                                ctx_slice = None
                                                if isinstance(record_contexts, (list, tuple)):
                                                    ctx_slice = list(record_contexts[s:e])
                                                futs.append(
                                                    flatten_executor.submit(
                                                        _safe_flatten_jsons_to_tsv_worker,
                                                        (
                                                            int(index_offset) + int(s),
                                                            list(batch_records[s:e]),
                                                            index_key,
                                                            tuple(except_keys or ()),
                                                            key_sep,
                                                            tmp_dir,
                                                            ctx_slice,
                                                            base_table,
                                                            tsv_extra_column_name,
                                                            tsv_allowed_cols_by_table,
                                                            bool(excepted_expand_dict),
                                                            dict(id_compactor.config) if id_compactor.enabled else None,
                                                            worker_workdir_token,
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
                                        # Disable this backend for the remainder of the run (prevents repeat failures).
                                        parallel_tsv_disabled = True
                                        try:
                                            if flatten_executor is not None:
                                                flatten_executor.shutdown(wait=False, cancel_futures=True)
                                        except Exception:
                                            pass
                                        flatten_executor = None
                                        raise _ParallelTSVFailed() from e
                                else:
                                    try:
                                        ctx_full = None
                                        if isinstance(record_contexts, (list, tuple)):
                                            ctx_full = list(record_contexts)
                                        with report.timer("json.flatten"):
                                            res = _safe_flatten_jsons_to_tsv_worker(
                                                (
                                                    int(index_offset),
                                                    list(batch_records),
                                                    index_key,
                                                    tuple(except_keys or ()),
                                                    key_sep,
                                                    tmp_dir,
                                                    ctx_full,
                                                    base_table,
                                                    tsv_extra_column_name,
                                                    tsv_allowed_cols_by_table,
                                                    bool(excepted_expand_dict),
                                                    dict(id_compactor.config) if id_compactor.enabled else None,
                                                    worker_workdir_token,
                                                )
                                            )
                                        if not isinstance(res, dict) or not res.get("ok"):
                                            err = res.get("error") if isinstance(res, dict) else None
                                            raise RuntimeError(f"tsv_worker_failed: {err}")
                                        results.append(res)
                                    except Exception as e:
                                        try:
                                            report.warn(
                                                stage="json_pipeline.flatten.parallel_tsv",
                                                message="TSV backend failed; falling back to serial rows backend",
                                                error={"type": type(e).__name__, "message": str(e)},
                                            )
                                        except Exception:
                                            pass
                                        parallel_tsv_disabled = True
                                        raise _ParallelTSVFailed() from e

                                tables_files: dict[str, list[dict]] = {}
                                load_groups: list[dict[str, Any]] = []

                                def _add_file(table: str, fi: dict | None) -> None:
                                    if not isinstance(fi, dict):
                                        return
                                    if not fi.get("path") or not fi.get("columns") or not fi.get("rows"):
                                        return
                                    try:
                                        _safe_worker_tsv_source(fi.get("path"))
                                    except Exception as e:
                                        report.warn(
                                            stage="json_pipeline.flatten.parallel_tsv.file",
                                            message="Ignoring unsafe TSV worker file",
                                            table=str(table),
                                            path=str(fi.get("path")),
                                            error={"type": type(e).__name__, "message": str(e)},
                                        )
                                        if not continue_on_error:
                                            raise RuntimeError(f"unsafe_tsv_worker_file: {e}") from e
                                        return
                                    tables_files.setdefault(str(table), []).append(fi)

                                for res in results:
                                    if not isinstance(res, dict) or not res.get("ok"):
                                        err = None
                                        if isinstance(res, dict):
                                            err = res.get("error")
                                        report.warn(
                                            stage="json_pipeline.flatten.parallel_tsv",
                                            message="Worker failed to produce TSV artifacts",
                                            error=err,
                                        )
                                        err_type = err.get("type") if isinstance(err, dict) else ""
                                        if err_type == "IdCompactionError" or not continue_on_error:
                                            raise RuntimeError(f"parallel_tsv_worker_failed: {err}")
                                        continue

                                    try:
                                        report.bump("records_ok", int(res.get("records_ok", 0) or 0))
                                        report.bump("records_failed", int(res.get("records_failed", 0) or 0))
                                        id_compactor.merge_summary(res.get("id_compaction"))
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
                                        safe_wd = _register_tsv_workdir(wd)
                                        if safe_wd:
                                            workdirs.append(safe_wd)

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

                                # Ensure any previous batch loads are fully applied before we touch DB for this batch.
                                if overlap_batches:
                                    _drain_pending_loads()

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
                                                column_descriptions=_column_descriptions_for(table_original),
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
                                            column_descriptions=_column_descriptions_for(table_original),
                                        )
                                        if isinstance(nm_created, NameMap):
                                            name_maps[table_original] = nm_created
                                            nm = nm_created
                                        if nm_created is not None:
                                            created_tables.add(table_original)
                                            report.bump("tables_created", 1)
                                            existing_cols_cache[nm.table_sql] = set(nm.columns_sql)
                                            if tsv_freeze_active and tsv_extra_column_name:
                                                frozen_allowed_cols_by_table_original[table_original] = set(nm.columns_original)

                                    if not load:
                                        if persist_tsv_files:
                                            for fi in files:
                                                _finalize_tsv_file(fi, table_original=table_original, phase="flatten")
                                            report.bump("tables_emitted", 1)
                                        continue

                                    if load:
                                        import tempfile
                                        import uuid

                                        groups: dict[tuple[str, ...], list[dict]] = {}
                                        for fi in files:
                                            cols_key = tuple(fi.get("columns") or ())
                                            if not cols_key:
                                                continue
                                            groups.setdefault(cols_key, []).append(fi)

                                        merged_entries: list[dict] = []

                                        # Optional: merge TSV fragments even when schemas differ by rewriting to the
                                        # per-batch union schema (`cols`). This can drastically reduce LOAD DATA calls
                                        # when parallel workers observe different sparse columns.
                                        union_merged = False
                                        if (
                                            bool(tsv_merge_union_schema)
                                            and len(groups) > 1
                                            and cols
                                            and len(cols) > 0
                                            and (
                                                int(tsv_union_merge_max_union_cols or 0) <= 0
                                                or len(cols) <= int(tsv_union_merge_max_union_cols)
                                            )
                                        ):
                                            union_len = float(len(cols))
                                            try:
                                                min_cov = min(float(len(k)) / union_len for k in groups.keys() if k)
                                            except Exception:
                                                min_cov = 0.0
                                            try:
                                                max_missing = max(int(len(cols)) - int(len(k)) for k in groups.keys() if k)
                                            except Exception:
                                                max_missing = int(len(cols))

                                            eligible = False
                                            try:
                                                if int(max_missing) <= int(tsv_union_merge_max_missing_cols or 0):
                                                    eligible = True
                                            except Exception:
                                                pass
                                            if not eligible and min_cov >= float(tsv_union_merge_min_coverage or 0.0):
                                                eligible = True

                                            if eligible:
                                                t_merge0 = time.perf_counter()
                                                tmp_path = None
                                                try:
                                                    union_cols = list(cols)
                                                    union_cols_key = tuple(union_cols)
                                                    with tempfile.NamedTemporaryFile(
                                                        mode="wb",
                                                        prefix=f"kisti_union_{uuid.uuid4().hex[:8]}_",
                                                        suffix=".tsv",
                                                        delete=False,
                                                        dir=tmp_dir,
                                                    ) as out:
                                                        tmp_path = out.name
                                                        merged_path = tmp_path
                                                        null_lit = b"\\N"

                                                        for cols_key, group_files in groups.items():
                                                            for gf in group_files:
                                                                path = gf.get("path")
                                                                if not path:
                                                                    continue
                                                                src_path = _safe_worker_tsv_source(path)
                                                                file_cols = tuple(gf.get("columns") or ())
                                                                if file_cols == union_cols_key:
                                                                    with open(str(src_path), "rb") as inp:
                                                                        shutil.copyfileobj(inp, out, length=1024 * 1024)
                                                                    continue

                                                                idx = {str(c): i for i, c in enumerate(file_cols)}
                                                                take = [idx.get(str(c)) for c in union_cols]
                                                                with open(str(src_path), "rb") as inp:
                                                                    for line in inp:
                                                                        if not line:
                                                                            continue
                                                                        if line.endswith(b"\n"):
                                                                            line = line[:-1]
                                                                        if line.endswith(b"\r"):
                                                                            line = line[:-1]
                                                                        vals = line.split(b"\t")
                                                                        out_vals = []
                                                                        for j in take:
                                                                            if j is None or j >= len(vals):
                                                                                out_vals.append(null_lit)
                                                                            else:
                                                                                out_vals.append(vals[j])
                                                                        out.write(b"\t".join(out_vals) + b"\n")

                                                    if merged_path:
                                                        merged_path = str(
                                                            _register_parent_tsv_file(
                                                                merged_path,
                                                                expected_prefix="kisti_union_",
                                                            )
                                                        )
                                                        # Delete originals after successful merge.
                                                        for group_files in groups.values():
                                                            for gf in group_files:
                                                                try:
                                                                    _safe_worker_tsv_source(gf.get("path")).unlink(missing_ok=True)
                                                                except Exception:
                                                                    pass

                                                        merged_entries = [
                                                            {
                                                                "path": str(merged_path),
                                                                "columns": list(union_cols),
                                                                "rows": int(sum(int(gf.get("rows") or 0) for g in groups.values() for gf in g)),
                                                            }
                                                        ]
                                                        union_merged = True
                                                        try:
                                                            report.bump("tsv_union_merged_tables", 1)
                                                            report.bump(
                                                                "tsv_union_merged_files",
                                                                int(sum(len(g) for g in groups.values())),
                                                            )
                                                            report.add_time_s("tsv.merge.union", time.perf_counter() - t_merge0)
                                                        except Exception:
                                                            pass
                                                except Exception:
                                                    if tmp_path:
                                                        try:
                                                            os.remove(str(tmp_path))
                                                        except Exception:
                                                            pass
                                                    union_merged = False

                                        if not union_merged:
                                            # Reduce LOAD DATA calls by concatenating files with identical schemas.
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
                                                            src_path = _safe_worker_tsv_source(gf.get("path"))
                                                            with open(str(src_path), "rb") as inp:
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
                                                merged_path = str(
                                                    _register_parent_tsv_file(
                                                        merged_path,
                                                        expected_prefix="kisti_merge_",
                                                    )
                                                )
                                                for gf in group_files:
                                                    try:
                                                        _safe_worker_tsv_source(gf.get("path")).unlink(missing_ok=True)
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
                                                "table_original": nm.table_original,
                                                "table_sql": nm.table_sql,
                                                "entries": merged_entries,
                                                "existing_cols": set(_get_existing_cols(nm.table_sql) or []) if inspector is not None else None,
                                            }
                                        )

                                if not load:
                                    return

                                # Execute LOAD DATA for this batch (optionally parallel across tables).
                                if load and load_groups:
                                    if overlap_batches and not load_parallel_disabled:
                                        # Overlapped load: submit and return immediately. Results are aggregated
                                        # when we drain before the next batch's DB work (or at pipeline end).
                                        import threading
                                        import time
                                        from concurrent.futures import ThreadPoolExecutor

                                        import pymysql

                                        eff_workers = int(db_load_parallel_tables) if int(db_load_parallel_tables or 0) > 0 else 1
                                        if load_executor is None or load_tls is None:
                                            load_tls = threading.local()
                                            if load_conns_lock is None:
                                                load_conns_lock = threading.Lock()

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
                                                load_tls.conn = conn
                                                try:
                                                    with load_conns_lock:
                                                        load_conns.append(conn)
                                                except Exception:
                                                    pass

                                            load_executor = ThreadPoolExecutor(
                                                max_workers=int(eff_workers),
                                                thread_name_prefix="kisti_load",
                                                initializer=_thread_init,
                                            )

                                        def _get_conn():
                                            return getattr(load_tls, "conn", None) if load_tls is not None else None

                                        def _load_group(g: dict) -> dict:
                                            nm = g.get("nm")
                                            table_original = str(g.get("table_original") or "")
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

                                            commit_per_file = str(load_data_commit_strategy_effective) == "file"
                                            commit_in_group = not commit_per_file

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
                                                        extra_column_name=tsv_extra_column_name,
                                                        columns_original=list(file_cols),
                                                        auto_alter_table=bool(auto_alter_table_effective),
                                                        column_type=column_type,
                                                        report=None,  # avoid thread-unsafe RunReport mutations
                                                        engine=engine,
                                                        existing_cols=existing_cols,
                                                        load_method=db_load_method,
                                                        fast_load_state=None,
                                                        load_data_commit=bool(commit_per_file),
                                                        local_infile_conn=conn,
                                                        column_descriptions=_column_descriptions_for(table_original),
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
                                                    _finalize_tsv_file(fi, table_original=table_sql, phase="load")
                                            if commit_in_group:
                                                try:
                                                    if res.get("loaded_any"):
                                                        if (not res.get("ok")) and (not continue_on_error):
                                                            conn.rollback()
                                                        else:
                                                            conn.commit()
                                                    else:
                                                        if not res.get("ok"):
                                                            conn.rollback()
                                                except Exception as e:
                                                    res["ok"] = False
                                                    res["errors"].append({"type": type(e).__name__, "message": f"commit_failed: {e}"})
                                            return res

                                        futs = []
                                        remaining_submit_groups: list[dict] = []
                                        submit_started_at = time.perf_counter()
                                        for i, g in enumerate(load_groups):
                                            try:
                                                futs.append(load_executor.submit(_load_group, g))
                                            except Exception as e:
                                                remaining_submit_groups = list(load_groups[i:])
                                                load_parallel_disabled = True
                                                report.warn(
                                                    stage="json_pipeline.load.overlap",
                                                    message="Overlapped loader submit failed; falling back to synchronous load",
                                                    error={"type": type(e).__name__, "message": str(e)},
                                                )
                                                break

                                        if futs and remaining_submit_groups:
                                            pending_load_futures = list(futs)
                                            pending_load_workdirs = []
                                            pending_load_tsv_files = []
                                            pending_load_submitted_at = submit_started_at
                                            pending_load_checkpoint_ctx = None
                                            pending_load_checkpoint_extra = None
                                            _drain_pending_loads()
                                            load_groups = remaining_submit_groups
                                        elif futs:
                                            # Attach to pending list and return; keep workdirs until drained.
                                            pending_load_checkpoint_ctx = dict(record_contexts[-1]) if record_contexts else None
                                            pending_load_checkpoint_extra = _batch_progress_extra(
                                                mode="overlap",
                                                parquet=parquet_progress,
                                            )
                                            pending_load_futures = list(futs)
                                            pending_load_workdirs = list(workdirs)
                                            pending_load_tsv_files = [str(p) for p in safe_tsv_files]
                                            pending_load_submitted_at = submit_started_at
                                            defer_workdir_cleanup = True
                                            return

                                    # Serial load: keep existing behavior/timings.
                                    if load_parallel_disabled or (not db_load_parallel_tables) or int(db_load_parallel_tables) <= 1 or len(load_groups) <= 1:
                                        commit_strategy = str(load_data_commit_strategy_effective)
                                        commit_per_file = commit_strategy == "file"
                                        commit_after_table = commit_strategy == "table"
                                        commit_after_batch = commit_strategy == "batch"
                                        batch_loaded_any = False

                                        for g in load_groups:
                                            nm = g.get("nm")
                                            table_original = str(g.get("table_original") or "")
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
                                                    # TSV backend can optionally populate __extra__ (freeze/hybrid frozen).
                                                    extra_column_name=tsv_extra_column_name,
                                                    columns_original=list(file_cols),
                                                    auto_alter_table=bool(auto_alter_table_effective),
                                                    column_type=column_type,
                                                    report=report,
                                                    engine=engine,
                                                    existing_cols=_get_existing_cols(str(table_sql)),
                                                    load_method=db_load_method,
                                                    fast_load_state=fast_load_state,
                                                    load_data_commit=bool(commit_per_file),
                                                    local_infile_conn=local_infile_conn,
                                                    column_descriptions=_column_descriptions_for(table_original),
                                                )
                                                if load_res is not None:
                                                    loaded_any = True
                                                    try:
                                                        report.bump("rows_loaded", int(fi.get("rows") or 0))
                                                    except Exception:
                                                        pass
                                                _finalize_tsv_file(fi, table_original=str(table_sql), phase="load")
                                            if loaded_any:
                                                batch_loaded_any = True
                                            if loaded_any and commit_after_table:
                                                t0 = time.perf_counter()
                                                local_infile_conn.commit()
                                                dt = time.perf_counter() - t0
                                                # Keep timing semantics comparable to per-file commits.
                                                report.add_time_s("db.load_data.exec", dt)
                                                report.add_time_s("db.load_data.total", dt)
                                                report.add_time_s("json.db.load", dt)
                                            if loaded_any:
                                                report.bump("tables_loaded", 1)
                                        if batch_loaded_any and commit_after_batch:
                                            t0 = time.perf_counter()
                                            local_infile_conn.commit()
                                            dt = time.perf_counter() - t0
                                            report.add_time_s("db.load_data.exec", dt)
                                            report.add_time_s("db.load_data.total", dt)
                                            report.add_time_s("json.db.load", dt)
                                        return

                                    # Parallel load across tables: one LOCAL INFILE connection per thread (reused across batches).
                                    import threading
                                    import time
                                    from concurrent.futures import ThreadPoolExecutor, as_completed

                                    import pymysql

                                    if load_executor is None or load_tls is None:
                                        load_tls = threading.local()
                                        if load_conns_lock is None:
                                            load_conns_lock = threading.Lock()

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
                                            load_tls.conn = conn
                                            try:
                                                with load_conns_lock:
                                                    load_conns.append(conn)
                                            except Exception:
                                                pass

                                        load_executor = ThreadPoolExecutor(
                                            max_workers=int(db_load_parallel_tables),
                                            thread_name_prefix="kisti_load",
                                            initializer=_thread_init,
                                        )

                                    def _get_conn():
                                        return getattr(load_tls, "conn", None) if load_tls is not None else None

                                    def _load_group(g: dict) -> dict:
                                        nm = g.get("nm")
                                        table_original = str(g.get("table_original") or "")
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

                                        commit_per_file = str(load_data_commit_strategy_effective) == "file"
                                        commit_in_group = not commit_per_file

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
                                                    extra_column_name=tsv_extra_column_name,
                                                    columns_original=list(file_cols),
                                                    auto_alter_table=bool(auto_alter_table_effective),
                                                    column_type=column_type,
                                                    report=None,  # avoid thread-unsafe RunReport mutations
                                                    engine=engine,
                                                    existing_cols=existing_cols,
                                                    load_method=db_load_method,
                                                    fast_load_state=None,
                                                    load_data_commit=bool(commit_per_file),
                                                    local_infile_conn=conn,
                                                    column_descriptions=_column_descriptions_for(table_original),
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
                                                _finalize_tsv_file(fi, table_original=table_sql, phase="load")
                                        if commit_in_group:
                                            try:
                                                if res.get("loaded_any"):
                                                    if (not res.get("ok")) and (not continue_on_error):
                                                        conn.rollback()
                                                    else:
                                                        conn.commit()
                                                else:
                                                    if not res.get("ok"):
                                                        conn.rollback()
                                            except Exception as e:
                                                res["ok"] = False
                                                res["errors"].append({"type": type(e).__name__, "message": f"commit_failed: {e}"})
                                        return res

                                    remaining_groups: list[dict] = []
                                    t0 = time.perf_counter()
                                    with report.timer("json.db.load"):
                                        futures = []
                                        for i, g in enumerate(load_groups):
                                            try:
                                                futures.append(load_executor.submit(_load_group, g))
                                            except Exception as e:
                                                remaining_groups = list(load_groups[i:])
                                                load_parallel_disabled = True
                                                report.warn(
                                                    stage="json_pipeline.load.parallel_tables",
                                                    message="Parallel table loader submit failed; falling back to serial for remaining tables",
                                                    error={"type": type(e).__name__, "message": str(e)},
                                                )
                                                break

                                        first_failed_table: str | None = None
                                        for fut in as_completed(futures):
                                            try:
                                                r = fut.result()
                                            except Exception as e:
                                                r = {
                                                    "table_sql": "",
                                                    "ok": False,
                                                    "loaded_any": False,
                                                    "rows_loaded": 0,
                                                    "load_ok": 0,
                                                    "load_failed": 1,
                                                    "load_data_ok": 0,
                                                    "existing_cols": None,
                                                    "errors": [{"type": type(e).__name__, "message": str(e)}],
                                                }

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
                                                if not continue_on_error and first_failed_table is None:
                                                    first_failed_table = str(r.get("table_sql") or "")

                                        if first_failed_table is not None:
                                            raise RuntimeError(f"parallel_table_load_failed: {first_failed_table}")

                                        if remaining_groups:
                                            # Ensure remaining tables are loaded serially (no duplication: they were never submitted).
                                            for g in remaining_groups:
                                                nm = g.get("nm")
                                                table_original = str(g.get("table_original") or "")
                                                table_sql = str(g.get("table_sql") or "")
                                                entries = g.get("entries") or []
                                                existing_cols = g.get("existing_cols")
                                                if not nm or not table_sql or not entries:
                                                    continue
                                                commit_per_file = str(load_data_commit_strategy_effective) == "file"
                                                commit_in_group = not commit_per_file
                                                loaded_any = False
                                                had_error = False
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
                                                            extra_column_name=tsv_extra_column_name,
                                                            columns_original=list(file_cols),
                                                            auto_alter_table=bool(auto_alter_table_effective),
                                                            column_type=column_type,
                                                            report=None,
                                                            engine=engine,
                                                            existing_cols=existing_cols,
                                                            load_method=db_load_method,
                                                            fast_load_state=None,
                                                            load_data_commit=bool(commit_per_file),
                                                            local_infile_conn=local_infile_conn,
                                                            column_descriptions=_column_descriptions_for(table_original),
                                                        )
                                                        loaded_any = True
                                                        report.bump("load_ok", 1)
                                                        report.bump("load_data_ok", 1)
                                                        report.bump("rows_loaded", int(fi.get("rows") or 0))
                                                    except Exception as e:
                                                        had_error = True
                                                        report.bump("load_failed", 1)
                                                        report.warn(
                                                            stage="json_pipeline.load.parallel_tables.fallback",
                                                            message="Serial fallback LOAD DATA failed",
                                                            table=table_sql,
                                                            error={"type": type(e).__name__, "message": str(e)},
                                                        )
                                                        if not continue_on_error:
                                                            raise
                                                    finally:
                                                        _finalize_tsv_file(fi, table_original=table_sql, phase="load")
                                                if commit_in_group and local_infile_conn is not None:
                                                    if loaded_any:
                                                        local_infile_conn.commit()
                                                    elif had_error:
                                                        local_infile_conn.rollback()
                                                report.bump("tables_loaded", 1)
                                                if existing_cols is not None:
                                                    try:
                                                        existing_cols_cache[str(table_sql)] = set(existing_cols)
                                                    except Exception:
                                                        pass
                                    dt = time.perf_counter() - t0
                                    # For parallel load, report wall time to keep share_pct meaningful.
                                    report.add_time_s("db.load_data.exec", dt)
                                    report.add_time_s("db.load_data.total", dt)
                                return
                            except _ParallelTSVFailed:
                                # Fall back to serial rows backend below.
                                pass
                            finally:
                                if not defer_workdir_cleanup:
                                    _finalize_remaining_tsv_artifacts(
                                        load_groups if "load_groups" in locals() else None
                                    )
                                    _cleanup_workdirs(workdirs)
                                    _cleanup_parent_tsv_files([str(p) for p in safe_tsv_files])

                        # Serial rows backend (rows -> TSV -> LOAD DATA) for maximal correctness/compatibility.
                        try:
                            with report.timer("json.flatten"):
                                rows_main, sub_rows_tot, excepted = extract_rows_from_jsons(
                                    batch_records,
                                    index_key=index_key,
                                    base_table=base_table,
                                    except_keys=except_keys,
                                    excepted_expand_dict=bool(excepted_expand_dict),
                                    sep=key_sep,
                                    id_compactor=id_compactor,
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

                        # If the previous batch is still loading, wait before doing any DB work for this batch.
                        if overlap_batches:
                            _drain_pending_loads()

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
                                        column_descriptions=_column_descriptions_for(table_original),
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
                                    column_descriptions=_column_descriptions_for(table_original),
                                )
                                if isinstance(nm_created, NameMap):
                                    name_maps[table_original] = nm_created
                                    nm = nm_created
                                if nm_created is not None:
                                    created_tables.add(table_original)
                                    report.bump("tables_created", 1)
                                    existing_cols_cache[nm.table_sql] = set(nm.columns_sql)
                                    if use_extra_column and not bool(auto_alter_table_effective):
                                        frozen_allowed_cols_by_table_original[table_original] = set(nm.columns_original)

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
                                    column_descriptions=_column_descriptions_for(table_original),
                                )
                                if load_res is not None:
                                    report.bump("tables_loaded", 1)
                                    try:
                                        report.bump("rows_loaded", int(len(rows)))
                                    except Exception:
                                        pass
                        return

                rust_unsupported = rust_arrow_unsupported_reason(
                    requested_backend=flatten_backend_requested,
                    persist_parquet_files=bool(persist_parquet_files),
                    create=bool(create),
                    load=bool(load),
                    index=bool(index),
                    optimize=bool(optimize),
                    emit_ddl=bool(emit_ddl),
                    custom_extract_fn=bool(custom_extract_fn),
                    id_compaction_enabled=bool(id_compactor.enabled),
                    excepted_expand_dict_enabled=bool(excepted_expand_dict),
                )
                if flatten_backend_requested == BACKEND_AUTO and rust_auto_disabled_reason:
                    rust_unsupported = rust_auto_disabled_reason
                if flatten_backend_requested == BACKEND_RUST_ARROW and rust_unsupported:
                    err = RuntimeError(f"rust-arrow backend unavailable for this run: {rust_unsupported}")
                    report.exception(
                        stage="json_pipeline.rust_arrow",
                        message="Rust Arrow backend cannot handle this JSON run",
                        exc=err,
                        reason=rust_unsupported,
                    )
                    q.write(stage="json_pipeline.rust_arrow", record={"table_name": base_table}, exc=err)
                    if not continue_on_error:
                        raise err
                    return

                if flatten_backend_requested in {BACKEND_AUTO, BACKEND_RUST_ARROW} and rust_unsupported is None:
                    rust_t0 = time.perf_counter()
                    rust_existing_parquet: set[str] = set()
                    rust_existing_table_dirs: set[str] = set()

                    def _safe_rust_table_dirs(root):
                        try:
                            if root.is_symlink() or not root.is_dir():
                                return
                        except Exception:
                            return
                        try:
                            children = list(root.iterdir())
                        except FileNotFoundError:
                            return
                        except Exception:
                            return
                        for table_dir in children:
                            try:
                                if table_dir.is_symlink() or not table_dir.is_dir():
                                    continue
                                yield table_dir
                            except Exception:
                                continue

                    def _safe_rust_batch_parquet_candidates(root, pattern):
                        for table_dir in _safe_rust_table_dirs(root):
                            try:
                                for path in table_dir.glob(pattern):
                                    if path.is_symlink() or not path.is_file():
                                        continue
                                    yield path
                            except Exception:
                                continue

                    try:
                        from pathlib import Path

                        root = Path(str(persist_parquet_dir))
                        pattern = f"b{int(batch_idx):06d}*.parquet"
                        rust_existing_parquet = {
                            str(path) for path in _safe_rust_batch_parquet_candidates(root, pattern)
                        }
                        rust_existing_table_dirs = {str(path) for path in _safe_rust_table_dirs(root)}
                    except Exception:
                        rust_existing_parquet = set()
                        rust_existing_table_dirs = set()

                    def _cleanup_rust_batch_parquet() -> None:
                        try:
                            from pathlib import Path

                            root = Path(str(persist_parquet_dir))
                            pattern = f"b{int(batch_idx):06d}*.parquet"
                            for path in _safe_rust_batch_parquet_candidates(root, pattern):
                                if str(path) in rust_existing_parquet:
                                    continue
                                try:
                                    path.unlink()
                                except Exception:
                                    pass
                            for table_dir in _safe_rust_table_dirs(root):
                                if str(table_dir) in rust_existing_table_dirs:
                                    continue
                                try:
                                    if not any(table_dir.iterdir()):
                                        table_dir.rmdir()
                                except Exception:
                                    pass
                        except Exception:
                            pass

                    def _record_auto_fallback(reason: str, *, rust_failed: bool) -> None:
                        nonlocal rust_auto_disabled_reason, rust_auto_fallback_batches
                        rust_auto_disabled_reason = str(reason)
                        if rust_failed:
                            rust_auto_fallback_batches += 1
                        effective = BACKEND_PYTHON
                        if rust_auto_success_batches > 0:
                            effective = "mixed"
                        report.set_artifact("flatten_backend_effective", effective)
                        report.set_artifact("flatten_backend_fallback_reason", str(reason))
                        report.set_artifact("flatten_backend_auto_disabled_reason", str(reason))
                        report.set_artifact("python_fallback_active", True)
                        report.set_artifact("rust_arrow_failed_batches", int(rust_auto_fallback_batches))
                        # Backward-compatible alias. This counts Rust failed batches,
                        # not every later batch handled by Python after fallback latches.
                        report.set_artifact("flatten_backend_fallback_batches", int(rust_auto_fallback_batches))
                        report.warn(
                            stage="json_pipeline.rust_arrow_auto_fallback",
                            message="Rust Arrow backend failed in auto mode; remaining batches will use the Python backend",
                            reason=str(reason),
                            effective_backend=effective,
                            rust_failed=bool(rust_failed),
                        )

                    def _read_rust_tables_for_db(tables_meta: list[Any]) -> dict[str, Any]:
                        try:
                            import pandas as pd
                        except Exception as e:
                            raise RuntimeError("rust-arrow DB bridge requires pandas to read parquet artifacts") from e

                        root = _assert_no_symlink_components(
                            persist_parquet_dir,
                            purpose="rust-arrow parquet DB bridge root",
                        )
                        try:
                            root_canon = root.resolve(strict=True)
                        except Exception as e:
                            raise RuntimeError(f"failed to resolve rust-arrow parquet root for DB bridge: {root}") from e

                        table_frames: dict[str, list[Any]] = {}
                        for table_info in tables_meta:
                            if not isinstance(table_info, Mapping):
                                continue
                            table_original = str(table_info.get("table") or "")
                            path_raw = str(table_info.get("path") or "")
                            if not table_original or not path_raw:
                                continue
                            path = _assert_no_symlink_components(
                                path_raw,
                                purpose="rust-arrow parquet DB bridge file",
                            )
                            try:
                                path_canon = path.resolve(strict=True)
                            except Exception as e:
                                raise RuntimeError(f"failed to resolve rust-arrow parquet file for DB bridge: {path}") from e
                            if not path_canon.is_relative_to(root_canon):
                                raise RuntimeError(f"rust-arrow parquet file is outside the configured root: {path}")
                            if path_canon.is_symlink() or not path_canon.is_file():
                                raise RuntimeError(f"rust-arrow parquet DB bridge path is not a safe file: {path}")
                            df = pd.read_parquet(str(path_canon))
                            table_frames.setdefault(table_original, []).append(df)
                        tables: dict[str, Any] = {}
                        for table_original, frames in table_frames.items():
                            if not frames:
                                continue
                            if len(frames) == 1:
                                tables[table_original] = frames[0]
                            else:
                                tables[table_original] = pd.concat(frames, ignore_index=True)
                        return tables

                    try:
                        persist_kwargs = {
                            "base_table": base_table,
                            "index_key": index_key,
                            "except_keys": except_keys,
                            "excepted_expand_dict": bool(excepted_expand_dict),
                            "sep": key_sep,
                            "parquet_dir": persist_parquet_dir,
                            "batch_idx": int(batch_idx),
                            "index_offset": int(index_offset),
                            "record_contexts": record_contexts,
                            "parallel_workers": int(parallel_workers or 0),
                            "parallel_table_writes": bool(rust_parallel_table_writes),
                            "columnar_accumulator": bool(rust_columnar_accumulator),
                            "parser_backend": rust_parser_backend,
                            "id_compaction": dict(id_compactor.config) if id_compactor.enabled else None,
                        }
                        if raw_json_lines is not None:
                            rust_result = persist_json_lines_batch_to_parquet(raw_json_lines, **persist_kwargs)
                        else:
                            rust_result = persist_json_batch_to_parquet(batch_records, **persist_kwargs)
                    except RustArrowBackendUnavailable as e:
                        if flatten_backend_requested == BACKEND_RUST_ARROW:
                            rust_explicit_failed = True
                            report.exception(
                                stage="json_pipeline.rust_arrow",
                                message="Rust Arrow backend failed before writing parquet artifacts",
                                exc=e,
                            )
                            q.write(stage="json_pipeline.rust_arrow", record={"table_name": base_table}, exc=e)
                            if not continue_on_error:
                                raise
                            return
                        _record_auto_fallback(f"{type(e).__name__}: {e}", rust_failed=False)
                    except Exception as e:
                        _cleanup_rust_batch_parquet()
                        if flatten_backend_requested == BACKEND_RUST_ARROW:
                            rust_explicit_failed = True
                            report.exception(
                                stage="json_pipeline.rust_arrow",
                                message="Rust Arrow backend failed while writing parquet artifacts",
                                exc=e,
                            )
                            q.write(stage="json_pipeline.rust_arrow", record={"table_name": base_table}, exc=e)
                            if not continue_on_error:
                                raise
                            return
                        _record_auto_fallback(f"{type(e).__name__}: {e}", rust_failed=True)
                    else:
                        if flatten_backend_requested == BACKEND_AUTO:
                            rust_auto_success_batches += 1
                        report.set_artifact("flatten_backend_effective", BACKEND_RUST_ARROW)
                        if raw_json_lines is not None and rust_result.get("parser_backend"):
                            report.set_artifact("rust_parser_backend_effective", str(rust_result.get("parser_backend")))
                        if "parser_fallbacks" in rust_result:
                            report.set_artifact(
                                "rust_parser_fallbacks",
                                int(report.artifacts.get("rust_parser_fallbacks", 0) or 0)
                                + int(rust_result.get("parser_fallbacks") or 0),
                            )
                        timings = (
                            rust_result.get("timings_ms")
                            if isinstance(rust_result.get("timings_ms"), Mapping)
                            else {}
                        )
                        total_ms = int(round((time.perf_counter() - rust_t0) * 1000.0))
                        rust_json_parse_ms = int(timings.get("rust_arrow.json_parse", 0) or 0)
                        py_to_json_ms = int(timings.get("rust_arrow.py_to_json", timings.get("py_to_json_ms", 0)) or 0)
                        flatten_ms = int(timings.get("json.flatten", timings.get("flatten_ms", total_ms)) or 0)
                        parquet_ms = int(timings.get("json.parquet.persist", timings.get("parquet_persist_ms", 0)) or 0)
                        rust_total_ms = int(timings.get("rust_arrow.total", timings.get("total_ms", total_ms)) or 0)
                        if "rust_arrow.json_parse" in timings:
                            report.add_time_ms("rust_arrow.json_parse", rust_json_parse_ms)
                        if py_to_json_ms > 0:
                            report.add_time_ms("rust_arrow.py_to_json", py_to_json_ms)
                        if flatten_ms > 0:
                            report.add_time_ms("json.flatten", flatten_ms)
                        if parquet_ms > 0:
                            report.add_time_ms("json.parquet.persist", parquet_ms)
                        if rust_total_ms > 0:
                            report.add_time_ms("rust_arrow.total", rust_total_ms)
                        for timing_key in _RUST_ARROW_DETAIL_TIMING_KEYS:
                            detail_ms = int(timings.get(timing_key, 0) or 0)
                            if detail_ms > 0:
                                report.add_time_ms(timing_key, detail_ms)
                        rust_unaccounted_ms = _rust_arrow_unaccounted_ms(timings)
                        if rust_unaccounted_ms > 0:
                            report.add_time_ms("rust_arrow.unaccounted_ms", rust_unaccounted_ms)
                        if id_compactor.enabled:
                            id_compactor.merge_summary(rust_result.get("id_compaction"))
                            report.set_artifact("id_compaction", id_compactor.summary())
                        report.bump("records_total", int(batch_record_count))
                        records_ok_value = rust_result.get("records_ok")
                        records_ok = int(records_ok_value) if records_ok_value is not None else int(batch_record_count)
                        report.bump("records_ok", int(records_ok))
                        failed = int(rust_result.get("records_failed") or 0)
                        if failed:
                            report.bump("records_failed", failed)
                            errors = list(rust_result.get("errors") or [])
                            error_indices = list(rust_result.get("error_indices") or [])
                            if raw_json_lines is not None and error_indices:
                                for local_i_raw, err_msg in zip(error_indices, errors):
                                    try:
                                        local_i = int(local_i_raw)
                                    except Exception:
                                        continue
                                    if local_i < 0 or local_i >= len(raw_json_lines):
                                        continue
                                    ctx = dict(record_contexts[local_i]) if record_contexts and local_i < len(record_contexts) else {}
                                    raw_line = raw_json_lines[local_i]
                                    if isinstance(raw_line, (bytes, bytearray)):
                                        raw_text = bytes(raw_line).decode("utf-8", errors="replace").rstrip("\r\n")
                                    else:
                                        raw_text = str(raw_line).rstrip("\r\n")
                                    q_context = {"source": ctx.get("source_path")}
                                    if ctx.get("line_no") is not None:
                                        q_context["line_no"] = ctx.get("line_no")
                                    if ctx.get("source_member") is not None:
                                        q_context["source_member"] = ctx.get("source_member")
                                    q.write(
                                        stage="iter_json_records",
                                        record=raw_text,
                                        index=ctx.get("line_no", local_i),
                                        exc=RuntimeError(str(err_msg)),
                                        **{k: v for k, v in q_context.items() if v is not None},
                                    )
                            report.warn(
                                stage="json_pipeline.rust_arrow",
                                message="Rust Arrow backend reported failed records",
                                failed=failed,
                                errors=errors[:3],
                            )

                        tables_meta = list(rust_result.get("tables") or [])
                        for table_info in tables_meta:
                            if not isinstance(table_info, Mapping):
                                continue
                            table_original = str(table_info.get("table") or "")
                            cols = [str(c) for c in list(table_info.get("columns") or []) if str(c)]
                            if table_original and cols:
                                _record_parquet_manifest_columns(table_original, cols)
                                ensure_name_map(table_original, cols)

                        parquet_tables_written = int(rust_result.get("parquet_tables_written") or len(tables_meta))
                        parquet_files_written = int(rust_result.get("parquet_files_persisted") or parquet_tables_written)
                        parquet_rows_written = int(rust_result.get("parquet_rows_emitted") or 0)
                        report.bump("parquet_files_persisted", parquet_files_written)
                        report.bump("parquet_rows_emitted", parquet_rows_written)
                        parquet_progress = {
                            "tables": int(parquet_tables_written),
                            "files_delta": int(parquet_files_written),
                            "rows_delta": int(parquet_rows_written),
                            "duration_ms": int(parquet_ms or total_ms),
                            "backend": BACKEND_RUST_ARROW,
                        }
                        if parquet_tables_written > 0:
                            report.bump("parquet_batches_total", 1)
                        report.set_artifact("latest_parquet_batch", dict(parquet_progress))
                        if record_contexts:
                            _write_progress_snapshot(
                                stage="parquet_persisted",
                                ctx=record_contexts[-1],
                                extra=_batch_progress_extra(
                                    mode="pre_load" if load else "parse_only",
                                    parquet=parquet_progress,
                                ),
                                force=True,
                            )
                        if create or load or emit_ddl:
                            if overlap_batches:
                                _drain_pending_loads()

                            rust_direct_load = bool(rust_db_load and load and not use_extra_column and not overlap_batches)
                            if rust_db_load and load and use_extra_column:
                                report.set_artifact("rust_db_load_disabled_reason", "schema freeze extra-column mode")
                            if rust_db_load and load and overlap_batches:
                                report.set_artifact("rust_db_load_disabled_reason", "overlap_batches is not supported")

                            rust_load_entries: list[dict[str, Any]] = []
                            if rust_direct_load:
                                report.set_artifact("rust_arrow_db_bridge", "rust_mysql")
                                report.set_artifact("rust_db_load_effective", True)
                                for table_info in tables_meta:
                                    if not isinstance(table_info, Mapping):
                                        continue
                                    table_original = str(table_info.get("table") or "")
                                    cols = [str(c) for c in list(table_info.get("columns") or []) if str(c)]
                                    path = str(table_info.get("path") or "")
                                    if not table_original or not cols or not path:
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
                                                column_descriptions=_column_descriptions_for(table_original),
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
                                            column_descriptions=_column_descriptions_for(table_original),
                                        )
                                        if isinstance(nm_created, NameMap):
                                            name_maps[table_original] = nm_created
                                            nm = nm_created
                                        if nm_created is not None:
                                            created_tables.add(table_original)
                                            report.bump("tables_created", 1)
                                            existing_cols_cache[nm.table_sql] = set(nm.columns_sql)

                                    rust_load_entries.append(
                                        {
                                            "path": path,
                                            "table_original": table_original,
                                            "table_sql": nm.table_sql,
                                            "columns_original": list(cols),
                                            "columns_sql": [nm.map_column(c) for c in cols],
                                        }
                                    )

                                if rust_load_entries:
                                    try:
                                        load_t0 = time.perf_counter()
                                        rust_load_res = load_parquet_files_to_mysql(
                                            rust_load_entries,
                                            db_config=dbc,
                                            batch_size=int(rust_db_load_batch_size),
                                        )
                                        load_ms = int(round((time.perf_counter() - load_t0) * 1000.0))
                                        report.add_time_ms("json.db.load", load_ms)
                                        timings = (
                                            rust_load_res.get("timings_ms")
                                            if isinstance(rust_load_res.get("timings_ms"), Mapping)
                                            else {}
                                        )
                                        rust_mysql_ms = int(timings.get("db.rust_mysql.load", load_ms) or 0)
                                        if rust_mysql_ms > 0:
                                            report.add_time_ms("db.rust_mysql.load", rust_mysql_ms)
                                        report.bump("tables_loaded", int(rust_load_res.get("tables_loaded") or 0))
                                        report.bump("rows_loaded", int(rust_load_res.get("rows_loaded") or 0))
                                        report.bump("rust_db_load_ok", 1)
                                        report.set_artifact(
                                            "latest_rust_db_load",
                                            {
                                                "tables": int(rust_load_res.get("tables_loaded") or 0),
                                                "files": int(rust_load_res.get("files_loaded") or 0),
                                                "rows": int(rust_load_res.get("rows_loaded") or 0),
                                                "duration_ms": int(load_ms),
                                                "transaction": bool(rust_load_res.get("transaction", True)),
                                            },
                                        )
                                    except Exception as e:
                                        report.exception(
                                            stage="json_pipeline.rust_db_load",
                                            message="Rust MySQL loader failed",
                                            exc=e,
                                        )
                                        q.write(stage="json_pipeline.rust_db_load", record={"table_name": base_table}, exc=e)
                                        report.bump("rust_db_load_failed", 1)
                                        if not continue_on_error:
                                            raise
                                        return
                            else:
                                try:
                                    tables = _read_rust_tables_for_db(tables_meta)
                                except Exception as e:
                                    report.exception(
                                        stage="json_pipeline.rust_arrow_db_bridge",
                                        message="Failed to read Rust Arrow parquet artifacts for Python DB stages",
                                        exc=e,
                                    )
                                    q.write(stage="json_pipeline.rust_arrow_db_bridge", record={"table_name": base_table}, exc=e)
                                    if not continue_on_error:
                                        raise
                                    return
                                report.set_artifact("rust_arrow_db_bridge", "parquet_to_dataframe")
                                report.set_artifact("rust_db_load_effective", False)

                            for table_original, df in ({} if rust_direct_load else tables).items():
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
                                            column_descriptions=_column_descriptions_for(table_original),
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
                                        column_descriptions=_column_descriptions_for(table_original),
                                    )
                                    if isinstance(nm_created, NameMap):
                                        name_maps[table_original] = nm_created
                                        nm = nm_created
                                    if nm_created is not None:
                                        created_tables.add(table_original)
                                        report.bump("tables_created", 1)
                                        existing_cols_cache[nm.table_sql] = set(nm.columns_sql)
                                        if use_extra_column and not bool(auto_alter_table_effective):
                                            frozen_allowed_cols_by_table_original[table_original] = set(nm.columns_original)

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
                                        column_descriptions=_column_descriptions_for(table_original),
                                    )
                                    if load_res is not None:
                                        report.bump("tables_loaded", 1)
                                        try:
                                            report.bump("rows_loaded", int(len(df)))
                                        except Exception:
                                            pass

                            if load and record_contexts:
                                _write_progress_snapshot(
                                    stage="loaded",
                                    ctx=record_contexts[-1],
                                    extra=_batch_progress_extra(mode="sync", parquet=parquet_progress),
                                    force=True,
                                )
                            elif record_contexts:
                                _write_progress_snapshot(
                                    stage="batch_done",
                                    ctx=record_contexts[-1],
                                    extra=_batch_progress_extra(mode="parse_only", parquet=parquet_progress),
                                    force=True,
                                )
                        return
                else:
                    effective_backend = BACKEND_PYTHON
                    if (
                        flatten_backend_requested == BACKEND_AUTO
                        and rust_auto_success_batches > 0
                        and rust_auto_disabled_reason
                    ):
                        effective_backend = "mixed"
                    report.set_artifact("flatten_backend_effective", effective_backend)
                    if flatten_backend_requested == BACKEND_AUTO and rust_unsupported:
                        report.set_artifact("flatten_backend_fallback_reason", str(rust_unsupported))
                        report.set_artifact("python_fallback_active", True)

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
                    if "excepted_expand_dict" in extract_params:
                        extract_kwargs["excepted_expand_dict"] = bool(excepted_expand_dict)
                    if "base_table" in extract_params or extract_accepts_kwargs:
                        extract_kwargs["base_table"] = base_table
                    if "id_compactor" in extract_params or extract_accepts_kwargs:
                        extract_kwargs["id_compactor"] = id_compactor

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

                if persist_parquet_files and tables:
                    parquet_files_before = int(report.stats.get("parquet_files_persisted", 0) or 0)
                    parquet_rows_before = int(report.stats.get("parquet_rows_emitted", 0) or 0)
                    parquet_tables_written = 0
                    parquet_t0 = time.perf_counter()
                    try:
                        with report.timer("json.parquet.persist"):
                            for table_original, df in tables.items():
                                cols = list(getattr(df, "columns", []))
                                if not cols:
                                    continue
                                _persist_parquet_table(df, table_original=table_original)
                                parquet_tables_written += 1
                    except Exception as e:
                        report.exception(
                            stage="json_pipeline.parquet_persist",
                            message="Failed to persist parquet artifact; skipping DB work for this batch",
                            exc=e,
                        )
                        if not continue_on_error:
                            raise
                        return
                    parquet_progress = {
                        "tables": int(parquet_tables_written),
                        "files_delta": int(int(report.stats.get("parquet_files_persisted", 0) or 0) - parquet_files_before),
                        "rows_delta": int(int(report.stats.get("parquet_rows_emitted", 0) or 0) - parquet_rows_before),
                        "duration_ms": int(round((time.perf_counter() - parquet_t0) * 1000.0)),
                    }
                    if parquet_tables_written > 0:
                        report.bump("parquet_batches_total", 1)
                    report.set_artifact("latest_parquet_batch", dict(parquet_progress))
                    if record_contexts:
                        _write_progress_snapshot(
                            stage="parquet_persisted",
                            ctx=record_contexts[-1],
                            extra=_batch_progress_extra(
                                mode="pre_load" if load else "parse_only",
                                parquet=parquet_progress,
                            ),
                            force=True,
                        )

                # If the previous batch is still loading, wait before doing any DB work for this batch.
                if overlap_batches:
                    _drain_pending_loads()

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
                                column_descriptions=_column_descriptions_for(table_original),
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
                            column_descriptions=_column_descriptions_for(table_original),
                        )
                        if isinstance(nm_created, NameMap):
                            name_maps[table_original] = nm_created
                            nm = nm_created
                        if nm_created is not None:
                            created_tables.add(table_original)
                            report.bump("tables_created", 1)
                            existing_cols_cache[nm.table_sql] = set(nm.columns_sql)
                            if use_extra_column and not bool(auto_alter_table_effective):
                                frozen_allowed_cols_by_table_original[table_original] = set(nm.columns_original)

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
                            column_descriptions=_column_descriptions_for(table_original),
                        )
                        if load_res is not None:
                            report.bump("tables_loaded", 1)
                            try:
                                report.bump("rows_loaded", int(len(df)))
                            except Exception:
                                pass

                # If we reached here, this batch's DB load completed synchronously.
                # Emit a "loaded" checkpoint so crash recovery can resume within the current shard.
                if load and record_contexts:
                    _write_progress_snapshot(
                        stage="loaded",
                        ctx=record_contexts[-1],
                        extra=_batch_progress_extra(mode="sync", parquet=parquet_progress),
                        force=True,
                    )
                elif record_contexts:
                    _write_progress_snapshot(
                        stage="batch_done",
                        ctx=record_contexts[-1],
                        extra=_batch_progress_extra(mode="parse_only", parquet=parquet_progress),
                        force=True,
                    )

            def _rust_raw_jsonl_disabled_reason() -> str | None:
                if not rust_raw_jsonl_parse_requested:
                    return "not requested"
                if flatten_backend_requested != BACKEND_RUST_ARROW:
                    return "requires flatten_backend=rust-arrow"
                if custom_extract_fn:
                    return "custom extract_fn supplied"
                if not bool(persist_parquet_files):
                    return "requires persist_parquet_files=true"
                if bool(create) or bool(load) or bool(index) or bool(optimize) or bool(emit_ddl):
                    return "requires parse/parquet-only run"
                if bool(use_streaming_rows) or bool(persist_tsv_files):
                    return "requires parquet artifact path"
                if dc.get("_resume_cursor") or dc.get("resume_cursor"):
                    return "resume cursor is not supported by raw JSONL parser"
                if dc.get("records_key") or dc.get("json_records_key"):
                    return "records_key is not supported by raw JSONL parser"
                if bool(excepted_expand_dict):
                    return "excepted_expand_dict=true is not supported by rust-arrow"
                return None

            def _rust_raw_jsonl_sources() -> tuple[list[dict[str, Any]] | None, str | None]:
                from pathlib import Path

                try:
                    source_infos = _resolve_json_sources(dc, report=report, apply_sampling=True)
                except Exception as e:
                    return None, f"failed to resolve JSON sources: {type(e).__name__}: {e}"
                sources: list[dict[str, Any]] = []
                configured_file_type = str(dc.get("file_type") or "").strip().lower()
                for _origin, path in source_infos:
                    file_type = configured_file_type or Path(str(path)).suffix.lstrip(".").lower()
                    if file_type not in {"jsonl", "ndjson", "jsonlines", "gz"}:
                        return None, f"unsupported file_type for raw JSONL parser: {file_type or '<empty>'}"
                    sources.append({"path": path, "file_type": file_type})
                if not sources:
                    return None, "no JSONL sources resolved"
                return sources, None

            raw_jsonl_disabled_reason = _rust_raw_jsonl_disabled_reason()
            raw_jsonl_sources = None
            if raw_jsonl_disabled_reason is None:
                raw_jsonl_sources, raw_jsonl_disabled_reason = _rust_raw_jsonl_sources()
            if raw_jsonl_disabled_reason and rust_raw_jsonl_parse_requested:
                report.set_artifact("rust_raw_jsonl_parse_disabled_reason", raw_jsonl_disabled_reason)
            report.set_artifact("rust_raw_jsonl_parse_effective", raw_jsonl_disabled_reason is None)
            raw_jsonl_file_disabled_reason = None
            if not rust_raw_jsonl_file_parse_requested:
                raw_jsonl_file_disabled_reason = "not requested"
            elif raw_jsonl_disabled_reason is not None:
                raw_jsonl_file_disabled_reason = raw_jsonl_disabled_reason
            elif raw_jsonl_sources is not None and any(
                str(item.get("file_type") or "").lower() == "gz" for item in raw_jsonl_sources
            ):
                raw_jsonl_file_disabled_reason = "gz sources are not supported by direct Rust JSONL file parser"
            elif id_compactor.enabled:
                raw_jsonl_file_disabled_reason = "id_compaction is not supported by direct Rust JSONL file parser"
            if raw_jsonl_file_disabled_reason and rust_raw_jsonl_file_parse_requested:
                report.set_artifact("rust_raw_jsonl_file_parse_disabled_reason", raw_jsonl_file_disabled_reason)
            report.set_artifact("rust_raw_jsonl_file_parse_effective", raw_jsonl_file_disabled_reason is None)

            batch_index_offset = 0
            if raw_jsonl_file_disabled_reason is None and raw_jsonl_sources is not None:
                import time

                try:
                    rust_t0 = time.perf_counter()
                    rust_result = persist_jsonl_sources_to_parquet(
                        [item["path"] for item in raw_jsonl_sources],
                        base_table=base_table,
                        index_key=index_key,
                        except_keys=except_keys,
                        excepted_expand_dict=bool(excepted_expand_dict),
                        sep=key_sep,
                        parquet_dir=persist_parquet_dir,
                        batch_idx=int(batch_no),
                        index_offset=int(global_index),
                        parallel_workers=int(parallel_workers or 0),
                        parallel_table_writes=bool(rust_parallel_table_writes),
                        columnar_accumulator=bool(rust_columnar_accumulator),
                        parser_backend=rust_parser_backend,
                        chunk_size=int(chunk_size),
                        parquet_flush_records=int(rust_parquet_flush_records or 0),
                        max_records=max_records,
                        id_compaction=None,
                    )
                except Exception as e:
                    rust_explicit_failed = True
                    report.exception(
                        stage="json_pipeline.rust_arrow_file_parse",
                        message="Rust Arrow direct JSONL file parser failed",
                        exc=e,
                    )
                    q.write(stage="json_pipeline.rust_arrow_file_parse", record={"table_name": base_table}, exc=e)
                    if not continue_on_error:
                        raise
                else:
                    report.set_artifact("flatten_backend_effective", BACKEND_RUST_ARROW)
                    if rust_result.get("parser_backend"):
                        report.set_artifact("rust_parser_backend_effective", str(rust_result.get("parser_backend")))
                    if "parser_fallbacks" in rust_result:
                        report.set_artifact(
                            "rust_parser_fallbacks",
                            int(report.artifacts.get("rust_parser_fallbacks", 0) or 0)
                            + int(rust_result.get("parser_fallbacks") or 0),
                        )
                    timings = rust_result.get("timings_ms") if isinstance(rust_result.get("timings_ms"), Mapping) else {}
                    total_ms = int(round((time.perf_counter() - rust_t0) * 1000.0))
                    rust_json_parse_ms = int(timings.get("rust_arrow.json_parse", 0) or 0)
                    flatten_ms = int(timings.get("json.flatten", 0) or 0)
                    parquet_ms = int(timings.get("json.parquet.persist", 0) or 0)
                    rust_total_ms = int(timings.get("rust_arrow.total", total_ms) or 0)
                    if rust_json_parse_ms > 0:
                        report.add_time_ms("rust_arrow.json_parse", rust_json_parse_ms)
                    if flatten_ms > 0:
                        report.add_time_ms("json.flatten", flatten_ms)
                    if parquet_ms > 0:
                        report.add_time_ms("json.parquet.persist", parquet_ms)
                    if rust_total_ms > 0:
                        report.add_time_ms("rust_arrow.total", rust_total_ms)
                    for timing_key in _RUST_ARROW_DETAIL_TIMING_KEYS:
                        detail_ms = int(timings.get(timing_key, 0) or 0)
                        if detail_ms > 0:
                            report.add_time_ms(timing_key, detail_ms)
                    rust_unaccounted_ms = _rust_arrow_unaccounted_ms(timings)
                    if rust_unaccounted_ms > 0:
                        report.add_time_ms("rust_arrow.unaccounted_ms", rust_unaccounted_ms)
                    records_read = int(rust_result.get("records_read") or 0)
                    bytes_read = int(rust_result.get("bytes_read") or 0)
                    records_ok = int(rust_result.get("records_ok") or 0)
                    failed = int(rust_result.get("records_failed") or 0)
                    report.bump("records_read", records_read)
                    report.bump("records_total", records_read)
                    report.bump("records_ok", records_ok)
                    if bytes_read:
                        report.bump("io_bytes_read", bytes_read)
                    if failed:
                        report.bump("records_failed", failed)
                        error_records = list(rust_result.get("error_records") or [])
                        for item in error_records:
                            if not isinstance(item, Mapping):
                                continue
                            q.write(
                                stage="iter_json_records",
                                record=str(item.get("raw_line") or ""),
                                index=item.get("line_no", item.get("record_index")),
                                exc=RuntimeError(str(item.get("error") or "Rust JSONL parse failed")),
                                source=item.get("source_path"),
                                line_no=item.get("line_no"),
                            )
                        report.warn(
                            stage="json_pipeline.rust_arrow_file_parse",
                            message="Rust Arrow direct JSONL parser reported failed records",
                            failed=failed,
                            errors=list(rust_result.get("errors") or [])[:3],
                        )

                    tables_meta = list(rust_result.get("tables") or [])
                    for table_info in tables_meta:
                        if not isinstance(table_info, Mapping):
                            continue
                        table_original = str(table_info.get("table") or "")
                        cols = [str(c) for c in list(table_info.get("columns") or []) if str(c)]
                        if table_original and cols:
                            _record_parquet_manifest_columns(table_original, cols)
                            ensure_name_map(table_original, cols)

                    parquet_tables_written = int(rust_result.get("parquet_tables_written") or len(tables_meta))
                    parquet_files_written = int(rust_result.get("parquet_files_persisted") or parquet_tables_written)
                    parquet_rows_written = int(rust_result.get("parquet_rows_emitted") or 0)
                    parquet_batches_total = int(rust_result.get("parquet_batches_total") or 0)
                    report.bump("parquet_files_persisted", parquet_files_written)
                    report.bump("parquet_rows_emitted", parquet_rows_written)
                    if parquet_batches_total:
                        report.bump("parquet_batches_total", parquet_batches_total)
                    batch_no += parquet_batches_total
                    global_index += records_read
                    parquet_progress = {
                        "tables": int(parquet_tables_written),
                        "files_delta": int(parquet_files_written),
                        "rows_delta": int(parquet_rows_written),
                        "duration_ms": int(parquet_ms or total_ms),
                        "backend": BACKEND_RUST_ARROW,
                        "direct_jsonl_file_parse": True,
                    }
                    report.set_artifact("latest_parquet_batch", dict(parquet_progress))
            elif raw_jsonl_disabled_reason is None and raw_jsonl_sources is not None:
                raw_batch: list[bytes | str] = []
                reached_max_records = False
                for raw_source_info in raw_jsonl_sources:
                    if reached_max_records or rust_explicit_failed:
                        break
                    raw_source = raw_source_info["path"]
                    raw_file_type = str(raw_source_info.get("file_type") or "").lower()
                    if raw_file_type == "gz":
                        import gzip

                        raw_fp_cm = gzip.open(raw_source, "rb")
                    else:
                        raw_fp_cm = open(raw_source, "rb")
                    with raw_fp_cm as raw_fp:
                        for line_no, raw_line in enumerate(raw_fp, start=1):
                            if not raw_line or raw_line.isspace():
                                continue
                            if max_records is not None and global_index >= max_records:
                                reached_max_records = True
                                break
                            try:
                                report.bump("io_bytes_read", int(len(raw_line)))
                            except Exception:
                                pass
                            rec_ctx = {
                                "source_path": str(raw_source),
                                "line_no": int(line_no),
                                "record_index": int(global_index),
                            }
                            report.bump("records_read", 1)
                            _write_progress_snapshot(stage="read", ctx=rec_ctx)
                            if not raw_batch:
                                batch_index_offset = global_index
                            raw_batch.append(raw_line)
                            batch_contexts.append(rec_ctx)
                            global_index += 1
                            if len(raw_batch) >= chunk_size:
                                flush_batch(
                                    [],
                                    index_offset=batch_index_offset,
                                    record_contexts=batch_contexts,
                                    raw_json_lines=raw_batch,
                                )
                                raw_batch = []
                                batch_contexts = []
                                if rust_explicit_failed:
                                    break

                if raw_batch and not rust_explicit_failed:
                    flush_batch(
                        [],
                        index_offset=batch_index_offset,
                        record_contexts=batch_contexts,
                        raw_json_lines=raw_batch,
                    )
            else:
                try:
                    record_iter = _iter_json_records(
                        dc,
                        report=report,
                        quarantine=q,
                        max_records=max_records,
                        with_context=True,
                    )
                except TypeError as e:
                    if "quarantine" not in str(e):
                        raise
                    record_iter = _iter_json_records(
                        dc,
                        report=report,
                        max_records=max_records,
                        with_context=True,
                    )
                for rec_out in record_iter:
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
                    _write_progress_snapshot(stage="read", ctx=rec_ctx)
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

            # Ensure any overlapped batch loads are finished before finalize steps.
            _drain_pending_loads()

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

            rust_json_parse_ms = int(report.timings_ms.get("rust_arrow.json_parse", 0) or 0)
            if rust_json_parse_ms > 0:
                tp["rust_arrow.json_parse.records_per_s"] = per_s(records_read, rust_json_parse_ms)

            rust_read_line_ms = int(report.timings_ms.get("rust_arrow.read_line", 0) or 0)
            if rust_read_line_ms > 0:
                tp["rust_arrow.read_line.records_per_s"] = per_s(records_read, rust_read_line_ms)
                if bytes_read > 0:
                    tp["rust_arrow.read_line.mb_per_s"] = (float(bytes_read) / (1024.0 * 1024.0)) / (
                        float(rust_read_line_ms) / 1000.0
                    )

            rust_number_validate_ms = int(report.timings_ms.get("rust_arrow.number_validate", 0) or 0)
            if rust_number_validate_ms > 0:
                tp["rust_arrow.number_validate.records_per_s"] = per_s(records_ok or records_read, rust_number_validate_ms)

            flatten_ms = int(report.timings_ms.get("json.flatten", 0) or 0)
            if flatten_ms > 0:
                tp["json.flatten.records_per_s"] = per_s(records_ok or records_read, flatten_ms)

            parquet_files_persisted = int(report.stats.get("parquet_files_persisted", 0) or 0)
            parquet_rows_emitted = int(report.stats.get("parquet_rows_emitted", 0) or 0)
            parquet_batches_total = int(report.stats.get("parquet_batches_total", 0) or 0)

            rust_columnar_merge_ms = int(report.timings_ms.get("rust_arrow.columnar_merge", 0) or 0)
            if rust_columnar_merge_ms > 0:
                tp["rust_arrow.columnar_merge.rows_per_s"] = per_s(parquet_rows_emitted, rust_columnar_merge_ms)

            parquet_persist_ms = int(report.timings_ms.get("json.parquet.persist", 0) or 0)
            if parquet_persist_ms > 0:
                tp["json.parquet.persist.files_per_s"] = per_s(parquet_files_persisted, parquet_persist_ms)
                tp["json.parquet.persist.rows_per_s"] = per_s(parquet_rows_emitted, parquet_persist_ms)
                tp["json.parquet.persist.batches_per_s"] = per_s(parquet_batches_total, parquet_persist_ms)

            rust_arrow_build_ms = int(report.timings_ms.get("rust_arrow.arrow_build", 0) or 0)
            if rust_arrow_build_ms > 0:
                tp["rust_arrow.arrow_build.rows_per_s"] = per_s(parquet_rows_emitted, rust_arrow_build_ms)

            rust_parquet_write_ms = int(report.timings_ms.get("rust_arrow.parquet_write", 0) or 0)
            if rust_parquet_write_ms > 0:
                tp["rust_arrow.parquet_write.files_per_s"] = per_s(parquet_files_persisted, rust_parquet_write_ms)
                tp["rust_arrow.parquet_write.rows_per_s"] = per_s(parquet_rows_emitted, rust_parquet_write_ms)

            rust_py_to_json_ms = int(report.timings_ms.get("rust_arrow.py_to_json", 0) or 0)
            if rust_py_to_json_ms > 0:
                tp["rust_arrow.py_to_json.records_per_s"] = per_s(records_read, rust_py_to_json_ms)

            rust_py_result_convert_ms = int(report.timings_ms.get("rust_arrow.py_result_convert", 0) or 0)
            if rust_py_result_convert_ms > 0:
                tp["rust_arrow.py_result_convert.rows_per_s"] = per_s(parquet_rows_emitted, rust_py_result_convert_ms)

            rust_total_ms = int(report.timings_ms.get("rust_arrow.total", 0) or 0)
            if rust_total_ms > 0:
                tp["rust_arrow.total.records_per_s"] = per_s(records_ok or records_read, rust_total_ms)

            load_ms = int(report.timings_ms.get("json.db.load", 0) or 0)
            if load_ms > 0:
                tp["json.db.load.rows_per_s"] = per_s(rows_loaded, load_ms)

            load_exec_ms = int(report.timings_ms.get("db.load_data.exec", 0) or 0)
            if load_exec_ms > 0:
                tp["db.load_data.exec.rows_per_s"] = per_s(rows_loaded, load_exec_ms)

            report.set_artifact("throughput", {k: v for k, v in tp.items() if v is not None})
        except Exception:
            pass
        if persist_tsv_files:
            report.set_artifact("persist_tsv_dir", str(persist_tsv_dir))
            report.set_artifact("persist_tsv_files_count", int(report.stats.get("tsv_files_persisted", 0) or 0))
            report.set_artifact("rows_emitted", int(report.stats.get("rows_emitted", 0) or 0))
        if persist_parquet_files:
            report.set_artifact("persist_parquet_dir", str(persist_parquet_dir))
            report.set_artifact("persist_parquet_batches_count", int(report.stats.get("parquet_batches_total", 0) or 0))
            report.set_artifact("persist_parquet_files_count", int(report.stats.get("parquet_files_persisted", 0) or 0))
            report.set_artifact("parquet_rows_emitted", int(report.stats.get("parquet_rows_emitted", 0) or 0))
        if id_compactor.enabled:
            summary = id_compactor.summary()
            if summary.get("ambiguous_columns"):
                report.warn(
                    stage="id_compaction",
                    message="Skipped ambiguous URL-like columns during ID compaction",
                    columns=list((summary.get("ambiguous_columns") or {}).keys())[:20],
                )
            if summary.get("collisions"):
                report.warn(
                    stage="id_compaction",
                    message="Column collisions occurred during ID compaction; original columns were preserved",
                    columns=list((summary.get("collisions") or {}).keys())[:20],
                )
        _write_schema_manifest()
        maybe_update_artifacts()
        return JsonRunResult(name_maps=name_maps, report=report)
    finally:
        report.add_time_s("pipeline.json.total", time.perf_counter() - t_total0)
        if load_executor is not None:
            try:
                load_executor.shutdown(wait=True, cancel_futures=True)
            except Exception:
                pass
            for c in list(load_conns):
                try:
                    c.close()
                except Exception:
                    pass
        if flatten_executor is not None:
            try:
                flatten_executor.shutdown(wait=True, cancel_futures=True)
            except Exception:
                pass
        if local_infile_conn is not None:
            try:
                local_infile_conn.close()
            except Exception:
                pass
