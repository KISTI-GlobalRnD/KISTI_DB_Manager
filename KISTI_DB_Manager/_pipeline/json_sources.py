from __future__ import annotations

import re
from typing import Any, Mapping

from ..config import coerce_data_config
from ..quarantine import NullQuarantineWriter, QuarantineWriter
from ..report import RunReport


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

    from ..config import join_path

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
