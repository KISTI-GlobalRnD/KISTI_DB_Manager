from __future__ import annotations

import hashlib
import json
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .config import coerce_data_config, join_path
from .namemap import NameMap, load_namemap
from .naming import canonicalize_column_names


DESCRIPTION_PROFILE_SCHEMA_VERSION = "2.0"

DESC_V2_COLUMNS = [
    "source_column",
    "sql_column",
    "description",
    "suggested_type",
    "type_family",
    "type_confidence",
    "type_reason",
    "row_count",
    "non_null_count",
    "null_count",
    "null_ratio",
    "empty_string_count",
    "empty_string_ratio",
    "min_len",
    "max_len",
    "p95_len",
    "max_byte_len",
    "numeric_min",
    "numeric_max",
    "date_min",
    "date_max",
    "unique_count",
    "unique_ratio",
    "top_value",
    "top_freq_ratio",
    "is_key_candidate",
    "index_recommended",
    "warnings",
    "Description",
    "Type",
    "Null_ratio",
    "is_key",
]


@dataclass(frozen=True)
class DescriptionProfileResult:
    desc_csv_path: Path
    profile_json_path: Path
    desc_rows: Any
    profile: dict[str, Any]
    name_map: NameMap


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    try:
        import pandas as pd

        if pd.isna(value):
            return None
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
    except Exception:
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _float_or_none(value: Any) -> float | None:
    safe = _json_safe(value)
    if safe is None:
        return None
    try:
        return float(safe)
    except Exception:
        return None


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _source_path(data_config: Mapping[str, Any]) -> Path:
    return Path(join_path(data_config["PATH"], data_config["file_name"])).expanduser().resolve()


def _default_desc_path(data_config: Mapping[str, Any]) -> Path:
    return Path(join_path(data_config["PATH"], f"{data_config['table_name']}_Desc.csv")).expanduser().resolve()


def _default_profile_path(data_config: Mapping[str, Any]) -> Path:
    return Path(join_path(data_config["PATH"], f"{data_config['table_name']}_profile.json")).expanduser().resolve()


def _varchar_or_text_type(max_byte_len: int, *, extra_ratio: float) -> str:
    import math

    if max_byte_len <= 0:
        return "VARCHAR(1)"
    required = max(1, int(math.ceil(max_byte_len * float(extra_ratio))))
    if required <= 64:
        power = 2 ** int(math.ceil(math.log(max(1, required), 2)))
        return f"VARCHAR({power})"
    if required <= 65_535:
        return "TEXT"
    if required <= 16_777_215:
        return "MEDIUMTEXT"
    return "LONGTEXT"


def _integer_type(min_value: float, max_value: float, *, min_year: int, max_year: int) -> str:
    if min_value > min_year and max_value < max_year:
        return "YEAR"
    unsigned = min_value >= 0
    limits = [
        ("TINYINT", 255 if unsigned else 127),
        ("SMALLINT", 65_535 if unsigned else 32_767),
        ("MEDIUMINT", 16_777_215 if unsigned else 8_388_607),
        ("INT", 4_294_967_295 if unsigned else 2_147_483_647),
        ("BIGINT", 18_446_744_073_709_551_615 if unsigned else 9_223_372_036_854_775_807),
    ]
    for name, limit in limits:
        if max_value <= limit and (unsigned or min_value >= -limit - 1):
            return f"{name} UNSIGNED" if unsigned else name
    return "BIGINT UNSIGNED" if unsigned else "BIGINT"


def _is_integral(values: Any) -> bool:
    try:
        return bool(((values % 1) == 0).all())
    except Exception:
        return False


def _looks_date_like(values: list[str]) -> bool:
    for value in values[:1000]:
        text = value.strip()
        if any(marker in text for marker in ("-", "/", ":", "T")):
            return True
    return False


def _bool_parse_success(values: list[str]) -> bool:
    allowed = {"0", "1", "true", "false", "t", "f", "yes", "no", "y", "n"}
    return bool(values) and all(value.strip().lower() in allowed for value in values)


def _profile_series(
    source_column: str,
    series: Any,
    *,
    sql_column: str,
    forced_key: bool,
    params: Mapping[str, Any],
) -> dict[str, Any]:
    import pandas as pd

    extra_ratio = float(params.get("Extra_ratio", params.get("extra_ratio", 1.5)))
    min_year = int(params.get("Min_Year", params.get("min_year", 1900)))
    max_year = int(params.get("Max_Year", params.get("max_year", 2100)))

    row_count = int(series.size)
    null_mask = series.isna()
    null_count = int(null_mask.sum())
    non_null = series[~null_mask]
    non_null_count = int(non_null.size)

    string_values = non_null.map(lambda value: str(value))
    stripped = string_values.map(lambda value: value.strip())
    empty_string_count = int((stripped == "").sum())
    data_values = stripped[stripped != ""]
    data_count = int(data_values.size)
    data_list = [str(value) for value in data_values.tolist()]

    lengths = string_values.map(len) if non_null_count else pd.Series([], dtype="int64")
    byte_lengths = string_values.map(lambda value: len(value.encode("utf-8"))) if non_null_count else pd.Series([], dtype="int64")
    min_len = int(lengths.min()) if non_null_count else None
    max_len = int(lengths.max()) if non_null_count else None
    p95_len = float(lengths.quantile(0.95)) if non_null_count else None
    max_byte_len = int(byte_lengths.max()) if non_null_count else 0

    unique_count = int(data_values.nunique(dropna=True)) if data_count else 0
    unique_ratio = float(unique_count / row_count) if row_count else None
    top_value = None
    top_freq_ratio = None
    if data_count:
        counts = data_values.value_counts(dropna=True)
        top_value = _json_safe(counts.index[0])
        top_freq_ratio = float(counts.iloc[0] / row_count) if row_count else None

    numeric_min = None
    numeric_max = None
    date_min = None
    date_max = None
    warning_flags: list[str] = []
    type_family = "empty"
    type_reason = "no_non_empty_values"
    type_confidence = 0.0

    if data_count == 0:
        suggested_type = "LONGTEXT"
        warning_flags.append("no_non_empty_values")
    elif _bool_parse_success(data_list):
        suggested_type = "BOOLEAN"
        type_family = "boolean"
        type_reason = "all_non_empty_values_parse_as_boolean_literals"
        type_confidence = 1.0
    else:
        numeric = pd.to_numeric(data_values, errors="coerce")
        numeric_success_count = int(numeric.notna().sum())
        numeric_success_ratio = float(numeric_success_count / data_count) if data_count else 0.0
        if numeric_success_ratio == 1.0:
            numeric_min = _float_or_none(numeric.min())
            numeric_max = _float_or_none(numeric.max())
            if _is_integral(numeric):
                suggested_type = _integer_type(float(numeric.min()), float(numeric.max()), min_year=min_year, max_year=max_year)
                type_family = "integer" if suggested_type != "YEAR" else "year"
                type_reason = "all_non_empty_values_parse_as_integer"
            else:
                suggested_type = "DOUBLE"
                type_family = "float"
                type_reason = "all_non_empty_values_parse_as_float"
            type_confidence = 1.0
        else:
            date_success_ratio = 0.0
            if _looks_date_like(data_list):
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", UserWarning)
                    dates = pd.to_datetime(data_values, errors="coerce")
                date_success_count = int(dates.notna().sum())
                date_success_ratio = float(date_success_count / data_count) if data_count else 0.0
                if date_success_ratio == 1.0:
                    date_min = _json_safe(dates.min())
                    date_max = _json_safe(dates.max())
                    suggested_type = "DATETIME"
                    type_family = "datetime"
                    type_reason = "all_non_empty_values_parse_as_datetime"
                    type_confidence = 1.0
                else:
                    if date_success_count:
                        warning_flags.append("mixed_datetime_parse_success")
                    suggested_type = _varchar_or_text_type(max_byte_len, extra_ratio=extra_ratio)
                    type_family = "text" if "TEXT" in suggested_type else "string"
                    type_reason = "fallback_string_type_due_to_mixed_datetime_values"
                    type_confidence = max(0.1, 1.0 - date_success_ratio)
            else:
                if numeric_success_count:
                    warning_flags.append("mixed_numeric_parse_success")
                suggested_type = _varchar_or_text_type(max_byte_len, extra_ratio=extra_ratio)
                type_family = "text" if "TEXT" in suggested_type else "string"
                type_reason = "fallback_string_type"
                type_confidence = max(0.1, 1.0 - numeric_success_ratio)

    if null_count:
        warning_flags.append("contains_nulls")
    if empty_string_count:
        warning_flags.append("contains_empty_strings")
    if max_byte_len > 65_535:
        warning_flags.append("very_large_text_values")

    null_ratio = float(null_count / row_count) if row_count else None
    empty_string_ratio = float(empty_string_count / row_count) if row_count else None
    full_non_empty = bool(row_count and null_count == 0 and empty_string_count == 0)
    all_unique = bool(row_count and unique_count == row_count)
    key_min_rows = int(params.get("key_min_rows", 20))
    text_index_safe = max_byte_len <= int(params.get("index_prefix_len", 191))
    if all_unique and not forced_key and row_count < key_min_rows:
        warning_flags.append("unique_in_small_sample")
    is_key_candidate = bool(forced_key or (full_non_empty and all_unique and row_count >= key_min_rows))
    index_recommended = bool(is_key_candidate and (type_family not in {"text"} or text_index_safe))

    row = {
        "source_column": source_column,
        "sql_column": sql_column,
        "description": "",
        "suggested_type": suggested_type,
        "type_family": type_family,
        "type_confidence": round(float(type_confidence), 6),
        "type_reason": type_reason,
        "row_count": row_count,
        "non_null_count": non_null_count,
        "null_count": null_count,
        "null_ratio": null_ratio,
        "empty_string_count": empty_string_count,
        "empty_string_ratio": empty_string_ratio,
        "min_len": min_len,
        "max_len": max_len,
        "p95_len": p95_len,
        "max_byte_len": max_byte_len,
        "numeric_min": numeric_min,
        "numeric_max": numeric_max,
        "date_min": date_min,
        "date_max": date_max,
        "unique_count": unique_count,
        "unique_ratio": unique_ratio,
        "top_value": top_value,
        "top_freq_ratio": top_freq_ratio,
        "is_key_candidate": is_key_candidate,
        "index_recommended": index_recommended,
        "warnings": ";".join(sorted(set(warning_flags))),
        "Description": "",
        "Type": suggested_type,
        "Null_ratio": null_ratio,
        "is_key": is_key_candidate,
    }
    return {key: _json_safe(row.get(key)) for key in DESC_V2_COLUMNS}


def build_description_profile(
    data_config: Mapping[str, Any],
    *,
    params: Mapping[str, Any] | None = None,
    backend: str = "python",
    name_map: NameMap | Mapping[str, Any] | None = None,
) -> tuple[Any, dict[str, Any], NameMap]:
    if backend not in {"auto", "python"}:
        raise ValueError(f"unsupported description backend: {backend}")

    from .preview import read_data_from_tabular

    dc = coerce_data_config(data_config)
    params = dict(params or {})
    df = read_data_from_tabular(dc)

    key_sep = dc.get("KEY_SEP", "__")
    nm = load_namemap(name_map) or load_namemap(dc.get("_name_map"))
    if nm is None:
        source_columns = canonicalize_column_names(df.columns, key_sep=key_sep)
        nm = NameMap.build(table_name=dc["table_name"], columns=source_columns, key_sep=key_sep)
    else:
        source_columns = nm.canonicalize_input_columns(df.columns)
        nm = nm.with_additional_columns(source_columns)

    raw_to_source: dict[str, str] = {}
    for original_column, source_column in zip(df.columns, source_columns):
        raw_to_source.setdefault(str(original_column), source_column)

    def resolve_forced_key(key: Any) -> str:
        key_s = str(key)
        return raw_to_source.get(key_s) or key_s.replace(".", key_sep)

    forced_keys = {resolve_forced_key(key) for key in dc.get("KEYs", []) if str(key)}
    if dc.get("KEY"):
        forced_keys.add(resolve_forced_key(dc["KEY"]))
    rows: list[dict[str, Any]] = []
    for original_column, source_column in zip(df.columns, source_columns):
        series = df[original_column]
        rows.append(
            _profile_series(
                source_column,
                series,
                sql_column=nm.map_column(source_column),
                forced_key=source_column in forced_keys,
                params=params,
            )
        )

    import pandas as pd

    desc_df = pd.DataFrame(rows, columns=DESC_V2_COLUMNS)
    desc_df.index = desc_df["source_column"]
    desc_df.index.name = None

    source = _source_path(dc)
    profile = {
        "schema_version": DESCRIPTION_PROFILE_SCHEMA_VERSION,
        "generated_at": _iso_now(),
        "backend": "python",
        "sampling_policy": {"mode": "full"},
        "source": {
            "file": str(source),
            "size_bytes": source.stat().st_size if source.exists() else None,
            "sha256": _sha256_file(source) if source.exists() else None,
            "row_count": int(len(df)),
            "table_name": str(dc.get("table_name") or ""),
        },
        "name_map": nm.to_dict(),
        "columns": rows,
        "warnings": sorted({warning for row in rows for warning in str(row.get("warnings") or "").split(";") if warning}),
    }
    return desc_df, profile, nm


def write_description_profile(
    data_config: Mapping[str, Any],
    *,
    params: Mapping[str, Any] | None = None,
    backend: str = "python",
    desc_csv_path: str | Path | None = None,
    profile_json_path: str | Path | None = None,
    name_map: NameMap | Mapping[str, Any] | None = None,
) -> DescriptionProfileResult:
    dc = coerce_data_config(data_config)
    desc_df, profile, nm = build_description_profile(dc, params=params, backend=backend, name_map=name_map)
    desc_path = Path(desc_csv_path).expanduser().resolve() if desc_csv_path else _default_desc_path(dc)
    profile_path = Path(profile_json_path).expanduser().resolve() if profile_json_path else _default_profile_path(dc)
    desc_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    desc_df.to_csv(desc_path)
    profile_path.write_text(json.dumps(profile, ensure_ascii=False, indent=2, default=_json_safe), encoding="utf-8")
    return DescriptionProfileResult(
        desc_csv_path=desc_path,
        profile_json_path=profile_path,
        desc_rows=desc_df,
        profile=profile,
        name_map=nm,
    )
