from __future__ import annotations

import hashlib
import re
from typing import Iterable


MYSQL_IDENTIFIER_MAX_LEN = 64


def sanitize_identifier(name: str, *, replacement: str = "_") -> str:
    """
    Make a best-effort safe identifier for MySQL/MariaDB:
    - Replace invalid characters with `_`
    - Collapse repeats
    - Strip leading/trailing underscores
    """
    if not name:
        return ""
    safe = re.sub(r"[^0-9A-Za-z_]+", replacement, str(name))
    safe = re.sub(rf"{re.escape(replacement)}+", replacement, safe)
    return safe.strip(replacement)


def quote_mysql_identifier(name: str) -> str:
    """Quote a MySQL/MariaDB identifier and escape embedded backticks."""
    return f"`{str(name).replace('`', '``')}`"


def _truncate_with_hash(value: str, *, max_len: int, fallback_prefix: str = "col") -> str:
    value = str(value)
    if max_len <= 0:
        raise ValueError("max_len must be positive")
    if len(value) <= max_len and value:
        return value
    digest = hashlib.md5(value.encode("utf-8")).hexdigest()[:8]
    suffix = f"_{digest}"
    if len(suffix) >= max_len:
        return digest[:max_len]
    head = value[: max_len - len(suffix)].rstrip("_")
    if not head:
        head = str(fallback_prefix or "col")[: max_len - len(suffix)].rstrip("_")
    if not head:
        head = "c"[: max_len - len(suffix)]
    return f"{head}{suffix}"[:max_len]


def _join_collision_suffix(
    base: str,
    suffix: str,
    *,
    suffix_sep: str,
    max_len: int | None,
    fallback_prefix: str,
) -> str:
    suffix = sanitize_identifier(str(suffix), replacement="_") or "dup"
    joiner = suffix_sep or "_"
    suffix_text = f"{joiner}{suffix}"
    base = str(base or fallback_prefix or "col")

    if max_len is None:
        head = base.rstrip("_") or str(fallback_prefix or "col")
        return f"{head}{suffix_text}"

    if len(suffix_text) >= max_len:
        digest = hashlib.md5(f"{base}{suffix_text}".encode("utf-8")).hexdigest()[:8]
        return _truncate_with_hash(f"{fallback_prefix}_{digest}", max_len=max_len, fallback_prefix=fallback_prefix)

    head_len = max_len - len(suffix_text)
    fallback = str(fallback_prefix or "col")
    head = base[:head_len].rstrip("_") or fallback[:head_len].rstrip("_") or "c"
    return f"{head}{suffix_text}"[:max_len]


def _canonical_collision_hint(raw_name: str, *, key_sep: str, duplicate_index: int) -> str | None:
    if duplicate_index > 1:
        if "." in raw_name:
            return f"dot_dup{duplicate_index}"
        return f"dup{duplicate_index}"
    if "." in raw_name:
        return "dot"
    if key_sep and key_sep in raw_name:
        return "raw"
    return None


def _canonical_group_hint(raw_name: str, *, key_sep: str, duplicate_index: int, group_size: int) -> str | None:
    if duplicate_index > 1:
        if "." in raw_name:
            return f"dot_dup{duplicate_index}"
        return f"dup{duplicate_index}"
    if group_size <= 1:
        return None
    return _canonical_collision_hint(raw_name, key_sep=key_sep, duplicate_index=duplicate_index)


def _leading_collision_hint(raw_name: str, *, sep: str, duplicate_index: int) -> str:
    if duplicate_index > 1:
        return f"dup{duplicate_index}"

    parts = [part for part in re.split(rf"{re.escape(sep)}|[._]+", str(raw_name)) if part]
    for part in parts:
        hint = sanitize_identifier(part, replacement="_")
        if hint and not hint.isdigit():
            return hint[:16]

    digest = hashlib.md5(str(raw_name).encode("utf-8")).hexdigest()[:8]
    return f"h{digest}"


def _dedupe_name(
    name: str,
    existing: set[str],
    *,
    max_len: int | None = None,
    fallback_prefix: str = "col",
    collision_hint: str | None = None,
    suffix_sep: str = "_",
    prefer_hint: bool = False,
) -> str:
    if not name:
        name = str(fallback_prefix or "col")
    if max_len is not None and len(name) > max_len:
        name = _truncate_with_hash(name, max_len=max_len, fallback_prefix=fallback_prefix)
    if name and name not in existing and not prefer_hint:
        return name

    base = name or str(fallback_prefix or "col")
    hint = sanitize_identifier(str(collision_hint), replacement="_") if collision_hint else ""
    if hint:
        candidate = _join_collision_suffix(
            base,
            hint,
            suffix_sep=suffix_sep,
            max_len=max_len,
            fallback_prefix=fallback_prefix,
        )
        if candidate and candidate not in existing:
            return candidate

    if name and name not in existing:
        return name

    counter = 2
    while True:
        suffix = f"{hint}_dup{counter}" if hint and not hint.startswith("dup") else f"dup{counter}"
        candidate = _join_collision_suffix(
            base,
            suffix,
            suffix_sep=suffix_sep,
            max_len=max_len,
            fallback_prefix=fallback_prefix,
        )
        if candidate and candidate not in existing:
            return candidate
        counter += 1
        if counter > 9999:
            digest = hashlib.md5(f"{base}:{len(existing)}:{counter}".encode("utf-8")).hexdigest()[:8]
            candidate = f"{str(fallback_prefix or 'col')}_{digest}"
            if max_len is not None:
                candidate = _truncate_with_hash(candidate, max_len=max_len, fallback_prefix=fallback_prefix)
            if candidate not in existing:
                return candidate
            counter += 1


def canonicalize_column_names(
    columns: Iterable[object],
    *,
    key_sep: str = "__",
    existing: Iterable[str] = (),
) -> list[str]:
    """Normalize column names to canonical form and make collisions explicit."""
    used = {str(item) for item in existing}
    out: list[str] = []
    raw_columns = [str(col) for col in columns]
    base_counts: dict[str, int] = {}
    for raw in raw_columns:
        base = raw.replace(".", key_sep)
        base_counts[base] = base_counts.get(base, 0) + 1

    raw_counts: dict[str, int] = {}
    for raw in raw_columns:
        base = raw.replace(".", key_sep)
        raw_counts[raw] = raw_counts.get(raw, 0) + 1
        group_size = base_counts.get(base, 0) + (1 if base in used else 0)
        hint = _canonical_group_hint(
            raw,
            key_sep=key_sep,
            duplicate_index=raw_counts[raw],
            group_size=group_size,
        )
        canonical = _dedupe_name(
            base,
            used,
            fallback_prefix="col",
            collision_hint=hint,
            suffix_sep=key_sep,
            prefer_hint=bool(hint),
        )
        used.add(canonical)
        out.append(canonical)
    return out


def truncate_table_name(table_name: str, max_len: int = MYSQL_IDENTIFIER_MAX_LEN) -> str:
    """Truncate table name to MySQL/MariaDB 64-char limit with a heuristic."""
    table_name = str(table_name)
    if len(table_name) <= max_len:
        return table_name

    parts = table_name.split("_")
    filtered_parts: list[str] = []
    for part in parts:
        if part and not part.isdigit() and len(part) > 1:
            filtered_parts.append(part)

    if not filtered_parts:
        return table_name[:max_len]

    result = ""
    for part in reversed(filtered_parts):
        trial = f"{part}_{result}" if result else part
        if len(trial) <= max_len:
            result = trial
        else:
            break

    result = result[:max_len].rstrip("_")
    return result if result else table_name[:max_len]


def truncate_column_names(
    columns: Iterable[object],
    *,
    sep: str = "__",
    max_len: int = MYSQL_IDENTIFIER_MAX_LEN,
) -> tuple[list[str], dict[str, str]]:
    """
    Truncate column names to MySQL/MariaDB 64-char limit and resolve collisions.

    Returns:
      - new_cols: list of truncated (and de-duplicated) names in input order
      - col_name_map: mapping from original -> new name (only when changed)
    """
    new_cols: list[str] = []
    col_name_map: dict[str, str] = {}
    raw_counts: dict[str, int] = {}

    for col in columns:
        original_col = str(col)
        raw_counts[original_col] = raw_counts.get(original_col, 0) + 1

        if len(original_col) <= max_len:
            new_col = original_col
        else:
            if sep in original_col:
                parts = [part for part in original_col.split(sep) if part]
                new_col = ""
                for part in reversed(parts):
                    trial = f"{part}{sep}{new_col}" if new_col else part
                    if len(trial) <= max_len:
                        new_col = trial
                    else:
                        if not new_col:
                            new_col = _truncate_with_hash(part, max_len=max_len, fallback_prefix="col")
                        break
                if not new_col:
                    new_col = _truncate_with_hash(original_col, max_len=max_len, fallback_prefix="col")
            else:
                new_col = _truncate_with_hash(original_col, max_len=max_len, fallback_prefix="col")

        hint = _leading_collision_hint(original_col, sep=sep, duplicate_index=raw_counts[original_col])
        new_col = _dedupe_name(
            new_col,
            set(new_cols),
            max_len=max_len,
            fallback_prefix="col",
            collision_hint=hint,
            suffix_sep=sep,
        )

        new_cols.append(new_col)
        if new_col != original_col:
            col_name_map[original_col] = new_col

    return new_cols, col_name_map


def make_index_name(
    table_name: str,
    column_name: str,
    *,
    prefix: str = "IDX",
    max_len: int = MYSQL_IDENTIFIER_MAX_LEN,
) -> str:
    """
    Generate a deterministic index name within MySQL/MariaDB identifier limits.

    Uses a short MD5 suffix when truncation is required to prevent collisions.
    """
    base = f"{prefix}_{table_name}_{column_name}"
    base = sanitize_identifier(base).upper()
    if len(base) <= max_len:
        return base

    digest = hashlib.md5(base.encode("utf-8")).hexdigest()[:8].upper()
    trimmed = base[: max_len - (1 + len(digest))].rstrip("_")
    return f"{trimmed}_{digest}"
