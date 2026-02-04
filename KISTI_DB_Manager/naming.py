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

    for col in columns:
        original_col = str(col)

        if len(original_col) <= max_len:
            new_col = original_col
        else:
            if sep in original_col:
                parts = original_col.split(sep)
                new_col = ""
                for part in reversed(parts):
                    trial = f"{part}{sep}{new_col}" if new_col else part
                    if len(trial) <= max_len:
                        new_col = trial
                    else:
                        break
                new_col = new_col[:max_len]
            else:
                new_col = original_col[:max_len]

        if new_col in new_cols:
            base_col = new_col
            counter = 1
            while new_col in new_cols:
                suffix = f"_{counter}"
                if len(base_col) + len(suffix) <= max_len:
                    new_col = base_col + suffix
                else:
                    truncated_len = max_len - len(suffix)
                    new_col = base_col[:truncated_len] + suffix
                counter += 1
                if counter > 999:
                    new_col = f"col_{len(new_cols)}"
                    break

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

