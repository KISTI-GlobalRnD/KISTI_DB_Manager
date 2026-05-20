from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from .naming import (
    MYSQL_IDENTIFIER_MAX_LEN,
    canonicalize_column_names,
    truncate_column_names,
    truncate_table_name,
)


def _unique_raw_aliases(raw_columns: list[str], canonical_columns: tuple[str, ...] | list[str]) -> dict[str, str]:
    raw_counts: dict[str, int] = {}
    for raw in raw_columns:
        raw_counts[raw] = raw_counts.get(raw, 0) + 1
    return {
        raw: str(canonical)
        for raw, canonical in zip(raw_columns, canonical_columns)
        if raw != str(canonical) and raw_counts.get(raw) == 1
    }


@dataclass(frozen=True)
class NameMap:
    """
    Canonical naming map shared across create/load/index steps.

    - table_sql: truncated table name (MySQL/MariaDB max 64 chars)
    - columns_sql: truncated/deduplicated column names in stable order
    - column_map: full mapping from canonical column -> sql column
    - column_aliases: optional mapping from raw input column -> canonical column
    """

    table_original: str
    table_sql: str
    key_sep: str
    columns_original: tuple[str, ...]
    columns_sql: tuple[str, ...]
    column_map: dict[str, str]
    column_aliases: dict[str, str] = field(default_factory=dict)

    @classmethod
    def build(
        cls,
        *,
        table_name: str,
        columns: Iterable[object],
        key_sep: str = "__",
        max_len: int = MYSQL_IDENTIFIER_MAX_LEN,
    ) -> "NameMap":
        table_original = str(table_name)
        table_sql = truncate_table_name(table_original, max_len=max_len)

        raw_columns = [str(column) for column in columns]
        columns_original = tuple(canonicalize_column_names(raw_columns, key_sep=key_sep))
        columns_sql_list, _changed = truncate_column_names(columns_original, sep=key_sep, max_len=max_len)
        columns_sql = tuple(columns_sql_list)
        column_map = {columns_original[i]: columns_sql[i] for i in range(len(columns_original))}
        column_aliases = _unique_raw_aliases(raw_columns, columns_original)

        return cls(
            table_original=table_original,
            table_sql=table_sql,
            key_sep=key_sep,
            columns_original=columns_original,
            columns_sql=columns_sql,
            column_map=column_map,
            column_aliases=column_aliases,
        )

    def changed_columns(self) -> dict[str, str]:
        return {k: v for k, v in self.column_map.items() if k != v}

    def map_column(self, name: str) -> str:
        name_s = str(name)
        if name_s in self.column_map:
            return self.column_map[name_s]
        alias = self.column_aliases.get(name_s)
        if alias:
            return self.column_map.get(alias, alias)
        canonical = name_s.replace(".", self.key_sep)
        return self.column_map.get(canonical) or name_s

    def canonicalize_input_columns(self, columns: Iterable[object]) -> list[str]:
        """
        Resolve raw input columns to this NameMap's canonical column namespace.

        This keeps raw aliases (for example ``a.b`` -> ``a__b``) separate from
        already-canonical internal references. Ambiguous repeated raw labels are
        left to the canonicalizer instead of being silently collapsed by aliasing.
        """

        raw_columns = [str(column) for column in columns]
        raw_counts: dict[str, int] = {}
        for raw in raw_columns:
            raw_counts[raw] = raw_counts.get(raw, 0) + 1

        alias_values = set(self.column_aliases.values())
        out: list[str | None] = [None] * len(raw_columns)
        pending: list[str] = []
        pending_positions: list[int] = []

        for idx, raw in enumerate(raw_columns):
            base = raw.replace(".", self.key_sep)
            if raw_counts.get(raw) == 1 and raw in self.column_aliases:
                out[idx] = self.column_aliases[raw]
            elif raw in self.column_map and not (raw == base and base in alias_values):
                out[idx] = raw
            elif base in self.column_map and base not in alias_values:
                out[idx] = base
            else:
                pending.append(raw)
                pending_positions.append(idx)

        for idx, canonical in zip(
            pending_positions,
            canonicalize_column_names(pending, key_sep=self.key_sep, existing=self.columns_original),
        ):
            out[idx] = canonical

        return [str(column) for column in out if column is not None]

    def with_additional_columns(
        self,
        columns: Iterable[object],
        *,
        max_len: int = MYSQL_IDENTIFIER_MAX_LEN,
    ) -> "NameMap":
        """
        Return a new NameMap extended with any columns not already present.

        Preserves existing canonical->sql mappings to keep create/load/index consistent
        across schema drift scenarios (new columns appear later).
        """

        existing_original = list(self.columns_original)
        existing_sql = list(self.columns_sql)
        column_map = dict(self.column_map)
        column_aliases = dict(self.column_aliases)
        initial_original = set(existing_original)
        pending_columns: list[object] = []

        for col in columns:
            col_s = str(col)
            base = str(col).replace(".", self.key_sep)
            if col_s in column_aliases:
                continue
            base_claimed_by_alias = base in set(column_aliases.values())
            if col_s in initial_original and not (col_s == base and base_claimed_by_alias):
                continue
            if col_s == base and base in initial_original and not base_claimed_by_alias:
                continue
            pending_columns.append(col)

        pending_raw = [str(col) for col in pending_columns]
        pending_canonical = canonicalize_column_names(pending_columns, key_sep=self.key_sep, existing=existing_original)
        pending_aliases = _unique_raw_aliases(pending_raw, pending_canonical)

        for raw, canonical in zip(pending_raw, pending_canonical):
            if canonical in column_map:
                continue

            new_sql = truncate_column_names([canonical], sep=self.key_sep, max_len=max_len)[0][0]
            if new_sql in existing_sql:
                new_sql = truncate_column_names([*existing_sql, canonical], sep=self.key_sep, max_len=max_len)[0][-1]

            existing_original.append(canonical)
            existing_sql.append(new_sql)
            column_map[canonical] = new_sql
            if raw in pending_aliases:
                column_aliases[raw] = pending_aliases[raw]

        return NameMap(
            table_original=self.table_original,
            table_sql=self.table_sql,
            key_sep=self.key_sep,
            columns_original=tuple(existing_original),
            columns_sql=tuple(existing_sql),
            column_map=column_map,
            column_aliases=column_aliases,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "table_original": self.table_original,
            "table_sql": self.table_sql,
            "key_sep": self.key_sep,
            "columns_original": list(self.columns_original),
            "columns_sql": list(self.columns_sql),
            "column_map": dict(self.column_map),
            "column_aliases": dict(self.column_aliases),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "NameMap":
        return cls(
            table_original=str(data["table_original"]),
            table_sql=str(data["table_sql"]),
            key_sep=str(data["key_sep"]),
            columns_original=tuple(data.get("columns_original", [])),
            columns_sql=tuple(data.get("columns_sql", [])),
            column_map=dict(data.get("column_map", {})),
            column_aliases=dict(data.get("column_aliases", {})),
        )


def load_namemap(value: Any) -> NameMap | None:
    if value is None:
        return None
    if isinstance(value, NameMap):
        return value
    if isinstance(value, Mapping):
        try:
            return NameMap.from_dict(value)
        except Exception:
            return None
    return None


def is_compatible(name_map: NameMap, *, table_name: str, key_sep: str, columns: Iterable[str]) -> bool:
    if name_map.table_original != str(table_name):
        return False
    if name_map.key_sep != str(key_sep):
        return False
    cols = tuple(columns)
    return name_map.columns_original == cols
