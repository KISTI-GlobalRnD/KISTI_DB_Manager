from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .naming import MYSQL_IDENTIFIER_MAX_LEN, truncate_column_names, truncate_table_name


@dataclass(frozen=True)
class NameMap:
    """
    Canonical naming map shared across create/load/index steps.

    - table_sql: truncated table name (MySQL/MariaDB max 64 chars)
    - columns_sql: truncated/deduplicated column names in stable order
    - column_map: full mapping from canonical column -> sql column
    """

    table_original: str
    table_sql: str
    key_sep: str
    columns_original: tuple[str, ...]
    columns_sql: tuple[str, ...]
    column_map: dict[str, str]

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

        columns_original = tuple(str(c).replace(".", key_sep) for c in columns)
        columns_sql_list, _changed = truncate_column_names(columns_original, sep=key_sep, max_len=max_len)
        columns_sql = tuple(columns_sql_list)
        column_map = {columns_original[i]: columns_sql[i] for i in range(len(columns_original))}

        return cls(
            table_original=table_original,
            table_sql=table_sql,
            key_sep=key_sep,
            columns_original=columns_original,
            columns_sql=columns_sql,
            column_map=column_map,
        )

    def changed_columns(self) -> dict[str, str]:
        return {k: v for k, v in self.column_map.items() if k != v}

    def map_column(self, name: str) -> str:
        return self.column_map.get(name, name)

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

        def truncate_one(col: str) -> str:
            if len(col) <= max_len:
                return col
            if self.key_sep in col:
                parts = col.split(self.key_sep)
                new_col = ""
                for part in reversed(parts):
                    trial = f"{part}{self.key_sep}{new_col}" if new_col else part
                    if len(trial) <= max_len:
                        new_col = trial
                    else:
                        break
                return new_col[:max_len]
            return col[:max_len]

        existing_original = list(self.columns_original)
        existing_sql = list(self.columns_sql)
        column_map = dict(self.column_map)

        for col in columns:
            canonical = str(col).replace(".", self.key_sep)
            if canonical in column_map:
                continue

            new_sql = truncate_one(canonical)
            if new_sql in existing_sql:
                base_col = new_sql
                counter = 1
                while new_sql in existing_sql:
                    suffix = f"_{counter}"
                    if len(base_col) + len(suffix) <= max_len:
                        new_sql = base_col + suffix
                    else:
                        truncated_len = max_len - len(suffix)
                        new_sql = base_col[:truncated_len] + suffix
                    counter += 1
                    if counter > 999:
                        new_sql = f"col_{len(existing_sql)}"
                        break

            existing_original.append(canonical)
            existing_sql.append(new_sql)
            column_map[canonical] = new_sql

        return NameMap(
            table_original=self.table_original,
            table_sql=self.table_sql,
            key_sep=self.key_sep,
            columns_original=tuple(existing_original),
            columns_sql=tuple(existing_sql),
            column_map=column_map,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "table_original": self.table_original,
            "table_sql": self.table_sql,
            "key_sep": self.key_sep,
            "columns_original": list(self.columns_original),
            "columns_sql": list(self.columns_sql),
            "column_map": dict(self.column_map),
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
