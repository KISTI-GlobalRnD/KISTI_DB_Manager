from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from ..config import coerce_db_config
from ..namemap import load_namemap
from ..naming import MYSQL_IDENTIFIER_MAX_LEN, truncate_table_name


@dataclass(frozen=True)
class TableInfo:
    name_sql: str
    name_original: str | None = None
    row_count: int | None = None
    row_count_exact: bool = False
    table_rows_estimate: int | None = None
    data_length: int | None = None
    index_length: int | None = None
    engine: str | None = None
    collation: str | None = None
    columns: list[dict[str, Any]] | None = None
    indexes: list[dict[str, Any]] | None = None

    def label(self) -> str:
        return self.name_original or self.name_sql

    def rows_label(self) -> str:
        if self.row_count is None:
            if self.table_rows_estimate is None:
                return "n/a"
            return f"~{self.table_rows_estimate}"
        return str(self.row_count)


class DBIntrospector:
    def __init__(self, db_config: Mapping[str, Any]):
        self.db_config = coerce_db_config(db_config)

    def _connect(self):
        try:
            import pymysql
        except Exception as e:  # pragma: no cover
            raise RuntimeError("DB introspection requires the 'db' extra (pymysql). Try: pip install -e '.[db]'") from e

        return pymysql.connect(
            host=self.db_config["host"],
            user=self.db_config["user"],
            password=self.db_config["password"],
            database=self.db_config.get("database"),
            port=int(self.db_config.get("port") or 3306),
            charset="utf8mb4",
            autocommit=True,
        )

    def list_tables_like(self, *, prefix: str) -> list[dict[str, Any]]:
        schema = self.db_config.get("database")
        if not schema:
            raise ValueError("db_config.database is required for DB introspection")

        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name, table_rows, data_length, index_length, engine, table_collation
                    FROM information_schema.tables
                    WHERE table_schema=%s AND table_name LIKE %s
                    ORDER BY table_name
                    """,
                    (schema, f"{prefix}%"),
                )
                rows = cur.fetchall() or []
        finally:
            conn.close()

        res = []
        for r in rows:
            res.append(
                {
                    "table_name": r[0],
                    "table_rows": int(r[1]) if r[1] is not None else None,
                    "data_length": int(r[2]) if r[2] is not None else None,
                    "index_length": int(r[3]) if r[3] is not None else None,
                    "engine": r[4],
                    "table_collation": r[5],
                }
            )
        return res

    def tables_meta(self, table_names: Iterable[str]) -> dict[str, dict[str, Any]]:
        schema = self.db_config.get("database")
        if not schema:
            raise ValueError("db_config.database is required for DB introspection")

        names = [str(n) for n in table_names if str(n)]
        # Keep stable + deduplicate while preserving order.
        deduped: list[str] = []
        seen: set[str] = set()
        for n in names:
            if n in seen:
                continue
            deduped.append(n)
            seen.add(n)

        if not deduped:
            return {}

        placeholders = ", ".join(["%s"] * len(deduped))
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT table_name, table_rows, data_length, index_length, engine, table_collation
                    FROM information_schema.tables
                    WHERE table_schema=%s AND table_name IN ({placeholders})
                    """,
                    [schema, *deduped],
                )
                rows = cur.fetchall() or []
        finally:
            conn.close()

        res: dict[str, dict[str, Any]] = {}
        for r in rows:
            res[str(r[0])] = {
                "table_rows": int(r[1]) if r[1] is not None else None,
                "data_length": int(r[2]) if r[2] is not None else None,
                "index_length": int(r[3]) if r[3] is not None else None,
                "engine": r[4],
                "table_collation": r[5],
            }
        return res

    def exact_row_counts(self, table_names: Iterable[str]) -> dict[str, int]:
        names = [str(n) for n in table_names if str(n)]
        deduped: list[str] = []
        seen: set[str] = set()
        for n in names:
            if n in seen:
                continue
            deduped.append(n)
            seen.add(n)

        if not deduped:
            return {}

        conn = self._connect()
        try:
            with conn.cursor() as cur:
                res: dict[str, int] = {}
                for t in deduped:
                    cur.execute(f"SELECT COUNT(*) FROM `{t.replace('`', '``')}`;")
                    row = cur.fetchone()
                    res[t] = int(row[0]) if row else 0
                return res
        finally:
            conn.close()

    def table_columns(self, *, table_name: str) -> list[dict[str, Any]]:
        schema = self.db_config.get("database")
        if not schema:
            raise ValueError("db_config.database is required for DB introspection")

        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name, data_type, column_type, is_nullable, column_key, extra
                    FROM information_schema.columns
                    WHERE table_schema=%s AND table_name=%s
                    ORDER BY ordinal_position
                    """,
                    (schema, table_name),
                )
                rows = cur.fetchall() or []
        finally:
            conn.close()

        return [
            {
                "name": r[0],
                "data_type": r[1],
                "column_type": r[2],
                "is_nullable": r[3],
                "column_key": r[4],
                "extra": r[5],
            }
            for r in rows
        ]

    def table_indexes(self, *, table_name: str) -> list[dict[str, Any]]:
        schema = self.db_config.get("database")
        if not schema:
            raise ValueError("db_config.database is required for DB introspection")

        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT index_name, non_unique, column_name, seq_in_index
                    FROM information_schema.statistics
                    WHERE table_schema=%s AND table_name=%s
                    ORDER BY index_name, seq_in_index
                    """,
                    (schema, table_name),
                )
                rows = cur.fetchall() or []
        finally:
            conn.close()

        return [
            {
                "index_name": r[0],
                "non_unique": int(r[1]) if r[1] is not None else None,
                "column_name": r[2],
                "seq_in_index": int(r[3]) if r[3] is not None else None,
            }
            for r in rows
        ]

    def exact_row_count(self, *, table_name: str) -> int:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM `{table_name.replace('`', '``')}`;")
                row = cur.fetchone()
                return int(row[0]) if row else 0
        finally:
            conn.close()

    def sample_rows(self, *, table_name: str, limit: int = 5) -> list[dict[str, Any]]:
        """
        Fetch a small sample from a table for review purposes.

        Values are truncated/serialized to keep reports lightweight.
        """
        limit = max(0, int(limit))
        if limit <= 0:
            return []

        import pymysql

        def qi(ident: str) -> str:
            return str(ident).replace("`", "``")

        def normalize(v: Any, max_len: int = 160) -> Any:
            if v is None:
                return None
            if isinstance(v, (bytes, bytearray, memoryview)):
                try:
                    v = bytes(v).decode("utf-8")
                except Exception:
                    v = repr(v)
            if isinstance(v, str):
                return v if len(v) <= max_len else v[: max_len - 1] + "…"
            s = str(v)
            return v if len(s) <= max_len else s[: max_len - 1] + "…"

        conn = self._connect()
        try:
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(f"SELECT * FROM `{qi(table_name)}` LIMIT {int(limit)};")
                rows = cur.fetchall() or []
        finally:
            conn.close()

        out: list[dict[str, Any]] = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            out.append({k: normalize(v) for k, v in r.items()})
        return out


def _collect_table_infos_from_report(
    *,
    base_table: str,
    report: Mapping[str, Any],
) -> list[TableInfo]:
    artifacts = report.get("artifacts") or {}

    name_maps_json = artifacts.get("name_maps_json")
    if isinstance(name_maps_json, Mapping):
        infos: list[TableInfo] = []
        for table_original, nm_dict in name_maps_json.items():
            nm = load_namemap(nm_dict)
            if nm is None:
                continue
            infos.append(TableInfo(name_sql=nm.table_sql, name_original=nm.table_original))
        # Ensure base exists
        if base_table and all(ti.name_original != base_table for ti in infos):
            infos.append(TableInfo(name_sql=truncate_table_name(base_table, max_len=MYSQL_IDENTIFIER_MAX_LEN), name_original=base_table))
        return sorted(infos, key=lambda t: t.name_sql)

    nm_dict = artifacts.get("name_map")
    nm = load_namemap(nm_dict) if nm_dict is not None else None
    if nm is not None:
        return [TableInfo(name_sql=nm.table_sql, name_original=nm.table_original)]

    return [TableInfo(name_sql=truncate_table_name(base_table, max_len=MYSQL_IDENTIFIER_MAX_LEN), name_original=base_table)]


def _collect_table_infos_from_db_prefix(
    *,
    db: DBIntrospector,
    base_table: str,
) -> list[TableInfo]:
    prefix_sql = truncate_table_name(base_table, max_len=MYSQL_IDENTIFIER_MAX_LEN)
    rows = db.list_tables_like(prefix=prefix_sql)
    infos: list[TableInfo] = []
    for r in rows:
        infos.append(
            TableInfo(
                name_sql=str(r["table_name"]),
                name_original=None,
                table_rows_estimate=r.get("table_rows"),
                data_length=r.get("data_length"),
                index_length=r.get("index_length"),
                engine=r.get("engine"),
                collation=r.get("table_collation"),
            )
        )
    return infos


def _merge_db_details(
    *,
    db: DBIntrospector,
    table_infos: list[TableInfo],
    exact_counts: bool,
) -> list[TableInfo]:
    meta_by_table: dict[str, dict[str, Any]] = {}
    try:
        meta_by_table = db.tables_meta([ti.name_sql for ti in table_infos])
    except Exception:
        meta_by_table = {}

    exact_counts_by_table: dict[str, int] = {}
    if exact_counts:
        try:
            exact_counts_by_table = db.exact_row_counts([ti.name_sql for ti in table_infos])
        except Exception:
            exact_counts_by_table = {}

    res: list[TableInfo] = []
    for ti in table_infos:
        cols = None
        idxs = None
        rc = exact_counts_by_table.get(ti.name_sql)
        rc_exact = rc is not None
        try:
            cols = db.table_columns(table_name=ti.name_sql)
            idxs = db.table_indexes(table_name=ti.name_sql)
        except Exception:
            cols = cols or None
            idxs = idxs or None

        meta = meta_by_table.get(ti.name_sql) or {}
        res.append(
            TableInfo(
                name_sql=ti.name_sql,
                name_original=ti.name_original,
                row_count=rc if rc is not None else ti.row_count,
                row_count_exact=rc_exact if rc is not None else ti.row_count_exact,
                table_rows_estimate=ti.table_rows_estimate if ti.table_rows_estimate is not None else meta.get("table_rows"),
                data_length=ti.data_length if ti.data_length is not None else meta.get("data_length"),
                index_length=ti.index_length if ti.index_length is not None else meta.get("index_length"),
                engine=ti.engine if ti.engine is not None else meta.get("engine"),
                collation=ti.collation if ti.collation is not None else meta.get("table_collation"),
                columns=cols,
                indexes=idxs,
            )
        )
    return res
