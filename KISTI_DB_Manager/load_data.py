"""Shared LOAD DATA dialects and helpers.

This module keeps staging writer settings and MariaDB/MySQL reader settings
paired. The DuckDB CSV/TSV stage uses CSV quoting with ``ESCAPE '"'`` and
``NULLSTR 'NULL'``; reading that file with MariaDB ``ESCAPED BY '\\'`` can
silently corrupt records containing backslash-quote sequences and quoted
newlines. Using ``NULLSTR '\\N'`` with MariaDB ``ESCAPED BY '"'`` also loads a
literal ``\\N`` string instead of SQL NULL.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

__all__ = [
    "LOAD_DATA_ESCAPE_TRANSLATE",
    "LoadDataDialect",
    "DUCKDB_LOAD_DATA_DIALECT",
    "MYSQL_GENERATED_TSV_DIALECT",
    "detect_line_terminator",
    "duckdb_sql_string",
    "load_data_local_infile_tabular_file",
    "mysql_escape_char_literal",
    "mysql_escape_load_data_value",
    "mysql_quote_string",
]


LOAD_DATA_ESCAPE_TRANSLATE = {
    ord("\\"): "\\\\",
    ord("\t"): "\\t",
    ord("\n"): "\\n",
    ord("\r"): "\\r",
    0: "\\0",
    26: "\\Z",  # Ctrl+Z (0x1a)
}


@dataclass
class LoadDataDialect:
    """Paired staging/LOAD DATA CSV dialect."""

    name: str
    delimiter: str = "\t"
    nullstr: str = r"\N"
    quote: str | None = '"'
    escape: str = "\\"
    header: bool = False

    def duckdb_copy_options_sql(self) -> str:
        parts = [
            "FORMAT CSV",
            f"HEADER {'TRUE' if self.header else 'FALSE'}",
            f"DELIMITER {duckdb_sql_string(self.delimiter)}",
            f"NULLSTR {duckdb_sql_string(self.nullstr)}",
        ]
        if self.quote:
            parts.append(f"QUOTE {duckdb_sql_string(self.quote)}")
        if self.escape:
            parts.append(f"ESCAPE {duckdb_sql_string(self.escape)}")
        return "(" + ", ".join(parts) + ")"

    def mysql_fields_lines_sql(self, *, line_terminator: str) -> str:
        sep_lit = mysql_escape_char_literal(self.delimiter)
        lt_lit = "\\r\\n" if line_terminator == "\r\n" else "\\n"
        escaped_lit = mysql_escape_char_literal(self.escape)
        quote_sql = ""
        if self.quote:
            quote_lit = mysql_escape_char_literal(self.quote)
            quote_sql = f" OPTIONALLY ENCLOSED BY '{quote_lit}'"
        return (
            "CHARACTER SET utf8mb4 "
            f"FIELDS TERMINATED BY '{sep_lit}'{quote_sql} ESCAPED BY '{escaped_lit}' "
            f"LINES TERMINATED BY '{lt_lit}' "
        )


DUCKDB_LOAD_DATA_DIALECT = LoadDataDialect(
    name="duckdb_csv_tsv",
    delimiter="\t",
    nullstr="NULL",
    quote='"',
    escape='"',
    header=False,
)

MYSQL_GENERATED_TSV_DIALECT = LoadDataDialect(
    name="mysql_generated_tsv",
    delimiter="\t",
    nullstr=r"\N",
    quote=None,
    escape="\\",
    header=False,
)


def mysql_quote_string(value: str) -> str:
    """Return a MySQL string literal with surrounding quotes."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def duckdb_sql_string(value: str) -> str:
    """Return a DuckDB string literal with surrounding quotes."""
    return "'" + str(value).replace("'", "''") + "'"


def mysql_escape_char_literal(ch: str) -> str:
    """Return MySQL string literal content for a delimiter/terminator."""
    if ch == "\t":
        return "\\t"
    if ch == "\n":
        return "\\n"
    if ch == "\r":
        return "\\r"
    if ch == "\0":
        return "\\0"
    if ch == "\\":
        return "\\\\"
    if ch == "'":
        return "\\'"
    return ch


def detect_line_terminator(path: str) -> str:
    try:
        with open(path, "rb") as f:
            chunk = f.read(8192)
        return "\r\n" if b"\r\n" in chunk else "\n"
    except Exception:
        return "\n"


def mysql_escape_load_data_value(value: Any) -> str:
    """
    Escape a single field for LOAD DATA with MySQL backslash escaping.

    - None/NaN -> \\N (NULL)
    - special chars -> backslash escapes so each record stays on one line
    """
    import json
    import math

    if value is None:
        return r"\N"

    try:
        if type(value).__name__ == "NAType":
            return r"\N"
    except Exception:
        pass

    if isinstance(value, float) and math.isnan(value):
        return r"\N"

    if isinstance(value, (dict, list)):
        try:
            import orjson

            value = orjson.dumps(value).decode("utf-8")
        except Exception:
            try:
                value = json.dumps(value, ensure_ascii=False)
            except Exception:
                value = str(value)
    elif isinstance(value, (bytes, bytearray, memoryview)):
        try:
            value = bytes(value).decode("utf-8", errors="replace")
        except Exception:
            value = str(value)
    elif isinstance(value, str):
        pass
    else:
        value = str(value)

    return value.translate(LOAD_DATA_ESCAPE_TRANSLATE)


def load_data_local_infile_tabular_file(
    *,
    conn,
    table_name: str,
    file_path: str,
    sep: str,
    columns_expr: list[str],
    ignore_lines: int = 1,
    escaped_by: str = "\\",
    dialect: LoadDataDialect | None = None,
    expected_rows: int | None = None,
    ignore_duplicates: bool = False,
    commit: bool = True,
    rollback_mode: str = "full",
    line_terminator: str | None = None,
    report=None,
) -> int:
    """
    Bulk load an on-disk delimited file via LOAD DATA LOCAL INFILE.

    Return the row count reported by MariaDB. When `expected_rows` is provided,
    compare it before commit so parser mismatches do not leave a bad chunk
    behind. `ignore_duplicates=True` emits `LOAD DATA ... IGNORE`; in that mode
    rowcount is the number of inserted rows, not necessarily the input row count.
    Pass `line_terminator` for files generated by our own writers; auto-detection
    can mistake CRLF inside quoted field data for the file's row separator.
    """

    def qi(ident: str) -> str:
        return str(ident).replace("`", "``")

    load_dialect = dialect or LoadDataDialect(
        name="caller_supplied_tabular_file",
        delimiter=sep,
        nullstr=r"\N",
        quote='"',
        escape=str(escaped_by or "\\")[:1],
        header=False,
    )
    if line_terminator is not None and line_terminator not in {"\n", "\r\n"}:
        raise ValueError("line_terminator must be '\\n', '\\r\\n', or None")
    lt = line_terminator or detect_line_terminator(file_path)

    ignore = f"IGNORE {int(ignore_lines)} LINES " if int(ignore_lines) > 0 else ""
    duplicate_mode = " IGNORE" if bool(ignore_duplicates) else ""
    sql = (
        "LOAD DATA LOCAL INFILE %s"
        + duplicate_mode
        + f" INTO TABLE `{qi(table_name)}` "
        + load_dialect.mysql_fields_lines_sql(line_terminator=lt)
        + ignore
        + "("
        + ", ".join(columns_expr)
        + ");"
    )

    t0 = None
    try:
        import time

        t0 = time.perf_counter()
    except Exception:
        t0 = None

    rollback_mode_norm = str(rollback_mode or "").strip().lower()
    use_savepoint = bool(not commit) and rollback_mode_norm in {"savepoint", "sp"}
    sp_name = "kisti_load_data"

    try:
        with conn.cursor() as cur:
            if use_savepoint:
                try:
                    cur.execute(f"SAVEPOINT {sp_name}")
                except Exception:
                    use_savepoint = False
            cur.execute(sql, (str(file_path),))
            loaded_rows = int(cur.rowcount or 0)
            if expected_rows is not None and int(loaded_rows) != int(expected_rows):
                raise RuntimeError(
                    "LOAD DATA inserted row count mismatch "
                    f"for {file_path}: expected {int(expected_rows)}, got {int(loaded_rows)}"
                )
            if use_savepoint:
                try:
                    cur.execute(f"RELEASE SAVEPOINT {sp_name}")
                except Exception:
                    pass
        if bool(commit):
            conn.commit()
        if report is not None:
            try:
                report.bump("db.load_data.rows_loaded", loaded_rows)
            except Exception:
                pass
        if report is not None and t0 is not None:
            try:
                import time

                report.add_time_s("db.load_data.exec", time.perf_counter() - t0)
            except Exception:
                pass
        return loaded_rows
    except Exception:
        try:
            if use_savepoint:
                try:
                    with conn.cursor() as cur:
                        cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                        try:
                            cur.execute(f"RELEASE SAVEPOINT {sp_name}")
                        except Exception:
                            pass
                except Exception:
                    conn.rollback()
            else:
                conn.rollback()
        except Exception:
            pass
        raise
