# KISTI_DB_Manager/manage.py
"""
Note
----
Made by Young Jin Kim (kimyoungjin06@gmail.com)
Last Update: 2024.02.05, YJ Kim

MariaDB/MySQL Handling for All type DB
To preprocess, import, export and manage the DB


Now Contains ...

Example
-------
# Database configuration
db_config = {
    'host': Host,  # Update as needed
    'user': User,       # Update as needed
    'password': Password,       # Update as needed
    'database': DB  # Update as needed
}

# Table name
table_name = 'example_table'

# Generate and execute CREATE TABLE SQL
create_table(f, table_name, db_config)
fill_table_from_file(f, table_name, db_config)

"""

from dataclasses import dataclass

from .naming import (
    canonicalize_column_names,
    make_index_name,
    quote_mysql_identifier,
    truncate_column_names,
    truncate_table_name,
)
from .config import coerce_data_config, coerce_db_config, join_path
# Re-export selected LOAD DATA helpers for compatibility. New code should
# import KISTI_DB_Manager.load_data directly.
from .load_data import (
    DUCKDB_LOAD_DATA_DIALECT,
    MYSQL_GENERATED_TSV_DIALECT,
    LoadDataDialect,
    detect_line_terminator as _detect_line_terminator,
    duckdb_sql_string as _duckdb_sql_string,
    load_data_local_infile_tabular_file as _load_data_local_infile_tabular_file,
    mysql_escape_char_literal as _sql_escape_char_literal,
    mysql_escape_load_data_value as _mysql_escape_load_data_value,
    mysql_quote_string as _sql_quote_string,
)
from .namemap import NameMap, is_compatible, load_namemap

__all__ = [
    "LoadDataDialect",
    "DUCKDB_LOAD_DATA_DIALECT",
    "MYSQL_GENERATED_TSV_DIALECT",
    "is_Null",
    "read_Description",
    "truncate_table_name",
    "truncate_column_names",
    "optimize_column_types",
    "generate_create_table_sql",
    "generate_create_table_sql_from_columns",
    "create_table",
    "create_table_from_columns",
    "convert_datetime",
    "fill_table_from_file",
    "fill_table_from_dataframe",
    "fill_table_from_rows",
    "fill_table_from_tsv_file",
    "drop_table",
    "drop_DB",
    "create_DB",
    "backup_database_subprocess",
    "set_index",
    "set_index_simple",
    "backup_database_pymysql",
    "optimize_table",
    "init_MySQL",
    "FastLoadState",
]

@dataclass
class FastLoadState:
    """
    Mutable state to disable fast loading after first failure.

    When db_load_method is "auto", we try fast load once and permanently fall back to
    pandas.to_sql after any error (e.g. server/local_infile disabled).
    """

    enabled: bool = True
    disabled_reason: str | None = None
    disabled_error: str | None = None

    def disable(self, *, reason: str, error: str | None = None) -> None:
        self.enabled = False
        self.disabled_reason = str(reason)
        self.disabled_error = str(error) if error is not None else None


def _normalize_db_load_method(value) -> str:
    s = str(value or "").strip().lower()
    if s in {"", "none", "default"}:
        return "to_sql"
    if s in {"auto", "to_sql", "load_data"}:
        return s
    return "to_sql"


def _is_nullish_value(value) -> bool:
    if value is None:
        return True
    try:
        if type(value).__name__ == "NAType":
            return True
    except Exception:
        pass
    try:
        import math

        return isinstance(value, float) and math.isnan(value)
    except Exception:
        return False


def _json_dumps_best_effort(obj) -> str:
    try:
        import orjson

        return orjson.dumps(obj).decode("utf-8")
    except Exception:
        import json

        return json.dumps(obj, ensure_ascii=False)


def _rename_row_keys(
    rows: list[dict],
    *,
    original_columns: list[str],
    canonical_columns: list[str],
    key_sep: str,
) -> list[dict]:
    rename_map = {
        str(original): str(canonical)
        for original, canonical in zip(original_columns, canonical_columns)
        if str(original) != str(canonical)
    }
    if not rename_map:
        return rows

    out: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            out.append(row)
            continue
        renamed: dict = {}
        for key, value in row.items():
            key_s = str(key)
            renamed_key = rename_map.get(key_s)
            if renamed_key is None:
                renamed_key = key_s.replace(".", key_sep)
            renamed[renamed_key] = value
        out.append(renamed)
    return out


def _mysql_comment_sql(text_value: str | None) -> str:
    text_s = str(text_value or "").strip()
    if not text_s:
        return ""
    text_s = text_s[:1024]
    text_s = text_s.replace("\\", "\\\\").replace("'", "''")
    return f" COMMENT '{text_s}'"


def _column_descriptions_by_sql(nm: NameMap, column_descriptions: dict[str, str] | None) -> dict[str, str]:
    desc = column_descriptions or {}
    if not desc:
        return {}
    out: dict[str, str] = {}
    for orig_col, sql_col in zip(nm.columns_original, nm.columns_sql):
        value = desc.get(orig_col) or desc.get(sql_col)
        if value:
            out[str(sql_col)] = str(value)
    return out


def _column_type_with_comment(
    column_type: str,
    column: str,
    desc_by_sql: dict[str, str] | None,
    existing_col_comments: dict[str, str] | None = None,
) -> str:
    comment = (desc_by_sql or {}).get(str(column)) or (existing_col_comments or {}).get(str(column))
    return f"{column_type}{_mysql_comment_sql(comment)}"


def _write_load_data_stage_with_duckdb(
    *,
    df,
    columns: list[str],
    file_path: str,
    report=None,
) -> None:
    """
    Write a LOAD DATA-compatible tab-delimited staging file using DuckDB.

    The resulting file uses CSV quoting rules with:
    - DELIMITER '\\t'
    - NULLSTR 'NULL'
    - QUOTE '"'
    - ESCAPE '"'

    CRITICAL: the MariaDB LOAD DATA reader must use the same escape
    character. Reading this file with ESCAPED BY '\\' can silently corrupt
    records that contain backslash-quote sequences and quoted newlines.
    Also keep the NULL marker as SQL NULL-compatible `NULL`; `\\N` is a
    literal string when MariaDB is using ESCAPED BY '"'.
    MySQL/MariaDB must consume this with
    `FIELDS TERMINATED BY '\\t' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\"'`.
    """
    import duckdb
    import json
    import time

    def dq(ident: str) -> str:
        return '"' + str(ident).replace('"', '""') + '"'

    cols = [str(c) for c in (columns or [])]
    if not cols:
        return

    t0 = time.perf_counter()
    con = duckdb.connect(database=":memory:")
    try:
        con.register("kisti_input_df", df)
        select_sql = ", ".join(dq(c) for c in cols)
        copy_sql = (
            f"COPY (SELECT {select_sql} FROM kisti_input_df) "
            f"TO {json.dumps(str(file_path))} "
            f"{DUCKDB_LOAD_DATA_DIALECT.duckdb_copy_options_sql()};"
        )
        con.execute(copy_sql)
    finally:
        try:
            con.unregister("kisti_input_df")
        except Exception:
            pass
        con.close()
    if report is not None:
        try:
            ms = int(round((time.perf_counter() - t0) * 1000.0))
            report.add_time_ms("db.load_data.stage_write", ms)
            report.add_time_ms("db.load_data.duckdb_stage_write", ms)
        except Exception:
            pass


def _load_data_local_infile_dataframe(
    *,
    conn,
    table_name: str,
    df,
    columns: list[str],
    staging_writer: str | None = None,
    staging_dir: str | None = None,
    report=None,
) -> None:
    """
    Bulk load a DataFrame via LOAD DATA LOCAL INFILE.
    """
    import os
    import tempfile

    def qi(ident: str) -> str:
        return str(ident).replace("`", "``")

    if not columns:
        return

    staging_writer_norm = str(staging_writer or "python").strip().lower()
    if staging_writer_norm not in {"python", "duckdb"}:
        raise ValueError(f"Unsupported staging_writer: {staging_writer}")
    staging_dir_norm = str(staging_dir or "/tmp").strip() or "/tmp"

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            prefix=f"kisti_load_{qi(table_name)}_",
            suffix=".tsv",
            delete=False,
            dir=staging_dir_norm,
        ) as f:
            tmp_path = f.name

        if staging_writer_norm == "duckdb":
            _write_load_data_stage_with_duckdb(
                df=df,
                columns=columns,
                file_path=tmp_path,
                report=report,
            )
            _load_data_local_infile_tabular_file(
                conn=conn,
                table_name=table_name,
                file_path=tmp_path,
                sep="\t",
                columns_expr=[f"`{qi(c)}`" for c in columns],
                ignore_lines=0,
                dialect=DUCKDB_LOAD_DATA_DIALECT,
                expected_rows=len(df),
                line_terminator="\n",
                report=report,
            )
        else:
            # Write rows (no header) with MySQL escape sequences.
            t0 = None
            try:
                import time

                t0 = time.perf_counter()
            except Exception:
                t0 = None
            with open(tmp_path, "w", encoding="utf-8", newline="\n") as f:
                escape = _mysql_escape_load_data_value
                write = f.write
                for row in df.itertuples(index=False, name=None):
                    write("\t".join(escape(v) for v in row) + "\n")
            if report is not None and t0 is not None:
                try:
                    import time

                    tsv_ms = int(round((time.perf_counter() - t0) * 1000.0))
                    report.add_time_ms("db.load_data.stage_write", tsv_ms)
                    report.add_time_ms("db.load_data.tsv_write", tsv_ms)
                except Exception:
                    pass

            _load_data_local_infile_tabular_file(
                conn=conn,
                table_name=table_name,
                file_path=tmp_path,
                sep="\t",
                columns_expr=[f"`{qi(c)}`" for c in columns],
                ignore_lines=0,
                dialect=MYSQL_GENERATED_TSV_DIALECT,
                expected_rows=len(df),
                line_terminator="\n",
                report=report,
            )
    finally:
        if tmp_path:
            try:
                os.remove(tmp_path)
            except Exception:
                pass


def _load_data_local_infile_rows(
    *,
    conn,
    table_name: str,
    rows: list[dict],
    columns_original: list[str],
    name_map: NameMap,
    report=None,
) -> None:
    """
    Bulk load row dicts via LOAD DATA LOCAL INFILE.

    - columns_original: canonical column names (file order)
    - name_map: used to map canonical -> SQL columns in LOAD DATA column list
    """
    import os
    import tempfile

    def qi(ident: str) -> str:
        return str(ident).replace("`", "``")

    if not rows or not columns_original:
        return

    columns_sql = [name_map.map_column(c) for c in columns_original]

    tmp_path = None
    try:
        t0 = None
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            prefix=f"kisti_load_{qi(table_name)}_",
            suffix=".tsv",
            delete=False,
            dir="/tmp",
        ) as f:
            tmp_path = f.name
            try:
                import time

                t0 = time.perf_counter()
            except Exception:
                t0 = None
            escape = _mysql_escape_load_data_value
            cols = list(columns_original)
            write = f.write
            for row in rows:
                row_get = row.get
                write("\t".join(escape(row_get(col)) for col in cols) + "\n")
            if report is not None and t0 is not None:
                try:
                    import time

                    report.add_time_s("db.load_data.tsv_write", time.perf_counter() - t0)
                except Exception:
                    pass

        _load_data_local_infile_tabular_file(
            conn=conn,
            table_name=table_name,
            file_path=tmp_path,
            sep="\t",
            columns_expr=[f"`{qi(c)}`" for c in columns_sql],
            ignore_lines=0,
            dialect=MYSQL_GENERATED_TSV_DIALECT,
            expected_rows=len(rows),
            line_terminator="\n",
            report=report,
        )
    finally:
        if tmp_path:
            try:
                os.remove(tmp_path)
            except Exception:
                pass


def _load_data_local_infile_tsv_file(
    *,
    conn,
    table_name: str,
    file_path: str,
    columns_sql: list[str],
    report=None,
    commit: bool = True,
    rollback_mode: str = "full",
) -> None:
    """
    Bulk load an existing TSV file via LOAD DATA LOCAL INFILE.

    Unlike `_load_data_local_infile_tabular_file`, this is tailored to the TSVs we generate:
    - tab-separated
    - no quotes
    - MySQL escape sequences (ESCAPED BY '\\\\')
    - newline-terminated
    """

    def qi(ident: str) -> str:
        return str(ident).replace("`", "``")

    if not columns_sql:
        return

    _load_data_local_infile_tabular_file(
        conn=conn,
        table_name=table_name,
        file_path=str(file_path),
        sep="\t",
        columns_expr=[f"`{qi(c)}`" for c in columns_sql],
        ignore_lines=0,
        dialect=MYSQL_GENERATED_TSV_DIALECT,
        report=report,
        commit=bool(commit),
        rollback_mode=rollback_mode,
        line_terminator="\n",
    )


def _get_or_build_name_map(
    data_config: dict,
    *,
    columns: list[str],
    name_map: NameMap | dict | None = None,
) -> NameMap:
    key_sep = data_config.get("KEY_SEP", "__")
    table_name = data_config["table_name"]

    nm = load_namemap(name_map) or load_namemap(data_config.get("_name_map"))
    columns_norm = (
        nm.canonicalize_input_columns(columns)
        if nm and nm.table_original == str(table_name) and nm.key_sep == str(key_sep)
        else canonicalize_column_names(columns, key_sep=key_sep)
    )
    if nm and nm.table_original == str(table_name) and nm.key_sep == str(key_sep):
        # Perfect match
        if is_compatible(nm, table_name=table_name, key_sep=key_sep, columns=tuple(columns_norm)):
            return nm
        # Schema drift: allow new columns to be appended while keeping existing mappings stable
        if set(nm.columns_original).issubset(set(columns_norm)):
            nm2 = nm.with_additional_columns(columns_norm, max_len=64)
            data_config["_name_map"] = nm2.to_dict()
            return nm2

    nm = NameMap.build(table_name=table_name, columns=columns_norm, key_sep=key_sep, max_len=64)
    data_config["_name_map"] = nm.to_dict()
    return nm


def _prepare_desc_and_namemap(
    data_config: dict,
    *,
    df_desc=None,
    name_map: NameMap | dict | None = None,
):
    """
    Normalize df_desc + build NameMap once to keep create/load/index consistent.
    """
    if df_desc is None:
        df_desc = read_Description(data_config)

    key_sep = data_config.get("KEY_SEP", "__")
    df_desc = df_desc.copy()
    df_desc.index = canonicalize_column_names(df_desc.index, key_sep=key_sep)
    df_desc = df_desc[df_desc.Type.notnull() & (df_desc.Type != "Unknown")]

    nm = _get_or_build_name_map(data_config, columns=list(df_desc.index), name_map=name_map)
    return df_desc, nm


def _extract_mysql_column_name(exc: BaseException | str) -> str | None:
    import re

    msg = str(exc)

    # Common fully-qualified pattern from MySQL/MariaDB errors:
    #   for column `db`.`table`.`col`
    m = re.search(r"for column ((?:`[^`]+`\.)*`[^`]+`)", msg)
    if m:
        parts = re.findall(r"`([^`]+)`", m.group(1))
        if parts:
            return parts[-1]

    patterns = [
        r"for column '([^']+)'",
        # Prefer the last identifier if it is qualified: `db`.`table`.`col`
        r"for column `[^`]+`\.`[^`]+`\.`([^`]+)`",
        r"for column `([^`]+)`",
        r"Unknown column '([^']+)'",
        r"Unknown column `([^`]+)`",
        r"column '([^']+)'",
        r"column `([^`]+)`",
    ]
    for pat in patterns:
        m = re.search(pat, msg)
        if m:
            col = m.group(1)
            # Defensive: if we captured a qualified name, take the last part.
            if "." in col:
                col = col.split(".")[-1]
            return col
    return None


def _df_applymap(df, func):
    """
    Pandas compatibility helper.

    - pandas < 2.x: DataFrame.applymap exists
    - newer pandas: applymap may be removed; DataFrame.map can exist
    """
    if hasattr(df, "applymap"):
        return df.applymap(func)
    if hasattr(df, "map"):
        return df.map(func)
    return df.apply(lambda col: col.map(func))


def _coerce_to_sql_method(method):
    """
    Normalize config-provided to_sql method.

    - None / "none" / "default" / "executemany" -> None (pandas default, typically executemany)
    - "multi" -> "multi"
    """
    if method is None:
        return None
    s = str(method).strip().lower()
    if s in {"", "none", "default", "executemany"}:
        return None
    if s == "multi":
        return "multi"
    return method


def is_Null(_type, _null_ratio, forced_null=False):
    """Return about Null part for SQL query """
    _type = str(_type)
    if forced_null==False:
        if _null_ratio == 0 and "NOT NULL" not in _type.upper():
            _type += ' NOT NULL'
    return _type


def read_Description(data_config):
    """Read the Description File"""
    import pandas as pd

    data_config = coerce_data_config(data_config)
    PATH, f = data_config["PATH"], data_config["file_name"]
    desc_file = join_path(PATH, f'{".".join(f.split(".")[:-1])}_Desc.csv')
    df_res = pd.read_csv(desc_file, index_col=0)
    key_sep = data_config.get("KEY_SEP", "__")
    df_res.index = canonicalize_column_names(df_res.index, key_sep=key_sep)
    return df_res


def optimize_column_types(column_types, max_row_size=8000):
    """
    행 크기 제한을 고려하여 컬럼 타입을 최적화
    큰 VARCHAR를 TEXT로 변경하여 행 크기 줄이기
    """
    optimized_types = {}
    total_varchar_size = 0
    
    # VARCHAR 크기 계산
    for col, dtype in column_types.items():
        if 'VARCHAR' in dtype.upper():
            # VARCHAR(n) 에서 n 추출
            import re
            match = re.search(r'VARCHAR\((\d+)\)', dtype.upper())
            if match:
                size = int(match.group(1))
                total_varchar_size += size * 3  # UTF8 기준 최대 3바이트
    
    # 행 크기가 제한을 초과하면 큰 VARCHAR를 TEXT로 변경
    if total_varchar_size > max_row_size:
        for col, dtype in column_types.items():
            if 'VARCHAR' in dtype.upper():
                import re
                match = re.search(r'VARCHAR\((\d+)\)', dtype.upper())
                if match:
                    size = int(match.group(1))
                    # 255자를 넘는 VARCHAR는 TEXT로 변경
                    if size > 255:
                        optimized_types[col] = dtype.replace(f'VARCHAR({size})', 'TEXT')
                    else:
                        optimized_types[col] = dtype
                else:
                    optimized_types[col] = dtype
            else:
                optimized_types[col] = dtype
    else:
        optimized_types = column_types.copy()
    
    return optimized_types


def generate_create_table_sql(data_config, df_desc=None, name_map: NameMap | dict | None = None):
    """Generate CREATE TABLE SQL based on a Description file and NameMap."""
    data_config = coerce_data_config(data_config, inplace=isinstance(data_config, dict))
    forced_null = data_config.get("forced_null", False)

    df_desc, nm = _prepare_desc_and_namemap(data_config, df_desc=df_desc, name_map=name_map)

    column_types = {idx: df_desc.loc[idx, "Type"] for idx in df_desc.index}
    for idx in df_desc.index:
        null_ratio = df_desc.loc[idx, "Null_ratio"]
        column_types[idx] = is_Null(column_types[idx], null_ratio, forced_null)

    sql_column_types = {nm.map_column(col): dtype for col, dtype in column_types.items()}
    sql_column_types = optimize_column_types(sql_column_types)

    ordered_cols = [c for c in nm.columns_sql if c in sql_column_types]
    columns = [f"{quote_mysql_identifier(col)} {sql_column_types[col]}" for col in ordered_cols]
    return f"CREATE TABLE {quote_mysql_identifier(nm.table_sql)} ({', '.join(columns)});"


# def create_table(data_config, db_config):
#     """
#     Creates a table in a MariaDB database based on a DataFrame structure.
#     """
#     from pymysql import Error
    
#     # Generate CREATE TABLE SQL statement
#     create_table_sql = generate_create_table_sql(data_config)
    
#     try:
#         # Connect to MariaDB
#         conn = pymysql.connect(**db_config)
#         cursor = conn.cursor()
        
#         # Execute CREATE TABLE SQL statement
#         cursor.execute(create_table_sql)
#         conn.commit()
#         print(f"Table `{data_config['table_name']}` created successfully.")
#     except Error as e:
#         print(f"Error: {e}")
#     finally:
#         pass
#         # if conn:
#         #     cursor.close()
#         #     conn.close()


def create_table(data_config, db_config, df_desc=None, name_map: NameMap | dict | None = None):
    """
    Creates a table in a MariaDB database based on a DataFrame structure.
    """
    import pymysql
    from pymysql import Error
    
    conn = None
    cursor = None
    
    try:
        data_config = coerce_data_config(data_config, inplace=isinstance(data_config, dict))
        db_config = coerce_db_config(db_config)

        df_desc, nm = _prepare_desc_and_namemap(data_config, df_desc=df_desc, name_map=name_map)

        if nm.table_original != nm.table_sql:
            print(f"테이블명 축약: {nm.table_original} → {nm.table_sql}")

        create_table_sql = generate_create_table_sql(data_config, df_desc=df_desc, name_map=nm)
        print(f"Creating table: {nm.table_sql}")
        
        # Connect to MariaDB
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()
        
        # 기존 테이블이 있다면 삭제 (선택사항)
        # cursor.execute(f"DROP TABLE IF EXISTS `{truncated_table_name}`")
        
        # Execute CREATE TABLE SQL statement
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"Table `{nm.table_sql}` created successfully.")
        return nm
        
    except Error as e:
        error_code = e.args[0] if e.args else 0
        
        if error_code == 1118:  # Row size too large
            print(f"행 크기 초과 에러 (테이블: {data_config.get('table_name')})")
            print("더 많은 VARCHAR를 TEXT로 변경하거나 컬럼 수를 줄여주세요.")
        elif error_code == 1103:  # Incorrect table name
            print(f"잘못된 테이블명 에러: {data_config.get('table_name')}")
        elif error_code == 1060:  # Duplicate column name  
            print(f"중복 컬럼명 에러 (테이블: {data_config.get('table_name')})")
        
        print(f"Error: {e}")
        
        if conn:
            conn.rollback()
            
    except Exception as e:
        print(f"Unexpected error: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def convert_datetime(df, data_config, FORMAT='%Y-%m-%d %H:%M:%S'):
    import pandas as pd

    # read df_desc
    df_desc = read_Description(data_config)
    # FORMAT = '%Y-%m-%d %H:%M:%S'
    msk = df_desc.Type == 'DATETIME'
    cols = df_desc[msk].index
    for col in cols:
        # 날짜 포맷을 MySQL이 인식할 수 있는 형식으로 변환
        df[col] = pd.to_datetime(df[col]).dt.strftime(FORMAT)
    return df

def fill_table_from_file(
    *args,
    df_desc=None,
    name_map: NameMap | dict | None = None,
    sep="__",
    report=None,
    load_method: str | None = None,
    fast_load_state: FastLoadState | None = None,
    local_infile_conn=None,
    column_descriptions: dict[str, str] | None = None,
):
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL
    from sqlalchemy import inspect, text
    from .preview import read_data_from_tabular
    from sqlalchemy.types import String
    import pandas as pd

    def qi(ident: str) -> str:
        return str(ident).replace("`", "``")

    def debug_columns(df, engine, table_name_prefix="debug_test"):
        for col in df.columns:
            try:
                temp_df = df[[col]].copy()
    
                # 값 중 'nan' 문자열이 있는지 확인하고 None으로 바꿈
                temp_df[col] = temp_df[col].apply(
                    lambda x: None if isinstance(x, str) and x.strip().lower() == 'nan' else x
                )
    
                # NaN → None
                temp_df = temp_df.where(pd.notnull(temp_df), None)
    
                # 임시 테이블명
                test_table = f"{table_name_prefix}__{col[:50]}"  # 최대 64자
                temp_df.to_sql(
                    test_table,
                    engine,
                    if_exists='replace',
                    index=False,
                    dtype={col: String(512)}  # 일단 모두 문자열로 처리
                )
    
                print(f"✅ Column OK: {col}")
            except Exception as e:
                print(f"❌ ERROR in column: {col}")
                print(f"    Type: {df[col].dtype}")
                print(f"    Sample values: {df[col].dropna().unique()[:5]}")
                print(f"    Exception: {e}")

    if len(args) == 2:
        data_config, db_config = args
    elif len(args) == 3:
        df_desc, data_config, db_config = args
    else:
        raise TypeError(
            "fill_table_from_file expects (data_config, db_config[, df_desc]) "
            "or legacy (df_desc, data_config, db_config)."
        )

    data_config = coerce_data_config(data_config, inplace=isinstance(data_config, dict))
    db_config = coerce_db_config(db_config)

    df_desc, nm = _prepare_desc_and_namemap(data_config, df_desc=df_desc, name_map=name_map)
    sep = nm.key_sep
    table_name = nm.table_sql
    Conv_DATETIME = data_config.get("Conv_DATETIME", False)
    include_extra_columns = bool(data_config.get("include_extra_columns", True))
    auto_alter_table = bool(data_config.get("auto_alter_table", True))
    fallback_on_insert_error = bool(data_config.get("fallback_on_insert_error", True))
    fallback_column_type = str(data_config.get("fallback_column_type", "LONGTEXT"))
    insert_retry_max = int(data_config.get("insert_retry_max", 5) or 0)
    to_sql_method = _coerce_to_sql_method(data_config.get("to_sql_method"))
    sanitize_nan_strings = bool(data_config.get("sanitize_nan_strings", False))
    convert_nan_to_none = bool(data_config.get("convert_nan_to_none", False))

    # Fast path for large delimited files: LOAD DATA LOCAL INFILE (best-effort)
    db_load_method = _normalize_db_load_method(load_method or data_config.get("db_load_method"))
    if fast_load_state is not None and not fast_load_state.enabled:
        db_load_method = "to_sql"

    file_type = str(data_config.get("file_type") or "").strip().lower()
    file_sep = data_config.get("SEP", data_config.get("FILE_SEP", "\t"))

    if (
        db_load_method in {"auto", "load_data"}
        and not Conv_DATETIME
        and file_type in {"csv", "txt"}
        and isinstance(file_sep, str)
        and len(file_sep) == 1
    ):
        import csv
        import pymysql

        created_conn = False
        conn = local_infile_conn
        if conn is None:
            try:
                conn = pymysql.connect(**db_config, charset="utf8mb4", autocommit=False, local_infile=1, connect_timeout=3)
                created_conn = True
            except Exception as e:
                if report:
                    report.warn(
                        stage="fill_table_from_file.load_data",
                        message="Fast load disabled (failed to open local_infile connection)",
                        table=table_name,
                        error=str(e),
                    )
                if fast_load_state is not None:
                    fast_load_state.disable(reason="conn_failed", error=str(e))
                conn = None

        if conn is not None:
            try:
                # Check server variable once to avoid expensive failures.
                with conn.cursor() as cur:
                    cur.execute("SELECT @@local_infile;")
                    row = cur.fetchone()
                if row is not None and str(row[0]) in {"0", "OFF", "off", "False", "false"}:
                    raise RuntimeError("Server variable @@local_infile=0 (LOCAL INFILE disabled)")

                file_path = join_path(data_config.get("PATH", ""), data_config.get("file_name", ""))
                with open(file_path, "r", encoding="utf-8", newline="") as f:
                    reader = csv.reader(f, delimiter=file_sep)
                    header = next(reader)
                if not header:
                    raise ValueError("Empty header row; cannot fast-load tabular file")
                header[0] = str(header[0]).lstrip("\ufeff")
                file_cols = nm.canonicalize_input_columns([str(c).strip() for c in header])

                cols_canonical_known = [col for col in df_desc.index if col in file_cols]
                cols_canonical = list(cols_canonical_known)
                if include_extra_columns:
                    extra_cols = [col for col in file_cols if col not in set(df_desc.index)]
                    if extra_cols:
                        nm = nm.with_additional_columns(extra_cols, max_len=64)
                        data_config["_name_map"] = nm.to_dict()
                        cols_canonical.extend(extra_cols)

                if len(cols_canonical) == 0:
                    print(f"No matching columns found for `{table_name}`; nothing to insert.")
                    return nm

                # Build LOAD DATA column expression list (in file order); ignore unknown columns via @dummy vars.
                allowed = set(cols_canonical)
                dummy_i = 0
                columns_expr: list[str] = []
                cols_sql_to_load: list[str] = []
                for c in file_cols:
                    if c in allowed:
                        col_sql = nm.map_column(c)
                        columns_expr.append(f"`{qi(col_sql)}`")
                        cols_sql_to_load.append(col_sql)
                    else:
                        dummy_i += 1
                        columns_expr.append(f"@dummy{dummy_i}")

                # Ensure columns exist (drift) before loading.
                existing_cols_set: set[str] = set()
                with conn.cursor() as cur:
                    cur.execute(f"SHOW COLUMNS FROM `{qi(table_name)}`;")
                    for r in cur.fetchall():
                        if r:
                            existing_cols_set.add(str(r[0]))

                missing_cols = [c for c in cols_sql_to_load if c not in existing_cols_set]
                if missing_cols and auto_alter_table:
                    type_by_sql: dict[str, str] = {}
                    try:
                        for col in cols_canonical:
                            sql_col = nm.map_column(col)
                            if col in getattr(df_desc, "index", []):
                                try:
                                    sql_type = str(df_desc.loc[col, "Type"])
                                except Exception:
                                    sql_type = "TEXT"
                                sql_type = sql_type.replace(" NOT NULL", "")
                                type_by_sql[sql_col] = sql_type
                            else:
                                type_by_sql[sql_col] = "TEXT"
                    except Exception:
                        type_by_sql = {}

                    from contextlib import nullcontext

                    add_clauses = [f"ADD COLUMN `{qi(c)}` {type_by_sql.get(c, 'TEXT')}" for c in missing_cols]
                    alter_sql = f"ALTER TABLE `{qi(table_name)}` " + ", ".join(add_clauses)
                    with (report.timer("db.alter") if report else nullcontext()):
                        with conn.cursor() as cur:
                            try:
                                cur.execute(alter_sql)
                                conn.commit()
                                existing_cols_set.update(missing_cols)
                                print(f"Added {len(missing_cols)} missing columns to `{table_name}` (fast load).")
                            except Exception:
                                conn.rollback()
                                for c in missing_cols:
                                    try:
                                        cur.execute(
                                            f"ALTER TABLE `{qi(table_name)}` ADD COLUMN `{qi(c)}` {type_by_sql.get(c, 'TEXT')};"
                                        )
                                        conn.commit()
                                        existing_cols_set.add(c)
                                        print(f"Added missing column `{c}` to `{table_name}` (fast load).")
                                    except Exception as e:
                                        conn.rollback()
                                        print(f"Warning: failed to add column `{c}` to `{table_name}`: {e}")

                from contextlib import nullcontext

                with (report.timer("db.load_data.total") if report else nullcontext()):
                    _load_data_local_infile_tabular_file(
                        conn=conn,
                        table_name=table_name,
                        file_path=file_path,
                        sep=file_sep,
                        columns_expr=columns_expr,
                        ignore_lines=1,
                        report=report,
                    )

                if report:
                    report.bump("load_data_ok", 1)
                print(f"Data inserted into table `{table_name}` successfully (LOAD DATA LOCAL INFILE).")
                return nm
            except Exception as e:
                if report:
                    report.warn(
                        stage="fill_table_from_file.load_data",
                        message="LOAD DATA LOCAL INFILE failed; falling back to pandas.to_sql",
                        table=table_name,
                        error=str(e),
                    )
                if fast_load_state is not None:
                    fast_load_state.disable(reason="load_data_failed", error=str(e))
                try:
                    conn.rollback()
                except Exception:
                    pass
            finally:
                if created_conn and conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
    
    # Read the file into a DataFrame
    df = read_data_from_tabular(data_config)
    
    # dot to underscore
    df.columns = nm.canonicalize_input_columns(df.columns)

    
    # DateTime Convert
    if Conv_DATETIME:
        df = convert_datetime(df, data_config)

    # Create a SQLAlchemy engine for the database connection
    url = URL.create(
        "mysql+pymysql",
        username=db_config.get("user"),
        password=db_config.get("password"),
        host=db_config.get("host"),
        port=db_config.get("port"),
        database=db_config.get("database"),
    )
    engine = create_engine(url)

    # Use pandas to_sql() function to insert data into the table
    # Replace 'append' with 'replace' if you want to overwrite existing data in the table
    cols_canonical_known = [col for col in df_desc.index if col in df.columns]
    cols_canonical = list(cols_canonical_known)
    if include_extra_columns:
        extra_cols = [col for col in df.columns if col not in set(df_desc.index)]
        if extra_cols:
            nm = nm.with_additional_columns(extra_cols, max_len=64)
            data_config["_name_map"] = nm.to_dict()
            cols_canonical.extend(extra_cols)
    if len(cols_canonical) == 0:
        print(f"No matching columns found for `{table_name}`; nothing to insert.")
        return nm

    cols_sql = [nm.map_column(col) for col in cols_canonical]
    df = df[cols_canonical].copy()
    df = df.rename(columns=nm.changed_columns())
    df = df[cols_sql].copy()
    
    # Optional sanitization (disabled by default for performance)
    if sanitize_nan_strings:
        try:
            obj_cols = list(df.select_dtypes(include=["object", "string"]).columns)
            if obj_cols:
                # Replace case-insensitive "nan" strings with NULL (best-effort)
                df[obj_cols] = df[obj_cols].replace(r"(?i)^\s*nan\s*$", None, regex=True)
        except Exception:
            # Best-effort only; never fail here
            pass
    if convert_nan_to_none:
        # This may convert numeric columns to object dtype; enable only when needed.
        df = df.where(pd.notnull(df), None)

    # print(df.dtypes)
    # debug_columns(df, engine, table_name_prefix=table_name + "_check")

    # Schema drift: ensure all DataFrame columns exist in the target table.
    try:
        inspector = inspect(engine)
        existing_cols = {c.get("name") for c in inspector.get_columns(table_name)}
    except Exception as e:
        print(f"Warning: could not inspect existing columns for `{table_name}`: {e}")
        existing_cols = set()

    missing_cols = [col for col in cols_sql if col not in existing_cols] if existing_cols else []
    if missing_cols and auto_alter_table:
        # Best-effort add missing columns as TEXT (or desc Type if available)
        type_by_sql: dict[str, str] = {}
        try:
            for col in cols_canonical:
                if col in getattr(df_desc, "index", []):
                    sql_col = nm.map_column(col)
                    try:
                        sql_type = str(df_desc.loc[col, "Type"])
                    except Exception:
                        sql_type = "TEXT"
                    # For drift-additions, prefer nullable columns to avoid hard failures
                    sql_type = sql_type.replace(" NOT NULL", "")
                    type_by_sql[sql_col] = sql_type
        except Exception:
            type_by_sql = {}

        from contextlib import nullcontext

        with (report.timer("db.alter") if report else nullcontext()):
            with engine.begin() as conn:
                add_clauses = [f"ADD COLUMN `{qi(col)}` {type_by_sql.get(col, 'TEXT')}" for col in missing_cols]
                alter_sql = f"ALTER TABLE `{qi(table_name)}` " + ", ".join(add_clauses)
                try:
                    conn.execute(text(alter_sql))
                    existing_cols.update(missing_cols)
                    print(f"Added {len(missing_cols)} missing columns to `{table_name}`.")
                except Exception:
                    for col in missing_cols:
                        col_type = type_by_sql.get(col, "TEXT")
                        try:
                            conn.execute(
                                text(
                                    f"ALTER TABLE `{qi(table_name)}` "
                                    f"ADD COLUMN `{qi(col)}` {col_type}"
                                )
                            )
                            existing_cols.add(col)
                            print(f"Added missing column `{col}` to `{table_name}` ({col_type}).")
                        except Exception as e:
                            print(f"Warning: failed to add column `{col}` to `{table_name}`: {e}")

    if existing_cols:
        keep_cols = [col for col in cols_sql if col in existing_cols]
        if len(keep_cols) != len(cols_sql):
            dropped = [col for col in cols_sql if col not in set(keep_cols)]
            print(f"Warning: dropping {len(dropped)} columns not present in `{table_name}`: {dropped[:10]}")
        cols_sql = keep_cols
        df = df[cols_sql].copy()

    # Insert only the processed columns
    chunksize = data_config.get("chunksize", None)
    attempts_left = max(0, insert_retry_max)
    altered_cols: set[str] = set()
    while True:
        try:
            from contextlib import nullcontext

            with (report.timer("db.to_sql") if report else nullcontext()):
                df.reset_index(drop=True).to_sql(
                    table_name,
                    engine,
                    if_exists="append",
                    index=False,
                    chunksize=chunksize,
                    method=to_sql_method,
                )
            break
        except Exception as e:
            if not fallback_on_insert_error or attempts_left <= 0:
                raise

            msg = str(e)
            col = _extract_mysql_column_name(e)
            if not col:
                raise

            unknown_col = "Unknown column" in msg
            if col in altered_cols and not unknown_col:
                raise

            if report:
                try:
                    report.warn(
                        stage="fill_table_from_file.fallback",
                        message="Insert failed; attempting to widen/add column and retry",
                        table=table_name,
                        column=col,
                        action="ADD" if unknown_col else "MODIFY",
                        sql_type=fallback_column_type,
                        error=msg,
                    )
                except Exception:
                    pass

            from contextlib import nullcontext

            with (report.timer("db.alter") if report else nullcontext()):
                with engine.begin() as conn:
                    if unknown_col:
                        conn.execute(
                            text(
                                f"ALTER TABLE `{qi(table_name)}` "
                                f"ADD COLUMN `{qi(col)}` {fallback_column_type}"
                            )
                        )
                    else:
                        conn.execute(
                            text(
                                f"ALTER TABLE `{qi(table_name)}` "
                                f"MODIFY COLUMN `{qi(col)}` {fallback_column_type}"
                            )
                        )

            altered_cols.add(col)
            attempts_left -= 1
            continue
    print(f"Data inserted into table `{table_name}` successfully.")
    return nm


def generate_create_table_sql_from_columns(
    *,
    table_name: str,
    columns: list[str],
    name_map: NameMap | dict | None = None,
    key_sep: str = "__",
    column_type: str = "LONGTEXT",
    column_descriptions: dict[str, str] | None = None,
) -> tuple[str, NameMap]:
    """
    Generate a simple CREATE TABLE statement from a list of columns.

    This is intended for robust ingestion (e.g., JSON flattening) where strict typing
    is less important than never failing due to drift.
    """

    def qi(ident: str) -> str:
        return str(ident).replace("`", "``")

    nm = load_namemap(name_map)
    if nm and nm.table_original == str(table_name) and nm.key_sep == str(key_sep):
        nm = nm.with_additional_columns(columns, max_len=64)
    else:
        nm = NameMap.build(table_name=table_name, columns=columns, key_sep=key_sep, max_len=64)

    desc = column_descriptions or {}
    sql_cols = [
        f"`{qi(sql_col)}` {column_type}{_mysql_comment_sql(desc.get(orig_col))}"
        for orig_col, sql_col in zip(nm.columns_original, nm.columns_sql)
    ]
    sql = f"CREATE TABLE IF NOT EXISTS `{qi(nm.table_sql)}` ({', '.join(sql_cols)});"
    return sql, nm


def create_table_from_columns(
    db_config,
    *,
    table_name: str,
    columns: list[str],
    name_map: NameMap | dict | None = None,
    key_sep: str = "__",
    column_type: str = "LONGTEXT",
    column_descriptions: dict[str, str] | None = None,
) -> NameMap:
    import pymysql

    db_config = coerce_db_config(db_config)
    sql, nm = generate_create_table_sql_from_columns(
        table_name=table_name,
        columns=columns,
        name_map=name_map,
        key_sep=key_sep,
        column_type=column_type,
        column_descriptions=column_descriptions,
    )

    conn = None
    cursor = None
    try:
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        print(f"Table `{nm.table_sql}` created/exists.")
        return nm
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def fill_table_from_dataframe(
    df,
    db_config,
    *,
    table_name: str,
    name_map: NameMap | dict,
    extra_column_name: str | None = None,
    auto_alter_table: bool = True,
    column_type: str = "LONGTEXT",
    fallback_on_insert_error: bool = True,
    fallback_column_type: str = "LONGTEXT",
    insert_retry_max: int = 5,
    report=None,
    chunksize=None,
    to_sql_method=None,
    sanitize_nan_strings: bool = False,
    convert_nan_to_none: bool = False,
    engine=None,
    existing_cols: set[str] | None = None,
    load_method: str | None = None,
    fast_load_state: FastLoadState | None = None,
    local_infile_conn=None,
    load_data_staging_writer: str | None = None,
    load_data_staging_dir: str | None = None,
    column_descriptions: dict[str, str] | None = None,
) -> NameMap:
    """
    Insert a pandas DataFrame into an existing table, with best-effort schema drift handling.

    - Renames columns using NameMap (canonical -> SQL)
    - Optionally packs unknown columns into extra_column_name when schema is frozen
    - Adds missing columns (ALTER TABLE) when auto_alter_table=True
    - On insert errors, best-effort widens the failing column (MODIFY/ADD) and retries
    """
    from sqlalchemy import create_engine, inspect, text
    from sqlalchemy.engine import URL
    import pandas as pd
    from contextlib import nullcontext

    def qi(ident: str) -> str:
        return str(ident).replace("`", "``")

    nm = load_namemap(name_map)
    if nm is None:
        raise TypeError("fill_table_from_dataframe requires a valid NameMap")

    extra_canon = None
    extra_sql = None
    if extra_column_name:
        extra_canon = nm.canonicalize_input_columns([extra_column_name])[0]
        nm = nm.with_additional_columns([extra_canon], max_len=64)
        extra_sql = nm.map_column(extra_canon)

    db_config = coerce_db_config(db_config)

    if engine is None:
        url = URL.create(
            "mysql+pymysql",
            username=db_config.get("user"),
            password=db_config.get("password"),
            host=db_config.get("host"),
            port=db_config.get("port"),
            database=db_config.get("database"),
        )
        engine = create_engine(url)

    # Normalize column names to canonical form first.
    canonical_cols = nm.canonicalize_input_columns(list(getattr(df, "columns", [])))
    if list(getattr(df, "columns", [])) != canonical_cols:
        df = df.copy()
        df.columns = canonical_cols

    nm = nm.with_additional_columns(canonical_cols, max_len=64)
    if extra_canon:
        extra_sql = nm.map_column(extra_canon)
    df = df.rename(columns=nm.changed_columns())
    cols_sql = list(getattr(df, "columns", []))
    desc_by_sql = _column_descriptions_by_sql(nm, column_descriptions)

    # Best-effort: align with existing table schema.
    existing_cols_set: set[str] | None
    existing_col_comments: dict[str, str] = {}
    if existing_cols is not None:
        existing_cols_set = existing_cols
    else:
        try:
            inspector = inspect(engine)
            inspected_columns = list(inspector.get_columns(table_name))
            existing_cols_set = {c.get("name") for c in inspected_columns}
            existing_col_comments = {
                str(c.get("name")): str(c.get("comment"))
                for c in inspected_columns
                if c.get("name") and c.get("comment")
            }
        except Exception as e:
            print(f"Warning: could not inspect existing columns for `{table_name}`: {e}")
            existing_cols_set = None

    missing_cols = [col for col in cols_sql if col not in existing_cols_set] if existing_cols_set else []

    # Ensure extra column exists (best-effort) so we can preserve unknown fields even when schema is frozen.
    if extra_sql and existing_cols_set is not None and extra_sql not in existing_cols_set:
        try:
            with (report.timer("db.alter") if report else nullcontext()):
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            f"ALTER TABLE `{qi(table_name)}` ADD COLUMN `{qi(extra_sql)}` "
                            f"{_column_type_with_comment(column_type, extra_sql, desc_by_sql)}"
                        )
                    )
            existing_cols_set.add(extra_sql)
        except Exception as e:
            if report:
                try:
                    report.warn(
                        stage="fill_table_from_dataframe.extra",
                        message="Failed to ensure extra column; unknown fields may be dropped",
                        table=table_name,
                        column=extra_sql,
                        error=str(e),
                    )
                except Exception:
                    pass

    if missing_cols and auto_alter_table:
        # Prefer one ALTER TABLE with multiple ADD COLUMN for performance.
        add_clauses = [
            f"ADD COLUMN `{qi(col)}` {_column_type_with_comment(column_type, col, desc_by_sql)}"
            for col in missing_cols
        ]
        alter_sql = f"ALTER TABLE `{qi(table_name)}` " + ", ".join(add_clauses)

        with (report.timer("db.alter") if report else nullcontext()):
            with engine.begin() as conn:
                try:
                    conn.execute(text(alter_sql))
                    for col in missing_cols:
                        if existing_cols_set is not None:
                            existing_cols_set.add(col)
                    print(f"Added {len(missing_cols)} missing columns to `{table_name}` ({column_type}).")
                except Exception:
                    for col in missing_cols:
                        try:
                            conn.execute(
                                text(
                                    f"ALTER TABLE `{qi(table_name)}` "
                                    f"ADD COLUMN `{qi(col)}` {_column_type_with_comment(column_type, col, desc_by_sql)}"
                                )
                            )
                            if existing_cols_set is not None:
                                existing_cols_set.add(col)
                            print(f"Added missing column `{col}` to `{table_name}` ({column_type}).")
                        except Exception as e:
                            print(f"Warning: failed to add column `{col}` to `{table_name}`: {e}")

    if existing_cols_set:
        drop_cols = [col for col in cols_sql if col not in existing_cols_set]
        if drop_cols:
            if extra_sql and extra_sql in existing_cols_set:
                try:
                    sql_to_orig = {nm.columns_sql[i]: nm.columns_original[i] for i in range(len(nm.columns_sql))}
                    sub = df[drop_cols].copy()
                    sub = sub.where(pd.notnull(sub), None)
                    extras: list[str | None] = []
                    for rec in sub.to_dict(orient="records"):
                        packed: dict[str, object] = {}
                        for k, v in (rec or {}).items():
                            if _is_nullish_value(v):
                                continue
                            packed[sql_to_orig.get(str(k), str(k))] = v
                        extras.append(_json_dumps_best_effort(packed) if packed else None)
                    df = df.copy()
                    df[extra_sql] = extras
                except Exception as e:
                    if report:
                        try:
                            report.warn(
                                stage="fill_table_from_dataframe.extra",
                                message="Failed to pack unknown columns into extra; they will be dropped",
                                table=table_name,
                                error=str(e),
                            )
                        except Exception:
                            pass
                if extra_sql not in set(getattr(df, "columns", [])):
                    try:
                        df = df.copy()
                        df[extra_sql] = None
                    except Exception:
                        pass

            keep_cols = [col for col in cols_sql if col in existing_cols_set]
            if extra_sql and extra_sql in existing_cols_set and extra_sql in set(getattr(df, "columns", [])):
                keep_cols.append(extra_sql)
            keep_cols = list(dict.fromkeys([c for c in keep_cols if c]))
            print(f"Warning: dropping {len(drop_cols)} columns not present in `{table_name}`: {drop_cols[:10]}")
            df = df[keep_cols].copy()

    # Fast path: LOAD DATA LOCAL INFILE (best-effort)
    load_method_norm = _normalize_db_load_method(load_method)
    if fast_load_state is not None and not fast_load_state.enabled:
        load_method_norm = "to_sql"

    if load_method_norm in {"auto", "load_data"} and local_infile_conn is not None:
        try:
            with (report.timer("db.load_data.total") if report else nullcontext()):
                _load_data_local_infile_dataframe(
                    conn=local_infile_conn,
                    table_name=table_name,
                    df=df,
                    columns=list(getattr(df, "columns", [])),
                    staging_writer=load_data_staging_writer,
                    staging_dir=load_data_staging_dir,
                    report=report,
                )
            if report:
                try:
                    report.bump("load_data_ok", 1)
                except Exception:
                    pass
            return nm
        except Exception as e:
            if report:
                try:
                    report.warn(
                        stage="fill_table_from_dataframe.load_data",
                        message="LOAD DATA LOCAL INFILE failed; falling back to to_sql",
                        table=table_name,
                        error=str(e),
                    )
                except Exception:
                    pass
            if fast_load_state is not None:
                fast_load_state.disable(reason="load_data_failed", error=str(e))

    to_sql_method = _coerce_to_sql_method(to_sql_method)

    # Optional sanitization (disabled by default for performance)
    if sanitize_nan_strings:
        try:
            obj_cols = list(df.select_dtypes(include=["object", "string"]).columns)
            if obj_cols:
                df[obj_cols] = df[obj_cols].replace(r"(?i)^\s*nan\s*$", None, regex=True)
        except Exception:
            pass
    if convert_nan_to_none:
        df = df.where(pd.notnull(df), None)

    # Note: chunksize=None means "let pandas decide"; pass explicitly for clarity.
    attempts_left = max(0, int(insert_retry_max or 0))
    altered_cols: set[str] = set()
    while True:
        try:
            with (report.timer("db.to_sql") if report else nullcontext()):
                df.reset_index(drop=True).to_sql(
                    table_name,
                    engine,
                    if_exists="append",
                    index=False,
                    chunksize=chunksize,
                    method=to_sql_method,
                )
            break
        except Exception as e:
            if not fallback_on_insert_error or attempts_left <= 0:
                raise

            msg = str(e)
            col = _extract_mysql_column_name(e)
            if not col:
                raise

            unknown_col = "Unknown column" in msg
            if col in altered_cols and not unknown_col:
                raise

            if report:
                try:
                    report.warn(
                        stage="fill_table_from_dataframe.fallback",
                        message="Insert failed; attempting to widen/add column and retry",
                        table=table_name,
                        column=col,
                        action="ADD" if unknown_col else "MODIFY",
                        sql_type=fallback_column_type,
                        error=msg,
                    )
                except Exception:
                    pass

            with engine.begin() as conn:
                if unknown_col:
                    conn.execute(
                        text(
                            f"ALTER TABLE `{qi(table_name)}` "
                            f"ADD COLUMN `{qi(col)}` {_column_type_with_comment(fallback_column_type, col, desc_by_sql)}"
                        )
                    )
                    if existing_cols_set is not None:
                        existing_cols_set.add(col)
                else:
                    conn.execute(
                        text(
                            f"ALTER TABLE `{qi(table_name)}` "
                            f"MODIFY COLUMN `{qi(col)}` "
                            f"{_column_type_with_comment(fallback_column_type, col, desc_by_sql, existing_col_comments)}"
                        )
                    )

            altered_cols.add(col)
            attempts_left -= 1
            continue
    return nm


def fill_table_from_rows(
    rows: list[dict],
    db_config,
    *,
    table_name: str,
    name_map: NameMap | dict,
    extra_column_name: str | None = None,
    columns_original: list[str] | None = None,
    auto_alter_table: bool = True,
    column_type: str = "LONGTEXT",
    report=None,
    engine=None,
    existing_cols: set[str] | None = None,
    load_method: str | None = None,
    fast_load_state: FastLoadState | None = None,
    local_infile_conn=None,
    column_descriptions: dict[str, str] | None = None,
):
    """
    Insert row dicts into an existing table with best-effort drift handling.

    Primary purpose: no-pandas fast path (rows -> TSV -> LOAD DATA LOCAL INFILE).
    Falls back to pandas.to_sql on errors when allowed by load_method/fast_load_state.
    """
    from sqlalchemy import create_engine, inspect, text
    from sqlalchemy.engine import URL
    from contextlib import nullcontext

    def qi(ident: str) -> str:
        return str(ident).replace("`", "``")

    nm = load_namemap(name_map)
    if nm is None:
        raise TypeError("fill_table_from_rows requires a valid NameMap")

    if not rows:
        return nm

    db_config = coerce_db_config(db_config)

    if engine is None:
        url = URL.create(
            "mysql+pymysql",
            username=db_config.get("user"),
            password=db_config.get("password"),
            host=db_config.get("host"),
            port=db_config.get("port"),
            database=db_config.get("database"),
        )
        engine = create_engine(url)

    if columns_original is None:
        columns_original = list(nm.columns_original)

    # Normalize to canonical naming (dots -> key_sep).
    raw_columns_original = list(columns_original)
    columns_original = nm.canonicalize_input_columns(raw_columns_original)
    rows = _rename_row_keys(
        rows,
        original_columns=raw_columns_original,
        canonical_columns=columns_original,
        key_sep=nm.key_sep,
    )

    extra_canon = None
    extra_sql = None
    if extra_column_name:
        extra_canon = nm.canonicalize_input_columns([extra_column_name])[0]
        if extra_canon not in set(columns_original):
            columns_original = list(columns_original) + [extra_canon]

    # Ensure NameMap covers requested columns (schema drift).
    nm = nm.with_additional_columns(columns_original, max_len=64)
    if extra_canon:
        extra_sql = nm.map_column(extra_canon)

    cols_sql = [nm.map_column(c) for c in columns_original]
    desc_by_sql = _column_descriptions_by_sql(nm, column_descriptions)

    # Best-effort: align with existing table schema.
    existing_cols_set: set[str] | None
    if existing_cols is not None:
        existing_cols_set = existing_cols
    else:
        try:
            inspector = inspect(engine)
            existing_cols_set = {c.get("name") for c in inspector.get_columns(table_name)}
        except Exception as e:
            print(f"Warning: could not inspect existing columns for `{table_name}`: {e}")
            existing_cols_set = None

    missing_cols = [col for col in cols_sql if col not in existing_cols_set] if existing_cols_set else []

    # Ensure extra column exists (best-effort) so we can preserve unknown fields even when schema is frozen.
    if extra_sql and existing_cols_set is not None and extra_sql not in existing_cols_set:
        try:
            with (report.timer("db.alter") if report else nullcontext()):
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            f"ALTER TABLE `{qi(table_name)}` ADD COLUMN `{qi(extra_sql)}` "
                            f"{_column_type_with_comment(column_type, extra_sql, desc_by_sql)}"
                        )
                    )
            existing_cols_set.add(extra_sql)
        except Exception as e:
            if report:
                try:
                    report.warn(
                        stage="fill_table_from_rows.extra",
                        message="Failed to ensure extra column; unknown fields may be dropped",
                        table=table_name,
                        column=extra_sql,
                        error=str(e),
                    )
                except Exception:
                    pass

    if missing_cols and auto_alter_table:
        add_clauses = [
            f"ADD COLUMN `{qi(col)}` {_column_type_with_comment(column_type, col, desc_by_sql)}"
            for col in missing_cols
        ]
        alter_sql = f"ALTER TABLE `{qi(table_name)}` " + ", ".join(add_clauses)
        with (report.timer("db.alter") if report else nullcontext()):
            with engine.begin() as conn:
                try:
                    conn.execute(text(alter_sql))
                    for col in missing_cols:
                        if existing_cols_set is not None:
                            existing_cols_set.add(col)
                    print(f"Added {len(missing_cols)} missing columns to `{table_name}` ({column_type}).")
                except Exception:
                    for col in missing_cols:
                        try:
                            conn.execute(
                                text(
                                    f"ALTER TABLE `{qi(table_name)}` "
                                    f"ADD COLUMN `{qi(col)}` {_column_type_with_comment(column_type, col, desc_by_sql)}"
                                )
                            )
                            if existing_cols_set is not None:
                                existing_cols_set.add(col)
                            print(f"Added missing column `{col}` to `{table_name}` ({column_type}).")
                        except Exception as e:
                            print(f"Warning: failed to add column `{col}` to `{table_name}`: {e}")

    keep_cols_sql = cols_sql
    keep_cols_original = list(columns_original)
    if existing_cols_set:
        keep_pairs = [(o, s) for o, s in zip(columns_original, cols_sql) if s in existing_cols_set]
        if len(keep_pairs) != len(cols_sql):
            dropped = [s for o, s in zip(columns_original, cols_sql) if s not in set(s for _, s in keep_pairs)]
            print(f"Warning: dropping {len(dropped)} columns not present in `{table_name}`: {dropped[:10]}")
        keep_cols_original = [o for o, _s in keep_pairs]
        keep_cols_sql = [s for _o, s in keep_pairs]

        # When schema is frozen (auto_alter_table=False), preserve dropped fields into extra column if configured.
        if extra_canon and extra_sql and extra_sql in existing_cols_set:
            dropped_original = {
                o
                for o, s in zip(columns_original, cols_sql)
                if s not in set(keep_cols_sql) and o != extra_canon
            }
            if dropped_original:
                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    extras = {k: v for k, v in r.items() if k in dropped_original and not _is_nullish_value(v)}
                    r[extra_canon] = _json_dumps_best_effort(extras) if extras else None

    load_method_norm = _normalize_db_load_method(load_method)
    if fast_load_state is not None and not fast_load_state.enabled:
        load_method_norm = "to_sql"

    if load_method_norm in {"auto", "load_data"} and local_infile_conn is not None:
        try:
            with (report.timer("db.load_data.total") if report else nullcontext()):
                _load_data_local_infile_rows(
                    conn=local_infile_conn,
                    table_name=table_name,
                    rows=rows,
                    columns_original=keep_cols_original,
                    name_map=nm,
                    report=report,
                )
            if report:
                try:
                    report.bump("load_data_ok", 1)
                except Exception:
                    pass
            return nm
        except Exception as e:
            if report:
                try:
                    report.warn(
                        stage="fill_table_from_rows.load_data",
                        message="LOAD DATA LOCAL INFILE failed; falling back to to_sql",
                        table=table_name,
                        error=str(e),
                    )
                except Exception:
                    pass
            if fast_load_state is not None:
                fast_load_state.disable(reason="load_data_failed", error=str(e))

    # Fallback: pandas.to_sql (best-effort)
    try:
        import pandas as pd
    except Exception as e:
        raise RuntimeError("pandas is required for fallback to_sql path") from e

    with (report.timer("db.to_sql") if report else nullcontext()):
        df = pd.DataFrame.from_records(rows)
        if keep_cols_original:
            df = df.reindex(columns=keep_cols_original)
        df = df.rename(columns=nm.changed_columns())

        df.reset_index(drop=True).to_sql(
            table_name,
            engine,
            if_exists="append",
            index=False,
        )
    return nm


def fill_table_from_tsv_file(
    file_path: str,
    db_config,
    *,
    table_name: str,
    name_map: NameMap | dict,
    columns_original: list[str],
    extra_column_name: str | None = None,
    auto_alter_table: bool = True,
    column_type: str = "LONGTEXT",
    report=None,
    engine=None,
    existing_cols: set[str] | None = None,
    load_method: str | None = None,
    fast_load_state: FastLoadState | None = None,
    load_data_commit: bool = True,
    local_infile_conn=None,
    column_descriptions: dict[str, str] | None = None,
):
    """
    Load a TSV file into an existing table with best-effort drift handling.

    Primary purpose: parallel JSON flatten where worker processes write per-table TSV
    files, and the parent process performs `LOAD DATA LOCAL INFILE` into MariaDB/MySQL.

    Notes:
    - This function is intended for the LOAD DATA path. It does not attempt to fall back
      to pandas.to_sql because the TSV uses MySQL escape sequences (e.g., \\t, \\n, \\N),
      and a correct reverse-unescape is non-trivial and would be slow.
    """
    from contextlib import nullcontext

    from sqlalchemy import create_engine, inspect, text
    from sqlalchemy.engine import URL

    def qi(ident: str) -> str:
        return str(ident).replace("`", "``")

    nm = load_namemap(name_map)
    if nm is None:
        raise TypeError("fill_table_from_tsv_file requires a valid NameMap")

    if not file_path:
        return nm

    db_config = coerce_db_config(db_config)

    if engine is None:
        url = URL.create(
            "mysql+pymysql",
            username=db_config.get("user"),
            password=db_config.get("password"),
            host=db_config.get("host"),
            port=db_config.get("port"),
            database=db_config.get("database"),
        )
        engine = create_engine(url)

    if not columns_original:
        return nm

    # Normalize to canonical naming (dots -> key_sep).
    columns_original = nm.canonicalize_input_columns(columns_original)

    extra_canon = None
    extra_sql = None
    if extra_column_name:
        extra_canon = nm.canonicalize_input_columns([extra_column_name])[0]
        if extra_canon not in set(columns_original):
            columns_original = list(columns_original) + [extra_canon]

    # Ensure NameMap covers requested columns (schema drift).
    nm = nm.with_additional_columns(columns_original, max_len=64)
    if extra_canon:
        extra_sql = nm.map_column(extra_canon)

    cols_sql = [nm.map_column(c) for c in columns_original]
    desc_by_sql = _column_descriptions_by_sql(nm, column_descriptions)

    # Best-effort: align with existing table schema.
    existing_cols_set: set[str] | None
    if existing_cols is not None:
        existing_cols_set = existing_cols
    else:
        try:
            inspector = inspect(engine)
            existing_cols_set = {c.get("name") for c in inspector.get_columns(table_name)}
        except Exception as e:
            print(f"Warning: could not inspect existing columns for `{table_name}`: {e}")
            existing_cols_set = None

    missing_cols = [col for col in cols_sql if col not in existing_cols_set] if existing_cols_set else []

    # Ensure extra column exists (best-effort) so we can accept extra_column_name in TSV schemas.
    if extra_sql and existing_cols_set is not None and extra_sql not in existing_cols_set:
        try:
            with (report.timer("db.alter") if report else nullcontext()):
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            f"ALTER TABLE `{qi(table_name)}` ADD COLUMN `{qi(extra_sql)}` "
                            f"{_column_type_with_comment(column_type, extra_sql, desc_by_sql)}"
                        )
                    )
            existing_cols_set.add(extra_sql)
        except Exception as e:
            if report:
                try:
                    report.warn(
                        stage="fill_table_from_tsv_file.extra",
                        message="Failed to ensure extra column; unknown fields may be dropped",
                        table=table_name,
                        column=extra_sql,
                        error=str(e),
                    )
                except Exception:
                    pass

    if missing_cols and auto_alter_table:
        add_clauses = [
            f"ADD COLUMN `{qi(col)}` {_column_type_with_comment(column_type, col, desc_by_sql)}"
            for col in missing_cols
        ]
        alter_sql = f"ALTER TABLE `{qi(table_name)}` " + ", ".join(add_clauses)
        with (report.timer("db.alter") if report else nullcontext()):
            with engine.begin() as conn:
                try:
                    conn.execute(text(alter_sql))
                    for col in missing_cols:
                        if existing_cols_set is not None:
                            existing_cols_set.add(col)
                    print(f"Added {len(missing_cols)} missing columns to `{table_name}` ({column_type}).")
                except Exception:
                    for col in missing_cols:
                        try:
                            conn.execute(
                                text(
                                    f"ALTER TABLE `{qi(table_name)}` "
                                    f"ADD COLUMN `{qi(col)}` {_column_type_with_comment(column_type, col, desc_by_sql)}"
                                )
                            )
                            if existing_cols_set is not None:
                                existing_cols_set.add(col)
                            print(f"Added missing column `{col}` to `{table_name}` ({column_type}).")
                        except Exception as e:
                            print(f"Warning: failed to add column `{col}` to `{table_name}`: {e}")

    keep_cols_sql = cols_sql
    keep_cols_original = list(columns_original)
    if existing_cols_set:
        keep_pairs = [(o, s) for o, s in zip(columns_original, cols_sql) if s in existing_cols_set]
        if len(keep_pairs) != len(cols_sql):
            dropped = [s for o, s in zip(columns_original, cols_sql) if s not in set(s for _, s in keep_pairs)]
            print(f"Warning: dropping {len(dropped)} columns not present in `{table_name}`: {dropped[:10]}")
        keep_cols_original = [o for o, _s in keep_pairs]
        keep_cols_sql = [s for _o, s in keep_pairs]

    load_method_norm = _normalize_db_load_method(load_method)
    if fast_load_state is not None and not fast_load_state.enabled:
        load_method_norm = "to_sql"

    if load_method_norm not in {"auto", "load_data"} or local_infile_conn is None:
        raise RuntimeError("fill_table_from_tsv_file requires LOAD DATA (local_infile_conn) and db_load_method=auto/load_data")

    try:
        with (report.timer("db.load_data.total") if report else nullcontext()):
            _load_data_local_infile_tsv_file(
                conn=local_infile_conn,
                table_name=table_name,
                file_path=str(file_path),
                columns_sql=list(keep_cols_sql),
                report=report,
                commit=bool(load_data_commit),
                rollback_mode=("savepoint" if not bool(load_data_commit) else "full"),
            )
        if report:
            try:
                report.bump("load_data_ok", 1)
            except Exception:
                pass
        return nm
    except Exception as e:
        if report:
            try:
                report.warn(
                    stage="fill_table_from_tsv_file.load_data",
                    message="LOAD DATA LOCAL INFILE failed",
                    table=table_name,
                    file_path=str(file_path),
                    error=str(e),
                )
            except Exception:
                pass
        if fast_load_state is not None:
            fast_load_state.disable(reason="load_data_failed", error=str(e))
        raise


def drop_table(table_name, db_config):
    """
    Drops a specified table from the database.

    Parameters:
    db_config (dict): A dictionary containing the database connection parameters.
    table_name (str): The name of the table to be dropped.
    """
    import pymysql
    from pymysql.err import ProgrammingError

    conn = None
    cursor = None
    db_config = coerce_db_config(db_config)

    try:
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()

        # Prepare the DROP TABLE statement
        drop_query = f"DROP TABLE IF EXISTS `{table_name}`;"

        # Execute the DROP TABLE statement
        cursor.execute(drop_query)
        conn.commit()

        print(f"Table `{table_name}` dropped successfully.")

    except ProgrammingError as e:
        print(f"An error occurred while dropping the table: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def drop_DB(database_name, db_config):
    """
    Drops a specified database in MariaDB or MySQL using pymysql.

    Parameters:
    db_config (dict): A dictionary containing the database server connection parameters.
    database_name (str): The name of the database to be dropped.
    """
    import pymysql

    db_config = coerce_db_config(db_config)

    conn = None
    cursor = None

    try:
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()

        # Execute the DROP DATABASE SQL statement
        cursor.execute(f"DROP DATABASE IF EXISTS `{database_name}`;")
        print(f"Database `{database_name}` dropped successfully.")
        cursor.close()
        conn.close()

    except pymysql.Error as e:
        print(f"Failed to drop database `{database_name}`. Error: {e}")
    # finally:
    #     # Ensure the connection is closed
    #     if conn:
    #         cursor.close()
    #         conn.close()


def create_DB(DB_name, CHARACTER_SET, COLLATE, db_config):
    """
    Create a specified table from the database.

    Parameters:
    db_config (dict): A dictionary containing the database connection parameters.
    table_name (str): The name of the table to be dropped.
    """
    import pymysql

    db_config = coerce_db_config(db_config)

    conn = None
    cursor = None

    try:
        conn = pymysql.connect(
            host=db_config.get("host"),
            user=db_config.get("user"),
            password=db_config.get("password"),
            port=db_config.get("port", 3306),
        )
        cursor = conn.cursor()

        # Prepare the DROP TABLE statement
        query = f"CREATE DATABASE IF NOT EXISTS `{DB_name}` CHARACTER SET {CHARACTER_SET} COLLATE {COLLATE};"

        # Execute the DROP TABLE statement
        cursor.execute(query)
        conn.commit()

        print(f"Database `{DB_name}` created successfully.")

    except pymysql.Error as e:
        print(f"Failed to create database `{DB_name}`. Error: {e}")
    finally:
        # Ensure the connection is closed even if an error occurs
        try:
            cursor.close()
            conn.close()
        except:
            pass


def backup_database_subprocess(db_config, data_config):
    """
    Backs up a MariaDB/MySQL database using mysqldump and a db_config dictionary.

    Parameters:
    db_config (dict): A dictionary containing the database connection parameters.
                      Expected keys: 'host', 'user', 'password', 'database'.
    output_file (str): Path to the output file where the backup will be saved.
    Note:
    Using subprocess allows for better handling of the command's stdout and stderr, and it can avoid shell injection vulnerabilities
    """
    import subprocess

    data_config = coerce_data_config(data_config)
    db_config = coerce_db_config(db_config)

    output_file = join_path(data_config["out_path"], f'{db_config["database"]}.sql')
    command = [
        "mysqldump",
        f"-h{db_config.get('host')}",
        f"-u{db_config.get('user')}",
        f"--password={db_config.get('password')}",
        db_config.get('database')
    ]
    with open(output_file, 'w') as f:
        subprocess.run(command, stdout=f)


def set_index(db_config, data_config, df_desc=None, name_map: NameMap | dict | None = None):
    """Set Index using Description File"""
    import pymysql

    conn = None
    cursor = None
    _sql = ""
    data_config = coerce_data_config(data_config, inplace=isinstance(data_config, dict))
    db_config = coerce_db_config(db_config)

    df_desc, nm = _prepare_desc_and_namemap(data_config, df_desc=df_desc, name_map=name_map)
    table_name = nm.table_sql

    try:
        # Connect to the database server
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()

        for col in df_desc.index:
            try:
                if "is_key" in df_desc.columns and bool(df_desc.loc[col, "is_key"]):
                    col_sql = nm.map_column(col)
                    idx_name = make_index_name(table_name, col_sql, max_len=64)
                    _sql = (
                        f"CREATE INDEX {quote_mysql_identifier(idx_name)} "
                        f"ON {quote_mysql_identifier(table_name)} ({quote_mysql_identifier(col_sql)});"
                    )
                    # Execute the CREATE INDEX SQL statement
                    cursor.execute(_sql)
                    print(f"Set Index the `{col}` on `{table_name}` successfully.")
            except pymysql.Error as e:
                error_code = e.args[0] if getattr(e, "args", None) else None
                if error_code == 1061:
                    # Duplicate key name
                    continue
                if error_code == 1170:
                    # BLOB/TEXT needs a prefix length
                    prefix_len = int(data_config.get("index_prefix_len", 191))
                    try:
                        _sql = (
                            f"CREATE INDEX {quote_mysql_identifier(idx_name)} "
                            f"ON {quote_mysql_identifier(table_name)} "
                            f"({quote_mysql_identifier(col_sql)}({prefix_len}));"
                        )
                        cursor.execute(_sql)
                        print(f"Set Index the `{col}` on `{table_name}` successfully (prefix={prefix_len}).")
                        continue
                    except pymysql.Error as e2:
                        print(f"Failed to create prefix index `{table_name}`. Error: {e2} with {_sql}")
                        continue
                print(f"Failed to create index `{table_name}`. Error: {e} with {_sql}")

    except pymysql.Error as e:
        print(f"Failed to create index `{table_name}`. Error: {e} with {_sql}")
    finally:
        # Ensure the connection is closed
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def set_index_simple(
    db_config,
    *,
    table_name: str,
    column: str,
    name_map: NameMap | dict,
    prefix_len: int | None = 191,
) -> None:
    """
    Create an index on a single column, best-effort.

    For LONGTEXT/TEXT columns, use prefix index when prefix_len is provided.
    """
    import pymysql

    nm = load_namemap(name_map)
    if nm is None:
        raise TypeError("set_index_simple requires a valid NameMap")

    db_config = coerce_db_config(db_config)

    conn = None
    cursor = None
    try:
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()

        col_sql = nm.map_column(column)
        idx_name = make_index_name(nm.table_sql, col_sql, max_len=64)
        if prefix_len:
            _sql = (
                f"CREATE INDEX {quote_mysql_identifier(idx_name)} "
                f"ON {quote_mysql_identifier(nm.table_sql)} "
                f"({quote_mysql_identifier(col_sql)}({int(prefix_len)}));"
            )
        else:
            _sql = (
                f"CREATE INDEX {quote_mysql_identifier(idx_name)} "
                f"ON {quote_mysql_identifier(nm.table_sql)} ({quote_mysql_identifier(col_sql)});"
            )
        cursor.execute(_sql)
        conn.commit()
        print(f"Set Index `{idx_name}` on `{nm.table_sql}` successfully.")
    except pymysql.Error as e:
        print(f"Failed to create index on `{nm.table_sql}`. Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def backup_database_pymysql(db_config, data_config):
    """
    Backs up a MariaDB/MySQL database using PyMySQL and saves the output as a .sql file.

    Parameters:
    db_config (dict): A dictionary containing the database connection parameters.
                      Expected keys: 'host', 'user', 'password', 'database'.
    data_config (dict): A dictionary containing the output file path.
                      Expected key: 'out_path' (path to save the backup file).
    """
    import pymysql
    import os

    data_config = coerce_data_config(data_config)
    db_config = coerce_db_config(db_config)
    
    connection = pymysql.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database'],
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with connection.cursor() as cursor:
            # 데이터베이스의 모든 테이블 이름을 가져옴
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()

            backup_file = os.path.join(data_config['out_path'], f"{db_config['database']}_backup.sql")

            with open(backup_file, 'w') as f:
                for table in tables:
                    table_name = list(table.values())[0]

                    # 테이블 생성문 덤프
                    cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
                    create_table_stmt = cursor.fetchone()["Create Table"]
                    f.write(f"\n-- Table structure for `{table_name}`\n")
                    f.write(f"{create_table_stmt};\n\n")

                    # 테이블 데이터 덤프
                    cursor.execute(f"SELECT * FROM `{table_name}`")
                    rows = cursor.fetchall()

                    if rows:
                        columns = ", ".join([f"`{col}`" for col in rows[0].keys()])
                        f.write(f"-- Dumping data for table `{table_name}`\n")
                        f.write(f"INSERT INTO `{table_name}` ({columns}) VALUES\n")
                        row_data = []

                        for row in rows:
                            values = ", ".join([f"'{str(value)}'" if value is not None else 'NULL' for value in row.values()])
                            row_data.append(f"({values})")
                        
                        f.write(",\n".join(row_data) + ";\n\n")
                    else:
                        f.write(f"-- No data for table `{table_name}`\n\n")
                    
        print(f"Backup completed successfully. File saved at: {backup_file}")

    except Exception as e:
        print(f"Error during backup: {e}")

    finally:
        connection.close()


def optimize_table(db_config, data_config, name_map: NameMap | dict | None = None):
    """Optimize the table for MariaDB"""
    import pymysql

    conn = None
    cursor = None
    data_config = coerce_data_config(data_config)
    db_config = coerce_db_config(db_config)
    nm = load_namemap(name_map) or load_namemap(data_config.get("_name_map"))
    table_name = nm.table_sql if nm else truncate_table_name(data_config.get("table_name", ""))

    try:
        # Connect to the database server
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()

        _sql = f"OPTIMIZE TABLE `{table_name}`;"
        # Execute the CREATE INDEX SQL statement
        cursor.execute(_sql)
        print(f"Optimize table `{table_name}` successfully.")

    except pymysql.Error as e:
        print(f"Failed to optimize table `{table_name}`. Error: {e}")
    finally:
        # Ensure the connection is closed
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def init_MySQL():
    """ Initializing"""
    import os
    # os.system('service mariadb start')
    os.system("systemctl start mariadb	")
