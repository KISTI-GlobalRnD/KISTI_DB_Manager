from __future__ import annotations


def quote_mysql_identifier(name: str) -> str:
    return str(name).replace("`", "``")


def match_table_prefix(base_table: str, key_sep: str, name: str) -> str | None:
    candidates = [
        f"{base_table}{key_sep}",
        f"{base_table}-SUB{key_sep}",
        f"{base_table}_SUB{key_sep}",
    ]
    for prefix in candidates:
        if str(name).startswith(prefix):
            return prefix
    return None


def table_depth(base_table: str, key_sep: str, name: str) -> int:
    name = str(name)
    if name == str(base_table):
        return 0
    prefix = match_table_prefix(str(base_table), str(key_sep), name)
    if prefix is None:
        return 0
    suffix = name[len(prefix) :]
    parts = [part for part in suffix.split(str(key_sep)) if part]
    return max(1, len(parts)) if parts else 0


def table_display_label(base_table: str, key_sep: str, name: str) -> str:
    name = str(name)
    if name == str(base_table):
        return name
    prefix = match_table_prefix(str(base_table), str(key_sep), name)
    if prefix is None:
        return name
    suffix = name[len(prefix) :]
    if not suffix:
        return name
    if prefix.startswith(f"{base_table}-SUB") or prefix.startswith(f"{base_table}_SUB"):
        suffix = f"SUB{key_sep}{suffix}"
    return suffix.replace(str(key_sep), "/")


def infer_table_role(depth: int, *, is_base: bool) -> str:
    if is_base:
        return "base"
    if depth <= 1:
        return "sub"
    return "nested"


def relationship_join_sql(
    *,
    parent_sql: str,
    child_sql: str,
    parent_column_sql: str = "id",
    child_column_sql: str = "id",
) -> str:
    parent_q = quote_mysql_identifier(parent_sql)
    child_q = quote_mysql_identifier(child_sql)
    parent_col_q = quote_mysql_identifier(parent_column_sql or "id")
    child_col_q = quote_mysql_identifier(child_column_sql or "id")
    return (
        f"SELECT p.`{parent_col_q}` AS parent_id, c.*\n"
        f"FROM `{parent_q}` p\n"
        f"LEFT JOIN `{child_q}` c ON p.`{parent_col_q}` = c.`{child_col_q}`\n"
        "LIMIT 5;"
    )


def fallback_join_sql(*, base_table_sql: str, table_sql: str) -> str:
    base_q = quote_mysql_identifier(base_table_sql)
    table_q = quote_mysql_identifier(table_sql)
    if str(table_sql) == str(base_table_sql):
        return f"SELECT *\nFROM `{base_q}`\nLIMIT 5;"
    return (
        "SELECT b.*, s.*\n"
        f"FROM `{base_q}` b\n"
        f"LEFT JOIN `{table_q}` s ON b.`id` = s.`id`\n"
        "LIMIT 5;"
    )
