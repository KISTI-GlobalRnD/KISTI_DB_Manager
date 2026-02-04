from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _table_key(table: Mapping[str, Any]) -> str:
    # Prefer SQL name for stable DB-level diffs.
    name_sql = table.get("name_sql")
    if name_sql:
        return str(name_sql)
    return str(table.get("name_original") or table.get("table") or "(unknown)")


def _columns_map(table: Mapping[str, Any]) -> dict[str, str | None]:
    cols = table.get("columns") or []
    res: dict[str, str | None] = {}
    if not isinstance(cols, list):
        return res
    for c in cols:
        if not isinstance(c, Mapping):
            continue
        name = c.get("name")
        if not name:
            continue
        typ = c.get("column_type") or c.get("data_type") or c.get("type")
        res[str(name)] = str(typ) if typ is not None else None
    return res


def _indexes_set(table: Mapping[str, Any]) -> set[tuple[str, str, int | None, int | None]]:
    idxs = table.get("indexes") or []
    res: set[tuple[str, str, int | None, int | None]] = set()
    if not isinstance(idxs, list):
        return res
    for ix in idxs:
        if not isinstance(ix, Mapping):
            continue
        name = str(ix.get("index_name") or "")
        col = str(ix.get("column_name") or "")
        seq = ix.get("seq_in_index")
        non_unique = ix.get("non_unique")
        try:
            seq = int(seq) if seq is not None else None
        except Exception:
            seq = None
        try:
            non_unique = int(non_unique) if non_unique is not None else None
        except Exception:
            non_unique = None
        if not name and not col:
            continue
        res.add((name, col, seq, non_unique))
    return res


def _row_value(table: Mapping[str, Any]) -> int | None:
    rows = table.get("rows")
    if rows is None:
        rows = table.get("rows_estimate")
    try:
        return int(rows) if rows is not None else None
    except Exception:
        return None


@dataclass(frozen=True)
class TableDiff:
    table: str
    cols_added: list[str]
    cols_removed: list[str]
    cols_type_changed: list[str]
    indexes_added: int
    indexes_removed: int
    rows_before: int | None
    rows_after: int | None


def diff_review_files(before_path: str | Path, after_path: str | Path) -> dict[str, Any]:
    before = _load_json(before_path)
    after = _load_json(after_path)

    before_tables = before.get("tables") or []
    after_tables = after.get("tables") or []
    if not isinstance(before_tables, list):
        before_tables = []
    if not isinstance(after_tables, list):
        after_tables = []

    before_by_key = {_table_key(t): t for t in before_tables if isinstance(t, Mapping)}
    after_by_key = {_table_key(t): t for t in after_tables if isinstance(t, Mapping)}

    before_keys = set(before_by_key.keys())
    after_keys = set(after_by_key.keys())

    added_tables = sorted(after_keys - before_keys)
    removed_tables = sorted(before_keys - after_keys)
    common_tables = sorted(before_keys & after_keys)

    table_diffs: list[TableDiff] = []
    for k in common_tables:
        b = before_by_key[k]
        a = after_by_key[k]

        b_cols = _columns_map(b)
        a_cols = _columns_map(a)
        cols_added = sorted(set(a_cols.keys()) - set(b_cols.keys()))
        cols_removed = sorted(set(b_cols.keys()) - set(a_cols.keys()))

        cols_type_changed = []
        for col in sorted(set(a_cols.keys()) & set(b_cols.keys())):
            bt = b_cols.get(col)
            at = a_cols.get(col)
            if bt is None or at is None:
                continue
            if str(bt) != str(at):
                cols_type_changed.append(col)

        b_idx = _indexes_set(b)
        a_idx = _indexes_set(a)

        rows_before = _row_value(b)
        rows_after = _row_value(a)

        table_diffs.append(
            TableDiff(
                table=k,
                cols_added=cols_added,
                cols_removed=cols_removed,
                cols_type_changed=cols_type_changed,
                indexes_added=len(a_idx - b_idx),
                indexes_removed=len(b_idx - a_idx),
                rows_before=rows_before,
                rows_after=rows_after,
            )
        )

    changed = [
        td
        for td in table_diffs
        if td.cols_added
        or td.cols_removed
        or td.cols_type_changed
        or td.indexes_added
        or td.indexes_removed
        or (td.rows_before is not None and td.rows_after is not None and td.rows_before != td.rows_after)
    ]

    return {
        "before": str(before_path),
        "after": str(after_path),
        "base_table_before": before.get("base_table"),
        "base_table_after": after.get("base_table"),
        "tables_before": len(before_by_key),
        "tables_after": len(after_by_key),
        "tables_added": added_tables,
        "tables_removed": removed_tables,
        "tables_changed": [td.__dict__ for td in changed],
    }


def render_review_diff_markdown(diff: Mapping[str, Any], *, max_list: int = 50) -> str:
    added = diff.get("tables_added") or []
    removed = diff.get("tables_removed") or []
    changed = diff.get("tables_changed") or []

    lines: list[str] = []
    lines.append("# Review Pack Diff")
    lines.append("")
    lines.append(f"- before: `{diff.get('before')}`")
    lines.append(f"- after: `{diff.get('after')}`")
    if diff.get("base_table_before") or diff.get("base_table_after"):
        lines.append(f"- base_table: `{diff.get('base_table_before')}` → `{diff.get('base_table_after')}`")
    lines.append("")

    lines.append("## Summary")
    lines.append("")
    lines.append("| Metric | before | after | delta |")
    lines.append("|---|---:|---:|---:|")
    b = int(diff.get("tables_before") or 0)
    a = int(diff.get("tables_after") or 0)
    lines.append(f"| tables | {b} | {a} | {a - b:+d} |")
    lines.append(f"| tables_added |  | {len(added)} |  |")
    lines.append(f"| tables_removed |  | {len(removed)} |  |")
    lines.append(f"| tables_changed |  | {len(changed)} |  |")
    lines.append("")

    if added:
        lines.append("## Added Tables")
        lines.append("")
        for t in list(added)[: int(max_list)]:
            lines.append(f"- `{t}`")
        if len(added) > int(max_list):
            lines.append(f"- ... +{len(added) - int(max_list)} more")
        lines.append("")

    if removed:
        lines.append("## Removed Tables")
        lines.append("")
        for t in list(removed)[: int(max_list)]:
            lines.append(f"- `{t}`")
        if len(removed) > int(max_list):
            lines.append(f"- ... +{len(removed) - int(max_list)} more")
        lines.append("")

    if changed:
        lines.append("## Changed Tables")
        lines.append("")
        lines.append("| table | +cols | -cols | type_changed | +idx | -idx | rows_before | rows_after |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
        for td in list(changed)[: int(max_list)]:
            lines.append(
                "| `{table}` | {pa} | {pr} | {tc} | {ia} | {ir} | {rb} | {ra} |".format(
                    table=td.get("table"),
                    pa=len(td.get("cols_added") or []),
                    pr=len(td.get("cols_removed") or []),
                    tc=len(td.get("cols_type_changed") or []),
                    ia=int(td.get("indexes_added") or 0),
                    ir=int(td.get("indexes_removed") or 0),
                    rb=td.get("rows_before") if td.get("rows_before") is not None else "",
                    ra=td.get("rows_after") if td.get("rows_after") is not None else "",
                )
            )
        if len(changed) > int(max_list):
            lines.append(f"\n- ... +{len(changed) - int(max_list)} more")
        lines.append("")

    return "\n".join(lines) + "\n"

