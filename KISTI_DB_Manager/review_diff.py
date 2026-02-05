from __future__ import annotations

import html
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


def _table_infos_from_doc(doc: Mapping[str, Any]) -> dict[str, "TableInfo"]:
    from .review import TableInfo

    tables = doc.get("tables") or []
    if not isinstance(tables, list):
        return {}

    res: dict[str, TableInfo] = {}
    for t in tables:
        if not isinstance(t, Mapping):
            continue
        name_sql = t.get("name_sql") or t.get("table") or t.get("name")
        if not name_sql:
            continue
        name_sql = str(name_sql)
        cols = t.get("columns")
        cols = cols if isinstance(cols, list) else None
        idxs = t.get("indexes")
        idxs = idxs if isinstance(idxs, list) else None

        row_count = t.get("rows")
        try:
            row_count = int(row_count) if row_count is not None else None
        except Exception:
            row_count = None

        row_est = t.get("rows_estimate")
        try:
            row_est = int(row_est) if row_est is not None else None
        except Exception:
            row_est = None

        rows_exact = bool(t.get("rows_exact") or False)
        data_length = t.get("data_length")
        index_length = t.get("index_length")
        try:
            data_length = int(data_length) if data_length is not None else None
        except Exception:
            data_length = None
        try:
            index_length = int(index_length) if index_length is not None else None
        except Exception:
            index_length = None

        res[name_sql] = TableInfo(
            name_sql=name_sql,
            name_original=(str(t.get("name_original")) if t.get("name_original") else None),
            row_count=row_count,
            row_count_exact=rows_exact,
            table_rows_estimate=row_est,
            data_length=data_length,
            index_length=index_length,
            engine=(str(t.get("engine")) if t.get("engine") else None),
            collation=(str(t.get("collation")) if t.get("collation") else None),
            columns=cols,
            indexes=idxs,
        )
    return res


def render_review_diff_schema_svg(
    *,
    before_doc: Mapping[str, Any],
    after_doc: Mapping[str, Any],
    diff: Mapping[str, Any],
) -> str:
    from .review import TableInfo, render_simple_svg

    before_infos = _table_infos_from_doc(before_doc)
    after_infos = _table_infos_from_doc(after_doc)

    union: dict[str, TableInfo] = dict(after_infos)
    for k, v in before_infos.items():
        union.setdefault(k, v)

    # Use SQL names for stable graph edges.
    table_infos_graph = [
        TableInfo(
            name_sql=ti.name_sql,
            name_original=None,
            row_count=ti.row_count,
            row_count_exact=ti.row_count_exact,
            table_rows_estimate=ti.table_rows_estimate,
            data_length=ti.data_length,
            index_length=ti.index_length,
            engine=ti.engine,
            collation=ti.collation,
            columns=ti.columns,
            indexes=ti.indexes,
        )
        for ti in sorted(union.values(), key=lambda t: t.name_sql)
    ]

    key_sep = str(after_doc.get("key_sep") or before_doc.get("key_sep") or "__")

    base = str(after_doc.get("base_table_sql") or after_doc.get("base_table") or before_doc.get("base_table_sql") or before_doc.get("base_table") or "")
    if base and base not in union:
        # When base_table is the original name, prefer base_table_sql if present.
        base_sql = str(after_doc.get("base_table_sql") or before_doc.get("base_table_sql") or "")
        base = base_sql if base_sql and base_sql in union else base
    if not base or base not in union:
        base = table_infos_graph[0].name_sql if table_infos_graph else "base"

    status_by_table: dict[str, str] = {}
    for t in diff.get("tables_added") or []:
        status_by_table[str(t)] = "added"
    for t in diff.get("tables_removed") or []:
        status_by_table[str(t)] = "removed"
    for td in diff.get("tables_changed") or []:
        if isinstance(td, Mapping) and td.get("table"):
            status_by_table[str(td.get("table"))] = "changed"

    class_by_sql: dict[str, str] = {}
    fill_by_sql: dict[str, str] = {}
    for t, st in status_by_table.items():
        if st == "added":
            class_by_sql[t] = "diff-added"
            fill_by_sql[t] = "#dafbe1"
        elif st == "removed":
            class_by_sql[t] = "diff-removed"
            fill_by_sql[t] = "#ffebe9"
        elif st == "changed":
            class_by_sql[t] = "diff-changed"
            fill_by_sql[t] = "#fff8c5"

    return render_simple_svg(
        base_table=base,
        table_infos=table_infos_graph,
        key_sep=key_sep,
        node_class_by_sql=class_by_sql,
        node_fill_by_sql=fill_by_sql,
        width=1600,
    )


def render_review_diff_html(
    *,
    before_path: str | Path,
    after_path: str | Path,
    diff: Mapping[str, Any],
    before_doc: Mapping[str, Any],
    after_doc: Mapping[str, Any],
    schema_svg_text: str,
    max_list: int = 50,
) -> str:
    def h(x: Any) -> str:
        return html.escape(str(x))

    added = list(diff.get("tables_added") or [])
    removed = list(diff.get("tables_removed") or [])
    changed = list(diff.get("tables_changed") or [])

    # Details data
    before_infos = _table_infos_from_doc(before_doc)
    after_infos = _table_infos_from_doc(after_doc)
    union_tables = sorted(set(before_infos.keys()) | set(after_infos.keys()))

    status_by_table: dict[str, str] = {t: "unchanged" for t in union_tables}
    for t in added:
        status_by_table[str(t)] = "added"
    for t in removed:
        status_by_table[str(t)] = "removed"
    for td in changed:
        if isinstance(td, Mapping) and td.get("table"):
            status_by_table[str(td.get("table"))] = "changed"

    changed_by_table: dict[str, Mapping[str, Any]] = {}
    for td in changed:
        if isinstance(td, Mapping) and td.get("table"):
            changed_by_table[str(td.get("table"))] = td

    # Inline SVG for interactivity
    svg_inline = str(schema_svg_text)
    if svg_inline.lstrip().startswith("<?xml"):
        svg_inline = svg_inline.split("?>", 1)[-1]

    key_sep = str(after_doc.get("key_sep") or before_doc.get("key_sep") or "__")
    base = str(
        after_doc.get("base_table_sql")
        or after_doc.get("base_table")
        or before_doc.get("base_table_sql")
        or before_doc.get("base_table")
        or ""
    )
    if base and base not in union_tables:
        base_sql = str(after_doc.get("base_table_sql") or before_doc.get("base_table_sql") or "")
        base = base_sql if base_sql and base_sql in union_tables else base
    if not base or base not in union_tables:
        base = union_tables[0] if union_tables else "base"

    key_sep_json = json.dumps(key_sep, ensure_ascii=False).replace("<", "\\u003c")
    base_json = json.dumps(base, ensure_ascii=False).replace("<", "\\u003c")

    def badge(status: str) -> str:
        cls = {
            "added": "badge added",
            "removed": "badge removed",
            "changed": "badge changed",
            "unchanged": "badge",
        }.get(status, "badge")
        return f'<span class="{h(cls)}">{h(status)}</span>'

    # Summary rows
    summary_rows = []
    summary_rows.append(f"<tr><td>tables_before</td><td>{int(diff.get('tables_before') or 0)}</td></tr>")
    summary_rows.append(f"<tr><td>tables_after</td><td>{int(diff.get('tables_after') or 0)}</td></tr>")
    summary_rows.append(f"<tr><td>tables_added</td><td>{len(added)}</td></tr>")
    summary_rows.append(f"<tr><td>tables_removed</td><td>{len(removed)}</td></tr>")
    summary_rows.append(f"<tr><td>tables_changed</td><td>{len(changed)}</td></tr>")

    # Lists
    added_list = "".join([f"<li><code>{h(t)}</code></li>" for t in added[: int(max_list)]]) or "<li class=\"muted\">(none)</li>"
    removed_list = "".join([f"<li><code>{h(t)}</code></li>" for t in removed[: int(max_list)]]) or "<li class=\"muted\">(none)</li>"

    # Changed table table
    changed_rows = []
    for td in changed[: int(max_list)]:
        if not isinstance(td, Mapping):
            continue
        t = td.get("table")
        if not t:
            continue
        details_id = f"table_{_sanitize_id(t)}"
        changed_rows.append(
            "<tr>"
            f"<td><code><a href=\"#{h(details_id)}\">{h(t)}</a></code></td>"
            f"<td>{len(td.get('cols_added') or [])}</td>"
            f"<td>{len(td.get('cols_removed') or [])}</td>"
            f"<td>{len(td.get('cols_type_changed') or [])}</td>"
            f"<td>{int(td.get('indexes_added') or 0)}</td>"
            f"<td>{int(td.get('indexes_removed') or 0)}</td>"
            "</tr>"
        )

    changed_table_html = (
        "".join(changed_rows)
        if changed_rows
        else '<tr><td colspan="6" class="muted">(none)</td></tr>'
    )

    # Per-table details (scroll targets)
    details_blocks = []
    for t in union_tables:
        st = status_by_table.get(t, "unchanged")
        info = after_infos.get(t) or before_infos.get(t)
        cols_n = len(info.columns or []) if info and info.columns is not None else None
        cols_label = str(cols_n) if cols_n is not None else "n/a"
        td = changed_by_table.get(t)

        body_lines = []
        body_lines.append(f"<div>status: {badge(st)}</div>")
        body_lines.append(f"<div>cols: <code>{h(cols_label)}</code></div>")
        if td:
            body_lines.append(
                "<div class=\"grid\">"
                f"<div>+cols: <code>{h(len(td.get('cols_added') or []))}</code></div>"
                f"<div>-cols: <code>{h(len(td.get('cols_removed') or []))}</code></div>"
                f"<div>type_changed: <code>{h(len(td.get('cols_type_changed') or []))}</code></div>"
                f"<div>+idx: <code>{h(int(td.get('indexes_added') or 0))}</code></div>"
                f"<div>-idx: <code>{h(int(td.get('indexes_removed') or 0))}</code></div>"
                "</div>"
            )
            # Small lists
            for key, label in [("cols_added", "cols_added"), ("cols_removed", "cols_removed"), ("cols_type_changed", "cols_type_changed")]:
                items = td.get(key) or []
                if items:
                    li = "".join([f"<li><code>{h(x)}</code></li>" for x in list(items)[:20]])
                    body_lines.append(f"<h4>{h(label)}</h4><ul>{li}</ul>")

        details_id = f"table_{_sanitize_id(t)}"
        details_blocks.append(
            f"<details class=\"details\" id=\"{h(details_id)}\" data-table=\"{h(t)}\">"
            f"<summary><code>{h(t)}</code> · {badge(st)}</summary>"
            "<div class=\"card\" style=\"margin-top: 12px;\">"
            + "".join(body_lines)
            + "</div></details>"
        )

    details_html = "".join(details_blocks) if details_blocks else '<div class="muted">(none)</div>'

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Review Pack Diff</title>
  <style>
    body {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; margin: 24px; color: #1f2328; }}
    code {{ background: #f6f8fa; padding: 2px 5px; border-radius: 6px; }}
    code a {{ color: inherit; text-decoration: none; }}
    code a:hover {{ text-decoration: underline; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #d0d7de; padding: 8px; font-size: 13px; }}
    th {{ background: #f6f8fa; text-align: left; }}
    .muted {{ color: #57606a; }}
    .card {{ border: 1px solid #d0d7de; border-radius: 12px; padding: 16px; margin: 16px 0; background: #ffffff; }}
	    details.details summary {{ cursor: pointer; }}
	    details.details {{ border: 1px solid #d0d7de; border-radius: 12px; padding: 10px 12px; margin: 10px 0; background: #fff; }}
	    .schema-toolbar {{ display: flex; gap: 8px; align-items: center; margin-bottom: 10px; flex-wrap: wrap; }}
	    .schema-toolbar .schema-option {{ display: inline-flex; gap: 6px; align-items: center; }}
	    .schema-toolbar input[type="search"] {{ flex: 1; padding: 8px 10px; border: 1px solid #d0d7de; border-radius: 10px; }}
	    .schema-toolbar input[type="range"] {{ width: 140px; }}
	    .schema-toolbar button {{ padding: 8px 12px; border: 1px solid #d0d7de; border-radius: 10px; background: #f6f8fa; cursor: pointer; }}
	    .schema-container {{ max-height: 70vh; overflow: auto; border: 1px solid #d0d7de; border-radius: 12px; padding: 8px; background: #fff; }}
	    .schema-container svg {{ max-width: 100%; height: auto; }}
	    .schema-container .node.hidden {{ display: none; }}
	    .schema-container .edge.hidden {{ display: none; }}
	    .schema-container .node.dim {{ opacity: 0.15; }}
	    .schema-container .node.match .box {{ stroke: #fb8c00; stroke-width: 2; }}
	    .schema-container .node.focus-root .box {{ stroke: #0969da; stroke-width: 3; stroke-dasharray: none; }}
	    .schema-container .node.focus-path .box {{ stroke: #0969da; stroke-width: 2; stroke-dasharray: 6 3; }}
	    .schema-container .edge.focus-path {{ stroke: #0969da; stroke-width: 2; stroke-dasharray: 6 3; }}
	    .schema-container .node.selected .box {{ stroke: #0969da; stroke-width: 2; }}
	    .schema-container .edge.selected {{ stroke: #0969da; stroke-width: 2; stroke-dasharray: none; }}
    .badge {{ display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; border: 1px solid #d0d7de; background: #f6f8fa; }}
    .badge.added {{ border-color: #1a7f37; background: #dafbe1; }}
    .badge.removed {{ border-color: #cf222e; background: #ffebe9; }}
    .badge.changed {{ border-color: #bf8700; background: #fff8c5; }}
    .grid {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 8px; margin-top: 10px; }}
  </style>
</head>
<body>
  <h1>Review Pack Diff</h1>
  <p class="muted">before: <code>{h(before_path)}</code> · after: <code>{h(after_path)}</code></p>

  <div class="card">
    <h2>Summary</h2>
    <table>
      <tbody>
        {''.join(summary_rows)}
      </tbody>
    </table>
  </div>

  <div class="card">
    <h2>Diagram</h2>
		    <div class="schema-toolbar">
		      <input id="schema-search" type="search" placeholder="Search table…" />
		      <button id="schema-reset" type="button">Reset</button>
		      <button id="schema-download-svg" type="button">SVG</button>
		      <button id="schema-download-png" type="button">PNG</button>
		      <span class="schema-option">
		        <span class="muted">Depth</span>
		        <input id="schema-depth" type="range" min="0" max="0" step="1" />
		        <code id="schema-depth-value">0</code>
		      </span>
		      <label class="schema-option">
		        <input id="schema-only-flagged" type="checkbox" />
		        <span class="muted">Only changed</span>
		      </label>
		      <span class="schema-option">
		        <label class="schema-option">
		          <input id="schema-focus" type="checkbox" />
		          <span class="muted">Focus</span>
		        </label>
		        <select id="schema-focus-mode">
		          <option value="subtree">subtree</option>
		          <option value="khop">k-hop</option>
		          <option value="path">path-to-base</option>
		        </select>
		        <span class="muted">hops</span>
		        <input id="schema-focus-hops" type="range" min="1" max="6" step="1" value="2" />
		        <code id="schema-focus-hops-value">2</code>
		        <label class="schema-option">
		          <input id="schema-focus-path" type="checkbox" checked />
		          <span class="muted">Base path</span>
		        </label>
		      </span>
		      <span id="schema-status" class="muted"></span>
		    </div>
		    <div id="schema-container" class="schema-container">
	      {svg_inline}
    </div>
    <p class="muted">Legend: {badge('added')} {badge('removed')} {badge('changed')} {badge('unchanged')}</p>
  </div>

  <div class="card">
    <h2>Added Tables</h2>
    <ul>{added_list}</ul>
  </div>

  <div class="card">
    <h2>Removed Tables</h2>
    <ul>{removed_list}</ul>
  </div>

  <div class="card">
    <h2>Changed Tables</h2>
    <table>
      <thead><tr><th>table</th><th>+cols</th><th>-cols</th><th>type_changed</th><th>+idx</th><th>-idx</th></tr></thead>
      <tbody>
        {changed_table_html}
      </tbody>
    </table>
  </div>

  <div class="card">
    <h2>Per-table Details</h2>
    {details_html}
  </div>

  <script>
  (function() {{
    const container = document.getElementById('schema-container');
    if (!container) return;

		    const search = document.getElementById('schema-search');
		    const reset = document.getElementById('schema-reset');
		    const dlSvg = document.getElementById('schema-download-svg');
		    const dlPng = document.getElementById('schema-download-png');
		    const status = document.getElementById('schema-status');
		    const svg = container.querySelector('svg');
		    if (!svg) return;

	    const nodes = Array.from(svg.querySelectorAll('.node'));
	    const edges = Array.from(svg.querySelectorAll('.edge'));
		    const KEY_SEP = {key_sep_json};
		    const BASE_TABLE_SQL = {base_json};
		    const depthInput = document.getElementById('schema-depth');
		    const depthValue = document.getElementById('schema-depth-value');
		    const onlyFlagged = document.getElementById('schema-only-flagged');
		    const focus = document.getElementById('schema-focus');
		    const focusMode = document.getElementById('schema-focus-mode');
		    const focusHops = document.getElementById('schema-focus-hops');
		    const focusHopsValue = document.getElementById('schema-focus-hops-value');
		    const focusBasePath = document.getElementById('schema-focus-path');

	    function matchPrefix(nameSql) {{
	      if (!nameSql || !BASE_TABLE_SQL || !KEY_SEP) return null;
	      const candidates = [
	        BASE_TABLE_SQL + KEY_SEP,
	        BASE_TABLE_SQL + '-SUB' + KEY_SEP,
	        BASE_TABLE_SQL + '_SUB' + KEY_SEP,
	      ];
	      for (const p of candidates) {{
	        if (nameSql.startsWith(p)) return p;
	      }}
	      return null;
	    }}

	    function nodeDepth(nameSql) {{
	      if (!nameSql) return 0;
	      if (nameSql === BASE_TABLE_SQL) return 0;
	      const prefix = matchPrefix(nameSql);
	      if (!prefix) return 0;
	      const suffix = nameSql.substring(prefix.length);
	      const parts = suffix.split(KEY_SEP).filter(Boolean);
	      return Math.max(1, parts.length);
	    }}

	    const depthBySql = {{}};
	    let maxDepth = 0;
	    const nodeBySql = {{}};
	    for (const n of nodes) {{
	      const sql = n.getAttribute('data-name-sql') || '';
	      if (!sql) continue;
	      nodeBySql[sql] = n;
	      const d = nodeDepth(sql);
	      depthBySql[sql] = d;
	      if (d > maxDepth) maxDepth = d;
	    }}

	    if (depthInput) {{
	      depthInput.max = String(maxDepth);
	      depthInput.value = String(maxDepth);
	    }}
	    if (depthValue) {{
	      depthValue.textContent = depthInput ? String(depthInput.value) : String(maxDepth);
	    }}

	    const detailsByTable = {{}};
	    for (const d of document.querySelectorAll('details.details[data-table]')) {{
	      const t = d.getAttribute('data-table');
      if (t) detailsByTable[t] = d;
    }}

		    function clearSelection() {{
		      for (const n of nodes) n.classList.remove('selected');
		      for (const e of edges) e.classList.remove('selected');
		    }}

		    const parentByChildSql = {{}};
		    const childrenByParentSql = {{}};
		    const neighborsBySql = {{}};
		    const edgeByChildSql = {{}};
		    function _addNeighbor(a, b) {{
		      if (!a || !b) return;
		      if (!neighborsBySql[a]) neighborsBySql[a] = new Set();
		      neighborsBySql[a].add(b);
		    }}
		    for (const e of edges) {{
		      const p = e.getAttribute('data-parent-sql') || '';
		      const c = e.getAttribute('data-child-sql') || '';
		      if (p && c && !(c in parentByChildSql)) parentByChildSql[c] = p;
		      if (p && c) {{
		        if (!childrenByParentSql[p]) childrenByParentSql[p] = [];
		        childrenByParentSql[p].push(c);
		        if (!(c in edgeByChildSql)) edgeByChildSql[c] = e;
		        _addNeighbor(p, c);
		        _addNeighbor(c, p);
		      }}
		    }}

		    function joinPathToBase(tableSql) {{
		      if (!tableSql || !BASE_TABLE_SQL) return null;
		      const path = [];
		      let cur = tableSql;
		      let safety = 0;
		      while (cur && safety++ < 1000) {{
		        path.push(cur);
		        if (cur === BASE_TABLE_SQL) break;
		        cur = parentByChildSql[cur];
		      }}
		      if (!path.length) return null;
		      if (path[path.length - 1] !== BASE_TABLE_SQL) return null;
		      path.reverse();
		      return path;
		    }}

		    let selectedTableSql = '';
		    let focusRootSql = '';
		    let prevFocusRootSql = '';
		    let focusPathNodes = [];
		    let focusPathEdges = [];

		    function clearFocusRoot() {{
		      if (prevFocusRootSql && nodeBySql[prevFocusRootSql]) {{
		        nodeBySql[prevFocusRootSql].classList.remove('focus-root');
		      }}
		      prevFocusRootSql = '';
		      focusRootSql = '';
		    }}

		    function setFocusRoot(sql) {{
		      const next = String(sql || '');
		      if (next === focusRootSql) return;
		      if (prevFocusRootSql && nodeBySql[prevFocusRootSql]) {{
		        nodeBySql[prevFocusRootSql].classList.remove('focus-root');
		      }}
		      focusRootSql = next;
		      prevFocusRootSql = next;
		      if (focusRootSql && nodeBySql[focusRootSql]) {{
		        nodeBySql[focusRootSql].classList.add('focus-root');
		      }}
		    }}

		    function clearFocusPath() {{
		      for (const n of focusPathNodes) {{
		        try {{ n.classList.remove('focus-path'); }} catch (_e) {{}}
		      }}
		      for (const e of focusPathEdges) {{
		        try {{ e.classList.remove('focus-path'); }} catch (_e) {{}}
		      }}
		      focusPathNodes = [];
		      focusPathEdges = [];
		    }}

		    function applyFocusPath(path) {{
		      clearFocusPath();
		      if (!path || !Array.isArray(path) || path.length < 2) return;
		      for (const sql of path) {{
		        const n = nodeBySql[String(sql || '')];
		        if (n) {{
		          n.classList.add('focus-path');
		          focusPathNodes.push(n);
		        }}
		      }}
		      for (let i = 1; i < path.length; i++) {{
		        const child = String(path[i] || '');
		        const e = edgeByChildSql[child];
		        if (e) {{
		          e.classList.add('focus-path');
		          focusPathEdges.push(e);
		        }}
		      }}
		    }}

		    function subtreeAllow(root) {{
		      const out = new Set();
		      const stack = [String(root || '')];
		      let safety = 0;
		      while (stack.length && safety++ < 200000) {{
		        const cur = stack.pop();
		        if (!cur || out.has(cur)) continue;
		        out.add(cur);
		        const kids = childrenByParentSql[cur] || [];
		        for (const c of kids) stack.push(c);
		      }}
		      return out;
		    }}

		    function khopAllow(root, hops) {{
		      const h = Math.max(1, Math.min(50, Number(hops || 1)));
		      const out = new Set();
		      const q = [[String(root || ''), 0]];
		      out.add(String(root || ''));
		      let safety = 0;
		      while (q.length && safety++ < 200000) {{
		        const item = q.shift();
		        if (!item) break;
		        const cur = item[0];
		        const d = item[1];
		        if (d >= h) continue;
		        const neigh = neighborsBySql[cur];
		        if (!neigh) continue;
		        for (const nb of neigh) {{
		          if (!nb || out.has(nb)) continue;
		          out.add(nb);
		          q.push([nb, d + 1]);
		        }}
		      }}
		      return out;
		    }}

		    function updateFocusControls() {{
		      const enabled = !!(focus && focus.checked);
		      const mode = focusMode ? String(focusMode.value || 'subtree') : 'subtree';
		      if (focusMode) focusMode.disabled = !enabled;
		      if (focusHops) focusHops.disabled = !enabled || mode !== 'khop';
		      if (focusBasePath) focusBasePath.disabled = !enabled || mode === 'path';
		      const hops = focusHops ? Number(focusHops.value || 2) : 2;
		      if (focusHopsValue) focusHopsValue.textContent = String(hops);
		    }}

		    function parseBool(v) {{
		      const s = String(v || '').toLowerCase();
		      return s === '1' || s === 'true' || s === 'yes' || s === 'y' || s === 'on';
		    }}

		    function buildUiState() {{
		      return {{
		        q: search ? String(search.value || '') : '',
		        depth: depthInput ? Number(depthInput.value || maxDepth) : maxDepth,
		        only: !!(onlyFlagged && onlyFlagged.checked),
		        focus: !!(focus && focus.checked),
		        fmode: focusMode ? String(focusMode.value || 'subtree') : 'subtree',
		        hops: focusHops ? Number(focusHops.value || 2) : 2,
		        bpath: !!(focusBasePath && focusBasePath.checked),
		        sel: String(selectedTableSql || ''),
		        froot: String(focusRootSql || ''),
		      }};
		    }}

		    const STORAGE_KEY = 'kisti-review:diff:' + String(BASE_TABLE_SQL || '');
		    let persistTimer = null;
		    let persistSuppressed = false;

		    function readStateFromUrl() {{
		      try {{
		        const params = new URLSearchParams(window.location.search || '');
		        const keys = ['q','depth','only','focus','fmode','hops','bpath','sel','froot'];
		        let has = false;
		        for (const k of keys) {{
		          if (params.has(k)) {{ has = true; break; }}
		        }}
		        if (!has) return null;
		        return {{
		          q: params.get('q') || '',
		          depth: params.get('depth'),
		          only: params.get('only'),
		          focus: params.get('focus'),
		          fmode: params.get('fmode') || '',
		          hops: params.get('hops'),
		          bpath: params.get('bpath'),
		          sel: params.get('sel') || '',
		          froot: params.get('froot') || '',
		        }};
		      }} catch (_e) {{
		        return null;
		      }}
		    }}

		    function readStateFromStorage() {{
		      try {{
		        if (!window.localStorage) return null;
		        const raw = localStorage.getItem(STORAGE_KEY);
		        if (!raw) return null;
		        return JSON.parse(raw);
		      }} catch (_e) {{
		        return null;
		      }}
		    }}

		    function applyState(st) {{
		      if (!st || typeof st !== 'object') return;
		      if (search && typeof st.q === 'string') search.value = st.q;
		      if (depthInput && st.depth != null) {{
		        const v = Number(st.depth);
		        if (isFinite(v)) depthInput.value = String(Math.max(0, Math.min(maxDepth, v)));
		      }}
		      if (onlyFlagged && st.only != null) onlyFlagged.checked = parseBool(st.only);
		      if (focus && st.focus != null) focus.checked = parseBool(st.focus);
		      if (focusMode && typeof st.fmode === 'string') {{
		        const m = String(st.fmode || '');
		        if (m === 'subtree' || m === 'khop' || m === 'path') focusMode.value = m;
		      }}
		      if (focusHops && st.hops != null) {{
		        const v = Number(st.hops);
		        if (isFinite(v)) focusHops.value = String(Math.max(1, Math.min(6, v)));
		      }}
		      if (focusBasePath && st.bpath != null) focusBasePath.checked = parseBool(st.bpath);
		      if (typeof st.sel === 'string') selectedTableSql = st.sel;
		      if (typeof st.froot === 'string') focusRootSql = st.froot;
		      updateFocusControls();
		      if (focusHopsValue && focusHops) focusHopsValue.textContent = String(focusHops.value || '');
		    }}

		    function writeStateToStorage(st) {{
		      try {{
		        if (!window.localStorage) return;
		        localStorage.setItem(STORAGE_KEY, JSON.stringify(st));
		      }} catch (_e) {{}}
		    }}

		    function writeStateToUrl(st) {{
		      try {{
		        const url = new URL(window.location.href);
		        const params = url.searchParams;
		        function setOrDel(k, v, def) {{
		          const sv = String(v == null ? '' : v);
		          const sd = String(def == null ? '' : def);
		          if (sv === sd || sv === '') params.delete(k);
		          else params.set(k, sv);
		        }}
		        setOrDel('q', st.q || '', '');
		        setOrDel('depth', String(st.depth), String(maxDepth));
		        setOrDel('only', st.only ? '1' : '', '');
		        setOrDel('focus', st.focus ? '1' : '', '');
		        setOrDel('fmode', st.fmode || '', 'subtree');
		        setOrDel('hops', String(st.hops), '2');
		        setOrDel('bpath', st.bpath ? '1' : '0', '1');
		        setOrDel('sel', st.sel || '', '');
		        setOrDel('froot', st.froot || '', '');
		        url.search = params.toString();
		        window.history.replaceState(null, '', url.toString());
		      }} catch (_e) {{}}
		    }}

		    function schedulePersist() {{
		      if (persistSuppressed) return;
		      if (persistTimer) clearTimeout(persistTimer);
		      persistTimer = setTimeout(() => {{
		        persistTimer = null;
		        const st = buildUiState();
		        writeStateToStorage(st);
		        writeStateToUrl(st);
		      }}, 200);
		    }}

		    function safeFileName(text) {{
		      const t = String(text || 'schema_diff');
		      const s = t.replace(/[^0-9A-Za-z_.-]+/g, '_');
		      return (s.length > 120 ? s.slice(0, 120) : s) || 'schema_diff';
		    }}

		    function downloadBlob(blob, filename) {{
		      try {{
		        const url = URL.createObjectURL(blob);
		        const a = document.createElement('a');
		        a.href = url;
		        a.download = String(filename || 'download');
		        document.body.appendChild(a);
		        a.click();
		        a.remove();
		        setTimeout(() => URL.revokeObjectURL(url), 2000);
		      }} catch (_e) {{}}
		    }}

		    const EXTRA_SVG_CSS = [
		      '.node.hidden{{display:none;}}',
		      '.edge.hidden{{display:none;}}',
		      '.node.dim{{opacity:0.15;}}',
		      '.node.match .box{{stroke:#fb8c00;stroke-width:2;}}',
		      '.node.focus-root .box{{stroke:#0969da;stroke-width:3;stroke-dasharray:none;}}',
		      '.node.focus-path .box{{stroke:#0969da;stroke-width:2;stroke-dasharray:6 3;}}',
		      '.edge.focus-path{{stroke:#0969da;stroke-width:2;stroke-dasharray:6 3;}}',
		      '.node.selected .box{{stroke:#0969da;stroke-width:2;stroke-dasharray:none;}}',
		      '.edge.selected{{stroke:#0969da;stroke-width:2;stroke-dasharray:none;}}',
		    ].join('\\n');

		    function exportSvgText() {{
		      const clone = svg.cloneNode(true);
		      for (const el of Array.from(clone.querySelectorAll('.hidden'))) {{
		        try {{ el.remove(); }} catch (_e) {{}}
		      }}
		      const ns = 'http://www.w3.org/2000/svg';
		      let styleEl = clone.querySelector('style');
		      if (!styleEl) {{
		        styleEl = document.createElementNS(ns, 'style');
		        clone.insertBefore(styleEl, clone.firstChild);
		      }}
		      styleEl.textContent = (styleEl.textContent || '') + '\\n' + EXTRA_SVG_CSS;
		      clone.setAttribute('xmlns', ns);
		      const xml = new XMLSerializer().serializeToString(clone);
		      if (xml.trim().startsWith('<?xml')) return xml;
		      return '<?xml version=\"1.0\" encoding=\"UTF-8\"?>\\n' + xml;
		    }}

		    function exportSvg() {{
		      const txt = exportSvgText();
		      const blob = new Blob([txt], {{ type: 'image/svg+xml;charset=utf-8' }});
		      const name = safeFileName(BASE_TABLE_SQL || 'schema_diff') + '.svg';
		      downloadBlob(blob, name);
		    }}

		    function exportPng() {{
		      try {{
		        const txt = exportSvgText();
		        const blob = new Blob([txt], {{ type: 'image/svg+xml;charset=utf-8' }});
		        const url = URL.createObjectURL(blob);
		        const img = new Image();
		        img.onload = () => {{
		          const w = Number(svg.getAttribute('width') || 0) || 1400;
		          const h = Number(svg.getAttribute('height') || 0) || 800;
		          const scale = 2;
		          const canvas = document.createElement('canvas');
		          canvas.width = Math.max(1, Math.floor(w * scale));
		          canvas.height = Math.max(1, Math.floor(h * scale));
		          const ctx = canvas.getContext('2d');
		          if (!ctx) {{
		            URL.revokeObjectURL(url);
		            return;
		          }}
		          ctx.fillStyle = '#ffffff';
		          ctx.fillRect(0, 0, canvas.width, canvas.height);
		          ctx.scale(scale, scale);
		          ctx.drawImage(img, 0, 0, w, h);
		          canvas.toBlob((pngBlob) => {{
		            if (pngBlob) {{
		              const name = safeFileName(BASE_TABLE_SQL || 'schema_diff') + '.png';
		              downloadBlob(pngBlob, name);
		            }}
		            URL.revokeObjectURL(url);
		          }}, 'image/png');
		        }};
		        img.onerror = () => {{
		          URL.revokeObjectURL(url);
		        }};
		        img.src = url;
		      }} catch (_e) {{}}
		    }}

	    function isFlagged(nodeEl) {{
	      return (
	        nodeEl.classList.contains('diff-added') ||
	        nodeEl.classList.contains('diff-removed') ||
	        nodeEl.classList.contains('diff-changed') ||
	        nodeEl.classList.contains('has-error') ||
	        nodeEl.classList.contains('has-warning') ||
	        nodeEl.classList.contains('has-quarantine')
	      );
	    }}

		    let visibleNodesCount = nodes.length;
		    function recomputeVisibility() {{
		      const depthLimit = depthInput ? Number(depthInput.value || maxDepth) : maxDepth;
		      if (depthValue) depthValue.textContent = String(depthLimit);
		      const only = !!(onlyFlagged && onlyFlagged.checked);
		      const focusEnabled = !!(focus && focus.checked);
		      const fmode = focusMode ? String(focusMode.value || 'subtree') : 'subtree';
		      const fhops = focusHops ? Number(focusHops.value || 2) : 2;
		      const basePathOn = focusEnabled && (fmode === 'path' || (!!focusBasePath && focusBasePath.checked));

		      updateFocusControls();

		      const allow = new Set();
		      if (only) {{
		        if (BASE_TABLE_SQL) allow.add(BASE_TABLE_SQL);
	        for (const n of nodes) {{
	          const sql = n.getAttribute('data-name-sql') || '';
	          if (!sql) continue;
	          if (!isFlagged(n)) continue;
	          if ((depthBySql[sql] || 0) > depthLimit) continue;
	          allow.add(sql);
	          let cur = sql;
	          let safety = 0;
	          while (safety++ < 1000) {{
	            const p = parentByChildSql[cur];
	            if (!p) break;
	            allow.add(p);
	            if (p === BASE_TABLE_SQL) break;
	            cur = p;
	          }}
		        }}
		      }}

		      let allowFocus = null;
		      if (focusEnabled) {{
		        const root = focusRootSql || selectedTableSql || BASE_TABLE_SQL;
		        if (root) {{
		          setFocusRoot(root);
		          if (fmode === 'khop') allowFocus = khopAllow(root, fhops);
		          else if (fmode === 'path') {{
		            const p = joinPathToBase(root);
		            allowFocus = new Set(p || [root]);
		          }} else allowFocus = subtreeAllow(root);

		          const p = basePathOn ? joinPathToBase(root) : null;
		          if (p && fmode !== 'path') {{
		            for (const x of p) allowFocus.add(String(x || ''));
		          }}
		          if (p) applyFocusPath(p);
		          else clearFocusPath();
		        }} else {{
		          clearFocusRoot();
		          clearFocusPath();
		        }}
		      }} else {{
		        clearFocusRoot();
		        clearFocusPath();
		      }}

		      visibleNodesCount = 0;
		      for (const n of nodes) {{
		        const sql = n.getAttribute('data-name-sql') || '';
		        const d = depthBySql[sql] || 0;
		        const withinDepth = d <= depthLimit;
		        const visible = withinDepth && (!only || allow.has(sql)) && (!allowFocus || allowFocus.has(sql));
		        n.classList.toggle('hidden', !visible);
		        if (visible) visibleNodesCount += 1;
		      }}

	      for (const e of edges) {{
	        const p = e.getAttribute('data-parent-sql') || '';
	        const c = e.getAttribute('data-child-sql') || '';
	        const pn = p ? nodeBySql[p] : null;
	        const cn = c ? nodeBySql[c] : null;
	        const visible = !!(pn && cn && !pn.classList.contains('hidden') && !cn.classList.contains('hidden'));
	        e.classList.toggle('hidden', !visible);
	      }}
	    }}

		    function applyFilter(q) {{
		      const query = (q || '').trim().toLowerCase();
		      let matches = 0;
	      for (const n of nodes) {{
	        if (n.classList.contains('hidden')) {{
	          n.classList.remove('dim');
	          n.classList.remove('match');
	          continue;
	        }}
	        const nameSql = (n.getAttribute('data-name-sql') || '').toLowerCase();
	        const name = (n.getAttribute('data-name') || '').toLowerCase();
	        const nameOrig = (n.getAttribute('data-name-original') || '').toLowerCase();
	        const ok = !query || nameSql.includes(query) || name.includes(query) || nameOrig.includes(query);
        n.classList.toggle('dim', !!query && !ok);
        n.classList.toggle('match', !!query && ok);
	        if (ok && query) matches += 1;
		      }}
		      if (status) {{
		        const focusLabel = (focus && focus.checked) ? ('focus: ' + String(focusRootSql || selectedTableSql || BASE_TABLE_SQL || '')) : '';
		        if (query) status.textContent = 'matches: ' + matches + ' / ' + visibleNodesCount + (focusLabel ? (' · ' + focusLabel) : '');
		        else if (visibleNodesCount !== nodes.length) status.textContent = 'visible: ' + visibleNodesCount + ' / ' + nodes.length + (focusLabel ? (' · ' + focusLabel) : '');
		        else status.textContent = focusLabel || '';
		      }}
		    }}

	    function selectTable(tableSql) {{
	      if (!tableSql) return;
	      selectedTableSql = tableSql;
	      clearSelection();
	      for (const n of nodes) {{
	        if ((n.getAttribute('data-name-sql') || '') === tableSql) {{
	          n.classList.add('selected');
        }}
      }}
      for (const e of edges) {{
        const p = e.getAttribute('data-parent-sql') || '';
        const c = e.getAttribute('data-child-sql') || '';
        if (p === tableSql || c === tableSql) {{
          e.classList.add('selected');
        }}
      }}
      const d = detailsByTable[tableSql];
	      if (d) {{
	        d.open = true;
	        d.scrollIntoView({{behavior: 'smooth', block: 'start'}});
	      }}
	      if (focus && focus.checked) {{
	        setFocusRoot(tableSql);
	        recomputeVisibility();
	        applyFilter(search ? search.value : '');
	      }}
	      schedulePersist();
	    }}

    for (const n of nodes) {{
      n.addEventListener('click', (ev) => {{
        ev.preventDefault();
        const tableSql = n.getAttribute('data-name-sql') || '';
        selectTable(tableSql);
      }});
    }}

		    if (search) {{
		      search.addEventListener('input', () => {{
		        applyFilter(search.value);
		        schedulePersist();
		      }});
		    }}
		    if (depthInput) {{
		      depthInput.addEventListener('input', () => {{
		        recomputeVisibility();
		        applyFilter(search ? search.value : '');
		        schedulePersist();
		      }});
		    }}
		    if (onlyFlagged) {{
		      onlyFlagged.addEventListener('change', () => {{
		        recomputeVisibility();
		        applyFilter(search ? search.value : '');
		        schedulePersist();
		      }});
		    }}
		    if (focus) {{
		      focus.addEventListener('change', () => {{
		        recomputeVisibility();
		        applyFilter(search ? search.value : '');
		        schedulePersist();
		      }});
		    }}
		    if (focusMode) {{
		      focusMode.addEventListener('change', () => {{
		        recomputeVisibility();
		        applyFilter(search ? search.value : '');
		        schedulePersist();
		      }});
		    }}
		    if (focusHops) {{
		      focusHops.addEventListener('input', () => {{
		        if (focusHopsValue) focusHopsValue.textContent = String(focusHops.value || '');
		        recomputeVisibility();
		        applyFilter(search ? search.value : '');
		        schedulePersist();
		      }});
		    }}
		    if (focusBasePath) {{
		      focusBasePath.addEventListener('change', () => {{
		        recomputeVisibility();
		        applyFilter(search ? search.value : '');
		        schedulePersist();
		      }});
		    }}
		    if (dlSvg) {{
		      dlSvg.addEventListener('click', () => {{
		        exportSvg();
		      }});
		    }}
		    if (dlPng) {{
		      dlPng.addEventListener('click', () => {{
		        exportPng();
		      }});
		    }}
		    if (reset) {{
		      reset.addEventListener('click', () => {{
		        if (search) search.value = '';
		        if (depthInput) depthInput.value = String(maxDepth);
		        if (onlyFlagged) onlyFlagged.checked = false;
		        if (focus) focus.checked = false;
		        if (focusMode) focusMode.value = 'subtree';
		        if (focusHops) focusHops.value = '2';
		        if (focusBasePath) focusBasePath.checked = true;
		        recomputeVisibility();
		        applyFilter('');
		        clearSelection();
		        schedulePersist();
		      }});
		    }}

		    // Restore UI state (URL has priority over localStorage).
		    persistSuppressed = true;
		    const initialState = readStateFromUrl() || readStateFromStorage();
		    if (initialState) applyState(initialState);
		    persistSuppressed = false;

		    recomputeVisibility();
		    applyFilter(search ? search.value : '');
		    if (selectedTableSql) {{
		      // Apply selection after initial render.
		      persistSuppressed = true;
		      try {{ selectTable(selectedTableSql); }} catch (_e) {{}}
		      persistSuppressed = false;
		    }}
		  }})();
	  </script>
</body>
</html>
"""


def _sanitize_id(text: str) -> str:
    # Local HTML id sanitizer for diff pages.
    return "".join([c if c.isalnum() or c in {"_", "-"} else "_" for c in str(text)])


def write_review_diff_report(
    *,
    before_path: str | Path,
    after_path: str | Path,
    out_dir: str | Path,
    max_list: int = 50,
) -> dict[str, str]:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    diff = diff_review_files(before_path, after_path)
    md = render_review_diff_markdown(diff, max_list=int(max_list))

    before_doc = _load_json(before_path)
    after_doc = _load_json(after_path)

    svg_text = render_review_diff_schema_svg(before_doc=before_doc, after_doc=after_doc, diff=diff)

    diff_json_path = out_path / "diff.json"
    md_path = out_path / "DIFF.md"
    svg_path = out_path / "schema_diff.svg"
    html_path = out_path / "diff.html"

    diff_json_path.write_text(json.dumps(diff, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(md, encoding="utf-8")
    svg_path.write_text(svg_text, encoding="utf-8")

    html_text = render_review_diff_html(
        before_path=before_path,
        after_path=after_path,
        diff=diff,
        before_doc=before_doc,
        after_doc=after_doc,
        schema_svg_text=svg_text,
        max_list=int(max_list),
    )
    html_path.write_text(html_text, encoding="utf-8")

    return {
        "out_dir": str(out_path),
        "diff_json": str(diff_json_path),
        "diff_md": str(md_path),
        "diff_html": str(html_path),
        "schema_diff_svg": str(svg_path),
    }
