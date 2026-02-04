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
    .schema-toolbar {{ display: flex; gap: 8px; align-items: center; margin-bottom: 10px; }}
    .schema-toolbar input[type="search"] {{ flex: 1; padding: 8px 10px; border: 1px solid #d0d7de; border-radius: 10px; }}
    .schema-toolbar button {{ padding: 8px 12px; border: 1px solid #d0d7de; border-radius: 10px; background: #f6f8fa; cursor: pointer; }}
    .schema-container {{ max-height: 70vh; overflow: auto; border: 1px solid #d0d7de; border-radius: 12px; padding: 8px; background: #fff; }}
    .schema-container svg {{ max-width: 100%; height: auto; }}
    .schema-container .node.dim {{ opacity: 0.15; }}
    .schema-container .node.match .box {{ stroke: #fb8c00; stroke-width: 2; }}
    .schema-container .node.selected .box {{ stroke: #0969da; stroke-width: 2; }}
    .schema-container .edge.selected {{ stroke: #0969da; stroke-width: 2; }}
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
    const status = document.getElementById('schema-status');
    const svg = container.querySelector('svg');
    if (!svg) return;

    const nodes = Array.from(svg.querySelectorAll('.node'));
    const edges = Array.from(svg.querySelectorAll('.edge'));

    const detailsByTable = {{}};
    for (const d of document.querySelectorAll('details.details[data-table]')) {{
      const t = d.getAttribute('data-table');
      if (t) detailsByTable[t] = d;
    }}

    function clearSelection() {{
      for (const n of nodes) n.classList.remove('selected');
      for (const e of edges) e.classList.remove('selected');
    }}

    function applyFilter(q) {{
      const query = (q || '').trim().toLowerCase();
      let matches = 0;
      for (const n of nodes) {{
        const nameSql = (n.getAttribute('data-name-sql') || '').toLowerCase();
        const name = (n.getAttribute('data-name') || '').toLowerCase();
        const nameOrig = (n.getAttribute('data-name-original') || '').toLowerCase();
        const ok = !query || nameSql.includes(query) || name.includes(query) || nameOrig.includes(query);
        n.classList.toggle('dim', !!query && !ok);
        n.classList.toggle('match', !!query && ok);
        if (ok && query) matches += 1;
      }}
      if (status) {{
        status.textContent = query ? ('matches: ' + matches + ' / ' + nodes.length) : '';
      }}
    }}

    function selectTable(tableSql) {{
      if (!tableSql) return;
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
    }}

    for (const n of nodes) {{
      n.addEventListener('click', (ev) => {{
        ev.preventDefault();
        const tableSql = n.getAttribute('data-name-sql') || '';
        selectTable(tableSql);
      }});
    }}

    if (search) {{
      search.addEventListener('input', () => applyFilter(search.value));
    }}
    if (reset) {{
      reset.addEventListener('click', () => {{
        if (search) search.value = '';
        applyFilter('');
        clearSelection();
      }});
    }}

    applyFilter(search ? search.value : '');
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
