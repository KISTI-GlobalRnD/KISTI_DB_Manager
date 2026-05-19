from __future__ import annotations

import html
import re
from pathlib import Path
from typing import Any, Iterable, Mapping

from .core import TableInfo


def _sanitize_mermaid_id(name: str) -> str:
    # Mermaid node ids must be alnum/_; keep it deterministic.
    return re.sub(r"[^0-9A-Za-z_]", "_", name)


def _sanitize_html_id(name: str) -> str:
    # HTML id: allow alnum/_/-; keep it deterministic.
    return re.sub(r"[^0-9A-Za-z_-]", "_", str(name))


def build_table_edges(
    *,
    base_table: str,
    tables: Iterable[str],
    key_sep: str = "__",
) -> list[tuple[str, str, str]]:
    """
    Build a tree of table relationships inferred from name prefixes.

    Returns (parent, child, label) edges using the provided table namespace.
    """
    base = str(base_table)
    key_sep = str(key_sep)

    table_set = {str(t) for t in tables if str(t)}
    table_set.add(base)

    def _match_prefix(name: str) -> str | None:
        # Support common legacy naming variants (ex: "<base>-SUB__...").
        candidates = [
            f"{base}{key_sep}",
            f"{base}-SUB{key_sep}",
            f"{base}_SUB{key_sep}",
        ]
        for p in candidates:
            if name.startswith(p):
                return p
        return None

    edges: list[tuple[str, str, str]] = []
    for child in sorted(table_set):
        if child == base:
            continue

        prefix = _match_prefix(child)
        if prefix is None:
            continue

        # Find nearest existing ancestor that is also a table name.
        suffix = child[len(prefix) :]
        parts = [p for p in suffix.split(key_sep) if p]
        parent = base
        for i in range(len(parts) - 1, 0, -1):
            cand = f"{prefix}{key_sep.join(parts[:i])}"
            if cand in table_set:
                parent = cand
                break

        if parent == base:
            label = suffix
        else:
            parent_suffix = parent[len(prefix) :] if parent.startswith(prefix) else ""
            label = suffix[len(parent_suffix) :]
            if label.startswith(key_sep):
                label = label[len(key_sep) :]

        edges.append((parent, child, label or suffix))

    return edges


def render_mermaid(
    *,
    base_table: str,
    table_infos: list[TableInfo],
    key_sep: str,
) -> str:
    tables = [ti.name_original or ti.name_sql for ti in table_infos]
    edges = build_table_edges(base_table=base_table, tables=tables, key_sep=key_sep)

    id_by_name = {t: _sanitize_mermaid_id(t) for t in tables + [base_table]}

    lines = ["graph TD"]
    for ti in table_infos:
        name = ti.name_original or ti.name_sql
        node_id = id_by_name.get(name, _sanitize_mermaid_id(name))
        title = html.escape(name)
        rows = ti.rows_label()
        lines.append(f'  {node_id}["{title}<br/>rows: {rows}"]')

    for parent, child, label in edges:
        p_id = id_by_name.get(parent, _sanitize_mermaid_id(parent))
        c_id = id_by_name.get(child, _sanitize_mermaid_id(child))
        lines.append(f"  {p_id} -->|{html.escape(label)}| {c_id}")

    return "\n".join(lines) + "\n"


def _svg_escape(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def render_simple_svg(
    *,
    base_table: str,
    table_infos: list[TableInfo],
    key_sep: str,
    width: int = 1800,
    x_step: int = 420,
    y_step: int = 44,
    box_w: int = 360,
    box_h: int = 58,
    node_class_by_sql: Mapping[str, str] | None = None,
    node_fill_by_sql: Mapping[str, str] | None = None,
) -> str:
    """
    Render a self-contained ERD-style SVG without external dependencies.
    """
    name_to_info = {ti.name_original or ti.name_sql: ti for ti in table_infos}
    tables = sorted(name_to_info.keys())

    def _match_prefix(name: str) -> str | None:
        candidates = [
            f"{base_table}{key_sep}",
            f"{base_table}-SUB{key_sep}",
            f"{base_table}_SUB{key_sep}",
        ]
        for p in candidates:
            if name.startswith(p):
                return p
        return None

    def _display_label(name: str) -> str:
        if name == base_table:
            return name
        prefix = _match_prefix(name)
        if prefix is None:
            return name
        suffix = name[len(prefix) :]
        if not suffix:
            return name
        # For legacy "<base>-SUB__..." style, show a "SUB/" prefix for readability.
        if prefix.startswith(f"{base_table}-SUB") or prefix.startswith(f"{base_table}_SUB"):
            suffix = f"SUB{key_sep}{suffix}"
        return suffix.replace(key_sep, "/")

    def _truncate_middle(text: str, *, max_chars: int = 30) -> str:
        text = str(text)
        if len(text) <= max_chars:
            return text
        if max_chars <= 3:
            return text[:max_chars]
        head = max(8, (max_chars - 1) // 2)
        tail = max(8, max_chars - 1 - head)
        return f"{text[:head]}…{text[-tail:]}"

    def _column_tag(name: str, col: Mapping[str, Any], *, is_base: bool) -> str:
        key = str(col.get("column_key") or "").upper()
        col_name = str(name)
        if key == "PRI":
            return "PK"
        if not is_base and col_name == "id":
            return "FK"
        if col_name.endswith("_id") and col_name != "id":
            return "FK"
        return ""

    def _select_columns(cols: list[dict[str, Any]], *, is_base: bool, limit: int = 8) -> list[dict[str, Any]]:
        if not cols:
            return []
        selected: list[dict[str, Any]] = []
        seen: set[str] = set()

        def add(col: dict[str, Any]) -> None:
            name = str(col.get("name") or "")
            if not name or name in seen:
                return
            selected.append(col)
            seen.add(name)

        for col in cols[:limit]:
            add(col)
        for col in cols:
            name = str(col.get("name") or "")
            if _column_tag(name, col, is_base=is_base):
                add(col)
        return selected[: max(limit, len(selected))]

    def _estimate_table_height(ti: TableInfo, *, is_base: bool) -> int:
        cols = list(ti.columns or [])
        visible = _select_columns(cols, is_base=is_base, limit=8)
        header_h = 42
        sub_h = 18
        metric_h = 18
        row_h = 20
        footer_h = 12
        more_h = 18 if len(cols) > len(visible) else 0
        return max(box_h, header_h + sub_h + metric_h + len(visible) * row_h + more_h + footer_h)

    def _cardinality_marker(x: int, y: int, *, side: str, kind: str) -> list[str]:
        if side not in {"left", "right"}:
            return []
        dx = 1 if side == "right" else -1
        out: list[str] = []
        if kind == "one":
            bx = x + (dx * 5)
            out.append(f'<line class="edge-card" x1="{bx}" y1="{y - 7}" x2="{bx}" y2="{y + 7}" />')
            return out
        px = x + (dx * 2)
        ex = x + (dx * 12)
        out.append(f'<line class="edge-card" x1="{px}" y1="{y}" x2="{ex}" y2="{y}" />')
        out.append(f'<line class="edge-card" x1="{px}" y1="{y}" x2="{ex}" y2="{y - 6}" />')
        out.append(f'<line class="edge-card" x1="{px}" y1="{y}" x2="{ex}" y2="{y + 6}" />')
        return out

    def depth(name: str) -> int:
        if name == base_table:
            return 0
        prefix = _match_prefix(name)
        if prefix is None:
            return 0
        suffix = name[len(prefix) :]
        parts = [p for p in suffix.split(key_sep) if p]
        return max(1, len(parts))

    depths: dict[str, int] = {t: depth(t) for t in tables}
    max_depth = max(depths.values()) if depths else 0

    # Stable ordering: by depth, then name.
    by_depth: dict[int, list[str]] = {d: [] for d in range(max_depth + 1)}
    for t in tables:
        by_depth.setdefault(depths[t], []).append(t)
    for d in by_depth:
        by_depth[d] = sorted(by_depth[d])

    pos: dict[str, tuple[int, int]] = {}
    box_height_by_table: dict[str, int] = {}
    for t in tables:
        ti = name_to_info.get(t)
        box_height_by_table[t] = _estimate_table_height(ti, is_base=(t == base_table)) if ti is not None else box_h

    for d in range(max_depth + 1):
        xs = 30 + d * x_step
        ys = 30
        for t in by_depth.get(d, []):
            pos[t] = (xs, ys)
            ys += box_height_by_table.get(t, box_h) + y_step

    edges = build_table_edges(base_table=base_table, tables=tables, key_sep=key_sep)

    max_y = 30
    for t, (_x, y) in pos.items():
        max_y = max(max_y, y + box_height_by_table.get(t, box_h) + 30)
    height = max(200, max_y)
    needed_width = 30 + (max_depth + 1) * x_step + box_w + 30
    width = max(int(width), int(needed_width))

    def node_color(name: str) -> tuple[str, str]:
        ti = name_to_info.get(name)
        if ti is not None and "__excepted__" in str(ti.name_sql):
            return "#FFF8C5", "#FDE68A"
        if name == base_table:
            return "#E6F0FF", "#BFDBFE"
        return "#F8FAFC", "#E5E7EB"

    lines: list[str] = []
    lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    lines.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{int(width)}" height="{int(height)}" viewBox="0 0 {int(width)} {int(height)}">'
    )
    lines.append("<style>")
    lines.append(".box { stroke: #1f2328; stroke-width: 1; rx: 10; ry: 10; }")
    lines.append(".head { stroke: none; }")
    lines.append(".label { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; font-size: 12px; font-weight: 700; fill: #111827; }")
    lines.append(".meta { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; font-size: 11px; fill: #57606a; }")
    lines.append(".col-name { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; font-size: 11px; fill: #111827; }")
    lines.append(".col-type { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 10px; fill: #475467; }")
    lines.append(".col-key { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; font-size: 10px; font-weight: 700; fill: #0550ae; }")
    lines.append(".col-more { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; font-size: 10px; fill: #6b7280; }")
    lines.append(".edge { stroke: #64748B; stroke-width: 1.5; fill: none; }")
    lines.append(".edge-card { stroke: #64748B; stroke-width: 1.3; fill: none; }")
    lines.append(".edge-label { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; font-size: 10px; fill: #475467; }")
    lines.append(".node { cursor: pointer; }")
    lines.append(".node.selected .box, .node.selected .head { stroke: #0f766e; stroke-width: 2.5; }")
    lines.append(".node.selected .col-name, .node.selected .col-key { fill: #0f172a; font-weight: 700; }")
    lines.append(".edge.selected + .edge-label, .edge.selected ~ .edge-card { stroke: #0f766e; fill: #0f766e; }")
    lines.append(".diff-added .box { stroke: #1a7f37; stroke-width: 2; }")
    lines.append(".diff-removed .box { stroke: #cf222e; stroke-width: 2; }")
    lines.append(".diff-changed .box { stroke: #bf8700; stroke-width: 2; }")
    lines.append("</style>")

    for parent, child, label in edges:
        if parent not in pos or child not in pos:
            continue
        px, py = pos[parent]
        cx, cy = pos[child]
        ph = box_height_by_table.get(parent, box_h)
        ch = box_height_by_table.get(child, box_h)
        x1 = px + box_w
        y1 = py + ph // 2
        x2 = cx
        y2 = cy + ch // 2
        mid = (x1 + x2) // 2
        p_info = name_to_info.get(parent)
        c_info = name_to_info.get(child)
        p_sql = p_info.name_sql if p_info is not None else parent
        c_sql = c_info.name_sql if c_info is not None else child
        lines.append('<g class="edge-group">')
        lines.append(
            f'<path class="edge" data-parent="{_svg_escape(parent)}" data-child="{_svg_escape(child)}" '
            f'data-parent-sql="{_svg_escape(p_sql)}" data-child-sql="{_svg_escape(c_sql)}" '
            f'd="M {x1} {y1} C {mid} {y1}, {mid} {y2}, {x2} {y2}" />'
        )
        edge_text = _truncate_middle(f"{label} · 1:N", max_chars=28)
        lines.append(f'<text class="edge-label" x="{mid}" y="{int((y1 + y2) / 2) - 6}" text-anchor="middle">{_svg_escape(edge_text)}</text>')
        lines.extend(_cardinality_marker(x1, y1, side="right", kind="one"))
        lines.extend(_cardinality_marker(x2, y2, side="left", kind="many"))
        lines.append("</g>")

    for name in tables:
        x, y = pos.get(name, (30, 30))
        ti = name_to_info.get(name)
        graph_label = _display_label(ti.label() if ti else name)
        primary_label = _truncate_middle(ti.name_sql if ti is not None else name, max_chars=36)
        secondary_label = _truncate_middle(graph_label, max_chars=34)
        rows = ti.rows_label() if ti else "n/a"
        cols = len(ti.columns or []) if ti and ti.columns is not None else None
        cols_label = str(cols) if cols is not None else "n/a"
        is_base = name == base_table
        visible_cols = _select_columns(list(ti.columns or []) if ti is not None else [], is_base=is_base, limit=8)
        node_h = box_height_by_table.get(name, box_h)
        head_h = 28

        rows_n = 0
        if ti is not None:
            try:
                if ti.row_count is not None:
                    rows_n = int(ti.row_count)
                elif ti.table_rows_estimate is not None:
                    rows_n = int(ti.table_rows_estimate)
            except Exception:
                rows_n = 0

        size_n = 0
        if ti is not None:
            try:
                size_n = int((ti.data_length or 0) + (ti.index_length or 0))
            except Exception:
                size_n = 0

        name_sql = ti.name_sql if ti is not None else name
        name_original = ti.name_original if ti is not None else None

        node_id = _sanitize_html_id(f"node_{name_sql}")
        extra_cls = ""
        if node_class_by_sql:
            try:
                extra_cls = str(node_class_by_sql.get(name_sql) or "").strip()
            except Exception:
                extra_cls = ""

        cls = "node" + (f" {extra_cls}" if extra_cls else "")
        attrs = [
            f'id="{_svg_escape(node_id)}"',
            f'class="{_svg_escape(cls)}"',
            f'data-name="{_svg_escape(name)}"',
            f'data-name-sql="{_svg_escape(name_sql)}"',
            f'data-rows="{int(rows_n)}"',
            f'data-rows-exact="{1 if ti is not None and ti.row_count_exact else 0}"',
            f'data-size="{int(size_n)}"',
        ]
        if cols is not None:
            attrs.append(f'data-cols="{int(cols)}"')
        if name_original:
            attrs.append(f'data-name-original="{_svg_escape(name_original)}"')

        title = f"{name_sql}"
        if name_original and name_original != name_sql:
            title = f"{name_original} ({name_sql})"
        title = f"{title} · rows: {rows} · cols: {cols_label}"

        fill = None
        if node_fill_by_sql:
            try:
                fill = node_fill_by_sql.get(name_sql)
            except Exception:
                fill = None
        body_fill, head_fill = node_color(name)
        fill_color = str(fill) if fill else body_fill
        head_color = head_fill if not fill else str(fill)

        lines.append(f"<g {' '.join(attrs)}>")
        lines.append(f"<title>{_svg_escape(title)}</title>")
        lines.append(
            f'<rect class="box" x="{x}" y="{y}" width="{box_w}" height="{node_h}" fill="{_svg_escape(fill_color)}" />'
        )
        lines.append(
            f'<rect class="head" x="{x}" y="{y}" width="{box_w}" height="{head_h}" rx="10" ry="10" fill="{_svg_escape(head_color)}" />'
        )
        lines.append(f'<text class="label" x="{x + 10}" y="{y + 18}">{_svg_escape(primary_label)}</text>')
        if secondary_label and secondary_label != primary_label:
            lines.append(f'<text class="meta" x="{x + 10}" y="{y + 43}">{_svg_escape(secondary_label)}</text>')
        lines.append(f'<text class="meta" x="{x + 10}" y="{y + 59}">rows: {_svg_escape(rows)} · cols: {_svg_escape(cols_label)}</text>')

        col_y = y + 78
        type_x = x + box_w - 58
        key_x = x + box_w - 12
        for col in visible_cols:
            col_name = str(col.get("name") or "")
            col_type = str(col.get("column_type") or col.get("data_type") or "")
            key_tag = _column_tag(col_name, col, is_base=is_base)
            lines.append(f'<text class="col-name" x="{x + 12}" y="{col_y}">{_svg_escape(_truncate_middle(col_name, max_chars=24))}</text>')
            lines.append(f'<text class="col-type" x="{type_x}" y="{col_y}" text-anchor="end">{_svg_escape(_truncate_middle(col_type, max_chars=14))}</text>')
            if key_tag:
                lines.append(f'<text class="col-key" x="{key_x}" y="{col_y}" text-anchor="end">{_svg_escape(key_tag)}</text>')
            col_y += 18

        hidden_cols = max(0, (len(ti.columns or []) if ti and ti.columns is not None else 0) - len(visible_cols))
        if hidden_cols > 0:
            lines.append(f'<text class="col-more" x="{x + 12}" y="{col_y}">… +{hidden_cols} more columns</text>')
        lines.append("</g>")

    lines.append("</svg>")
    return "\n".join(lines) + "\n"


def _maybe_svg_to_png(svg_text: str, out_path: Path) -> bool:
    try:
        import cairosvg  # type: ignore
    except Exception:
        return False
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cairosvg.svg2png(bytestring=svg_text.encode("utf-8"), write_to=str(out_path))
    return True
