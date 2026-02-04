from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from .config import coerce_data_config, coerce_db_config
from .namemap import NameMap, load_namemap
from .naming import MYSQL_IDENTIFIER_MAX_LEN, truncate_table_name


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_json(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except TypeError:
        return repr(value)


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _parse_formats(value: str | None) -> set[str]:
    if not value:
        return {"md", "html", "svg"}
    items = [v.strip().lower() for v in str(value).split(",")]
    return {v for v in items if v}


def _bool(value: Any) -> bool:
    return bool(value) and str(value).lower() not in {"0", "false", "no", "off", "none"}


def _mask_db_config(db_config: Mapping[str, Any]) -> dict[str, Any]:
    masked = dict(db_config)
    if masked.get("password"):
        masked["password"] = "***"
    return masked


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
    width: int = 1400,
    x_step: int = 260,
    y_step: int = 70,
    box_w: int = 240,
    box_h: int = 44,
    node_class_by_sql: Mapping[str, str] | None = None,
    node_fill_by_sql: Mapping[str, str] | None = None,
) -> str:
    """
    Render a lightweight SVG table graph without external dependencies.
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

    def _wrap_path(text: str, *, max_chars: int = 30, max_lines: int = 2) -> list[str]:
        """
        Wrap a path-like string (delimited by "/") into up to max_lines.
        Adds an ellipsis when truncated.
        """
        text = str(text)
        if len(text) <= max_chars:
            return [text]

        parts = text.split("/")
        lines: list[str] = []
        i = 0
        while i < len(parts) and len(lines) < max_lines:
            line = parts[i]
            i += 1
            while i < len(parts):
                cand = f"{line}/{parts[i]}"
                if len(cand) <= max_chars:
                    line = cand
                    i += 1
                else:
                    break
            lines.append(line)

        truncated = i < len(parts)
        # Also treat overlong final line as truncated.
        if lines and len(lines[-1]) > max_chars:
            truncated = True
            lines[-1] = lines[-1][: max(0, max_chars - 1)]

        if truncated and lines:
            if len(lines[-1]) >= max_chars:
                lines[-1] = lines[-1][: max(0, max_chars - 1)] + "…"
            else:
                lines[-1] = lines[-1] + "…"

        # Final safety truncation
        out: list[str] = []
        for l in lines:
            l = str(l)
            if len(l) <= max_chars:
                out.append(l)
            else:
                out.append(l[: max(0, max_chars - 1)] + "…")
        return out or [text[: max(0, max_chars - 1)] + "…"]

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

    # Layout: stack each depth column.
    pos: dict[str, tuple[int, int]] = {}
    for d in range(max_depth + 1):
        xs = 30 + d * x_step
        for i, t in enumerate(by_depth.get(d, [])):
            ys = 30 + i * y_step
            pos[t] = (xs, ys)

    edges = build_table_edges(base_table=base_table, tables=tables, key_sep=key_sep)

    # Compute viewbox height.
    max_y = 30
    for _t, (_x, y) in pos.items():
        max_y = max(max_y, y + box_h + 30)
    height = max(200, max_y)
    needed_width = 30 + (max_depth + 1) * x_step + box_w + 30
    width = max(int(width), int(needed_width))

    def node_color(name: str) -> str:
        return "#E6F0FF" if name == base_table else "#F6F8FA"

    # SVG
    lines: list[str] = []
    lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    lines.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{int(width)}" height="{int(height)}" viewBox="0 0 {int(width)} {int(height)}">'
    )
    lines.append("<style>")
    lines.append(".box { stroke: #1f2328; stroke-width: 1; rx: 8; ry: 8; }")
    lines.append(".label { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; font-size: 12px; fill: #1f2328; }")
    lines.append(".meta { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; font-size: 11px; fill: #57606a; }")
    lines.append(".edge { stroke: #57606a; stroke-width: 1; fill: none; }")
    lines.append(".node { cursor: pointer; }")
    lines.append(".diff-added .box { stroke: #1a7f37; stroke-width: 2; }")
    lines.append(".diff-removed .box { stroke: #cf222e; stroke-width: 2; }")
    lines.append(".diff-changed .box { stroke: #bf8700; stroke-width: 2; }")
    lines.append("</style>")

    # Edges first (under nodes)
    for parent, child, _label in edges:
        if parent not in pos or child not in pos:
            continue
        px, py = pos[parent]
        cx, cy = pos[child]
        x1 = px + box_w
        y1 = py + box_h // 2
        x2 = cx
        y2 = cy + box_h // 2
        mid = (x1 + x2) // 2
        p_info = name_to_info.get(parent)
        c_info = name_to_info.get(child)
        p_sql = p_info.name_sql if p_info is not None else parent
        c_sql = c_info.name_sql if c_info is not None else child
        lines.append(
            f'<path class="edge" data-parent="{_svg_escape(parent)}" data-child="{_svg_escape(child)}" '
            f'data-parent-sql="{_svg_escape(p_sql)}" data-child-sql="{_svg_escape(c_sql)}" '
            f'd="M {x1} {y1} C {mid} {y1}, {mid} {y2}, {x2} {y2}" />'
        )

    # Nodes
    for name in tables:
        x, y = pos.get(name, (30, 30))
        ti = name_to_info.get(name)
        label = _display_label(ti.label() if ti else name)
        label_lines = _wrap_path(label, max_chars=30, max_lines=2)
        rows = ti.rows_label() if ti else "n/a"
        cols = len(ti.columns or []) if ti and ti.columns is not None else None
        cols_label = str(cols) if cols is not None else "n/a"

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
        ]
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
        fill_color = str(fill) if fill else node_color(name)

        lines.append(f"<g {' '.join(attrs)}>")
        lines.append(f"<title>{_svg_escape(title)}</title>")
        lines.append(
            f'<rect class="box" x="{x}" y="{y}" width="{box_w}" height="{box_h}" fill="{_svg_escape(fill_color)}" />'
        )
        if len(label_lines) == 1:
            lines.append(f'<text class="label" x="{x + 10}" y="{y + 18}">{_svg_escape(label_lines[0])}</text>')
            lines.append(f'<text class="meta" x="{x + 10}" y="{y + 34}">rows: {_svg_escape(rows)} · cols: {_svg_escape(cols_label)}</text>')
        else:
            # Two-line label: keep meta on a third line (still fits in box_h).
            lines.append(f'<text class="label" x="{x + 10}" y="{y + 16}">{_svg_escape(label_lines[0])}</text>')
            lines.append(f'<text class="label" x="{x + 10}" y="{y + 30}">{_svg_escape(label_lines[1])}</text>')
            lines.append(f'<text class="meta" x="{x + 10}" y="{y + 42}">rows: {_svg_escape(rows)} · cols: {_svg_escape(cols_label)}</text>')
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


def _render_markdown(
    *,
    generated_at: str,
    base_table: str,
    base_table_sql: str,
    key_sep: str,
    formats: set[str],
    config_path: str | None,
    report_path: str | None,
    db_enabled: bool,
    exact_counts: bool,
    db_config_masked: Mapping[str, Any] | None,
    db_error: str | None,
    mermaid_text: str | None,
    table_infos: list[TableInfo],
    issues: list[dict[str, Any]] | None,
) -> str:
    total_tables = len(table_infos)
    total_rows_known = 0
    any_estimated = False
    for ti in table_infos:
        if ti.row_count is not None:
            total_rows_known += int(ti.row_count)
        elif ti.table_rows_estimate is not None:
            any_estimated = True

    lines: list[str] = []
    lines.append("# KISTI DB Review Pack")
    lines.append("")
    lines.append(f"- generated_at: `{generated_at}`")
    if config_path:
        lines.append(f"- config: `{config_path}`")
    if report_path:
        lines.append(f"- report: `{report_path}`")
    lines.append(f"- base_table: `{base_table}`")
    lines.append(f"- base_table_sql: `{base_table_sql}`")
    lines.append(f"- key_sep: `{key_sep}`")
    if db_enabled and db_config_masked:
        lines.append(f"- db: `{db_config_masked.get('host')}:{db_config_masked.get('port')}/{db_config_masked.get('database')}`")
    lines.append(f"- db_introspection: `{'on' if db_enabled else 'off'}`")
    if db_error:
        lines.append(f"- db_error: `{db_error}`")
    lines.append("")

    lines.append("## Summary")
    lines.append("")
    lines.append(f"- tables: `{total_tables}`")
    if exact_counts:
        lines.append("- row_count: **exact** (`COUNT(*)`)")
    else:
        lines.append("- row_count: **estimated** (`information_schema.tables.table_rows`) by default")
    if any_estimated and not exact_counts:
        lines.append("- note: values prefixed with `~` are estimates")
    if issues:
        lines.append(f"- run_issues: `{len(issues)}` (from report)")
    lines.append("")

    if "svg" in formats:
        lines.append("## Diagram")
        lines.append("")
        lines.append("- `schema.svg`")
        if "png" in formats:
            lines.append("- `schema.png` (best-effort; requires `cairosvg`)")
        lines.append("")
        lines.append("![](schema.svg)")
        lines.append("")

    if mermaid_text and (("mmd" in formats) or ("mermaid" in formats)):
        lines.append("## Mermaid")
        lines.append("")
        lines.append("```mermaid")
        lines.append(mermaid_text.rstrip())
        lines.append("```")
        lines.append("")

    lines.append("## Tables")
    lines.append("")
    lines.append("| Table | rows | cols | size | id_index |")
    lines.append("|---|---:|---:|---:|:---:|")
    for ti in table_infos:
        cols_n = len(ti.columns or []) if ti.columns is not None else 0
        size = None
        if ti.data_length is not None or ti.index_length is not None:
            size = int((ti.data_length or 0) + (ti.index_length or 0))
        idx_on_id = False
        if ti.indexes:
            idx_on_id = any(str(ix.get("column_name")).lower() == "id" for ix in ti.indexes)
        lines.append(
            f"| `{ti.name_sql}` | {ti.rows_label()} | {cols_n if cols_n else 'n/a'} | {size if size is not None else 'n/a'} | {'Y' if idx_on_id else ''} |"
        )
    lines.append("")

    lines.append("## Join Tips")
    lines.append("")
    lines.append("All sub-tables (if any) are typically joinable via the `id` column:")
    lines.append("")
    lines.append("```sql")
    lines.append(f"SELECT b.*, s.*")
    lines.append(f"FROM `{base_table_sql}` b")
    lines.append(f"LEFT JOIN `{base_table_sql}{key_sep}<sub>` s ON b.id = s.id")
    lines.append("LIMIT 5;")
    lines.append("```")
    lines.append("")

    if issues:
        lines.append("## Issues (from run report)")
        lines.append("")
        for it in issues[:30]:
            stage = it.get("stage")
            lvl = it.get("level")
            msg = it.get("message")
            lines.append(f"- `{lvl}` `{stage}`: {msg}")
        if len(issues) > 30:
            lines.append(f"- ... +{len(issues) - 30} more")
        lines.append("")

    return "\n".join(lines)


def _render_html(
    *,
    title: str,
    markdown_path: str | None,
    schema_svg_path: str | None,
    schema_svg_text: str | None = None,
    mermaid_path: str | None,
    meta: Mapping[str, Any],
    table_infos: list[TableInfo],
    issues: list[dict[str, Any]] | None,
    samples_by_table: Mapping[str, list[dict[str, Any]]] | None = None,
) -> str:
    def h(x: Any) -> str:
        return html.escape(str(x))

    rows = []
    for ti in table_infos:
        details_id = f"table_{_sanitize_html_id(ti.name_sql)}"
        cols_n = len(ti.columns or []) if ti.columns is not None else ""
        size = ""
        if ti.data_length is not None or ti.index_length is not None:
            size = str(int((ti.data_length or 0) + (ti.index_length or 0)))
        idx_on_id = ""
        if ti.indexes:
            idx_on_id = "Y" if any(str(ix.get("column_name")).lower() == "id" for ix in ti.indexes) else ""
        rows.append(
            "<tr>"
            f"<td><code><a href=\"#{h(details_id)}\">{h(ti.name_sql)}</a></code></td>"
            f"<td>{h(ti.rows_label())}</td>"
            f"<td>{h(cols_n)}</td>"
            f"<td>{h(size)}</td>"
            f"<td>{h(idx_on_id)}</td>"
            "</tr>"
        )

    issue_rows = []
    for it in (issues or [])[:200]:
        issue_rows.append(
            "<tr>"
            f"<td><code>{h(it.get('level'))}</code></td>"
            f"<td><code>{h(it.get('stage'))}</code></td>"
            f"<td>{h(it.get('message'))}</td>"
            "</tr>"
        )

    meta_items = "".join([f"<li><b>{h(k)}</b>: <code>{h(v)}</code></li>" for k, v in meta.items()])

    links = []
    if markdown_path:
        label = Path(markdown_path).name
        links.append(f'<a href="{h(markdown_path)}">{h(label)}</a>')
    if schema_svg_path:
        links.append(f'<a href="{h(schema_svg_path)}">schema.svg</a>')
    if mermaid_path:
        links.append(f'<a href="{h(mermaid_path)}">schema.mmd</a>')

    links_html = " · ".join(links)

    svg_embed = ""
    if schema_svg_text:
        # Inline SVG for interactivity. Strip XML prolog for HTML embedding.
        svg_inline = str(schema_svg_text)
        if svg_inline.lstrip().startswith("<?xml"):
            svg_inline = svg_inline.split("?>", 1)[-1]
        svg_embed = (
            "<div class=\"schema-toolbar\">"
            "<input id=\"schema-search\" type=\"search\" placeholder=\"Search table…\" />"
            "<button id=\"schema-reset\" type=\"button\">Reset</button>"
            "<span id=\"schema-status\" class=\"muted\"></span>"
            "</div>"
            "<div id=\"schema-container\" class=\"schema-container\">"
            + svg_inline
            + "</div>"
        )
    elif schema_svg_path:
        svg_embed = f'<img src="{h(schema_svg_path)}" alt="schema" style="max-width: 100%; height: auto; border: 1px solid #d0d7de; border-radius: 8px; padding: 8px; background: #fff;" />'

    details_blocks = []
    empty_row_4 = '<tr><td colspan="4" class="muted">(none)</td></tr>'
    for ti in table_infos:
        cols_html = ""
        if ti.columns is not None:
            col_rows = []
            for c in ti.columns:
                col_rows.append(
                    "<tr>"
                    f"<td><code>{h(c.get('name'))}</code></td>"
                    f"<td><code>{h(c.get('column_type') or c.get('data_type'))}</code></td>"
                    f"<td>{h(c.get('is_nullable'))}</td>"
                    f"<td><code>{h(c.get('column_key') or '')}</code></td>"
                    "</tr>"
                )
            cols_body = "".join(col_rows) if col_rows else empty_row_4
            cols_html = (
                "<h4>Columns</h4>"
                "<table><thead><tr><th>name</th><th>type</th><th>nullable</th><th>key</th></tr></thead>"
                f"<tbody>{cols_body}</tbody></table>"
            )

        idx_html = ""
        if ti.indexes is not None:
            idx_rows = []
            for ix in ti.indexes:
                idx_rows.append(
                    "<tr>"
                    f"<td><code>{h(ix.get('index_name'))}</code></td>"
                    f"<td><code>{h(ix.get('column_name'))}</code></td>"
                    f"<td>{h(ix.get('seq_in_index'))}</td>"
                    f"<td>{h(ix.get('non_unique'))}</td>"
                    "</tr>"
                )
            idx_body = "".join(idx_rows) if idx_rows else empty_row_4
            idx_html = (
                "<h4>Indexes</h4>"
                "<table><thead><tr><th>index</th><th>column</th><th>seq</th><th>non_unique</th></tr></thead>"
                f"<tbody>{idx_body}</tbody></table>"
            )

        samples_html = ""
        if samples_by_table and samples_by_table.get(ti.name_sql):
            try:
                sample_text = json.dumps(samples_by_table.get(ti.name_sql), ensure_ascii=False, indent=2)
            except Exception:
                sample_text = repr(samples_by_table.get(ti.name_sql))
            samples_html = "<h4>Samples</h4>" + f"<pre>{h(sample_text)}</pre>"

        body_html = cols_html + idx_html + samples_html
        if not body_html:
            body_html = '<div class="muted">(no per-table details available)</div>'

        details_id = f"table_{_sanitize_html_id(ti.name_sql)}"
        summary_bits = [f"<code>{h(ti.name_sql)}</code>"]
        if ti.name_original and ti.name_original != ti.name_sql:
            summary_bits.append(f"<span class=\"muted\">({h(ti.name_original)})</span>")
        summary_bits.append(f"<span class=\"muted\">rows: {h(ti.rows_label())}</span>")
        summary = " · ".join(summary_bits)

        details_blocks.append(
            f"<details class=\"details\" id=\"{h(details_id)}\" data-table=\"{h(ti.name_sql)}\">"
            f"<summary>{summary}</summary>"
            f"<div class=\"card\" style=\"margin-top: 12px;\">"
            f"<div class=\"muted\">engine: <code>{h(ti.engine or '')}</code> · collation: <code>{h(ti.collation or '')}</code></div>"
            f"{body_html}"
            "</div>"
            "</details>"
        )

    details_html = "".join(details_blocks) if details_blocks else '<div class="muted">(no per-table details available)</div>'
    issue_table_body = "".join(issue_rows) if issue_rows else '<tr><td colspan="3" class="muted">(none)</td></tr>'

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{h(title)}</title>
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
  </style>
</head>
<body>
  <h1>{h(title)}</h1>
  <p class="muted">{links_html}</p>

  <div class="card">
    <h2>Meta</h2>
    <ul>
      {meta_items}
    </ul>
  </div>

  <div class="card">
    <h2>Diagram</h2>
    {svg_embed}
  </div>

  <div class="card">
    <h2>Tables</h2>
    <table>
      <thead>
        <tr><th>Table</th><th>rows</th><th>cols</th><th>size</th><th>id_index</th></tr>
      </thead>
      <tbody>
        {''.join(rows)}
      </tbody>
    </table>
  </div>

  <div class="card">
    <h2>Table Details</h2>
    <p class="muted">Expanded view (requires DB introspection for columns/indexes).</p>
    {details_html}
  </div>

  <div class="card">
    <h2>Issues (from run report)</h2>
    <table>
      <thead><tr><th>level</th><th>stage</th><th>message</th></tr></thead>
      <tbody>
        {issue_table_body}
      </tbody>
    </table>
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


def _render_plan_markdown(
    *,
    generated_at: str,
    config_path: str,
    base_table: str,
    base_table_sql: str,
    key_sep: str,
    file_name: str,
    file_type: str,
    max_records: int | None,
    stats: Mapping[str, Any],
    issues: list[dict[str, Any]] | None,
    table_infos: list[TableInfo],
    formats: set[str],
) -> str:
    lines: list[str] = []
    lines.append("# KISTI DB Review Plan (Pre-load)")
    lines.append("")
    lines.append(f"- generated_at: `{generated_at}`")
    lines.append(f"- config: `{config_path}`")
    lines.append(f"- input: `{file_name}` (`{file_type}`)")
    lines.append(f"- base_table: `{base_table}`")
    lines.append(f"- base_table_sql: `{base_table_sql}`")
    lines.append(f"- key_sep: `{key_sep}`")
    if max_records is not None:
        lines.append(f"- max_records: `{max_records}` (sample cap)")
    if stats:
        if "records_read" in stats:
            lines.append(f"- records_read: `{stats.get('records_read')}`")
        if "records_ok" in stats:
            lines.append(f"- records_ok: `{stats.get('records_ok')}`")
        if "records_failed" in stats:
            lines.append(f"- records_failed: `{stats.get('records_failed')}`")
        if "batches_total" in stats:
            lines.append(f"- batches_total: `{stats.get('batches_total')}`")
    if issues:
        lines.append(f"- issues: `{len(issues)}`")
    lines.append("")

    lines.append("## Notes")
    lines.append("")
    lines.append("- This plan is generated **before DB load**.")
    lines.append("- If later records have new keys/branches, **additional tables/columns may appear** at load time.")
    lines.append("")

    if "svg" in formats:
        lines.append("## Diagram")
        lines.append("")
        lines.append("- `schema.svg`")
        if "png" in formats:
            lines.append("- `schema.png` (best-effort; requires `cairosvg`)")
        lines.append("")
        lines.append("![](schema.svg)")
        lines.append("")

    if "mmd" in formats or "mermaid" in formats:
        lines.append("## Mermaid")
        lines.append("")
        lines.append("- `schema.mmd`")
        lines.append("")

    lines.append("## Tables (predicted)")
    lines.append("")
    lines.append("| Table | cols |")
    lines.append("|---|---:|")
    for ti in table_infos:
        cols_n = len(ti.columns or []) if ti.columns is not None else 0
        lines.append(f"| `{ti.name_sql}` | {cols_n if cols_n else 'n/a'} |")
    lines.append("")

    if "ddl" in formats or True:
        lines.append("## DDL")
        lines.append("")
        lines.append("- `ddl.sql` (concatenated)")
        lines.append("- `ddl.json` (per-table mapping)")
        lines.append("")

    lines.append("## Join Tips")
    lines.append("")
    lines.append("All sub-tables (if any) are typically joinable via the `id` column:")
    lines.append("")
    lines.append("```sql")
    lines.append("SELECT b.*, s.*")
    lines.append(f"FROM `{base_table_sql}` b")
    lines.append(f"LEFT JOIN `{base_table_sql}{key_sep}<sub>` s ON b.id = s.id")
    lines.append("LIMIT 5;")
    lines.append("```")
    lines.append("")

    if issues:
        lines.append("## Issues (from plan run)")
        lines.append("")
        for it in issues[:30]:
            stage = it.get("stage")
            lvl = it.get("level")
            msg = it.get("message")
            lines.append(f"- `{lvl}` `{stage}`: {msg}")
        if len(issues) > 30:
            lines.append(f"- ... +{len(issues) - 30} more")
        lines.append("")

    return "\n".join(lines) + "\n"


def generate_review_plan(
    *,
    config_path: str,
    out_dir: str,
    formats: str | None = None,
    max_records: int | None = 1000,
    generate_desc: bool = False,
) -> dict[str, Any]:
    """
    Generate a pre-load review plan (no DB writes).

    - For JSON inputs: runs `run_json_pipeline(..., create/load/index/optimize=False)` on up to max_records
      and emits predicted NameMaps + DDL + schema diagrams.
    - For tabular inputs: can optionally generate a description CSV (generate_desc=True), then emits NameMap + DDL.
    """
    from .pipeline import run_json_pipeline, run_tabular_pipeline
    from .quarantine import NullQuarantineWriter
    from .report import RunReport

    cfg = _load_json(config_path)
    data_config = coerce_data_config(cfg.get("data_config") or cfg.get("data") or {})
    db_config = coerce_db_config(cfg.get("db_config") or cfg.get("db") or {})

    base_table = str(data_config.get("table_name") or "").strip()
    if not base_table:
        raise ValueError("data_config.table_name is required")
    key_sep = str(data_config.get("KEY_SEP", "__"))
    base_table_sql = truncate_table_name(base_table, max_len=MYSQL_IDENTIFIER_MAX_LEN)

    file_name = str(data_config.get("file_name") or "")
    file_type = str(data_config.get("file_type") or "").lower()

    fmt = _parse_formats(formats) if formats is not None else {"md", "html", "svg", "mmd"}
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    report = RunReport()
    ddls: dict[str, str] = {}
    table_infos: list[TableInfo] = []

    if file_type in {"jsonl", "ndjson", "jsonlines", "json", "gz", "zip"}:
        res = run_json_pipeline(
            data_config,
            db_config,
            emit_ddl=True,
            create=False,
            load=False,
            index=False,
            optimize=False,
            continue_on_error=True,
            report=report,
            quarantine=NullQuarantineWriter(),
            max_records=max_records,
        )
        report.finish()
        rd = res.report.to_dict()
        ddls = dict((rd.get("artifacts") or {}).get("create_table_sql_json") or {})

        nm_by_table = (rd.get("artifacts") or {}).get("name_maps_json") or {}
        if isinstance(nm_by_table, Mapping):
            for table_original, nm_dict in nm_by_table.items():
                nm = load_namemap(nm_dict)
                if nm is None:
                    continue
                cols = [{"name": c, "column_type": "LONGTEXT"} for c in nm.columns_sql]
                table_infos.append(TableInfo(name_sql=nm.table_sql, name_original=nm.table_original, columns=cols))

    else:
        # Tabular: reuse the tabular pipeline preparation path (no DB steps)
        res = run_tabular_pipeline(
            data_config,
            db_config,
            generate_desc=bool(generate_desc),
            emit_ddl=True,
            create=False,
            load=False,
            index=False,
            optimize=False,
            continue_on_error=True,
            report=report,
            quarantine=NullQuarantineWriter(),
        )
        res.report.finish()
        rd = res.report.to_dict()
        ddl = (rd.get("artifacts") or {}).get("create_table_sql")
        if isinstance(ddl, str) and ddl.strip():
            ddls = {base_table: ddl}
        nm = load_namemap((rd.get("artifacts") or {}).get("name_map"))
        if nm is not None:
            cols = [{"name": c, "column_type": None} for c in nm.columns_sql]
            table_infos.append(TableInfo(name_sql=nm.table_sql, name_original=nm.table_original, columns=cols))

    # Normalize
    table_infos = sorted(table_infos, key=lambda t: t.name_sql)
    for ti in table_infos:
        if ti.name_original == base_table:
            base_table_sql = ti.name_sql
            break

    use_original_names = any(ti.name_original for ti in table_infos)
    base_table_graph = base_table if use_original_names else base_table_sql

    # Artifacts: schema
    mermaid = render_mermaid(base_table=base_table_graph, table_infos=table_infos, key_sep=key_sep)
    svg_text = render_simple_svg(base_table=base_table_graph, table_infos=table_infos, key_sep=key_sep)

    mermaid_path = out_path / "schema.mmd"
    svg_path = out_path / "schema.svg"
    _write_text(mermaid_path, mermaid)
    _write_text(svg_path, svg_text)

    png_path = out_path / "schema.png"
    png_written = False
    if "png" in fmt:
        png_written = _maybe_svg_to_png(svg_text, png_path)

    # DDL files
    ddl_json_path = out_path / "ddl.json"
    ddl_sql_path = out_path / "ddl.sql"
    _write_text(ddl_json_path, json.dumps(ddls, ensure_ascii=False, indent=2))
    ddl_sql_concat = "\n".join([s.rstrip() for s in ddls.values() if isinstance(s, str)]) + ("\n" if ddls else "")
    _write_text(ddl_sql_path, ddl_sql_concat)

    # Report JSON (plan run)
    report_path = out_path / "plan_run_report.json"
    _write_text(report_path, report.to_json())

    generated_at = _utc_now_iso()
    plan_md_path = out_path / "PLAN.md"
    plan_html_path = out_path / "plan.html"

    md = _render_plan_markdown(
        generated_at=generated_at,
        config_path=config_path,
        base_table=base_table,
        base_table_sql=base_table_sql,
        key_sep=key_sep,
        file_name=file_name,
        file_type=file_type,
        max_records=int(max_records) if max_records is not None else None,
        stats=report.stats,
        issues=[it.to_dict() for it in report.issues] if hasattr(report, "issues") else None,
        table_infos=table_infos,
        formats=fmt,
    )
    _write_text(plan_md_path, md)

    html_text = _render_html(
        title=f"Review Plan: {base_table}",
        markdown_path="PLAN.md",
        schema_svg_path="schema.svg",
        schema_svg_text=svg_text,
        mermaid_path="schema.mmd",
        meta={
            "generated_at": generated_at,
            "config": config_path,
            "input": file_name,
            "file_type": file_type,
            "base_table": base_table,
            "base_table_sql": base_table_sql,
            "key_sep": key_sep,
            "mode": "plan",
            "max_records": max_records if max_records is not None else "",
        },
        table_infos=table_infos,
        issues=[it.to_dict() for it in report.issues] if hasattr(report, "issues") else None,
    )
    _write_text(plan_html_path, html_text)

    plan_json = {
        "generated_at": generated_at,
        "config": config_path,
        "base_table": base_table,
        "base_table_sql": base_table_sql,
        "key_sep": key_sep,
        "mode": "plan",
        "max_records": int(max_records) if max_records is not None else None,
        "stats": dict(report.stats),
        "issues": [it.to_dict() for it in report.issues],
        "ddl_json": "ddl.json",
        "ddl_sql": "ddl.sql",
        "tables": [
            {
                "name_sql": ti.name_sql,
                "name_original": ti.name_original,
                "columns": ti.columns,
            }
            for ti in table_infos
        ],
        "artifacts": {
            "schema_svg": "schema.svg",
            "schema_png": "schema.png" if png_written else None,
            "schema_mmd": "schema.mmd",
            "plan_md": "PLAN.md",
            "plan_html": "plan.html",
            "plan_run_report": "plan_run_report.json",
        },
    }
    _write_text(out_path / "plan.json", json.dumps(plan_json, ensure_ascii=False, indent=2))

    return {
        "out_dir": str(out_path),
        "plan_md": str(plan_md_path),
        "plan_html": str(plan_html_path),
        "schema_svg": str(svg_path),
        "schema_png": str(png_path) if png_written else None,
        "schema_mmd": str(mermaid_path),
        "ddl_json": str(ddl_json_path),
        "ddl_sql": str(ddl_sql_path),
        "plan_json": str(out_path / "plan.json"),
        "plan_run_report": str(report_path),
    }


def generate_review_pack(
    *,
    config_path: str,
    out_dir: str,
    report_path: str | None = None,
    formats: str | None = None,
    db_enabled: bool = True,
    exact_counts: bool = False,
    sample_rows: int | None = None,
    sample_max_tables: int = 20,
) -> dict[str, Any]:
    """
    Generate a review pack for a v2 run.

    Inputs:
    - config_path: JSON containing {data_config, db_config}
    - report_path: optional RunReport JSON to enrich mapping + issues
    - db_enabled: when True, attempt to introspect DB (requires `.[db]`)
    - exact_counts: when True, uses COUNT(*) per table (slow on large tables)

    Outputs (in out_dir):
    - REVIEW.md
    - review.html
    - schema.mmd
    - schema.svg
    - schema.png (best-effort; requires cairosvg and formats include png)
    - review.json (machine-readable summary)
    """
    cfg = _load_json(config_path)
    data_config = coerce_data_config(cfg.get("data_config") or cfg.get("data") or {})
    db_config = coerce_db_config(cfg.get("db_config") or cfg.get("db") or {})

    base_table = str(data_config.get("table_name") or "").strip()
    if not base_table:
        raise ValueError("data_config.table_name is required")
    key_sep = str(data_config.get("KEY_SEP", "__"))
    base_table_sql = truncate_table_name(base_table, max_len=MYSQL_IDENTIFIER_MAX_LEN)

    report = _load_json(report_path) if report_path else None
    issues = (report.get("issues") if report else None) or None

    fmt = _parse_formats(formats)

    # 1) Seed table list
    table_infos = _collect_table_infos_from_report(base_table=base_table, report=report) if report else []

    # 2) Optional DB introspection (fills rows/cols/indexes + optional samples)
    db_masked = None
    db_error = None
    samples_by_table: dict[str, list[dict[str, Any]]] = {}
    if db_enabled:
        db_masked = _mask_db_config(db_config)
        try:
            db = DBIntrospector(db_config)
            if not table_infos:
                table_infos = _collect_table_infos_from_db_prefix(db=db, base_table=base_table)
            table_infos = _merge_db_details(db=db, table_infos=table_infos, exact_counts=bool(exact_counts))
            if sample_rows is not None and int(sample_rows) > 0:
                sr = int(sample_rows)
                mt = max(1, int(sample_max_tables or 1))
                ordered = sorted(table_infos, key=lambda t: (0 if t.name_sql == base_table_sql else 1, t.name_sql))
                for ti in ordered[:mt]:
                    try:
                        samples_by_table[ti.name_sql] = db.sample_rows(table_name=ti.name_sql, limit=sr)
                    except Exception:
                        continue
        except Exception as e:
            # Keep best-effort: still produce pack with non-DB info.
            db_enabled = False
            db_error = str(e)

    # Normalize ordering
    table_infos = sorted(table_infos, key=lambda t: t.name_sql)
    for ti in table_infos:
        if ti.name_original == base_table:
            base_table_sql = ti.name_sql
            break

    use_original_names = any(ti.name_original for ti in table_infos)
    base_table_graph = base_table if use_original_names else base_table_sql

    generated_at = _utc_now_iso()
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # Mermaid
    mermaid = render_mermaid(base_table=base_table_graph, table_infos=table_infos, key_sep=key_sep)
    mermaid_path = out_path / "schema.mmd"
    if "mmd" in fmt or "mermaid" in fmt:
        _write_text(mermaid_path, mermaid)
    else:
        # Always write for HTML linking.
        _write_text(mermaid_path, mermaid)

    # SVG (+ optional PNG)
    svg_text = render_simple_svg(base_table=base_table_graph, table_infos=table_infos, key_sep=key_sep)
    svg_path = out_path / "schema.svg"
    if "svg" in fmt:
        _write_text(svg_path, svg_text)
    else:
        _write_text(svg_path, svg_text)

    png_path = out_path / "schema.png"
    png_written = False
    if "png" in fmt:
        png_written = _maybe_svg_to_png(svg_text, png_path)

    # Markdown
    md_path = out_path / "REVIEW.md"
    md = _render_markdown(
        generated_at=generated_at,
        base_table=base_table,
        base_table_sql=base_table_sql,
        key_sep=key_sep,
        formats=fmt,
        config_path=config_path,
        report_path=report_path,
        db_enabled=db_enabled,
        exact_counts=bool(exact_counts),
        db_config_masked=db_masked,
        db_error=db_error,
        mermaid_text=mermaid,
        table_infos=table_infos,
        issues=issues,
    )
    if "md" in fmt:
        _write_text(md_path, md)
    else:
        _write_text(md_path, md)

    # HTML
    html_path = out_path / "review.html"
    title = f"Review Pack: {base_table}"
    html_text = _render_html(
        title=title,
        markdown_path="REVIEW.md",
        schema_svg_path="schema.svg",
        schema_svg_text=svg_text,
        mermaid_path="schema.mmd",
        meta={
            "generated_at": generated_at,
            "config": config_path,
            "report": report_path or "",
            "base_table": base_table,
            "base_table_sql": base_table_sql,
            "key_sep": key_sep,
            "db_enabled": db_enabled,
            "db_error": db_error or "",
            "row_count": "exact" if exact_counts else "estimated",
            "sample_rows": int(sample_rows) if sample_rows is not None else "",
            "sample_max_tables": int(sample_max_tables),
        },
        table_infos=table_infos,
        issues=issues,
        samples_by_table=samples_by_table or None,
    )
    if "html" in fmt:
        _write_text(html_path, html_text)
    else:
        _write_text(html_path, html_text)

    # Machine-readable summary
    review_json: dict[str, Any] = {
        "generated_at": generated_at,
        "config": config_path,
        "report": report_path,
        "base_table": base_table,
        "base_table_sql": base_table_sql,
        "key_sep": key_sep,
        "db_enabled": bool(db_enabled),
        "db_error": db_error,
        "row_count_exact": bool(exact_counts),
        "table_name_namespace": "original" if use_original_names else "sql",
        "sample_rows": int(sample_rows) if sample_rows is not None and int(sample_rows) > 0 else None,
        "sample_max_tables": int(sample_max_tables),
        "db_config": _mask_db_config(db_config),
        "tables": [
            {
                "name_sql": ti.name_sql,
                "name_original": ti.name_original,
                "rows": ti.row_count,
                "rows_estimate": ti.table_rows_estimate,
                "rows_exact": ti.row_count_exact,
                "data_length": ti.data_length,
                "index_length": ti.index_length,
                "engine": ti.engine,
                "collation": ti.collation,
                "columns": ti.columns,
                "indexes": ti.indexes,
            }
            for ti in table_infos
        ],
        "issues": issues or [],
        "artifacts": {
            "schema_svg": "schema.svg",
            "schema_png": "schema.png" if png_written else None,
            "schema_mmd": "schema.mmd",
            "review_md": "REVIEW.md",
            "review_html": "review.html",
        },
    }
    if samples_by_table:
        _write_text(out_path / "samples.json", json.dumps(samples_by_table, ensure_ascii=False, indent=2))
        review_json["artifacts"]["samples_json"] = "samples.json"
    _write_text(out_path / "review.json", json.dumps(review_json, ensure_ascii=False, indent=2))

    return {
        "out_dir": str(out_path),
        "review_md": str(md_path),
        "review_html": str(html_path),
        "schema_svg": str(svg_path),
        "schema_png": str(png_path) if png_written else None,
        "schema_mmd": str(mermaid_path),
        "review_json": str(out_path / "review.json"),
    }
