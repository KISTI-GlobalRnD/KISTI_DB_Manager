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
    box_h: int = 58,
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

    def _truncate_middle(text: str, *, max_chars: int = 30) -> str:
        text = str(text)
        if len(text) <= max_chars:
            return text
        if max_chars <= 3:
            return text[:max_chars]
        head = max(8, (max_chars - 1) // 2)
        tail = max(8, max_chars - 1 - head)
        return f"{text[:head]}…{text[-tail:]}"

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
        ti = name_to_info.get(name)
        if ti is not None and "__excepted__" in str(ti.name_sql):
            return "#FFF8C5"
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
        graph_label = _display_label(ti.label() if ti else name)
        primary_label = _truncate_middle(ti.name_sql if ti is not None else name, max_chars=32)
        secondary_label = _truncate_middle(graph_label, max_chars=34)
        rows = ti.rows_label() if ti else "n/a"
        cols = len(ti.columns or []) if ti and ti.columns is not None else None
        cols_label = str(cols) if cols is not None else "n/a"

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
        fill_color = str(fill) if fill else node_color(name)

        lines.append(f"<g {' '.join(attrs)}>")
        lines.append(f"<title>{_svg_escape(title)}</title>")
        lines.append(
            f'<rect class="box" x="{x}" y="{y}" width="{box_w}" height="{box_h}" fill="{_svg_escape(fill_color)}" />'
        )
        lines.append(f'<text class="label" x="{x + 10}" y="{y + 17}">{_svg_escape(primary_label)}</text>')
        if secondary_label and secondary_label != primary_label:
            lines.append(f'<text class="meta" x="{x + 10}" y="{y + 33}">{_svg_escape(secondary_label)}</text>')
        lines.append(f'<text class="meta" x="{x + 10}" y="{y + 49}">rows: {_svg_escape(rows)} · cols: {_svg_escape(cols_label)}</text>')
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
    table_badges: Mapping[str, Mapping[str, int]] | None = None,
    timings_ms: Mapping[str, Any] | None = None,
    stats: Mapping[str, Any] | None = None,
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
            "<button id=\"schema-download-svg\" type=\"button\">SVG</button>"
            "<button id=\"schema-download-png\" type=\"button\">PNG</button>"
            "<span class=\"schema-option\">"
            "<span class=\"muted\">Depth</span>"
            "<input id=\"schema-depth\" type=\"range\" min=\"0\" max=\"0\" step=\"1\" />"
            "<code id=\"schema-depth-value\">0</code>"
            "</span>"
            "<label class=\"schema-option\">"
            "<input id=\"schema-only-flagged\" type=\"checkbox\" />"
            "<span class=\"muted\">Only flagged</span>"
            "</label>"
            "<span class=\"schema-option\">"
            "<span class=\"muted\">Color</span>"
            "<select id=\"schema-colorby\">"
            "<option value=\"\">none</option>"
            "<option value=\"rows\">rows</option>"
            "<option value=\"size\">size</option>"
            "</select>"
            "</span>"
            "<span class=\"schema-option\">"
            "<span class=\"muted\">Top</span>"
            "<input id=\"schema-top-pct\" type=\"range\" min=\"1\" max=\"100\" step=\"1\" value=\"100\" />"
            "<code id=\"schema-top-pct-value\">100%</code>"
            "</span>"
            "<span class=\"schema-option\">"
            "<label class=\"schema-option\">"
            "<input id=\"schema-focus\" type=\"checkbox\" />"
            "<span class=\"muted\">Focus</span>"
            "</label>"
            "<select id=\"schema-focus-mode\">"
            "<option value=\"subtree\">subtree</option>"
            "<option value=\"khop\">k-hop</option>"
            "<option value=\"path\">path-to-base</option>"
            "</select>"
            "<span class=\"muted\">hops</span>"
            "<input id=\"schema-focus-hops\" type=\"range\" min=\"1\" max=\"6\" step=\"1\" value=\"2\" />"
            "<code id=\"schema-focus-hops-value\">2</code>"
            "<label class=\"schema-option\">"
            "<input id=\"schema-focus-path\" type=\"checkbox\" checked />"
            "<span class=\"muted\">Base path</span>"
            "</label>"
            "</span>"
            "<span id=\"schema-status\" class=\"muted\"></span>"
            "</div>"
            "<div id=\"schema-container\" class=\"schema-container\">"
            + svg_inline
            + "</div>"
            "<div id=\"schema-legend\" class=\"schema-legend\" style=\"display:none\">"
            "<div class=\"schema-legend-row\">"
            "<span class=\"muted\">Heatmap</span>"
            "<code id=\"schema-legend-metric\"></code>"
            "<span class=\"muted\">min</span><code id=\"schema-legend-min\"></code>"
            "<span class=\"muted\">max</span><code id=\"schema-legend-max\"></code>"
            "<span class=\"muted\">cutoff</span><code id=\"schema-legend-cutoff\"></code>"
            "</div>"
            "<div class=\"schema-legend-bar\"></div>"
            "</div>"
            "<div class=\"schema-join\">"
            "<div class=\"schema-join-toolbar\">"
            "<span class=\"muted\">Join SQL (via <code>id</code>)</span>"
            "<button id=\"schema-join-copy\" type=\"button\">Copy</button>"
            "<span id=\"schema-join-status\" class=\"muted\"></span>"
            "</div>"
            "<pre id=\"schema-join-sql\" class=\"sql-block\"></pre>"
            "</div>"
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
            samples = samples_by_table.get(ti.name_sql)
            samples_n = len(samples) if isinstance(samples, list) else ""
            try:
                sample_text = json.dumps(samples, ensure_ascii=False, indent=2)
            except Exception:
                sample_text = repr(samples)
            pre_id = f"samples_{_sanitize_html_id(ti.name_sql)}"
            samples_html = (
                "<details class=\"subdetails\">"
                f"<summary><b>Samples</b> <span class=\"muted\">({h(samples_n)} rows)</span></summary>"
                "<div class=\"samples-toolbar\">"
                f"<input class=\"samples-search\" type=\"search\" placeholder=\"Search in samples…\" data-target=\"{h(pre_id)}\" />"
                f"<button class=\"samples-copy\" type=\"button\" data-target=\"{h(pre_id)}\">Copy</button>"
                f"<span class=\"muted\" data-target-status=\"{h(pre_id)}\"></span>"
                "</div>"
                f"<pre id=\"{h(pre_id)}\" class=\"samples-pre\">{h(sample_text)}</pre>"
                "</details>"
            )

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

    # Badge counts for SVG overlay (errors/warnings/quarantine).
    known_tables = {ti.name_sql for ti in table_infos}
    badge_counts: dict[str, dict[str, int]] = {}

    for it in issues or []:
        if not isinstance(it, Mapping):
            continue
        lvl = str(it.get("level") or "").strip().lower()
        if lvl not in {"error", "warning"}:
            continue
        ctx = it.get("context") or {}
        table = None
        if isinstance(ctx, Mapping):
            for k in ("table", "table_name", "table_sql"):
                v = ctx.get(k)
                if v:
                    table = str(v)
                    break
        if not table or table not in known_tables:
            continue
        badge_counts.setdefault(table, {}).setdefault(lvl, 0)
        badge_counts[table][lvl] += 1

    if table_badges:
        for t, counts in table_badges.items():
            if not t:
                continue
            table = str(t)
            if table not in known_tables:
                continue
            if not isinstance(counts, Mapping):
                continue
            for k, v in counts.items():
                if not k:
                    continue
                try:
                    n = int(v)
                except Exception:
                    continue
                if n <= 0:
                    continue
                badge_counts.setdefault(table, {}).setdefault(str(k), 0)
                badge_counts[table][str(k)] += n

    badge_counts_json = json.dumps(badge_counts, ensure_ascii=False).replace("<", "\\u003c")

    base_table_sql_value = str(meta.get("base_table_sql") or meta.get("base_table") or "")
    key_sep_value = str(meta.get("key_sep") or meta.get("KEY_SEP") or "__")
    base_table_sql_json = json.dumps(base_table_sql_value, ensure_ascii=False).replace("<", "\\u003c")
    key_sep_json = json.dumps(key_sep_value, ensure_ascii=False).replace("<", "\\u003c")

    timings_section = ""
    if timings_ms and isinstance(timings_ms, Mapping):
        items: list[tuple[str, int]] = []
        for k, v in timings_ms.items():
            try:
                ms = int(v)
            except Exception:
                continue
            if ms <= 0:
                continue
            items.append((str(k), ms))
        items.sort(key=lambda kv: (-kv[1], kv[0]))
        total_ms = sum(ms for _k, ms in items)
        max_ms = max((ms for _k, ms in items), default=0)

        timing_rows = []
        for k, ms in items[:30]:
            pct = int(round((ms / max_ms) * 100)) if max_ms > 0 else 0
            timing_rows.append(
                "<tr>"
                f"<td><code>{h(k)}</code></td>"
                f"<td style=\"text-align:right;\">{ms}</td>"
                f"<td style=\"text-align:right;\">{ms/1000.0:.3f}</td>"
                "<td>"
                "<div class=\"bar\"><div class=\"bar-fill\" style=\"width: "
                + h(pct)
                + "%\"></div></div>"
                "</td>"
                "</tr>"
            )

        body = "".join(timing_rows) if timing_rows else '<tr><td colspan="4" class="muted">(none)</td></tr>'
        timings_section = f"""
  <div class="card">
    <h2>Timings</h2>
    <p class="muted">Total: <code>{h(total_ms)}</code> ms ({total_ms/1000.0:.3f}s). Showing top {h(min(len(items), 30))}.</p>
    <table>
      <thead><tr><th>key</th><th style="text-align:right;">ms</th><th style="text-align:right;">sec</th><th>share</th></tr></thead>
      <tbody>
        {body}
      </tbody>
    </table>
  </div>
""".rstrip()

    stats_section = ""
    if stats and isinstance(stats, Mapping):
        sitems: list[tuple[str, int]] = []
        for k, v in stats.items():
            try:
                n = int(v)
            except Exception:
                continue
            if n == 0:
                continue
            sitems.append((str(k), n))
        sitems.sort(key=lambda kv: (-kv[1], kv[0]))
        srows = []
        for k, n in sitems[:40]:
            srows.append("<tr>" f"<td><code>{h(k)}</code></td>" f"<td style=\"text-align:right;\">{n}</td>" "</tr>")
        body = "".join(srows) if srows else '<tr><td colspan="2" class="muted">(none)</td></tr>'
        stats_section = f"""
  <div class="card">
    <h2>Stats</h2>
    <p class="muted">Showing top {h(min(len(sitems), 40))} non-zero counters.</p>
    <table>
      <thead><tr><th>key</th><th style="text-align:right;">value</th></tr></thead>
      <tbody>
        {body}
      </tbody>
    </table>
  </div>
""".rstrip()

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
	    .bar {{ height: 10px; border: 1px solid #d0d7de; border-radius: 999px; overflow: hidden; background: #ffffff; }}
	    .bar-fill {{ height: 100%; background: #0969da; }}
	    details.details summary {{ cursor: pointer; }}
	    details.details {{ border: 1px solid #d0d7de; border-radius: 12px; padding: 10px 12px; margin: 10px 0; background: #fff; }}
	    details.subdetails summary {{ cursor: pointer; }}
	    details.subdetails {{ border: 1px solid #d0d7de; border-radius: 12px; padding: 10px 12px; margin: 10px 0; background: #ffffff; }}
	    .samples-toolbar {{ display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin: 10px 0; }}
	    .samples-toolbar input[type="search"] {{ flex: 1; padding: 8px 10px; border: 1px solid #d0d7de; border-radius: 10px; }}
	    .samples-toolbar button {{ padding: 8px 12px; border: 1px solid #d0d7de; border-radius: 10px; background: #f6f8fa; cursor: pointer; }}
	    .samples-pre {{ border: 1px solid #d0d7de; border-radius: 12px; padding: 10px 12px; background: #f6f8fa; overflow: auto; max-height: 260px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; font-size: 12px; }}
	    .samples-pre mark {{ background: #ffdf5d; }}
		    .schema-toolbar {{ display: flex; gap: 8px; align-items: center; margin-bottom: 10px; flex-wrap: wrap; }}
		    .schema-toolbar .schema-option {{ display: inline-flex; gap: 6px; align-items: center; }}
		    .schema-toolbar input[type="search"] {{ flex: 1; padding: 8px 10px; border: 1px solid #d0d7de; border-radius: 10px; }}
		    .schema-toolbar input[type="range"] {{ width: 140px; }}
		    .schema-toolbar select {{ padding: 8px 10px; border: 1px solid #d0d7de; border-radius: 10px; background: #ffffff; }}
		    .schema-toolbar button {{ padding: 8px 12px; border: 1px solid #d0d7de; border-radius: 10px; background: #f6f8fa; cursor: pointer; }}
		    .schema-container {{ max-height: 70vh; overflow: auto; border: 1px solid #d0d7de; border-radius: 12px; padding: 8px; background: #fff; }}
		    .schema-container svg {{ max-width: 100%; height: auto; }}
		    .schema-legend {{ margin-top: 10px; }}
		    .schema-legend-row {{ display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-bottom: 6px; }}
		    .schema-legend-bar {{ height: 10px; border: 1px solid #d0d7de; border-radius: 999px; background: linear-gradient(90deg, #e6f0ff, #0969da); }}
		    .schema-join {{ margin-top: 10px; }}
		    .schema-join-toolbar {{ display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-bottom: 6px; }}
		    .schema-join-toolbar button {{ padding: 8px 12px; border: 1px solid #d0d7de; border-radius: 10px; background: #f6f8fa; cursor: pointer; }}
		    .sql-block {{ border: 1px solid #d0d7de; border-radius: 12px; padding: 10px 12px; background: #f6f8fa; overflow: auto; max-height: 240px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; font-size: 12px; }}
		    .schema-container .node.hidden {{ display: none; }}
		    .schema-container .edge.hidden {{ display: none; }}
	    .schema-container .node.dim {{ opacity: 0.15; }}
	    .schema-container .node.match .box {{ stroke: #fb8c00; stroke-width: 2; }}
	    .schema-container .node.has-error .box {{ stroke: #cf222e; stroke-width: 2; }}
	    .schema-container .node.has-warning .box {{ stroke: #bf8700; stroke-width: 2; }}
	    .schema-container .node.has-quarantine .box {{ stroke: #8250df; stroke-width: 2; }}
	    .schema-container .node.focus-root .box {{ stroke: #0969da; stroke-width: 3; stroke-dasharray: none; }}
	    .schema-container .node.focus-path .box {{ stroke: #0969da; stroke-width: 2; stroke-dasharray: 6 3; }}
	    .schema-container .edge.focus-path {{ stroke: #0969da; stroke-width: 2; stroke-dasharray: 6 3; }}
	    .schema-container .node.selected .box {{ stroke: #0969da; stroke-width: 2; stroke-dasharray: none; }}
	    .schema-container .edge.selected {{ stroke: #0969da; stroke-width: 2; stroke-dasharray: none; }}
    .schema-container .badge-text {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; font-size: 11px; font-weight: 600; }}
    .schema-container .badge-error {{ fill: #cf222e; }}
    .schema-container .badge-warning {{ fill: #bf8700; }}
    .schema-container .badge-quarantine {{ fill: #8250df; }}
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

  {timings_section}

  {stats_section}

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
	    const dlSvg = document.getElementById('schema-download-svg');
	    const dlPng = document.getElementById('schema-download-png');
	    const status = document.getElementById('schema-status');
	    const svg = container.querySelector('svg');
	    if (!svg) return;

	    const nodes = Array.from(svg.querySelectorAll('.node'));
	    const edges = Array.from(svg.querySelectorAll('.edge'));
	    const badgeCounts = {badge_counts_json} || {{}};
	    const KEY_SEP = {key_sep_json};
		    const BASE_TABLE_SQL = {base_table_sql_json};
		    const depthInput = document.getElementById('schema-depth');
		    const depthValue = document.getElementById('schema-depth-value');
		    const onlyFlagged = document.getElementById('schema-only-flagged');
		    const colorBy = document.getElementById('schema-colorby');
		    const topPct = document.getElementById('schema-top-pct');
		    const topPctValue = document.getElementById('schema-top-pct-value');
		    const legend = document.getElementById('schema-legend');
		    const legendMetric = document.getElementById('schema-legend-metric');
		    const legendMin = document.getElementById('schema-legend-min');
		    const legendMax = document.getElementById('schema-legend-max');
			    const legendCutoff = document.getElementById('schema-legend-cutoff');
			    const joinSql = document.getElementById('schema-join-sql');
			    const joinCopy = document.getElementById('schema-join-copy');
			    const joinStatus = document.getElementById('schema-join-status');
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
		    const rectBySql = {{}};
		    const metricBySql = {{rows: {{}}, size: {{}}}};
		    const sortedMetricVals = {{rows: [], size: []}};
		    for (const n of nodes) {{
		      const sql = n.getAttribute('data-name-sql') || '';
		      if (!sql) continue;
		      nodeBySql[sql] = n;
		      const rect = n.querySelector('rect.box');
		      if (rect) {{
		        rectBySql[sql] = rect;
		        if (!rect.dataset.origFill) rect.dataset.origFill = rect.getAttribute('fill') || '';
		      }}
		      const rowsVal = Number(n.getAttribute('data-rows') || 0) || 0;
		      const sizeVal = Number(n.getAttribute('data-size') || 0) || 0;
		      metricBySql.rows[sql] = rowsVal;
		      metricBySql.size[sql] = sizeVal;
		      if (sql !== BASE_TABLE_SQL && rowsVal > 0) sortedMetricVals.rows.push(rowsVal);
		      if (sql !== BASE_TABLE_SQL && sizeVal > 0) sortedMetricVals.size.push(sizeVal);
		      const d = nodeDepth(sql);
		      depthBySql[sql] = d;
		      if (d > maxDepth) maxDepth = d;
		    }}
		    sortedMetricVals.rows.sort((a, b) => a - b);
		    sortedMetricVals.size.sort((a, b) => a - b);

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

    function applyBadges(counts) {{
      if (!counts) return;
      const ns = 'http://www.w3.org/2000/svg';
      for (const n of nodes) {{
        const tableSql = n.getAttribute('data-name-sql') || '';
        if (!tableSql) continue;
        const c = counts[tableSql];
        if (!c) continue;

        const err = Number(c.error || 0);
        const warn = Number(c.warning || 0);
        const quar = Number(c.quarantine || 0);

        if (err > 0) n.classList.add('has-error');
        else if (warn > 0) n.classList.add('has-warning');
        if (quar > 0) n.classList.add('has-quarantine');

        let text = '';
        if (err > 0) text += ('E' + err);
        else if (warn > 0) text += ('W' + warn);
        if (quar > 0) text += (text ? ' ' : '') + ('Q' + quar);
        if (!text) continue;

        const rect = n.querySelector('rect.box');
        if (!rect) continue;
        const x = Number(rect.getAttribute('x') || 0);
        const y = Number(rect.getAttribute('y') || 0);
        const w = Number(rect.getAttribute('width') || 0);
        const tx = x + w - 10;
        const ty = y + 16;

        const t = document.createElementNS(ns, 'text');
        t.setAttribute('x', String(tx));
        t.setAttribute('y', String(ty));
        t.setAttribute('text-anchor', 'end');
        t.setAttribute('class', 'badge-text');
        if (err > 0) t.classList.add('badge-error');
        else if (warn > 0) t.classList.add('badge-warning');
        else if (quar > 0) t.classList.add('badge-quarantine');
        t.textContent = text;
        n.appendChild(t);
      }}
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

		    function isFlagged(nodeEl) {{
		      return (
		        nodeEl.classList.contains('has-error') ||
		        nodeEl.classList.contains('has-warning') ||
		        nodeEl.classList.contains('has-quarantine') ||
		        nodeEl.classList.contains('diff-added') ||
		        nodeEl.classList.contains('diff-removed') ||
		        nodeEl.classList.contains('diff-changed')
		      );
		    }}

		    function clamp01(x) {{
		      return Math.max(0, Math.min(1, Number(x)));
		    }}

		    function formatInt(n) {{
		      const x = Number(n || 0);
		      if (!isFinite(x)) return String(n);
		      try {{
		        return x.toLocaleString();
		      }} catch (_e) {{
		        return String(x);
		      }}
		    }}

		    function formatBytes(bytes) {{
		      const x = Number(bytes || 0);
		      if (!isFinite(x) || x <= 0) return '0 B';
		      const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
		      let v = x;
		      let u = 0;
		      while (v >= 1024 && u < units.length - 1) {{
		        v = v / 1024;
		        u += 1;
		      }}
		      const digits = v >= 100 ? 0 : (v >= 10 ? 1 : 2);
		      return v.toFixed(digits) + ' ' + units[u];
		    }}

		    async function copyText(text) {{
		      const t = String(text || '');
		      if (!t) return false;
		      try {{
		        if (navigator.clipboard && navigator.clipboard.writeText) {{
		          await navigator.clipboard.writeText(t);
		          return true;
		        }}
		      }} catch (_e) {{}}
		      try {{
		        const ta = document.createElement('textarea');
		        ta.value = t;
		        ta.setAttribute('readonly', 'readonly');
		        ta.style.position = 'fixed';
		        ta.style.left = '-1000px';
		        ta.style.top = '-1000px';
		        document.body.appendChild(ta);
		        ta.select();
		        const ok = document.execCommand('copy');
		        document.body.removeChild(ta);
		        return !!ok;
		      }} catch (_e) {{
		        return false;
		      }}
		    }}

		    let joinTimer = null;
		    function flashJoinStatus(msg) {{
		      if (!joinStatus) return;
		      joinStatus.textContent = String(msg || '');
		      if (joinTimer) clearTimeout(joinTimer);
		      joinTimer = setTimeout(() => {{
		        joinStatus.textContent = '';
		      }}, 1400);
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

		    function renderJoinSql(tableSql) {{
		      const t = String(tableSql || '');
		      if (!t) return '';
		      if (!BASE_TABLE_SQL) return 'SELECT *\\nFROM `' + t + '`\\nLIMIT 5;';
		      if (t === BASE_TABLE_SQL) return 'SELECT *\\nFROM `' + BASE_TABLE_SQL + '`\\nLIMIT 5;';
		      const path = joinPathToBase(t);
		      if (!path) return 'SELECT *\\nFROM `' + t + '`\\nLIMIT 5;';
		      let sql = 'SELECT\\n  b.*';
		      for (let i = 1; i < path.length; i++) {{
		        sql += `,\\n  t${{i}}.*`;
		      }}
		      sql += `\\nFROM \\`${{BASE_TABLE_SQL}}\\` b`;
		      for (let i = 1; i < path.length; i++) {{
		        sql += `\\nLEFT JOIN \\`${{path[i]}}\\` t${{i}} ON b.id = t${{i}}.id`;
		      }}
		      sql += '\\nLIMIT 5;';
		      return sql;
		    }}

		    function updateJoinSql(tableSql) {{
		      if (!joinSql) return;
		      const txt = renderJoinSql(tableSql || BASE_TABLE_SQL);
		      joinSql.textContent = txt;
		    }}

		    function cutoffFor(metric, pct) {{
		      const arr = (sortedMetricVals[metric] || []);
		      if (!arr.length) return null;
		      const p = Number(pct);
		      if (!isFinite(p)) return arr[0];
		      const q = 1 - (Math.max(1, Math.min(100, p)) / 100);
		      const idx = Math.floor(q * (arr.length - 1));
		      return arr[Math.max(0, Math.min(arr.length - 1, idx))];
		    }}

		    const HEAT_MIN = [230, 240, 255]; // #e6f0ff
		    const HEAT_MAX = [9, 105, 218];  // #0969da
		    function heatColor(t) {{
		      const tt = clamp01(t);
		      const r = Math.round(HEAT_MIN[0] + (HEAT_MAX[0] - HEAT_MIN[0]) * tt);
		      const g = Math.round(HEAT_MIN[1] + (HEAT_MAX[1] - HEAT_MIN[1]) * tt);
		      const b = Math.round(HEAT_MIN[2] + (HEAT_MAX[2] - HEAT_MIN[2]) * tt);
		      const toHex = (x) => x.toString(16).padStart(2, '0');
		      return '#' + toHex(r) + toHex(g) + toHex(b);
		    }}

			    function applyHeatmap() {{
			      const metric = colorBy ? String(colorBy.value || '') : '';
			      const pct = topPct ? Number(topPct.value || 100) : 100;
			      if (topPctValue) topPctValue.textContent = String(pct) + '%';

		      const enabled = (metric === 'rows' || metric === 'size');
		      if (topPct) topPct.disabled = !enabled;

		      if (!enabled) {{
		        if (legend) legend.style.display = 'none';
		        if (legendMetric) legendMetric.textContent = '';
		        if (legendMin) legendMin.textContent = '';
		        if (legendMax) legendMax.textContent = '';
		        if (legendCutoff) legendCutoff.textContent = '';
		        for (const sql in rectBySql) {{
		          const rect = rectBySql[sql];
		          if (!rect) continue;
		          rect.setAttribute('fill', rect.dataset.origFill || rect.getAttribute('fill') || '');
		        }}
		        return;
		      }}

		      const arr = (sortedMetricVals[metric] || []);
		      if (!arr.length) {{
		        if (legend) legend.style.display = '';
		        if (legendMetric) legendMetric.textContent = metric;
		        if (legendMin) legendMin.textContent = 'n/a';
		        if (legendMax) legendMax.textContent = 'n/a';
		        if (legendCutoff) legendCutoff.textContent = pct < 100 ? 'n/a' : '';
		        for (const sql in rectBySql) {{
		          const rect = rectBySql[sql];
		          if (!rect) continue;
		          rect.setAttribute('fill', rect.dataset.origFill || rect.getAttribute('fill') || '');
		        }}
		        return;
		      }}

		      const minV = arr[0];
		      const maxV = arr[arr.length - 1];
		      const minLog = Math.log10(minV + 1);
		      const maxLog = Math.log10(maxV + 1);
		      const denom = (maxLog - minLog) || 1;

		      for (const sql in rectBySql) {{
		        const rect = rectBySql[sql];
		        if (!rect) continue;
		        if (sql === BASE_TABLE_SQL) {{
		          rect.setAttribute('fill', rect.dataset.origFill || rect.getAttribute('fill') || '');
		          continue;
		        }}
		        const v = Number((metricBySql[metric] || {{}})[sql] || 0);
		        if (!isFinite(v) || v <= 0) {{
		          rect.setAttribute('fill', '#f6f8fa');
		          continue;
		        }}
		        const t = (Math.log10(v + 1) - minLog) / denom;
		        rect.setAttribute('fill', heatColor(t));
		      }}

		      if (legend) legend.style.display = '';
		      if (legendMetric) legendMetric.textContent = metric;
		      if (legendMin) legendMin.textContent = metric === 'size' ? formatBytes(minV) : formatInt(minV);
		      if (legendMax) legendMax.textContent = metric === 'size' ? formatBytes(maxV) : formatInt(maxV);
		      const cutoff = pct < 100 ? cutoffFor(metric, pct) : null;
		      if (legendCutoff) {{
		        if (pct >= 100) legendCutoff.textContent = '';
		        else if (cutoff === null) legendCutoff.textContent = 'n/a';
		        else legendCutoff.textContent = metric === 'size' ? formatBytes(cutoff) : formatInt(cutoff);
			      }}
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
			      // Edge mapping is child->edge for our inferred tree.
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

			    let visibleNodesCount = nodes.length;
			    function recomputeVisibility() {{
			      const depthLimit = depthInput ? Number(depthInput.value || maxDepth) : maxDepth;
			      if (depthValue) depthValue.textContent = String(depthLimit);
			      const only = !!(onlyFlagged && onlyFlagged.checked);
			      const metric = colorBy ? String(colorBy.value || '') : '';
			      const pct = topPct ? Number(topPct.value || 100) : 100;
			      const metricEnabled = (metric === 'rows' || metric === 'size');
			      const metricActive = metricEnabled && pct < 100 && (sortedMetricVals[metric] || []).length > 0;
			      const focusEnabled = !!(focus && focus.checked);
			      const fmode = focusMode ? String(focusMode.value || 'subtree') : 'subtree';
			      const fhops = focusHops ? Number(focusHops.value || 2) : 2;
			      const basePathOn = focusEnabled && (fmode === 'path' || (!!focusBasePath && focusBasePath.checked));

			      updateFocusControls();

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

		      const allowMetric = new Set();
		      if (metricActive) {{
		        const cutoff = cutoffFor(metric, pct);
		        if (cutoff !== null && BASE_TABLE_SQL) allowMetric.add(BASE_TABLE_SQL);
		        for (const n of nodes) {{
		          const sql = n.getAttribute('data-name-sql') || '';
		          if (!sql) continue;
		          if (sql === BASE_TABLE_SQL) continue;
		          if ((depthBySql[sql] || 0) > depthLimit) continue;
		          const v = Number((metricBySql[metric] || {{}})[sql] || 0);
		          if (!isFinite(v) || cutoff === null || v < cutoff) continue;
		          allowMetric.add(sql);
		          let cur = sql;
		          let safety = 0;
		          while (safety++ < 1000) {{
		            const p = parentByChildSql[cur];
		            if (!p) break;
		            allowMetric.add(p);
		            if (p === BASE_TABLE_SQL) break;
		            cur = p;
		          }}
		        }}
		      }}

			      visibleNodesCount = 0;
			      for (const n of nodes) {{
			        const sql = n.getAttribute('data-name-sql') || '';
			        const d = depthBySql[sql] || 0;
			        const withinDepth = d <= depthLimit;
			        const visible =
			          withinDepth &&
			          (!only || allow.has(sql)) &&
			          (!metricActive || allowMetric.has(sql)) &&
			          (!allowFocus || allowFocus.has(sql));
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

		    function safeFileName(text) {{
		      const t = String(text || 'schema');
		      const s = t.replace(/[^0-9A-Za-z_.-]+/g, '_');
		      return (s.length > 120 ? s.slice(0, 120) : s) || 'schema';
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
		      '.node.has-error .box{{stroke:#cf222e;stroke-width:2;}}',
		      '.node.has-warning .box{{stroke:#bf8700;stroke-width:2;}}',
		      '.node.has-quarantine .box{{stroke:#8250df;stroke-width:2;}}',
		      '.node.focus-root .box{{stroke:#0969da;stroke-width:3;stroke-dasharray:none;}}',
		      '.node.focus-path .box{{stroke:#0969da;stroke-width:2;stroke-dasharray:6 3;}}',
		      '.edge.focus-path{{stroke:#0969da;stroke-width:2;stroke-dasharray:6 3;}}',
		      '.node.selected .box{{stroke:#0969da;stroke-width:2;stroke-dasharray:none;}}',
		      '.edge.selected{{stroke:#0969da;stroke-width:2;stroke-dasharray:none;}}',
		      '.badge-text{{font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial;font-size:11px;font-weight:600;}}',
		      '.badge-error{{fill:#cf222e;}}',
		      '.badge-warning{{fill:#bf8700;}}',
		      '.badge-quarantine{{fill:#8250df;}}',
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
		      const name = safeFileName(BASE_TABLE_SQL || 'schema') + '.svg';
		      downloadBlob(blob, name);
		    }}

		    function exportPng() {{
		      try {{
		        const txt = exportSvgText();
		        const blob = new Blob([txt], {{ type: 'image/svg+xml;charset=utf-8' }});
		        const url = URL.createObjectURL(blob);
		        const img = new Image();
		        img.onload = () => {{
		          const w = Number(svg.getAttribute('width') || 0) || (svg.viewBox && svg.viewBox.baseVal ? Number(svg.viewBox.baseVal.width || 0) : 0) || 1400;
		          const h = Number(svg.getAttribute('height') || 0) || (svg.viewBox && svg.viewBox.baseVal ? Number(svg.viewBox.baseVal.height || 0) : 0) || 800;
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
		              const name = safeFileName(BASE_TABLE_SQL || 'schema') + '.png';
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

		    function parseBool(v) {{
		      const s = String(v || '').toLowerCase();
		      return s === '1' || s === 'true' || s === 'yes' || s === 'y' || s === 'on';
		    }}

		    function buildUiState() {{
		      return {{
		        q: search ? String(search.value || '') : '',
		        depth: depthInput ? Number(depthInput.value || maxDepth) : maxDepth,
		        flagged: !!(onlyFlagged && onlyFlagged.checked),
		        color: colorBy ? String(colorBy.value || '') : '',
		        top: topPct ? Number(topPct.value || 100) : 100,
		        focus: !!(focus && focus.checked),
		        fmode: focusMode ? String(focusMode.value || 'subtree') : 'subtree',
		        hops: focusHops ? Number(focusHops.value || 2) : 2,
		        bpath: !!(focusBasePath && focusBasePath.checked),
		        sel: String(selectedTableSql || ''),
		        froot: String(focusRootSql || ''),
		      }};
		    }}

		    const STORAGE_KEY = 'kisti-review:schema:' + String(BASE_TABLE_SQL || '');

		    function readStateFromUrl() {{
		      try {{
		        const params = new URLSearchParams(window.location.search || '');
		        const keys = ['q','depth','flagged','color','top','focus','fmode','hops','bpath','sel','froot'];
		        let has = false;
		        for (const k of keys) {{
		          if (params.has(k)) {{ has = true; break; }}
		        }}
		        if (!has) return null;
		        return {{
		          q: params.get('q') || '',
		          depth: params.get('depth'),
		          flagged: params.get('flagged'),
		          color: params.get('color') || '',
		          top: params.get('top'),
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
		      if (onlyFlagged && st.flagged != null) onlyFlagged.checked = parseBool(st.flagged);

		      if (colorBy && typeof st.color === 'string') {{
		        const c = String(st.color || '');
		        colorBy.value = c;
		      }}
		      if (topPct && st.top != null) {{
		        const v = Number(st.top);
		        if (isFinite(v)) topPct.value = String(Math.max(1, Math.min(100, v)));
		      }}

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
		        setOrDel('flagged', st.flagged ? '1' : '', '');
		        setOrDel('color', st.color || '', '');
		        setOrDel('top', String(st.top), '100');
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

		    let persistTimer = null;
		    let persistSuppressed = false;
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
		      updateJoinSql(tableSql);
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
				    if (colorBy) {{
				      colorBy.addEventListener('change', () => {{
				        if (topPct) topPct.value = '100';
				        applyHeatmap();
			        recomputeVisibility();
			        applyFilter(search ? search.value : '');
			        schedulePersist();
			      }});
			    }}
				    if (topPct) {{
				      topPct.addEventListener('input', () => {{
				        applyHeatmap();
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
				    if (joinCopy) {{
				      joinCopy.addEventListener('click', async () => {{
				        const ok = await copyText(joinSql ? joinSql.textContent : '');
				        flashJoinStatus(ok ? 'copied' : 'copy failed');
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
			        if (colorBy) colorBy.value = '';
			        if (topPct) topPct.value = '100';
			        applyHeatmap();
				        recomputeVisibility();
				        applyFilter('');
				        clearSelection();
				        updateJoinSql(BASE_TABLE_SQL);
				        schedulePersist();
				      }});
				    }}

				    // Restore UI state (URL has priority over localStorage).
				    persistSuppressed = true;
				    const initialState = readStateFromUrl() || readStateFromStorage();
				    if (initialState) applyState(initialState);
				    persistSuppressed = false;

				    applyBadges(badgeCounts);
				    applyHeatmap();
				    updateJoinSql(BASE_TABLE_SQL);
				    recomputeVisibility();
				    applyFilter(search ? search.value : '');
				    if (selectedTableSql) {{
				      // Apply selection after initial render.
				      persistSuppressed = true;
				      try {{ selectTable(selectedTableSql); }} catch (_e) {{}}
				      persistSuppressed = false;
				    }}

				    // Samples UX: collapse/search/copy (best-effort).
				    const samplePres = Array.from(document.querySelectorAll('pre.samples-pre'));
				    for (const p of samplePres) {{
			      if (!p.dataset.raw) p.dataset.raw = p.textContent || '';
			    }}

			    function escapeHtml(text) {{
			      return String(text || '')
			        .replace(/&/g, '&amp;')
			        .replace(/</g, '&lt;')
			        .replace(/>/g, '&gt;');
			    }}

			    function highlightHtml(text, query) {{
			      const t = String(text || '');
			      const q = String(query || '').trim();
			      if (!q) return {{ html: escapeHtml(t), count: 0 }};
			      const tl = t.toLowerCase();
			      const ql = q.toLowerCase();
			      let idx = 0;
			      let count = 0;
			      let out = '';
			      while (true) {{
			        const pos = tl.indexOf(ql, idx);
			        if (pos === -1) break;
			        out += escapeHtml(t.slice(idx, pos));
			        out += '<mark>' + escapeHtml(t.slice(pos, pos + q.length)) + '</mark>';
			        idx = pos + q.length;
			        count += 1;
			        if (count > 5000) break;
			      }}
			      out += escapeHtml(t.slice(idx));
			      return {{ html: out, count }};
			    }}

			    function flashStatus(el, msg) {{
			      if (!el) return;
			      const text = String(msg || '');
			      el.textContent = text;
			      setTimeout(() => {{
			        if (el.textContent === text) el.textContent = '';
			      }}, 1400);
			    }}

			    for (const inp of document.querySelectorAll('input.samples-search[data-target]')) {{
			      inp.addEventListener('input', () => {{
			        const targetId = inp.getAttribute('data-target') || '';
			        const pre = targetId ? document.getElementById(targetId) : null;
			        if (!pre) return;
			        const raw = pre.dataset.raw || pre.textContent || '';
			        const q = inp.value || '';
			        const r = highlightHtml(raw, q);
			        pre.innerHTML = r.html;
			        const st = document.querySelector('[data-target-status=\"' + targetId + '\"]');
			        if (st) st.textContent = q.trim() ? ('matches: ' + r.count) : '';
			      }});
			    }}

			    for (const btn of document.querySelectorAll('button.samples-copy[data-target]')) {{
			      btn.addEventListener('click', async () => {{
			        const targetId = btn.getAttribute('data-target') || '';
			        const pre = targetId ? document.getElementById(targetId) : null;
			        if (!pre) return;
			        const raw = pre.dataset.raw || pre.textContent || '';
			        const ok = await copyText(raw);
			        const st = document.querySelector('[data-target-status=\"' + targetId + '\"]');
			        flashStatus(st, ok ? 'copied' : 'copy failed');
			      }});
			    }}
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
    timings_ms: Mapping[str, Any] | None,
    artifacts: Mapping[str, Any] | None,
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

    if timings_ms:
        try:
            total_ms = int((timings_ms or {}).get("pipeline.json.total") or 0)
        except Exception:
            total_ms = 0
        if total_ms <= 0:
            try:
                total_ms = sum(int(v) for v in (timings_ms or {}).values() if int(v) > 0)
            except Exception:
                total_ms = 0
        if total_ms > 0:
            lines.append("## Sample Profile")
            lines.append("")
            lines.append(f"- total_ms: `{total_ms}`")
            try:
                rr = int((stats or {}).get("records_read") or 0)
            except Exception:
                rr = 0
            if rr > 0:
                rps = float(rr) / max(0.001, (float(total_ms) / 1000.0))
                lines.append(f"- records_per_sec: `{rps:.2f}`")
            rows: list[tuple[str, int]] = []
            for k, v in (timings_ms or {}).items():
                try:
                    ms = int(v)
                except Exception:
                    continue
                if ms <= 0:
                    continue
                rows.append((str(k), int(ms)))
            rows = sorted(rows, key=lambda x: x[1], reverse=True)[:8]
            if rows:
                lines.append("- top_timings:")
                for k, v in rows:
                    share = (100.0 * float(v) / float(total_ms)) if total_ms > 0 else 0.0
                    lines.append(f"  - `{k}`: `{v}ms` ({share:.1f}%)")
            lines.append("")

    auto_except = (artifacts or {}).get("auto_except") if isinstance(artifacts, Mapping) else None
    if isinstance(auto_except, Mapping) and bool(auto_except.get("enabled")):
        sample = auto_except.get("sample") if isinstance(auto_except.get("sample"), Mapping) else {}
        thresholds = auto_except.get("thresholds") if isinstance(auto_except.get("thresholds"), Mapping) else {}
        estimate = auto_except.get("estimate") if isinstance(auto_except.get("estimate"), Mapping) else {}
        detected = list(auto_except.get("detected_except_keys") or [])
        lines.append("## Auto Except")
        lines.append("")
        lines.append(f"- enabled: `{auto_except.get('enabled')}`")
        if sample:
            lines.append(
                f"- sample: records `{sample.get('records_sampled')}` / requested `{sample.get('records_requested')}`, "
                f"sources `{sample.get('sources_sampled')}` / requested `{sample.get('max_sources_requested')}`, "
                f"duration `{sample.get('duration_s')}`s"
            )
        if thresholds:
            lines.append(
                f"- thresholds: unique_keys>={thresholds.get('unique_key_threshold')}, "
                f"min_obs>={thresholds.get('min_observations')}, "
                f"novelty>={thresholds.get('novelty_threshold')}"
            )
        eta_range = estimate.get("eta_seconds_range") if isinstance(estimate.get("eta_seconds_range"), list) else None
        if eta_range and len(eta_range) == 2:
            try:
                eta_lo = float(eta_range[0])
                eta_hi = float(eta_range[1])
                lines.append(f"- eta_estimate_s: `{eta_lo:.1f} ~ {eta_hi:.1f}`")
            except Exception:
                pass
        lines.append(f"- detected_except_keys: `{len(detected)}`")
        for k in detected[:20]:
            lines.append(f"  - `{k}`")
        if len(detected) > 20:
            lines.append(f"  - ... +{len(detected) - 20} more")
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
    data_overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Generate a pre-load review plan (no DB writes).

    - For JSON inputs: runs `run_json_pipeline(..., create/load/index/optimize=False)` on up to max_records
      and emits predicted NameMaps + DDL + schema diagrams.
    - For tabular inputs: can optionally generate a description CSV (generate_desc=True), then emits NameMap + DDL.
    - data_overrides: optional runtime overrides for data_config (e.g., auto-except knobs).
    """
    from .pipeline import run_json_pipeline, run_tabular_pipeline
    from .quarantine import NullQuarantineWriter
    from .report import RunReport

    cfg = _load_json(config_path)
    data_config = coerce_data_config(cfg.get("data_config") or cfg.get("data") or {})
    if data_overrides:
        for k, v in data_overrides.items():
            if v is None:
                continue
            data_config[str(k)] = v
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
    rd: dict[str, Any] = {}

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
        timings_ms=report.timings_ms,
        artifacts=(rd.get("artifacts") if isinstance(rd, Mapping) else {}) or {},
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
        timings_ms=getattr(report, "timings_ms", None),
        stats=getattr(report, "stats", None),
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
        "timings_ms": dict(report.timings_ms),
        "issues": [it.to_dict() for it in report.issues],
        "auto_except": ((rd.get("artifacts") or {}).get("auto_except") if isinstance(rd, Mapping) else None),
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
    quarantine_path: str | None = None,
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

    # Optional: quarantine overlay counts (best-effort).
    quarantine_counts_by_table: dict[str, int] = {}
    quarantine_total = 0
    quarantine_error: str | None = None
    if quarantine_path:
        sql_by_original: dict[str, str] = {}
        try:
            artifacts = (report or {}).get("artifacts") or {}
            nm_by_table = artifacts.get("name_maps_json") or {}
            if isinstance(nm_by_table, Mapping):
                for _k, nm in nm_by_table.items():
                    if isinstance(nm, Mapping) and nm.get("table_original") and nm.get("table_sql"):
                        sql_by_original[str(nm.get("table_original"))] = str(nm.get("table_sql"))
        except Exception:
            sql_by_original = {}

        known_sql = {ti.name_sql for ti in table_infos}
        try:
            with open(quarantine_path, encoding="utf-8") as f:
                for line in f:
                    raw = line.strip()
                    if not raw:
                        continue
                    try:
                        entry = json.loads(raw)
                    except Exception:
                        continue
                    quarantine_total += 1
                    table = None
                    ctx = entry.get("context") or {}
                    rec = entry.get("record") or {}
                    if isinstance(ctx, Mapping):
                        for k in ("table", "table_name", "table_sql"):
                            v = ctx.get(k)
                            if v:
                                table = str(v)
                                break
                    if table is None and isinstance(rec, Mapping):
                        for k in ("table", "table_name", "table_sql"):
                            v = rec.get(k)
                            if v:
                                table = str(v)
                                break
                    if not table:
                        continue

                    # Normalize to SQL table name where possible.
                    if table in known_sql:
                        table_sql = table
                    elif table in sql_by_original and sql_by_original[table] in known_sql:
                        table_sql = sql_by_original[table]
                    else:
                        # Best-effort: try truncation to 64.
                        table_sql = truncate_table_name(table, max_len=MYSQL_IDENTIFIER_MAX_LEN)
                        if table_sql not in known_sql:
                            continue

                    quarantine_counts_by_table[table_sql] = int(quarantine_counts_by_table.get(table_sql, 0)) + 1
        except Exception as e:
            quarantine_error = str(e)

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
            "quarantine": quarantine_path or "",
            "quarantine_entries": int(quarantine_total) if quarantine_path else "",
            "quarantine_error": quarantine_error or "",
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
        table_badges=({t: {"quarantine": n} for t, n in quarantine_counts_by_table.items()} if quarantine_counts_by_table else None),
        timings_ms=(report.get("timings_ms") if report else None),
        stats=(report.get("stats") if report else None),
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
