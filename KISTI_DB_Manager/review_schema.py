from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from .config import coerce_data_config, coerce_db_config
from .namemap import load_namemap
from .naming import MYSQL_IDENTIFIER_MAX_LEN, truncate_table_name
from .review import (
    DBIntrospector,
    TableInfo,
    _collect_table_infos_from_db_prefix,
    _collect_table_infos_from_report,
    _load_json,
    _mask_db_config,
    _maybe_svg_to_png,
    _merge_db_details,
    _parse_formats,
    _utc_now_iso,
    _write_text,
    build_table_edges,
    render_mermaid,
    render_simple_svg,
)


def _human_int(value: int | None) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{int(value):,}"
    except Exception:
        return str(value)


def _human_bytes(value: int | None) -> str:
    if value is None:
        return "n/a"
    try:
        n = float(int(value))
    except Exception:
        return str(value)
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    idx = 0
    while n >= 1024.0 and idx < len(units) - 1:
        n /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(n)} {units[idx]}"
    return f"{n:.1f} {units[idx]}"


def _qi(name: str) -> str:
    return str(name).replace("`", "``")


def _match_prefix(base_table: str, key_sep: str, name: str) -> str | None:
    candidates = [
        f"{base_table}{key_sep}",
        f"{base_table}-SUB{key_sep}",
        f"{base_table}_SUB{key_sep}",
    ]
    for prefix in candidates:
        if str(name).startswith(prefix):
            return prefix
    return None


def _table_depth(base_table: str, key_sep: str, name: str) -> int:
    name = str(name)
    if name == str(base_table):
        return 0
    prefix = _match_prefix(str(base_table), str(key_sep), name)
    if prefix is None:
        return 0
    suffix = name[len(prefix) :]
    parts = [part for part in suffix.split(str(key_sep)) if part]
    return max(1, len(parts)) if parts else 0


def _table_display_label(base_table: str, key_sep: str, name: str) -> str:
    name = str(name)
    if name == str(base_table):
        return name
    prefix = _match_prefix(str(base_table), str(key_sep), name)
    if prefix is None:
        return name
    suffix = name[len(prefix) :]
    if not suffix:
        return name
    if prefix.startswith(f"{base_table}-SUB") or prefix.startswith(f"{base_table}_SUB"):
        suffix = f"SUB{key_sep}{suffix}"
    return suffix.replace(str(key_sep), "/")


def _infer_role(depth: int, *, is_base: bool) -> str:
    if is_base:
        return "base"
    if depth <= 1:
        return "sub"
    return "nested"


def _collect_predicted_columns_by_sql(report: Mapping[str, Any] | None) -> dict[str, list[dict[str, Any]]]:
    artifacts = (report or {}).get("artifacts") or {}
    result: dict[str, list[dict[str, Any]]] = {}
    name_maps_json = artifacts.get("name_maps_json")
    if isinstance(name_maps_json, Mapping):
        for _table_original, nm_dict in name_maps_json.items():
            nm = load_namemap(nm_dict)
            if nm is None:
                continue
            cols = []
            for col in nm.columns_sql:
                cols.append(
                    {
                        "name": str(col),
                        "data_type": "longtext",
                        "column_type": "LONGTEXT",
                        "is_nullable": "YES",
                        "column_key": "PRI" if str(col) == "id" else "",
                        "extra": "",
                    }
                )
            result[nm.table_sql] = cols
    else:
        nm = load_namemap(artifacts.get("name_map"))
        if nm is not None:
            result[nm.table_sql] = [
                {
                    "name": str(col),
                    "data_type": "longtext",
                    "column_type": "LONGTEXT",
                    "is_nullable": "YES",
                    "column_key": "PRI" if str(col) == "id" else "",
                    "extra": "",
                }
                for col in nm.columns_sql
            ]
    return result


def _apply_predicted_columns(
    table_infos: list[TableInfo],
    predicted_by_sql: Mapping[str, list[dict[str, Any]]],
) -> list[TableInfo]:
    out: list[TableInfo] = []
    for ti in table_infos:
        cols = ti.columns if ti.columns else predicted_by_sql.get(ti.name_sql)
        out.append(
            TableInfo(
                name_sql=ti.name_sql,
                name_original=ti.name_original,
                row_count=ti.row_count,
                row_count_exact=ti.row_count_exact,
                table_rows_estimate=ti.table_rows_estimate,
                data_length=ti.data_length,
                index_length=ti.index_length,
                engine=ti.engine,
                collation=ti.collation,
                columns=cols,
                indexes=ti.indexes,
            )
        )
    return out


def _collect_ddls_by_sql(
    *,
    report: Mapping[str, Any] | None,
    table_infos: Iterable[TableInfo],
) -> dict[str, str]:
    artifacts = (report or {}).get("artifacts") or {}
    ddls: dict[str, str] = {}
    raw = artifacts.get("create_table_sql_json")
    if isinstance(raw, Mapping):
        for key, value in raw.items():
            if isinstance(value, str) and value.strip():
                ddls[str(key)] = value
    single = artifacts.get("create_table_sql")
    if isinstance(single, str) and single.strip():
        for ti in table_infos:
            if ti.name_sql not in ddls:
                ddls[ti.name_sql] = single
                break
    if ddls:
        return ddls

    synthesized: dict[str, str] = {}
    for ti in table_infos:
        if not ti.columns:
            continue
        lines: list[str] = []
        pk_cols: list[str] = []
        for col in ti.columns:
            name = str(col.get("name") or "").strip()
            if not name:
                continue
            column_type = str(col.get("column_type") or col.get("data_type") or "LONGTEXT")
            nullable = str(col.get("is_nullable") or "YES").upper() == "YES"
            extra = str(col.get("extra") or "").strip()
            if str(col.get("column_key") or "").upper() == "PRI":
                pk_cols.append(name)
            part = f"  `{_qi(name)}` {column_type}"
            if not nullable:
                part += " NOT NULL"
            if extra:
                part += f" {extra}"
            lines.append(part)
        if pk_cols:
            cols_sql = ", ".join(f"`{_qi(col)}`" for col in pk_cols)
            lines.append(f"  PRIMARY KEY ({cols_sql})")
        if not lines:
            continue
        synthesized[ti.name_sql] = (
            f"CREATE TABLE `{_qi(ti.name_sql)}` (\n"
            + ",\n".join(lines)
            + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
        )
    return synthesized


def _collect_issue_counts_by_sql(
    *,
    issues: list[dict[str, Any]] | None,
    table_infos: Iterable[TableInfo],
) -> dict[str, dict[str, int]]:
    known_sql = {ti.name_sql for ti in table_infos}
    by_original = {ti.name_original: ti.name_sql for ti in table_infos if ti.name_original}
    counts: dict[str, dict[str, int]] = {}
    for issue in issues or []:
        if not isinstance(issue, Mapping):
            continue
        level = str(issue.get("level") or "").strip().lower()
        if not level:
            continue
        context = issue.get("context") or {}
        table = None
        if isinstance(context, Mapping):
            for key in ("table", "table_name", "table_sql"):
                value = context.get(key)
                if value:
                    table = str(value)
                    break
        if not table:
            continue
        table_sql = table
        if table_sql not in known_sql:
            table_sql = by_original.get(table) or truncate_table_name(table, max_len=MYSQL_IDENTIFIER_MAX_LEN)
        if table_sql not in known_sql:
            continue
        bucket = counts.setdefault(table_sql, {"error": 0, "warning": 0})
        bucket[level] = int(bucket.get(level, 0)) + 1
    return counts


def _collect_quarantine_counts_by_sql(
    *,
    quarantine_path: str | None,
    report: Mapping[str, Any] | None,
    table_infos: Iterable[TableInfo],
) -> tuple[dict[str, int], int, str | None]:
    if not quarantine_path:
        return {}, 0, None
    known_sql = {ti.name_sql for ti in table_infos}
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

    counts: dict[str, int] = {}
    total = 0
    error: str | None = None
    try:
        with open(quarantine_path, encoding="utf-8") as handle:
            for line in handle:
                raw = line.strip()
                if not raw:
                    continue
                try:
                    entry = json.loads(raw)
                except Exception:
                    continue
                total += 1
                context = entry.get("context") or {}
                record = entry.get("record") or {}
                table = None
                if isinstance(context, Mapping):
                    for key in ("table", "table_name", "table_sql"):
                        value = context.get(key)
                        if value:
                            table = str(value)
                            break
                if table is None and isinstance(record, Mapping):
                    for key in ("table", "table_name", "table_sql"):
                        value = record.get(key)
                        if value:
                            table = str(value)
                            break
                if not table:
                    continue
                table_sql = table
                if table_sql not in known_sql:
                    table_sql = sql_by_original.get(table) or truncate_table_name(table, max_len=MYSQL_IDENTIFIER_MAX_LEN)
                if table_sql not in known_sql:
                    continue
                counts[table_sql] = int(counts.get(table_sql, 0)) + 1
    except Exception as exc:
        error = str(exc)
    return counts, total, error


SCHEMA_VIEWER_TEMPLATE = """<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>__TITLE__</title>
  <style>
    :root {
      --bg: #f5f6ef;
      --panel: #ffffff;
      --ink: #172127;
      --muted: #5e6a6f;
      --line: #d6ddd8;
      --accent: #0f766e;
      --accent-soft: #d9f3ef;
      --accent-strong: #115e59;
      --warn: #b45309;
      --warn-soft: #fff1d6;
      --error: #b42318;
      --error-soft: #fde8e7;
      --shadow: 0 18px 50px rgba(23, 33, 39, 0.08);
      --radius: 18px;
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, Liberation Mono, monospace;
      --sans: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif;
    }
    * { box-sizing: border-box; }
    html { scroll-behavior: smooth; }
    body {
      margin: 0;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, #edf9f5 0, transparent 34%),
        radial-gradient(circle at top right, #fff3dd 0, transparent 28%),
        var(--bg);
      font-family: var(--sans);
    }
    a { color: inherit; }
    code, pre { font-family: var(--mono); }
    .hero {
      padding: 36px 28px 20px;
      border-bottom: 1px solid rgba(214, 221, 216, 0.8);
      background: linear-gradient(180deg, rgba(255,255,255,0.92), rgba(255,255,255,0.68));
      backdrop-filter: blur(8px);
      position: sticky;
      top: 0;
      z-index: 20;
    }
    .hero-inner {
      max-width: 1540px;
      margin: 0 auto;
      display: flex;
      gap: 20px;
      justify-content: space-between;
      align-items: flex-end;
      flex-wrap: wrap;
    }
    .eyebrow {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 6px 12px;
      background: rgba(255,255,255,0.86);
      color: var(--muted);
      font-size: 12px;
    }
    h1 { margin: 10px 0 8px; font-size: 34px; line-height: 1.1; }
    .subtitle { color: var(--muted); max-width: 860px; margin: 0; }
    .hero-meta {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
      justify-content: flex-end;
    }
    .chip {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 8px 12px;
      background: rgba(255,255,255,0.88);
      font-size: 12px;
      color: var(--muted);
    }
    .chip code { color: var(--ink); }
    .layout {
      max-width: 1540px;
      margin: 0 auto;
      padding: 24px 28px 44px;
      display: grid;
      grid-template-columns: 320px minmax(0, 1fr);
      gap: 24px;
      align-items: start;
    }
    .sidebar {
      position: sticky;
      top: 146px;
      display: grid;
      gap: 16px;
    }
    .card {
      border: 1px solid rgba(214, 221, 216, 0.9);
      border-radius: var(--radius);
      background: rgba(255,255,255,0.92);
      box-shadow: var(--shadow);
      padding: 18px;
    }
    .nav-links { display: grid; gap: 8px; }
    .nav-links a {
      text-decoration: none;
      color: var(--muted);
      border: 1px solid transparent;
      border-radius: 12px;
      padding: 10px 12px;
      background: rgba(246, 248, 247, 0.8);
      transition: 0.18s ease;
    }
    .nav-links a:hover, .nav-links a.active {
      color: var(--ink);
      border-color: rgba(15, 118, 110, 0.28);
      background: rgba(217, 243, 239, 0.7);
      transform: translateX(2px);
    }
    .toolbar { display: grid; gap: 10px; }
    .toolbar input[type=\"search\"], .toolbar select {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      background: #fff;
      color: var(--ink);
    }
    .toolbar label {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-size: 13px;
      color: var(--muted);
    }
    .table-list { display: grid; gap: 8px; max-height: 52vh; overflow: auto; }
    .table-item {
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #fff;
      padding: 10px 12px;
      cursor: pointer;
      transition: 0.18s ease;
    }
    .table-item:hover { border-color: rgba(15, 118, 110, 0.35); transform: translateY(-1px); }
    .table-item.selected { border-color: var(--accent); background: var(--accent-soft); }
    .table-item-title { font-size: 13px; font-weight: 700; overflow-wrap: anywhere; }
    .table-item-meta { margin-top: 6px; color: var(--muted); font-size: 12px; display: flex; gap: 8px; flex-wrap: wrap; }
    .badge-row { display: flex; gap: 6px; flex-wrap: wrap; margin-top: 8px; }
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 5px;
      border-radius: 999px;
      padding: 4px 8px;
      font-size: 11px;
      font-weight: 700;
      border: 1px solid var(--line);
      background: #f5f7f6;
      color: var(--muted);
    }
    .badge.base { background: #ecfdf5; border-color: #99f6e4; color: var(--accent-strong); }
    .badge.sub { background: #eff6ff; border-color: #bfdbfe; color: #1d4ed8; }
    .badge.nested { background: #faf5ff; border-color: #d8b4fe; color: #7e22ce; }
    .badge.warn { background: var(--warn-soft); border-color: #fed7aa; color: var(--warn); }
    .badge.error { background: var(--error-soft); border-color: #fecaca; color: var(--error); }
    .badge.quarantine { background: #f5f3ff; border-color: #ddd6fe; color: #6d28d9; }
    .main { display: grid; gap: 22px; }
    .section { scroll-margin-top: 120px; }
    .section-head { display: flex; justify-content: space-between; gap: 16px; align-items: flex-end; flex-wrap: wrap; margin-bottom: 12px; }
    .section-head h2 { margin: 0; font-size: 24px; }
    .section-head p { margin: 0; color: var(--muted); }
    .stats-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 14px; }
    .stat-card {
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
      background: linear-gradient(180deg, #fff, #f8fbfa);
    }
    .stat-label { font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.06em; }
    .stat-value { margin-top: 10px; font-size: 28px; font-weight: 800; }
    .stat-note { margin-top: 8px; font-size: 12px; color: var(--muted); }
    .diagram-shell { border: 1px solid var(--line); border-radius: 18px; background: #fff; overflow: hidden; }
    .diagram-toolbar {
      display: flex; justify-content: space-between; gap: 10px; align-items: center; flex-wrap: wrap;
      padding: 12px 14px; border-bottom: 1px solid var(--line); background: #fafcfc;
    }
    .diagram-stage { padding: 14px; max-height: 72vh; overflow: auto; }
    .diagram-stage svg { max-width: 100%; height: auto; }
    .schema-container .node { cursor: pointer; transition: opacity 0.18s ease; }
    .schema-container .node.dim { opacity: 0.16; }
    .schema-container .edge.dim { opacity: 0.08; }
    .schema-container .node.selected .box { stroke: var(--accent); stroke-width: 3; }
    .schema-container .edge.selected { stroke: var(--accent); stroke-width: 2.5; }
    .group-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 14px; }
    .group-card { border: 1px solid var(--line); border-radius: 18px; padding: 16px; background: #fff; }
    .group-card h3 { margin: 0 0 6px; font-size: 18px; }
    .group-count { font-size: 28px; font-weight: 800; margin: 8px 0 12px; }
    .group-list { display: flex; gap: 6px; flex-wrap: wrap; }
    .group-list button {
      border: 1px solid var(--line); background: #f7faf8; border-radius: 999px; padding: 5px 9px; cursor: pointer;
      font-size: 12px; color: var(--muted);
    }
    .catalog { display: grid; gap: 14px; }
    .table-card {
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 18px;
      background: rgba(255,255,255,0.95);
      box-shadow: var(--shadow);
      scroll-margin-top: 132px;
    }
    .table-card.selected { border-color: rgba(15, 118, 110, 0.45); box-shadow: 0 24px 60px rgba(15, 118, 110, 0.12); }
    .table-header { display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; flex-wrap: wrap; }
    .table-title { margin: 0; font-size: 20px; overflow-wrap: anywhere; }
    .table-subtitle { margin: 6px 0 0; color: var(--muted); font-size: 13px; }
    .metric-strip { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 14px; }
    .metric-pill {
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 6px 10px;
      background: #f7faf8;
      color: var(--muted);
      font-size: 12px;
    }
    details.block {
      margin-top: 14px;
      border: 1px solid var(--line);
      border-radius: 16px;
      overflow: hidden;
      background: #fff;
    }
    details.block > summary {
      cursor: pointer;
      list-style: none;
      padding: 12px 14px;
      background: #fafcfc;
      font-weight: 700;
      border-bottom: 1px solid transparent;
    }
    details.block[open] > summary { border-bottom-color: var(--line); }
    .block-body { padding: 14px; }
    table.grid { width: 100%; border-collapse: collapse; font-size: 12px; }
    table.grid th, table.grid td { border: 1px solid var(--line); padding: 8px 9px; vertical-align: top; text-align: left; }
    table.grid th { background: #f6f9f8; }
    pre.code {
      margin: 0;
      border-radius: 16px;
      padding: 14px;
      overflow: auto;
      background: #0f1721;
      color: #e6edf3;
      font-size: 12px;
      line-height: 1.45;
    }
    .muted { color: var(--muted); }
    .empty { color: var(--muted); font-style: italic; }
    @media (max-width: 1220px) {
      .layout { grid-template-columns: 1fr; }
      .sidebar { position: static; }
      .stats-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 720px) {
      .hero { padding: 24px 18px 18px; }
      .layout { padding: 18px; }
      .stats-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header class=\"hero\">
    <div class=\"hero-inner\">
      <div>
        <span class=\"eyebrow\">Schema Viewer</span>
        <h1>__H1__</h1>
        <p class=\"subtitle\">GoldenSet 쪽의 self-contained schema contract viewer 패턴을 그대로 참고해, 요약 카드 · SVG schema · 검색 가능한 테이블 카탈로그를 하나의 HTML로 묶었습니다.</p>
      </div>
      <div class=\"hero-meta\">
        __HERO_CHIPS__
      </div>
    </div>
  </header>

  <div class=\"layout\">
    <aside class=\"sidebar\">
      <div class=\"card\">
        <div class=\"nav-links\">
          <a href=\"#overview\">Overview</a>
          <a href=\"#diagram\">Diagram</a>
          <a href=\"#groups\">Logical Groups</a>
          <a href=\"#catalog\">Table Catalog</a>
        </div>
      </div>

      <div class=\"card\">
        <div class=\"toolbar\">
          <input id=\"table-search\" type=\"search\" placeholder=\"Search table, column, ddl…\" />
          <select id=\"table-sort\">
            <option value=\"depth\">Sort: depth</option>
            <option value=\"rows\">Sort: rows</option>
            <option value=\"cols\">Sort: columns</option>
            <option value=\"size\">Sort: size</option>
            <option value=\"name\">Sort: name</option>
          </select>
          <label><input id=\"only-flagged\" type=\"checkbox\" /> only flagged</label>
          <label><input id=\"only-nested\" type=\"checkbox\" /> nested only</label>
        </div>
      </div>

      <div class=\"card\">
        <div style=\"display:flex;justify-content:space-between;gap:8px;align-items:center;margin-bottom:10px;\">
          <strong>Tables</strong>
          <span id=\"table-count\" class=\"muted\"></span>
        </div>
        <div id=\"table-list\" class=\"table-list\"></div>
      </div>
    </aside>

    <main class=\"main\">
      <section id=\"overview\" class=\"section\">
        <div class=\"section-head\">
          <div>
            <h2>Overview</h2>
            <p>DB introspection 결과와 run report 예측 스키마를 하나의 viewer payload로 합쳤습니다.</p>
          </div>
        </div>
        <div class=\"stats-grid\" id=\"stats-grid\"></div>
      </section>

      <section id=\"diagram\" class=\"section\">
        <div class=\"section-head\">
          <div>
            <h2>Schema Diagram</h2>
            <p>SVG 노드 클릭 시 좌측 목록과 아래 테이블 카드가 동기화됩니다.</p>
          </div>
        </div>
        <div class=\"diagram-shell\">
          <div class=\"diagram-toolbar\">
            <div class=\"muted\">Inline SVG · search and selection synced</div>
            <div class=\"muted\" id=\"diagram-status\"></div>
          </div>
          <div id=\"schema-container\" class=\"diagram-stage schema-container\">__SVG_INLINE__</div>
        </div>
      </section>

      <section id=\"groups\" class=\"section\">
        <div class=\"section-head\">
          <div>
            <h2>Logical Groups</h2>
            <p>Depth별로 묶어서 base/sub/nested 구조를 빠르게 훑을 수 있게 했습니다.</p>
          </div>
        </div>
        <div id=\"group-grid\" class=\"group-grid\"></div>
      </section>

      <section id=\"catalog\" class=\"section\">
        <div class=\"section-head\">
          <div>
            <h2>Table Catalog</h2>
            <p>DDL preview, column catalog, index metadata, sample rows를 한 번에 확인합니다.</p>
          </div>
        </div>
        <div id=\"catalog-grid\" class=\"catalog\"></div>
      </section>
    </main>
  </div>

  <script>
    const PAYLOAD = __PAYLOAD__;
    const tables = Array.isArray(PAYLOAD.tables) ? PAYLOAD.tables.slice() : [];
    const groups = Array.isArray(PAYLOAD.groups) ? PAYLOAD.groups.slice() : [];
    const statsGrid = document.getElementById('stats-grid');
    const groupGrid = document.getElementById('group-grid');
    const tableList = document.getElementById('table-list');
    const catalogGrid = document.getElementById('catalog-grid');
    const tableCount = document.getElementById('table-count');
    const searchInput = document.getElementById('table-search');
    const sortSelect = document.getElementById('table-sort');
    const onlyFlagged = document.getElementById('only-flagged');
    const onlyNested = document.getElementById('only-nested');
    const diagramStatus = document.getElementById('diagram-status');
    const navLinks = Array.from(document.querySelectorAll('.nav-links a'));
    const svgRoot = document.querySelector('#schema-container svg');
    let selectedTableSql = '';

    function escHtml(value) {
      return String(value == null ? '' : value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }

    function formatInt(value) {
      if (value == null || value === '') return 'n/a';
      const n = Number(value);
      if (!Number.isFinite(n)) return String(value);
      return new Intl.NumberFormat('en-US').format(Math.round(n));
    }

    function formatBytes(value) {
      if (value == null || value === '') return 'n/a';
      const n = Number(value);
      if (!Number.isFinite(n)) return String(value);
      const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
      let size = n;
      let idx = 0;
      while (size >= 1024 && idx < units.length - 1) {
        size /= 1024;
        idx += 1;
      }
      if (idx === 0) return formatInt(size) + ' ' + units[idx];
      return size.toFixed(1) + ' ' + units[idx];
    }

    function isFlagged(table) {
      return (table.issue_error_count || 0) > 0 || (table.issue_warning_count || 0) > 0 || (table.quarantine_count || 0) > 0;
    }

    function badgeHtml(table) {
      const items = [];
      items.push('<span class="badge ' + escHtml(table.role) + '">' + escHtml(table.role_label) + '</span>');
      if ((table.issue_error_count || 0) > 0) {
        items.push('<span class="badge error">error ' + formatInt(table.issue_error_count) + '</span>');
      }
      if ((table.issue_warning_count || 0) > 0) {
        items.push('<span class="badge warn">warning ' + formatInt(table.issue_warning_count) + '</span>');
      }
      if ((table.quarantine_count || 0) > 0) {
        items.push('<span class="badge quarantine">quarantine ' + formatInt(table.quarantine_count) + '</span>');
      }
      return items.join('');
    }

    function matchesTable(table, query) {
      if (!query) return true;
      return String(table.search_blob || '').includes(query);
    }

    function currentTables() {
      const query = String(searchInput.value || '').trim().toLowerCase();
      let filtered = tables.filter((table) => matchesTable(table, query));
      if (onlyFlagged.checked) {
        filtered = filtered.filter((table) => isFlagged(table));
      }
      if (onlyNested.checked) {
        filtered = filtered.filter((table) => String(table.role) !== 'base');
      }
      const key = String(sortSelect.value || 'depth');
      filtered.sort((a, b) => {
        if (key === 'rows') {
          return (Number(b.rows_sort || 0) - Number(a.rows_sort || 0)) || String(a.name_sql).localeCompare(String(b.name_sql));
        }
        if (key === 'cols') {
          return (Number(b.column_count || 0) - Number(a.column_count || 0)) || String(a.name_sql).localeCompare(String(b.name_sql));
        }
        if (key === 'size') {
          return (Number(b.size_bytes || 0) - Number(a.size_bytes || 0)) || String(a.name_sql).localeCompare(String(b.name_sql));
        }
        if (key === 'name') {
          return String(a.name_sql).localeCompare(String(b.name_sql));
        }
        return (Number(a.depth || 0) - Number(b.depth || 0)) || String(a.name_sql).localeCompare(String(b.name_sql));
      });
      return filtered;
    }

    function renderStats(filtered) {
      const rows = filtered.reduce((acc, table) => acc + Number(table.rows_sort || 0), 0);
      const cols = filtered.reduce((acc, table) => acc + Number(table.column_count || 0), 0);
      const size = filtered.reduce((acc, table) => acc + Number(table.size_bytes || 0), 0);
      const flagged = filtered.filter((table) => isFlagged(table)).length;
      const cards = [
        { label: 'Tables in view', value: formatInt(filtered.length), note: 'search/filter 적용 결과' },
        { label: 'Rows in view', value: formatInt(rows), note: 'exact 또는 estimate를 합산' },
        { label: 'Columns in view', value: formatInt(cols), note: 'DDL/column catalog 기준' },
        { label: 'Flagged tables', value: formatInt(flagged), note: 'error / warning / quarantine overlay' },
      ];
      statsGrid.innerHTML = cards.map((card) => (
        '<div class="stat-card">' +
          '<div class="stat-label">' + escHtml(card.label) + '</div>' +
          '<div class="stat-value">' + escHtml(card.value) + '</div>' +
          '<div class="stat-note">' + escHtml(card.note) + '</div>' +
        '</div>'
      )).join('');
      diagramStatus.textContent = filtered.length + ' / ' + tables.length + ' tables visible';
    }

    function renderGroups(filtered) {
      const visible = new Set(filtered.map((table) => String(table.name_sql)));
      groupGrid.innerHTML = groups.map((group) => {
        const members = (group.table_sqls || []).filter((name) => visible.has(String(name)));
        const buttons = members.slice(0, 24).map((name) => {
          const table = tables.find((item) => String(item.name_sql) === String(name));
          const label = table ? table.display_short : name;
          return '<button type="button" data-select-table="' + escHtml(name) + '">' + escHtml(label) + '</button>';
        }).join('');
        const extra = members.length > 24 ? ('<span class="muted">+' + formatInt(members.length - 24) + ' more</span>') : '';
        return (
          '<div class="group-card">' +
            '<h3>' + escHtml(group.label) + '</h3>' +
            '<div class="muted">' + escHtml(group.description || '') + '</div>' +
            '<div class="group-count">' + formatInt(members.length) + '</div>' +
            '<div class="group-list">' + buttons + extra + '</div>' +
          '</div>'
        );
      }).join('');
      for (const button of Array.from(groupGrid.querySelectorAll('[data-select-table]'))) {
        button.addEventListener('click', () => selectTable(String(button.getAttribute('data-select-table') || ''), true));
      }
    }

    function renderTableList(filtered) {
      tableCount.textContent = formatInt(filtered.length);
      tableList.innerHTML = filtered.map((table) => (
        '<div class="table-item" data-table-sql="' + escHtml(table.name_sql) + '">' +
          '<div class="table-item-title">' + escHtml(table.display_short) + '</div>' +
          '<div class="table-item-meta">' +
            '<span>rows ' + escHtml(table.rows_label) + '</span>' +
            '<span>cols ' + formatInt(table.column_count) + '</span>' +
            '<span>depth ' + formatInt(table.depth) + '</span>' +
          '</div>' +
          '<div class="badge-row">' + badgeHtml(table) + '</div>' +
        '</div>'
      )).join('') || '<div class="empty">No matching tables.</div>';
      for (const item of Array.from(tableList.querySelectorAll('.table-item'))) {
        item.addEventListener('click', () => selectTable(String(item.getAttribute('data-table-sql') || ''), true));
      }
    }

    function renderColumnsTable(columns) {
      if (!Array.isArray(columns) || !columns.length) return '<div class="empty">No column metadata.</div>';
      const rows = columns.map((col) => (
        '<tr>' +
          '<td><code>' + escHtml(col.name || '') + '</code></td>' +
          '<td><code>' + escHtml(col.column_type || col.data_type || '') + '</code></td>' +
          '<td>' + escHtml(col.is_nullable || '') + '</td>' +
          '<td><code>' + escHtml(col.column_key || '') + '</code></td>' +
          '<td><code>' + escHtml(col.extra || '') + '</code></td>' +
        '</tr>'
      )).join('');
      return '<table class="grid"><thead><tr><th>name</th><th>type</th><th>nullable</th><th>key</th><th>extra</th></tr></thead><tbody>' + rows + '</tbody></table>';
    }

    function renderIndexesTable(indexes) {
      if (!Array.isArray(indexes) || !indexes.length) return '<div class="empty">No index metadata.</div>';
      const rows = indexes.map((ix) => (
        '<tr>' +
          '<td><code>' + escHtml(ix.index_name || '') + '</code></td>' +
          '<td><code>' + escHtml(ix.column_name || '') + '</code></td>' +
          '<td>' + escHtml(ix.seq_in_index || '') + '</td>' +
          '<td>' + escHtml(ix.non_unique || '') + '</td>' +
        '</tr>'
      )).join('');
      return '<table class="grid"><thead><tr><th>index</th><th>column</th><th>seq</th><th>non_unique</th></tr></thead><tbody>' + rows + '</tbody></table>';
    }

    function renderSamples(samples) {
      if (!Array.isArray(samples) || !samples.length) return '<div class="empty">No embedded sample rows.</div>';
      return '<pre class="code">' + escHtml(JSON.stringify(samples, null, 2)) + '</pre>';
    }

    function renderCatalog(filtered) {
      catalogGrid.innerHTML = filtered.map((table) => (
        '<article class="table-card" id="table-' + escHtml(table.name_sql) + '" data-table-sql="' + escHtml(table.name_sql) + '">' +
          '<div class="table-header">' +
            '<div>' +
              '<h3 class="table-title"><code>' + escHtml(table.name_sql) + '</code></h3>' +
              '<div class="table-subtitle">' + escHtml(table.display_full) + (table.name_original && table.name_original !== table.name_sql ? (' · orig ' + escHtml(table.name_original)) : '') + '</div>' +
              '<div class="badge-row">' + badgeHtml(table) + '</div>' +
            '</div>' +
            '<div class="muted">path depth ' + formatInt(table.depth) + '</div>' +
          '</div>' +
          '<div class="metric-strip">' +
            '<span class="metric-pill">rows ' + escHtml(table.rows_label) + '</span>' +
            '<span class="metric-pill">cols ' + formatInt(table.column_count) + '</span>' +
            '<span class="metric-pill">indexes ' + formatInt(table.index_count) + '</span>' +
            '<span class="metric-pill">size ' + escHtml(table.size_label) + '</span>' +
            '<span class="metric-pill">engine ' + escHtml(table.engine || 'n/a') + '</span>' +
          '</div>' +
          '<details class="block" open>' +
            '<summary>DDL preview</summary>' +
            '<div class="block-body"><pre class="code">' + escHtml(table.ddl || '-- no ddl available') + '</pre></div>' +
          '</details>' +
          '<details class="block">' +
            '<summary>Columns (' + formatInt(table.column_count) + ')</summary>' +
            '<div class="block-body">' + renderColumnsTable(table.columns) + '</div>' +
          '</details>' +
          '<details class="block">' +
            '<summary>Indexes (' + formatInt(table.index_count) + ')</summary>' +
            '<div class="block-body">' + renderIndexesTable(table.indexes) + '</div>' +
          '</details>' +
          '<details class="block">' +
            '<summary>Join SQL</summary>' +
            '<div class="block-body"><pre class="code">' + escHtml(table.join_sql || '-- no join hint available') + '</pre></div>' +
          '</details>' +
          '<details class="block">' +
            '<summary>Samples (' + formatInt(table.sample_count) + ')</summary>' +
            '<div class="block-body">' + renderSamples(table.samples) + '</div>' +
          '</details>' +
        '</article>'
      )).join('') || '<div class="empty">No matching tables.</div>';
      for (const card of Array.from(catalogGrid.querySelectorAll('.table-card'))) {
        card.addEventListener('click', (event) => {
          const target = event.target;
          if (target && (target.closest('summary') || target.closest('button') || target.closest('pre'))) return;
          selectTable(String(card.getAttribute('data-table-sql') || ''), false);
        });
      }
    }

    function syncNav() {
      const sections = ['overview', 'diagram', 'groups', 'catalog'].map((id) => document.getElementById(id)).filter(Boolean);
      let active = 'overview';
      for (const section of sections) {
        const rect = section.getBoundingClientRect();
        if (rect.top <= 170) active = section.id;
      }
      for (const link of navLinks) {
        link.classList.toggle('active', String(link.getAttribute('href') || '') === '#' + active);
      }
    }

    function applySvgState(filtered) {
      if (!svgRoot) return;
      const visible = new Set(filtered.map((table) => String(table.name_sql)));
      for (const node of Array.from(svgRoot.querySelectorAll('.node'))) {
        const nameSql = String(node.getAttribute('data-name-sql') || '');
        const dim = visible.size > 0 && !visible.has(nameSql);
        node.classList.toggle('dim', dim);
        node.classList.toggle('selected', nameSql && selectedTableSql === nameSql);
      }
      for (const edge of Array.from(svgRoot.querySelectorAll('.edge'))) {
        const parentSql = String(edge.getAttribute('data-parent-sql') || '');
        const childSql = String(edge.getAttribute('data-child-sql') || '');
        const edgeVisible = visible.has(parentSql) && visible.has(childSql);
        edge.classList.toggle('dim', !edgeVisible);
        edge.classList.toggle('selected', selectedTableSql && (parentSql === selectedTableSql || childSql === selectedTableSql));
      }
    }

    function applyListAndCardSelection() {
      for (const item of Array.from(document.querySelectorAll('.table-item'))) {
        item.classList.toggle('selected', String(item.getAttribute('data-table-sql') || '') === selectedTableSql);
      }
      for (const card of Array.from(document.querySelectorAll('.table-card'))) {
        card.classList.toggle('selected', String(card.getAttribute('data-table-sql') || '') === selectedTableSql);
      }
    }

    function bindSvg() {
      if (!svgRoot) return;
      for (const node of Array.from(svgRoot.querySelectorAll('.node'))) {
        node.addEventListener('click', () => {
          selectTable(String(node.getAttribute('data-name-sql') || ''), true);
        });
      }
    }

    function selectTable(nameSql, scrollIntoView) {
      selectedTableSql = String(nameSql || '');
      applyListAndCardSelection();
      applySvgState(currentTables());
      if (scrollIntoView && selectedTableSql) {
        const card = document.getElementById('table-' + CSS.escape(selectedTableSql));
        if (card) {
          card.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
      }
    }

    function refresh() {
      const filtered = currentTables();
      renderStats(filtered);
      renderGroups(filtered);
      renderTableList(filtered);
      renderCatalog(filtered);
      applySvgState(filtered);
      if (selectedTableSql && !filtered.some((table) => String(table.name_sql) === selectedTableSql)) {
        selectedTableSql = '';
      }
      applyListAndCardSelection();
      syncNav();
    }

    searchInput.addEventListener('input', refresh);
    sortSelect.addEventListener('change', refresh);
    onlyFlagged.addEventListener('change', refresh);
    onlyNested.addEventListener('change', refresh);
    window.addEventListener('scroll', syncNav, { passive: true });
    bindSvg();
    refresh();
  </script>
</body>
</html>
"""


def _render_schema_viewer_html(
    *,
    title: str,
    base_table: str,
    meta: Mapping[str, Any],
    svg_text: str,
    payload: Mapping[str, Any],
) -> str:
    svg_inline = str(svg_text or "")
    if svg_inline.lstrip().startswith("<?xml"):
        svg_inline = svg_inline.split("?>", 1)[-1]
    hero_chip_items: list[str] = []
    for label, value in (
        ("base", base_table),
        ("schema", meta.get("database") or ""),
        ("mode", meta.get("mode") or "schema-viewer"),
        ("generated", meta.get("generated_at") or ""),
    ):
        if not value:
            continue
        hero_chip_items.append(f'<span class="chip">{label}: <code>{str(value)}</code></span>')
    template = SCHEMA_VIEWER_TEMPLATE
    return (
        template.replace("__TITLE__", title)
        .replace("__H1__", base_table)
        .replace("__HERO_CHIPS__", "".join(hero_chip_items))
        .replace("__SVG_INLINE__", svg_inline)
        .replace("__PAYLOAD__", json.dumps(payload, ensure_ascii=False).replace("<", "\\u003c"))
    )


def generate_schema_viewer(
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
    cfg = _load_json(config_path)
    data_config = coerce_data_config(cfg.get("data_config") or cfg.get("data") or {})
    db_config = coerce_db_config(cfg.get("db_config") or cfg.get("db") or {})

    base_table = str(data_config.get("table_name") or "").strip()
    if not base_table:
        raise ValueError("data_config.table_name is required")
    key_sep = str(data_config.get("KEY_SEP", "__"))
    base_table_sql = truncate_table_name(base_table, max_len=MYSQL_IDENTIFIER_MAX_LEN)

    report = _load_json(report_path) if report_path else None
    issues = (report.get("issues") if isinstance(report, Mapping) else None) or None
    predicted_columns = _collect_predicted_columns_by_sql(report if isinstance(report, Mapping) else None)
    fmt = _parse_formats(formats) if formats is not None else {"html", "svg", "mmd"}

    table_infos = _collect_table_infos_from_report(base_table=base_table, report=report) if isinstance(report, Mapping) else []
    if not table_infos:
        table_infos = [TableInfo(name_sql=base_table_sql, name_original=base_table)]

    db_masked = None
    db_error = None
    samples_by_table: dict[str, list[dict[str, Any]]] = {}
    if db_enabled:
        db_masked = _mask_db_config(db_config)
        try:
            db = DBIntrospector(db_config)
            if not table_infos or (len(table_infos) == 1 and table_infos[0].name_sql == base_table_sql and not report_path):
                table_infos = _collect_table_infos_from_db_prefix(db=db, base_table=base_table)
                if not table_infos:
                    table_infos = [TableInfo(name_sql=base_table_sql, name_original=base_table)]
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
        except Exception as exc:
            db_enabled = False
            db_error = str(exc)

    table_infos = _apply_predicted_columns(sorted(table_infos, key=lambda ti: ti.name_sql), predicted_columns)
    ddls_by_sql = _collect_ddls_by_sql(report=report if isinstance(report, Mapping) else None, table_infos=table_infos)
    issue_counts_by_sql = _collect_issue_counts_by_sql(issues=issues if isinstance(issues, list) else None, table_infos=table_infos)
    quarantine_counts_by_sql, quarantine_total, quarantine_error = _collect_quarantine_counts_by_sql(
        quarantine_path=quarantine_path,
        report=report if isinstance(report, Mapping) else None,
        table_infos=table_infos,
    )

    use_original_names = any(ti.name_original for ti in table_infos)
    base_table_graph = base_table if use_original_names else base_table_sql
    mermaid = render_mermaid(base_table=base_table_graph, table_infos=table_infos, key_sep=key_sep)
    svg_text = render_simple_svg(base_table=base_table_graph, table_infos=table_infos, key_sep=key_sep)
    edges = build_table_edges(
        base_table=base_table_graph,
        tables=[ti.name_original or ti.name_sql for ti in table_infos],
        key_sep=key_sep,
    )

    table_payloads: list[dict[str, Any]] = []
    totals = {"rows": 0, "columns": 0, "size_bytes": 0}
    depth_groups: dict[int, list[str]] = {}
    issue_tables = 0
    for ti in table_infos:
        graph_name = ti.name_original or ti.name_sql
        depth = _table_depth(base_table_graph, key_sep, graph_name)
        is_base = str(graph_name) == str(base_table_graph)
        role = _infer_role(depth, is_base=is_base)
        cols = list(ti.columns or [])
        idxs = list(ti.indexes or [])
        rows_sort = ti.row_count if ti.row_count is not None else ti.table_rows_estimate
        size_bytes = int((ti.data_length or 0) + (ti.index_length or 0)) if (ti.data_length is not None or ti.index_length is not None) else 0
        issue_counts = issue_counts_by_sql.get(ti.name_sql) or {}
        quarantine_count = int(quarantine_counts_by_sql.get(ti.name_sql) or 0)
        if issue_counts or quarantine_count:
            issue_tables += 1
        totals["rows"] += int(rows_sort or 0)
        totals["columns"] += len(cols)
        totals["size_bytes"] += int(size_bytes or 0)
        depth_groups.setdefault(depth, []).append(ti.name_sql)
        display_short = _table_display_label(base_table_graph, key_sep, graph_name)
        join_sql = (
            f"SELECT b.*, s.*\n"
            f"FROM `{_qi(base_table_sql)}` b\n"
            f"LEFT JOIN `{_qi(ti.name_sql)}` s ON b.id = s.id\n"
            f"LIMIT 5;"
            if ti.name_sql != base_table_sql
            else f"SELECT *\nFROM `{_qi(base_table_sql)}`\nLIMIT 5;"
        )
        ddl = ddls_by_sql.get(ti.name_sql) or ddls_by_sql.get(graph_name) or ""
        payload = {
            "name_sql": ti.name_sql,
            "name_original": ti.name_original,
            "display_short": display_short,
            "display_full": graph_name,
            "role": role,
            "role_label": "BASE" if role == "base" else ("SUB" if role == "sub" else "NESTED"),
            "depth": depth,
            "rows_sort": int(rows_sort or 0),
            "rows_label": ti.rows_label(),
            "column_count": len(cols),
            "index_count": len(idxs),
            "size_bytes": int(size_bytes or 0),
            "size_label": _human_bytes(size_bytes),
            "engine": ti.engine,
            "collation": ti.collation,
            "columns": cols,
            "indexes": idxs,
            "samples": samples_by_table.get(ti.name_sql) or [],
            "sample_count": len(samples_by_table.get(ti.name_sql) or []),
            "ddl": ddl,
            "join_sql": join_sql,
            "issue_error_count": int(issue_counts.get("error") or 0),
            "issue_warning_count": int(issue_counts.get("warning") or 0),
            "quarantine_count": quarantine_count,
        }
        payload["search_blob"] = " ".join(
            [
                str(payload.get("name_sql") or ""),
                str(payload.get("name_original") or ""),
                str(payload.get("display_short") or ""),
                ddl,
                " ".join(str(col.get("name") or "") for col in cols),
                " ".join(str(ix.get("index_name") or "") for ix in idxs),
            ]
        ).lower()
        table_payloads.append(payload)

    groups = []
    for depth in sorted(depth_groups):
        if depth == 0:
            label = "Depth 0 · Base"
            description = "메인 테이블"
        elif depth == 1:
            label = "Depth 1 · First-level subtables"
            description = "base 바로 아래에서 분기된 첫 번째 subtable"
        else:
            label = f"Depth {depth} · Nested subtables"
            description = f"경로 깊이 {depth} 단계의 nested subtable"
        groups.append({"depth": depth, "label": label, "description": description, "table_sqls": sorted(depth_groups[depth])})

    payload = {
        "meta": {
            "generated_at": _utc_now_iso(),
            "config": config_path,
            "report": report_path or "",
            "database": db_masked.get("database") if db_masked else db_config.get("database"),
            "db_enabled": bool(db_enabled),
            "db_error": db_error,
            "quarantine": quarantine_path or "",
            "quarantine_entries": quarantine_total,
            "quarantine_error": quarantine_error,
            "base_table": base_table,
            "base_table_sql": base_table_sql,
            "key_sep": key_sep,
            "mode": "schema-viewer",
        },
        "summary": {
            "table_count": len(table_payloads),
            "rows_total": int(totals["rows"]),
            "columns_total": int(totals["columns"]),
            "size_bytes_total": int(totals["size_bytes"]),
            "flagged_table_count": int(issue_tables),
            "edge_count": len(edges),
        },
        "tables": table_payloads,
        "groups": groups,
        "edges": [{"parent": parent, "child": child, "label": label} for parent, child, label in edges],
    }

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    mermaid_path = out_path / "schema.mmd"
    svg_path = out_path / "schema.svg"
    html_path = out_path / "schema_viewer.html"
    json_path = out_path / "schema_viewer.json"
    png_path = out_path / "schema.png"

    _write_text(mermaid_path, mermaid)
    _write_text(svg_path, svg_text)
    png_written = False
    if "png" in fmt:
        png_written = _maybe_svg_to_png(svg_text, png_path)

    html_text = _render_schema_viewer_html(
        title=f"Schema Viewer: {base_table}",
        base_table=base_table,
        meta=payload["meta"],
        svg_text=svg_text,
        payload=payload,
    )
    _write_text(html_path, html_text)
    _write_text(json_path, json.dumps(payload, ensure_ascii=False, indent=2))

    return {
        "out_dir": str(out_path),
        "schema_viewer_html": str(html_path),
        "schema_viewer_json": str(json_path),
        "schema_svg": str(svg_path),
        "schema_png": str(png_path) if png_written else None,
        "schema_mmd": str(mermaid_path),
    }
