from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any, Mapping

from .core import TableInfo
from .schema_render import _sanitize_html_id


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
