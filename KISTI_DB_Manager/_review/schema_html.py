from __future__ import annotations

import json
from typing import Any, Mapping


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
    .mini-badges { display: flex; gap: 5px; flex-wrap: wrap; }
    .mini-badge {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 2px 6px;
      font-size: 11px;
      font-weight: 700;
      border: 1px solid var(--line);
      background: #f7faf8;
      color: var(--muted);
      white-space: nowrap;
    }
    .mini-badge.good { background: #ecfdf5; border-color: #99f6e4; color: var(--accent-strong); }
    .mini-badge.warn { background: var(--warn-soft); border-color: #fed7aa; color: var(--warn); }
    .mini-badge.risk { background: var(--error-soft); border-color: #fecaca; color: var(--error); }
    .mini-badge.key { background: #eef2ff; border-color: #c7d2fe; color: #3730a3; }
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
    .overview-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 14px;
      margin-top: 14px;
    }
    .overview-panel {
      border: 1px solid var(--line);
      border-radius: 18px;
      background: #fff;
      padding: 16px;
    }
    .overview-panel h3 { margin: 0 0 10px; font-size: 15px; }
    .overview-list { display: grid; gap: 8px; }
    .overview-row {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: baseline;
      color: var(--muted);
      font-size: 13px;
    }
    .overview-row strong { color: var(--ink); }
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
    .relationship-grid { display: grid; gap: 10px; }
    .relationship-card {
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #f8fbfa;
      padding: 12px;
    }
    .relationship-card.needs-review {
      border-color: #fed7aa;
      background: #fffaf0;
    }
    .relationship-catalog-toolbar {
      display: grid;
      grid-template-columns: minmax(220px, 1fr) repeat(3, minmax(150px, 190px));
      gap: 10px;
      margin-bottom: 12px;
    }
    .relationship-catalog-toolbar input,
    .relationship-catalog-toolbar select {
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      background: #fff;
      color: var(--ink);
      width: 100%;
    }
    .relationship-catalog { display: grid; gap: 12px; }
    .relationship-catalog-card {
      border: 1px solid var(--line);
      border-radius: 18px;
      background: rgba(255,255,255,0.96);
      padding: 14px;
      box-shadow: var(--shadow);
    }
    .relationship-catalog-card.needs-review {
      border-color: #fed7aa;
      background: #fffaf0;
    }
    .relationship-catalog-card.selected {
      border-color: rgba(15, 118, 110, 0.5);
      box-shadow: 0 24px 60px rgba(15, 118, 110, 0.12);
    }
    .relationship-route {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr);
      gap: 10px;
      align-items: center;
      font-size: 13px;
      font-weight: 800;
    }
    .relationship-route code { overflow-wrap: anywhere; }
    .relationship-actions { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 10px; }
    .link-button {
      border: 1px solid var(--line);
      background: #f7faf8;
      border-radius: 999px;
      padding: 5px 9px;
      cursor: pointer;
      font-size: 12px;
      color: var(--muted);
    }
    .link-button:hover {
      border-color: rgba(15, 118, 110, 0.35);
      color: var(--ink);
    }
    .relationship-title {
      display: flex;
      justify-content: space-between;
      gap: 10px;
      align-items: baseline;
      margin-bottom: 8px;
      font-size: 13px;
      font-weight: 800;
    }
    .relationship-title code { overflow-wrap: anywhere; }
    .relationship-label { color: var(--muted); font-size: 12px; }
    .relationship-evidence {
      margin-top: 10px;
      padding-top: 10px;
      border-top: 1px solid var(--line);
      display: grid;
      gap: 8px;
    }
    .relationship-evidence-title {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
      font-size: 12px;
      font-weight: 800;
    }
    .evidence-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 6px;
      font-size: 12px;
      color: var(--muted);
    }
    .relationship-card pre { margin-top: 8px; }
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
      .overview-grid { grid-template-columns: 1fr; }
      .relationship-catalog-toolbar { grid-template-columns: 1fr 1fr; }
    }
    @media (max-width: 720px) {
      .hero { padding: 24px 18px 18px; }
      .layout { padding: 18px; }
      .stats-grid { grid-template-columns: 1fr; }
      .relationship-catalog-toolbar { grid-template-columns: 1fr; }
      .relationship-route { grid-template-columns: 1fr; }
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
          <a href=\"#relationships\">Relationships</a>
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
          <label><input id=\"only-needs-review\" type=\"checkbox\" /> needs review</label>
          <label><input id=\"only-relation-warnings\" type=\"checkbox\" /> relation warnings</label>
          <label><input id=\"only-disconnected\" type=\"checkbox\" /> disconnected</label>
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
        <div class=\"overview-grid\" id=\"relational-overview\"></div>
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

      <section id=\"relationships\" class=\"section\">
        <div class=\"section-head\">
          <div>
            <h2>Relationship Catalog</h2>
            <p>관계 후보를 parent-child, join key, review priority, evidence 기준으로 검토합니다.</p>
          </div>
          <div class=\"muted\" id=\"relationship-count\"></div>
        </div>
        <div class=\"relationship-catalog-toolbar\">
          <input id=\"relationship-search\" type=\"search\" placeholder=\"Search relationships, keys, warnings…\" />
          <select id=\"relationship-priority\">
            <option value=\"all\">Priority: all</option>
            <option value=\"needs_review\">Needs review</option>
            <option value=\"accept_hint\">Accept hints</option>
            <option value=\"review\">Review</option>
            <option value=\"high_risk\">High risk</option>
            <option value=\"structural\">Structural only</option>
          </select>
          <select id=\"relationship-key-source\">
            <option value=\"all\">Key source: all</option>
          </select>
          <select id=\"relationship-sort\">
            <option value=\"priority\">Sort: priority</option>
            <option value=\"confidence\">Sort: confidence</option>
            <option value=\"warnings\">Sort: warnings</option>
            <option value=\"parent\">Sort: parent</option>
          </select>
        </div>
        <div id=\"relationship-catalog\" class=\"relationship-catalog\"></div>
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
    const edges = Array.isArray(PAYLOAD.edges) ? PAYLOAD.edges.slice() : [];
    const groups = Array.isArray(PAYLOAD.groups) ? PAYLOAD.groups.slice() : [];
    const statsGrid = document.getElementById('stats-grid');
    const relationalOverview = document.getElementById('relational-overview');
    const groupGrid = document.getElementById('group-grid');
    const relationshipCatalog = document.getElementById('relationship-catalog');
    const relationshipCount = document.getElementById('relationship-count');
    const relationshipSearch = document.getElementById('relationship-search');
    const relationshipPriority = document.getElementById('relationship-priority');
    const relationshipKeySource = document.getElementById('relationship-key-source');
    const relationshipSort = document.getElementById('relationship-sort');
    const tableList = document.getElementById('table-list');
    const catalogGrid = document.getElementById('catalog-grid');
    const tableCount = document.getElementById('table-count');
    const searchInput = document.getElementById('table-search');
    const sortSelect = document.getElementById('table-sort');
    const onlyFlagged = document.getElementById('only-flagged');
    const onlyNested = document.getElementById('only-nested');
    const onlyNeedsReview = document.getElementById('only-needs-review');
    const onlyRelationWarnings = document.getElementById('only-relation-warnings');
    const onlyDisconnected = document.getElementById('only-disconnected');
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

    function reviewPriorityClass(value) {
      const priority = String(value || '');
      if (priority === 'high_risk') return 'risk';
      if (priority === 'review') return 'warn';
      if (priority === 'accept_hint') return 'good';
      return '';
    }

    function reviewPriorityRank(value) {
      const priority = String(value || '');
      if (priority === 'high_risk') return 0;
      if (priority === 'review') return 1;
      if (priority === 'accept_hint') return 2;
      if (priority === 'structural') return 3;
      return 4;
    }

    function reviewPriorityLabel(value) {
      const priority = String(value || '').replace(/_/g, ' ');
      return priority || 'unreviewed';
    }

    function relationshipDecisionClass(value) {
      const decision = String(value || '');
      if (decision === 'accepted') return 'good';
      if (decision === 'rejected') return 'risk';
      if (decision === 'needs_review' || decision === 'deferred') return 'warn';
      return '';
    }

    function relationshipDecisionLabel(value) {
      const decision = String(value || '').replace(/_/g, ' ');
      return decision || 'undecided';
    }

    function edgePriority(edge) {
      const priority = String(edge.relationship_review_priority || '');
      if (priority) return priority;
      if (Number(edge.relationship_candidate_count || 0) > 0) return 'candidate';
      return 'structural';
    }

    function edgeKeySources(edge) {
      return Array.isArray(edge.relationship_key_match_sources)
        ? edge.relationship_key_match_sources.map((item) => String(item || '').trim()).filter(Boolean)
        : [];
    }

    function edgeNeedsReview(edge) {
      const priority = edgePriority(edge);
      return priority === 'review' || priority === 'high_risk' || Boolean(edge.relationship_needs_review);
    }

    function tableNeedsReview(table) {
      return Number(table.relationship_needs_review_count || 0) > 0;
    }

    function summarizeValues(values) {
      const counts = {};
      for (const raw of values || []) {
        const value = String(raw || '').trim();
        if (!value) continue;
        counts[value] = Number(counts[value] || 0) + 1;
      }
      const entries = Object.keys(counts).sort().map((key) => key + ' ' + formatInt(counts[key]));
      return entries.length ? entries.join(', ') : 'n/a';
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
      if ((table.relationship_candidate_count || 0) > 0) {
        items.push('<span class="badge">candidate ' + formatInt(table.relationship_candidate_count) + '</span>');
      }
      const priorityCounts = table.relationship_review_priority_counts && typeof table.relationship_review_priority_counts === 'object'
        ? table.relationship_review_priority_counts
        : {};
      if (Number(priorityCounts.high_risk || 0) > 0) {
        items.push('<span class="badge error">high risk ' + formatInt(priorityCounts.high_risk) + '</span>');
      }
      if (Number(priorityCounts.review || 0) > 0) {
        items.push('<span class="badge warn">review ' + formatInt(priorityCounts.review) + '</span>');
      }
      if (Number(priorityCounts.accept_hint || 0) > 0 && Number(table.relationship_needs_review_count || 0) === 0) {
        items.push('<span class="badge base">accepted hints ' + formatInt(priorityCounts.accept_hint) + '</span>');
      }
      if (Array.isArray(table.relationship_key_match_sources) && table.relationship_key_match_sources.length) {
        items.push('<span class="badge">key ' + escHtml(table.relationship_key_match_sources.join(', ')) + '</span>');
      }
      const decisionCounts = table.relationship_decision_counts && typeof table.relationship_decision_counts === 'object'
        ? table.relationship_decision_counts
        : {};
      if (Number(table.relationship_decision_count || 0) > 0) {
        const decisionBits = Object.keys(decisionCounts).sort().map((key) => key.replace(/_/g, ' ') + ' ' + formatInt(decisionCounts[key]));
        items.push('<span class="badge">decision ' + escHtml(decisionBits.join(', ')) + '</span>');
      }
      if ((table.relationship_warning_count || 0) > 0) {
        items.push('<span class="badge warn">relation warning ' + formatInt(table.relationship_warning_count) + '</span>');
      }
      if (table.is_disconnected && String(table.role) !== 'base') {
        items.push('<span class="badge warn">disconnected</span>');
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
      if (onlyNeedsReview.checked) {
        filtered = filtered.filter((table) => tableNeedsReview(table));
      }
      if (onlyRelationWarnings.checked) {
        filtered = filtered.filter((table) => Number(table.relationship_warning_count || 0) > 0);
      }
      if (onlyDisconnected.checked) {
        filtered = filtered.filter((table) => Boolean(table.is_disconnected));
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

    function relationshipSearchBlob(edge) {
      const candidates = Array.isArray(edge.relationship_candidates) ? edge.relationship_candidates : [];
      const candidateBits = [];
      for (const candidate of candidates) {
        if (!candidate || typeof candidate !== 'object') continue;
        const evidence = candidate.evidence && typeof candidate.evidence === 'object' ? candidate.evidence : {};
        candidateBits.push(
          candidate.relationship_type,
          candidate.review_priority,
          candidate.confidence_bucket,
          candidate.parent_column_sql,
          candidate.child_column_sql,
          Array.isArray(candidate.warnings) ? candidate.warnings.join(' ') : candidate.warnings,
          evidence.source,
          evidence.key_match_source
        );
      }
      const decisions = Array.isArray(edge.relationship_decisions) ? edge.relationship_decisions : [];
      for (const decision of decisions) {
        if (!decision || typeof decision !== 'object') continue;
        candidateBits.push(
          decision.decision,
          decision.reason,
          decision.reviewed_by,
          decision.reviewed_at,
          decision.parent_column_sql,
          decision.child_column_sql
        );
      }
      return [
        edge.parent_sql,
        edge.child_sql,
        edge.parent_display,
        edge.child_display,
        edge.label,
        edge.relationship_source,
        edge.relationship_status,
        edge.relationship_type,
        edge.relationship_review_priority,
        edge.relationship_decision_status,
        edge.parent_column_sql,
        edge.child_column_sql,
        edge.join_sql,
        edgeKeySources(edge).join(' '),
        candidateBits.join(' '),
      ].map((item) => String(item || '')).join(' ').toLowerCase();
    }

    function currentRelationships(filteredTables) {
      const visible = new Set(filteredTables.map((table) => String(table.name_sql)));
      const query = String(relationshipSearch.value || '').trim().toLowerCase();
      const priority = String(relationshipPriority.value || 'all');
      const keySource = String(relationshipKeySource.value || 'all');
      let filtered = edges.filter((edge) => edgeTouchesVisible(edge, visible));
      if (query) {
        filtered = filtered.filter((edge) => relationshipSearchBlob(edge).includes(query));
      }
      if (priority === 'needs_review') {
        filtered = filtered.filter((edge) => edgeNeedsReview(edge));
      } else if (priority === 'structural') {
        filtered = filtered.filter((edge) => Number(edge.relationship_candidate_count || 0) === 0);
      } else if (priority !== 'all') {
        filtered = filtered.filter((edge) => edgePriority(edge) === priority);
      }
      if (keySource === 'none') {
        filtered = filtered.filter((edge) => edgeKeySources(edge).length === 0);
      } else if (keySource !== 'all') {
        filtered = filtered.filter((edge) => edgeKeySources(edge).includes(keySource));
      }
      const sortKey = String(relationshipSort.value || 'priority');
      filtered.sort((a, b) => {
        if (sortKey === 'confidence') {
          return (Number(b.relationship_confidence_max || 0) - Number(a.relationship_confidence_max || 0))
            || String(a.parent_sql).localeCompare(String(b.parent_sql))
            || String(a.child_sql).localeCompare(String(b.child_sql));
        }
        if (sortKey === 'warnings') {
          return (Number(b.relationship_warning_count || 0) - Number(a.relationship_warning_count || 0))
            || reviewPriorityRank(edgePriority(a)) - reviewPriorityRank(edgePriority(b))
            || String(a.parent_sql).localeCompare(String(b.parent_sql));
        }
        if (sortKey === 'parent') {
          return String(a.parent_sql).localeCompare(String(b.parent_sql))
            || String(a.child_sql).localeCompare(String(b.child_sql));
        }
        return reviewPriorityRank(edgePriority(a)) - reviewPriorityRank(edgePriority(b))
          || Number(b.relationship_warning_count || 0) - Number(a.relationship_warning_count || 0)
          || String(a.parent_sql).localeCompare(String(b.parent_sql))
          || String(a.child_sql).localeCompare(String(b.child_sql));
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
      const datasetProfile = PAYLOAD.dataset_profile && typeof PAYLOAD.dataset_profile === 'object' ? PAYLOAD.dataset_profile : null;
      if (datasetProfile) {
        cards.push({
          label: 'Relation candidates',
          value: formatInt(datasetProfile.relationship_candidate_count || 0),
          note: 'dataset profile overlay'
        });
      }
      statsGrid.innerHTML = cards.map((card) => (
        '<div class="stat-card">' +
          '<div class="stat-label">' + escHtml(card.label) + '</div>' +
          '<div class="stat-value">' + escHtml(card.value) + '</div>' +
          '<div class="stat-note">' + escHtml(card.note) + '</div>' +
        '</div>'
      )).join('');
      renderRelationalOverview(filtered);
      diagramStatus.textContent = filtered.length + ' / ' + tables.length + ' tables visible';
    }

    function renderOverviewPanel(title, rows) {
      return (
        '<div class="overview-panel">' +
          '<h3>' + escHtml(title) + '</h3>' +
          '<div class="overview-list">' +
            rows.map((row) => (
              '<div class="overview-row">' +
                '<span>' + escHtml(row.label) + '</span>' +
                '<strong>' + escHtml(row.value) + '</strong>' +
              '</div>'
            )).join('') +
          '</div>' +
        '</div>'
      );
    }

    function edgeTouchesVisible(edge, visible) {
      return visible.has(String(edge.parent_sql || '')) || visible.has(String(edge.child_sql || ''));
    }

    function renderRelationalOverview(filtered) {
      if (!relationalOverview) return;
      const visible = new Set(filtered.map((table) => String(table.name_sql)));
      const allEdges = Array.isArray(PAYLOAD.edges) ? PAYLOAD.edges : [];
      const visibleEdges = allEdges.filter((edge) => edgeTouchesVisible(edge, visible));
      const roleCounts = filtered.reduce((acc, table) => {
        const role = String(table.role || 'unknown');
        acc[role] = Number(acc[role] || 0) + 1;
        return acc;
      }, {});
      const candidateEdges = visibleEdges.filter((edge) => Number(edge.relationship_candidate_count || 0) > 0);
      const relationWarnings = visibleEdges.reduce((acc, edge) => acc + Number(edge.relationship_warning_count || 0), 0);
      const keySources = [];
      for (const edge of visibleEdges) {
        if (Array.isArray(edge.relationship_key_match_sources)) {
          keySources.push(...edge.relationship_key_match_sources);
        }
      }
      const disconnected = filtered.filter((table) => table.is_disconnected && String(table.role) !== 'base');
      const datasetProfile = PAYLOAD.dataset_profile && typeof PAYLOAD.dataset_profile === 'object' ? PAYLOAD.dataset_profile : null;
      const relationshipDecisions = PAYLOAD.relationship_decisions && typeof PAYLOAD.relationship_decisions === 'object' ? PAYLOAD.relationship_decisions : null;
      const audit = datasetProfile && datasetProfile.audit && typeof datasetProfile.audit === 'object' ? datasetProfile.audit : null;
      const reviewPriorityCounts = audit && audit.review_priority_counts && typeof audit.review_priority_counts === 'object'
        ? audit.review_priority_counts
        : {};
      const needsReview = Number(reviewPriorityCounts.review || 0) + Number(reviewPriorityCounts.high_risk || 0);
      const unmatchedCandidates = datasetProfile
        ? Number((datasetProfile.unmatched_relationship_candidates || []).length || 0)
        : 0;
      const relationshipRows = [
        { label: 'Visible relationships', value: formatInt(visibleEdges.length) },
        { label: 'Candidate-backed', value: formatInt(candidateEdges.length) },
        { label: 'Relation warnings', value: formatInt(relationWarnings) },
      ];
      if (audit) {
        relationshipRows.push({ label: 'Accept hints', value: formatInt(reviewPriorityCounts.accept_hint || 0) });
        relationshipRows.push({ label: 'Needs review', value: formatInt(needsReview) });
        relationshipRows.push({ label: 'Key sources', value: summarizeValues(keySources) });
      }
      if (relationshipDecisions) {
        const decisionCounts = relationshipDecisions.decision_counts && typeof relationshipDecisions.decision_counts === 'object'
          ? relationshipDecisions.decision_counts
          : {};
        const decisionValues = Object.keys(decisionCounts).flatMap((key) => Array(Number(decisionCounts[key] || 0)).fill(key));
        relationshipRows.push({ label: 'Operator decisions', value: formatInt(relationshipDecisions.decision_count || 0) });
        relationshipRows.push({ label: 'Decision status', value: summarizeValues(decisionValues) });
      }
      const coverageRows = [
        { label: 'Disconnected non-base tables', value: formatInt(disconnected.length) },
        { label: 'Unmatched candidates', value: formatInt(unmatchedCandidates) },
        { label: 'Dataset overlay', value: datasetProfile ? 'on' : 'off' },
      ];
      if (audit) {
        coverageRows.push({ label: 'Skipped hints', value: formatInt(audit.skipped_candidate_count || 0) });
      }
      relationalOverview.innerHTML = [
        renderOverviewPanel('Table Roles', [
          { label: 'Base tables', value: formatInt(roleCounts.base || 0) },
          { label: 'Sub tables', value: formatInt(roleCounts.sub || 0) },
          { label: 'Nested tables', value: formatInt(roleCounts.nested || 0) },
        ]),
        renderOverviewPanel('Relationship Evidence', relationshipRows),
        renderOverviewPanel('Coverage Gaps', coverageRows),
      ].join('');
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
      const rows = columns.map((col) => {
        const profile = col.description_profile && typeof col.description_profile === 'object' ? col.description_profile : null;
        const suggested = profile && profile.suggested_type ? String(profile.suggested_type) : '';
        const actualType = String(col.column_type || col.data_type || '');
        const typeHint = suggested && suggested !== actualType
          ? '<div class="muted">suggested <code>' + escHtml(suggested) + '</code></div>'
          : '';
        const warnings = profile && profile.warnings ? String(profile.warnings) : '';
        return (
        '<tr>' +
          '<td><code>' + escHtml(col.name || '') + '</code></td>' +
          '<td><code>' + escHtml(actualType) + '</code>' + typeHint + '</td>' +
          '<td>' + escHtml(col.is_nullable || '') + '</td>' +
          '<td><code>' + escHtml(col.column_key || '') + '</code></td>' +
          '<td>' + renderColumnProfile(profile) + '</td>' +
          '<td>' + (warnings ? '<span class="mini-badge warn">' + escHtml(warnings) + '</span>' : '') + '</td>' +
        '</tr>'
        );
      }).join('');
      return '<table class="grid"><thead><tr><th>name</th><th>type</th><th>nullable</th><th>key</th><th>profile</th><th>warnings</th></tr></thead><tbody>' + rows + '</tbody></table>';
    }

    function ratioLabel(value) {
      if (value === null || value === undefined || value === '') return '';
      const n = Number(value);
      if (!Number.isFinite(n)) return String(value);
      return (n * 100).toFixed(n < 0.01 ? 2 : 1) + '%';
    }

    function renderColumnProfile(profile) {
      if (!profile) return '<span class="muted">n/a</span>';
      const badges = [];
      if (profile.type_confidence !== undefined && profile.type_confidence !== null) {
        badges.push('<span class="mini-badge">type ' + escHtml(ratioLabel(profile.type_confidence)) + '</span>');
      }
      if (profile.null_ratio !== undefined && profile.null_ratio !== null) {
        badges.push('<span class="mini-badge">null ' + escHtml(ratioLabel(profile.null_ratio)) + '</span>');
      }
      if (profile.unique_ratio !== undefined && profile.unique_ratio !== null) {
        badges.push('<span class="mini-badge">uniq ' + escHtml(ratioLabel(profile.unique_ratio)) + '</span>');
      }
      if (profile.index_recommended) {
        badges.push('<span class="mini-badge good">index</span>');
      }
      if (profile.type_reason) {
        badges.push('<span class="mini-badge">' + escHtml(profile.type_reason) + '</span>');
      }
      return '<div class="mini-badges">' + badges.join('') + '</div>';
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

    function renderRelationshipCandidate(candidate) {
      if (!candidate || typeof candidate !== 'object') return '';
      const evidence = candidate.evidence && typeof candidate.evidence === 'object' ? candidate.evidence : {};
      const warnings = Array.isArray(candidate.warnings)
        ? candidate.warnings.filter(Boolean).join(', ')
        : String(candidate.warnings || '');
      const keyMatchSource = evidence.key_match_source ? String(evidence.key_match_source) : '';
      const priority = candidate.review_priority ? String(candidate.review_priority) : '';
      const badges = [
        '<span class="mini-badge good">' + escHtml(candidate.status || 'candidate') + '</span>',
        '<span class="mini-badge">confidence ' + escHtml(ratioLabel(candidate.confidence)) + '</span>',
        '<span class="mini-badge ' + reviewPriorityClass(priority) + '">' + escHtml(reviewPriorityLabel(priority)) + '</span>',
        '<span class="mini-badge">' + escHtml(candidate.relationship_type || 'relationship') + '</span>'
      ];
      if (candidate.confidence_bucket) {
        badges.push('<span class="mini-badge">' + escHtml(candidate.confidence_bucket) + '</span>');
      }
      if (keyMatchSource) {
        badges.push('<span class="mini-badge key">key ' + escHtml(keyMatchSource) + '</span>');
      }
      if (candidate.risk_score !== undefined && candidate.risk_score !== null) {
        badges.push('<span class="mini-badge">risk ' + escHtml(ratioLabel(candidate.risk_score)) + '</span>');
      }
      const evidenceRows = [
        ['source', evidence.source],
        ['key match', evidence.key_match_source],
        ['parent uniq', ratioLabel(evidence.parent_unique_ratio)],
        ['child null', ratioLabel(evidence.child_null_ratio)],
        ['parent col', candidate.parent_column_sql],
        ['child col', candidate.child_column_sql],
      ].filter((item) => item[1] !== undefined && item[1] !== null && String(item[1]) !== '');
      const evidenceHtml = evidenceRows.map((item) => (
        '<div><span class="muted">' + escHtml(item[0]) + '</span> <code>' + escHtml(item[1]) + '</code></div>'
      )).join('');
      return (
        '<div class="relationship-evidence">' +
          '<div class="relationship-evidence-title">' + badges.join('') + '</div>' +
          (evidenceHtml ? '<div class="evidence-grid">' + evidenceHtml + '</div>' : '') +
          (warnings ? '<div><span class="mini-badge warn">' + escHtml(warnings) + '</span></div>' : '') +
        '</div>'
      );
    }

    function renderRelationshipDecision(decision) {
      if (!decision || typeof decision !== 'object') return '';
      const status = String(decision.decision || 'undecided');
      const badges = [
        '<span class="mini-badge ' + relationshipDecisionClass(status) + '">operator ' + escHtml(relationshipDecisionLabel(status)) + '</span>'
      ];
      if (decision.reviewed_by) {
        badges.push('<span class="mini-badge">by ' + escHtml(decision.reviewed_by) + '</span>');
      }
      if (decision.reviewed_at) {
        badges.push('<span class="mini-badge">' + escHtml(decision.reviewed_at) + '</span>');
      }
      const rows = [
        ['parent col', decision.parent_column_sql],
        ['child col', decision.child_column_sql],
        ['reason', decision.reason],
        ['notes', decision.notes],
        ['source', decision.source],
      ].filter((item) => item[1] !== undefined && item[1] !== null && String(item[1]) !== '');
      const rowHtml = rows.map((item) => (
        '<div><span class="muted">' + escHtml(item[0]) + '</span> <code>' + escHtml(item[1]) + '</code></div>'
      )).join('');
      return (
        '<div class="relationship-evidence">' +
          '<div class="relationship-evidence-title">' + badges.join('') + '</div>' +
          (rowHtml ? '<div class="evidence-grid">' + rowHtml + '</div>' : '') +
        '</div>'
      );
    }

    function renderRelationshipEdges(edges, direction) {
      if (!Array.isArray(edges) || !edges.length) return '';
      return edges.map((edge) => {
        const counterpartSql = direction === 'parent' ? edge.parent_sql : edge.child_sql;
        const counterpartDisplay = direction === 'parent' ? edge.parent_display : edge.child_display;
        const prefix = direction === 'parent' ? 'Parent' : 'Child';
        const candidates = Array.isArray(edge.relationship_candidates) ? edge.relationship_candidates : [];
        const candidateHtml = candidates.map(renderRelationshipCandidate).join('');
        const edgePriority = String(edge.relationship_review_priority || '');
        const edgeKeySources = Array.isArray(edge.relationship_key_match_sources) ? edge.relationship_key_match_sources : [];
        const edgeBadges = [
          '<span class="mini-badge">' + escHtml(edge.relationship_source || 'structural_naming') + '</span>',
          '<span class="mini-badge">' + escHtml(edge.relationship_status || 'structural') + '</span>',
          '<span class="mini-badge">' + escHtml(edge.relationship_type || 'relationship') + '</span>'
        ];
        if (Number(edge.relationship_candidate_count || 0) > 0) {
          edgeBadges.push('<span class="mini-badge">candidates ' + formatInt(edge.relationship_candidate_count) + '</span>');
        }
        if (edgePriority) {
          edgeBadges.push('<span class="mini-badge ' + reviewPriorityClass(edgePriority) + '">' + escHtml(reviewPriorityLabel(edgePriority)) + '</span>');
        }
        if (edgeKeySources.length) {
          edgeBadges.push('<span class="mini-badge key">key ' + escHtml(edgeKeySources.join(', ')) + '</span>');
        }
        if (edge.relationship_decision_status) {
          edgeBadges.push('<span class="mini-badge ' + relationshipDecisionClass(edge.relationship_decision_status) + '">operator ' + escHtml(relationshipDecisionLabel(edge.relationship_decision_status)) + '</span>');
        }
        if (edge.parent_column_sql || edge.child_column_sql) {
          edgeBadges.push('<span class="mini-badge">' + escHtml(edge.parent_column_sql || 'id') + ' -> ' + escHtml(edge.child_column_sql || 'id') + '</span>');
        }
        if (Number(edge.relationship_warning_count || 0) > 0) {
          edgeBadges.push('<span class="mini-badge warn">warnings ' + formatInt(edge.relationship_warning_count) + '</span>');
        }
        return (
          '<div class="relationship-card' + (edge.relationship_needs_review ? ' needs-review' : '') + '">' +
            '<div class="relationship-title">' +
              '<span>' + escHtml(prefix) + ': <code>' + escHtml(counterpartSql || counterpartDisplay || '') + '</code></span>' +
              '<span class="relationship-label">' + escHtml(edge.label || '') + '</span>' +
            '</div>' +
            '<div class="mini-badges">' + edgeBadges.join('') + '</div>' +
            '<pre class="code">' + escHtml(edge.join_sql || '') + '</pre>' +
            candidateHtml +
            (Array.isArray(edge.relationship_decisions) && edge.relationship_decisions.length ? edge.relationship_decisions.map(renderRelationshipDecision).join('') : '') +
          '</div>'
        );
      }).join('');
    }

    function renderRelationships(table) {
      const parents = Array.isArray(table.parent_edges) ? table.parent_edges : [];
      const children = Array.isArray(table.child_edges) ? table.child_edges : [];
      if (!parents.length && !children.length) return '<div class="empty">No inferred parent/child edges.</div>';
      const blocks = [];
      if (parents.length) blocks.push(renderRelationshipEdges(parents, 'parent'));
      if (children.length) blocks.push(renderRelationshipEdges(children, 'child'));
      return '<div class="relationship-grid">' + blocks.join('') + '</div>';
    }

    function renderRelationshipCatalog(filteredEdges) {
      relationshipCount.textContent = formatInt(filteredEdges.length) + ' / ' + formatInt(edges.length) + ' relationships';
      relationshipCatalog.innerHTML = filteredEdges.map((edge) => {
        const priority = edgePriority(edge);
        const keySources = edgeKeySources(edge);
        const warningCount = Number(edge.relationship_warning_count || 0);
        const confidence = Number(edge.relationship_confidence_max || 0);
        const badges = [
          '<span class="mini-badge">' + escHtml(edge.relationship_source || 'structural_naming') + '</span>',
          '<span class="mini-badge ' + reviewPriorityClass(priority) + '">' + escHtml(reviewPriorityLabel(priority)) + '</span>',
        ];
        if (Number(edge.relationship_candidate_count || 0) > 0) {
          badges.push('<span class="mini-badge">candidates ' + formatInt(edge.relationship_candidate_count) + '</span>');
        }
        if (keySources.length) {
          badges.push('<span class="mini-badge key">key ' + escHtml(keySources.join(', ')) + '</span>');
        }
        if (edge.relationship_decision_status) {
          badges.push('<span class="mini-badge ' + relationshipDecisionClass(edge.relationship_decision_status) + '">operator ' + escHtml(relationshipDecisionLabel(edge.relationship_decision_status)) + '</span>');
        }
        if (edge.parent_column_sql || edge.child_column_sql) {
          badges.push('<span class="mini-badge">' + escHtml(edge.parent_column_sql || 'id') + ' -> ' + escHtml(edge.child_column_sql || 'id') + '</span>');
        }
        if (confidence > 0) {
          badges.push('<span class="mini-badge">confidence ' + escHtml(ratioLabel(confidence)) + '</span>');
        }
        if (warningCount > 0) {
          badges.push('<span class="mini-badge warn">warnings ' + formatInt(warningCount) + '</span>');
        }
        const candidates = Array.isArray(edge.relationship_candidates) ? edge.relationship_candidates : [];
        const candidatePreview = candidates.length
          ? '<details class="block"><summary>Candidate evidence (' + formatInt(candidates.length) + ')</summary><div class="block-body">' + candidates.map(renderRelationshipCandidate).join('') + '</div></details>'
          : '';
        const decisions = Array.isArray(edge.relationship_decisions) ? edge.relationship_decisions : [];
        const decisionPreview = decisions.length
          ? '<details class="block"><summary>Operator decisions (' + formatInt(decisions.length) + ')</summary><div class="block-body">' + decisions.map(renderRelationshipDecision).join('') + '</div></details>'
          : '';
        return (
          '<article class="relationship-catalog-card' + (edgeNeedsReview(edge) ? ' needs-review' : '') + '" data-parent-sql="' + escHtml(edge.parent_sql || '') + '" data-child-sql="' + escHtml(edge.child_sql || '') + '">' +
            '<div class="relationship-route">' +
              '<code>' + escHtml(edge.parent_sql || edge.parent_display || '') + '</code>' +
              '<span class="muted">-></span>' +
              '<code>' + escHtml(edge.child_sql || edge.child_display || '') + '</code>' +
            '</div>' +
            '<div class="badge-row">' + badges.join('') + '</div>' +
            '<div class="relationship-actions">' +
              '<button type="button" class="link-button" data-select-table="' + escHtml(edge.parent_sql || '') + '">Parent table</button>' +
              '<button type="button" class="link-button" data-select-table="' + escHtml(edge.child_sql || '') + '">Child table</button>' +
              '<span class="muted">' + escHtml(edge.label || '') + '</span>' +
            '</div>' +
            '<details class="block">' +
              '<summary>Join SQL</summary>' +
              '<div class="block-body"><pre class="code">' + escHtml(edge.join_sql || '-- no join hint available') + '</pre></div>' +
            '</details>' +
            candidatePreview +
            decisionPreview +
          '</article>'
        );
      }).join('') || '<div class="empty">No matching relationships.</div>';
      for (const button of Array.from(relationshipCatalog.querySelectorAll('[data-select-table]'))) {
        button.addEventListener('click', () => selectTable(String(button.getAttribute('data-select-table') || ''), true));
      }
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
            '<span class="metric-pill">relations ' + formatInt(table.relationship_count) + '</span>' +
            '<span class="metric-pill">candidates ' + formatInt(table.relationship_candidate_count || 0) + '</span>' +
            '<span class="metric-pill">needs review ' + formatInt(table.relationship_needs_review_count || 0) + '</span>' +
            '<span class="metric-pill">size ' + escHtml(table.size_label) + '</span>' +
            '<span class="metric-pill">engine ' + escHtml(table.engine || 'n/a') + '</span>' +
          '</div>' +
          '<details class="block" open>' +
            '<summary>Relationships (' + formatInt(table.relationship_count) + ')</summary>' +
            '<div class="block-body">' + renderRelationships(table) + '</div>' +
          '</details>' +
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
      const sections = ['overview', 'diagram', 'groups', 'relationships', 'catalog'].map((id) => document.getElementById(id)).filter(Boolean);
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
      for (const card of Array.from(document.querySelectorAll('.relationship-catalog-card'))) {
        const parentSql = String(card.getAttribute('data-parent-sql') || '');
        const childSql = String(card.getAttribute('data-child-sql') || '');
        card.classList.toggle('selected', Boolean(selectedTableSql) && (parentSql === selectedTableSql || childSql === selectedTableSql));
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

    function initRelationshipKeySourceOptions() {
      const keySources = Array.from(new Set(edges.flatMap((edge) => edgeKeySources(edge)))).sort();
      const options = ['<option value="all">Key source: all</option>'];
      for (const source of keySources) {
        options.push('<option value="' + escHtml(source) + '">' + escHtml(source) + '</option>');
      }
      options.push('<option value="none">No key source</option>');
      relationshipKeySource.innerHTML = options.join('');
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
      const filteredRelationships = currentRelationships(filtered);
      renderStats(filtered);
      renderGroups(filtered);
      renderTableList(filtered);
      renderRelationshipCatalog(filteredRelationships);
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
    onlyNeedsReview.addEventListener('change', refresh);
    onlyRelationWarnings.addEventListener('change', refresh);
    onlyDisconnected.addEventListener('change', refresh);
    relationshipSearch.addEventListener('input', refresh);
    relationshipPriority.addEventListener('change', refresh);
    relationshipKeySource.addEventListener('change', refresh);
    relationshipSort.addEventListener('change', refresh);
    window.addEventListener('scroll', syncNav, { passive: true });
    bindSvg();
    initRelationshipKeySourceOptions();
    refresh();
  </script>
</body>
</html>
"""

def render_schema_viewer_html(
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
    return (
        SCHEMA_VIEWER_TEMPLATE
        .replace("__TITLE__", title)
        .replace("__H1__", base_table)
        .replace("__HERO_CHIPS__", "".join(hero_chip_items))
        .replace("__SVG_INLINE__", svg_inline)
        .replace("__PAYLOAD__", json.dumps(payload, ensure_ascii=False).replace("<", "\\u003c"))
    )
