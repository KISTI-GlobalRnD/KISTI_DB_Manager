from __future__ import annotations

import html
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from .config import coerce_data_config, join_path
from .naming import MYSQL_IDENTIFIER_MAX_LEN, truncate_table_name


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _is_nullish(v: Any) -> bool:
    if v is None:
        return True
    try:
        if type(v).__name__ == "NAType":
            return True
    except Exception:
        pass
    try:
        return isinstance(v, float) and math.isnan(v)
    except Exception:
        return False


def _summarize_value(v: Any, *, max_len: int = 200) -> str:
    if _is_nullish(v):
        return "null"
    if isinstance(v, (bytes, bytearray, memoryview)):
        try:
            v = bytes(v).decode("utf-8", errors="replace")
        except Exception:
            v = str(v)
    if isinstance(v, str):
        s = v
    else:
        try:
            s = json.dumps(v, ensure_ascii=False)
        except Exception:
            s = str(v)
    if len(s) > max_len:
        s = s[: max_len - 1] + "…"
    return s


def _jsonable(obj: Any, *, max_list: int = 50, max_str: int = 500) -> Any:
    if _is_nullish(obj):
        return None
    if isinstance(obj, (bool, int)):
        return obj
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, str):
        return obj if len(obj) <= max_str else (obj[: max_str - 1] + "…")
    if isinstance(obj, (bytes, bytearray, memoryview)):
        try:
            return _jsonable(bytes(obj).decode("utf-8", errors="replace"), max_list=max_list, max_str=max_str)
        except Exception:
            return str(obj)
    if isinstance(obj, Mapping):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            try:
                ks = str(k)
            except Exception:
                ks = repr(k)
            out[ks] = _jsonable(v, max_list=max_list, max_str=max_str)
        return out
    if isinstance(obj, list):
        items = obj[: max_list]
        return [_jsonable(x, max_list=max_list, max_str=max_str) for x in items] + (["…"] if len(obj) > max_list else [])
    try:
        return str(obj)
    except Exception:
        return repr(obj)


def _normalize_record(obj: Any) -> dict[str, Any]:
    if isinstance(obj, dict):
        return obj
    return {"root": obj}


def _fallback_flatten_json_separate_lists(nested_json: Any, *, except_keys: list[str] | None, sep: str) -> tuple[dict, dict, dict]:
    if except_keys is None:
        except_keys = []

    single_values: dict = {}
    multiple_values: dict = {}
    excepted_values: dict = {}

    def flatten(x: Any, name: str = "") -> None:
        if isinstance(x, Mapping):
            for a, v in x.items():
                a = str(a)
                full_key = f"{name}{a}" if name else a
                if a in except_keys or full_key in except_keys:
                    excepted_values[full_key] = v
                else:
                    flatten(v, full_key + sep)
            return

        if isinstance(x, list):
            # If the list contains dictionaries, flatten each dictionary but keep the list structure.
            if len(x) > 0 and isinstance(x[0], Mapping):
                flat_list = []
                for item in x:
                    if not isinstance(item, Mapping):
                        continue
                    sub_single, sub_multiple, _sub_excepted = _fallback_flatten_json_separate_lists(item, except_keys=except_keys, sep=sep)
                    flat_list.append({**sub_single, **sub_multiple})
                multiple_values[name[: -len(sep)]] = flat_list
            else:
                multiple_values[name[: -len(sep)]] = x
            return

        single_values[name[: -len(sep)]] = x

    flatten(nested_json)
    return single_values, multiple_values, excepted_values


def _fallback_flatten_nested_json_with_list_rows(
    _json: Any,
    *,
    index_key: str,
    index: int,
    except_keys: list[str] | None,
    sep: str,
) -> tuple[dict, dict[str, list[dict]], dict]:
    if except_keys is None:
        except_keys = []

    single, multiples, excepted = _fallback_flatten_json_separate_lists(_json, except_keys=except_keys, sep=sep)
    if index_key not in single or single.get(index_key) in {None, ""}:
        single[index_key] = index
    _id = single.get(index_key)

    sub_rows: dict[str, list[dict]] = {}
    for key, value_list in (multiples or {}).items():
        if not value_list:
            continue

        rows: list[dict] = []
        if isinstance(value_list, list) and isinstance(value_list[0], dict):
            for item in value_list:
                if not isinstance(item, dict):
                    continue
                row: dict = {}
                for col, val in item.items():
                    col = str(col)
                    col2 = col if col == key else f"{key}{sep}{col}"
                    row[col2] = val
                row[index_key] = _id
                rows.append(row)
        else:
            for val in value_list:
                rows.append({index_key: _id, key: val})

        if rows:
            sub_rows[str(key)] = rows

    for key, value in list((excepted or {}).items()):
        if isinstance(value, dict):
            value[index_key] = _id
            excepted[key] = value
        else:
            excepted[key] = {index_key: _id, "value": value}

    return single, sub_rows, excepted


def _iter_preview_records(data_config: Mapping[str, Any], *, max_records: int = 1) -> Iterable[dict[str, Any]]:
    dc = coerce_data_config(data_config)
    path = Path(join_path(dc.get("PATH", ""), dc.get("file_name", "")))
    file_type = str(dc.get("file_type") or "").lower() or path.suffix.lstrip(".").lower()

    if file_type == "zip":
        # Best-effort: if the zip contains XML, parse the first XML member (or a configured inner file).
        import zipfile

        inner = str(dc.get("xml_file_name") or dc.get("inner_file_name") or dc.get("json_file_name") or "").strip()
        with zipfile.ZipFile(path, "r") as zf:
            names = zf.namelist()
            cand = None
            if inner and inner in names:
                cand = inner
            else:
                for ext in (".xml", ".jsonl", ".ndjson", ".json"):
                    for nm in names:
                        if str(nm).lower().endswith(ext):
                            cand = nm
                            break
                    if cand:
                        break
            if not cand:
                raise ValueError("ZIP preview: no .xml/.json/.jsonl member found")
            if str(cand).lower().endswith(".xml"):
                try:
                    import xmltodict
                except Exception as e:
                    raise RuntimeError("XML preview requires xmltodict (dependency missing?)") from e
                raw = zf.read(cand)
                yield _normalize_record(xmltodict.parse(raw))
                return

        # JSON in zip (delegate to iterator with explicit inner file if detected).
        from .pipeline import _iter_json_records

        dc2 = dict(dc)
        if cand:
            dc2["json_file_name"] = cand

        n = 0
        for rec in _iter_json_records(dc2, report=None, max_records=max_records):
            yield _normalize_record(rec)
            n += 1
            if max_records is not None and n >= int(max_records):
                return
        return

    if file_type in {"jsonl", "ndjson", "jsonlines", "json", "gz"}:
        from .pipeline import _iter_json_records

        n = 0
        for rec in _iter_json_records(dc, report=None, max_records=max_records):
            yield _normalize_record(rec)
            n += 1
            if max_records is not None and n >= int(max_records):
                return
        return

    if file_type == "xml" or path.suffix.lower() == ".xml":
        try:
            import xmltodict
        except Exception as e:
            raise RuntimeError("XML preview requires xmltodict (dependency missing?)") from e

        raw = path.read_bytes()
        yield _normalize_record(xmltodict.parse(raw))
        return

    raise ValueError(f"Unsupported preview file_type={file_type!r} (path={str(path)!r})")


@dataclass(frozen=True)
class RawNode:
    path: str
    parent: str
    label: str
    kind: str  # dict | list | value
    branch_type: str  # Value | Dict | List of Dict | List of Value | Value in List of Dict
    dtype: str
    n_children: int
    list_len: int | None
    sample: str | None
    excepted: bool


def _build_raw_structure(
    record: Mapping[str, Any],
    *,
    sep: str,
    except_keys: set[str],
    max_nodes: int = 5000,
    max_list_items: int = 30,
    max_value_len: int = 200,
) -> tuple[list[RawNode], bool]:
    root_path = "root"
    nodes: dict[str, RawNode] = {}
    children: dict[str, set[str]] = {}
    truncated = False

    def is_excepted_path(path: str) -> bool:
        if not except_keys:
            return False
        if path in except_keys:
            return True
        for ek in except_keys:
            if ek and path.startswith(ek + sep):
                return True
        return False

    def add_edge(parent: str, child: str) -> None:
        children.setdefault(parent, set()).add(child)

    def add_node(node: RawNode) -> None:
        nonlocal truncated
        if node.path in nodes:
            return
        if len(nodes) >= max_nodes:
            truncated = True
            return
        nodes[node.path] = node

    def walk(obj: Any, *, path: str, parent: str, parent_list_dict: bool) -> None:
        nonlocal truncated
        if truncated:
            return

        label = path.split(sep)[-1] if path else root_path
        exc = is_excepted_path(path) if path and path != root_path else False

        # Dict-like
        if isinstance(obj, Mapping):
            add_node(
                RawNode(
                    path=path or root_path,
                    parent=parent,
                    label=label,
                    kind="dict",
                    branch_type="Dict",
                    dtype=type(obj).__name__,
                    n_children=0,
                    list_len=None,
                    sample=None,
                    excepted=exc,
                )
            )
            items = list(obj.items())
            try:
                items.sort(key=lambda kv: str(kv[0]))
            except Exception:
                pass
            for k, v in items:
                ks = str(k)
                child_path = ks if not path or path == root_path else f"{path}{sep}{ks}"
                add_edge(path or root_path, child_path)
                walk(v, path=child_path, parent=(path or root_path), parent_list_dict=False)
            return

        # List-like
        if isinstance(obj, list):
            subkind = "list_dict" if any(isinstance(x, Mapping) for x in obj[: max_list_items]) else "list_value"
            branch_type = "List of Dict" if subkind == "list_dict" else "List of Value"
            add_node(
                RawNode(
                    path=path or root_path,
                    parent=parent,
                    label=label,
                    kind="list",
                    branch_type=branch_type,
                    dtype=type(obj).__name__,
                    n_children=0,
                    list_len=len(obj),
                    sample=_summarize_value(obj[:3], max_len=max_value_len) if obj else "[]",
                    excepted=exc,
                )
            )
            if subkind == "list_dict":
                # Union keys across a prefix of items.
                keys: set[str] = set()
                sample_by_key: dict[str, Any] = {}
                for it in obj[: max_list_items]:
                    if not isinstance(it, Mapping):
                        continue
                    for k in it.keys():
                        ks = str(k)
                        if ks not in keys:
                            keys.add(ks)
                            try:
                                sample_by_key[ks] = it.get(k)
                            except Exception:
                                sample_by_key[ks] = None
                for ks in sorted(keys):
                    child_path = ks if not path or path == root_path else f"{path}{sep}{ks}"
                    add_edge(path or root_path, child_path)
                    walk(sample_by_key.get(ks), path=child_path, parent=(path or root_path), parent_list_dict=True)
            return

        # Value leaf
        leaf_type = "Value in List of Dict" if parent_list_dict else "Value"
        add_node(
            RawNode(
                path=path or root_path,
                parent=parent,
                label=label,
                kind="value",
                branch_type=leaf_type,
                dtype=type(obj).__name__,
                n_children=0,
                list_len=None,
                sample=_summarize_value(obj, max_len=max_value_len),
                excepted=exc,
            )
        )

    walk(record, path=root_path, parent="", parent_list_dict=False)

    # Patch in child counts
    for p, kids in children.items():
        n = nodes.get(p)
        if not n:
            continue
        nodes[p] = RawNode(
            path=n.path,
            parent=n.parent,
            label=n.label,
            kind=n.kind,
            branch_type=n.branch_type,
            dtype=n.dtype,
            n_children=len(kids),
            list_len=n.list_len,
            sample=n.sample,
            excepted=n.excepted,
        )

    # Ensure all nodes that appear as children exist (best-effort).
    for p, kids in list(children.items()):
        for c in kids:
            if c in nodes:
                continue
            nodes[c] = RawNode(
                path=c,
                parent=p,
                label=c.split(sep)[-1],
                kind="value",
                branch_type="Value",
                dtype="(unknown)",
                n_children=0,
                list_len=None,
                sample=None,
                excepted=is_excepted_path(c),
            )

    out = sorted(nodes.values(), key=lambda n: (len(n.path.split(sep)), n.path))
    return out, truncated


def _flatten_preview(
    record: Mapping[str, Any],
    *,
    base_table: str,
    key_sep: str,
    index_key: str,
    except_keys: list[str],
    record_index: int,
    max_rows_per_sub: int = 5,
) -> dict[str, Any]:
    try:
        from .processing import flatten_nested_json_with_list_rows  # type: ignore
    except Exception:
        # Allow preview to run even when optional heavy deps are missing in the environment.
        def flatten_nested_json_with_list_rows(*args, **kwargs):
            return _fallback_flatten_nested_json_with_list_rows(*args, **kwargs)

    row, sub_rows, excepted = flatten_nested_json_with_list_rows(
        dict(record),
        index_key=index_key,
        index=record_index,
        except_keys=list(except_keys) if except_keys is not None else None,
        sep=key_sep,
    )

    base_table_sql = truncate_table_name(base_table, max_len=MYSQL_IDENTIFIER_MAX_LEN)
    base_row = _jsonable(row, max_list=50, max_str=500)

    subs_out: list[dict[str, Any]] = []
    for sub_key in sorted((sub_rows or {}).keys()):
        rows = sub_rows.get(sub_key) or []
        cols: set[str] = set()
        for r in rows:
            if isinstance(r, Mapping):
                cols |= {str(k) for k in r.keys()}
        cols_sorted = [index_key] + sorted([c for c in cols if c != index_key])
        table_original = f"{base_table}{key_sep}{sub_key}"
        table_sql = truncate_table_name(table_original, max_len=MYSQL_IDENTIFIER_MAX_LEN)
        subs_out.append(
            {
                "sub_key": str(sub_key),
                "table_original": table_original,
                "table_sql": table_sql,
                "n_rows": len(rows),
                "columns": cols_sorted,
                "sample_rows": [_jsonable(r, max_list=30, max_str=300) for r in rows[:max_rows_per_sub]],
            }
        )

    # Excepted branches (best-effort)
    excepted_out = _jsonable(excepted or {}, max_list=30, max_str=500)

    return {
        "base_table_original": base_table,
        "base_table_sql": base_table_sql,
        "index_key": index_key,
        "base_row": base_row,
        "subtables": subs_out,
        "excepted": excepted_out,
    }


def _compute_diff(
    raw_nodes: list[RawNode],
    *,
    key_sep: str,
    index_key: str,
    flat: Mapping[str, Any],
) -> dict[str, Any]:
    # Raw "mappable" keys: scalar leaves + list-of-value nodes.
    raw_mappable: set[str] = set()
    raw_excepted: set[str] = set()
    for n in raw_nodes:
        if n.path == "root":
            continue
        if n.excepted:
            raw_excepted.add(n.path)
        if n.kind == "value":
            raw_mappable.add(n.path)
        elif n.kind == "list" and n.branch_type == "List of Value":
            raw_mappable.add(n.path)

    flat_keys: set[str] = set()
    base_row = flat.get("base_row") if isinstance(flat, Mapping) else None
    if isinstance(base_row, Mapping):
        flat_keys |= {str(k) for k in base_row.keys()}
    for st in (flat.get("subtables") or []) if isinstance(flat, Mapping) else []:
        if not isinstance(st, Mapping):
            continue
        cols = st.get("columns") or []
        if isinstance(cols, list):
            flat_keys |= {str(c) for c in cols}

    # Exclude injected index_key from strict "extra" reporting (it can be auto-filled).
    flat_keys_for_extra = set(flat_keys)
    flat_keys_for_extra.discard(index_key)

    missing = sorted([k for k in raw_mappable if k not in flat_keys and k not in raw_excepted])
    excepted_missing = sorted([k for k in raw_mappable if k not in flat_keys and k in raw_excepted])
    extra = sorted([k for k in flat_keys_for_extra if k not in raw_mappable])

    return {
        "raw_mappable_keys": sorted(raw_mappable),
        "flat_keys": sorted(flat_keys),
        "missing": missing,
        "missing_excepted": excepted_missing,
        "extra": extra,
        "counts": {
            "raw_nodes": len(raw_nodes),
            "raw_mappable": len(raw_mappable),
            "flat_keys": len(flat_keys),
            "missing": len(missing),
            "missing_excepted": len(excepted_missing),
            "extra": len(extra),
        },
    }


def render_review_preview_html(*, meta: Mapping[str, Any], previews: list[dict[str, Any]]) -> str:
    def h(x: Any) -> str:
        return html.escape(str(x))

    meta_json = json.dumps(meta, ensure_ascii=False).replace("<", "\\u003c")
    previews_json = json.dumps(previews, ensure_ascii=False).replace("<", "\\u003c")

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Raw vs Flatten Preview</title>
  <style>
    body {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; margin: 24px; color: #1f2328; }}
    code {{ background: #f6f8fa; padding: 2px 5px; border-radius: 6px; }}
    .muted {{ color: #57606a; }}
    .card {{ border: 1px solid #d0d7de; border-radius: 12px; padding: 16px; margin: 16px 0; background: #ffffff; }}
    .row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
    @media (max-width: 1200px) {{ .row {{ grid-template-columns: 1fr; }} }}
    .toolbar {{ display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }}
    .toolbar input[type="search"] {{ flex: 1; min-width: 280px; padding: 8px 10px; border: 1px solid #d0d7de; border-radius: 10px; }}
    .toolbar select {{ padding: 8px 10px; border: 1px solid #d0d7de; border-radius: 10px; background: #fff; }}
    .toolbar label {{ display: inline-flex; gap: 6px; align-items: center; }}
    .pill {{ display: inline-flex; gap: 6px; align-items: center; padding: 3px 8px; border: 1px solid #d0d7de; border-radius: 999px; background: #f6f8fa; font-size: 12px; }}

    details {{ border: 1px solid #d0d7de; border-radius: 10px; padding: 8px 10px; background: #fff; margin: 6px 0; }}
    details > summary {{ cursor: pointer; }}
    ul.tree {{ list-style: none; padding-left: 16px; margin: 6px 0; }}
    ul.tree li {{ margin: 3px 0; }}

    .node {{ display: inline-flex; gap: 8px; align-items: center; cursor: pointer; padding: 2px 6px; border-radius: 8px; }}
    .node:hover {{ background: #f6f8fa; }}
    .node.selected {{ outline: 2px solid #0969da; background: #eff6ff; }}
    .node.dim {{ opacity: 0.2; }}
    .node.missing {{ outline: 1px solid #cf222e; }}
    .node.excepted {{ opacity: 0.75; }}

    .badge {{ display: inline-block; padding: 1px 8px; border-radius: 999px; font-size: 12px; border: 1px solid #d0d7de; background: #f6f8fa; }}
    .b-value {{ border-color: #8250df; background: #fbefff; }}
    .b-dict {{ border-color: #0969da; background: #ddf4ff; }}
    .b-listdict {{ border-color: #1a7f37; background: #dafbe1; }}
    .b-listval {{ border-color: #bf8700; background: #fff8c5; }}
    .b-vld {{ border-color: #6e7781; background: #f6f8fa; }}

    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #d0d7de; padding: 6px 8px; font-size: 12px; vertical-align: top; }}
    th {{ background: #f6f8fa; text-align: left; }}
    .flat-key {{ cursor: pointer; padding: 2px 6px; border-radius: 8px; display: inline-block; }}
    .flat-key:hover {{ background: #f6f8fa; }}
    .flat-key.selected {{ outline: 2px solid #0969da; background: #eff6ff; }}
    .flat-key.dim {{ opacity: 0.2; }}
    .flat-key.extra {{ outline: 1px solid #bf8700; }}
    pre {{ background: #0b1020; color: #e6edf3; padding: 10px 12px; border-radius: 12px; overflow: auto; }}
  </style>
</head>
<body>
  <h1>Raw vs Flatten Preview</h1>
  <p class="muted">Interactive sanity-check: raw structure → flattened base/sub tables.</p>

  <div class="card">
    <div class="toolbar">
      <span class="pill">config: <code>{h(meta.get('config') or '')}</code></span>
      <span class="pill">input: <code>{h(meta.get('input') or '')}</code></span>
      <span class="pill">type: <code>{h(meta.get('file_type') or '')}</code></span>
      <span class="pill">sep: <code>{h(meta.get('key_sep') or '')}</code></span>
      <span class="pill">index: <code>{h(meta.get('index_key') or '')}</code></span>
    </div>
    <div style="margin-top: 10px;" class="toolbar">
      <select id="record-select"></select>
      <input id="q" type="search" placeholder="Search path/key…" />
      <label><input id="only-missing" type="checkbox" /> only missing</label>
      <span id="status" class="muted"></span>
    </div>
  </div>

  <div class="row">
    <div class="card">
      <h2>Raw structure</h2>
      <div class="muted" style="margin-bottom: 8px;">
        Legend:
        <span class="badge b-dict">Dict</span>
        <span class="badge b-listdict">List of Dict</span>
        <span class="badge b-listval">List of Value</span>
        <span class="badge b-value">Value</span>
        <span class="badge b-vld">Value in List of Dict</span>
      </div>
      <div id="raw-tree"></div>
    </div>

    <div class="card">
      <h2>Flatten result</h2>
      <div id="flat-view"></div>
    </div>
  </div>

  <div class="card">
    <h2>Diff summary</h2>
    <div id="diff"></div>
  </div>

  <script>
    const META = {meta_json};
    const PREVIEWS = {previews_json};

    const sel = document.getElementById('record-select');
    const q = document.getElementById('q');
    const onlyMissing = document.getElementById('only-missing');
    const status = document.getElementById('status');
    const rawTree = document.getElementById('raw-tree');
    const flatView = document.getElementById('flat-view');
    const diffEl = document.getElementById('diff');

    let currentIndex = 0;
    let flatIndex = new Map(); // key -> array of elements
    let rawIndex = new Map(); // path -> element

    function badgeClass(branchType) {{
      const t = String(branchType || '');
      if (t === 'Dict') return 'badge b-dict';
      if (t === 'List of Dict') return 'badge b-listdict';
      if (t === 'List of Value') return 'badge b-listval';
      if (t === 'Value in List of Dict') return 'badge b-vld';
      return 'badge b-value';
    }}

    function setStatus(text) {{
      if (!status) return;
      status.textContent = String(text || '');
    }}

    function clearSelection() {{
      for (const el of document.querySelectorAll('.node.selected, .flat-key.selected')) {{
        el.classList.remove('selected');
      }}
    }}

    function selectRawPath(path) {{
      clearSelection();
      const el = rawIndex.get(String(path || ''));
      if (el) el.classList.add('selected');
      const p = String(path || '');
      const sep = String(META.key_sep || '__');

      let hits = 0;
      if (flatIndex.has(p)) {{
        for (const it of flatIndex.get(p) || []) {{
          it.classList.add('selected');
          hits += 1;
        }}
      }} else if (p) {{
        // Prefix match for dict nodes / list nodes.
        const pref = p + sep;
        for (const [k, els] of flatIndex.entries()) {{
          if (String(k).startsWith(pref)) {{
            for (const it of els || []) it.classList.add('selected');
            hits += (els || []).length;
          }}
        }}
      }}
      setStatus(hits ? ('matches: ' + hits) : 'no matching flattened keys');
    }}

    function selectFlatKey(key) {{
      clearSelection();
      const k = String(key || '');
      if (flatIndex.has(k)) {{
        for (const it of flatIndex.get(k) || []) it.classList.add('selected');
      }}
      const el = rawIndex.get(k);
      if (el) {{
        el.classList.add('selected');
        el.scrollIntoView({{behavior: 'smooth', block: 'center'}});
      }}
    }}

    function applyQuery() {{
      const query = String(q && q.value ? q.value : '').trim().toLowerCase();
      const only = !!(onlyMissing && onlyMissing.checked);
      let matchRaw = 0;
      let matchFlat = 0;

      for (const el of document.querySelectorAll('[data-path]')) {{
        const p = String(el.getAttribute('data-path') || '').toLowerCase();
        const okQ = !query || p.includes(query);
        const isMissing = el.classList.contains('missing');
        const okM = !only || isMissing;
        el.classList.toggle('dim', (!!query && !okQ) || (only && !isMissing));
        if (okQ && okM) matchRaw += 1;
      }}

      for (const el of document.querySelectorAll('[data-key]')) {{
        const k = String(el.getAttribute('data-key') || '').toLowerCase();
        const okQ = !query || k.includes(query);
        el.classList.toggle('dim', !!query && !okQ);
        if (okQ) matchFlat += 1;
      }}

      const d = PREVIEWS[currentIndex] && PREVIEWS[currentIndex].diff ? PREVIEWS[currentIndex].diff : null;
      if (d && d.counts) {{
        const qLabel = query ? ('q matches raw: ' + matchRaw + ' · flat: ' + matchFlat + ' · ') : '';
        setStatus(qLabel + 'missing: ' + d.counts.missing + ' (+excepted ' + d.counts.missing_excepted + ') · extra: ' + d.counts.extra);
      }}
    }}

    function renderRaw(nodes) {{
      rawIndex = new Map();
      const byParent = new Map();
      for (const n of nodes || []) {{
        const parent = String(n.parent || '');
        if (!byParent.has(parent)) byParent.set(parent, []);
        byParent.get(parent).push(n);
      }}
      for (const [p, arr] of byParent.entries()) {{
        arr.sort((a, b) => String(a.path || '').localeCompare(String(b.path || '')));
      }}

      function renderNode(n) {{
        const kids = byParent.get(String(n.path || '')) || [];
        const badge = '<span class=\"' + badgeClass(n.branch_type) + '\">' + String(n.branch_type || '') + '</span>';
        const meta = [];
        if (n.kind === 'dict') meta.push('<span class=\"muted\">keys=' + (n.n_children || 0) + '</span>');
        if (n.kind === 'list') meta.push('<span class=\"muted\">len=' + (n.list_len == null ? '?' : n.list_len) + '</span>');
        if (n.kind === 'value' && n.sample != null) meta.push('<code>' + String(n.sample || '') + '</code>');
        const classes = ['node'];
        if (n.excepted) classes.push('excepted');
        if (n.status === 'missing') classes.push('missing');
        const head =
          '<span class=\"' + classes.join(' ') + '\" data-path=\"' + String(n.path || '') + '\">' +
            '<code>' + String(n.label || '') + '</code> ' + badge + ' ' + meta.join(' ') +
          '</span>';
        if (!kids.length) {{
          return '<li>' + head + '</li>';
        }}
        const open = String(n.path || '') === 'root';
        return '<li><details ' + (open ? 'open' : '') + '><summary>' + head + '</summary>' +
          '<ul class=\"tree\">' + kids.map(renderNode).join('') + '</ul></details></li>';
      }}

      const root = (nodes || []).find(x => String(x.path || '') === 'root') || (nodes || [])[0];
      if (!root) {{
        rawTree.innerHTML = '<div class=\"muted\">(no nodes)</div>';
        return;
      }}
      const topKids = byParent.get('root') || [];
      const html = '<ul class=\"tree\">' + renderNode(root) + '</ul>';
      rawTree.innerHTML = html;

      for (const el of Array.from(document.querySelectorAll('.node[data-path]'))) {{
        rawIndex.set(String(el.getAttribute('data-path') || ''), el);
        el.addEventListener('click', (ev) => {{
          ev.preventDefault();
          const p = el.getAttribute('data-path') || '';
          selectRawPath(p);
        }});
      }}
    }}

    function renderFlat(flat, diff) {{
      flatIndex = new Map();
      function addKeyEl(key, el) {{
        const k = String(key || '');
        if (!flatIndex.has(k)) flatIndex.set(k, []);
        flatIndex.get(k).push(el);
      }}

      const base = flat && flat.base_row ? flat.base_row : {{}};
      const subtables = flat && flat.subtables ? flat.subtables : [];
      const excepted = flat && flat.excepted ? flat.excepted : {{}};
      const extraSet = new Set((diff && diff.extra) ? diff.extra : []);

      let html = '';
      html += '<h3>Base row</h3>';
      html += '<table><thead><tr><th>key</th><th>value</th></tr></thead><tbody>';
      const keys = Object.keys(base || {{}}).sort();
      for (const k of keys) {{
        const v = base[k];
        html += '<tr><td><span class=\"flat-key' + (extraSet.has(k) ? ' extra' : '') + '\" data-key=\"' + k + '\"><code>' + k + '</code></span></td><td><code>' + String(v) + '</code></td></tr>';
      }}
      html += '</tbody></table>';

      html += '<h3 style=\"margin-top: 14px;\">Sub tables</h3>';
      if (!subtables.length) {{
        html += '<div class=\"muted\">(none)</div>';
      }} else {{
        for (const st of subtables) {{
          const cols = Array.isArray(st.columns) ? st.columns : [];
          html += '<details><summary><code>' + String(st.table_sql || st.table_original || st.sub_key || '') + '</code>' +
            ' <span class=\"muted\">rows=' + String(st.n_rows || 0) + '</span></summary>';
          html += '<div style=\"margin-top: 8px;\">columns:</div><div>';
          html += cols.map(c => '<span class=\"flat-key' + (extraSet.has(String(c)) ? ' extra' : '') + '\" data-key=\"' + String(c) + '\"><code>' + String(c) + '</code></span>').join(' ');
          html += '</div>';
          const sample = Array.isArray(st.sample_rows) ? st.sample_rows : [];
          if (sample.length) {{
            html += '<div style=\"margin-top: 8px;\" class=\"muted\">sample rows (first ' + sample.length + '):</div>';
            html += '<pre>' + JSON.stringify(sample, null, 2) + '</pre>';
          }}
          html += '</details>';
        }}
      }}

      html += '<h3 style=\"margin-top: 14px;\">Excepted</h3>';
      const exKeys = Object.keys(excepted || {{}}).sort();
      if (!exKeys.length) {{
        html += '<div class=\"muted\">(none)</div>';
      }} else {{
        html += '<pre>' + JSON.stringify(excepted, null, 2) + '</pre>';
      }}

      flatView.innerHTML = html;

      for (const el of Array.from(document.querySelectorAll('.flat-key[data-key]'))) {{
        addKeyEl(el.getAttribute('data-key') || '', el);
        el.addEventListener('click', (ev) => {{
          ev.preventDefault();
          const k = el.getAttribute('data-key') || '';
          selectFlatKey(k);
        }});
      }}
    }}

    function renderDiff(diff) {{
      if (!diff) {{
        diffEl.innerHTML = '<div class=\"muted\">(no diff)</div>';
        return;
      }}
      const c = diff.counts || {{}};
      let html = '';
      html += '<div class=\"toolbar\">' +
        '<span class=\"pill\">raw_nodes: <code>' + String(c.raw_nodes || 0) + '</code></span>' +
        '<span class=\"pill\">raw_mappable: <code>' + String(c.raw_mappable || 0) + '</code></span>' +
        '<span class=\"pill\">flat_keys: <code>' + String(c.flat_keys || 0) + '</code></span>' +
        '<span class=\"pill\">missing: <code>' + String(c.missing || 0) + '</code></span>' +
        '<span class=\"pill\">missing_excepted: <code>' + String(c.missing_excepted || 0) + '</code></span>' +
        '<span class=\"pill\">extra: <code>' + String(c.extra || 0) + '</code></span>' +
      '</div>';

      function listBlock(title, items, cls) {{
        const arr = Array.isArray(items) ? items : [];
        if (!arr.length) return '<div class=\"muted\">' + title + ': (none)</div>';
        const lis = arr.slice(0, 200).map(x => '<li><code>' + String(x) + '</code></li>').join('');
        const more = arr.length > 200 ? ('<li class=\"muted\">… +' + (arr.length - 200) + ' more</li>') : '';
        return '<details><summary>' + title + ' (<code>' + arr.length + '</code>)</summary><ul class=\"tree\">' + lis + more + '</ul></details>';
      }}

      html += '<div style=\"margin-top: 10px;\">' +
        listBlock('Missing (unexpected)', diff.missing, 'missing') +
        listBlock('Missing (excepted subtree)', diff.missing_excepted, 'excepted') +
        listBlock('Extra (flatten-only)', diff.extra, 'extra') +
      '</div>';

      diffEl.innerHTML = html;
    }}

    function renderAll(i) {{
      currentIndex = Math.max(0, Math.min(PREVIEWS.length - 1, Number(i || 0)));
      const pv = PREVIEWS[currentIndex] || {{}};
      renderRaw(pv.raw_nodes || []);
      renderFlat(pv.flatten || {{}}, pv.diff || null);
      renderDiff(pv.diff || null);
      applyQuery();
    }}

    function init() {{
      if (!sel) return;
      sel.innerHTML = '';
      for (let i = 0; i < PREVIEWS.length; i++) {{
        const pv = PREVIEWS[i] || {{}};
        const label = pv.label || ('record #' + i);
        const opt = document.createElement('option');
        opt.value = String(i);
        opt.textContent = String(label);
        sel.appendChild(opt);
      }}
      sel.addEventListener('change', () => renderAll(sel.value));
      if (q) q.addEventListener('input', applyQuery);
      if (onlyMissing) onlyMissing.addEventListener('change', applyQuery);
      renderAll(0);
    }}

    init();
  </script>
</body>
</html>
"""


def write_review_preview_report(
    *,
    config_path: str,
    out_dir: str,
    max_records: int = 3,
    max_nodes: int = 5000,
) -> dict[str, str]:
    cfg = _load_json(config_path)
    data_config = coerce_data_config(cfg.get("data_config") or cfg.get("data") or {})

    base_table = str(data_config.get("table_name") or "").strip() or "base"
    file_name = str(data_config.get("file_name") or "")
    file_type = str(data_config.get("file_type") or "").lower()
    key_sep = str(data_config.get("KEY_SEP") or "__")
    index_key = str(data_config.get("index_key") or "id")
    except_keys = list(data_config.get("except_keys") or [])
    except_set = {str(x) for x in except_keys if str(x)}

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    records = list(_iter_preview_records(data_config, max_records=int(max_records)))
    previews: list[dict[str, Any]] = []
    for i, rec in enumerate(records):
        raw_nodes, truncated = _build_raw_structure(
            rec,
            sep=key_sep,
            except_keys=except_set,
            max_nodes=int(max_nodes),
        )
        flat = _flatten_preview(
            rec,
            base_table=base_table,
            key_sep=key_sep,
            index_key=index_key,
            except_keys=except_keys,
            record_index=i,
        )
        diff = _compute_diff(raw_nodes, key_sep=key_sep, index_key=index_key, flat=flat)

        # Mark node status for UI (missing leaf keys only).
        flat_key_set = set(diff.get("flat_keys") or [])
        missing_set = set(diff.get("missing") or [])
        raw_out = []
        for n in raw_nodes:
            status = ""
            if n.kind == "value" or (n.kind == "list" and n.branch_type == "List of Value"):
                if n.path in missing_set:
                    status = "missing"
                elif n.path in flat_key_set:
                    status = "matched"
                elif n.excepted:
                    status = "excepted"
            raw_out.append({**n.__dict__, "status": status})

        label = f"record #{i}"
        try:
            base_row = flat.get("base_row") if isinstance(flat, Mapping) else None
            if isinstance(base_row, Mapping) and index_key in base_row:
                label = f"record #{i} (id={base_row.get(index_key)})"
        except Exception:
            pass

        previews.append(
            {
                "label": label,
                "raw_nodes": raw_out,
                "raw_truncated": bool(truncated),
                "flatten": flat,
                "diff": diff,
            }
        )

    meta = {
        "generated_at": _utc_now_iso(),
        "config": config_path,
        "input": file_name,
        "file_type": file_type,
        "base_table": base_table,
        "base_table_sql": truncate_table_name(base_table, max_len=MYSQL_IDENTIFIER_MAX_LEN),
        "key_sep": key_sep,
        "index_key": index_key,
        "except_keys": except_keys,
        "max_records": int(max_records),
        "max_nodes": int(max_nodes),
    }

    json_path = out_path / "preview.json"
    html_path = out_path / "preview.html"

    _write_text(json_path, json.dumps({"meta": meta, "previews": previews}, ensure_ascii=False, indent=2))
    _write_text(html_path, render_review_preview_html(meta=meta, previews=previews))

    return {
        "out_dir": str(out_path),
        "preview_json": str(json_path),
        "preview_html": str(html_path),
    }
