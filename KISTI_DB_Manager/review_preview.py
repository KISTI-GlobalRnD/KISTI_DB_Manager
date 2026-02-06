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


def _compute_union_view(
    previews: list[dict[str, Any]],
    *,
    key_sep: str,
    except_keys: set[str],
    max_union_nodes: int = 20000,
) -> dict[str, Any]:
    total = len(previews)
    if total <= 0:
        return {"records": 0, "nodes": [], "counts": {"nodes": 0, "exceptions": 0}, "truncated": False}

    def is_excepted_path(path: str) -> bool:
        if not except_keys:
            return False
        if path in except_keys:
            return True
        for ek in except_keys:
            if ek and path.startswith(ek + key_sep):
                return True
        return False

    def parent_of(path: str) -> str:
        if path == "root":
            return ""
        if key_sep in path:
            return path.rsplit(key_sep, 1)[0]
        return "root"

    stats_by_path: dict[str, dict[str, Any]] = {}
    any_truncated = False

    def bump(d: dict[str, int], k: str) -> None:
        try:
            d[k] = int(d.get(k, 0)) + 1
        except Exception:
            d[k] = 1

    for rec_i, pv in enumerate(previews):
        any_truncated = any_truncated or bool(pv.get("raw_truncated"))
        raw_nodes = pv.get("raw_nodes") or []
        if not isinstance(raw_nodes, list):
            continue
        for n in raw_nodes:
            if not isinstance(n, Mapping):
                continue
            path = str(n.get("path") or "")
            if not path:
                continue
            st = stats_by_path.get(path)
            if st is None:
                st = {
                    "records": set(),
                    "kind_counts": {},
                    "branch_type_counts": {},
                    "dtype_counts": {},
                    "samples": [],
                    "list_len_min": None,
                    "list_len_max": None,
                    "list_len_sum": 0,
                    "list_len_count": 0,
                }
                stats_by_path[path] = st

            try:
                st["records"].add(int(rec_i))
            except Exception:
                st["records"].add(rec_i)

            kind = str(n.get("kind") or "")
            branch_type = str(n.get("branch_type") or "")
            dtype = str(n.get("dtype") or "")

            if kind:
                bump(st["kind_counts"], kind)
            if branch_type:
                bump(st["branch_type_counts"], branch_type)
            if dtype:
                bump(st["dtype_counts"], dtype)

            sample = n.get("sample")
            if isinstance(sample, str) and sample and len(st["samples"]) < 3 and sample not in st["samples"]:
                st["samples"].append(sample)

            list_len = n.get("list_len")
            try:
                list_len_int = int(list_len) if list_len is not None else None
            except Exception:
                list_len_int = None
            if list_len_int is not None and list_len_int >= 0:
                st["list_len_count"] = int(st["list_len_count"]) + 1
                st["list_len_sum"] = int(st["list_len_sum"]) + int(list_len_int)
                st["list_len_min"] = list_len_int if st["list_len_min"] is None else min(int(st["list_len_min"]), list_len_int)
                st["list_len_max"] = list_len_int if st["list_len_max"] is None else max(int(st["list_len_max"]), list_len_int)

    def mode_of(counter: Mapping[str, int], default: str = "") -> str:
        if not counter:
            return default
        try:
            # Stable tie-break by key
            return sorted(counter.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))[0][0]
        except Exception:
            return next(iter(counter.keys()), default)

    # Build nodes
    union_nodes: list[dict[str, Any]] = []
    for path, st in stats_by_path.items():
        present = len(st.get("records") or [])
        coverage = float(present) / float(total) if total else 0.0
        kind_counts = dict(st.get("kind_counts") or {})
        branch_counts = dict(st.get("branch_type_counts") or {})
        dtype_counts = dict(st.get("dtype_counts") or {})
        kind_mode = mode_of(kind_counts)
        branch_mode = mode_of(branch_counts)
        dtype_mode = mode_of(dtype_counts)

        type_drift = False
        try:
            type_drift = len(kind_counts.keys()) > 1 or len(branch_counts.keys()) > 1 or (kind_mode == "value" and len(dtype_counts.keys()) > 1)
        except Exception:
            type_drift = False

        cov_drift = coverage < 0.999999
        exception = bool(type_drift or cov_drift)

        ll_count = int(st.get("list_len_count") or 0)
        ll_avg = None
        if ll_count > 0:
            try:
                ll_avg = float(st.get("list_len_sum") or 0) / float(ll_count)
            except Exception:
                ll_avg = None

        union_nodes.append(
            {
                "path": path,
                "parent": parent_of(path),
                "label": "root" if path == "root" else path.split(key_sep)[-1],
                "kind": kind_mode,
                "branch_type": branch_mode,
                "dtype": dtype_mode,
                "coverage": coverage,
                "present": int(present),
                "total": int(total),
                "kind_counts": kind_counts,
                "branch_type_counts": branch_counts,
                "dtype_counts": dtype_counts,
                "type_drift": bool(type_drift),
                "cov_drift": bool(cov_drift),
                "exception": bool(exception),
                "excepted": bool(is_excepted_path(path)),
                "samples": list(st.get("samples") or []),
                "list_len": (
                    {
                        "min": st.get("list_len_min"),
                        "max": st.get("list_len_max"),
                        "avg": ll_avg,
                        "count": ll_count,
                    }
                    if ll_count > 0
                    else None
                ),
            }
        )

    # Compute children counts
    children_by_parent: dict[str, set[str]] = {}
    for n in union_nodes:
        p = str(n.get("parent") or "")
        c = str(n.get("path") or "")
        children_by_parent.setdefault(p, set()).add(c)
    for n in union_nodes:
        n["n_children"] = len(children_by_parent.get(str(n.get("path") or ""), set()))
        try:
            if str(n.get("path")) == "root":
                n["depth"] = 0
            else:
                n["depth"] = len(str(n.get("path") or "").split(key_sep))
        except Exception:
            n["depth"] = 0

    union_nodes.sort(key=lambda n: (int(n.get("depth") or 0), str(n.get("path") or "")))

    # Optional truncation
    truncated = False
    if max_union_nodes is not None:
        try:
            max_union_nodes = int(max_union_nodes)
        except Exception:
            max_union_nodes = None
    if max_union_nodes is not None and max_union_nodes > 0 and len(union_nodes) > max_union_nodes:
        truncated = True
        union_nodes = union_nodes[:max_union_nodes]

    exceptions = sum(1 for n in union_nodes if n.get("exception"))

    return {
        "records": int(total),
        "nodes": union_nodes,
        "counts": {"nodes": len(union_nodes), "exceptions": int(exceptions)},
        "truncated": bool(truncated or any_truncated),
    }


def render_review_preview_html(*, meta: Mapping[str, Any], previews: list[dict[str, Any]], union: Mapping[str, Any] | None = None) -> str:
    def h(x: Any) -> str:
        return html.escape(str(x))

    meta_json = json.dumps(meta, ensure_ascii=False).replace("<", "\\u003c")
    previews_json = json.dumps(previews, ensure_ascii=False).replace("<", "\\u003c")
    union_json = json.dumps(union or {}, ensure_ascii=False).replace("<", "\\u003c")

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
    .pill.pill-table {{ border-color: #0969da; background: #ddf4ff; }}

    .schema-wrap {{ position: relative; }}
    .schema-lines {{ position: absolute; inset: 0; width: 100%; height: 100%; pointer-events: none; }}
    .schema-line {{ stroke: #0969da; stroke-opacity: 0.6; stroke-width: 2; fill: none; }}

    .btn {{ padding: 8px 10px; border: 1px solid #d0d7de; border-radius: 10px; background: #fff; cursor: pointer; }}
    .btn:hover {{ background: #f6f8fa; }}

    .schema-grid {{ display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 14px 40px; align-items: start; }}
    .schema-grid > * {{ min-width: 0; }}
    @media (max-width: 1200px) {{ .schema-grid {{ grid-template-columns: 1fr; }} }}
    .schema-box {{ border: 1px solid #d0d7de; border-radius: 12px; padding: 10px 12px; background: #fff; cursor: pointer; }}
    .schema-box:hover {{ background: #f6f8fa; }}
    .schema-box.selected {{ outline: 2px solid #0969da; background: #eff6ff; }}
    .schema-box code {{ white-space: normal; overflow-wrap: anywhere; }}
    .schema-title {{ display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }}
    .schema-meta {{ margin-top: 6px; font-size: 12px; color: #57606a; }}

    #diagram-card {{ display: none; }}
    body.diagram-mode #diagram-card {{ display: block; }}
    body.diagram-mode #union {{ display: none; }}
    body.diagram-mode #detail-row {{ display: none; }}
    body.diagram-mode #diff-card {{ display: none; }}
    body.diagram-mode #q, body.diagram-mode #only-missing, body.diagram-mode #status {{ display: none; }}
    body.diagram-mode .toolbar label {{ display: none; }}
    body.diagram-mode #open-diagram {{ display: none; }}

    .diagram-row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
    @media (max-width: 1200px) {{ .diagram-row {{ grid-template-columns: 1fr; }} }}
    .diagram-pane {{ border: 1px solid #d0d7de; border-radius: 12px; padding: 12px; background: #fff; overflow: auto; max-height: 72vh; }}
    .diagram-pane svg {{ width: 100%; height: auto; }}
    .diag-node {{ cursor: pointer; }}
    .diag-node:hover rect {{ filter: brightness(0.98); }}
    .diag-node.selected rect {{ stroke: #0969da; stroke-width: 3; }}

    details {{ border: 1px solid #d0d7de; border-radius: 10px; padding: 8px 10px; background: #fff; margin: 6px 0; }}
    details.table-split {{ border-color: #0969da; background: #ddf4ff33; }}
    details > summary {{ cursor: pointer; }}
    ul.tree {{ list-style: none; padding-left: 16px; margin: 6px 0; }}
    ul.tree li {{ margin: 3px 0; }}
    li.table-split-leaf {{ border: 1px dashed #0969da; border-radius: 10px; padding: 6px 8px; margin: 6px 0; }}

    .node {{ display: inline-flex; gap: 8px; align-items: center; cursor: pointer; padding: 2px 6px; border-radius: 8px; }}
    .node:hover {{ background: #f6f8fa; }}
    .node.selected {{ outline: 2px solid #0969da; background: #eff6ff; }}
    .node.dim {{ opacity: 0.2; }}
    .node.missing {{ outline: 1px solid #cf222e; }}
    .node.excepted {{ opacity: 0.75; }}
    .node.table-root {{ font-weight: 600; }}

    .u-node {{ display: inline-flex; gap: 8px; align-items: center; cursor: pointer; padding: 2px 6px; border-radius: 8px; }}
    .u-node:hover {{ background: #f6f8fa; }}
    .u-node.selected {{ outline: 2px solid #0969da; background: #eff6ff; }}
    .u-node.u-exc {{ outline: 1px solid #bf8700; }}
    .u-node.u-cov {{ outline: 1px solid #bf8700; }}
    .u-node.u-drift {{ outline: 1px solid #cf222e; }}
    .u-node.u-excepted {{ opacity: 0.75; }}

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
      <button id="open-expanded" class="btn" type="button">Open expanded view</button>
      <button id="open-diagram" class="btn" type="button">Open diagram view</button>
      <input id="q" type="search" placeholder="Search path/key…" />
      <label><input id="only-missing" type="checkbox" /> only missing</label>
      <span id="status" class="muted"></span>
    </div>
  </div>

  <div class="card" id="union">
    <h2>Union structure (across sampled records)</h2>
    <p class="muted">Shows coverage and type drift across the previewed records. Use this to spot “exception” branches.</p>
    <div class="toolbar">
      <input id="union-q" type="search" placeholder="Search union path…" />
      <label><input id="union-only-exc" type="checkbox" checked /> only exceptions</label>
      <span class="muted">coverage ≤</span>
      <input id="union-cov" type="range" min="0" max="100" step="1" value="100" />
      <code id="union-cov-value">100</code>
      <span id="union-status" class="muted"></span>
    </div>
    <div class="row" style="margin-top: 10px;">
      <div>
        <div id="union-tree"></div>
      </div>
      <div>
        <h3>Details</h3>
        <div id="union-detail" class="muted">(click a union node)</div>
      </div>
    </div>
  </div>

  <div class="card" id="diagram-card">
    <h2>Diagram view (overview)</h2>
    <p class="muted">One-glance view: left = raw structure (table split points), right = resulting table schema.</p>
    <div class="muted" id="diagram-status" style="margin: 6px 0 10px 0;"></div>
    <div class="diagram-row">
      <div class="diagram-pane">
        <div class="muted" style="margin-bottom: 6px;">
          Raw structure diagram
          <span class="badge b-dict">Dict</span>
          <span class="badge b-listdict">List of Dict</span>
          <span class="badge b-listval">List of Value</span>
          <span class="badge b-value">Value</span>
          <span class="badge b-vld">Value in List of Dict</span>
        </div>
        <div id="diagram-raw"></div>
      </div>
      <div class="diagram-pane">
        <div class="muted" style="margin-bottom: 6px;">Table schema diagram</div>
        <div id="diagram-schema"></div>
      </div>
    </div>
  </div>

  <div class="row" id="detail-row">
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

  <div class="card" id="diff-card">
    <h2>Diff summary</h2>
    <div id="diff"></div>
  </div>

  <script>
    const META = {meta_json};
    const PREVIEWS = {previews_json};
    const UNION = {union_json};

    const sel = document.getElementById('record-select');
    const q = document.getElementById('q');
    const onlyMissing = document.getElementById('only-missing');
    const status = document.getElementById('status');
    const openExpanded = document.getElementById('open-expanded');
    const openDiagram = document.getElementById('open-diagram');
    const diagramStatus = document.getElementById('diagram-status');
    const diagramRaw = document.getElementById('diagram-raw');
    const diagramSchema = document.getElementById('diagram-schema');
    const rawTree = document.getElementById('raw-tree');
    const flatView = document.getElementById('flat-view');
    const diffEl = document.getElementById('diff');
    const unionQ = document.getElementById('union-q');
    const unionOnlyExc = document.getElementById('union-only-exc');
    const unionCov = document.getElementById('union-cov');
    const unionCovValue = document.getElementById('union-cov-value');
    const unionStatus = document.getElementById('union-status');
    const unionTree = document.getElementById('union-tree');
    const unionDetail = document.getElementById('union-detail');

    let currentIndex = 0;
    let flatIndex = new Map(); // key -> array of elements
    let rawIndex = new Map(); // path -> element
    let unionIndex = new Map(); // path -> element
    let unionLiIndex = new Map(); // path -> <li>
    let unionNodeByPath = new Map();
    let unionParentByPath = new Map();
    let schemaBaseKeys = [];
    let schemaBaseBox = null;
    let schemaSubBoxes = [];
    let subtableIndexByKey = new Map(); // sub_key -> index
    let schemaWrapEl = null;
    let schemaLinesEl = null;
    let isExpandedView = false;
    let isDiagramView = false;
    let diagRawIndex = new Map(); // path -> <g>
    let diagSchemaIndex = new Map(); // sub_key -> <g>

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

    function clearSchemaSelection() {{
      for (const el of document.querySelectorAll('.schema-box.selected')) {{
        el.classList.remove('selected');
      }}
    }}

    function highlightSchemaForPath(path) {{
      clearSchemaSelection();
      const p = String(path || '');
      if (!p) return 0;
      const sep = String(META.key_sep || '__');

      let hits = 0;
      if (schemaBaseBox && Array.isArray(schemaBaseKeys) && schemaBaseKeys.length) {{
        for (const k of schemaBaseKeys) {{
          const ks = String(k || '');
          if (ks === p || ks.startsWith(p + sep)) {{
            schemaBaseBox.classList.add('selected');
            hits += 1;
            break;
          }}
        }}
      }}

      for (const it of schemaSubBoxes || []) {{
        const sk = String(it.sub_key || '');
        if (!sk || !it.el) continue;
        if (sk === p || sk.startsWith(p + sep) || p.startsWith(sk + sep)) {{
          it.el.classList.add('selected');
          hits += 1;
        }}
      }}
      return hits;
    }}

    function drawSchemaLines() {{
      try {{
        const wrap = schemaWrapEl || document.getElementById('schema-wrap');
        const svg = schemaLinesEl || document.getElementById('schema-lines');
        schemaWrapEl = wrap;
        schemaLinesEl = svg;
        if (!wrap || !svg) return;
        svg.innerHTML = '';
        if (!schemaBaseBox || !schemaSubBoxes || !schemaSubBoxes.length) return;

        const subs = (schemaSubBoxes || []).map(x => x && x.el).filter(x => x && x.getBoundingClientRect);
        if (!subs.length) return;

        const wrapRect = wrap.getBoundingClientRect();
        const baseRect = schemaBaseBox.getBoundingClientRect();
        const minSubLeft = Math.min.apply(null, subs.map(el => el.getBoundingClientRect().left));

        // Only draw connectors in 2-column layout (base left, subs right).
        if (!(baseRect.right < (minSubLeft - 10))) return;

        const w = Math.max(1, Math.round(wrapRect.width));
        const h = Math.max(1, Math.round(wrapRect.height));
        svg.setAttribute('viewBox', '0 0 ' + w + ' ' + h);
        svg.setAttribute('width', String(w));
        svg.setAttribute('height', String(h));

        const sx0 = baseRect.right - wrapRect.left;
        const sy = baseRect.top - wrapRect.top + baseRect.height / 2;

        function addPath(d) {{
          const p = document.createElementNS('http://www.w3.org/2000/svg', 'path');
          p.setAttribute('d', d);
          p.setAttribute('class', 'schema-line');
          svg.appendChild(p);
        }}

        for (const el of subs) {{
          const r = el.getBoundingClientRect();
          const ex0 = r.left - wrapRect.left;
          const ey = r.top - wrapRect.top + r.height / 2;
          const dist0 = ex0 - sx0;
          if (!(dist0 > 0)) continue;
          const margin = Math.min(12, Math.max(2, dist0 * 0.15));
          const sx = sx0 + margin;
          const ex = ex0 - margin;
          const dist = ex - sx;
          if (!(dist > 0)) continue;
          const x1 = sx + dist * 0.4;
          const x2 = sx + dist * 0.6;
          const d = 'M ' + sx + ' ' + sy + ' C ' + x1 + ' ' + sy + ', ' + x2 + ' ' + ey + ', ' + ex + ' ' + ey;
          addPath(d);
        }}
      }} catch (e) {{
        return;
      }}
    }}

    function applyExpandedView() {{
      // Keep union as-is (it can be large); focus on raw + subtables.
      for (const d of document.querySelectorAll('#raw-tree details')) d.open = true;
      for (const d of document.querySelectorAll('#flat-view details')) d.open = true;
      for (const d of document.querySelectorAll('#diff details')) d.open = true;
    }}

    function openExpandedView() {{
      try {{
        const url = new URL(window.location.href);
        url.searchParams.set('view', 'expanded');
        url.searchParams.set('record', String(currentIndex || 0));
        window.open(url.toString(), '_blank');
      }} catch (e) {{
        // Fallback: best-effort open same path with query params.
        const rec = String(currentIndex || 0);
        window.open(String(window.location.href).split('#')[0] + '?view=expanded&record=' + encodeURIComponent(rec), '_blank');
      }}
    }}

    function openDiagramView() {{
      try {{
        const url = new URL(window.location.href);
        url.searchParams.set('view', 'diagram');
        url.searchParams.set('record', String(currentIndex || 0));
        window.open(url.toString(), '_blank');
      }} catch (e) {{
        const rec = String(currentIndex || 0);
        window.open(String(window.location.href).split('#')[0] + '?view=diagram&record=' + encodeURIComponent(rec), '_blank');
      }}
    }}

    function openExpandedFocus(focus, sameTab) {{
      const f = String(focus || '');
      const useSameTab = !!sameTab;
      try {{
        const url = new URL(window.location.href);
        url.searchParams.set('view', 'expanded');
        url.searchParams.set('record', String(currentIndex || 0));
        if (f) url.searchParams.set('focus', f);
        else url.searchParams.delete('focus');
        if (useSameTab) {{
          window.location.href = url.toString();
          return;
        }}
        const w = window.open(url.toString(), '_blank');
        if (!w) window.location.href = url.toString(); // popup blocked fallback
      }} catch (e) {{
        const rec = encodeURIComponent(String(currentIndex || 0));
        const qs = f ? ('?view=expanded&record=' + rec + '&focus=' + encodeURIComponent(f)) : ('?view=expanded&record=' + rec);
        const target = String(window.location.href).split('#')[0] + qs;
        if (useSameTab) {{
          window.location.href = target;
          return;
        }}
        const w = window.open(target, '_blank');
        if (!w) window.location.href = target;
      }}
    }}

    function clearUnionSelection() {{
      for (const el of document.querySelectorAll('.u-node.selected')) {{
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
      highlightSchemaForPath(p);
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
      highlightSchemaForPath(k);
    }}

    function openRawAncestors(path) {{
      const p = String(path || '');
      if (!p) return;
      const sep = String(META.key_sep || '__');
      const parts = p.split(sep).filter(x => String(x || '').length);
      let pref = '';
      for (let i = 0; i < parts.length; i++) {{
        pref = pref ? (pref + sep + parts[i]) : parts[i];
        const el = rawIndex.get(pref);
        if (!el) continue;
        const det = el.closest && el.closest('details');
        if (det) det.open = true;
      }}
    }}

    function focusToPath(pathOrKey) {{
      const p = String(pathOrKey || '');
      if (!p) return;
      openRawAncestors(p);

      if (rawIndex.has(p)) {{
        selectRawPath(p);
        const el = rawIndex.get(p);
        if (el) {{
          try {{ el.scrollIntoView({{behavior: 'smooth', block: 'center'}}); }} catch (e) {{}}
        }}
      }} else if (flatIndex.has(p)) {{
        selectFlatKey(p);
      }} else {{
        selectRawPath(p);
      }}

      if (subtableIndexByKey && subtableIndexByKey.has(p)) {{
        const idx = String(subtableIndexByKey.get(p));
        const det = document.getElementById('st-' + idx);
        if (det && det.tagName && String(det.tagName).toLowerCase() === 'details') {{
          det.open = true;
          try {{ det.scrollIntoView({{behavior: 'smooth', block: 'start'}}); }} catch (e) {{}}
        }}
      }}
    }}

    function renderUnion() {{
      unionIndex = new Map();
      unionLiIndex = new Map();
      unionNodeByPath = new Map();
      unionParentByPath = new Map();

      const nodes = (UNION && UNION.nodes) ? UNION.nodes : [];
      if (!Array.isArray(nodes) || nodes.length === 0) {{
        if (unionTree) unionTree.innerHTML = '<div class=\"muted\">(no union nodes)</div>';
        return;
      }}

      for (const n of nodes) {{
        if (!n || typeof n !== 'object') continue;
        const p = String(n.path || '');
        unionNodeByPath.set(p, n);
        unionParentByPath.set(p, String(n.parent || ''));
      }}

      const byParent = new Map();
      for (const n of nodes) {{
        const parent = String(n.parent || '');
        if (!byParent.has(parent)) byParent.set(parent, []);
        byParent.get(parent).push(n);
      }}
      for (const [p, arr] of byParent.entries()) {{
        arr.sort((a, b) => String(a.path || '').localeCompare(String(b.path || '')));
      }}

      function covLabel(n) {{
        const present = Number(n.present || 0);
        const total = Number(n.total || 0);
        const cov = (total > 0) ? (present / total) : 0;
        const pct = Math.round(cov * 1000) / 10;
        return pct + '% (' + present + '/' + total + ')';
      }}

      function driftLabel(counts) {{
        if (!counts || typeof counts !== 'object') return '';
        const keys = Object.keys(counts);
        if (keys.length <= 1) return '';
        keys.sort((a, b) => String(a).localeCompare(String(b)));
        const parts = [];
        for (const k of keys) {{
          parts.push(k + ':' + String(counts[k] || 0));
        }}
        return parts.join(', ');
      }}

      function renderNode(n) {{
        const kids = byParent.get(String(n.path || '')) || [];
        const b = '<span class=\"' + badgeClass(n.branch_type) + '\">' + String(n.branch_type || '') + '</span>';
        const cov = '<span class=\"pill\">cov <code>' + covLabel(n) + '</code></span>';
        const drift = n.type_drift ? ('<span class=\"pill\" style=\"border-color:#cf222e;background:#ffebe9;\">drift</span>') : '';
        const exc = n.exception ? ' u-exc' : '';
        const driftCls = n.type_drift ? ' u-drift' : '';
        const covCls = (n.cov_drift && !n.type_drift) ? ' u-cov' : '';
        const exptCls = n.excepted ? ' u-excepted' : '';
        const head =
          '<span class=\"u-node' + exc + driftCls + covCls + exptCls + '\" data-path=\"' + String(n.path || '') + '\">' +
            '<code>' + String(n.label || '') + '</code> ' + b + ' ' + cov + ' ' + drift +
          '</span>';

        const liAttr = ' data-path=\"' + String(n.path || '') + '\"';
        if (!kids.length) {{
          return '<li' + liAttr + '>' + head + '</li>';
        }}
        const open = String(n.path || '') === 'root';
        return '<li' + liAttr + '><details ' + (open ? 'open' : '') + '><summary>' + head + '</summary>' +
          '<ul class=\"tree\">' + kids.map(renderNode).join('') + '</ul></details></li>';
      }}

      const root = nodes.find(x => String(x.path || '') === 'root') || nodes[0];
      const html = '<ul class=\"tree\">' + renderNode(root) + '</ul>';
      if (unionTree) unionTree.innerHTML = html;

      for (const li of Array.from(unionTree ? unionTree.querySelectorAll('li[data-path]') : [])) {{
        const p = String(li.getAttribute('data-path') || '');
        unionLiIndex.set(p, li);
      }}
      for (const el of Array.from(unionTree ? unionTree.querySelectorAll('.u-node[data-path]') : [])) {{
        const p = String(el.getAttribute('data-path') || '');
        unionIndex.set(p, el);
        el.addEventListener('click', (ev) => {{
          ev.preventDefault();
          clearUnionSelection();
          el.classList.add('selected');
          const n = unionNodeByPath.get(p);
          if (unionDetail) {{
            const info = n ? n : {{ path: p }};
            const kinds = driftLabel(info.kind_counts);
            const btypes = driftLabel(info.branch_type_counts);
            const dtypes = driftLabel(info.dtype_counts);
            const ll = info.list_len ? JSON.stringify(info.list_len) : '';
            const samples = Array.isArray(info.samples) ? info.samples.join('\\n') : '';
            unionDetail.innerHTML =
              '<div><code>' + String(info.path || '') + '</code></div>' +
              '<div class=\"muted\">coverage: <code>' + covLabel(info) + '</code></div>' +
              (info.exception ? '<div class=\"muted\">exception: <code>true</code></div>' : '') +
              (kinds ? '<div class=\"muted\">kinds: <code>' + kinds + '</code></div>' : '') +
              (btypes ? '<div class=\"muted\">branch types: <code>' + btypes + '</code></div>' : '') +
              (dtypes ? '<div class=\"muted\">dtypes: <code>' + dtypes + '</code></div>' : '') +
              (ll ? '<div class=\"muted\">list_len: <code>' + ll + '</code></div>' : '') +
              (samples ? '<div class=\"muted\">samples:</div><pre>' + samples + '</pre>' : '');
          }}
        }});
      }}

      if (unionCovValue && unionCov) unionCovValue.textContent = String(unionCov.value || '100');
      applyUnionFilter();
    }}

    function applyUnionFilter() {{
      const query = String(unionQ && unionQ.value ? unionQ.value : '').trim().toLowerCase();
      const onlyExc = !!(unionOnlyExc && unionOnlyExc.checked);
      const covMax = unionCov ? Number(unionCov.value || 100) : 100;
      if (unionCovValue) unionCovValue.textContent = String(covMax);

      const visible = new Set();
      let matches = 0;

      for (const [p, n] of unionNodeByPath.entries()) {{
        const path = String(p || '');
        const okQ = !query || path.toLowerCase().includes(query);
        const okExc = !onlyExc || !!n.exception;
        const covPct = Number(n.coverage || 0) * 100.0;
        const okCov = covMax >= 100 || covPct <= covMax || !!n.type_drift;
        if (okQ && okExc && okCov) {{
          visible.add(path);
          matches += 1;
        }}
      }}

      // Ensure context: add ancestors of visible nodes.
      for (const p of Array.from(visible)) {{
        let cur = p;
        let safety = 0;
        while (cur && safety++ < 2000) {{
          visible.add(cur);
          const parent = unionParentByPath.get(cur) || '';
          if (!parent) break;
          cur = parent;
        }}
      }}
      visible.add('root');

      let shown = 0;
      for (const [p, li] of unionLiIndex.entries()) {{
        const show = visible.has(p);
        li.style.display = show ? '' : 'none';
        if (show) shown += 1;
      }}

      if (unionStatus) {{
        const total = (UNION && UNION.counts) ? Number(UNION.counts.nodes || 0) : unionLiIndex.size;
        const exc = (UNION && UNION.counts) ? Number(UNION.counts.exceptions || 0) : 0;
        unionStatus.textContent = 'shown: ' + shown + ' / ' + total + ' · matches: ' + matches + ' · exceptions: ' + exc + (UNION && UNION.truncated ? ' · truncated' : '');
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

    function renderRaw(nodes, flat) {{
      rawIndex = new Map();
      const split = new Map();
      try {{
        const subtables = (flat && flat.subtables) ? flat.subtables : [];
        if (Array.isArray(subtables)) {{
          for (const st of subtables) {{
            if (!st || typeof st !== 'object') continue;
            const sk = String(st.sub_key || '');
            if (!sk) continue;
            const tn = String(st.table_sql || st.table_original || st.sub_key || '');
            split.set(sk, tn);
          }}
        }}
      }} catch (e) {{}}

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
        const p = String(n.path || '');
        const splitTable = split.get(p) || '';
        const isSplit = !!splitTable;
        const splitPill = isSplit
          ? (' <span class=\"pill pill-table\" title=\"subtable: ' + String(splitTable).replace(/\"/g, '&quot;') + '\">TABLE</span>')
          : '';
        const meta = [];
        if (n.kind === 'dict') meta.push('<span class=\"muted\">keys=' + (n.n_children || 0) + '</span>');
        if (n.kind === 'list') meta.push('<span class=\"muted\">len=' + (n.list_len == null ? '?' : n.list_len) + '</span>');
        if (n.kind === 'value' && n.sample != null) meta.push('<code>' + String(n.sample || '') + '</code>');
        const classes = ['node'];
        if (n.excepted) classes.push('excepted');
        if (n.status === 'missing') classes.push('missing');
        if (isSplit) classes.push('table-root');
        const head =
          '<span class=\"' + classes.join(' ') + '\" data-path=\"' + String(n.path || '') + '\">' +
            '<code>' + String(n.label || '') + '</code> ' + badge + splitPill + ' ' + meta.join(' ') +
          '</span>';
        if (!kids.length) {{
          return '<li' + (isSplit ? ' class=\"table-split-leaf\"' : '') + '>' + head + '</li>';
        }}
        const open = String(n.path || '') === 'root';
        const dCls = isSplit ? ' class=\"table-split\"' : '';
        return '<li><details' + dCls + ' ' + (open ? 'open' : '') + '><summary>' + head + '</summary>' +
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
      schemaBaseKeys = [];
      schemaBaseBox = null;
      schemaSubBoxes = [];
      subtableIndexByKey = new Map();

      function addKeyEl(key, el) {{
        const k = String(key || '');
        if (!flatIndex.has(k)) flatIndex.set(k, []);
        flatIndex.get(k).push(el);
      }}

      const base = flat && flat.base_row ? flat.base_row : {{}};
      const subtables = flat && flat.subtables ? flat.subtables : [];
      const excepted = flat && flat.excepted ? flat.excepted : {{}};
      const extraSet = new Set((diff && diff.extra) ? diff.extra : []);
      const baseTableSql = String((flat && (flat.base_table_sql || flat.base_table_original)) || '');
      const baseTableOrig = String((flat && flat.base_table_original) || '');
      const indexKey = String((flat && flat.index_key) || (META && META.index_key) || '');

      let html = '';
      html += '<h3>Schema (tables)</h3>';
      html += '<div class=\"schema-wrap\" id=\"schema-wrap\"><svg class=\"schema-lines\" id=\"schema-lines\"></svg>';
      html += '<div class=\"schema-grid\">';
      html += '<div class=\"schema-box\" data-schema-type=\"base\" title=\"' + String(baseTableOrig || baseTableSql).replace(/\"/g, '&quot;') + '\">' +
        '<div class=\"schema-title\"><span class=\"pill\">BASE</span> <code>' + String(baseTableSql || '(base)').replace(/</g, '&lt;') + '</code></div>' +
        (baseTableOrig && baseTableOrig !== baseTableSql ? ('<div class=\"schema-meta\">orig: <code>' + String(baseTableOrig).replace(/</g, '&lt;') + '</code></div>') : '') +
        '<div class=\"schema-meta\">join key: <code>' + String(indexKey).replace(/</g, '&lt;') + '</code> · cols: <code>' + String(Object.keys(base || {{}}).length) + '</code></div>' +
      '</div>';

      html += '<div>';
      if (!subtables.length) {{
        html += '<div class=\"muted\">(no subtables)</div>';
      }} else {{
        for (let i = 0; i < subtables.length; i++) {{
          const st = subtables[i] || {{}};
          const cols = Array.isArray(st.columns) ? st.columns : [];
          const subKey = String(st.sub_key || '');
          const tSql = String(st.table_sql || st.table_original || st.sub_key || '');
          const tOrig = String(st.table_original || '');
          const nRows = String(st.n_rows || 0);
          html += '<div class=\"schema-box\" data-schema-type=\"sub\" data-subkey=\"' + subKey.replace(/\"/g, '&quot;') + '\" data-st-index=\"' + String(i) + '\" title=\"' + subKey.replace(/\"/g, '&quot;') + '\">' +
            '<div class=\"schema-title\"><span class=\"pill\">SUB</span> <code>' + tSql.replace(/</g, '&lt;') + '</code></div>' +
            (tOrig && tOrig !== tSql ? ('<div class=\"schema-meta\">orig: <code>' + tOrig.replace(/</g, '&lt;') + '</code></div>') : '') +
            (subKey ? ('<div class=\"schema-meta\">sub_key: <code>' + subKey.replace(/</g, '&lt;') + '</code></div>') : '') +
            '<div class=\"schema-meta\">join: <code>' + String(indexKey).replace(/</g, '&lt;') + '</code> · rows: <code>' + nRows + '</code> · cols: <code>' + String(cols.length) + '</code></div>' +
          '</div>';
        }}
      }}
      html += '</div>';
      html += '</div>';
      html += '</div>';

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
        for (let i = 0; i < subtables.length; i++) {{
          const st = subtables[i];
          const cols = Array.isArray(st.columns) ? st.columns : [];
          const subKey = String(st.sub_key || '');
          if (subKey) subtableIndexByKey.set(subKey, i);
          html += '<details id=\"st-' + String(i) + '\"><summary><code>' + String(st.table_sql || st.table_original || st.sub_key || '') + '</code>' +
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

      schemaBaseKeys = keys;
      schemaBaseBox = document.querySelector('.schema-box[data-schema-type=\"base\"]');
      schemaSubBoxes = [];
      for (const el of Array.from(document.querySelectorAll('.schema-box[data-schema-type=\"sub\"][data-subkey]'))) {{
        schemaSubBoxes.push({{sub_key: String(el.getAttribute('data-subkey') || ''), el: el}});
      }}
      if (schemaBaseBox) {{
        schemaBaseBox.addEventListener('click', (ev) => {{
          ev.preventDefault();
          if (indexKey && base && Object.prototype.hasOwnProperty.call(base, indexKey)) {{
            selectFlatKey(indexKey);
          }} else {{
            clearSelection();
            clearSchemaSelection();
            schemaBaseBox.classList.add('selected');
          }}
        }});
      }}
      for (const it of schemaSubBoxes) {{
        if (!it || !it.el) continue;
        it.el.addEventListener('click', (ev) => {{
          ev.preventDefault();
          const sk = String(it.el.getAttribute('data-subkey') || '');
          const idx = String(it.el.getAttribute('data-st-index') || '');
          if (sk) selectRawPath(sk);
          const det = document.getElementById('st-' + idx);
          if (det && det.tagName && String(det.tagName).toLowerCase() === 'details') {{
            det.open = true;
            det.scrollIntoView({{behavior: 'smooth', block: 'start'}});
          }}
        }});
      }}
      drawSchemaLines();
      setTimeout(drawSchemaLines, 0);

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

    function escXml(text) {{
      return String(text || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/\"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }}

    function wrapPath(text, maxChars, maxLines) {{
      const s = String(text || '');
      if (s.length <= maxChars) return [s];
      const parts = s.split('/');
      const lines = [];
      let i = 0;
      while (i < parts.length && lines.length < maxLines) {{
        let line = parts[i];
        i += 1;
        while (i < parts.length) {{
          const cand = line + '/' + parts[i];
          if (cand.length <= maxChars) {{
            line = cand;
            i += 1;
          }} else {{
            break;
          }}
        }}
        lines.push(line);
      }}
      if (!lines.length) return [s.slice(0, Math.max(0, maxChars - 1)) + '…'];
      if (i < parts.length) {{
        lines[lines.length - 1] = lines[lines.length - 1] + '…';
      }}
      for (let j = 0; j < lines.length; j++) {{
        if (String(lines[j]).length > maxChars) {{
          lines[j] = String(lines[j]).slice(0, Math.max(0, maxChars - 1)) + '…';
        }}
      }}
      return lines;
    }}

    function branchColors(branchType) {{
      const t = String(branchType || '');
      if (t === 'Dict') return {{fill: '#ddf4ff', stroke: '#0969da'}};
      if (t === 'List of Dict') return {{fill: '#dafbe1', stroke: '#1a7f37'}};
      if (t === 'List of Value') return {{fill: '#fff8c5', stroke: '#bf8700'}};
      if (t === 'Value in List of Dict') return {{fill: '#f6f8fa', stroke: '#6e7781'}};
      return {{fill: '#fbefff', stroke: '#8250df'}};
    }}

    function buildSchemaDiagramSvg(flat) {{
      const sep = String(META.key_sep || '__');
      const baseName = String((flat && (flat.base_table_sql || flat.base_table_original)) || (META.base_table || 'base'));
      const idxKey = String((flat && flat.index_key) || (META.index_key || 'id'));
      const baseCols = (flat && flat.base_row) ? Object.keys(flat.base_row).length : 0;
      const subs = (flat && Array.isArray(flat.subtables)) ? flat.subtables : [];

      const margin = 20;
      const baseW = 320;
      const baseH = 70;
      const subW = 460;
      const subH = 78;
      const gapY = 14;

      const subCount = subs.length;
      const totalSubH = subCount ? (subCount * subH + (subCount - 1) * gapY) : baseH;
      const contentH = Math.max(totalSubH, baseH);

      const baseX = margin;
      const subX = margin + baseW + 140;
      const baseY = margin + (contentH - baseH) / 2;
      const subY0 = margin + (contentH - totalSubH) / 2;

      const svgW = subX + subW + margin;
      const svgH = margin * 2 + contentH;

      function edgePath(x1, y1, x2, y2) {{
        const dist = x2 - x1;
        const dx = Math.max(60, dist * 0.55);
        const mx = x1 + dx;
        return 'M ' + x1 + ' ' + y1 + ' C ' + mx + ' ' + y1 + ', ' + mx + ' ' + y2 + ', ' + x2 + ' ' + y2;
      }}

      let svg = '';
      svg += '<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 ' + svgW + ' ' + svgH + '\" role=\"img\">';
      svg += '<style>' +
        '.edge{{stroke:#0969da;stroke-opacity:0.35;stroke-width:2;fill:none;}}' +
        '.box{{fill:#fff;stroke:#d0d7de;stroke-width:1.5;}}' +
        '.label{{font-size:12px;fill:#1f2328;font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial;}}' +
        '.meta{{font-size:11px;fill:#57606a;font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial;}}' +
        '</style>';

      const bx1 = baseX + baseW;
      const by1 = baseY + baseH / 2;
      for (let i = 0; i < subs.length; i++) {{
        const y = subY0 + i * (subH + gapY) + subH / 2;
        svg += '<path class=\"edge\" d=\"' + edgePath(bx1, by1, subX, y) + '\"></path>';
      }}

      const baseTitle = baseName.length > 42 ? (baseName.slice(0, 41) + '…') : baseName;
      svg += '<g class=\"diag-node\" data-subkey=\"\">' +
        '<rect class=\"box\" x=\"' + baseX + '\" y=\"' + baseY + '\" width=\"' + baseW + '\" height=\"' + baseH + '\" rx=\"12\" ry=\"12\"></rect>' +
        '<text class=\"label\" x=\"' + (baseX + 12) + '\" y=\"' + (baseY + 26) + '\">BASE</text>' +
        '<text class=\"label\" x=\"' + (baseX + 62) + '\" y=\"' + (baseY + 26) + '\">' + escXml(baseTitle) + '</text>' +
        '<text class=\"meta\" x=\"' + (baseX + 12) + '\" y=\"' + (baseY + 50) + '\">index: ' + escXml(idxKey) + ' · cols: ' + String(baseCols) + '</text>' +
      '</g>';

      for (let i = 0; i < subs.length; i++) {{
        const st = subs[i] || {{}};
        const subKey = String(st.sub_key || '');
        const title = String(st.table_sql || st.table_original || subKey || '');
        const rows = Number(st.n_rows || 0);
        const cols = Array.isArray(st.columns) ? st.columns.length : 0;

        const disp = 'SUB/' + (subKey ? subKey.split(sep).join('/') : title);
        const lines = wrapPath(disp, 40, 2);

        const x = subX;
        const y = subY0 + i * (subH + gapY);

        svg += '<g class=\"diag-node\" data-subkey=\"' + escXml(subKey) + '\">';
        svg += '<title>' + escXml(title || subKey) + '</title>';
        svg += '<rect class=\"box\" x=\"' + x + '\" y=\"' + y + '\" width=\"' + subW + '\" height=\"' + subH + '\" rx=\"12\" ry=\"12\"></rect>';
        svg += '<text class=\"label\" x=\"' + (x + 12) + '\" y=\"' + (y + 24) + '\">' + escXml(lines[0] || '') + '</text>';
        if (lines.length > 1) {{
          svg += '<text class=\"label\" x=\"' + (x + 12) + '\" y=\"' + (y + 40) + '\">' + escXml(lines[1] || '') + '</text>';
          svg += '<text class=\"meta\" x=\"' + (x + 12) + '\" y=\"' + (y + 64) + '\">rows: ' + String(rows) + ' · cols: ' + String(cols) + '</text>';
        }} else {{
          svg += '<text class=\"meta\" x=\"' + (x + 12) + '\" y=\"' + (y + 48) + '\">rows: ' + String(rows) + ' · cols: ' + String(cols) + '</text>';
          svg += '<text class=\"meta\" x=\"' + (x + 12) + '\" y=\"' + (y + 64) + '\">index: ' + escXml(idxKey) + '</text>';
        }}
        svg += '</g>';
      }}

      svg += '</svg>';
      return svg;
    }}

    function buildRawDiagramSvg(nodes, flat) {{
      const sep = String(META.key_sep || '__');
      const all = Array.isArray(nodes) ? nodes : [];
      const total = all.length || 0;
      const subs = (flat && Array.isArray(flat.subtables)) ? flat.subtables : [];

      const splitPaths = new Set();
      for (const st of subs) {{
        const sk = String(st && st.sub_key ? st.sub_key : '');
        if (sk) splitPaths.add(sk);
      }}

      const nodeBy = new Map();
      const children = new Map();
      for (const n of all) {{
        if (!n || typeof n !== 'object') continue;
        const p = String(n.path || '');
        const par = String(n.parent || '');
        nodeBy.set(p, n);
        if (par) {{
          if (!children.has(par)) children.set(par, []);
          children.get(par).push(p);
        }}
      }}
      for (const [k, arr] of children.entries()) {{
        arr.sort((a, b) => String(a).localeCompare(String(b)));
      }}

      const keep = new Set();
      keep.add('root');
      for (const c of (children.get('root') || [])) keep.add(c);

      for (const sk of splitPaths) {{
        keep.add(sk);
        let cur = sk;
        let safety = 0;
        while (cur && safety++ < 2000) {{
          const nn = nodeBy.get(cur);
          if (!nn) break;
          const par = String(nn.parent || '');
          if (!par) break;
          keep.add(par);
          cur = par;
        }}
      }}

      const maxNodes = 350;
      if (total <= maxNodes) {{
        for (const n of all) keep.add(String(n.path || ''));
      }} else {{
        const q = ['root'];
        const seen = new Set(q);
        while (q.length && keep.size < maxNodes) {{
          const p = String(q.shift() || '');
          const kids = children.get(p) || [];
          for (const c of kids) {{
            if (keep.size >= maxNodes) break;
            if (!keep.has(c)) keep.add(c);
            if (!seen.has(c)) {{
              q.push(c);
              seen.add(c);
            }}
          }}
        }}
      }}

      const kidsBy = new Map();
      for (const p of keep) {{
        const kids = children.get(String(p)) || [];
        const out = [];
        for (const c of kids) {{
          if (keep.has(c)) out.push(c);
        }}
        out.sort((a, b) => String(a).localeCompare(String(b)));
        kidsBy.set(String(p), out);
      }}

      const yPos = new Map();
      let leaf = 0;
      const visiting = new Set();
      const yStep = 26;
      function assignY(p) {{
        const key = String(p || '');
        if (yPos.has(key)) return yPos.get(key);
        if (visiting.has(key)) {{
          const y = leaf * yStep;
          leaf += 1;
          yPos.set(key, y);
          return y;
        }}
        visiting.add(key);
        const kids = kidsBy.get(key) || [];
        let y = 0;
        if (!kids.length) {{
          y = leaf * yStep;
          leaf += 1;
        }} else {{
          let sum = 0;
          for (const c of kids) sum += assignY(c);
          y = sum / kids.length;
        }}
        visiting.delete(key);
        yPos.set(key, y);
        return y;
      }}
      assignY('root');

      function depth(p) {{
        const key = String(p || '');
        if (!key || key === 'root') return 0;
        return key.split(sep).length;
      }}

      let maxDepth = 0;
      for (const p of keep) {{
        maxDepth = Math.max(maxDepth, depth(p));
      }}

      const margin = 20;
      const boxW = 150;
      const boxH = 22;
      const xStep = 180;
      const svgW = margin * 2 + (maxDepth + 1) * xStep + boxW;
      const svgH = margin * 2 + Math.max(1, leaf) * yStep + boxH;

      function nodeXY(p) {{
        const d = depth(p);
        const x = margin + d * xStep;
        const y = margin + (yPos.get(String(p)) || 0);
        return {{x: x, y: y}};
      }}

      function edgePath(x1, y1, x2, y2) {{
        const dist = x2 - x1;
        const dx = Math.max(40, dist * 0.55);
        const mx = x1 + dx;
        return 'M ' + x1 + ' ' + y1 + ' C ' + mx + ' ' + y1 + ', ' + mx + ' ' + y2 + ', ' + x2 + ' ' + y2;
      }}

      let svg = '';
      svg += '<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 ' + svgW + ' ' + svgH + '\" role=\"img\">';
      svg += '<style>' +
        '.edge{{stroke:#6e7781;stroke-opacity:0.35;stroke-width:1;fill:none;}}' +
        '.lbl{{font-size:12px;fill:#1f2328;font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial;}}' +
        '.tag{{font-size:11px;fill:#1f2328;font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial;}}' +
        '</style>';

      // edges
      for (const p of keep) {{
        const kids = kidsBy.get(String(p)) || [];
        if (!kids.length) continue;
        const a = nodeXY(p);
        for (const c of kids) {{
          const b = nodeXY(c);
          svg += '<path class=\"edge\" d=\"' + edgePath(a.x + boxW, a.y + boxH / 2, b.x, b.y + boxH / 2) + '\"></path>';
        }}
      }}

      const drawOrder = Array.from(keep).sort((a, b) => {{
        const da = depth(a);
        const db = depth(b);
        if (da !== db) return da - db;
        return String(a).localeCompare(String(b));
      }});

      for (const p of drawOrder) {{
        const n = nodeBy.get(String(p)) || {{}};
        const a = nodeXY(p);
        const colors = branchColors(n.branch_type);
        const isSplit = splitPaths.has(String(p));
        const strokeW = isSplit ? 2.5 : 1.5;
        const fill = colors.fill;
        let stroke = colors.stroke;
        if (String(n.status || '') === 'missing') stroke = '#cf222e';
        const opacity = n.excepted ? 0.75 : 1.0;
        const label = (n.label != null) ? String(n.label) : String(p);
        const text = label.length > 18 ? (label.slice(0, 17) + '…') : label;

        svg += '<g class=\"diag-node\" data-path=\"' + escXml(String(p)) + '\" opacity=\"' + String(opacity) + '\">';
        svg += '<title>' + escXml(String(p)) + '</title>';
        svg += '<rect x=\"' + a.x + '\" y=\"' + a.y + '\" width=\"' + boxW + '\" height=\"' + boxH + '\" rx=\"8\" ry=\"8\" fill=\"' + fill + '\" stroke=\"' + stroke + '\" stroke-width=\"' + strokeW + '\"></rect>';
        svg += '<text class=\"lbl\" x=\"' + (a.x + 8) + '\" y=\"' + (a.y + 15) + '\">' + escXml(text) + '</text>';
        if (isSplit) {{
          svg += '<text class=\"tag\" x=\"' + (a.x + boxW - 46) + '\" y=\"' + (a.y + 15) + '\">TABLE</text>';
        }}
        svg += '</g>';
      }}

      svg += '</svg>';
      return {{svg: svg, shown: keep.size, total: total, pruned: keep.size < total}};
    }}

    function clearDiagramSelection() {{
      for (const el of document.querySelectorAll('#diagram-card .diag-node.selected')) {{
        el.classList.remove('selected');
      }}
    }}

    function selectDiagramRaw(path) {{
      const sep = String(META.key_sep || '__');
      clearDiagramSelection();
      const p = String(path || '');
      const el = diagRawIndex.get(p);
      if (el) el.classList.add('selected');
      for (const [sk, it] of diagSchemaIndex.entries()) {{
        if (!sk || !it) continue;
        if (sk === p || sk.startsWith(p + sep) || p.startsWith(sk + sep)) {{
          it.classList.add('selected');
        }}
      }}
    }}

    function selectDiagramSchema(subKey) {{
      clearDiagramSelection();
      const sk = String(subKey || '');
      const el = diagSchemaIndex.get(sk);
      if (el) el.classList.add('selected');
      const rawEl = diagRawIndex.get(sk);
      if (rawEl) {{
        rawEl.classList.add('selected');
        try {{ rawEl.scrollIntoView({{behavior: 'smooth', block: 'center'}}); }} catch (e) {{}}
      }}
    }}

    function renderDiagram(pv) {{
      if (!diagramRaw || !diagramSchema) return;
      diagRawIndex = new Map();
      diagSchemaIndex = new Map();
      clearDiagramSelection();

      const rawNodes = (pv && pv.raw_nodes) ? pv.raw_nodes : [];
      const flat = (pv && pv.flatten) ? pv.flatten : {{}};

      const rr = buildRawDiagramSvg(rawNodes, flat);
      diagramRaw.innerHTML = rr && rr.svg ? rr.svg : '<div class=\"muted\">(no raw diagram)</div>';
      diagramSchema.innerHTML = buildSchemaDiagramSvg(flat);

      if (diagramStatus) {{
        const extra = rr && rr.pruned ? ' · pruned' : '';
        const trunc = pv && pv.raw_truncated ? ' · raw truncated' : '';
        diagramStatus.textContent = 'raw nodes shown: ' + String(rr && rr.shown ? rr.shown : 0) + ' / ' + String(rr && rr.total ? rr.total : 0) + extra + trunc + ' · tip: double-click a node to open expanded details (Shift: same tab)';
      }}

      for (const el of Array.from(diagramRaw.querySelectorAll('.diag-node[data-path]'))) {{
        const p = String(el.getAttribute('data-path') || '');
        if (p) diagRawIndex.set(p, el);
        el.addEventListener('click', (ev) => {{
          ev.preventDefault();
          const path = el.getAttribute('data-path') || '';
          selectDiagramRaw(path);
        }});
        el.addEventListener('dblclick', (ev) => {{
          ev.preventDefault();
          const path = el.getAttribute('data-path') || '';
          openExpandedFocus(path, ev.shiftKey);
        }});
      }}
      for (const el of Array.from(diagramSchema.querySelectorAll('.diag-node[data-subkey]'))) {{
        const sk = String(el.getAttribute('data-subkey') || '');
        if (sk) diagSchemaIndex.set(sk, el);
        el.addEventListener('click', (ev) => {{
          ev.preventDefault();
          const subKey = el.getAttribute('data-subkey') || '';
          if (subKey) selectDiagramSchema(subKey);
        }});
        el.addEventListener('dblclick', (ev) => {{
          ev.preventDefault();
          const subKey = el.getAttribute('data-subkey') || '';
          openExpandedFocus(subKey, ev.shiftKey);
        }});
      }}
    }}

    function renderAll(i) {{
      currentIndex = Math.max(0, Math.min(PREVIEWS.length - 1, Number(i || 0)));
      const pv = PREVIEWS[currentIndex] || {{}};
      if (isDiagramView) {{
        renderDiagram(pv);
        return;
      }}
      renderRaw(pv.raw_nodes || [], pv.flatten || {{}});
      renderFlat(pv.flatten || {{}}, pv.diff || null);
      renderDiff(pv.diff || null);
      applyQuery();
      if (isExpandedView) applyExpandedView();
    }}

    function init() {{
      if (!sel) return;
      const params = new URLSearchParams(window.location.search || '');
      const viewMode = String(params.get('view') || '');
      isExpandedView = viewMode === 'expanded';
      isDiagramView = viewMode === 'diagram';
      const initialRecord = params.get('record');
      const focus = params.get('focus');
      if (isDiagramView) {{
        try {{ document.body.classList.add('diagram-mode'); }} catch (e) {{}}
      }}
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
      if (unionQ) unionQ.addEventListener('input', applyUnionFilter);
      if (unionOnlyExc) unionOnlyExc.addEventListener('change', applyUnionFilter);
      if (unionCov) unionCov.addEventListener('input', applyUnionFilter);
      window.addEventListener('resize', () => requestAnimationFrame(drawSchemaLines));
      if (openExpanded) openExpanded.addEventListener('click', openExpandedView);
      if (openDiagram) openDiagram.addEventListener('click', openDiagramView);
      renderAll(initialRecord != null ? initialRecord : 0);
      try {{
        if (initialRecord != null) sel.value = String(initialRecord);
      }} catch (e) {{}}
      if (!isDiagramView) renderUnion();
      if (isExpandedView) {{
        setTimeout(() => {{
          applyExpandedView();
          drawSchemaLines();
          if (focus) focusToPath(focus);
        }}, 0);
      }} else if (focus && !isDiagramView) {{
        setTimeout(() => focusToPath(focus), 0);
      }}
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
    max_union_nodes: int = 20000,
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
        "max_union_nodes": int(max_union_nodes),
    }

    union = _compute_union_view(previews, key_sep=key_sep, except_keys=except_set, max_union_nodes=int(max_union_nodes))

    json_path = out_path / "preview.json"
    html_path = out_path / "preview.html"

    _write_text(json_path, json.dumps({"meta": meta, "previews": previews, "union": union}, ensure_ascii=False, indent=2))
    _write_text(html_path, render_review_preview_html(meta=meta, previews=previews, union=union))

    return {
        "out_dir": str(out_path),
        "preview_json": str(json_path),
        "preview_html": str(html_path),
    }
