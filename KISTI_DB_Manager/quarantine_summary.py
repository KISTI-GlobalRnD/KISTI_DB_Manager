from __future__ import annotations

import html
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_json(value: Any) -> Any:
    try:
        json.dumps(value)
        return value
    except TypeError:
        return repr(value)


def _truncate_text(value: Any, max_len: int = 500) -> str:
    s = str(value)
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def _record_hint(record: Any, *, max_keys: int = 30, max_text: int = 200) -> dict[str, Any]:
    if isinstance(record, dict):
        keys = list(record.keys())
        return {
            "type": "dict",
            "keys": keys[:max_keys],
            "keys_total": len(keys),
        }
    if isinstance(record, list):
        return {"type": "list", "len": len(record)}
    if record is None:
        return {"type": "null"}
    return {"type": type(record).__name__, "text": _truncate_text(record, max_len=max_text)}


def summarize_quarantine(
    path: str | Path,
    *,
    max_samples_per_stage: int = 3,
    max_entries: int | None = None,
) -> dict[str, Any]:
    """
    Stream a quarantine JSONL file and return an aggregate summary.

    This is designed to work on very large files with bounded memory:
    - Counts by stage / exception_type / (stage, exception_type)
    - Keeps only a small number of samples per stage
    """
    path = Path(path)
    max_entries = int(max_entries) if max_entries is not None and int(max_entries) > 0 else None

    total = 0
    parse_errors = 0
    stages: dict[str, int] = {}
    exc_types: dict[str, int] = {}
    stage_exc: dict[str, int] = {}
    samples_by_stage: dict[str, list[dict[str, Any]]] = {}
    first_ts: str | None = None
    last_ts: str | None = None

    with open(path, encoding="utf-8") as f:
        for line in f:
            if max_entries is not None and total >= max_entries:
                break
            raw = line.strip()
            if not raw:
                continue
            try:
                entry = json.loads(raw)
            except Exception:
                parse_errors += 1
                continue

            total += 1
            ts = entry.get("timestamp")
            if isinstance(ts, str):
                if first_ts is None:
                    first_ts = ts
                last_ts = ts

            stage = str(entry.get("stage") or "(unknown)")
            exc = entry.get("exception_type")
            exc = str(exc) if exc else "(none)"

            stages[stage] = int(stages.get(stage, 0)) + 1
            exc_types[exc] = int(exc_types.get(exc, 0)) + 1
            stage_exc_key = f"{stage}::{exc}"
            stage_exc[stage_exc_key] = int(stage_exc.get(stage_exc_key, 0)) + 1

            if int(max_samples_per_stage) > 0:
                bucket = samples_by_stage.setdefault(stage, [])
                if len(bucket) < int(max_samples_per_stage):
                    bucket.append(
                        {
                            "timestamp": entry.get("timestamp"),
                            "index": entry.get("index"),
                            "stage": stage,
                            "exception_type": entry.get("exception_type"),
                            "exception_message": _truncate_text(entry.get("exception_message"), max_len=300),
                            "context": _safe_json(entry.get("context") or {}),
                            "record": _record_hint(entry.get("record")),
                        }
                    )

    def top_items(d: Mapping[str, int], n: int = 50) -> list[tuple[str, int]]:
        return sorted(d.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))[:n]

    return {
        "generated_at": _utc_now_iso(),
        "path": str(path),
        "limited": max_entries is not None,
        "max_entries": max_entries,
        "total_entries": total,
        "parse_errors": parse_errors,
        "first_timestamp": first_ts,
        "last_timestamp": last_ts,
        "counts": {
            "by_stage": dict(stages),
            "by_exception_type": dict(exc_types),
            "by_stage_exception": dict(stage_exc),
        },
        "top": {
            "stages": top_items(stages, n=50),
            "exception_types": top_items(exc_types, n=50),
            "stage_exception": top_items(stage_exc, n=50),
        },
        "samples_by_stage": samples_by_stage,
    }


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def render_quarantine_markdown(summary: Mapping[str, Any]) -> str:
    total = int(summary.get("total_entries") or 0)
    limited = bool(summary.get("limited"))
    max_entries = summary.get("max_entries")
    parse_errors = int(summary.get("parse_errors") or 0)
    first_ts = summary.get("first_timestamp")
    last_ts = summary.get("last_timestamp")

    lines: list[str] = []
    lines.append("# Quarantine Summary")
    lines.append("")
    lines.append(f"- generated_at: `{summary.get('generated_at')}`")
    lines.append(f"- path: `{summary.get('path')}`")
    lines.append(f"- total_entries: `{total}`")
    if limited:
        lines.append(f"- note: limited to first `{max_entries}` entries")
    if parse_errors:
        lines.append(f"- parse_errors: `{parse_errors}`")
    if first_ts:
        lines.append(f"- first_timestamp: `{first_ts}`")
    if last_ts:
        lines.append(f"- last_timestamp: `{last_ts}`")
    lines.append("")

    def table(title: str, rows: Iterable[tuple[str, int]], headers: tuple[str, str] = ("key", "count")):
        lines.append(f"## {title}")
        lines.append("")
        lines.append(f"| {headers[0]} | {headers[1]} |")
        lines.append("|---|---:|")
        for k, v in rows:
            lines.append(f"| `{k}` | {int(v)} |")
        lines.append("")

    top = summary.get("top") or {}
    table("Stages", top.get("stages") or [])
    table("Exception Types", top.get("exception_types") or [], headers=("exception_type", "count"))
    table("Stage × Exception", top.get("stage_exception") or [], headers=("stage::exception", "count"))

    samples = summary.get("samples_by_stage") or {}
    if samples:
        lines.append("## Samples")
        lines.append("")
        for stage in sorted(samples.keys()):
            lines.append(f"### `{stage}`")
            lines.append("")
            for s in samples.get(stage) or []:
                lines.append("```json")
                lines.append(json.dumps(s, ensure_ascii=False, indent=2))
                lines.append("```")
                lines.append("")

    return "\n".join(lines) + "\n"


def render_quarantine_html(summary: Mapping[str, Any]) -> str:
    def h(x: Any) -> str:
        return html.escape(str(x))

    top = summary.get("top") or {}
    stages = top.get("stages") or []
    excs = top.get("exception_types") or []
    stage_excs = top.get("stage_exception") or []
    samples = summary.get("samples_by_stage") or {}

    def rows(items: list[tuple[str, int]]) -> str:
        if not items:
            return '<tr><td colspan="2" class="muted">(none)</td></tr>'
        return "".join([f"<tr><td><code>{h(k)}</code></td><td>{int(v)}</td></tr>" for k, v in items])

    samples_html = []
    for stage in sorted(samples.keys()):
        blocks = []
        for s in samples.get(stage) or []:
            blocks.append(f"<pre>{h(json.dumps(s, ensure_ascii=False, indent=2))}</pre>")
        samples_html.append(
            f"<details><summary><code>{h(stage)}</code> ({len(samples.get(stage) or [])} samples)</summary>"
            + "".join(blocks)
            + "</details>"
        )

    samples_section = "".join(samples_html) if samples_html else '<div class="muted">(none)</div>'

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Quarantine Summary</title>
  <style>
    body {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; margin: 24px; color: #1f2328; }}
    code {{ background: #f6f8fa; padding: 2px 5px; border-radius: 6px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #d0d7de; padding: 8px; font-size: 13px; }}
    th {{ background: #f6f8fa; text-align: left; }}
    .muted {{ color: #57606a; }}
    .card {{ border: 1px solid #d0d7de; border-radius: 12px; padding: 16px; margin: 16px 0; background: #ffffff; }}
    pre {{ background: #f6f8fa; padding: 12px; border-radius: 12px; overflow: auto; }}
    details summary {{ cursor: pointer; }}
  </style>
</head>
<body>
  <h1>Quarantine Summary</h1>
  <div class="card">
    <ul>
      <li><b>generated_at</b>: <code>{h(summary.get('generated_at'))}</code></li>
      <li><b>path</b>: <code>{h(summary.get('path'))}</code></li>
      <li><b>total_entries</b>: <code>{h(summary.get('total_entries'))}</code></li>
      <li><b>parse_errors</b>: <code>{h(summary.get('parse_errors'))}</code></li>
      <li><b>limited</b>: <code>{h(summary.get('limited'))}</code></li>
      <li><b>max_entries</b>: <code>{h(summary.get('max_entries') or '')}</code></li>
    </ul>
  </div>

  <div class="card">
    <h2>Stages</h2>
    <table><thead><tr><th>stage</th><th>count</th></tr></thead><tbody>{rows(stages)}</tbody></table>
  </div>

  <div class="card">
    <h2>Exception Types</h2>
    <table><thead><tr><th>exception_type</th><th>count</th></tr></thead><tbody>{rows(excs)}</tbody></table>
  </div>

  <div class="card">
    <h2>Stage × Exception</h2>
    <table><thead><tr><th>stage::exception</th><th>count</th></tr></thead><tbody>{rows(stage_excs)}</tbody></table>
  </div>

  <div class="card">
    <h2>Samples</h2>
    <div class="muted">Stored samples are truncated/hinted to avoid huge HTML.</div>
    {samples_section}
  </div>
</body>
</html>
"""


def write_quarantine_report(
    *,
    path: str,
    out_dir: str,
    formats: str = "md,html,json",
    max_samples: int = 3,
    max_entries: int | None = None,
) -> dict[str, Any]:
    fmt = {x.strip().lower() for x in str(formats).split(",") if x.strip()}
    summary = summarize_quarantine(path, max_samples_per_stage=max_samples, max_entries=max_entries)

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    md_path = out / "QUARANTINE.md"
    html_path = out / "quarantine.html"
    json_path = out / "quarantine_summary.json"

    if "json" in fmt:
        _write_text(json_path, json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        _write_text(json_path, json.dumps(summary, ensure_ascii=False, indent=2))

    if "md" in fmt:
        _write_text(md_path, render_quarantine_markdown(summary))
    else:
        _write_text(md_path, render_quarantine_markdown(summary))

    if "html" in fmt:
        _write_text(html_path, render_quarantine_html(summary))
    else:
        _write_text(html_path, render_quarantine_html(summary))

    return {
        "out_dir": str(out),
        "quarantine_md": str(md_path),
        "quarantine_html": str(html_path),
        "quarantine_json": str(json_path),
    }
